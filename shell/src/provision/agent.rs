use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentIdentity {
    pub protocol_version: u32,
    pub agent_id: String,
    pub hostname: String,
    pub machine_fingerprint: String,
    pub mount_root: String,
    pub paired_cluster: Option<String>,
    pub committed_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairRequest {
    pub cluster_id: String,
    pub commissioner_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairResponse {
    pub identity: AgentIdentity,
    pub credential: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLine {
    pub kind: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResponse {
    pub success: bool,
    pub message: String,
    #[serde(default)]
    pub lines: Vec<AgentLine>,
    #[serde(default)]
    pub data: Value,
}

impl AgentResponse {
    fn success(message: impl Into<String>, data: Value) -> Self {
        Self {
            success: true,
            message: message.into(),
            lines: Vec::new(),
            data,
        }
    }

    fn failure(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: message.into(),
            lines: Vec::new(),
            data: Value::Null,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum AgentCommand {
    Prerequisites,
    Inventory,
    InspectDisk {
        target: String,
    },
    FormatDisk {
        target: String,
        label: String,
        confirmation: String,
    },
    MountDisk {
        label: String,
    },
    Prepare {
        envelope: DeploymentEnvelope,
        host_id: String,
    },
    Commit {
        plan_hash: String,
    },
    Abort {
        plan_hash: String,
    },
    Service {
        target: ServiceTarget,
        action: AgentServiceAction,
    },
    Status {
        node_id: String,
        gateway: bool,
    },
    Logs {
        target: ServiceTarget,
        lines: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentServiceAction {
    Start,
    Stop,
    Restart,
    Enable,
    Disable,
}

impl AgentServiceAction {
    fn host_action(&self) -> ServiceAction {
        match self {
            Self::Start => ServiceAction::Start,
            Self::Stop => ServiceAction::Stop,
            Self::Restart => ServiceAction::Restart,
            Self::Enable => ServiceAction::Enable,
            Self::Disable => ServiceAction::Disable,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServiceTarget {
    Node { node_id: String },
    Gateway,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PairRecord {
    format_version: u32,
    cluster_id: String,
    commissioner_id: String,
    credential_sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LocalIdentity {
    format_version: u32,
    agent_id: String,
    machine_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StageManifest {
    format_version: u32,
    cluster_id: String,
    host_id: String,
    node_ids: Vec<String>,
    plan_hash: String,
    env_sha256: String,
    certificate_sha256: String,
    private_key_sha256: String,
    disks: Vec<DiskSpec>,
    staged_content_sha256: String,
}

pub struct AgentController {
    settings: InstallSettings,
    host: Arc<dyn HostOps>,
    setup_token_sha256: String,
    identity: LocalIdentity,
}

impl AgentController {
    pub fn new(
        settings: InstallSettings,
        setup_token: &str,
        host: Arc<dyn HostOps>,
    ) -> Result<Self, String> {
        fs::create_dir_all(&settings.setup_root).map_err(|error| {
            format!(
                "create setup root {}: {error}",
                settings.setup_root.display()
            )
        })?;
        let identity_path = settings.setup_root.join("agent_identity.json");
        let identity = if identity_path.exists() {
            read_json(&identity_path, "agent identity")?
        } else {
            let identity = LocalIdentity {
                format_version: AGENT_PROTOCOL_VERSION,
                agent_id: format!("agent-{}", Uuid::new_v4()),
                machine_fingerprint: machine_fingerprint(),
            };
            write_json(&identity_path, &identity, 0o600)?;
            identity
        };
        if identity.format_version != AGENT_PROTOCOL_VERSION {
            return Err(format!(
                "unsupported local agent identity format {}",
                identity.format_version
            ));
        }
        Ok(Self {
            settings,
            host,
            setup_token_sha256: sha256_hex(setup_token.as_bytes()),
            identity,
        })
    }

    pub fn identity(&self) -> AgentIdentity {
        let pair = self.load_pair().ok().flatten();
        AgentIdentity {
            protocol_version: AGENT_PROTOCOL_VERSION,
            agent_id: self.identity.agent_id.clone(),
            hostname: hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
            machine_fingerprint: self.identity.machine_fingerprint.clone(),
            mount_root: self.settings.mount_root.to_string_lossy().into_owned(),
            paired_cluster: pair.map(|record| record.cluster_id),
            committed_hash: read_trimmed(&self.committed_hash_path()).ok(),
        }
    }

    pub fn pair(&self, token: &str, request: PairRequest) -> Result<PairResponse, String> {
        if !constant_time_equal(
            sha256_hex(token.as_bytes()).as_bytes(),
            self.setup_token_sha256.as_bytes(),
        ) {
            return Err("invalid agent setup token".into());
        }
        validate_id(&request.cluster_id, "cluster ID")?;
        validate_id(&request.commissioner_id, "commissioner ID")?;
        if self.committed_hash_path().exists() {
            return Err(
                "this agent already has a committed N2 deployment; erase/reset it explicitly before pairing a different commissioner"
                    .into(),
            );
        }
        if let Some(existing) = self.load_pair()?
            && (existing.cluster_id != request.cluster_id
                || existing.commissioner_id != request.commissioner_id)
        {
            return Err(format!(
                "agent is already paired to cluster '{}' by commissioner '{}'",
                existing.cluster_id, existing.commissioner_id
            ));
        }
        let credential = random_secret();
        let record = PairRecord {
            format_version: AGENT_PROTOCOL_VERSION,
            cluster_id: request.cluster_id,
            commissioner_id: request.commissioner_id,
            credential_sha256: sha256_hex(credential.as_bytes()),
        };
        write_json(&self.pair_path(), &record, 0o600)?;
        Ok(PairResponse {
            identity: self.identity(),
            credential,
        })
    }

    pub(crate) fn authenticate(&self, credential: &str) -> Result<PairRecord, String> {
        let pair = self
            .load_pair()?
            .ok_or_else(|| "agent has not been paired".to_string())?;
        let observed = sha256_hex(credential.as_bytes());
        if !constant_time_equal(observed.as_bytes(), pair.credential_sha256.as_bytes()) {
            return Err("invalid agent credential".into());
        }
        Ok(pair)
    }

    pub(crate) fn execute(&self, pair: &PairRecord, command: AgentCommand) -> AgentResponse {
        match self.execute_inner(pair, command) {
            Ok(response) => response,
            Err(error) => AgentResponse::failure(error),
        }
    }

    fn execute_inner(
        &self,
        pair: &PairRecord,
        command: AgentCommand,
    ) -> Result<AgentResponse, String> {
        match command {
            AgentCommand::Prerequisites => Ok(AgentResponse::success(
                "prerequisites verified",
                serde_json::to_value(self.host.check_prerequisites()?)
                    .map_err(|error| error.to_string())?,
            )),
            AgentCommand::Inventory => Ok(AgentResponse::success(
                "disk inventory",
                serde_json::to_value(self.host.list_disks()?).map_err(|error| error.to_string())?,
            )),
            AgentCommand::InspectDisk { target } => {
                let disks = self.host.list_disks()?;
                let disk = find_disk(&disks, &target)
                    .ok_or_else(|| format!("disk '{target}' was not found on this node"))?;
                Ok(AgentResponse::success(
                    format_disk_details(disk),
                    serde_json::to_value(disk).map_err(|error| error.to_string())?,
                ))
            }
            AgentCommand::FormatDisk {
                target,
                label,
                confirmation,
            } => {
                self.ensure_local_services_stopped()?;
                if !valid_ndisk_label(&label) {
                    return Err("disk label must be N2DISK followed by exactly three digits".into());
                }
                let disks = self.host.list_disks()?;
                let disk = find_disk(&disks, &target)
                    .ok_or_else(|| format!("disk '{target}' was not found on this node"))?;
                let serial = disk_serial(disk);
                let expected = format!("FORMAT {} {label} {serial}", self.identity.agent_id);
                if confirmation != expected {
                    return Err(format!(
                        "format confirmation mismatch; expected '{expected}'"
                    ));
                }
                let mut lines = Vec::new();
                let result =
                    self.host
                        .format_disk(disk, &label, &self.settings, &mut |kind, text| {
                            lines.push(AgentLine {
                                kind: output_kind_name(kind).into(),
                                text,
                            })
                        })?;
                Ok(AgentResponse {
                    success: true,
                    message: format!(
                        "{} mounted at {} and remains unassigned",
                        result.label,
                        result.mount_path.display()
                    ),
                    lines,
                    data: json!({
                        "label": result.label,
                        "partition_path": result.partition_path,
                        "uuid": result.uuid,
                        "mount_path": result.mount_path,
                    }),
                })
            }
            AgentCommand::MountDisk { label } => {
                self.ensure_local_services_stopped()?;
                if !valid_ndisk_label(&label) {
                    return Err("disk label must be N2DISK followed by exactly three digits".into());
                }
                let disks = self.host.list_disks()?;
                let disk = find_disk(&disks, &label)
                    .ok_or_else(|| format!("formatted disk '{label}' was not found"))?;
                let mut lines = Vec::new();
                let result =
                    self.host
                        .mount_disk(disk, &label, &self.settings, &mut |kind, text| {
                            lines.push(AgentLine {
                                kind: output_kind_name(kind).into(),
                                text,
                            })
                        })?;
                Ok(AgentResponse {
                    success: true,
                    message: format!("{} ready at {}", result.label, result.mount_path.display()),
                    lines,
                    data: json!({"label": result.label, "uuid": result.uuid}),
                })
            }
            AgentCommand::Prepare { envelope, host_id } => {
                if envelope.spec.cluster_id != pair.cluster_id {
                    return Err("deployment cluster does not match paired cluster".into());
                }
                self.prepare(&envelope, &host_id)?;
                Ok(AgentResponse::success(
                    format!("host {host_id} prepared at hash {}", envelope.plan_hash),
                    json!({"plan_hash": envelope.plan_hash, "host_id": host_id}),
                ))
            }
            AgentCommand::Commit { plan_hash } => {
                self.commit(&plan_hash)?;
                Ok(AgentResponse::success(
                    format!("deployment {plan_hash} committed locally"),
                    json!({"plan_hash": plan_hash}),
                ))
            }
            AgentCommand::Abort { plan_hash } => {
                self.abort(&plan_hash)?;
                Ok(AgentResponse::success(
                    format!("staged deployment {plan_hash} removed"),
                    Value::Null,
                ))
            }
            AgentCommand::Service { target, action } => {
                self.ensure_committed()?;
                self.validate_service_target(&target)?;
                let service = self.service_name(&target);
                self.host.service_action(&service, action.host_action())?;
                Ok(AgentResponse::success(
                    format!(
                        "{} requested for {service}.service",
                        service_action_name(&action)
                    ),
                    json!({"service": service, "active": self.host.service_is_active(&service)}),
                ))
            }
            AgentCommand::Status { node_id, gateway } => {
                self.validate_service_target(&ServiceTarget::Node {
                    node_id: node_id.clone(),
                })?;
                if gateway {
                    self.validate_service_target(&ServiceTarget::Gateway)?;
                }
                let node_service = self.node_service(&node_id);
                Ok(AgentResponse::success(
                    "local service status",
                    json!({
                        "node_id": node_id,
                        "node_active": self.host.service_is_active(&node_service),
                        "gateway_active": gateway.then(|| self.host.service_is_active(&self.settings.gateway_service_name)),
                        "committed_hash": read_trimmed(&self.committed_hash_path()).ok(),
                    }),
                ))
            }
            AgentCommand::Logs { target, lines } => {
                self.validate_service_target(&target)?;
                let service = self.service_name(&target);
                Ok(AgentResponse::success(
                    self.host.recent_logs(&service, lines.clamp(1, 1000))?,
                    Value::Null,
                ))
            }
        }
    }

    fn prepare(&self, envelope: &DeploymentEnvelope, host_id: &str) -> Result<(), String> {
        self.host.require_privileged()?;
        self.host
            .identity_exists(&self.settings.run_user, &self.settings.run_group)?;
        ensure_executable(&self.settings.engine_binary, "N2 engine binary")?;
        self.ensure_local_services_stopped()?;
        envelope.validate_hash()?;
        envelope.spec.validate()?;
        let host = envelope
            .spec
            .hosts
            .get(host_id)
            .ok_or_else(|| format!("host '{host_id}' is not in the deployment"))?;
        if host.agent.agent_id != self.identity.agent_id
            || host.agent.machine_fingerprint != self.identity.machine_fingerprint
        {
            return Err(format!(
                "host '{host_id}' is bound to a different physical provisioning agent"
            ));
        }
        let local_nodes = envelope.spec.nodes_for_host(host_id);
        if local_nodes.is_empty() {
            return Err(format!("host '{host_id}' owns no N2 node instances"));
        }
        for node in &local_nodes {
            if self.host.service_is_active(&self.node_service(&node.id)) {
                return Err(format!(
                    "local N2 node '{}' must be stopped before prepare",
                    node.id
                ));
            }
        }
        if host.agent.mount_root != self.settings.mount_root.to_string_lossy() {
            return Err("the committed mount root differs from this agent's mount root".into());
        }
        self.validate_local_disks(&envelope.spec, host_id)?;
        if envelope.certificate.is_empty() || envelope.private_key.is_empty() {
            return Err("TLS certificate and private key cannot be empty".into());
        }
        validate_env(&envelope.env)?;

        let stage = self.stage_path(&envelope.plan_hash);
        if stage.exists() {
            fs::remove_dir_all(&stage)
                .map_err(|error| format!("clear staged deployment {}: {error}", stage.display()))?;
        }
        let membership = render_membership(&envelope.spec)?;
        let membership_bytes = serde_json::to_vec_pretty(&membership)
            .map_err(|error| format!("serialize membership: {error}"))?;
        for node in &local_nodes {
            let node_stage = stage.join("nodes").join(&node.id);
            let state_stage = node_stage.join("state");
            let cert_stage = node_stage.join("certificates");
            fs::create_dir_all(&state_stage).map_err(|error| error.to_string())?;
            fs::create_dir_all(&cert_stage).map_err(|error| error.to_string())?;
            atomic_write(
                &node_stage.join("n2_config.ini"),
                render_node_config(&envelope.spec, &node.id, &self.settings)?.as_bytes(),
                0o640,
            )?;
            atomic_write(
                &state_stage.join("n2_membership.json"),
                &membership_bytes,
                0o640,
            )?;
            atomic_write(&cert_stage.join("cert.pem"), &envelope.certificate, 0o640)?;
            atomic_write(&cert_stage.join("key.pem"), &envelope.private_key, 0o600)?;
        }
        atomic_write(&stage.join("n2.env"), envelope.env.as_bytes(), 0o600)?;
        atomic_write(
            &stage.join("node.service"),
            render_node_unit(&self.settings).as_bytes(),
            0o644,
        )?;

        if envelope.spec.gateway_for_host(host_id).is_some() {
            let gateway_stage = stage.join("gateway");
            fs::create_dir_all(gateway_stage.join("certificates"))
                .map_err(|error| error.to_string())?;
            atomic_write(
                &gateway_stage.join("n2_gateway_config.ini"),
                render_gateway_config(&envelope.spec, host_id, &self.settings)?.as_bytes(),
                0o640,
            )?;
            atomic_write(
                &gateway_stage.join("certificates/cert.pem"),
                &envelope.certificate,
                0o640,
            )?;
            atomic_write(
                &gateway_stage.join("certificates/key.pem"),
                &envelope.private_key,
                0o600,
            )?;
            atomic_write(
                &stage.join("gateway.service"),
                render_gateway_unit(&self.settings).as_bytes(),
                0o644,
            )?;
        }

        let staged_content_sha256 = stage_content_hash(&stage)?;

        let manifest = StageManifest {
            format_version: AGENT_PROTOCOL_VERSION,
            cluster_id: envelope.spec.cluster_id.clone(),
            host_id: host_id.to_string(),
            node_ids: local_nodes.iter().map(|node| node.id.clone()).collect(),
            plan_hash: envelope.plan_hash.clone(),
            env_sha256: sha256_hex(envelope.env.as_bytes()),
            certificate_sha256: sha256_hex(&envelope.certificate),
            private_key_sha256: sha256_hex(&envelope.private_key),
            disks: local_nodes
                .iter()
                .flat_map(|node| node.disks.values().cloned())
                .collect(),
            staged_content_sha256,
        };
        write_json(&stage.join("manifest.json"), &manifest, 0o600)
    }

    fn commit(&self, plan_hash: &str) -> Result<(), String> {
        validate_hash(plan_hash)?;
        if read_trimmed(&self.committed_hash_path()).ok().as_deref() == Some(plan_hash) {
            return Ok(());
        }
        self.ensure_local_services_stopped()?;
        let stage = self.stage_path(plan_hash);
        let manifest: StageManifest = read_json(&stage.join("manifest.json"), "stage manifest")?;
        if manifest.plan_hash != plan_hash {
            return Err("staged deployment hash does not match commit request".into());
        }
        if stage_content_hash(&stage)? != manifest.staged_content_sha256 {
            return Err("staged deployment content changed after prepare".into());
        }
        if stage.join("gateway").exists() {
            ensure_executable(&self.settings.gateway_binary, "N2 gateway binary")?;
        }

        for disk in &manifest.disks {
            let storage = self.settings.mount_path(&disk.label).join("n2");
            fs::create_dir_all(&storage)
                .map_err(|error| format!("create storage root {}: {error}", storage.display()))?;
            self.host.apply_ownership(
                &storage,
                &self.settings.run_user,
                &self.settings.run_group,
            )?;
        }
        for node_id in &manifest.node_ids {
            let live_node = self.settings.node_dir(node_id);
            let staged_node = stage.join("nodes").join(node_id);
            fs::create_dir_all(live_node.join("state")).map_err(|error| error.to_string())?;
            fs::create_dir_all(live_node.join("certificates"))
                .map_err(|error| error.to_string())?;
            copy_atomic(
                &staged_node.join("n2_config.ini"),
                &live_node.join("n2_config.ini"),
                0o640,
            )?;
            copy_atomic(
                &staged_node.join("state/n2_membership.json"),
                &live_node.join("state/n2_membership.json"),
                0o640,
            )?;
            copy_atomic(
                &staged_node.join("certificates/cert.pem"),
                &live_node.join("certificates/cert.pem"),
                0o640,
            )?;
            copy_atomic(
                &staged_node.join("certificates/key.pem"),
                &live_node.join("certificates/key.pem"),
                0o600,
            )?;
            self.host.apply_ownership(
                &live_node,
                &self.settings.run_user,
                &self.settings.run_group,
            )?;
        }
        copy_atomic(&stage.join("n2.env"), &self.settings.env_path, 0o600)?;
        copy_atomic(
            &stage.join("node.service"),
            &self.settings.node_unit_path(),
            0o644,
        )?;

        let gateway_stage = stage.join("gateway");
        if gateway_stage.exists() {
            fs::create_dir_all(self.settings.gateway_runtime.join("certificates"))
                .map_err(|error| error.to_string())?;
            copy_atomic(
                &gateway_stage.join("n2_gateway_config.ini"),
                &self.settings.gateway_runtime.join("n2_gateway_config.ini"),
                0o640,
            )?;
            copy_atomic(
                &gateway_stage.join("certificates/cert.pem"),
                &self.settings.gateway_runtime.join("certificates/cert.pem"),
                0o640,
            )?;
            copy_atomic(
                &gateway_stage.join("certificates/key.pem"),
                &self.settings.gateway_runtime.join("certificates/key.pem"),
                0o600,
            )?;
            copy_atomic(
                &stage.join("gateway.service"),
                &self.settings.gateway_unit_path(),
                0o644,
            )?;
            self.host.apply_ownership(
                &self.settings.gateway_runtime,
                &self.settings.run_user,
                &self.settings.run_group,
            )?;
        }
        self.host.reload_systemd()?;
        atomic_write(&self.committed_hash_path(), plan_hash.as_bytes(), 0o600)?;
        Ok(())
    }

    fn abort(&self, plan_hash: &str) -> Result<(), String> {
        validate_hash(plan_hash)?;
        if read_trimmed(&self.committed_hash_path()).ok().as_deref() == Some(plan_hash) {
            return Err("cannot abort a locally committed deployment".into());
        }
        let stage = self.stage_path(plan_hash);
        if stage.exists() {
            fs::remove_dir_all(&stage)
                .map_err(|error| format!("remove {}: {error}", stage.display()))?;
        }
        Ok(())
    }

    fn validate_local_disks(&self, spec: &ClusterSpec, host_id: &str) -> Result<(), String> {
        let local_nodes = spec.nodes_for_host(host_id);
        let disks = self.host.list_disks()?;
        for node in local_nodes {
            if node.disks.is_empty() {
                return Err(format!("node '{}' owns no storage disks", node.id));
            }
            for (label, binding) in &node.disks {
                let disk = find_disk(&disks, label).ok_or_else(|| {
                    format!("{label} is not visible on physical host '{host_id}'")
                })?;
                let expected_mount = self.settings.mount_path(label);
                if disk.filesystem.as_deref() != Some("xfs")
                    || !disk
                        .mountpoints
                        .iter()
                        .any(|mount| Path::new(mount) == expected_mount)
                {
                    return Err(format!(
                        "{label} must be mounted XFS at {} on physical host '{host_id}'",
                        expected_mount.display()
                    ));
                }
                if disk.uuid.as_deref() != Some(binding.filesystem_uuid.as_str()) {
                    return Err(format!(
                        "{label} UUID changed on physical host '{host_id}'; expected {}, observed {}",
                        binding.filesystem_uuid,
                        disk.uuid.as_deref().unwrap_or("none")
                    ));
                }
                if !binding.serial.is_empty() && disk.serial != binding.serial {
                    return Err(format!(
                        "{label} hardware serial changed on physical host '{host_id}'"
                    ));
                }
            }
        }
        Ok(())
    }

    fn ensure_local_services_stopped(&self) -> Result<(), String> {
        if self
            .host
            .service_is_active(&self.settings.gateway_service_name)
        {
            return Err("local N2 gateway must be stopped before provisioning".into());
        }
        let pair = self.load_pair()?.map(|record| record.cluster_id);
        if let Some(cluster) = pair {
            let plan_root = self.settings.runtime_root.join("nodes");
            if let Ok(entries) = fs::read_dir(plan_root) {
                for entry in entries.flatten() {
                    let node_id = entry.file_name().to_string_lossy().into_owned();
                    if self.host.service_is_active(&self.node_service(&node_id)) {
                        return Err(format!(
                            "local N2 node '{node_id}' must be stopped before provisioning cluster '{cluster}'"
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn ensure_committed(&self) -> Result<String, String> {
        read_trimmed(&self.committed_hash_path())
            .map_err(|_| "this physical node has no committed N2 deployment".into())
    }

    fn validate_service_target(&self, target: &ServiceTarget) -> Result<(), String> {
        let hash = self.ensure_committed()?;
        let manifest: StageManifest = read_json(
            &self.stage_path(&hash).join("manifest.json"),
            "stage manifest",
        )?;
        match target {
            ServiceTarget::Node { node_id } if manifest.node_ids.contains(node_id) => Ok(()),
            ServiceTarget::Node { node_id } => Err(format!(
                "physical agent owns nodes [{}], not '{node_id}'",
                manifest.node_ids.join(", ")
            )),
            ServiceTarget::Gateway if self.stage_path(&hash).join("gateway").exists() => Ok(()),
            ServiceTarget::Gateway => {
                Err("this physical agent does not own a committed gateway".into())
            }
        }
    }

    fn service_name(&self, target: &ServiceTarget) -> String {
        match target {
            ServiceTarget::Node { node_id } => self.node_service(node_id),
            ServiceTarget::Gateway => self.settings.gateway_service_name.clone(),
        }
    }

    fn node_service(&self, node_id: &str) -> String {
        format!("{}@{node_id}", self.settings.service_name)
    }

    fn pair_path(&self) -> PathBuf {
        self.settings.setup_root.join("agent_pair.json")
    }

    fn committed_hash_path(&self) -> PathBuf {
        self.settings.setup_root.join("committed.sha256")
    }

    fn stage_path(&self, hash: &str) -> PathBuf {
        self.settings.setup_root.join("staged").join(hash)
    }

    fn load_pair(&self) -> Result<Option<PairRecord>, String> {
        if !self.pair_path().exists() {
            return Ok(None);
        }
        read_json(&self.pair_path(), "agent pair").map(Some)
    }
}
