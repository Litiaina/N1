use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::thread;
use std::time::Duration;

use rand::{RngCore, rngs::OsRng};
use rayon::prelude::*;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::install::{
    BlockDisk, CommandOutcome, Confirmation, Emit, HostOps, InstallSettings, OutputKind,
    ServiceAction, atomic_write, find_disk, format_disk_details, human_bytes, render_gateway_unit,
    render_node_unit, valid_ndisk_label,
};

const AGENT_PROTOCOL_VERSION: u32 = 1;
const PLAN_FORMAT_VERSION: u32 = 2;
const MEMBERSHIP_FORMAT_VERSION: u32 = 5;

static PROVISION_HTTP_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(900))
        .build()
        .expect("build N2 provisioning HTTP client")
});

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
        node_id: String,
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
    node_id: String,
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
            AgentCommand::Prepare { envelope, node_id } => {
                if envelope.spec.cluster_id != pair.cluster_id {
                    return Err("deployment cluster does not match paired cluster".into());
                }
                self.prepare(&envelope, &node_id)?;
                Ok(AgentResponse::success(
                    format!("node {node_id} prepared at hash {}", envelope.plan_hash),
                    json!({"plan_hash": envelope.plan_hash, "node_id": node_id}),
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

    fn prepare(&self, envelope: &DeploymentEnvelope, node_id: &str) -> Result<(), String> {
        self.host.require_privileged()?;
        self.host
            .identity_exists(&self.settings.run_user, &self.settings.run_group)?;
        ensure_executable(&self.settings.engine_binary, "N2 engine binary")?;
        self.ensure_local_services_stopped()?;
        envelope.validate_hash()?;
        envelope.spec.validate()?;
        let node = envelope
            .spec
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("node '{node_id}' is not in the deployment"))?;
        if node.agent.agent_id != self.identity.agent_id
            || node.agent.machine_fingerprint != self.identity.machine_fingerprint
        {
            return Err(format!(
                "node '{node_id}' is bound to a different physical provisioning agent"
            ));
        }
        if self.host.service_is_active(&self.node_service(node_id)) {
            return Err(format!(
                "local N2 node '{node_id}' must be stopped before prepare"
            ));
        }
        if node.agent.mount_root != self.settings.mount_root.to_string_lossy() {
            return Err("the committed mount root differs from this agent's mount root".into());
        }
        self.validate_local_disks(&envelope.spec, node_id)?;
        if envelope.certificate.is_empty() || envelope.private_key.is_empty() {
            return Err("TLS certificate and private key cannot be empty".into());
        }
        validate_env(&envelope.env)?;

        let stage = self.stage_path(&envelope.plan_hash);
        if stage.exists() {
            fs::remove_dir_all(&stage)
                .map_err(|error| format!("clear staged deployment {}: {error}", stage.display()))?;
        }
        let node_stage = stage.join("node");
        let state_stage = node_stage.join("state");
        let cert_stage = node_stage.join("certificates");
        fs::create_dir_all(&state_stage).map_err(|error| error.to_string())?;
        fs::create_dir_all(&cert_stage).map_err(|error| error.to_string())?;

        let membership = render_membership(&envelope.spec)?;
        let membership_bytes = serde_json::to_vec_pretty(&membership)
            .map_err(|error| format!("serialize membership: {error}"))?;
        atomic_write(
            &node_stage.join("n2_config.ini"),
            render_node_config(&envelope.spec, node_id, &self.settings)?.as_bytes(),
            0o640,
        )?;
        atomic_write(
            &state_stage.join("n2_membership.json"),
            &membership_bytes,
            0o640,
        )?;
        atomic_write(&cert_stage.join("cert.pem"), &envelope.certificate, 0o640)?;
        atomic_write(&cert_stage.join("key.pem"), &envelope.private_key, 0o600)?;
        atomic_write(&stage.join("n2.env"), envelope.env.as_bytes(), 0o600)?;
        atomic_write(
            &stage.join("node.service"),
            render_node_unit(&self.settings).as_bytes(),
            0o644,
        )?;

        if envelope
            .spec
            .gateway_for_agent(&self.identity.agent_id)
            .is_some()
        {
            let gateway_stage = stage.join("gateway");
            fs::create_dir_all(gateway_stage.join("certificates"))
                .map_err(|error| error.to_string())?;
            atomic_write(
                &gateway_stage.join("gateway.ini"),
                render_gateway_config(&envelope.spec, &self.identity.agent_id, &self.settings)?
                    .as_bytes(),
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
            node_id: node_id.to_string(),
            plan_hash: envelope.plan_hash.clone(),
            env_sha256: sha256_hex(envelope.env.as_bytes()),
            certificate_sha256: sha256_hex(&envelope.certificate),
            private_key_sha256: sha256_hex(&envelope.private_key),
            disks: node.disks.values().cloned().collect(),
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

        let live_node = self.settings.node_dir(&manifest.node_id);
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
        fs::create_dir_all(live_node.join("state")).map_err(|error| error.to_string())?;
        fs::create_dir_all(live_node.join("certificates")).map_err(|error| error.to_string())?;
        copy_atomic(
            &stage.join("node/n2_config.ini"),
            &live_node.join("n2_config.ini"),
            0o640,
        )?;
        copy_atomic(
            &stage.join("node/state/n2_membership.json"),
            &live_node.join("state/n2_membership.json"),
            0o640,
        )?;
        copy_atomic(
            &stage.join("node/certificates/cert.pem"),
            &live_node.join("certificates/cert.pem"),
            0o640,
        )?;
        copy_atomic(
            &stage.join("node/certificates/key.pem"),
            &live_node.join("certificates/key.pem"),
            0o600,
        )?;
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
                &gateway_stage.join("gateway.ini"),
                &self.settings.gateway_runtime.join("gateway.ini"),
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
        self.host.apply_ownership(
            &live_node,
            &self.settings.run_user,
            &self.settings.run_group,
        )?;
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

    fn validate_local_disks(&self, spec: &ClusterSpec, node_id: &str) -> Result<(), String> {
        let node = spec.nodes.get(node_id).expect("validated node");
        let disks = self.host.list_disks()?;
        if node.disks.is_empty() {
            return Err(format!("node '{node_id}' owns no storage disks"));
        }
        for (label, binding) in &node.disks {
            let disk = find_disk(&disks, label)
                .ok_or_else(|| format!("{label} is not visible on physical node '{node_id}'"))?;
            let expected_mount = self.settings.mount_path(label);
            if disk.filesystem.as_deref() != Some("xfs")
                || !disk
                    .mountpoints
                    .iter()
                    .any(|mount| Path::new(mount) == expected_mount)
            {
                return Err(format!(
                    "{label} must be mounted XFS at {} on physical node '{node_id}'",
                    expected_mount.display()
                ));
            }
            if disk.uuid.as_deref() != Some(binding.filesystem_uuid.as_str()) {
                return Err(format!(
                    "{label} UUID changed on physical node '{node_id}'; expected {}, observed {}",
                    binding.filesystem_uuid,
                    disk.uuid.as_deref().unwrap_or("none")
                ));
            }
            if !binding.serial.is_empty() && disk.serial != binding.serial {
                return Err(format!(
                    "{label} hardware serial changed on physical node '{node_id}'"
                ));
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
            ServiceTarget::Node { node_id } if node_id == &manifest.node_id => Ok(()),
            ServiceTarget::Node { node_id } => Err(format!(
                "physical agent owns node '{}', not '{node_id}'",
                manifest.node_id
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentBinding {
    pub url: String,
    pub agent_id: String,
    pub machine_fingerprint: String,
    pub mount_root: String,
    pub credential: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeSpec {
    pub id: String,
    pub advertised_host: String,
    pub https_port: u16,
    pub native_port: u16,
    pub agent: AgentBinding,
    pub disks: BTreeMap<String, DiskSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskSpec {
    pub label: String,
    pub filesystem_uuid: String,
    pub stable_id: String,
    pub serial: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolSpec {
    pub id: String,
    pub generation: u64,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub node_ids: Vec<String>,
    pub writable: bool,
}

impl PoolSpec {
    fn width(&self) -> usize {
        self.data_shards.saturating_add(self.parity_shards)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewaySpec {
    pub id: String,
    pub node_id: String,
    pub bind_host: String,
    pub https_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSpec {
    pub format_version: u32,
    pub cluster_id: String,
    pub topology_epoch: u64,
    pub nodes: BTreeMap<String, NodeSpec>,
    pub pools: BTreeMap<String, PoolSpec>,
    pub reserve_nodes: BTreeSet<String>,
    pub gateways: BTreeMap<String, GatewaySpec>,
}

impl ClusterSpec {
    fn validate(&self) -> Result<(), String> {
        if self.format_version != PLAN_FORMAT_VERSION {
            return Err(format!(
                "unsupported Shell plan format {}",
                self.format_version
            ));
        }
        validate_id(&self.cluster_id, "cluster ID")?;
        if self.topology_epoch == 0 {
            return Err("topology epoch must be positive".into());
        }
        if self.pools.is_empty() {
            return Err("at least one server pool is required".into());
        }
        let mut claimed = BTreeSet::new();
        let mut generations = BTreeSet::new();
        for (pool_key, pool) in &self.pools {
            if pool_key != &pool.id {
                return Err(format!(
                    "pool map key '{pool_key}' does not match pool ID '{}'",
                    pool.id
                ));
            }
            validate_id(&pool.id, "pool ID")?;
            if pool.generation == 0 || !generations.insert(pool.generation) {
                return Err("every server pool requires a unique positive generation".into());
            }
            if pool.data_shards == 0 || pool.parity_shards == 0 {
                return Err(format!("pool '{}' requires K >= 1 and M >= 1", pool.id));
            }
            if pool.node_ids.len() != pool.width() {
                return Err(format!(
                    "pool '{}' EC {}+{} requires exactly {} nodes, but has {}",
                    pool.id,
                    pool.data_shards,
                    pool.parity_shards,
                    pool.width(),
                    pool.node_ids.len()
                ));
            }
            for node_id in &pool.node_ids {
                if !self.nodes.contains_key(node_id) {
                    return Err(format!(
                        "pool '{}' references unknown node '{node_id}'",
                        pool.id
                    ));
                }
                if !claimed.insert(node_id.clone()) {
                    return Err(format!("node '{node_id}' is assigned more than once"));
                }
            }
        }
        for node_id in &self.reserve_nodes {
            if !self.nodes.contains_key(node_id) {
                return Err(format!("reserve set references unknown node '{node_id}'"));
            }
            if !claimed.insert(node_id.clone()) {
                return Err(format!(
                    "node '{node_id}' cannot be both active and reserve"
                ));
            }
        }
        if claimed.len() != self.nodes.len() {
            let unassigned = self
                .nodes
                .keys()
                .filter(|node| !claimed.contains(*node))
                .cloned()
                .collect::<Vec<_>>();
            return Err(format!(
                "nodes must belong to exactly one server pool or the reserve set; unassigned: {}",
                unassigned.join(", ")
            ));
        }
        let mut labels = BTreeSet::new();
        let mut agent_ids = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for (node_key, node) in &self.nodes {
            if node_key != &node.id {
                return Err(format!(
                    "node map key '{node_key}' does not match node ID '{}'",
                    node.id
                ));
            }
            validate_id(&node.id, "node ID")?;
            validate_host(&node.advertised_host)?;
            if !agent_ids.insert(node.agent.agent_id.clone()) {
                return Err(format!(
                    "physical provisioning agent '{}' is assigned to more than one N2 node",
                    node.agent.agent_id
                ));
            }
            for port in [node.https_port, node.native_port] {
                if port == 0 || !endpoints.insert((node.advertised_host.clone(), port)) {
                    return Err(format!(
                        "node '{}' has an invalid or colliding advertised endpoint",
                        node.id
                    ));
                }
            }
            if node.disks.is_empty() {
                return Err(format!("node '{}' has no assigned storage disk", node.id));
            }
            for (label, disk) in &node.disks {
                if !valid_ndisk_label(label) || !labels.insert(label.clone()) {
                    return Err(format!("disk label '{label}' is invalid or duplicated"));
                }
                if disk.label != *label || disk.filesystem_uuid.is_empty() {
                    return Err(format!(
                        "disk binding '{label}' has inconsistent identity metadata"
                    ));
                }
            }
        }
        let mut gateway_agents = BTreeSet::new();
        for (gateway_key, gateway) in &self.gateways {
            if gateway_key != &gateway.id {
                return Err(format!(
                    "gateway map key '{gateway_key}' does not match gateway ID '{}'",
                    gateway.id
                ));
            }
            if !self.nodes.contains_key(&gateway.node_id) {
                return Err(format!(
                    "gateway '{}' references unknown node '{}'",
                    gateway.id, gateway.node_id
                ));
            }
            validate_host(&gateway.bind_host)?;
            let node = &self.nodes[&gateway.node_id];
            if gateway.https_port == 0
                || gateway.https_port == node.https_port
                || gateway.https_port == node.native_port
            {
                return Err(format!(
                    "gateway '{}' port collides with its local node",
                    gateway.id
                ));
            }
            if !gateway_agents.insert(node.agent.agent_id.clone()) {
                return Err(format!(
                    "physical agent '{}' hosts more than one gateway",
                    node.agent.agent_id
                ));
            }
        }
        Ok(())
    }

    fn gateway_for_agent(&self, agent_id: &str) -> Option<&GatewaySpec> {
        self.gateways.values().find(|gateway| {
            self.nodes
                .get(&gateway.node_id)
                .is_some_and(|node| node.agent.agent_id == agent_id)
        })
    }

    fn sanitized(&self) -> Self {
        let mut clone = self.clone();
        for node in clone.nodes.values_mut() {
            node.agent.credential.clear();
        }
        clone
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentEnvelope {
    pub spec: ClusterSpec,
    pub env: String,
    pub certificate: Vec<u8>,
    pub private_key: Vec<u8>,
    pub plan_hash: String,
}

impl DeploymentEnvelope {
    fn new(
        spec: ClusterSpec,
        env: String,
        certificate: Vec<u8>,
        private_key: Vec<u8>,
    ) -> Result<Self, String> {
        let spec = spec.sanitized();
        let plan_hash = deployment_hash(&spec, &env, &certificate, &private_key)?;
        Ok(Self {
            spec,
            env,
            certificate,
            private_key,
            plan_hash,
        })
    }

    fn validate_hash(&self) -> Result<(), String> {
        let actual = deployment_hash(&self.spec, &self.env, &self.certificate, &self.private_key)?;
        if actual != self.plan_hash {
            return Err("deployment content does not match its canonical SHA-256 hash".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommissionerPlan {
    spec: ClusterSpec,
    commissioner_id: String,
    tls_certificate_path: Option<String>,
    tls_private_key_path: Option<String>,
    env: String,
    committed_hash: Option<String>,
}

pub struct ProvisionShell {
    settings: InstallSettings,
    allow_shared_failure_domain: bool,
}

impl ProvisionShell {
    pub fn new(
        settings: InstallSettings,
        allow_shared_failure_domain: bool,
    ) -> Result<Self, String> {
        Ok(Self {
            settings,
            allow_shared_failure_domain,
        })
    }

    pub fn execute(
        &self,
        line: &str,
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> CommandOutcome {
        match self.execute_inner(line, confirmation, emit) {
            Ok(mut outcome) => {
                outcome.prompt_state = self.current_prompt_state();
                outcome
            }
            Err(error) => {
                emit(OutputKind::Error, error);
                CommandOutcome {
                    success: false,
                    prompt_state: self.current_prompt_state(),
                    ..CommandOutcome::default()
                }
            }
        }
    }

    pub fn current_prompt_state(&self) -> String {
        let Ok(plan) = self.load_plan() else {
            return "unconfigured".into();
        };
        if plan.committed_hash.is_some() {
            format!("{}:committed", plan.spec.cluster_id)
        } else {
            format!("{}:draft", plan.spec.cluster_id)
        }
    }

    fn execute_inner(
        &self,
        line: &str,
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let arguments = split_command(line)?;
        if arguments.is_empty() {
            return Ok(success_outcome());
        }
        match arguments[0].as_str() {
            "help" => {
                emit(OutputKind::Normal, HELP_TEXT.into());
                Ok(success_outcome())
            }
            "clear" => Ok(CommandOutcome {
                success: true,
                clear: true,
                ..CommandOutcome::default()
            }),
            "version" => {
                emit(
                    OutputKind::Normal,
                    format!(
                        "N2 Shell {} / agent protocol v{AGENT_PROTOCOL_VERSION}",
                        env!("CARGO_PKG_VERSION")
                    ),
                );
                Ok(success_outcome())
            }
            "cluster" => self.cluster_command(&arguments[1..], confirmation, emit),
            "node" => self.node_command(&arguments[1..], emit),
            "pool" => self.pool_command(&arguments[1..], emit),
            "reserve" => self.reserve_command(&arguments[1..], emit),
            "lsblk" => self.lsblk_command(&arguments[1..], emit),
            "disk" => self.disk_command(&arguments[1..], confirmation, emit),
            "config" => self.config_command(&arguments[1..], emit),
            "gateway" => self.gateway_command(&arguments[1..], emit),
            "logs" => self.logs_command(&arguments[1..], emit),
            command => Err(format!("unknown command '{command}'; type 'help'")),
        }
    }

    fn cluster_command(
        &self,
        arguments: &[String],
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        match arguments.first().map(String::as_str).unwrap_or("") {
            "init" => {
                let cluster_id = arguments.get(1).ok_or("usage: cluster init <cluster-id>")?;
                validate_id(cluster_id, "cluster ID")?;
                if self.plan_path().exists() {
                    return Err("a commissioner plan already exists; use 'cluster show'".into());
                }
                let commissioner_id = format!("commissioner-{}", Uuid::new_v4());
                let plan = CommissionerPlan {
                    spec: ClusterSpec {
                        format_version: PLAN_FORMAT_VERSION,
                        cluster_id: cluster_id.clone(),
                        topology_epoch: 1,
                        nodes: BTreeMap::new(),
                        pools: BTreeMap::new(),
                        reserve_nodes: BTreeSet::new(),
                        gateways: BTreeMap::new(),
                    },
                    commissioner_id,
                    tls_certificate_path: None,
                    tls_private_key_path: None,
                    env: generated_env(),
                    committed_hash: None,
                };
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("cluster '{cluster_id}' draft initialized; pair every physical node agent next"),
                );
                Ok(success_outcome())
            }
            "show" => {
                emit(OutputKind::Normal, self.plan_summary()?);
                Ok(success_outcome())
            }
            "validate" => {
                let plan = self.load_plan()?;
                self.validate_remote_plan(&plan, emit)?;
                Ok(success_outcome())
            }
            "commit" => {
                let mut plan = self.load_plan()?;
                self.validate_remote_plan(&plan, emit)?;
                let envelope = self.envelope(&plan)?;
                let expected = format!("COMMIT {} {}", plan.spec.cluster_id, envelope.plan_hash);
                if confirmation != Some(expected.as_str()) {
                    return Ok(CommandOutcome {
                        confirmation: Some(Confirmation {
                            prompt: format!(
                                "Prepare and commit topology {} to {} independent physical agents. Services remain stopped.\nType '{}' to continue:",
                                envelope.plan_hash,
                                plan.spec.nodes.len(),
                                expected
                            ),
                            expected,
                        }),
                        ..CommandOutcome::default()
                    });
                }
                self.distributed_commit(&plan, &envelope, emit)?;
                plan.committed_hash = Some(envelope.plan_hash.clone());
                self.save_plan(&plan)?;
                write_json(&self.last_committed_plan_path(), &plan, 0o600)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "deployment {} committed on every physical node; no engine or gateway was started",
                        envelope.plan_hash
                    ),
                );
                Ok(success_outcome())
            }
            "evolve" => {
                let mut plan = self.require_committed_plan()?;
                let epoch = parse_u64(
                    arguments.get(1),
                    "usage: cluster evolve <new-topology-epoch>",
                )?;
                if epoch <= plan.spec.topology_epoch {
                    return Err(format!(
                        "new topology epoch must be greater than {}",
                        plan.spec.topology_epoch
                    ));
                }
                write_json(&self.last_committed_plan_path(), &plan, 0o600)?;
                plan.spec.topology_epoch = epoch;
                plan.committed_hash = None;
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "topology epoch {epoch} opened as a draft; the currently committed cluster keeps running until a new prepare/commit succeeds"
                    ),
                );
                Ok(success_outcome())
            }
            "discard-draft" => {
                let plan = self.load_plan()?;
                if plan.committed_hash.is_some() {
                    return Err("there is no uncommitted topology draft to discard".into());
                }
                let committed: CommissionerPlan =
                    read_json(&self.last_committed_plan_path(), "last committed plan")?;
                self.save_plan(&committed)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "draft discarded; commissioner restored committed topology {}",
                        committed.committed_hash.as_deref().unwrap_or("unknown")
                    ),
                );
                Ok(success_outcome())
            }
            "start" => {
                let plan = self.require_committed_plan()?;
                self.cluster_start(&plan, emit)?;
                Ok(success_outcome())
            }
            "stop" => {
                let plan = self.runtime_plan()?;
                self.service_cluster(&plan, AgentServiceAction::Stop, true, emit)?;
                Ok(success_outcome())
            }
            "restart" => {
                let plan = self.require_committed_plan()?;
                self.service_cluster(&plan, AgentServiceAction::Stop, true, emit)?;
                self.cluster_start(&plan, emit)?;
                Ok(success_outcome())
            }
            "enable" | "disable" => {
                let plan = if arguments[0] == "enable" {
                    self.require_committed_plan()?
                } else {
                    self.runtime_plan()?
                };
                let action = if arguments[0] == "enable" {
                    AgentServiceAction::Enable
                } else {
                    AgentServiceAction::Disable
                };
                self.service_cluster(&plan, action, true, emit)?;
                Ok(success_outcome())
            }
            "status" => {
                emit(OutputKind::Normal, self.plan_summary()?);
                let plan = self.runtime_plan()?;
                self.cluster_status(&plan, emit)?;
                Ok(success_outcome())
            }
            _ => Err("usage: cluster <init|show|validate|commit|evolve|discard-draft|start|stop|restart|enable|disable|status>".into()),
        }
    }

    fn node_command(&self, arguments: &[String], emit: Emit<'_>) -> Result<CommandOutcome, String> {
        match arguments.first().map(String::as_str).unwrap_or("") {
            "pair" => {
                let usage = "usage: node pair <node-id> <agent-url> <agent-setup-token> <advertised-host> <https-port> <native-port>";
                let node_id = arguments.get(1).ok_or(usage)?;
                let agent_url = normalize_agent_url(arguments.get(2).ok_or(usage)?)?;
                let pair_token = arguments.get(3).ok_or(usage)?;
                let advertised_host = arguments.get(4).ok_or(usage)?;
                let https_port = parse_port(arguments.get(5), usage)?;
                let native_port = parse_port(arguments.get(6), usage)?;
                validate_id(node_id, "node ID")?;
                validate_host(advertised_host)?;
                if https_port == native_port {
                    return Err("HTTPS and native ports must differ".into());
                }
                let mut plan = self.load_plan()?;
                self.ensure_draft(&plan)?;
                if plan.spec.nodes.contains_key(node_id) {
                    return Err(format!(
                        "node '{node_id}' is already paired; node identity cannot be overwritten"
                    ));
                }
                let response = self.pair_agent(
                    &agent_url,
                    pair_token,
                    &PairRequest {
                        cluster_id: plan.spec.cluster_id.clone(),
                        commissioner_id: plan.commissioner_id.clone(),
                    },
                )?;
                if response.identity.protocol_version != AGENT_PROTOCOL_VERSION {
                    return Err(format!(
                        "agent protocol {} is incompatible with required protocol {}",
                        response.identity.protocol_version, AGENT_PROTOCOL_VERSION
                    ));
                }
                if !self.allow_shared_failure_domain
                    && plan.spec.nodes.values().any(|existing| {
                        existing.id != *node_id
                            && existing.agent.machine_fingerprint
                                == response.identity.machine_fingerprint
                    })
                {
                    return Err(
                        "another paired node has the same machine fingerprint; one physical host is one failure domain (use --allow-shared-failure-domain only for laboratory simulation)"
                            .into(),
                    );
                }
                for existing in plan.spec.nodes.values() {
                    if existing.id != *node_id
                        && existing.advertised_host == *advertised_host
                        && [existing.https_port, existing.native_port]
                            .iter()
                            .any(|port| *port == https_port || *port == native_port)
                    {
                        return Err(format!("endpoint collides with node '{}'", existing.id));
                    }
                }
                plan.spec.nodes.insert(
                    node_id.clone(),
                    NodeSpec {
                        id: node_id.clone(),
                        advertised_host: advertised_host.clone(),
                        https_port,
                        native_port,
                        agent: AgentBinding {
                            url: agent_url,
                            agent_id: response.identity.agent_id,
                            machine_fingerprint: response.identity.machine_fingerprint,
                            mount_root: response.identity.mount_root,
                            credential: response.credential,
                        },
                        disks: BTreeMap::new(),
                    },
                );
                plan.committed_hash = None;
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "paired physical {node_id} ({}) at https://{}:{https_port} / n2://{}:{native_port}",
                        response.identity.hostname, advertised_host, advertised_host
                    ),
                );
                Ok(success_outcome())
            }
            "list" => {
                let plan = self.load_plan()?;
                for node in plan.spec.nodes.values() {
                    emit(
                        OutputKind::Normal,
                        format!(
                            "{}  agent={}  host={}  disks={}  failure-domain={}",
                            node.id,
                            node.agent.url,
                            node.advertised_host,
                            if node.disks.is_empty() {
                                "—".into()
                            } else {
                                node.disks.keys().cloned().collect::<Vec<_>>().join(",")
                            },
                            &node.agent.machine_fingerprint
                                [..16.min(node.agent.machine_fingerprint.len())]
                        ),
                    );
                }
                Ok(success_outcome())
            }
            _ => Err("usage: node <pair|list> ...".into()),
        }
    }

    fn pool_command(&self, arguments: &[String], emit: Emit<'_>) -> Result<CommandOutcome, String> {
        let mut plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        match arguments.first().map(String::as_str).unwrap_or("") {
            "create" => {
                let usage = "usage: pool create <pool-id> <generation> <data> <parity>";
                let id = arguments.get(1).ok_or(usage)?;
                validate_id(id, "pool ID")?;
                let generation = parse_u64(arguments.get(2), usage)?;
                let data_shards = parse_usize(arguments.get(3), usage)?;
                let parity_shards = parse_usize(arguments.get(4), usage)?;
                if data_shards == 0 || parity_shards == 0 {
                    return Err("N2 EC requires at least 1 data and 1 parity node".into());
                }
                if plan.spec.pools.contains_key(id) {
                    return Err(format!(
                        "pool '{id}' already exists; static pool contracts cannot be overwritten"
                    ));
                }
                plan.spec.pools.insert(
                    id.clone(),
                    PoolSpec {
                        id: id.clone(),
                        generation,
                        data_shards,
                        parity_shards,
                        node_ids: Vec::new(),
                        writable: true,
                    },
                );
                plan.spec.topology_epoch = plan.spec.topology_epoch.max(generation);
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "pool {id} generation {generation} created with static EC {data_shards}+{parity_shards}"
                    ),
                );
                Ok(success_outcome())
            }
            "enroll" => {
                let pool_id = arguments
                    .get(1)
                    .ok_or("usage: pool enroll <pool-id> <node-id> [node-id ...]")?;
                if arguments.len() < 3 {
                    return Err("pool enroll requires at least one node ID".into());
                }
                let pool = plan
                    .spec
                    .pools
                    .get_mut(pool_id)
                    .ok_or_else(|| format!("pool '{pool_id}' is not defined"))?;
                for node_id in &arguments[2..] {
                    if !plan.spec.nodes.contains_key(node_id) {
                        return Err(format!("node '{node_id}' is not paired"));
                    }
                    if !pool.node_ids.contains(node_id) {
                        pool.node_ids.push(node_id.clone());
                    }
                }
                if pool.node_ids.len() > pool.width() {
                    return Err(format!(
                        "pool '{}' accepts exactly {} nodes",
                        pool.id,
                        pool.width()
                    ));
                }
                let count = pool.node_ids.len();
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("pool {pool_id} now has {count} enrolled node(s)"),
                );
                Ok(success_outcome())
            }
            "seal" | "writable" => {
                let pool_id = arguments
                    .get(1)
                    .ok_or("usage: pool <seal|writable> <pool-id>")?;
                let pool = plan
                    .spec
                    .pools
                    .get_mut(pool_id)
                    .ok_or_else(|| format!("pool '{pool_id}' is not defined"))?;
                pool.writable = arguments[0] == "writable";
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("pool {pool_id} state set to {}", arguments[0]),
                );
                Ok(success_outcome())
            }
            _ => Err("usage: pool <create|enroll|seal|writable> ...".into()),
        }
    }

    fn reserve_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let mut plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        match arguments.first().map(String::as_str).unwrap_or("") {
            "enroll" => {
                let node_id = arguments.get(1).ok_or("usage: reserve enroll <node-id>")?;
                if !plan.spec.nodes.contains_key(node_id) {
                    return Err(format!("node '{node_id}' is not paired"));
                }
                plan.spec.reserve_nodes.insert(node_id.clone());
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("node {node_id} enrolled in the cluster reserve set"),
                );
                Ok(success_outcome())
            }
            "remove" => {
                let node_id = arguments.get(1).ok_or("usage: reserve remove <node-id>")?;
                plan.spec.reserve_nodes.remove(node_id);
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("node {node_id} removed from the draft reserve set"),
                );
                Ok(success_outcome())
            }
            _ => Err("usage: reserve <enroll|remove> <node-id>".into()),
        }
    }

    fn lsblk_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let node_id = arguments.first().ok_or("usage: lsblk <node-id>")?;
        let plan = self.load_plan()?;
        let node = plan
            .spec
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("node '{node_id}' is not paired"))?;
        let response = self.agent_call(&node.agent, &AgentCommand::Inventory)?;
        ensure_agent_success(&response)?;
        let disks: Vec<BlockDisk> = serde_json::from_value(response.data)
            .map_err(|error| format!("decode {node_id} inventory: {error}"))?;
        emit(
            OutputKind::Info,
            format!("PHYSICAL NODE {node_id} / {}", node.agent.url),
        );
        emit(
            OutputKind::Normal,
            format!(
                "{:<12} {:>10}  {:<10} {:<12} {:<16} {}",
                "NAME", "SIZE", "FORMAT", "STATE", "LABEL", "MOUNT"
            ),
        );
        for disk in disks {
            emit(
                if disk.system {
                    OutputKind::Warning
                } else {
                    OutputKind::Normal
                },
                format!(
                    "{:<12} {:>10}  {:<10} {:<12} {:<16} {}",
                    disk.name,
                    human_bytes(disk.size_bytes),
                    disk.filesystem.as_deref().unwrap_or("—"),
                    disk.state(),
                    disk.label.as_deref().unwrap_or("—"),
                    if disk.mountpoints.is_empty() {
                        "—".into()
                    } else {
                        disk.mountpoints.join(",")
                    }
                ),
            );
        }
        Ok(success_outcome())
    }

    fn disk_command(
        &self,
        arguments: &[String],
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        match arguments.first().map(String::as_str).unwrap_or("") {
            "inspect" => {
                let usage = "usage: disk inspect <node-id> <device-or-label>";
                let node = plan
                    .spec
                    .nodes
                    .get(arguments.get(1).ok_or(usage)?)
                    .ok_or_else(|| "physical node is not paired".to_string())?;
                let target = arguments.get(2).ok_or(usage)?;
                let response = self.agent_call(
                    &node.agent,
                    &AgentCommand::InspectDisk {
                        target: target.clone(),
                    },
                )?;
                ensure_agent_success(&response)?;
                emit(OutputKind::Normal, response.message);
                Ok(success_outcome())
            }
            "format" => {
                let usage = "usage: disk format <node-id> <device> <N2DISKxxx>";
                let node_id = arguments.get(1).ok_or(usage)?;
                let node = plan
                    .spec
                    .nodes
                    .get(node_id)
                    .ok_or_else(|| format!("node '{node_id}' is not paired"))?;
                let target = arguments.get(2).ok_or(usage)?;
                let label = arguments.get(3).ok_or(usage)?;
                if !valid_ndisk_label(label) {
                    return Err("disk label must be N2DISK followed by exactly three digits".into());
                }
                if plan
                    .spec
                    .nodes
                    .values()
                    .any(|existing| existing.disks.contains_key(label))
                {
                    return Err(format!(
                        "disk label {label} is already assigned in this cluster draft"
                    ));
                }
                let inventory = self.inventory(node)?;
                let disk = find_disk(&inventory, target)
                    .ok_or_else(|| format!("disk '{target}' was not found on {node_id}"))?;
                if disk.system || disk.read_only || !disk.mountpoints.is_empty() {
                    return Err(format!("{} is not a safe formatting candidate", disk.path));
                }
                let expected = format!(
                    "FORMAT {} {label} {}",
                    node.agent.agent_id,
                    disk_serial(disk)
                );
                if confirmation != Some(expected.as_str()) {
                    return Ok(CommandOutcome {
                        confirmation: Some(Confirmation {
                            prompt: format!(
                                "WARNING: destroy signatures on physical node {node_id}, device {}, model '{}', serial '{}'.\nType '{}' to continue:",
                                disk.path,
                                disk.model,
                                disk_serial(disk),
                                expected
                            ),
                            expected,
                        }),
                        ..CommandOutcome::default()
                    });
                }
                let response = self.agent_call(
                    &node.agent,
                    &AgentCommand::FormatDisk {
                        target: target.clone(),
                        label: label.clone(),
                        confirmation: expected,
                    },
                )?;
                emit_agent_response(&response, emit)?;
                Ok(success_outcome())
            }
            "mount" => {
                let usage = "usage: disk mount <node-id> <N2DISKxxx>";
                let node_id = arguments.get(1).ok_or(usage)?;
                let node = plan
                    .spec
                    .nodes
                    .get(node_id)
                    .ok_or_else(|| format!("node '{node_id}' is not paired"))?;
                let label = arguments.get(2).ok_or(usage)?;
                let response = self.agent_call(
                    &node.agent,
                    &AgentCommand::MountDisk {
                        label: label.clone(),
                    },
                )?;
                emit_agent_response(&response, emit)?;
                Ok(success_outcome())
            }
            "assign" => {
                let usage = "usage: disk assign <node-id> <N2DISKxxx>";
                let node_id = arguments.get(1).ok_or(usage)?;
                let label = arguments.get(2).ok_or(usage)?;
                if !valid_ndisk_label(label) {
                    return Err("invalid N2 disk label".into());
                }
                if plan
                    .spec
                    .nodes
                    .values()
                    .any(|node| node.id != *node_id && node.disks.contains_key(label))
                {
                    return Err(format!("{label} belongs to another physical node"));
                }
                let node = plan
                    .spec
                    .nodes
                    .get(node_id)
                    .ok_or_else(|| format!("node '{node_id}' is not paired"))?;
                let inventory = self.inventory(node)?;
                let disk = find_disk(&inventory, label)
                    .ok_or_else(|| format!("{label} is not visible on {node_id}"))?;
                let expected_mount = Path::new(&node.agent.mount_root).join(label);
                if disk.filesystem.as_deref() != Some("xfs")
                    || !disk
                        .mountpoints
                        .iter()
                        .any(|mount| Path::new(mount) == expected_mount)
                {
                    return Err(format!(
                        "{label} is not mounted XFS at {} on {node_id}",
                        expected_mount.display()
                    ));
                }
                let mut plan = plan;
                let filesystem_uuid = disk
                    .uuid
                    .clone()
                    .ok_or_else(|| format!("{label} has no filesystem UUID"))?;
                plan.spec
                    .nodes
                    .get_mut(node_id)
                    .expect("present")
                    .disks
                    .insert(
                        label.clone(),
                        DiskSpec {
                            label: label.clone(),
                            filesystem_uuid,
                            stable_id: disk.stable_id.clone(),
                            serial: disk.serial.clone(),
                            size_bytes: disk.size_bytes,
                        },
                    );
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("{label} assigned to physical node {node_id}"),
                );
                Ok(success_outcome())
            }
            "unassign" => {
                let usage = "usage: disk unassign <node-id> <N2DISKxxx>";
                let node_id = arguments.get(1).ok_or(usage)?;
                let label = arguments.get(2).ok_or(usage)?;
                let mut plan = plan;
                let node = plan
                    .spec
                    .nodes
                    .get_mut(node_id)
                    .ok_or_else(|| format!("node '{node_id}' is not paired"))?;
                if node.disks.remove(label).is_none() {
                    return Err(format!("{label} is not assigned to {node_id}"));
                }
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("{label} unassigned from {node_id}"),
                );
                Ok(success_outcome())
            }
            _ => Err("usage: disk <inspect|format|mount|assign|unassign> ...".into()),
        }
    }

    fn config_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let mut plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        if arguments.first().map(String::as_str) != Some("tls") {
            return Err("usage: config tls <certificate> <private-key>".into());
        }
        let certificate = absolute_path(
            arguments
                .get(1)
                .ok_or("usage: config tls <certificate> <private-key>")?,
        )?;
        let private_key = absolute_path(
            arguments
                .get(2)
                .ok_or("usage: config tls <certificate> <private-key>")?,
        )?;
        ensure_regular_file(&certificate, "TLS certificate")?;
        ensure_regular_file(&private_key, "TLS private key")?;
        plan.tls_certificate_path = Some(certificate.to_string_lossy().into_owned());
        plan.tls_private_key_path = Some(private_key.to_string_lossy().into_owned());
        self.save_plan(&plan)?;
        emit(
            OutputKind::Success,
            "shared TLS sources configured for distributed staging".into(),
        );
        Ok(success_outcome())
    }

    fn gateway_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let mut plan = self.load_plan()?;
        self.ensure_draft(&plan)?;
        match arguments.first().map(String::as_str).unwrap_or("") {
            "add" => {
                let usage = "usage: gateway add <gateway-id> <node-id> <bind-host> <https-port>";
                let id = arguments.get(1).ok_or(usage)?;
                let node_id = arguments.get(2).ok_or(usage)?;
                let bind_host = arguments.get(3).ok_or(usage)?;
                let https_port = parse_port(arguments.get(4), usage)?;
                validate_id(id, "gateway ID")?;
                validate_host(bind_host)?;
                if plan.spec.gateways.contains_key(id) {
                    return Err(format!(
                        "gateway '{id}' already exists; remove it from this draft before redefining it"
                    ));
                }
                if !plan.spec.nodes.contains_key(node_id) {
                    return Err(format!("node '{node_id}' is not paired"));
                }
                if plan
                    .spec
                    .gateways
                    .values()
                    .any(|gateway| gateway.node_id == *node_id && gateway.id != *id)
                {
                    return Err(format!("physical node '{node_id}' already hosts a gateway"));
                }
                plan.spec.gateways.insert(
                    id.clone(),
                    GatewaySpec {
                        id: id.clone(),
                        node_id: node_id.clone(),
                        bind_host: bind_host.clone(),
                        https_port,
                    },
                );
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "gateway {id} assigned to {node_id} at https://{bind_host}:{https_port}"
                    ),
                );
                Ok(success_outcome())
            }
            "remove" => {
                let id = arguments
                    .get(1)
                    .ok_or("usage: gateway remove <gateway-id>")?;
                plan.spec.gateways.remove(id);
                self.save_plan(&plan)?;
                emit(
                    OutputKind::Success,
                    format!("gateway {id} removed from draft"),
                );
                Ok(success_outcome())
            }
            _ => Err("usage: gateway <add|remove> ...".into()),
        }
    }

    fn logs_command(&self, arguments: &[String], emit: Emit<'_>) -> Result<CommandOutcome, String> {
        let usage = "usage: logs <node-id|gateway-id> [count]";
        let target = arguments.first().ok_or(usage)?;
        let lines = arguments
            .get(1)
            .map(|value| value.parse::<usize>())
            .transpose()
            .map_err(|_| usage)?
            .unwrap_or(100);
        let plan = self.load_plan()?;
        let (node, service_target) = if let Some(node) = plan.spec.nodes.get(target) {
            (
                node,
                ServiceTarget::Node {
                    node_id: node.id.clone(),
                },
            )
        } else if let Some(gateway) = plan.spec.gateways.get(target) {
            (
                plan.spec
                    .nodes
                    .get(&gateway.node_id)
                    .expect("validated gateway node"),
                ServiceTarget::Gateway,
            )
        } else {
            return Err(format!("unknown node or gateway '{target}'"));
        };
        let response = self.agent_call(
            &node.agent,
            &AgentCommand::Logs {
                target: service_target,
                lines,
            },
        )?;
        emit_agent_response(&response, emit)?;
        Ok(success_outcome())
    }

    fn validate_remote_plan(&self, plan: &CommissionerPlan, emit: Emit<'_>) -> Result<(), String> {
        plan.spec.validate()?;
        if !self.allow_shared_failure_domain {
            let distinct = plan
                .spec
                .nodes
                .values()
                .map(|node| &node.agent.machine_fingerprint)
                .collect::<BTreeSet<_>>();
            if distinct.len() != plan.spec.nodes.len() {
                return Err("multiple N2 nodes share one physical machine fingerprint; this is not an independent failure-domain topology".into());
            }
        }
        let certificate = plan
            .tls_certificate_path
            .as_ref()
            .ok_or("TLS certificate is not configured")?;
        let private_key = plan
            .tls_private_key_path
            .as_ref()
            .ok_or("TLS private key is not configured")?;
        ensure_regular_file(Path::new(certificate), "TLS certificate")?;
        ensure_regular_file(Path::new(private_key), "TLS private key")?;
        validate_env(&plan.env)?;

        let results = plan
            .spec
            .nodes
            .par_iter()
            .map(|(node_id, node)| {
                let result = self
                    .agent_call(&node.agent, &AgentCommand::Inventory)
                    .and_then(|response| {
                        ensure_agent_success(&response)?;
                        let disks: Vec<BlockDisk> = serde_json::from_value(response.data)
                            .map_err(|error| format!("decode inventory: {error}"))?;
                        for (label, binding) in &node.disks {
                            let disk = find_disk(&disks, label)
                                .ok_or_else(|| format!("{label} is absent"))?;
                            let expected = Path::new(&node.agent.mount_root).join(label);
                            if disk.filesystem.as_deref() != Some("xfs")
                                || !disk
                                    .mountpoints
                                    .iter()
                                    .any(|mount| Path::new(mount) == expected)
                            {
                                return Err(format!(
                                    "{label} is not mounted XFS at {}",
                                    expected.display()
                                ));
                            }
                            if disk.uuid.as_deref() != Some(binding.filesystem_uuid.as_str()) {
                                return Err(format!("{label} filesystem UUID changed"));
                            }
                            if !binding.serial.is_empty() && disk.serial != binding.serial {
                                return Err(format!("{label} hardware serial changed"));
                            }
                        }
                        Ok(())
                    });
                (node_id.clone(), result)
            })
            .collect::<Vec<_>>();
        let failures = results
            .into_iter()
            .filter_map(|(node, result)| result.err().map(|error| format!("{node}: {error}")))
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            return Err(format!(
                "physical-node validation failed:\n  {}",
                failures.join("\n  ")
            ));
        }
        let envelope = self.envelope(plan)?;
        emit(
            OutputKind::Success,
            format!(
                "Validation: PASS\nCluster: {}\nTopology hash: {}\nServer pools: {}\nPhysical nodes: {}\nPhysical failure domains: {}\nStorage disks: {}\nReserve nodes: {}\nGateways: {}",
                plan.spec.cluster_id,
                envelope.plan_hash,
                plan.spec.pools.len(),
                plan.spec.nodes.len(),
                plan.spec
                    .nodes
                    .values()
                    .map(|node| &node.agent.machine_fingerprint)
                    .collect::<BTreeSet<_>>()
                    .len(),
                plan.spec
                    .nodes
                    .values()
                    .map(|node| node.disks.len())
                    .sum::<usize>(),
                plan.spec.reserve_nodes.len(),
                plan.spec.gateways.len(),
            ),
        );
        Ok(())
    }

    fn distributed_commit(
        &self,
        plan: &CommissionerPlan,
        envelope: &DeploymentEnvelope,
        emit: Emit<'_>,
    ) -> Result<(), String> {
        emit(
            OutputKind::Info,
            format!(
                "[1/2] Preparing {} physical nodes in parallel",
                plan.spec.nodes.len()
            ),
        );
        let prepared = plan
            .spec
            .nodes
            .par_iter()
            .map(|(node_id, node)| {
                let result = self
                    .agent_call(
                        &node.agent,
                        &AgentCommand::Prepare {
                            envelope: envelope.clone(),
                            node_id: node_id.clone(),
                        },
                    )
                    .and_then(|response| ensure_agent_success(&response));
                (node_id.clone(), result)
            })
            .collect::<Vec<_>>();
        let failures = prepared
            .iter()
            .filter_map(|(node, result)| {
                result
                    .as_ref()
                    .err()
                    .map(|error| format!("{node}: {error}"))
            })
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            plan.spec.nodes.par_iter().for_each(|(_, node)| {
                let _ = self.agent_call(
                    &node.agent,
                    &AgentCommand::Abort {
                        plan_hash: envelope.plan_hash.clone(),
                    },
                );
            });
            return Err(format!(
                "prepare failed; every successful stage was aborted:\n  {}",
                failures.join("\n  ")
            ));
        }
        emit(
            OutputKind::Success,
            "[1/2] Every physical node acknowledged the same topology hash".into(),
        );

        emit(
            OutputKind::Info,
            "[2/2] Committing prepared local state in parallel".into(),
        );
        let committed = plan
            .spec
            .nodes
            .par_iter()
            .map(|(node_id, node)| {
                let result = self
                    .agent_call(
                        &node.agent,
                        &AgentCommand::Commit {
                            plan_hash: envelope.plan_hash.clone(),
                        },
                    )
                    .and_then(|response| ensure_agent_success(&response));
                (node_id.clone(), result)
            })
            .collect::<Vec<_>>();
        let failures = committed
            .into_iter()
            .filter_map(|(node, result)| result.err().map(|error| format!("{node}: {error}")))
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            return Err(format!(
                "commit acknowledgements are incomplete; no service was activated. Re-run the identical commit to converge idempotently:\n  {}",
                failures.join("\n  ")
            ));
        }
        emit(
            OutputKind::Success,
            "[2/2] Every physical node committed; activation remains separate".into(),
        );
        Ok(())
    }

    fn cluster_start(&self, plan: &CommissionerPlan, emit: Emit<'_>) -> Result<(), String> {
        self.service_cluster(plan, AgentServiceAction::Start, false, emit)?;
        thread::sleep(Duration::from_millis(1000));
        let statuses = self.status_results(plan);
        let unhealthy = statuses
            .iter()
            .filter_map(|(node, result)| match result {
                Ok(value) if value.get("node_active").and_then(Value::as_bool) == Some(true) => {
                    None
                }
                Ok(_) => Some(format!("{node}: service inactive")),
                Err(error) => Some(format!("{node}: {error}")),
            })
            .collect::<Vec<_>>();
        if !unhealthy.is_empty() {
            let _ = self.service_cluster(plan, AgentServiceAction::Stop, false, emit);
            return Err(format!(
                "node activation failed; all reachable nodes were stopped and gateways were never started:\n  {}",
                unhealthy.join("\n  ")
            ));
        }
        for gateway in plan.spec.gateways.values() {
            let node = plan
                .spec
                .nodes
                .get(&gateway.node_id)
                .expect("validated gateway");
            let response = self.agent_call(
                &node.agent,
                &AgentCommand::Service {
                    target: ServiceTarget::Gateway,
                    action: AgentServiceAction::Start,
                },
            )?;
            ensure_agent_success(&response)?;
            emit(
                OutputKind::Success,
                format!("gateway '{}' started on {}", gateway.id, gateway.node_id),
            );
        }
        Ok(())
    }

    fn service_cluster(
        &self,
        plan: &CommissionerPlan,
        action: AgentServiceAction,
        include_gateways: bool,
        emit: Emit<'_>,
    ) -> Result<(), String> {
        if include_gateways
            && matches!(
                action,
                AgentServiceAction::Stop | AgentServiceAction::Disable
            )
        {
            for gateway in plan.spec.gateways.values() {
                let node = plan
                    .spec
                    .nodes
                    .get(&gateway.node_id)
                    .expect("validated gateway");
                let response = self.agent_call(
                    &node.agent,
                    &AgentCommand::Service {
                        target: ServiceTarget::Gateway,
                        action: action.clone(),
                    },
                )?;
                ensure_agent_success(&response)?;
            }
        }
        let results = plan
            .spec
            .nodes
            .par_iter()
            .map(|(node_id, node)| {
                let result = self
                    .agent_call(
                        &node.agent,
                        &AgentCommand::Service {
                            target: ServiceTarget::Node {
                                node_id: node_id.clone(),
                            },
                            action: action.clone(),
                        },
                    )
                    .and_then(|response| ensure_agent_success(&response));
                (node_id.clone(), result)
            })
            .collect::<Vec<_>>();
        let failures = results
            .into_iter()
            .filter_map(|(node, result)| result.err().map(|error| format!("{node}: {error}")))
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            return Err(format!(
                "cluster service action incomplete:\n  {}",
                failures.join("\n  ")
            ));
        }
        if include_gateways
            && matches!(
                action,
                AgentServiceAction::Start
                    | AgentServiceAction::Restart
                    | AgentServiceAction::Enable
            )
        {
            for gateway in plan.spec.gateways.values() {
                let node = plan
                    .spec
                    .nodes
                    .get(&gateway.node_id)
                    .expect("validated gateway");
                let response = self.agent_call(
                    &node.agent,
                    &AgentCommand::Service {
                        target: ServiceTarget::Gateway,
                        action: action.clone(),
                    },
                )?;
                ensure_agent_success(&response)?;
            }
        }
        emit(
            OutputKind::Success,
            format!(
                "{} completed across {} physical nodes",
                service_action_name(&action),
                plan.spec.nodes.len()
            ),
        );
        Ok(())
    }

    fn cluster_status(&self, plan: &CommissionerPlan, emit: Emit<'_>) -> Result<(), String> {
        for (node_id, status) in self.status_results(plan) {
            match status {
                Ok(value) => {
                    let active = value
                        .get("node_active")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    emit(
                        if active {
                            OutputKind::Success
                        } else {
                            OutputKind::Warning
                        },
                        format!("{node_id}: {}", if active { "active" } else { "inactive" }),
                    );
                }
                Err(error) => emit(
                    OutputKind::Error,
                    format!("{node_id}: agent unreachable: {error}"),
                ),
            }
        }
        for gateway in plan.spec.gateways.values() {
            let node = plan.spec.nodes.get(&gateway.node_id).expect("gateway node");
            let response = self.agent_call(
                &node.agent,
                &AgentCommand::Status {
                    node_id: node.id.clone(),
                    gateway: true,
                },
            );
            match response {
                Ok(response) if response.success => {
                    let active = response
                        .data
                        .get("gateway_active")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    emit(
                        if active {
                            OutputKind::Success
                        } else {
                            OutputKind::Warning
                        },
                        format!(
                            "gateway {} on {}: {}",
                            gateway.id,
                            gateway.node_id,
                            if active { "active" } else { "inactive" }
                        ),
                    );
                }
                Ok(response) => emit(
                    OutputKind::Error,
                    format!("gateway {}: {}", gateway.id, response.message),
                ),
                Err(error) => emit(
                    OutputKind::Error,
                    format!("gateway {}: {error}", gateway.id),
                ),
            }
        }
        Ok(())
    }

    fn status_results(&self, plan: &CommissionerPlan) -> Vec<(String, Result<Value, String>)> {
        plan.spec
            .nodes
            .par_iter()
            .map(|(node_id, node)| {
                let gateway = plan
                    .spec
                    .gateways
                    .values()
                    .any(|item| item.node_id == *node_id);
                let result = self
                    .agent_call(
                        &node.agent,
                        &AgentCommand::Status {
                            node_id: node_id.clone(),
                            gateway,
                        },
                    )
                    .and_then(|response| {
                        ensure_agent_success(&response)?;
                        Ok(response.data)
                    });
                (node_id.clone(), result)
            })
            .collect()
    }

    fn inventory(&self, node: &NodeSpec) -> Result<Vec<BlockDisk>, String> {
        let response = self.agent_call(&node.agent, &AgentCommand::Inventory)?;
        ensure_agent_success(&response)?;
        serde_json::from_value(response.data).map_err(|error| format!("decode inventory: {error}"))
    }

    fn pair_agent(
        &self,
        url: &str,
        token: &str,
        request: &PairRequest,
    ) -> Result<PairResponse, String> {
        let response = PROVISION_HTTP_CLIENT
            .post(format!("{url}/agent/v1/pair"))
            .header("x-n2-setup-token", token)
            .json(request)
            .send()
            .map_err(|error| format!("pair agent {url}: {error}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "agent pairing failed ({}): {}",
                response.status(),
                response.text().unwrap_or_default()
            ));
        }
        response
            .json()
            .map_err(|error| format!("decode pair response: {error}"))
    }

    fn agent_call(
        &self,
        binding: &AgentBinding,
        command: &AgentCommand,
    ) -> Result<AgentResponse, String> {
        let response = PROVISION_HTTP_CLIENT
            .post(format!("{}/agent/v1/command", binding.url))
            .bearer_auth(&binding.credential)
            .json(command)
            .send()
            .map_err(|error| format!("agent {} request failed: {error}", binding.url))?;
        if !response.status().is_success() {
            return Err(format!(
                "agent {} returned HTTP {}: {}",
                binding.url,
                response.status(),
                response.text().unwrap_or_default()
            ));
        }
        response
            .json()
            .map_err(|error| format!("decode agent {} response: {error}", binding.url))
    }

    fn envelope(&self, plan: &CommissionerPlan) -> Result<DeploymentEnvelope, String> {
        let certificate = fs::read(
            plan.tls_certificate_path
                .as_ref()
                .ok_or("TLS certificate is not configured")?,
        )
        .map_err(|error| format!("read TLS certificate: {error}"))?;
        let private_key = fs::read(
            plan.tls_private_key_path
                .as_ref()
                .ok_or("TLS private key is not configured")?,
        )
        .map_err(|error| format!("read TLS private key: {error}"))?;
        DeploymentEnvelope::new(
            plan.spec.clone(),
            plan.env.clone(),
            certificate,
            private_key,
        )
    }

    fn plan_summary(&self) -> Result<String, String> {
        let plan = self.load_plan()?;
        let mut output = format!(
            "N2 DISTRIBUTED CLUSTER PLAN\n  cluster: {} / epoch {}\n  commissioner: {}\n  state: {}\n  physical nodes: {}\n  server pools: {}\n  reserve nodes: {}\n  gateways: {}",
            plan.spec.cluster_id,
            plan.spec.topology_epoch,
            plan.commissioner_id,
            if plan.committed_hash.is_some() {
                "committed"
            } else {
                "draft"
            },
            plan.spec.nodes.len(),
            plan.spec.pools.len(),
            plan.spec.reserve_nodes.len(),
            plan.spec.gateways.len(),
        );
        for pool in plan.spec.pools.values() {
            output.push_str(&format!(
                "\n  pool {} generation {} / EC {}+{} / {}/{} nodes / {}",
                pool.id,
                pool.generation,
                pool.data_shards,
                pool.parity_shards,
                pool.node_ids.len(),
                pool.width(),
                if pool.writable { "writable" } else { "sealed" }
            ));
        }
        for node in plan.spec.nodes.values() {
            output.push_str(&format!(
                "\n  {}  agent={}  https://{}:{}  n2://{}:{}  disks=[{}]",
                node.id,
                node.agent.url,
                node.advertised_host,
                node.https_port,
                node.advertised_host,
                node.native_port,
                node.disks.keys().cloned().collect::<Vec<_>>().join(",")
            ));
        }
        if let Some(hash) = plan.committed_hash {
            output.push_str(&format!("\n  committed hash: {hash}"));
        }
        Ok(output)
    }

    fn ensure_draft(&self, plan: &CommissionerPlan) -> Result<(), String> {
        if plan.committed_hash.is_some() {
            return Err("the cluster plan is committed; topology evolution requires a new staged epoch, not mutation of this committed plan".into());
        }
        Ok(())
    }

    fn require_committed_plan(&self) -> Result<CommissionerPlan, String> {
        let plan = self.load_plan()?;
        if plan.committed_hash.is_none() {
            return Err("run 'cluster commit' before activation".into());
        }
        Ok(plan)
    }

    fn runtime_plan(&self) -> Result<CommissionerPlan, String> {
        let plan = self.load_plan()?;
        if plan.committed_hash.is_some() {
            return Ok(plan);
        }
        read_json(&self.last_committed_plan_path(), "last committed plan")
    }

    fn plan_path(&self) -> PathBuf {
        self.settings
            .setup_root
            .join("n2_cluster_commissioner.json")
    }

    fn last_committed_plan_path(&self) -> PathBuf {
        self.settings
            .setup_root
            .join("n2_cluster_last_committed.json")
    }

    fn load_plan(&self) -> Result<CommissionerPlan, String> {
        read_json(&self.plan_path(), "commissioner plan")
    }

    fn save_plan(&self, plan: &CommissionerPlan) -> Result<(), String> {
        write_json(&self.plan_path(), plan, 0o600)
    }
}

fn render_membership(spec: &ClusterSpec) -> Result<Value, String> {
    spec.validate()?;
    let pools = spec
        .pools
        .values()
        .map(|pool| {
            json!({
                "id": pool.id,
                "generation": pool.generation,
                "policy": {
                    "object_data_shards": pool.data_shards,
                    "object_parity_shards": pool.parity_shards,
                },
                "state": if pool.writable { "writable" } else { "sealed" },
                "slot_ids": pool.node_ids,
            })
        })
        .collect::<Vec<_>>();
    let active_ids = spec
        .pools
        .values()
        .flat_map(|pool| pool.node_ids.iter())
        .collect::<BTreeSet<_>>();
    let bindings = active_ids
        .into_iter()
        .map(|node_id| {
            let node = spec.nodes.get(node_id).expect("validated active node");
            json!({
                "slot_id": node.id,
                "instance_id": node.id,
                "binding_epoch": 1,
                "endpoint": format!("https://{}:{}", node.advertised_host, node.https_port),
                "native_endpoint": format!("n2://{}:{}", node.advertised_host, node.native_port),
                "state": "online",
                "weight": 1,
            "storage_roots": node.disks.keys().map(|label| json!({
                    "id": root_id(&node.id, label),
                    "path": Path::new(&node.agent.mount_root).join(label).join("n2"),
                })).collect::<Vec<_>>(),
                "joined_epoch": 1,
            })
        })
        .collect::<Vec<_>>();
    let reserves = spec
        .reserve_nodes
        .iter()
        .map(|node_id| {
            let node = spec.nodes.get(node_id).expect("validated reserve");
            json!({
                "instance_id": node.id,
                "endpoint": format!("https://{}:{}", node.advertised_host, node.https_port),
                "native_endpoint": format!("n2://{}:{}", node.advertised_host, node.native_port),
                "state": "online",
            "storage_roots": node.disks.keys().map(|label| json!({
                    "id": root_id(&node.id, label),
                    "path": Path::new(&node.agent.mount_root).join(label).join("n2"),
                })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "format_version": MEMBERSHIP_FORMAT_VERSION,
        "cluster_id": spec.cluster_id,
        "epoch": spec.topology_epoch,
        "binding_epoch": 1,
        "pools": pools,
        "slot_bindings": bindings,
        "reserve_nodes": reserves,
        "retired_instances": [],
    }))
}

fn render_node_config(
    spec: &ClusterSpec,
    node_id: &str,
    settings: &InstallSettings,
) -> Result<String, String> {
    let node = spec
        .nodes
        .get(node_id)
        .ok_or_else(|| format!("unknown node '{node_id}'"))?;
    let pool = spec
        .pools
        .values()
        .find(|pool| pool.node_ids.contains(&node_id.to_string()));
    let (pool_id, generation, data, parity) = if let Some(pool) = pool {
        (
            pool.id.as_str(),
            pool.generation,
            pool.data_shards,
            pool.parity_shards,
        )
    } else {
        let first = spec
            .pools
            .values()
            .next()
            .ok_or("no server pool configured")?;
        (
            first.id.as_str(),
            first.generation,
            first.data_shards,
            first.parity_shards,
        )
    };
    let roots = node
        .disks
        .keys()
        .map(|label| {
            format!(
                "{}={}",
                root_id(node_id, label),
                settings.mount_path(label).join("n2").display()
            )
        })
        .collect::<Vec<_>>();
    let node_dir = settings.node_dir(node_id);
    Ok(format!(
        "[server]\nhost=0.0.0.0\nhttps_port={}\nnative_port={}\ncert_path={}\nkey_path={}\n\n[disk_config]\nstorage_roots={}\nruntime_path={}\n\n[cluster_config]\nnode_id={}\npool_id={}\npool_generation={}\nnode_endpoint=https://{}:{}\nnative_endpoint=n2://{}:{}\nmembership_file={}\nobject_data_shards={}\nobject_parity_shards={}\nauto_rebalance=false\n\n[garbage_collector]\ncleanup_interval_seconds=3600\nfile_ttl_seconds=3600\nretention_seconds=2592000\n\n[auto_healing]\nenabled=true\ninitial_delay_seconds=30\nscan_interval_seconds=900\ncoordinator_nodes=auto\nmax_parallel_fragments=2\nmax_objects_per_fragment_cycle=256\nmax_duration_seconds=30\nlease_seconds=90\nchecksum_scan_every_cycles=24\n\n[set_limits]\nmax_file_size=5368709120\n\n[jwt_token_config]\nlogin_token_expiration=900\n\n[rate_limiter_config]\nenable_rate_limiting=true\nauth_limit_per_second=2\nauth_limit_burst_size=5\nview_limit_per_second=30\nview_limit_burst_size=100\nupload_limit_per_second=300\nupload_limit_burst_size=3000\nglobal_limit_per_second=200\nglobal_limit_burst_size=300\n\n[upload_config]\nmax_concurrent_io=auto\nadaptive_concurrency_enabled=true\nadaptive_concurrency_interval_ms=auto\nwriter_buffer_size=auto\nupload_memory_budget_bytes=auto\nupload_part_strict_durability_default=false\nupload_io_gate_enabled=true\nstream_max_concurrency=auto\nstream_chunk_size=auto\nstream_readahead_bytes=auto\nstream_locality_bias_enabled=true\nmonitor_interval_seconds=auto\nmonitor_deep_scan_interval_seconds=auto\nmax_part_size=52428800\ndata_shards={}\nparity_shards={}\nstripe_block_size=262144\n",
        node.https_port,
        node.native_port,
        node_dir.join("certificates/cert.pem").display(),
        node_dir.join("certificates/key.pem").display(),
        roots.join(","),
        node_dir.join("state").display(),
        node.id,
        pool_id,
        generation,
        node.advertised_host,
        node.https_port,
        node.advertised_host,
        node.native_port,
        node_dir.join("state/n2_membership.json").display(),
        data,
        parity,
        data,
        parity,
    ))
}

fn render_gateway_config(
    spec: &ClusterSpec,
    agent_id: &str,
    settings: &InstallSettings,
) -> Result<String, String> {
    let gateway = spec
        .gateway_for_agent(agent_id)
        .ok_or("gateway is not assigned")?;
    let mut output = format!(
        "[server]\nhost={}\nhttps_port={}\ncert_path={}\nkey_path={}\n\n[routing]\nhealth_interval_seconds=5\nrequest_timeout_seconds=300\n\n",
        gateway.bind_host,
        gateway.https_port,
        settings
            .gateway_runtime
            .join("certificates/cert.pem")
            .display(),
        settings
            .gateway_runtime
            .join("certificates/key.pem")
            .display(),
    );
    for node in spec.nodes.values() {
        output.push_str(&format!(
            "[node.{}]\nid={}\nhttp_url=https://{}:{}\nnative_url=n2://{}:{}\nweight=1\n\n",
            node.id,
            node.id,
            node.advertised_host,
            node.https_port,
            node.advertised_host,
            node.native_port
        ));
    }
    Ok(output)
}

fn deployment_hash(
    spec: &ClusterSpec,
    env: &str,
    certificate: &[u8],
    private_key: &[u8],
) -> Result<String, String> {
    let canonical = json!({
        "spec": spec.sanitized(),
        "env_sha256": sha256_hex(env.as_bytes()),
        "certificate_sha256": sha256_hex(certificate),
        "private_key_sha256": sha256_hex(private_key),
    });
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| format!("serialize canonical deployment: {error}"))?;
    Ok(sha256_hex(&bytes))
}

fn ensure_agent_success(response: &AgentResponse) -> Result<(), String> {
    if response.success {
        Ok(())
    } else {
        Err(response.message.clone())
    }
}

fn emit_agent_response(response: &AgentResponse, emit: Emit<'_>) -> Result<(), String> {
    ensure_agent_success(response)?;
    for line in &response.lines {
        emit(
            match line.kind.as_str() {
                "success" => OutputKind::Success,
                "warning" => OutputKind::Warning,
                "error" => OutputKind::Error,
                "info" => OutputKind::Info,
                _ => OutputKind::Normal,
            },
            line.text.clone(),
        );
    }
    emit(OutputKind::Success, response.message.clone());
    Ok(())
}

fn output_kind_name(kind: OutputKind) -> &'static str {
    match kind {
        OutputKind::Normal => "normal",
        OutputKind::Info => "info",
        OutputKind::Success => "success",
        OutputKind::Warning => "warning",
        OutputKind::Error => "error",
    }
}

fn service_action_name(action: &AgentServiceAction) -> &'static str {
    match action {
        AgentServiceAction::Start => "start",
        AgentServiceAction::Stop => "stop",
        AgentServiceAction::Restart => "restart",
        AgentServiceAction::Enable => "enable",
        AgentServiceAction::Disable => "disable",
    }
}

fn write_json<T: Serialize>(path: &Path, value: &T, mode: u32) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| format!("serialize {}: {error}", path.display()))?;
    atomic_write(path, &bytes, mode)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path, label: &str) -> Result<T, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("read {label} {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse {label} {}: {error}", path.display()))
}

fn copy_atomic(source: &Path, target: &Path, mode: u32) -> Result<(), String> {
    let bytes =
        fs::read(source).map_err(|error| format!("read staged {}: {error}", source.display()))?;
    atomic_write(target, &bytes, mode)
}

fn stage_content_hash(stage: &Path) -> Result<String, String> {
    let mut relative_paths = vec![
        "n2.env",
        "node.service",
        "node/n2_config.ini",
        "node/state/n2_membership.json",
        "node/certificates/cert.pem",
        "node/certificates/key.pem",
    ];
    if stage.join("gateway").exists() {
        relative_paths.extend([
            "gateway.service",
            "gateway/gateway.ini",
            "gateway/certificates/cert.pem",
            "gateway/certificates/key.pem",
        ]);
    }
    let mut hasher = Sha256::new();
    for relative in relative_paths {
        hasher.update(relative.as_bytes());
        hasher.update([0]);
        let bytes = fs::read(stage.join(relative))
            .map_err(|error| format!("read staged {relative}: {error}"))?;
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn read_trimmed(path: &Path) -> Result<String, String> {
    fs::read_to_string(path)
        .map(|value| value.trim().to_string())
        .map_err(|error| format!("read {}: {error}", path.display()))
}

fn machine_fingerprint() -> String {
    let source = fs::read("/etc/machine-id")
        .or_else(|_| fs::read("/var/lib/dbus/machine-id"))
        .unwrap_or_else(|_| {
            hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .as_bytes()
                .to_vec()
        });
    sha256_hex(&source)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn validate_hash(value: &str) -> Result<(), String> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err("plan hash must be exactly 64 hexadecimal characters".into())
    }
}

fn random_secret() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn generated_env() -> String {
    format!(
        "JWT_SECRET={}\nACCESS_TOKEN_DEFAULT_TTL_SECS=900\nACCESS_TOKEN_MAX_TTL_SECS=3600\nREFRESH_TOKEN_TTL_SECS=604800\nAUTH_KEY={}\nMASTER_KEY={}\nADMIN_KEY={}\nN2_CLUSTER_SECRET={}\n",
        random_secret(),
        random_secret(),
        random_secret(),
        random_secret(),
        random_secret()
    )
}

fn validate_env(value: &str) -> Result<(), String> {
    for key in [
        "JWT_SECRET",
        "AUTH_KEY",
        "MASTER_KEY",
        "ADMIN_KEY",
        "N2_CLUSTER_SECRET",
    ] {
        if !value.lines().any(|line| {
            line.split_once('=')
                .is_some_and(|(name, data)| name == key && !data.is_empty())
        }) {
            return Err(format!("generated cluster environment is missing {key}"));
        }
    }
    Ok(())
}

fn disk_serial(disk: &BlockDisk) -> &str {
    if disk.serial.is_empty() {
        disk.name.as_str()
    } else {
        disk.serial.as_str()
    }
}

fn root_id(node_id: &str, label: &str) -> String {
    format!("{node_id}-{}", label.to_ascii_lowercase())
}

fn absolute_path(value: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(value);
    if !path.is_absolute() || value.chars().any(char::is_control) {
        return Err("path must be absolute and contain no control characters".into());
    }
    Ok(path)
}

fn ensure_regular_file(path: &Path, label: &str) -> Result<(), String> {
    let metadata = path
        .metadata()
        .map_err(|error| format!("inspect {label} {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{label} {} is not a regular file", path.display()));
    }
    Ok(())
}

fn ensure_executable(path: &Path, label: &str) -> Result<(), String> {
    ensure_regular_file(path, label)?;
    let mode = path
        .metadata()
        .map_err(|error| format!("inspect {label} {}: {error}", path.display()))?
        .permissions()
        .mode();
    if mode & 0o111 == 0 {
        return Err(format!("{label} {} is not executable", path.display()));
    }
    Ok(())
}

fn normalize_agent_url(value: &str) -> Result<String, String> {
    let value = value.trim_end_matches('/');
    if !(value.starts_with("http://") || value.starts_with("https://"))
        || value.contains(['\n', '\r'])
    {
        return Err("agent URL must be an http:// or https:// URL".into());
    }
    Ok(value.to_string())
}

fn validate_id(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!(
            "{label} must contain only letters, numbers, '-' or '_'"
        ));
    }
    Ok(())
}

fn validate_host(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 253
        || value.contains(['/', '\\', ':'])
        || value.chars().any(char::is_whitespace)
    {
        return Err("host must be an IP address or DNS name without scheme or port".into());
    }
    Ok(())
}

fn parse_port(value: Option<&String>, usage: &str) -> Result<u16, String> {
    value
        .ok_or(usage)?
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| usage.to_string())
}

fn parse_usize(value: Option<&String>, usage: &str) -> Result<usize, String> {
    value
        .ok_or(usage)?
        .parse::<usize>()
        .map_err(|_| usage.to_string())
}

fn parse_u64(value: Option<&String>, usage: &str) -> Result<u64, String> {
    value
        .ok_or(usage)?
        .parse::<u64>()
        .map_err(|_| usage.to_string())
}

fn split_command(input: &str) -> Result<Vec<String>, String> {
    let mut arguments = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in input.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' && quote != Some('\'') {
            escaped = true;
            continue;
        }
        if let Some(active) = quote {
            if character == active {
                quote = None;
            } else {
                current.push(character);
            }
            continue;
        }
        match character {
            '\'' | '"' => quote = Some(character),
            character if character.is_whitespace() => {
                if !current.is_empty() {
                    arguments.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(character),
        }
    }
    if escaped || quote.is_some() {
        return Err("incomplete quote or escape".into());
    }
    if !current.is_empty() {
        arguments.push(current);
    }
    Ok(arguments)
}

fn success_outcome() -> CommandOutcome {
    CommandOutcome {
        success: true,
        ..CommandOutcome::default()
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

const HELP_TEXT: &str = "COMMANDS
  help
  clear
  version
  cluster init <cluster-id>
  node pair <node-id> <agent-url> <token> <advertised-host> <https-port> <native-port>
  node list
  lsblk <node-id>
  disk inspect <node-id> <device-or-label>
  disk format <node-id> <device> <N2DISKxxx>
  disk mount <node-id> <N2DISKxxx>
  disk assign <node-id> <N2DISKxxx>
  disk unassign <node-id> <N2DISKxxx>
  pool create <pool-id> <generation> <data> <parity>
  pool enroll <pool-id> <node-id> [node-id ...]
  pool seal|writable <pool-id>
  reserve enroll|remove <node-id>
  config tls <certificate> <private-key>
  gateway add <gateway-id> <node-id> <bind-host> <https-port>
  gateway remove <gateway-id>
  cluster show|validate|commit
  cluster evolve <new-topology-epoch>
  cluster discard-draft
  cluster start|stop|restart|enable|disable|status
  logs <node-id|gateway-id> [count]

Every disk command executes only on its owning physical agent. Cluster commit
uses canonical SHA-256 identity, parallel prepare, and idempotent local commit.
The commissioner and Shell are not part of the N2 data path after activation.";

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Default)]
    struct FakeHost {
        disks: Mutex<Vec<BlockDisk>>,
        active: Mutex<BTreeSet<String>>,
    }

    impl HostOps for FakeHost {
        fn require_privileged(&self) -> Result<(), String> {
            Ok(())
        }

        fn check_prerequisites(&self) -> Result<Vec<String>, String> {
            Ok(vec!["fake tools".into()])
        }

        fn list_disks(&self) -> Result<Vec<BlockDisk>, String> {
            Ok(self.disks.lock().unwrap().clone())
        }

        fn format_disk(
            &self,
            _disk: &BlockDisk,
            _label: &str,
            _settings: &InstallSettings,
            _emit: Emit<'_>,
        ) -> Result<crate::install::FormatResult, String> {
            Err("not used".into())
        }

        fn mount_disk(
            &self,
            _disk: &BlockDisk,
            _label: &str,
            _settings: &InstallSettings,
            _emit: Emit<'_>,
        ) -> Result<crate::install::FormatResult, String> {
            Err("not used".into())
        }

        fn service_action(&self, service: &str, action: ServiceAction) -> Result<String, String> {
            match action {
                ServiceAction::Start | ServiceAction::Restart => {
                    self.active.lock().unwrap().insert(service.into());
                }
                ServiceAction::Stop => {
                    self.active.lock().unwrap().remove(service);
                }
                ServiceAction::Enable | ServiceAction::Disable => {}
            }
            Ok(String::new())
        }

        fn service_is_active(&self, service: &str) -> bool {
            self.active.lock().unwrap().contains(service)
        }

        fn reload_systemd(&self) -> Result<(), String> {
            Ok(())
        }

        fn identity_exists(&self, _user: &str, _group: &str) -> Result<(), String> {
            Ok(())
        }

        fn apply_ownership(&self, _path: &Path, _user: &str, _group: &str) -> Result<(), String> {
            Ok(())
        }

        fn recent_logs(&self, service: &str, _lines: usize) -> Result<String, String> {
            Ok(format!("logs for {service}"))
        }
    }

    #[test]
    fn canonical_hash_excludes_agent_credentials() {
        let mut spec = sample_spec();
        let first = deployment_hash(&spec, "A=1", b"cert", b"key").unwrap();
        spec.nodes.get_mut("node-001").unwrap().agent.credential = "different".into();
        let second = deployment_hash(&spec, "A=1", b"cert", b"key").unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn exact_pool_width_and_unique_failure_roles_are_enforced() {
        let mut spec = sample_spec();
        assert!(spec.validate().is_ok());
        spec.pools.get_mut("pool-001").unwrap().node_ids.pop();
        assert!(spec.validate().unwrap_err().contains("requires exactly"));
    }

    #[test]
    fn active_node_cannot_also_be_reserve() {
        let mut spec = sample_spec();
        spec.reserve_nodes.insert("node-001".into());
        assert!(
            spec.validate()
                .unwrap_err()
                .contains("both active and reserve")
        );
    }

    #[test]
    fn gateway_rendering_is_bound_to_its_physical_agent() {
        let mut spec = sample_spec();
        spec.gateways.insert(
            "gateway-001".into(),
            GatewaySpec {
                id: "gateway-001".into(),
                node_id: "node-002".into(),
                bind_host: "0.0.0.0".into(),
                https_port: 50111,
            },
        );
        let root = test_root("gateway-binding");
        let settings = test_settings(&root);
        let node_one_agent = &spec.nodes["node-001"].agent.agent_id;
        let node_two_agent = &spec.nodes["node-002"].agent.agent_id;
        assert!(render_gateway_config(&spec, node_one_agent, &settings).is_err());
        let rendered = render_gateway_config(&spec, node_two_agent, &settings).unwrap();
        assert!(rendered.contains("host=0.0.0.0"));
        assert!(rendered.contains("https_port=50111"));
        assert!(rendered.contains("[node.node-001]"));
        assert!(rendered.contains("[node.node-002]"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn format_confirmation_is_bound_to_agent_disk_and_label() {
        let disk = BlockDisk {
            name: "sdb".into(),
            path: "/dev/sdb".into(),
            stable_id: "id".into(),
            model: "disk".into(),
            serial: "SERIAL".into(),
            size_bytes: 1,
            read_only: false,
            removable: false,
            system: false,
            mountpoints: vec![],
            filesystem: None,
            label: None,
            uuid: None,
            partition_path: None,
        };
        assert_eq!(
            format!(
                "FORMAT {} {} {}",
                "agent-a",
                "N2DISK001",
                disk_serial(&disk)
            ),
            "FORMAT agent-a N2DISK001 SERIAL"
        );
    }

    #[test]
    fn staged_content_tamper_is_rejected_and_service_ownership_is_fenced() {
        let root = test_root("stage-fencing");
        let settings = test_settings(&root);
        prepare_test_files(&settings);
        let disk = test_disk(&settings, "N2DISK001", "uuid-1", "serial-1");
        let host = Arc::new(FakeHost {
            disks: Mutex::new(vec![disk]),
            ..FakeHost::default()
        });
        let agent = AgentController::new(settings.clone(), "0123456789abcdef", host).unwrap();
        let pair = agent
            .pair(
                "0123456789abcdef",
                PairRequest {
                    cluster_id: "cluster-test".into(),
                    commissioner_id: "commissioner-test".into(),
                },
            )
            .unwrap();
        let record = agent.authenticate(&pair.credential).unwrap();
        let envelope = test_envelope(&agent.identity(), &settings);
        let prepared = agent.execute(
            &record,
            AgentCommand::Prepare {
                envelope: envelope.clone(),
                node_id: "node-001".into(),
            },
        );
        assert!(prepared.success, "{}", prepared.message);
        fs::write(
            settings
                .setup_root
                .join("staged")
                .join(&envelope.plan_hash)
                .join("node/n2_config.ini"),
            b"tampered",
        )
        .unwrap();
        let committed = agent.execute(
            &record,
            AgentCommand::Commit {
                plan_hash: envelope.plan_hash,
            },
        );
        assert!(!committed.success);
        assert!(committed.message.contains("changed after prepare"));
        assert!(!settings.setup_root.join("committed.sha256").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prepare_commit_is_idempotent_and_agent_cannot_control_another_node() {
        let root = test_root("commit-fencing");
        let settings = test_settings(&root);
        prepare_test_files(&settings);
        let disk = test_disk(&settings, "N2DISK001", "uuid-1", "serial-1");
        let host = Arc::new(FakeHost {
            disks: Mutex::new(vec![disk]),
            ..FakeHost::default()
        });
        let agent = AgentController::new(settings.clone(), "0123456789abcdef", host).unwrap();
        let pair = agent
            .pair(
                "0123456789abcdef",
                PairRequest {
                    cluster_id: "cluster-test".into(),
                    commissioner_id: "commissioner-test".into(),
                },
            )
            .unwrap();
        let record = agent.authenticate(&pair.credential).unwrap();
        let envelope = test_envelope(&agent.identity(), &settings);
        assert!(
            agent
                .execute(
                    &record,
                    AgentCommand::Prepare {
                        envelope: envelope.clone(),
                        node_id: "node-001".into(),
                    }
                )
                .success
        );
        for _ in 0..2 {
            let result = agent.execute(
                &record,
                AgentCommand::Commit {
                    plan_hash: envelope.plan_hash.clone(),
                },
            );
            assert!(result.success, "{}", result.message);
        }
        let wrong = agent.execute(
            &record,
            AgentCommand::Service {
                target: ServiceTarget::Node {
                    node_id: "node-002".into(),
                },
                action: AgentServiceAction::Start,
            },
        );
        assert!(!wrong.success);
        assert!(wrong.message.contains("owns node 'node-001'"));
        let own = agent.execute(
            &record,
            AgentCommand::Service {
                target: ServiceTarget::Node {
                    node_id: "node-001".into(),
                },
                action: AgentServiceAction::Start,
            },
        );
        assert!(own.success, "{}", own.message);
        fs::remove_dir_all(root).unwrap();
    }

    fn sample_spec() -> ClusterSpec {
        let nodes = (1..=2)
            .map(|index| {
                let id = format!("node-{index:03}");
                (
                    id.clone(),
                    NodeSpec {
                        id,
                        advertised_host: format!("10.0.0.{index}"),
                        https_port: 50001,
                        native_port: 50002,
                        agent: AgentBinding {
                            url: format!("http://10.0.0.{index}:50902"),
                            agent_id: format!("agent-{index}"),
                            machine_fingerprint: format!("machine-{index}"),
                            mount_root: "/mnt".into(),
                            credential: format!("secret-{index}"),
                        },
                        disks: BTreeMap::from([(
                            format!("N2DISK{index:03}"),
                            DiskSpec {
                                label: format!("N2DISK{index:03}"),
                                filesystem_uuid: format!("uuid-{index}"),
                                stable_id: format!("stable-{index}"),
                                serial: format!("serial-{index}"),
                                size_bytes: 1,
                            },
                        )]),
                    },
                )
            })
            .collect();
        ClusterSpec {
            format_version: PLAN_FORMAT_VERSION,
            cluster_id: "cluster-test".into(),
            topology_epoch: 1,
            nodes,
            pools: BTreeMap::from([(
                "pool-001".into(),
                PoolSpec {
                    id: "pool-001".into(),
                    generation: 1,
                    data_shards: 1,
                    parity_shards: 1,
                    node_ids: vec!["node-001".into(), "node-002".into()],
                    writable: true,
                },
            )]),
            reserve_nodes: BTreeSet::new(),
            gateways: BTreeMap::new(),
        }
    }

    fn test_root(name: &str) -> PathBuf {
        let base = std::env::var_os("N2_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = base.join(format!("n2-shell-{name}-{}-{unique}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn test_settings(root: &Path) -> InstallSettings {
        InstallSettings {
            engine_binary: root.join("bin/litiaina-n2"),
            gateway_binary: root.join("bin/litiaina-n2-gateway"),
            runtime_root: root.join("runtime/n2"),
            gateway_runtime: root.join("runtime/gateway"),
            env_path: root.join("etc/n2.env"),
            setup_root: root.join("setup"),
            mount_root: root.join("mnt"),
            fstab_path: root.join("fstab"),
            service_name: "n2-test".into(),
            gateway_service_name: "n2-gateway-test".into(),
            systemd_unit_dir: root.join("systemd"),
            run_user: "test".into(),
            run_group: "test".into(),
        }
    }

    fn prepare_test_files(settings: &InstallSettings) {
        fs::create_dir_all(settings.engine_binary.parent().unwrap()).unwrap();
        for binary in [&settings.engine_binary, &settings.gateway_binary] {
            fs::write(binary, b"binary").unwrap();
            let mut permissions = fs::metadata(binary).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(binary, permissions).unwrap();
        }
    }

    fn test_disk(settings: &InstallSettings, label: &str, uuid: &str, serial: &str) -> BlockDisk {
        let mount = settings.mount_path(label);
        fs::create_dir_all(&mount).unwrap();
        BlockDisk {
            name: "sdb".into(),
            path: "/dev/sdb".into(),
            stable_id: "/dev/disk/by-id/test-sdb".into(),
            model: "test".into(),
            serial: serial.into(),
            size_bytes: 1024,
            read_only: false,
            removable: false,
            system: false,
            mountpoints: vec![mount.to_string_lossy().into_owned()],
            filesystem: Some("xfs".into()),
            label: Some(label.into()),
            uuid: Some(uuid.into()),
            partition_path: Some("/dev/sdb1".into()),
        }
    }

    fn test_envelope(identity: &AgentIdentity, settings: &InstallSettings) -> DeploymentEnvelope {
        let mut spec = sample_spec();
        let node = spec.nodes.get_mut("node-001").unwrap();
        node.agent.agent_id = identity.agent_id.clone();
        node.agent.machine_fingerprint = identity.machine_fingerprint.clone();
        node.agent.mount_root = settings.mount_root.to_string_lossy().into_owned();
        node.disks = BTreeMap::from([(
            "N2DISK001".into(),
            DiskSpec {
                label: "N2DISK001".into(),
                filesystem_uuid: "uuid-1".into(),
                stable_id: "/dev/disk/by-id/test-sdb".into(),
                serial: "serial-1".into(),
                size_bytes: 1024,
            },
        )]);
        DeploymentEnvelope::new(spec, generated_env(), b"cert".to_vec(), b"key".to_vec()).unwrap()
    }
}
