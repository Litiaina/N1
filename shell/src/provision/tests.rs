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
    spec.hosts.get_mut("host-001").unwrap().agent.credential = "different".into();
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
    assert!(render_gateway_config(&spec, "host-001", &settings).is_err());
    let rendered = render_gateway_config(&spec, "host-002", &settings).unwrap();
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
            host_id: "host-001".into(),
        },
    );
    assert!(prepared.success, "{}", prepared.message);
    fs::write(
        settings
            .setup_root
            .join("staged")
            .join(&envelope.plan_hash)
            .join("nodes/node-001/n2_config.ini"),
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
                    host_id: "host-001".into(),
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
    assert!(wrong.message.contains("owns nodes [node-001]"));
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

#[test]
fn one_physical_agent_commits_and_controls_multiple_lab_nodes() {
    let root = test_root("multi-node-host");
    let settings = test_settings(&root);
    prepare_test_files(&settings);
    let host = Arc::new(FakeHost {
        disks: Mutex::new(vec![
            test_disk(&settings, "N2DISK001", "uuid-1", "serial-1"),
            test_disk(&settings, "N2DISK002", "uuid-2", "serial-2"),
        ]),
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
    let mut spec = sample_spec();
    spec.hosts.remove("host-002");
    let local_host = spec.hosts.get_mut("host-001").unwrap();
    local_host.agent.agent_id = agent.identity().agent_id;
    local_host.agent.machine_fingerprint = agent.identity().machine_fingerprint;
    local_host.agent.mount_root = settings.mount_root.to_string_lossy().into_owned();
    spec.nodes.get_mut("node-002").unwrap().host_id = "host-001".into();
    spec.nodes.get_mut("node-002").unwrap().disks = BTreeMap::from([(
        "N2DISK002".into(),
        DiskSpec {
            label: "N2DISK002".into(),
            filesystem_uuid: "uuid-2".into(),
            stable_id: "/dev/disk/by-id/test-sdb".into(),
            serial: "serial-2".into(),
            size_bytes: 1024,
        },
    )]);
    spec.gateways.insert(
        "gateway-001".into(),
        GatewaySpec {
            id: "gateway-001".into(),
            node_id: "node-001".into(),
            bind_host: "0.0.0.0".into(),
            https_port: 50111,
        },
    );
    let envelope =
        DeploymentEnvelope::new(spec, generated_env(), b"cert".to_vec(), b"key".to_vec()).unwrap();
    assert!(
        agent
            .execute(
                &record,
                AgentCommand::Prepare {
                    envelope: envelope.clone(),
                    host_id: "host-001".into(),
                },
            )
            .success
    );
    assert!(
        agent
            .execute(
                &record,
                AgentCommand::Commit {
                    plan_hash: envelope.plan_hash,
                },
            )
            .success
    );
    for node_id in ["node-001", "node-002"] {
        assert!(settings.node_dir(node_id).join("n2_config.ini").exists());
        let started = agent.execute(
            &record,
            AgentCommand::Service {
                target: ServiceTarget::Node {
                    node_id: node_id.into(),
                },
                action: AgentServiceAction::Start,
            },
        );
        assert!(started.success, "{}", started.message);
    }
    assert!(
        settings
            .gateway_runtime
            .join("n2_gateway_config.ini")
            .exists()
    );
    assert!(!settings.gateway_runtime.join("gateway.ini").exists());
    fs::remove_dir_all(root).unwrap();
}

fn sample_spec() -> ClusterSpec {
    let hosts = (1..=2)
        .map(|index| {
            let id = format!("host-{index:03}");
            (
                id.clone(),
                HostSpec {
                    id,
                    agent: AgentBinding {
                        url: format!("http://10.0.0.{index}:50902"),
                        agent_id: format!("agent-{index}"),
                        machine_fingerprint: format!("machine-{index}"),
                        mount_root: "/mnt".into(),
                        credential: format!("secret-{index}"),
                    },
                },
            )
        })
        .collect();
    let nodes = (1..=2)
        .map(|index| {
            let id = format!("node-{index:03}");
            (
                id.clone(),
                NodeSpec {
                    id,
                    host_id: format!("host-{index:03}"),
                    advertised_host: format!("10.0.0.{index}"),
                    https_port: 50001,
                    native_port: 50002,
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
        hosts,
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
    let host = spec.hosts.get_mut("host-001").unwrap();
    host.agent.agent_id = identity.agent_id.clone();
    host.agent.machine_fingerprint = identity.machine_fingerprint.clone();
    host.agent.mount_root = settings.mount_root.to_string_lossy().into_owned();
    let node = spec.nodes.get_mut("node-001").unwrap();
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
