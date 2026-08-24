use super::*;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Default)]
struct FakeHost {
    disks: Mutex<Vec<BlockDisk>>,
    active: Mutex<bool>,
    actions: Mutex<Vec<String>>,
}

impl FakeHost {
    fn with_disks(disks: Vec<BlockDisk>) -> Self {
        Self {
            disks: Mutex::new(disks),
            ..Self::default()
        }
    }
}

impl HostOps for FakeHost {
    fn require_privileged(&self) -> Result<(), String> {
        Ok(())
    }

    fn check_prerequisites(&self) -> Result<Vec<String>, String> {
        Ok(vec!["fake prerequisites".into()])
    }

    fn list_disks(&self) -> Result<Vec<BlockDisk>, String> {
        Ok(self.disks.lock().unwrap().clone())
    }

    fn format_disk(
        &self,
        disk: &BlockDisk,
        label: &str,
        settings: &InstallSettings,
        emit: Emit<'_>,
    ) -> Result<FormatResult, String> {
        emit(OutputKind::Info, "fake format".into());
        let mount = settings.mount_path(label);
        fs::create_dir_all(&mount).map_err(|error| error.to_string())?;
        let mut disks = self.disks.lock().unwrap();
        let stored = disks
            .iter_mut()
            .find(|item| item.path == disk.path)
            .unwrap();
        stored.filesystem = Some("xfs".into());
        stored.label = Some(label.into());
        stored.uuid = Some(format!("uuid-{label}"));
        stored.partition_path = Some(format!("{}1", disk.path));
        stored.mountpoints = vec![mount.to_string_lossy().to_string()];
        Ok(FormatResult {
            label: label.into(),
            partition_path: format!("{}1", disk.path),
            uuid: format!("uuid-{label}"),
            mount_path: mount,
        })
    }

    fn mount_disk(
        &self,
        disk: &BlockDisk,
        label: &str,
        settings: &InstallSettings,
        emit: Emit<'_>,
    ) -> Result<FormatResult, String> {
        emit(OutputKind::Info, "fake mount".into());
        let mount = settings.mount_path(label);
        fs::create_dir_all(&mount).map_err(|error| error.to_string())?;
        let mut disks = self.disks.lock().unwrap();
        let stored = disks
            .iter_mut()
            .find(|item| item.path == disk.path)
            .unwrap();
        stored.mountpoints = vec![mount.to_string_lossy().to_string()];
        Ok(FormatResult {
            label: label.into(),
            partition_path: stored
                .partition_path
                .clone()
                .unwrap_or_else(|| stored.path.clone()),
            uuid: stored.uuid.clone().unwrap_or_else(|| "test-uuid".into()),
            mount_path: mount,
        })
    }

    fn service_action(&self, _service: &str, action: ServiceAction) -> Result<String, String> {
        self.actions.lock().unwrap().push(action.as_str().into());
        match action {
            ServiceAction::Start | ServiceAction::Restart => *self.active.lock().unwrap() = true,
            ServiceAction::Stop => *self.active.lock().unwrap() = false,
            _ => {}
        }
        Ok(String::new())
    }

    fn service_is_active(&self, _service: &str) -> bool {
        *self.active.lock().unwrap()
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

    fn recent_logs(&self, _service: &str, _lines: usize) -> Result<String, String> {
        Ok("test log".into())
    }
}

fn temporary_root(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("n1-shell-{name}-{}-{unique}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    root
}

fn settings(root: &Path) -> InstallSettings {
    InstallSettings {
        install_dir: root.join("install"),
        engine_binary: root.join("install/litiaina-n1"),
        mount_root: root.join("mnt"),
        fstab_path: root.join("fstab"),
        service_name: "litiaina-n1-test".into(),
        systemd_unit_dir: root.join("systemd"),
        run_user: "test".into(),
        run_group: "test".into(),
    }
}

fn disk(path: &str, system: bool) -> BlockDisk {
    BlockDisk {
        name: path.trim_start_matches("/dev/").into(),
        path: path.into(),
        stable_id: format!("/dev/disk/by-id/test-{}", path.trim_start_matches("/dev/")),
        model: "Test Disk".into(),
        serial: format!("SERIAL-{}", path.trim_start_matches("/dev/")),
        size_bytes: 4 * 1024 * 1024 * 1024,
        read_only: false,
        removable: false,
        system,
        mountpoints: if system { vec!["/".into()] } else { Vec::new() },
        filesystem: if system { Some("ext4".into()) } else { None },
        label: None,
        uuid: None,
        partition_path: None,
    }
}

fn execute(
    shell: &InstallShell,
    command: &str,
    confirmation: Option<&str>,
) -> (CommandOutcome, Vec<String>) {
    let mut lines = Vec::new();
    let result = shell.execute(command, confirmation, &mut |_, line| lines.push(line));
    (result, lines)
}

#[test]
fn parser_handles_quotes_and_rejects_incomplete_input() {
    assert_eq!(
        split_command("config tls '/tmp/a cert.pem' /tmp/key.pem").unwrap(),
        vec!["config", "tls", "/tmp/a cert.pem", "/tmp/key.pem"]
    );
    assert!(split_command("config tls 'broken").is_err());
}

#[test]
fn protected_system_disk_can_never_reach_confirmation() {
    let root = temporary_root("protected");
    let host = Arc::new(FakeHost::with_disks(vec![disk("/dev/sda", true)]));
    let shell = InstallShell::new(settings(&root), host);
    let (result, output) = execute(&shell, "disk format /dev/sda NDISK001", None);
    assert!(!result.success);
    assert!(result.confirmation.is_none());
    assert!(output.join("\n").contains("protected system disk"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn format_requires_exact_model_bound_confirmation() {
    let root = temporary_root("confirmation");
    let host = Arc::new(FakeHost::with_disks(vec![disk("/dev/sdb", false)]));
    let shell = InstallShell::new(settings(&root), host);
    let (pending, _) = execute(&shell, "disk format /dev/sdb NDISK001", None);
    let confirmation = pending.confirmation.expect("confirmation");
    assert_eq!(confirmation.expected, "FORMAT NDISK001 SERIAL-sdb");
    let (rejected, _) = execute(
        &shell,
        "disk format /dev/sdb NDISK001",
        Some("FORMAT NDISK001"),
    );
    assert!(rejected.confirmation.is_some());
    let (completed, _) = execute(
        &shell,
        "disk format /dev/sdb NDISK001",
        Some("FORMAT NDISK001 SERIAL-sdb"),
    );
    assert!(completed.success);
    assert_eq!(shell.host.list_disks().unwrap()[0].state(), "mounted");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn existing_fstab_target_blocks_reformat_before_disk_changes() {
    let root = temporary_root("fstab-collision");
    let settings = settings(&root);
    fs::write(
        &settings.fstab_path,
        format!(
            "{MANAGED_FSTAB_PREFIX}NDISK001\nUUID=old {} xfs defaults 0 2\n",
            settings.mount_path("NDISK001").display()
        ),
    )
    .unwrap();
    let host = Arc::new(FakeHost::with_disks(vec![disk("/dev/sdb", false)]));
    let shell = InstallShell::new(settings, host.clone());
    let (result, output) = execute(
        &shell,
        "disk format /dev/sdb NDISK001",
        Some("FORMAT NDISK001 SERIAL-sdb"),
    );
    assert!(!result.success);
    assert!(output.join("\n").contains("already reserves"));
    assert!(host.list_disks().unwrap()[0].filesystem.is_none());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn initialized_unmounted_xfs_disk_can_resume_at_mount_stage() {
    let root = temporary_root("resume-mount");
    let settings = settings(&root);
    let mut initialized = disk("/dev/sdb", false);
    initialized.filesystem = Some("xfs".into());
    initialized.label = Some("NDISK001".into());
    initialized.uuid = Some("uuid-NDISK001".into());
    initialized.partition_path = Some("/dev/sdb1".into());
    let host = Arc::new(FakeHost::with_disks(vec![initialized]));
    let shell = InstallShell::new(settings.clone(), host.clone());
    let (result, output) = execute(&shell, "disk mount NDISK001", None);
    assert!(result.success, "{}", output.join("\n"));
    assert_eq!(
        host.list_disks().unwrap()[0].mountpoints,
        vec![
            settings
                .mount_path("NDISK001")
                .to_string_lossy()
                .to_string()
        ]
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn four_disk_assignment_and_ec_validation_is_deterministic() {
    let root = temporary_root("topology");
    let settings = settings(&root);
    fs::create_dir_all(&settings.install_dir).unwrap();
    fs::write(&settings.engine_binary, b"engine").unwrap();
    let mut permissions = fs::metadata(&settings.engine_binary).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&settings.engine_binary, permissions).unwrap();
    fs::write(settings.install_dir.join("cert.pem"), b"cert").unwrap();
    fs::write(settings.install_dir.join("key.pem"), b"key").unwrap();
    fs::write(settings.env_path(), generated_env()).unwrap();
    fs::create_dir_all(&settings.systemd_unit_dir).unwrap();
    fs::write(settings.unit_path(), b"test unit").unwrap();
    let host = Arc::new(FakeHost::with_disks(vec![
        disk("/dev/sdb", false),
        disk("/dev/sdc", false),
        disk("/dev/sdd", false),
        disk("/dev/sde", false),
    ]));
    let shell = InstallShell::new(settings.clone(), host);
    for (index, device) in ["sdb", "sdc", "sdd", "sde"].iter().enumerate() {
        let label = format!("NDISK{:03}", index + 1);
        let confirmation = format!("FORMAT {label} SERIAL-{device}");
        assert!(
            execute(
                &shell,
                &format!("disk format /dev/{device} {label}"),
                Some(&confirmation)
            )
            .0
            .success
        );
        assert!(
            execute(&shell, &format!("disk assign {label} both"), None)
                .0
                .success
        );
    }
    assert!(execute(&shell, "config ec 3 1", None).0.success);
    let mut config = shell.load_or_create_config().unwrap();
    let control = settings.install_dir.join("fs-storage-path");
    fs::create_dir_all(&control).unwrap();
    config
        .with_section(Some("disk_config"))
        .set("fs_storage_path", control.to_string_lossy().as_ref());
    config
        .with_section(Some("server"))
        .set(
            "cert_path",
            settings
                .install_dir
                .join("cert.pem")
                .to_string_lossy()
                .as_ref(),
        )
        .set(
            "key_path",
            settings
                .install_dir
                .join("key.pem")
                .to_string_lossy()
                .as_ref(),
        );
    shell.write_config(&config).unwrap();
    let (validated, output) = execute(&shell, "config validate", None);
    assert!(validated.success, "{}", output.join("\n"));
    assert!(output.join("\n").contains("EC: 3 + 1"));
    let (started, output) = execute(&shell, "n1 start", None);
    assert!(started.success, "{}", output.join("\n"));
    let (rejected, output) = execute(&shell, "config ec 2 1", None);
    assert!(!rejected.success);
    assert!(output.join("\n").contains("stop N1"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn initialization_is_restart_safe_and_keeps_service_disabled() {
    let root = temporary_root("init");
    let settings = settings(&root);
    fs::create_dir_all(&settings.install_dir).unwrap();
    fs::write(&settings.engine_binary, b"engine").unwrap();
    let mut permissions = fs::metadata(&settings.engine_binary).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&settings.engine_binary, permissions).unwrap();
    let host = Arc::new(FakeHost::default());
    let shell = InstallShell::new(settings.clone(), host.clone());

    let (first, output) = execute(&shell, "n1 init", None);
    assert!(first.success, "{}", output.join("\n"));
    assert!(settings.config_path().is_file());
    assert!(settings.env_path().is_file());
    assert!(settings.unit_path().is_file());
    assert!(!host.service_is_active(&settings.service_name));
    assert_eq!(host.actions.lock().unwrap().as_slice(), ["disable"]);

    let original_env = fs::read(settings.env_path()).unwrap();
    let (second, output) = execute(&shell, "n1 init", None);
    assert!(second.success, "{}", output.join("\n"));
    assert_eq!(fs::read(settings.env_path()).unwrap(), original_env);
    assert_eq!(
        host.actions.lock().unwrap().as_slice(),
        ["disable", "disable"]
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn lsblk_parser_propagates_system_mounts_to_parent_disk() {
    let raw = r#"{
          "blockdevices": [
            {"name":"/dev/nvme0n1","kname":"nvme0n1","path":"/dev/nvme0n1","type":"disk","size":1000,"ro":false,"rm":false,
             "children":[{"name":"/dev/nvme0n1p1","path":"/dev/nvme0n1p1","type":"part","fstype":"ext4","mountpoints":["/"]}]},
            {"name":"/dev/sdb","kname":"sdb","path":"/dev/sdb","type":"disk","size":2000,"ro":false,"rm":false,"mountpoints":[null]}
          ]
        }"#;
    let parsed = parse_lsblk_json(raw, Vec::new()).unwrap();
    assert!(parsed[0].system);
    assert_eq!(parsed[0].state(), "protected");
    assert!(!parsed[1].system);
    assert_eq!(parsed[1].state(), "candidate");
}
