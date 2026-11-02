//! Exercise the actual offline entry points used by the shipped service units.
//! No system service, listener, credential file or enrolled pool is required.
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::Command,
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("n1-offline-cli-{}", ulid::Ulid::new()));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
    fn private_json(&self, name: &str, value: Value) -> PathBuf {
        let path = self.0.join(name);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        write!(file, "{value}").unwrap();
        path
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn run(binary: &str, prefix: &[&str], operation: &str, flag: &str, path: &Path) -> Value {
    let output = Command::new(binary)
        .args(prefix)
        .args([operation, flag])
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn integrated_and_standalone_helpers_execute_disabled_jobs_and_recovery_without_server() {
    for (binary, prefix) in [
        (env!("CARGO_BIN_EXE_litiaina-n1"), &["backup"][..]),
        (env!("CARGO_BIN_EXE_litiaina-n1-backup"), &[][..]),
    ] {
        let f = Fixture::new();
        let state = f.0.join("state");
        let config = f.private_json("job.json", json!({"enabled":false,"state_dir":state}));
        let result = run(binary, prefix, "run-job", "--config", &config);
        assert_eq!(result["status"]["phase"], "disabled", "{result}");
        let result = run(binary, prefix, "job-status", "--config", &config);
        assert_eq!(result["status"]["phase"], "disabled", "{result}");
        assert_eq!(
            run(binary, prefix, "recover-job", "--state-dir", &state)["recovered"],
            true
        );
        assert!(!f.0.join(".n1-arc-pool.json").exists());
        fs::write(state.join("status.json"), b"corrupt").unwrap();
        let output = Command::new(binary)
            .args(prefix)
            .args(["recover-job", "--state-dir"])
            .arg(&state)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert_eq!(fs::read(state.join("status.json")).unwrap(), b"corrupt");
    }
}

#[test]
fn backup_service_units_use_the_offline_engine_entry_point() {
    for unit in [
        include_str!("../../deploy/systemd/litiaina-n1-backup.service"),
        include_str!("../../deploy/systemd/litiaina-n1-backup-recover.service"),
    ] {
        for line in unit
            .lines()
            .filter(|line| line.starts_with("ExecStart=") || line.starts_with("ExecStopPost="))
        {
            let (_, command) = line.split_once('=').unwrap();
            let args: Vec<_> = command.split_whitespace().collect();
            assert_eq!(args[0], "/opt/litiaina/n1/litiaina-n1");
            assert_eq!(args[1], "backup");
            let output = Command::new(env!("CARGO_BIN_EXE_litiaina-n1"))
                .args([args[1], args[2], "--help"])
                .output()
                .unwrap();
            assert!(output.status.success(), "{line}: {output:?}");
        }
    }
    assert!(
        include_str!("../../deploy/systemd/litiaina-n1.service.in")
            .contains("ExecStart=__BINARY__ serve --config __CONFIG_FILE__")
    );
}
