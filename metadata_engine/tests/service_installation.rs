//! Installer contracts: independent roles, protected secrets and no dry-run effects.
#![cfg(target_os = "linux")]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output},
};

struct Fixture {
    root: tempfile::TempDir,
    install: PathBuf,
    component: &'static str,
}

impl Fixture {
    fn new(component: &'static str) -> Self {
        let root = tempfile::Builder::new()
            .prefix("service-install-")
            .tempdir()
            .unwrap();
        let install = root.path().join("service tree");
        fs::create_dir(&install).unwrap();
        let executable = install.join(format!("litiaina-{component}"));
        fs::write(&executable, "#!/bin/sh\nexit 98\n").unwrap();
        fs::set_permissions(executable, fs::Permissions::from_mode(0o700)).unwrap();
        let secrets = install.join(format!("{component}.env"));
        fs::write(&secrets, "JWT_SECRET=test-only\n").unwrap();
        fs::set_permissions(secrets, fs::Permissions::from_mode(0o600)).unwrap();
        let config = if component == "m1" {
            "[disk_config]\nfs_storage_path=control\n[metadata_engine]\ndata_dir=db\nstaging_dir=staging\nbackup_dir=backups\nobject_native_url=n1s://objects.example:50002\n[storage_pool.NotLocal]\nroots=/not-local-object-a,/not-local-object-b\n"
        } else {
            "[disk_config]\nfs_storage_path=control\n[storage_pool.A]\nroots=object-first,object-second\n[metadata_engine]\ndata_dir=not-n1\n"
        };
        fs::write(install.join(format!("{component}_config.ini")), config).unwrap();
        Self {
            root,
            install,
            component,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new("bash")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join(format!("../scripts/install-{}-systemd.sh", self.component)),
            )
            .arg("--install-dir")
            .arg(&self.install)
            .arg("--dry-run")
            .args(args)
            .output()
            .unwrap()
    }

    fn unit(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
}

#[test]
fn metadata_installation_uses_its_own_paths_without_a_local_object_service() {
    let f = Fixture::new("m1");
    let unit = f.unit(&[]);
    for expected in [
        "Description=Litiaina M1 Metadata Store",
        "litiaina-m1",
        "m1.env",
        "m1_config.ini",
        "serve --config",
        "NoNewPrivileges=true",
        "TimeoutStopSec=300",
    ] {
        assert!(unit.contains(expected), "missing {expected}: {unit}");
    }
    let mounts = unit
        .lines()
        .find(|line| line.starts_with("RequiresMountsFor="))
        .unwrap();
    for expected in ["/control", "/db", "/staging", "/backups"] {
        assert!(mounts.contains(expected), "{mounts}");
    }
    assert!(!unit.contains("not-local-object"));
    assert!(!unit.contains("Requires=litiaina-n1"));
    assert!(!unit.contains("n1.env"));
    assert!(!f.install.join("db").exists());
    assert!(!f.install.join("control").exists());
}

#[test]
fn original_object_installer_keeps_its_defaults_and_object_mounts() {
    let f = Fixture::new("n1");
    let unit = f.unit(&[]);
    for expected in [
        "Description=Litiaina N1 Object Storage",
        "litiaina-n1",
        "n1.env",
        "n1_config.ini",
        "/object-first",
        "/object-second",
    ] {
        assert!(unit.contains(expected), "{unit}");
    }
    assert!(!unit.contains("not-n1"));
    assert!(!unit.contains("m1.env"));
    assert!(!f.install.join("control").exists());
}

#[test]
fn protected_file_and_service_supplied_credentials_remain_independent() {
    let f = Fixture::new("m1");
    fs::set_permissions(f.install.join("m1.env"), fs::Permissions::from_mode(0o644)).unwrap();
    let rejected = f.run(&[]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("owner-only"));
    fs::remove_file(f.install.join("m1.env")).unwrap();
    fs::rename(
        f.install.join("m1_config.ini"),
        f.install.join("metadata.ini"),
    )
    .unwrap();
    let unit = f.unit(&[
        "--no-env-file",
        "--config-file",
        f.install.join("metadata.ini").to_str().unwrap(),
        "--service-name",
        "metadata-worker",
    ]);
    assert!(!unit.contains("EnvironmentFile="));
    assert!(unit.contains("metadata.ini"));
}

#[test]
fn dry_run_never_calls_systemctl_or_executes_the_engine() {
    let f = Fixture::new("m1");
    let stubs = f.install.join("stubs");
    fs::create_dir(&stubs).unwrap();
    let systemctl = stubs.join("systemctl");
    fs::write(&systemctl, "#!/bin/sh\nexit 99\n").unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o700)).unwrap();
    let output = Command::new("bash")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../scripts/install-m1-systemd.sh"))
        .arg("--install-dir")
        .arg(&f.install)
        .args(["--dry-run", "--enable", "--start"])
        .env(
            "PATH",
            format!("{}:{}", stubs.display(), std::env::var("PATH").unwrap()),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "requires systemd-analyze on the deployment host"]
fn rendered_units_are_accepted_by_systemd() {
    for component in ["n1", "m1"] {
        let f = Fixture::new(component);
        let unit = f.root.path().join(format!("litiaina-{component}.service"));
        fs::write(&unit, f.unit(&[])).unwrap();
        let checked = Command::new("systemd-analyze")
            .arg("verify")
            .arg(&unit)
            .output()
            .expect("Linux qualification requires systemd-analyze");
        assert!(
            checked.status.success(),
            "{}",
            String::from_utf8_lossy(&checked.stderr)
        );
    }
}
