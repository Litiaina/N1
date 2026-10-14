//! Bound every external operation. Children have their own process group so a
//! timed-out transfer cannot retain repository leases through an orphan child.
use super::super::{Result, invalid};
use super::config::{Config, Replica, Service};
use super::workflow::Driver;
use std::fs::OpenOptions;
use std::os::unix::{fs::OpenOptionsExt, process::CommandExt};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub struct Processes {
    pub directory: PathBuf,
    pub service_timeout: u64,
    pub helper: PathBuf,
    pub helper_backup_subcommand: bool,
}
impl Processes {
    pub fn run(&self, mut command: Command, seconds: u64) -> Result<Vec<u8>> {
        let output = self.directory.join("operation.stdout");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&output)?;
        let mut child = command
            .stdin(Stdio::null())
            .stdout(file)
            .stderr(Stdio::inherit())
            .process_group(0)
            .spawn()?;
        let started = Instant::now();
        loop {
            if let Some(status) = child.try_wait()? {
                if !status.success() {
                    return Err(invalid(format!(
                        "backup operation exited {status}; see journal for details"
                    )));
                }
                return super::super::fs::read_regular(&output, 65536);
            }
            if started.elapsed() >= Duration::from_secs(seconds) {
                // SAFETY: child is still owned and unreaped; its dedicated PGID
                // is the child PID. It is not the job's or engine's process group.
                unsafe {
                    libc::kill(-(child.id() as i32), libc::SIGKILL);
                }
                let _ = child.wait();
                return Err(invalid(format!(
                    "backup operation exceeded {seconds}s and was terminated"
                )));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    fn service(&self, service: &Service, action: &str) -> Command {
        let mut cmd = Command::new("/usr/bin/systemctl");
        if service.user {
            cmd.arg("--user");
        }
        cmd.arg(action);
        if action == "show" {
            cmd.args(["--property=LoadState", "--property=ActiveState"]);
        }
        cmd.arg("--").arg(&service.unit);
        cmd
    }
    fn helper_command(&self) -> Command {
        let mut cmd = Command::new(&self.helper);
        if self.helper_backup_subcommand {
            cmd.arg("backup");
        }
        cmd
    }
    fn helper(&self, operation: &str, config: &Config, snapshot: Option<&str>) -> Command {
        let mut cmd = self.helper_command();
        cmd.arg(operation)
            .arg("--repository")
            .arg(&config.repository)
            .arg("--key-file")
            .arg(&config.key_file);
        if let Some(snapshot) = snapshot {
            cmd.arg("--snapshot").arg(snapshot);
        }
        cmd
    }
    fn prune_replica(&self, replica: &Replica, snapshot: &str, seconds: u64) -> Result<()> {
        if replica.keep_last == 0 {
            return Ok(());
        }
        let cmd = if let Some(url) = &replica.url {
            let destination = super::super::network::Destination {
                url: url.clone(),
                repository: replica.repository.to_string_lossy().into_owned(),
                token_file: replica
                    .token_file
                    .clone()
                    .ok_or_else(|| invalid("missing replica token"))?,
                ca_file: replica.ca_file.clone(),
                allow_plaintext: replica.allow_plaintext,
            };
            super::super::network::run_async(super::super::network::completed_operation(
                &destination,
                super::super::network::Request {
                    operation: "prune".into(),
                    repository: destination.repository.clone(),
                    snapshot: snapshot.into(),
                    keep_last: replica.keep_last,
                    confirm: true,
                    ..Default::default()
                },
                seconds,
            ))?;
            return Ok(());
        } else {
            let mut cmd = self.helper_command();
            cmd.arg("prune")
                .arg("--repository")
                .arg(&replica.repository)
                .arg("--key-file")
                .arg(&replica.key_file)
                .arg("--keep-last")
                .arg(replica.keep_last.to_string())
                .arg("--protect")
                .arg(snapshot);
            cmd
        };
        self.run(cmd, seconds)?;
        Ok(())
    }
}
impl Driver for Processes {
    fn preflight(&mut self, config: &Config) -> Result<()> {
        let roots = super::super::capture::roots_from_config(&config.engine_config)?;
        let repo =
            super::super::repository::Repository::open(&config.repository, &config.key_file, true)?;
        let state = std::fs::canonicalize(&config.state_dir)?;
        for root in &roots {
            let root = std::path::Path::new(&root.path);
            if state.starts_with(root)
                || root.starts_with(&state)
                || repo.path.starts_with(root)
                || root.starts_with(&repo.path)
            {
                return Err(invalid(
                    "job state/repository must be outside every storage root",
                ));
            }
            // Check initialized lease files without acquiring them from the running engine.
            super::super::fs::read_regular(
                &root.join(crate::core::instance_lease::LEASE_FILE),
                128,
            )?;
        }
        if state.starts_with(&repo.path) || repo.path.starts_with(&state) {
            return Err(invalid("job state and repository must not overlap"));
        }
        if let Some(replica) = &config.replica
            && replica.url.is_none()
        {
            let target = super::super::repository::Repository::open(
                &replica.repository,
                &replica.key_file,
                true,
            )?;
            if target.key.id != repo.key.id {
                return Err(invalid("replica encryption key differs"));
            }
            if target.path.starts_with(&repo.path)
                || repo.path.starts_with(&target.path)
                || state.starts_with(&target.path)
                || target.path.starts_with(&state)
                || roots.iter().any(|r| {
                    target.path.starts_with(&r.path)
                        || std::path::Path::new(&r.path).starts_with(&target.path)
                })
            {
                return Err(invalid(
                    "replica must be outside source, state and repository roots",
                ));
            }
        }
        Ok(())
    }
    fn active(&mut self, service: &Service) -> Result<bool> {
        let data = self.run(self.service(service, "show"), self.service_timeout)?;
        let text = std::str::from_utf8(&data)?;
        if !text.lines().any(|l| l == "LoadState=loaded") {
            return Err(invalid("N1 service is not loaded"));
        }
        if text.lines().any(|l| l == "ActiveState=active") {
            Ok(true)
        } else if text.lines().any(|l| l == "ActiveState=inactive") {
            Ok(false)
        } else {
            Err(invalid(
                "N1 service is not in a stable active/inactive state",
            ))
        }
    }
    fn stop(&mut self, service: &Service) -> Result<()> {
        self.run(self.service(service, "stop"), self.service_timeout)?;
        if self.active(service)? {
            return Err(invalid("N1 did not stop for offline capture"));
        }
        Ok(())
    }
    fn start(&mut self, service: &Service) -> Result<()> {
        self.run(self.service(service, "start"), self.service_timeout)?;
        Ok(())
    }
    fn capture(&mut self, config: &Config) -> Result<String> {
        if let Some(source) = &config.online_source {
            let result =
                super::super::network::run_async(super::super::network::completed_operation(
                    source,
                    super::super::network::Request {
                        operation: "create-online".into(),
                        repository: source.repository.clone(),
                        confirm: true,
                        ..Default::default()
                    },
                    config.capture_timeout_seconds,
                ))?;
            super::super::online::validate_result(&result)?;
            return Ok(result["snapshot"]
                .as_str()
                .ok_or_else(|| invalid("online capture did not return a completed snapshot"))?
                .into());
        }
        let mut cmd = self.helper("create", config, None);
        cmd.arg("--config")
            .arg(&config.engine_config)
            .arg("--workers")
            .arg(config.workers.to_string());
        let result: serde_json::Value =
            serde_json::from_slice(&self.run(cmd, config.capture_timeout_seconds)?)?;
        Ok(result["snapshot"]
            .as_str()
            .ok_or_else(|| invalid("capture did not return a snapshot identity"))?
            .into())
    }
    fn verify(&mut self, config: &Config, snapshot: &str) -> Result<()> {
        self.run(
            self.helper("verify", config, Some(snapshot)),
            config.operation_timeout_seconds,
        )?;
        Ok(())
    }
    fn replicate(&mut self, config: &Config, snapshot: &str) -> Result<()> {
        let replica = config
            .replica
            .as_ref()
            .ok_or_else(|| invalid("missing replica settings"))?;
        let mut cmd = self.helper("replicate", config, Some(snapshot));
        cmd.arg("--target-repository").arg(&replica.repository);
        if let Some(url) = &replica.url {
            cmd.arg("--target-url").arg(url).arg("--token-file").arg(
                replica
                    .token_file
                    .as_ref()
                    .ok_or_else(|| invalid("missing replica token file"))?,
            );
            if let Some(ca) = &replica.ca_file {
                cmd.arg("--ca-file").arg(ca);
            }
            if replica.allow_plaintext {
                cmd.arg("--allow-plaintext");
            }
        } else {
            cmd.arg("--target-key-file").arg(&replica.key_file);
        }
        // The receive protocol verifies and fsyncs before its completion ACK.
        self.run(cmd, config.operation_timeout_seconds)?;
        Ok(())
    }
    fn retain(&mut self, config: &Config, snapshot: &str) -> Result<()> {
        // Independent destination retention is explicitly opted into.
        if let Some(replica) = &config.replica {
            self.prune_replica(replica, snapshot, config.operation_timeout_seconds)?;
        }
        if config.keep_last > 0 {
            let mut cmd = self.helper("prune", config, None);
            cmd.arg("--keep-last")
                .arg(config.keep_last.to_string())
                .arg("--protect")
                .arg(snapshot);
            self.run(cmd, config.operation_timeout_seconds)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod invocation_tests {
    use super::*;

    #[test]
    fn integrated_and_legacy_jobs_use_the_correct_child_entrypoint() {
        for integrated in [false, true] {
            let driver = Processes {
                directory: "/unused".into(),
                service_timeout: 1,
                helper: "/test/engine".into(),
                helper_backup_subcommand: integrated,
            };
            for operation in ["create", "verify", "replicate", "prune"] {
                let command = driver.helper(operation, &Config::default(), None);
                let args = command.get_args().collect::<Vec<_>>();
                if integrated {
                    assert_eq!(args[0], "backup");
                }
                assert_eq!(args[usize::from(integrated)], operation);
            }
        }
    }
}
