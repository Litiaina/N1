use super::{
    config::{Config, Service},
    state::State,
    workflow::{self, Driver},
};
use crate::core::backup::{Result, invalid};
use std::path::PathBuf;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("n1-auto-backup-{}", ulid::Ulid::new()));
        Self(path)
    }
    fn state(&self) -> State {
        // Parallel tests spawn subprocesses. Between fork and exec they can
        // temporarily inherit another test's CLOEXEC flock descriptor, delaying
        // its release after drop(state). Retry only this positive reopen helper;
        // the explicit overlapping-run refusal assertions still call State::open
        // directly. Production lock acquisition and safety are unchanged.
        for _ in 0..100 {
            match State::open(&self.0) {
                Ok(state) => return state,
                Err(error) if error.to_string().contains("already in use") => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(error) => panic!("open fixture state: {error}"),
            }
        }
        State::open(&self.0).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct Fake {
    active: bool,
    fail: &'static str,
    attempts: usize,
    calls: Vec<&'static str>,
}
impl Fake {
    fn call(&mut self, name: &'static str) -> Result<()> {
        self.calls.push(name);
        if self.fail == name {
            Err(invalid(format!("injected {name} failure")))
        } else {
            Ok(())
        }
    }
}
impl Driver for Fake {
    fn preflight(&mut self, _: &Config) -> Result<()> {
        self.call("preflight")
    }
    fn active(&mut self, _: &Service) -> Result<bool> {
        Ok(self.active)
    }
    fn stop(&mut self, _: &Service) -> Result<()> {
        self.active = false;
        self.call("stop")
    }
    fn start(&mut self, _: &Service) -> Result<()> {
        self.call("start")?;
        self.active = true;
        Ok(())
    }
    fn capture(&mut self, config: &Config) -> Result<String> {
        assert!(!self.active || config.online_source.is_some());
        self.call("capture")?;
        Ok(ulid::Ulid::new().to_string())
    }
    fn verify(&mut self, _: &Config, _: &str) -> Result<()> {
        self.call("verify")
    }
    fn replicate(&mut self, _: &Config, _: &str) -> Result<()> {
        self.attempts += 1;
        self.call("replicate")
    }
    fn retain(&mut self, _: &Config, _: &str) -> Result<()> {
        self.call("retain")
    }
    fn retry_pause(&mut self) {}
}
fn config() -> Config {
    Config {
        enabled: true,
        ..Config::default()
    }
}

#[test]
fn online_jobs_never_stop_or_restart_the_engine_even_on_failure() {
    for failure in ["", "capture", "verify", "replicate"] {
        let fixture = Fixture::new();
        let mut state = fixture.state();
        let mut config = config();
        config.online_source = Some(crate::core::backup::network::Destination {
            url: "https://storage.example:21001".into(),
            repository: "primary".into(),
            token_file: "/private/operator.token".into(),
            ca_file: None,
            allow_plaintext: false,
        });
        let mut driver = Fake {
            active: true,
            fail: failure,
            ..Fake::default()
        };
        let result = workflow::execute(&mut state, &config, &mut driver);
        assert_eq!(result.is_ok(), !matches!(failure, "capture" | "verify"));
        assert!(driver.active);
        assert!(!driver.calls.contains(&"stop") && !driver.calls.contains(&"start"));
        assert!(state.status.restart.is_none());
    }
}

#[test]
fn job_restarts_before_verification_and_retention() {
    let fixture = Fixture::new();
    let mut state = fixture.state();
    let mut driver = Fake {
        active: true,
        ..Fake::default()
    };
    workflow::execute(&mut state, &config(), &mut driver).unwrap();
    assert_eq!(
        driver.calls,
        ["preflight", "stop", "capture", "start", "verify", "retain"]
    );
    assert!(driver.active);
    assert_eq!(state.status.phase, "complete");
    assert!(state.status.restart.is_none());
    assert!(state.status.last_success_at.is_some());
}

#[test]
fn every_failure_path_preserves_restart_and_prevents_false_success() {
    for stage in ["preflight", "stop", "capture", "verify", "retain", "start"] {
        let fixture = Fixture::new();
        let mut state = fixture.state();
        let mut driver = Fake {
            active: true,
            fail: stage,
            ..Fake::default()
        };
        assert!(workflow::execute(&mut state, &config(), &mut driver).is_err());
        assert_eq!(state.status.phase, "failed");
        assert!(state.status.last_success_at.is_none());
        if stage == "start" {
            assert!(state.status.restart.is_some());
            driver.fail = "";
            drop(state);
            let mut state = fixture.state();
            workflow::recover(&mut state, &mut driver).unwrap();
            assert!(state.status.restart.is_none());
        }
        assert!(driver.active, "{stage}");
        if matches!(stage, "preflight" | "stop" | "capture" | "verify" | "start") {
            assert!(!driver.calls.contains(&"retain"));
        }
    }
}

#[test]
fn disabled_job_and_intentionally_inactive_engine_remain_untouched() {
    let fixture = Fixture::new();
    let mut state = fixture.state();
    let mut driver = Fake::default();
    workflow::execute(&mut state, &Config::default(), &mut driver).unwrap();
    assert!(driver.calls.is_empty());
    workflow::execute(&mut state, &config(), &mut driver).unwrap();
    assert_eq!(driver.calls, ["preflight", "capture", "verify", "retain"]);
    assert!(!driver.active);
}

#[test]
fn failed_replication_retries_same_snapshot_without_another_outage_or_retention() {
    let fixture = Fixture::new();
    let mut state = fixture.state();
    let mut cfg = config();
    cfg.replica = Some(super::config::Replica {
        repository: "/backup/replica".into(),
        key_file: "/backup/key".into(),
        url: None,
        token_file: None,
        ca_file: None,
        allow_plaintext: false,
        keep_last: 2,
    });
    let mut driver = Fake {
        active: true,
        fail: "replicate",
        ..Fake::default()
    };
    assert!(workflow::execute(&mut state, &cfg, &mut driver).is_err());
    assert_eq!(driver.attempts, 3);
    assert!(driver.active);
    assert!(!driver.calls.contains(&"retain"));
    let pending = state.status.pending_snapshot.clone().unwrap();
    drop(state);
    let mut state = fixture.state();
    driver.fail = "";
    driver.calls.clear();
    workflow::execute(&mut state, &cfg, &mut driver).unwrap();
    assert_eq!(driver.calls, ["preflight", "verify", "replicate", "retain"]);
    assert_eq!(
        state.status.last_success_snapshot.as_deref(),
        Some(pending.as_str())
    );
}

#[test]
fn durable_restart_intent_survives_controller_loss_and_config_recovery() {
    let fixture = Fixture::new();
    let mut state = fixture.state();
    state.status.restart = Some(config().service);
    state.phase("capturing").unwrap();
    drop(state);
    let mut state = fixture.state();
    let mut driver = Fake::default();
    workflow::recover(&mut state, &mut driver).unwrap();
    assert_eq!(driver.calls, ["start"]);
    assert!(driver.active);
    assert!(state.status.restart.is_none());
}

#[test]
fn job_overlap_corrupt_status_and_unsafe_config_fail_closed() {
    let fixture = Fixture::new();
    let state = fixture.state();
    assert!(State::open(&fixture.0).is_err());
    drop(state);
    std::fs::write(fixture.0.join("status.json"), b"truncated").unwrap();
    assert!(State::open(&fixture.0).is_err());
    let mut cfg = config();
    cfg.service.unit = "--all".into();
    assert!(cfg.validate().is_err());
    cfg = config();
    cfg.repository = "relative".into();
    assert!(cfg.validate().is_err());
    cfg = config();
    cfg.capture_timeout_seconds = 0;
    assert!(cfg.validate().is_err());
    assert!(serde_json::from_str::<Config>(r#"{"enabeld":true}"#).is_err());
}

#[test]
fn pending_snapshot_refuses_changed_destination() {
    let fixture = Fixture::new();
    let mut state = fixture.state();
    let mut driver = Fake {
        active: true,
        fail: "verify",
        ..Fake::default()
    };
    let mut cfg = config();
    assert!(workflow::execute(&mut state, &cfg, &mut driver).is_err());
    cfg.repository = "/different/repository".into();
    driver.fail = "";
    driver.calls.clear();
    assert!(workflow::execute(&mut state, &cfg, &mut driver).is_err());
    assert_eq!(driver.calls, ["preflight"]);
}

#[test]
fn process_timeout_terminates_its_child_and_returns_failure() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let driver = super::process::Processes {
        directory: state.path.clone(),
        service_timeout: 1,
        helper: "/bin/false".into(),
        helper_backup_subcommand: false,
    };
    let mut cmd = std::process::Command::new("/bin/sleep");
    cmd.arg("30");
    let now = std::time::Instant::now();
    assert!(
        driver
            .run(cmd, 1)
            .unwrap_err()
            .to_string()
            .contains("terminated")
    );
    assert!(now.elapsed() < std::time::Duration::from_secs(5));
}
