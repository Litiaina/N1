use super::super::{Result, invalid};
use super::config::{Config, Service};
use super::state::State;

pub trait Driver {
    fn preflight(&mut self, config: &Config) -> Result<()>;
    fn active(&mut self, service: &Service) -> Result<bool>;
    fn stop(&mut self, service: &Service) -> Result<()>;
    fn start(&mut self, service: &Service) -> Result<()>;
    fn capture(&mut self, config: &Config) -> Result<String>;
    fn verify(&mut self, config: &Config, snapshot: &str) -> Result<()>;
    fn replicate(&mut self, config: &Config, snapshot: &str) -> Result<()>;
    fn retain(&mut self, config: &Config, snapshot: &str) -> Result<()>;
    fn retry_pause(&mut self) {
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}

pub fn recover(state: &mut State, driver: &mut impl Driver) -> Result<()> {
    if let Some(service) = state.status.restart.clone() {
        service.validate()?;
        // Keep the durable intent until start AND its active-state check succeed.
        driver.start(&service)?;
        if !driver.active(&service)? {
            return Err(invalid("engine did not become active after backup"));
        }
        state.status.restart = None;
        state.save()?;
    }
    Ok(())
}

pub fn execute(state: &mut State, config: &Config, driver: &mut impl Driver) -> Result<()> {
    let result = execute_inner(state, config, driver);
    // Restart on every ordinary error, including capture/verification/state I/O.
    let recovery = recover(state, driver);
    let outcome = match (result, recovery) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (result, Err(error)) => Err(invalid(format!(
            "{}; ENGINE RESTART REQUIRED: {error}",
            result
                .err()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "backup finished".into())
        ))),
    };
    if let Err(error) = &outcome {
        state.status.phase = "failed".into();
        state.status.error = Some(error.to_string().chars().take(4096).collect());
        // The original error remains visible even if status storage itself failed.
        if let Err(save) = state.save() {
            eprintln!("Cannot persist backup failure status: {save}");
        }
    }
    outcome
}

fn execute_inner(state: &mut State, config: &Config, driver: &mut impl Driver) -> Result<()> {
    recover(state, driver)?;
    if !config.enabled {
        return state.phase("disabled");
    }
    driver.preflight(config)?;
    let identity = super::super::crypto::digest(&serde_json::to_vec(config)?);
    state.status.error = None;
    let snapshot = if let Some(snapshot) = state.status.pending_snapshot.clone() {
        if state.status.pending_config.as_deref() != Some(&identity) {
            return Err(invalid(
                "pending backup belongs to different settings; finish it with its original job configuration",
            ));
        }
        snapshot
    } else {
        let was_active = config.online_source.is_none() && driver.active(&config.service)?;
        if was_active {
            state.status.restart = Some(config.service.clone());
            state.phase("stopping")?;
            driver.stop(&config.service)?;
        }
        state.phase("capturing")?;
        let capture = driver.capture(config);
        if let Ok(snapshot) = &capture {
            super::super::fs::safe_id(snapshot)?;
            state.status.pending_snapshot = Some(snapshot.clone());
            state.status.pending_config = Some(identity);
        }
        // Save snapshot progress while the restart intent remains set.
        state.save()?;
        recover(state, driver)?;
        capture?
    };
    state.phase("verifying")?;
    driver.verify(config, &snapshot)?;
    if config.replica.is_some() {
        state.phase("replicating")?;
        for attempt in 1..=config.replication_attempts {
            match driver.replicate(config, &snapshot) {
                Ok(()) => break,
                Err(error) if attempt == config.replication_attempts => return Err(error),
                Err(error) => {
                    eprintln!(
                        "Replication attempt {attempt} failed: {error}; retrying verified snapshot"
                    );
                    driver.retry_pause();
                }
            }
        }
    }
    state.phase("retaining")?;
    driver.retain(config, &snapshot)?;
    state.status.last_success_at = Some(chrono::Utc::now().timestamp());
    state.status.last_success_snapshot = Some(snapshot);
    state.status.pending_snapshot = None;
    state.status.pending_config = None;
    state.phase("complete")
}
