//! Optional systemd-supervised offline backup jobs, separate from the engine.
mod config;
mod process;
mod state;
mod workflow;

use super::Result;
use std::path::Path;

pub fn run(path: &Path) -> Result<()> {
    let config = config::Config::load(path)?;
    if config.enabled {
        // Reject misplaced state BEFORE writing a journal/lock into engine data.
        let candidate = if config.state_dir.try_exists()? {
            std::fs::canonicalize(&config.state_dir)?
        } else {
            let parent = config
                .state_dir
                .parent()
                .ok_or_else(|| super::invalid("state directory has no parent"))?;
            std::fs::canonicalize(parent)?.join(
                config
                    .state_dir
                    .file_name()
                    .ok_or_else(|| super::invalid("invalid state directory"))?,
            )
        };
        for root in super::capture::roots_from_config(&config.engine_config)? {
            if candidate.starts_with(&root.path) || Path::new(&root.path).starts_with(&candidate) {
                return Err(super::invalid("job state must be outside engine roots"));
            }
        }
        let repository = std::fs::canonicalize(&config.repository)?;
        if candidate.starts_with(&repository) || repository.starts_with(&candidate) {
            return Err(super::invalid("job state and repository must not overlap"));
        }
    }
    let mut state = state::State::open(&config.state_dir)?;
    let mut driver = process::Processes {
        directory: state.path.clone(),
        service_timeout: config.service_timeout_seconds,
        helper: std::env::current_exe()?,
    };
    if let Some(service) = state.status.restart.clone() {
        let _previous = state::service_lock(&service)?;
        workflow::recover(&mut state, &mut driver)?;
    }
    if !config.enabled {
        return state.phase("disabled");
    }
    let _service = state::service_lock(&config.service)?;
    workflow::execute(&mut state, &config, &mut driver)
}

/// Recovery entrypoint for ExecStopPost and the independent OnFailure/boot unit.
/// Uses the recorded service, not a potentially edited job config.
pub fn recover(path: &Path) -> Result<()> {
    let mut state = state::State::open(path)?;
    if let Some(service) = state.status.restart.clone() {
        let _service = state::service_lock(&service)?;
        let mut driver = process::Processes {
            directory: state.path.clone(),
            service_timeout: 330,
            helper: std::env::current_exe()?,
        };
        if let Err(error) = workflow::recover(&mut state, &mut driver) {
            state.status.phase = "restart_required".into();
            state.status.error = Some(error.to_string().chars().take(4096).collect());
            state.save()?;
            return Err(error);
        }
        state.status.error =
            Some("interrupted backup job; engine restarted by recovery supervisor".into());
        state.phase("interrupted")?;
    } else if matches!(
        state.status.phase.as_str(),
        "stopping" | "capturing" | "verifying" | "replicating" | "retaining"
    ) {
        state.status.error =
            Some("backup job interrupted; no successful completion recorded".into());
        state.phase("interrupted")?;
    }
    Ok(())
}

pub fn status(path: &Path) -> Result<()> {
    let config = config::Config::load(path)?;
    let status = state::State::read(&config.state_dir)?;
    let age = status
        .last_success_at
        .map(|time| chrono::Utc::now().timestamp().saturating_sub(time));
    let healthy = config.enabled
        && status.phase == "complete"
        && status.restart.is_none()
        && age.is_some_and(|age| age >= 0 && age as u64 <= config.max_age_seconds);
    println!(
        "{}",
        serde_json::json!({"enabled":config.enabled,"healthy":healthy,
        "max_age_seconds":config.max_age_seconds,"last_success_age_seconds":age,"status":status})
    );
    if config.enabled && !healthy {
        return Err(super::invalid(
            "automatic backup is overdue, incomplete or failed",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
