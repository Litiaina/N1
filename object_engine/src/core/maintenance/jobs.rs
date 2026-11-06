//! Optional, paced maintenance jobs. A stopped/over-budget scrub is an
//! incomplete job, never a declaration that the object engine has failed.
use crate::core::{Error, MaintenanceReport, Result, ScrubOptions, Store};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceConfig {
    pub gc: Option<Schedule>,
    pub scrub: Option<ScrubSchedule>,
    pub calendar_scrub: Option<crate::core::auto_healing::schedule::CalendarScrub>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    pub every_seconds: u64,
    pub start_delay_seconds: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScrubSchedule {
    pub schedule: Schedule,
    pub options: ScrubOptions,
}
impl Schedule {
    fn validate(&self) -> Result<()> {
        if !(1..=90 * 86400).contains(&self.every_seconds) || self.start_delay_seconds > 30 * 86400
        {
            return Err(Error::Invalid(
                "maintenance interval must be 1 second through 90 days; delay at most 30 days"
                    .into(),
            ));
        }
        Ok(())
    }
}
impl MaintenanceConfig {
    pub fn validate(&self) -> Result<()> {
        if self.scrub.is_some() && self.calendar_scrub.is_some() {
            return Err(Error::Invalid(
                "choose an interval or a calendar scrub schedule".into(),
            ));
        }
        if let Some(calendar) = &self.calendar_scrub {
            calendar.validate()?;
        }
        if let Some(gc) = &self.gc {
            gc.validate()?;
        }
        if let Some(scrub) = &self.scrub {
            scrub.schedule.validate()?;
            scrub.options.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct JobStatus {
    pub id: String,
    pub kind: &'static str,
    pub phase: &'static str,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub report: Option<MaintenanceReport>,
    pub error: Option<String>,
}
struct Job {
    status: JobStatus,
    cancel: CancellationToken,
}
#[derive(Default)]
pub struct Jobs {
    current: Mutex<Option<Job>>,
}
impl Jobs {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn status(&self) -> Result<Option<JobStatus>> {
        Ok(self
            .current
            .lock()
            .map_err(|_| Error::Unavailable("maintenance state poisoned".into()))?
            .as_ref()
            .map(|j| j.status.clone()))
    }
    pub fn gc(self: &Arc<Self>, store: Arc<Store>) -> Result<JobStatus> {
        self.start(store, None, false)
    }
    pub fn scrub(self: &Arc<Self>, store: Arc<Store>, options: ScrubOptions) -> Result<JobStatus> {
        options.validate()?;
        self.start(store, Some(options), false)
    }
    pub(crate) fn calendar_scrub(
        self: &Arc<Self>,
        store: Arc<Store>,
        options: ScrubOptions,
        no_timeout: bool,
    ) -> Result<JobStatus> {
        options.validate()?;
        self.start(store, Some(options), no_timeout)
    }
    fn start(
        self: &Arc<Self>,
        store: Arc<Store>,
        options: Option<ScrubOptions>,
        no_timeout: bool,
    ) -> Result<JobStatus> {
        let mut current = self
            .current
            .lock()
            .map_err(|_| Error::Unavailable("maintenance state poisoned".into()))?;
        if current
            .as_ref()
            .is_some_and(|j| matches!(j.status.phase, "running" | "stopping"))
        {
            return Err(Error::Conflict(
                "a maintenance job is already active".into(),
            ));
        }
        let mut random = [0; 16];
        OsRng.fill_bytes(&mut random);
        let id = hex::encode(random);
        let cancel = CancellationToken::new();
        let status = JobStatus {
            id: id.clone(),
            kind: if options.is_some() { "scrub" } else { "gc" },
            phase: "running",
            started_at: now(),
            finished_at: None,
            report: None,
            error: None,
        };
        *current = Some(Job {
            status: status.clone(),
            cancel: cancel.clone(),
        });
        drop(current);
        let jobs = self.clone();
        tokio::spawn(async move {
            let result = match options {
                Some(options) => store.scheduled_scrub(options, cancel, no_timeout).await,
                None => store.garbage_collect(cancel).await,
            };
            if let Ok(mut current) = jobs.current.lock()
                && let Some(job) = current.as_mut().filter(|j| j.status.id == id)
            {
                job.status.finished_at = Some(now());
                match result {
                    Ok(report) => {
                        job.status.phase = if report.stopped {
                            "stopped"
                        } else if report.incomplete > 0 || report.active_skipped > 0 {
                            "incomplete"
                        } else {
                            "completed"
                        };
                        job.status.report = Some(report);
                    }
                    Err(error) => {
                        job.status.phase = "incomplete";
                        job.status.error = Some(error.to_string());
                    }
                }
            }
        });
        Ok(status)
    }
    pub fn stop(&self, id: &str) -> Result<JobStatus> {
        let mut current = self
            .current
            .lock()
            .map_err(|_| Error::Unavailable("maintenance state poisoned".into()))?;
        let job = current
            .as_mut()
            .filter(|j| j.status.id == id)
            .ok_or(Error::NotFound)?;
        if job.status.phase == "running" {
            job.status.phase = "stopping";
            job.cancel.cancel();
        }
        Ok(job.status.clone())
    }
    pub fn stop_current(&self) {
        if let Ok(mut current) = self.current.lock()
            && let Some(job) = current.as_mut()
            && job.status.phase == "running"
        {
            job.status.phase = "stopping";
            job.cancel.cancel();
        }
    }
    pub async fn wait_idle(&self) {
        while self
            .status()
            .ok()
            .flatten()
            .is_some_and(|j| matches!(j.phase, "running" | "stopping"))
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    pub fn schedule(
        self: &Arc<Self>,
        store: Arc<Store>,
        config: MaintenanceConfig,
        stop: CancellationToken,
    ) {
        if let Some(gc) = config.gc {
            self.schedule_one(store.clone(), gc, None, stop.clone());
        }
        if let Some(scrub) = config.scrub {
            self.schedule_one(store, scrub.schedule, Some(scrub.options), stop);
        }
    }
    fn schedule_one(
        self: &Arc<Self>,
        store: Arc<Store>,
        schedule: Schedule,
        options: Option<ScrubOptions>,
        stop: CancellationToken,
    ) {
        let jobs = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(schedule.start_delay_seconds)) => {}
            }
            let mut timer = tokio::time::interval(Duration::from_secs(schedule.every_seconds));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = timer.tick() => {
                        let started = match &options {
                            Some(options) => jobs.scrub(store.clone(), options.clone()),
                            None => jobs.gc(store.clone()),
                        };
                        if let Err(e) = started {
                            tracing::debug!("scheduled maintenance skipped: {e}");
                        }
                    }
                }
            }
        });
    }
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
