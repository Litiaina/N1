//! Bounded recovery job ownership. Stopping requests cooperative cancellation;
//! accepted filesystem work and repository leases stay owned until it returns.
use crate::core::{Error, Result};
use serde::Serialize;
use serde_json::Value;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize)]
pub struct JobStatus {
    pub id: String,
    pub kind: &'static str,
    pub phase: &'static str,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub result: Option<Value>,
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
            .map_err(|_| poisoned())?
            .as_ref()
            .map(|job| job.status.clone()))
    }
    pub fn start<F, Fut>(self: &Arc<Self>, kind: &'static str, run: F) -> Result<JobStatus>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        let mut current = self.current.lock().map_err(|_| poisoned())?;
        if current
            .as_ref()
            .is_some_and(|j| matches!(j.status.phase, "running" | "stopping"))
        {
            return Err(Error::Conflict("a recovery job is already active".into()));
        }
        let cancel = CancellationToken::new();
        let status = JobStatus {
            id: ulid::Ulid::new().to_string(),
            kind,
            phase: "running",
            started_at: now(),
            finished_at: None,
            result: None,
            error: None,
        };
        *current = Some(Job {
            status: status.clone(),
            cancel: cancel.clone(),
        });
        drop(current);
        let jobs = self.clone();
        let id = status.id.clone();
        tokio::spawn(async move {
            let result = tokio::spawn(async move { run(cancel).await })
                .await
                .unwrap_or_else(|e| {
                    Err(Error::Unavailable(format!("recovery worker failed: {e}")))
                });
            if let Ok(mut current) = jobs.current.lock()
                && let Some(job) = current.as_mut().filter(|j| j.status.id == id)
            {
                job.status.finished_at = Some(now());
                match result {
                    Ok(value) => {
                        job.status.phase = "completed";
                        job.status.result = Some(value);
                    }
                    Err(error) => {
                        job.status.phase = if matches!(&error,Error::Io(e) if e.kind()==std::io::ErrorKind::Interrupted)
                        {
                            "stopped"
                        } else {
                            "incomplete"
                        };
                        job.status.error = Some(error.to_string());
                    }
                }
            }
        });
        Ok(status)
    }
    pub fn stop(&self, id: &str) -> Result<JobStatus> {
        let mut current = self.current.lock().map_err(|_| poisoned())?;
        let job = current
            .as_mut()
            .filter(|job| job.status.id == id)
            .ok_or(Error::NotFound)?;
        if job.status.phase == "running" {
            job.status.phase = "stopping";
            job.cancel.cancel();
        }
        Ok(job.status.clone())
    }
    pub fn stop_current(&self) {
        if let Ok(Some(job)) = self.status() {
            let _ = self.stop(&job.id);
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
}
fn poisoned() -> Error {
    Error::Unavailable("recovery job state poisoned".into())
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn stop_keeps_job_slot_owned_until_accepted_work_finishes_and_panics_are_reported() {
        let jobs = Jobs::new();
        let release = Arc::new(tokio::sync::Notify::new());
        let owner = release.clone();
        let status = jobs
            .start("verify", move |cancel| async move {
                cancel.cancelled().await;
                owner.notified().await;
                Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "recovery job stopped",
                )))
            })
            .unwrap();
        assert!(matches!(
            jobs.start("other", |_| async { Ok(Value::Null) }),
            Err(Error::Conflict(_))
        ));
        assert_eq!(jobs.stop(&status.id).unwrap().phase, "stopping");
        tokio::task::yield_now().await;
        assert!(matches!(
            jobs.start("other", |_| async { Ok(Value::Null) }),
            Err(Error::Conflict(_))
        ));
        release.notify_one();
        jobs.wait_idle().await;
        assert_eq!(jobs.status().unwrap().unwrap().phase, "stopped");
        jobs.start("panic", |_| async { panic!("injected worker panic") })
            .unwrap();
        jobs.wait_idle().await;
        let failed = jobs.status().unwrap().unwrap();
        assert_eq!(failed.phase, "incomplete");
        assert!(failed.error.unwrap().contains("worker failed"));
    }
}
