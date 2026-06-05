use crate::enums::app_error_enum::AppError;
use crate::io_control::priority::IoPriority;
use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_util::sync::CancellationToken;

static JOBS: Lazy<Mutex<HashMap<String, ActiveIoJob>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static NEXT_JOB_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IoJobKind {
    ClusterRepair,
    ClusterRebalance,
    ObjectRedistribution,
    DeepMonitor,
    Scrub,
}

#[derive(Clone, Debug)]
struct ActiveIoJob {
    id: u64,
    job: String,
    kind: IoJobKind,
    priority: IoPriority,
    started_at_unix: i64,
    cancel: CancellationToken,
}

#[derive(Clone, Debug, Serialize)]
pub struct IoJobSnapshot {
    pub job: String,
    pub kind: IoJobKind,
    pub priority: IoPriority,
    pub started_at_unix: i64,
    pub cancel_requested: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct IoJobRegistrySnapshot {
    pub active_jobs: Vec<IoJobSnapshot>,
    pub active_job_count: usize,
}

pub struct IoJobGuard {
    job: String,
    id: u64,
    cancel: CancellationToken,
}

impl IoJobGuard {
    pub fn token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

impl Drop for IoJobGuard {
    fn drop(&mut self) {
        let Ok(mut jobs) = JOBS.lock() else {
            return;
        };
        if jobs
            .get(&self.job)
            .is_some_and(|active| active.id == self.id)
        {
            jobs.remove(&self.job);
        }
    }
}

pub fn register_io_job(
    job: &str,
    kind: IoJobKind,
    priority: IoPriority,
) -> Result<IoJobGuard, AppError> {
    let job = job.trim();
    if job.is_empty() {
        return Err(AppError::InternalServerError(
            "N2 IO job missing name".into(),
        ));
    }
    let mut jobs = JOBS
        .lock()
        .map_err(|_| AppError::InternalServerError("N2 IO job registry poisoned".into()))?;
    if jobs.contains_key(job) {
        return Err(AppError::Conflict(format!("{job} is already running")));
    }
    let id = NEXT_JOB_ID.fetch_add(1, Ordering::Relaxed);
    let cancel = CancellationToken::new();
    jobs.insert(
        job.to_string(),
        ActiveIoJob {
            id,
            job: job.to_string(),
            kind,
            priority,
            started_at_unix: chrono::Utc::now().timestamp(),
            cancel: cancel.clone(),
        },
    );
    Ok(IoJobGuard {
        job: job.to_string(),
        id,
        cancel,
    })
}

pub fn request_io_job_cancel(job: &str) -> bool {
    let Ok(jobs) = JOBS.lock() else {
        return false;
    };
    let Some(active) = jobs.get(job.trim()) else {
        return false;
    };
    active.cancel.cancel();
    true
}

pub fn io_job_registry_snapshot() -> IoJobRegistrySnapshot {
    let Ok(jobs) = JOBS.lock() else {
        return IoJobRegistrySnapshot {
            active_jobs: Vec::new(),
            active_job_count: 0,
        };
    };
    let mut active_jobs = jobs
        .values()
        .map(|job| IoJobSnapshot {
            job: job.job.clone(),
            kind: job.kind,
            priority: job.priority,
            started_at_unix: job.started_at_unix,
            cancel_requested: job.cancel.is_cancelled(),
        })
        .collect::<Vec<_>>();
    active_jobs.sort_by(|a, b| a.job.cmp(&b.job));
    IoJobRegistrySnapshot {
        active_job_count: active_jobs.len(),
        active_jobs,
    }
}
