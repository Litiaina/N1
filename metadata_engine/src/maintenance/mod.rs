//! Metadata-owned, bounded maintenance. Only the public native object API is
//! used here; member paths and object-engine implementation are not available.
mod gc;
mod migration;
use crate::{
    Error, MetadataExecutor, RecordKey, Result, Table, fragments,
    namespace::{Namespace, store::lifecycle},
    object_store::NativeObjectStore,
    server::{
        PoolProfile,
        error::{ApiError, ApiResult},
    },
};
use n1_link::PoolId;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Serialize)]
pub struct Config {
    pub enabled: bool,
    pub cleanup_interval_seconds: u64,
    pub trash_retention_seconds: u64,
    pub batch_size: usize,
    pub object_concurrency: usize,
    pub pause_millis: u64,
    pub bytes_per_second: u64,
    pub max_part_size: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            cleanup_interval_seconds: 3600,
            trash_retention_seconds: 2592000,
            batch_size: 128,
            object_concurrency: 1,
            pause_millis: 25,
            bytes_per_second: 16 * 1024 * 1024,
            max_part_size: 50 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Gc,
    Repair,
    Redistribution,
}
impl Kind {
    pub fn parse(value: &str) -> ApiResult<Self> {
        match value {
            "gc" => Ok(Self::Gc),
            "repair" => Ok(Self::Repair),
            "redistribution" => Ok(Self::Redistribution),
            _ => Err(ApiError::invalid("unknown maintenance job")),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Gc => "gc",
            Self::Repair => "repair",
            Self::Redistribution => "redistribution",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub fragment: Option<PoolId>,
    pub object: Option<String>,
    pub source_pool: Option<PoolId>,
    pub target_pool: Option<PoolId>,
    pub dry_run: bool,
    pub repair: bool,
    pub batch_size: usize,
    pub object_concurrency: usize,
    pub pause_millis: u64,
    pub bytes_per_second: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub kind: Kind,
    pub phase: String,
    pub request: Request,
    pub started_at_unix: u64,
    pub updated_at_unix: u64,
    pub scanned: u64,
    pub processed: u64,
    pub bytes_processed: u64,
    pub failures: u64,
    pub deferred: u64,
    pub last_error: Option<String>,
    cursor: Option<RecordKey>,
    pool_cursor: usize,
}
impl Job {
    pub fn running(&self) -> bool {
        matches!(self.phase.as_str(), "running" | "stopping" | "interrupted")
    }
}
struct Slot {
    job: Job,
    cancel: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
}
pub(crate) struct Manager {
    db: MetadataExecutor,
    objects: NativeObjectStore,
    pub config: Config,
    pools: Vec<PoolProfile>,
    staging: std::path::PathBuf,
    upload_locks: Arc<crate::uploads::UploadLocks>,
    slots: Mutex<BTreeMap<Kind, Slot>>,
    admission: tokio::sync::Mutex<()>,
    stopping: AtomicBool,
    scheduler: Mutex<Option<JoinHandle<()>>>,
    stop: tokio_util::sync::CancellationToken,
}
fn job_key(kind: Kind) -> Result<RecordKey> {
    lifecycle::catalog(format!("maintenance-job/{}", kind.name()))
}
impl Manager {
    /// Explicit integrity scans share bounded maintenance resources, never the
    /// foreground transaction or upload connection permits.
    pub(crate) fn diagnostic_handles(&self) -> (MetadataExecutor, NativeObjectStore) {
        (self.db.clone(), self.objects.clone())
    }
    pub(crate) fn new(
        db: MetadataExecutor,
        objects: NativeObjectStore,
        config: Config,
        pools: Vec<PoolProfile>,
        staging: std::path::PathBuf,
        upload_locks: Arc<crate::uploads::UploadLocks>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            objects,
            config,
            pools,
            staging,
            upload_locks,
            slots: Mutex::new(BTreeMap::new()),
            admission: tokio::sync::Mutex::new(()),
            stopping: AtomicBool::new(false),
            scheduler: Mutex::new(None),
            stop: tokio_util::sync::CancellationToken::new(),
        })
    }
    pub fn default_request(&self, fragment: Option<PoolId>) -> Request {
        Request {
            fragment,
            object: None,
            source_pool: None,
            target_pool: None,
            dry_run: false,
            repair: true,
            batch_size: self.config.batch_size,
            object_concurrency: self.config.object_concurrency,
            pause_millis: self.config.pause_millis,
            bytes_per_second: self.config.bytes_per_second,
        }
    }
    async fn save(&self, job: &Job) -> ApiResult<()> {
        let record = job.clone();
        let record = self
            .db
            .execute(move |db| {
                db.transact(64, |tx| {
                    let mut record = record.clone();
                    if record.phase == "running"
                        && tx
                            .get(Table::Reclamation, &job_key(record.kind)?)?
                            .map(fragments::decode::<Job>)
                            .transpose()?
                            .is_some_and(|old| old.id == record.id && old.phase == "stopping")
                    {
                        record.phase = "stopping".into();
                    }
                    tx.put(
                        Table::Reclamation,
                        &job_key(record.kind)?,
                        &fragments::encode(&record)?,
                    )?;
                    Ok(record)
                })
            })
            .await?;
        if let Some(slot) = self
            .slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&job.kind)
        {
            slot.job = record;
        }
        Ok(())
    }
    pub async fn status(&self, kind: Kind) -> ApiResult<serde_json::Value> {
        let job = self
            .slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&kind)
            .map(|slot| slot.job.clone());
        let job = match job {
            Some(job) => Some(job),
            None => {
                self.db
                    .execute(move |db| {
                        db.snapshot()
                            .get(Table::Reclamation, &job_key(kind)?)?
                            .map(fragments::decode::<Job>)
                            .transpose()
                    })
                    .await?
            }
        };
        let mut value = job
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| ApiError::invalid("invalid maintenance state"))?
            .unwrap_or_else(|| serde_json::json!({"kind":kind,"phase":"idle"}));
        value["running"] = serde_json::json!(job.as_ref().is_some_and(Job::running));
        value["policy"] = serde_json::to_value(&self.config).expect("maintenance configuration");
        value["owner"] = serde_json::json!("metadata_engine");
        Ok(value)
    }
    pub async fn start(self: &Arc<Self>, kind: Kind, mut request: Request) -> ApiResult<Job> {
        let _gate = self.admission.lock().await;
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Closed.into());
        }
        if request
            .source_pool
            .is_some_and(|source| !self.pools.iter().any(|pool| pool.id == source))
        {
            return Err(ApiError::invalid("unknown source pool"));
        }
        if request.batch_size == 0
            || request.batch_size > self.config.batch_size
            || request.object_concurrency == 0
            || request.object_concurrency > self.config.object_concurrency
            || request.pause_millis < self.config.pause_millis
            || request.pause_millis > 60000
            || request.bytes_per_second == 0
            || request.bytes_per_second > self.config.bytes_per_second
        {
            return Err(ApiError::invalid(
                "maintenance controls exceed configured bounds",
            ));
        }
        if kind == Kind::Repair {
            request.object_concurrency = 1;
        }
        if kind == Kind::Redistribution {
            let source = request
                .source_pool
                .ok_or_else(|| ApiError::invalid("source_pool is required"))?;
            let target = request
                .target_pool
                .ok_or_else(|| ApiError::invalid("target_pool is required"))?;
            if source == target
                || !self.pools.iter().any(|p| p.id == source)
                || !self.pools.iter().any(|p| p.id == target && !p.sealed)
            {
                return Err(ApiError::invalid(
                    "select different enrolled source and unsealed destination pools",
                ));
            }
        }
        let previous = self
            .db
            .execute(move |db| {
                db.snapshot()
                    .get(Table::Reclamation, &job_key(kind)?)?
                    .map(fragments::decode::<Job>)
                    .transpose()
            })
            .await?;
        if self
            .slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&kind)
            .is_some_and(|s| s.job.running() && s.task.as_ref().is_some_and(|t| !t.is_finished()))
        {
            return Err(ApiError(
                axum::http::StatusCode::CONFLICT,
                "maintenance job already running".into(),
            ));
        }
        let now = crate::middleware::auth::now();
        let job = if let Some(mut old) =
            previous.filter(|old| old.phase == "incomplete" && kind == Kind::Redistribution)
        {
            if old.request.fragment != request.fragment
                || old.request.object != request.object
                || old.request.source_pool != request.source_pool
                || old.request.target_pool != request.target_pool
                || old.request.dry_run != request.dry_run
            {
                return Err(ApiError(axum::http::StatusCode::CONFLICT,"retry the incomplete migration with its original parameters before starting another".into()));
            }
            old.request = request;
            old.phase = "running".into();
            old.last_error = None;
            old.failures = 0;
            old
        } else {
            Job {
                id: Ulid::new().to_string(),
                kind,
                phase: "running".into(),
                request,
                started_at_unix: now,
                updated_at_unix: now,
                scanned: 0,
                processed: 0,
                bytes_processed: 0,
                failures: 0,
                deferred: 0,
                last_error: None,
                cursor: None,
                pool_cursor: 0,
            }
        };
        self.save(&job).await?;
        self.launch(job.clone());
        Ok(job)
    }
    fn launch(self: &Arc<Self>, mut job: Job) {
        job.request.object_concurrency = job
            .request
            .object_concurrency
            .min(self.config.object_concurrency)
            .max(1);
        job.request.batch_size = job.request.batch_size.min(self.config.batch_size).max(1);
        job.request.bytes_per_second = job
            .request
            .bytes_per_second
            .min(self.config.bytes_per_second)
            .max(1);
        job.request.pause_millis = job.request.pause_millis.max(self.config.pause_millis);
        let cancel = Arc::new(AtomicBool::new(job.phase == "stopping"));
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).insert(
            job.kind,
            Slot {
                job: job.clone(),
                cancel: cancel.clone(),
                task: None,
            },
        );
        let this = self.clone();
        let kind = job.kind;
        let task = tokio::spawn(async move {
            this.run(job, cancel).await;
        });
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&kind)
            .expect("installed job")
            .task = Some(task);
    }
    pub async fn stop_job(self: &Arc<Self>, kind: Kind) -> ApiResult<()> {
        let _gate = self.admission.lock().await;
        let (cached, active) = {
            let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(slot) = slots.get_mut(&kind) {
                slot.cancel.store(true, Ordering::Release);
                (
                    Some(slot.job.clone()),
                    slot.task.as_ref().is_some_and(|task| !task.is_finished()),
                )
            } else {
                (None, false)
            }
        };
        let current = match cached {
            Some(job) => Some(job),
            None => {
                self.db
                    .execute(move |db| {
                        db.snapshot()
                            .get(Table::Reclamation, &job_key(kind)?)?
                            .map(fragments::decode::<Job>)
                            .transpose()
                    })
                    .await?
            }
        };
        let mut job = current
            .filter(|job| job.running() || job.phase == "incomplete")
            .ok_or_else(|| ApiError::invalid("no matching job is running or incomplete"))?;
        job.phase = "stopping".into();
        self.save(&job).await?;
        if !active {
            self.launch(job);
        }
        Ok(())
    }
    pub(crate) async fn resume_and_schedule(self: &Arc<Self>) -> ApiResult<()> {
        for kind in [Kind::Gc, Kind::Repair, Kind::Redistribution] {
            let job = self
                .db
                .execute(move |db| {
                    db.snapshot()
                        .get(Table::Reclamation, &job_key(kind)?)?
                        .map(fragments::decode::<Job>)
                        .transpose()
                })
                .await?;
            if let Some(job) = job.filter(Job::running) {
                self.launch(job);
            }
        }
        if self.config.enabled {
            let this = self.clone();
            let task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(
                    this.config.cleanup_interval_seconds,
                ));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                interval.tick().await; // First scheduled pass follows one interval.
                loop {
                    tokio::select! {
                        _ = this.stop.cancelled() => break,
                        _ = interval.tick() => {
                            let _ = this.start(Kind::Gc, this.default_request(None)).await;
                        }
                    }
                }
            });
            *self.scheduler.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
        }
        Ok(())
    }
    async fn run(self: Arc<Self>, mut job: Job, cancel: Arc<AtomicBool>) {
        let result = match job.kind {
            Kind::Gc => self.gc(&mut job, &cancel).await,
            Kind::Repair => self.repair(&mut job, &cancel).await,
            Kind::Redistribution => self.migrate(&mut job, &cancel).await,
        };
        job.updated_at_unix = crate::middleware::auth::now();
        if self.stopping.load(Ordering::Acquire) {
            job.phase = if cancel.load(Ordering::Acquire) {
                "stopping"
            } else {
                "interrupted"
            }
            .into();
        } else if let Err(error) = result {
            job.phase = "incomplete".into();
            job.last_error = Some(error.to_string());
            job.failures += 1;
        } else if cancel.load(Ordering::Acquire) {
            job.phase = "stopped".into();
        } else {
            job.phase = if job.failures > 0 {
                "incomplete"
            } else {
                "completed"
            }
            .into();
        }
        if let Err(error) = self.save(&job).await {
            tracing::error!(%error,"maintenance state could not be persisted; prior intent retained");
        }
    }
    fn interrupted(&self, cancel: &AtomicBool) -> bool {
        self.stopping.load(Ordering::Acquire) || cancel.load(Ordering::Acquire)
    }
    async fn pace(&self, millis: u64) {
        if millis > 0 {
            tokio::time::sleep(Duration::from_millis(millis)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }
    pub(crate) async fn shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        self.stop.cancel();
        let scheduler = self
            .scheduler
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(task) = scheduler {
            let _ = task.await;
        }
        let tasks: Vec<_> = self
            .slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values_mut()
            .filter_map(|s| s.task.take())
            .collect();
        for task in tasks {
            let _ = task.await;
        }
        self.objects.shutdown().await;
        self.db.shutdown().await;
    }
}
use ulid::Ulid;
