use super::{
    BuilderError,
    builder::{self, ActiveProgress},
    models::*,
};
use crate::{
    MetadataExecutor, Table, fragments,
    object_store::NativeObjectStore,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
static CONTEXT: OnceLock<Context> = OnceLock::new();
pub(super) struct Context {
    pub db: MetadataExecutor,
    pub objects: NativeObjectStore,
    pub root: PathBuf,
    pub config: ExportConfig,
    pub jobs: Mutex<HashMap<String, Arc<ActiveProgress>>>,
    pub tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub stopping: AtomicBool,
    pub stop: tokio_util::sync::CancellationToken,
    pub gates: Mutex<HashMap<String, Weak<RwLock<()>>>>,
    pub queue: Arc<Semaphore>,
    pub slots: Arc<Semaphore>,
    pub downloads: Arc<Semaphore>,
}
pub(super) fn context() -> &'static Context {
    CONTEXT.get().expect("metadata export service initialized")
}
pub(super) fn export_config() -> &'static ExportConfig {
    &context().config
}
pub(super) fn artifact_job_directory(root: &Path, id: &str) -> PathBuf {
    root.join("artifacts").join(id)
}
pub(super) fn manifest_path(id: &str) -> Result<PathBuf, BuilderError> {
    validate_id(id)
        .map_err(|_| BuilderError::InternalServerError("invalid export identity".into()))?;
    Ok(context()
        .root
        .join("manifests")
        .join(format!("{id}.ndjson")))
}
pub(super) fn validate_id(id: &str) -> ApiResult<()> {
    if ulid::Ulid::from_string(id).map_or(true, |v| v.to_string() != id) {
        return Err(ApiError::invalid("invalid export identity"));
    }
    Ok(())
}
pub(super) async fn cleanup_artifact_chunks(id: &str) {
    if validate_id(id).is_ok() {
        let _ = tokio::fs::remove_dir_all(artifact_job_directory(&context().root, id)).await;
    }
}
pub(super) async fn load(id: &str) -> ApiResult<PersistedExport> {
    validate_id(id)?;
    let key = fragments::key(format!("export/{id}"))?;
    Ok(context()
        .db
        .execute(move |db| {
            fragments::decode(
                db.snapshot()
                    .get(Table::Reclamation, &key)?
                    .ok_or(crate::Error::NotFound)?,
            )
        })
        .await?)
}
pub(super) async fn insert(record: PersistedExport) -> ApiResult<()> {
    persist(record, true).await
}
pub(super) async fn save(record: PersistedExport) -> ApiResult<()> {
    persist(record, false).await
}
pub(super) async fn cancel(id: String) -> ApiResult<()> {
    let key = fragments::key(format!("export/{id}"))?;
    context()
        .db
        .execute(move |db| {
            db.transact(64, |tx| {
                let bytes = tx
                    .get(Table::Reclamation, &key)?
                    .ok_or(crate::Error::NotFound)?;
                let mut record: PersistedExport = fragments::decode(bytes)?;
                tx.remove(Table::Reclamation, &super::lifecycle::expiry(&record)?)?;
                // Preserve the latest reservation/chunks if publication raced the
                // request's authorization read. Cleanup releases their charge.
                record.phase = ExportPhase::Cancelled;
                record.updated_at_unix = crate::middleware::auth::now() as i64;
                record.expires_at_unix = record.updated_at_unix;
                tx.put(
                    Table::Reclamation,
                    &super::lifecycle::expiry(&record)?,
                    b"1",
                )?;
                tx.put(Table::Reclamation, &key, &fragments::encode(&record)?)
            })
        })
        .await?;
    Ok(())
}
async fn persist(mut record: PersistedExport, create: bool) -> ApiResult<()> {
    record.updated_at_unix = crate::middleware::auth::now() as i64;
    let key = fragments::key(format!("export/{}", record.id))?;
    let limit = export_config().max_retained_bytes;
    context()
        .db
        .execute(move |db| {
            db.transact(64, |tx| {
                let previous = tx
                    .get(Table::Reclamation, &key)?
                    .map(fragments::decode::<PersistedExport>)
                    .transpose()?;
                if create && previous.is_some() {
                    return Err(crate::Error::Conflict);
                }
                if !create && previous.is_none() {
                    return Err(crate::Error::NotFound);
                }
                if let Some(previous) = &previous {
                    if previous.phase == ExportPhase::Cancelled {
                        return Ok(());
                    }
                    tx.remove(Table::Reclamation, &super::lifecycle::expiry(previous)?)?;
                }
                super::lifecycle::charge(
                    tx,
                    previous.as_ref().map_or(0, |p| p.reserved_bytes),
                    record.reserved_bytes,
                    limit,
                )?;
                tx.put(
                    Table::Reclamation,
                    &super::lifecycle::expiry(&record)?,
                    b"1",
                )?;
                tx.put(Table::Reclamation, &key, &fragments::encode(&record)?)
            })
        })
        .await?;
    Ok(())
}
pub(super) async fn initialize(app: &Arc<App>) -> ApiResult<()> {
    let config: ExportConfig =
        serde_json::from_value(serde_json::to_value(&app.config.export).expect("export settings"))
            .map_err(|_| ApiError::invalid("invalid export settings"))?;
    let root = app.config.staging_dir.join("exports");
    crate::uploads::private_directory(&root).await?;
    crate::uploads::private_directory(&root.join("manifests")).await?;
    crate::uploads::private_directory(&root.join("artifacts")).await?;
    let _ = builder::export_buffer_plan(&config, 1);
    let slots = config.max_concurrent_jobs;
    let queued = config.max_queued_jobs;
    let downloads = config.max_concurrent_downloads;
    CONTEXT
        .set(Context {
            db: app.db.clone(),
            objects: app.service.objects(),
            root,
            config,
            jobs: Mutex::new(HashMap::new()),
            tasks: Mutex::new(Vec::new()),
            stopping: AtomicBool::new(false),
            stop: tokio_util::sync::CancellationToken::new(),
            gates: Mutex::new(HashMap::new()),
            queue: Arc::new(Semaphore::new(queued)),
            slots: Arc::new(Semaphore::new(slots)),
            downloads: Arc::new(Semaphore::new(downloads)),
        })
        .map_err(|_| ApiError::invalid("one metadata service may run per process"))?;
    super::archive::recover().await?;
    let recovery = tokio::spawn(super::scheduler::recover());
    let cleanup = tokio::spawn(super::scheduler::run());
    context()
        .tasks
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .extend([
            tokio::spawn(async move {
                match recovery.await {
                    Ok(Ok(())) => {}
                    result => tracing::error!(?result, "export recovery admission stopped"),
                }
            }),
            cleanup,
        ]);
    Ok(())
}
pub(super) fn admit() -> ApiResult<OwnedSemaphorePermit> {
    let ctx = context();
    if ctx.stopping.load(Ordering::Acquire) {
        return Err(ApiError::invalid("export service is shutting down"));
    }
    ctx.queue.clone().try_acquire_owned().map_err(|_| {
        ApiError(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "Export queue is full".into(),
        )
    })
}
pub(super) fn enqueue_admitted(
    record: PersistedExport,
    permit: OwnedSemaphorePermit,
) -> ApiResult<()> {
    let ctx = context();
    let mut tasks = ctx.tasks.lock().unwrap_or_else(|e| e.into_inner());
    if ctx.stopping.load(Ordering::Acquire) {
        return Err(ApiError::invalid("export service is shutting down"));
    }
    let progress = Arc::new(ActiveProgress::new());
    {
        let mut jobs = ctx.jobs.lock().unwrap_or_else(|e| e.into_inner());
        if jobs.contains_key(&record.id) {
            return Ok(());
        }
        jobs.insert(record.id.clone(), progress.clone());
    }
    let task = tokio::spawn(async move {
        let _permit = permit;
        let slot = ctx.slots.acquire().await;
        if slot.is_err() {
            return;
        }
        let _slot = slot.unwrap();
        let id = record.id.clone();
        let _lifecycle = super::lifecycle::gate(&id).read_owned().await;
        let mut record = record;
        let result = async {
            record = load(&record.id).await?;
            if record.phase.is_terminal() {
                return Ok(());
            }
            progress.ensure_running()?;
            if record.expires_at_unix <= crate::middleware::auth::now() as i64 {
                return Err(ApiError::invalid("export no longer queued"));
            }
            if !record.manifest_ready {
                record.phase = ExportPhase::CapturingManifest;
                save(record.clone()).await?;
                let request = record.request.clone();
                let fragment = record.fragment.clone();
                let manifest = manifest_path(&record.id)?;
                let cancelled = progress.cancel.clone();
                let summary = ctx
                    .db
                    .execute(move |db| {
                        super::manifest::capture_with_cancel(
                            &db,
                            &fragment,
                            &request,
                            &manifest,
                            Some(&cancelled),
                        )
                    })
                    .await?;
                record.objects_total = summary.objects;
                record.source_bytes_total = summary.source_bytes;
                record.manifest_ready = true;
                save(record.clone()).await?;
            }
            record.reserved_bytes = super::lifecycle::estimated_bytes(&record);
            save(record.clone()).await?;
            progress.ensure_running()?;
            record.phase = ExportPhase::Building;
            save(record.clone()).await?;
            let artifact = builder::build_artifact(
                &record.id,
                &record.fragment,
                &record.format,
                vec![ctx.root.clone()],
                &ctx.config,
                progress.clone(),
                if ctx.config.max_retained_bytes == 0 {
                    0
                } else {
                    record.reserved_bytes
                },
            )
            .await?;
            progress.ensure_running()?;
            record.chunks = artifact.chunks;
            record.artifact_size_bytes = Some(artifact.size_bytes);
            record.etag_sha256 = Some(artifact.sha256);
            record.objects_completed = progress.objects_completed.load(Ordering::Relaxed);
            record.source_bytes_completed = progress.source_bytes_completed.load(Ordering::Relaxed);
            record.artifact_bytes_completed = artifact.size_bytes;
            record.reserved_bytes = artifact.size_bytes;
            record.phase = ExportPhase::Ready;
            record.updated_at_unix = crate::middleware::auth::now() as i64;
            save(record.clone()).await?;
            Ok::<(), ApiError>(())
        }
        .await;
        if let Err(error) = result {
            if super::lifecycle::cleanup_chunks(&id).await.is_ok() {
                record.reserved_bytes = 0;
                record.chunks.clear();
                record.artifact_size_bytes = None;
                record.etag_sha256 = None;
                record.artifact_bytes_completed = 0;
            }
            record.phase = if ctx.stopping.load(Ordering::Acquire) {
                ExportPhase::Queued
            } else if progress.cancel.load(Ordering::Acquire) {
                ExportPhase::Cancelled
            } else {
                ExportPhase::Failed
            };
            if record.phase == ExportPhase::Failed {
                record.objects_completed = progress.objects_completed.load(Ordering::Relaxed);
                record.source_bytes_completed =
                    progress.source_bytes_completed.load(Ordering::Relaxed);
            }
            record.error = Some(error.to_string());
            let _ = save(record.clone()).await;
        }
        ctx.jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
        // A graceful stop queues the same immutable manifest for restart.
        // Its source versions must stay pinned while the service is down.
        if record.phase.is_terminal() {
            let job = id.clone();
            if let Err(error) = ctx
                .db
                .execute(move |db| crate::namespace::Namespace::new(db).unpin_job(&job))
                .await
            {
                tracing::error!(%error,"export source pins retained after cleanup failure");
            }
        }
    });
    tasks.retain(|task| !task.is_finished());
    tasks.push(task);
    Ok(())
}
pub(super) async fn shutdown() {
    let Some(ctx) = CONTEXT.get() else {
        return;
    };
    ctx.stopping.store(true, Ordering::Release);
    ctx.stop.cancel();
    for progress in ctx.jobs.lock().unwrap_or_else(|e| e.into_inner()).values() {
        progress.cancel.store(true, Ordering::Release);
    }
    let tasks = std::mem::take(&mut *ctx.tasks.lock().unwrap_or_else(|e| e.into_inner()));
    for task in tasks {
        let _ = task.await;
    }
}
