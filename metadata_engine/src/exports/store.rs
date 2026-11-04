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
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Semaphore;
static CONTEXT: OnceLock<Context> = OnceLock::new();
pub(super) struct Context {
    pub db: MetadataExecutor,
    pub objects: NativeObjectStore,
    pub root: PathBuf,
    pub config: ExportConfig,
    pub jobs: Mutex<HashMap<String, Arc<ActiveProgress>>>,
    pub tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub stopping: AtomicBool,
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
pub(super) async fn save(record: PersistedExport) -> ApiResult<()> {
    let key = fragments::key(format!("export/{}", record.id))?;
    context()
        .db
        .execute(move |db| {
            db.transact(64, |tx| {
                if let Some(bytes) = tx.get(Table::Reclamation, &key)? {
                    let previous: PersistedExport = fragments::decode(bytes)?;
                    if previous.phase == ExportPhase::Cancelled {
                        return Ok(());
                    }
                }
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
            queue: Arc::new(Semaphore::new(queued)),
            slots: Arc::new(Semaphore::new(slots)),
            downloads: Arc::new(Semaphore::new(downloads)),
        })
        .map_err(|_| ApiError::invalid("one metadata service may run per process"))?;
    let mut after = None;
    loop {
        let cursor = after.clone();
        let rows = context()
            .db
            .execute(move |db| {
                db.snapshot().scan(
                    Table::Reclamation,
                    fragments::CATALOG,
                    "export/",
                    cursor.as_ref(),
                    1000,
                )
            })
            .await?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|r| r.0.clone());
        for (_, bytes) in rows {
            let record: PersistedExport = fragments::decode(bytes)?;
            if !record.phase.is_terminal() {
                enqueue(record).await?;
            } else {
                let job = record.id;
                context()
                    .db
                    .execute(move |db| crate::namespace::Namespace::new(db).unpin_job(&job))
                    .await?;
            }
        }
    }
    Ok(())
}
pub(super) async fn enqueue(record: PersistedExport) -> ApiResult<()> {
    let ctx = context();
    if ctx.stopping.load(Ordering::Acquire) {
        return Err(ApiError::invalid("export service is shutting down"));
    }
    let permit = ctx.queue.clone().try_acquire_owned().map_err(|_| {
        ApiError(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "Export queue is full".into(),
        )
    })?;
    let progress = Arc::new(ActiveProgress::new());
    ctx.jobs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(record.id.clone(), progress.clone());
    let task = tokio::spawn(async move {
        let _permit = permit;
        let slot = ctx.slots.acquire().await;
        if slot.is_err() {
            return;
        }
        let _slot = slot.unwrap();
        let id = record.id.clone();
        let mut record = record;
        let result = async {
            if !record.manifest_ready {
                record.phase = ExportPhase::CapturingManifest;
                save(record.clone()).await?;
                let request = record.request.clone();
                let fragment = record.fragment.clone();
                let manifest = manifest_path(&record.id)?;
                let (count, bytes) = ctx
                    .db
                    .execute(move |db| {
                        super::manifest::capture(&db, &fragment, &request, &manifest)
                    })
                    .await?;
                record.objects_total = count;
                record.source_bytes_total = bytes;
                record.manifest_ready = true;
                save(record.clone()).await?;
            }
            record.phase = ExportPhase::Building;
            save(record.clone()).await?;
            let artifact = builder::build_artifact(
                &record.id,
                &record.fragment,
                &record.format,
                vec![ctx.root.clone()],
                &ctx.config,
                progress.clone(),
            )
            .await?;
            progress.ensure_running()?;
            record.chunks = artifact.chunks;
            record.artifact_size_bytes = Some(artifact.size_bytes);
            record.etag_sha256 = Some(artifact.sha256);
            record.objects_completed = progress.objects_completed.load(Ordering::Relaxed);
            record.source_bytes_completed = progress.source_bytes_completed.load(Ordering::Relaxed);
            record.artifact_bytes_completed = artifact.size_bytes;
            record.phase = ExportPhase::Ready;
            record.updated_at_unix = crate::middleware::auth::now() as i64;
            save(record.clone()).await?;
            Ok::<(), ApiError>(())
        }
        .await;
        if let Err(error) = result {
            record.phase = if ctx.stopping.load(Ordering::Acquire) {
                ExportPhase::Queued
            } else if progress.cancel.load(Ordering::Acquire) {
                ExportPhase::Cancelled
            } else {
                ExportPhase::Failed
            };
            record.error = Some(error.to_string());
            let _ = save(record).await;
        }
        ctx.jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
        let job = id.clone();
        if let Err(error) = ctx
            .db
            .execute(move |db| crate::namespace::Namespace::new(db).unpin_job(&job))
            .await
        {
            tracing::error!(%error,"export source pins retained after cleanup failure");
        }
    });
    let mut tasks = ctx.tasks.lock().unwrap_or_else(|e| e.into_inner());
    tasks.retain(|task| !task.is_finished());
    tasks.push(task);
    Ok(())
}
pub(super) async fn shutdown() {
    let Some(ctx) = CONTEXT.get() else {
        return;
    };
    ctx.stopping.store(true, Ordering::Release);
    for progress in ctx.jobs.lock().unwrap_or_else(|e| e.into_inner()).values() {
        progress.cancel.store(true, Ordering::Release);
    }
    let tasks = std::mem::take(&mut *ctx.tasks.lock().unwrap_or_else(|e| e.into_inner()));
    for task in tasks {
        let _ = task.await;
    }
}
