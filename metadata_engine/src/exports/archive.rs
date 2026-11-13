//! Direct NOA archives share the retained export writer, bounded admission and
//! version pins. Only M1 owns the temporary manifest and crash-cleanup intent.
use super::{
    DownloadArchiveRequest,
    builder::{self, ActiveProgress},
    manifest as capture, store,
};
use crate::{
    Table, fragments,
    middleware::auth,
    namespace::Namespace,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use axum::{
    Json,
    body::Body,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, TryStreamExt};
use serde::Deserialize;
use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};
use tokio::{io::AsyncWriteExt, sync::oneshot};

enum Selection {
    Json(DownloadArchiveRequest),
    Lines(Body),
}

pub(crate) async fn selected(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<DownloadArchiveRequest>,
) -> ApiResult<Response> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    if request.include_internal || !matches!(request.format.as_str(), "tar" | "zip") {
        return Err(ApiError::invalid(
            "archive requires zip or tar and named object data",
        ));
    }
    if request.keys.len() + request.entries.len() + request.prefixes.len() > 10000 {
        return Err(ApiError::invalid(
            "archive selection exceeds 10000 entries; use a TAR manifest",
        ));
    }
    if !request.export_all
        && request.keys.is_empty()
        && request.entries.is_empty()
        && request.prefix.is_none()
        && request.prefixes.is_empty()
    {
        return Err(ApiError::invalid("archive selection is empty"));
    }
    let format = if request.export_all
        || request.prefix.is_some()
        || !request.prefixes.is_empty()
        || request.keys.len() + request.entries.len() > 1000
    {
        "tar"
    } else {
        &request.format
    };
    let name = filename(request.filename.as_deref(), format)?;
    start(
        claims.fragment,
        format.to_owned(),
        name,
        Selection::Json(request),
    )
    .await
}

#[derive(Deserialize)]
pub(crate) struct ManifestQuery {
    format: Option<String>,
    filename: Option<String>,
}
pub(crate) async fn manifest(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(query): Query<ManifestQuery>,
    body: Body,
) -> ApiResult<Response> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    if query.format.as_deref().unwrap_or("tar") != "tar" {
        return Err(ApiError::invalid("manifest archive supports tar only"));
    }
    start(
        claims.fragment,
        "tar".into(),
        filename(query.filename.as_deref(), "tar")?,
        Selection::Lines(body),
    )
    .await
}

fn filename(name: Option<&str>, format: &str) -> ApiResult<String> {
    let name = name.unwrap_or("export");
    if name.is_empty()
        || name.len() > 250
        || name.contains(['/', '\\'])
        || name.chars().any(char::is_control)
    {
        return Err(ApiError::invalid("invalid archive filename"));
    }
    Ok(if name.ends_with(&format!(".{format}")) {
        name.to_owned()
    } else {
        format!("{name}.{format}")
    })
}

struct CancelOnDrop(Arc<ActiveProgress>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel.store(true, Ordering::Release);
    }
}

fn input_path(id: &str) -> PathBuf {
    store::context()
        .root
        .join("manifests")
        .join(format!("{id}.input"))
}

async fn start(
    fragment: String,
    format: String,
    filename: String,
    selection: Selection,
) -> ApiResult<Response> {
    let ctx = store::context();
    if ctx.stopping.load(Ordering::Acquire) {
        return Err(ApiError::invalid("archive service is shutting down"));
    }
    let queued = ctx.queue.clone().try_acquire_owned().map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "Export queue is full".into(),
        )
    })?;
    let id = ulid::Ulid::new().to_string();
    let intent = fragments::key(format!("archive/{id}"))?;
    let progress = Arc::new(ActiveProgress::new());
    let guard = CancelOnDrop(progress.clone());
    ctx.jobs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(id.clone(), progress.clone());
    let (ready_tx, ready_rx) = oneshot::channel();
    let (completion_tx, completion_rx) = oneshot::channel();
    let (reader, writer) = tokio::io::duplex(256 * 1024);
    let worker_format = format.clone();
    let task = tokio::spawn(async move {
        let _queued = queued;
        let mut ready = Some(ready_tx);
        // Own persistence before the handler's first await. A disconnect while
        // the intent commits must still have a worker that performs cleanup.
        let result = async {
            ctx.db
                .execute(move |db| db.transact(64, |tx| tx.put(Table::Reclamation, &intent, b"1")))
                .await?;
            build(
                &id,
                &fragment,
                &worker_format,
                selection,
                progress,
                &mut ready,
                writer,
            )
            .await
        }
        .await;
        let cleaned = cleanup(&id).await;
        if let Err(error) = &cleaned {
            tracing::error!(%id, %error, "archive cleanup intent retained");
        }
        let result = result.and(cleaned);
        ctx.jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
        // Terminal responses also wait for cleanup. In particular, a rejected
        // manifest must not leave a source pin behind its completed HTTP error.
        if let Err(error) = result {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Err(error));
                let _ = completion_tx.send(Ok(()));
            } else {
                let _ = completion_tx.send(Err(std::io::Error::other(error.to_string())));
            }
        } else {
            let _ = completion_tx.send(Ok(()));
        }
    });
    {
        let mut tasks = ctx.tasks.lock().unwrap_or_else(|e| e.into_inner());
        tasks.retain(|t| !t.is_finished());
        tasks.push(task);
    }
    let content_length = ready_rx
        .await
        .map_err(|_| ApiError::invalid("archive worker stopped"))??;
    let stream = super::archive_stream::completed_stream(reader, completion_rx).map(move |chunk| {
        let _keep = &guard;
        chunk
    });
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        "content-length",
        content_length
            .to_string()
            .parse()
            .expect("u64 content length"),
    );
    response.headers_mut().insert(
        "content-type",
        if format == "tar" {
            "application/x-tar"
        } else {
            "application/zip"
        }
        .parse()
        .expect("static type"),
    );
    let name = percent_encoding::utf8_percent_encode(&filename, percent_encoding::NON_ALPHANUMERIC);
    response.headers_mut().insert(
        "content-disposition",
        format!("attachment; filename*=UTF-8''{name}")
            .parse()
            .map_err(|_| ApiError::invalid("invalid archive filename"))?,
    );
    Ok(response)
}

async fn spool(
    id: &str,
    body: Body,
    cancelled: &std::sync::atomic::AtomicBool,
) -> ApiResult<PathBuf> {
    let path = input_path(id);
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .await?;
    let mut chunks = body.into_data_stream();
    let mut line_bytes = 0;
    while let Some(chunk) =
        tokio::time::timeout(std::time::Duration::from_secs(30), chunks.try_next())
            .await
            .map_err(|_| {
                ApiError(
                    StatusCode::REQUEST_TIMEOUT,
                    "Archive manifest body stalled".into(),
                )
            })?
            .map_err(|_| ApiError::invalid("archive manifest body ended early"))?
    {
        if cancelled.load(Ordering::Acquire) {
            return Err(ApiError::invalid("archive cancelled"));
        }
        // Reject oversized lines while receiving rather than filling the spool
        // before discovering that the first line cannot be a valid selection.
        for byte in &chunk {
            line_bytes += 1;
            if line_bytes > 8192 {
                return Err(ApiError::invalid(
                    "archive manifest line exceeds 8192 bytes",
                ));
            }
            if *byte == b'\n' {
                line_bytes = 0;
            }
        }
        output.write_all(&chunk).await?;
    }
    output.flush().await?;
    Ok(path)
}

async fn build(
    id: &str,
    fragment: &str,
    format: &str,
    selection: Selection,
    progress: Arc<ActiveProgress>,
    ready: &mut Option<oneshot::Sender<ApiResult<u64>>>,
    writer: tokio::io::DuplexStream,
) -> ApiResult<()> {
    let ctx = store::context();
    let _slot = ctx
        .slots
        .acquire()
        .await
        .map_err(|_| ApiError::invalid("export admission closed"))?;
    if progress.cancel.load(Ordering::Acquire) {
        return Err(ApiError::invalid("archive cancelled"));
    }
    let path = store::manifest_path(id)?;
    let identity = fragment.to_owned();
    let cancelled = progress.cancel.clone();
    let summary = match selection {
        Selection::Json(request) => {
            ctx.db
                .execute(move |db| {
                    capture::capture_with_cancel(&db, &identity, &request, &path, Some(&cancelled))
                })
                .await?
        }
        Selection::Lines(body) => {
            let input = spool(id, body, &cancelled).await?;
            ctx.db
                .execute(move |db| {
                    capture::capture_lines(&db, &identity, &input, &path, &cancelled)
                })
                .await?
        }
    };
    if ready
        .take()
        .expect("single ready response")
        .send(Ok(summary.archive_bytes(format)))
        .is_err()
    {
        return Err(ApiError::invalid("archive client disconnected"));
    }
    tokio::select! {
        result = builder::write_stream(id, fragment, format, writer, progress.clone()) => result?,
        _ = async {
            while !progress.cancel.load(Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        } => return Err(ApiError::invalid("archive cancelled")),
    }
    Ok(())
}

async fn cleanup(id: &str) -> ApiResult<()> {
    store::validate_id(id)?;
    let path = store::manifest_path(id)?;
    for file in [&path, &path.with_extension("partial"), &input_path(id)] {
        match tokio::fs::remove_file(file).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    let directory = path.parent().expect("manifest directory").to_owned();
    tokio::task::spawn_blocking(move || std::fs::File::open(directory)?.sync_all())
        .await
        .map_err(|_| ApiError::invalid("archive cleanup task failed"))??;
    let job = id.to_owned();
    store::context()
        .db
        .execute(move |db| {
            Namespace::new(db.clone()).unpin_job(&job)?;
            let key = fragments::key(format!("archive/{job}"))?;
            db.transact(64, |tx| tx.remove(Table::Reclamation, &key))
        })
        .await?;
    Ok(())
}

pub(super) async fn recover() -> ApiResult<()> {
    loop {
        let rows = store::context()
            .db
            .execute(|db| {
                db.snapshot().scan(
                    Table::Reclamation,
                    fragments::CATALOG,
                    "archive/",
                    None,
                    128,
                )
            })
            .await?;
        if rows.is_empty() {
            return Ok(());
        }
        for (key, _) in rows {
            cleanup(
                key.identifier()
                    .strip_prefix("archive/")
                    .expect("scoped prefix"),
            )
            .await?;
        }
    }
}
