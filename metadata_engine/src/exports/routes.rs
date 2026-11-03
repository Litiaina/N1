use super::{DownloadArchiveRequest, builder::ActiveProgress, models::*, store};
use crate::{
    middleware::auth,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
fn check_record(record: &PersistedExport, claims: &auth::Claims) -> ApiResult<()> {
    if record.fragment != claims.fragment {
        return Err(ApiError::forbidden());
    }
    if record.expires_at_unix <= auth::now() as i64 {
        return Err(ApiError(StatusCode::GONE, "Export has expired".into()));
    }
    Ok(())
}
pub(crate) async fn create(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(mut request): Json<DownloadArchiveRequest>,
) -> ApiResult<(StatusCode, Json<CreateExportResponse>)> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    if request.include_internal {
        return Err(ApiError::invalid(
            "internal object data cannot be exported through named NOA",
        ));
    }
    if !["zip", "tar"].contains(&request.format.as_str()) {
        return Err(ApiError::invalid("export format must be zip or tar"));
    }
    if request.keys.len() + request.entries.len() + request.prefixes.len() > 4096 {
        return Err(ApiError::invalid("export selection exceeds 4096 entries"));
    }
    if !request.export_all
        && request.keys.is_empty()
        && request.entries.is_empty()
        && request.prefixes.is_empty()
        && request.prefix.is_none()
    {
        return Err(ApiError::invalid("export selection is empty"));
    }
    let id = ulid::Ulid::new().to_string();
    let filename = request
        .filename
        .take()
        .unwrap_or_else(|| format!("archive.{}", request.format));
    if filename.len() > 255 || filename.contains(['\r', '\n', '\0', '/', '\\']) {
        return Err(ApiError::invalid("invalid archive filename"));
    }
    request.filename = Some(filename.clone());
    let now = auth::now() as i64;
    let record = PersistedExport {
        id: id.clone(),
        fragment: claims.fragment,
        request: request.clone(),
        phase: ExportPhase::Queued,
        format: request.format,
        filename,
        created_at_unix: now,
        updated_at_unix: now,
        expires_at_unix: now + store::export_config().retention_seconds as i64,
        manifest_ready: false,
        objects_total: 0,
        objects_completed: 0,
        source_bytes_total: 0,
        source_bytes_completed: 0,
        artifact_bytes_completed: 0,
        reserved_bytes: 0,
        artifact_size_bytes: None,
        etag_sha256: None,
        chunks: Vec::new(),
        error: None,
    };
    store::save(record.clone()).await?;
    if let Err(error) = store::enqueue(record.clone()).await {
        let mut failed = record;
        failed.phase = ExportPhase::Failed;
        failed.error = Some(error.to_string());
        store::save(failed).await?;
        return Err(error);
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(CreateExportResponse {
            export_id: id.clone(),
            phase: ExportPhase::Queued,
            status_url: format!("/noa/v1/exports/{id}"),
            download_url: format!("/noa/v1/exports/{id}/download"),
            expires_at_unix: record.expires_at_unix,
        }),
    ))
}
pub(crate) async fn status(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<ExportStatusResponse>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let mut record = store::load(&id).await?;
    check_record(&record, &claims)?;
    if let Some(progress) = store::context()
        .jobs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&id)
    {
        update_progress(&mut record, progress);
    }
    Ok(Json(ExportStatusResponse::from_state(&record)))
}
fn update_progress(record: &mut PersistedExport, progress: &ActiveProgress) {
    record.objects_completed = progress.objects_completed.load(Ordering::Relaxed);
    record.source_bytes_completed = progress.source_bytes_completed.load(Ordering::Relaxed);
    record.artifact_bytes_completed = progress.artifact_bytes_completed.load(Ordering::Relaxed);
}
pub(crate) async fn cancel(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let mut record = store::load(&id).await?;
    check_record(&record, &claims)?;
    if let Some(progress) = store::context()
        .jobs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&id)
    {
        progress.cancel.store(true, Ordering::Release);
    }
    record.phase = ExportPhase::Cancelled;
    store::save(record).await?;
    Ok(Json(json!({"export_id":id,"phase":"cancelled"})))
}
pub(crate) async fn download(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let record = store::load(&id).await?;
    check_record(&record, &claims)?;
    stream(record, headers).await
}
async fn stream(record: PersistedExport, headers: HeaderMap) -> ApiResult<Response> {
    if record.phase != ExportPhase::Ready {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "Export artifact is not ready".into(),
        ));
    }
    let total = record
        .artifact_size_bytes
        .ok_or_else(|| ApiError::invalid("export artifact length is missing"))?;
    let etag = format!("\"{}\"", record.etag_sha256.as_deref().unwrap_or(""));
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        == Some(etag.as_str())
    {
        return Ok(StatusCode::NOT_MODIFIED.into_response());
    }
    let mut partial = false;
    let (offset, length) = if let Some(range) = headers.get(header::RANGE) {
        partial = true;
        crate::server::routes::read::parse_range(
            range
                .to_str()
                .map_err(|_| ApiError::invalid("invalid range"))?,
            total,
        )?
    } else {
        (0, total)
    };
    let permit = store::context()
        .downloads
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::invalid("export downloads closed"))?;
    let chunks = record.chunks;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);
    tokio::spawn(async move {
        let _permit = permit;
        let result = async {
            for chunk in chunks {
                let start = offset.max(chunk.logical_offset);
                let end = (offset + length).min(chunk.logical_offset + chunk.size_bytes);
                if start >= end {
                    continue;
                }
                let mut file = tokio::fs::File::open(chunk.path).await?;
                let mut buffer = vec![0; 64 * 1024];
                let mut hash = Sha256::new();
                let mut count = 0;
                loop {
                    let n = file.read(&mut buffer).await?;
                    if n == 0 {
                        break;
                    }
                    hash.update(&buffer[..n]);
                    count += n as u64;
                }
                if count != chunk.size_bytes || hex::encode(hash.finalize()) != chunk.sha256 {
                    return Err(std::io::Error::other(
                        "export artifact chunk checksum mismatch",
                    ));
                }
                file.seek(std::io::SeekFrom::Start(start - chunk.logical_offset))
                    .await?;
                let mut remaining = end - start;
                while remaining > 0 {
                    let wanted = remaining.min(buffer.len() as u64) as usize;
                    let n = file.read(&mut buffer[..wanted]).await?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "export artifact ended early",
                        ));
                    }
                    remaining -= n as u64;
                    if tx
                        .send(Ok(bytes::Bytes::copy_from_slice(&buffer[..n])))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = tx.send(Err(error)).await;
        }
    });
    let mut response =
        Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)).into_response();
    *response.status_mut() = if partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let h = response.headers_mut();
    h.insert(
        header::CONTENT_LENGTH,
        length.to_string().parse().expect("length"),
    );
    h.insert(
        header::CONTENT_TYPE,
        if record.format == "tar" {
            "application/x-tar"
        } else {
            "application/zip"
        }
        .parse()
        .expect("content type"),
    );
    h.insert(header::ETAG, etag.parse().expect("artifact hash"));
    h.insert(header::ACCEPT_RANGES, "bytes".parse().expect("static"));
    let name =
        percent_encoding::utf8_percent_encode(&record.filename, percent_encoding::NON_ALPHANUMERIC);
    h.insert(
        header::CONTENT_DISPOSITION,
        format!("attachment; filename*=UTF-8''{name}")
            .parse()
            .map_err(|_| ApiError::invalid("invalid archive filename"))?,
    );
    if partial {
        h.insert(
            header::CONTENT_RANGE,
            format!("bytes {offset}-{}/{total}", offset + length - 1)
                .parse()
                .expect("range"),
        );
    }
    Ok(response)
}
#[derive(Clone, Serialize, Deserialize)]
struct Grant {
    purpose: String,
    fragment: String,
    epoch: u64,
    export_id: String,
    exp: u64,
}
pub(crate) async fn signed_url(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let record = store::load(&id).await?;
    check_record(&record, &claims)?;
    if record.phase != ExportPhase::Ready {
        return Err(ApiError::invalid("export is not ready"));
    }
    let ttl = body["expires_in"].as_u64().unwrap_or(300).clamp(1, 3600);
    let grant = Grant {
        purpose: "n1.named.export".into(),
        fragment: claims.fragment,
        epoch: claims.epoch,
        export_id: id,
        exp: (auth::now() + ttl).min(record.expires_at_unix as u64),
    };
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &grant,
        &jsonwebtoken::EncodingKey::from_secret(app.config.jwt_secret.as_bytes()),
    )
    .map_err(|_| ApiError::invalid("cannot sign export"))?;
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let path = format!("/noa/v1/signed-exports/download?token={token}");
    let url = if host.is_empty() {
        path
    } else {
        format!(
            "{}://{host}{path}",
            if app.config.cert_path.is_some() {
                "https"
            } else {
                "http"
            }
        )
    };
    Ok(Json(
        json!({"url":url,"signed_url":url,"expires_at":grant.exp}),
    ))
}
pub(crate) async fn signed_download(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let token = q.get("token").ok_or_else(ApiError::unauthorized)?;
    let mut validation = jsonwebtoken::Validation::default();
    validation.leeway = 0;
    let grant = jsonwebtoken::decode::<Grant>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(app.config.jwt_secret.as_bytes()),
        &validation,
    )
    .map_err(|_| ApiError::unauthorized())?
    .claims;
    if grant.purpose != "n1.named.export" {
        return Err(ApiError::unauthorized());
    }
    let identity = grant.fragment.clone();
    let fragment = app
        .db
        .execute(move |db| crate::fragments::resolve(&db, &identity))
        .await?;
    if fragment.epoch != grant.epoch {
        return Err(ApiError::unauthorized());
    }
    let record = store::load(&grant.export_id).await?;
    if record.fragment != grant.fragment || record.expires_at_unix <= auth::now() as i64 {
        return Err(ApiError::unauthorized());
    }
    stream(record, headers).await
}
