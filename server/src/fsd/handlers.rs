use crate::enums::app_error_enum::AppError;
use crate::fsd::backend::{DirEntry, InProcessBackend};
use crate::fsd::config::FsdConfig;
use crate::fsd::gateway::FsdGateway;
use crate::fsd::model::{NodeKind, WriteHandle};
use crate::structs::auth_structs::Claims;
use axum::body::{to_bytes, Body};
use axum::extract::{Path as AxumPath, Query};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json as AxumJson;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

static FSD_GATEWAY: OnceLock<Arc<FsdGateway<InProcessBackend>>> = OnceLock::new();
static WRITE_HANDLES: OnceLock<Arc<Mutex<HashMap<String, WriteHandle>>>> = OnceLock::new();
const FSD_DEFAULT_READ_LENGTH_BYTES: usize = 256 * 1024;
const FSD_MAX_READ_LENGTH_BYTES: usize = 1024 * 1024;
const FSD_MAX_WRITE_CHUNK_BYTES: usize = 64 * 1024 * 1024;

fn fsd_gateway() -> Arc<FsdGateway<InProcessBackend>> {
    FSD_GATEWAY
        .get_or_init(|| {
            let config = FsdConfig::default();
            let part_size = if config.enabled {
                config.upload_part_size_bytes
            } else {
                4 * 1024 * 1024
            };
            let backend = Arc::new(InProcessBackend::new(
                config.staging_root.clone(),
                part_size,
            ));
            Arc::new(FsdGateway::new(backend))
        })
        .clone()
}

fn write_handles() -> Arc<Mutex<HashMap<String, WriteHandle>>> {
    WRITE_HANDLES
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

fn ensure_claims_for_path(
    claims: &Claims,
    mount_path: &str,
    permission: &str,
) -> Result<(), AppError> {
    let path = crate::fsd::model::FsdPath::parse(mount_path)?;

    if claims.fragment != path.fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    match permission {
        "read" if !claims.read => Err(AppError::Forbidden("Read permission required".to_string())),
        "write" if !claims.write => {
            Err(AppError::Forbidden("Write permission required".to_string()))
        }
        "modify" if !claims.modify => Err(AppError::Forbidden(
            "Modify permission required".to_string(),
        )),
        "delete" if !claims.delete => Err(AppError::Forbidden(
            "Delete permission required".to_string(),
        )),
        _ => Ok(()),
    }
}

fn ensure_claims_for_handle(claims: &Claims, handle: &WriteHandle) -> Result<(), AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".to_string()));
    }
    if claims.fragment != handle.path.fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    Ok(())
}

#[derive(Serialize)]
pub struct DirEntryResponse {
    pub name: String,
    pub path: String,
    pub kind: String,
    pub size_bytes: u64,
    pub modified_at_unix: i64,
}

impl From<DirEntry> for DirEntryResponse {
    fn from(value: DirEntry) -> Self {
        let kind = match value.metadata.kind {
            NodeKind::File => "file",
            NodeKind::Directory => "directory",
        }
        .to_string();

        Self {
            name: value.name,
            path: value.path.mount_relative(),
            kind,
            size_bytes: value.metadata.size_bytes,
            modified_at_unix: value.metadata.modified_at_unix,
        }
    }
}

#[derive(Serialize)]
pub struct StatResponse {
    pub path: String,
    pub kind: String,
    pub size_bytes: u64,
    pub modified_at_unix: i64,
}

#[derive(Deserialize)]
pub struct ReadQuery {
    pub offset: Option<u64>,
    pub length: Option<usize>,
}

#[derive(Deserialize)]
pub struct BeginWriteRequest {
    pub expected_size_bytes: Option<u64>,
}

#[derive(Serialize)]
pub struct BeginWriteResponse {
    pub session_id: String,
    pub version_id: String,
}

#[derive(Deserialize)]
pub struct WriteChunkQuery {
    pub offset: u64,
}

#[derive(Deserialize)]
pub struct RenameRequest {
    pub from: String,
    pub to: String,
}

pub async fn fsd_list_dir(
    claims: Claims,
    AxumPath(mount_path): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    ensure_claims_for_path(&claims, &mount_path, "read")?;

    let entries = fsd_gateway().list_dir(&mount_path).await?;
    let payload: Vec<DirEntryResponse> = entries.into_iter().map(Into::into).collect();
    Ok(AxumJson(payload))
}

pub async fn fsd_stat(
    claims: Claims,
    AxumPath(mount_path): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    ensure_claims_for_path(&claims, &mount_path, "read")?;

    let metadata = fsd_gateway().stat(&mount_path).await?;
    let kind = match metadata.kind {
        NodeKind::File => "file",
        NodeKind::Directory => "directory",
    }
    .to_string();

    Ok(AxumJson(StatResponse {
        path: mount_path,
        kind,
        size_bytes: metadata.size_bytes,
        modified_at_unix: metadata.modified_at_unix,
    }))
}

pub async fn fsd_read(
    claims: Claims,
    AxumPath(mount_path): AxumPath<String>,
    Query(query): Query<ReadQuery>,
) -> Result<impl IntoResponse, AppError> {
    ensure_claims_for_path(&claims, &mount_path, "read")?;

    let offset = query.offset.unwrap_or(0);
    let length = query
        .length
        .unwrap_or(FSD_DEFAULT_READ_LENGTH_BYTES)
        .clamp(1, FSD_MAX_READ_LENGTH_BYTES);
    let bytes = fsd_gateway()
        .read_range(&mount_path, offset, length)
        .await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        "content-length",
        HeaderValue::from_str(&bytes.len().to_string()).map_err(|e| {
            AppError::InternalServerError(format!("invalid fsd content-length header: {}", e))
        })?,
    );

    Ok((StatusCode::OK, headers, bytes))
}

pub async fn fsd_write_begin(
    claims: Claims,
    AxumPath(mount_path): AxumPath<String>,
    AxumJson(body): AxumJson<BeginWriteRequest>,
) -> Result<impl IntoResponse, AppError> {
    ensure_claims_for_path(&claims, &mount_path, "write")?;

    let handle = fsd_gateway()
        .begin_write(&mount_path, body.expected_size_bytes)
        .await?;

    write_handles()
        .lock()
        .await
        .insert(handle.session_id.clone(), handle.clone());

    Ok((
        StatusCode::CREATED,
        AxumJson(BeginWriteResponse {
            session_id: handle.session_id,
            version_id: handle.version_id,
        }),
    ))
}

pub async fn fsd_write_chunk(
    claims: Claims,
    AxumPath(session_id): AxumPath<String>,
    Query(query): Query<WriteChunkQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    let handles = write_handles();
    let handle = {
        let guard = handles.lock().await;
        guard.get(&session_id).cloned().ok_or(AppError::NotFound)?
    };

    ensure_claims_for_handle(&claims, &handle)?;

    let chunk = to_bytes(body, FSD_MAX_WRITE_CHUNK_BYTES)
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("fsd failed to read chunk body: {}", e))
        })?
        .to_vec();

    fsd_gateway()
        .write_chunk(&handle, query.offset, chunk)
        .await?;

    Ok(StatusCode::OK)
}

pub async fn fsd_write_commit(
    claims: Claims,
    AxumPath(session_id): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    let handles = write_handles();
    let handle = {
        let guard = handles.lock().await;
        guard.get(&session_id).cloned().ok_or(AppError::NotFound)?
    };

    ensure_claims_for_handle(&claims, &handle)?;

    fsd_gateway().commit_write(&handle).await?;
    handles.lock().await.remove(&session_id);

    Ok(StatusCode::OK)
}

pub async fn fsd_write_abort(
    claims: Claims,
    AxumPath(session_id): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    let handles = write_handles();
    let handle = {
        let guard = handles.lock().await;
        guard.get(&session_id).cloned().ok_or(AppError::NotFound)?
    };

    ensure_claims_for_handle(&claims, &handle)?;

    fsd_gateway().abort_write(&handle).await?;
    handles.lock().await.remove(&session_id);

    Ok(StatusCode::OK)
}

pub async fn fsd_delete(
    claims: Claims,
    AxumPath(mount_path): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    ensure_claims_for_path(&claims, &mount_path, "delete")?;

    fsd_gateway().delete(&mount_path).await?;
    Ok(StatusCode::OK)
}

pub async fn fsd_mkdir(
    claims: Claims,
    AxumPath(mount_path): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    ensure_claims_for_path(&claims, &mount_path, "write")?;

    fsd_gateway().mkdir(&mount_path).await?;
    Ok(StatusCode::CREATED)
}

pub async fn fsd_rmdir(
    claims: Claims,
    AxumPath(mount_path): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    ensure_claims_for_path(&claims, &mount_path, "delete")?;

    fsd_gateway().rmdir(&mount_path).await?;
    Ok(StatusCode::OK)
}

pub async fn fsd_rename(
    claims: Claims,
    AxumJson(body): AxumJson<RenameRequest>,
) -> Result<impl IntoResponse, AppError> {
    ensure_claims_for_path(&claims, &body.from, "modify")?;
    ensure_claims_for_path(&claims, &body.to, "modify")?;

    fsd_gateway().rename(&body.from, &body.to).await?;
    Ok(StatusCode::OK)
}
