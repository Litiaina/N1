use crate::enums::app_error_enum::AppError;
use crate::fuse::model::{FsdPath, NodeKind, NodeMetadata, WriteHandle, encode_virtual_name};
use crate::n1_core::delete::{
    delete_object, delete_object_permanently, empty_trash, recover_object,
};
use crate::n1_core::disk_manager::{
    ChunkQuery, InitiateUploadParams, ListObjectsResult, ObjectKeyQuery, ObjectMetadata,
    RenameObjectRequest, UploadPartQuery,
};
use crate::n1_core::download::download_object_chunk_direct;
use crate::n1_core::index;
use crate::n1_core::modify::rename_object;
use crate::n1_core::retrieve::{get_object_metadata_direct, list_objects_direct};
use crate::n1_core::upload::{
    cancel_upload_session, finalize_object_upload, initiate_object_upload, object_upload_part,
};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use axum::Json as AxumJson;
use axum::body::{Body, to_bytes};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub path: FsdPath,
    pub metadata: NodeMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirListPage {
    pub entries: Vec<DirEntry>,
    pub continuation_token: Option<String>,
}

pub trait FsdBackend: Send + Sync + 'static {
    fn list_dir_page<'a>(
        &'a self,
        path: &'a FsdPath,
        continuation_token: Option<String>,
        page_limit: usize,
    ) -> BoxFuture<'a, Result<DirListPage, AppError>>;
    fn list_dir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<Vec<DirEntry>, AppError>>;
    fn stat<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<NodeMetadata, AppError>>;
    fn read_range<'a>(
        &'a self,
        path: &'a FsdPath,
        offset: u64,
        length: usize,
    ) -> BoxFuture<'a, Result<Vec<u8>, AppError>>;
    fn begin_write<'a>(
        &'a self,
        path: &'a FsdPath,
        expected_size_bytes: Option<u64>,
    ) -> BoxFuture<'a, Result<WriteHandle, AppError>>;
    fn write_chunk<'a>(
        &'a self,
        handle: &'a WriteHandle,
        offset: u64,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AppError>>;
    fn commit_write<'a>(&'a self, handle: &'a WriteHandle) -> BoxFuture<'a, Result<(), AppError>>;
    fn abort_write<'a>(&'a self, handle: &'a WriteHandle) -> BoxFuture<'a, Result<(), AppError>>;
    fn delete<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>>;
    fn mkdir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>>;
    fn rmdir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>>;
    fn rename<'a>(
        &'a self,
        from: &'a FsdPath,
        to: &'a FsdPath,
    ) -> BoxFuture<'a, Result<(), AppError>>;
}

#[derive(Debug, Clone)]
struct StagedPendingWrite {
    path: FsdPath,
    staging_path: PathBuf,
    expected_size_bytes: Option<u64>,
    max_written_end: u64,
}

#[derive(Debug, Clone)]
struct DirectPendingWrite {
    path: FsdPath,
    expected_size_bytes: u64,
    expected_parts: u64,
    upload_session_id: String,
    upload_version_id: String,
    part_size_bytes: u64,
    next_offset: u64,
    next_part_index: u64,
    current_part: Vec<u8>,
    uploaded_parts: HashSet<u64>,
}

#[derive(Debug, Clone)]
enum PendingWriteState {
    Staged(StagedPendingWrite),
    Direct(DirectPendingWrite),
}

#[derive(Debug, Deserialize)]
struct InitiateUploadResponseBody {
    session_id: String,
    version_id: String,
}

#[derive(Clone)]
pub struct InProcessBackend {
    app_state: Arc<AppState>,
    staging_root: PathBuf,
    upload_part_size_bytes: usize,
    pending_writes: Arc<Mutex<HashMap<String, PendingWriteState>>>,
}

const FSD_CONTROL_BODY_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const FSD_MAX_READ_BYTES: usize = 1024 * 1024;
const FSD_LIST_PAGE_LIMIT_DEFAULT: usize = 1000;
const FSD_LIST_PAGE_LIMIT_MAX: usize = 1000;
const FSD_TRASH_DIR_NAME: &str = ".n1-trash";

impl InProcessBackend {
    pub fn new(staging_root: impl Into<PathBuf>, upload_part_size_bytes: usize) -> Self {
        let part_size = upload_part_size_bytes.clamp(64 * 1024, 50 * 1024 * 1024);
        Self {
            app_state: Arc::new(AppState {
                ws_manager: WsConnectionManager::new(),
            }),
            staging_root: staging_root.into(),
            upload_part_size_bytes: part_size,
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn claims_for(fragment: &str) -> Claims {
        Claims {
            fragment: fragment.to_string(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        }
    }

    async fn ensure_staging_root(&self) -> Result<(), AppError> {
        fs::create_dir_all(&self.staging_root)
            .await
            .map_err(|e| AppError::Io(e, "create fsd staging root".to_string()))
    }

    async fn response_bytes(
        response: Response,
        max_len: usize,
    ) -> Result<(StatusCode, Vec<u8>), AppError> {
        let status = response.status();
        let body = to_bytes(response.into_body(), max_len)
            .await
            .map_err(|e| {
                AppError::InternalServerError(format!("fsd failed to read response body: {}", e))
            })?
            .to_vec();
        Ok((status, body))
    }

    fn status_to_error(status: StatusCode, body: &[u8]) -> AppError {
        let message = if body.is_empty() {
            format!("fsd backend call failed with status {}", status)
        } else {
            String::from_utf8_lossy(body).to_string()
        };

        match status {
            StatusCode::BAD_REQUEST => AppError::BadRequest(message),
            StatusCode::UNAUTHORIZED => AppError::Unauthorized,
            StatusCode::FORBIDDEN => AppError::Forbidden(message),
            StatusCode::NOT_FOUND => AppError::NotFound,
            StatusCode::CONFLICT => AppError::Conflict(message),
            StatusCode::PAYLOAD_TOO_LARGE => AppError::PayloadTooLarge(message),
            StatusCode::RANGE_NOT_SATISFIABLE => AppError::RangeNotSatisfiable,
            StatusCode::INSUFFICIENT_STORAGE => AppError::InsufficientStorage,
            _ => AppError::InternalServerError(message),
        }
    }

    async fn parse_json_response<T: DeserializeOwned>(
        response: Response,
        accepted: &[StatusCode],
    ) -> Result<T, AppError> {
        let (status, body) = Self::response_bytes(response, FSD_CONTROL_BODY_LIMIT_BYTES).await?;
        if !accepted.contains(&status) {
            return Err(Self::status_to_error(status, &body));
        }

        serde_json::from_slice::<T>(&body).map_err(|e| {
            AppError::InternalServerError(format!("fsd failed to parse response json: {}", e))
        })
    }

    async fn expect_empty_response(
        response: Response,
        accepted: &[StatusCode],
    ) -> Result<(), AppError> {
        let status = response.status();
        if accepted.contains(&status) {
            // Successful control-path responses can legitimately carry large JSON payloads
            // (e.g. finalize upload returns object metadata for very large multipart files).
            // The backend does not need that body, so avoid buffering it under control limits.
            return Ok(());
        }

        let (_status, body) = Self::response_bytes(response, FSD_CONTROL_BODY_LIMIT_BYTES).await?;
        Err(Self::status_to_error(status, &body))
    }

    async fn list_page_with_limit(
        &self,
        fragment: &str,
        continuation_token: Option<String>,
        page_limit: usize,
    ) -> Result<ListObjectsResult, AppError> {
        let limit = page_limit.clamp(1, FSD_LIST_PAGE_LIMIT_MAX);
        let mut params = HashMap::new();
        params.insert("limit".to_string(), limit.to_string());
        params.insert("include_internal".to_string(), "true".to_string());
        if let Some(token) = continuation_token {
            params.insert("continuation_token".to_string(), token);
        }

        list_objects_direct(fragment, params, false).await
    }

    async fn list_trash_page_with_limit(
        &self,
        fragment: &str,
        continuation_token: Option<String>,
        page_limit: usize,
    ) -> Result<ListObjectsResult, AppError> {
        let limit = page_limit.clamp(1, FSD_LIST_PAGE_LIMIT_MAX);
        let mut params = HashMap::new();
        params.insert("limit".to_string(), limit.to_string());
        params.insert("include_internal".to_string(), "true".to_string());
        if let Some(token) = continuation_token {
            params.insert("continuation_token".to_string(), token);
        }

        list_objects_direct(fragment, params, true).await
    }

    async fn fetch_object_metadata(
        &self,
        fragment: &str,
        object_key: &str,
    ) -> Result<ObjectMetadata, AppError> {
        get_object_metadata_direct(fragment, object_key).await
    }

    fn split_trash_virtual_key<'a>(key: &'a str) -> Option<&'a str> {
        if key == FSD_TRASH_DIR_NAME {
            Some("")
        } else {
            key.strip_prefix(FSD_TRASH_DIR_NAME)
                .and_then(|v| v.strip_prefix('/'))
        }
    }

    fn is_trash_root_name(key: &str) -> bool {
        key == FSD_TRASH_DIR_NAME
    }

    fn is_trash_virtual_path(path: &FsdPath) -> bool {
        Self::split_trash_virtual_key(path.object_key.as_str()).is_some()
    }

    fn to_trash_virtual_path(path: &FsdPath, key: &str) -> FsdPath {
        FsdPath {
            fragment: path.fragment.clone(),
            object_key: format!("{}/{}", FSD_TRASH_DIR_NAME, key.trim_matches('/')),
        }
    }

    async fn trash_exact_object_metadata(
        &self,
        fragment: &str,
        object_key: &str,
    ) -> Result<Option<ObjectMetadata>, AppError> {
        let exact = object_key.trim_matches('/').to_string();
        if exact.is_empty() {
            return Ok(None);
        }
        match index::read_latest_trashed_object(fragment, &exact).await {
            Ok(meta) => Ok(Some(meta)),
            Err(AppError::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn trash_has_entries(&self, fragment: &str) -> Result<bool, AppError> {
        let page = self.list_trash_page_with_limit(fragment, None, 1).await?;
        Ok(!page.objects.is_empty())
    }

    async fn cleanup_remote_upload_session(
        &self,
        fragment: &str,
        version_id: &str,
        object_key: &str,
    ) {
        let _ = cancel_upload_session(
            Self::claims_for(fragment),
            AxumPath((fragment.to_string(), version_id.to_string())),
            Query(ObjectKeyQuery {
                object_key: object_key.to_string(),
                version_id: None,
            }),
        )
        .await;
    }

    async fn start_remote_upload(
        &self,
        path: &FsdPath,
        total_size: u64,
    ) -> Result<(InitiateUploadResponseBody, u64), AppError> {
        let part_size = self.upload_part_size_bytes as u64;
        let expected_parts = if total_size == 0 {
            1
        } else {
            ((total_size + part_size - 1) / part_size).max(1)
        };

        let init_response = initiate_object_upload(
            Self::claims_for(&path.fragment),
            AxumPath((path.fragment.clone(), path.object_key.clone())),
            AxumJson(InitiateUploadParams {
                content_type: Some("application/octet-stream".to_string()),
                total_size_bytes: total_size,
                expected_parts,
                part_size_bytes: Some(part_size),
            }),
        )
        .await?
        .into_response();

        let init_body = Self::parse_json_response::<InitiateUploadResponseBody>(
            init_response,
            &[StatusCode::CREATED],
        )
        .await?;

        Ok((init_body, expected_parts))
    }

    async fn upload_remote_part(
        &self,
        fragment: &str,
        version_id: &str,
        part_index: u64,
        chunk: Vec<u8>,
    ) -> Result<(), AppError> {
        let chunk_len = chunk.len();
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-length",
            HeaderValue::from_str(&chunk_len.to_string()).map_err(|e| {
                AppError::InternalServerError(format!("fsd invalid content-length header: {}", e))
            })?,
        );

        let part_response = object_upload_part(
            Self::claims_for(fragment),
            headers,
            AxumPath((fragment.to_string(), version_id.to_string(), part_index)),
            Query(UploadPartQuery {
                checksum: None,
                durability: None,
                strict_durability: None,
                io_gate: None,
            }),
            Body::from(chunk),
        )
        .await?
        .into_response();

        Self::expect_empty_response(part_response, &[StatusCode::OK]).await
    }

    async fn finalize_remote_upload(
        &self,
        path: &FsdPath,
        version_id: &str,
        session_id: &str,
        total_size: u64,
        expected_parts: u64,
    ) -> Result<(), AppError> {
        let finalize_response = finalize_object_upload(
            State(Arc::clone(&self.app_state)),
            Self::claims_for(&path.fragment),
            AxumPath((
                path.fragment.clone(),
                version_id.to_string(),
                session_id.to_string(),
            )),
            Query(ObjectKeyQuery {
                object_key: path.object_key.clone(),
                version_id: None,
            }),
            AxumJson(InitiateUploadParams {
                content_type: Some("application/octet-stream".to_string()),
                total_size_bytes: total_size,
                expected_parts,
                part_size_bytes: Some(self.upload_part_size_bytes as u64),
            }),
        )
        .await?
        .into_response();

        Self::expect_empty_response(finalize_response, &[StatusCode::OK]).await
    }

    async fn commit_direct_write(&self, pending: &mut DirectPendingWrite) -> Result<(), AppError> {
        if pending.next_offset != pending.expected_size_bytes {
            return Err(AppError::BadRequest(
                "write stream ended before expected size".to_string(),
            ));
        }

        if pending.expected_size_bytes == 0 && pending.uploaded_parts.is_empty() {
            self.upload_remote_part(
                pending.path.fragment.as_str(),
                pending.upload_version_id.as_str(),
                0,
                Vec::new(),
            )
            .await?;
            pending.uploaded_parts.insert(0);
            pending.next_part_index = 1;
        } else if !pending.current_part.is_empty() {
            let part_bytes = mem::take(&mut pending.current_part);
            let part_index = pending.next_part_index;
            self.upload_remote_part(
                pending.path.fragment.as_str(),
                pending.upload_version_id.as_str(),
                part_index,
                part_bytes,
            )
            .await?;
            pending.uploaded_parts.insert(part_index);
            pending.next_part_index = pending.next_part_index.saturating_add(1);
        }

        if pending.uploaded_parts.len() as u64 != pending.expected_parts {
            return Err(AppError::BadRequest(
                "incomplete multipart upload parts at close".to_string(),
            ));
        }

        self.finalize_remote_upload(
            &pending.path,
            pending.upload_version_id.as_str(),
            pending.upload_session_id.as_str(),
            pending.expected_size_bytes,
            pending.expected_parts,
        )
        .await
    }

    async fn commit_staged_write(&self, pending: &StagedPendingWrite) -> Result<(), AppError> {
        let meta = fs::metadata(&pending.staging_path)
            .await
            .map_err(|e| AppError::Io(e, "stat fsd staging file".to_string()))?;

        let mut total_size = meta.len().max(pending.max_written_end);
        if let Some(expected) = pending.expected_size_bytes {
            total_size = expected;
        }

        let part_size = self.upload_part_size_bytes as u64;
        let expected_parts = if total_size == 0 {
            1
        } else {
            ((total_size + part_size - 1) / part_size).max(1)
        };

        let fragment = pending.path.fragment.clone();
        let object_key = pending.path.object_key.clone();

        let (init_body, _) = self.start_remote_upload(&pending.path, total_size).await?;

        let mut sent = 0u64;
        let mut part_index = 0u64;

        let mut file = OpenOptions::new()
            .read(true)
            .open(&pending.staging_path)
            .await
            .map_err(|e| AppError::Io(e, "open fsd staging file for commit".to_string()))?;
        file.seek(SeekFrom::Start(0))
            .await
            .map_err(|e| AppError::Io(e, "seek fsd staging file".to_string()))?;

        while part_index < expected_parts {
            let remaining = total_size.saturating_sub(sent);
            let chunk_len = min(part_size, remaining) as usize;
            let mut chunk = vec![0u8; chunk_len];

            if chunk_len > 0 {
                file.read_exact(&mut chunk)
                    .await
                    .map_err(|e| AppError::Io(e, "read fsd staging part".to_string()))?;
            }

            if let Err(err) = self
                .upload_remote_part(
                    fragment.as_str(),
                    init_body.version_id.as_str(),
                    part_index,
                    chunk,
                )
                .await
            {
                self.cleanup_remote_upload_session(&fragment, &init_body.version_id, &object_key)
                    .await;
                return Err(err);
            }

            sent = sent.saturating_add(chunk_len as u64);
            part_index = part_index.saturating_add(1);
        }

        if let Err(err) = self
            .finalize_remote_upload(
                &pending.path,
                init_body.version_id.as_str(),
                init_body.session_id.as_str(),
                total_size,
                expected_parts,
            )
            .await
        {
            self.cleanup_remote_upload_session(&fragment, &init_body.version_id, &object_key)
                .await;
            return Err(err);
        }

        Ok(())
    }

    async fn write_staged_chunk(
        &self,
        pending: &mut StagedPendingWrite,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<(), AppError> {
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| AppError::BadRequest("write offset overflow".to_string()))?;

        if let Some(expected) = pending.expected_size_bytes {
            if end > expected {
                return Err(AppError::BadRequest(
                    "write exceeds expected object size".to_string(),
                ));
            }
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&pending.staging_path)
            .await
            .map_err(|e| AppError::Io(e, "open fsd staging file".to_string()))?;

        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| AppError::Io(e, "seek fsd staging file".to_string()))?;
        file.write_all(&bytes)
            .await
            .map_err(|e| AppError::Io(e, "write fsd staging file".to_string()))?;

        pending.max_written_end = pending.max_written_end.max(end);
        Ok(())
    }

    async fn write_direct_chunk(
        &self,
        pending: &mut DirectPendingWrite,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<(), AppError> {
        if offset != pending.next_offset {
            return Err(AppError::BadRequest(
                "non-sequential SMB write offsets are not supported for direct multipart path"
                    .to_string(),
            ));
        }

        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| AppError::BadRequest("write offset overflow".to_string()))?;
        if end > pending.expected_size_bytes {
            return Err(AppError::BadRequest(
                "write exceeds expected object size".to_string(),
            ));
        }

        pending.current_part.extend_from_slice(&bytes);
        pending.next_offset = end;

        let part_size = pending.part_size_bytes as usize;
        while pending.current_part.len() >= part_size {
            let remainder = pending.current_part.split_off(part_size);
            let part_bytes = mem::replace(&mut pending.current_part, remainder);
            let part_index = pending.next_part_index;

            self.upload_remote_part(
                pending.path.fragment.as_str(),
                pending.upload_version_id.as_str(),
                part_index,
                part_bytes,
            )
            .await?;

            pending.uploaded_parts.insert(part_index);
            pending.next_part_index = pending.next_part_index.saturating_add(1);
        }

        Ok(())
    }

    async fn commit_pending_write(&self, pending: &mut PendingWriteState) -> Result<(), AppError> {
        match pending {
            PendingWriteState::Staged(p) => self.commit_staged_write(p).await,
            PendingWriteState::Direct(p) => self.commit_direct_write(p).await,
        }
    }

    async fn delete_active_object_with_fallback(
        &self,
        fragment: &str,
        object_key: &str,
    ) -> Result<(), AppError> {
        let soft_delete_result = delete_object(
            State(Arc::clone(&self.app_state)),
            Self::claims_for(fragment),
            AxumPath((fragment.to_string(), object_key.to_string())),
        )
        .await;

        match soft_delete_result {
            Ok(response) => {
                let response = response.into_response();
                let (status, body) =
                    Self::response_bytes(response, FSD_CONTROL_BODY_LIMIT_BYTES).await?;
                if status == StatusCode::OK {
                    Ok(())
                } else if status == StatusCode::CONFLICT {
                    self.delete_object_permanently(fragment, object_key).await
                } else {
                    Err(Self::status_to_error(status, &body))
                }
            }
            Err(AppError::Conflict(_)) => {
                self.delete_object_permanently(fragment, object_key).await
            }
            Err(e) => Err(e),
        }
    }

    async fn rename_live_object(
        &self,
        fragment: &str,
        source_key: &str,
        destination_key: &str,
    ) -> Result<(), AppError> {
        let response = rename_object(
            State(Arc::clone(&self.app_state)),
            Self::claims_for(fragment),
            AxumJson(RenameObjectRequest {
                source_fragment: fragment.to_string(),
                source_key: source_key.to_string(),
                destination_fragment: fragment.to_string(),
                destination_key: destination_key.to_string(),
            }),
        )
        .await?
        .into_response();

        Self::expect_empty_response(response, &[StatusCode::OK]).await
    }

    async fn delete_object_permanently(
        &self,
        fragment: &str,
        object_key: &str,
    ) -> Result<(), AppError> {
        let permanent = delete_object_permanently(
            State(Arc::clone(&self.app_state)),
            Self::claims_for(fragment),
            AxumPath((fragment.to_string(), object_key.to_string())),
        )
        .await?
        .into_response();

        Self::expect_empty_response(permanent, &[StatusCode::OK, StatusCode::NO_CONTENT]).await
    }
}

impl FsdBackend for InProcessBackend {
    fn list_dir_page<'a>(
        &'a self,
        path: &'a FsdPath,
        continuation_token: Option<String>,
        page_limit: usize,
    ) -> BoxFuture<'a, Result<DirListPage, AppError>> {
        Box::pin(async move {
            if let Some(trash_sub_key) = Self::split_trash_virtual_key(path.object_key.as_str()) {
                let trash_key = trash_sub_key.trim_matches('/').to_string();
                if !trash_key.is_empty() {
                    if self
                        .trash_exact_object_metadata(&path.fragment, &trash_key)
                        .await?
                        .is_some()
                    {
                        return Err(AppError::BadRequest(
                            "fsd flat mode does not expose nested directories".to_string(),
                        ));
                    }
                    return Err(AppError::NotFound);
                }

                let page = self
                    .list_trash_page_with_limit(
                        &path.fragment,
                        continuation_token.clone(),
                        page_limit,
                    )
                    .await?;
                let entries = page
                    .objects
                    .into_iter()
                    .map(|object| DirEntry {
                        name: encode_virtual_name(&object.object_key),
                        path: Self::to_trash_virtual_path(path, &object.object_key),
                        metadata: NodeMetadata {
                            kind: NodeKind::File,
                            size_bytes: object.total_size_bytes,
                            modified_at_unix: object.created_at,
                        },
                    })
                    .collect();

                return Ok(DirListPage {
                    entries,
                    continuation_token: page.continuation_token,
                });
            }

            if !path.is_fragment_root() {
                if self
                    .fetch_object_metadata(&path.fragment, &path.object_key)
                    .await
                    .is_ok()
                {
                    return Err(AppError::BadRequest(
                        "fsd flat mode does not expose nested directories".to_string(),
                    ));
                }
                return Err(AppError::NotFound);
            }

            let page = self
                .list_page_with_limit(&path.fragment, continuation_token.clone(), page_limit)
                .await?;
            let mut entries = Vec::new();
            if continuation_token.is_none() && self.trash_has_entries(&path.fragment).await? {
                entries.push(DirEntry {
                    name: FSD_TRASH_DIR_NAME.to_string(),
                    path: FsdPath {
                        fragment: path.fragment.clone(),
                        object_key: FSD_TRASH_DIR_NAME.to_string(),
                    },
                    metadata: NodeMetadata {
                        kind: NodeKind::Directory,
                        size_bytes: 0,
                        modified_at_unix: 0,
                    },
                });
            }
            entries.extend(page.objects.into_iter().map(|object| DirEntry {
                name: encode_virtual_name(&object.object_key),
                path: FsdPath {
                    fragment: path.fragment.clone(),
                    object_key: object.object_key,
                },
                metadata: NodeMetadata {
                    kind: NodeKind::File,
                    size_bytes: object.total_size_bytes,
                    modified_at_unix: object.created_at,
                },
            }));

            Ok(DirListPage {
                entries,
                continuation_token: page.continuation_token,
            })
        })
    }

    fn list_dir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<Vec<DirEntry>, AppError>> {
        Box::pin(async move {
            let mut all_entries = Vec::new();
            let mut continuation_token: Option<String> = None;
            loop {
                let page = self
                    .list_dir_page(
                        path,
                        continuation_token.clone(),
                        FSD_LIST_PAGE_LIMIT_DEFAULT,
                    )
                    .await?;
                all_entries.extend(page.entries);
                continuation_token = page.continuation_token;
                if continuation_token.is_none() {
                    break;
                }
            }
            Ok(all_entries)
        })
    }

    fn stat<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<NodeMetadata, AppError>> {
        Box::pin(async move {
            if path.is_fragment_root() {
                return Ok(NodeMetadata {
                    kind: NodeKind::Directory,
                    size_bytes: 0,
                    modified_at_unix: 0,
                });
            }

            if Self::is_trash_root_name(path.object_key.as_str()) {
                if self.trash_has_entries(&path.fragment).await? {
                    return Ok(NodeMetadata {
                        kind: NodeKind::Directory,
                        size_bytes: 0,
                        modified_at_unix: 0,
                    });
                }
                return Err(AppError::NotFound);
            }

            if let Some(trash_sub_key) = Self::split_trash_virtual_key(path.object_key.as_str()) {
                let key = trash_sub_key.trim_matches('/').to_string();
                if key.is_empty() {
                    return Ok(NodeMetadata {
                        kind: NodeKind::Directory,
                        size_bytes: 0,
                        modified_at_unix: 0,
                    });
                }

                if let Some(meta) = self
                    .trash_exact_object_metadata(&path.fragment, &key)
                    .await?
                {
                    return Ok(NodeMetadata {
                        kind: NodeKind::File,
                        size_bytes: meta.total_size_bytes,
                        modified_at_unix: meta.created_at,
                    });
                }

                return Err(AppError::NotFound);
            }

            match self
                .fetch_object_metadata(&path.fragment, &path.object_key)
                .await
            {
                Ok(meta) => Ok(NodeMetadata {
                    kind: NodeKind::File,
                    size_bytes: meta.total_size_bytes,
                    modified_at_unix: meta.created_at,
                }),
                Err(AppError::NotFound) => Err(AppError::NotFound),
                Err(e) => Err(e),
            }
        })
    }

    fn read_range<'a>(
        &'a self,
        path: &'a FsdPath,
        offset: u64,
        length: usize,
    ) -> BoxFuture<'a, Result<Vec<u8>, AppError>> {
        Box::pin(async move {
            if path.is_fragment_root() {
                return Err(AppError::BadRequest(
                    "cannot read a fragment root path".to_string(),
                ));
            }

            if Self::is_trash_virtual_path(path) {
                return Err(AppError::BadRequest(
                    "cannot read objects directly from trash namespace".to_string(),
                ));
            }

            if length == 0 {
                return Ok(Vec::new());
            }

            let capped_len = length.clamp(1, FSD_MAX_READ_BYTES);

            download_object_chunk_direct(
                &path.fragment,
                &ChunkQuery {
                    object_key: path.object_key.clone(),
                    offset,
                    chunk_size: capped_len,
                },
            )
            .await
        })
    }

    fn begin_write<'a>(
        &'a self,
        path: &'a FsdPath,
        expected_size_bytes: Option<u64>,
    ) -> BoxFuture<'a, Result<WriteHandle, AppError>> {
        Box::pin(async move {
            if path.is_fragment_root() || Self::is_trash_root_name(path.object_key.as_str()) {
                return Err(AppError::BadRequest(
                    "cannot write to a virtual directory path".to_string(),
                ));
            }

            if Self::is_trash_virtual_path(path) {
                return Err(AppError::BadRequest(
                    "cannot write into trash namespace".to_string(),
                ));
            }

            let handle_id = Ulid::new().to_string();
            let pending = if let Some(expected) = expected_size_bytes {
                let (init_body, expected_parts) = self.start_remote_upload(path, expected).await?;
                PendingWriteState::Direct(DirectPendingWrite {
                    path: path.clone(),
                    expected_size_bytes: expected,
                    expected_parts,
                    upload_session_id: init_body.session_id,
                    upload_version_id: init_body.version_id,
                    part_size_bytes: self.upload_part_size_bytes as u64,
                    next_offset: 0,
                    next_part_index: 0,
                    current_part: Vec::new(),
                    uploaded_parts: HashSet::new(),
                })
            } else {
                self.ensure_staging_root().await?;
                let staging_path = self.staging_root.join(format!("{}.fsd.upload", handle_id));
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&staging_path)
                    .await
                    .map_err(|e| AppError::Io(e, "create fsd staging file".to_string()))?;
                drop(file);
                PendingWriteState::Staged(StagedPendingWrite {
                    path: path.clone(),
                    staging_path,
                    expected_size_bytes: None,
                    max_written_end: 0,
                })
            };

            self.pending_writes
                .lock()
                .await
                .insert(handle_id.clone(), pending);

            Ok(WriteHandle {
                session_id: handle_id.clone(),
                version_id: handle_id,
                expected_size_bytes,
                path: path.clone(),
            })
        })
    }

    fn write_chunk<'a>(
        &'a self,
        handle: &'a WriteHandle,
        offset: u64,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            if bytes.is_empty() {
                return Ok(());
            }

            let mut pending = {
                let mut guard = self.pending_writes.lock().await;
                guard.remove(&handle.session_id).ok_or(AppError::NotFound)?
            };

            let write_result = match &mut pending {
                PendingWriteState::Staged(p) => self.write_staged_chunk(p, offset, bytes).await,
                PendingWriteState::Direct(p) => self.write_direct_chunk(p, offset, bytes).await,
            };

            self.pending_writes
                .lock()
                .await
                .insert(handle.session_id.clone(), pending);

            write_result
        })
    }

    fn commit_write<'a>(&'a self, handle: &'a WriteHandle) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            let mut pending = {
                let mut guard = self.pending_writes.lock().await;
                guard.remove(&handle.session_id)
            }
            .ok_or(AppError::NotFound)?;

            match self.commit_pending_write(&mut pending).await {
                Ok(()) => {
                    if let PendingWriteState::Staged(staged) = &pending {
                        let _ = fs::remove_file(&staged.staging_path).await;
                    }
                    Ok(())
                }
                Err(e) => {
                    self.pending_writes
                        .lock()
                        .await
                        .insert(handle.session_id.clone(), pending);
                    Err(e)
                }
            }
        })
    }

    fn abort_write<'a>(&'a self, handle: &'a WriteHandle) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            let pending = {
                let mut guard = self.pending_writes.lock().await;
                guard.remove(&handle.session_id)
            };

            if let Some(state) = pending {
                match state {
                    PendingWriteState::Staged(staged) => {
                        let _ = fs::remove_file(staged.staging_path).await;
                    }
                    PendingWriteState::Direct(direct) => {
                        self.cleanup_remote_upload_session(
                            direct.path.fragment.as_str(),
                            direct.upload_version_id.as_str(),
                            direct.path.object_key.as_str(),
                        )
                        .await;
                    }
                }
            }

            Ok(())
        })
    }

    fn delete<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            if path.is_fragment_root() {
                return Err(AppError::BadRequest(
                    "cannot delete a fragment root path".to_string(),
                ));
            }

            if Self::is_trash_root_name(path.object_key.as_str()) {
                let response = empty_trash(
                    State(Arc::clone(&self.app_state)),
                    Self::claims_for(&path.fragment),
                    AxumPath(path.fragment.clone()),
                )
                .await?
                .into_response();
                return Self::expect_empty_response(
                    response,
                    &[StatusCode::NO_CONTENT, StatusCode::OK],
                )
                .await;
            }

            if let Some(trash_sub_key) = Self::split_trash_virtual_key(path.object_key.as_str()) {
                let object_key = trash_sub_key.trim_matches('/').to_string();
                if object_key.is_empty() {
                    return Err(AppError::BadRequest(
                        "invalid trash object path".to_string(),
                    ));
                }

                let response = delete_object_permanently(
                    State(Arc::clone(&self.app_state)),
                    Self::claims_for(&path.fragment),
                    AxumPath((path.fragment.clone(), object_key)),
                )
                .await?
                .into_response();

                return Self::expect_empty_response(
                    response,
                    &[StatusCode::OK, StatusCode::NO_CONTENT],
                )
                .await;
            }

            self.delete_active_object_with_fallback(&path.fragment, &path.object_key)
                .await
        })
    }

    fn mkdir<'a>(&'a self, _path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            Err(AppError::BadRequest(
                "directories are not supported in fsd flat mode".to_string(),
            ))
        })
    }

    fn rmdir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            if Self::is_trash_root_name(path.object_key.as_str()) {
                let response = empty_trash(
                    State(Arc::clone(&self.app_state)),
                    Self::claims_for(&path.fragment),
                    AxumPath(path.fragment.clone()),
                )
                .await?
                .into_response();
                return Self::expect_empty_response(
                    response,
                    &[StatusCode::NO_CONTENT, StatusCode::OK],
                )
                .await;
            }
            Err(AppError::BadRequest(
                "directories are not supported in fsd flat mode".to_string(),
            ))
        })
    }

    fn rename<'a>(
        &'a self,
        from: &'a FsdPath,
        to: &'a FsdPath,
    ) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            if from.is_fragment_root() || to.is_fragment_root() {
                return Err(AppError::BadRequest(
                    "cannot rename fragment root paths".to_string(),
                ));
            }

            if from.fragment != to.fragment {
                return Err(AppError::BadRequest(
                    "cross-fragment rename is not supported".to_string(),
                ));
            }

            if Self::is_trash_root_name(to.object_key.as_str()) {
                return Err(AppError::BadRequest(
                    "cannot rename an object to trash root".to_string(),
                ));
            }

            let from_trash = Self::split_trash_virtual_key(from.object_key.as_str())
                .map(|k| k.trim_matches('/').to_string());
            let to_trash = Self::split_trash_virtual_key(to.object_key.as_str())
                .map(|k| k.trim_matches('/').to_string());

            match (from_trash, to_trash) {
                (Some(from_key), None) => {
                    if from_key.is_empty() {
                        return Err(AppError::BadRequest("cannot rename trash root".to_string()));
                    }

                    let recover_response = recover_object(
                        State(Arc::clone(&self.app_state)),
                        Self::claims_for(&from.fragment),
                        AxumPath((from.fragment.clone(), from_key.clone())),
                    )
                    .await?
                    .into_response();

                    Self::expect_empty_response(recover_response, &[StatusCode::OK]).await?;

                    if from_key == to.object_key {
                        return Ok(());
                    }

                    let rename_response = rename_object(
                        State(Arc::clone(&self.app_state)),
                        Self::claims_for(&from.fragment),
                        AxumJson(RenameObjectRequest {
                            source_fragment: from.fragment.clone(),
                            source_key: from_key,
                            destination_fragment: to.fragment.clone(),
                            destination_key: to.object_key.clone(),
                        }),
                    )
                    .await?
                    .into_response();

                    return Self::expect_empty_response(rename_response, &[StatusCode::OK]).await;
                }
                (None, Some(to_key)) => {
                    if to_key != from.object_key {
                        return Err(AppError::BadRequest(
                            "moving object to trash requires matching object key".to_string(),
                        ));
                    }

                    let response = delete_object(
                        State(Arc::clone(&self.app_state)),
                        Self::claims_for(&from.fragment),
                        AxumPath((from.fragment.clone(), from.object_key.clone())),
                    )
                    .await?
                    .into_response();

                    return Self::expect_empty_response(response, &[StatusCode::OK]).await;
                }
                (Some(_), Some(_)) => {
                    return Err(AppError::BadRequest(
                        "renaming inside trash namespace is not supported".to_string(),
                    ));
                }
                (None, None) => {}
            }

            self.fetch_object_metadata(&from.fragment, &from.object_key)
                .await?;
            self.rename_live_object(&from.fragment, &from.object_key, &to.object_key)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::response::Response;

    #[test]
    fn split_trash_virtual_key_root_and_child() {
        assert_eq!(
            InProcessBackend::split_trash_virtual_key(FSD_TRASH_DIR_NAME),
            Some("")
        );
        assert_eq!(
            InProcessBackend::split_trash_virtual_key(".n1-trash/a/b.bin"),
            Some("a/b.bin")
        );
        assert_eq!(
            InProcessBackend::split_trash_virtual_key(".Trash-1000/a/b.bin"),
            None
        );
        assert_eq!(InProcessBackend::split_trash_virtual_key("a/b.bin"), None);
    }

    #[test]
    fn detect_trash_virtual_path() {
        let trash = FsdPath {
            fragment: "f".to_string(),
            object_key: ".n1-trash/video.mp4".to_string(),
        };
        let live = FsdPath {
            fragment: "f".to_string(),
            object_key: "video.mp4".to_string(),
        };
        assert!(InProcessBackend::is_trash_virtual_path(&trash));
        assert!(!InProcessBackend::is_trash_virtual_path(&live));
    }

    #[test]
    fn build_trash_virtual_path() {
        let base = FsdPath {
            fragment: "frag".to_string(),
            object_key: String::new(),
        };
        let got = InProcessBackend::to_trash_virtual_path(&base, "a/b/c.mp4");
        assert_eq!(got.fragment, "frag");
        assert_eq!(got.object_key, ".n1-trash/a/b/c.mp4");
    }

    #[tokio::test]
    async fn expect_empty_response_ignores_large_body_for_accepted_status() {
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(vec![0u8; FSD_CONTROL_BODY_LIMIT_BYTES + 1]))
            .expect("response");

        let result = InProcessBackend::expect_empty_response(response, &[StatusCode::OK]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn expect_empty_response_returns_status_error_for_non_accepted_status() {
        let response = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("bad request"))
            .expect("response");

        let result = InProcessBackend::expect_empty_response(response, &[StatusCode::OK]).await;
        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }
}
