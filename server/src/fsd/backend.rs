use crate::enums::app_error_enum::AppError;
use crate::fsd::model::{FsdPath, NodeKind, NodeMetadata, WriteHandle};
use crate::object_storage::delete::{delete_object, delete_object_permanently, recover_object};
use crate::object_storage::disk_manager::{
    ChunkQuery, InitiateUploadParams, ObjectKeyQuery, ObjectMetadata, RenameObjectRequest,
    UploadPartQuery,
};
use crate::object_storage::download::download_object_chunk;
use crate::object_storage::modify::rename_object;
use crate::object_storage::retrieve::{get_object_metadata, list_objects, list_trash_objects};
use crate::object_storage::upload::{
    cancel_upload_session, finalize_object_upload, initiate_object_upload, object_upload_part,
};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use axum::body::{to_bytes, Body};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json as AxumJson;
use futures_util::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
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

pub trait FsdBackend: Send + Sync + 'static {
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
struct PendingWrite {
    path: FsdPath,
    staging_path: PathBuf,
    expected_size_bytes: Option<u64>,
    max_written_end: u64,
}

#[derive(Debug, Deserialize)]
struct ListObjectsResponse {
    objects: Vec<ObjectMetadata>,
    #[serde(default)]
    common_prefixes: Vec<String>,
    continuation_token: Option<String>,
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
    pending_writes: Arc<Mutex<HashMap<String, PendingWrite>>>,
}

const FSD_CONTROL_BODY_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const FSD_MAX_READ_BYTES: usize = 1024 * 1024;
const FSD_DIR_MARKER_NAME: &str = ".n1fsd_dir";
const FSD_TRASH_DIR_NAME: &str = ".Trash-1000";

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
        let (status, body) = Self::response_bytes(response, FSD_CONTROL_BODY_LIMIT_BYTES).await?;
        if !accepted.contains(&status) {
            return Err(Self::status_to_error(status, &body));
        }
        Ok(())
    }

    async fn list_page(
        &self,
        fragment: &str,
        prefix: &str,
        continuation_token: Option<String>,
    ) -> Result<ListObjectsResponse, AppError> {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "1000".to_string());
        params.insert("delimiter".to_string(), "/".to_string());
        if !prefix.is_empty() {
            params.insert("prefix".to_string(), prefix.to_string());
        }
        if let Some(token) = continuation_token {
            params.insert("continuation_token".to_string(), token);
        }

        let response = list_objects(
            Self::claims_for(fragment),
            AxumPath(fragment.to_string()),
            Query(params),
        )
        .await?
        .into_response();

        Self::parse_json_response(response, &[StatusCode::OK]).await
    }

    async fn list_trash_page(
        &self,
        fragment: &str,
        prefix: &str,
        continuation_token: Option<String>,
    ) -> Result<ListObjectsResponse, AppError> {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "1000".to_string());
        if !prefix.is_empty() {
            params.insert("prefix".to_string(), prefix.to_string());
        }
        if let Some(token) = continuation_token {
            params.insert("continuation_token".to_string(), token);
        }

        let response = list_trash_objects(
            Self::claims_for(fragment),
            AxumPath(fragment.to_string()),
            Query(params),
        )
        .await?
        .into_response();

        Self::parse_json_response(response, &[StatusCode::OK]).await
    }

    async fn fetch_object_metadata(
        &self,
        fragment: &str,
        object_key: &str,
    ) -> Result<ObjectMetadata, AppError> {
        let response = get_object_metadata(
            Self::claims_for(fragment),
            AxumPath((fragment.to_string(), object_key.to_string())),
        )
        .await?
        .into_response();

        Self::parse_json_response(response, &[StatusCode::OK]).await
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
        let mut continuation_token: Option<String> = None;
        let exact = object_key.trim_matches('/').to_string();
        if exact.is_empty() {
            return Ok(None);
        }

        loop {
            let page = self
                .list_trash_page(fragment, &exact, continuation_token.clone())
                .await?;
            for object in page.objects {
                if object.object_key == exact {
                    return Ok(Some(object));
                }
            }
            continuation_token = page.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(None)
    }

    async fn trash_directory_exists(
        &self,
        fragment: &str,
        dir_key: &str,
    ) -> Result<bool, AppError> {
        let dir_key = dir_key.trim_matches('/').to_string();
        if dir_key.is_empty() {
            return Ok(true);
        }
        let prefix = format!("{}/", dir_key);
        let page = self.list_trash_page(fragment, &prefix, None).await?;
        Ok(!page.objects.is_empty())
    }

    async fn directory_exists(&self, path: &FsdPath) -> Result<bool, AppError> {
        if path.is_fragment_root() {
            return Ok(true);
        }

        let prefix = format!("{}/", path.object_key.trim_matches('/'));
        let page = self.list_page(&path.fragment, &prefix, None).await?;
        Ok(!page.objects.is_empty() || !page.common_prefixes.is_empty())
    }

    fn is_dir_marker_name(name: &str) -> bool {
        name == FSD_DIR_MARKER_NAME
    }

    fn dir_marker_key(dir_key: &str) -> String {
        format!("{}/{}", dir_key.trim_matches('/'), FSD_DIR_MARKER_NAME)
    }

    async fn put_empty_object(&self, path: FsdPath) -> Result<(), AppError> {
        self.ensure_staging_root().await?;

        let staging_path = self
            .staging_root
            .join(format!("{}.fsd.dir.upload", Ulid::new()));

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&staging_path)
            .await
            .map_err(|e| AppError::Io(e, "create fsd directory marker staging file".to_string()))?;
        drop(file);

        let pending = PendingWrite {
            path,
            staging_path: staging_path.clone(),
            expected_size_bytes: Some(0),
            max_written_end: 0,
        };

        let result = self.commit_pending_write(&pending).await;
        let _ = fs::remove_file(&staging_path).await;
        result
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

    async fn commit_pending_write(&self, pending: &PendingWrite) -> Result<(), AppError> {
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

        let init_response = initiate_object_upload(
            Self::claims_for(&fragment),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumJson(InitiateUploadParams {
                content_type: Some("application/octet-stream".to_string()),
                total_size_bytes: total_size,
                expected_parts,
            }),
        )
        .await?
        .into_response();

        let init_body = Self::parse_json_response::<InitiateUploadResponseBody>(
            init_response,
            &[StatusCode::CREATED],
        )
        .await?;

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

            let mut headers = HeaderMap::new();
            headers.insert(
                "content-length",
                HeaderValue::from_str(&chunk_len.to_string()).map_err(|e| {
                    AppError::InternalServerError(format!(
                        "fsd invalid content-length header: {}",
                        e
                    ))
                })?,
            );

            let part_response = object_upload_part(
                Self::claims_for(&fragment),
                headers,
                AxumPath((fragment.clone(), init_body.version_id.clone(), part_index)),
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

            if let Err(err) = Self::expect_empty_response(part_response, &[StatusCode::OK]).await {
                self.cleanup_remote_upload_session(&fragment, &init_body.version_id, &object_key)
                    .await;
                return Err(err);
            }

            sent = sent.saturating_add(chunk_len as u64);
            part_index = part_index.saturating_add(1);
        }

        let finalize_response = finalize_object_upload(
            State(Arc::clone(&self.app_state)),
            Self::claims_for(&fragment),
            AxumPath((
                fragment.clone(),
                init_body.version_id.clone(),
                init_body.session_id.clone(),
            )),
            Query(ObjectKeyQuery {
                object_key: object_key.clone(),
                version_id: None,
            }),
            AxumJson(InitiateUploadParams {
                content_type: Some("application/octet-stream".to_string()),
                total_size_bytes: total_size,
                expected_parts,
            }),
        )
        .await?
        .into_response();

        if let Err(err) = Self::expect_empty_response(finalize_response, &[StatusCode::OK]).await {
            self.cleanup_remote_upload_session(&fragment, &init_body.version_id, &object_key)
                .await;
            return Err(err);
        }

        Ok(())
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
            Err(AppError::Conflict(_)) => self.delete_object_permanently(fragment, object_key).await,
            Err(e) => Err(e),
        }
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
    fn list_dir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<Vec<DirEntry>, AppError>> {
        Box::pin(async move {
            if let Some(trash_sub_key) = Self::split_trash_virtual_key(path.object_key.as_str()) {
                let mut continuation_token: Option<String> = None;
                let mut entries = Vec::new();
                let mut seen = HashSet::new();
                let trash_prefix = trash_sub_key.trim_matches('/').to_string();
                let scan_prefix = if trash_prefix.is_empty() {
                    String::new()
                } else {
                    format!("{}/", trash_prefix)
                };

                loop {
                    let page = self
                        .list_trash_page(&path.fragment, &scan_prefix, continuation_token.clone())
                        .await?;

                    for object in page.objects {
                        let rel = if scan_prefix.is_empty() {
                            object.object_key.as_str()
                        } else {
                            object
                                .object_key
                                .strip_prefix(&scan_prefix)
                                .unwrap_or(&object.object_key)
                        };

                        let rel = rel.trim_matches('/');
                        if rel.is_empty() {
                            continue;
                        }

                        if let Some((first, _)) = rel.split_once('/') {
                            let first = first.trim_matches('/');
                            if first.is_empty() {
                                continue;
                            }
                            let child_key = if trash_prefix.is_empty() {
                                first.to_string()
                            } else {
                                format!("{}/{}", trash_prefix, first)
                            };
                            if !seen.insert(format!("d:{}", child_key)) {
                                continue;
                            }
                            entries.push(DirEntry {
                                name: first.to_string(),
                                path: Self::to_trash_virtual_path(path, &child_key),
                                metadata: NodeMetadata {
                                    kind: NodeKind::Directory,
                                    size_bytes: 0,
                                    modified_at_unix: 0,
                                },
                            });
                            continue;
                        }

                        if !seen.insert(format!("f:{}", object.object_key)) {
                            continue;
                        }

                        entries.push(DirEntry {
                            name: rel.to_string(),
                            path: Self::to_trash_virtual_path(path, &object.object_key),
                            metadata: NodeMetadata {
                                kind: NodeKind::File,
                                size_bytes: object.total_size_bytes,
                                modified_at_unix: object.created_at,
                            },
                        });
                    }

                    continuation_token = page.continuation_token;
                    if continuation_token.is_none() {
                        break;
                    }
                }

                if Self::is_trash_root_name(path.object_key.as_str()) {
                    return Ok(entries);
                }

                if entries.is_empty() {
                    if self
                        .trash_exact_object_metadata(&path.fragment, &trash_prefix)
                        .await?
                        .is_some()
                    {
                        return Err(AppError::BadRequest(
                            "fsd path is not a directory".to_string(),
                        ));
                    }
                    return Err(AppError::NotFound);
                }

                return Ok(entries);
            }

            let prefix = if path.object_key.is_empty() {
                String::new()
            } else {
                format!("{}/", path.object_key.trim_matches('/'))
            };

            let mut continuation_token: Option<String> = None;
            let mut entries = Vec::new();
            let mut seen = HashSet::new();

            loop {
                let page = self
                    .list_page(&path.fragment, &prefix, continuation_token.clone())
                    .await?;

                for common_prefix in page.common_prefixes {
                    let object_key = common_prefix.trim_end_matches('/').to_string();
                    if object_key.is_empty() {
                        continue;
                    }
                    if !seen.insert(format!("d:{}", object_key)) {
                        continue;
                    }

                    let rel = if prefix.is_empty() {
                        object_key.as_str()
                    } else {
                        object_key.strip_prefix(&prefix).unwrap_or(&object_key)
                    };

                    let name = rel.trim_matches('/').to_string();
                    if name.is_empty() {
                        continue;
                    }

                    entries.push(DirEntry {
                        name,
                        path: FsdPath {
                            fragment: path.fragment.clone(),
                            object_key,
                        },
                        metadata: NodeMetadata {
                            kind: NodeKind::Directory,
                            size_bytes: 0,
                            modified_at_unix: 0,
                        },
                    });
                }

                for object in page.objects {
                    if !seen.insert(format!("f:{}", object.object_key)) {
                        continue;
                    }

                    let rel = if prefix.is_empty() {
                        object.object_key.as_str()
                    } else {
                        object
                            .object_key
                            .strip_prefix(&prefix)
                            .unwrap_or(&object.object_key)
                    };

                    let name = rel.trim_matches('/').to_string();
                    if name.is_empty()
                        || name.contains('/')
                        || Self::is_dir_marker_name(name.as_str())
                    {
                        continue;
                    }

                    entries.push(DirEntry {
                        name,
                        path: FsdPath {
                            fragment: path.fragment.clone(),
                            object_key: object.object_key.clone(),
                        },
                        metadata: NodeMetadata {
                            kind: NodeKind::File,
                            size_bytes: object.total_size_bytes,
                            modified_at_unix: object.created_at,
                        },
                    });
                }

                continuation_token = page.continuation_token;
                if continuation_token.is_none() {
                    break;
                }
            }

            if path.is_fragment_root() {
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

            if !path.is_fragment_root() && entries.is_empty() {
                if self
                    .fetch_object_metadata(&path.fragment, &path.object_key)
                    .await
                    .is_ok()
                {
                    return Err(AppError::BadRequest(
                        "fsd path is not a directory".to_string(),
                    ));
                }
            }

            Ok(entries)
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
                return Ok(NodeMetadata {
                    kind: NodeKind::Directory,
                    size_bytes: 0,
                    modified_at_unix: 0,
                });
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

                if self.trash_directory_exists(&path.fragment, &key).await? {
                    return Ok(NodeMetadata {
                        kind: NodeKind::Directory,
                        size_bytes: 0,
                        modified_at_unix: 0,
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
                Err(AppError::NotFound) => {
                    if self.directory_exists(path).await? {
                        Ok(NodeMetadata {
                            kind: NodeKind::Directory,
                            size_bytes: 0,
                            modified_at_unix: 0,
                        })
                    } else {
                        Err(AppError::NotFound)
                    }
                }
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

            let response = download_object_chunk(
                Self::claims_for(&path.fragment),
                AxumPath(path.fragment.clone()),
                Query(ChunkQuery {
                    object_key: path.object_key.clone(),
                    offset,
                    chunk_size: capped_len,
                }),
            )
            .await?
            .into_response();

            let (status, body) = Self::response_bytes(response, capped_len).await?;
            if status == StatusCode::NO_CONTENT {
                return Ok(Vec::new());
            }

            if status != StatusCode::OK {
                return Err(Self::status_to_error(status, &body));
            }

            Ok(body)
        })
    }

    fn begin_write<'a>(
        &'a self,
        path: &'a FsdPath,
        expected_size_bytes: Option<u64>,
    ) -> BoxFuture<'a, Result<WriteHandle, AppError>> {
        Box::pin(async move {
            if path.is_fragment_root() {
                return Err(AppError::BadRequest(
                    "cannot write to a fragment root path".to_string(),
                ));
            }

            if Self::is_trash_virtual_path(path) {
                return Err(AppError::BadRequest(
                    "cannot write into trash namespace".to_string(),
                ));
            }

            self.ensure_staging_root().await?;

            let handle_id = Ulid::new().to_string();
            let staging_path = self.staging_root.join(format!("{}.fsd.upload", handle_id));

            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&staging_path)
                .await
                .map_err(|e| AppError::Io(e, "create fsd staging file".to_string()))?;

            if let Some(expected) = expected_size_bytes {
                file.set_len(expected)
                    .await
                    .map_err(|e| AppError::Io(e, "preallocate fsd staging file".to_string()))?;
            }

            drop(file);

            let pending = PendingWrite {
                path: path.clone(),
                staging_path,
                expected_size_bytes,
                max_written_end: 0,
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

            let (staging_path, expected_size) = {
                let guard = self.pending_writes.lock().await;
                let pending = guard
                    .get(&handle.session_id)
                    .ok_or_else(|| AppError::NotFound)?;
                (pending.staging_path.clone(), pending.expected_size_bytes)
            };

            let end = offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| AppError::BadRequest("write offset overflow".to_string()))?;

            if let Some(expected) = expected_size {
                if end > expected {
                    return Err(AppError::BadRequest(
                        "write exceeds expected object size".to_string(),
                    ));
                }
            }

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&staging_path)
                .await
                .map_err(|e| AppError::Io(e, "open fsd staging file".to_string()))?;

            file.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| AppError::Io(e, "seek fsd staging file".to_string()))?;
            file.write_all(&bytes)
                .await
                .map_err(|e| AppError::Io(e, "write fsd staging file".to_string()))?;
            file.flush()
                .await
                .map_err(|e| AppError::Io(e, "flush fsd staging file".to_string()))?;

            let mut guard = self.pending_writes.lock().await;
            let pending = guard
                .get_mut(&handle.session_id)
                .ok_or(AppError::NotFound)?;
            pending.max_written_end = pending.max_written_end.max(end);

            Ok(())
        })
    }

    fn commit_write<'a>(&'a self, handle: &'a WriteHandle) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            let pending = {
                let mut guard = self.pending_writes.lock().await;
                guard.remove(&handle.session_id)
            }
            .ok_or(AppError::NotFound)?;

            match self.commit_pending_write(&pending).await {
                Ok(()) => {
                    let _ = fs::remove_file(&pending.staging_path).await;
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
                let _ = fs::remove_file(state.staging_path).await;
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
                return Err(AppError::BadRequest("cannot delete trash root".to_string()));
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

    fn mkdir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            if path.is_fragment_root() {
                return Err(AppError::BadRequest(
                    "cannot create fragment root directory".to_string(),
                ));
            }

            if Self::is_trash_virtual_path(path) {
                return Err(AppError::BadRequest(
                    "cannot create directory in trash namespace".to_string(),
                ));
            }

            let dir_key = path.object_key.trim_matches('/').to_string();
            if dir_key.is_empty() {
                return Err(AppError::BadRequest("invalid directory path".to_string()));
            }

            if self
                .fetch_object_metadata(&path.fragment, &dir_key)
                .await
                .is_ok()
            {
                return Err(AppError::Conflict(
                    "path already exists as file".to_string(),
                ));
            }

            if self
                .directory_exists(&FsdPath {
                    fragment: path.fragment.clone(),
                    object_key: dir_key.clone(),
                })
                .await?
            {
                return Err(AppError::Conflict("directory already exists".to_string()));
            }

            if let Some((parent_key, _)) = dir_key.rsplit_once('/') {
                if !parent_key.is_empty() {
                    if self
                        .fetch_object_metadata(&path.fragment, parent_key)
                        .await
                        .is_ok()
                    {
                        return Err(AppError::BadRequest(
                            "parent path is not a directory".to_string(),
                        ));
                    }

                    if !self
                        .directory_exists(&FsdPath {
                            fragment: path.fragment.clone(),
                            object_key: parent_key.to_string(),
                        })
                        .await?
                    {
                        return Err(AppError::NotFound);
                    }
                }
            }

            let marker_path = FsdPath {
                fragment: path.fragment.clone(),
                object_key: Self::dir_marker_key(&dir_key),
            };

            self.put_empty_object(marker_path).await
        })
    }

    fn rmdir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            if path.is_fragment_root() {
                return Err(AppError::BadRequest(
                    "cannot remove fragment root directory".to_string(),
                ));
            }

            if Self::is_trash_virtual_path(path) {
                return Err(AppError::BadRequest(
                    "cannot remove directory in trash namespace".to_string(),
                ));
            }

            let dir_key = path.object_key.trim_matches('/').to_string();
            if dir_key.is_empty() {
                return Err(AppError::BadRequest("invalid directory path".to_string()));
            }

            if self
                .fetch_object_metadata(&path.fragment, &dir_key)
                .await
                .is_ok()
            {
                return Err(AppError::BadRequest("path is not a directory".to_string()));
            }

            if !self
                .directory_exists(&FsdPath {
                    fragment: path.fragment.clone(),
                    object_key: dir_key.clone(),
                })
                .await?
            {
                return Err(AppError::NotFound);
            }

            let mut continuation_token: Option<String> = None;
            let marker_key = Self::dir_marker_key(&dir_key);
            let mut marker_found = false;
            let prefix = format!("{}/", dir_key);

            loop {
                let page = self
                    .list_page(&path.fragment, &prefix, continuation_token.clone())
                    .await?;

                if !page.common_prefixes.is_empty() {
                    return Err(AppError::Conflict("directory is not empty".to_string()));
                }

                for object in page.objects {
                    if object.object_key == marker_key {
                        marker_found = true;
                        continue;
                    }
                    return Err(AppError::Conflict("directory is not empty".to_string()));
                }

                continuation_token = page.continuation_token;
                if continuation_token.is_none() {
                    break;
                }
            }

            if !marker_found {
                return Err(AppError::NotFound);
            }

            self.delete_active_object_with_fallback(&path.fragment, &marker_key)
                .await
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

            let response = rename_object(
                State(Arc::clone(&self.app_state)),
                Self::claims_for(&from.fragment),
                AxumJson(RenameObjectRequest {
                    source_fragment: from.fragment.clone(),
                    source_key: from.object_key.clone(),
                    destination_fragment: to.fragment.clone(),
                    destination_key: to.object_key.clone(),
                }),
            )
            .await?
            .into_response();

            Self::expect_empty_response(response, &[StatusCode::OK]).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_trash_virtual_key_root_and_child() {
        assert_eq!(
            InProcessBackend::split_trash_virtual_key(FSD_TRASH_DIR_NAME),
            Some("")
        );
        assert_eq!(
            InProcessBackend::split_trash_virtual_key(".Trash-1000/a/b.bin"),
            Some("a/b.bin")
        );
        assert_eq!(
            InProcessBackend::split_trash_virtual_key(".trash000/a/b.bin"),
            None
        );
        assert_eq!(InProcessBackend::split_trash_virtual_key("a/b.bin"), None);
    }

    #[test]
    fn detect_trash_virtual_path() {
        let trash = FsdPath {
            fragment: "f".to_string(),
            object_key: ".Trash-1000/video.mp4".to_string(),
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
        assert_eq!(got.object_key, ".Trash-1000/a/b/c.mp4");
    }
}
