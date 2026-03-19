use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::ADMIN_KEY;
use crate::n1_core::disk_manager::ObjectPathManager;
use crate::n1_core::modify::sync_dir;
use crate::n2_core::config::{effective_cross_params, n2_runtime_config};
use crate::n2_core::model::{
    CrossParams, DeleteN2ObjectQuery, GetObjectQuery, InternalStoreShardQuery, ListN2ObjectsQuery,
    ListN2VersionsQuery, N2ClusterShardRef, N2DeleteObjectResponse, N2DiskShardRef,
    N2ListObjectVersionsResponse, N2ListObjectsResponse, N2ObjectMap, N2ObjectSummary,
    N2ObjectVersionSummary, N2PutObjectResponse, N2ReconcileObjectResult, N2ReconcileResponse,
    N2RenameObjectResponse, NodeDescriptor, ObjectIdentity, PutObjectQuery, ReconcileFragmentQuery,
    RenameN2ObjectRequest,
};
use crate::n2_core::state::ensure_cluster_write_ready;
use crate::n2_core::state::{get_membership, resolve_placement};
use crate::structs::auth_structs::Claims;
use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
};
use axum::response::{IntoResponse, Response};
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use futures_util::StreamExt;
use reqwest::Body as ReqwestBody;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use simd_erasure_core::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::{HashMap, HashSet};
use std::path::{Path as StdPath, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};
use ulid::Ulid;

const N2_OBJECT_MAP_EXT: &str = ".n2map";
const N2_OBJECT_LATEST_EXT: &str = ".n2latest";
const N2_LOCAL_CSHARD_META_FILE: &str = "cshard.meta";
const N2_INTERNAL_TOKEN_HEADER: &str = "x-n2-internal-token";
const N2_REMOTE_CHUNK_FLUSH_MIN_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LocalClusterShardMeta {
    fragment: String,
    version_id: String,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
    cluster_size_bytes: u64,
    disk_shard_size_bytes: u64,
    disk_shards: Vec<N2DiskShardRef>,
}

#[derive(Clone, Debug)]
struct ClusterShardFileSource {
    cluster_shard_index: usize,
    path: PathBuf,
    size_bytes: usize,
}

#[derive(Clone, Debug)]
struct LocalCoordinatorShardSession {
    target: NodeDescriptor,
    cluster_shard_index: usize,
    temp_root: PathBuf,
    temp_file_path: PathBuf,
    checksum: Sha256,
    size_bytes: u64,
    failed: Option<String>,
}

#[derive(Clone, Debug)]
struct RemoteCoordinatorShardSession {
    target: NodeDescriptor,
    cluster_shard_index: usize,
    session_id: String,
    pending: Vec<u8>,
    checksum: Sha256,
    size_bytes: u64,
    failed: Option<String>,
}

#[derive(Clone, Debug)]
enum CoordinatorShardSession {
    Local(LocalCoordinatorShardSession),
    Remote(RemoteCoordinatorShardSession),
}

#[derive(Clone, Debug, Serialize)]
struct InternalStoreShardRemoteQuery<'a> {
    uid: &'a str,
    object_key: &'a str,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
}

#[derive(Clone, Debug)]
struct InternalShardUploadSession {
    object: ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
    temp_root: PathBuf,
    temp_file_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalShardSessionResponse {
    session_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalShardChunkAck {
    session_id: String,
    written_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalShardAbortResponse {
    session_id: String,
    aborted: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InternalMapReadQuery {
    object_key: String,
    version_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InternalMapDeleteQuery {
    object_key: String,
    version_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalMapUpsertResponse {
    local_mirrors_written: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalMapDeleteResponse {
    local_map_files_deleted: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalShardDeleteResponse {
    deleted_paths: usize,
}

fn is_admin_claim(claims: &Claims) -> bool {
    claims.fragment == *ADMIN_KEY
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint.trim().trim_end_matches('/').to_ascii_lowercase()
}

fn is_local_target(target: &NodeDescriptor) -> bool {
    let cfg = n2_runtime_config();
    target.node_id == cfg.node_id
        || normalize_endpoint(&target.endpoint) == normalize_endpoint(&cfg.node_endpoint)
}

fn shard_matches_target(shard: &N2ClusterShardRef, target: &NodeDescriptor) -> bool {
    shard.node_id == target.node_id
        || normalize_endpoint(&shard.endpoint) == normalize_endpoint(&target.endpoint)
}

type InternalShardUploadSessionStore = Mutex<HashMap<String, InternalShardUploadSession>>;
static INTERNAL_SHARD_UPLOAD_SESSIONS: OnceLock<InternalShardUploadSessionStore> = OnceLock::new();

fn internal_shard_upload_sessions() -> &'static InternalShardUploadSessionStore {
    INTERNAL_SHARD_UPLOAD_SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

impl CoordinatorShardSession {
    fn cluster_shard_index(&self) -> usize {
        match self {
            CoordinatorShardSession::Local(v) => v.cluster_shard_index,
            CoordinatorShardSession::Remote(v) => v.cluster_shard_index,
        }
    }

    fn target(&self) -> &NodeDescriptor {
        match self {
            CoordinatorShardSession::Local(v) => &v.target,
            CoordinatorShardSession::Remote(v) => &v.target,
        }
    }

    fn is_failed(&self) -> bool {
        match self {
            CoordinatorShardSession::Local(v) => v.failed.is_some(),
            CoordinatorShardSession::Remote(v) => v.failed.is_some(),
        }
    }

    fn mark_failed(&mut self, reason: String) {
        match self {
            CoordinatorShardSession::Local(v) => {
                if v.failed.is_none() {
                    v.failed = Some(reason);
                }
            }
            CoordinatorShardSession::Remote(v) => {
                if v.failed.is_none() {
                    v.failed = Some(reason);
                }
            }
        }
    }
}

fn normalize_n2_stripe_block_size(v: usize) -> usize {
    let clamped = v.clamp(4 * 1024, 4 * 1024 * 1024);
    ((clamped + 4095) / 4096) * 4096
}

fn object_key_hash(object_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    hex::encode(hasher.finalize())
}

fn map_dir_for_root(root: &StdPath, fragment: &str, key_hash: &str) -> PathBuf {
    root.join(fragment)
        .join("n2")
        .join("index")
        .join(&key_hash[0..2])
        .join(&key_hash[2..4])
        .join(&key_hash[4..6])
        .join(&key_hash[6..8])
}

fn map_path_for_root(root: &StdPath, fragment: &str, key_hash: &str, version_id: &str) -> PathBuf {
    map_dir_for_root(root, fragment, key_hash)
        .join(format!("{}#{}{}", key_hash, version_id, N2_OBJECT_MAP_EXT))
}

fn latest_path_for_root(root: &StdPath, fragment: &str, key_hash: &str) -> PathBuf {
    map_dir_for_root(root, fragment, key_hash).join(format!("{}{}", key_hash, N2_OBJECT_LATEST_EXT))
}

async fn write_atomic(path: &StdPath, data: &[u8]) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::Io(e, format!("mkdir {:?}", parent)))?;
    }
    let tmp = path.with_extension(format!("tmp.{}", Ulid::new()));
    fs::write(&tmp, data)
        .await
        .map_err(|e| AppError::Io(e, format!("write tmp {:?}", tmp)))?;
    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {:?} -> {:?}", tmp, path)))?;
    if let Some(parent) = path.parent() {
        let _ = sync_dir(parent).await;
    }
    Ok(())
}

async fn persist_n2_object_map_local(map: &N2ObjectMap) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No index roots configured".to_string(),
        ));
    }

    let key_hash = object_key_hash(map.object.object_key.as_str());
    let map_bytes = rmp_serde::to_vec(map).map_err(|e| {
        AppError::InternalServerError(format!("encode n2 object map failed: {}", e))
    })?;
    let latest_bytes = map.object.version_id.as_bytes().to_vec();

    let mut writes_ok = 0usize;
    for root in &roots {
        let map_path = map_path_for_root(
            root,
            map.object.fragment.as_str(),
            &key_hash,
            map.object.version_id.as_str(),
        );
        let latest_path = latest_path_for_root(root, map.object.fragment.as_str(), &key_hash);
        if write_atomic(&map_path, &map_bytes).await.is_ok()
            && write_atomic(&latest_path, &latest_bytes).await.is_ok()
        {
            writes_ok = writes_ok.saturating_add(1);
        } else {
            warn!(
                "n2 map mirror write failed root={} key={} version={}",
                root.display(),
                map.object.object_key,
                map.object.version_id
            );
        }
    }

    if writes_ok == 0 {
        return Err(AppError::InternalServerError(
            "failed to persist n2 object map on all mirrors".to_string(),
        ));
    }

    Ok(writes_ok)
}

async fn list_n2_map_versions_local(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let key_hash = object_key_hash(object_key);
    let mut versions = HashSet::new();
    for root in &roots {
        let dir = map_dir_for_root(root, fragment, key_hash.as_str());
        let exists = fs::try_exists(&dir)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", dir)))?;
        if !exists {
            continue;
        }
        let mut entries = match fs::read_dir(&dir).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(N2_OBJECT_MAP_EXT) {
                continue;
            }
            let Some(rest) = name.strip_prefix(format!("{}#", key_hash).as_str()) else {
                continue;
            };
            let Some(version) = rest.strip_suffix(N2_OBJECT_MAP_EXT) else {
                continue;
            };
            if !version.is_empty() {
                versions.insert(version.to_string());
            }
        }
    }

    let mut out = versions.into_iter().collect::<Vec<_>>();
    out.sort();
    Ok(out)
}

async fn refresh_n2_latest_marker_local(fragment: &str, object_key: &str) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Ok(());
    }

    let key_hash = object_key_hash(object_key);
    let latest_version = list_n2_map_versions_local(fragment, object_key)
        .await?
        .pop();
    if let Some(version) = latest_version {
        let latest_bytes = version.into_bytes();
        let mut writes_ok = 0usize;
        for root in &roots {
            let latest_path = latest_path_for_root(root, fragment, key_hash.as_str());
            if write_atomic(&latest_path, &latest_bytes).await.is_ok() {
                writes_ok = writes_ok.saturating_add(1);
            }
        }
        if writes_ok == 0 {
            return Err(AppError::InternalServerError(
                "failed to update n2 latest marker on all mirrors".to_string(),
            ));
        }
    } else {
        for root in &roots {
            let latest_path = latest_path_for_root(root, fragment, key_hash.as_str());
            let _ = fs::remove_file(&latest_path).await;
            if let Some(parent) = latest_path.parent() {
                let _ = sync_dir(parent).await;
            }
        }
    }
    Ok(())
}

async fn delete_n2_object_map_local(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Ok(0);
    }

    let key_hash = object_key_hash(object_key);
    let mut deleted = 0usize;
    for root in &roots {
        let map_path = map_path_for_root(root, fragment, key_hash.as_str(), version_id);
        let exists = fs::try_exists(&map_path)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", map_path)))?;
        if !exists {
            continue;
        }
        fs::remove_file(&map_path)
            .await
            .map_err(|e| AppError::Io(e, format!("remove {:?}", map_path)))?;
        deleted = deleted.saturating_add(1);
        if let Some(parent) = map_path.parent() {
            let _ = sync_dir(parent).await;
        }
    }

    refresh_n2_latest_marker_local(fragment, object_key).await?;
    Ok(deleted)
}

async fn replicate_n2_object_map_remote(
    http_client: &reqwest::Client,
    endpoint: &str,
    map: &N2ObjectMap,
) -> Result<(), AppError> {
    let cfg = n2_runtime_config();
    let url = format!(
        "{}/v4/n2/internal/map/upsert",
        endpoint.trim_end_matches('/')
    );
    let response = http_client
        .post(url.as_str())
        .header(N2_INTERNAL_TOKEN_HEADER, cfg.internal_token.as_str())
        .json(map)
        .send()
        .await
        .map_err(|e| AppError::InternalServerError(format!("send n2 map upsert failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "n2 map upsert failed status={} body={}",
            status, body
        )));
    }
    Ok(())
}

async fn delete_n2_object_map_remote(
    http_client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<usize, AppError> {
    let cfg = n2_runtime_config();
    let url = format!(
        "{}/v4/n2/internal/map/{}",
        endpoint.trim_end_matches('/'),
        fragment
    );
    let response = http_client
        .delete(url.as_str())
        .header(N2_INTERNAL_TOKEN_HEADER, cfg.internal_token.as_str())
        .query(&[("object_key", object_key), ("version_id", version_id)])
        .send()
        .await
        .map_err(|e| AppError::InternalServerError(format!("send n2 map delete failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "n2 map delete failed status={} body={}",
            status, body
        )));
    }
    let parsed = response
        .json::<InternalMapDeleteResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("decode n2 map delete response failed: {}", e))
        })?;
    Ok(parsed.local_map_files_deleted)
}

async fn persist_n2_object_map(map: &N2ObjectMap) -> Result<(), AppError> {
    let local_writes = persist_n2_object_map_local(map).await?;
    let cfg = n2_runtime_config();

    let mut remote_endpoints = map
        .shards
        .iter()
        .map(|s| s.endpoint.clone())
        .filter(|endpoint| normalize_endpoint(endpoint) != normalize_endpoint(&cfg.node_endpoint))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    remote_endpoints.sort();

    if remote_endpoints.is_empty() {
        return Ok(());
    }

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;
    let mut replicated = 0usize;
    for endpoint in &remote_endpoints {
        if replicate_n2_object_map_remote(&http_client, endpoint.as_str(), map)
            .await
            .is_ok()
        {
            replicated = replicated.saturating_add(1);
        } else {
            warn!(
                "n2 map replication failed fragment={} object_key={} version={} endpoint={}",
                map.object.fragment, map.object.object_key, map.object.version_id, endpoint
            );
        }
    }

    if replicated == 0 {
        return Err(AppError::Conflict(format!(
            "object committed locally ({} mirrors) but map replication failed on all {} remote targets",
            local_writes,
            remote_endpoints.len()
        )));
    }

    Ok(())
}

async fn read_n2_map_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<N2ObjectMap, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    let key_hash = object_key_hash(object_key);

    for root in roots {
        let map_path = map_path_for_root(&root, fragment, &key_hash, version_id);
        let exists = fs::try_exists(&map_path)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", map_path)))?;
        if !exists {
            continue;
        }

        let bytes = fs::read(&map_path)
            .await
            .map_err(|e| AppError::Io(e, format!("read {:?}", map_path)))?;
        let parsed = rmp_serde::from_slice::<N2ObjectMap>(&bytes).map_err(|e| {
            AppError::InternalServerError(format!("decode n2 object map failed: {}", e))
        })?;
        return Ok(parsed);
    }

    Err(AppError::NotFound)
}

async fn discover_latest_n2_version(fragment: &str, object_key: &str) -> Result<String, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    let key_hash = object_key_hash(object_key);

    for root in &roots {
        let latest_path = latest_path_for_root(root, fragment, &key_hash);
        let exists = fs::try_exists(&latest_path)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", latest_path)))?;
        if !exists {
            continue;
        }
        let bytes = fs::read(&latest_path)
            .await
            .map_err(|e| AppError::Io(e, format!("read {:?}", latest_path)))?;
        let raw = String::from_utf8_lossy(&bytes).trim().to_string();
        if !raw.is_empty() {
            return Ok(raw);
        }
    }

    let mut discovered = Vec::new();
    for root in &roots {
        let dir = map_dir_for_root(root, fragment, &key_hash);
        let exists = fs::try_exists(&dir)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", dir)))?;
        if !exists {
            continue;
        }
        let mut entries = match fs::read_dir(&dir).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(N2_OBJECT_MAP_EXT) {
                continue;
            }
            let Some(rest) = name.strip_prefix(format!("{}#", key_hash).as_str()) else {
                continue;
            };
            let Some(version) = rest.strip_suffix(N2_OBJECT_MAP_EXT) else {
                continue;
            };
            if !version.is_empty() {
                discovered.push(version.to_string());
            }
        }
    }

    discovered.sort();
    discovered.pop().ok_or(AppError::NotFound)
}

async fn load_n2_object_map_local(
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<N2ObjectMap, AppError> {
    let version = match version_id {
        Some(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => discover_latest_n2_version(fragment, object_key).await?,
    };
    read_n2_map_version(fragment, object_key, version.as_str()).await
}

async fn load_n2_object_map_remote(
    http_client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<N2ObjectMap, AppError> {
    let cfg = n2_runtime_config();
    let url = format!(
        "{}/v4/n2/internal/map/{}",
        endpoint.trim_end_matches('/'),
        fragment
    );
    let mut request = http_client
        .get(url.as_str())
        .header(N2_INTERNAL_TOKEN_HEADER, cfg.internal_token.as_str())
        .query(&[("object_key", object_key)]);
    if let Some(v) = version_id {
        request = request.query(&[("version_id", v)]);
    }
    let response = request.send().await.map_err(|e| {
        AppError::InternalServerError(format!("request remote n2 map failed: {}", e))
    })?;
    if response.status() == StatusCode::NOT_FOUND {
        return Err(AppError::NotFound);
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote n2 map read failed status={} body={}",
            status, body
        )));
    }
    response
        .json::<N2ObjectMap>()
        .await
        .map_err(|e| AppError::InternalServerError(format!("decode remote n2 map failed: {}", e)))
}

async fn load_n2_object_map(
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<N2ObjectMap, AppError> {
    let local_result = load_n2_object_map_local(fragment, object_key, version_id).await;
    if local_result.is_ok() {
        return local_result;
    }

    let membership = match get_membership().await {
        Ok(v) => v,
        Err(_) => return local_result,
    };
    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(&cfg.node_endpoint);
    let mut remote_endpoints = membership
        .nodes
        .iter()
        .filter(|n| n.active)
        .map(|n| n.endpoint.clone())
        .filter(|endpoint| normalize_endpoint(endpoint) != local_endpoint)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    remote_endpoints.sort();
    if remote_endpoints.is_empty() {
        return local_result;
    }

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;
    let mut last_err = None;
    for endpoint in remote_endpoints {
        match load_n2_object_map_remote(
            &http_client,
            endpoint.as_str(),
            fragment,
            object_key,
            version_id,
        )
        .await
        {
            Ok(map) => return Ok(map),
            Err(AppError::NotFound) => {}
            Err(e) => last_err = Some(e),
        }
    }

    if let Some(err) = last_err {
        return Err(err);
    }
    local_result
}

async fn collect_n2_map_paths(root: &StdPath, max_paths: usize) -> Result<Vec<PathBuf>, AppError> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = match fs::read_dir(&dir).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AppError::Io(e, format!("read dir entry {:?}", dir)))?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .map_err(|e| AppError::Io(e, format!("file type {:?}", path)))?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            if !name.ends_with(N2_OBJECT_MAP_EXT) {
                continue;
            }
            out.push(path);
            if out.len() >= max_paths {
                return Ok(out);
            }
        }
    }
    Ok(out)
}

async fn list_fragment_n2_maps(
    fragment: &str,
    max_objects: usize,
) -> Result<Vec<N2ObjectMap>, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let mut seen_map_ids = HashSet::new();
    let mut out = Vec::new();
    for root in roots {
        let index_root = root.join(fragment).join("n2").join("index");
        let exists = fs::try_exists(&index_root)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", index_root)))?;
        if !exists {
            continue;
        }

        // Pull a bounded number of files per root; mirrors typically contain the same keys.
        let mut map_paths =
            collect_n2_map_paths(&index_root, max_objects.saturating_mul(8)).await?;
        map_paths.sort();
        for path in map_paths {
            let Some(file_name) = path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            if !seen_map_ids.insert(file_name.to_string()) {
                continue;
            }
            let bytes = match fs::read(&path).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            let map = match rmp_serde::from_slice::<N2ObjectMap>(&bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if map.object.fragment != fragment {
                continue;
            }
            out.push(map);
            if out.len() >= max_objects {
                return Ok(out);
            }
        }
    }

    Ok(out)
}

async fn list_n2_maps_for_object(
    fragment: &str,
    object_key: &str,
    max_versions: usize,
) -> Result<Vec<N2ObjectMap>, AppError> {
    let key = object_key.trim().trim_matches('/').to_string();
    if key.is_empty() {
        return Ok(Vec::new());
    }

    let mut maps = list_fragment_n2_maps(fragment, max_versions.saturating_mul(32)).await?;
    maps.retain(|m| m.object.object_key == key);
    maps.sort_by(|a, b| {
        b.committed_at
            .cmp(&a.committed_at)
            .then_with(|| b.object.version_id.cmp(&a.object.version_id))
    });
    Ok(maps)
}

fn validate_internal_token(headers: &HeaderMap) -> Result<(), AppError> {
    let cfg = n2_runtime_config();
    let Some(token) = headers
        .get(N2_INTERNAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
    else {
        return Err(AppError::Forbidden("missing internal n2 token".to_string()));
    };

    if token != cfg.internal_token {
        return Err(AppError::Forbidden("invalid internal n2 token".to_string()));
    }
    Ok(())
}

fn encode_cluster_stripe_blocks(
    stripe: &[u8],
    kc: usize,
    mc: usize,
    block_size: usize,
) -> Result<Vec<Vec<u8>>, AppError> {
    if stripe.len() != kc.saturating_mul(block_size) {
        return Err(AppError::BadRequest(format!(
            "invalid stripe length {} for kc={} block_size={}",
            stripe.len(),
            kc,
            block_size
        )));
    }

    let mut out = Vec::with_capacity(kc.saturating_add(mc));
    for i in 0..kc {
        let start = i * block_size;
        let end = start + block_size;
        out.push(stripe[start..end].to_vec());
    }

    if mc > 0 {
        let mut encoder = ReedSolomonEncoder::new(kc, mc, block_size)
            .map_err(|e| AppError::InternalServerError(format!("RS init: {}", e)))?;
        for original in &out {
            encoder
                .add_original_shard(original.as_slice())
                .map_err(|e| AppError::InternalServerError(format!("RS add shard: {}", e)))?;
        }
        let encoded = encoder
            .encode()
            .map_err(|e| AppError::InternalServerError(format!("RS encode: {}", e)))?;
        for i in 0..mc {
            let parity = encoded.recovery(i).ok_or_else(|| {
                AppError::InternalServerError("missing RS parity shard".to_string())
            })?;
            out.push(parity.to_vec());
        }
    }

    Ok(out)
}

async fn store_local_cluster_shard_from_file(
    source_path: &StdPath,
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
) -> Result<N2ClusterShardRef, AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    if blob_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob roots configured".to_string(),
        ));
    }
    let kd_usize = usize::from(kd);
    let md_usize = usize::from(md);
    if kd_usize == 0 {
        return Err(AppError::BadRequest("kd must be >= 1".to_string()));
    }
    if blob_roots.len() < kd_usize {
        return Err(AppError::InternalServerError(format!(
            "insufficient blob roots for kd={} (have {})",
            kd_usize,
            blob_roots.len()
        )));
    }

    let nd = kd_usize.saturating_add(md_usize);
    let mut source_file = fs::File::open(source_path)
        .await
        .map_err(|e| AppError::Io(e, format!("open source shard {:?}", source_path)))?;
    let source_size = source_file
        .metadata()
        .await
        .map_err(|e| AppError::Io(e, format!("stat source shard {:?}", source_path)))?
        .len();

    let stripe_data_width = stripe_block_size as u64 * kd_usize as u64;
    let stripe_count = if source_size == 0 {
        0
    } else {
        (source_size + stripe_data_width - 1) / stripe_data_width
    };
    let disk_shard_size_bytes = stripe_count * stripe_block_size as u64;

    let mut writers = Vec::with_capacity(nd);
    let mut disk_shards = Vec::with_capacity(nd);
    for disk_shard_index in 0..nd {
        let disk_idx = (cluster_shard_index + disk_shard_index) % blob_roots.len();
        let cshard_dir = blob_roots[disk_idx]
            .join(object.fragment.as_str())
            .join("n2_data")
            .join("blobs")
            .join(object.version_id.as_str())
            .join(format!("cshard_{}", cluster_shard_index));
        fs::create_dir_all(&cshard_dir)
            .await
            .map_err(|e| AppError::Io(e, format!("mkdir {:?}", cshard_dir)))?;

        let shard_path = cshard_dir.join(format!("disk_shard_{}.bin", disk_shard_index));
        let mut file = fs::File::create(&shard_path)
            .await
            .map_err(|e| AppError::Io(e, format!("create {:?}", shard_path)))?;
        if disk_shard_size_bytes > 0 {
            file.set_len(disk_shard_size_bytes)
                .await
                .map_err(|e| AppError::Io(e, format!("prealloc {:?}", shard_path)))?;
            file.seek(SeekFrom::Start(0))
                .await
                .map_err(|e| AppError::Io(e, format!("seek {:?}", shard_path)))?;
        }
        writers.push(file);

        disk_shards.push(N2DiskShardRef {
            disk_shard_index,
            absolute_path: shard_path.to_string_lossy().to_string(),
            size_bytes: disk_shard_size_bytes,
            checksum: String::new(),
            is_parity: disk_shard_index >= kd_usize,
        });
    }

    let mut cluster_hasher = Sha256::new();
    let mut shard_hashers: Vec<Sha256> = (0..nd).map(|_| Sha256::new()).collect();

    for _ in 0..stripe_count {
        let mut stripe = vec![0u8; stripe_data_width as usize];
        let mut read_total = 0usize;
        while read_total < stripe.len() {
            let n = source_file
                .read(&mut stripe[read_total..])
                .await
                .map_err(|e| AppError::Io(e, "read source shard".to_string()))?;
            if n == 0 {
                break;
            }
            read_total += n;
        }
        if read_total > 0 {
            cluster_hasher.update(&stripe[..read_total]);
        }

        if md_usize > 0 {
            let mut encoder = ReedSolomonEncoder::new(kd_usize, md_usize, stripe_block_size)
                .map_err(|e| AppError::InternalServerError(format!("local RS init: {}", e)))?;
            for i in 0..kd_usize {
                let start = i * stripe_block_size;
                let end = start + stripe_block_size;
                encoder
                    .add_original_shard(&stripe[start..end])
                    .map_err(|e| AppError::InternalServerError(format!("local RS add: {}", e)))?;
            }
            let encoded = encoder
                .encode()
                .map_err(|e| AppError::InternalServerError(format!("local RS encode: {}", e)))?;

            for i in 0..kd_usize {
                let start = i * stripe_block_size;
                let end = start + stripe_block_size;
                let slice = &stripe[start..end];
                shard_hashers[i].update(slice);
                writers[i]
                    .write_all(slice)
                    .await
                    .map_err(|e| AppError::Io(e, "write local data disk shard".to_string()))?;
            }
            for i in 0..md_usize {
                let parity = encoded.recovery(i).ok_or_else(|| {
                    AppError::InternalServerError("local missing parity shard".to_string())
                })?;
                shard_hashers[kd_usize + i].update(parity);
                writers[kd_usize + i]
                    .write_all(parity)
                    .await
                    .map_err(|e| AppError::Io(e, "write local parity disk shard".to_string()))?;
            }
        } else {
            for i in 0..kd_usize {
                let start = i * stripe_block_size;
                let end = start + stripe_block_size;
                let slice = &stripe[start..end];
                shard_hashers[i].update(slice);
                writers[i]
                    .write_all(slice)
                    .await
                    .map_err(|e| AppError::Io(e, "write local disk shard".to_string()))?;
            }
        }
    }

    for w in &mut writers {
        w.flush()
            .await
            .map_err(|e| AppError::Io(e, "flush local disk shard".to_string()))?;
    }

    for (idx, hash) in shard_hashers.iter_mut().enumerate() {
        disk_shards[idx].checksum = hex::encode(hash.clone().finalize());
    }

    let local_meta = LocalClusterShardMeta {
        fragment: object.fragment.clone(),
        version_id: object.version_id.clone(),
        cluster_shard_index,
        kd,
        md,
        stripe_block_size,
        cluster_size_bytes: source_size,
        disk_shard_size_bytes,
        disk_shards: disk_shards.clone(),
    };
    let local_meta_bytes = rmp_serde::to_vec(&local_meta).map_err(|e| {
        AppError::InternalServerError(format!("encode local cshard meta failed: {}", e))
    })?;
    let mut seen_dirs = HashMap::<String, bool>::new();
    for shard in &disk_shards {
        let parent = StdPath::new(&shard.absolute_path).parent().ok_or_else(|| {
            AppError::InternalServerError("disk shard path missing parent".to_string())
        })?;
        let parent_key = parent.to_string_lossy().to_string();
        if seen_dirs.contains_key(&parent_key) {
            continue;
        }
        seen_dirs.insert(parent_key, true);
        let meta_path = parent.join(N2_LOCAL_CSHARD_META_FILE);
        write_atomic(&meta_path, &local_meta_bytes).await?;
    }

    let cfg = n2_runtime_config();
    Ok(N2ClusterShardRef {
        cluster_shard_index,
        node_id: cfg.node_id.clone(),
        endpoint: cfg.node_endpoint.clone(),
        size_bytes: source_size,
        checksum: hex::encode(cluster_hasher.finalize()),
        disk_shards,
    })
}

async fn store_local_cluster_shard_from_bytes(
    bytes: &[u8],
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
) -> Result<N2ClusterShardRef, AppError> {
    let tmp_root = std::env::temp_dir().join(format!("n2-local-store-{}", Ulid::new()));
    fs::create_dir_all(&tmp_root)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {:?}", tmp_root)))?;
    let tmp_path = tmp_root.join(format!("cluster_shard_{}.bin", cluster_shard_index));
    fs::write(&tmp_path, bytes)
        .await
        .map_err(|e| AppError::Io(e, format!("write {:?}", tmp_path)))?;
    let stored = store_local_cluster_shard_from_file(
        tmp_path.as_path(),
        object,
        cluster_shard_index,
        kd,
        md,
        stripe_block_size,
    )
    .await;
    let _ = fs::remove_dir_all(&tmp_root).await;
    stored
}

async fn begin_local_coordinator_shard_session(
    target: NodeDescriptor,
    cluster_shard_index: usize,
) -> Result<CoordinatorShardSession, AppError> {
    let temp_root = std::env::temp_dir().join(format!(
        "n2-coordinator-local-{}-{}",
        cluster_shard_index,
        Ulid::new()
    ));
    fs::create_dir_all(&temp_root)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {:?}", temp_root)))?;
    let temp_file_path = temp_root.join("cluster_shard.bin");
    fs::File::create(&temp_file_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {:?}", temp_file_path)))?;

    Ok(CoordinatorShardSession::Local(
        LocalCoordinatorShardSession {
            target,
            cluster_shard_index,
            temp_root,
            temp_file_path,
            checksum: Sha256::new(),
            size_bytes: 0,
            failed: None,
        },
    ))
}

async fn begin_remote_cluster_shard_session(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
) -> Result<String, AppError> {
    let base = target.endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/session/begin/{}/{}/{}",
        base, object.fragment, object.version_id, cluster_shard_index
    );
    let query = InternalStoreShardRemoteQuery {
        uid: object.uid.as_str(),
        object_key: object.object_key.as_str(),
        kd,
        md,
        stripe_block_size,
    };
    let response = client
        .post(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .query(&query)
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote begin shard session failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote begin shard session status={} body={}",
            status, text
        )));
    }
    let payload = response
        .json::<InternalShardSessionResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("decode shard session begin response: {}", e))
        })?;
    Ok(payload.session_id)
}

async fn append_remote_cluster_shard_chunk(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    session_id: &str,
    bytes: Vec<u8>,
) -> Result<(), AppError> {
    if bytes.is_empty() {
        return Ok(());
    }
    let base = target.endpoint.trim_end_matches('/');
    let url = format!("{}/v4/n2/internal/session/chunk/{}", base, session_id);
    let response = client
        .put(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .body(bytes)
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote shard chunk failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote shard chunk status={} body={}",
            status, text
        )));
    }
    let _ = response
        .json::<InternalShardChunkAck>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("decode shard chunk ack failed: {}", e))
        })?;
    Ok(())
}

async fn commit_remote_cluster_shard_session(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    session_id: &str,
) -> Result<N2ClusterShardRef, AppError> {
    let base = target.endpoint.trim_end_matches('/');
    let url = format!("{}/v4/n2/internal/session/commit/{}", base, session_id);
    let response = client
        .post(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote shard commit failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote shard commit status={} body={}",
            status, text
        )));
    }
    response.json::<N2ClusterShardRef>().await.map_err(|e| {
        AppError::InternalServerError(format!("decode remote shard commit response failed: {}", e))
    })
}

async fn abort_remote_cluster_shard_session(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    session_id: &str,
) -> Result<(), AppError> {
    let base = target.endpoint.trim_end_matches('/');
    let url = format!("{}/v4/n2/internal/session/abort/{}", base, session_id);
    let response = client
        .post(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote shard abort failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote shard abort status={} body={}",
            status, text
        )));
    }
    let _ = response
        .json::<InternalShardAbortResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!(
                "decode remote shard abort response failed: {}",
                e
            ))
        })?;
    Ok(())
}

async fn append_coordinator_shard_data(
    client: &reqwest::Client,
    session: &mut CoordinatorShardSession,
    bytes: &[u8],
    remote_flush_threshold: usize,
) -> Result<(), AppError> {
    if bytes.is_empty() {
        return Ok(());
    }
    match session {
        CoordinatorShardSession::Local(local) => {
            if local.failed.is_some() {
                return Ok(());
            }
            let mut file = OpenOptions::new()
                .append(true)
                .open(&local.temp_file_path)
                .await
                .map_err(|e| AppError::Io(e, format!("open {:?}", local.temp_file_path)))?;
            file.write_all(bytes)
                .await
                .map_err(|e| AppError::Io(e, "append local coordinator shard".to_string()))?;
            local.checksum.update(bytes);
            local.size_bytes = local.size_bytes.saturating_add(bytes.len() as u64);
        }
        CoordinatorShardSession::Remote(remote) => {
            if remote.failed.is_some() {
                return Ok(());
            }
            remote.pending.extend_from_slice(bytes);
            remote.checksum.update(bytes);
            remote.size_bytes = remote.size_bytes.saturating_add(bytes.len() as u64);
            if remote.pending.len() >= remote_flush_threshold {
                let chunk = std::mem::take(&mut remote.pending);
                append_remote_cluster_shard_chunk(
                    client,
                    &remote.target,
                    &remote.session_id,
                    chunk,
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn flush_coordinator_shard_data(
    client: &reqwest::Client,
    session: &mut CoordinatorShardSession,
) -> Result<(), AppError> {
    match session {
        CoordinatorShardSession::Local(_) => Ok(()),
        CoordinatorShardSession::Remote(remote) => {
            if remote.pending.is_empty() {
                return Ok(());
            }
            let chunk = std::mem::take(&mut remote.pending);
            append_remote_cluster_shard_chunk(client, &remote.target, &remote.session_id, chunk)
                .await
        }
    }
}

async fn abort_coordinator_shard_session(
    client: &reqwest::Client,
    session: &mut CoordinatorShardSession,
) -> Result<(), AppError> {
    match session {
        CoordinatorShardSession::Local(local) => {
            let _ = fs::remove_dir_all(&local.temp_root).await;
            Ok(())
        }
        CoordinatorShardSession::Remote(remote) => {
            let _ = remote.pending.clear();
            abort_remote_cluster_shard_session(client, &remote.target, remote.session_id.as_str())
                .await
        }
    }
}

async fn commit_coordinator_shard_session(
    client: &reqwest::Client,
    session: &mut CoordinatorShardSession,
    object: &ObjectIdentity,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
) -> Result<N2ClusterShardRef, AppError> {
    match session {
        CoordinatorShardSession::Local(local) => {
            let stored = store_local_cluster_shard_from_file(
                local.temp_file_path.as_path(),
                object,
                local.cluster_shard_index,
                kd,
                md,
                stripe_block_size,
            )
            .await;
            let _ = fs::remove_dir_all(&local.temp_root).await;
            stored
        }
        CoordinatorShardSession::Remote(remote) => {
            if !remote.pending.is_empty() {
                let chunk = std::mem::take(&mut remote.pending);
                append_remote_cluster_shard_chunk(
                    client,
                    &remote.target,
                    &remote.session_id,
                    chunk,
                )
                .await?;
            }
            commit_remote_cluster_shard_session(client, &remote.target, remote.session_id.as_str())
                .await
        }
    }
}

async fn load_local_cluster_shard_meta(
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<LocalClusterShardMeta, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_blob_roots();
    for root in roots {
        let meta_path = root
            .join(fragment)
            .join("n2_data")
            .join("blobs")
            .join(version_id)
            .join(format!("cshard_{}", cluster_shard_index))
            .join(N2_LOCAL_CSHARD_META_FILE);
        let exists = fs::try_exists(&meta_path)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", meta_path)))?;
        if !exists {
            continue;
        }
        let bytes = fs::read(&meta_path)
            .await
            .map_err(|e| AppError::Io(e, format!("read {:?}", meta_path)))?;
        let meta = rmp_serde::from_slice::<LocalClusterShardMeta>(&bytes).map_err(|e| {
            AppError::InternalServerError(format!("decode local cshard meta failed: {}", e))
        })?;
        return Ok(meta);
    }

    Err(AppError::NotFound)
}

async fn read_local_cluster_shard(
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<Vec<u8>, AppError> {
    let meta = load_local_cluster_shard_meta(fragment, version_id, cluster_shard_index).await?;
    let kd = usize::from(meta.kd);
    let md = usize::from(meta.md);
    if kd == 0 {
        return Err(AppError::InternalServerError(
            "local cshard meta has invalid kd".to_string(),
        ));
    }
    if meta.cluster_size_bytes == 0 {
        return Ok(Vec::new());
    }
    let stripe_count = if meta.disk_shard_size_bytes == 0 {
        0
    } else {
        (meta.disk_shard_size_bytes / meta.stripe_block_size as u64) as usize
    };

    let mut files = HashMap::<usize, fs::File>::new();
    for shard in &meta.disk_shards {
        if let Ok(file) = fs::File::open(shard.absolute_path.as_str()).await {
            files.insert(shard.disk_shard_index, file);
        }
    }

    let mut out = Vec::with_capacity((stripe_count * kd * meta.stripe_block_size).max(1024));
    if md == 0 {
        for stripe_idx in 0..stripe_count {
            let offset = stripe_idx as u64 * meta.stripe_block_size as u64;
            for disk_shard_index in 0..kd {
                let file = files.get_mut(&disk_shard_index).ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "missing required data disk shard {} for cshard read",
                        disk_shard_index
                    ))
                })?;
                file.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(|e| AppError::Io(e, "seek local data disk shard".to_string()))?;
                let mut block = vec![0u8; meta.stripe_block_size];
                file.read_exact(&mut block)
                    .await
                    .map_err(|e| AppError::Io(e, "read local data disk shard".to_string()))?;
                out.extend_from_slice(&block);
            }
        }
        out.truncate(meta.cluster_size_bytes as usize);
        return Ok(out);
    }

    for stripe_idx in 0..stripe_count {
        let offset = stripe_idx as u64 * meta.stripe_block_size as u64;
        let mut decoder = ReedSolomonDecoder::new(kd, md, meta.stripe_block_size)
            .map_err(|e| AppError::InternalServerError(format!("local RS decode init: {}", e)))?;

        let mut originals: Vec<Option<Vec<u8>>> = vec![None; kd];
        let mut added = 0usize;

        for shard in &meta.disk_shards {
            let Some(file) = files.get_mut(&shard.disk_shard_index) else {
                continue;
            };
            if file.seek(SeekFrom::Start(offset)).await.is_err() {
                continue;
            }
            let mut block = vec![0u8; meta.stripe_block_size];
            if file.read_exact(&mut block).await.is_err() {
                continue;
            }

            let add_res = if shard.disk_shard_index < kd {
                originals[shard.disk_shard_index] = Some(block.clone());
                decoder.add_original_shard(shard.disk_shard_index, &block)
            } else {
                decoder.add_recovery_shard(shard.disk_shard_index - kd, &block)
            };
            if add_res.is_ok() {
                added = added.saturating_add(1);
            }
        }

        if added < kd {
            return Err(AppError::InternalServerError(format!(
                "insufficient disk shards to rebuild cshard (need {}, have {})",
                kd, added
            )));
        }

        let decoded = decoder
            .decode()
            .map_err(|e| AppError::InternalServerError(format!("local RS decode: {}", e)))?;
        for (idx, data) in decoded.restored_original_iter() {
            if idx < kd {
                originals[idx] = Some(data.to_vec());
            }
        }

        for maybe in originals {
            let block = maybe.ok_or_else(|| {
                AppError::InternalServerError("missing restored original block".to_string())
            })?;
            out.extend_from_slice(&block);
        }
    }

    out.truncate(meta.cluster_size_bytes as usize);
    Ok(out)
}

async fn read_local_cluster_shard_to_file(
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
    destination_path: &StdPath,
) -> Result<usize, AppError> {
    let meta = load_local_cluster_shard_meta(fragment, version_id, cluster_shard_index).await?;
    let kd = usize::from(meta.kd);
    let md = usize::from(meta.md);
    if kd == 0 {
        return Err(AppError::InternalServerError(
            "local cshard meta has invalid kd".to_string(),
        ));
    }

    if let Some(parent) = destination_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::Io(e, format!("mkdir {:?}", parent)))?;
    }
    let mut destination_file = fs::File::create(destination_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {:?}", destination_path)))?;

    if meta.cluster_size_bytes == 0 {
        destination_file
            .flush()
            .await
            .map_err(|e| AppError::Io(e, "flush local cshard destination".to_string()))?;
        return Ok(0);
    }

    let stripe_count = if meta.disk_shard_size_bytes == 0 {
        0
    } else {
        (meta.disk_shard_size_bytes / meta.stripe_block_size as u64) as usize
    };

    let mut files = HashMap::<usize, fs::File>::new();
    for shard in &meta.disk_shards {
        if let Ok(file) = fs::File::open(shard.absolute_path.as_str()).await {
            files.insert(shard.disk_shard_index, file);
        }
    }

    if md == 0 {
        for stripe_idx in 0..stripe_count {
            let offset = stripe_idx as u64 * meta.stripe_block_size as u64;
            for disk_shard_index in 0..kd {
                let file = files.get_mut(&disk_shard_index).ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "missing required data disk shard {} for cshard read",
                        disk_shard_index
                    ))
                })?;
                file.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(|e| AppError::Io(e, "seek local data disk shard".to_string()))?;
                let mut block = vec![0u8; meta.stripe_block_size];
                file.read_exact(&mut block)
                    .await
                    .map_err(|e| AppError::Io(e, "read local data disk shard".to_string()))?;
                destination_file
                    .write_all(&block)
                    .await
                    .map_err(|e| AppError::Io(e, "write local cshard destination".to_string()))?;
            }
        }
    } else {
        for stripe_idx in 0..stripe_count {
            let offset = stripe_idx as u64 * meta.stripe_block_size as u64;
            let mut decoder =
                ReedSolomonDecoder::new(kd, md, meta.stripe_block_size).map_err(|e| {
                    AppError::InternalServerError(format!("local RS decode init: {}", e))
                })?;

            let mut originals: Vec<Option<Vec<u8>>> = vec![None; kd];
            let mut added = 0usize;

            for shard in &meta.disk_shards {
                let Some(file) = files.get_mut(&shard.disk_shard_index) else {
                    continue;
                };
                if file.seek(SeekFrom::Start(offset)).await.is_err() {
                    continue;
                }
                let mut block = vec![0u8; meta.stripe_block_size];
                if file.read_exact(&mut block).await.is_err() {
                    continue;
                }

                let add_res = if shard.disk_shard_index < kd {
                    originals[shard.disk_shard_index] = Some(block.clone());
                    decoder.add_original_shard(shard.disk_shard_index, &block)
                } else {
                    decoder.add_recovery_shard(shard.disk_shard_index - kd, &block)
                };
                if add_res.is_ok() {
                    added = added.saturating_add(1);
                }
            }

            if added < kd {
                return Err(AppError::InternalServerError(format!(
                    "insufficient disk shards to rebuild cshard (need {}, have {})",
                    kd, added
                )));
            }

            let decoded = decoder
                .decode()
                .map_err(|e| AppError::InternalServerError(format!("local RS decode: {}", e)))?;
            for (idx, data) in decoded.restored_original_iter() {
                if idx < kd {
                    originals[idx] = Some(data.to_vec());
                }
            }

            for maybe in originals {
                let block = maybe.ok_or_else(|| {
                    AppError::InternalServerError("missing restored original block".to_string())
                })?;
                destination_file
                    .write_all(block.as_slice())
                    .await
                    .map_err(|e| AppError::Io(e, "write local cshard destination".to_string()))?;
            }
        }
    }

    destination_file
        .set_len(meta.cluster_size_bytes)
        .await
        .map_err(|e| AppError::Io(e, "truncate local cshard destination".to_string()))?;
    destination_file
        .flush()
        .await
        .map_err(|e| AppError::Io(e, "flush local cshard destination".to_string()))?;
    Ok(meta.cluster_size_bytes as usize)
}

async fn delete_local_cluster_shard(
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let mut deleted_paths = 0usize;
    for root in blob_roots {
        let cshard_dir = root
            .join(fragment)
            .join("n2_data")
            .join("blobs")
            .join(version_id)
            .join(format!("cshard_{}", cluster_shard_index));
        let exists = fs::try_exists(&cshard_dir)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", cshard_dir)))?;
        if !exists {
            continue;
        }
        fs::remove_dir_all(&cshard_dir)
            .await
            .map_err(|e| AppError::Io(e, format!("remove {:?}", cshard_dir)))?;
        deleted_paths = deleted_paths.saturating_add(1);

        if let Some(version_dir) = cshard_dir.parent() {
            if let Ok(mut entries) = fs::read_dir(version_dir).await {
                let mut has_children = false;
                while let Ok(Some(_)) = entries.next_entry().await {
                    has_children = true;
                    break;
                }
                if !has_children {
                    let _ = fs::remove_dir(version_dir).await;
                }
            }
        }
    }
    Ok(deleted_paths)
}

async fn store_remote_cluster_shard_bytes(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
    bytes: Vec<u8>,
) -> Result<N2ClusterShardRef, AppError> {
    store_remote_cluster_shard_with_body(
        client,
        target,
        object,
        cluster_shard_index,
        kd,
        md,
        stripe_block_size,
        ReqwestBody::from(bytes),
    )
    .await
}

async fn store_remote_cluster_shard_with_body(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
    body: ReqwestBody,
) -> Result<N2ClusterShardRef, AppError> {
    let base = target.endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/store/{}/{}/{}",
        base, object.fragment, object.version_id, cluster_shard_index
    );

    let query = InternalStoreShardRemoteQuery {
        uid: object.uid.as_str(),
        object_key: object.object_key.as_str(),
        kd,
        md,
        stripe_block_size,
    };

    let response = client
        .post(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .query(&query)
        .body(body)
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote store shard request failed: {}", e)))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote store shard status={} body={}",
            status, text
        )));
    }

    response.json::<N2ClusterShardRef>().await.map_err(|e| {
        AppError::InternalServerError(format!("decode remote store shard response failed: {}", e))
    })
}

async fn fetch_remote_cluster_shard(
    client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<Vec<u8>, AppError> {
    let base = endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/read/{}/{}/{}",
        base, fragment, version_id, cluster_shard_index
    );
    let resp = client
        .get(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote read shard request failed: {}", e)))?;
    if !resp.status().is_success() {
        return Err(AppError::InternalServerError(format!(
            "remote read shard returned status {}",
            resp.status()
        )));
    }
    resp.bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| AppError::InternalServerError(format!("read remote shard body failed: {}", e)))
}

async fn fetch_remote_cluster_shard_to_file(
    client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
    destination_path: &StdPath,
) -> Result<usize, AppError> {
    let base = endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/read/{}/{}/{}",
        base, fragment, version_id, cluster_shard_index
    );
    let response = client
        .get(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote read shard request failed: {}", e)))?;
    if !response.status().is_success() {
        return Err(AppError::InternalServerError(format!(
            "remote read shard returned status {}",
            response.status()
        )));
    }

    if let Some(parent) = destination_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::Io(e, format!("mkdir {:?}", parent)))?;
    }
    let mut destination_file = fs::File::create(destination_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {:?}", destination_path)))?;
    let mut written = 0usize;
    let mut stream = response.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::InternalServerError(format!("read remote shard body chunk failed: {}", e))
        })?;
        if chunk.is_empty() {
            continue;
        }
        destination_file
            .write_all(&chunk)
            .await
            .map_err(|e| AppError::Io(e, "write remote staged shard".to_string()))?;
        written = written.saturating_add(chunk.len());
    }
    destination_file
        .flush()
        .await
        .map_err(|e| AppError::Io(e, "flush remote staged shard".to_string()))?;
    Ok(written)
}

async fn delete_remote_cluster_shard(
    client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<usize, AppError> {
    let base = endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/delete/{}/{}/{}",
        base, fragment, version_id, cluster_shard_index
    );
    let response = client
        .delete(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote delete shard request failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote delete shard status={} body={}",
            status, body
        )));
    }
    let parsed = response
        .json::<InternalShardDeleteResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!(
                "decode remote delete shard response failed: {}",
                e
            ))
        })?;
    Ok(parsed.deleted_paths)
}

#[cfg(test)]
fn reconstruct_object_from_cluster_shards(
    kc: usize,
    mc: usize,
    stripe_block_size: usize,
    total_size_bytes: u64,
    mut available: Vec<(usize, Vec<u8>)>,
) -> Result<Vec<u8>, AppError> {
    if kc == 0 {
        return Err(AppError::InternalServerError("invalid kc=0".to_string()));
    }
    if available.len() < kc {
        return Err(AppError::InternalServerError(format!(
            "insufficient cluster shards for reconstruction (need {}, have {})",
            kc,
            available.len()
        )));
    }
    available.sort_by_key(|(idx, _)| *idx);

    let max_shard_size = available.iter().map(|(_, b)| b.len()).max().unwrap_or(0);
    if max_shard_size == 0 {
        return Ok(Vec::new());
    }
    let stripe_count = max_shard_size.div_ceil(stripe_block_size);
    let mut out = Vec::with_capacity((stripe_count * kc * stripe_block_size).max(1024));

    if mc == 0 {
        let mut data_shards = HashMap::<usize, Vec<u8>>::new();
        for (idx, bytes) in available {
            if idx < kc {
                data_shards.entry(idx).or_insert(bytes);
            }
        }
        if data_shards.len() < kc {
            return Err(AppError::InternalServerError(format!(
                "insufficient data shards without parity (need {}, have {})",
                kc,
                data_shards.len()
            )));
        }
        for stripe_idx in 0..stripe_count {
            let start = stripe_idx * stripe_block_size;
            for data_idx in 0..kc {
                let shard_bytes = data_shards.get(&data_idx).ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "missing data shard {} for no-parity reconstruction",
                        data_idx
                    ))
                })?;
                let mut block = vec![0u8; stripe_block_size];
                if start < shard_bytes.len() {
                    let copy_len = (shard_bytes.len() - start).min(stripe_block_size);
                    block[..copy_len].copy_from_slice(&shard_bytes[start..start + copy_len]);
                }
                out.extend_from_slice(&block);
            }
        }
        out.truncate(total_size_bytes as usize);
        return Ok(out);
    }

    for stripe_idx in 0..stripe_count {
        let mut decoder = ReedSolomonDecoder::new(kc, mc, stripe_block_size).map_err(|e| {
            AppError::InternalServerError(format!("reconstruct decoder init failed: {}", e))
        })?;
        let mut originals: Vec<Option<Vec<u8>>> = vec![None; kc];
        let mut added = 0usize;

        for (cluster_idx, shard_bytes) in &available {
            let start = stripe_idx * stripe_block_size;
            let end = start + stripe_block_size;
            if end > shard_bytes.len() {
                continue;
            }
            let block = &shard_bytes[start..end];

            let add_res = if *cluster_idx < kc {
                originals[*cluster_idx] = Some(block.to_vec());
                decoder.add_original_shard(*cluster_idx, block)
            } else {
                decoder.add_recovery_shard(*cluster_idx - kc, block)
            };
            if add_res.is_ok() {
                added = added.saturating_add(1);
            }
            if added >= kc {
                // no-op; keep adding can still help but not required
            }
        }

        if added < kc {
            return Err(AppError::InternalServerError(format!(
                "insufficient cluster blocks for stripe {} (need {}, have {})",
                stripe_idx, kc, added
            )));
        }

        let decoded = decoder.decode().map_err(|e| {
            AppError::InternalServerError(format!("reconstruct decode failed: {}", e))
        })?;
        for (idx, data) in decoded.restored_original_iter() {
            if idx < kc {
                originals[idx] = Some(data.to_vec());
            }
        }

        for maybe in originals {
            let block = maybe.ok_or_else(|| {
                AppError::InternalServerError(
                    "missing reconstructed original data block".to_string(),
                )
            })?;
            out.extend_from_slice(&block);
        }
    }

    out.truncate(total_size_bytes as usize);
    Ok(out)
}

fn parse_single_byte_range(
    headers: &HeaderMap,
    total_size_bytes: u64,
) -> Result<Option<(u64, u64)>, AppError> {
    let Some(raw) = headers.get(RANGE).and_then(|v| v.to_str().ok()) else {
        return Ok(None);
    };
    let range = raw.trim();
    if !range.starts_with("bytes=") {
        return Err(AppError::RangeNotSatisfiable);
    }
    let spec = &range["bytes=".len()..];
    if spec.contains(',') {
        return Err(AppError::RangeNotSatisfiable);
    }
    let Some((start_raw, end_raw)) = spec.split_once('-') else {
        return Err(AppError::RangeNotSatisfiable);
    };
    if total_size_bytes == 0 {
        return Err(AppError::RangeNotSatisfiable);
    }

    let (start, end) = if start_raw.is_empty() {
        // Suffix range: bytes=-N
        let suffix = end_raw
            .parse::<u64>()
            .map_err(|_| AppError::RangeNotSatisfiable)?;
        if suffix == 0 {
            return Err(AppError::RangeNotSatisfiable);
        }
        let len = suffix.min(total_size_bytes);
        let start = total_size_bytes.saturating_sub(len);
        (start, total_size_bytes - 1)
    } else {
        let start = start_raw
            .parse::<u64>()
            .map_err(|_| AppError::RangeNotSatisfiable)?;
        let end = if end_raw.is_empty() {
            total_size_bytes - 1
        } else {
            end_raw
                .parse::<u64>()
                .map_err(|_| AppError::RangeNotSatisfiable)?
        };
        (start, end.min(total_size_bytes - 1))
    };

    if start > end || start >= total_size_bytes {
        return Err(AppError::RangeNotSatisfiable);
    }

    Ok(Some((start, end)))
}

fn reconstructed_stream_error(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, message)
}

async fn send_reconstructed_stream_err(
    tx: &mpsc::Sender<Result<Bytes, std::io::Error>>,
    err: AppError,
) {
    let _ = tx
        .send(Err(reconstructed_stream_error(err.to_string())))
        .await;
}

async fn read_cluster_shard_file_block(
    file: &mut fs::File,
    shard_size: usize,
    stripe_idx: usize,
    stripe_block_size: usize,
) -> Result<Vec<u8>, AppError> {
    let offset = stripe_idx.saturating_mul(stripe_block_size);
    let mut block = vec![0u8; stripe_block_size];
    if offset >= shard_size {
        return Ok(block);
    }
    let copy_len = (shard_size - offset).min(stripe_block_size);
    file.seek(SeekFrom::Start(offset as u64))
        .await
        .map_err(|e| AppError::Io(e, "seek staged cluster shard file".to_string()))?;
    file.read_exact(&mut block[..copy_len])
        .await
        .map_err(|e| AppError::Io(e, "read staged cluster shard file".to_string()))?;
    Ok(block)
}

fn reconstruct_object_stream_from_cluster_shard_files(
    kc: usize,
    mc: usize,
    stripe_block_size: usize,
    total_size_bytes: u64,
    mut sources: Vec<ClusterShardFileSource>,
    range: Option<(u64, u64)>,
    staged_root: PathBuf,
) -> ReceiverStream<Result<Bytes, std::io::Error>> {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(8);
    tokio::spawn(async move {
        let run = async {
            if kc == 0 {
                return Err(AppError::InternalServerError("invalid kc=0".to_string()));
            }
            if sources.len() < kc {
                return Err(AppError::InternalServerError(format!(
                    "insufficient cluster shards for reconstruction (need {}, have {})",
                    kc,
                    sources.len()
                )));
            }
            sources.sort_by_key(|s| s.cluster_shard_index);
            let mut seen_indices = HashSet::new();
            sources.retain(|s| seen_indices.insert(s.cluster_shard_index));

            let mut staged_files = Vec::<(usize, usize, fs::File)>::new();
            for source in sources {
                let file = fs::File::open(source.path.as_path())
                    .await
                    .map_err(|e| AppError::Io(e, format!("open {:?}", source.path)))?;
                staged_files.push((source.cluster_shard_index, source.size_bytes, file));
            }
            if staged_files.len() < kc {
                return Err(AppError::InternalServerError(format!(
                    "insufficient opened shard files for reconstruction (need {}, have {})",
                    kc,
                    staged_files.len()
                )));
            }

            let max_shard_size = staged_files
                .iter()
                .map(|(_, size, _)| *size)
                .max()
                .unwrap_or(0);
            if max_shard_size == 0 || total_size_bytes == 0 {
                return Ok(());
            }
            let stripe_count = max_shard_size.div_ceil(stripe_block_size);
            let (target_start, target_end) =
                range.unwrap_or((0, total_size_bytes.saturating_sub(1)));
            let mut object_cursor = 0u64;

            for stripe_idx in 0..stripe_count {
                let stripe_result = if mc == 0 {
                    let mut data_blocks = HashMap::<usize, Vec<u8>>::new();
                    for (cluster_idx, size_bytes, file) in &mut staged_files {
                        if *cluster_idx >= kc || data_blocks.contains_key(cluster_idx) {
                            continue;
                        }
                        let block = read_cluster_shard_file_block(
                            file,
                            *size_bytes,
                            stripe_idx,
                            stripe_block_size,
                        )
                        .await?;
                        data_blocks.insert(*cluster_idx, block);
                    }
                    if data_blocks.len() < kc {
                        Err(AppError::InternalServerError(format!(
                            "insufficient data shards without parity (need {}, have {})",
                            kc,
                            data_blocks.len()
                        )))
                    } else {
                        let mut stripe = Vec::with_capacity(kc * stripe_block_size);
                        for data_idx in 0..kc {
                            let block = data_blocks.remove(&data_idx).ok_or_else(|| {
                                AppError::InternalServerError(format!(
                                    "missing data shard {} for no-parity reconstruction",
                                    data_idx
                                ))
                            })?;
                            stripe.extend_from_slice(block.as_slice());
                        }
                        Ok(stripe)
                    }
                } else {
                    let mut decoder =
                        ReedSolomonDecoder::new(kc, mc, stripe_block_size).map_err(|e| {
                            AppError::InternalServerError(format!(
                                "reconstruct decoder init failed: {}",
                                e
                            ))
                        })?;
                    let mut originals: Vec<Option<Vec<u8>>> = vec![None; kc];
                    let mut added = 0usize;

                    for (cluster_idx, size_bytes, file) in &mut staged_files {
                        let block = read_cluster_shard_file_block(
                            file,
                            *size_bytes,
                            stripe_idx,
                            stripe_block_size,
                        )
                        .await?;
                        let add_res = if *cluster_idx < kc {
                            originals[*cluster_idx] = Some(block.clone());
                            decoder.add_original_shard(*cluster_idx, &block)
                        } else {
                            decoder.add_recovery_shard(*cluster_idx - kc, &block)
                        };
                        if add_res.is_ok() {
                            added = added.saturating_add(1);
                        }
                    }

                    if added < kc {
                        Err(AppError::InternalServerError(format!(
                            "insufficient shards for stripe {} (need {}, have {})",
                            stripe_idx, kc, added
                        )))
                    } else {
                        let decoded = decoder.decode().map_err(|e| {
                            AppError::InternalServerError(format!(
                                "reconstruct decode failed: {}",
                                e
                            ))
                        })?;
                        for (idx, data) in decoded.restored_original_iter() {
                            if idx < kc {
                                originals[idx] = Some(data.to_vec());
                            }
                        }
                        let mut stripe = Vec::with_capacity(kc * stripe_block_size);
                        for maybe in originals {
                            let block = maybe.ok_or_else(|| {
                                AppError::InternalServerError(
                                    "missing restored original block".to_string(),
                                )
                            })?;
                            stripe.extend_from_slice(block.as_slice());
                        }
                        Ok(stripe)
                    }
                };

                let stripe = stripe_result?;
                let mut stripe_len = stripe.len() as u64;
                if object_cursor + stripe_len > total_size_bytes {
                    stripe_len = total_size_bytes.saturating_sub(object_cursor);
                }
                if stripe_len == 0 {
                    break;
                }

                let stripe_start = object_cursor;
                let stripe_end = object_cursor + stripe_len - 1;
                object_cursor = object_cursor.saturating_add(stripe_len);

                if stripe_end < target_start || stripe_start > target_end {
                    if stripe_start > target_end {
                        break;
                    }
                    continue;
                }

                let emit_start = target_start.max(stripe_start) - stripe_start;
                let emit_end_exclusive = (target_end.min(stripe_end) - stripe_start + 1) as usize;
                let emit_start_usize = emit_start as usize;
                let payload = Bytes::copy_from_slice(&stripe[emit_start_usize..emit_end_exclusive]);
                if tx.send(Ok(payload)).await.is_err() {
                    return Ok(());
                }

                if stripe_end >= target_end {
                    break;
                }
            }
            Ok::<(), AppError>(())
        }
        .await;

        if let Err(err) = run {
            send_reconstructed_stream_err(&tx, err).await;
        }
        let _ = fs::remove_dir_all(&staged_root).await;
    });
    ReceiverStream::new(rx)
}

fn reconstruct_cluster_shard_from_available(
    kc: usize,
    mc: usize,
    stripe_block_size: usize,
    target_cluster_shard_index: usize,
    available: &HashMap<usize, Vec<u8>>,
) -> Result<Vec<u8>, AppError> {
    if kc == 0 {
        return Err(AppError::InternalServerError("invalid kc=0".to_string()));
    }
    if available.len() < kc {
        return Err(AppError::InternalServerError(format!(
            "insufficient cluster shards for reconstruction (need {}, have {})",
            kc,
            available.len()
        )));
    }

    let max_shard_size = available.values().map(Vec::len).max().unwrap_or(0);
    if max_shard_size == 0 {
        return Ok(Vec::new());
    }
    if mc == 0 {
        if target_cluster_shard_index >= kc {
            return Err(AppError::InternalServerError(format!(
                "cannot reconstruct parity shard {} when mc=0",
                target_cluster_shard_index
            )));
        }
        let shard = available.get(&target_cluster_shard_index).ok_or_else(|| {
            AppError::InternalServerError(format!(
                "cannot reconstruct missing data shard {} without parity",
                target_cluster_shard_index
            ))
        })?;
        let mut out = shard.clone();
        out.resize(max_shard_size, 0);
        return Ok(out);
    }

    let stripe_count = max_shard_size.div_ceil(stripe_block_size);
    let mut out = Vec::with_capacity(max_shard_size);

    for stripe_idx in 0..stripe_count {
        let mut decoder = ReedSolomonDecoder::new(kc, mc, stripe_block_size).map_err(|e| {
            AppError::InternalServerError(format!("reconstruct shard decoder init failed: {}", e))
        })?;
        let mut originals: Vec<Option<Vec<u8>>> = vec![None; kc];
        let mut added = 0usize;

        for (cluster_idx, shard_bytes) in available {
            let start = stripe_idx * stripe_block_size;
            let mut block = vec![0u8; stripe_block_size];
            if start < shard_bytes.len() {
                let available_len = (shard_bytes.len() - start).min(stripe_block_size);
                block[..available_len].copy_from_slice(&shard_bytes[start..start + available_len]);
            }

            let add_res = if *cluster_idx < kc {
                originals[*cluster_idx] = Some(block.clone());
                decoder.add_original_shard(*cluster_idx, &block)
            } else {
                decoder.add_recovery_shard(*cluster_idx - kc, &block)
            };
            if add_res.is_ok() {
                added = added.saturating_add(1);
            }
        }

        if added < kc {
            return Err(AppError::InternalServerError(format!(
                "insufficient cluster blocks for stripe {} (need {}, have {})",
                stripe_idx, kc, added
            )));
        }

        let decoded = decoder.decode().map_err(|e| {
            AppError::InternalServerError(format!("reconstruct shard decode failed: {}", e))
        })?;
        for (idx, data) in decoded.restored_original_iter() {
            if idx < kc && originals[idx].is_none() {
                originals[idx] = Some(data.to_vec());
            }
        }

        let target_block = if target_cluster_shard_index < kc {
            originals[target_cluster_shard_index]
                .clone()
                .ok_or_else(|| {
                    AppError::InternalServerError(
                        "missing reconstructed original data block".to_string(),
                    )
                })?
        } else {
            let mut encoder = ReedSolomonEncoder::new(kc, mc, stripe_block_size).map_err(|e| {
                AppError::InternalServerError(format!("reconstruct parity init failed: {}", e))
            })?;
            for original in &originals {
                let block = original.as_ref().ok_or_else(|| {
                    AppError::InternalServerError(
                        "missing reconstructed original block for parity rebuild".to_string(),
                    )
                })?;
                encoder.add_original_shard(block).map_err(|e| {
                    AppError::InternalServerError(format!("reconstruct parity add failed: {}", e))
                })?;
            }
            let encoded = encoder.encode().map_err(|e| {
                AppError::InternalServerError(format!("reconstruct parity encode failed: {}", e))
            })?;
            encoded
                .recovery(target_cluster_shard_index - kc)
                .ok_or_else(|| {
                    AppError::InternalServerError(
                        "reconstructed parity shard index out of bounds".to_string(),
                    )
                })?
                .to_vec()
        };

        out.extend_from_slice(&target_block);
    }

    out.truncate(max_shard_size);
    Ok(out)
}

fn content_type_from_query_or_headers(query: &PutObjectQuery, headers: &HeaderMap) -> String {
    if let Some(v) = query.content_type.as_ref() {
        return v.to_string();
    }
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string()
}

pub async fn put_n2_object(
    claims: Claims,
    headers: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<PutObjectQuery>,
    body: Body,
) -> Result<AxumJson<N2PutObjectResponse>, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    let cluster_view = ensure_cluster_write_ready(query.expected_epoch).await?;

    let cfg = n2_runtime_config();
    let cross_params = effective_cross_params();
    let content_type = content_type_from_query_or_headers(&query, &headers);
    let kc = query.kc.unwrap_or(cross_params.kc);
    let mc = query.mc.unwrap_or(cross_params.mc);
    let kd = query.kd.unwrap_or(cross_params.kd);
    let md = query.md.unwrap_or(cross_params.md);
    let stripe_block_size = normalize_n2_stripe_block_size(
        query
            .stripe_block_size
            .unwrap_or(cfg.cluster_stripe_block_size),
    );

    if kc == 0 || kd == 0 {
        return Err(AppError::BadRequest(
            "kc and kd must be greater than 0".to_string(),
        ));
    }

    let uid = query.uid.unwrap_or_else(|| claims.fragment.clone());
    let version_id = query.version_id.unwrap_or_else(|| Ulid::new().to_string());
    let object = ObjectIdentity {
        uid,
        fragment: fragment.clone(),
        object_key: object_key.trim_start_matches('/').to_string(),
        version_id: version_id.clone(),
    };
    let placement = resolve_placement(object.clone(), kc, mc).await?;
    let nc = usize::from(kc).saturating_add(usize::from(mc));

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;

    let mut sessions = Vec::with_capacity(nc);
    for (idx, target) in placement.targets.iter().enumerate() {
        let result = if is_local_target(target) {
            begin_local_coordinator_shard_session(target.clone(), idx).await
        } else {
            begin_remote_cluster_shard_session(
                &http_client,
                target,
                &object,
                idx,
                kd,
                md,
                stripe_block_size,
            )
            .await
            .map(|session_id| {
                CoordinatorShardSession::Remote(RemoteCoordinatorShardSession {
                    target: target.clone(),
                    cluster_shard_index: idx,
                    session_id,
                    pending: Vec::new(),
                    checksum: Sha256::new(),
                    size_bytes: 0,
                    failed: None,
                })
            })
        };

        match result {
            Ok(session) => {
                sessions.push(session);
            }
            Err(e) => {
                warn!(
                    "n2 shard session begin failed fragment={} object_key={} version={} shard={} target_node={} target_endpoint={} err={}",
                    object.fragment,
                    object.object_key,
                    object.version_id,
                    idx,
                    target.node_id,
                    target.endpoint,
                    e
                );
            }
        }
    }

    if sessions.iter().filter(|s| !s.is_failed()).count() < usize::from(kc) {
        for session in &mut sessions {
            let _ = abort_coordinator_shard_session(&http_client, session).await;
        }
        return Err(AppError::Conflict(format!(
            "quorum not reached before ingest: required {}, ready {}",
            kc,
            sessions.iter().filter(|s| !s.is_failed()).count()
        )));
    }

    let stripe_width = stripe_block_size.saturating_mul(usize::from(kc));
    let mut total_size_bytes = 0u64;
    let mut pending = BytesMut::with_capacity(stripe_width.max(stripe_block_size));
    let remote_flush_threshold =
        N2_REMOTE_CHUNK_FLUSH_MIN_BYTES.max(stripe_block_size.saturating_mul(4));

    let mut body_stream = body.into_data_stream();
    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read n2 upload stream".to_string(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        total_size_bytes = total_size_bytes.saturating_add(chunk.len() as u64);
        pending.extend_from_slice(&chunk);

        while pending.len() >= stripe_width {
            let stripe = pending.split_to(stripe_width);
            let shard_blocks = encode_cluster_stripe_blocks(
                stripe.as_ref(),
                usize::from(kc),
                usize::from(mc),
                stripe_block_size,
            )?;
            for session in &mut sessions {
                if session.is_failed() {
                    continue;
                }
                let cluster_idx = session.cluster_shard_index();
                let Some(block) = shard_blocks.get(cluster_idx) else {
                    session.mark_failed("missing encoded shard block".to_string());
                    continue;
                };
                if let Err(e) = append_coordinator_shard_data(
                    &http_client,
                    session,
                    block,
                    remote_flush_threshold,
                )
                .await
                {
                    warn!(
                        "n2 shard append failed fragment={} object_key={} version={} shard={} target_node={} err={}",
                        object.fragment,
                        object.object_key,
                        object.version_id,
                        session.cluster_shard_index(),
                        session.target().node_id,
                        e
                    );
                    session.mark_failed(e.to_string());
                }
            }
            let active_sessions = sessions.iter().filter(|s| !s.is_failed()).count();
            if active_sessions < usize::from(kc) {
                for session in &mut sessions {
                    let _ = abort_coordinator_shard_session(&http_client, session).await;
                }
                return Err(AppError::Conflict(format!(
                    "quorum dropped during ingest: required {}, active {}",
                    kc, active_sessions
                )));
            }
        }
    }

    if !pending.is_empty() {
        let mut stripe = vec![0u8; stripe_width];
        stripe[..pending.len()].copy_from_slice(&pending);
        let shard_blocks = encode_cluster_stripe_blocks(
            stripe.as_slice(),
            usize::from(kc),
            usize::from(mc),
            stripe_block_size,
        )?;
        for session in &mut sessions {
            if session.is_failed() {
                continue;
            }
            let cluster_idx = session.cluster_shard_index();
            let Some(block) = shard_blocks.get(cluster_idx) else {
                session.mark_failed("missing encoded shard block".to_string());
                continue;
            };
            if let Err(e) =
                append_coordinator_shard_data(&http_client, session, block, remote_flush_threshold)
                    .await
            {
                warn!(
                    "n2 shard append failed fragment={} object_key={} version={} shard={} target_node={} err={}",
                    object.fragment,
                    object.object_key,
                    object.version_id,
                    session.cluster_shard_index(),
                    session.target().node_id,
                    e
                );
                session.mark_failed(e.to_string());
            }
        }
    }

    let mut committed = Vec::new();
    for session in &mut sessions {
        if session.is_failed() {
            let _ = abort_coordinator_shard_session(&http_client, session).await;
            continue;
        }
        if let Err(e) = flush_coordinator_shard_data(&http_client, session).await {
            warn!(
                "n2 shard flush failed fragment={} object_key={} version={} shard={} target_node={} err={}",
                object.fragment,
                object.object_key,
                object.version_id,
                session.cluster_shard_index(),
                session.target().node_id,
                e
            );
            session.mark_failed(e.to_string());
            let _ = abort_coordinator_shard_session(&http_client, session).await;
            continue;
        }

        match commit_coordinator_shard_session(
            &http_client,
            session,
            &object,
            kd,
            md,
            stripe_block_size,
        )
        .await
        {
            Ok(mut shard_ref) => {
                // Keep canonical checksum/size from streamed coordinator view when missing.
                match session {
                    CoordinatorShardSession::Local(v) => {
                        if shard_ref.size_bytes == 0 {
                            shard_ref.size_bytes = v.size_bytes;
                        }
                        if shard_ref.checksum.is_empty() {
                            shard_ref.checksum = hex::encode(v.checksum.clone().finalize());
                        }
                    }
                    CoordinatorShardSession::Remote(v) => {
                        if shard_ref.size_bytes == 0 {
                            shard_ref.size_bytes = v.size_bytes;
                        }
                        if shard_ref.checksum.is_empty() {
                            shard_ref.checksum = hex::encode(v.checksum.clone().finalize());
                        }
                    }
                }
                committed.push(shard_ref);
            }
            Err(e) => {
                warn!(
                    "n2 shard commit failed fragment={} object_key={} version={} shard={} target_node={} err={}",
                    object.fragment,
                    object.object_key,
                    object.version_id,
                    session.cluster_shard_index(),
                    session.target().node_id,
                    e
                );
                session.mark_failed(e.to_string());
                let _ = abort_coordinator_shard_session(&http_client, session).await;
            }
        }
    }

    if committed.len() < usize::from(kc) {
        return Err(AppError::Conflict(format!(
            "quorum not reached: required {}, committed {}",
            kc,
            committed.len()
        )));
    }

    committed.sort_by_key(|s| s.cluster_shard_index);

    let now = Utc::now().timestamp();
    let object_map = N2ObjectMap {
        epoch: placement.epoch,
        object: object.clone(),
        params: CrossParams { kc, mc, kd, md },
        stripe_block_size,
        total_size_bytes,
        content_type,
        write_quorum: kc,
        created_at: now,
        committed_at: now,
        shards: committed,
    };

    persist_n2_object_map(&object_map).await?;

    info!(
        "n2 put committed fragment={} object_key={} version={} size={} quorum={} committed={} membership_epoch={}",
        object.fragment,
        object.object_key,
        object.version_id,
        object_map.total_size_bytes,
        object_map.write_quorum,
        object_map.shards.len(),
        cluster_view.membership_epoch
    );

    Ok(AxumJson(N2PutObjectResponse {
        epoch: object_map.epoch,
        version_id,
        total_size_bytes: object_map.total_size_bytes,
        committed_shards: object_map.shards.len(),
        required_quorum: object_map.write_quorum,
        object: object_map,
    }))
}

pub async fn get_n2_object(
    claims: Claims,
    headers: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<GetObjectQuery>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    let map = load_n2_object_map(
        fragment.as_str(),
        object_key.as_str(),
        query.version_id.as_deref(),
    )
    .await?;

    let cfg = n2_runtime_config();
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;

    let kc = usize::from(map.params.kc);
    let mc = usize::from(map.params.mc);
    let staged_root = std::env::temp_dir().join(format!("n2-read-stage-{}", Ulid::new()));
    fs::create_dir_all(&staged_root)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {:?}", staged_root)))?;

    let mut staged_sources = Vec::new();
    for (source_idx, shard) in map.shards.iter().enumerate() {
        let staged_path = staged_root.join(format!(
            "cluster_shard_{}_{}.bin",
            shard.cluster_shard_index, source_idx
        ));
        let result = if is_local_target(&NodeDescriptor {
            node_id: shard.node_id.clone(),
            endpoint: shard.endpoint.clone(),
            epoch: map.epoch,
            active: true,
        }) {
            read_local_cluster_shard_to_file(
                map.object.fragment.as_str(),
                map.object.version_id.as_str(),
                shard.cluster_shard_index,
                staged_path.as_path(),
            )
            .await
        } else {
            fetch_remote_cluster_shard_to_file(
                &http_client,
                shard.endpoint.as_str(),
                map.object.fragment.as_str(),
                map.object.version_id.as_str(),
                shard.cluster_shard_index,
                staged_path.as_path(),
            )
            .await
        };

        match result {
            Ok(size_bytes) => staged_sources.push(ClusterShardFileSource {
                cluster_shard_index: shard.cluster_shard_index,
                path: staged_path,
                size_bytes,
            }),
            Err(e) => warn!(
                "n2 shard read failed fragment={} object_key={} version={} shard={} node={} endpoint={} err={}",
                map.object.fragment,
                map.object.object_key,
                map.object.version_id,
                shard.cluster_shard_index,
                shard.node_id,
                shard.endpoint,
                e
            ),
        }
    }

    if staged_sources.len() < kc {
        let _ = fs::remove_dir_all(&staged_root).await;
        return Err(AppError::InternalServerError(format!(
            "insufficient cluster shards available for read: need {}, have {}",
            kc,
            staged_sources.len()
        )));
    }

    let range = match parse_single_byte_range(&headers, map.total_size_bytes) {
        Ok(v) => v,
        Err(e) => {
            let _ = fs::remove_dir_all(&staged_root).await;
            return Err(e);
        }
    };
    let (status, content_len, content_range_value) = if let Some((start, end)) = range {
        (
            StatusCode::PARTIAL_CONTENT,
            end.saturating_sub(start).saturating_add(1),
            Some(format!("bytes {}-{}/{}", start, end, map.total_size_bytes)),
        )
    } else {
        (StatusCode::OK, map.total_size_bytes, None)
    };
    let stream = reconstruct_object_stream_from_cluster_shard_files(
        kc,
        mc,
        map.stripe_block_size,
        map.total_size_bytes,
        staged_sources,
        range,
        staged_root,
    );

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    let content_type_header = HeaderValue::from_str(map.content_type.as_str())
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, content_type_header);
    response
        .headers_mut()
        .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(v) = HeaderValue::from_str(content_len.to_string().as_str()) {
        response.headers_mut().insert(CONTENT_LENGTH, v);
    }
    if let Some(content_range_value) = content_range_value {
        if let Ok(v) = HeaderValue::from_str(content_range_value.as_str()) {
            response.headers_mut().insert(CONTENT_RANGE, v);
        }
    }
    Ok(response)
}

pub async fn list_n2_objects(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(query): Query<ListN2ObjectsQuery>,
) -> Result<AxumJson<N2ListObjectsResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    let limit = query.limit.unwrap_or(256).clamp(1, 10_000);
    let scan_limit = limit.saturating_mul(32).clamp(limit, 100_000);
    let maps = list_fragment_n2_maps(fragment.as_str(), scan_limit).await?;

    let mut latest_by_key: HashMap<String, N2ObjectMap> = HashMap::new();
    for map in maps {
        let key = map.object.object_key.clone();
        if let Some(current) = latest_by_key.get(&key) {
            let should_replace = map.committed_at > current.committed_at
                || (map.committed_at == current.committed_at
                    && map.object.version_id > current.object.version_id);
            if should_replace {
                latest_by_key.insert(key, map);
            }
        } else {
            latest_by_key.insert(key, map);
        }
    }

    let mut objects = latest_by_key
        .into_values()
        .map(|map| N2ObjectSummary {
            object_key: map.object.object_key,
            latest_version_id: map.object.version_id,
            epoch: map.epoch,
            total_size_bytes: map.total_size_bytes,
            content_type: map.content_type,
            committed_at: map.committed_at,
            shard_count: map.shards.len(),
        })
        .collect::<Vec<_>>();
    objects.sort_by(|a, b| a.object_key.cmp(&b.object_key));
    objects.truncate(limit);

    Ok(AxumJson(N2ListObjectsResponse {
        fragment,
        limit,
        objects,
    }))
}

pub async fn list_n2_object_versions(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<ListN2VersionsQuery>,
) -> Result<AxumJson<N2ListObjectVersionsResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    let key = object_key.trim().trim_matches('/').to_string();
    if key.is_empty() {
        return Err(AppError::BadRequest(
            "object_key cannot be empty".to_string(),
        ));
    }
    let limit = query.limit.unwrap_or(256).clamp(1, 10_000);
    let mut maps = list_n2_maps_for_object(fragment.as_str(), key.as_str(), 100_000).await?;
    maps.truncate(limit);

    let versions = maps
        .into_iter()
        .map(|map| N2ObjectVersionSummary {
            version_id: map.object.version_id,
            epoch: map.epoch,
            total_size_bytes: map.total_size_bytes,
            content_type: map.content_type,
            committed_at: map.committed_at,
            shard_count: map.shards.len(),
        })
        .collect::<Vec<_>>();

    Ok(AxumJson(N2ListObjectVersionsResponse {
        fragment,
        object_key: key,
        versions,
    }))
}

pub async fn delete_n2_object(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<DeleteN2ObjectQuery>,
) -> Result<AxumJson<N2DeleteObjectResponse>, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden(
            "Delete permission required".to_string(),
        ));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    ensure_cluster_write_ready(query.expected_epoch).await?;

    let key = object_key.trim().trim_matches('/').to_string();
    if key.is_empty() {
        return Err(AppError::BadRequest(
            "object_key cannot be empty".to_string(),
        ));
    }

    let mut maps = list_n2_maps_for_object(fragment.as_str(), key.as_str(), 100_000).await?;
    if maps.is_empty() {
        return Err(AppError::NotFound);
    }
    if let Some(version_id) = query.version_id.as_ref().map(|v| v.trim().to_string()) {
        maps.retain(|m| m.object.version_id == version_id);
        if maps.is_empty() {
            return Err(AppError::NotFound);
        }
    }

    let purge_shards = query.purge_shards.unwrap_or(true);
    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(&cfg.node_endpoint);
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;

    let mut deleted_map_files = 0usize;
    let mut attempted_shard_deletes = 0usize;
    let mut deleted_shard_paths = 0usize;
    let mut shard_delete_failures = 0usize;

    for map in &maps {
        deleted_map_files = deleted_map_files.saturating_add(
            delete_n2_object_map_local(
                map.object.fragment.as_str(),
                map.object.object_key.as_str(),
                map.object.version_id.as_str(),
            )
            .await?,
        );

        let mut remote_endpoints = map
            .shards
            .iter()
            .map(|s| s.endpoint.clone())
            .filter(|endpoint| normalize_endpoint(endpoint) != local_endpoint)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        remote_endpoints.sort();
        for endpoint in remote_endpoints {
            match delete_n2_object_map_remote(
                &http_client,
                endpoint.as_str(),
                map.object.fragment.as_str(),
                map.object.object_key.as_str(),
                map.object.version_id.as_str(),
            )
            .await
            {
                Ok(removed) => {
                    deleted_map_files = deleted_map_files.saturating_add(removed);
                }
                Err(err) => {
                    warn!(
                        "n2 remote map delete failed fragment={} object_key={} version={} endpoint={} err={}",
                        map.object.fragment,
                        map.object.object_key,
                        map.object.version_id,
                        endpoint,
                        err
                    );
                }
            }
        }
    }

    if purge_shards {
        for map in &maps {
            for shard in &map.shards {
                attempted_shard_deletes = attempted_shard_deletes.saturating_add(1);
                let target = NodeDescriptor {
                    node_id: shard.node_id.clone(),
                    endpoint: shard.endpoint.clone(),
                    epoch: map.epoch,
                    active: true,
                };
                let deleted = if is_local_target(&target) {
                    delete_local_cluster_shard(
                        map.object.fragment.as_str(),
                        map.object.version_id.as_str(),
                        shard.cluster_shard_index,
                    )
                    .await
                } else {
                    delete_remote_cluster_shard(
                        &http_client,
                        shard.endpoint.as_str(),
                        map.object.fragment.as_str(),
                        map.object.version_id.as_str(),
                        shard.cluster_shard_index,
                    )
                    .await
                };
                match deleted {
                    Ok(paths) => {
                        deleted_shard_paths = deleted_shard_paths.saturating_add(paths);
                    }
                    Err(err) => {
                        shard_delete_failures = shard_delete_failures.saturating_add(1);
                        warn!(
                            "n2 shard delete failed fragment={} object_key={} version={} shard={} node={} endpoint={} err={}",
                            map.object.fragment,
                            map.object.object_key,
                            map.object.version_id,
                            shard.cluster_shard_index,
                            shard.node_id,
                            shard.endpoint,
                            err
                        );
                    }
                }
            }
        }
    }

    Ok(AxumJson(N2DeleteObjectResponse {
        fragment,
        object_key: key,
        deleted_versions: maps.len(),
        deleted_map_files,
        attempted_shard_deletes,
        deleted_shard_paths,
        shard_delete_failures,
    }))
}

pub async fn rename_n2_object(
    claims: Claims,
    AxumJson(mut request): AxumJson<RenameN2ObjectRequest>,
) -> Result<AxumJson<N2RenameObjectResponse>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required".to_string(),
        ));
    }

    request.fragment = request.fragment.trim().to_string();
    request.source_object_key = request
        .source_object_key
        .trim()
        .trim_matches('/')
        .to_string();
    request.destination_object_key = request
        .destination_object_key
        .trim()
        .trim_matches('/')
        .to_string();

    if request.fragment.is_empty() {
        return Err(AppError::BadRequest("fragment cannot be empty".to_string()));
    }
    if request.source_object_key.is_empty() || request.destination_object_key.is_empty() {
        return Err(AppError::BadRequest(
            "source and destination object keys are required".to_string(),
        ));
    }
    if request.source_object_key == request.destination_object_key {
        return Err(AppError::BadRequest(
            "source and destination object keys must differ".to_string(),
        ));
    }

    if !is_admin_claim(&claims) && claims.fragment != request.fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    ensure_cluster_write_ready(request.expected_epoch).await?;

    let destination_exists = list_n2_maps_for_object(
        request.fragment.as_str(),
        request.destination_object_key.as_str(),
        1,
    )
    .await?;
    if !destination_exists.is_empty() {
        return Err(AppError::Conflict(format!(
            "destination object already exists: {}",
            request.destination_object_key
        )));
    }

    let source_maps = list_n2_maps_for_object(
        request.fragment.as_str(),
        request.source_object_key.as_str(),
        100_000,
    )
    .await?;
    if source_maps.is_empty() {
        return Err(AppError::NotFound);
    }

    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(&cfg.node_endpoint);
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;

    let mut deleted_old_map_files = 0usize;
    let mut moved_versions = 0usize;
    for mut map in source_maps {
        let old_object_key = map.object.object_key.clone();
        let old_version = map.object.version_id.clone();
        let mut remote_endpoints = map
            .shards
            .iter()
            .map(|s| s.endpoint.clone())
            .filter(|endpoint| normalize_endpoint(endpoint) != local_endpoint)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        remote_endpoints.sort();

        map.object.object_key = request.destination_object_key.clone();
        persist_n2_object_map(&map).await?;

        deleted_old_map_files = deleted_old_map_files.saturating_add(
            delete_n2_object_map_local(
                request.fragment.as_str(),
                old_object_key.as_str(),
                old_version.as_str(),
            )
            .await?,
        );
        for endpoint in remote_endpoints {
            match delete_n2_object_map_remote(
                &http_client,
                endpoint.as_str(),
                request.fragment.as_str(),
                old_object_key.as_str(),
                old_version.as_str(),
            )
            .await
            {
                Ok(removed) => {
                    deleted_old_map_files = deleted_old_map_files.saturating_add(removed);
                }
                Err(err) => {
                    warn!(
                        "n2 remote old-map delete failed fragment={} source_key={} version={} endpoint={} err={}",
                        request.fragment, old_object_key, old_version, endpoint, err
                    );
                }
            }
        }
        moved_versions = moved_versions.saturating_add(1);
    }

    Ok(AxumJson(N2RenameObjectResponse {
        fragment: request.fragment,
        source_object_key: request.source_object_key,
        destination_object_key: request.destination_object_key,
        moved_versions,
        deleted_old_map_files,
    }))
}

pub async fn reconcile_n2_fragment(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(query): Query<ReconcileFragmentQuery>,
) -> Result<AxumJson<N2ReconcileResponse>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required".to_string(),
        ));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    let cluster_view = ensure_cluster_write_ready(query.expected_epoch).await?;

    let cfg = n2_runtime_config();
    let dry_run = query.dry_run.unwrap_or(false);
    let cleanup_orphans = query.cleanup_orphans.unwrap_or(true);
    let max_objects = query.max_objects.unwrap_or(128).clamp(1, 10_000);
    let maps = list_fragment_n2_maps(fragment.as_str(), max_objects).await?;

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;

    let mut results = Vec::with_capacity(maps.len());
    let mut updated_objects = 0usize;
    let mut failed_objects = 0usize;

    for mut map in maps {
        let old_epoch = map.epoch;
        let reconcile = async {
            let placement =
                resolve_placement(map.object.clone(), map.params.kc, map.params.mc).await?;
            let kc = usize::from(map.params.kc);
            let nc = usize::from(map.params.kc) + usize::from(map.params.mc);
            let previous_shards = map.shards.clone();
            let mut available_shard_bytes: HashMap<usize, Vec<u8>> = HashMap::new();

            for shard in &map.shards {
                if available_shard_bytes.contains_key(&shard.cluster_shard_index) {
                    continue;
                }
                let result = if is_local_target(&NodeDescriptor {
                    node_id: shard.node_id.clone(),
                    endpoint: shard.endpoint.clone(),
                    epoch: map.epoch,
                    active: true,
                }) {
                    read_local_cluster_shard(
                        map.object.fragment.as_str(),
                        map.object.version_id.as_str(),
                        shard.cluster_shard_index,
                    )
                    .await
                } else {
                    fetch_remote_cluster_shard(
                        &http_client,
                        shard.endpoint.as_str(),
                        map.object.fragment.as_str(),
                        map.object.version_id.as_str(),
                        shard.cluster_shard_index,
                    )
                    .await
                };
                if let Ok(bytes) = result {
                    available_shard_bytes.insert(shard.cluster_shard_index, bytes);
                }
            }

            if available_shard_bytes.len() < kc {
                return Err(AppError::InternalServerError(format!(
                    "insufficient readable shards for reconcile: need {}, have {}",
                    kc,
                    available_shard_bytes.len()
                )));
            }

            let mut next_shards = Vec::with_capacity(nc);
            let mut planned_shards = 0usize;
            let mut repaired_shards = 0usize;
            let mut cleaned_orphans = 0usize;
            let mut cleanup_failures = 0usize;

            for idx in 0..nc {
                let target = &placement.targets[idx];
                if let Some(existing) = map
                    .shards
                    .iter()
                    .find(|s| s.cluster_shard_index == idx && shard_matches_target(s, target))
                {
                    next_shards.push(existing.clone());
                    continue;
                }

                planned_shards = planned_shards.saturating_add(1);
                if dry_run {
                    continue;
                }

                let shard_bytes = if let Some(existing) = available_shard_bytes.get(&idx) {
                    existing.clone()
                } else {
                    let rebuilt = reconstruct_cluster_shard_from_available(
                        usize::from(map.params.kc),
                        usize::from(map.params.mc),
                        map.stripe_block_size,
                        idx,
                        &available_shard_bytes,
                    )?;
                    available_shard_bytes.insert(idx, rebuilt.clone());
                    rebuilt
                };

                let stored = if is_local_target(target) {
                    store_local_cluster_shard_from_bytes(
                        &shard_bytes,
                        &map.object,
                        idx,
                        map.params.kd,
                        map.params.md,
                        map.stripe_block_size,
                    )
                    .await?
                } else {
                    store_remote_cluster_shard_bytes(
                        &http_client,
                        target,
                        &map.object,
                        idx,
                        map.params.kd,
                        map.params.md,
                        map.stripe_block_size,
                        shard_bytes,
                    )
                    .await?
                };

                next_shards.push(stored);
                repaired_shards = repaired_shards.saturating_add(1);
            }

            if !dry_run {
                next_shards.sort_by_key(|s| s.cluster_shard_index);
                if next_shards.len() < kc {
                    return Err(AppError::InternalServerError(format!(
                        "reconcile produced insufficient shard refs: need {}, have {}",
                        kc,
                        next_shards.len()
                    )));
                }
                let stale_shards = previous_shards
                    .iter()
                    .filter(|old| {
                        !next_shards.iter().any(|next| {
                            old.cluster_shard_index == next.cluster_shard_index
                                && (old.node_id == next.node_id
                                    || normalize_endpoint(old.endpoint.as_str())
                                        == normalize_endpoint(next.endpoint.as_str()))
                        })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                map.shards = next_shards;
                map.epoch = placement.epoch;
                map.committed_at = Utc::now().timestamp();
                persist_n2_object_map(&map).await?;

                if cleanup_orphans && !stale_shards.is_empty() {
                    for stale in stale_shards {
                        let target = NodeDescriptor {
                            node_id: stale.node_id.clone(),
                            endpoint: stale.endpoint.clone(),
                            epoch: map.epoch,
                            active: true,
                        };
                        let cleanup_result = if is_local_target(&target) {
                            delete_local_cluster_shard(
                                map.object.fragment.as_str(),
                                map.object.version_id.as_str(),
                                stale.cluster_shard_index,
                            )
                            .await
                        } else {
                            delete_remote_cluster_shard(
                                &http_client,
                                stale.endpoint.as_str(),
                                map.object.fragment.as_str(),
                                map.object.version_id.as_str(),
                                stale.cluster_shard_index,
                            )
                            .await
                        };

                        match cleanup_result {
                            Ok(paths) => {
                                cleaned_orphans = cleaned_orphans.saturating_add(paths);
                            }
                            Err(err) => {
                                cleanup_failures = cleanup_failures.saturating_add(1);
                                warn!(
                                    "n2 reconcile orphan cleanup failed fragment={} object_key={} version={} shard={} node={} endpoint={} err={}",
                                    map.object.fragment,
                                    map.object.object_key,
                                    map.object.version_id,
                                    stale.cluster_shard_index,
                                    stale.node_id,
                                    stale.endpoint,
                                    err
                                );
                            }
                        }
                    }
                }
            }

            Ok::<(usize, usize, usize, usize, u64), AppError>((
                planned_shards,
                repaired_shards,
                cleaned_orphans,
                cleanup_failures,
                placement.epoch,
            ))
        }
        .await;

        match reconcile {
            Ok((
                planned_shards,
                repaired_shards,
                cleaned_orphans,
                cleanup_failures,
                target_epoch,
            )) => {
                if planned_shards > 0 {
                    updated_objects = updated_objects.saturating_add(1);
                }
                results.push(N2ReconcileObjectResult {
                    object_key: map.object.object_key.clone(),
                    version_id: map.object.version_id.clone(),
                    old_epoch,
                    target_epoch,
                    planned_shards,
                    repaired_shards,
                    cleaned_orphans,
                    cleanup_failures,
                    status: if dry_run {
                        "planned".to_string()
                    } else {
                        "reconciled".to_string()
                    },
                    error: None,
                });
            }
            Err(err) => {
                failed_objects = failed_objects.saturating_add(1);
                warn!(
                    "n2 reconcile failed fragment={} object_key={} version={} err={}",
                    map.object.fragment, map.object.object_key, map.object.version_id, err
                );
                results.push(N2ReconcileObjectResult {
                    object_key: map.object.object_key.clone(),
                    version_id: map.object.version_id.clone(),
                    old_epoch,
                    target_epoch: old_epoch,
                    planned_shards: 0,
                    repaired_shards: 0,
                    cleaned_orphans: 0,
                    cleanup_failures: 0,
                    status: "failed".to_string(),
                    error: Some(err.to_string()),
                });
            }
        }
    }

    info!(
        "n2 reconcile completed fragment={} scanned={} updated={} failed={} dry_run={} membership_epoch={}",
        fragment,
        results.len(),
        updated_objects,
        failed_objects,
        dry_run,
        cluster_view.membership_epoch
    );

    Ok(AxumJson(N2ReconcileResponse {
        fragment,
        dry_run,
        scanned_objects: results.len(),
        updated_objects,
        failed_objects,
        results,
    }))
}

pub async fn n2_internal_begin_shard(
    headers: HeaderMap,
    Path((fragment, version_id, cluster_shard_index)): Path<(String, String, usize)>,
    Query(query): Query<InternalStoreShardQuery>,
) -> Result<AxumJson<InternalShardSessionResponse>, AppError> {
    validate_internal_token(&headers)?;
    if query.kd == 0 {
        return Err(AppError::BadRequest("kd must be >= 1".to_string()));
    }
    let stripe_block_size = normalize_n2_stripe_block_size(query.stripe_block_size);
    let session_id = Ulid::new().to_string();
    let temp_root =
        std::env::temp_dir().join(format!("n2-internal-session-{}", session_id.as_str()));
    fs::create_dir_all(&temp_root)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {:?}", temp_root)))?;
    let temp_file_path = temp_root.join("cluster_shard.bin");
    fs::File::create(&temp_file_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {:?}", temp_file_path)))?;

    let object = ObjectIdentity {
        uid: query.uid.unwrap_or_else(|| fragment.clone()),
        fragment,
        object_key: query
            .object_key
            .unwrap_or_else(|| "<internal-cshard-session>".to_string()),
        version_id,
    };

    let session = InternalShardUploadSession {
        object,
        cluster_shard_index,
        kd: query.kd,
        md: query.md,
        stripe_block_size,
        temp_root,
        temp_file_path,
    };
    internal_shard_upload_sessions()
        .lock()
        .await
        .insert(session_id.clone(), session);
    Ok(AxumJson(InternalShardSessionResponse { session_id }))
}

pub async fn n2_internal_write_shard_chunk(
    headers: HeaderMap,
    Path(session_id): Path<String>,
    body: Body,
) -> Result<AxumJson<InternalShardChunkAck>, AppError> {
    validate_internal_token(&headers)?;
    let temp_file_path = {
        let guard = internal_shard_upload_sessions().lock().await;
        let session = guard.get(session_id.as_str()).ok_or(AppError::NotFound)?;
        session.temp_file_path.clone()
    };

    let mut file = OpenOptions::new()
        .append(true)
        .open(&temp_file_path)
        .await
        .map_err(|e| AppError::Io(e, format!("open {:?}", temp_file_path)))?;
    let mut stream = body.into_data_stream();
    let mut written = 0u64;
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read internal shard session chunk body".to_string(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        file.write_all(&chunk)
            .await
            .map_err(|e| AppError::Io(e, "append internal shard session chunk".to_string()))?;
        written = written.saturating_add(chunk.len() as u64);
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, "flush internal shard session chunk".to_string()))?;

    Ok(AxumJson(InternalShardChunkAck {
        session_id,
        written_bytes: written,
    }))
}

pub async fn n2_internal_commit_shard(
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<AxumJson<N2ClusterShardRef>, AppError> {
    validate_internal_token(&headers)?;
    let session = internal_shard_upload_sessions()
        .lock()
        .await
        .remove(session_id.as_str())
        .ok_or(AppError::NotFound)?;
    let stored = store_local_cluster_shard_from_file(
        session.temp_file_path.as_path(),
        &session.object,
        session.cluster_shard_index,
        session.kd,
        session.md,
        session.stripe_block_size,
    )
    .await;
    let _ = fs::remove_dir_all(&session.temp_root).await;
    stored.map(AxumJson)
}

pub async fn n2_internal_abort_shard(
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<AxumJson<InternalShardAbortResponse>, AppError> {
    validate_internal_token(&headers)?;
    if let Some(session) = internal_shard_upload_sessions()
        .lock()
        .await
        .remove(session_id.as_str())
    {
        let _ = fs::remove_dir_all(&session.temp_root).await;
    }
    Ok(AxumJson(InternalShardAbortResponse {
        session_id,
        aborted: true,
    }))
}

pub async fn n2_internal_store_shard(
    headers: HeaderMap,
    Path((fragment, version_id, cluster_shard_index)): Path<(String, String, usize)>,
    Query(query): Query<InternalStoreShardQuery>,
    body: Body,
) -> Result<AxumJson<N2ClusterShardRef>, AppError> {
    validate_internal_token(&headers)?;

    if query.kd == 0 {
        return Err(AppError::BadRequest("kd must be >= 1".to_string()));
    }
    let stripe_block_size = normalize_n2_stripe_block_size(query.stripe_block_size);

    let tmp_root = std::env::temp_dir().join(format!("n2-internal-store-{}", Ulid::new()));
    fs::create_dir_all(&tmp_root)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {:?}", tmp_root)))?;
    let tmp_file_path = tmp_root.join("cluster_shard.bin");
    let mut tmp_file = fs::File::create(&tmp_file_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {:?}", tmp_file_path)))?;

    let mut stream = body.into_data_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read internal shard request body".to_string(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        tmp_file
            .write_all(&chunk)
            .await
            .map_err(|e| AppError::Io(e, "write internal temp shard".to_string()))?;
    }
    tmp_file
        .flush()
        .await
        .map_err(|e| AppError::Io(e, "flush internal temp shard".to_string()))?;
    drop(tmp_file);

    let object = ObjectIdentity {
        uid: query.uid.unwrap_or_else(|| fragment.clone()),
        fragment: fragment.clone(),
        object_key: query
            .object_key
            .unwrap_or_else(|| "<internal-cshard>".to_string()),
        version_id,
    };

    let stored = store_local_cluster_shard_from_file(
        tmp_file_path.as_path(),
        &object,
        cluster_shard_index,
        query.kd,
        query.md,
        stripe_block_size,
    )
    .await?;

    let _ = fs::remove_dir_all(&tmp_root).await;
    Ok(AxumJson(stored))
}

pub async fn n2_internal_read_shard(
    headers: HeaderMap,
    Path((fragment, version_id, cluster_shard_index)): Path<(String, String, usize)>,
) -> Result<impl IntoResponse, AppError> {
    validate_internal_token(&headers)?;
    let bytes =
        read_local_cluster_shard(fragment.as_str(), version_id.as_str(), cluster_shard_index)
            .await?;
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

pub async fn n2_internal_delete_shard(
    headers: HeaderMap,
    Path((fragment, version_id, cluster_shard_index)): Path<(String, String, usize)>,
) -> Result<AxumJson<InternalShardDeleteResponse>, AppError> {
    validate_internal_token(&headers)?;
    let deleted_paths =
        delete_local_cluster_shard(fragment.as_str(), version_id.as_str(), cluster_shard_index)
            .await?;
    Ok(AxumJson(InternalShardDeleteResponse { deleted_paths }))
}

pub async fn n2_internal_upsert_map(
    headers: HeaderMap,
    AxumJson(map): AxumJson<N2ObjectMap>,
) -> Result<AxumJson<InternalMapUpsertResponse>, AppError> {
    validate_internal_token(&headers)?;
    let writes = persist_n2_object_map_local(&map).await?;
    Ok(AxumJson(InternalMapUpsertResponse {
        local_mirrors_written: writes,
    }))
}

pub async fn n2_internal_read_map(
    headers: HeaderMap,
    Path(fragment): Path<String>,
    Query(query): Query<InternalMapReadQuery>,
) -> Result<AxumJson<N2ObjectMap>, AppError> {
    validate_internal_token(&headers)?;
    let object_key = query.object_key.trim();
    if object_key.is_empty() {
        return Err(AppError::BadRequest(
            "object_key is required for internal map read".to_string(),
        ));
    }
    let map = load_n2_object_map_local(fragment.as_str(), object_key, query.version_id.as_deref())
        .await?;
    Ok(AxumJson(map))
}

pub async fn n2_internal_delete_map(
    headers: HeaderMap,
    Path(fragment): Path<String>,
    Query(query): Query<InternalMapDeleteQuery>,
) -> Result<AxumJson<InternalMapDeleteResponse>, AppError> {
    validate_internal_token(&headers)?;
    let object_key = query.object_key.trim();
    let version_id = query.version_id.trim();
    if object_key.is_empty() || version_id.is_empty() {
        return Err(AppError::BadRequest(
            "object_key and version_id are required for internal map delete".to_string(),
        ));
    }
    let deleted = delete_n2_object_map_local(fragment.as_str(), object_key, version_id).await?;
    Ok(AxumJson(InternalMapDeleteResponse {
        local_map_files_deleted: deleted,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::n1_core::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
    use crate::n2_core::model::{
        DeleteN2ObjectQuery, GetObjectQuery, ListN2ObjectsQuery, ListN2VersionsQuery,
        PutObjectQuery, ReconcileFragmentQuery, RenameN2ObjectRequest, UpsertMembershipRequest,
    };
    use crate::n2_core::state::{get_membership, upsert_membership};
    use crate::structs::auth_structs::Claims;
    use axum::Router;
    use axum::body::to_bytes;
    use axum::extract::{Path as AxumPath, Query as AxumQuery};
    use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
    use axum::response::IntoResponse;
    use axum::routing::{get, post, put};
    use std::sync::{Arc, LazyLock};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    static STORAGE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn full_claims(fragment: &str) -> Claims {
        Claims {
            fragment: fragment.to_string(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        }
    }

    fn internal_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            N2_INTERNAL_TOKEN_HEADER,
            HeaderValue::from_str(n2_runtime_config().internal_token.as_str())
                .expect("internal token header"),
        );
        headers
    }

    fn local_node_descriptor() -> NodeDescriptor {
        let cfg = n2_runtime_config();
        NodeDescriptor {
            node_id: cfg.node_id.clone(),
            endpoint: cfg.node_endpoint.clone(),
            epoch: 0,
            active: true,
        }
    }

    async fn ensure_n2_test_storage_roots() {
        // Keep the same global test root convention used by workflow matrix tests so
        // OnceLock-backed disk roots remain coherent across the full test process.
        let base = std::env::temp_dir().join(format!("n1-matrix-{}", std::process::id()));
        let index_1 = base.join("index_1");
        let index_2 = base.join("index_2");
        let blob_1 = base.join("blob_1");
        let blob_2 = base.join("blob_2");
        let blob_3 = base.join("blob_3");
        for p in [&index_1, &index_2, &blob_1, &blob_2, &blob_3] {
            let _ = fs::create_dir_all(p).await;
        }

        if INDEX_STORE_DISKS.get().is_none() {
            INDEX_STORE_DISKS
                .set(vec![
                    index_1.to_string_lossy().to_string(),
                    index_2.to_string_lossy().to_string(),
                ])
                .expect("set INDEX_STORE_DISKS");
        }
        if BLOB_STORE_DISKS.get().is_none() {
            BLOB_STORE_DISKS
                .set(vec![
                    blob_1.to_string_lossy().to_string(),
                    blob_2.to_string_lossy().to_string(),
                    blob_3.to_string_lossy().to_string(),
                ])
                .expect("set BLOB_STORE_DISKS");
        }
        if FS_STORAGE_ROOT.get().is_none() {
            FS_STORAGE_ROOT
                .set(base.to_string_lossy().to_string())
                .expect("set FS_STORAGE_ROOT");
        }
    }

    async fn spawn_internal_session_server() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route(
                "/v4/n2/internal/session/begin/{fragment}/{version_id}/{cluster_shard_index}",
                post(n2_internal_begin_shard),
            )
            .route(
                "/v4/n2/internal/session/chunk/{session_id}",
                put(n2_internal_write_shard_chunk),
            )
            .route(
                "/v4/n2/internal/session/commit/{session_id}",
                post(n2_internal_commit_shard),
            )
            .route(
                "/v4/n2/internal/session/abort/{session_id}",
                post(n2_internal_abort_shard),
            )
            .route(
                "/v4/n2/internal/read/{fragment}/{version_id}/{cluster_shard_index}",
                get(n2_internal_read_shard),
            )
            .route(
                "/v4/n2/internal/store/{fragment}/{version_id}/{cluster_shard_index}",
                post(n2_internal_store_shard),
            )
            .route(
                "/v4/n2/internal/delete/{fragment}/{version_id}/{cluster_shard_index}",
                axum::routing::delete(n2_internal_delete_shard),
            )
            .route("/v4/n2/internal/map/upsert", post(n2_internal_upsert_map))
            .route("/v4/n2/internal/map/{fragment}", get(n2_internal_read_map))
            .route(
                "/v4/n2/internal/map/{fragment}",
                axum::routing::delete(n2_internal_delete_map),
            );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind internal session server");
        let addr = listener.local_addr().expect("local addr");
        let join = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve internal routes");
        });
        (format!("http://{}", addr), join)
    }

    fn ensure_auth_env_for_tests() {
        // SAFETY: test-only setup before handlers touch these env-backed LazyLocks.
        unsafe {
            if std::env::var("ADMIN_KEY").is_err() {
                std::env::set_var("ADMIN_KEY", "admin");
            }
            if std::env::var("MASTER_KEY").is_err() {
                std::env::set_var("MASTER_KEY", "master");
            }
            if std::env::var("AUTH_KEY").is_err() {
                std::env::set_var("AUTH_KEY", "auth");
            }
            if std::env::var("JWT_SECRET").is_err() {
                std::env::set_var("JWT_SECRET", "jwt-secret-for-tests");
            }
        }
    }

    fn sample_bytes(len: usize, seed: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        for (i, b) in out.iter_mut().enumerate() {
            *b = ((i + seed) % 251) as u8;
        }
        out
    }

    #[test]
    fn reconstruct_roundtrip_with_kc2_mc1_missing_one_data_shard() {
        let kc = 2usize;
        let mc = 1usize;
        let block_size = 4096usize;
        let data = sample_bytes(15_777, 17);

        let stripe_data_width = kc * block_size;
        let stripe_count = data.len().div_ceil(stripe_data_width);
        let shard_size = stripe_count * block_size;
        let mut shards = vec![
            vec![0u8; shard_size],
            vec![0u8; shard_size],
            vec![0u8; shard_size],
        ];

        for stripe_idx in 0..stripe_count {
            let start = stripe_idx * stripe_data_width;
            let end = (start + stripe_data_width).min(data.len());
            let mut stripe = vec![0u8; stripe_data_width];
            stripe[..end - start].copy_from_slice(&data[start..end]);

            let mut enc = ReedSolomonEncoder::new(kc, mc, block_size).expect("encoder");
            enc.add_original_shard(&stripe[0..block_size])
                .expect("add shard0");
            enc.add_original_shard(&stripe[block_size..2 * block_size])
                .expect("add shard1");
            let encoded = enc.encode().expect("encode");

            let shard_off = stripe_idx * block_size;
            shards[0][shard_off..shard_off + block_size].copy_from_slice(&stripe[0..block_size]);
            shards[1][shard_off..shard_off + block_size]
                .copy_from_slice(&stripe[block_size..2 * block_size]);
            shards[2][shard_off..shard_off + block_size]
                .copy_from_slice(encoded.recovery(0).expect("parity"));
        }

        let reconstructed = reconstruct_object_from_cluster_shards(
            kc,
            mc,
            block_size,
            data.len() as u64,
            vec![(1, shards[1].clone()), (2, shards[2].clone())],
        )
        .expect("reconstruct");
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn reconstruct_fails_when_available_shards_below_quorum() {
        let err =
            reconstruct_object_from_cluster_shards(2, 1, 4096, 10, vec![(0, vec![0u8; 4096])])
                .expect_err("should fail");
        match err {
            AppError::InternalServerError(_) => {}
            _ => panic!("expected InternalServerError"),
        }
    }

    #[test]
    fn reconstruct_cluster_shard_recovers_missing_data_shard() {
        let kc = 2usize;
        let mc = 1usize;
        let block_size = 4096usize;
        let data = sample_bytes(23_777, 7);

        let stripe_data_width = kc * block_size;
        let stripe_count = data.len().div_ceil(stripe_data_width);
        let shard_size = stripe_count * block_size;
        let mut shards = vec![
            vec![0u8; shard_size],
            vec![0u8; shard_size],
            vec![0u8; shard_size],
        ];

        for stripe_idx in 0..stripe_count {
            let start = stripe_idx * stripe_data_width;
            let end = (start + stripe_data_width).min(data.len());
            let mut stripe = vec![0u8; stripe_data_width];
            stripe[..end - start].copy_from_slice(&data[start..end]);

            let mut enc = ReedSolomonEncoder::new(kc, mc, block_size).expect("encoder");
            enc.add_original_shard(&stripe[0..block_size])
                .expect("add shard0");
            enc.add_original_shard(&stripe[block_size..2 * block_size])
                .expect("add shard1");
            let encoded = enc.encode().expect("encode");

            let shard_off = stripe_idx * block_size;
            shards[0][shard_off..shard_off + block_size].copy_from_slice(&stripe[0..block_size]);
            shards[1][shard_off..shard_off + block_size]
                .copy_from_slice(&stripe[block_size..2 * block_size]);
            shards[2][shard_off..shard_off + block_size]
                .copy_from_slice(encoded.recovery(0).expect("parity"));
        }

        let mut available = HashMap::new();
        available.insert(1usize, shards[1].clone());
        available.insert(2usize, shards[2].clone());

        let rebuilt = reconstruct_cluster_shard_from_available(kc, mc, block_size, 0, &available)
            .expect("rebuild data shard");
        assert_eq!(rebuilt, shards[0]);
    }

    #[test]
    fn reconstruct_cluster_shard_recovers_parity_shard() {
        let kc = 2usize;
        let mc = 1usize;
        let block_size = 4096usize;
        let data = sample_bytes(11_900, 29);

        let stripe_data_width = kc * block_size;
        let stripe_count = data.len().div_ceil(stripe_data_width);
        let shard_size = stripe_count * block_size;
        let mut shards = vec![
            vec![0u8; shard_size],
            vec![0u8; shard_size],
            vec![0u8; shard_size],
        ];

        for stripe_idx in 0..stripe_count {
            let start = stripe_idx * stripe_data_width;
            let end = (start + stripe_data_width).min(data.len());
            let mut stripe = vec![0u8; stripe_data_width];
            stripe[..end - start].copy_from_slice(&data[start..end]);

            let mut enc = ReedSolomonEncoder::new(kc, mc, block_size).expect("encoder");
            enc.add_original_shard(&stripe[0..block_size])
                .expect("add shard0");
            enc.add_original_shard(&stripe[block_size..2 * block_size])
                .expect("add shard1");
            let encoded = enc.encode().expect("encode");

            let shard_off = stripe_idx * block_size;
            shards[0][shard_off..shard_off + block_size].copy_from_slice(&stripe[0..block_size]);
            shards[1][shard_off..shard_off + block_size]
                .copy_from_slice(&stripe[block_size..2 * block_size]);
            shards[2][shard_off..shard_off + block_size]
                .copy_from_slice(encoded.recovery(0).expect("parity"));
        }

        let mut available = HashMap::new();
        available.insert(0usize, shards[0].clone());
        available.insert(1usize, shards[1].clone());

        let rebuilt = reconstruct_cluster_shard_from_available(kc, mc, block_size, 2, &available)
            .expect("rebuild parity shard");
        assert_eq!(rebuilt, shards[2]);
    }

    #[test]
    fn encode_cluster_stripe_blocks_produces_expected_count_and_size() {
        let kc = 2usize;
        let mc = 1usize;
        let block_size = 4096usize;
        let mut stripe = vec![0u8; kc * block_size];
        for (idx, b) in stripe.iter_mut().enumerate() {
            *b = (idx % 251) as u8;
        }

        let blocks =
            encode_cluster_stripe_blocks(stripe.as_slice(), kc, mc, block_size).expect("encode");
        assert_eq!(blocks.len(), kc + mc);
        assert!(blocks.iter().all(|b| b.len() == block_size));
        assert_eq!(blocks[0].as_slice(), &stripe[0..block_size]);
        assert_eq!(blocks[1].as_slice(), &stripe[block_size..2 * block_size]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_session_begin_chunk_commit_read_roundtrip() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let fragment = format!("frag-{}", Ulid::new());
        let version_id = Ulid::new().to_string();
        let chunk_a = sample_bytes(5_000, 3);
        let chunk_b = sample_bytes(7_000, 9);
        let mut expected = chunk_a.clone();
        expected.extend_from_slice(&chunk_b);

        let begin = n2_internal_begin_shard(
            internal_headers(),
            AxumPath((fragment.clone(), version_id.clone(), 0usize)),
            AxumQuery(InternalStoreShardQuery {
                uid: Some(fragment.clone()),
                object_key: Some("videos/session-roundtrip.bin".to_string()),
                kd: 1,
                md: 0,
                stripe_block_size: 4096,
            }),
        )
        .await
        .expect("begin session");
        let session_id = begin.0.session_id.clone();

        let _ = n2_internal_write_shard_chunk(
            internal_headers(),
            AxumPath(session_id.clone()),
            Body::from(chunk_a),
        )
        .await
        .expect("write chunk a");
        let _ = n2_internal_write_shard_chunk(
            internal_headers(),
            AxumPath(session_id.clone()),
            Body::from(chunk_b),
        )
        .await
        .expect("write chunk b");

        let committed = n2_internal_commit_shard(internal_headers(), AxumPath(session_id.clone()))
            .await
            .expect("commit");
        assert_eq!(committed.0.cluster_shard_index, 0);
        assert_eq!(committed.0.size_bytes as usize, expected.len());

        let read_resp = n2_internal_read_shard(
            internal_headers(),
            AxumPath((fragment.clone(), version_id.clone(), 0usize)),
        )
        .await
        .expect("read shard")
        .into_response();
        let read_bytes = to_bytes(read_resp.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert_eq!(read_bytes.as_ref(), expected.as_slice());

        let after_commit = n2_internal_write_shard_chunk(
            internal_headers(),
            AxumPath(session_id),
            Body::from(vec![1u8; 32]),
        )
        .await;
        assert!(
            matches!(after_commit, Err(AppError::NotFound)),
            "session should be closed after commit"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_session_abort_rejects_further_writes_and_commit() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let fragment = format!("frag-{}", Ulid::new());
        let version_id = Ulid::new().to_string();
        let begin = n2_internal_begin_shard(
            internal_headers(),
            AxumPath((fragment, version_id, 0usize)),
            AxumQuery(InternalStoreShardQuery {
                uid: None,
                object_key: Some("videos/session-abort.bin".to_string()),
                kd: 1,
                md: 0,
                stripe_block_size: 4096,
            }),
        )
        .await
        .expect("begin session");
        let session_id = begin.0.session_id.clone();

        let _ = n2_internal_write_shard_chunk(
            internal_headers(),
            AxumPath(session_id.clone()),
            Body::from(vec![9u8; 2048]),
        )
        .await
        .expect("write chunk before abort");

        let _ = n2_internal_abort_shard(internal_headers(), AxumPath(session_id.clone()))
            .await
            .expect("abort");

        let chunk_after_abort = n2_internal_write_shard_chunk(
            internal_headers(),
            AxumPath(session_id.clone()),
            Body::from(vec![1u8; 128]),
        )
        .await;
        assert!(matches!(chunk_after_abort, Err(AppError::NotFound)));

        let commit_after_abort =
            n2_internal_commit_shard(internal_headers(), AxumPath(session_id)).await;
        assert!(matches!(commit_after_abort, Err(AppError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_session_requires_token() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let result = n2_internal_begin_shard(
            HeaderMap::new(),
            AxumPath((
                format!("frag-{}", Ulid::new()),
                Ulid::new().to_string(),
                0usize,
            )),
            AxumQuery(InternalStoreShardQuery {
                uid: None,
                object_key: None,
                kd: 1,
                md: 0,
                stripe_block_size: 4096,
            }),
        )
        .await;
        assert!(matches!(result, Err(AppError::Forbidden(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn put_get_n2_object_remote_session_pipeline_roundtrip() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let (remote_endpoint, remote_server) = spawn_internal_session_server().await;
        let previous_membership = get_membership().await.expect("current membership");
        let cfg = n2_runtime_config();

        let remote_node = NodeDescriptor {
            node_id: format!("remote-{}", Ulid::new()),
            endpoint: remote_endpoint.clone(),
            epoch: 0,
            active: true,
        };
        let local_node = NodeDescriptor {
            node_id: cfg.node_id.clone(),
            endpoint: cfg.node_endpoint.clone(),
            epoch: 0,
            active: true,
        };
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node, remote_node],
        })
        .await
        .expect("set membership with local+remote nodes");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/remote-session-path.bin".to_string();
        let payload = sample_bytes(2_600_000, 41);
        let mut put_headers = HeaderMap::new();
        put_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let put_result = put_n2_object(
            full_claims(fragment.as_str()),
            put_headers,
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(2),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(256 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put n2 object");
        assert_eq!(put_result.0.required_quorum, 2);
        assert_eq!(put_result.0.committed_shards, 2);

        let get_resp = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put_result.0.version_id.clone()),
            }),
        )
        .await
        .expect("get n2 object")
        .into_response();
        let got = to_bytes(get_resp.into_body(), usize::MAX)
            .await
            .expect("read get body");
        assert_eq!(got.as_ref(), payload.as_slice());

        // Restore membership and stop remote server.
        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
        remote_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_n2_object_rejects_stale_expected_epoch() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        let current = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");
        let stale_epoch = current.epoch.saturating_sub(1);

        let result = put_n2_object(
            full_claims("frag-stale"),
            HeaderMap::new(),
            AxumPath((
                "frag-stale".to_string(),
                "videos/stale-epoch.bin".to_string(),
            )),
            AxumQuery(PutObjectQuery {
                uid: Some("frag-stale".to_string()),
                version_id: None,
                expected_epoch: Some(stale_epoch),
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(64 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(sample_bytes(32_000, 1)),
        )
        .await;
        assert!(matches!(result, Err(AppError::Conflict(_))));

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_n2_object_honors_single_range_requests() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/range-check.bin".to_string();
        let payload = sample_bytes(900_000, 23);
        let version_id = Ulid::new().to_string();
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: Some(version_id.clone()),
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put object");
        assert_eq!(put.0.version_id, version_id);

        let mut headers = HeaderMap::new();
        headers.insert(RANGE, HeaderValue::from_static("bytes=100-999"));
        let response = get_n2_object(
            full_claims(fragment.as_str()),
            headers,
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(version_id.clone()),
            }),
        )
        .await
        .expect("get range")
        .into_response();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|v| v.to_str().ok()),
            Some(format!("bytes 100-999/{}", payload.len()).as_str())
        );
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("range bytes");
        assert_eq!(bytes.as_ref(), &payload[100..1000]);

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_map_read_falls_back_to_remote_replica_when_local_maps_are_missing() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/map-fallback.bin".to_string();
        let payload = sample_bytes(1_200_000, 99);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(256 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put object");

        let remote_map = Arc::new(put.0.object.clone());
        let internal_token = n2_runtime_config().internal_token.clone();
        let app = Router::new().route(
            "/v4/n2/internal/map/{fragment}",
            get({
                let remote_map = Arc::clone(&remote_map);
                let internal_token = internal_token.clone();
                move |headers: HeaderMap,
                      AxumPath(route_fragment): AxumPath<String>,
                      AxumQuery(query): AxumQuery<InternalMapReadQuery>| {
                    let remote_map = Arc::clone(&remote_map);
                    let internal_token = internal_token.clone();
                    async move {
                        let token_ok = headers
                            .get(N2_INTERNAL_TOKEN_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v == internal_token.as_str())
                            .unwrap_or(false);
                        if !token_ok {
                            return Err(StatusCode::FORBIDDEN);
                        }
                        if route_fragment != remote_map.object.fragment
                            || query.object_key.trim() != remote_map.object.object_key
                        {
                            return Err(StatusCode::NOT_FOUND);
                        }
                        if let Some(version_id) = query.version_id.as_ref() {
                            if version_id.trim() != remote_map.object.version_id {
                                return Err(StatusCode::NOT_FOUND);
                            }
                        }
                        Ok::<AxumJson<N2ObjectMap>, StatusCode>(AxumJson((*remote_map).clone()))
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind map fallback server");
        let map_endpoint = format!("http://{}", listener.local_addr().expect("map server addr"));
        let map_server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve map fallback server");
        });

        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: format!("map-replica-{}", Ulid::new()),
                    endpoint: map_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("set membership with map replica endpoint");

        let removed = delete_n2_object_map_local(
            fragment.as_str(),
            object_key.as_str(),
            put.0.version_id.as_str(),
        )
        .await
        .expect("delete local map copies");
        assert!(removed > 0);

        let response = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("get object via remote map fallback")
        .into_response();
        let got = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read get body");
        assert_eq!(got.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
        map_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_object_api_parity_list_versions_rename_and_delete() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let source_key = "docs/report.bin".to_string();
        let destination_key = "archive/report.bin".to_string();
        let v1 = Ulid::new().to_string();
        let v2 = Ulid::new().to_string();

        let _ = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), source_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: Some(v1.clone()),
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(sample_bytes(250_000, 31)),
        )
        .await
        .expect("put version 1");

        let _ = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), source_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: Some(v2.clone()),
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(sample_bytes(260_000, 32)),
        )
        .await
        .expect("put version 2");

        let before = list_n2_object_versions(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), source_key.clone())),
            AxumQuery(ListN2VersionsQuery { limit: Some(10) }),
        )
        .await
        .expect("list versions before rename");
        assert_eq!(before.0.versions.len(), 2);

        let rename = rename_n2_object(
            full_claims(fragment.as_str()),
            AxumJson(RenameN2ObjectRequest {
                fragment: fragment.clone(),
                source_object_key: source_key.clone(),
                destination_object_key: destination_key.clone(),
                expected_epoch: None,
            }),
        )
        .await
        .expect("rename n2 object");
        assert_eq!(rename.0.moved_versions, 2);

        let old_versions = list_n2_object_versions(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), source_key.clone())),
            AxumQuery(ListN2VersionsQuery { limit: Some(10) }),
        )
        .await
        .expect("list old key versions");
        assert!(old_versions.0.versions.is_empty());

        let new_versions = list_n2_object_versions(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), destination_key.clone())),
            AxumQuery(ListN2VersionsQuery { limit: Some(10) }),
        )
        .await
        .expect("list new key versions");
        assert_eq!(new_versions.0.versions.len(), 2);

        let listed = list_n2_objects(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ListN2ObjectsQuery { limit: Some(100) }),
        )
        .await
        .expect("list n2 objects")
        .0;
        assert!(
            listed
                .objects
                .iter()
                .any(|row| row.object_key == destination_key)
        );

        let delete_one = delete_n2_object(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), destination_key.clone())),
            AxumQuery(DeleteN2ObjectQuery {
                version_id: Some(v1.clone()),
                expected_epoch: None,
                purge_shards: Some(false),
            }),
        )
        .await
        .expect("delete one version");
        assert_eq!(delete_one.0.deleted_versions, 1);

        let after_one = list_n2_object_versions(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), destination_key.clone())),
            AxumQuery(ListN2VersionsQuery { limit: Some(10) }),
        )
        .await
        .expect("list versions after one delete");
        assert_eq!(after_one.0.versions.len(), 1);

        let delete_all = delete_n2_object(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), destination_key.clone())),
            AxumQuery(DeleteN2ObjectQuery {
                version_id: None,
                expected_epoch: None,
                purge_shards: Some(false),
            }),
        )
        .await
        .expect("delete all versions");
        assert_eq!(delete_all.0.deleted_versions, 1);

        let after_all = list_n2_objects(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ListN2ObjectsQuery { limit: Some(100) }),
        )
        .await
        .expect("list n2 objects after deletes")
        .0;
        assert!(
            after_all
                .objects
                .iter()
                .all(|row| row.object_key != destination_key)
        );

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_reconcile_cleans_orphan_shards_when_map_has_stale_entries() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        let fragment = format!("frag-{}", Ulid::new());
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");

        let object_key = "videos/reconcile-orphan.bin".to_string();
        let payload = sample_bytes(1_000_000, 55);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put local-only object");

        let map = load_n2_object_map_local(
            fragment.as_str(),
            object_key.as_str(),
            Some(put.0.version_id.as_str()),
        )
        .await
        .expect("load map");
        let original_cshard =
            read_local_cluster_shard(fragment.as_str(), put.0.version_id.as_str(), 0)
                .await
                .expect("read original cshard");
        let stale_ref = store_local_cluster_shard_from_bytes(
            original_cshard.as_slice(),
            &map.object,
            1,
            map.params.kd,
            map.params.md,
            map.stripe_block_size,
        )
        .await
        .expect("store stale cshard");
        let mut stale_map = map.clone();
        stale_map.shards.push(stale_ref);
        stale_map.shards.sort_by_key(|s| s.cluster_shard_index);
        persist_n2_object_map(&stale_map)
            .await
            .expect("persist stale map");

        let stale_before =
            read_local_cluster_shard(fragment.as_str(), put.0.version_id.as_str(), 1)
                .await
                .expect("stale shard should exist before reconcile");
        assert!(!stale_before.is_empty());

        let reconcile = reconcile_n2_fragment(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ReconcileFragmentQuery {
                max_objects: Some(10),
                dry_run: Some(false),
                expected_epoch: None,
                cleanup_orphans: Some(true),
            }),
        )
        .await
        .expect("reconcile fragment")
        .0;
        assert_eq!(reconcile.failed_objects, 0);
        assert_eq!(reconcile.updated_objects, 0);
        assert_eq!(reconcile.results.len(), 1);
        assert_eq!(reconcile.results[0].cleanup_failures, 0);
        assert!(reconcile.results[0].cleaned_orphans > 0);

        let stale_after =
            read_local_cluster_shard(fragment.as_str(), put.0.version_id.as_str(), 1).await;
        assert!(
            matches!(stale_after, Err(AppError::NotFound)),
            "stale shard should be removed after reconcile cleanup"
        );

        let response = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read reconciled object")
        .into_response();
        let got = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read reconciled body");
        assert_eq!(got.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_read_survives_single_node_loss_in_k2_m1_layout() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let (remote_a_endpoint, remote_a_server) = spawn_internal_session_server().await;
        let (remote_b_endpoint, remote_b_server) = spawn_internal_session_server().await;
        let previous_membership = get_membership().await.expect("current membership");

        let nodes = vec![
            local_node_descriptor(),
            NodeDescriptor {
                node_id: format!("remote-a-{}", Ulid::new()),
                endpoint: remote_a_endpoint.clone(),
                epoch: 0,
                active: true,
            },
            NodeDescriptor {
                node_id: format!("remote-b-{}", Ulid::new()),
                endpoint: remote_b_endpoint.clone(),
                epoch: 0,
                active: true,
            },
        ];
        upsert_membership(UpsertMembershipRequest { epoch: None, nodes })
            .await
            .expect("set three-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/node-loss.bin".to_string();
        let payload = sample_bytes(2_000_000, 77);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(2),
                mc: Some(1),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(256 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put k2+m1 object");
        assert_eq!(put.0.committed_shards, 3);

        remote_a_server.abort();

        let response = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read after one remote node loss")
        .into_response();
        let got = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert_eq!(got.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
        remote_b_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn n2_five_node_two_loss_rejoin_reconcile_roundtrip() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let (remote_a_endpoint, remote_a_server) = spawn_internal_session_server().await;
        let (remote_b_endpoint, remote_b_server) = spawn_internal_session_server().await;
        let (remote_c_endpoint, remote_c_server) = spawn_internal_session_server().await;
        let (remote_d_endpoint, remote_d_server) = spawn_internal_session_server().await;

        let remote_a_id = format!("remote-a-{}", Ulid::new());
        let remote_b_id = format!("remote-b-{}", Ulid::new());
        let remote_c_id = format!("remote-c-{}", Ulid::new());
        let remote_d_id = format!("remote-d-{}", Ulid::new());
        let previous_membership = get_membership().await.expect("current membership");

        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: remote_a_id.clone(),
                    endpoint: remote_a_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_b_id.clone(),
                    endpoint: remote_b_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_c_id.clone(),
                    endpoint: remote_c_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_d_id.clone(),
                    endpoint: remote_d_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("set five-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/five-node-loss-rejoin.bin".to_string();
        let payload = sample_bytes(3_300_000, 117);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(3),
                mc: Some(2),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(256 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put k3+m2 object");
        assert_eq!(put.0.required_quorum, 3);
        assert_eq!(put.0.committed_shards, 5);

        // Simulate two-node outage in a k=3,m=2 layout.
        remote_a_server.abort();
        remote_b_server.abort();

        let before_rejoin = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read after two-node loss")
        .into_response();
        let before_rejoin_bytes = to_bytes(before_rejoin.into_body(), usize::MAX)
            .await
            .expect("read body before rejoin");
        assert_eq!(before_rejoin_bytes.as_ref(), payload.as_slice());

        // Rejoin with replacement nodes and reconcile to new placement epoch.
        let (remote_e_endpoint, remote_e_server) = spawn_internal_session_server().await;
        let (remote_f_endpoint, remote_f_server) = spawn_internal_session_server().await;
        let remote_e_id = format!("remote-e-{}", Ulid::new());
        let remote_f_id = format!("remote-f-{}", Ulid::new());

        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: remote_c_id.clone(),
                    endpoint: remote_c_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_d_id.clone(),
                    endpoint: remote_d_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_e_id.clone(),
                    endpoint: remote_e_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_f_id.clone(),
                    endpoint: remote_f_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("set five-node membership after rejoin");

        let reconcile = reconcile_n2_fragment(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ReconcileFragmentQuery {
                max_objects: Some(10),
                dry_run: Some(false),
                expected_epoch: None,
                cleanup_orphans: Some(true),
            }),
        )
        .await
        .expect("reconcile after membership change")
        .0;
        assert_eq!(reconcile.failed_objects, 0);
        assert_eq!(reconcile.scanned_objects, 1);
        assert_eq!(reconcile.updated_objects, 1);
        assert_eq!(reconcile.results.len(), 1);
        assert_eq!(reconcile.results[0].status, "reconciled");
        assert!(reconcile.results[0].error.is_none());
        assert!(reconcile.results[0].planned_shards > 0);
        assert!(reconcile.results[0].repaired_shards > 0);
        assert!(reconcile.results[0].target_epoch >= reconcile.results[0].old_epoch);

        let map = load_n2_object_map_local(
            fragment.as_str(),
            object_key.as_str(),
            Some(put.0.version_id.as_str()),
        )
        .await
        .expect("load map after reconcile");
        assert_eq!(map.epoch, reconcile.results[0].target_epoch);
        assert_eq!(map.shards.len(), 5);
        let mut shard_indexes = map
            .shards
            .iter()
            .map(|shard| shard.cluster_shard_index)
            .collect::<Vec<_>>();
        shard_indexes.sort_unstable();
        assert_eq!(shard_indexes, vec![0, 1, 2, 3, 4]);

        let after_reconcile = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read after rejoin+reconcile")
        .into_response();
        let after_reconcile_bytes = to_bytes(after_reconcile.into_body(), usize::MAX)
            .await
            .expect("read body after reconcile");
        assert_eq!(after_reconcile_bytes.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;

        remote_c_server.abort();
        remote_d_server.abort();
        remote_e_server.abort();
        remote_f_server.abort();
    }
}
