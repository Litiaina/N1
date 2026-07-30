use crate::core::capacity_manager::{
    FragmentBlobUsage, StorageRootUsage, StorageUsage, get_fragment_blob_usage,
    get_storage_root_usage, get_storage_usage,
};
use crate::core::checksum::{Sha256Stream, format_sha256};
use crate::core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{ObjectMetadata, ObjectPathManager, ShardInfo};
use crate::core::monitoring::system_health_snapshot;
use crate::core::upload::{DirectWriter, create_direct_file};
use crate::core::{index, index_store};
use crate::distribution::layout::ClusterEcLayout;
use crate::distribution::membership::{
    ClusterNode, ClusterRuntime, active_cluster_runtime, try_active_cluster_runtime,
};
use crate::distribution::placement::build_placement_plan;
use crate::enums::app_error_enum::AppError;
use crate::io_control::{
    PeerOperation, acquire_peer_operation, record_capacity_cache_failure,
    record_capacity_cache_hit, record_capacity_cache_invalidation, record_capacity_cache_miss,
    record_capacity_cache_refresh, record_io_latency, record_peer_progress,
    set_capacity_cache_size, usage_cache_ttl_seconds,
};
use crate::protocol::frame::{read_response_body, read_response_control, write_request_control};
use crate::protocol::types::{NativeRequest, NativeResponse};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use axum::{Json, Router};
use bytes::Bytes;
use futures_util::StreamExt;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, ErrorKind, SeekFrom};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration as StdDuration, Instant};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};
use tokio_util::io::ReaderStream;
use ulid::Ulid;

use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;

static PEER_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("build N2 peer HTTP client")
});
static USAGE_CACHE: LazyLock<Mutex<HashMap<String, CachedPeerUsage>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_READ_TIMEOUT: Duration = Duration::from_secs(20);
const PEER_WRITE_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_PEER_MIN_TRANSFER_MIB_PER_SEC: u64 = 4;
const DEFAULT_PEER_TRANSFER_IDLE_TIMEOUT_SECS: u64 = 60;
const MAX_PEER_TRANSFER_TIMEOUT_SECS: u64 = 3600;
const NATIVE_PEER_STREAM_BUFFER_BYTES: usize = 1024 * 1024;

pub(crate) fn peer_http_client() -> Result<reqwest::Client, AppError> {
    Ok(PEER_HTTP_CLIENT.clone())
}

fn endpoint_is_native(endpoint: &str) -> bool {
    endpoint.trim_start().starts_with("n2://")
}

fn native_socket_addr(endpoint: &str) -> Result<String, AppError> {
    let trimmed = endpoint.trim();
    let Some(addr) = trimmed.strip_prefix("n2://") else {
        return Err(AppError::BadRequest(format!(
            "N2 native peer endpoint must start with n2://, got {trimmed}"
        )));
    };
    if addr.trim().is_empty() {
        return Err(AppError::BadRequest(
            "N2 native peer endpoint is empty".into(),
        ));
    }
    Ok(addr.to_string())
}

fn peer_transfer_timeout(bytes: u64) -> Duration {
    let min_mib_per_sec = std::env::var("N2_PEER_MIN_TRANSFER_MIB_PER_SEC")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_PEER_MIN_TRANSFER_MIB_PER_SEC)
        .clamp(1, 1024);
    let min_bytes_per_sec = min_mib_per_sec.saturating_mul(1024 * 1024).max(1);
    let transfer_secs = bytes.div_ceil(min_bytes_per_sec);
    Duration::from_secs(
        20u64
            .saturating_add(transfer_secs)
            .clamp(20, MAX_PEER_TRANSFER_TIMEOUT_SECS),
    )
}

fn peer_transfer_idle_timeout() -> Duration {
    Duration::from_secs(
        std::env::var("N2_PEER_TRANSFER_IDLE_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_PEER_TRANSFER_IDLE_TIMEOUT_SECS)
            .clamp(5, MAX_PEER_TRANSFER_TIMEOUT_SECS),
    )
}

async fn copy_with_idle_timeout<R, W>(
    reader: &mut R,
    writer: &mut W,
    idle_timeout: Duration,
) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0u8; NATIVE_PEER_STREAM_BUFFER_BYTES];
    let mut copied = 0u64;
    loop {
        let read = timeout(idle_timeout, reader.read(&mut buffer))
            .await
            .map_err(|_| io::Error::new(ErrorKind::TimedOut, "peer stream read stalled"))??;
        if read == 0 {
            return Ok(copied);
        }
        timeout(idle_timeout, writer.write_all(&buffer[..read]))
            .await
            .map_err(|_| io::Error::new(ErrorKind::TimedOut, "peer stream write stalled"))??;
        copied = copied.saturating_add(read as u64);
    }
}

#[derive(Clone, Debug)]
pub struct PeerShardTarget {
    pub pool_id: String,
    pub pool_generation: u64,
    /// Stable logical slot.
    pub node_id: String,
    /// Physical instance currently bound to `node_id`.
    pub instance_id: String,
    pub binding_epoch: u64,
    pub root_id: String,
    pub endpoint: String,
    pub fragment: String,
    pub object_id: String,
    pub version_id: String,
    pub part_index: u64,
    pub shard_index: usize,
    pub is_parity: bool,
    pub expected_len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerObjectMetadataRequest {
    pub target_slot_id: String,
    pub target_instance_id: String,
    pub binding_epoch: u64,
    pub metadata: ObjectMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerObjectInventoryRequest {
    pub fragment: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataReplicaObservation {
    pub node_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataReplicaRepairOutcome {
    pub expected_replicas: usize,
    pub repaired_replicas: u64,
    pub healthy: bool,
    pub observations: Vec<MetadataReplicaObservation>,
}

#[derive(Clone, Debug, Default)]
pub struct ClusterObjectInventory {
    pub objects: BTreeMap<String, ObjectMetadata>,
    pub expected_blob_nodes: usize,
    pub responding_blob_nodes: usize,
    pub errors: Vec<String>,
}

impl ClusterObjectInventory {
    pub fn complete(&self) -> bool {
        self.responding_blob_nodes == self.expected_blob_nodes
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerObjectResolveRequest {
    pub fragment: String,
    pub object_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerObjectBatchResolveRequest {
    pub fragment: String,
    pub object_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum PeerFragmentControlRequest {
    Provision {
        fragment_hash: String,
        stable_fragment_id: String,
        key: String,
    },
    Rename {
        current_hash: String,
        new_hash: String,
        stable_fragment_id: String,
    },
    RotateKey {
        fragment_hash: String,
        key: String,
    },
    Delete {
        stable_fragment_id: String,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerUsageQuery {
    #[serde(default)]
    pub fragment: Option<String>,
    #[serde(default)]
    pub refresh: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerUsageResponse {
    pub node_id: String,
    pub capacity: StorageUsage,
    pub roots: Vec<StorageRootUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fragment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fragment_usage: Option<FragmentBlobUsage>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerMonitorQuery {
    #[serde(default)]
    pub fragment: Option<String>,
    #[serde(default)]
    pub include_usage: bool,
    #[serde(default)]
    pub refresh: bool,
    #[serde(default)]
    pub deep_scan: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerMonitorResponse {
    pub node_id: String,
    pub sampled_at_unix: i64,
    pub health: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<PeerUsageResponse>,
}

#[derive(Clone, Debug)]
struct CachedPeerUsage {
    inserted: Instant,
    response: PeerUsageResponse,
}

#[derive(Debug, Deserialize)]
pub struct PeerShardQuery {
    pub pool_id: String,
    pub pool_generation: u64,
    pub slot_id: String,
    pub instance_id: String,
    pub binding_epoch: u64,
    pub root_id: String,
    #[serde(default)]
    pub parity: Option<bool>,
    #[serde(default)]
    pub expected_len: Option<u64>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub length: Option<usize>,
}

pub struct RemoteShardWriter {
    target: PeerShardTarget,
    writer: Option<DuplexStream>,
    handle: tokio::task::JoinHandle<Result<ShardInfo, AppError>>,
}

impl RemoteShardWriter {
    pub fn spawn(target: PeerShardTarget) -> Result<Self, AppError> {
        let permit = acquire_peer_operation(&target.node_id, PeerOperation::Write)?;
        let (writer, reader) = tokio::io::duplex(1024 * 1024);
        let upload_target = target.clone();
        let handle = tokio::spawn(async move {
            let started = Instant::now();
            let label = if endpoint_is_native(&upload_target.endpoint) {
                "peer.write.native"
            } else {
                "peer.write.http"
            };
            let result = if endpoint_is_native(&upload_target.endpoint) {
                native_put_peer_shard(&upload_target, reader).await
            } else {
                http_put_peer_shard(&upload_target, reader).await
            };
            record_io_latency(
                label,
                started.elapsed(),
                upload_target.expected_len,
                result.is_ok(),
            );
            match result {
                Ok(info) => {
                    permit.complete();
                    Ok(info)
                }
                Err(err) => Err(err),
            }
        });
        Ok(Self {
            target,
            writer: Some(writer),
            handle,
        })
    }

    pub async fn finish(mut self) -> Result<ShardInfo, AppError> {
        if let Some(mut writer) = self.writer.take() {
            writer
                .shutdown()
                .await
                .map_err(|e| AppError::Io(e, "shutdown remote N2 shard upload stream".into()))?;
        }
        let mut info = self.handle.await.map_err(|e| {
            AppError::InternalServerError(format!("remote N2 shard writer join failed: {e}"))
        })??;
        info.absolute_path = encode_peer_shard_locator(&self.target);
        Ok(info)
    }
}

pub async fn put_peer_shard_bytes(
    target: PeerShardTarget,
    bytes: &[u8],
) -> Result<ShardInfo, AppError> {
    let mut writer = RemoteShardWriter::spawn(target)?;
    writer
        .write_all(bytes)
        .await
        .map_err(|e| AppError::Io(e, "write repaired N2 peer shard stream".into()))?;
    writer.finish().await
}

async fn http_put_peer_shard(
    target: &PeerShardTarget,
    reader: DuplexStream,
) -> Result<ShardInfo, AppError> {
    let url = peer_shard_url(target);
    let client = peer_http_client()?;
    let body = reqwest::Body::wrap_stream(ReaderStream::new(reader));
    let mut req = client.put(url).body(body);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = req
        .timeout(peer_transfer_timeout(target.expected_len))
        .send()
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("peer shard upload failed: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "peer shard upload returned {}: {}",
            status, text
        )));
    }
    response
        .json::<ShardInfo>()
        .await
        .map_err(|e| AppError::InternalServerError(format!("decode peer shard response: {e}")))
}

async fn native_put_peer_shard(
    target: &PeerShardTarget,
    mut reader: DuplexStream,
) -> Result<ShardInfo, AppError> {
    let addr = native_socket_addr(&target.endpoint)?;
    let mut stream = timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(&addr))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} timed out",
                target.node_id, addr
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} failed: {}",
                target.node_id, addr, e
            ))
        })?;
    let _ = stream.set_nodelay(true);
    let payload = NativePeerShardPayload {
        secret: cluster_secret(),
        pool_id: target.pool_id.clone(),
        pool_generation: target.pool_generation,
        slot_id: target.node_id.clone(),
        instance_id: target.instance_id.clone(),
        binding_epoch: target.binding_epoch,
        fragment: target.fragment.clone(),
        root_id: target.root_id.clone(),
        object_id: target.object_id.clone(),
        version_id: target.version_id.clone(),
        part_index: target.part_index,
        shard_index: target.shard_index,
        parity: target.is_parity,
        expected_len: Some(target.expected_len),
        offset: None,
        length: None,
    };
    let request = NativeRequest {
        id: format!("peer-put-{}", Ulid::new()),
        op: "peer.shard.put".to_string(),
        payload: Some(serde_json::to_string(&payload).map_err(|e| {
            AppError::InternalServerError(format!("encode native peer put payload: {e}"))
        })?),
        body: None,
        body_len: Some(target.expected_len),
    };
    timeout(
        PEER_WRITE_TIMEOUT,
        write_request_control(&mut stream, &request),
    )
    .await
    .map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "write N2 native peer put control to {} timed out",
            target.node_id
        ))
    })?
    .map_err(|e| {
        AppError::ServiceUnavailable(format!("write N2 native peer put control failed: {e}"))
    })?;
    let copied = copy_with_idle_timeout(&mut reader, &mut stream, peer_transfer_idle_timeout())
        .await
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("stream N2 native peer shard failed: {e}"))
        })?;
    if copied != target.expected_len {
        return Err(AppError::ServiceUnavailable(format!(
            "stream N2 native peer shard to {} ended after {} bytes, expected {}",
            target.node_id, copied, target.expected_len
        )));
    }
    timeout(PEER_WRITE_TIMEOUT, stream.shutdown())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "shutdown N2 native peer shard stream to {} timed out",
                target.node_id
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "shutdown N2 native peer shard stream failed: {e}"
            ))
        })?;
    let response = timeout(
        peer_transfer_idle_timeout(),
        read_native_peer_response(&mut stream),
    )
    .await
    .map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "read N2 native peer put response from {} timed out",
            target.node_id
        ))
    })??;
    if !response.ok {
        return Err(native_peer_response_error("put", target, &response));
    }
    let payload = response.payload.as_deref().ok_or_else(|| {
        AppError::InternalServerError("N2 native peer put response missing payload".into())
    })?;
    serde_json::from_str::<ShardInfo>(payload).map_err(|e| {
        AppError::InternalServerError(format!("decode N2 native peer put response: {e}"))
    })
}

impl AsyncWrite for RemoteShardWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "peer closed"))?;
        match Pin::new(writer).poll_write(cx, buf) {
            Poll::Ready(Ok(bytes)) => {
                record_peer_progress(&self.target.node_id, PeerOperation::Write, bytes as u64);
                Poll::Ready(Ok(bytes))
            }
            other => other,
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "peer closed"))?;
        Pin::new(writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(writer) = self.writer.as_mut() {
            Pin::new(writer).poll_shutdown(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

pub(crate) fn cluster_secret() -> Option<String> {
    std::env::var("N2_CLUSTER_SECRET")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn authorize_peer(headers: &HeaderMap) -> Result<(), AppError> {
    let Some(secret) = cluster_secret() else {
        return Ok(());
    };
    let provided = headers
        .get("x-n2-cluster-secret")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided == secret {
        Ok(())
    } else {
        Err(AppError::Forbidden("N2 peer secret mismatch".into()))
    }
}

async fn post_object_metadata(
    node: &ClusterNode,
    binding_epoch: u64,
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    let client = peer_http_client()?;
    let url = format!(
        "{}/n2/peer/objects/publish",
        node.endpoint.trim_end_matches('/')
    );
    let mut request = client.post(url).json(&PeerObjectMetadataRequest {
        target_slot_id: node.id.clone(),
        target_instance_id: node.instance_id.clone(),
        binding_epoch,
        metadata: metadata.clone(),
    });
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = request
        .timeout(PEER_WRITE_TIMEOUT)
        .send()
        .await
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "publish N2 object metadata to {}: {err}",
                node.endpoint
            ))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "publish N2 object metadata to {} returned {status}: {body}",
            node.endpoint
        )));
    }
    Ok(())
}

fn validate_metadata_pool_contract(
    runtime: &ClusterRuntime,
    metadata: &ObjectMetadata,
) -> Result<BTreeSet<String>, AppError> {
    let pool = runtime.membership.pool(&metadata.pool_id).ok_or_else(|| {
        AppError::Conflict(format!(
            "object metadata references unknown server pool '{}'",
            metadata.pool_id
        ))
    })?;
    if pool.generation != metadata.pool_generation
        || pool.policy.object_data_shards != usize::from(metadata.data_shards)
        || pool.policy.object_parity_shards != usize::from(metadata.parity_shards)
    {
        return Err(AppError::Conflict(
            "object metadata does not match its immutable server-pool EC contract".into(),
        ));
    }
    let mut participant_ids = BTreeSet::new();
    for shard in metadata.parts.iter().flat_map(|part| part.shards.iter()) {
        let locator = parse_peer_shard_locator(&shard.absolute_path).ok_or_else(|| {
            AppError::InternalServerError(format!(
                "N2 committed shard {} has no native placement locator",
                shard.file_name
            ))
        })?;
        if !pool
            .node_ids
            .iter()
            .any(|node_id| node_id == &locator.node_id)
        {
            return Err(AppError::Conflict(format!(
                "object shard references node '{}' outside pool '{}'",
                locator.node_id, pool.id
            )));
        }
        participant_ids.insert(locator.node_id);
    }
    if participant_ids.is_empty() {
        return Err(AppError::InternalServerError(
            "N2 object metadata has no shard-bearing participants".into(),
        ));
    }
    let required_width = pool.policy.object_width();
    if participant_ids.len() != required_width {
        return Err(AppError::Conflict(format!(
            "object metadata participants do not match pool '{}' static {}+{} layout",
            pool.id, pool.policy.object_data_shards, pool.policy.object_parity_shards
        )));
    }
    for part in &metadata.parts {
        let mut part_nodes = BTreeSet::new();
        let mut shard_indices = BTreeSet::new();
        for shard in &part.shards {
            let locator = parse_peer_shard_locator(&shard.absolute_path).ok_or_else(|| {
                AppError::Conflict(format!(
                    "object part {} shard '{}' has no exact peer locator",
                    part.part_index, shard.file_name
                ))
            })?;
            part_nodes.insert(locator.node_id);
            shard_indices.insert(shard.shard_index);
        }
        if part.shards.len() != required_width
            || part_nodes != participant_ids
            || shard_indices != (0..required_width).collect::<BTreeSet<_>>()
        {
            return Err(AppError::Conflict(format!(
                "object part {} does not contain exactly one indexed shard on each of {} pool participants",
                part.part_index, required_width
            )));
        }
    }
    Ok(participant_ids)
}

pub async fn publish_cluster_object_metadata(metadata: &ObjectMetadata) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let participant_ids = validate_metadata_pool_contract(&runtime, metadata)?;

    let required_metadata_writes = usize::from(metadata.data_shards);
    let mut tasks = futures_util::stream::FuturesUnordered::new();
    for node_id in participant_ids {
        let node = runtime
            .membership
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .cloned()
            .ok_or_else(|| {
                AppError::InternalServerError(format!(
                    "N2 shard-bearing participant {node_id} is absent from membership"
                ))
            })?;
        let metadata = metadata.clone();
        let local_node_id = runtime.local_node_id.clone();
        let binding_epoch = runtime.membership.binding_epoch;
        tasks.push(async move {
            let participant = node_id.clone();
            if node_id == local_node_id {
                index::publish_object(&metadata).await.map(|_| participant)
            } else {
                post_object_metadata(&node, binding_epoch, &metadata)
                    .await
                    .map(|_| participant)
            }
        });
    }

    let mut successes = 0usize;
    let mut failures = Vec::new();
    while let Some(result) = tasks.next().await {
        match result {
            Ok(_) => successes = successes.saturating_add(1),
            Err(err) => failures.push(err.to_string()),
        }
    }
    if successes < required_metadata_writes {
        return Err(AppError::ServiceUnavailable(format!(
            "survival NCDX publication failed for {}/{} version {}: {} successful, K={} required; {}",
            metadata.fragment,
            metadata.object_key,
            metadata.version_id,
            successes,
            required_metadata_writes,
            failures.join("; ")
        )));
    }
    Ok(())
}

fn object_metadata_bytes(metadata: &ObjectMetadata) -> Result<Vec<u8>, AppError> {
    rmp_serde::to_vec_named(metadata).map_err(AppError::from)
}

fn metadata_participant_nodes(metadata: &ObjectMetadata) -> Result<BTreeSet<String>, AppError> {
    let mut nodes = BTreeSet::new();
    for shard in metadata.parts.iter().flat_map(|part| part.shards.iter()) {
        let locator = parse_peer_shard_locator(&shard.absolute_path).ok_or_else(|| {
            AppError::Conflict(format!(
                "object metadata shard '{}' has no exact peer locator",
                shard.file_name
            ))
        })?;
        nodes.insert(locator.node_id);
    }
    if nodes.is_empty() && metadata.total_size_bytes != 0 {
        return Err(AppError::Conflict(
            "non-empty object metadata has no shard-bearing participants".into(),
        ));
    }
    Ok(nodes)
}

async fn metadata_on_node(
    runtime: &ClusterRuntime,
    node: &ClusterNode,
    metadata: &ObjectMetadata,
) -> Result<Option<ObjectMetadata>, AppError> {
    let request = PeerObjectResolveRequest {
        fragment: metadata.fragment.clone(),
        object_id: metadata.object_key.clone(),
        version_id: Some(metadata.version_id.clone()),
    };
    let records = if node.id == runtime.local_node_id {
        local_object_metadata(&request)?
    } else {
        post_object_resolve(&node.endpoint, &request).await?
    };
    Ok(records.into_iter().next())
}

async fn repair_metadata_on_node(
    runtime: &ClusterRuntime,
    node: &ClusterNode,
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    if node.id == runtime.local_node_id {
        index::publish_object(metadata).await.map(|_| ())
    } else {
        post_object_metadata(node, runtime.membership.binding_epoch, metadata).await
    }
}

/// Verify and restore the immutable NCDX metadata copy on every exact
/// shard-bearing participant. Divergent immutable metadata is never replaced.
pub async fn inspect_and_repair_metadata_replicas(
    metadata: &ObjectMetadata,
) -> Result<MetadataReplicaRepairOutcome, AppError> {
    let runtime = active_cluster_runtime()?;
    let participant_ids = metadata_participant_nodes(metadata)?;
    let expected_bytes = object_metadata_bytes(metadata)?;
    let mut observations = Vec::with_capacity(participant_ids.len());
    let mut missing = Vec::new();
    for node_id in &participant_ids {
        let node = runtime
            .membership
            .nodes
            .iter()
            .find(|node| node.id == *node_id)
            .ok_or_else(|| {
                AppError::Conflict(format!(
                    "object metadata participant '{node_id}' is absent from membership"
                ))
            })?;
        match metadata_on_node(&runtime, node, metadata).await {
            Ok(Some(found)) if object_metadata_bytes(&found)? == expected_bytes => {
                observations.push(MetadataReplicaObservation {
                    node_id: node_id.clone(),
                    status: "healthy".into(),
                    error: None,
                });
            }
            Ok(Some(_)) => observations.push(MetadataReplicaObservation {
                node_id: node_id.clone(),
                status: "divergent".into(),
                error: Some("immutable NCDX metadata differs".into()),
            }),
            Ok(None) => {
                observations.push(MetadataReplicaObservation {
                    node_id: node_id.clone(),
                    status: "missing".into(),
                    error: None,
                });
                missing.push(node.clone());
            }
            Err(error) => {
                observations.push(MetadataReplicaObservation {
                    node_id: node_id.clone(),
                    status: "unavailable".into(),
                    error: Some(error.to_string()),
                });
                missing.push(node.clone());
            }
        }
    }

    let mut repaired_replicas = 0u64;
    let mut repaired_nodes = BTreeSet::new();
    for node in missing {
        if repair_metadata_on_node(&runtime, &node, metadata)
            .await
            .is_ok()
        {
            repaired_replicas = repaired_replicas.saturating_add(1);
            repaired_nodes.insert(node.id.clone());
        }
    }

    let mut healthy = true;
    let mut final_observations = Vec::with_capacity(participant_ids.len());
    for node_id in participant_ids {
        let node = runtime
            .membership
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .expect("metadata participant membership checked");
        match metadata_on_node(&runtime, node, metadata).await {
            Ok(Some(found)) if object_metadata_bytes(&found)? == expected_bytes => {
                final_observations.push(MetadataReplicaObservation {
                    status: if repaired_nodes.contains(&node_id) {
                        "repaired"
                    } else {
                        "healthy"
                    }
                    .into(),
                    node_id,
                    error: None,
                });
            }
            Ok(Some(_)) => {
                healthy = false;
                final_observations.push(MetadataReplicaObservation {
                    node_id,
                    status: "divergent".into(),
                    error: Some("immutable NCDX metadata differs after repair".into()),
                });
            }
            Ok(None) => {
                healthy = false;
                final_observations.push(MetadataReplicaObservation {
                    node_id,
                    status: "missing".into(),
                    error: Some("NCDX metadata remained missing after repair".into()),
                });
            }
            Err(error) => {
                healthy = false;
                final_observations.push(MetadataReplicaObservation {
                    node_id,
                    status: "unavailable".into(),
                    error: Some(error.to_string()),
                });
            }
        }
    }

    Ok(MetadataReplicaRepairOutcome {
        expected_replicas: final_observations.len(),
        repaired_replicas,
        healthy,
        observations: final_observations,
    })
}

async fn post_object_inventory(
    endpoint: &str,
    fragment: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let client = peer_http_client()?;
    let mut request = client
        .post(format!(
            "{}/n2/peer/objects/inventory",
            endpoint.trim_end_matches('/')
        ))
        .json(&PeerObjectInventoryRequest {
            fragment: fragment.to_string(),
        });
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = request
        .timeout(PEER_READ_TIMEOUT)
        .send()
        .await
        .map_err(|error| {
            AppError::ServiceUnavailable(format!(
                "read N2 object inventory from {endpoint}: {error}"
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 object inventory peer {endpoint} returned {}",
            response.status()
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!(
            "decode N2 object inventory from {endpoint}: {error}"
        ))
    })
}

pub async fn collect_cluster_object_inventory(
    fragment: &str,
) -> Result<ClusterObjectInventory, AppError> {
    let runtime = active_cluster_runtime()?;
    let nodes = runtime.membership.nodes.clone();
    let expected_blob_nodes = nodes.len();
    let local_id = runtime.local_node_id.clone();
    let results = futures_util::future::join_all(nodes.into_iter().map(|node| {
        let fragment = fragment.to_string();
        let local_id = local_id.clone();
        async move {
            let result = if node.id == local_id {
                index::list_fragment_live_objects(&fragment).await
            } else {
                post_object_inventory(&node.endpoint, &fragment).await
            };
            (node.id, result)
        }
    }))
    .await;
    let mut inventory = ClusterObjectInventory {
        expected_blob_nodes,
        ..ClusterObjectInventory::default()
    };
    for (node_id, result) in results {
        let records = match result {
            Ok(records) => {
                inventory.responding_blob_nodes = inventory.responding_blob_nodes.saturating_add(1);
                records
            }
            Err(error) => {
                inventory.errors.push(format!("{node_id}: {error}"));
                continue;
            }
        };
        for record in records {
            match inventory.objects.get(&record.object_key) {
                Some(existing) if existing.version_id == record.version_id => {
                    if object_metadata_bytes(existing)? != object_metadata_bytes(&record)? {
                        return Err(AppError::Conflict(format!(
                            "divergent immutable NCDX inventory for object {} version {}",
                            record.object_key, record.version_id
                        )));
                    }
                }
                Some(existing) if existing.version_id > record.version_id => {}
                _ => {
                    inventory.objects.insert(record.object_key.clone(), record);
                }
            }
        }
    }
    Ok(inventory)
}

pub async fn peer_publish_object_metadata(
    headers: HeaderMap,
    Json(request): Json<PeerObjectMetadataRequest>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve
        || request.binding_epoch != runtime.membership.binding_epoch
        || request.target_slot_id != runtime.local_node_id
        || request.target_instance_id != runtime.local_instance_id
    {
        return Err(AppError::Conflict(
            "stale or misbound NCDX publication request".into(),
        ));
    }
    validate_metadata_pool_contract(&runtime, &request.metadata)?;
    index::publish_object(&request.metadata).await?;
    Ok(StatusCode::CREATED)
}

pub async fn peer_object_inventory(
    headers: HeaderMap,
    Json(request): Json<PeerObjectInventoryRequest>,
) -> Result<Json<Vec<ObjectMetadata>>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        index::list_fragment_live_objects(&request.fragment).await?,
    ))
}

fn local_object_metadata(
    request: &PeerObjectResolveRequest,
) -> Result<Vec<ObjectMetadata>, AppError> {
    if let Some(version_id) = request.version_id.as_deref() {
        return Ok(index_store::read_live_version(
            &request.fragment,
            &request.object_id,
            version_id,
        )?
        .into_iter()
        .collect());
    }
    index_store::list_live_versions(&request.fragment, &request.object_id)
}

fn local_object_metadata_heads(
    request: &PeerObjectBatchResolveRequest,
) -> Result<Vec<ObjectMetadata>, AppError> {
    if request.object_ids.len() > 10_000 {
        return Err(AppError::BadRequest(
            "object metadata batch exceeds 10000 ids".into(),
        ));
    }
    let mut out = Vec::new();
    for object_id in &request.object_ids {
        if let Some(metadata) = index_store::read_current_live_head(&request.fragment, object_id)? {
            out.push(metadata);
        }
    }
    Ok(out)
}

async fn post_object_resolve(
    endpoint: &str,
    request: &PeerObjectResolveRequest,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let client = peer_http_client()?;
    let url = format!("{}/n2/peer/objects/resolve", endpoint.trim_end_matches('/'));
    let mut builder = client.post(url).json(request);
    if let Some(secret) = cluster_secret() {
        builder = builder.header("x-n2-cluster-secret", secret);
    }
    let response = builder
        .timeout(PEER_READ_TIMEOUT)
        .send()
        .await
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "resolve N2 object metadata from {endpoint}: {err}"
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "resolve N2 object metadata from {endpoint} returned {}",
            response.status()
        )));
    }
    response.json::<Vec<ObjectMetadata>>().await.map_err(|err| {
        AppError::InternalServerError(format!("decode N2 object metadata from {endpoint}: {err}"))
    })
}

async fn post_object_batch_resolve(
    endpoint: &str,
    request: &PeerObjectBatchResolveRequest,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let client = peer_http_client()?;
    let url = format!(
        "{}/n2/peer/objects/resolve-batch",
        endpoint.trim_end_matches('/')
    );
    let mut builder = client.post(url).json(request);
    if let Some(secret) = cluster_secret() {
        builder = builder.header("x-n2-cluster-secret", secret);
    }
    let response = builder
        .timeout(PEER_READ_TIMEOUT)
        .send()
        .await
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "resolve N2 object metadata batch from {endpoint}: {err}"
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "resolve N2 object metadata batch from {endpoint} returned {}",
            response.status()
        )));
    }
    response.json::<Vec<ObjectMetadata>>().await.map_err(|err| {
        AppError::InternalServerError(format!(
            "decode N2 object metadata batch from {endpoint}: {err}"
        ))
    })
}

fn merge_immutable_object_record(
    by_object: &mut BTreeMap<String, ObjectMetadata>,
    record: ObjectMetadata,
) -> Result<(), AppError> {
    let object_id = record.object_key.clone();
    match by_object.get(&object_id) {
        Some(existing)
            if existing.version_id == record.version_id
                && object_metadata_bytes(existing)? != object_metadata_bytes(&record)? =>
        {
            Err(AppError::InternalServerError(format!(
                "divergent immutable N2 metadata for object {} version {}",
                object_id, record.version_id
            )))
        }
        Some(existing) if existing.version_id >= record.version_id => Ok(()),
        _ => {
            by_object.insert(object_id, record);
            Ok(())
        }
    }
}

fn exact_metadata_node_batches(
    runtime: &ClusterRuntime,
    fragment: &str,
    object_ids: &[String],
) -> Vec<(ClusterNode, Vec<String>)> {
    let readable = runtime.membership.readable_nodes();
    if readable.is_empty() {
        return Vec::new();
    }
    let readable_by_id = readable
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect::<BTreeMap<_, _>>();
    let mut per_node = BTreeMap::<String, Vec<String>>::new();

    for object_id in object_ids {
        for pool in &runtime.membership.pools {
            let nodes = runtime.membership.nodes_for_pool(&pool.id);
            let layout = ClusterEcLayout {
                data_shards: pool.policy.object_data_shards,
                parity_shards: pool.policy.object_parity_shards,
            };
            if let Ok(plan) = build_placement_plan(
                fragment,
                object_id,
                "",
                &pool.id,
                pool.generation,
                runtime.membership.epoch,
                runtime.membership.binding_epoch,
                layout,
                &nodes,
            ) {
                for placement in plan.placements {
                    if readable_by_id.contains_key(&placement.node_id) {
                        per_node
                            .entry(placement.node_id)
                            .or_default()
                            .push(object_id.clone());
                    }
                }
            }
        }
    }

    per_node
        .into_iter()
        .filter_map(|(node_id, ids)| {
            readable_by_id
                .get(&node_id)
                .cloned()
                .map(|node| (node, ids))
        })
        .collect()
}

async fn resolve_object_heads_from_batches(
    runtime: &ClusterRuntime,
    fragment: &str,
    batches: Vec<(ClusterNode, Vec<String>)>,
) -> Result<(BTreeMap<String, ObjectMetadata>, BTreeSet<(String, String)>), AppError> {
    let mut tasks = futures_util::stream::FuturesUnordered::new();
    for (node, object_ids) in batches {
        if object_ids.is_empty() {
            continue;
        }
        let request = PeerObjectBatchResolveRequest {
            fragment: fragment.to_string(),
            object_ids,
        };
        let local_node_id = runtime.local_node_id.clone();
        tasks.push(async move {
            let result = if node.id == local_node_id {
                local_object_metadata_heads(&request)
            } else {
                post_object_batch_resolve(&node.endpoint, &request).await
            };
            (node, request.object_ids, result)
        });
    }

    let mut by_object = BTreeMap::new();
    let mut proven = BTreeSet::new();
    while let Some((node, object_ids, result)) = tasks.next().await {
        let Ok(records) = result else {
            continue;
        };
        let pool_id = runtime
            .membership
            .pool_for_node(&node.id)
            .map(|pool| pool.id.clone())
            .ok_or_else(|| {
                AppError::Conflict(format!(
                    "metadata source node '{}' has no server pool",
                    node.id
                ))
            })?;
        for object_id in object_ids {
            proven.insert((pool_id.clone(), object_id));
        }
        for record in records {
            validate_metadata_pool_contract(runtime, &record)?;
            merge_immutable_object_record(&mut by_object, record)?;
        }
    }
    Ok((by_object, proven))
}

fn ensure_exact_head_visibility(
    runtime: &ClusterRuntime,
    object_ids: &[String],
    proven: &BTreeSet<(String, String)>,
) -> Result<(), AppError> {
    let unproven = runtime
        .membership
        .pools
        .iter()
        .flat_map(|pool| {
            object_ids
                .iter()
                .map(move |object_id| (pool.id.clone(), object_id.clone()))
        })
        .filter(|pair| !proven.contains(pair))
        .count();
    if unproven > 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "exact NCDX visibility is unproven for {unproven} server-pool/object pair(s)"
        )));
    }
    Ok(())
}

fn ensure_exact_pool_visibility(
    runtime: &ClusterRuntime,
    proven_pools: &BTreeSet<String>,
) -> Result<(), AppError> {
    if runtime
        .membership
        .pools
        .iter()
        .any(|pool| !proven_pools.contains(&pool.id))
    {
        return Err(AppError::ServiceUnavailable(
            "exact NCDX visibility is unproven for one or more server pools".into(),
        ));
    }
    Ok(())
}

pub async fn resolve_cluster_object_heads(
    fragment: &str,
    object_ids: &[String],
) -> Result<BTreeMap<String, ObjectMetadata>, AppError> {
    if object_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    if object_ids.len() > 10_000 {
        return Err(AppError::BadRequest(
            "object metadata batch exceeds 10000 ids".into(),
        ));
    }
    let request = PeerObjectBatchResolveRequest {
        fragment: fragment.to_string(),
        object_ids: object_ids.to_vec(),
    };
    let Some(runtime) = crate::distribution::membership::try_active_cluster_runtime() else {
        return local_object_metadata_heads(&request).map(|records| {
            records
                .into_iter()
                .map(|record| (record.object_key.clone(), record))
                .collect()
        });
    };

    let exact = exact_metadata_node_batches(&runtime, fragment, object_ids);
    let (by_object, proven) = resolve_object_heads_from_batches(&runtime, fragment, exact).await?;
    ensure_exact_head_visibility(&runtime, object_ids, &proven)?;
    Ok(by_object)
}

async fn resolve_object_versions_from_nodes(
    runtime: &ClusterRuntime,
    request: &PeerObjectResolveRequest,
    nodes: Vec<ClusterNode>,
) -> Result<(BTreeMap<String, ObjectMetadata>, BTreeSet<String>), AppError> {
    let mut tasks = futures_util::stream::FuturesUnordered::new();
    for node in nodes {
        let request = request.clone();
        let local_node_id = runtime.local_node_id.clone();
        tasks.push(async move {
            let result = if node.id == local_node_id {
                local_object_metadata(&request)
            } else {
                post_object_resolve(&node.endpoint, &request).await
            };
            (node, result)
        });
    }

    let mut by_version = BTreeMap::<String, ObjectMetadata>::new();
    let mut proven_pools = BTreeSet::new();
    while let Some((node, result)) = tasks.next().await {
        let Ok(records) = result else {
            continue;
        };
        let pool_id = runtime
            .membership
            .pool_for_node(&node.id)
            .map(|pool| pool.id.clone())
            .ok_or_else(|| {
                AppError::Conflict(format!(
                    "metadata source node '{}' has no server pool",
                    node.id
                ))
            })?;
        proven_pools.insert(pool_id);
        for record in records {
            validate_metadata_pool_contract(runtime, &record)?;
            match by_version.get(&record.version_id) {
                Some(existing)
                    if object_metadata_bytes(existing)? != object_metadata_bytes(&record)? =>
                {
                    return Err(AppError::InternalServerError(format!(
                        "divergent immutable N2 metadata for object {} version {}",
                        request.object_id, record.version_id
                    )));
                }
                Some(_) => {}
                None => {
                    by_version.insert(record.version_id.clone(), record);
                }
            }
        }
    }
    Ok((by_version, proven_pools))
}

pub async fn resolve_cluster_object_versions(
    fragment: &str,
    object_id: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let request = PeerObjectResolveRequest {
        fragment: fragment.to_string(),
        object_id: object_id.to_string(),
        version_id: None,
    };
    let Some(runtime) = crate::distribution::membership::try_active_cluster_runtime() else {
        return local_object_metadata(&request);
    };
    let exact_nodes = exact_metadata_node_batches(&runtime, fragment, &[object_id.to_string()])
        .into_iter()
        .map(|(node, _)| node)
        .collect::<Vec<_>>();
    let (by_version, proven_pools) =
        resolve_object_versions_from_nodes(&runtime, &request, exact_nodes).await?;
    ensure_exact_pool_visibility(&runtime, &proven_pools)?;
    Ok(by_version.into_values().collect())
}

pub async fn resolve_cluster_object_version(
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let request = PeerObjectResolveRequest {
        fragment: fragment.to_string(),
        object_id: object_id.to_string(),
        version_id: Some(version_id.to_string()),
    };
    let Some(runtime) = crate::distribution::membership::try_active_cluster_runtime() else {
        return Ok(local_object_metadata(&request)?.into_iter().next());
    };

    let exact_nodes = exact_metadata_node_batches(&runtime, fragment, &[object_id.to_string()])
        .into_iter()
        .map(|(node, _)| node)
        .collect::<Vec<_>>();
    let (mut by_version, proven_pools) =
        resolve_object_versions_from_nodes(&runtime, &request, exact_nodes).await?;
    ensure_exact_pool_visibility(&runtime, &proven_pools)?;
    Ok(by_version.remove(version_id))
}

pub async fn peer_resolve_object_metadata(
    headers: HeaderMap,
    Json(request): Json<PeerObjectResolveRequest>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_object_metadata(&request)?))
}

pub async fn peer_resolve_object_metadata_batch(
    headers: HeaderMap,
    Json(request): Json<PeerObjectBatchResolveRequest>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_object_metadata_heads(&request)?))
}

async fn apply_fragment_control(request: &PeerFragmentControlRequest) -> Result<(), AppError> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    match request {
        PeerFragmentControlRequest::Provision {
            fragment_hash,
            stable_fragment_id,
            key,
        } => {
            crate::core::fragment_manager::provision_fragment_local(
                fragment_hash,
                stable_fragment_id,
                key,
            )
            .await
        }
        PeerFragmentControlRequest::Rename {
            current_hash,
            new_hash,
            stable_fragment_id,
        } => {
            crate::core::fragment_manager::rename_fragment_alias_local(
                current_hash,
                new_hash,
                stable_fragment_id,
            )
            .await
        }
        PeerFragmentControlRequest::RotateKey { fragment_hash, key } => {
            crate::core::fragment_manager::rotate_fragment_key_local(fragment_hash, key).await
        }
        PeerFragmentControlRequest::Delete { stable_fragment_id } => {
            crate::core::fragment_manager::delete_fragment_local(stable_fragment_id)
        }
    }
}

pub async fn replicate_fragment_control(
    request: PeerFragmentControlRequest,
) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let client = peer_http_client()?;
    for node in runtime
        .membership
        .nodes
        .iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        let mut builder = client
            .post(format!(
                "{}/n2/peer/fragments/control",
                node.endpoint.trim_end_matches('/')
            ))
            .json(&request);
        if let Some(secret) = cluster_secret() {
            builder = builder.header("x-n2-cluster-secret", secret);
        }
        let response = timeout(Duration::from_secs(20), builder.send())
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable(format!("fragment control timed out at {}", node.id))
            })?
            .map_err(|err| {
                AppError::ServiceUnavailable(format!(
                    "fragment control failed at {}: {err}",
                    node.id
                ))
            })?;
        if !response.status().is_success() {
            return Err(AppError::ServiceUnavailable(format!(
                "fragment control returned {} from {}",
                response.status(),
                node.id
            )));
        }
    }
    apply_fragment_control(&request).await
}

async fn peer_fragment_snapshot(headers: HeaderMap) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        crate::core::fragment_manager::local_fragment_control_snapshot().await?,
    ))
}

pub async fn sync_fragment_controls_from_cluster() -> Result<usize, AppError> {
    if crate::core::operation_gate::is_cluster_safety_read_only() {
        return Ok(0);
    }
    let runtime = active_cluster_runtime()?;
    let client = peer_http_client()?;
    let mut by_alias =
        BTreeMap::<String, crate::core::fragment_manager::FragmentControlSnapshot>::new();

    for node in runtime
        .membership
        .nodes
        .iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        let mut request = client.get(format!(
            "{}/n2/peer/fragments/snapshot",
            node.endpoint.trim_end_matches('/')
        ));
        if let Some(secret) = cluster_secret() {
            request = request.header("x-n2-cluster-secret", secret);
        }
        let response = match timeout(Duration::from_secs(10), request.send()).await {
            Ok(Ok(response)) if response.status().is_success() => response,
            _ => continue,
        };
        let snapshots = response
            .json::<Vec<crate::core::fragment_manager::FragmentControlSnapshot>>()
            .await
            .map_err(|err| {
                AppError::InternalServerError(format!(
                    "decode fragment control snapshot from {}: {err}",
                    node.id
                ))
            })?;
        for snapshot in snapshots {
            if let Some(existing) = by_alias.get(&snapshot.fragment_hash)
                && existing.metadata.fragment != snapshot.metadata.fragment
            {
                return Err(AppError::Conflict(format!(
                    "fragment alias {} resolves to conflicting stable identities {} and {}",
                    snapshot.fragment_hash, existing.metadata.fragment, snapshot.metadata.fragment
                )));
            }
            by_alias
                .entry(snapshot.fragment_hash.clone())
                .or_insert(snapshot);
        }
    }

    let mut applied = 0usize;
    for snapshot in by_alias.into_values() {
        if let Ok(local) =
            crate::core::fragment_metadata::read_fragment_metadata(&snapshot.fragment_hash).await
            && local.fragment == snapshot.metadata.fragment
            && local.updated >= snapshot.metadata.updated
        {
            continue;
        }
        crate::core::fragment_manager::apply_fragment_control_snapshot_local(&snapshot).await?;
        applied += 1;
    }
    Ok(applied)
}

#[derive(Clone, Debug, Serialize)]
struct PeerFragmentSyncResponse {
    applied: usize,
}

async fn peer_fragment_sync(headers: HeaderMap) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(PeerFragmentSyncResponse {
        applied: sync_fragment_controls_from_cluster().await?,
    }))
}

pub fn spawn_fragment_control_sync_task() {
    tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            match sync_fragment_controls_from_cluster().await {
                Ok(0) => {}
                Ok(count) => {
                    tracing::debug!("synchronized {count} N2 fragment control record(s)")
                }
                Err(err) => tracing::warn!("fragment control synchronization failed: {err}"),
            }
        }
    });
}

async fn peer_fragment_control(
    headers: HeaderMap,
    Json(request): Json<PeerFragmentControlRequest>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    apply_fragment_control(&request).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn local_peer_usage_snapshot(
    fragment: Option<String>,
) -> Result<PeerUsageResponse, AppError> {
    local_peer_usage_snapshot_refresh(fragment, false).await
}

pub async fn local_peer_usage_snapshot_refresh(
    fragment: Option<String>,
    refresh: bool,
) -> Result<PeerUsageResponse, AppError> {
    let key = usage_cache_key(&fragment);
    let ttl = StdDuration::from_secs(usage_cache_ttl_seconds());
    if !refresh && let Ok(cache) = USAGE_CACHE.lock() {
        publish_usage_cache_size(&cache);
        if let Some(cached) = cache.get(&key)
            && cached.inserted.elapsed() < ttl
        {
            record_capacity_cache_hit();
            return Ok(cached.response.clone());
        }
    }
    record_capacity_cache_miss();
    let started = Instant::now();
    let result = collect_local_peer_usage(fragment.clone()).await;
    record_io_latency("capacity.local_usage", started.elapsed(), 0, result.is_ok());
    match result {
        Ok(response) => {
            if let Ok(mut cache) = USAGE_CACHE.lock() {
                cache.insert(
                    key,
                    CachedPeerUsage {
                        inserted: Instant::now(),
                        response: response.clone(),
                    },
                );
                publish_usage_cache_size(&cache);
            }
            record_capacity_cache_refresh();
            Ok(response)
        }
        Err(err) => {
            record_capacity_cache_failure();
            Err(err)
        }
    }
}

async fn collect_local_peer_usage(fragment: Option<String>) -> Result<PeerUsageResponse, AppError> {
    let runtime = active_cluster_runtime()?;
    let capacity = get_storage_usage().await?;
    let roots = get_storage_root_usage().await?;
    let fragment_usage = match fragment.as_deref() {
        Some(value) if !value.trim().is_empty() => Some(get_fragment_blob_usage(value).await?),
        _ => None,
    };
    Ok(PeerUsageResponse {
        node_id: runtime.local_node_id.clone(),
        capacity,
        roots,
        fragment,
        fragment_usage,
    })
}

fn usage_cache_key(fragment: &Option<String>) -> String {
    fragment
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("__cluster__")
        .to_string()
}

fn publish_usage_cache_size(cache: &HashMap<String, CachedPeerUsage>) {
    set_capacity_cache_size(cache.len(), 0);
}

pub fn invalidate_local_usage_cache(_fragment: Option<&str>) {
    if let Ok(mut cache) = USAGE_CACHE.lock() {
        cache.clear();
        publish_usage_cache_size(&cache);
    }
    record_capacity_cache_invalidation();
}

pub async fn local_peer_monitor_snapshot(
    state: Arc<AppState>,
    fragment: Option<String>,
    include_usage: bool,
    refresh: bool,
    deep_scan: bool,
) -> Result<PeerMonitorResponse, AppError> {
    let runtime = active_cluster_runtime()?;
    let claims = Claims {
        fragment: fragment.clone().unwrap_or_default(),
        fragment_alias: None,
        read: true,
        write: false,
        modify: false,
        delete: false,
        exp: usize::MAX,
    };
    let mut params = std::collections::HashMap::new();
    if let Some(fragment) = fragment.as_deref().filter(|value| !value.trim().is_empty()) {
        params.insert("fragment".to_string(), fragment.to_string());
    }
    if refresh {
        params.insert("refresh".to_string(), "true".to_string());
    }
    if deep_scan {
        params.insert("deep_scan".to_string(), "true".to_string());
    }
    let health = system_health_snapshot(state, claims, params).await?;
    let sampled_at_unix = health.sampled_at;
    let health = serde_json::to_value(health).map_err(|err| {
        AppError::InternalServerError(format!("encode N2 peer monitor snapshot: {err}"))
    })?;
    let usage = if include_usage {
        Some(local_peer_usage_snapshot_refresh(fragment, refresh).await?)
    } else {
        None
    };
    Ok(PeerMonitorResponse {
        node_id: runtime.local_node_id.clone(),
        sampled_at_unix,
        health,
        usage,
    })
}

fn encode_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn decode_segment(value: &str) -> Option<String> {
    percent_decode_str(value)
        .decode_utf8()
        .ok()
        .map(|v| v.to_string())
}

#[derive(Clone, Debug)]
pub struct PeerShardLocator {
    pub pool_id: String,
    pub pool_generation: u64,
    pub node_id: String,
    pub instance_id: String,
    pub binding_epoch: u64,
    pub root_id: String,
    pub endpoint: String,
    pub fragment: String,
    pub object_id: String,
    pub version_id: String,
    pub part_index: u64,
    pub shard_index: usize,
    pub is_parity: bool,
}

pub fn encode_peer_shard_locator(target: &PeerShardTarget) -> String {
    format!(
        "n2-peer-v3|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        encode_segment(&target.pool_id),
        target.pool_generation,
        encode_segment(&target.node_id),
        encode_segment(&target.root_id),
        encode_segment(&target.fragment),
        encode_segment(&target.object_id),
        encode_segment(&target.version_id),
        target.part_index,
        target.shard_index,
        target.is_parity
    )
}

pub fn parse_peer_shard_locator(value: &str) -> Option<PeerShardLocator> {
    let mut parts = value.split('|');
    if parts.next()? != "n2-peer-v3" {
        return None;
    }
    let pool_id = decode_segment(parts.next()?)?;
    let pool_generation = parts.next()?.parse().ok()?;
    let node_id = decode_segment(parts.next()?)?;
    let root_id = decode_segment(parts.next()?)?;
    let fragment = decode_segment(parts.next()?)?;
    let object_id = decode_segment(parts.next()?)?;
    let version_id = decode_segment(parts.next()?)?;
    let part_index = parts.next()?.parse().ok()?;
    let shard_index = parts.next()?.parse().ok()?;
    let is_parity = parts.next()?.parse().ok()?;
    let (instance_id, binding_epoch, endpoint) = try_active_cluster_runtime()
        .and_then(|runtime| {
            let pool = runtime.membership.pool(&pool_id)?;
            if pool.generation != pool_generation
                || !pool.node_ids.iter().any(|slot| slot == &node_id)
            {
                return None;
            }
            let binding = runtime
                .membership
                .nodes
                .iter()
                .find(|node| node.id == node_id)?;
            let endpoint = binding
                .native_endpoint
                .clone()
                .unwrap_or_else(|| binding.endpoint.clone());
            Some((
                binding.instance_id.clone(),
                runtime.membership.binding_epoch,
                endpoint,
            ))
        })
        .unwrap_or_else(|| (String::new(), 0, String::new()));
    Some(PeerShardLocator {
        pool_id,
        pool_generation,
        node_id,
        instance_id,
        binding_epoch,
        root_id,
        endpoint,
        fragment,
        object_id,
        version_id,
        part_index,
        shard_index,
        is_parity,
    })
}

pub fn resolve_peer_shard_locator(
    locator: &PeerShardLocator,
) -> Result<PeerShardLocator, AppError> {
    let runtime = active_cluster_runtime()?;
    let pool = runtime.membership.pool(&locator.pool_id).ok_or_else(|| {
        AppError::Conflict(format!(
            "recorded shard references unknown pool '{}'",
            locator.pool_id
        ))
    })?;
    if pool.generation != locator.pool_generation
        || !pool.node_ids.iter().any(|slot| slot == &locator.node_id)
    {
        return Err(AppError::Conflict(format!(
            "recorded shard logical slot '{}' is not in pool '{}' generation {}",
            locator.node_id, locator.pool_id, locator.pool_generation
        )));
    }
    let binding = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.id == locator.node_id)
        .ok_or_else(|| {
            AppError::ServiceUnavailable(format!(
                "logical slot '{}' is currently unbound",
                locator.node_id
            ))
        })?;
    if !binding
        .storage_roots
        .iter()
        .any(|root| root.id == locator.root_id)
    {
        return Err(AppError::Conflict(format!(
            "logical root '{}' is not present on slot '{}'",
            locator.root_id, locator.node_id
        )));
    }
    let mut resolved = locator.clone();
    resolved.instance_id = binding.instance_id.clone();
    resolved.binding_epoch = runtime.membership.binding_epoch;
    resolved.endpoint = binding
        .native_endpoint
        .clone()
        .unwrap_or_else(|| binding.endpoint.clone());
    Ok(resolved)
}

fn peer_shard_url(target: &PeerShardTarget) -> String {
    format!(
        "{}/n2/peer/shards/{}/{}/{}/{}/{}?pool_id={}&pool_generation={}&slot_id={}&instance_id={}&binding_epoch={}&root_id={}&parity={}&expected_len={}",
        target.endpoint.trim_end_matches('/'),
        encode_segment(&target.fragment),
        encode_segment(&target.object_id),
        encode_segment(&target.version_id),
        target.part_index,
        target.shard_index,
        encode_segment(&target.pool_id),
        target.pool_generation,
        encode_segment(&target.node_id),
        encode_segment(&target.instance_id),
        target.binding_epoch,
        encode_segment(&target.root_id),
        target.is_parity,
        target.expected_len
    )
}

fn peer_shard_read_url(locator: &PeerShardLocator, offset: u64, length: usize) -> String {
    format!(
        "{}/n2/peer/shards/{}/{}/{}/{}/{}?pool_id={}&pool_generation={}&slot_id={}&instance_id={}&binding_epoch={}&root_id={}&parity={}&offset={}&length={}",
        locator.endpoint.trim_end_matches('/'),
        encode_segment(&locator.fragment),
        encode_segment(&locator.object_id),
        encode_segment(&locator.version_id),
        locator.part_index,
        locator.shard_index,
        encode_segment(&locator.pool_id),
        locator.pool_generation,
        encode_segment(&locator.node_id),
        encode_segment(&locator.instance_id),
        locator.binding_epoch,
        encode_segment(&locator.root_id),
        locator.is_parity,
        offset,
        length
    )
}

pub async fn fetch_peer_shard_chunk(
    locator: &PeerShardLocator,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, AppError> {
    let locator = resolve_peer_shard_locator(locator)?;
    let permit = acquire_peer_operation(&locator.node_id, PeerOperation::Read)?;
    let started = Instant::now();
    let label = if endpoint_is_native(&locator.endpoint) {
        "peer.read.native"
    } else {
        "peer.read.http"
    };
    let result = if endpoint_is_native(&locator.endpoint) {
        native_get_peer_shard(&locator, offset, length).await
    } else {
        http_get_peer_shard(&locator, offset, length).await
    };
    let bytes_len = result.as_ref().map(|bytes| bytes.len() as u64).unwrap_or(0);
    record_io_latency(label, started.elapsed(), bytes_len, result.is_ok());
    match result {
        Ok(bytes) => {
            permit.record_progress(bytes.len() as u64);
            permit.complete();
            Ok(bytes)
        }
        Err(err) => Err(err),
    }
}

async fn http_get_peer_shard(
    locator: &PeerShardLocator,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, AppError> {
    let client = peer_http_client()?;
    let mut req = client.get(peer_shard_read_url(locator, offset, length));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = req.timeout(PEER_READ_TIMEOUT).send().await.map_err(|e| {
        AppError::ServiceUnavailable(format!(
            "peer shard read failed from {}: {e}",
            locator.node_id
        ))
    })?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(AppError::NotFound);
        }
        return Err(AppError::ServiceUnavailable(format!(
            "peer shard read from {} returned {}: {}",
            locator.node_id, status, text
        )));
    }
    timeout(PEER_READ_TIMEOUT, response.bytes())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "peer shard body read timed out from {}",
                locator.node_id
            ))
        })?
        .map(|bytes| bytes.to_vec())
        .map_err(|e| AppError::ServiceUnavailable(format!("read peer shard body: {e}")))
}

async fn native_get_peer_shard(
    locator: &PeerShardLocator,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, AppError> {
    let addr = native_socket_addr(&locator.endpoint)?;
    let mut stream = timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(&addr))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} timed out",
                locator.node_id, addr
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} failed: {}",
                locator.node_id, addr, e
            ))
        })?;
    let _ = stream.set_nodelay(true);
    let payload = NativePeerShardPayload {
        secret: cluster_secret(),
        pool_id: locator.pool_id.clone(),
        pool_generation: locator.pool_generation,
        slot_id: locator.node_id.clone(),
        instance_id: locator.instance_id.clone(),
        binding_epoch: locator.binding_epoch,
        fragment: locator.fragment.clone(),
        root_id: locator.root_id.clone(),
        object_id: locator.object_id.clone(),
        version_id: locator.version_id.clone(),
        part_index: locator.part_index,
        shard_index: locator.shard_index,
        parity: locator.is_parity,
        expected_len: None,
        offset: Some(offset),
        length: Some(length),
    };
    let request = NativeRequest {
        id: format!("peer-get-{}", Ulid::new()),
        op: "peer.shard.get".to_string(),
        payload: Some(serde_json::to_string(&payload).map_err(|e| {
            AppError::InternalServerError(format!("encode native peer get payload: {e}"))
        })?),
        body: None,
        body_len: None,
    };
    timeout(
        PEER_WRITE_TIMEOUT,
        write_request_control(&mut stream, &request),
    )
    .await
    .map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "write N2 native peer get control to {} timed out",
            locator.node_id
        ))
    })?
    .map_err(|e| {
        AppError::ServiceUnavailable(format!("write N2 native peer get control failed: {e}"))
    })?;
    let response = timeout(PEER_READ_TIMEOUT, read_native_peer_response(&mut stream))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "read N2 native peer get response from {} timed out",
                locator.node_id
            ))
        })??;
    if !response.ok {
        return Err(native_peer_locator_response_error(
            "get", locator, &response,
        ));
    }
    let body_len = response.body_len.unwrap_or(0);
    timeout(PEER_READ_TIMEOUT, read_response_body(&mut stream, body_len))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "read N2 native peer shard body from {} timed out",
                locator.node_id
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("read N2 native peer shard body failed: {e}"))
        })
}

async fn read_native_peer_response(stream: &mut TcpStream) -> Result<NativeResponse, AppError> {
    read_response_control(stream)
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("read N2 native peer response: {e}")))?
        .ok_or_else(|| AppError::ServiceUnavailable("N2 native peer closed early".into()))
}

fn native_peer_response_error(
    op: &str,
    target: &PeerShardTarget,
    response: &NativeResponse,
) -> AppError {
    AppError::ServiceUnavailable(format!(
        "N2 native peer shard {op} returned status {:?} from {}: {}",
        response.status,
        target.node_id,
        response
            .error
            .as_ref()
            .map(|err| err.message.as_str())
            .unwrap_or("unknown native peer error")
    ))
}

fn native_peer_locator_response_error(
    op: &str,
    locator: &PeerShardLocator,
    response: &NativeResponse,
) -> AppError {
    if response.status == Some(StatusCode::NOT_FOUND.as_u16()) {
        return AppError::NotFound;
    }
    AppError::ServiceUnavailable(format!(
        "N2 native peer shard {op} returned status {:?} from {}: {}",
        response.status,
        locator.node_id,
        response
            .error
            .as_ref()
            .map(|err| err.message.as_str())
            .unwrap_or("unknown native peer error")
    ))
}

fn stable_name(part_index: u64, shard_index: usize, is_parity: bool) -> String {
    let ext = if is_parity {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{part_index}_shard_{shard_index}{ext}")
}

fn shard_dir(
    root_id: &str,
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<std::path::PathBuf, AppError> {
    let runtime = active_cluster_runtime()?;
    let root = runtime
        .local_storage_roots
        .iter()
        .find(|root| root.id == root_id)
        .ok_or_else(|| {
            AppError::ServiceUnavailable(format!(
                "blob root '{}' is not configured on node '{}'",
                root_id, runtime.local_node_id
            ))
        })?;
    let pm = ObjectPathManager::new();
    Ok(pm.get_shard_dir_on_disk(
        &std::path::PathBuf::from(&root.path),
        fragment,
        object_id,
        version_id,
    ))
}

fn validate_peer_binding(
    pool_id: &str,
    pool_generation: u64,
    slot_id: &str,
    instance_id: &str,
    binding_epoch: u64,
    root_id: &str,
) -> Result<Arc<ClusterRuntime>, AppError> {
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve {
        return Err(AppError::ServiceUnavailable(format!(
            "reserve instance '{}' is not bound to a logical slot",
            runtime.local_instance_id
        )));
    }
    if binding_epoch != runtime.membership.binding_epoch {
        return Err(AppError::Conflict(format!(
            "stale N2 binding epoch {binding_epoch}; current epoch is {}",
            runtime.membership.binding_epoch
        )));
    }
    let pool = runtime
        .membership
        .pool(pool_id)
        .ok_or_else(|| AppError::Conflict(format!("unknown server pool '{pool_id}'")))?;
    if pool.generation != pool_generation
        || !pool.node_ids.iter().any(|candidate| candidate == slot_id)
    {
        return Err(AppError::Conflict(format!(
            "logical slot '{slot_id}' is not part of pool '{pool_id}' generation {pool_generation}"
        )));
    }
    let binding = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.id == slot_id)
        .ok_or_else(|| AppError::Conflict(format!("logical slot '{slot_id}' is unbound")))?;
    if binding.instance_id != instance_id
        || runtime.local_instance_id != instance_id
        || runtime.local_node_id != slot_id
        || binding.binding_epoch > binding_epoch
    {
        return Err(AppError::Conflict(format!(
            "logical slot '{slot_id}' is not bound to physical instance '{instance_id}'"
        )));
    }
    if !binding.storage_roots.iter().any(|root| root.id == root_id) {
        return Err(AppError::Conflict(format!(
            "logical root '{root_id}' is not bound on slot '{slot_id}'"
        )));
    }
    Ok(runtime)
}

pub async fn peer_put_shard(
    headers: HeaderMap,
    Path((fragment, object_id, version_id, part_index, shard_index)): Path<(
        String,
        String,
        String,
        u64,
        usize,
    )>,
    Query(query): Query<PeerShardQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    validate_peer_binding(
        &query.pool_id,
        query.pool_generation,
        &query.slot_id,
        &query.instance_id,
        query.binding_epoch,
        &query.root_id,
    )?;
    let started = Instant::now();
    let is_parity = query.parity.unwrap_or(false);
    let expected_len = query.expected_len.unwrap_or(0);
    let dir = shard_dir(&query.root_id, &fragment, &object_id, &version_id)?;
    fs::create_dir_all(&dir)
        .await
        .map_err(|e| AppError::Io(e, format!("create peer shard dir {}", dir.display())))?;
    let file_name = stable_name(part_index, shard_index, is_parity);
    let tmp_path = dir.join(format!("{file_name}.tmp.{}", Ulid::new()));
    let stable_path = dir.join(&file_name);
    let file = create_direct_file(&tmp_path).await?;
    let mut file = DirectWriter::new(file);
    let mut stream = body.into_data_stream();
    let mut hasher = Sha256Stream::new();
    let mut written = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read peer shard body".into(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        written = written.saturating_add(chunk.len() as u64);
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| AppError::Io(e, format!("write peer shard {}", tmp_path.display())))?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, format!("flush peer shard {}", tmp_path.display())))?;
    if expected_len > 0 && written != expected_len {
        drop(file);
        let _ = fs::remove_file(&tmp_path).await;
        return Err(AppError::BadRequest(format!(
            "peer shard length mismatch: got {}, expected {}",
            written, expected_len
        )));
    }
    file.sync_data()
        .await
        .map_err(|e| AppError::Io(e, format!("sync peer shard {}", tmp_path.display())))?;
    drop(file);
    if fs::try_exists(&stable_path).await.unwrap_or(false) {
        let _ = fs::remove_file(&stable_path).await;
    }
    fs::rename(&tmp_path, &stable_path).await.map_err(|e| {
        AppError::Io(
            e,
            format!(
                "publish peer shard {} -> {}",
                tmp_path.display(),
                stable_path.display()
            ),
        )
    })?;
    crate::core::upload::sync_dir(&dir).await?;
    let info = ShardInfo {
        shard_index,
        file_name,
        absolute_path: stable_path.to_string_lossy().to_string(),
        size_bytes: written,
        checksum: format_sha256(&hasher.finalize()),
        block_checksums: Vec::new(),
        checksum_block_size: 0,
        is_parity,
    };
    invalidate_local_usage_cache(Some(&fragment));
    record_io_latency("peer.local_shard_put", started.elapsed(), written, true);
    Ok((StatusCode::CREATED, Json(info)))
}

pub async fn peer_get_shard(
    headers: HeaderMap,
    Path((fragment, object_id, version_id, part_index, shard_index)): Path<(
        String,
        String,
        String,
        u64,
        usize,
    )>,
    Query(query): Query<PeerShardQuery>,
) -> Result<Response, AppError> {
    authorize_peer(&headers)?;
    validate_peer_binding(
        &query.pool_id,
        query.pool_generation,
        &query.slot_id,
        &query.instance_id,
        query.binding_epoch,
        &query.root_id,
    )?;
    let dir = shard_dir(&query.root_id, &fragment, &object_id, &version_id)?;
    let file_name = stable_name(part_index, shard_index, query.parity.unwrap_or(false));
    let path = dir.join(file_name);
    let mut file = fs::File::open(&path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppError::NotFound
        } else {
            AppError::Io(e, format!("open peer shard {}", path.display()))
        }
    })?;
    if let Some(length) = query.length {
        let offset = query.offset.unwrap_or(0);
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| AppError::Io(e, format!("seek peer shard {}", path.display())))?;
        let mut buf = vec![0u8; length];
        let mut read = 0usize;
        while read < length {
            let n = file.read(&mut buf[read..]).await.map_err(|e| {
                AppError::Io(e, format!("read peer shard range {}", path.display()))
            })?;
            if n == 0 {
                break;
            }
            read += n;
        }
        buf.truncate(read);
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(buf))
            .unwrap());
    }
    let stream = ReaderStream::new(file);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::from_stream(stream.map(|result| {
            result.map(Bytes::from).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("peer shard read: {e}"))
            })
        })))
        .unwrap())
}

pub async fn peer_usage(
    headers: HeaderMap,
    Query(query): Query<PeerUsageQuery>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        local_peer_usage_snapshot_refresh(query.fragment, query.refresh).await?,
    ))
}

pub async fn peer_storage_readiness(headers: HeaderMap) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        crate::core::capacity_manager::get_storage_readiness().await?,
    ))
}

pub async fn peer_monitor(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<PeerMonitorQuery>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        local_peer_monitor_snapshot(
            state,
            query.fragment,
            query.include_usage,
            query.refresh,
            query.deep_scan,
        )
        .await?,
    ))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativePeerShardPayload {
    #[serde(default)]
    pub secret: Option<String>,
    pub pool_id: String,
    pub pool_generation: u64,
    pub slot_id: String,
    pub instance_id: String,
    pub binding_epoch: u64,
    pub fragment: String,
    pub root_id: String,
    pub object_id: String,
    pub version_id: String,
    pub part_index: u64,
    pub shard_index: usize,
    #[serde(default)]
    pub parity: bool,
    #[serde(default)]
    pub expected_len: Option<u64>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub length: Option<usize>,
}

fn authorize_peer_secret(secret: Option<&str>) -> Result<(), AppError> {
    let Some(expected) = cluster_secret() else {
        return Ok(());
    };
    if secret.unwrap_or_default() == expected {
        Ok(())
    } else {
        Err(AppError::Forbidden("N2 peer secret mismatch".into()))
    }
}

pub async fn native_peer_put_shard_reader<R>(
    payload: NativePeerShardPayload,
    reader: &mut R,
    body_len: u64,
) -> Result<ShardInfo, AppError>
where
    R: AsyncRead + Unpin,
{
    authorize_peer_secret(payload.secret.as_deref())?;
    validate_peer_binding(
        &payload.pool_id,
        payload.pool_generation,
        &payload.slot_id,
        &payload.instance_id,
        payload.binding_epoch,
        &payload.root_id,
    )?;
    let expected_len = payload.expected_len.unwrap_or(body_len);
    let dir = shard_dir(
        &payload.root_id,
        &payload.fragment,
        &payload.object_id,
        &payload.version_id,
    )?;
    fs::create_dir_all(&dir)
        .await
        .map_err(|e| AppError::Io(e, format!("create native peer shard dir {}", dir.display())))?;
    let file_name = stable_name(payload.part_index, payload.shard_index, payload.parity);
    let tmp_path = dir.join(format!("{file_name}.tmp.{}", Ulid::new()));
    let stable_path = dir.join(&file_name);
    let file = create_direct_file(&tmp_path).await?;
    let mut file = DirectWriter::new(file);
    let mut hasher = Sha256Stream::new();
    let mut written = 0u64;
    let mut buf = vec![0u8; 256 * 1024];
    while written < body_len {
        let wanted = ((body_len - written) as usize).min(buf.len());
        let n = timeout(
            peer_transfer_idle_timeout(),
            reader.read(&mut buf[..wanted]),
        )
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "native peer shard body stalled while writing {}",
                tmp_path.display()
            ))
        })?
        .map_err(|e| {
            AppError::Io(
                e,
                format!("read native peer shard body {}", tmp_path.display()),
            )
        })?;
        if n == 0 {
            break;
        }
        written = written.saturating_add(n as u64);
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n]).await.map_err(|e| {
            AppError::Io(e, format!("write native peer shard {}", tmp_path.display()))
        })?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, format!("flush native peer shard {}", tmp_path.display())))?;
    if written != body_len || (expected_len > 0 && written != expected_len) {
        drop(file);
        let _ = fs::remove_file(&tmp_path).await;
        return Err(AppError::BadRequest(format!(
            "native peer shard length mismatch: got {}, body_len {}, expected {}",
            written, body_len, expected_len
        )));
    }
    file.sync_data()
        .await
        .map_err(|e| AppError::Io(e, format!("sync native peer shard {}", tmp_path.display())))?;
    drop(file);
    if fs::try_exists(&stable_path).await.unwrap_or(false) {
        let _ = fs::remove_file(&stable_path).await;
    }
    fs::rename(&tmp_path, &stable_path).await.map_err(|e| {
        AppError::Io(
            e,
            format!(
                "publish native peer shard {} -> {}",
                tmp_path.display(),
                stable_path.display()
            ),
        )
    })?;
    crate::core::upload::sync_dir(&dir).await?;
    invalidate_local_usage_cache(Some(&payload.fragment));
    Ok(ShardInfo {
        shard_index: payload.shard_index,
        file_name,
        absolute_path: stable_path.to_string_lossy().to_string(),
        size_bytes: written,
        checksum: format_sha256(&hasher.finalize()),
        block_checksums: Vec::new(),
        checksum_block_size: 0,
        is_parity: payload.parity,
    })
}

pub async fn native_peer_get_shard_bytes(
    payload: NativePeerShardPayload,
) -> Result<Vec<u8>, AppError> {
    authorize_peer_secret(payload.secret.as_deref())?;
    validate_peer_binding(
        &payload.pool_id,
        payload.pool_generation,
        &payload.slot_id,
        &payload.instance_id,
        payload.binding_epoch,
        &payload.root_id,
    )?;
    let dir = shard_dir(
        &payload.root_id,
        &payload.fragment,
        &payload.object_id,
        &payload.version_id,
    )?;
    let file_name = stable_name(payload.part_index, payload.shard_index, payload.parity);
    let path = dir.join(file_name);
    let mut file = fs::File::open(&path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppError::NotFound
        } else {
            AppError::Io(e, format!("open native peer shard {}", path.display()))
        }
    })?;
    if let Some(length) = payload.length {
        file.seek(SeekFrom::Start(payload.offset.unwrap_or(0)))
            .await
            .map_err(|e| AppError::Io(e, format!("seek native peer shard {}", path.display())))?;
        let mut buf = vec![0u8; length];
        let mut read = 0usize;
        while read < length {
            let n = file.read(&mut buf[read..]).await.map_err(|e| {
                AppError::Io(
                    e,
                    format!("read native peer shard range {}", path.display()),
                )
            })?;
            if n == 0 {
                break;
            }
            read += n;
        }
        buf.truncate(read);
        return Ok(buf);
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .await
        .map_err(|e| AppError::Io(e, format!("read native peer shard {}", path.display())))?;
    Ok(buf)
}

pub fn native_peer_bytes_response(id: String, bytes: Vec<u8>) -> NativeResponse {
    NativeResponse {
        id,
        ok: true,
        status: Some(StatusCode::OK.as_u16()),
        payload: Some(json!({"bytes": bytes.len()}).to_string()),
        body_len: Some(bytes.len() as u64),
        body: Some(ByteBuf::from(bytes)),
        error: None,
    }
}

pub fn build_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/usage", axum::routing::get(peer_usage))
        .route(
            "/n2/peer/storage-readiness",
            axum::routing::get(peer_storage_readiness),
        )
        .route("/n2/peer/monitor", axum::routing::get(peer_monitor))
        .route(
            "/n2/peer/objects/publish",
            axum::routing::post(peer_publish_object_metadata),
        )
        .route(
            "/n2/peer/objects/resolve",
            axum::routing::post(peer_resolve_object_metadata),
        )
        .route(
            "/n2/peer/objects/resolve-batch",
            axum::routing::post(peer_resolve_object_metadata_batch),
        )
        .route(
            "/n2/peer/objects/inventory",
            axum::routing::post(peer_object_inventory),
        )
        .route(
            "/n2/peer/fragments/control",
            axum::routing::post(peer_fragment_control),
        )
        .route(
            "/n2/peer/fragments/snapshot",
            axum::routing::get(peer_fragment_snapshot),
        )
        .route(
            "/n2/peer/fragments/sync",
            axum::routing::post(peer_fragment_sync),
        )
        .route(
            "/n2/peer/shards/{:fragment}/{:object_id}/{:version_id}/{:part_index}/{:shard_index}",
            put(peer_put_shard).get(peer_get_shard),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::ClusterLayoutPolicy;
    use crate::distribution::membership::{
        ClusterMembership, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState, PoolState,
        ServerPool, StorageRoot,
    };

    fn metadata_runtime() -> ClusterRuntime {
        let nodes = (0..3)
            .map(|index| ClusterNode {
                id: format!("n{index}"),
                instance_id: format!("n{index}"),
                binding_epoch: 1,
                endpoint: format!("https://n{index}:50001"),
                native_endpoint: Some(format!("n2://n{index}:50002")),
                state: Some(NodeState::Online),
                weight: 1,
                storage_roots: vec![StorageRoot {
                    id: format!("r{index}"),
                    physical_id: None,
                    path: format!("/disk/{index}"),
                }],
                joined_epoch: 1,
                capacity_bytes: None,
                used_bytes: None,
                last_seen_unix: None,
            })
            .collect::<Vec<_>>();
        ClusterRuntime {
            local_node_id: "n0".into(),
            local_instance_id: "n0".into(),
            local_is_reserve: false,
            local_pool_id: "pool-a".into(),
            local_storage_roots: nodes[0].storage_roots.clone(),
            membership_path: std::path::PathBuf::from("/membership"),
            layout_policy: ClusterLayoutPolicy {
                data_shards: 2,
                parity_shards: 1,
            },
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "cluster-a".into(),
                epoch: 1,
                binding_epoch: 1,
                pools: vec![ServerPool {
                    id: "pool-a".into(),
                    generation: 1,
                    policy: ClusterPolicy {
                        object_data_shards: 2,
                        object_parity_shards: 1,
                    },
                    state: PoolState::Writable,
                    node_ids: (0..3).map(|index| format!("n{index}")).collect(),
                }],
                nodes,
                reserve_nodes: Vec::new(),
                retired_instances: Vec::new(),
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn peer_shard_locator_round_trips_endpoint_and_identity() {
        let target = PeerShardTarget {
            pool_id: "pool-a".into(),
            pool_generation: 1,
            node_id: "node-a".into(),
            instance_id: "instance-a".into(),
            binding_epoch: 1,
            root_id: "blob-root-a".into(),
            endpoint: "https://node-a.local:50001".into(),
            fragment: "frag".into(),
            object_id: "object-1".into(),
            version_id: "01KTEST".into(),
            part_index: 7,
            shard_index: 2,
            is_parity: true,
            expected_len: 128,
        };
        let encoded = encode_peer_shard_locator(&target);
        let decoded = parse_peer_shard_locator(&encoded).expect("locator");
        assert_eq!(decoded.node_id, target.node_id);
        assert_eq!(decoded.root_id, target.root_id);
        assert!(decoded.endpoint.is_empty());
        assert_eq!(decoded.pool_id, target.pool_id);
        assert_eq!(decoded.pool_generation, target.pool_generation);
        assert_eq!(decoded.fragment, target.fragment);
        assert_eq!(decoded.object_id, target.object_id);
        assert_eq!(decoded.version_id, target.version_id);
        assert_eq!(decoded.part_index, target.part_index);
        assert_eq!(decoded.shard_index, target.shard_index);
        assert_eq!(decoded.is_parity, target.is_parity);
    }

    #[test]
    fn native_endpoint_detection_is_explicit() {
        assert!(endpoint_is_native("n2://127.0.0.1:50002"));
        assert!(!endpoint_is_native("https://127.0.0.1:50001"));
        assert_eq!(
            native_socket_addr("n2://node-a.local:50002").unwrap(),
            "node-a.local:50002"
        );
    }

    #[test]
    fn peer_transfer_timeout_scales_for_large_shards() {
        let small = peer_transfer_timeout(1024);
        let large = peer_transfer_timeout(1024 * 1024 * 1024);
        assert!(large > small);
        assert!(small >= PEER_WRITE_TIMEOUT);
    }

    #[test]
    fn metadata_lookup_uses_every_exact_static_pool_holder_without_substitution() {
        let mut runtime = metadata_runtime();
        let object_id = "object-a".to_string();
        let pool = runtime.membership.pool("pool-a").unwrap();
        let nodes = runtime.membership.nodes_for_pool("pool-a");
        let plan = build_placement_plan(
            "fragment",
            &object_id,
            "",
            "pool-a",
            1,
            1,
            1,
            ClusterEcLayout {
                data_shards: 2,
                parity_shards: 1,
            },
            &nodes,
        )
        .unwrap();
        let expected = plan
            .placements
            .iter()
            .map(|placement| placement.node_id.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(expected.len(), pool.policy.object_width());

        let selected =
            exact_metadata_node_batches(&runtime, "fragment", std::slice::from_ref(&object_id))
                .into_iter()
                .map(|(node, _)| node.id)
                .collect::<BTreeSet<_>>();
        assert_eq!(selected, expected);

        let failed_holder = expected.iter().next().unwrap().clone();
        runtime
            .membership
            .nodes
            .iter_mut()
            .find(|node| node.id == failed_holder)
            .unwrap()
            .state = Some(NodeState::Offline);
        let degraded =
            exact_metadata_node_batches(&runtime, "fragment", std::slice::from_ref(&object_id))
                .into_iter()
                .map(|(node, _)| node.id)
                .collect::<BTreeSet<_>>();
        assert_eq!(degraded.len(), expected.len() - 1);
        assert!(degraded.is_subset(&expected));
    }

    #[test]
    fn metadata_absence_requires_one_exact_holder_response_per_pool() {
        let runtime = metadata_runtime();
        let object_ids = vec!["object-a".to_string()];
        let proven = BTreeSet::from([("pool-a".to_string(), "object-a".to_string())]);
        ensure_exact_head_visibility(&runtime, &object_ids, &proven).unwrap();
        ensure_exact_pool_visibility(&runtime, &BTreeSet::from(["pool-a".to_string()])).unwrap();
        assert!(ensure_exact_head_visibility(&runtime, &object_ids, &BTreeSet::new()).is_err());
        assert!(ensure_exact_pool_visibility(&runtime, &BTreeSet::new()).is_err());
    }

    #[tokio::test]
    async fn peer_copy_idle_timeout_resets_when_bytes_keep_moving() {
        let (mut sender, mut receiver) = tokio::io::duplex(64);
        let producer = tokio::spawn(async move {
            for value in 0..6u8 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                sender.write_all(&[value]).await.unwrap();
            }
        });
        let mut sink = tokio::io::sink();
        let copied = copy_with_idle_timeout(&mut receiver, &mut sink, Duration::from_millis(50))
            .await
            .unwrap();
        producer.await.unwrap();
        assert_eq!(copied, 6);
    }

    #[tokio::test]
    async fn peer_copy_idle_timeout_rejects_a_stalled_stream() {
        let (_sender, mut receiver) = tokio::io::duplex(64);
        let mut sink = tokio::io::sink();
        let err = copy_with_idle_timeout(&mut receiver, &mut sink, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::TimedOut);
    }
}
