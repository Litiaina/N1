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
use crate::distribution::root_routing::{ordered_local_roots, writable_local_roots};
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

mod fragment_control;
mod metadata;
mod shard_access;
mod shard_transfer;
mod usage;

pub use fragment_control::*;
pub use metadata::*;
pub use shard_access::*;
pub use shard_transfer::*;
pub use usage::*;

use fragment_control::{peer_fragment_control, peer_fragment_snapshot, peer_fragment_sync};
#[cfg(test)]
use metadata::{
    ensure_exact_head_visibility, ensure_exact_pool_visibility, exact_metadata_node_batches,
};
use shard_access::{native_peer_response_error, read_native_peer_response};
#[cfg(test)]
use shard_access::{open_local_shard_for_read, peer_shard_url};

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
    /// Standard multipart publishes the stable shard name immediately but
    /// defers file/directory fsync until the single finalize boundary.
    pub deferred_durability: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerObjectMetadataRequest {
    pub target_slot_id: String,
    pub target_instance_id: String,
    pub binding_epoch: u64,
    pub metadata: ObjectMetadata,
    #[serde(default)]
    pub sync_shards_before_publish: bool,
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
    #[serde(default)]
    pub deferred_durability: bool,
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

#[cfg(test)]
#[path = "peer/tests.rs"]
mod tests;
