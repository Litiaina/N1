use crate::core::checksum::Sha256Digest;
use crate::core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{
    FinalizeIncompleteResponse, InitiateUploadParams, InitiateUploadResponse, ObjectMetadata,
    ObjectPathManager, PartMetadata, ShardInfo, UploadPartQuery, UploadStatusResponse,
};
use crate::core::ec::ErasureStreamWriter;
use crate::core::index;
use crate::core::upload::{
    ActiveSessionInfo, FinalizeUploadResult, ListUploadsResponse, UploadSessionManifest,
};
use crate::core::upload::{DirectWriter, create_direct_file};
use crate::distribution::membership::{active_cluster_runtime, pool_write_nodes};
use crate::distribution::peer::{
    PeerShardTarget, RemoteShardWriter, encode_peer_shard_locator, invalidate_local_usage_cache,
    publish_cluster_object_metadata, publish_cluster_object_metadata_after_shard_sync,
};
use crate::distribution::placement::{ClusterPlacementPlan, build_placement_plan};
use crate::distribution::root_routing::writable_local_roots;
use crate::distribution::{
    ClusterEcLayout, resolve_configured_layout, resolve_survival_write_layout,
};
use crate::enums::app_error_enum::AppError;
use crate::io_control::record_io_latency;
use crate::settings::config::CONFIG;
use crate::structs::auth_structs::Claims;
use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::future::join_all;
use serde::Serialize;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::fs;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::warn;
use ulid::Ulid;

mod durable;
mod manifest;
mod standard;
mod writer;

pub use durable::*;
pub use manifest::*;
pub use standard::*;
pub use writer::*;

const MIN_DYNAMIC_STRIPE_SIZE: usize = 4 * 1024;
const TINY_OBJECT_STRIPE_4K_LIMIT: u64 = 64 * 1024;
const SMALL_OBJECT_STRIPE_16K_LIMIT: u64 = 1024 * 1024;
pub const CLUSTER_ONE_SHOT_MAX_BYTES: u64 = 16 * 1024 * 1024;

const STANDARD_SESSION_OPEN: u8 = 0;
const STANDARD_SESSION_FINALIZING: u8 = 1;
const STANDARD_SESSION_FINALIZED: u8 = 2;
const STANDARD_SESSION_CANCELLED: u8 = 3;
const STANDARD_PART_EMPTY: u8 = 0;
const STANDARD_PART_UPLOADING: u8 = 1;
const STANDARD_PART_COMPLETE: u8 = 2;

/// Coordinator-local state for N2's speed-first multipart family. DashMap
/// shards only the tiny registry/part maps; no request body, EC work, peer I/O,
/// fsync, NCDX publication, or NCNX publication runs under a map guard.
static STANDARD_UPLOADS: LazyLock<DashMap<String, Arc<StandardUploadSession>>> =
    LazyLock::new(DashMap::new);

#[derive(Debug)]
struct StandardPartSlot {
    state: AtomicU8,
    record: OnceLock<PartMetadata>,
}

impl StandardPartSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(STANDARD_PART_EMPTY),
            record: OnceLock::new(),
        }
    }
}

#[derive(Debug)]
struct StandardUploadSession {
    manifest: UploadSessionManifest,
    logical_path: String,
    lifecycle: AtomicU8,
    in_flight_parts: AtomicUsize,
    last_activity_at: AtomicI64,
    parts: DashMap<u64, Arc<StandardPartSlot>>,
    finalized_metadata: OnceLock<ObjectMetadata>,
}

fn standard_session_key(fragment: &str, version_id: &str) -> String {
    format!("{fragment}\u{001f}{version_id}")
}

fn standard_session(fragment: &str, version_id: &str) -> Option<Arc<StandardUploadSession>> {
    STANDARD_UPLOADS
        .get(&standard_session_key(fragment, version_id))
        .map(|entry| Arc::clone(entry.value()))
}

struct StandardPartClaim {
    session: Arc<StandardUploadSession>,
    slot: Arc<StandardPartSlot>,
    completed: bool,
}

impl StandardPartClaim {
    fn complete(mut self, part: PartMetadata) -> Result<(), AppError> {
        if self.session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_CANCELLED {
            return Err(AppError::Conflict("Upload session was cancelled".into()));
        }
        self.slot
            .record
            .set(part)
            .map_err(|_| AppError::Conflict("Multipart part was completed concurrently".into()))?;
        self.session
            .last_activity_at
            .store(Utc::now().timestamp(), Ordering::Release);
        self.slot
            .state
            .store(STANDARD_PART_COMPLETE, Ordering::Release);
        self.completed = true;
        Ok(())
    }
}

impl Drop for StandardPartClaim {
    fn drop(&mut self) {
        if !self.completed {
            self.slot
                .state
                .store(STANDARD_PART_EMPTY, Ordering::Release);
        }
        self.session.in_flight_parts.fetch_sub(1, Ordering::AcqRel);
    }
}

enum StandardPartClaimResult {
    Claimed(StandardPartClaim),
    AlreadyComplete,
}

fn standard_request_matches(
    session: &StandardUploadSession,
    logical_path: &str,
    params: &InitiateUploadParams,
) -> bool {
    let content_type_matches = params
        .content_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|requested| requested == session.manifest.content_type)
        .unwrap_or(true);
    let part_size_matches = params
        .part_size_bytes
        .filter(|value| *value > 0)
        .map(|requested| requested == session.manifest.part_size_bytes)
        .unwrap_or(true);

    session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_OPEN
        && session.logical_path == logical_path.trim_start_matches('/')
        && session.manifest.total_size_bytes == params.total_size_bytes
        && session.manifest.expected_parts == params.expected_parts
        && part_size_matches
        && content_type_matches
}

fn claim_standard_part(
    session: Arc<StandardUploadSession>,
    part_index: u64,
) -> Result<StandardPartClaimResult, AppError> {
    if part_index >= session.manifest.expected_parts {
        return Err(AppError::BadRequest("Part index out of range".into()));
    }
    if session.lifecycle.load(Ordering::Acquire) != STANDARD_SESSION_OPEN {
        return Err(AppError::Conflict(
            "Multipart session is not accepting parts".into(),
        ));
    }
    let slot = session
        .parts
        .entry(part_index)
        .or_insert_with(|| Arc::new(StandardPartSlot::new()))
        .clone();
    match slot.state.compare_exchange(
        STANDARD_PART_EMPTY,
        STANDARD_PART_UPLOADING,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => {
            session.in_flight_parts.fetch_add(1, Ordering::AcqRel);
            session
                .last_activity_at
                .store(Utc::now().timestamp(), Ordering::Release);
            if session.lifecycle.load(Ordering::Acquire) != STANDARD_SESSION_OPEN {
                slot.state.store(STANDARD_PART_EMPTY, Ordering::Release);
                session.in_flight_parts.fetch_sub(1, Ordering::AcqRel);
                return Err(AppError::Conflict("Multipart session is finalizing".into()));
            }
            Ok(StandardPartClaimResult::Claimed(StandardPartClaim {
                session,
                slot,
                completed: false,
            }))
        }
        Err(STANDARD_PART_COMPLETE) => Ok(StandardPartClaimResult::AlreadyComplete),
        Err(STANDARD_PART_UPLOADING) => Err(AppError::Conflict(
            "Multipart part is already uploading".into(),
        )),
        Err(_) => Err(AppError::Conflict("Multipart part is unavailable".into())),
    }
}

fn validate_cluster_one_shot_size(content_length: u64) -> Result<(), AppError> {
    if content_length > CLUSTER_ONE_SHOT_MAX_BYTES {
        return Err(AppError::PayloadTooLarge(format!(
            "Object too large for one-shot upload; use multipart for objects over {} bytes",
            CLUSTER_ONE_SHOT_MAX_BYTES
        )));
    }
    Ok(())
}

trait EmptyFallback {
    fn if_empty(self, fallback: String) -> String;
}

impl EmptyFallback for String {
    fn if_empty(self, fallback: String) -> String {
        if self.trim().is_empty() {
            fallback
        } else {
            self
        }
    }
}

#[cfg(test)]
#[path = "upload/tests.rs"]
mod tests;
