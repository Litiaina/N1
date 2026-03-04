use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::future::poll_fn;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use block_manager::DirectExtentIo;
use chrono::Utc;
use futures_util::StreamExt;
use futures_util::future::join_all;
use once_cell::sync::{Lazy, OnceCell};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::fs as tokio_fs;
use tokio::io::AsyncWrite;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use ulid::Ulid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::blob_store::{
    allocate_raw_direct_io, configured_blob_disk_count, flush_raw_metadata_for_addresses,
    free_raw_address, open_raw_direct_io_for_address, raw_blob_address_from_extent,
    raw_blob_debug_path, stable_shard_file_name,
};
use crate::n1_core::disk_manager::{
    FinalizeIncompleteResponse, InitiateUploadParams, InitiateUploadResponse, ObjectKeyQuery,
    ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt, ShardInfo,
    UploadPartQuery, UploadStatusResponse, configured_fs_storage_root, default_stripe_size,
};
use crate::n1_core::ec::{
    AlignedBuffer, RawErasureStreamWriter, recycle_pooled_aligned_buffer,
    take_pooled_aligned_buffer,
};
use crate::n1_core::index;
use crate::n1_core::quota_manager::{try_reserve_quota, update_quota};
use crate::n1_core::raw_io_ring::{RawIoCompletion, RawIoRing, RawIoSubmitError};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;

pub static MAX_PART_SIZE_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();
pub static DATA_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static PARITY_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static WRITER_BUFFER_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static RAW_WRITER_INFLIGHT_LIMIT_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static STRIPE_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static MAX_IO_SEMAPHORE: OnceCell<usize> = OnceCell::new();
pub static ADAPTIVE_IO_TARGET_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC: OnceCell<AtomicBool> = OnceCell::new();
pub static UPLOAD_IO_GATE_ENABLED_ATOMIC: OnceCell<AtomicBool> = OnceCell::new();

static IO_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(*MAX_IO_SEMAPHORE.get().unwrap()));
static ACTIVE_IO_INFLIGHT: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));
struct AdaptiveIoGuard<'a> {
    _permit: tokio::sync::SemaphorePermit<'a>,
}

impl Drop for AdaptiveIoGuard<'_> {
    fn drop(&mut self) {
        ACTIVE_IO_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn acquire_adaptive_io_guard() -> Result<AdaptiveIoGuard<'static>, AppError> {
    loop {
        let target = ADAPTIVE_IO_TARGET_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or_else(|| *MAX_IO_SEMAPHORE.get().unwrap_or(&128usize))
            .max(1);

        let active = ACTIVE_IO_INFLIGHT.load(Ordering::Relaxed);
        if active < target {
            if ACTIVE_IO_INFLIGHT
                .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    match IO_SEMAPHORE.acquire().await {
        Ok(permit) => Ok(AdaptiveIoGuard { _permit: permit }),
        Err(_) => {
            ACTIVE_IO_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
            Err(AppError::InternalServerError("Server busy".into()))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadDurabilityMode {
    Strict,
    Batched,
}

impl UploadDurabilityMode {
    fn strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

fn parse_upload_durability_mode(query: &UploadPartQuery) -> UploadDurabilityMode {
    if let Some(v) = query.durability.as_deref() {
        match v.trim().to_ascii_lowercase().as_str() {
            "strict" | "sync" | "durable" => return UploadDurabilityMode::Strict,
            "batched" | "async" | "fast" | "relaxed" => return UploadDurabilityMode::Batched,
            _ => {}
        }
    }
    if let Some(strict) = query.strict_durability {
        if strict {
            UploadDurabilityMode::Strict
        } else {
            UploadDurabilityMode::Batched
        }
    } else if UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(false)
    {
        UploadDurabilityMode::Strict
    } else {
        UploadDurabilityMode::Batched
    }
}

#[cfg(test)]
mod tests {
    use super::{UploadDurabilityMode, parse_upload_durability_mode};
    use crate::n1_core::disk_manager::UploadPartQuery;

    #[test]
    fn upload_durability_false_override_forces_batched() {
        let q = UploadPartQuery {
            checksum: None,
            durability: None,
            strict_durability: Some(false),
            io_gate: None,
        };
        assert!(matches!(
            parse_upload_durability_mode(&q),
            UploadDurabilityMode::Batched
        ));
    }

    #[test]
    fn upload_durability_accepts_string_and_bool_override() {
        let q_batched = UploadPartQuery {
            checksum: None,
            durability: Some("batched".to_string()),
            strict_durability: None,
            io_gate: None,
        };
        assert!(matches!(
            parse_upload_durability_mode(&q_batched),
            UploadDurabilityMode::Batched
        ));

        let q_strict = UploadPartQuery {
            checksum: None,
            durability: None,
            strict_durability: Some(true),
            io_gate: None,
        };
        assert!(matches!(
            parse_upload_durability_mode(&q_strict),
            UploadDurabilityMode::Strict
        ));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedShardSlot {
    pub shard_index: usize,
    pub file_name: String,
    pub raw_blob_address: crate::n1_core::disk_manager::RawBlobAddress,
    pub is_parity: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedPartLayout {
    pub part_index: u64,
    pub size_bytes: u64,
    pub shard_size_bytes: u64,
    pub shards: Vec<PlannedShardSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartCommitReceipt {
    pub size_bytes: u64,
    pub created_at: i64,
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub shard_checksums: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSessionManifest {
    pub fragment: String,
    pub object_key: String,
    pub version_id: String,
    pub session_id: String,
    pub total_size_bytes: u64,
    pub expected_parts: u64,
    pub content_type: String,
    pub created_at: i64,
    pub stripe_size_bytes: usize,
    #[serde(default = "default_upload_data_shards")]
    pub data_shards: usize,
    #[serde(default = "default_upload_parity_shards")]
    pub parity_shards: usize,
    #[serde(default)]
    pub planned_parts: Vec<PlannedPartLayout>,
}

fn default_upload_data_shards() -> usize {
    DATA_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed) as usize)
        .unwrap_or(1)
}

fn default_upload_parity_shards() -> usize {
    PARITY_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed) as usize)
        .unwrap_or(0)
}

impl UploadSessionManifest {
    pub fn planned_part_layout(&self, part_index: u64) -> Option<&PlannedPartLayout> {
        self.planned_parts
            .iter()
            .find(|layout| layout.part_index == part_index)
    }

    pub fn all_planned_raw_addresses(
        &self,
    ) -> Vec<crate::n1_core::disk_manager::RawBlobAddress> {
        self.planned_parts
            .iter()
            .flat_map(|layout| layout.shards.iter())
            .map(|shard| shard.raw_blob_address.clone())
            .collect()
    }

    pub fn materialize_part_from_receipt(
        &self,
        part_index: u64,
        receipt: &PartCommitReceipt,
    ) -> Option<PartMetadata> {
        let layout = self.planned_part_layout(part_index)?;
        if layout.shards.len() != receipt.shard_checksums.len() {
            return None;
        }

        let shards = layout
            .shards
            .iter()
            .zip(receipt.shard_checksums.iter())
            .map(|(planned, checksum)| ShardInfo {
                shard_index: planned.shard_index,
                file_name: planned.file_name.clone(),
                absolute_path: raw_blob_debug_path(&planned.raw_blob_address),
                raw_blob_address: Some(planned.raw_blob_address.clone()),
                size_bytes: layout.shard_size_bytes,
                checksum: checksum.clone(),
                is_parity: planned.is_parity,
            })
            .collect();

        Some(PartMetadata {
            part_index,
            size_bytes: receipt.size_bytes,
            created_at: receipt.created_at,
            shards,
            checksum: receipt.checksum.clone(),
        })
    }

}

fn derive_planned_part_sizes(
    params: &InitiateUploadParams,
    max_part_size: u64,
) -> Result<Option<Vec<u64>>, AppError> {
    if let Some(explicit_sizes) = params.part_sizes_bytes.as_ref() {
        if explicit_sizes.is_empty() {
            return Err(AppError::BadRequest(
                "part_sizes_bytes must not be empty".into(),
            ));
        }
        if explicit_sizes.len() as u64 != params.expected_parts {
            return Err(AppError::BadRequest(format!(
                "part_sizes_bytes length {} does not match expected_parts {}",
                explicit_sizes.len(),
                params.expected_parts
            )));
        }
        let total = explicit_sizes.iter().try_fold(0u64, |acc, size| {
            if *size == 0 {
                return None;
            }
            Some(acc.saturating_add(*size))
        });
        let Some(total) = total else {
            return Err(AppError::BadRequest(
                "part_sizes_bytes must contain only positive values".into(),
            ));
        };
        if explicit_sizes.iter().any(|size| *size > max_part_size) {
            return Err(AppError::BadRequest(format!(
                "part_sizes_bytes contains a part larger than max_part_size {}",
                max_part_size
            )));
        }
        if total != params.total_size_bytes {
            return Err(AppError::BadRequest(format!(
                "part_sizes_bytes total {} does not match total_size_bytes {}",
                total, params.total_size_bytes
            )));
        }
        return Ok(Some(explicit_sizes.clone()));
    }

    if let Some(part_size_bytes) = params.part_size_bytes {
        if part_size_bytes == 0 {
            return Err(AppError::BadRequest(
                "part_size_bytes must be greater than 0".into(),
            ));
        }
        if part_size_bytes > max_part_size {
            return Err(AppError::BadRequest(format!(
                "part_size_bytes {} exceeds max_part_size {}",
                part_size_bytes, max_part_size
            )));
        }
        let derived_parts = params.total_size_bytes.div_ceil(part_size_bytes).max(1);
        if derived_parts != params.expected_parts {
            return Err(AppError::BadRequest(format!(
                "part_size_bytes {} implies {} part(s), expected_parts is {}",
                part_size_bytes, derived_parts, params.expected_parts
            )));
        }
        let mut sizes = Vec::with_capacity(derived_parts as usize);
        let mut remaining = params.total_size_bytes;
        for _ in 0..derived_parts {
            let take = remaining.min(part_size_bytes);
            sizes.push(take);
            remaining = remaining.saturating_sub(take);
        }
        return Ok(Some(sizes));
    }

    if params.expected_parts == 1 {
        if params.total_size_bytes > max_part_size {
            return Err(AppError::BadRequest(format!(
                "single-part upload size {} exceeds max_part_size {}",
                params.total_size_bytes, max_part_size
            )));
        }
        return Ok(Some(vec![params.total_size_bytes]));
    }

    Ok(None)
}

async fn reserve_planned_part_layouts(
    part_sizes: &[u64],
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
) -> Result<Vec<PlannedPartLayout>, AppError> {
    let total_shards = data_shards.saturating_add(parity_shards).max(1);
    let num_disks = configured_blob_disk_count()?;
    let mut planned_parts = Vec::with_capacity(part_sizes.len());
    let mut reserved_addresses = Vec::new();

    for (part_index, size_bytes) in part_sizes.iter().copied().enumerate() {
        let stripe_data_width = block_size as u64 * data_shards.max(1) as u64;
        let stripe_count = size_bytes.div_ceil(stripe_data_width);
        let shard_size_bytes = stripe_count * block_size as u64;
        let mut shards = Vec::with_capacity(total_shards);

        for shard_index in 0..total_shards {
            let disk_idx = (part_index + shard_index) % num_disks;
            let (raw_blob_address, io) = allocate_raw_direct_io(disk_idx, shard_size_bytes)?;
            drop(io);
            reserved_addresses.push(raw_blob_address.clone());
            shards.push(PlannedShardSlot {
                shard_index,
                file_name: stable_shard_file_name(
                    part_index as u64,
                    shard_index,
                    shard_index >= data_shards,
                ),
                raw_blob_address,
                is_parity: shard_index >= data_shards,
            });
        }

        planned_parts.push(PlannedPartLayout {
            part_index: part_index as u64,
            size_bytes,
            shard_size_bytes,
            shards,
        });
    }

    if let Err(e) = flush_raw_metadata_for_addresses(&reserved_addresses).await {
        cleanup_raw_addresses_best_effort(&reserved_addresses).await;
        return Err(e);
    }

    Ok(planned_parts)
}

#[derive(Serialize)]
pub struct ActiveSessionInfo {
    pub session_id: String,
    pub version_id: String,
    pub object_key: Option<String>,
    pub created_at: i64,
    pub reserved_size: u64,
    pub total_size_bytes: Option<u64>,
    pub expected_parts: Option<u64>,
    pub committed_parts: u64,
    pub committed_bytes: u64,
}

#[derive(Serialize)]
pub struct ListUploadsResponse {
    pub sessions: Vec<ActiveSessionInfo>,
    pub total: usize,
}

pub struct DirectWriter {
    file: Arc<fs::File>,
    write_buffer: AlignedBuffer,
    write_task: Option<tokio::task::JoinHandle<Result<AlignedBuffer, std::io::Error>>>,
}

impl DirectWriter {
    pub fn new(file: fs::File) -> Self {
        let configured = WRITER_BUFFER_SIZE_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(256 * 1024);
        let aligned = ((configured.max(4096) + 4095) / 4096) * 4096;
        Self {
            file: Arc::new(file),
            write_buffer: AlignedBuffer::new(aligned),
            write_task: None,
        }
    }

    fn poll_write_task(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(task) = &mut self.write_task {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Ok(Ok(returned_buffer))) => {
                    self.write_buffer = returned_buffer;
                    self.write_task = None;
                }
                Poll::Ready(Ok(Err(e))) => {
                    self.write_task = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Ready(Err(e)) => {
                    self.write_task = None;
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn spawn_buffer_flush(&mut self) {
        if self.write_buffer.len() == 0 || self.write_task.is_some() {
            return;
        }

        let file = self.file.clone();
        let mut to_write = std::mem::replace(&mut self.write_buffer, AlignedBuffer::new(0));

        let task = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut f = &*file;
            f.write_all(to_write.as_slice())?;
            to_write.clear();
            Ok::<AlignedBuffer, std::io::Error>(to_write)
        });

        self.write_task = Some(task);
    }

    pub async fn sync_data(&self) -> Result<(), std::io::Error> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.sync_all()).await?
    }
}

impl AsyncWrite for DirectWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if matches!(self.poll_write_task(cx), Poll::Pending) {
            return Poll::Pending;
        }

        if self.write_buffer.len() == self.write_buffer.capacity() {
            self.spawn_buffer_flush();
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let remaining = self.write_buffer.capacity() - self.write_buffer.len();
        let to_copy = std::cmp::min(remaining, buf.len());
        self.write_buffer.extend_from_slice(&buf[..to_copy]);

        if self.write_buffer.len() == self.write_buffer.capacity() {
            self.spawn_buffer_flush();
        }

        Poll::Ready(Ok(to_copy))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if self.write_buffer.len() > 0 && self.write_task.is_none() {
            self.spawn_buffer_flush();
        }

        if matches!(self.poll_write_task(cx), Poll::Ready(Ok(()))) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.as_mut().poll_flush(cx)
    }
}

pub struct RawDirectWriter {
    io: DirectExtentIo,
    ring: RawIoRing,
    raw_blob_address: crate::n1_core::disk_manager::RawBlobAddress,
    block_size: usize,
    reusable_buffers: Vec<AlignedBuffer>,
    inflight_limit: usize,
    inflight_writes: usize,
    write_error: Option<std::io::Error>,
    next_offset: u64,
}

impl RawDirectWriter {
    pub fn new(io: DirectExtentIo, block_size: usize) -> Self {
        let aligned = ((block_size.max(4096) + 4095) / 4096) * 4096;
        let inflight_limit = RAW_WRITER_INFLIGHT_LIMIT_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(4)
            .clamp(2, 32);
        let ring = RawIoRing::new(io.clone(), inflight_limit);
        let mut writer = Self {
            raw_blob_address: raw_blob_address_from_extent(io.raw_address()),
            io,
            ring,
            block_size: aligned,
            reusable_buffers: Vec::new(),
            inflight_limit,
            inflight_writes: 0,
            write_error: None,
            next_offset: 0,
        };
        writer.prefill_reusable_buffers();
        writer
    }

    fn prefill_target(inflight_limit: usize) -> usize {
        match inflight_limit {
            0..=8 => 1,
            9..=16 => 2,
            _ => 2,
        }
    }

    fn clone_write_error(&self) -> Option<std::io::Error> {
        self.write_error
            .as_ref()
            .map(|e| std::io::Error::new(e.kind(), e.to_string()))
    }

    fn handle_write_completions<I>(&mut self, completions: I) -> Result<(), std::io::Error>
    where
        I: IntoIterator<Item = RawIoCompletion>,
    {
        for completion in completions {
            match completion {
                RawIoCompletion::WriteOk(mut returned_buffer) => {
                    returned_buffer.clear();
                    if returned_buffer.capacity() == self.block_size {
                        self.reusable_buffers.push(returned_buffer);
                    }
                    self.inflight_writes = self.inflight_writes.saturating_sub(1);
                }
                RawIoCompletion::WriteErr(returned_buffer, e) => {
                    if let Some(mut returned_buffer) = returned_buffer {
                        returned_buffer.clear();
                        if returned_buffer.capacity() == self.block_size {
                            self.reusable_buffers.push(returned_buffer);
                        }
                    }
                    self.inflight_writes = self.inflight_writes.saturating_sub(1);
                    self.write_error = Some(e);
                    return Err(self
                        .clone_write_error()
                        .expect("write error should be present"));
                }
            }
        }

        if let Some(err) = self.clone_write_error() {
            Err(err)
        } else {
            Ok(())
        }
    }

    fn poll_write_completions(&mut self, cx: &mut Context<'_>) -> Result<(), std::io::Error> {
        if let Some(err) = self.clone_write_error() {
            return Err(err);
        }
        self.handle_write_completions(self.ring.poll_completions(cx))
    }

    fn reclaim_write_completions_now(&mut self) -> Result<(), std::io::Error> {
        self.handle_write_completions(self.ring.take_completions_now())
    }

    fn prefill_reusable_buffers(&mut self) {
        let target = Self::prefill_target(self.inflight_limit).min(self.inflight_limit);
        while self.reusable_buffers.len() < target {
            self.reusable_buffers
                .push(take_pooled_aligned_buffer(self.block_size));
        }
    }

    pub fn acquire_block_buffer(&mut self) -> Result<AlignedBuffer, std::io::Error> {
        self.reclaim_write_completions_now()?;
        Ok(self
            .reusable_buffers
            .pop()
            .unwrap_or_else(|| take_pooled_aligned_buffer(self.block_size)))
    }

    fn try_submit_owned_buffer(&mut self, buffer: AlignedBuffer) -> Result<(), RawIoSubmitError> {
        let write_offset = self.next_offset;
        let write_len = buffer.len() as u64;
        match self.ring.submit_write(write_offset, buffer) {
            Ok(()) => {
                self.next_offset = self.next_offset.saturating_add(write_len);
                self.inflight_writes = self.inflight_writes.saturating_add(1);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn wait_for_capacity(&mut self) -> Result<(), std::io::Error> {
        self.reclaim_write_completions_now()?;
        if self.inflight_writes < self.inflight_limit {
            return Ok(());
        }

        poll_fn(|cx| match self.poll_write_completions(cx) {
            Ok(()) if self.inflight_writes < self.inflight_limit => Poll::Ready(Ok(())),
            Ok(()) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        })
        .await
    }

    pub async fn write_aligned_block(
        &mut self,
        mut buffer: AlignedBuffer,
    ) -> Result<(), std::io::Error> {
        if buffer.len() == 0 {
            return Ok(());
        }
        if buffer.len() != self.block_size || buffer.capacity() != self.block_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "raw writer requires full aligned blocks of {} bytes (got len={} capacity={})",
                    self.block_size,
                    buffer.len(),
                    buffer.capacity()
                ),
            ));
        }

        if let Some(err) = self.clone_write_error() {
            return Err(err);
        }

        loop {
            self.wait_for_capacity().await?;
            match self.try_submit_owned_buffer(buffer) {
                Ok(()) => return Ok(()),
                Err(RawIoSubmitError::Full(returned_buffer)) => {
                    buffer = returned_buffer;
                }
                Err(RawIoSubmitError::Closed(returned_buffer)) => {
                    drop(returned_buffer);
                    let err = std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "raw writer worker channel closed",
                    );
                    self.write_error = Some(std::io::Error::new(err.kind(), err.to_string()));
                    return Err(err);
                }
            }
        }
    }

    pub async fn flush(&mut self) -> Result<(), std::io::Error> {
        self.reclaim_write_completions_now()?;
        if self.inflight_writes == 0 {
            return Ok(());
        }

        poll_fn(|cx| match self.poll_write_completions(cx) {
            Ok(()) if self.inflight_writes == 0 => Poll::Ready(Ok(())),
            Ok(()) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        })
        .await
    }

    pub async fn sync_data(&self) -> Result<(), std::io::Error> {
        let io = self.io.clone();
        tokio::task::spawn_blocking(move || {
            io.flush_data()
                .map_err(|e| std::io::Error::other(format!("block manager: {}", e)))
        })
        .await?
    }

    pub fn raw_blob_address(&self) -> &crate::n1_core::disk_manager::RawBlobAddress {
        &self.raw_blob_address
    }
}

impl Drop for RawDirectWriter {
    fn drop(&mut self) {
        let _ = self.reclaim_write_completions_now();
        for buffer in self.reusable_buffers.drain(..) {
            recycle_pooled_aligned_buffer(buffer);
        }
    }
}

async fn cleanup_raw_addresses_best_effort(
    addresses: &[crate::n1_core::disk_manager::RawBlobAddress],
) {
    let futs = addresses.iter().cloned().map(|raw| async move {
        let _ = free_raw_address(&raw).await;
    });
    let _ = join_all(futs).await;
}

fn raw_addresses_from_writers(
    writers: &[Option<RawDirectWriter>],
) -> Vec<crate::n1_core::disk_manager::RawBlobAddress> {
    writers
        .iter()
        .filter_map(|writer| {
            writer
                .as_ref()
                .map(|writer| writer.raw_blob_address().clone())
        })
        .collect()
}

pub async fn create_direct_file(path: &PathBuf) -> Result<fs::File, AppError> {
    let path_clone = path.clone();

    if let Some(parent) = path.parent() {
        let _ = tokio_fs::create_dir_all(parent).await;
    }

    tokio::task::spawn_blocking(move || {
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);

        #[cfg(target_os = "linux")]
        {
            options.custom_flags(libc::O_DIRECT);
        }

        #[cfg(windows)]
        {
            const FILE_FLAG_NO_BUFFERING: u32 = 0x20000000;
            options.custom_flags(FILE_FLAG_NO_BUFFERING);
        }

        options
            .open(&path_clone)
            .map_err(|e| AppError::Io(e, format!("create {}", path_clone.display())))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Spawn blocking failed: {}", e)))?
}

#[cfg(unix)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let dir = tokio_fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open dir for sync: {:?}", path)))?;

    dir.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync dir: {:?}", path)))?;
    Ok(())
}

#[cfg(windows)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

        let result = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path);

        match result {
            Ok(file) => {
                let _ = file.sync_all();
            }
            Err(e) => {
                warn!(
                    "Windows: Failed to open directory for sync (non-fatal): {} - {}",
                    path.display(),
                    e
                );
            }
        }

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(not(any(unix, windows)))]
pub async fn sync_dir(_path: &std::path::Path) -> Result<(), AppError> {
    Ok(())
}

fn session_parts_dir_from_state_path(state_path: &std::path::Path, version_id: &str) -> PathBuf {
    state_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(format!("{}.parts", version_id))
}

fn session_part_meta_path_from_state_path(
    state_path: &std::path::Path,
    version_id: &str,
    part_index: u64,
) -> PathBuf {
    session_parts_dir_from_state_path(state_path, version_id)
        .join(format!("{:020}.msgpack", part_index))
}

pub(crate) async fn session_part_meta_paths(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Vec<PathBuf>, AppError> {
    Ok(index::get_session_state_paths(fragment, version_id)?
        .into_iter()
        .map(|state_path| {
            session_part_meta_path_from_state_path(&state_path, version_id, part_index)
        })
        .collect())
}

async fn session_part_meta_dirs(
    fragment: &str,
    version_id: &str,
) -> Result<Vec<PathBuf>, AppError> {
    Ok(index::get_session_state_paths(fragment, version_id)?
        .into_iter()
        .map(|state_path| session_parts_dir_from_state_path(&state_path, version_id))
        .collect())
}

async fn read_messagepack_if_exists<T>(path: &std::path::Path) -> Result<Option<T>, AppError>
where
    T: for<'de> Deserialize<'de>,
{
    match tokio_fs::read(path).await {
        Ok(bytes) => {
            let value = rmp_serde::from_slice::<T>(&bytes).map_err(|e| {
                AppError::InternalServerError(format!("decode {}: {}", path.display(), e))
            })?;
            Ok(Some(value))
        }
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                || e.kind() == std::io::ErrorKind::NotADirectory =>
        {
            Ok(None)
        }
        Err(e) => Err(AppError::Io(e, format!("read {}", path.display()))),
    }
}

async fn load_session_state_any(
    fragment: &str,
    version_id: &str,
) -> Result<Option<index::IndexSessionState>, AppError> {
    for path in index::get_session_state_paths(fragment, version_id)? {
        if let Some(state) = read_messagepack_if_exists::<index::IndexSessionState>(&path).await? {
            return Ok(Some(state));
        }
    }
    Ok(None)
}

fn raw_address_key(raw: &crate::n1_core::disk_manager::RawBlobAddress) -> (String, u64, u64, u64) {
    (
        raw.device_id.clone(),
        raw.allocation_id,
        raw.offset_bytes,
        raw.size_bytes,
    )
}

async fn load_part_meta_from_paths(paths: &[PathBuf]) -> Result<Option<PartMetadata>, AppError> {
    for path in paths {
        if let Some(meta) = read_messagepack_if_exists::<PartMetadata>(path).await? {
            return Ok(Some(meta));
        }
    }
    Ok(None)
}

pub(crate) async fn load_all_part_metas_from_disk(
    fragment: &str,
    version_id: &str,
) -> Result<Vec<PartMetadata>, AppError> {
    let mut parts = BTreeMap::new();
    for dir in session_part_meta_dirs(fragment, version_id).await? {
        let mut entries = match tokio_fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::NotADirectory =>
            {
                continue;
            }
            Err(e) => return Err(AppError::Io(e, format!("read_dir {}", dir.display()))),
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AppError::Io(e, format!("read_dir {}", dir.display())))?
        {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("msgpack") {
                continue;
            }
            if let Some(meta) = read_messagepack_if_exists::<PartMetadata>(&path).await? {
                parts.entry(meta.part_index).or_insert(meta);
            }
        }
    }
    Ok(parts.into_values().collect())
}

async fn write_atomic_messagepack<T: Serialize>(
    path: &std::path::Path,
    value: &T,
) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent dir for {}", path.display()))
    })?;
    tokio_fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;

    let bytes = rmp_serde::to_vec(value)
        .map_err(|e| AppError::InternalServerError(format!("serialize {}: {}", path.display(), e)))?;
    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|v| v.to_str()).unwrap_or("meta"),
        Ulid::new()
    ));
    let mut file = tokio_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("open {}", tmp.display())))?;
    tokio::io::AsyncWriteExt::write_all(&mut file, &bytes)
        .await
        .map_err(|e| AppError::Io(e, format!("write {}", tmp.display())))?;
    file.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync {}", tmp.display())))?;
    drop(file);

    tokio_fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;
    sync_dir(parent).await?;
    Ok(())
}

async fn maybe_replicate_part_meta<F>(label: &'static str, fut: F)
where
    F: std::future::Future<Output = Result<(), AppError>> + Send + 'static,
{
    #[cfg(test)]
    {
        if let Err(e) = fut.await {
            warn!("{} failed: {}", label, e);
        }
    }
    #[cfg(not(test))]
    {
        tokio::spawn(async move {
            if let Err(e) = fut.await {
                warn!("{} failed: {}", label, e);
            }
        });
    }
}

async fn persist_part_meta_on_roots(
    fragment: &str,
    version_id: &str,
    part_meta: &PartMetadata,
    replica_write_mode: index::IndexReplicaWriteMode,
) -> Result<(), AppError> {
    let paths = session_part_meta_paths(fragment, version_id, part_meta.part_index).await?;
    if paths.is_empty() {
        return Err(AppError::NotFound);
    }

    write_atomic_messagepack(&paths[0], part_meta).await?;
    if paths.len() > 1 {
        let replica_paths = paths[1..].to_vec();
        let part_meta_clone = part_meta.clone();
        let replicate_fut = async move {
            for path in replica_paths {
                write_atomic_messagepack(&path, &part_meta_clone).await?;
            }
            Ok(())
        };
        match replica_write_mode {
            index::IndexReplicaWriteMode::BackgroundBestEffort => {
                maybe_replicate_part_meta("upload part metadata replica sync", replicate_fut).await;
            }
            index::IndexReplicaWriteMode::SyncBestEffort => {
                if let Err(e) = replicate_fut.await {
                    warn!(
                        "upload part metadata replica sync failed fragment={} version={} part={}: {}",
                        fragment, version_id, part_meta.part_index, e
                    );
                }
            }
        }
    }

    Ok(())
}

pub(crate) async fn cleanup_upload_part_metadata_for_version(
    fragment: &str,
    version_id: &str,
    free_raw_extents: bool,
) {
    let manifest = if let Ok(Some(state)) = load_session_state_any(fragment, version_id).await {
        Some(state.manifest)
    } else if let Ok(Some(receipt)) = index::read_removed_session_receipt(fragment, version_id).await
    {
        Some(receipt.state.manifest)
    } else {
        None
    };
    let part_metas = load_all_part_metas_from_disk(fragment, version_id)
        .await
        .unwrap_or_default();

    let mut reserved_keys = BTreeSet::new();
    let mut reserved_addresses = Vec::new();
    if let Some(manifest) = manifest.as_ref() {
        for raw in manifest.all_planned_raw_addresses() {
            let key = raw_address_key(&raw);
            if reserved_keys.insert(key) {
                reserved_addresses.push(raw);
            }
        }
    }
    for part in &part_metas {
        for shard in &part.shards {
            if let Some(raw) = shard.raw_blob_address.as_ref() {
                let key = raw_address_key(raw);
                if reserved_keys.insert(key) {
                    reserved_addresses.push(raw.clone());
                }
            }
        }
    }

    if free_raw_extents {
        if !reserved_addresses.is_empty() {
            cleanup_raw_addresses_best_effort(&reserved_addresses).await;
        }
    }

    if let Ok(dirs) = session_part_meta_dirs(fragment, version_id).await {
        for dir in dirs {
            if tokio_fs::try_exists(&dir).await.unwrap_or(false) {
                let _ = tokio_fs::remove_dir_all(&dir).await;
            }
        }
    }

    if let Ok(paths) = index::get_session_state_paths(fragment, version_id) {
        for path in paths {
            match tokio_fs::remove_file(&path).await {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        let _ = sync_dir(parent).await;
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::NotFound
                        || e.kind() == std::io::ErrorKind::NotADirectory => {}
                Err(_) => {}
            }
        }
    }

    let _ = index::delete_removed_session_receipt(fragment, version_id).await;
}

async fn cleanup_failed_part_attempt(
    fragment: &str,
    version_id: &str,
    part_index: u64,
    _attempt_id: &str,
    raw_addresses: &[crate::n1_core::disk_manager::RawBlobAddress],
    free_raw_addresses: bool,
) {
    if free_raw_addresses && !raw_addresses.is_empty() {
        cleanup_raw_addresses_best_effort(raw_addresses).await;
    }
    let _ = (fragment, version_id, part_index);
}

pub(crate) async fn cleanup_orphaned_upload_artifacts_for_fragment(
    fragment: &str,
    now: std::time::SystemTime,
    file_ttl: std::time::Duration,
) -> Result<u64, AppError> {
    let path_manager = ObjectPathManager::new();
    let mut orphan_versions = BTreeSet::new();
    for root in path_manager.get_all_index_roots() {
        let partitions_root = root.join(fragment).join("index").join("partitions");
        let mut partition_entries = match tokio_fs::read_dir(&partitions_root).await {
            Ok(entries) => entries,
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::NotADirectory =>
            {
                continue;
            }
            Err(e) => {
                return Err(AppError::Io(
                    e,
                    format!("read_dir {}", partitions_root.display()),
                ));
            }
        };
        while let Some(partition_entry) = partition_entries
            .next_entry()
            .await
            .map_err(|e| AppError::Io(e, format!("read_dir {}", partitions_root.display())))?
        {
            let sessions_dir = partition_entry.path().join("sessions");
            let mut session_entries = match tokio_fs::read_dir(&sessions_dir).await {
                Ok(entries) => entries,
                Err(e)
                    if e.kind() == std::io::ErrorKind::NotFound
                        || e.kind() == std::io::ErrorKind::NotADirectory =>
                {
                    continue;
                }
                Err(e) => {
                    return Err(AppError::Io(
                        e,
                        format!("read_dir {}", sessions_dir.display()),
                    ));
                }
            };
            while let Some(entry) = session_entries
                .next_entry()
                .await
                .map_err(|e| AppError::Io(e, format!("read_dir {}", sessions_dir.display())))?
            {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
                    continue;
                };
                if !name.ends_with(".removed.msgpack") {
                    continue;
                }
                let version_id = name.trim_end_matches(".removed.msgpack");
                let modified = tokio_fs::metadata(&path)
                    .await
                    .and_then(|meta| meta.modified())
                    .unwrap_or(now);
                if now
                    .duration_since(modified)
                    .unwrap_or(std::time::Duration::ZERO)
                    <= file_ttl
                {
                    continue;
                }
                if index::load_session(fragment, version_id).await?.is_some() {
                    continue;
                }
                let Some(receipt) = index::read_removed_session_receipt(fragment, version_id).await?
                else {
                    continue;
                };
                let published = index::read_object_version(
                    fragment,
                    &receipt.state.manifest.object_key,
                    version_id,
                )
                .await
                .is_ok()
                    || index::read_trashed_object_version(
                        fragment,
                        &receipt.state.manifest.object_key,
                        version_id,
                    )
                    .await
                    .is_ok();
                if published {
                    cleanup_upload_part_metadata_for_version(fragment, version_id, false).await;
                } else {
                    orphan_versions.insert(version_id.to_string());
                }
            }
        }
    }

    for version_id in &orphan_versions {
        cleanup_upload_part_metadata_for_version(fragment, version_id, true).await;
    }
    Ok(orphan_versions.len() as u64)
}

pub(crate) async fn read_part_meta_any(
    _path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Option<PartMetadata> {
    let paths = session_part_meta_paths(fragment, version_id, part_index)
        .await
        .ok()?;
    load_part_meta_from_paths(&paths).await.ok().flatten()
}

async fn cleanup_blob_tmp_attempt_files_for_version(fragment: &str, version_id: &str) {
    let root = configured_fs_storage_root()
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::env::temp_dir().join("n1-raw-stage"));
    let stage_dir = root.join(".n1-raw-stage").join(fragment).join(version_id);
    if tokio_fs::try_exists(&stage_dir).await.unwrap_or(false) {
        let _ = tokio_fs::remove_dir_all(&stage_dir).await;
    }
}

pub async fn initiate_object_upload(
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    if params.expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }

    if let Err(e) = try_reserve_quota(&fragment, params.total_size_bytes).await {
        if let AppError::Unauthorized = e {
            return Err(AppError::Forbidden(
                "Fragment not initialized. Quota file missing.".into(),
            ));
        } else {
            return Err(e);
        }
    }

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();

    let stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    let data_shards = DATA_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize;
    let parity_shards = PARITY_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize;
    let planned_part_sizes = derive_planned_part_sizes(&params, max_part_size)?;
    let planned_parts = if let Some(part_sizes) = planned_part_sizes.as_ref() {
        reserve_planned_part_layouts(part_sizes, data_shards, parity_shards, stripe_size).await?
    } else {
        Vec::new()
    };

    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: object_key.clone(),
        version_id: version_id.clone(),
        session_id: session_id.clone(),
        total_size_bytes: params.total_size_bytes,
        expected_parts: params.expected_parts.max(1),
        content_type: params
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        created_at: Utc::now().timestamp(),
        stripe_size_bytes: stripe_size,
        data_shards,
        parity_shards,
        planned_parts,
    };
    let receipt = ReservationReceipt {
        amount: params.total_size_bytes,
    };
    if let Err(e) = index::open_session(&manifest, Some(receipt)).await {
        if !manifest.planned_parts.is_empty() {
            cleanup_raw_addresses_best_effort(&manifest.all_planned_raw_addresses()).await;
        }
        error!("Failed to open index session: {}", e);
        let _ = update_quota(&fragment, -(params.total_size_bytes as i64)).await;
        return Err(AppError::InternalServerError(
            "Storage backend unavailable".into(),
        ));
    }

    info!(
        "Initiated upload: {}/{} (v: {}, s: {})",
        fragment, object_key, version_id, session_id
    );

    Ok((
        StatusCode::CREATED,
        AxumJson(InitiateUploadResponse {
            session_id,
            version_id,
        }),
    ))
}

pub async fn list_active_uploads(
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
        .min(100);
    let offset: usize = params
        .get("offset")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let states = index::list_active_sessions(&fragment).await?;
    if states.is_empty() {
        return Ok(AxumJson(ListUploadsResponse {
            sessions: vec![],
            total: 0,
        }));
    }
    let mut sessions: Vec<ActiveSessionInfo> = Vec::with_capacity(states.len());
    for state in states {
        let part_metas = load_all_part_metas_from_disk(&fragment, &state.manifest.version_id)
            .await
            .unwrap_or_default();
        sessions.push(ActiveSessionInfo {
            session_id: state.manifest.session_id,
            version_id: state.manifest.version_id,
            object_key: Some(state.manifest.object_key),
            created_at: state.manifest.created_at,
            reserved_size: state.reservation.map(|r| r.amount).unwrap_or(0),
            total_size_bytes: Some(state.manifest.total_size_bytes),
            expected_parts: Some(state.manifest.expected_parts),
            committed_parts: part_metas.len() as u64,
            committed_bytes: part_metas.iter().map(|part| part.size_bytes).sum(),
        });
    }

    sessions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    let total = sessions.len();
    let paged_sessions = sessions.into_iter().skip(offset).take(limit).collect();

    Ok(AxumJson(ListUploadsResponse {
        sessions: paged_sessions,
        total,
    }))
}

pub async fn get_upload_status(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let state = load_session_state_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::NotFound)?;
    let mut part_metas = load_all_part_metas_from_disk(&fragment, &version_id).await?;
    part_metas.sort_by_key(|part| part.part_index);
    let uploaded_parts = part_metas
        .iter()
        .map(|part| part.part_index)
        .collect::<Vec<_>>();
    let committed_bytes = part_metas.iter().map(|part| part.size_bytes).sum::<u64>();
    Ok(AxumJson(UploadStatusResponse {
        uploaded_parts,
        committed_parts: part_metas.len() as u64,
        committed_bytes,
        expected_parts: Some(state.manifest.expected_parts),
        total_size_bytes: Some(state.manifest.total_size_bytes),
        created_at: Some(state.manifest.created_at),
        session_id: Some(state.manifest.session_id),
        object_key: Some(state.manifest.object_key),
    }))
}

pub async fn object_upload_part(
    claims: Claims,
    headers: HeaderMap,
    AxumPath((fragment, version_id, part_index)): AxumPath<(String, String, u64)>,
    Query(query): Query<UploadPartQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let io_gate_enabled = query.io_gate.unwrap_or_else(|| {
        UPLOAD_IO_GATE_ENABLED_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(true)
    });
    let _io_guard = if io_gate_enabled {
        Some(acquire_adaptive_io_guard().await?)
    } else {
        None
    };
    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();

    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    let session_state = load_session_state_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::NotFound)?;
    let manifest = session_state.manifest;

    let content_length = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or(AppError::BadRequest(
            "Content-Length header required".into(),
        ))?;

    if content_length > max_part_size {
        return Ok((StatusCode::PAYLOAD_TOO_LARGE, "Part too large").into_response());
    }

    if part_index >= manifest.expected_parts {
        return Err(AppError::BadRequest(format!(
            "part_index {} is outside expected_parts {}",
            part_index, manifest.expected_parts
        )));
    }

    let data_shards = manifest.data_shards.max(1);
    let parity_shards = manifest.parity_shards;
    let total_shards = data_shards + parity_shards;
    let block_size = manifest.stripe_size_bytes;

    if block_size % 4096 != 0 {
        return Err(AppError::InternalServerError(format!(
            "stripe_size_bytes must be multiple of 4096 for O_DIRECT (got {})",
            block_size
        )));
    }

    let expected_checksum = query
        .checksum
        .as_deref()
        .map(|value: &str| value.trim())
        .filter(|s| !s.is_empty());

    if let Some(existing) =
        read_part_meta_any(&ObjectPathManager::new(), &fragment, &version_id, part_index).await
    {
        if existing.size_bytes != content_length {
            return Ok((
                StatusCode::CONFLICT,
                "Part already uploaded but size differs",
            )
                .into_response());
        }
        if let Some(expected) = expected_checksum {
            if let Some(existing_checksum) = existing.checksum.as_deref() {
                if !existing_checksum.eq_ignore_ascii_case(expected) {
                    return Ok((
                        StatusCode::CONFLICT,
                        "Part already uploaded but checksum differs",
                    )
                        .into_response());
                }
            } else {
                return Ok((
                    StatusCode::CONFLICT,
                    "Part already uploaded without checksum metadata",
                )
                    .into_response());
            }
        }
        return Ok((StatusCode::OK, "Part already uploaded").into_response());
    }

    let attempt_id = Ulid::new().to_string();
    let planned_layout = manifest.planned_part_layout(part_index).cloned();
    if let Some(layout) = planned_layout.as_ref() {
        if layout.size_bytes != content_length {
            return Err(AppError::BadRequest(format!(
                "part size {} does not match planned upload geometry {}",
                content_length, layout.size_bytes
            )));
        }
    }

    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = content_length.div_ceil(stripe_data_width);
    let final_shard_len = stripe_count * block_size as u64;

    let num_disks = configured_blob_disk_count()?;
    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }

    let mut writers: Vec<Option<RawDirectWriter>> = Vec::with_capacity(total_shards);
    let mut stable_names: Vec<String> = Vec::with_capacity(total_shards);
    let mut allocated_raw_addrs = Vec::with_capacity(total_shards);
    let raw_addresses_owned = planned_layout.is_none();
    let mut successful_opens = 0usize;

    if let Some(layout) = planned_layout.as_ref() {
        if layout.shards.len() != total_shards {
            return Err(AppError::InternalServerError(format!(
                "planned upload geometry shard count mismatch for part {}",
                part_index
            )));
        }
        if layout.shard_size_bytes != final_shard_len {
            return Err(AppError::InternalServerError(format!(
                "planned upload geometry shard size mismatch for part {}",
                part_index
            )));
        }

        for shard in &layout.shards {
            let io = match open_raw_direct_io_for_address(&shard.raw_blob_address) {
                Ok(io) => io,
                Err(e) => return Err(e),
            };
            allocated_raw_addrs.push(shard.raw_blob_address.clone());
            stable_names.push(shard.file_name.clone());
            writers.push(Some(RawDirectWriter::new(io, block_size)));
            successful_opens += 1;
        }
    } else {
        for i in 0..total_shards {
            let is_parity = i >= data_shards;
            let stable_name = stable_shard_file_name(part_index, i, is_parity);
            let disk_idx = (part_index as usize + i) % num_disks;
            let (raw_addr, io) = match allocate_raw_direct_io(disk_idx, final_shard_len) {
                Ok(value) => value,
                Err(e) => {
                    cleanup_raw_addresses_best_effort(&allocated_raw_addrs).await;
                    return Err(e);
                }
            };
            allocated_raw_addrs.push(raw_addr);
            stable_names.push(stable_name);
            writers.push(Some(RawDirectWriter::new(io, block_size)));
            successful_opens += 1;
        }
    }

    if successful_opens < data_shards {
        if raw_addresses_owned {
            cleanup_raw_addresses_best_effort(&allocated_raw_addrs).await;
        }
        return Err(AppError::InternalServerError(format!(
            "Insufficient healthy disks for write quorum. Need {}, got {}.",
            data_shards, successful_opens
        )));
    }

    let calc_total_checksum = expected_checksum.is_some();

    let mut stream_writer = RawErasureStreamWriter::new(
        writers,
        data_shards,
        parity_shards,
        final_shard_len as usize,
        calc_total_checksum,
        block_size,
    )?;

    let mut body_stream = body.into_data_stream();
    let mut total_bytes_read = 0u64;

    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "stream read".into(),
            )
        })?;

        if chunk.is_empty() {
            continue;
        }

        total_bytes_read += chunk.len() as u64;

        if total_bytes_read > content_length {
            cleanup_failed_part_attempt(
                &fragment,
                &version_id,
                part_index,
                &attempt_id,
                &allocated_raw_addrs,
                raw_addresses_owned,
            )
            .await;
            return Err(AppError::BadRequest("Body exceeds Content-Length".into()));
        }

        if let Err(e) = stream_writer.write(&chunk).await {
            cleanup_failed_part_attempt(
                &fragment,
                &version_id,
                part_index,
                &attempt_id,
                &allocated_raw_addrs,
                raw_addresses_owned,
            )
            .await;
            return Err(e);
        }
    }

    if total_bytes_read != content_length {
        cleanup_failed_part_attempt(
            &fragment,
            &version_id,
            part_index,
            &attempt_id,
            &allocated_raw_addrs,
            raw_addresses_owned,
        )
        .await;
        return Err(AppError::BadRequest(format!(
            "Incomplete body: got {} bytes, expected {}",
            total_bytes_read, content_length
        )));
    }

    if let Err(e) = stream_writer.finalize().await {
        cleanup_failed_part_attempt(
            &fragment,
            &version_id,
            part_index,
            &attempt_id,
            &allocated_raw_addrs,
            raw_addresses_owned,
        )
        .await;
        return Err(e);
    }

    if let Some(expected) = expected_checksum {
        let calculated = stream_writer.get_total_checksum();
        if let Some(calc_str) = calculated.as_ref() {
            if !expected.eq_ignore_ascii_case(calc_str) {
                cleanup_failed_part_attempt(
                    &fragment,
                    &version_id,
                    part_index,
                    &attempt_id,
                    &allocated_raw_addrs,
                    raw_addresses_owned,
                )
                .await;
                return Ok((StatusCode::BAD_REQUEST, "Checksum mismatch").into_response());
            }
        }
    }

    let shard_checksums = stream_writer.get_shard_checksums();
    let total_checksum = stream_writer.get_total_checksum();
    let writers = stream_writer.into_writers();

    let raw_addresses = raw_addresses_from_writers(&writers);
    if raw_addresses_owned {
        if let Err(e) = flush_raw_metadata_for_addresses(&raw_addresses).await {
            cleanup_failed_part_attempt(
                &fragment,
                &version_id,
                part_index,
                &attempt_id,
                &raw_addresses,
                true,
            )
            .await;
            return Err(e);
        }
    }
    if strict_durability {
        let sync_futs = writers.iter().filter_map(|w_opt| {
            w_opt.as_ref().map(|w| async move {
                if let Err(e) = w.sync_data().await {
                    warn!("Failed to fsync shard: {}", e);
                    return Err(AppError::Io(e, "fsync shard".into()));
                }
                Ok(())
            })
        });

        let sync_results = join_all(sync_futs).await;
        for r in sync_results {
            if let Err(e) = r {
                cleanup_failed_part_attempt(
                    &fragment,
                    &version_id,
                    part_index,
                    &attempt_id,
                    &raw_addresses,
                    raw_addresses_owned,
                )
                .await;
                return Err(e);
            }
        }
    }

    let mut shards_info = Vec::with_capacity(total_shards);

    for i in 0..total_shards {
        let raw_addr = raw_addresses[i].clone();
        shards_info.push(ShardInfo {
            shard_index: i,
            file_name: stable_names[i].clone(),
            absolute_path: raw_blob_debug_path(&raw_addr),
            raw_blob_address: Some(raw_addr),
            size_bytes: final_shard_len,
            checksum: shard_checksums.get(i).cloned().unwrap_or_default(),
            is_parity: i >= data_shards,
        });
    }

    let part_meta = PartMetadata {
        part_index,
        size_bytes: total_bytes_read,
        created_at: Utc::now().timestamp(),
        checksum: if calc_total_checksum {
            total_checksum
        } else {
            None
        },
        shards: shards_info,
    };
    if let Err(e) = persist_part_meta_on_roots(
        &fragment,
        &version_id,
        &part_meta,
        if strict_durability {
            index::IndexReplicaWriteMode::SyncBestEffort
        } else {
            index::IndexReplicaWriteMode::BackgroundBestEffort
        },
    )
    .await
    {
        cleanup_failed_part_attempt(
            &fragment,
            &version_id,
            part_index,
            &attempt_id,
            &raw_addresses,
            raw_addresses_owned,
        )
        .await;
        return Err(e);
    }

    Ok((StatusCode::OK, "Part uploaded").into_response())
}

pub async fn finalize_object_upload(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, session_id)): AxumPath<(String, String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let object_key = key_query.object_key;
    if index::read_object_version(&fragment, &object_key, &version_id)
        .await
        .is_ok()
    {
        return Ok((StatusCode::OK, "Upload finalized.").into_response());
    }

    let session_state = load_session_state_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::NotFound)?;
    let manifest = session_state.manifest.clone();
    if manifest.session_id != session_id {
        return Err(AppError::Forbidden("Session id mismatch".into()));
    }
    if manifest.object_key != object_key {
        return Err(AppError::Forbidden("Object key mismatch".into()));
    }

    let expected_parts = manifest.expected_parts.max(params.expected_parts.max(1));

    if expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }

    let mut parts = load_all_part_metas_from_disk(&fragment, &version_id).await?;
    let present = parts.iter().map(|part| part.part_index).collect::<BTreeSet<_>>();

    let mut missing = Vec::new();
    for i in 0..expected_parts {
        if !present.contains(&i) {
            missing.push(i);
        }
    }

    if !missing.is_empty() {
        return Ok((
            StatusCode::CONFLICT,
            AxumJson(FinalizeIncompleteResponse {
                uploaded_parts: present.into_iter().collect(),
                missing_parts: missing,
                expected_parts,
            }),
        )
            .into_response());
    }

    parts.sort_by_key(|p| p.part_index);

    let mut total_size_calculated = 0u64;
    for part in &parts {
        total_size_calculated += part.size_bytes;
    }

    let content_type = Some(manifest.content_type.clone())
        .or_else(|| params.content_type.clone())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id.clone(),
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: total_size_calculated,
        // Preserve upload recency from session creation time so a slow or stale
        // finalize cannot demote a newer version of the same object key.
        created_at: manifest.created_at,
        parts,
        is_delete_marker: false,
        stripe_size_bytes: manifest.stripe_size_bytes,
    };

    index::publish_object(&object_meta).await?;
    let _ = index::remove_session(&fragment, &version_id, "finalized").await;
    cleanup_upload_part_metadata_for_version(&fragment, &version_id, false).await;

    let reserved_size = manifest.total_size_bytes as i64;

    let actual_size = total_size_calculated as i64;
    let delta = actual_size - reserved_size;

    if delta != 0 {
        update_quota(&fragment, delta).await?;
    }

    let stripped_meta = ObjectMetadata {
        parts: Vec::new(),
        ..object_meta.clone()
    };
    state.emit(&fragment, StorageEventPayload::FileUploaded(stripped_meta));

    Ok((StatusCode::OK, AxumJson(json!(object_meta))).into_response())
}

pub async fn cancel_upload_session(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    if index::read_object_version(&fragment, &key_query.object_key, &version_id)
        .await
        .is_ok()
    {
        return Err(AppError::Conflict(
            "Cannot cancel: Object is already finalized.".to_string(),
        ));
    }

    let session_state = load_session_state_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::NotFound)?;
    let refund_amount = session_state
        .reservation
        .map(|state| state.amount)
        .unwrap_or(0);

    cleanup_blob_tmp_attempt_files_for_version(&fragment, &version_id).await;
    let _ = index::remove_session(&fragment, &version_id, "cancelled").await;
    cleanup_upload_part_metadata_for_version(&fragment, &version_id, true).await;

    if refund_amount > 0 {
        if let Err(e) = update_quota(&fragment, -(refund_amount as i64)).await {
            error!(
                "Failed to refund quota for cancelled upload {}: {}",
                fragment, e
            );
        }
    }

    Ok(StatusCode::NO_CONTENT)
}
