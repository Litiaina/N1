use crate::enums::app_error_enum::AppError;
use crate::n1_core::upload::RawDirectWriter;
use crc32fast::Hasher;
use once_cell::sync::Lazy;
use simd_erasure_core::ReedSolomonEncoder;
use std::alloc::{Layout, alloc, dealloc};
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::{info, warn};

pub const ALIGNMENT: usize = 4096;
const DEFAULT_EC_LOG_INTERVAL_SECS: u64 = 30;

static EC_STRIPES_TOTAL: AtomicU64 = AtomicU64::new(0);
static EC_BYTES_IN_TOTAL: AtomicU64 = AtomicU64::new(0);
static EC_RS_NS_TOTAL: AtomicU64 = AtomicU64::new(0);
static EC_WRITE_NS_TOTAL: AtomicU64 = AtomicU64::new(0);
static EC_FLUSH_NS_TOTAL: AtomicU64 = AtomicU64::new(0);
static EC_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static EC_LAST_LOG_SECS: AtomicU64 = AtomicU64::new(0);
const MAX_POOLED_ALIGNED_BUFFERS_PER_SIZE: usize = 32;
static ALIGNED_BUFFER_POOL: Lazy<Mutex<HashMap<usize, Vec<AlignedBuffer>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

static EC_LOG_INTERVAL_SECS: Lazy<u64> = Lazy::new(|| {
    std::env::var("N1_EC_METRICS_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(5, 3600))
        .unwrap_or(DEFAULT_EC_LOG_INTERVAL_SECS)
});

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn maybe_log_ec_metrics() {
    let now = now_unix_secs();
    let interval = *EC_LOG_INTERVAL_SECS;
    let last = EC_LAST_LOG_SECS.load(Ordering::Relaxed);

    if now.saturating_sub(last) < interval {
        return;
    }

    if EC_LAST_LOG_SECS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let stripes = EC_STRIPES_TOTAL.load(Ordering::Relaxed);
    let bytes = EC_BYTES_IN_TOTAL.load(Ordering::Relaxed);
    let rs_ns = EC_RS_NS_TOTAL.load(Ordering::Relaxed);
    let write_ns = EC_WRITE_NS_TOTAL.load(Ordering::Relaxed);
    let flush_ns = EC_FLUSH_NS_TOTAL.load(Ordering::Relaxed);
    let errors = EC_ERRORS_TOTAL.load(Ordering::Relaxed);

    let mib = bytes as f64 / (1024.0 * 1024.0);
    let rs_ms = rs_ns as f64 / 1_000_000.0;
    let write_ms = write_ns as f64 / 1_000_000.0;
    let flush_ms = flush_ns as f64 / 1_000_000.0;
    let total_io_s = ((write_ns + flush_ns) as f64 / 1_000_000_000.0).max(0.000_001);
    let effective_mib_s = mib / total_io_s;

    info!(
        "EC metrics: stripes={}, input_mib={:.2}, rs_ms={:.2}, write_ms={:.2}, flush_ms={:.2}, effective_mib_s={:.2}, errors={}",
        stripes, mib, rs_ms, write_ms, flush_ms, effective_mib_s, errors
    );
}

pub(crate) fn take_pooled_aligned_buffer(size: usize) -> AlignedBuffer {
    if let Ok(mut pool) = ALIGNED_BUFFER_POOL.lock() {
        if let Some(buffers) = pool.get_mut(&size) {
            if let Some(mut buffer) = buffers.pop() {
                buffer.clear();
                return buffer;
            }
        }
    }

    AlignedBuffer::new(size)
}

pub(crate) fn recycle_pooled_aligned_buffer(mut buffer: AlignedBuffer) {
    let size = buffer.capacity();
    buffer.clear();

    if let Ok(mut pool) = ALIGNED_BUFFER_POOL.lock() {
        let buffers = pool.entry(size).or_default();
        if buffers.len() < MAX_POOLED_ALIGNED_BUFFERS_PER_SIZE {
            buffers.push(buffer);
        }
    }
}

pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
    len: usize,
}

impl AlignedBuffer {
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, ALIGNMENT).expect("Invalid alignment");
        if size == 0 {
            return Self {
                ptr: NonNull::dangling(),
                layout,
                len: 0,
            };
        }

        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr).unwrap_or_else(|| panic!("Aligned allocation failed"));
        Self {
            ptr,
            layout,
            len: 0,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn as_full_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let remaining = self.layout.size() - self.len;
        let to_copy = std::cmp::min(remaining, data.len());
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr().add(self.len), to_copy);
        }
        self.len += to_copy;
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.layout.size()
    }

    pub fn fill_zeros(&mut self) {
        let remaining = self.layout.size() - self.len;
        if remaining > 0 {
            unsafe {
                std::ptr::write_bytes(self.ptr.as_ptr().add(self.len), 0, remaining);
            }
            self.len = self.layout.size();
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if self.layout.size() > 0 {
            unsafe {
                dealloc(self.ptr.as_ptr(), self.layout);
            }
        }
    }
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

pub struct ErasureStreamWriter<W: AsyncWrite + Unpin + Send> {
    writers: Vec<Option<W>>,
    data_shards: usize,
    parity_shards: usize,
    shard_len_limit: usize,
    buffer: AlignedBuffer,
    bytes_written_to_shards: usize,
    total_hasher: Option<Hasher>,
    shard_hashers: Vec<Hasher>,
    rs_encoder: Option<ReedSolomonEncoder>,
    block_size: usize,
}

impl<W: AsyncWrite + Unpin + Send> ErasureStreamWriter<W> {
    pub fn new(
        writers: Vec<Option<W>>,
        data_shards: usize,
        parity_shards: usize,
        shard_len_limit: usize,
        calc_total_checksum: bool,
        block_size: usize,
    ) -> Result<Self, AppError> {
        if data_shards == 0 {
            return Err(AppError::InternalServerError(
                "data_shards must be greater than 0".into(),
            ));
        }
        if block_size == 0 || block_size % ALIGNMENT != 0 {
            return Err(AppError::InternalServerError(format!(
                "block_size must be a non-zero multiple of {}",
                ALIGNMENT
            )));
        }
        let expected_writers = data_shards + parity_shards;
        if writers.len() != expected_writers {
            return Err(AppError::InternalServerError(format!(
                "writers length mismatch: expected {}, got {}",
                expected_writers,
                writers.len()
            )));
        }
        if shard_len_limit % block_size != 0 {
            return Err(AppError::InternalServerError(format!(
                "shard_len_limit ({}) must be a multiple of block_size ({})",
                shard_len_limit, block_size
            )));
        }

        let total_hasher = if calc_total_checksum {
            Some(Hasher::new())
        } else {
            None
        };
        let shard_hashers = (0..(data_shards + parity_shards))
            .map(|_| Hasher::new())
            .collect();

        let rs_encoder = if parity_shards > 0 {
            Some(
                ReedSolomonEncoder::new(data_shards, parity_shards, block_size).map_err(|e| {
                    AppError::InternalServerError(format!("RS init check failed: {:?}", e))
                })?,
            )
        } else {
            None
        };

        Ok(Self {
            writers,
            data_shards,
            parity_shards,
            shard_len_limit,
            buffer: take_pooled_aligned_buffer(block_size * data_shards),
            bytes_written_to_shards: 0,
            total_hasher,
            shard_hashers,
            rs_encoder,
            block_size,
        })
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<(), AppError> {
        EC_BYTES_IN_TOTAL.fetch_add(data.len() as u64, Ordering::Relaxed);

        if let Some(h) = self.total_hasher.as_mut() {
            h.update(data);
        }

        let mut offset = 0;
        let stripe_capacity = self.buffer.capacity();

        while offset < data.len() {
            let remaining_in_buffer = stripe_capacity - self.buffer.len();
            let to_copy = std::cmp::min(remaining_in_buffer, data.len() - offset);

            self.buffer
                .extend_from_slice(&data[offset..offset + to_copy]);
            offset += to_copy;

            if self.buffer.len() == stripe_capacity {
                self.process_stripe().await?;
            }
        }

        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<(), AppError> {
        if self.buffer.len() > 0 {
            self.buffer.fill_zeros();
            self.process_stripe().await?;
        }

        let current_len = self.bytes_written_to_shards;
        if current_len < self.shard_len_limit {
            let needed = self.shard_len_limit - current_len;
            let mut padding_buf = AlignedBuffer::new(needed);
            padding_buf.fill_zeros();

            for h in self.shard_hashers.iter_mut() {
                h.update(padding_buf.as_slice());
            }

            for writer_opt in self.writers.iter_mut() {
                if let Some(writer) = writer_opt {
                    let write_start = Instant::now();
                    if let Err(e) = writer.write_all(padding_buf.as_slice()).await {
                        EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        warn!("Failed to write padding to shard: {}", e);
                        *writer_opt = None;
                    }
                    EC_WRITE_NS_TOTAL
                        .fetch_add(write_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
            }
        }

        if self.writers.iter().filter(|w| w.is_some()).count() < self.data_shards {
            EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(AppError::InternalServerError(
                "Not enough healthy disks to persist data".into(),
            ));
        }

        for w_opt in self.writers.iter_mut() {
            if let Some(w) = w_opt {
                let flush_start = Instant::now();
                if let Err(e) = w.flush().await {
                    EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!("Failed to flush shard writer: {}", e);
                    *w_opt = None;
                }
                EC_FLUSH_NS_TOTAL
                    .fetch_add(flush_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        }

        if self.writers.iter().filter(|w| w.is_some()).count() < self.data_shards {
            EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(AppError::InternalServerError(
                "Not enough healthy disks to persist data".into(),
            ));
        }

        maybe_log_ec_metrics();
        Ok(())
    }

    async fn process_stripe(&mut self) -> Result<(), AppError> {
        let block_size = self.block_size;
        let buffer_slice = self.buffer.as_full_slice();

        if self.parity_shards > 0 {
            let encoder = self.rs_encoder.as_mut().ok_or_else(|| {
                EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                AppError::InternalServerError("RS encoder missing for parity setup".into())
            })?;

            for i in 0..self.data_shards {
                let start = i * block_size;
                let end = start + block_size;
                encoder
                    .add_original_shard(&buffer_slice[start..end])
                    .map_err(|e| AppError::InternalServerError(format!("RS add: {:?}", e)))?;
            }

            let rs_start = Instant::now();
            let result = encoder
                .encode()
                .map_err(|e| AppError::InternalServerError(format!("RS encode: {:?}", e)))?;
            EC_RS_NS_TOTAL.fetch_add(rs_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            for i in 0..self.data_shards {
                let start = i * block_size;
                let end = start + block_size;
                self.shard_hashers[i].update(&buffer_slice[start..end]);
            }
            for i in 0..self.parity_shards {
                let slice = result.recovery(i).ok_or_else(|| {
                    EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    AppError::InternalServerError("RS parity shard generation mismatch".into())
                })?;
                self.shard_hashers[self.data_shards + i].update(slice);
            }
            let mut successful_writes = 0;
            for (i, writer_opt) in self.writers.iter_mut().enumerate() {
                if let Some(writer) = writer_opt {
                    let slice = if i < self.data_shards {
                        let start = i * block_size;
                        let end = start + block_size;
                        &self.buffer.as_full_slice()[start..end]
                    } else {
                        let parity_idx = i - self.data_shards;
                        result.recovery(parity_idx).ok_or_else(|| {
                            EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            AppError::InternalServerError("RS parity shard access mismatch".into())
                        })?
                    };

                    let write_start = Instant::now();
                    if let Err(e) = writer.write_all(slice).await {
                        EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        warn!("Failed to write stripe to shard: {}", e);
                        *writer_opt = None;
                    } else {
                        successful_writes += 1;
                    }
                    EC_WRITE_NS_TOTAL
                        .fetch_add(write_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
            }

            if successful_writes < self.data_shards {
                EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Err(AppError::InternalServerError(
                    "Not enough healthy disks to persist data".into(),
                ));
            }
        } else {
            for i in 0..self.data_shards {
                let start = i * block_size;
                let end = start + block_size;
                self.shard_hashers[i].update(&buffer_slice[start..end]);
            }

            let mut successful_writes = 0;
            for (i, writer_opt) in self.writers.iter_mut().enumerate() {
                if let Some(writer) = writer_opt {
                    let start = i * block_size;
                    let end = start + block_size;
                    let slice = &self.buffer.as_full_slice()[start..end];

                    let write_start = Instant::now();
                    if let Err(e) = writer.write_all(slice).await {
                        EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        warn!("Failed to write stripe to shard: {}", e);
                        *writer_opt = None;
                    } else {
                        successful_writes += 1;
                    }
                    EC_WRITE_NS_TOTAL
                        .fetch_add(write_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
            }

            if successful_writes < self.data_shards {
                EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Err(AppError::InternalServerError(
                    "Not enough healthy disks to persist data".into(),
                ));
            }
        }

        self.buffer.clear();
        self.bytes_written_to_shards += block_size;
        let stripes = EC_STRIPES_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
        if stripes % 256 == 0 {
            maybe_log_ec_metrics();
        }
        Ok(())
    }

    pub fn get_shard_checksums(&self) -> Vec<String> {
        self.shard_hashers
            .iter()
            .map(|h| format!("{:08x}", h.clone().finalize()))
            .collect()
    }

    pub fn into_writers(self) -> Vec<Option<W>> {
        let Self {
            writers, buffer, ..
        } = self;
        recycle_pooled_aligned_buffer(buffer);
        writers
    }
}

pub struct RawErasureStreamWriter {
    writers: Vec<Option<RawDirectWriter>>,
    data_shards: usize,
    parity_shards: usize,
    shard_len_limit: usize,
    data_blocks: Vec<AlignedBuffer>,
    stripe_bytes_filled: usize,
    bytes_written_to_shards: usize,
    total_hasher: Option<Hasher>,
    shard_hashers: Vec<Hasher>,
    rs_encoder: Option<ReedSolomonEncoder>,
    block_size: usize,
}

impl RawErasureStreamWriter {
    pub fn new(
        mut writers: Vec<Option<RawDirectWriter>>,
        data_shards: usize,
        parity_shards: usize,
        shard_len_limit: usize,
        calc_total_checksum: bool,
        block_size: usize,
    ) -> Result<Self, AppError> {
        if data_shards == 0 {
            return Err(AppError::InternalServerError(
                "data_shards must be greater than 0".into(),
            ));
        }
        if block_size == 0 || block_size % ALIGNMENT != 0 {
            return Err(AppError::InternalServerError(format!(
                "block_size must be a non-zero multiple of {}",
                ALIGNMENT
            )));
        }
        let expected_writers = data_shards + parity_shards;
        if writers.len() != expected_writers {
            return Err(AppError::InternalServerError(format!(
                "writers length mismatch: expected {}, got {}",
                expected_writers,
                writers.len()
            )));
        }
        if shard_len_limit % block_size != 0 {
            return Err(AppError::InternalServerError(format!(
                "shard_len_limit ({}) must be a multiple of block_size ({})",
                shard_len_limit, block_size
            )));
        }

        let total_hasher = if calc_total_checksum {
            Some(Hasher::new())
        } else {
            None
        };
        let shard_hashers = (0..expected_writers).map(|_| Hasher::new()).collect();
        let rs_encoder = if parity_shards > 0 {
            Some(
                ReedSolomonEncoder::new(data_shards, parity_shards, block_size).map_err(|e| {
                    AppError::InternalServerError(format!("RS init check failed: {:?}", e))
                })?,
            )
        } else {
            None
        };

        let mut data_blocks = Vec::with_capacity(data_shards);
        for idx in 0..data_shards {
            let block = match writers.get_mut(idx).and_then(Option::as_mut) {
                Some(writer) => writer.acquire_block_buffer().map_err(|e| {
                    AppError::Io(e, format!("acquire raw block buffer for shard {}", idx))
                })?,
                None => take_pooled_aligned_buffer(block_size),
            };
            data_blocks.push(block);
        }

        Ok(Self {
            writers,
            data_shards,
            parity_shards,
            shard_len_limit,
            data_blocks,
            stripe_bytes_filled: 0,
            bytes_written_to_shards: 0,
            total_hasher,
            shard_hashers,
            rs_encoder,
            block_size,
        })
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<(), AppError> {
        EC_BYTES_IN_TOTAL.fetch_add(data.len() as u64, Ordering::Relaxed);

        if let Some(h) = self.total_hasher.as_mut() {
            h.update(data);
        }

        let mut offset = 0usize;
        let stripe_capacity = self.block_size * self.data_shards;

        while offset < data.len() {
            let shard_idx = self.stripe_bytes_filled / self.block_size;
            let offset_in_block = self.stripe_bytes_filled % self.block_size;
            let remaining_in_block = self.block_size - offset_in_block;
            let to_copy = std::cmp::min(remaining_in_block, data.len() - offset);

            self.data_blocks[shard_idx].extend_from_slice(&data[offset..offset + to_copy]);
            offset += to_copy;
            self.stripe_bytes_filled += to_copy;

            if self.stripe_bytes_filled == stripe_capacity {
                self.process_stripe().await?;
            }
        }

        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<(), AppError> {
        if self.stripe_bytes_filled > 0 {
            for block in &mut self.data_blocks {
                block.fill_zeros();
            }
            self.process_stripe().await?;
        }

        while self.bytes_written_to_shards < self.shard_len_limit {
            for block in &mut self.data_blocks {
                block.clear();
                block.fill_zeros();
            }
            self.process_stripe().await?;
        }

        if self.writers.iter().filter(|w| w.is_some()).count() < self.data_shards {
            EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(AppError::InternalServerError(
                "Not enough healthy disks to persist data".into(),
            ));
        }

        for writer_opt in self.writers.iter_mut() {
            if let Some(writer) = writer_opt {
                let flush_start = Instant::now();
                if let Err(e) = writer.flush().await {
                    EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!("Failed to flush raw shard writer: {}", e);
                    *writer_opt = None;
                }
                EC_FLUSH_NS_TOTAL
                    .fetch_add(flush_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        }

        if self.writers.iter().filter(|w| w.is_some()).count() < self.data_shards {
            EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(AppError::InternalServerError(
                "Not enough healthy disks to persist data".into(),
            ));
        }

        maybe_log_ec_metrics();
        Ok(())
    }

    async fn process_stripe(&mut self) -> Result<(), AppError> {
        let block_size = self.block_size;

        if self.parity_shards > 0 {
            let encoder = self.rs_encoder.as_mut().ok_or_else(|| {
                EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                AppError::InternalServerError("RS encoder missing for parity setup".into())
            })?;

            for block in &self.data_blocks {
                encoder
                    .add_original_shard(block.as_full_slice())
                    .map_err(|e| AppError::InternalServerError(format!("RS add: {:?}", e)))?;
            }

            let rs_start = Instant::now();
            let result = encoder
                .encode()
                .map_err(|e| AppError::InternalServerError(format!("RS encode: {:?}", e)))?;
            EC_RS_NS_TOTAL.fetch_add(rs_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            for i in 0..self.data_shards {
                self.shard_hashers[i].update(self.data_blocks[i].as_full_slice());
            }
            for i in 0..self.parity_shards {
                let slice = result.recovery(i).ok_or_else(|| {
                    EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    AppError::InternalServerError("RS parity shard generation mismatch".into())
                })?;
                self.shard_hashers[self.data_shards + i].update(slice);
            }

            let mut successful_writes = 0usize;
            for i in 0..self.writers.len() {
                let write_start = Instant::now();
                let write_result = {
                    let writer_opt = &mut self.writers[i];
                    let Some(writer) = writer_opt.as_mut() else {
                        if i < self.data_shards {
                            self.data_blocks[i].clear();
                        }
                        continue;
                    };

                    let block = if i < self.data_shards {
                        let next_block = writer.acquire_block_buffer().map_err(|e| {
                            AppError::Io(e, format!("acquire raw block buffer for shard {}", i))
                        })?;
                        std::mem::replace(&mut self.data_blocks[i], next_block)
                    } else {
                        let parity_idx = i - self.data_shards;
                        let slice = result.recovery(parity_idx).ok_or_else(|| {
                            EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            AppError::InternalServerError("RS parity shard access mismatch".into())
                        })?;
                        let mut parity_block = writer.acquire_block_buffer().map_err(|e| {
                            AppError::Io(
                                e,
                                format!("acquire raw parity block buffer for shard {}", i),
                            )
                        })?;
                        parity_block.extend_from_slice(slice);
                        parity_block
                    };

                    writer.write_aligned_block(block).await
                };

                match write_result {
                    Ok(()) => successful_writes += 1,
                    Err(e) => {
                        EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        warn!("Failed to write raw stripe block to shard {}: {}", i, e);
                        self.writers[i] = None;
                    }
                }
                EC_WRITE_NS_TOTAL
                    .fetch_add(write_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            if successful_writes < self.data_shards {
                EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Err(AppError::InternalServerError(
                    "Not enough healthy disks to persist data".into(),
                ));
            }
        } else {
            for i in 0..self.data_shards {
                self.shard_hashers[i].update(self.data_blocks[i].as_full_slice());
            }

            let mut successful_writes = 0usize;
            for i in 0..self.writers.len() {
                let write_start = Instant::now();
                let write_result = {
                    let writer_opt = &mut self.writers[i];
                    let Some(writer) = writer_opt.as_mut() else {
                        if i < self.data_shards {
                            self.data_blocks[i].clear();
                        }
                        continue;
                    };

                    let next_block = writer.acquire_block_buffer().map_err(|e| {
                        AppError::Io(e, format!("acquire raw block buffer for shard {}", i))
                    })?;
                    let block = std::mem::replace(&mut self.data_blocks[i], next_block);
                    writer.write_aligned_block(block).await
                };

                match write_result {
                    Ok(()) => successful_writes += 1,
                    Err(e) => {
                        EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        warn!("Failed to write raw stripe block to shard {}: {}", i, e);
                        self.writers[i] = None;
                    }
                }
                EC_WRITE_NS_TOTAL
                    .fetch_add(write_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            if successful_writes < self.data_shards {
                EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Err(AppError::InternalServerError(
                    "Not enough healthy disks to persist data".into(),
                ));
            }
        }

        self.stripe_bytes_filled = 0;
        self.bytes_written_to_shards += block_size;
        let stripes = EC_STRIPES_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
        if stripes % 256 == 0 {
            maybe_log_ec_metrics();
        }
        Ok(())
    }

    pub fn get_total_checksum(&self) -> Option<String> {
        self.total_hasher
            .as_ref()
            .map(|h| format!("{:08x}", h.clone().finalize()))
    }

    pub fn get_shard_checksums(&self) -> Vec<String> {
        self.shard_hashers
            .iter()
            .map(|h| format!("{:08x}", h.clone().finalize()))
            .collect()
    }

    pub fn into_writers(self) -> Vec<Option<RawDirectWriter>> {
        let Self {
            writers,
            data_blocks,
            ..
        } = self;
        for block in data_blocks {
            recycle_pooled_aligned_buffer(block);
        }
        writers
    }
}
