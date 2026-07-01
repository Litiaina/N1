use crate::enums::app_error_enum::AppError;
use crc32fast::Hasher;
use futures_util::future::join_all;
use futures_util::future::poll_fn;
use once_cell::sync::Lazy;
use simd_erasure_core::ReedSolomonEncoder;
use std::alloc::{Layout, alloc, dealloc};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
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

static EC_LOG_INTERVAL_SECS: Lazy<u64> = Lazy::new(|| {
    std::env::var("N2_EC_METRICS_INTERVAL_SECS")
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

    pub async fn read_from<R>(&mut self, reader: &mut R, max_bytes: usize) -> std::io::Result<usize>
    where
        R: AsyncRead + Unpin,
    {
        let spare = self.layout.size().saturating_sub(self.len).min(max_bytes);
        if spare == 0 {
            return Ok(0);
        }

        let start = self.len;
        let read = unsafe {
            let ptr = self.ptr.as_ptr().add(start).cast::<MaybeUninit<u8>>();
            let uninit = std::slice::from_raw_parts_mut(ptr, spare);
            let mut read_buf = ReadBuf::uninit(uninit);
            poll_fn(|cx| Pin::new(&mut *reader).poll_read(cx, &mut read_buf)).await?;
            read_buf.filled().len()
        };
        self.len += read;
        Ok(read)
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
    shard_block_checksums: Vec<Vec<u32>>,
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
        let shard_block_checksums = vec![Vec::new(); data_shards + parity_shards];

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
            buffer: AlignedBuffer::new(block_size * data_shards),
            bytes_written_to_shards: 0,
            total_hasher,
            shard_hashers,
            shard_block_checksums,
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

    pub async fn write_exact_from_reader<R>(
        &mut self,
        reader: &mut R,
        content_length: u64,
    ) -> Result<u64, AppError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let mut total_bytes_read = 0u64;
        let stripe_capacity = self.buffer.capacity();

        while total_bytes_read < content_length {
            if self.buffer.len() == stripe_capacity {
                self.process_stripe().await?;
            }

            let before = self.buffer.len();
            let remaining = (content_length - total_bytes_read).min(usize::MAX as u64) as usize;
            let read = self
                .buffer
                .read_from(reader, remaining)
                .await
                .map_err(|e| AppError::Io(e, "native upload stream".into()))?;
            if read == 0 {
                return Err(AppError::BadRequest(format!(
                    "Incomplete body: got {} bytes, expected {}",
                    total_bytes_read, content_length
                )));
            }

            EC_BYTES_IN_TOTAL.fetch_add(read as u64, Ordering::Relaxed);
            if let Some(h) = self.total_hasher.as_mut() {
                h.update(&self.buffer.as_slice()[before..before + read]);
            }
            total_bytes_read += read as u64;
        }

        Ok(total_bytes_read)
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
            for checksums in self.shard_block_checksums.iter_mut() {
                checksums.extend(
                    padding_buf
                        .as_slice()
                        .chunks(self.block_size)
                        .map(crc32fast::hash),
                );
            }

            let padding_slices = vec![padding_buf.as_slice(); self.writers.len()];
            write_slices_to_shards(&mut self.writers, &padding_slices, self.data_shards).await?;
        }

        if self.writers.iter().filter(|w| w.is_some()).count() < self.data_shards {
            EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(AppError::InternalServerError(
                "Not enough healthy disks to persist data".into(),
            ));
        }

        flush_shard_writers(&mut self.writers, self.data_shards).await?;

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
                self.shard_block_checksums[i].push(crc32fast::hash(&buffer_slice[start..end]));
            }
            for i in 0..self.parity_shards {
                let slice = result.recovery(i).ok_or_else(|| {
                    EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    AppError::InternalServerError("RS parity shard generation mismatch".into())
                })?;
                self.shard_hashers[self.data_shards + i].update(slice);
                self.shard_block_checksums[self.data_shards + i].push(crc32fast::hash(slice));
            }
            let mut shard_slices = Vec::with_capacity(self.data_shards + self.parity_shards);
            for (i, writer_opt) in self.writers.iter().enumerate() {
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
                if writer_opt.is_some() {
                    shard_slices.push(slice);
                } else {
                    shard_slices.push(&[]);
                }
            }
            write_slices_to_shards(&mut self.writers, &shard_slices, self.data_shards).await?;
        } else {
            for i in 0..self.data_shards {
                let start = i * block_size;
                let end = start + block_size;
                self.shard_hashers[i].update(&buffer_slice[start..end]);
                self.shard_block_checksums[i].push(crc32fast::hash(&buffer_slice[start..end]));
            }

            let mut shard_slices = Vec::with_capacity(self.data_shards);
            for (i, writer_opt) in self.writers.iter().enumerate() {
                let start = i * block_size;
                let end = start + block_size;
                if writer_opt.is_some() {
                    shard_slices.push(&self.buffer.as_full_slice()[start..end]);
                } else {
                    shard_slices.push(&[]);
                }
            }
            write_slices_to_shards(&mut self.writers, &shard_slices, self.data_shards).await?;
        }

        self.buffer.clear();
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

    pub fn get_shard_block_checksums(&self) -> Vec<Vec<u32>> {
        self.shard_block_checksums.clone()
    }

    pub fn into_writers(self) -> Vec<Option<W>> {
        self.writers
    }
}

async fn write_slices_to_shards<W>(
    writers: &mut [Option<W>],
    slices: &[&[u8]],
    data_shards: usize,
) -> Result<(), AppError>
where
    W: AsyncWrite + Unpin + Send,
{
    let write_futs = writers
        .iter_mut()
        .enumerate()
        .filter_map(|(idx, writer_opt)| {
            let writer = writer_opt.as_mut()?;
            let slice = *slices.get(idx)?;
            Some(async move {
                let write_start = Instant::now();
                let result = writer.write_all(slice).await;
                (idx, result, write_start.elapsed().as_nanos() as u64)
            })
        });

    let results = join_all(write_futs).await;
    let mut successful_writes = 0usize;
    let mut failed = Vec::new();

    for (idx, result, elapsed_ns) in results {
        EC_WRITE_NS_TOTAL.fetch_add(elapsed_ns, Ordering::Relaxed);
        match result {
            Ok(()) => successful_writes += 1,
            Err(err) => {
                EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                warn!("Failed to write stripe to shard: {}", err);
                failed.push(idx);
            }
        }
    }

    for idx in failed {
        if let Some(slot) = writers.get_mut(idx) {
            *slot = None;
        }
    }

    if successful_writes < data_shards {
        EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
        return Err(AppError::InternalServerError(
            "Not enough healthy disks to persist data".into(),
        ));
    }

    Ok(())
}

async fn flush_shard_writers<W>(
    writers: &mut [Option<W>],
    data_shards: usize,
) -> Result<(), AppError>
where
    W: AsyncWrite + Unpin + Send,
{
    let flush_futs = writers
        .iter_mut()
        .enumerate()
        .filter_map(|(idx, writer_opt)| {
            let writer = writer_opt.as_mut()?;
            Some(async move {
                let flush_start = Instant::now();
                let result = writer.flush().await;
                (idx, result, flush_start.elapsed().as_nanos() as u64)
            })
        });

    let results = join_all(flush_futs).await;
    let mut failed = Vec::new();

    for (idx, result, elapsed_ns) in results {
        EC_FLUSH_NS_TOTAL.fetch_add(elapsed_ns, Ordering::Relaxed);
        if let Err(err) = result {
            EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            warn!("Failed to flush shard writer: {}", err);
            failed.push(idx);
        }
    }

    for idx in failed {
        if let Some(slot) = writers.get_mut(idx) {
            *slot = None;
        }
    }

    if writers.iter().filter(|w| w.is_some()).count() < data_shards {
        EC_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
        return Err(AppError::InternalServerError(
            "Not enough healthy disks to persist data".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ErasureStreamWriter;

    #[tokio::test]
    async fn stream_writer_records_checksums_for_every_immutable_shard_block() {
        let block_size = 4096;
        let input = (0..block_size * 2)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let writers = vec![
            Some(tokio::io::sink()),
            Some(tokio::io::sink()),
            Some(tokio::io::sink()),
        ];
        let mut writer = ErasureStreamWriter::new(writers, 2, 1, block_size, false, block_size)
            .expect("create writer");
        writer.write(&input).await.expect("write stripe");
        writer.finalize().await.expect("finalize");

        let checksums = writer.get_shard_block_checksums();
        assert_eq!(checksums.len(), 3);
        assert!(checksums.iter().all(|blocks| blocks.len() == 1));
        assert_eq!(checksums[0][0], crc32fast::hash(&input[..block_size]));
        assert_eq!(checksums[1][0], crc32fast::hash(&input[block_size..]));
    }
}
