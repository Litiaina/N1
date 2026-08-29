use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use ecs_core::{
    ChecksumPolicy, CompletionPolicy, Digest, EcLayout, EcsError, EcsErrorKind, EncodeOptions,
    EncodeOutput, ErasureStreamEncoder, ShardWriters, metrics_snapshot,
};
use once_cell::sync::Lazy;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

use crate::enums::app_error_enum::AppError;

pub use ecs_core::{ALIGNMENT, AlignedBuffer};

const DEFAULT_EC_LOG_INTERVAL_SECS: u64 = 30;
static EC_LAST_LOG_SECS: AtomicU64 = AtomicU64::new(0);
static EC_LOG_INTERVAL_SECS: Lazy<u64> = Lazy::new(|| {
    std::env::var("N2_EC_METRICS_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.clamp(5, 3600))
        .unwrap_or(DEFAULT_EC_LOG_INTERVAL_SECS)
});

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn maybe_log_ec_metrics() {
    let now = now_unix_secs();
    let last = EC_LAST_LOG_SECS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < *EC_LOG_INTERVAL_SECS {
        return;
    }
    if EC_LAST_LOG_SECS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let metrics = metrics_snapshot();
    let source_mib = metrics.source_bytes as f64 / (1024.0 * 1024.0);
    let codec_ms = metrics.codec_nanoseconds as f64 / 1_000_000.0;
    let checksum_ms = metrics.checksum_nanoseconds as f64 / 1_000_000.0;
    let write_ms = metrics.write_nanoseconds as f64 / 1_000_000.0;
    let flush_ms = metrics.flush_nanoseconds as f64 / 1_000_000.0;
    let io_seconds =
        (metrics.write_nanoseconds + metrics.flush_nanoseconds) as f64 / 1_000_000_000.0;
    let effective_mib_s = source_mib / io_seconds.max(0.000_001);

    info!(
        "EC metrics: stripes={}, input_mib={:.2}, codec_ms={:.2}, checksum_ms={:.2}, write_ms={:.2}, flush_ms={:.2}, effective_mib_s={:.2}, errors={}, missing_pieces={}",
        metrics.stripes,
        source_mib,
        codec_ms,
        checksum_ms,
        write_ms,
        flush_ms,
        effective_mib_s,
        metrics.errors,
        metrics.degraded_pieces,
    );
}

fn map_ecs_error(error: EcsError, context: &'static str) -> AppError {
    if error.kind() == EcsErrorKind::CapacityExhausted {
        return AppError::InsufficientStorage;
    }
    if let Some(raw_os_error) = error.raw_os_error() {
        return AppError::Io(io::Error::from_raw_os_error(raw_os_error), context.into());
    }

    match error.kind() {
        EcsErrorKind::IncompleteInput
        | EcsErrorKind::InputTooLarge
        | EcsErrorKind::ChecksumMismatch => AppError::BadRequest(error.to_string()),
        EcsErrorKind::CapacityExhausted => AppError::InsufficientStorage,
        EcsErrorKind::ReaderFailure
        | EcsErrorKind::WriterFailure
        | EcsErrorKind::InsufficientShards
        | EcsErrorKind::StreamFailed
        | EcsErrorKind::Cancelled => {
            AppError::ServiceUnavailable(format!("ECS Core {context} failed: {error}"))
        }
        EcsErrorKind::InvalidLayout
        | EcsErrorKind::InvalidStripeSize
        | EcsErrorKind::CodecFailure
        | EcsErrorKind::InternalInvariant => {
            AppError::InternalServerError(format!("ECS Core {context} invariant failed: {error}"))
        }
    }
}

/// N2 compatibility adapter over ECS Core's bounded streaming encoder.
///
/// N2 retains ownership of pool authority, node/disk placement, survivor-write
/// admission, temporary files, durability sync, atomic publication, NCDX/NCNX,
/// and repair scheduling. ECS Core owns only deterministic bytes-to-EC-pieces.
pub struct ErasureStreamWriter<W>
where
    W: AsyncWrite + Unpin + Send,
{
    encoder: Option<ErasureStreamEncoder<W>>,
    output: Option<EncodeOutput<W>>,
}

impl<W> ErasureStreamWriter<W>
where
    W: AsyncWrite + Unpin + Send,
{
    pub fn new(
        mut writers: Vec<Option<W>>,
        data_shards: usize,
        parity_shards: usize,
        expected_source_bytes: u64,
        calculate_source_checksum: bool,
        block_size: usize,
    ) -> Result<Self, AppError> {
        let layout = EcLayout::new(data_shards, parity_shards, block_size)
            .map_err(|error| map_ecs_error(error, "initialize"))?;
        if writers.len() != layout.total_shards() {
            return Err(AppError::InternalServerError(format!(
                "writer layout mismatch: expected {}, got {}",
                layout.total_shards(),
                writers.len()
            )));
        }
        let parity = writers.split_off(data_shards);
        let encoder = ErasureStreamEncoder::new(
            layout,
            ShardWriters::new(writers, parity),
            expected_source_bytes,
            EncodeOptions {
                completion: CompletionPolicy::AllowReconstructable,
                checksum: Some(ChecksumPolicy::sha256_with_blocks(
                    calculate_source_checksum,
                )),
                expected_source_checksum: None,
            },
        )
        .map_err(|error| map_ecs_error(error, "initialize"))?;

        Ok(Self {
            encoder: Some(encoder),
            output: None,
        })
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<(), AppError> {
        self.encoder_mut()?
            .write(data)
            .await
            .map_err(|error| map_ecs_error(error, "write"))
    }

    pub async fn write_exact_from_reader<R>(
        &mut self,
        reader: &mut R,
        content_length: u64,
    ) -> Result<u64, AppError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let read = self
            .encoder_mut()?
            .copy_exact(reader)
            .await
            .map_err(|error| map_ecs_error(error, "read source"))?;
        if read != content_length {
            return Err(AppError::BadRequest(format!(
                "Incomplete body: got {read} bytes, expected {content_length}"
            )));
        }
        Ok(read)
    }

    pub async fn finalize(&mut self) -> Result<(), AppError> {
        let encoder = self.encoder.take().ok_or_else(|| {
            AppError::InternalServerError("ECS Core encoder was already finalized".into())
        })?;
        let output = encoder
            .finish()
            .await
            .map_err(|error| map_ecs_error(error, "finalize"))?;
        self.output = Some(output);
        maybe_log_ec_metrics();
        Ok(())
    }

    pub fn get_total_checksum(&self) -> Option<String> {
        self.output
            .as_ref()
            .and_then(|output| output.report.source_checksum.as_ref())
            .map(Digest::to_hex)
    }

    pub fn get_shard_checksums(&self) -> Vec<String> {
        self.output
            .as_ref()
            .map(|output| {
                output
                    .report
                    .pieces
                    .iter()
                    .map(|piece| {
                        piece
                            .checksum
                            .as_ref()
                            .map(Digest::to_hex)
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_shard_block_checksums(&self) -> Vec<Vec<[u8; 32]>> {
        let Some(output) = self.output.as_ref() else {
            return Vec::new();
        };
        output
            .report
            .pieces
            .iter()
            .map(|piece| {
                piece
                    .block_checksums
                    .iter()
                    .map(|digest| match digest {
                        Digest::Sha256(value) => *value,
                        Digest::Crc32(_) => {
                            unreachable!("N2 configures ECS Core with SHA-256 only")
                        }
                    })
                    .collect()
            })
            .collect()
    }

    pub fn into_writers(self) -> Vec<Option<W>> {
        let output = self
            .output
            .expect("N2 must finalize ECS Core before retrieving piece writers");
        output
            .writers
            .data
            .into_iter()
            .chain(output.writers.parity)
            .collect()
    }

    fn encoder_mut(&mut self) -> Result<&mut ErasureStreamEncoder<W>, AppError> {
        self.encoder.as_mut().ok_or_else(|| {
            AppError::InternalServerError("ECS Core encoder was already finalized".into())
        })
    }
}

#[cfg(test)]
mod tests;
