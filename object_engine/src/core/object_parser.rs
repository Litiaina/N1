//! Streaming controls retained from N1's object parser. LINK supplies verified
//! stripes; admission and client-facing chunk sizes remain independent of format.
use crate::core::io_scheduler::FairIoGate;
use crate::core::{Error, Result};
use once_cell::sync::{Lazy, OnceCell};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::OwnedSemaphorePermit;

pub static STREAM_MAX_CONCURRENCY: OnceCell<usize> = OnceCell::new();
pub static STREAM_CHUNK_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static ADAPTIVE_STREAM_TARGET_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static STREAM_READAHEAD_BYTES_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static STREAM_LOCALITY_BIAS_ENABLED_ATOMIC: OnceCell<AtomicBool> = OnceCell::new();
static STREAM_GATE: Lazy<Arc<FairIoGate>> = Lazy::new(|| {
    Arc::new(FairIoGate::new(
        *STREAM_MAX_CONCURRENCY.get().unwrap_or(&128),
    ))
});
static ACTIVE_STREAM_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn foreground_streams_active() -> bool {
    ACTIVE_STREAM_INFLIGHT.load(Ordering::Relaxed) != 0
}

pub(crate) fn set_stream_target(target: usize) {
    STREAM_GATE.set_target(target);
}

pub(crate) struct AdaptiveStreamGuard {
    permit: Option<OwnedSemaphorePermit>,
}
impl Drop for AdaptiveStreamGuard {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            STREAM_GATE.release(permit);
            ACTIVE_STREAM_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
pub(crate) async fn acquire_adaptive_stream_guard() -> Result<AdaptiveStreamGuard> {
    let permit = tokio::time::timeout(
        crate::io_control::foreground_wait_timeout(),
        STREAM_GATE.semaphore.clone().acquire_owned(),
    )
    .await
    .map_err(|_| Error::Unavailable("stream admission timed out".into()))?
    .map_err(|_| Error::Unavailable("stream admission closed".into()))?;
    ACTIVE_STREAM_INFLIGHT.fetch_add(1, Ordering::Relaxed);
    Ok(AdaptiveStreamGuard {
        permit: Some(permit),
    })
}

pub(crate) fn stream_chunk_size_bytes() -> usize {
    STREAM_CHUNK_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(256 * 1024)
        .clamp(4096, 4 * 1024 * 1024)
}
pub(crate) fn stream_readahead_bytes() -> usize {
    STREAM_READAHEAD_BYTES_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(512 * 1024)
        .clamp(64 * 1024, 8 * 1024 * 1024)
}
pub(crate) fn adaptive_emit_bytes() -> usize {
    let base = stream_chunk_size_bytes().min(stream_readahead_bytes());
    let target = ADAPTIVE_STREAM_TARGET_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(|| *STREAM_MAX_CONCURRENCY.get().unwrap_or(&128))
        .max(1);
    let active = ACTIVE_STREAM_INFLIGHT.load(Ordering::Relaxed);
    let adjusted = if active >= target.saturating_mul(2) {
        base / 4
    } else if active >= target {
        base / 2
    } else if active.saturating_mul(2) >= target {
        base.saturating_mul(3) / 4
    } else {
        base
    };
    adjusted.clamp(4096, 4 * 1024 * 1024)
}
