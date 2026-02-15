use crate::object_storage::object_parser::{
    ADAPTIVE_STREAM_TARGET_ATOMIC, STREAM_CHUNK_SIZE_ATOMIC, STREAM_MAX_CONCURRENCY,
    STREAM_READAHEAD_BYTES_ATOMIC,
};
use crate::object_storage::system::get_system_metrics;
use crate::object_storage::upload::{ADAPTIVE_IO_TARGET_ATOMIC, MAX_IO_SEMAPHORE};
use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

pub static ADAPTIVE_CONCURRENCY_ENABLED_ATOMIC: OnceCell<AtomicBool> = OnceCell::new();
pub static ADAPTIVE_CONCURRENCY_INTERVAL_MS_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();

fn enabled() -> bool {
    ADAPTIVE_CONCURRENCY_ENABLED_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(true)
}

fn interval_ms() -> u64 {
    ADAPTIVE_CONCURRENCY_INTERVAL_MS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(1000)
        .clamp(100, 10_000)
}

fn clamp_next(cur: usize, min_v: usize, max_v: usize, down_pct: usize, up_pct: usize) -> usize {
    if down_pct > 0 {
        let next = cur.saturating_mul(100 - down_pct) / 100;
        return next.max(min_v).min(max_v);
    }
    if up_pct > 0 {
        let next = cur.saturating_mul(100 + up_pct) / 100;
        return next.max(min_v).min(max_v);
    }
    cur.max(min_v).min(max_v)
}

fn clamp_next_aligned(
    cur: usize,
    min_v: usize,
    max_v: usize,
    down_pct: usize,
    up_pct: usize,
) -> usize {
    let next = clamp_next(cur, min_v, max_v, down_pct, up_pct);
    ((next.max(4096) + 4095) / 4096) * 4096
}

pub async fn run_adaptive_concurrency_loop() {
    let io_max = *MAX_IO_SEMAPHORE.get().unwrap_or(&128usize);
    let stream_max = *STREAM_MAX_CONCURRENCY.get().unwrap_or(&128usize);
    let io_min = (io_max / 8).max(4);
    let stream_min = (stream_max / 8).max(8);
    let stream_chunk_max = STREAM_CHUNK_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(256 * 1024)
        .clamp(64 * 1024, 4 * 1024 * 1024);
    let stream_readahead_max = STREAM_READAHEAD_BYTES_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(512 * 1024)
        .clamp(128 * 1024, 8 * 1024 * 1024)
        .max(stream_chunk_max);
    let stream_chunk_min = (stream_chunk_max / 4).max(64 * 1024);
    let stream_readahead_min = (stream_readahead_max / 4).max(128 * 1024);

    if ADAPTIVE_IO_TARGET_ATOMIC.get().is_none() {
        let _ = ADAPTIVE_IO_TARGET_ATOMIC.set(AtomicUsize::new(io_max));
    }
    if ADAPTIVE_STREAM_TARGET_ATOMIC.get().is_none() {
        let _ = ADAPTIVE_STREAM_TARGET_ATOMIC.set(AtomicUsize::new(stream_max));
    }

    loop {
        if !enabled() {
            tokio::time::sleep(Duration::from_millis(interval_ms())).await;
            continue;
        }

        let m = get_system_metrics().await;
        let cores = std::thread::available_parallelism()
            .map(|n| n.get() as f64)
            .unwrap_or(1.0)
            .max(1.0);
        let load_ratio = m.load_avg_1m / cores;

        let (down_pct, up_pct) = if m.cpu_usage_percent >= 95.0
            || m.ram_usage_percent >= 95.0
            || load_ratio >= 2.0
        {
            (50, 0)
        } else if m.cpu_usage_percent >= 85.0 || m.ram_usage_percent >= 90.0 || load_ratio >= 1.3 {
            (25, 0)
        } else if m.cpu_usage_percent <= 55.0 && m.ram_usage_percent <= 75.0 && load_ratio <= 0.8 {
            (0, 10)
        } else {
            (0, 0)
        };

        if let Some(io_target) = ADAPTIVE_IO_TARGET_ATOMIC.get() {
            let cur = io_target.load(Ordering::Relaxed);
            let next = clamp_next(cur, io_min, io_max, down_pct, up_pct);
            io_target.store(next, Ordering::Relaxed);
        }

        if let Some(stream_target) = ADAPTIVE_STREAM_TARGET_ATOMIC.get() {
            let cur = stream_target.load(Ordering::Relaxed);
            let next = clamp_next(cur, stream_min, stream_max, down_pct, up_pct);
            stream_target.store(next, Ordering::Relaxed);
        }

        if let Some(chunk_atomic) = STREAM_CHUNK_SIZE_ATOMIC.get() {
            let cur = chunk_atomic.load(Ordering::Relaxed);
            let next =
                clamp_next_aligned(cur, stream_chunk_min, stream_chunk_max, down_pct, up_pct);
            chunk_atomic.store(next, Ordering::Relaxed);
        }

        if let Some(readahead_atomic) = STREAM_READAHEAD_BYTES_ATOMIC.get() {
            let cur = readahead_atomic.load(Ordering::Relaxed);
            let mut next = clamp_next_aligned(
                cur,
                stream_readahead_min,
                stream_readahead_max,
                down_pct,
                up_pct,
            );
            if let Some(chunk_atomic) = STREAM_CHUNK_SIZE_ATOMIC.get() {
                let chunk = chunk_atomic.load(Ordering::Relaxed);
                next = next.max(chunk);
            }
            readahead_atomic.store(next, Ordering::Relaxed);
        }

        tokio::time::sleep(Duration::from_millis(interval_ms())).await;
    }
}
