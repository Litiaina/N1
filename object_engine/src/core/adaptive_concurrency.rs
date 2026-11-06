use crate::core::io_scheduler::{DiskIoPressure, disk_io_pressure, set_foreground_io_target};
use crate::core::object_parser::{
    ADAPTIVE_STREAM_TARGET_ATOMIC, STREAM_CHUNK_SIZE_ATOMIC, STREAM_MAX_CONCURRENCY,
    STREAM_READAHEAD_BYTES_ATOMIC,
};
use crate::core::system::get_system_metrics;
use crate::core::upload::{ADAPTIVE_IO_TARGET_ATOMIC, MAX_IO_SEMAPHORE};
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
        .clamp(1000, 300_000)
}

fn clamp_next(cur: usize, min_v: usize, max_v: usize, down_pct: usize, up_pct: usize) -> usize {
    let cur = cur.max(min_v).min(max_v);
    if down_pct > 0 {
        let retained_pct = 100usize.saturating_sub(down_pct.min(100));
        let next = cur.saturating_mul(retained_pct) / 100;
        return next.max(min_v).min(max_v);
    }
    if up_pct > 0 && cur < max_v {
        // Percentage growth performed with integer arithmetic must still make
        // forward progress. Without the minimum increment, values such as
        // 4 * 110 / 100 truncate back to 4 and permanently pin the adaptive
        // controller at its floor after a pressure event.
        let increase = cur.saturating_mul(up_pct).div_ceil(100).max(1);
        let next = cur.saturating_add(increase);
        return next.max(min_v).min(max_v);
    }
    cur
}

fn clamp_next_aligned(
    cur: usize,
    min_v: usize,
    max_v: usize,
    down_pct: usize,
    up_pct: usize,
) -> usize {
    let next = clamp_next(cur, min_v, max_v, down_pct, up_pct);
    next.max(4096).div_ceil(4096) * 4096
}

// Queued demand alone must not pin a reduced target forever. Recover within
// existing bounds; saturation, stalls and host resource pressure still apply.
fn adjustment(
    io_pressure: DiskIoPressure,
    cpu_usage_percent: f64,
    ram_usage_percent: f64,
    load_ratio: f64,
) -> (usize, usize) {
    // Linux load average includes tasks blocked on disk I/O, not just runnable
    // CPU work. Let the I/O scheduler classify that pressure; load alone must
    // not repeatedly halve admission or prevent recovery after a slow flush.
    let cpu_backlog = cpu_usage_percent >= 70.0;
    if io_pressure == DiskIoPressure::Stalled
        || cpu_usage_percent >= 95.0
        || ram_usage_percent >= 95.0
        || (cpu_backlog && load_ratio >= 2.0)
    {
        (50, 0)
    } else if cpu_usage_percent >= 85.0
        || ram_usage_percent >= 90.0
        || (cpu_backlog && load_ratio >= 1.3)
    {
        (25, 0)
    } else if io_pressure <= DiskIoPressure::Pressured
        && cpu_usage_percent <= 55.0
        && ram_usage_percent <= 75.0
    {
        (0, 10)
    } else {
        (0, 0)
    }
}

pub async fn run_adaptive_concurrency_loop() {
    run_adaptive_concurrency_until(tokio_util::sync::CancellationToken::new()).await;
}

pub async fn run_adaptive_concurrency_until(stop: tokio_util::sync::CancellationToken) {
    let io_max = *MAX_IO_SEMAPHORE.get().unwrap_or(&128usize);
    let stream_max = *STREAM_MAX_CONCURRENCY.get().unwrap_or(&128usize);
    let io_min = (io_max / 8).max(4).min(io_max);
    let stream_min = (stream_max / 8).max(8).min(stream_max);
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
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(interval_ms())) => {}
            }
            continue;
        }

        let m = get_system_metrics().await;
        let cores = std::thread::available_parallelism()
            .map(|n| n.get() as f64)
            .unwrap_or(1.0)
            .max(1.0);
        let load_ratio = m.load_avg_1m / cores;

        let io_pressure = disk_io_pressure();
        let (down_pct, up_pct) = adjustment(
            io_pressure,
            m.cpu_usage_percent,
            m.ram_usage_percent,
            load_ratio,
        );

        if let Some(io_target) = ADAPTIVE_IO_TARGET_ATOMIC.get() {
            let cur = io_target.load(Ordering::Relaxed);
            let next = clamp_next(cur, io_min, io_max, down_pct, up_pct);
            io_target.store(next, Ordering::Relaxed);
            set_foreground_io_target(next);
        }

        if let Some(stream_target) = ADAPTIVE_STREAM_TARGET_ATOMIC.get() {
            let cur = stream_target.load(Ordering::Relaxed);
            let next = clamp_next(cur, stream_min, stream_max, down_pct, up_pct);
            stream_target.store(next, Ordering::Relaxed);
            crate::core::object_parser::set_stream_target(next);
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

        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(interval_ms())) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{adjustment, clamp_next};
    use crate::core::io_scheduler::DiskIoPressure;

    #[test]
    fn healthy_queued_work_recovers_capacity_without_waiting_for_an_empty_queue() {
        let mut target = 4;
        for expected in [5, 6, 6] {
            let (down, up) = adjustment(DiskIoPressure::Pressured, 35.0, 48.0, 0.7);
            target = clamp_next(target, 4, 6, down, up);
            assert_eq!(target, expected);
        }
    }

    #[test]
    fn saturation_or_resource_pressure_does_not_trigger_growth() {
        assert_eq!(
            adjustment(DiskIoPressure::Saturated, 35.0, 48.0, 0.7),
            (0, 0)
        );
        assert_eq!(
            adjustment(DiskIoPressure::Stalled, 35.0, 48.0, 0.7),
            (50, 0)
        );
        for (cpu, ram, load) in [(95.0, 48.0, 0.7), (35.0, 95.0, 0.7), (75.0, 48.0, 2.0)] {
            assert_eq!(
                adjustment(DiskIoPressure::Pressured, cpu, ram, load),
                (50, 0)
            );
        }
        for (cpu, ram, load) in [(85.0, 48.0, 0.7), (35.0, 90.0, 0.7), (75.0, 48.0, 1.3)] {
            assert_eq!(
                adjustment(DiskIoPressure::Pressured, cpu, ram, load),
                (25, 0)
            );
        }
        for (cpu, ram, load) in [(56.0, 48.0, 0.7), (35.0, 76.0, 0.7), (69.0, 48.0, 2.0)] {
            assert_eq!(
                adjustment(DiskIoPressure::Pressured, cpu, ram, load),
                (0, 0)
            );
        }
    }

    #[test]
    fn io_wait_load_does_not_masquerade_as_cpu_saturation() {
        for load in [1.3, 2.0, 20.0] {
            assert_eq!(
                adjustment(DiskIoPressure::Pressured, 35.0, 48.0, load),
                (0, 10),
                "I/O-blocked load must not prevent bounded recovery"
            );
            assert_eq!(
                adjustment(DiskIoPressure::Saturated, 35.0, 48.0, load),
                (0, 0),
                "actual disk saturation must still prevent growth"
            );
            assert_eq!(
                adjustment(DiskIoPressure::Stalled, 35.0, 48.0, load),
                (50, 0),
                "actual disk stalls must still reduce concurrency"
            );
        }
    }

    #[test]
    fn busy_cpu_and_load_still_reduce_capacity() {
        assert_eq!(adjustment(DiskIoPressure::Normal, 70.0, 48.0, 1.3), (25, 0));
        assert_eq!(adjustment(DiskIoPressure::Normal, 70.0, 48.0, 2.0), (50, 0));
    }

    #[test]
    fn upward_recovery_always_advances_below_the_maximum() {
        for current in 4..8 {
            let next = clamp_next(current, 4, 8, 0, 10);
            assert!(
                next > current,
                "adaptive recovery did not advance from {current}: {next}"
            );
            assert!(next <= 8);
        }
    }

    #[test]
    fn recovery_reaches_the_maximum_from_the_minimum() {
        let mut current = 4;
        for _ in 0..8 {
            current = clamp_next(current, 4, 8, 0, 10);
        }
        assert_eq!(current, 8);
    }

    #[test]
    fn pressure_reduction_respects_the_configured_floor() {
        assert_eq!(clamp_next(8, 4, 8, 50, 0), 4);
        assert_eq!(clamp_next(4, 4, 8, 50, 0), 4);
    }

    #[test]
    fn stable_and_out_of_range_targets_remain_bounded() {
        assert_eq!(clamp_next(3, 4, 8, 0, 0), 4);
        assert_eq!(clamp_next(12, 4, 8, 0, 10), 8);
        assert_eq!(clamp_next(8, 4, 8, 0, 10), 8);
    }
}
