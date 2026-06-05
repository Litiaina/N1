use serde::Serialize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

static CACHE_ENTRIES: AtomicUsize = AtomicUsize::new(0);
static DIRTY_ENTRIES: AtomicUsize = AtomicUsize::new(0);
static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);
static REFRESHES: AtomicU64 = AtomicU64::new(0);
static INVALIDATIONS: AtomicU64 = AtomicU64::new(0);
static FAILURES: AtomicU64 = AtomicU64::new(0);

const DEFAULT_USAGE_CACHE_TTL_SECS: u64 = 15;

#[derive(Clone, Debug, Serialize)]
pub struct CapacityCacheSnapshot {
    pub ttl_seconds: u64,
    pub entries: usize,
    pub dirty_entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub refreshes: u64,
    pub invalidations: u64,
    pub failures: u64,
}

pub fn usage_cache_ttl_seconds() -> u64 {
    std::env::var("N2_USAGE_CACHE_TTL_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_USAGE_CACHE_TTL_SECS)
        .clamp(1, 3600)
}

pub fn record_capacity_cache_hit() {
    HITS.fetch_add(1, Ordering::Relaxed);
}

pub fn record_capacity_cache_miss() {
    MISSES.fetch_add(1, Ordering::Relaxed);
}

pub fn record_capacity_cache_refresh() {
    REFRESHES.fetch_add(1, Ordering::Relaxed);
}

pub fn record_capacity_cache_invalidation() {
    INVALIDATIONS.fetch_add(1, Ordering::Relaxed);
}

pub fn record_capacity_cache_failure() {
    FAILURES.fetch_add(1, Ordering::Relaxed);
}

pub fn set_capacity_cache_size(entries: usize, dirty_entries: usize) {
    CACHE_ENTRIES.store(entries, Ordering::Relaxed);
    DIRTY_ENTRIES.store(dirty_entries, Ordering::Relaxed);
}

pub fn capacity_cache_snapshot() -> CapacityCacheSnapshot {
    CapacityCacheSnapshot {
        ttl_seconds: usage_cache_ttl_seconds(),
        entries: CACHE_ENTRIES.load(Ordering::Relaxed),
        dirty_entries: DIRTY_ENTRIES.load(Ordering::Relaxed),
        hits: HITS.load(Ordering::Relaxed),
        misses: MISSES.load(Ordering::Relaxed),
        refreshes: REFRESHES.load(Ordering::Relaxed),
        invalidations: INVALIDATIONS.load(Ordering::Relaxed),
        failures: FAILURES.load(Ordering::Relaxed),
    }
}
