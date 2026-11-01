use crate::enums::app_error_enum::AppError;
use crate::io_control::{
    ProgressTracker, StallSnapshot, foreground_wait_timeout, record_io_latency, stall_threshold,
};
use once_cell::sync::{Lazy, OnceCell};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;

static SCHEDULER_CONFIG: OnceCell<DiskIoSchedulerConfig> = OnceCell::new();
static TOTAL_GATE: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(scheduler_config().total_permits)));
static FOREGROUND_GATE: Lazy<FairIoGate> =
    Lazy::new(|| FairIoGate::new(scheduler_config().foreground_permits));
static BACKGROUND_GATE: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(scheduler_config().background_permits)));
static FOREGROUND_FRAGMENT_GATES: Lazy<Mutex<HashMap<String, Arc<Semaphore>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static BACKGROUND_FRAGMENT_GATES: Lazy<Mutex<HashMap<String, Arc<Semaphore>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static ACTIVE_FOREGROUND_IO: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));
static ACTIVE_BACKGROUND_IO: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));
static QUEUED_FOREGROUND_IO: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));
static QUEUED_BACKGROUND_IO: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));
static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);
static ACTIVE_OPERATIONS: Lazy<Mutex<HashMap<u64, Arc<ActiveIoOperation>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static CLASS_COUNTERS: Lazy<Vec<ClassCounters>> = Lazy::new(|| {
    (0..DiskIoClass::COUNT)
        .map(|_| ClassCounters::default())
        .collect()
});

const DISK_IO_WAIT_LOG_SECS: u64 = 30;
const MAX_ACTIVE_OPERATION_SNAPSHOTS: usize = 64;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct DiskIoSchedulerConfig {
    pub total_permits: usize,
    pub foreground_permits: usize,
    pub background_permits: usize,
    pub foreground_per_fragment_permits: usize,
    pub background_per_fragment_permits: usize,
}

impl DiskIoSchedulerConfig {
    pub fn from_runtime(max_concurrent_io: usize, blob_roots: usize) -> Self {
        derive_scheduler_config(max_concurrent_io, blob_roots)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskIoClass {
    ForegroundRead,
    ForegroundWrite,
    BackgroundRepair,
    BackgroundGc,
    BackgroundRedistribution,
    BackgroundExport,
}

impl DiskIoClass {
    const COUNT: usize = 6;
    const ALL: [Self; Self::COUNT] = [
        Self::ForegroundRead,
        Self::ForegroundWrite,
        Self::BackgroundRepair,
        Self::BackgroundGc,
        Self::BackgroundRedistribution,
        Self::BackgroundExport,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::ForegroundRead => "foreground read",
            Self::ForegroundWrite => "foreground write",
            Self::BackgroundRepair => "background repair",
            Self::BackgroundGc => "background gc",
            Self::BackgroundRedistribution => "background redistribution",
            Self::BackgroundExport => "background export",
        }
    }

    fn metric_label(self) -> &'static str {
        match self {
            Self::ForegroundRead => "foreground_read",
            Self::ForegroundWrite => "foreground_write",
            Self::BackgroundRepair => "background_repair",
            Self::BackgroundGc => "background_gc",
            Self::BackgroundRedistribution => "background_redistribution",
            Self::BackgroundExport => "background_export",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::ForegroundRead => 0,
            Self::ForegroundWrite => 1,
            Self::BackgroundRepair => 2,
            Self::BackgroundGc => 3,
            Self::BackgroundRedistribution => 4,
            Self::BackgroundExport => 5,
        }
    }

    fn is_foreground(self) -> bool {
        matches!(self, Self::ForegroundRead | Self::ForegroundWrite)
    }
}

#[derive(Default)]
struct ClassCounters {
    queued: AtomicUsize,
    completed: AtomicU64,
    failed: AtomicU64,
    bytes: AtomicU64,
    stalls: AtomicU64,
}

struct ActiveIoOperation {
    id: u64,
    class: DiskIoClass,
    context: String,
    fragment: Option<String>,
    progress: Arc<ProgressTracker>,
    stall_reported: AtomicBool,
}

/// Resize the existing FIFO semaphore without making waiters surrender their
/// queue position. A lower target retires idle permits immediately, then takes
/// any remainder from finishing operations; in-flight work is never cancelled.
struct FairIoGate {
    semaphore: Arc<Semaphore>,
    maximum: usize,
    state: Mutex<FairIoGateState>,
}

struct FairIoGateState {
    target: usize,
    reserved: Vec<OwnedSemaphorePermit>,
}

impl FairIoGate {
    fn new(maximum: usize) -> Self {
        let maximum = maximum.max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(maximum)),
            maximum,
            state: Mutex::new(FairIoGateState {
                target: maximum,
                reserved: Vec::new(),
            }),
        }
    }

    fn target(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .target
    }

    fn set_target(&self, target: usize) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.target = target.clamp(1, self.maximum);
        let reserve = self.maximum - state.target;
        while state.reserved.len() > reserve {
            drop(state.reserved.pop());
        }
        while state.reserved.len() < reserve {
            let Ok(permit) = self.semaphore.clone().try_acquire_owned() else {
                break;
            };
            state.reserved.push(permit);
        }
    }

    fn release(&self, permit: OwnedSemaphorePermit) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.reserved.len() < self.maximum - state.target {
            state.reserved.push(permit);
        }
        // Otherwise `permit` returns to the semaphore's existing FIFO queue.
    }
}

#[derive(Clone)]
pub struct IoProgress {
    progress: Arc<ProgressTracker>,
}

impl IoProgress {
    pub fn record(&self, bytes: u64) {
        self.progress.record(bytes);
    }

    pub fn heartbeat(&self) {
        self.progress.heartbeat();
    }
}

pub struct DiskIoGuard {
    operation: Arc<ActiveIoOperation>,
    started: Instant,
    completed: bool,
    _total: OwnedSemaphorePermit,
    _class: Option<OwnedSemaphorePermit>,
    _fragment: Option<OwnedSemaphorePermit>,
}

impl DiskIoGuard {
    pub fn record_progress(&self, bytes: u64) {
        self.operation.progress.record(bytes);
    }

    pub fn heartbeat(&self) {
        self.operation.progress.heartbeat();
    }

    pub fn progress_handle(&self) -> IoProgress {
        IoProgress {
            progress: self.operation.progress.clone(),
        }
    }

    pub fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for DiskIoGuard {
    fn drop(&mut self) {
        if self.operation.class.is_foreground() {
            ACTIVE_FOREGROUND_IO.fetch_sub(1, Ordering::Relaxed);
        } else {
            ACTIVE_BACKGROUND_IO.fetch_sub(1, Ordering::Relaxed);
        }

        if let Ok(mut active) = ACTIVE_OPERATIONS.lock() {
            active.remove(&self.operation.id);
        }

        let snapshot = self.operation.progress.snapshot(stall_threshold());
        let counters = class_counters(self.operation.class);
        counters.bytes.fetch_add(snapshot.bytes, Ordering::Relaxed);
        if self.completed {
            counters.completed.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.failed.fetch_add(1, Ordering::Relaxed);
        }
        if snapshot.stalled && !self.operation.stall_reported.swap(true, Ordering::Relaxed) {
            counters.stalls.fetch_add(1, Ordering::Relaxed);
        }

        record_io_latency(
            format!("disk.operation.{}", self.operation.class.metric_label()),
            self.started.elapsed(),
            snapshot.bytes,
            self.completed,
        );
        if self.operation.class.is_foreground()
            && let Some(permit) = self._class.take()
        {
            FOREGROUND_GATE.release(permit);
        }
    }
}

struct QueueRegistration {
    foreground: bool,
    class: DiskIoClass,
}

impl QueueRegistration {
    fn new(class: DiskIoClass) -> Self {
        if class.is_foreground() {
            QUEUED_FOREGROUND_IO.fetch_add(1, Ordering::Relaxed);
        } else {
            QUEUED_BACKGROUND_IO.fetch_add(1, Ordering::Relaxed);
        }
        class_counters(class).queued.fetch_add(1, Ordering::Relaxed);
        Self {
            foreground: class.is_foreground(),
            class,
        }
    }
}

impl Drop for QueueRegistration {
    fn drop(&mut self) {
        if self.foreground {
            QUEUED_FOREGROUND_IO.fetch_sub(1, Ordering::Relaxed);
        } else {
            QUEUED_BACKGROUND_IO.fetch_sub(1, Ordering::Relaxed);
        }
        class_counters(self.class)
            .queued
            .fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn configure_disk_io_scheduler(config: DiskIoSchedulerConfig) -> Result<(), &'static str> {
    SCHEDULER_CONFIG
        .set(config)
        .map_err(|_| "disk IO scheduler was already configured")?;
    Ok(())
}

pub fn set_foreground_io_target(target: usize) {
    FOREGROUND_GATE.set_target(target);
}

/// A cheap pacing hint, not an admission decision or a disk-health sample.
/// Background work shares the physical devices with both admitted and queued
/// foreground operations, even when its semaphore permits are separate.
pub(crate) fn foreground_io_pending() -> bool {
    ACTIVE_FOREGROUND_IO.load(Ordering::Relaxed) != 0
        || QUEUED_FOREGROUND_IO.load(Ordering::Relaxed) != 0
        || crate::core::object_parser::foreground_streams_active()
}

pub fn disk_io_scheduler_config() -> DiskIoSchedulerConfig {
    scheduler_config()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskIoPressure {
    Idle,
    Normal,
    Pressured,
    Saturated,
    Stalled,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiskIoClassSnapshot {
    pub class: String,
    pub active: usize,
    pub queued: usize,
    pub completed: u64,
    pub failed: u64,
    pub bytes: u64,
    pub stall_events: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ActiveDiskIoSnapshot {
    pub operation_id: u64,
    pub class: String,
    pub context: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fragment: Option<String>,
    pub progress: StallSnapshot,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiskIoSchedulerSnapshot {
    pub config: DiskIoSchedulerConfig,
    pub foreground_target: usize,
    pub pressure: DiskIoPressure,
    pub active_foreground: usize,
    pub active_background: usize,
    pub queued_foreground: usize,
    pub queued_background: usize,
    pub total_available: usize,
    pub foreground_available: usize,
    pub background_available: usize,
    pub classes: Vec<DiskIoClassSnapshot>,
    pub active_operations: Vec<ActiveDiskIoSnapshot>,
}

pub fn disk_io_scheduler_snapshot() -> DiskIoSchedulerSnapshot {
    let cfg = scheduler_config();
    let active_foreground = ACTIVE_FOREGROUND_IO.load(Ordering::Relaxed);
    let active_background = ACTIVE_BACKGROUND_IO.load(Ordering::Relaxed);
    let queued_foreground = QUEUED_FOREGROUND_IO.load(Ordering::Relaxed);
    let queued_background = QUEUED_BACKGROUND_IO.load(Ordering::Relaxed);
    let active_operations = active_operation_snapshots();
    let pressure = calculate_pressure(
        active_foreground,
        active_background,
        queued_foreground,
        queued_background,
        cfg.total_permits,
        active_operations
            .iter()
            .any(|operation| operation.progress.stalled),
    );

    let classes = DiskIoClass::ALL
        .iter()
        .copied()
        .map(|class| {
            let counters = class_counters(class);
            DiskIoClassSnapshot {
                class: class.metric_label().to_string(),
                active: active_operations
                    .iter()
                    .filter(|operation| operation.class == class.metric_label())
                    .count(),
                queued: counters.queued.load(Ordering::Relaxed),
                completed: counters.completed.load(Ordering::Relaxed),
                failed: counters.failed.load(Ordering::Relaxed),
                bytes: counters.bytes.load(Ordering::Relaxed),
                stall_events: counters.stalls.load(Ordering::Relaxed),
            }
        })
        .collect();

    DiskIoSchedulerSnapshot {
        config: cfg,
        foreground_target: foreground_target(),
        pressure,
        active_foreground,
        active_background,
        queued_foreground,
        queued_background,
        total_available: TOTAL_GATE.available_permits(),
        foreground_available: FOREGROUND_GATE.semaphore.available_permits(),
        background_available: BACKGROUND_GATE.available_permits(),
        classes,
        active_operations,
    }
}

pub fn disk_io_pressure() -> DiskIoPressure {
    let active_foreground = ACTIVE_FOREGROUND_IO.load(Ordering::Relaxed);
    let active_background = ACTIVE_BACKGROUND_IO.load(Ordering::Relaxed);
    let queued_foreground = QUEUED_FOREGROUND_IO.load(Ordering::Relaxed);
    let queued_background = QUEUED_BACKGROUND_IO.load(Ordering::Relaxed);
    let stalled = ACTIVE_OPERATIONS
        .lock()
        .map(|operations| {
            operations
                .values()
                .any(|operation| operation.progress.snapshot(stall_threshold()).stalled)
        })
        .unwrap_or(false);
    calculate_pressure(
        active_foreground,
        active_background,
        queued_foreground,
        queued_background,
        scheduler_config().total_permits,
        stalled,
    )
}

pub async fn acquire_disk_io(
    class: DiskIoClass,
    fragment: Option<&str>,
    context: &str,
) -> Result<DiskIoGuard, AppError> {
    let queued_at = Instant::now();
    let _queued = QueueRegistration::new(class);
    let result = if class.is_foreground() {
        match tokio::time::timeout(
            foreground_wait_timeout(),
            acquire_disk_io_inner(class, fragment, context),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                // Capture permit owners at the failure, not after the caller
                // has drained workers. This reads bounded in-memory counters;
                // it does not start a health scan or touch the storage roots.
                warn!(
                    class = class.label(),
                    context,
                    waited_ms = queued_at.elapsed().as_millis() as u64,
                    scheduler = ?disk_io_scheduler_snapshot(),
                    "N1 foreground admission deadline exceeded"
                );
                Err(AppError::ServiceUnavailable(format!(
                    "disk scheduler admission timed out for {} after {}ms",
                    class.label(),
                    foreground_wait_timeout().as_millis()
                )))
            }
        }
    } else {
        loop {
            match tokio::time::timeout(
                Duration::from_secs(DISK_IO_WAIT_LOG_SECS),
                acquire_disk_io_inner(class, fragment, context),
            )
            .await
            {
                Ok(result) => break result,
                Err(_) => warn!(
                    "N1 disk scheduler still waiting after {}s (class={}, context={}); preserving maintenance operation",
                    queued_at.elapsed().as_secs(),
                    class.label(),
                    context
                ),
            }
        }
    };
    record_io_latency(
        format!("disk.queue.{}", class.metric_label()),
        queued_at.elapsed(),
        0,
        result.is_ok(),
    );
    result
}

pub fn try_acquire_disk_io(
    class: DiskIoClass,
    fragment: Option<&str>,
) -> Result<DiskIoGuard, AppError> {
    let started = Instant::now();
    let result = (|| {
        let total = TOTAL_GATE.clone().try_acquire_owned().map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "disk scheduler is saturated for {}",
                class.label()
            ))
        })?;
        let class_permit = class_gate(class).clone().try_acquire_owned().map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "disk scheduler class '{}' is saturated",
                class.label()
            ))
        })?;
        let fragment_permit = fragment
            .map(|fragment| fragment_gate(class, fragment))
            .transpose()?
            .map(|gate| {
                gate.try_acquire_owned().map_err(|_| {
                    AppError::ServiceUnavailable(format!(
                        "disk scheduler is saturated for this fragment ({})",
                        class.label()
                    ))
                })
            })
            .transpose()?;

        register_active(
            class,
            fragment,
            class.label(),
            total,
            class_permit,
            fragment_permit,
        )
    })();
    record_io_latency(
        format!("disk.queue.{}", class.metric_label()),
        started.elapsed(),
        0,
        result.is_ok(),
    );
    result
}

async fn acquire_disk_io_inner(
    class: DiskIoClass,
    fragment: Option<&str>,
    context: &str,
) -> Result<DiskIoGuard, AppError> {
    let fragment_permit = match fragment {
        Some(fragment) => Some(
            fragment_gate(class, fragment)?
                .acquire_owned()
                .await
                .map_err(|_| {
                    AppError::InternalServerError("disk scheduler fragment gate closed".into())
                })?,
        ),
        None => None,
    };
    // Do not occupy the shared lane while waiting for a class whose adaptive
    // capacity is lower. Background work retains its configured lane.
    let class_permit = class_gate(class)
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::InternalServerError("disk scheduler class closed".into()))?;
    let total = TOTAL_GATE
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::InternalServerError("disk scheduler closed".into()))?;

    register_active(
        class,
        fragment,
        context,
        total,
        class_permit,
        fragment_permit,
    )
}

fn register_active(
    class: DiskIoClass,
    fragment: Option<&str>,
    context: &str,
    total: OwnedSemaphorePermit,
    class_permit: OwnedSemaphorePermit,
    fragment_permit: Option<OwnedSemaphorePermit>,
) -> Result<DiskIoGuard, AppError> {
    if class.is_foreground() {
        ACTIVE_FOREGROUND_IO.fetch_add(1, Ordering::Relaxed);
    } else {
        ACTIVE_BACKGROUND_IO.fetch_add(1, Ordering::Relaxed);
    }

    let operation = Arc::new(ActiveIoOperation {
        id: NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed),
        class,
        context: context.to_string(),
        fragment: fragment.map(str::to_string),
        progress: Arc::new(ProgressTracker::new()),
        stall_reported: AtomicBool::new(false),
    });
    if let Ok(mut active) = ACTIVE_OPERATIONS.lock() {
        active.insert(operation.id, operation.clone());
    }

    Ok(DiskIoGuard {
        operation,
        started: Instant::now(),
        completed: false,
        _total: total,
        _class: Some(class_permit),
        _fragment: fragment_permit,
    })
}

fn active_operation_snapshots() -> Vec<ActiveDiskIoSnapshot> {
    let Ok(operations) = ACTIVE_OPERATIONS.lock() else {
        return Vec::new();
    };
    let mut snapshots = operations
        .values()
        .map(|operation| {
            let progress = operation.progress.snapshot(stall_threshold());
            if progress.stalled && !operation.stall_reported.swap(true, Ordering::Relaxed) {
                class_counters(operation.class)
                    .stalls
                    .fetch_add(1, Ordering::Relaxed);
            }
            ActiveDiskIoSnapshot {
                operation_id: operation.id,
                class: operation.class.metric_label().to_string(),
                context: operation.context.clone(),
                fragment: operation.fragment.clone(),
                progress,
            }
        })
        .collect::<Vec<_>>();
    snapshots.sort_by_key(|operation| operation.operation_id);
    snapshots.truncate(MAX_ACTIVE_OPERATION_SNAPSHOTS);
    snapshots
}

fn calculate_pressure(
    active_foreground: usize,
    active_background: usize,
    queued_foreground: usize,
    queued_background: usize,
    total_permits: usize,
    stalled: bool,
) -> DiskIoPressure {
    if stalled {
        return DiskIoPressure::Stalled;
    }
    let active = active_foreground.saturating_add(active_background);
    let queued = queued_foreground.saturating_add(queued_background);
    if queued_foreground > 0 && active >= total_permits.max(1) {
        return DiskIoPressure::Saturated;
    }
    if queued > 0 || active.saturating_mul(100) >= total_permits.max(1).saturating_mul(75) {
        return DiskIoPressure::Pressured;
    }
    if active > 0 {
        DiskIoPressure::Normal
    } else {
        DiskIoPressure::Idle
    }
}

fn class_counters(class: DiskIoClass) -> &'static ClassCounters {
    &CLASS_COUNTERS[class.index()]
}

fn class_gate(class: DiskIoClass) -> &'static Arc<Semaphore> {
    if class.is_foreground() {
        &FOREGROUND_GATE.semaphore
    } else {
        &BACKGROUND_GATE
    }
}

fn fragment_gate(class: DiskIoClass, fragment: &str) -> Result<Arc<Semaphore>, AppError> {
    let fragment = fragment.trim();
    if fragment.is_empty() {
        return Ok(Arc::new(Semaphore::new(1)));
    }

    let cfg = scheduler_config();
    let (map, limit) = if class.is_foreground() {
        (
            &*FOREGROUND_FRAGMENT_GATES,
            cfg.foreground_per_fragment_permits,
        )
    } else {
        (
            &*BACKGROUND_FRAGMENT_GATES,
            cfg.background_per_fragment_permits,
        )
    };
    let mut gates = map
        .lock()
        .map_err(|_| AppError::InternalServerError("disk scheduler gate poisoned".into()))?;
    Ok(gates
        .entry(fragment.to_string())
        .or_insert_with(|| Arc::new(Semaphore::new(limit)))
        .clone())
}

fn foreground_target() -> usize {
    FOREGROUND_GATE.target()
}

fn scheduler_config() -> DiskIoSchedulerConfig {
    *SCHEDULER_CONFIG
        .get()
        .unwrap_or(&derive_scheduler_config(8, 2))
}

fn derive_scheduler_config(max_concurrent_io: usize, blob_roots: usize) -> DiskIoSchedulerConfig {
    let foreground = max_concurrent_io.max(1);
    let disk_floor = blob_roots.max(1);
    let background = (foreground / 4).max(1).min(disk_floor.max(1)).min(8);
    let total = foreground.saturating_add(background).max(1);
    let foreground_per_fragment = match blob_roots {
        0 | 1 => foreground.min(4),
        2 => foreground.min(6),
        3 | 4 => foreground.min(blob_roots.saturating_mul(2)),
        _ => foreground.min(blob_roots.saturating_mul(2)).min(16),
    }
    .max(1);
    let background_per_fragment = (background / 2).max(1);

    DiskIoSchedulerConfig {
        total_permits: total,
        foreground_permits: foreground,
        background_permits: background,
        foreground_per_fragment_permits: foreground_per_fragment,
        background_per_fragment_permits: background_per_fragment,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DiskIoClass, DiskIoPressure, FairIoGate, calculate_pressure, derive_scheduler_config,
    };

    #[tokio::test]
    async fn adaptive_shrink_preserves_waiters_fifo_position() {
        let gate = FairIoGate::new(2);
        let held_a = gate.semaphore.clone().acquire_owned().await.unwrap();
        let held_b = gate.semaphore.clone().acquire_owned().await.unwrap();
        gate.set_target(1);

        let mut first = Box::pin(gate.semaphore.clone().acquire_owned());
        assert!(futures_util::poll!(&mut first).is_pending());
        gate.release(held_a);
        assert!(futures_util::poll!(&mut first).is_pending());

        let mut later = Box::pin(gate.semaphore.clone().acquire_owned());
        assert!(futures_util::poll!(&mut later).is_pending());
        gate.release(held_b);
        // A new arrival cannot take the finishing operation's slot ahead of
        // the already-queued request, even at a target below the hard limit.
        assert!(futures_util::poll!(&mut later).is_pending());
        let first = first.await.unwrap();
        assert_eq!(gate.semaphore.available_permits(), 0);
        gate.release(first);
        gate.release(later.await.unwrap());
        assert_eq!(gate.semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn adaptive_growth_wakes_waiters_without_a_finishing_operation() {
        let gate = FairIoGate::new(3);
        gate.set_target(1);
        let held = gate.semaphore.clone().acquire_owned().await.unwrap();
        let mut first = Box::pin(gate.semaphore.clone().acquire_owned());
        let mut second = Box::pin(gate.semaphore.clone().acquire_owned());
        assert!(futures_util::poll!(&mut first).is_pending());
        assert!(futures_util::poll!(&mut second).is_pending());
        gate.set_target(3);
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert!(gate.semaphore.clone().try_acquire_owned().is_err());
        gate.release(first);
        gate.release(second);
        gate.release(held);
        assert_eq!(gate.semaphore.available_permits(), 3);
    }

    #[tokio::test]
    async fn cancelled_adaptive_waiter_does_not_lose_capacity_or_block_successor() {
        let gate = FairIoGate::new(2);
        gate.set_target(1);
        let held = gate.semaphore.clone().acquire_owned().await.unwrap();
        let mut cancelled = Box::pin(gate.semaphore.clone().acquire_owned());
        let mut successor = Box::pin(gate.semaphore.clone().acquire_owned());
        assert!(futures_util::poll!(&mut cancelled).is_pending());
        assert!(futures_util::poll!(&mut successor).is_pending());
        drop(cancelled);
        gate.release(held);
        gate.release(successor.await.unwrap());
        gate.set_target(2);
        assert_eq!(gate.semaphore.available_permits(), 2);
    }

    #[tokio::test]
    async fn adaptive_capacity_changes_never_leak_or_exceed_hard_limit() {
        let gate = FairIoGate::new(3);
        let a = gate.semaphore.clone().acquire_owned().await.unwrap();
        let b = gate.semaphore.clone().acquire_owned().await.unwrap();
        let c = gate.semaphore.clone().acquire_owned().await.unwrap();
        gate.set_target(1);
        gate.set_target(3);
        gate.release(a);
        gate.release(b);
        gate.release(c);
        for target in (0..10000).map(|value| value % 5) {
            gate.set_target(target);
            let expected = target.clamp(1, 3);
            assert_eq!(gate.target(), expected);
            assert_eq!(gate.semaphore.available_permits(), expected);
            assert_eq!(gate.state.lock().unwrap().reserved.len(), 3 - expected);
        }
        assert_eq!(FairIoGate::new(0).target(), 1);
    }

    #[test]
    fn scheduler_config_reserves_small_background_lane() {
        let cfg = derive_scheduler_config(8, 2);
        assert_eq!(cfg.foreground_permits, 8);
        assert_eq!(cfg.background_permits, 2);
        assert_eq!(cfg.total_permits, 10);
        assert_eq!(cfg.foreground_per_fragment_permits, 6);
        assert_eq!(cfg.background_per_fragment_permits, 1);
    }

    #[test]
    fn scheduler_config_handles_minimum_hardware() {
        let cfg = derive_scheduler_config(2, 1);
        assert_eq!(cfg.foreground_permits, 2);
        assert_eq!(cfg.background_permits, 1);
        assert_eq!(cfg.total_permits, 3);
        assert_eq!(cfg.foreground_per_fragment_permits, 2);
        assert_eq!(cfg.background_per_fragment_permits, 1);
    }

    #[test]
    fn scheduler_config_bounds_single_fragment_without_serializing_uploads() {
        let cfg = derive_scheduler_config(16, 1);
        assert_eq!(cfg.foreground_permits, 16);
        assert_eq!(cfg.foreground_per_fragment_permits, 4);
    }

    #[test]
    fn scheduler_class_labels_are_operator_readable() {
        assert_eq!(DiskIoClass::ForegroundWrite.label(), "foreground write");
        assert_eq!(DiskIoClass::BackgroundRepair.label(), "background repair");
        assert_eq!(DiskIoClass::BackgroundExport.label(), "background export");
    }

    #[test]
    fn pressure_prioritizes_stalls_and_foreground_saturation() {
        assert_eq!(
            calculate_pressure(1, 0, 0, 0, 8, true),
            DiskIoPressure::Stalled
        );
        assert_eq!(
            calculate_pressure(7, 1, 1, 0, 8, false),
            DiskIoPressure::Saturated
        );
        assert_eq!(
            calculate_pressure(6, 0, 0, 0, 8, false),
            DiskIoPressure::Pressured
        );
    }
}
