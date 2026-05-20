use crate::enums::app_error_enum::AppError;
use once_cell::sync::{Lazy, OnceCell};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;

pub static DISK_IO_TARGET_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();

static SCHEDULER_CONFIG: OnceCell<DiskIoSchedulerConfig> = OnceCell::new();
static TOTAL_GATE: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(scheduler_config().total_permits)));
static FOREGROUND_GATE: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(scheduler_config().foreground_permits)));
static BACKGROUND_GATE: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(scheduler_config().background_permits)));
static FOREGROUND_FRAGMENT_GATES: Lazy<Mutex<HashMap<String, Arc<Semaphore>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static BACKGROUND_FRAGMENT_GATES: Lazy<Mutex<HashMap<String, Arc<Semaphore>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static ACTIVE_FOREGROUND_IO: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));
static ACTIVE_BACKGROUND_IO: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

const DISK_IO_WAIT_LOG_SECS: u64 = 30;

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
}

impl DiskIoClass {
    pub fn label(self) -> &'static str {
        match self {
            Self::ForegroundRead => "foreground read",
            Self::ForegroundWrite => "foreground write",
            Self::BackgroundRepair => "background repair",
            Self::BackgroundGc => "background gc",
            Self::BackgroundRedistribution => "background redistribution",
        }
    }

    fn is_foreground(self) -> bool {
        matches!(self, Self::ForegroundRead | Self::ForegroundWrite)
    }
}

pub struct DiskIoGuard {
    class: DiskIoClass,
    _total: OwnedSemaphorePermit,
    _class: OwnedSemaphorePermit,
    _fragment: Option<OwnedSemaphorePermit>,
}

impl Drop for DiskIoGuard {
    fn drop(&mut self) {
        if self.class.is_foreground() {
            ACTIVE_FOREGROUND_IO.fetch_sub(1, Ordering::Relaxed);
        } else {
            ACTIVE_BACKGROUND_IO.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub fn configure_disk_io_scheduler(config: DiskIoSchedulerConfig) -> Result<(), &'static str> {
    SCHEDULER_CONFIG
        .set(config)
        .map_err(|_| "disk IO scheduler was already configured")?;
    let _ = DISK_IO_TARGET_ATOMIC.set(AtomicUsize::new(config.foreground_permits));
    Ok(())
}

pub fn disk_io_scheduler_config() -> DiskIoSchedulerConfig {
    scheduler_config()
}

pub fn disk_io_scheduler_snapshot() -> DiskIoSchedulerSnapshot {
    let cfg = scheduler_config();
    DiskIoSchedulerSnapshot {
        config: cfg,
        foreground_target: foreground_target(),
        active_foreground: ACTIVE_FOREGROUND_IO.load(Ordering::Relaxed),
        active_background: ACTIVE_BACKGROUND_IO.load(Ordering::Relaxed),
        total_available: TOTAL_GATE.available_permits(),
        foreground_available: FOREGROUND_GATE.available_permits(),
        background_available: BACKGROUND_GATE.available_permits(),
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct DiskIoSchedulerSnapshot {
    pub config: DiskIoSchedulerConfig,
    pub foreground_target: usize,
    pub active_foreground: usize,
    pub active_background: usize,
    pub total_available: usize,
    pub foreground_available: usize,
    pub background_available: usize,
}

pub async fn acquire_disk_io(
    class: DiskIoClass,
    fragment: Option<&str>,
    context: &str,
) -> Result<DiskIoGuard, AppError> {
    let started = Instant::now();
    loop {
        match tokio::time::timeout(
            Duration::from_secs(DISK_IO_WAIT_LOG_SECS),
            acquire_disk_io_inner(class, fragment),
        )
        .await
        {
            Ok(result) => return result,
            Err(_) => warn!(
                "N1 disk scheduler still waiting after {}s (class={}, context={}); preserving operation and applying backpressure",
                started.elapsed().as_secs(),
                class.label(),
                context
            ),
        }
    }
}

pub fn try_acquire_disk_io(
    class: DiskIoClass,
    fragment: Option<&str>,
) -> Result<DiskIoGuard, AppError> {
    let total = TOTAL_GATE.clone().try_acquire_owned().map_err(|_| {
        AppError::ServiceUnavailable(format!("disk scheduler is saturated for {}", class.label()))
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

    register_active(class, total, class_permit, fragment_permit)
}

async fn acquire_disk_io_inner(
    class: DiskIoClass,
    fragment: Option<&str>,
) -> Result<DiskIoGuard, AppError> {
    loop {
        let total = TOTAL_GATE
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AppError::InternalServerError("disk scheduler closed".into()))?;
        let class_permit = class_gate(class)
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AppError::InternalServerError("disk scheduler class closed".into()))?;
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

        match register_active(class, total, class_permit, fragment_permit) {
            Ok(guard) => return Ok(guard),
            Err(AppError::ServiceUnavailable(_)) if class.is_foreground() => {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

fn register_active(
    class: DiskIoClass,
    total: OwnedSemaphorePermit,
    class_permit: OwnedSemaphorePermit,
    fragment_permit: Option<OwnedSemaphorePermit>,
) -> Result<DiskIoGuard, AppError> {
    if class.is_foreground() {
        loop {
            let target = foreground_target();
            let active = ACTIVE_FOREGROUND_IO.load(Ordering::Relaxed);
            if active >= target {
                return Err(AppError::ServiceUnavailable(
                    "foreground disk IO target is saturated".into(),
                ));
            }
            if ACTIVE_FOREGROUND_IO
                .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(DiskIoGuard {
                    class,
                    _total: total,
                    _class: class_permit,
                    _fragment: fragment_permit,
                });
            }
        }
    }

    ACTIVE_BACKGROUND_IO.fetch_add(1, Ordering::Relaxed);
    Ok(DiskIoGuard {
        class,
        _total: total,
        _class: class_permit,
        _fragment: fragment_permit,
    })
}

fn class_gate(class: DiskIoClass) -> &'static Arc<Semaphore> {
    if class.is_foreground() {
        &FOREGROUND_GATE
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
    DISK_IO_TARGET_ATOMIC
        .get()
        .map(|target| target.load(Ordering::Relaxed))
        .unwrap_or_else(|| scheduler_config().foreground_permits)
        .max(1)
        .min(scheduler_config().foreground_permits)
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
    let foreground_per_fragment = (foreground / 2).max(1);
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
    use super::{DiskIoClass, derive_scheduler_config};

    #[test]
    fn scheduler_config_reserves_small_background_lane() {
        let cfg = derive_scheduler_config(8, 2);
        assert_eq!(cfg.foreground_permits, 8);
        assert_eq!(cfg.background_permits, 2);
        assert_eq!(cfg.total_permits, 10);
        assert_eq!(cfg.foreground_per_fragment_permits, 4);
        assert_eq!(cfg.background_per_fragment_permits, 1);
    }

    #[test]
    fn scheduler_config_handles_minimum_hardware() {
        let cfg = derive_scheduler_config(2, 1);
        assert_eq!(cfg.foreground_permits, 2);
        assert_eq!(cfg.background_permits, 1);
        assert_eq!(cfg.total_permits, 3);
        assert_eq!(cfg.foreground_per_fragment_permits, 1);
        assert_eq!(cfg.background_per_fragment_permits, 1);
    }

    #[test]
    fn scheduler_class_labels_are_operator_readable() {
        assert_eq!(DiskIoClass::ForegroundWrite.label(), "foreground write");
        assert_eq!(DiskIoClass::BackgroundRepair.label(), "background repair");
    }
}
