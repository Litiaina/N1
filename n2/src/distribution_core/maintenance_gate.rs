use crate::distribution_core::config::n2_runtime_config;
use crate::enums::app_error_enum::AppError;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

static GLOBAL_REPAIR_GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
static FRAGMENT_REPAIR_GATES: OnceLock<Mutex<HashMap<String, Arc<Semaphore>>>> = OnceLock::new();
static OBJECT_REPAIR_GATES: OnceLock<Mutex<HashMap<String, Arc<Semaphore>>>> = OnceLock::new();

pub(crate) struct N2RepairPermit {
    _global: OwnedSemaphorePermit,
    _fragment: OwnedSemaphorePermit,
    _object: Option<OwnedSemaphorePermit>,
}

fn global_repair_gate() -> Arc<Semaphore> {
    GLOBAL_REPAIR_GATE
        .get_or_init(|| {
            Arc::new(Semaphore::new(
                n2_runtime_config().repair_global_concurrency.max(1),
            ))
        })
        .clone()
}

async fn named_gate(
    gates: &'static OnceLock<Mutex<HashMap<String, Arc<Semaphore>>>>,
    key: String,
    limit: usize,
) -> Arc<Semaphore> {
    let map = gates.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().await;
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Semaphore::new(limit.max(1))))
        .clone()
}

pub(crate) async fn acquire_fragment_repair_permit(
    fragment: &str,
) -> Result<N2RepairPermit, AppError> {
    let global = global_repair_gate()
        .try_acquire_owned()
        .map_err(|_| AppError::ServiceUnavailable("n2 repair gate is saturated".to_string()))?;
    let fragment_gate = named_gate(
        &FRAGMENT_REPAIR_GATES,
        fragment.to_string(),
        n2_runtime_config().repair_per_fragment_concurrency,
    )
    .await;
    let fragment_permit = fragment_gate.try_acquire_owned().map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "n2 repair gate is saturated for fragment {fragment}"
        ))
    })?;

    Ok(N2RepairPermit {
        _global: global,
        _fragment: fragment_permit,
        _object: None,
    })
}

pub(crate) async fn acquire_object_repair_permit(
    fragment: &str,
    object_key: &str,
) -> Result<N2RepairPermit, AppError> {
    let global = global_repair_gate()
        .try_acquire_owned()
        .map_err(|_| AppError::ServiceUnavailable("n2 repair gate is saturated".to_string()))?;
    let fragment_gate = named_gate(
        &FRAGMENT_REPAIR_GATES,
        fragment.to_string(),
        n2_runtime_config().repair_per_fragment_concurrency,
    )
    .await;
    let fragment_permit = fragment_gate.try_acquire_owned().map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "n2 repair gate is saturated for fragment {fragment}"
        ))
    })?;
    let object_key = format!("{fragment}/{object_key}");
    let object_gate = named_gate(&OBJECT_REPAIR_GATES, object_key, 1).await;
    let object_permit = object_gate.try_acquire_owned().map_err(|_| {
        AppError::ServiceUnavailable("n2 object repair is already running".to_string())
    })?;

    Ok(N2RepairPermit {
        _global: global,
        _fragment: fragment_permit,
        _object: Some(object_permit),
    })
}
