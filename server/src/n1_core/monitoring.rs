use crate::enums::app_error_enum::AppError;
use crate::n1_core::blob_store::{RawBlobDeviceStatus, raw_blob_device_statuses};
use crate::n1_core::disk_manager::{configured_blob_store_disks, configured_index_store_disks};
use crate::n1_core::index;
use crate::n1_core::redistribution::get_redist_status;
use crate::n1_core::repair_manager::get_repair_status;
use crate::n1_core::system::{SystemMetrics, get_system_metrics};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;
use axum::{
    Json,
    extract::{Query, State},
    response::IntoResponse,
};
use futures_util::future::join_all;
use once_cell::sync::OnceCell;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::sync::{Semaphore, oneshot};
use tokio::time::timeout;
use ulid::Ulid;

static DEEP_SCAN_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(1));
const DISK_CHECK_TIMEOUT: Duration = Duration::from_millis(5000);
pub static MONITOR_INTERVAL_SECS_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();
pub static MONITOR_DEEP_SCAN_INTERVAL_SECS_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();
static HEALTH_CACHE: LazyLock<Arc<tokio::sync::RwLock<Option<SystemHealthResponse>>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(None)));
static LAST_DEEP_SCAN_AT: LazyLock<Arc<tokio::sync::RwLock<Option<Instant>>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(None)));

fn monitor_interval() -> Duration {
    let secs = MONITOR_INTERVAL_SECS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(2)
        .clamp(1, 3600);
    Duration::from_secs(secs)
}

fn deep_scan_interval() -> Duration {
    let secs = MONITOR_DEEP_SCAN_INTERVAL_SECS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(60)
        .clamp(5, 24 * 3600);
    Duration::from_secs(secs)
}

#[derive(Serialize, Clone)]
pub struct DiskStatus {
    pub path: String,
    pub role: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_backend: Option<RawBlobDeviceStatus>,
}

#[derive(Serialize, Clone)]
pub struct ConsistencyStats {
    pub total_objects_scanned: u64,
    pub index_mirror_health: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mirror_counts: Option<HashMap<String, u64>>,
}

#[derive(Serialize, Clone)]
pub struct SystemHealthResponse {
    pub status: String,
    pub disks: Vec<DiskStatus>,
    pub consistency: ConsistencyStats,
    pub system: SystemMetrics,
    pub summary: String,
}

async fn check_index_disk_accessibility(path_str: String) -> DiskStatus {
    let path = PathBuf::from(&path_str);

    let (status, latency_ms, error) = if path.exists() {
        let check_result = timeout(DISK_CHECK_TIMEOUT, async {
            match fs::metadata(&path).await {
                Ok(meta) => {
                    if meta.permissions().readonly() {
                        return (
                            "readonly".to_string(),
                            Some("Disk is mounted read-only".to_string()),
                            None,
                        );
                    }

                    let start_time = Instant::now();
                    let check_file_name = format!(".n1_health_{}", Ulid::new().to_string());
                    let check_path = path.join(check_file_name);
                    let payload = b"n1_io_check";

                    if let Err(e) = fs::write(&check_path, payload).await {
                        return (
                            "readonly".to_string(),
                            Some(format!("Write test failed: {}", e)),
                            None,
                        );
                    }

                    let read_result = fs::read(&check_path).await;
                    let delete_result = fs::remove_file(&check_path).await;

                    match read_result {
                        Ok(data) => {
                            if data == payload {
                                if let Err(e) = delete_result {
                                    return (
                                        "degraded".to_string(),
                                        Some(format!("Write/Read OK, delete failed: {}", e)),
                                        None,
                                    );
                                }
                                let latency = start_time.elapsed().as_millis() as u64;
                                let status = if latency > 1000 {
                                    "slow".to_string()
                                } else {
                                    "online".to_string()
                                };
                                (status, None, Some(latency))
                            } else {
                                (
                                    "corruption".to_string(),
                                    Some("Data read mismatch".to_string()),
                                    None,
                                )
                            }
                        }
                        Err(e) => (
                            "write_only".to_string(),
                            Some(format!("Write OK, Read failed: {}", e)),
                            None,
                        ),
                    }
                }
                Err(e) => (
                    "offline".to_string(),
                    Some(format!("Failed to read metadata: {}", e)),
                    None,
                ),
            }
        })
        .await;

        match check_result {
            Ok((s, e, l)) => (s, l, e),
            Err(_) => (
                "slow".to_string(),
                None,
                Some("Disk I/O timed out (>5s)".to_string()),
            ),
        }
    } else {
        (
            "offline".to_string(),
            None,
            Some("Path does not exist".to_string()),
        )
    };

    DiskStatus {
        path: path_str,
        role: "index".to_string(),
        status,
        latency_ms,
        error,
        raw_backend: None,
    }
}

fn map_raw_blob_status_to_disk_status(raw: RawBlobDeviceStatus) -> DiskStatus {
    let mut status = raw.status.clone();
    if status == "managed" {
        status = "online".to_string();
    }
    DiskStatus {
        path: raw.path.clone(),
        role: "blob".to_string(),
        status,
        latency_ms: None,
        error: raw.error.clone(),
        raw_backend: Some(raw),
    }
}

async fn collect_system_health(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> Result<SystemHealthResponse, AppError> {
    let system_metrics = get_system_metrics().await;
    let redist_status = get_redist_status().await;
    let repair_status = get_repair_status().await;

    let index_disks_config = configured_index_store_disks()?;
    let blob_disks_config = configured_blob_store_disks()?;

    let mut check_futures = Vec::new();
    for disk in &index_disks_config {
        check_futures.push(check_index_disk_accessibility(disk.clone()));
    }
    let mut disk_results = join_all(check_futures).await;
    let raw_blob_results = raw_blob_device_statuses()?
        .into_iter()
        .map(map_raw_blob_status_to_disk_status)
        .collect::<Vec<_>>();
    disk_results.extend(raw_blob_results);

    let mut online_index_count = 0;
    let mut online_blob_count = 0;
    let mut active_index_roots = Vec::new();
    let mut disk_statuses = Vec::new();

    for stat in disk_results {
        if stat.status == "online" || stat.status == "slow" {
            if stat.role == "index" {
                online_index_count += 1;
                active_index_roots.push(PathBuf::from(&stat.path));
            } else if stat.status == "online" || stat.status == "unformatted" {
                online_blob_count += 1;
            }
        }
        disk_statuses.push(stat);
    }

    let deep_scan = params
        .get("deep_scan")
        .map(|v| v == "true")
        .unwrap_or(false);
    let mut total_objects = 0u64;
    let mut scan_performed = false;
    let mut scan_busy = false;

    if deep_scan && !active_index_roots.is_empty() {
        if let Ok(_permit) = DEEP_SCAN_SEMAPHORE.try_acquire() {
            scan_performed = true;
            let roots_clone = active_index_roots.clone();
            let (tx, rx) = oneshot::channel();

            std::thread::spawn(move || {
                let mut fragments = std::collections::BTreeSet::new();
                for root in roots_clone {
                    let Ok(entries) = std::fs::read_dir(root) else {
                        continue;
                    };
                    for entry in entries.filter_map(|entry| entry.ok()) {
                        if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                            if let Some(name) = entry.file_name().to_str() {
                                if !name.starts_with('.') {
                                    fragments.insert(name.to_string());
                                }
                            }
                        }
                    }
                }
                let _ = tx.send(fragments.into_iter().collect::<Vec<_>>());
            });

            let fragments = rx.await.map_err(|e| {
                AppError::InternalServerError(format!("Consistency check thread failed: {}", e))
            })?;

            for fragment in fragments {
                total_objects += index::list_fragment_object_versions(&fragment, false)
                    .await
                    .map(|versions| versions.len() as u64)
                    .unwrap_or(0);
            }
        } else {
            scan_busy = true;
        }
    }

    let index_health = if scan_busy {
        "skipped (scan in progress)".to_string()
    } else if !scan_performed {
        "skipped (use ?deep_scan=true)".to_string()
    } else {
        "partitioned (index authoritative)".to_string()
    };

    let total_index = index_disks_config.len();
    let total_blob = blob_disks_config.len();

    let (status, summary) = if online_index_count == 0 {
        (
            "critical",
            "ALL Metadata (Index) disks are offline. System cannot function.".to_string(),
        )
    } else if online_blob_count == 0 {
        (
            "critical",
            "ALL Data (Blob) disks are offline. Data is inaccessible.".to_string(),
        )
    } else if online_index_count < total_index {
        (
            "degraded",
            format!(
                "Running on {}/{} index mirrors. Redundancy lost.",
                online_index_count, total_index
            ),
        )
    } else if online_blob_count < total_blob {
        (
            "degraded",
            format!(
                "Running on {}/{} data disks. Performance/capacity reduced.",
                online_blob_count, total_blob
            ),
        )
    } else if index_health == "desynchronized" {
        (
            "warning",
            "Index mirrors have mismatched active object metadata counts. Run repair job."
                .to_string(),
        )
    } else {
        ("healthy", "All systems operational.".to_string())
    };

    let monitoring_update = StorageEventPayload::MonitoringUpdate {
        system: serde_json::to_value(&system_metrics).unwrap_or(serde_json::Value::Null),
        repair_job: serde_json::to_value(&repair_status).ok(),
        redist_job: serde_json::to_value(&redist_status).ok(),
        shadow_index_status: index_health.clone(),
        shadow_index_healthy: index_health != "desynchronized",
    };
    state.ws_manager.broadcast_system_wide(monitoring_update);

    Ok(SystemHealthResponse {
        status: status.to_string(),
        disks: disk_statuses,
        consistency: ConsistencyStats {
            total_objects_scanned: total_objects,
            index_mirror_health: index_health,
            mirror_counts: None,
        },
        system: system_metrics,
        summary,
    })
}

pub async fn run_system_monitor(state: Arc<AppState>) {
    let mut ticker = tokio::time::interval(monitor_interval());

    loop {
        ticker.tick().await;

        let do_deep_scan = {
            let last = LAST_DEEP_SCAN_AT.read().await;
            match *last {
                Some(t) => t.elapsed() >= deep_scan_interval(),
                None => true,
            }
        };

        let mut params = HashMap::new();
        if do_deep_scan {
            params.insert("deep_scan".to_string(), "true".to_string());
        }

        match collect_system_health(&state, &params).await {
            Ok(snapshot) => {
                *HEALTH_CACHE.write().await = Some(snapshot);
                if do_deep_scan {
                    *LAST_DEEP_SCAN_AT.write().await = Some(Instant::now());
                }
            }
            Err(_) => {
                // Keep serving the last good snapshot if probe fails.
            }
        }
    }
}

pub async fn get_system_health(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    let deep_scan = params
        .get("deep_scan")
        .map(|v| v == "true")
        .unwrap_or(false);
    let refresh = params.get("refresh").map(|v| v == "true").unwrap_or(false);

    if deep_scan || refresh {
        let snapshot = collect_system_health(&state, &params).await?;
        *HEALTH_CACHE.write().await = Some(snapshot.clone());
        if deep_scan {
            *LAST_DEEP_SCAN_AT.write().await = Some(Instant::now());
        }
        return Ok(Json(snapshot));
    }

    if let Some(cached) = HEALTH_CACHE.read().await.clone() {
        return Ok(Json(cached));
    }

    let snapshot = collect_system_health(&state, &params).await?;
    *HEALTH_CACHE.write().await = Some(snapshot.clone());
    Ok(Json(snapshot))
}

pub async fn get_raw_blob_backend_status(
    claims: Claims,
) -> Result<Json<Vec<RawBlobDeviceStatus>>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(raw_blob_device_statuses()?))
}
