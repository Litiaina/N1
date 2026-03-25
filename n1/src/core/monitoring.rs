use crate::core::disk_manager::{BLOB_STORE_DISKS, INDEX_STORE_DISKS};
use crate::core::redistribution::get_redist_status;
use crate::core::repair_manager::get_repair_status;
use crate::core::system::{SystemMetrics, get_system_metrics};
use crate::enums::app_error_enum::AppError;
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
#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::sync::{Semaphore, oneshot};
use tokio::time::timeout;
use ulid::Ulid;
use walkdir::WalkDir;

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
    pub total_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct ConsistencyStats {
    pub total_partition_dbs_scanned: u64,
    pub index_partition_health: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_db_counts: Option<HashMap<String, u64>>,
}

#[derive(Serialize, Clone)]
pub struct SystemHealthResponse {
    pub status: String,
    pub disks: Vec<DiskStatus>,
    pub consistency: ConsistencyStats,
    pub system: SystemMetrics,
    pub summary: String,
}

fn should_count_partition_db_file(path: &std::path::Path) -> bool {
    let name = match path.file_name().and_then(|n| n.to_str()) {
        Some(s) => s,
        None => return false,
    };

    if !name.ends_with(".ndxdb") {
        return false;
    }

    if path
        .components()
        .any(|component| component.as_os_str() == std::ffi::OsStr::new(".trash"))
    {
        return false;
    }

    let Some(parent) = path.parent() else {
        return false;
    };
    let Some(grandparent) = parent.parent() else {
        return false;
    };

    parent.file_name().and_then(|n| n.to_str()) == Some("partitions")
        && grandparent.file_name().and_then(|n| n.to_str()) == Some("indexdb")
}

#[cfg(test)]
mod tests {
    use super::should_count_partition_db_file;
    use std::path::Path;

    #[test]
    fn deep_scan_counts_only_partition_db_files() {
        assert!(should_count_partition_db_file(Path::new(
            "frag/indexdb/partitions/0001.ndxdb"
        )));
        assert!(!should_count_partition_db_file(Path::new(
            "frag/indexdb/intents/0001.pending.redist.intent.msgpack"
        )));
        assert!(!should_count_partition_db_file(Path::new(
            "frag/indexdb/partitions/0001.ndxdb.gc"
        )));
        assert!(!should_count_partition_db_file(Path::new(
            "frag/.trash/indexdb/partitions/0001.ndxdb"
        )));
    }
}

async fn check_disk_accessibility(path_str: String, role: String) -> DiskStatus {
    let path = PathBuf::from(&path_str);
    let capacity = disk_capacity_snapshot(&path);

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
        role,
        status,
        total_bytes: capacity.as_ref().map(|v| v.total_bytes),
        used_bytes: capacity.as_ref().map(|v| v.used_bytes),
        available_bytes: capacity.as_ref().map(|v| v.available_bytes),
        usage_percent: capacity.as_ref().map(|v| v.usage_percent),
        latency_ms,
        error,
    }
}

#[derive(Clone, Copy)]
struct DiskCapacitySnapshot {
    total_bytes: u64,
    used_bytes: u64,
    available_bytes: u64,
    usage_percent: f64,
}

#[cfg(unix)]
fn disk_capacity_snapshot(path: &std::path::Path) -> Option<DiskCapacitySnapshot> {
    let raw_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(raw_path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }

    let stats = unsafe { stats.assume_init() };
    let block_size = u128::from(if stats.f_frsize > 0 {
        stats.f_frsize
    } else {
        stats.f_bsize
    });
    if block_size == 0 {
        return None;
    }

    let total_bytes = u128::from(stats.f_blocks).saturating_mul(block_size);
    let free_bytes = u128::from(stats.f_bfree).saturating_mul(block_size);
    let available_bytes = u128::from(stats.f_bavail).saturating_mul(block_size);
    let used_bytes = total_bytes.saturating_sub(free_bytes);
    let usage_percent = if total_bytes == 0 {
        0.0
    } else {
        (used_bytes as f64 / total_bytes as f64) * 100.0
    };

    Some(DiskCapacitySnapshot {
        total_bytes: total_bytes.min(u128::from(u64::MAX)) as u64,
        used_bytes: used_bytes.min(u128::from(u64::MAX)) as u64,
        available_bytes: available_bytes.min(u128::from(u64::MAX)) as u64,
        usage_percent,
    })
}

#[cfg(not(unix))]
fn disk_capacity_snapshot(_path: &std::path::Path) -> Option<DiskCapacitySnapshot> {
    None
}

async fn collect_system_health(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> Result<SystemHealthResponse, AppError> {
    let system_metrics = get_system_metrics().await;
    let redist_status = get_redist_status().await;
    let repair_status = get_repair_status().await;

    let index_disks_config = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError("Index config missing".into()))?;

    let blob_disks_config = BLOB_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError("Blob config missing".into()))?;

    let mut check_futures = Vec::new();

    for disk in index_disks_config {
        check_futures.push(check_disk_accessibility(disk.clone(), "index".to_string()));
    }
    for disk in blob_disks_config {
        check_futures.push(check_disk_accessibility(disk.clone(), "blob".to_string()));
    }

    let disk_results = join_all(check_futures).await;

    let mut online_index_count = 0;
    let mut online_blob_count = 0;
    let mut active_index_roots = Vec::new();
    let mut disk_statuses = Vec::new();

    for stat in disk_results {
        if stat.status == "online" || stat.status == "slow" {
            if stat.role == "index" {
                online_index_count += 1;
                active_index_roots.push(PathBuf::from(&stat.path));
            } else {
                online_blob_count += 1;
            }
        }
        disk_statuses.push(stat);
    }

    let deep_scan = params
        .get("deep_scan")
        .map(|v| v == "true")
        .unwrap_or(false);
    let mut partition_db_counts = Vec::new();
    let mut partition_map = HashMap::new();
    let mut scan_performed = false;
    let mut scan_busy = false;

    if deep_scan && !active_index_roots.is_empty() {
        if let Ok(_permit) = DEEP_SCAN_SEMAPHORE.try_acquire() {
            scan_performed = true;
            let roots_clone = active_index_roots.clone();
            let (tx, rx) = oneshot::channel();

            std::thread::spawn(move || {
                if let Some(core_ids) = core_affinity::get_core_ids() {
                    if let Some(last_core) = core_ids.last() {
                        let _ = core_affinity::set_for_current(*last_core);
                    }
                }

                let mut counts = Vec::new();
                for root in roots_clone {
                    let mut current_disk_count = 0u64;
                    let walker = WalkDir::new(&root).into_iter();

                    for entry in walker.filter_map(|e| e.ok()) {
                        let path = entry.path();
                        if entry.file_type().is_file() && should_count_partition_db_file(path) {
                            current_disk_count += 1;
                        }

                        if current_disk_count % 1000 == 0 {
                            std::thread::yield_now();
                        }
                    }

                    counts.push(current_disk_count);
                }

                let _ = tx.send(counts);
            });

            partition_db_counts = rx.await.map_err(|e| {
                AppError::InternalServerError(format!("Consistency check thread failed: {}", e))
            })?;

            for (i, root) in active_index_roots.iter().enumerate() {
                if let Some(count) = partition_db_counts.get(i) {
                    partition_map.insert(root.to_string_lossy().to_string(), *count);
                }
            }
        } else {
            scan_busy = true;
        }
    }

    let total_partition_dbs = partition_db_counts.iter().copied().sum::<u64>();

    let index_health = if scan_busy {
        "skipped (scan in progress)".to_string()
    } else if !scan_performed {
        "skipped (use ?deep_scan=true)".to_string()
    } else if partition_db_counts.is_empty() {
        "empty".to_string()
    } else {
        "partitioned".to_string()
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
                "Running on {}/{} index roots. Partition redundancy reduced.",
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
    } else {
        ("healthy", "All systems operational.".to_string())
    };

    let monitoring_update = StorageEventPayload::MonitoringUpdate {
        system: serde_json::to_value(&system_metrics).unwrap_or(serde_json::Value::Null),
        repair_job: serde_json::to_value(&repair_status).ok(),
        redist_job: serde_json::to_value(&redist_status).ok(),
        shadow_index_status: index_health.clone(),
        shadow_index_healthy: online_index_count == total_index,
    };
    state.ws_manager.broadcast_system_wide(monitoring_update);

    Ok(SystemHealthResponse {
        status: status.to_string(),
        disks: disk_statuses,
        consistency: ConsistencyStats {
            total_partition_dbs_scanned: total_partition_dbs,
            index_partition_health: index_health,
            partition_db_counts: if !partition_map.is_empty() {
                Some(partition_map)
            } else {
                None
            },
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
