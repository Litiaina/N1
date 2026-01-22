use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::redistribution::get_redist_status;
use crate::object_storage::repair_manager::get_repair_status;
use crate::object_storage::system::{SystemMetrics, get_system_metrics};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{BLOB_STORE_DISKS, INDEX_STORE_DISKS};
use crate::websocket::ws_structs::StorageEventPayload;

use axum::{
    Json,
    extract::{Query, State},
    response::IntoResponse,
};
use futures_util::future::join_all;
use mongodb::bson::doc;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::sync::{Semaphore, oneshot};
use tokio::time::timeout;
use ulid::Ulid;
use walkdir::WalkDir;

static DEEP_SCAN_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(1));
const DISK_CHECK_TIMEOUT: Duration = Duration::from_millis(5000);

#[derive(Serialize)]
pub struct DiskStatus {
    pub path: String,
    pub role: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ConsistencyStats {
    pub total_objects_scanned: u64,
    pub index_mirror_health: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mirror_counts: Option<HashMap<String, u64>>,
}

#[derive(Serialize)]
pub struct SystemHealthResponse {
    pub status: String,
    pub database_connected: bool,
    pub disks: Vec<DiskStatus>,
    pub consistency: ConsistencyStats,
    pub system: SystemMetrics,
    pub summary: String,
}

async fn check_disk_accessibility(path_str: String, role: String) -> DiskStatus {
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
        role,
        status,
        latency_ms,
        error,
    }
}

pub async fn get_system_health(
    State(state): State<Arc<AppState>>,
    _claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let system_metrics = get_system_metrics().await;
    let redist_status = get_redist_status().await;
    let repair_status = get_repair_status().await;

    let db_connected = state
        .db_client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .is_ok();

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
                active_index_roots.push(PathBuf::from(&stat.path).join(OBJECT_DATA_ROOT));
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
    let mut object_counts = Vec::new();
    let mut mirror_map = HashMap::new();
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
                        if entry.file_type().is_file()
                            && path.extension().map_or(false, |ext| ext == "bson")
                        {
                            if path.components().any(|c| c.as_os_str() == "blobs") {
                                continue;
                            }

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

            object_counts = rx.await.map_err(|e| {
                AppError::InternalServerError(format!("Consistency check thread failed: {}", e))
            })?;

            for (i, root) in active_index_roots.iter().enumerate() {
                if let Some(count) = object_counts.get(i) {
                    mirror_map.insert(root.to_string_lossy().to_string(), *count);
                }
            }
        } else {
            scan_busy = true;
        }
    }

    let total_objects = object_counts.first().copied().unwrap_or(0);

    let index_health = if scan_busy {
        "skipped (scan in progress)".to_string()
    } else if !scan_performed {
        "skipped (use ?deep_scan=true)".to_string()
    } else if object_counts.is_empty() {
        "unknown".to_string()
    } else {
        let first = object_counts[0];
        let all_match = object_counts.iter().all(|&c| c == first);
        if all_match {
            "synchronized".to_string()
        } else {
            "desynchronized".to_string()
        }
    };

    let total_index = index_disks_config.len();
    let total_blob = blob_disks_config.len();

    let (status, summary) = if !db_connected {
        ("critical", "Database connection lost.".to_string())
    } else if online_index_count == 0 {
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
            "Index mirrors have mismatched object counts. Run repair job.".to_string(),
        )
    } else {
        ("healthy", "All systems operational.".to_string())
    };

    let monitoring_update = StorageEventPayload::MonitoringUpdate {
        system: serde_json::to_value(&system_metrics).unwrap_or(serde_json::Value::Null),
        repair_job: serde_json::to_value(&repair_status).ok(),
        redist_job: serde_json::to_value(&redist_status).ok(),
        shadow_index_status: index_health.clone(),
        shadow_index_healthy: index_health == "synchronized",
    };
    state.ws_manager.broadcast_system_wide(monitoring_update);

    Ok(Json(SystemHealthResponse {
        status: status.to_string(),
        database_connected: db_connected,
        disks: disk_statuses,
        consistency: ConsistencyStats {
            total_objects_scanned: total_objects,
            index_mirror_health: index_health,
            mirror_counts: if !mirror_map.is_empty() {
                Some(mirror_map)
            } else {
                None
            },
        },
        system: system_metrics,
        summary,
    }))
}
