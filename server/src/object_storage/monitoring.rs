use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{BLOB_STORE_DISKS, INDEX_STORE_DISKS};
use axum::{
    Json,
    extract::{Query, State},
    response::IntoResponse,
};
use mongodb::bson::doc;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use tokio::fs;
use tokio::sync::{Semaphore, oneshot};
use walkdir::WalkDir;

static DEEP_SCAN_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(1));

#[derive(Serialize)]
pub struct DiskStatus {
    pub path: String,
    pub role: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ConsistencyStats {
    pub total_objects_scanned: u64,
    pub index_mirror_health: String,
}

#[derive(Serialize)]
pub struct SystemHealthResponse {
    pub status: String,
    pub database_connected: bool,
    pub disks: Vec<DiskStatus>,
    pub consistency: ConsistencyStats,
    pub summary: String,
}

async fn check_disk_accessibility(path_str: &str, role: &str) -> DiskStatus {
    let path = PathBuf::from(path_str);
    let mut status = "offline";
    let mut error_msg = None;

    if path.exists() {
        match fs::metadata(&path).await {
            Ok(meta) => {
                if meta.permissions().readonly() {
                    status = "readonly";
                    error_msg = Some("Disk is mounted read-only".to_string());
                } else {
                    status = "online";
                }
            }
            Err(e) => {
                error_msg = Some(format!("Failed to read metadata: {}", e));
            }
        }
    } else {
        error_msg = Some("Path does not exist".to_string());
    }

    DiskStatus {
        path: path_str.to_string(),
        role: role.to_string(),
        status: status.to_string(),
        error: error_msg,
    }
}

pub async fn get_system_health(
    State(state): State<Arc<AppState>>,
    _claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
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

    let mut disk_statuses = Vec::new();
    let mut online_index_count = 0;
    let mut online_blob_count = 0;
    let mut active_index_roots = Vec::new();

    for disk in index_disks_config {
        let stat = check_disk_accessibility(disk, "index").await;
        if stat.status == "online" {
            online_index_count += 1;
            active_index_roots.push(PathBuf::from(disk).join(OBJECT_DATA_ROOT));
        }
        disk_statuses.push(stat);
    }

    for disk in blob_disks_config {
        let stat = check_disk_accessibility(disk, "blob").await;
        if stat.status == "online" {
            online_blob_count += 1;
        }
        disk_statuses.push(stat);
    }

    let deep_scan = params
        .get("deep_scan")
        .map(|v| v == "true")
        .unwrap_or(false);
    let mut object_counts = Vec::new();
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
                    let count = WalkDir::new(&root)
                        .into_iter()
                        .filter_map(|e| e.ok())
                        .filter(|e| {
                            e.file_type().is_file()
                                && e.path().extension().map_or(false, |ext| ext == "bson")
                        })
                        .count() as u64;
                    counts.push(count);
                    std::thread::yield_now();
                }

                let _ = tx.send(counts);
            });

            object_counts = rx.await.map_err(|e| {
                AppError::InternalServerError(format!("Consistency check thread failed: {}", e))
            })?;
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

    Ok(Json(SystemHealthResponse {
        status: status.to_string(),
        database_connected: db_connected,
        disks: disk_statuses,
        consistency: ConsistencyStats {
            total_objects_scanned: total_objects,
            index_mirror_health: index_health,
        },
        summary,
    }))
}
