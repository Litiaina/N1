use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{BLOB_STORE_DISKS, INDEX_STORE_DISKS};
use axum::{Json, extract::State, response::IntoResponse};
use mongodb::bson::doc;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;

#[derive(Serialize)]
pub struct DiskStatus {
    pub path: String,
    pub role: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct SystemHealthResponse {
    pub status: String,
    pub database_connected: bool,
    pub disks: Vec<DiskStatus>,
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
) -> Result<impl IntoResponse, AppError> {
    let db_connected = state
        .db_client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .is_ok();

    let index_disks = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError("Index config missing".into()))?;

    let blob_disks = BLOB_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError("Blob config missing".into()))?;

    let mut disk_statuses = Vec::new();
    let mut online_index_count = 0;
    let mut online_blob_count = 0;

    for disk in index_disks {
        let stat = check_disk_accessibility(disk, "index").await;
        if stat.status == "online" {
            online_index_count += 1;
        }
        disk_statuses.push(stat);
    }

    for disk in blob_disks {
        let stat = check_disk_accessibility(disk, "blob").await;
        if stat.status == "online" {
            online_blob_count += 1;
        }
        disk_statuses.push(stat);
    }

    let total_index = index_disks.len();
    let total_blob = blob_disks.len();

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
                "Running on {}/{} index mirrors. redundancy lost.",
                online_index_count, total_index
            ),
        )
    } else if online_blob_count < total_blob {
        (
            "degraded",
            format!(
                "Running on {}/{} data disks. performance/capacity reduced.",
                online_blob_count, total_blob
            ),
        )
    } else {
        ("healthy", "All systems operational.".to_string())
    };

    Ok(Json(SystemHealthResponse {
        status: status.to_string(),
        database_connected: db_connected,
        disks: disk_statuses,
        summary,
    }))
}
