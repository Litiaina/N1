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
    pub sampled_at: i64,
    pub status: String,
    pub disks: Vec<DiskStatus>,
    pub consistency: ConsistencyStats,
    pub system: SystemMetrics,
    pub uploads: index::UploadRuntimeStats,
    pub index_runtime: index::IndexRuntimeStats,
    pub storage: StoragePlaneStats,
    pub summary: String,
}

#[derive(Serialize, Clone, Default)]
pub struct StoragePlaneStats {
    pub index_roots_total: u64,
    pub index_roots_online: u64,
    pub blob_devices_total: u64,
    pub blob_devices_online: u64,
    pub raw_total_bytes: u64,
    pub raw_used_bytes: u64,
    pub raw_available_bytes: u64,
}

async fn check_index_disk_accessibility(path_str: String) -> DiskStatus {
    let path = PathBuf::from(&path_str);

    let (status, latency_ms, error) = if path.exists() {
        let check_result = timeout(DISK_CHECK_TIMEOUT, async {
            match fs::metadata(&path).await {
                Ok(meta) => {
                    if !meta.is_dir() {
                        return (
                            "offline".to_string(),
                            Some("Index root path is not a directory".to_string()),
                            None,
                        );
                    }

                    let start_time = Instant::now();
                    let mut entries = match fs::read_dir(&path).await {
                        Ok(entries) => entries,
                        Err(e) => {
                            return (
                                "offline".to_string(),
                                Some(format!("Failed to read dir: {}", e)),
                                None,
                            );
                        }
                    };
                    if let Err(e) = entries.next_entry().await {
                        return (
                            "offline".to_string(),
                            Some(format!("Failed to read dir entry: {}", e)),
                            None,
                        );
                    }
                    let latency = start_time.elapsed().as_millis() as u64;
                    let readonly = meta.permissions().readonly();
                    (
                        if readonly {
                            "readonly".to_string()
                        } else if latency > 1000 {
                            "slow".to_string()
                        } else {
                            "online".to_string()
                        },
                        readonly.then(|| "Index root is mounted read-only".to_string()),
                        Some(latency),
                    )
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
                Some("Disk probe timed out (>5s)".to_string()),
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

fn is_index_healthy_status(status: &str) -> bool {
    matches!(status, "online" | "slow" | "readonly")
}

fn is_blob_healthy_status(status: &str) -> bool {
    matches!(status, "online" | "slow" | "unformatted")
}

fn compute_storage_plane_stats(disks: &[DiskStatus]) -> StoragePlaneStats {
    let mut stats = StoragePlaneStats::default();
    for disk in disks {
        match disk.role.as_str() {
            "index" => {
                stats.index_roots_total += 1;
                if is_index_healthy_status(&disk.status) {
                    stats.index_roots_online += 1;
                }
            }
            "blob" => {
                stats.blob_devices_total += 1;
                if is_blob_healthy_status(&disk.status) {
                    stats.blob_devices_online += 1;
                }
                if let Some(raw) = disk.raw_backend.as_ref() {
                    stats.raw_total_bytes =
                        stats.raw_total_bytes.saturating_add(raw.total_bytes.unwrap_or(0));
                    stats.raw_used_bytes =
                        stats.raw_used_bytes.saturating_add(raw.used_bytes.unwrap_or(0));
                    stats.raw_available_bytes = stats
                        .raw_available_bytes
                        .saturating_add(raw.available_bytes.unwrap_or(0));
                }
            }
            _ => {}
        }
    }
    stats
}

async fn enumerate_fragment_names(roots: &[PathBuf]) -> Vec<String> {
    let mut fragments = std::collections::BTreeSet::new();
    for root in roots {
        let mut entries = match fs::read_dir(root).await {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let is_dir = entry
                .file_type()
                .await
                .map(|ft| ft.is_dir())
                .unwrap_or(false);
            if !is_dir {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                if !name.starts_with('.') {
                    fragments.insert(name.to_string());
                }
            }
        }
    }
    fragments.into_iter().collect()
}

async fn collect_consistency_counts(
    roots: &[PathBuf],
) -> Result<(u64, Option<HashMap<String, u64>>, bool), AppError> {
    let fragments = enumerate_fragment_names(roots).await;
    let mut total_objects = 0u64;
    let mut scan_failed = false;

    for fragment in fragments {
        match index::count_fragment_latest_objects(&fragment, false).await {
            Ok(count) => {
                total_objects = total_objects.saturating_add(count);
            }
            Err(_) => {
                scan_failed = true;
            }
        }
    }

    Ok((total_objects, None, scan_failed))
}

async fn collect_upload_stats_from_fragments(
    roots: &[PathBuf],
) -> Result<index::UploadRuntimeStats, AppError> {
    let fragments = enumerate_fragment_names(roots).await;
    let mut stats = index::UploadRuntimeStats::default();
    let now = chrono::Utc::now().timestamp();

    for fragment in fragments {
        let sessions = index::list_active_sessions(&fragment).await?;
        if sessions.is_empty() {
            continue;
        }
        stats.active_fragments += 1;
        for session in sessions {
            stats.active_sessions += 1;
            stats.reserved_bytes = stats
                .reserved_bytes
                .saturating_add(session.reservation.map(|r| r.amount).unwrap_or(0));
            stats.committed_parts = stats
                .committed_parts
                .saturating_add(session.parts.len() as u64);
            stats.committed_bytes = stats.committed_bytes.saturating_add(
                session
                    .parts
                    .values()
                    .map(|part| part.size_bytes)
                    .sum::<u64>(),
            );
            let age_seconds = now.saturating_sub(session.manifest.created_at).max(0) as u64;
            stats.oldest_session_age_seconds = Some(
                stats
                    .oldest_session_age_seconds
                    .map(|current| current.max(age_seconds))
                    .unwrap_or(age_seconds),
            );
        }
    }

    Ok(stats)
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
    let mut upload_stats = index::collect_runtime_upload_stats().await;
    let index_runtime = index::collect_index_runtime_stats().await;

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
        if is_index_healthy_status(&stat.status) || is_blob_healthy_status(&stat.status) {
            if stat.role == "index" {
                online_index_count += 1;
                active_index_roots.push(PathBuf::from(&stat.path));
            } else if is_blob_healthy_status(&stat.status) {
                online_blob_count += 1;
            }
        }
        disk_statuses.push(stat);
    }
    let storage = compute_storage_plane_stats(&disk_statuses);

    let deep_scan = params
        .get("deep_scan")
        .map(|v| v == "true")
        .unwrap_or(false);
    let mut total_objects = 0u64;
    let mut mirror_counts = None;
    let mut scan_performed = false;
    let mut scan_busy = false;
    let mut scan_failed = false;

    if deep_scan && !active_index_roots.is_empty() {
        if let Ok(_permit) = DEEP_SCAN_SEMAPHORE.try_acquire() {
            scan_performed = true;
            let roots_clone = active_index_roots.clone();
            let (tx, rx) = oneshot::channel();

            tokio::spawn(async move {
                let _ = tx.send(collect_consistency_counts(&roots_clone).await);
            });

            match rx.await {
                Ok(Ok((count, counts, failed))) => {
                    total_objects = count;
                    mirror_counts = counts;
                    scan_failed = failed;
                    if let Ok(scanned_uploads) = collect_upload_stats_from_fragments(&active_index_roots).await
                    {
                        upload_stats = scanned_uploads;
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(AppError::InternalServerError(format!(
                        "Consistency check task failed: {}",
                        e
                    )));
                }
            }
        } else {
            scan_busy = true;
        }
    }

    let index_health = if scan_busy {
        "skipped (scan in progress)".to_string()
    } else if !scan_performed {
        "skipped (use ?deep_scan=true)".to_string()
    } else if scan_failed {
        "degraded (scan completed with read errors)".to_string()
    } else {
        "authoritative latest scan".to_string()
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
    } else if scan_failed {
        (
            "warning",
            "Deep consistency scan completed with read errors. Review index root health."
                .to_string(),
        )
    } else {
        let mut summary = "All systems operational.".to_string();
        if upload_stats.active_sessions > 0 {
            summary.push_str(&format!(
                " {} active upload session(s) holding {} byte(s).",
                upload_stats.active_sessions, upload_stats.reserved_bytes
            ));
        }
        ("healthy", summary)
    };

    let monitoring_update = StorageEventPayload::MonitoringUpdate {
        system: serde_json::to_value(&system_metrics).unwrap_or(serde_json::Value::Null),
        repair_job: serde_json::to_value(&repair_status).ok(),
        redist_job: serde_json::to_value(&redist_status).ok(),
        shadow_index_status: index_health.clone(),
        shadow_index_healthy: !scan_failed,
    };
    state.ws_manager.broadcast_system_wide(monitoring_update);

    Ok(SystemHealthResponse {
        sampled_at: chrono::Utc::now().timestamp(),
        status: status.to_string(),
        disks: disk_statuses,
        consistency: ConsistencyStats {
            total_objects_scanned: total_objects,
            index_mirror_health: index_health,
            mirror_counts,
        },
        system: system_metrics,
        uploads: upload_stats,
        index_runtime,
        storage,
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
