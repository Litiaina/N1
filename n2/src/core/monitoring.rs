use crate::core::constants::N2_BLOB_DIR;
use crate::core::disk_manager::{ObjectPathManager, STORAGE_DISKS};
use crate::core::fragment_identity::{
    fragment_root_on, list_fragment_aliases, physical_fragment_root_on, stable_fragment_id_for_root,
};
use crate::core::fragment_manager::is_fragment_hash;
use crate::core::fragment_metadata::resolve_stable_fragment_id;
use crate::core::garbage_collector::get_gc_status;
use crate::core::index;
use crate::core::io_scheduler::{DiskIoSchedulerSnapshot, disk_io_scheduler_snapshot};
use crate::core::operation_gate::{MaintenanceAccessMode, current_maintenance_access_mode};
use crate::core::redistribution::{
    RedistributionIntentDebtSnapshot, collect_redistribution_intent_debt, get_redist_status,
};
use crate::core::redistribution::{RedistributionScope, check_needs_redistribution};
use crate::core::repair_manager::get_repair_status;
use crate::core::system::{SystemMetrics, get_system_metrics};
use crate::core::upload::{DATA_SHARDS_ATOMIC, PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC};
use crate::enums::app_error_enum::AppError;
use crate::io_control::{IoControlSnapshot, io_control_snapshot};
use crate::namespace::replica::inspect_namespace_replicas;
use crate::namespace::runtime::{NamespaceRuntimeStats, global_namespace_runtime};
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;
use axum::{
    Json,
    extract::{Query, State},
    response::IntoResponse,
};
use chrono::Utc;
use futures_util::future::join_all;
use once_cell::sync::OnceCell;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
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
static REDIST_INTENT_DEBT_CACHE: LazyLock<
    Arc<tokio::sync::RwLock<Option<CachedRedistributionIntentDebt>>>,
> = LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(None)));
const REDIST_INTENT_DEBT_REFRESH_INTERVAL: Duration = Duration::from_secs(15);

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

pub(crate) fn human_bytes_for_display(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut idx = 0usize;
    while value >= 1024.0 && idx < UNITS.len() - 1 {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{bytes} {}", UNITS[idx])
    } else {
        format!("{value:.2} {}", UNITS[idx])
    }
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
    pub total_object_records_scanned: u64,
    pub object_record_health: String,
    pub namespace_replicas: NamespaceReplicaHealthStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_record_counts: Option<HashMap<String, u64>>,
}

#[derive(Serialize, Clone, Default)]
pub struct NamespaceReplicaHealthStats {
    pub fragments_checked: u64,
    pub fragments_degraded: u64,
    pub fragments_critical: u64,
    pub missing_replicas: u64,
    pub divergent_replicas: u64,
    pub corrupt_replicas: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub degraded_fragments: Vec<String>,
}

#[derive(Serialize, Clone, Default)]
pub struct UploadRuntimeStats {
    pub active_fragments: u64,
    pub active_sessions: u64,
    pub reserved_bytes: u64,
    pub committed_parts: u64,
    pub committed_bytes: u64,
    pub stale_sessions: u64,
    pub stale_reserved_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_session_age_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_stale_session_age_seconds: Option<u64>,
}

#[derive(Serialize, Clone, Default)]
pub struct ObjectRecordRuntimeStats {
    pub scanned_object_records: u64,
    pub shard_roots_online: u64,
}

#[derive(Serialize, Clone, Default)]
pub struct StoragePlaneStats {
    pub storage_devices_total: u64,
    pub storage_devices_online: u64,
    pub raw_total_bytes: u64,
    pub raw_used_bytes: u64,
    pub raw_available_bytes: u64,
    pub heterogeneous_storage_capacity: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_capacity_imbalance_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement_limiter_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement_limiter_available_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement_limiter_usage_percent: Option<f64>,
}

#[derive(Serialize, Clone, Default)]
pub struct MaintenanceRuntimeStats {
    pub access_mode: String,
    pub repair_running: bool,
    pub repair_phase: String,
    pub redistribution_running: bool,
    pub redistribution_phase: String,
    pub gc_running: bool,
    pub gc_phase: String,
    pub pending_redistribution_intents: u64,
    pub failed_redistribution_intents: u64,
    pub fragments_with_pending_redistribution: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_pending_redistribution_intent_age_seconds: Option<u64>,
}

#[derive(Serialize, Clone)]
pub struct SystemHealthResponse {
    pub sampled_at: i64,
    pub status: String,
    pub disks: Vec<DiskStatus>,
    pub consistency: ConsistencyStats,
    pub system: SystemMetrics,
    pub uploads: UploadRuntimeStats,
    pub io_scheduler: DiskIoSchedulerSnapshot,
    pub io_control: IoControlSnapshot,
    pub object_record_runtime: ObjectRecordRuntimeStats,
    pub namespace_runtime: NamespaceRuntimeStats,
    pub storage: StoragePlaneStats,
    pub maintenance: MaintenanceRuntimeStats,
    pub summary: String,
}

#[derive(Clone)]
struct CachedRedistributionIntentDebt {
    collected_at: Instant,
    snapshot: RedistributionIntentDebtSnapshot,
}

struct StoragePlacementRisk {
    heterogeneous_storage_capacity: bool,
    storage_capacity_imbalance_ratio: Option<f64>,
    placement_limiter_path: Option<String>,
    placement_limiter_available_bytes: Option<u64>,
    placement_limiter_usage_percent: Option<f64>,
}

#[derive(Default)]
struct FragmentRedundancyDebt {
    pending_gc_blob_fragments: Vec<String>,
    sampled_degraded_fragments: Vec<String>,
}

fn should_count_index_record(path: &std::path::Path) -> bool {
    let name = match path.file_name().and_then(|n| n.to_str()) {
        Some(s) => s,
        None => return false,
    };

    if !name.ends_with(".ncdx") {
        return false;
    }

    if path
        .components()
        .any(|component| matches!(component.as_os_str().to_str(), Some(".trash" | ".uploads")))
    {
        return false;
    }

    let Some(parent) = path.parent() else {
        return false;
    };
    parent
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|parent_name| parent_name.starts_with("@o_"))
}

#[cfg(test)]
mod tests {
    use super::should_count_index_record;
    use std::path::Path;

    #[test]
    fn deep_scan_counts_only_live_object_records() {
        assert!(should_count_index_record(Path::new(
            "frag/.objects/ab/cd/@o_abc/01HZ.123.456.7.ncdx"
        )));
        assert!(!should_count_index_record(Path::new(
            "frag/.uploads/0001.pending.ncdx"
        )));
        assert!(!should_count_index_record(Path::new("frag/.fragment.ncdx")));
        assert!(!should_count_index_record(Path::new(
            "frag/.trash/.objects/ab/cd/@o_abc/01HZ.123.456.7.ncdx"
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
                    let check_file_name = format!(".n2_health_{}", Ulid::new().to_string());
                    let check_path = path.join(check_file_name);
                    let payload = b"n2_io_check";

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

async fn list_known_fragments(index_roots: &[PathBuf], blob_roots: &[PathBuf]) -> Vec<String> {
    let index_roots = index_roots.to_vec();
    let blob_roots = blob_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut seen = HashSet::new();
        for root in index_roots {
            if !root.exists() {
                continue;
            }
            for name in list_fragment_aliases(&root) {
                let Some(stable) = stable_fragment_id_for_root(&root, &name) else {
                    continue;
                };
                let has_metadata = blob_roots.iter().any(|blob_root| {
                    physical_fragment_root_on(blob_root, &stable)
                        .join(crate::core::constants::FRAGMENT_METADATA_FILE)
                        .is_file()
                });
                if is_fragment_hash(&name) && has_metadata {
                    seen.insert(name);
                }
            }
        }
        let mut out: Vec<String> = seen.into_iter().collect();
        out.sort();
        out
    })
    .await
    .unwrap_or_default()
}

async fn fragment_has_blob_payload(fragment: &str, blob_roots: &[PathBuf]) -> bool {
    let fragment = fragment.to_string();
    let roots = blob_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let stable = ObjectPathManager::new().resolve_stable_fragment_id(&fragment);
        for root in roots {
            let blob_dir = physical_fragment_root_on(&root, &stable).join(N2_BLOB_DIR);
            for entry in WalkDir::new(&blob_dir)
                .min_depth(5)
                .max_depth(5)
                .into_iter()
                .filter_map(Result::ok)
            {
                if entry.file_type().is_dir()
                    && entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with("@o_"))
                {
                    return true;
                }
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

async fn fragment_has_indexed_payload(
    fragment: &str,
    _active_index_roots: &[PathBuf],
) -> Result<bool, AppError> {
    Ok(global_namespace_runtime()
        .reachability_snapshot_if_present(fragment)
        .await?
        .is_some_and(|snapshot| !snapshot.referenced_object_ids.is_empty()))
}

async fn fragment_has_sampled_redundancy_debt(
    fragment: &str,
    deep_scan: bool,
    target_data: usize,
    target_parity: usize,
    target_stripe_size: usize,
    active_blob_roots: &[PathBuf],
    active_index_roots: &[PathBuf],
) -> Result<bool, AppError> {
    let page_limit = if deep_scan { 128 } else { 32 };
    let mut live_token = None;
    let mut checked = 0usize;

    loop {
        let page = index::scan_fragment_live_metadata_page(
            fragment,
            None,
            None,
            page_limit,
            live_token.clone(),
            true,
        )
        .await?;

        for meta in page.objects {
            if meta.is_delete_marker {
                continue;
            }
            for part in &meta.parts {
                for shard in &part.shards {
                    if !fs::try_exists(&shard.absolute_path).await.unwrap_or(false) {
                        return Ok(true);
                    }
                }
            }

            let needs = check_needs_redistribution(
                &meta,
                RedistributionScope::All,
                target_data,
                target_parity,
                target_stripe_size,
                active_blob_roots.len(),
                active_blob_roots,
                active_index_roots,
                false,
            )
            .await;

            if needs.blob || needs.index {
                return Ok(true);
            }

            checked += 1;
            if !deep_scan && checked >= page_limit {
                return Ok(false);
            }
        }

        match page.continuation_token {
            Some(token) if deep_scan => live_token = Some(token),
            _ => break,
        }
    }

    Ok(false)
}

async fn detect_fragment_redundancy_debt(
    active_index_roots: &[PathBuf],
    active_blob_roots: &[PathBuf],
    deep_scan: bool,
    target_fragment: Option<&str>,
) -> Result<FragmentRedundancyDebt, AppError> {
    if active_index_roots.is_empty() && active_blob_roots.is_empty() {
        return Ok(FragmentRedundancyDebt::default());
    }

    let mut fragments = list_known_fragments(active_index_roots, active_blob_roots).await;
    if let Some(target) = target_fragment {
        fragments.retain(|fragment| fragment == target);
    }
    if fragments.is_empty() {
        return Ok(FragmentRedundancyDebt::default());
    }

    let target_data = DATA_SHARDS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(1);
    let target_parity = PARITY_SHARDS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(0);
    let target_stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(262_144);

    let mut debt = FragmentRedundancyDebt::default();
    for fragment in fragments {
        let has_indexed_payload =
            fragment_has_indexed_payload(&fragment, active_index_roots).await?;
        if !has_indexed_payload {
            if fragment_has_blob_payload(&fragment, active_blob_roots).await {
                debt.pending_gc_blob_fragments.push(fragment);
            }
            continue;
        }

        if fragment_has_sampled_redundancy_debt(
            &fragment,
            deep_scan,
            target_data,
            target_parity,
            target_stripe_size,
            active_blob_roots,
            active_index_roots,
        )
        .await?
        {
            debt.sampled_degraded_fragments.push(fragment);
        }
    }

    Ok(debt)
}

async fn detect_namespace_replica_debt(
    active_index_roots: &[PathBuf],
    active_blob_roots: &[PathBuf],
    deep_scan: bool,
    target_fragment: Option<&str>,
) -> Result<NamespaceReplicaHealthStats, AppError> {
    if active_index_roots.is_empty() {
        return Ok(NamespaceReplicaHealthStats::default());
    }

    let mut fragments = list_known_fragments(active_index_roots, active_blob_roots).await;
    if let Some(target) = target_fragment {
        fragments.retain(|fragment| fragment == target);
    }
    let mut stats = NamespaceReplicaHealthStats::default();

    for fragment in fragments {
        let inspection = inspect_namespace_replicas(&fragment, active_index_roots).await?;
        let has_namespace_materialized = inspection.present_replicas > 0;
        let has_indexed_payload = if has_namespace_materialized {
            true
        } else if deep_scan {
            fragment_has_indexed_payload(&fragment, active_index_roots).await?
        } else {
            false
        };

        if !has_indexed_payload {
            continue;
        }

        stats.fragments_checked += 1;
        if inspection.healthy {
            continue;
        }

        stats.fragments_degraded += 1;
        stats.missing_replicas += inspection.missing_replicas;
        stats.divergent_replicas += inspection.divergent_replicas;
        stats.corrupt_replicas += inspection.corrupt_replicas;

        if inspection.valid_replicas == 0 {
            stats.fragments_critical += 1;
        }

        if stats.degraded_fragments.len() < 20 {
            stats.degraded_fragments.push(fragment);
        }
    }

    Ok(stats)
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
    let target_fragment = match params
        .get("fragment")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        Some(fragment) => Some(resolve_stable_fragment_id(&fragment).await?),
        None => None,
    };
    let system_metrics = get_system_metrics().await;
    let redist_status = get_redist_status().await;
    let repair_status = get_repair_status().await;
    let gc_status = get_gc_status().await;

    let storage_disks_config = STORAGE_DISKS.get().ok_or(AppError::InternalServerError(
        "Storage config missing".into(),
    ))?;

    let mut check_futures = Vec::new();

    for disk in storage_disks_config {
        check_futures.push(check_disk_accessibility(
            disk.clone(),
            "storage".to_string(),
        ));
    }

    let disk_results = join_all(check_futures).await;

    let mut online_storage_count = 0;
    let mut active_storage_roots = Vec::new();
    let mut disk_statuses = Vec::new();

    for stat in disk_results {
        if stat.status == "online" || stat.status == "slow" {
            online_storage_count += 1;
            active_storage_roots.push(PathBuf::from(&stat.path));
        }
        disk_statuses.push(stat);
    }

    let deep_scan = params
        .get("deep_scan")
        .map(|v| v == "true")
        .unwrap_or(false);
    let force_refresh = params.get("refresh").map(|v| v == "true").unwrap_or(false);
    let mut object_record_counts = Vec::new();
    let mut object_record_map = HashMap::new();
    let mut scan_performed = false;
    let mut scan_busy = false;

    if deep_scan && !active_storage_roots.is_empty() {
        if let Ok(_permit) = DEEP_SCAN_SEMAPHORE.try_acquire() {
            scan_performed = true;
            let roots_clone = active_storage_roots.clone();
            let target_fragment_for_scan = target_fragment.clone();
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
                    let scan_root = target_fragment_for_scan
                        .as_ref()
                        .map(|fragment| fragment_root_on(&root, fragment))
                        .unwrap_or_else(|| root.clone());
                    let walker = WalkDir::new(&scan_root).into_iter();

                    for entry in walker.filter_map(|e| e.ok()) {
                        let path = entry.path();
                        if entry.file_type().is_file() && should_count_index_record(path) {
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

            object_record_counts = rx.await.map_err(|e| {
                AppError::InternalServerError(format!("Consistency check thread failed: {}", e))
            })?;

            for (i, root) in active_storage_roots.iter().enumerate() {
                if let Some(count) = object_record_counts.get(i) {
                    object_record_map.insert(root.to_string_lossy().to_string(), *count);
                }
            }
        } else {
            scan_busy = true;
        }
    }

    let total_object_records = object_record_counts.iter().copied().sum::<u64>();

    let object_record_health = if scan_busy {
        "skipped (scan in progress)".to_string()
    } else if !scan_performed {
        "skipped (use ?deep_scan=true)".to_string()
    } else if object_record_counts.is_empty() {
        "empty".to_string()
    } else {
        "records-scanned".to_string()
    };

    let total_storage = storage_disks_config.len();
    let uploads =
        collect_upload_runtime_stats(&active_storage_roots, target_fragment.as_deref()).await?;
    let namespace_runtime = global_namespace_runtime().runtime_stats();
    let redistribution_debt = load_redistribution_intent_debt(
        &active_storage_roots,
        force_refresh || deep_scan,
        target_fragment.clone(),
    )
    .await;
    let fragment_redundancy_debt = detect_fragment_redundancy_debt(
        &active_storage_roots,
        &active_storage_roots,
        deep_scan,
        target_fragment.as_deref(),
    )
    .await?;
    let namespace_replica_debt = detect_namespace_replica_debt(
        &active_storage_roots,
        &active_storage_roots,
        deep_scan,
        target_fragment.as_deref(),
    )
    .await?;
    let storage = collect_storage_plane_stats(
        &disk_statuses,
        total_storage as u64,
        online_storage_count as u64,
    );
    let placement_limiter_low = storage
        .placement_limiter_available_bytes
        .map(|bytes| bytes <= 8 * 1024 * 1024 * 1024)
        .unwrap_or(false);
    let placement_limiter_summary = storage
        .placement_limiter_path
        .as_ref()
        .zip(storage.placement_limiter_available_bytes)
        .map(|(path, available)| {
            format!(
                "Placement limited by {} ({} free).",
                path,
                human_bytes_for_display(available)
            )
        });

    let (status, mut summary) = if online_storage_count == 0 {
        (
            "critical",
            "All configured storage devices are offline. System cannot function.".to_string(),
        )
    } else if online_storage_count < total_storage {
        (
            "degraded",
            format!(
                "Running on {}/{} storage devices. Redundancy and capacity are reduced.",
                online_storage_count, total_storage
            ),
        )
    } else if !fragment_redundancy_debt
        .sampled_degraded_fragments
        .is_empty()
    {
        (
            "degraded",
            format!(
                "Fragments have missing or out-of-layout shard copies and need repair/redistribution: {}.",
                fragment_redundancy_debt
                    .sampled_degraded_fragments
                    .join(", ")
            ),
        )
    } else if namespace_replica_debt.fragments_critical > 0 {
        (
            "critical",
            format!(
                "N2 index has no valid replica for {} fragment(s): {}.",
                namespace_replica_debt.fragments_critical,
                namespace_replica_debt.degraded_fragments.join(", ")
            ),
        )
    } else if namespace_replica_debt.fragments_degraded > 0 {
        (
            "degraded",
            format!(
                "N2 namespace voters need synchronization for {} fragment(s): {}.",
                namespace_replica_debt.fragments_degraded,
                namespace_replica_debt.degraded_fragments.join(", ")
            ),
        )
    } else if placement_limiter_low {
        (
            "degraded",
            placement_limiter_summary.clone().unwrap_or_else(|| {
                "Storage placement is capacity-limited; uploads may fail before aggregate capacity is exhausted.".to_string()
            }),
        )
    } else {
        ("healthy", "All systems operational.".to_string())
    };

    if status == "healthy" {
        if !fragment_redundancy_debt
            .pending_gc_blob_fragments
            .is_empty()
        {
            summary.push_str(&format!(
                " Headless blob payload is pending GC for fragment(s): {}.",
                fragment_redundancy_debt
                    .pending_gc_blob_fragments
                    .join(", ")
            ));
        }
        if storage.heterogeneous_storage_capacity {
            if let Some(ratio) = storage.storage_capacity_imbalance_ratio {
                let note = if let Some(limit) = placement_limiter_summary.as_ref() {
                    format!(" Storage devices are size-imbalanced ({ratio:.2}x). {limit}")
                } else {
                    format!(" Storage devices are size-imbalanced ({ratio:.2}x).")
                };
                summary.push_str(&note);
            }
        } else if let Some(limit) = placement_limiter_summary.as_ref() {
            summary.push(' ');
            summary.push_str(limit);
        }
    }

    let monitoring_update = StorageEventPayload::MonitoringUpdate {
        system: serde_json::to_value(&system_metrics).unwrap_or(serde_json::Value::Null),
        uploads: serde_json::to_value(&uploads).ok(),
        namespace_runtime: serde_json::to_value(&namespace_runtime).ok(),
        maintenance: serde_json::to_value(MaintenanceRuntimeStats {
            access_mode: maintenance_access_mode_label(current_maintenance_access_mode()).into(),
            repair_running: repair_status.is_running,
            repair_phase: repair_status.phase.clone(),
            redistribution_running: redist_status.is_running,
            redistribution_phase: redist_status.phase.clone(),
            gc_running: gc_status.is_running,
            gc_phase: gc_status.phase.clone(),
            pending_redistribution_intents: redistribution_debt.pending_intents,
            failed_redistribution_intents: redistribution_debt.failed_intents,
            fragments_with_pending_redistribution: redistribution_debt
                .fragments_with_pending_intents,
            oldest_pending_redistribution_intent_age_seconds: redistribution_debt
                .oldest_pending_intent_age_seconds,
        })
        .ok(),
        repair_job: serde_json::to_value(&repair_status).ok(),
        redist_job: serde_json::to_value(&redist_status).ok(),
        object_record_status: object_record_health.clone(),
        namespace_healthy: online_storage_count == total_storage,
        sampled_at: Some(Utc::now().timestamp()),
        summary: Some(summary.clone()),
    };
    state.ws_manager.broadcast_system_wide(monitoring_update);

    Ok(SystemHealthResponse {
        sampled_at: Utc::now().timestamp(),
        status: status.to_string(),
        disks: disk_statuses,
        consistency: ConsistencyStats {
            total_object_records_scanned: total_object_records,
            object_record_health,
            namespace_replicas: namespace_replica_debt,
            object_record_counts: if !object_record_map.is_empty() {
                Some(object_record_map)
            } else {
                None
            },
        },
        system: system_metrics,
        uploads,
        io_scheduler: disk_io_scheduler_snapshot(),
        io_control: io_control_snapshot(),
        object_record_runtime: ObjectRecordRuntimeStats {
            scanned_object_records: total_object_records,
            shard_roots_online: online_storage_count as u64,
        },
        namespace_runtime,
        storage,
        maintenance: MaintenanceRuntimeStats {
            access_mode: maintenance_access_mode_label(current_maintenance_access_mode()).into(),
            repair_running: repair_status.is_running,
            repair_phase: repair_status.phase,
            redistribution_running: redist_status.is_running,
            redistribution_phase: redist_status.phase,
            gc_running: gc_status.is_running,
            gc_phase: gc_status.phase,
            pending_redistribution_intents: redistribution_debt.pending_intents,
            failed_redistribution_intents: redistribution_debt.failed_intents,
            fragments_with_pending_redistribution: redistribution_debt
                .fragments_with_pending_intents,
            oldest_pending_redistribution_intent_age_seconds: redistribution_debt
                .oldest_pending_intent_age_seconds,
        },
        summary,
    })
}

async fn load_redistribution_intent_debt(
    active_index_roots: &[PathBuf],
    force_refresh: bool,
    target_fragment: Option<String>,
) -> RedistributionIntentDebtSnapshot {
    if active_index_roots.is_empty() {
        return RedistributionIntentDebtSnapshot::default();
    }

    if target_fragment.is_none() && !force_refresh {
        let cached = REDIST_INTENT_DEBT_CACHE.read().await.clone();
        if let Some(cached) = cached {
            if cached.collected_at.elapsed() < REDIST_INTENT_DEBT_REFRESH_INTERVAL {
                return cached.snapshot;
            }
        }
    }

    let snapshot =
        collect_redistribution_intent_debt(active_index_roots, target_fragment.clone()).await;
    if target_fragment.is_none() {
        *REDIST_INTENT_DEBT_CACHE.write().await = Some(CachedRedistributionIntentDebt {
            collected_at: Instant::now(),
            snapshot: snapshot.clone(),
        });
    }
    snapshot
}

async fn collect_upload_runtime_stats(
    active_index_roots: &[PathBuf],
    target_fragment: Option<&str>,
) -> Result<UploadRuntimeStats, AppError> {
    if active_index_roots.is_empty() {
        return Ok(UploadRuntimeStats::default());
    }

    let roots = active_index_roots.to_vec();
    let target_fragment_owned = target_fragment.map(str::to_string);
    let fragments = tokio::task::spawn_blocking(move || -> Vec<String> {
        let mut seen = HashSet::new();
        if let Some(fragment) = target_fragment_owned {
            if roots
                .iter()
                .any(|root| fragment_root_on(root, &fragment).is_dir())
            {
                seen.insert(fragment);
            }
            return seen.into_iter().collect();
        }
        for root in roots {
            if !root.exists() {
                continue;
            }
            for name in list_fragment_aliases(&root) {
                if is_fragment_hash(&name) {
                    seen.insert(stable_fragment_id_for_root(&root, &name).unwrap_or(name));
                }
            }
        }
        let mut out: Vec<String> = seen.into_iter().collect();
        out.sort();
        out
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("upload stats join error: {}", e)))?;

    let now = Utc::now().timestamp();
    let stale_after = CONFIG
        .garbage_collector
        .file_ttl_seconds
        .min(i64::MAX as u64) as i64;
    let mut stats = UploadRuntimeStats::default();
    let mut oldest_created_at: Option<i64> = None;
    let mut oldest_stale_created_at: Option<i64> = None;

    for fragment in fragments {
        let sessions = index::list_active_sessions(&fragment).await?;
        let mut fragment_has_active = false;

        for session in sessions
            .into_iter()
            .filter(|state| state.finalized_at.is_none() && state.aborted_at.is_none())
        {
            let session_created_at = session.manifest.created_at;
            fragment_has_active = true;
            stats.active_sessions = stats.active_sessions.saturating_add(1);
            let reserved_bytes = session
                .reservation
                .map(|receipt| receipt.amount)
                .unwrap_or(0);
            stats.reserved_bytes = stats.reserved_bytes.saturating_add(reserved_bytes);

            if !session.part_receipts.is_empty() {
                stats.committed_parts = stats
                    .committed_parts
                    .saturating_add(session.part_receipts.len() as u64);
                stats.committed_bytes = stats.committed_bytes.saturating_add(
                    session
                        .part_receipts
                        .values()
                        .map(|receipt| receipt.size_bytes)
                        .sum::<u64>(),
                );
            } else {
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
            }

            if stale_after > 0 && now.saturating_sub(session.last_activity_at) >= stale_after {
                stats.stale_sessions = stats.stale_sessions.saturating_add(1);
                stats.stale_reserved_bytes =
                    stats.stale_reserved_bytes.saturating_add(reserved_bytes);
                oldest_stale_created_at = Some(match oldest_stale_created_at {
                    Some(current) => current.min(session_created_at),
                    None => session_created_at,
                });
            }

            oldest_created_at = Some(match oldest_created_at {
                Some(current) => current.min(session_created_at),
                None => session_created_at,
            });
        }

        if fragment_has_active {
            stats.active_fragments = stats.active_fragments.saturating_add(1);
        }
    }

    stats.oldest_session_age_seconds = oldest_created_at.map(|created_at| {
        if now > created_at {
            (now - created_at) as u64
        } else {
            0
        }
    });
    stats.oldest_stale_session_age_seconds = oldest_stale_created_at.map(|created_at| {
        if now > created_at {
            (now - created_at) as u64
        } else {
            0
        }
    });

    Ok(stats)
}

fn maintenance_access_mode_label(mode: MaintenanceAccessMode) -> &'static str {
    match mode {
        MaintenanceAccessMode::Availability => "availability",
        MaintenanceAccessMode::ReadOnly => "read_only",
        MaintenanceAccessMode::Quiesced => "quiesced",
    }
}

fn collect_storage_plane_stats(
    disk_statuses: &[DiskStatus],
    storage_devices_total: u64,
    storage_devices_online: u64,
) -> StoragePlaneStats {
    let mut raw_total_bytes = 0u64;
    let mut raw_used_bytes = 0u64;
    let mut raw_available_bytes = 0u64;

    for disk in disk_statuses.iter().filter(|disk| disk.role == "storage") {
        raw_total_bytes = raw_total_bytes.saturating_add(disk.total_bytes.unwrap_or(0));
        raw_used_bytes = raw_used_bytes.saturating_add(disk.used_bytes.unwrap_or(0));
        raw_available_bytes = raw_available_bytes.saturating_add(disk.available_bytes.unwrap_or(0));
    }

    let blob_risk = analyze_storage_placement_risk(disk_statuses);

    StoragePlaneStats {
        storage_devices_total,
        storage_devices_online,
        raw_total_bytes,
        raw_used_bytes,
        raw_available_bytes,
        heterogeneous_storage_capacity: blob_risk.heterogeneous_storage_capacity,
        storage_capacity_imbalance_ratio: blob_risk.storage_capacity_imbalance_ratio,
        placement_limiter_path: blob_risk.placement_limiter_path,
        placement_limiter_available_bytes: blob_risk.placement_limiter_available_bytes,
        placement_limiter_usage_percent: blob_risk.placement_limiter_usage_percent,
    }
}

fn analyze_storage_placement_risk(disk_statuses: &[DiskStatus]) -> StoragePlacementRisk {
    let storage_disks: Vec<&DiskStatus> = disk_statuses
        .iter()
        .filter(|disk| disk.role == "storage" && disk.total_bytes.unwrap_or(0) > 0)
        .collect();

    if storage_disks.is_empty() {
        return StoragePlacementRisk {
            heterogeneous_storage_capacity: false,
            storage_capacity_imbalance_ratio: None,
            placement_limiter_path: None,
            placement_limiter_available_bytes: None,
            placement_limiter_usage_percent: None,
        };
    }

    let smallest_total = storage_disks
        .iter()
        .filter_map(|disk| disk.total_bytes)
        .min()
        .unwrap_or(0);
    let largest_total = storage_disks
        .iter()
        .filter_map(|disk| disk.total_bytes)
        .max()
        .unwrap_or(0);
    let storage_capacity_imbalance_ratio = if smallest_total > 0 && largest_total >= smallest_total
    {
        Some(largest_total as f64 / smallest_total as f64)
    } else {
        None
    };
    let heterogeneous_storage_capacity = storage_capacity_imbalance_ratio
        .map(|ratio| ratio >= 1.5)
        .unwrap_or(false);

    let limiter = storage_disks
        .iter()
        .filter_map(|disk| {
            disk.available_bytes
                .map(|available| (available, disk.path.clone(), disk.usage_percent))
        })
        .min_by_key(|(available, _, _)| *available);

    StoragePlacementRisk {
        heterogeneous_storage_capacity,
        storage_capacity_imbalance_ratio,
        placement_limiter_path: limiter.as_ref().map(|(_, path, _)| path.clone()),
        placement_limiter_available_bytes: limiter.as_ref().map(|(available, _, _)| *available),
        placement_limiter_usage_percent: limiter.and_then(|(_, _, usage)| usage),
    }
}

pub async fn run_system_monitor(state: Arc<AppState>) {
    let mut ticker = tokio::time::interval(monitor_interval());
    ticker.tick().await;

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
    Ok(Json(system_health_snapshot(state, claims, params).await?))
}

pub async fn system_health_snapshot(
    state: Arc<AppState>,
    claims: Claims,
    params: HashMap<String, String>,
) -> Result<SystemHealthResponse, AppError> {
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
        return Ok(snapshot);
    }

    if let Some(cached) = HEALTH_CACHE.read().await.clone() {
        return Ok(cached);
    }

    let snapshot = collect_system_health(&state, &params).await?;
    *HEALTH_CACHE.write().await = Some(snapshot.clone());
    Ok(snapshot)
}
