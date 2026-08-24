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
        let inspection =
            inspect_namespace_replicas(&fragment, active_index_roots, deep_scan).await?;
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

#[path = "monitoring/snapshot.rs"]
mod snapshot;

use snapshot::disk_capacity_snapshot;
pub use snapshot::*;
