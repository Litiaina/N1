use crate::core::disk_manager::{BLOB_STORE_DISKS, INDEX_STORE_DISKS};
use crate::core::garbage_collector::get_gc_status;
use crate::core::operation_gate::{
    safety_read_only_reason, set_auto_healing_fault, set_safety_read_only,
    topology_read_only_reason,
};
use crate::core::redistribution::get_redist_status;
use crate::core::repair_manager::{
    RepairCheckMode, RepairRunConfig, RepairRunOptions, get_repair_status,
    run_repair_job_with_options,
};
use crate::core::root_identity::{expected_root_identities, root_matches_expected_identity};
use crate::core::upload::{DATA_SHARDS_ATOMIC, PARITY_SHARDS_ATOMIC};
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use chrono::Utc;
use futures_util::{StreamExt, stream};
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Default)]
pub struct AutoHealingStatus {
    pub enabled: bool,
    pub phase: String,
    pub write_safe: bool,
    pub read_only_reason: Option<String>,
    pub configured_blob_roots: usize,
    pub writable_blob_roots: usize,
    pub distinct_writable_blob_devices: usize,
    pub writable_index_roots: usize,
    pub root_identity_error: Option<String>,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub last_topology_check_at: Option<i64>,
    pub last_availability_scan_at: Option<i64>,
    pub last_checksum_scrub_at: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TopologyAssessment {
    pub configured_blob_roots: usize,
    pub writable_blob_roots: usize,
    pub distinct_writable_blob_devices: usize,
    pub writable_index_roots: usize,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub write_safe: bool,
    pub reason: Option<String>,
    pub root_identity_error: Option<String>,
}

#[derive(Debug)]
struct RootProbe {
    writable: bool,
    device_id: Option<u64>,
}

static AUTO_HEALING_STATUS: LazyLock<Arc<RwLock<AutoHealingStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(AutoHealingStatus::default())));

pub fn arm_topology_safety_gate() {
    set_safety_read_only(Some(
        "N1 is read-only while the configured EC topology is being validated.".to_string(),
    ));
}

pub async fn get_auto_healing_status() -> AutoHealingStatus {
    AUTO_HEALING_STATUS.read().await.clone()
}

#[cfg(unix)]
fn root_device_id(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.dev()
}

#[cfg(not(unix))]
fn root_device_id(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}

fn probe_writable_root(path: &Path, expected: &BTreeMap<String, String>) -> RootProbe {
    if !root_matches_expected_identity(path, expected) {
        return RootProbe {
            writable: false,
            device_id: None,
        };
    }
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => metadata,
        _ => {
            return RootProbe {
                writable: false,
                device_id: None,
            };
        }
    };

    let probe_path = path.join(format!(".n1-topology-probe-{}", Ulid::new()));
    let result = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&probe_path)?;
        file.write_all(b"n1")?;
        file.flush()?;
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = [0u8; 2];
        file.read_exact(&mut bytes)?;
        if &bytes != b"n1" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "topology probe read mismatch",
            ));
        }
        drop(file);
        std::fs::remove_file(&probe_path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&probe_path);
    }

    RootProbe {
        writable: result.is_ok(),
        device_id: result.is_ok().then(|| root_device_id(&metadata)),
    }
}

fn evaluate_topology_counts(
    configured_blob_roots: usize,
    writable_blob_roots: usize,
    distinct_writable_blob_devices: usize,
    writable_index_roots: usize,
    data_shards: usize,
    parity_shards: usize,
) -> TopologyAssessment {
    let required = data_shards.saturating_add(parity_shards);
    let reason = if data_shards == 0 || required == 0 {
        Some("N1 has an invalid zero-width EC configuration.".to_string())
    } else if writable_index_roots == 0 {
        Some("N1 has no writable index root; namespace mutations are unsafe.".to_string())
    } else if configured_blob_roots < required {
        Some(format!(
            "N1 is read-only: {}D+{}P requires {} blob roots, but only {} are configured.",
            data_shards, parity_shards, required, configured_blob_roots
        ))
    } else if writable_blob_roots < required {
        Some(format!(
            "N1 is read-only: {}D+{}P requires {} writable blob roots, but only {} are writable.",
            data_shards, parity_shards, required, writable_blob_roots
        ))
    } else if distinct_writable_blob_devices < required {
        Some(format!(
            "N1 is read-only: {}D+{}P requires {} distinct writable devices, but only {} are available.",
            data_shards, parity_shards, required, distinct_writable_blob_devices
        ))
    } else {
        None
    };

    TopologyAssessment {
        configured_blob_roots,
        writable_blob_roots,
        distinct_writable_blob_devices,
        writable_index_roots,
        data_shards,
        parity_shards,
        write_safe: reason.is_none(),
        reason,
        root_identity_error: None,
    }
}

pub async fn assess_current_topology() -> TopologyAssessment {
    let blob_roots = BLOB_STORE_DISKS.get().cloned().unwrap_or_default();
    let index_roots = INDEX_STORE_DISKS.get().cloned().unwrap_or_default();
    let data_shards = DATA_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(std::sync::atomic::Ordering::Acquire))
        .unwrap_or(0);
    let parity_shards = PARITY_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(std::sync::atomic::Ordering::Acquire))
        .unwrap_or(0);

    let identities = expected_root_identities();
    let identity_error = identities.as_ref().err().cloned();
    let identities = Arc::new(identities.unwrap_or_default());

    async fn probe_paths(
        paths: Vec<String>,
        identities: Arc<BTreeMap<String, String>>,
    ) -> Vec<RootProbe> {
        stream::iter(paths)
            .map(|path| {
                let identities = identities.clone();
                async move {
                    tokio::task::spawn_blocking(move || {
                        probe_writable_root(Path::new(&path), identities.as_ref())
                    })
                    .await
                    .unwrap_or(RootProbe {
                        writable: false,
                        device_id: None,
                    })
                }
            })
            .buffer_unordered(8)
            .collect()
            .await
    }

    let configured_blob_roots = blob_roots.len();
    let (blob_probes, index_probes) = tokio::join!(
        probe_paths(blob_roots, identities.clone()),
        probe_paths(index_roots, identities)
    );
    let writable_blob_roots = blob_probes.iter().filter(|probe| probe.writable).count();
    let distinct_writable_blob_devices = blob_probes
        .iter()
        .filter(|probe| probe.writable)
        .filter_map(|probe| probe.device_id)
        .collect::<HashSet<_>>()
        .len();
    let writable_index_roots = index_probes.iter().filter(|probe| probe.writable).count();

    let mut assessment = evaluate_topology_counts(
        configured_blob_roots,
        writable_blob_roots,
        distinct_writable_blob_devices,
        writable_index_roots,
        data_shards,
        parity_shards,
    );
    if let Some(error) = identity_error {
        assessment.write_safe = false;
        assessment.reason = Some(format!(
            "N1 is read-only because root identity authority is unavailable: {error}"
        ));
        assessment.root_identity_error = Some(error);
    }
    assessment
}

async fn update_topology_status(assessment: &TopologyAssessment) {
    let mut status = AUTO_HEALING_STATUS.write().await;
    status.enabled = CONFIG.auto_healing.enabled;
    status.write_safe = assessment.write_safe;
    status.read_only_reason = safety_read_only_reason();
    status.configured_blob_roots = assessment.configured_blob_roots;
    status.writable_blob_roots = assessment.writable_blob_roots;
    status.distinct_writable_blob_devices = assessment.distinct_writable_blob_devices;
    status.writable_index_roots = assessment.writable_index_roots;
    status.root_identity_error = assessment.root_identity_error.clone();
    status.data_shards = assessment.data_shards;
    status.parity_shards = assessment.parity_shards;
    status.last_topology_check_at = Some(Utc::now().timestamp());
    if !assessment.write_safe {
        status.phase = "read_only_topology_unsafe".to_string();
    } else if !CONFIG.auto_healing.enabled {
        status.phase = "topology_safe_scans_disabled".to_string();
    } else if status.phase != "scan_failed" && !status.phase.starts_with("running_") {
        status.phase = "idle".to_string();
    }
}

async fn maintenance_is_busy() -> bool {
    get_repair_status().await.is_running
        || get_redist_status().await.is_running
        || get_gc_status().await.is_running
}

async fn start_automatic_scan(check_mode: RepairCheckMode, state: Arc<AppState>) {
    let label = match check_mode {
        RepairCheckMode::Availability => "availability_scan",
        _ => "checksum_scrub",
    };
    {
        let mut status = AUTO_HEALING_STATUS.write().await;
        status.phase = format!("running_{label}");
        status.last_error = None;
    }

    let cfg = &CONFIG.auto_healing;
    let run_cfg = RepairRunConfig {
        dry_run: false,
        target_fragment: None,
        check_mode,
        meta_concurrency: cfg.object_scan_concurrency,
        shard_repair_concurrency: cfg.shard_repair_concurrency,
        meta_batch_size: cfg.metadata_batch_size,
    };
    let access_mode = check_mode.maintenance_access_mode();
    let result = run_repair_job_with_options(
        run_cfg,
        RepairRunOptions {
            fast_mode: false,
            access_mode,
            replay_pending_redistribution: false,
        },
        Arc::new(state.ws_manager.clone()),
    )
    .await;

    let mut status = AUTO_HEALING_STATUS.write().await;
    match result {
        Ok(()) => {
            set_auto_healing_fault(None);
            let now = Utc::now().timestamp();
            if check_mode == RepairCheckMode::Availability {
                status.last_availability_scan_at = Some(now);
            } else {
                status.last_checksum_scrub_at = Some(now);
            }
            status.phase = if status.write_safe {
                "idle".to_string()
            } else {
                "read_only_topology_unsafe".to_string()
            };
        }
        Err(error) => {
            let message = error.to_string();
            warn!("Automatic N1 {label} failed: {message}");
            status.phase = "scan_failed".to_string();
            status.last_error = Some(message);
            if !matches!(error, crate::enums::app_error_enum::AppError::Conflict(_)) {
                set_auto_healing_fault(Some(format!(
                    "N1 is read-only because automatic {label} failed; the controller will retry: {}",
                    status.last_error.as_deref().unwrap_or("unknown failure")
                )));
            }
        }
    }
}

pub async fn run_auto_healing_controller(state: Arc<AppState>) {
    if BLOB_STORE_DISKS.get().is_none()
        || INDEX_STORE_DISKS.get().is_none()
        || DATA_SHARDS_ATOMIC.get().is_none()
        || PARITY_SHARDS_ATOMIC.get().is_none()
    {
        warn!(
            "N1 auto-healing controller was not started because storage topology is uninitialized"
        );
        return;
    }

    let cfg = CONFIG.auto_healing.clone();
    let topology_interval = Duration::from_secs(cfg.topology_check_interval_seconds.max(1));
    let mut availability_due = Instant::now() + Duration::from_secs(cfg.initial_scan_delay_seconds);
    let mut checksum_due =
        Instant::now() + Duration::from_secs(cfg.checksum_scrub_interval_seconds);
    let mut safe_since: Option<Instant> = None;

    info!(
        "N1 auto-healing controller started: enabled={} topology={}s availability={}s checksum={}s object_concurrency={} repair_concurrency={}",
        cfg.enabled,
        topology_interval.as_secs(),
        cfg.availability_scan_interval_seconds,
        cfg.checksum_scrub_interval_seconds,
        cfg.object_scan_concurrency,
        cfg.shard_repair_concurrency
    );

    loop {
        let assessment = assess_current_topology().await;
        if assessment.write_safe {
            let became_safe_at = safe_since.get_or_insert_with(Instant::now);
            if became_safe_at.elapsed()
                >= Duration::from_secs(cfg.topology_recovery_stability_seconds)
            {
                if topology_read_only_reason().is_some() {
                    info!("N1 topology is stable and write-safe; leaving automatic read-only mode");
                }
                set_safety_read_only(None);
            }
        } else {
            safe_since = None;
            if topology_read_only_reason() != assessment.reason {
                error!(
                    "N1 entered automatic read-only mode: {}",
                    assessment.reason.as_deref().unwrap_or("topology unsafe")
                );
            }
            set_safety_read_only(assessment.reason.clone());
        }
        update_topology_status(&assessment).await;

        if AUTO_HEALING_STATUS.read().await.phase == "scan_failed" {
            availability_due = Instant::now();
        }

        if cfg.enabled && !maintenance_is_busy().await {
            let now = Instant::now();
            let mode = if now >= checksum_due {
                checksum_due = now + Duration::from_secs(cfg.checksum_scrub_interval_seconds);
                availability_due =
                    now + Duration::from_secs(cfg.availability_scan_interval_seconds);
                Some(RepairCheckMode::Checksum)
            } else if now >= availability_due {
                availability_due =
                    now + Duration::from_secs(cfg.availability_scan_interval_seconds);
                Some(RepairCheckMode::Availability)
            } else {
                None
            };

            if let Some(mode) = mode {
                let scan_state = Arc::clone(&state);
                tokio::spawn(async move {
                    start_automatic_scan(mode, scan_state).await;
                });
            }
        }

        let next_check = if assessment.write_safe {
            safe_since
                .map(|since| {
                    Duration::from_secs(cfg.topology_recovery_stability_seconds)
                        .saturating_sub(since.elapsed())
                        .min(topology_interval)
                })
                .filter(|remaining| !remaining.is_zero())
                .unwrap_or(topology_interval)
        } else {
            topology_interval
        };
        tokio::time::sleep(next_check).await;
    }
}

#[cfg(test)]
mod tests {
    use super::evaluate_topology_counts;

    #[test]
    fn expanded_topology_stays_write_safe_without_changing_ec_width() {
        let assessment = evaluate_topology_counts(8, 8, 8, 2, 4, 2);
        assert!(assessment.write_safe);
        assert_eq!(assessment.data_shards, 4);
        assert_eq!(assessment.parity_shards, 2);
    }

    #[test]
    fn insufficient_or_non_distinct_roots_fail_closed() {
        let missing = evaluate_topology_counts(6, 5, 5, 2, 4, 2);
        assert!(!missing.write_safe);
        assert!(
            missing
                .reason
                .as_deref()
                .unwrap()
                .contains("writable blob roots")
        );

        let duplicate_devices = evaluate_topology_counts(6, 6, 5, 2, 4, 2);
        assert!(!duplicate_devices.write_safe);
        assert!(
            duplicate_devices
                .reason
                .as_deref()
                .unwrap()
                .contains("distinct writable devices")
        );
    }

    #[test]
    fn index_authority_loss_fails_closed() {
        let assessment = evaluate_topology_counts(6, 6, 6, 0, 4, 2);
        assert!(!assessment.write_safe);
        assert!(assessment.reason.as_deref().unwrap().contains("index root"));
    }
}
