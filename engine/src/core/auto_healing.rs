use crate::core::disk_manager::{BLOB_STORE_DISKS, INDEX_STORE_DISKS};
use crate::core::operation_gate::{
    safety_read_only_reason, set_safety_read_only, topology_read_only_reason,
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
    pub configured_index_roots: usize,
    pub writable_index_roots: usize,
    pub root_identity_error: Option<String>,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub last_topology_check_at: Option<i64>,
    pub last_availability_scan_at: Option<i64>,
    pub last_checksum_scrub_at: Option<i64>,
    pub last_error: Option<String>,
}

impl AutoHealingStatus {
    fn with_safety_gate(mut self, reason: Option<String>) -> Self {
        // Topology probes are periodic; the mutation gate can change between
        // probes when an online repair completes. Never report its old state.
        self.write_safe &= reason.is_none();
        self.read_only_reason = reason;
        self
    }
}

#[derive(Debug, Clone)]
pub struct TopologyAssessment {
    pub configured_blob_roots: usize,
    pub writable_blob_roots: usize,
    pub distinct_writable_blob_devices: usize,
    pub configured_index_roots: usize,
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
    AUTO_HEALING_STATUS
        .read()
        .await
        .clone()
        .with_safety_gate(safety_read_only_reason())
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
    configured_index_roots: usize,
    writable_index_roots: usize,
    data_shards: usize,
    parity_shards: usize,
) -> TopologyAssessment {
    let required = data_shards.saturating_add(parity_shards);
    let reason = if data_shards == 0 || required == 0 {
        Some("N1 has an invalid zero-width EC configuration.".to_string())
    } else if configured_index_roots == 0 {
        Some("N1 has no configured index root; namespace mutations are unsafe.".to_string())
    } else if writable_index_roots != configured_index_roots {
        Some(format!(
            "N1 is read-only: namespace authority requires all {} configured index roots, but only {} are writable and identity-valid.",
            configured_index_roots, writable_index_roots
        ))
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
        configured_index_roots,
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
            .buffered(8)
            .collect()
            .await
    }

    let configured_blob_roots = blob_roots.len();
    let configured_index_roots = index_roots.len();
    let (blob_probes, index_probes) = tokio::join!(
        probe_paths(blob_roots.clone(), identities.clone()),
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
        configured_index_roots,
        writable_index_roots,
        data_shards,
        parity_shards,
    );
    if !crate::core::storage_pool::configured().is_empty() {
        // A degraded old pool must not become a global namespace writer gate.
        // New uploads need one complete pool; mutations enforce all exact
        // holders of their own home pool inside the per-key coordinator.
        let writable_pool = crate::core::storage_pool::configured()
            .iter()
            .rev()
            .find(|pool| {
                let probes = pool
                    .roots
                    .iter()
                    .filter_map(|root| {
                        blob_roots
                            .iter()
                            .position(|path| Path::new(path) == root)
                            .map(|index| &blob_probes[index])
                    })
                    .collect::<Vec<_>>();
                !pool.sealed
                    && probes.len() == pool.width()
                    && probes.iter().all(|p| p.writable)
                    && probes
                        .iter()
                        .filter_map(|p| p.device_id)
                        .collect::<HashSet<_>>()
                        .len()
                        == pool.width()
            });
        assessment.write_safe = writable_pool.is_some();
        assessment.reason = writable_pool
            .is_none()
            .then(|| "N1 is read-only: no complete writable storage pool is available".into());
        if let Some(pool) = writable_pool {
            assessment.data_shards = usize::from(pool.data_shards);
            assessment.parity_shards = usize::from(pool.parity_shards);
        }
    }
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
    // This legacy status field describes automatic full scans, not read repair.
    status.enabled = false;
    status.write_safe = assessment.write_safe;
    status.read_only_reason = safety_read_only_reason();
    status.configured_blob_roots = assessment.configured_blob_roots;
    status.writable_blob_roots = assessment.writable_blob_roots;
    status.distinct_writable_blob_devices = assessment.distinct_writable_blob_devices;
    status.configured_index_roots = assessment.configured_index_roots;
    status.writable_index_roots = assessment.writable_index_roots;
    status.root_identity_error = assessment.root_identity_error.clone();
    status.data_shards = assessment.data_shards;
    status.parity_shards = assessment.parity_shards;
    status.last_topology_check_at = Some(Utc::now().timestamp());
    if !assessment.write_safe {
        status.phase = "read_only_topology_unsafe".to_string();
    } else {
        status.phase = "on_read_only".to_string();
    }
}

pub async fn run_auto_healing_controller(_state: Arc<AppState>) {
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
    let mut safe_since: Option<Instant> = None;

    info!(
        "N1 read-triggered repair enabled; full object scans require an explicit repair request; member identity probe interval={}s",
        topology_interval.as_secs()
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
    use super::{AutoHealingStatus, evaluate_topology_counts};

    #[test]
    fn newly_failed_scan_overrides_previously_write_safe_status() {
        let previous = AutoHealingStatus {
            write_safe: true,
            phase: "scan_failed".into(),
            ..Default::default()
        };
        let status = previous.with_safety_gate(Some("automatic availability_scan failed".into()));
        assert!(!status.write_safe);
        assert_eq!(
            status.read_only_reason.as_deref(),
            Some("automatic availability_scan failed")
        );
    }

    #[test]
    fn successful_retry_removes_stale_reason_without_waiting_for_topology_poll() {
        let previous = AutoHealingStatus {
            write_safe: true,
            read_only_reason: Some("previous scan failure".into()),
            ..Default::default()
        };
        let status = previous.with_safety_gate(None);
        assert!(status.write_safe);
        assert!(status.read_only_reason.is_none());
    }

    #[test]
    fn clearing_scan_fault_does_not_make_unsafe_topology_write_safe() {
        let status = AutoHealingStatus::default().with_safety_gate(None);
        assert!(!status.write_safe);
    }

    #[test]
    fn expanded_topology_stays_write_safe_without_changing_ec_width() {
        let assessment = evaluate_topology_counts(8, 8, 8, 2, 2, 4, 2);
        assert!(assessment.write_safe);
        assert_eq!(assessment.data_shards, 4);
        assert_eq!(assessment.parity_shards, 2);
    }

    #[test]
    fn insufficient_or_non_distinct_roots_fail_closed() {
        let missing = evaluate_topology_counts(6, 5, 5, 2, 2, 4, 2);
        assert!(!missing.write_safe);
        assert!(
            missing
                .reason
                .as_deref()
                .unwrap()
                .contains("writable blob roots")
        );

        let duplicate_devices = evaluate_topology_counts(6, 6, 5, 2, 2, 4, 2);
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
        let assessment = evaluate_topology_counts(6, 6, 6, 2, 1, 4, 2);
        assert!(!assessment.write_safe);
        assert!(assessment.reason.as_deref().unwrap().contains("index root"));
    }

    #[test]
    fn every_configured_index_root_is_required_for_namespace_authority() {
        let assessment = evaluate_topology_counts(4, 4, 4, 4, 3, 3, 1);
        assert!(!assessment.write_safe);
        assert_eq!(assessment.configured_index_roots, 4);
        assert_eq!(assessment.writable_index_roots, 3);
        assert!(assessment.reason.as_deref().unwrap().contains("all 4"));
    }
}
