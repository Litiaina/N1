//! On-demand, read-only pool diagnostics. No persistent cache or upload-path
//! work. Identity/readiness and live content verification are separate claims.
use super::{
    fragment_identity::physical_fragment_root_on,
    index_store::FragmentMeta,
    storage_admin,
    storage_pool::{self, StoragePool},
};
use crate::{enums::app_error_enum::AppError, structs::auth_structs::Claims};
use axum::{Json, extract::Query};
use serde::Serialize;
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, OpenOptions},
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
    sync::Arc,
};
use tokio::sync::Semaphore;

static CONTENT_SCAN: std::sync::LazyLock<Arc<Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(Semaphore::new(1)));

#[derive(Serialize)]
pub struct PoolHealthResponse {
    pub sampled_at: i64,
    pub fragment: String,
    pub allocation_policy: &'static str,
    pub pools: Vec<PoolHealth>,
}
#[derive(Serialize)]
pub struct PoolHealth {
    pub id: String,
    pub generation: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub namespace_replicas: usize,
    pub sealed: bool,
    pub current_write_pool: bool,
    pub readiness: String,
    pub members: Vec<MemberHealth>,
    pub estimated_data_available_bytes: Option<u64>,
    pub fragment_metadata_replicas: usize,
    pub fragment_state: String,
    pub integrity: Integrity,
}
#[derive(Serialize)]
pub struct MemberHealth {
    pub slot: usize,
    pub member_id: String,
    pub path: String,
    pub status: String,
    pub available_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub total_inodes: Option<u64>,
    pub available_inodes: Option<u64>,
    pub capacity_warning: Option<String>,
    pub error: Option<String>,
}
#[derive(Default, Serialize)]
pub struct Integrity {
    pub status: String,
    pub scope: &'static str,
    pub files_scanned: u64,
    pub versions_checked: u64,
    pub ncx_replicas_checked: u64,
    pub elapsed_ms: u64,
    pub error: Option<String>,
}

fn fragment_bytes(root: &Path, fragment: &str) -> Result<Vec<u8>, String> {
    let path = physical_fragment_root_on(root, fragment).join(".fragment.ndx");
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| e.to_string())?;
    if !file.metadata().map_err(|e| e.to_string())?.is_file() {
        return Err("fragment metadata is not a regular file".into());
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > 1024 * 1024 {
        return Err("fragment metadata exceeds limit".into());
    }
    let meta: FragmentMeta = rmp_serde::from_slice(&bytes).map_err(|e| e.to_string())?;
    if meta.engine_format_version != 4
        || meta.stable_fragment_id != fragment
        || meta.state != "active"
    {
        return Err("fragment identity/format/state mismatch".into());
    }
    Ok(bytes)
}

fn quick_pool(pool: &StoragePool, fragment: &str, write_pool: Option<&str>) -> PoolHealth {
    let mut devices = BTreeSet::new();
    let mut canonical_fragment = None;
    let mut fragment_replicas = 0;
    let mut divergent_fragment = false;
    let members: Vec<_> = pool
        .roots
        .iter()
        .enumerate()
        .map(|(slot, root)| {
            let identity = (|| -> Result<(), String> {
                let meta = fs::symlink_metadata(root).map_err(|e| e.to_string())?;
                if !meta.is_dir() || !devices.insert(meta.dev()) {
                    return Err("member is not an independent filesystem directory".into());
                }
                if root
                    .join(super::instance_lease::RESTORE_PENDING)
                    .try_exists()
                    .map_err(|e| e.to_string())?
                {
                    return Err("member has unfinished offline maintenance".into());
                }
                let member = storage_pool::read_member(root)?
                    .ok_or("missing self-identifying member descriptor")?;
                if !member.pool.same_geometry(pool)
                    || member.member_slot != slot
                    || member.pool.members != pool.members
                    || member.member_id != pool.members[slot]
                    || member.pool.address_roots() != pool.address_roots()
                {
                    return Err("member descriptor differs from enrolled identity".into());
                }
                if !storage_pool::root_available(root) {
                    return Err("member root identity is unavailable".into());
                }
                Ok(())
            })();
            if identity.is_ok() {
                if let Ok(bytes) = fragment_bytes(root, fragment) {
                    fragment_replicas += 1;
                    if let Some(old) = canonical_fragment.as_ref() {
                        divergent_fragment |= old != &bytes;
                    } else {
                        canonical_fragment = Some(bytes);
                    }
                }
            }
            let writable =
                identity.is_ok() && rustix::fs::access(root, rustix::fs::Access::WRITE_OK).is_ok();
            let capacity = identity
                .is_ok()
                .then(|| super::monitoring::disk_capacity_snapshot(root))
                .flatten();
            MemberHealth {
                slot,
                member_id: pool.members.get(slot).cloned().unwrap_or_default(),
                path: root.display().to_string(),
                status: if identity.is_err() {
                    "unavailable"
                } else if !writable {
                    "read_only"
                } else {
                    "ready"
                }
                .into(),
                available_bytes: capacity.map(|c| c.available_bytes),
                total_bytes: capacity.map(|c| c.total_bytes),
                total_inodes: capacity.and_then(|c| c.total_inodes),
                available_inodes: capacity.and_then(|c| c.available_inodes),
                capacity_warning: capacity.and_then(capacity_warning),
                error: identity.err(),
            }
        })
        .collect();
    let available = members.iter().filter(|m| m.status != "unavailable").count();
    let all_writable = members.iter().all(|m| m.status == "ready");
    let estimated = members
        .iter()
        .map(|m| m.available_bytes)
        .collect::<Option<Vec<_>>>()
        .and_then(|v| v.into_iter().min())
        .map(|bytes| bytes.saturating_mul(u64::from(pool.data_shards)));
    PoolHealth {
        id: pool.id.clone(),
        generation: pool.generation,
        data_shards: pool.data_shards,
        parity_shards: pool.parity_shards,
        namespace_replicas: pool.namespace_replica_count(),
        sealed: pool.sealed,
        current_write_pool: write_pool == Some(pool.id.as_str()),
        readiness: if available < usize::from(pool.data_shards) {
            "critical"
        } else if available != pool.width() {
            "degraded"
        } else if !all_writable {
            "read_only"
        } else {
            "ready"
        }
        .into(),
        members,
        estimated_data_available_bytes: estimated,
        fragment_metadata_replicas: fragment_replicas,
        fragment_state: if divergent_fragment {
            "divergent"
        } else if fragment_replicas == 0 {
            "absent_or_invalid"
        } else if fragment_replicas != pool.width() {
            "degraded"
        } else {
            "ready"
        }
        .into(),
        integrity: Integrity {
            status: "not_scanned".into(),
            scope: "selected_fragment_live_not_snapshot",
            ..Default::default()
        },
    }
}

fn capacity_warning(capacity: super::monitoring::DiskCapacitySnapshot) -> Option<String> {
    if capacity.available_inodes == Some(0) || capacity.available_bytes == 0 {
        Some("Storage exhausted: uploads and NCX mutations can fail; add capacity and preserve metadata headroom".into())
    } else if (capacity.available_bytes as u128) * 100 < (capacity.total_bytes as u128) * 5
        || capacity.available_inodes.is_some_and(|n| n < 1024)
    {
        Some("Low storage headroom: expand/seal before exhaustion; old-pool NCX mutations still need space".into())
    } else {
        None
    }
}

pub async fn snapshot(
    claims: Claims,
    params: HashMap<String, String>,
) -> Result<PoolHealthResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let fragment = super::fragment_metadata::resolve_stable_fragment_id(&claims.fragment).await?;
    if let Some(requested) = params.get("fragment") {
        let requested = super::fragment_metadata::resolve_stable_fragment_id(requested).await?;
        if requested != fragment {
            return Err(AppError::Forbidden(
                "Pool content diagnostics are scoped to the authenticated fragment".into(),
            ));
        }
    }
    let selected = params.get("pool").filter(|id| !id.is_empty());
    if let Some(id) = selected {
        storage_pool::by_id(id)?;
    }
    let pools = storage_pool::configured()
        .iter()
        .filter(|p| selected.is_none_or(|id| id == &p.id))
        .cloned()
        .collect::<Vec<_>>();
    let write_pool = storage_pool::write_pool().ok().map(|pool| pool.id.clone());
    let deep = params.get("deep_scan").is_some_and(|v| v == "true");
    let permit = if deep {
        Some(CONTENT_SCAN.clone().try_acquire_owned().map_err(|_| {
            AppError::ServiceUnavailable(
                "a pool integrity scan is already running; retry after it finishes".into(),
            )
        })?)
    } else {
        None
    };
    // The worker owns the permit through completion even if HTTP is cancelled.
    // Metadata/pieces are inspected only; no repair, mkdir, or identity promotion.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut results = Vec::new();
        for pool in pools {
            let mut result = quick_pool(&pool, &fragment, write_pool.as_deref());
            if deep {
                if result.readiness == "degraded" || result.readiness == "critical" || result.fragment_state != "ready" {
                    result.integrity.status = "unavailable".into();
                    result.integrity.error = Some("complete accepted members and matching fragment metadata are required for full verification".into());
                } else {
                    let started = std::time::Instant::now();
                    let mut report = storage_admin::Report::default();
                    let scan = storage_admin::verify_pool_scoped(&pool, None, Some(&fragment), &mut report);
                    result.integrity.status = if scan.is_ok() { "passed" } else { "failed" }.into();
                    result.integrity.error = scan.err().map(|e| e.to_string());
                    result.integrity.files_scanned = report.files;
                    result.integrity.versions_checked = report.versions;
                    result.integrity.ncx_replicas_checked = report.pointers;
                    result.integrity.elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                    // Do not certify a member changed/removed during the scan.
                    let after = quick_pool(&pool, &fragment, write_pool.as_deref());
                    if after.members.iter().any(|m| m.status == "unavailable") || after.fragment_state != "ready" {
                        result.integrity.status = "unavailable".into();
                        result.integrity.error = Some("member/fragment readiness changed during verification".into());
                    }
                }
            }
            results.push(result);
        }
        PoolHealthResponse { sampled_at: chrono::Utc::now().timestamp(), fragment, allocation_policy: "newest_complete_unsealed_pool_no_free_space_balancing", pools: results }
    }).await.map_err(|e| AppError::InternalServerError(format!("pool health worker: {e}")))
}

pub async fn get_pool_health(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<PoolHealthResponse>, AppError> {
    Ok(Json(snapshot(claims, params).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_warning_reports_headroom_without_certifying_integrity() {
        let mut capacity = super::super::monitoring::DiskCapacitySnapshot {
            total_bytes: 1_000_000,
            used_bytes: 100_000,
            available_bytes: 900_000,
            usage_percent: 10.0,
            total_inodes: Some(100_000),
            available_inodes: Some(90_000),
        };
        assert!(capacity_warning(capacity).is_none());
        capacity.available_inodes = None; // Some filesystems don't expose inode capacity.
        assert!(capacity_warning(capacity).is_none());
        capacity.available_inodes = Some(500);
        assert!(
            capacity_warning(capacity)
                .unwrap()
                .contains("Low storage headroom")
        );
        capacity.available_inodes = Some(0);
        assert!(
            capacity_warning(capacity)
                .unwrap()
                .contains("Storage exhausted")
        );
        capacity.available_inodes = None;
        capacity.available_bytes = 49_999;
        assert!(
            capacity_warning(capacity)
                .unwrap()
                .contains("Low storage headroom")
        );
        capacity.available_bytes = 0;
        assert!(
            capacity_warning(capacity)
                .unwrap()
                .contains("Storage exhausted")
        );
    }

    struct Fixture {
        pool: StoragePool,
        fragment: String,
    }
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("n1-pool-health-{}", ulid::Ulid::new()));
            fs::create_dir_all(&root).unwrap();
            let pool = StoragePool {
                id: "A".into(),
                generation: 1,
                roots: vec![root],
                members: vec![ulid::Ulid::new().to_string()],
                legacy_roots: vec![],
                data_shards: 1,
                parity_shards: 0,
                sealed: false,
            };
            storage_pool::write_member(&pool, 0).unwrap();
            let fragment = ulid::Ulid::new().to_string();
            let physical = physical_fragment_root_on(&pool.roots[0], &fragment);
            fs::create_dir_all(&physical).unwrap();
            let meta = FragmentMeta {
                engine_format_version: 4,
                stable_fragment_id: fragment.clone(),
                state: "active".into(),
                committed_bytes: 0,
                reserved_bytes: 0,
                trash_bytes: 0,
                created_at_unix: 1,
                updated_at_unix: 1,
                key_phc: None,
                nt_hash_hex: None,
                index_partition_count: 64,
            };
            fs::write(
                physical.join(".fragment.ndx"),
                rmp_serde::to_vec_named(&meta).unwrap(),
            )
            .unwrap();
            Self { pool, fragment }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.pool.roots[0]);
        }
    }

    #[test]
    fn descriptor_and_fragment_readiness_do_not_claim_content_integrity() {
        let fixture = Fixture::new();
        let result = quick_pool(&fixture.pool, &fixture.fragment, Some("A"));
        assert_eq!(result.readiness, "ready");
        assert_eq!(result.fragment_state, "ready");
        assert!(result.current_write_pool);
        assert_eq!(result.integrity.status, "not_scanned");
        assert_eq!(result.integrity.versions_checked, 0);
        assert_eq!(
            result.estimated_data_available_bytes,
            result.members[0].available_bytes
        );
        // Reports contain identity/coverage, never fragment credentials.
        let json = serde_json::to_string(&result).unwrap();
        assert!(!json.contains("key_phc"));
        assert!(!json.contains("nt_hash_hex"));
        // Native diagnostics must use numeric MessagePack fields, not u128's
        // binary encoding (which JSON-valued consumers cannot decode).
        let native: serde_json::Value =
            rmp_serde::from_slice(&rmp_serde::to_vec_named(&result).unwrap()).unwrap();
        assert_eq!(native["integrity"]["elapsed_ms"], 0);
    }

    #[test]
    fn unfinished_or_missing_member_is_not_promoted_by_diagnostics() {
        let fixture = Fixture::new();
        let root = &fixture.pool.roots[0];
        fs::write(
            root.join(super::super::instance_lease::RESTORE_PENDING),
            b"pending",
        )
        .unwrap();
        let result = quick_pool(&fixture.pool, &fixture.fragment, None);
        assert_eq!(result.readiness, "critical");
        assert!(result.members[0].available_bytes.is_none());
        assert!(
            root.join(super::super::instance_lease::RESTORE_PENDING)
                .exists()
        );
        fs::remove_file(root.join(super::super::instance_lease::RESTORE_PENDING)).unwrap();
        fs::remove_file(root.join(".n1-pool.json")).unwrap();
        assert_eq!(
            quick_pool(&fixture.pool, &fixture.fragment, None).readiness,
            "critical"
        );
        assert!(!root.join(".n1-pool.json").exists());
    }

    #[test]
    fn fragment_scope_excludes_other_fragments_but_never_passes_absent_fragment() {
        let fixture = Fixture::new();
        let other = ulid::Ulid::new().to_string();
        let dir = physical_fragment_root_on(&fixture.pool.roots[0], &other);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(".fragment.ndx"), b"corrupt other fragment").unwrap();
        let mut report = storage_admin::Report::default();
        storage_admin::verify_pool_scoped(
            &fixture.pool,
            None,
            Some(&fixture.fragment),
            &mut report,
        )
        .unwrap();
        assert_eq!(report.files, 1);
        assert!(storage_admin::verify_pool_scoped(&fixture.pool, None, None, &mut report).is_err());
        assert!(
            storage_admin::verify_pool_scoped(
                &fixture.pool,
                None,
                Some("../../escape"),
                &mut report
            )
            .is_err()
        );
        assert!(
            storage_admin::verify_pool_scoped(
                &fixture.pool,
                None,
                Some(&ulid::Ulid::new().to_string()),
                &mut report
            )
            .is_err()
        );
    }

    #[test]
    fn pool_results_do_not_inherit_another_pools_failure() {
        let healthy = Fixture::new();
        let failed = Fixture::new();
        fs::remove_file(failed.pool.roots[0].join(".n1-pool.json")).unwrap();
        assert_eq!(
            quick_pool(&healthy.pool, &healthy.fragment, None).readiness,
            "ready"
        );
        assert_eq!(
            quick_pool(&failed.pool, &failed.fragment, None).readiness,
            "critical"
        );
    }
}
