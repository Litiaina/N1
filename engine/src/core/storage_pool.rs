//! Immutable, independently expandable N1 4 storage pools.
//!
//! A pool is one EC failure domain. Adding a pool never changes a previously
//! enrolled pool's members, namespace replica placement, or object geometry.

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use ini::Ini;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::enums::app_error_enum::AppError;

pub static STORAGE_POOLS: OnceLock<Vec<StoragePool>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberDescriptor {
    pub member_format: String,
    pub member_slot: usize,
    pub member_id: String,
    #[serde(flatten)]
    pub pool: StoragePool,
}

pub fn read_member(root: &Path) -> Result<Option<MemberDescriptor>, String> {
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(root.join(".n1-pool.json"))
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read member descriptor: {e}")),
    };
    let metadata = file.metadata().map_err(|e| e.to_string())?;
    if !metadata.is_file() || metadata.len() > 1024 * 1024 {
        return Err("invalid/oversized pool descriptor".into());
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > 1024 * 1024 {
        return Err("pool descriptor grew beyond its limit".into());
    }
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("invalid pool descriptor: {e}"))?;
    if value.get("member_format").is_none() {
        return Ok(None);
    }
    let descriptor: MemberDescriptor =
        serde_json::from_value(value).map_err(|e| format!("invalid member descriptor: {e}"))?;
    if descriptor.member_format != "n1-pool-member-v1"
        || descriptor.member_slot >= descriptor.pool.width()
        || descriptor.pool.members.len() != descriptor.pool.width()
        || descriptor.member_id.is_empty()
        || descriptor.pool.members[descriptor.member_slot] != descriptor.member_id
        || descriptor.pool.roots.len() != descriptor.pool.width()
        || (!descriptor.pool.legacy_roots.is_empty()
            && descriptor.pool.legacy_roots.len() != descriptor.pool.width())
    {
        return Err("invalid self-identifying pool member".into());
    }
    Ok(Some(descriptor))
}

pub fn write_member(pool: &StoragePool, slot: usize) -> Result<(), String> {
    let mut identity = pool.clone();
    identity.roots = pool.address_roots().to_vec();
    identity.sealed = false;
    let descriptor = MemberDescriptor {
        member_format: "n1-pool-member-v1".into(),
        member_slot: slot,
        member_id: pool
            .members
            .get(slot)
            .ok_or("member identity is not enrolled")?
            .clone(),
        pool: identity,
    };
    let path = pool.roots[slot].join(".n1-pool.json");
    let bytes = serde_json::to_vec(&descriptor).map_err(|e| e.to_string())?;
    if fs::read(&path).ok().as_deref() != Some(bytes.as_slice()) {
        crate::core::index_store::write_pool_identity(&path, &bytes).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Bind candidate locations by descriptors. Paths are discovery hints, never
/// membership. This reads only O(member count) small topology records.
pub fn bind_locations(
    mut pools: Vec<StoragePool>,
    control: &Path,
) -> Result<Vec<StoragePool>, String> {
    let enrolled: Vec<StoragePool> = match fs::read(control.join(".n1/storage-pools.json")) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).map_err(|e| format!("invalid pool catalog: {e}"))?
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(format!("read pool catalog: {e}")),
    };
    let registry = crate::core::root_identity::registered_roots(control)?;
    let mut identities = BTreeSet::new();
    for pool in &mut pools {
        let old = enrolled.iter().find(|old| old.id == pool.id);
        if old.is_some_and(|old| !old.same_geometry(pool)) {
            return Err("enrolled pool geometry/generation changed".into());
        }
        let candidates = pool.roots.clone();
        let descriptors = candidates
            .iter()
            .map(|root| {
                if !root.is_dir()
                    || root
                        .join(crate::core::instance_lease::RESTORE_PENDING)
                        .exists()
                {
                    Ok(None)
                } else {
                    read_member(root)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let imported = descriptors.iter().flatten().next().map(|d| &d.pool);
        let identity = old.or(imported);
        pool.legacy_roots = identity
            .map(|p| p.address_roots().to_vec())
            .unwrap_or_else(|| candidates.clone());
        pool.members = match identity {
            Some(p) if !p.members.is_empty() => p.members.clone(),
            Some(p) if old.is_some() => p
                .roots
                .iter()
                .map(|root| {
                    registry
                        .get(&root.to_string_lossy().into_owned())
                        .cloned()
                        .ok_or_else(|| {
                            format!("legacy root '{}' has no accepted identity", root.display())
                        })
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => candidates
                .iter()
                .map(|root| {
                    crate::core::root_identity::member_id_on(root)
                        .map(|id| id.unwrap_or_else(|| ulid::Ulid::new().to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        if pool.members.len() != pool.width()
            || pool
                .members
                .iter()
                .any(|id| id.is_empty() || !identities.insert(id.clone()))
        {
            return Err("invalid or duplicate accepted member identity".into());
        }
        let mut ordered = vec![None; pool.width()];
        let mut unresolved = Vec::new();
        for (position, (root, descriptor)) in candidates.into_iter().zip(descriptors).enumerate() {
            if let Some(d) = descriptor {
                if !d.pool.same_geometry(pool)
                    || d.pool.members != pool.members
                    || d.pool.address_roots() != pool.address_roots()
                {
                    return Err(
                        "member does not belong to the accepted pool/geometry/membership".into(),
                    );
                }
                let slot = d.member_slot;
                if ordered[slot].replace(root).is_some() {
                    return Err("duplicate mounted member slot".into());
                }
            } else {
                // Legacy members must remain at their original locations for
                // the one-time descriptor upgrade. Blank known slots remain
                // unavailable; only offline replacement can initialize them.
                if let Ok(bytes) = fs::read(root.join(".n1-pool.json")) {
                    if !root
                        .join(crate::core::instance_lease::RESTORE_PENDING)
                        .exists()
                    {
                        let legacy: StoragePool = serde_json::from_slice(&bytes)
                            .map_err(|e| format!("invalid legacy pool descriptor: {e}"))?;
                        if !legacy.same_geometry(pool)
                            || legacy.roots != pool.legacy_roots
                            || root != pool.legacy_roots[position]
                        {
                            return Err(
                                "legacy member must be upgraded at its original slot/location"
                                    .into(),
                            );
                        }
                    }
                }
                unresolved.push((position, root));
            }
        }
        for (slot, root) in unresolved {
            let slot = if ordered[slot].is_none() {
                slot
            } else {
                let missing: Vec<_> = ordered
                    .iter()
                    .enumerate()
                    .filter_map(|(slot, root)| root.is_none().then_some(slot))
                    .collect();
                if missing.len() != 1 {
                    return Err("ambiguous missing/replacement member slot".into());
                }
                missing[0]
            };
            if ordered[slot].replace(root).is_some() {
                return Err("ambiguous missing/replacement member slot".into());
            }
        }
        pool.roots = ordered
            .into_iter()
            .map(|root| root.ok_or_else(|| "missing member location".to_string()))
            .collect::<Result<_, _>>()?;
    }
    validate_catalog(&enrolled, &pools)?;
    Ok(pools)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePool {
    pub id: String,
    pub generation: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub roots: Vec<PathBuf>,
    /// Accepted identities in logical shard-slot order, independent of mounts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<String>,
    /// Original addresses, retained only to resolve pre-portability NDX bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub legacy_roots: Vec<PathBuf>,
    #[serde(default)]
    pub sealed: bool,
}

impl StoragePool {
    pub fn address_roots(&self) -> &[PathBuf] {
        if self.legacy_roots.is_empty() {
            &self.roots
        } else {
            &self.legacy_roots
        }
    }

    pub fn same_geometry(&self, other: &Self) -> bool {
        self.id == other.id
            && self.generation == other.generation
            && self.data_shards == other.data_shards
            && self.parity_shards == other.parity_shards
    }
    pub fn width(&self) -> usize {
        usize::from(self.data_shards) + usize::from(self.parity_shards)
    }

    pub fn namespace_replica_count(&self) -> usize {
        usize::from(self.parity_shards) + 1
    }

    pub fn available_roots(&self) -> Vec<PathBuf> {
        self.roots
            .iter()
            .filter(|root| root_available(root))
            .cloned()
            .collect()
    }

    pub fn writable(&self) -> bool {
        !self.sealed
            && self.roots.iter().all(|root| {
                root_available(root)
                    && rustix::fs::access(root, rustix::fs::Access::WRITE_OK).is_ok()
            })
    }

    pub fn namespace_roots(&self, fragment: &str, key: &str) -> Vec<PathBuf> {
        let mut ranked = self
            .roots
            .iter()
            .enumerate()
            .map(|(slot, root)| {
                let mut hash = Sha256::new();
                for component in [
                    "n1-4-namespace",
                    self.id.as_str(),
                    &self.generation.to_string(),
                    fragment,
                    key,
                    &slot.to_string(),
                ] {
                    hash.update(component.as_bytes());
                    hash.update([0]);
                }
                (<[u8; 32]>::from(hash.finalize()), slot, root.clone())
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        ranked
            .into_iter()
            .take(self.namespace_replica_count())
            .map(|(_, _, root)| root)
            .collect()
    }
}

pub fn root_available(root: &Path) -> bool {
    root.is_dir()
        && !root
            .join(crate::core::instance_lease::RESTORE_PENDING)
            .exists()
        && (!crate::core::root_identity::root_identity_enforcement_initialized()
            || crate::core::root_identity::configured_root_identity_is_current(root))
}

pub fn parse(config: &Ini) -> Result<Vec<StoragePool>, String> {
    let mut pools = Vec::new();
    let mut ids = BTreeSet::new();
    let mut generations = BTreeSet::new();
    let mut all_roots = Vec::<PathBuf>::new();
    for (section, values) in config.iter() {
        let Some(id) = section.and_then(|name| name.strip_prefix("storage_pool.")) else {
            continue;
        };
        if id.is_empty()
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
            || !ids.insert(id.to_string())
        {
            return Err(format!("invalid or duplicate storage pool id '{id}'"));
        }
        let number = |key: &str| -> Result<u64, String> {
            values
                .get(key)
                .ok_or_else(|| format!("storage_pool.{id}.{key} is required"))?
                .parse()
                .map_err(|_| format!("storage_pool.{id}.{key} must be an integer"))
        };
        let generation = number("generation")?;
        if generation == 0 || !generations.insert(generation) {
            return Err("storage pool generations must be unique and positive".into());
        }
        let data_shards =
            u16::try_from(number("data_shards")?).map_err(|_| "pool data_shards exceeds u16")?;
        let parity_shards = u16::try_from(number("parity_shards")?)
            .map_err(|_| "pool parity_shards exceeds u16")?;
        let roots = values
            .get("roots")
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .map(|path| {
                if !path.is_absolute() {
                    return Err(format!("pool root '{}' must be absolute", path.display()));
                }
                if path.components().any(|part| {
                    matches!(
                        part,
                        std::path::Component::ParentDir | std::path::Component::CurDir
                    )
                }) {
                    return Err(format!("pool root '{}' must be normalized", path.display()));
                }
                Ok(fs::canonicalize(&path).unwrap_or(path))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let pool = StoragePool {
            id: id.into(),
            generation,
            data_shards,
            parity_shards,
            roots,
            members: Vec::new(),
            legacy_roots: Vec::new(),
            sealed: match values.get("sealed").unwrap_or("false") {
                "true" => true,
                "false" => false,
                _ => return Err(format!("storage_pool.{id}.sealed must be true or false")),
            },
        };
        if data_shards == 0 || pool.width() > 256 || pool.roots.len() != pool.width() {
            return Err(format!(
                "storage pool '{id}' requires exactly K+P roots, K>=1 and K+P<=256"
            ));
        }
        for root in &pool.roots {
            if all_roots
                .iter()
                .any(|other| root.starts_with(other) || other.starts_with(root))
            {
                return Err(format!(
                    "storage pool root '{}' overlaps another configured root",
                    root.display()
                ));
            }
            all_roots.push(root.clone());
        }
        pools.push(pool);
    }
    if pools.is_empty() {
        return Err("configure at least one [storage_pool.NAME] with generation, roots, data_shards and parity_shards".into());
    }
    pools.sort_by_key(|pool| pool.generation);
    Ok(pools)
}

pub fn configured() -> &'static [StoragePool] {
    STORAGE_POOLS.get().map(Vec::as_slice).unwrap_or(&[])
}

/// Every new stored address names a logical member, never its current mount.
pub fn record_address(path: &Path) -> String {
    for pool in configured() {
        for (slot, root) in pool.roots.iter().enumerate() {
            if let Ok(relative) = path.strip_prefix(root) {
                return format!(
                    "n1://{}/{}/{}/{}",
                    pool.id,
                    pool.generation,
                    slot,
                    relative.display()
                );
            }
        }
    }
    // Isolated codec/tests without enrolled pools retain their original layout.
    path.to_string_lossy().into_owned()
}

pub fn resolve_address_in(pools: &[StoragePool], address: &str) -> Result<PathBuf, String> {
    if let Some(logical) = address.strip_prefix("n1://") {
        let mut components = logical.splitn(4, '/');
        let id = components.next().ok_or("missing logical pool")?;
        let generation: u64 = components
            .next()
            .ok_or("missing logical generation")?
            .parse()
            .map_err(|_| "invalid logical generation")?;
        let slot: usize = components
            .next()
            .ok_or("missing logical slot")?
            .parse()
            .map_err(|_| "invalid logical slot")?;
        let relative = Path::new(components.next().ok_or("missing logical relative path")?);
        if relative
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err("invalid member-relative address".into());
        }
        let pool = pools
            .iter()
            .find(|p| p.id == id && p.generation == generation)
            .ok_or("address belongs to an unknown pool/generation")?;
        return pool
            .roots
            .get(slot)
            .map(|root| root.join(relative))
            .ok_or_else(|| "address slot outside pool".into());
    }
    let path = Path::new(address);
    if !path.is_absolute()
        || path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err("invalid recorded storage address".into());
    }
    // Original roots take precedence: even swapped mount names cannot change
    // an old record's logical shard slot. There is no relocation alias chain.
    for pool in pools {
        for (slot, original) in pool.address_roots().iter().enumerate() {
            if let Ok(relative) = path.strip_prefix(original) {
                return Ok(pool.roots[slot].join(relative));
            }
        }
    }
    if pools.is_empty() {
        return Ok(path.to_path_buf());
    }
    Err("recorded address is outside enrolled members".into())
}

pub fn resolve_address(address: &str) -> Result<PathBuf, AppError> {
    resolve_address_in(configured(), address).map_err(AppError::Conflict)
}

pub fn by_id(id: &str) -> Result<&'static StoragePool, AppError> {
    configured()
        .iter()
        .find(|pool| pool.id == id)
        .ok_or_else(|| {
            AppError::ServiceUnavailable(format!("storage pool '{id}' is not configured"))
        })
}

pub fn write_pool() -> Result<&'static StoragePool, AppError> {
    configured()
        .iter()
        .rev()
        .find(|pool| pool.writable())
        .ok_or_else(|| {
            AppError::ServiceUnavailable("no complete writable storage pool is available".into())
        })
}

pub fn pool_for_root(root: &Path) -> Option<&'static StoragePool> {
    configured()
        .iter()
        .find(|pool| pool.roots.iter().any(|member| root.starts_with(member)))
}

pub fn pool_for_metadata(
    meta: &crate::core::disk_manager::ObjectMetadata,
) -> Option<&'static StoragePool> {
    let first = meta.parts.iter().flat_map(|p| &p.shards).next()?;
    let path = resolve_address(&first.absolute_path).ok()?;
    let pool = pool_for_root(&path)?;
    (meta.data_shards == pool.data_shards
        && meta.parity_shards == pool.parity_shards
        && meta.parts.iter().all(|part| {
            part.shards.len() == pool.width()
                && part.shards.iter().all(|shard| {
                    pool.roots.get(shard.shard_index).is_some_and(|root| {
                        resolve_address(&shard.absolute_path)
                            .is_ok_and(|path| path.starts_with(root))
                    })
                })
        }))
    .then_some(pool)
}

/// Pool membership is persisted on every root and checked before starting the
/// engine. Root replacement continues to use the existing identity protocol.
pub fn enroll() -> Result<(), String> {
    let (catalog_path, mut catalog) = enrollment_catalog()?;
    validate_catalog(&catalog, configured())?;
    enroll_members(&catalog_path, &mut catalog)
}

/// Validate immutable membership before root-identity reconciliation can write
/// anything. A rejected configuration must not decommission old root IDs.
pub fn validate_existing_catalog() -> Result<(), String> {
    let (_, catalog) = enrollment_catalog()?;
    validate_catalog(&catalog, configured())
}

fn enrollment_catalog() -> Result<(PathBuf, Vec<StoragePool>), String> {
    let workspace = crate::core::disk_manager::FS_STORAGE_ROOT
        .get()
        .ok_or("pool enrollment requires fs_storage_path")?;
    let catalog_path = Path::new(workspace).join(".n1/storage-pools.json");
    let catalog: Vec<StoragePool> = match fs::read(&catalog_path) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).map_err(|e| format!("invalid pool catalog: {e}"))?
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(format!("read pool catalog: {e}")),
    };
    Ok((catalog_path, catalog))
}

fn enroll_members(catalog_path: &Path, catalog: &mut Vec<StoragePool>) -> Result<(), String> {
    for pool in configured() {
        #[cfg(unix)]
        let mut devices = BTreeSet::new();
        for root in &pool.roots {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if let Ok(meta) = fs::metadata(root) {
                    if !devices.insert(meta.dev()) {
                        return Err(format!(
                            "storage pool '{}' has multiple roots on one physical failure domain",
                            pool.id
                        ));
                    }
                }
            }
            if !root_available(root) {
                continue;
            }
            if let Some(member) = read_member(root)? {
                let slot = pool.roots.iter().position(|r| r == root).unwrap();
                if !member.pool.same_geometry(pool)
                    || member.member_slot != slot
                    || member.member_id != pool.members[slot]
                {
                    return Err("member identity changed during startup".into());
                }
            }
            write_member(pool, pool.roots.iter().position(|r| r == root).unwrap())?;
        }
        let mut immutable = pool.clone();
        immutable.roots = pool.address_roots().to_vec();
        immutable.sealed = false;
        if let Some(old) = catalog.iter_mut().find(|old| old.id == pool.id) {
            *old = immutable;
        } else {
            catalog.push(immutable);
        }
    }
    let bytes = serde_json::to_vec(&catalog).map_err(|e| e.to_string())?;
    crate::core::index_store::write_pool_identity(catalog_path, &bytes)
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) fn validate_catalog(
    enrolled: &[StoragePool],
    configured: &[StoragePool],
) -> Result<(), String> {
    let maximum_generation = enrolled.iter().map(|p| p.generation).max().unwrap_or(0);
    for old in enrolled {
        let mut current = configured
            .iter()
            .find(|p| p.id == old.id)
            .ok_or_else(|| format!("enrolled storage pool '{}' cannot be removed", old.id))?
            .clone();
        current.sealed = false;
        let legacy_matches = if current.members.is_empty() {
            current.roots == old.roots
        } else {
            current.address_roots() == old.address_roots()
                && current.members.len() == current.width()
                && (old.members.is_empty() || current.members == old.members)
        };
        if !current.same_geometry(old) || !legacy_matches {
            return Err(format!("enrolled storage pool '{}' is immutable", old.id));
        }
    }
    for new in configured
        .iter()
        .filter(|p| !enrolled.iter().any(|old| old.id == p.id))
    {
        if new.generation <= maximum_generation {
            return Err(format!(
                "new pool '{}' must have a later generation",
                new.id
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        base: PathBuf,
        control: PathBuf,
        pool: StoragePool,
    }
    impl Fixture {
        fn new() -> Self {
            let base =
                std::env::temp_dir().join(format!("n1-member-binding-{}", ulid::Ulid::new()));
            let control = base.join("control");
            fs::create_dir_all(control.join(".n1")).unwrap();
            let roots = vec![base.join("member-0"), base.join("member-1")];
            for root in &roots {
                fs::create_dir(root).unwrap();
            }
            let pool = StoragePool {
                id: "A".into(),
                generation: 1,
                data_shards: 1,
                parity_shards: 1,
                roots: roots.clone(),
                members: vec!["member0".into(), "member1".into()],
                legacy_roots: roots,
                sealed: false,
            };
            for slot in 0..pool.width() {
                write_member(&pool, slot).unwrap();
            }
            fs::write(
                control.join(".n1/storage-pools.json"),
                serde_json::to_vec(&vec![pool.clone()]).unwrap(),
            )
            .unwrap();
            Self {
                base,
                control,
                pool,
            }
        }
        fn relocated(&self) -> StoragePool {
            let mut current = self.pool.clone();
            current.roots = self
                .pool
                .roots
                .iter()
                .enumerate()
                .map(|(slot, root)| {
                    let moved = self.base.join(format!("new-location-{slot}"));
                    fs::rename(root, &moved).unwrap();
                    moved
                })
                .collect();
            current.members.clear();
            current.legacy_roots.clear();
            current
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.base);
        }
    }

    #[test]
    fn descriptors_bind_relocated_and_reordered_roots_without_old_paths() {
        let f = Fixture::new();
        let mut candidate = f.relocated();
        let expected = candidate.roots.clone();
        candidate.roots.reverse();
        let bound = bind_locations(vec![candidate], &f.control).unwrap();
        assert_eq!(bound[0].roots, expected);
        assert_eq!(bound[0].members, f.pool.members);
        assert!(f.pool.roots.iter().all(|root| !root.exists()));
        let old = f.pool.roots[1].join("@f_f/.objects/a/b/@o_o/v/p.parity");
        assert_eq!(
            resolve_address_in(&bound, old.to_str().unwrap()).unwrap(),
            expected[1].join("@f_f/.objects/a/b/@o_o/v/p.parity")
        );
        assert_eq!(
            resolve_address_in(&bound, "n1://A/1/0/@f_f/.objects/a/b/@o_o/v/p.shard").unwrap(),
            expected[0].join("@f_f/.objects/a/b/@o_o/v/p.shard")
        );
    }

    #[test]
    fn duplicate_wrong_pool_and_retired_member_descriptors_are_rejected() {
        let f = Fixture::new();
        let candidate = f.relocated();
        let first = fs::read(candidate.roots[0].join(".n1-pool.json")).unwrap();
        let second = fs::read(candidate.roots[1].join(".n1-pool.json")).unwrap();
        fs::write(candidate.roots[1].join(".n1-pool.json"), &first).unwrap();
        assert!(bind_locations(vec![candidate.clone()], &f.control).is_err());
        for field in ["id", "member_id"] {
            let mut invalid: serde_json::Value = serde_json::from_slice(&second).unwrap();
            invalid[field] = serde_json::json!("unaccepted");
            fs::write(
                candidate.roots[1].join(".n1-pool.json"),
                serde_json::to_vec(&invalid).unwrap(),
            )
            .unwrap();
            assert!(bind_locations(vec![candidate.clone()], &f.control).is_err());
        }
    }

    #[test]
    fn missing_slot_is_not_renumbered_or_automatically_initialized() {
        let f = Fixture::new();
        fs::remove_dir_all(&f.pool.roots[0]).unwrap();
        let bound = bind_locations(vec![f.pool.clone()], &f.control).unwrap();
        assert_eq!(bound[0].roots, f.pool.roots);
        assert_eq!(bound[0].members, f.pool.members);
        assert!(!bound[0].roots[0].exists());
    }

    #[test]
    fn startup_binding_never_certifies_a_partial_copy_or_removes_its_fence() {
        let f = Fixture::new();
        let pending = f.pool.roots[0].join(crate::core::instance_lease::RESTORE_PENDING);
        fs::write(&pending, b"interrupted copy").unwrap();
        let bound = bind_locations(vec![f.pool.clone()], &f.control).unwrap();
        assert!(!root_available(&bound[0].roots[0]));
        assert!(pending.exists());
    }

    #[test]
    fn one_fenced_slot_can_resume_after_discovery_order_changes() {
        let f = Fixture::new();
        let mut candidate = f.relocated();
        fs::write(
            candidate.roots[0].join(crate::core::instance_lease::RESTORE_PENDING),
            b"partial",
        )
        .unwrap();
        let expected = candidate.roots.clone();
        candidate.roots.reverse();
        let bound = bind_locations(vec![candidate], &f.control).unwrap();
        assert_eq!(bound[0].roots, expected);
        assert!(!root_available(&bound[0].roots[0]));
    }

    #[test]
    fn logical_addresses_reject_escape_wrong_generation_unknown_pool_and_slot() {
        let f = Fixture::new();
        for address in [
            "n1://A/1/0/../../outside",
            "n1://A/1/0//outside",
            "n1://A/2/0/file",
            "n1://B/1/0/file",
            "n1://A/1/2/file",
            "n1://A/x/0/file",
            "/outside/file",
        ] {
            assert!(
                resolve_address_in(std::slice::from_ref(&f.pool), address).is_err(),
                "{address}"
            );
        }
    }

    #[test]
    fn original_address_slots_survive_mount_name_swaps_without_alias_chains() {
        let f = Fixture::new();
        let mut pool = f.pool.clone();
        pool.roots.reverse();
        let old = f.pool.roots[0].join("piece");
        assert_eq!(
            resolve_address_in(&[pool.clone()], old.to_str().unwrap()).unwrap(),
            pool.roots[0].join("piece")
        );
        assert_eq!(
            resolve_address_in(&[pool.clone()], "n1://A/1/0/piece").unwrap(),
            pool.roots[0].join("piece")
        );
        pool.roots[0] = f.base.join("moved-again");
        assert_eq!(
            resolve_address_in(&[pool.clone()], old.to_str().unwrap()).unwrap(),
            pool.roots[0].join("piece")
        );
    }

    fn config() -> Ini {
        Ini::load_from_str("[storage_pool.A]\ngeneration=1\ndata_shards=3\nparity_shards=1\nroots=/pool/a0,/pool/a1,/pool/a2,/pool/a3\n[storage_pool.B]\ngeneration=2\ndata_shards=4\nparity_shards=2\nroots=/pool/b0,/pool/b1,/pool/b2,/pool/b3,/pool/b4,/pool/b5\n").unwrap()
    }

    #[test]
    fn expansion_retains_pool_geometry_and_namespace_placement() {
        let pools = parse(&config()).unwrap();
        assert_eq!(pools[0].width(), 4);
        assert_eq!(pools[1].width(), 6);
        assert_eq!(
            pools[0].namespace_roots("fragment", "a/b"),
            pools[0].namespace_roots("fragment", "a/b")
        );
        assert_eq!(pools[0].namespace_roots("fragment", "a/b").len(), 2);
        assert_eq!(pools[1].namespace_roots("fragment", "a/b").len(), 3);
        // Every allowed set of failed roots leaves at least one pointer.
        for pool in pools {
            let replicas = pool.namespace_roots("fragment", "a/b");
            for mask in 0usize..(1usize << pool.width()) {
                if mask.count_ones() as usize > usize::from(pool.parity_shards) {
                    continue;
                }
                assert!(replicas.iter().any(|root| mask
                    & (1 << pool.roots.iter().position(|r| r == root).unwrap())
                    == 0));
            }
        }
    }

    #[test]
    fn enrollment_is_append_only_but_sealing_is_mutable() {
        let pools = parse(&config()).unwrap();
        let enrolled = &pools[..1];
        assert!(validate_catalog(enrolled, &pools).is_ok());
        assert!(validate_catalog(enrolled, &pools[1..]).is_err());
        let mut changed = pools.clone();
        changed[0].sealed = true;
        assert!(validate_catalog(enrolled, &changed).is_ok());
        changed[0].roots.swap(0, 1);
        assert!(validate_catalog(enrolled, &changed).is_err());
        let mut changed = pools.clone();
        changed[0].data_shards = 2;
        changed[0].parity_shards = 2;
        assert!(validate_catalog(enrolled, &changed).is_err());
        let mut changed = pools.clone();
        changed[1].generation = 1;
        assert!(validate_catalog(enrolled, &changed).is_err());
    }

    #[test]
    fn rejects_overlapping_roots_and_duplicate_generations() {
        let mut config = config();
        config
            .with_section(Some("storage_pool.B"))
            .set("generation", "1");
        assert!(parse(&config).is_err());
        config
            .with_section(Some("storage_pool.B"))
            .set("generation", "2")
            .set(
                "roots",
                "/pool/a0,/pool/b1,/pool/b2,/pool/b3,/pool/b4,/pool/b5",
            );
        assert!(parse(&config).is_err());
    }
}
