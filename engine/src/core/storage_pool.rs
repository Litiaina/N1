//! Immutable, independently expandable N1 4 storage pools.
//!
//! A pool is one EC failure domain. Adding a pool never changes a previously
//! enrolled pool's members, namespace replica placement, or object geometry.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use ini::Ini;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::enums::app_error_enum::AppError;

pub static STORAGE_POOLS: OnceLock<Vec<StoragePool>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePool {
    pub id: String,
    pub generation: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub roots: Vec<PathBuf>,
    #[serde(default)]
    pub sealed: bool,
}

impl StoragePool {
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
                (<[u8; 32]>::from(hash.finalize()), root.clone())
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        ranked
            .into_iter()
            .take(self.namespace_replica_count())
            .map(|(_, root)| root)
            .collect()
    }
}

pub fn root_available(root: &Path) -> bool {
    root.is_dir()
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
    let pool = pool_for_root(Path::new(&first.absolute_path))?;
    (meta.data_shards == pool.data_shards
        && meta.parity_shards == pool.parity_shards
        && meta.parts.iter().all(|part| {
            part.shards.len() == pool.width()
                && part.shards.iter().all(|shard| {
                    pool.roots
                        .get(shard.shard_index)
                        .is_some_and(|root| Path::new(&shard.absolute_path).starts_with(root))
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
            let path = root.join(".n1-pool.json");
            let mut immutable = pool.clone();
            immutable.sealed = false;
            match fs::read(&path) {
                Ok(bytes) => {
                    let stored: StoragePool = serde_json::from_slice(&bytes)
                        .map_err(|e| format!("invalid pool identity '{}': {e}", path.display()))?;
                    if stored != immutable {
                        return Err(format!(
                            "storage pool '{}' changed enrolled geometry or membership",
                            pool.id
                        ));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    let bytes = serde_json::to_vec(&immutable).map_err(|e| e.to_string())?;
                    crate::core::index_store::write_pool_identity(&path, &bytes)
                        .map_err(|e| e.to_string())?;
                }
                Err(e) => return Err(format!("read pool identity '{}': {e}", path.display())),
            }
        }
        if !catalog.iter().any(|old| old.id == pool.id) {
            let mut immutable = pool.clone();
            immutable.sealed = false;
            catalog.push(immutable);
        }
    }
    let bytes = serde_json::to_vec(&catalog).map_err(|e| e.to_string())?;
    crate::core::index_store::write_pool_identity(catalog_path, &bytes)
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn validate_catalog(enrolled: &[StoragePool], configured: &[StoragePool]) -> Result<(), String> {
    let maximum_generation = enrolled.iter().map(|p| p.generation).max().unwrap_or(0);
    for old in enrolled {
        let mut current = configured
            .iter()
            .find(|p| p.id == old.id)
            .ok_or_else(|| format!("enrolled storage pool '{}' cannot be removed", old.id))?
            .clone();
        current.sealed = false;
        if &current != old {
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
