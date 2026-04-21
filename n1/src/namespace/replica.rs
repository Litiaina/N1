use crate::enums::app_error_enum::AppError;
use crate::namespace::catalog::NamespaceCatalog;

use indexdb::{Database, ReadableDatabase, TableDefinition, TableError};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs;
use ulid::Ulid;

pub const NAMESPACE_V2_DIR: &str = "namespace_v2";
pub const NAMESPACE_V2_FILE: &str = "catalog.ndxdb";

const CATALOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("catalog");
const CATALOG_ROW_KEY: &str = "catalog";

#[derive(Debug, Clone, Serialize, Default)]
pub struct NamespaceReplicaInspection {
    pub fragment: String,
    pub expected_replicas: u64,
    pub present_replicas: u64,
    pub valid_replicas: u64,
    pub missing_replicas: u64,
    pub divergent_replicas: u64,
    pub corrupt_replicas: u64,
    pub healthy: bool,
    pub canonical_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct NamespaceReplicaRepairReport {
    pub fragment: String,
    pub expected_replicas: u64,
    pub missing_repaired: u64,
    pub divergent_repaired: u64,
    pub corrupt_found: u64,
    pub selected_source: Option<String>,
}

#[derive(Clone)]
struct RawNamespaceReplica {
    path: PathBuf,
    bytes: Option<Vec<u8>>,
    mtime: SystemTime,
    len: u64,
    catalog_hash: Option<[u8; 32]>,
    valid: bool,
}

pub fn namespace_db_path_for_root(root: &Path, fragment: &str) -> PathBuf {
    root.join(fragment)
        .join(NAMESPACE_V2_DIR)
        .join(NAMESPACE_V2_FILE)
}

pub fn namespace_db_paths_for_roots(fragment: &str, roots: &[PathBuf]) -> Vec<PathBuf> {
    roots
        .iter()
        .map(|root| namespace_db_path_for_root(root, fragment))
        .collect()
}

pub async fn inspect_namespace_replicas(
    fragment: &str,
    index_roots: &[PathBuf],
) -> Result<NamespaceReplicaInspection, AppError> {
    let replicas = read_namespace_replicas(fragment, index_roots).await;
    let best = choose_best_namespace_replica(&replicas);
    Ok(build_inspection(fragment, &replicas, best))
}

pub async fn repair_namespace_replicas(
    fragment: &str,
    index_roots: &[PathBuf],
    dry_run: bool,
) -> Result<NamespaceReplicaRepairReport, AppError> {
    let replicas = read_namespace_replicas(fragment, index_roots).await;
    let Some(best_index) = choose_best_namespace_replica(&replicas) else {
        return Ok(NamespaceReplicaRepairReport {
            fragment: fragment.to_string(),
            expected_replicas: replicas.len() as u64,
            corrupt_found: replicas
                .iter()
                .filter(|replica| replica.bytes.is_some() && !replica.valid)
                .count() as u64,
            ..NamespaceReplicaRepairReport::default()
        });
    };

    let canonical = replicas[best_index].bytes.clone().unwrap_or_default();
    let canonical_hash = replicas[best_index].catalog_hash;
    let mut report = NamespaceReplicaRepairReport {
        fragment: fragment.to_string(),
        expected_replicas: replicas.len() as u64,
        selected_source: Some(replicas[best_index].path.to_string_lossy().to_string()),
        ..NamespaceReplicaRepairReport::default()
    };

    for replica in replicas {
        match replica.bytes {
            None => {
                report.missing_repaired += 1;
                if !dry_run {
                    atomic_write(&replica.path, &canonical)
                        .await
                        .map_err(|err| {
                            AppError::Io(
                                err,
                                format!("repair namespace replica {}", replica.path.display()),
                            )
                        })?;
                }
            }
            Some(_) => {
                if !replica.valid {
                    report.corrupt_found += 1;
                }
                if replica.catalog_hash != canonical_hash {
                    report.divergent_repaired += 1;
                    if !dry_run {
                        atomic_write(&replica.path, &canonical)
                            .await
                            .map_err(|err| {
                                AppError::Io(
                                    err,
                                    format!("repair namespace replica {}", replica.path.display()),
                                )
                            })?;
                    }
                }
            }
        }
    }

    Ok(report)
}

async fn read_namespace_replicas(
    fragment: &str,
    index_roots: &[PathBuf],
) -> Vec<RawNamespaceReplica> {
    let mut replicas = Vec::with_capacity(index_roots.len());
    for path in namespace_db_paths_for_roots(fragment, index_roots) {
        replicas.push(read_namespace_replica(path).await);
    }
    replicas
}

async fn read_namespace_replica(path: PathBuf) -> RawNamespaceReplica {
    match fs::read(&path).await {
        Ok(bytes) => {
            let meta = fs::metadata(&path).await.ok();
            let mtime = meta
                .as_ref()
                .and_then(|meta| meta.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta
                .as_ref()
                .map(|meta| meta.len())
                .unwrap_or(bytes.len() as u64);
            let catalog_hash = read_namespace_catalog_hash(&bytes).await;
            let valid = catalog_hash.is_some();
            RawNamespaceReplica {
                path,
                bytes: Some(bytes),
                mtime,
                len,
                catalog_hash,
                valid,
            }
        }
        Err(_) => RawNamespaceReplica {
            path,
            bytes: None,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            catalog_hash: None,
            valid: false,
        },
    }
}

fn choose_best_namespace_replica(replicas: &[RawNamespaceReplica]) -> Option<usize> {
    let mut hash_counts: HashMap<[u8; 32], usize> = HashMap::new();
    for replica in replicas.iter().filter(|replica| replica.valid) {
        if let Some(hash) = replica.catalog_hash {
            *hash_counts.entry(hash).or_default() += 1;
        }
    }
    let canonical_hash = choose_canonical_catalog_hash(&hash_counts)?;

    let mut best: Option<usize> = None;
    for (idx, replica) in replicas.iter().enumerate() {
        if !replica.valid || replica.catalog_hash != Some(canonical_hash) {
            continue;
        }

        let replace = match best {
            None => true,
            Some(current_idx) => {
                let current = &replicas[current_idx];
                replica.mtime > current.mtime
                    || (replica.mtime == current.mtime && replica.len > current.len)
                    || (replica.mtime == current.mtime
                        && replica.len == current.len
                        && replica.path > current.path)
            }
        };

        if replace {
            best = Some(idx);
        }
    }
    best
}

fn choose_canonical_catalog_hash(hash_counts: &HashMap<[u8; 32], usize>) -> Option<[u8; 32]> {
    let mut counts = hash_counts
        .iter()
        .map(|(hash, count)| (*hash, *count))
        .collect::<Vec<_>>();
    counts.sort_by(|(left_hash, left_count), (right_hash, right_count)| {
        left_count
            .cmp(right_count)
            .then_with(|| left_hash.cmp(right_hash))
    });
    let (best_hash, best_count) = *counts.last()?;
    let tied = counts
        .iter()
        .rev()
        .skip(1)
        .any(|(_, count)| *count == best_count);
    if tied { None } else { Some(best_hash) }
}

fn build_inspection(
    fragment: &str,
    replicas: &[RawNamespaceReplica],
    best: Option<usize>,
) -> NamespaceReplicaInspection {
    let canonical_hash = best.and_then(|idx| replicas[idx].catalog_hash);
    let mut hash_counts: HashMap<[u8; 32], usize> = HashMap::new();
    let mut inspection = NamespaceReplicaInspection {
        fragment: fragment.to_string(),
        expected_replicas: replicas.len() as u64,
        canonical_path: best.map(|idx| replicas[idx].path.to_string_lossy().to_string()),
        ..NamespaceReplicaInspection::default()
    };

    for replica in replicas {
        match replica.bytes.as_ref() {
            None => inspection.missing_replicas += 1,
            Some(_) => {
                inspection.present_replicas += 1;
                if replica.valid {
                    inspection.valid_replicas += 1;
                    if let Some(hash) = replica.catalog_hash {
                        *hash_counts.entry(hash).or_default() += 1;
                    }
                    if let Some(hash) = canonical_hash {
                        if replica.catalog_hash != Some(hash) {
                            inspection.divergent_replicas += 1;
                        }
                    }
                } else {
                    inspection.corrupt_replicas += 1;
                }
            }
        }
    }
    if canonical_hash.is_none() && hash_counts.len() > 1 {
        inspection.divergent_replicas = inspection.valid_replicas;
    }

    inspection.healthy = inspection.expected_replicas > 0
        && inspection.missing_replicas == 0
        && inspection.divergent_replicas == 0
        && inspection.corrupt_replicas == 0
        && inspection.valid_replicas == inspection.expected_replicas;
    inspection
}

async fn read_namespace_catalog_hash(bytes: &[u8]) -> Option<[u8; 32]> {
    let bytes = bytes.to_vec();
    tokio::task::spawn_blocking(move || namespace_catalog_semantic_hash(&bytes))
        .await
        .unwrap_or(None)
}

pub fn namespace_catalog_semantic_hash(bytes: &[u8]) -> Option<[u8; 32]> {
    let dir = std::env::temp_dir().join(format!("n1-namespace-v2-validate-{}", Ulid::new()));
    let path = dir.join(NAMESPACE_V2_FILE);

    let hash = (|| -> Option<[u8; 32]> {
        if std::fs::create_dir_all(&dir).is_err() {
            return None;
        }
        if std::fs::write(&path, bytes).is_err() {
            return None;
        }
        let db = match std::panic::catch_unwind(|| Database::create(&path)) {
            Ok(Ok(db)) => db,
            _ => return None,
        };
        let read_txn = match db.begin_read() {
            Ok(txn) => txn,
            Err(_) => return None,
        };
        let table = match read_txn.open_table(CATALOG_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return None,
            Err(_) => return None,
        };
        let Some(row) = table.get(CATALOG_ROW_KEY).ok().flatten() else {
            return None;
        };
        let mut catalog = match rmp_serde::from_slice::<NamespaceCatalog>(row.value()) {
            Ok(catalog) => catalog,
            Err(_) => return None,
        };
        if catalog.rebuild_secondary_indices().is_err() {
            return None;
        }
        rmp_serde::to_vec(&catalog)
            .ok()
            .map(|canonical| sha256_bytes(&canonical))
    })();

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&dir);
    hash
}

async fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).await?;
    let temp_path = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(NAMESPACE_V2_FILE),
        Ulid::new()
    ));
    fs::write(&temp_path, data).await?;
    fs::rename(&temp_path, path).await?;
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}
