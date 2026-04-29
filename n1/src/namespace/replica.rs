use crate::core::indexdb_store::open_n1_indexdb;
use crate::enums::app_error_enum::AppError;

use indexdb::Database;
use indexdb::n1 as n1_indexdb;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs;
use ulid::Ulid;

pub const INDEXDB_DIR: &str = "indexdb";
pub const INDEXDB_FILE: &str = "index.ndxdb";

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
    present: bool,
    mtime: SystemTime,
    len: u64,
    metadata_hash: Option<[u8; 32]>,
    valid: bool,
}

pub fn namespace_db_path_for_root(root: &Path, fragment: &str) -> PathBuf {
    root.join(fragment).join(INDEXDB_DIR).join(INDEXDB_FILE)
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
                .filter(|replica| replica.present && !replica.valid)
                .count() as u64,
            ..NamespaceReplicaRepairReport::default()
        });
    };

    let canonical_hash = replicas[best_index].metadata_hash;
    let source_path = replicas[best_index].path.clone();
    let mut report = NamespaceReplicaRepairReport {
        fragment: fragment.to_string(),
        expected_replicas: replicas.len() as u64,
        selected_source: Some(source_path.to_string_lossy().to_string()),
        ..NamespaceReplicaRepairReport::default()
    };

    for replica in replicas {
        if !replica.present {
            report.missing_repaired += 1;
            if !dry_run {
                copy_namespace_db_file(&source_path, &replica.path)
                    .await
                    .map_err(|err| {
                        AppError::Io(
                            err,
                            format!("repair namespace replica {}", replica.path.display()),
                        )
                    })?;
            }
            continue;
        }

        if !replica.valid {
            report.corrupt_found += 1;
        }
        if replica.metadata_hash != canonical_hash {
            report.divergent_repaired += 1;
            if !dry_run {
                copy_namespace_db_file(&source_path, &replica.path)
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
    match fs::metadata(&path).await {
        Ok(meta) => {
            let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta.len();
            let hash = namespace_indexdb_semantic_hash(path.clone()).await;
            RawNamespaceReplica {
                path,
                present: true,
                mtime,
                len,
                metadata_hash: hash,
                valid: hash.is_some(),
            }
        }
        Err(_) => RawNamespaceReplica {
            path,
            present: false,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            metadata_hash: None,
            valid: false,
        },
    }
}

fn choose_best_namespace_replica(replicas: &[RawNamespaceReplica]) -> Option<usize> {
    let mut hash_counts: HashMap<[u8; 32], usize> = HashMap::new();
    for replica in replicas.iter().filter(|replica| replica.valid) {
        if let Some(hash) = replica.metadata_hash {
            *hash_counts.entry(hash).or_default() += 1;
        }
    }
    let canonical_hash = choose_canonical_metadata_hash(&hash_counts)?;

    let mut best: Option<usize> = None;
    for (idx, replica) in replicas.iter().enumerate() {
        if !replica.valid || replica.metadata_hash != Some(canonical_hash) {
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

fn choose_canonical_metadata_hash(hash_counts: &HashMap<[u8; 32], usize>) -> Option<[u8; 32]> {
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
    let canonical_hash = best.and_then(|idx| replicas[idx].metadata_hash);
    let mut hash_counts: HashMap<[u8; 32], usize> = HashMap::new();
    let mut inspection = NamespaceReplicaInspection {
        fragment: fragment.to_string(),
        expected_replicas: replicas.len() as u64,
        canonical_path: best.map(|idx| replicas[idx].path.to_string_lossy().to_string()),
        ..NamespaceReplicaInspection::default()
    };

    for replica in replicas {
        if !replica.present {
            inspection.missing_replicas += 1;
            continue;
        }
        inspection.present_replicas += 1;
        if replica.valid {
            inspection.valid_replicas += 1;
            if let Some(hash) = replica.metadata_hash {
                *hash_counts.entry(hash).or_default() += 1;
            }
            if let Some(hash) = canonical_hash {
                if replica.metadata_hash != Some(hash) {
                    inspection.divergent_replicas += 1;
                }
            }
        } else {
            inspection.corrupt_replicas += 1;
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

async fn namespace_indexdb_semantic_hash(path: PathBuf) -> Option<[u8; 32]> {
    tokio::task::spawn_blocking(move || {
        let db = open_n1_indexdb(&path).ok()?;
        namespace_indexdb_semantic_hash_blocking(&db).ok()
    })
    .await
    .unwrap_or(None)
}

fn namespace_indexdb_semantic_hash_blocking(db: &Database) -> Result<[u8; 32], AppError> {
    let mut hasher = Sha256::new();
    let mut after = None;
    loop {
        let nodes = db
            .n1_list_namespace_nodes(after.as_deref(), 1000)
            .map_err(|err| AppError::InternalServerError(format!("namespace hash scan: {err}")))?;
        if nodes.is_empty() {
            break;
        }
        after = nodes.last().map(|node| node.node_id.clone());
        for node in nodes {
            hash_field(&mut hasher, "namespace-node");
            hash_field(&mut hasher, &node.node_id);
            hash_field(&mut hasher, node.parent_node_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, &node.name);
            hash_field(&mut hasher, namespace_kind(node.kind));
            hash_field(&mut hasher, node.object_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, node.head_version_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, &node.created_at_unix.to_string());
            hash_field(&mut hasher, &node.updated_at_unix.to_string());
        }
    }

    let mut after = None;
    loop {
        let roots = db
            .n1_list_namespace_trash_roots_all(after.as_deref(), 1000)
            .map_err(|err| {
                AppError::InternalServerError(format!("namespace trash hash scan: {err}"))
            })?;
        if roots.is_empty() {
            break;
        }
        after = roots.last().map(|root| root.trash_entry_id.clone());
        for root in roots {
            hash_field(&mut hasher, "namespace-trash-root");
            hash_field(&mut hasher, &root.trash_entry_id);
            hash_field(&mut hasher, &root.root_node_id);
            hash_field(&mut hasher, &root.original_parent_node_id);
            hash_field(&mut hasher, &root.original_name);
            hash_field(&mut hasher, &root.original_path);
            hash_field(&mut hasher, namespace_kind(root.kind));
            hash_field(&mut hasher, root.object_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, root.head_version_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, &root.deleted_at_unix.to_string());
            hash_field(
                &mut hasher,
                &root
                    .retired_at_unix
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            );
            hash_field(
                &mut hasher,
                &root
                    .purged_at_unix
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            );
            hash_field(&mut hasher, trash_state(root.state));
        }
    }

    for state in [
        n1_indexdb::ObjectState::Live,
        n1_indexdb::ObjectState::Trash,
        n1_indexdb::ObjectState::Deleted,
    ] {
        let mut after = None;
        loop {
            let rows = db
                .n1_list_object_heads(state, None, after.as_deref(), 1000)
                .map_err(|err| {
                    AppError::InternalServerError(format!("object head hash scan: {err}"))
                })?;
            if rows.is_empty() {
                break;
            }
            after = rows.last().map(|head| head.object_key.clone());
            for head in rows {
                hash_field(&mut hasher, "head");
                hash_field(&mut hasher, &head.object_id);
                hash_field(&mut hasher, &head.object_key);
                hash_field(&mut hasher, &head.head_version_id);
                hash_field(&mut hasher, &head.owner_uid);
                hash_field(&mut hasher, &head.content_type);
                hash_field(&mut hasher, &head.total_size_bytes.to_string());
                hash_field(&mut hasher, object_state(head.state));
                hash_field(&mut hasher, &head.generation.to_string());
                hash_field(&mut hasher, &head.created_at_unix.to_string());
                hash_field(&mut hasher, &head.updated_at_unix.to_string());
            }
        }
    }

    Ok(hasher.finalize().into())
}

fn namespace_kind(kind: n1_indexdb::NamespaceEntryKind) -> &'static str {
    match kind {
        n1_indexdb::NamespaceEntryKind::File => "file",
        n1_indexdb::NamespaceEntryKind::Directory => "directory",
    }
}

fn trash_state(state: n1_indexdb::TrashEntryState) -> &'static str {
    match state {
        n1_indexdb::TrashEntryState::Trashed => "trashed",
        n1_indexdb::TrashEntryState::Retired => "retired",
        n1_indexdb::TrashEntryState::Purged => "purged",
    }
}

fn object_state(state: n1_indexdb::ObjectState) -> &'static str {
    match state {
        n1_indexdb::ObjectState::Live => "live",
        n1_indexdb::ObjectState::Trash => "trash",
        n1_indexdb::ObjectState::Deleted => "deleted",
    }
}

fn hash_field(hasher: &mut Sha256, value: &str) {
    hasher.update(value.as_bytes());
    hasher.update([0]);
}

async fn copy_namespace_db_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let temp_path = parent.join(format!(
            ".{}.tmp.{}",
            destination
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(INDEXDB_FILE),
            Ulid::new()
        ));
        remove_indexdb_file_set(&temp_path)?;
        let db = open_n1_indexdb(&source).map_err(|err| std::io::Error::other(err.to_string()))?;
        db.backup_to_path(&temp_path)
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        remove_indexdb_file_set(&destination)?;
        std::fs::rename(&temp_path, &destination)?;
        remove_indexdb_file_set(&temp_path)?;
        Ok(())
    })
    .await
    .unwrap_or_else(|err| Err(std::io::Error::other(err.to_string())))
}

fn remove_indexdb_file_set(path: &Path) -> std::io::Result<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(INDEXDB_FILE);
    for candidate in [
        path.to_path_buf(),
        path.with_file_name(format!("{file_name}-wal")),
        path.with_file_name(format!("{file_name}-shm")),
    ] {
        match std::fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}
