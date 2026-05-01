use crate::core::fragment_metadata::fragment_index_partition_count;
use crate::core::indexdb_store::open_n1_indexdb;
use crate::core::indexdb_store::read_partition_meta_exact;
use crate::enums::app_error_enum::AppError;

use indexdb::Database;
use indexdb::n1 as n1_indexdb;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs;

pub const INDEXDB_DIR: &str = "indexdb";
pub const INDEXDB_FILE: &str = "index.ndxdb";
const INDEXDB_SHARD_FILE_PREFIX: &str = "shard_";

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
    generation: u64,
    metadata_hash: Option<[u8; 32]>,
    valid: bool,
}

pub fn namespace_db_path_for_root(root: &Path, fragment: &str) -> PathBuf {
    namespace_shard_db_path_for_root(root, fragment, 0)
}

fn namespace_shard_file(shard_id: u16) -> String {
    format!("{}{:04x}.ndxdb", INDEXDB_SHARD_FILE_PREFIX, shard_id)
}

fn namespace_shard_db_path_for_root(root: &Path, fragment: &str, shard_id: u16) -> PathBuf {
    root.join(fragment)
        .join(INDEXDB_DIR)
        .join(namespace_shard_file(shard_id))
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
    let shard_count = fragment_index_partition_count(fragment)?;
    let mut inspection = NamespaceReplicaInspection {
        fragment: fragment.to_string(),
        healthy: true,
        ..NamespaceReplicaInspection::default()
    };
    for shard_id in 0..shard_count {
        let replicas = read_namespace_shard_replicas(fragment, shard_id, index_roots).await;
        if replicas.iter().all(|replica| !replica.present) {
            continue;
        }
        let best = choose_best_namespace_replica(&replicas);
        let shard = build_inspection(fragment, &replicas, best);
        inspection.expected_replicas += shard.expected_replicas;
        inspection.present_replicas += shard.present_replicas;
        inspection.valid_replicas += shard.valid_replicas;
        inspection.missing_replicas += shard.missing_replicas;
        inspection.divergent_replicas += shard.divergent_replicas;
        inspection.corrupt_replicas += shard.corrupt_replicas;
        inspection.healthy &= shard.healthy;
        if inspection.canonical_path.is_none() {
            inspection.canonical_path = shard.canonical_path;
        }
    }
    Ok(inspection)
}

pub async fn repair_namespace_replicas(
    fragment: &str,
    index_roots: &[PathBuf],
    dry_run: bool,
) -> Result<NamespaceReplicaRepairReport, AppError> {
    let mut report = NamespaceReplicaRepairReport {
        fragment: fragment.to_string(),
        ..NamespaceReplicaRepairReport::default()
    };
    let shard_count = fragment_index_partition_count(fragment)?;
    for shard_id in 0..shard_count {
        let replicas = read_namespace_shard_replicas(fragment, shard_id, index_roots).await;
        if replicas.iter().all(|replica| !replica.present) {
            continue;
        }
        report.expected_replicas += replicas.len() as u64;
        let Some(best_index) = choose_best_namespace_replica(&replicas) else {
            report.corrupt_found += replicas
                .iter()
                .filter(|replica| replica.present && !replica.valid)
                .count() as u64;
            continue;
        };

        let canonical_hash = replicas[best_index].metadata_hash;
        let canonical_generation = replicas[best_index].generation;
        let source_path = replicas[best_index].path.clone();
        if report.selected_source.is_none() {
            report.selected_source = Some(source_path.to_string_lossy().to_string());
        }

        for replica in replicas {
            if !replica.present {
                report.missing_repaired += 1;
                if !dry_run {
                    copy_namespace_replica_file(&source_path, &replica.path)?;
                }
                continue;
            }

            if !replica.valid {
                report.corrupt_found += 1;
                if !dry_run {
                    copy_namespace_replica_file(&source_path, &replica.path)?;
                }
                continue;
            }
            if replica.metadata_hash != canonical_hash || replica.generation != canonical_generation
            {
                report.divergent_repaired += 1;
                if !dry_run {
                    copy_namespace_replica_file(&source_path, &replica.path)?;
                }
            }
        }
    }

    Ok(report)
}

async fn read_namespace_shard_replicas(
    fragment: &str,
    shard_id: u16,
    index_roots: &[PathBuf],
) -> Vec<RawNamespaceReplica> {
    let mut replicas = Vec::with_capacity(index_roots.len());
    for root in index_roots {
        replicas.push(
            read_namespace_replica(namespace_shard_db_path_for_root(root, fragment, shard_id))
                .await,
        );
    }
    replicas
}

async fn read_namespace_replica(path: PathBuf) -> RawNamespaceReplica {
    match fs::metadata(&path).await {
        Ok(meta) => {
            let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta.len();
            let inspection = namespace_indexdb_semantic_hash(path.clone()).await;
            RawNamespaceReplica {
                path,
                present: true,
                mtime,
                len,
                generation: inspection
                    .as_ref()
                    .map(|(_, generation)| *generation)
                    .unwrap_or(0),
                metadata_hash: inspection.map(|(hash, _)| hash),
                valid: inspection.is_some(),
            }
        }
        Err(_) => RawNamespaceReplica {
            path,
            present: false,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            generation: 0,
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
                replica.generation > current.generation
                    || (replica.generation == current.generation && replica.len > current.len)
                    || (replica.generation == current.generation
                        && replica.len == current.len
                        && replica.mtime > current.mtime)
                    || (replica.mtime == current.mtime
                        && replica.len == current.len
                        && replica.generation == current.generation
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

async fn namespace_indexdb_semantic_hash(path: PathBuf) -> Option<([u8; 32], u64)> {
    tokio::task::spawn_blocking(move || {
        let db = open_n1_indexdb(&path).ok()?;
        let generation = read_partition_meta_exact(&db)
            .ok()
            .flatten()
            .map(|meta| meta.committed_generation)
            .unwrap_or(0);
        namespace_indexdb_semantic_hash_blocking(&db)
            .ok()
            .map(|hash| (hash, generation))
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
            if let Some(head) = db
                .n1_get_namespace_file_head(&node.node_id)
                .map_err(|err| {
                    AppError::InternalServerError(format!("namespace file head hash scan: {err}"))
                })?
            {
                hash_field(&mut hasher, "namespace-file-head");
                hash_field(&mut hasher, &head.node_id);
                hash_field(&mut hasher, &head.object_id);
                hash_field(&mut hasher, &head.version_id);
                hash_field(&mut hasher, &head.upload_session_id);
                hash_field(&mut hasher, &head.owner_uid);
                hash_field(&mut hasher, &head.content_type);
                hash_field(&mut hasher, &head.total_size_bytes.to_string());
                hash_field(&mut hasher, &head.created_at_unix.to_string());
                hash_field(&mut hasher, if head.is_delete_marker { "1" } else { "0" });
                hash_field(&mut hasher, &head.stripe_size_bytes.to_string());
                hash_field(&mut hasher, &head.updated_at_unix.to_string());
            }
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

fn hash_field(hasher: &mut Sha256, value: &str) {
    hasher.update(value.as_bytes());
    hasher.update([0]);
}

fn copy_namespace_replica_file(source: &Path, destination: &Path) -> Result<(), AppError> {
    if source == destination {
        return Ok(());
    }
    let parent = destination.parent().ok_or_else(|| {
        AppError::InternalServerError(format!(
            "namespace destination '{}' has no parent",
            destination.display()
        ))
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|err| AppError::Io(err, format!("mkdir {}", parent.display())))?;
    let file_name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("shard.ndxdb");
    let temp_path = parent.join(format!(".{}.tmp-{}", file_name, ulid::Ulid::new()));
    remove_indexdb_file_set(destination)?;
    remove_indexdb_file_set(&temp_path)?;
    let source_db = open_n1_indexdb(source)?;
    source_db
        .backup_to_path(&temp_path)
        .map_err(|err| AppError::InternalServerError(format!("namespace replica backup: {err}")))?;
    std::fs::rename(&temp_path, destination).map_err(|err| {
        AppError::Io(
            err,
            format!(
                "rename {} -> {}",
                temp_path.display(),
                destination.display()
            ),
        )
    })?;
    remove_indexdb_file_set(&temp_path)?;
    Ok(())
}

fn remove_indexdb_file_set(path: &Path) -> Result<(), AppError> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("shard.ndxdb");
    for candidate in [
        path.to_path_buf(),
        path.with_file_name(format!("{}-wal", file_name)),
        path.with_file_name(format!("{}-shm", file_name)),
    ] {
        match std::fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(AppError::Io(err, format!("remove {}", candidate.display()))),
        }
    }
    Ok(())
}
