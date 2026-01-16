use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::invalidate_cache;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, PARITY_SUFFIX, SHARD_SUFFIX};
use crate::object_storage::ec::ErasureStreamWriter;
use crate::object_storage::recovery::read_or_recover_chunk;
use crate::object_storage::upload::{
    DATA_SHARDS_ATOMIC, DirectWriter, PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC, create_direct_file,
};
use crate::structs::object_storage_structs::INDEX_STORE_DISKS;
use crate::structs::object_storage_structs::{
    ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo, default_stripe_size,
};
use axum::{extract::Query, http::StatusCode, response::IntoResponse};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::fs as tokio_fs;
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

pub async fn trigger_redistribution_job_api(
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let dry_run = params.get("dry_run").map(|v| v == "true").unwrap_or(false);

    info!(
        "API received request to trigger redistribution job (dry_run={})",
        dry_run
    );

    tokio::spawn(async move {
        if let Err(e) = run_redistribute_job(dry_run).await {
            error!("Manual Redistribution Job Failed: {}", e);
        }
    });

    (
        StatusCode::ACCEPTED,
        "Storage redistribution job initiated.",
    )
}

pub async fn run_redistribute_job(dry_run: bool) -> Result<(), AppError> {
    let start_time = Instant::now();
    let path_manager = ObjectPathManager::new();
    let target_data_shards = DATA_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize;
    let target_parity_shards = PARITY_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize;
    let target_stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);
    let target_blob_roots = path_manager.get_blob_roots();
    let target_num_disks = target_blob_roots.len();

    let index_roots_config = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError("Index Config missing".into()))?;

    let mut active_index_root = None;
    for root in index_roots_config {
        let path = PathBuf::from(root).join(OBJECT_DATA_ROOT);
        if path.exists() {
            active_index_root = Some(path);
            break;
        }
    }
    let index_root = match active_index_root {
        Some(path) => path,
        None => {
            warn!("No active index root found for redistribution scan.");
            return Ok(());
        }
    };

    info!(
        "Starting Redistribution Scan. Config: {}D+{}P, Stripe: {} bytes. Index Root: {:?}, Blob Disks: {}",
        target_data_shards, target_parity_shards, target_stripe_size, index_root, target_num_disks
    );

    let mut user_entries = match tokio_fs::read_dir(&index_root).await {
        Ok(e) => e,
        Err(e) => {
            error!("Failed to read index root: {}", e);
            return Ok(());
        }
    };

    let mut stats_users = 0;
    let mut stats_scanned = 0;
    let mut stats_migrated = 0;
    let mut stats_errors = 0;

    while let Ok(Some(user_entry)) = user_entries.next_entry().await {
        let user_path = user_entry.path();
        if !user_path.is_dir() {
            continue;
        }
        stats_users += 1;

        let mut frag_entries = match tokio_fs::read_dir(&user_path).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(frag_entry)) = frag_entries.next_entry().await {
            let index_dir = frag_entry.path().join("index");
            if !index_dir.exists() {
                continue;
            }

            let (tx, mut rx) = tokio::sync::mpsc::channel(100);

            let index_dir_clone = index_dir.clone();
            tokio::task::spawn_blocking(move || {
                let walker = WalkDir::new(index_dir_clone).into_iter();
                for entry in walker.filter_map(|e| e.ok()) {
                    if entry.file_type().is_file()
                        && entry.path().extension().map_or(false, |ext| ext == "bson")
                    {
                        if tx.blocking_send(entry.path().to_owned()).is_err() {
                            break;
                        }
                    }
                }
            });

            while let Some(meta_path) = rx.recv().await {
                if let Ok(data) = tokio_fs::read(&meta_path).await {
                    if let Ok(mut meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                        stats_scanned += 1;

                        if meta.is_delete_marker {
                            continue;
                        }

                        let needs = check_needs_redistribution(
                            &meta,
                            target_data_shards,
                            target_parity_shards,
                            target_stripe_size,
                            target_num_disks,
                            &target_blob_roots,
                        );

                        if needs {
                            if !dry_run {
                                info!(
                                    "Migrating object: {}/{} (v: {})",
                                    meta.fragment, meta.object_key, meta.version_id
                                );
                                if let Err(e) = redistribute_object(
                                    &mut meta,
                                    target_data_shards,
                                    target_parity_shards,
                                    target_stripe_size,
                                    &target_blob_roots,
                                )
                                .await
                                {
                                    error!(
                                        "Failed to redistribute {}/{}: {}",
                                        meta.fragment, meta.object_key, e
                                    );
                                    stats_errors += 1;
                                } else {
                                    stats_migrated += 1;
                                }
                            } else {
                                info!(
                                    "[DRY RUN] Would redistribute {}/{} (Reason: configuration mismatch)",
                                    meta.fragment, meta.object_key
                                );
                                stats_migrated += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    info!(
        "Redistribution Job Completed in {:.2?}. Users: {}, Objects Scanned: {}, Migrated/Pending: {}, Errors: {}",
        start_time.elapsed(),
        stats_users,
        stats_scanned,
        stats_migrated,
        stats_errors
    );
    Ok(())
}

fn check_needs_redistribution(
    meta: &ObjectMetadata,
    target_data: usize,
    target_parity: usize,
    target_stripe_size: usize,
    target_num_disks: usize,
    target_roots: &[PathBuf],
) -> bool {
    if meta.parts.is_empty() {
        return false;
    }

    if meta.stripe_size_bytes != target_stripe_size {
        debug!(
            "Object {} needs redistribution: Stripe size mismatch (Current: {}, Target: {})",
            meta.object_key, meta.stripe_size_bytes, target_stripe_size
        );
        return true;
    }

    let part0 = &meta.parts[0];
    let current_data = part0.shards.iter().filter(|s| !s.is_parity).count();
    let current_parity = part0.shards.iter().filter(|s| s.is_parity).count();

    if current_data != target_data || current_parity != target_parity {
        debug!(
            "Object {} needs redistribution: EC config mismatch (Current: {}D+{}P, Target: {}D+{}P)",
            meta.object_key, current_data, current_parity, target_data, target_parity
        );
        return true;
    }

    for part in &meta.parts {
        for shard in &part.shards {
            let expected_idx = (part.part_index as usize + shard.shard_index) % target_num_disks;
            if expected_idx >= target_roots.len() {
                return true;
            }
            let expected_root = target_roots[expected_idx].to_string_lossy();
            if !shard.absolute_path.starts_with(expected_root.as_ref()) {
                debug!(
                    "Object {} needs redistribution: Placement mismatch (Shard {} on wrong disk)",
                    meta.object_key, shard.shard_index
                );
                return true;
            }
        }
    }
    false
}

async fn redistribute_object(
    meta: &mut ObjectMetadata,
    target_data_shards: usize,
    target_parity_shards: usize,
    target_stripe_size: usize,
    target_blob_roots: &[PathBuf],
) -> Result<(), AppError> {
    let start_time = Instant::now();
    let path_manager = ObjectPathManager::new();
    let num_disks = target_blob_roots.len();
    let mut file_cache = HashMap::new();
    let old_block_size = meta.stripe_size_bytes;
    let new_block_size = target_stripe_size;

    let old_parts = std::mem::take(&mut meta.parts);
    let mut new_parts = Vec::with_capacity(old_parts.len());
    let mut shards_to_cleanup = Vec::new();

    for old_part in old_parts {
        let old_part_arc = Arc::new(old_part);
        let old_data_count = old_part_arc.shards.iter().filter(|s| !s.is_parity).count() as u64;
        let old_stripe_size = (old_block_size as u64) * old_data_count;
        let new_stripe_width = (new_block_size * target_data_shards) as u64;
        let new_stripe_count = (old_part_arc.size_bytes + new_stripe_width - 1) / new_stripe_width;
        let new_shard_len = new_stripe_count * new_block_size as u64;

        let mut writers: Vec<Option<DirectWriter>> =
            Vec::with_capacity(target_data_shards + target_parity_shards);
        let mut new_shard_infos = Vec::new();
        let mut temp_paths = Vec::new();

        for i in 0..(target_data_shards + target_parity_shards) {
            let is_parity = i >= target_data_shards;
            let ext = if is_parity {
                PARITY_SUFFIX
            } else {
                SHARD_SUFFIX
            };
            let file_name = format!("part_{}_shard_{}{}", old_part_arc.part_index, i, ext);
            let disk_idx = (old_part_arc.part_index as usize + i) % num_disks;
            let target_disk = &target_blob_roots[disk_idx];

            if !target_disk.exists() {
                writers.push(None);
                continue;
            }

            let shard_dir = path_manager.get_shard_dir_on_disk(
                target_disk,
                &meta.owner_uid,
                &meta.fragment,
                &meta.version_id,
            );
            if !shard_dir.exists() {
                let _ = tokio_fs::create_dir_all(&shard_dir).await;
            }
            let final_path = shard_dir.join(&file_name);
            let temp_path = shard_dir.join(format!("{}.tmp", file_name));

            match create_direct_file(&temp_path).await {
                Ok(file) => {
                    let file_clone = file
                        .try_clone()
                        .map_err(|e| AppError::Io(e, "clone".into()))?;
                    if let Ok(_) =
                        tokio::task::spawn_blocking(move || file_clone.set_len(new_shard_len)).await
                    {
                        writers.push(Some(DirectWriter::new(file)));
                        temp_paths.push((temp_path, final_path.clone()));
                        new_shard_infos.push(ShardInfo {
                            shard_index: i,
                            file_name,
                            absolute_path: final_path.to_string_lossy().to_string(),
                            size_bytes: new_shard_len,
                            checksum: "".to_string(),
                            is_parity,
                        });
                    } else {
                        writers.push(None);
                    }
                }
                Err(_) => writers.push(None),
            }
        }

        let mut stream_writer = ErasureStreamWriter::new(
            writers,
            target_data_shards,
            target_parity_shards,
            new_shard_len as usize,
            true,
            new_block_size,
        );
        let mut pos = 0u64;
        let mut batch_buffer = Vec::with_capacity(new_block_size);

        while pos < old_part_arc.size_bytes {
            let relative_pos = pos;
            let stripe_index = relative_pos / old_stripe_size;
            let offset_in_stripe = relative_pos % old_stripe_size;
            let shard_index = (offset_in_stripe / (old_block_size as u64)) as usize;
            let offset_in_shard_block = offset_in_stripe % (old_block_size as u64);
            let file_offset = (stripe_index * (old_block_size as u64)) + offset_in_shard_block;
            let to_read = ((old_block_size as u64 - offset_in_shard_block)
                .min(old_part_arc.size_bytes - pos)) as usize;
            if to_read == 0 {
                break;
            }

            let chunk_data = read_or_recover_chunk(
                &mut file_cache,
                &PathBuf::new(),
                old_part_arc.clone(),
                shard_index,
                file_offset,
                to_read,
                old_block_size,
            )
            .await?;
            batch_buffer.extend_from_slice(&chunk_data);
            if batch_buffer.len() >= new_block_size
                || (pos + to_read as u64) >= old_part_arc.size_bytes
            {
                stream_writer.write(&batch_buffer).await?;
                batch_buffer.clear();
            }
            pos += to_read as u64;
        }

        stream_writer.finalize().await?;
        let checksums = stream_writer.get_shard_checksums();

        for (i, (tmp, final_p)) in temp_paths.iter().enumerate() {
            if let Some(sum) = checksums.get(i) {
                new_shard_infos[i].checksum = sum.clone();
            }
            if let Err(e) = tokio_fs::rename(tmp, final_p).await {
                error!(
                    "Failed to commit migrated shard {}: {}",
                    final_p.display(),
                    e
                );
                return Err(AppError::Io(e, "rename migrated shard".into()));
            }
        }

        let new_part = PartMetadata {
            part_index: old_part_arc.part_index,
            size_bytes: old_part_arc.size_bytes,
            created_at: old_part_arc.created_at,
            checksum: old_part_arc.checksum.clone(),
            shards: new_shard_infos,
        };
        new_parts.push(new_part);

        let new_paths: HashSet<String> = new_parts
            .last()
            .unwrap()
            .shards
            .iter()
            .map(|s| s.absolute_path.clone())
            .collect();
        for old_shard in &old_part_arc.shards {
            if !new_paths.contains(&old_shard.absolute_path) {
                shards_to_cleanup.push(old_shard.absolute_path.clone());
            }
        }
        file_cache.clear();
    }

    meta.parts = new_parts;
    meta.stripe_size_bytes = new_block_size;
    let bson_data =
        mongodb::bson::to_vec(&*meta).map_err(|_| AppError::InternalServerError("BSON".into()))?;

    let all_paths = path_manager.get_all_metadata_paths(
        &meta.owner_uid,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
    );
    for path in all_paths {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                let _ = tokio_fs::create_dir_all(parent).await;
            }
        }
        let tmp = path.with_extension("tmp");
        if let Ok(_) = tokio_fs::write(&tmp, &bson_data).await {
            let _ = tokio_fs::rename(&tmp, &path).await;
        }
    }

    invalidate_cache(&meta.fragment, &meta.object_key, &meta.owner_uid).await;

    let mut cleaned_count = 0;
    for path in shards_to_cleanup {
        if let Ok(_) = tokio_fs::remove_file(&path).await {
            cleaned_count += 1;
        }
    }

    info!(
        "Redistributed object {}/{} successfully in {:.2?}. Cleaned {} old shards.",
        meta.fragment,
        meta.object_key,
        start_time.elapsed(),
        cleaned_count
    );

    Ok(())
}
