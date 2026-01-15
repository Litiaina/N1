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
use tokio::fs as tokio_fs;
use tracing::{error, info, warn};
use walkdir::WalkDir;

pub async fn run_redistribute_job(dry_run: bool) -> Result<(), AppError> {
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
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;

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
            warn!("No accessible index roots found. Redistribution job skipped.");
            return Ok(());
        }
    };

    info!(
        "Starting Redistribution Job. Target: {}D+{}P | Stripe: {} bytes | Disks: {}",
        target_data_shards, target_parity_shards, target_stripe_size, target_num_disks
    );

    let mut user_entries = tokio_fs::read_dir(&index_root)
        .await
        .map_err(|e| AppError::Io(e, "read index root".into()))?;

    while let Ok(Some(user_entry)) = user_entries.next_entry().await {
        let user_path = user_entry.path();
        if !user_path.is_dir() {
            continue;
        }

        let mut frag_entries = match tokio_fs::read_dir(&user_path).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(frag_entry)) = frag_entries.next_entry().await {
            let index_dir = frag_entry.path().join("index");
            if !index_dir.exists() {
                continue;
            }

            let meta_files: Vec<PathBuf> = tokio::task::spawn_blocking(move || {
                WalkDir::new(index_dir)
                    .into_iter()
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.file_type().is_file()
                            && e.path().extension().map_or(false, |ext| ext == "bson")
                    })
                    .map(|e| e.path().to_owned())
                    .collect()
            })
            .await
            .map_err(|e| AppError::InternalServerError(format!("WalkDir failed: {}", e)))?;

            for meta_path in meta_files {
                if let Ok(data) = tokio_fs::read(&meta_path).await {
                    if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                        if meta.is_delete_marker {
                            continue;
                        }

                        let needs_update = check_needs_redistribution(
                            &meta,
                            target_data_shards,
                            target_parity_shards,
                            target_stripe_size,
                            target_num_disks,
                            &target_blob_roots,
                        );

                        if needs_update {
                            info!(
                                "Redistributing object: {}/{}",
                                meta.fragment, meta.object_key
                            );
                            if !dry_run {
                                let frag = meta.fragment.clone();
                                let key = meta.object_key.clone();
                                if let Err(e) = redistribute_object(
                                    meta,
                                    target_data_shards,
                                    target_parity_shards,
                                    target_stripe_size,
                                    &target_blob_roots,
                                )
                                .await
                                {
                                    error!("Failed to redistribute object {}/{}: {}", frag, key, e);
                                }
                            } else {
                                info!(
                                    "[DRY RUN] Would redistribute {}/{}",
                                    meta.fragment, meta.object_key
                                );
                            }
                        } else {
                            if !dry_run {
                                if let Err(e) = sync_metadata_to_mirrors(&meta, &data).await {
                                    error!("Failed to sync metadata mirrors: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    info!("Redistribution Job Complete.");
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
        return true;
    }

    let part0 = &meta.parts[0];
    let current_data = part0.shards.iter().filter(|s| !s.is_parity).count();
    let current_parity = part0.shards.iter().filter(|s| s.is_parity).count();

    if current_data != target_data || current_parity != target_parity {
        return true;
    }

    for part in &meta.parts {
        for shard in &part.shards {
            let expected_disk_idx =
                (part.part_index as usize + shard.shard_index) % target_num_disks;

            if expected_disk_idx >= target_roots.len() {
                return true;
            }

            let expected_root_str = target_roots[expected_disk_idx].to_string_lossy();
            if !shard.absolute_path.starts_with(expected_root_str.as_ref()) {
                return true;
            }
        }
    }

    false
}

async fn redistribute_object(
    mut meta: ObjectMetadata,
    target_data_shards: usize,
    target_parity_shards: usize,
    target_stripe_size: usize,
    target_blob_roots: &[PathBuf],
) -> Result<(), AppError> {
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

            let shard_dir = path_manager.get_shard_dir_on_disk(
                target_disk,
                &meta.owner_uid,
                &meta.fragment,
                &meta.version_id,
            );

            if !shard_dir.exists() {
                tokio_fs::create_dir_all(&shard_dir)
                    .await
                    .map_err(|e| AppError::Io(e, "create shard dir".into()))?;
            }

            let final_path = shard_dir.join(&file_name);
            let temp_path = shard_dir.join(format!("{}.tmp", file_name));

            let file = create_direct_file(&temp_path).await?;

            let file_clone = file
                .try_clone()
                .map_err(|e| AppError::Io(e, "clone handle".into()))?;

            tokio::task::spawn_blocking(move || file_clone.set_len(new_shard_len))
                .await
                .map_err(|e| {
                    AppError::InternalServerError(format!("prealloc thread failed: {}", e))
                })?
                .map_err(|e| AppError::Io(e, "prealloc set_len".into()))?;

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

            let bytes_remaining_in_block = (old_block_size as u64) - offset_in_shard_block;
            let bytes_remaining_in_part = old_part_arc.size_bytes - pos;

            let to_read = bytes_remaining_in_block.min(bytes_remaining_in_part) as usize;

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
            tokio::task::yield_now().await;
        }

        stream_writer.finalize().await?;

        let checksums = stream_writer.get_shard_checksums();
        for (i, (tmp, final_p)) in temp_paths.iter().enumerate() {
            if let Some(sum) = checksums.get(i) {
                new_shard_infos[i].checksum = sum.clone();
            }
            tokio_fs::rename(tmp, final_p)
                .await
                .map_err(|e| AppError::Io(e, "rename new shard".into()))?;
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

    let bson_data = mongodb::bson::to_vec(&meta)
        .map_err(|_| AppError::InternalServerError("BSON serialization failed".into()))?;

    if let Err(e) = sync_metadata_to_mirrors(&meta, &bson_data).await {
        error!("Failed to update metadata after redistribution: {}", e);
        return Err(e);
    }

    invalidate_cache(&meta.fragment, &meta.object_key, &meta.owner_uid).await;

    for path in shards_to_cleanup {
        let _ = tokio_fs::remove_file(&path).await;
    }

    Ok(())
}

async fn sync_metadata_to_mirrors(meta: &ObjectMetadata, data: &[u8]) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let all_paths = path_manager.get_all_metadata_paths(
        &meta.owner_uid,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
    );

    for path in all_paths {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio_fs::create_dir_all(parent)
                    .await
                    .map_err(|e| AppError::Io(e, "create meta dir".into()))?;
            }
        }
        let tmp = path.with_extension("tmp");
        tokio_fs::write(&tmp, data)
            .await
            .map_err(|e| AppError::Io(e, "write meta tmp".into()))?;
        tokio_fs::rename(tmp, path)
            .await
            .map_err(|e| AppError::Io(e, "rename meta".into()))?;
    }
    Ok(())
}

pub async fn trigger_redistribute_job_api(
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let dry_run = params.get("dry_run").map(|v| v == "true").unwrap_or(false);

    tokio::spawn(async move {
        if let Err(e) = run_redistribute_job(dry_run).await {
            error!("Manual Redistribution Job Failed: {}", e);
        }
    });

    (StatusCode::ACCEPTED, "Redistribution job initiated.")
}
