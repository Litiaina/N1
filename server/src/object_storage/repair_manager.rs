use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::recovery::{repair_entire_shard, verify_shard_integrity};
use crate::structs::object_storage_structs::INDEX_STORE_DISKS;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use axum::{extract::Query, http::StatusCode, response::IntoResponse};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, File};
use tracing::{error, info, warn};
use walkdir::WalkDir;

pub async fn run_repair_job(dry_run: bool) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();

    let index_roots_config = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;

    let index_roots: Vec<PathBuf> = index_roots_config
        .iter()
        .map(|s| PathBuf::from(s).join(OBJECT_DATA_ROOT))
        .collect();
    let primary_index_root =
        index_roots
            .iter()
            .find(|p| p.exists())
            .ok_or(AppError::InternalServerError(
                "No index disk available".into(),
            ))?;

    let blob_roots = path_manager.get_blob_roots();
    let num_disks = blob_roots.len();

    info!(
        "Starting Storage Repair Job (Source: {:?})",
        primary_index_root
    );

    let mut user_entries = fs::read_dir(primary_index_root)
        .await
        .map_err(|e| AppError::Io(e, "read index root".into()))?;

    while let Ok(Some(user_entry)) = user_entries.next_entry().await {
        let user_path = user_entry.path();
        if !user_path.is_dir() {
            continue;
        }

        let mut frag_entries = fs::read_dir(user_path)
            .await
            .map_err(|e| AppError::Io(e, "read user root".into()))?;
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

            let blob_roots_clone = blob_roots.clone();

            for meta_path in meta_files {
                if let Ok(data) = fs::read(&meta_path).await {
                    if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                        if meta.is_delete_marker {
                            continue;
                        }

                        if let Err(e) = repair_metadata_mirrors(&meta, &data, dry_run).await {
                            error!(
                                "Failed to sync metadata mirrors for {}/{}: {}",
                                meta.fragment, meta.object_key, e
                            );
                        }

                        process_object_repair(meta, &blob_roots_clone, num_disks, dry_run).await?;
                    }
                }
            }
        }
    }

    info!("Storage Repair Job Complete.");
    Ok(())
}

async fn repair_metadata_mirrors(
    meta: &ObjectMetadata,
    data: &[u8],
    dry_run: bool,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();

    let all_paths = path_manager.get_all_metadata_paths(
        &meta.owner_uid,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
    );

    for path in all_paths {
        let mut needs_repair = false;

        match fs::metadata(&path).await {
            Ok(file_meta) => {
                if file_meta.len() != data.len() as u64 {
                    warn!(
                        "Metadata mismatch on mirror (size diff): {:?}. Marking for overwrite.",
                        path
                    );
                    needs_repair = true;
                }
            }
            Err(_) => {
                needs_repair = true;
            }
        }

        if needs_repair {
            info!("Repairing metadata on mirror: {:?}", path);

            if !dry_run {
                if let Some(parent) = path.parent() {
                    if !parent.exists() {
                        fs::create_dir_all(parent)
                            .await
                            .map_err(|e| AppError::Io(e, "create meta dir".into()))?;
                    }
                }

                fs::write(&path, data)
                    .await
                    .map_err(|e| AppError::Io(e, "write meta mirror".into()))?;
            }
        }
    }

    Ok(())
}

async fn process_object_repair(
    meta: ObjectMetadata,
    blob_roots: &[PathBuf],
    num_disks: usize,
    dry_run: bool,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let meta_arc = Arc::new(meta);

    for part in &meta_arc.parts {
        let part_arc = Arc::new(part.clone());
        let mut missing_shards = Vec::new();

        for shard in &part.shards {
            let disk_idx = (part.part_index as usize + shard.shard_index) % num_disks;
            if disk_idx >= blob_roots.len() {
                error!("Config Mismatch: Disk index out of bounds.");
                continue;
            }

            let disk_root = &blob_roots[disk_idx];
            let shard_dir = path_manager.get_shard_dir_on_disk(
                disk_root,
                &meta_arc.owner_uid,
                &meta_arc.fragment,
                &meta_arc.version_id,
            );
            let shard_path = shard_dir.join(&shard.file_name);

            let mut is_healthy = false;
            if fs::try_exists(&shard_path).await.unwrap_or(false) {
                if let Ok(mut f) = File::open(&shard_path).await {
                    if verify_shard_integrity(&mut f, &shard.checksum).await {
                        is_healthy = true;
                    } else {
                        warn!("Corrupt shard detected: {:?}", shard_path);
                    }
                }
            }

            if !is_healthy {
                missing_shards.push(shard.shard_index);
            }
        }

        if missing_shards.is_empty() {
            continue;
        }

        info!(
            "Object {}/{} Part {} is degraded. Missing shards: {:?}. Initiating repair...",
            meta_arc.fragment, meta_arc.object_key, part.part_index, missing_shards
        );

        if dry_run {
            info!("Dry Run: Skipping actual repair.");
            continue;
        }

        for missing_idx in missing_shards {
            let part_for_task = part_arc.clone();

            if let Err(e) = repair_entire_shard(part_for_task, missing_idx).await {
                error!(
                    "Failed to repair shard {} for object {}/{}: {}",
                    missing_idx, meta_arc.fragment, meta_arc.object_key, e
                );
            } else {
                info!("Successfully repaired shard index {}", missing_idx);
            }
        }
    }

    Ok(())
}

pub async fn trigger_repair_job_api(
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let dry_run = params.get("dry_run").map(|v| v == "true").unwrap_or(false);

    tokio::spawn(async move {
        if let Err(e) = run_repair_job(dry_run).await {
            error!("Manual Repair Job Failed: {}", e);
        }
    });

    (StatusCode::ACCEPTED, "Storage repair job initiated.")
}
