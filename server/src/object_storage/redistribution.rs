use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{
    GC_SUFFIX, META_TMP_EXT, METADATA_EXT, PARITY_SUFFIX, PART_META_SUFFIX, QUOTA_FILE,
    REDIST_TMP_SUFFIX, RESERVATION_FILE, SHARD_SUFFIX,
};
use crate::object_storage::disk_manager::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo,
    default_stripe_size,
};
use crate::object_storage::ec::ErasureStreamWriter;
use crate::object_storage::recovery::read_or_recover_chunk;
use crate::object_storage::upload::{
    DATA_SHARDS_ATOMIC, DirectWriter, PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC, create_direct_file,
    sync_dir,
};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};

use chrono::Utc;
use futures_util::stream::{self, Stream};
use serde::Serialize;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use tokio::fs as tokio_fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Default)]
pub struct RedistributionJobStatus {
    pub is_running: bool,
    pub phase: String,

    pub total_fragments_scanned: u64,
    pub total_objects_scanned: u64,
    pub total_objects_need_redistribution: u64,
    pub total_objects_migrated: u64,
    pub total_errors: u64,

    pub current_fragment: String,
    pub last_object: Option<String>,
    pub last_error: Option<String>,

    pub start_timestamp: i64,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
}

static REDIST_STATUS: LazyLock<Arc<tokio::sync::RwLock<RedistributionJobStatus>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(RedistributionJobStatus::default())));

static STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

#[inline]
fn should_stop() -> bool {
    STOP_SIGNAL.load(Ordering::Relaxed)
}

pub async fn get_redist_status() -> RedistributionJobStatus {
    REDIST_STATUS.read().await.clone()
}

pub async fn monitor_redistribution_job_api(
    claims: Claims,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    let stream = stream::unfold((), |()| async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let state = REDIST_STATUS.read().await.clone();

        let event = Event::default()
            .json_data(state)
            .unwrap_or_else(|_| Event::default().data("serialization error"));
        Some((Ok(event), ()))
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

pub async fn trigger_redistribution_job_api(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permissions required".into(),
        ));
    }

    {
        let st = REDIST_STATUS.read().await;
        if st.is_running {
            return Err(AppError::Conflict("Job already running".into()));
        }
    }

    let dry_run = params.get("dry_run").map(|v| v == "true").unwrap_or(false);
    let ws_manager = Arc::new(state.ws_manager.clone());
    let ws_manger_clone = ws_manager.clone();

    tokio::spawn(async move {
        if let Err(e) = run_redistribution_job(dry_run, ws_manager).await {
            error!("Redistribution job failed: {}", e);
            let mut st = REDIST_STATUS.write().await;
            st.is_running = false;
            st.phase = "Failed".to_string();
            st.last_error = Some(e.to_string());

            ws_manger_clone
                .broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        if dry_run {
            "Redistribution initiated (dry run)."
        } else {
            "Redistribution initiated."
        },
    )
        .into_response())
}

pub async fn stop_redistribution_job_api(claims: Claims, State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permissions required".into(),
        ));
    }

    STOP_SIGNAL.store(true, Ordering::Relaxed);
    let mut st = REDIST_STATUS.write().await;
    if st.is_running {
        st.phase = "Stopping...".to_string();

        state
            .ws_manager
            .broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }
    Ok((StatusCode::OK, "Stop signal sent.").into_response())
}

fn get_shard_dir_custom(
    disk_root: &Path,
    fragment_hash: &str,
    version_id: &str,
    is_trash: bool,
) -> PathBuf {
    if is_trash {
        disk_root
            .join(fragment_hash)
            .join(".trash")
            .join("blob")
            .join(version_id)
    } else {
        disk_root.join(fragment_hash).join("blob").join(version_id)
    }
}

fn get_all_metadata_paths_custom(
    index_roots: &[PathBuf],
    fragment_hash: &str,
    object_key: &str,
    version_id: &str,
    is_trash: bool,
) -> Vec<PathBuf> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());

    let shard1 = &key_hash[0..2];
    let shard2 = &key_hash[2..4];
    let shard3 = &key_hash[4..6];
    let shard4 = &key_hash[6..8];
    let filename = format!("{}#{}{}", key_hash, version_id, METADATA_EXT);

    index_roots
        .iter()
        .map(|root| {
            if is_trash {
                root.join(fragment_hash)
                    .join(".trash")
                    .join("index")
                    .join(shard1)
                    .join(shard2)
                    .join(shard3)
                    .join(shard4)
                    .join(&filename)
            } else {
                root.join(fragment_hash)
                    .join("index")
                    .join(shard1)
                    .join(shard2)
                    .join(shard3)
                    .join(shard4)
                    .join(&filename)
            }
        })
        .collect()
}

pub async fn run_redistribution_job(
    dry_run: bool,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();

    {
        let mut st = REDIST_STATUS.write().await;
        *st = RedistributionJobStatus {
            is_running: true,
            phase: "Initializing".to_string(),
            start_timestamp: Utc::now().timestamp(),
            ..Default::default()
        };
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    let target_data_shards = DATA_SHARDS_ATOMIC
        .get()
        .ok_or(AppError::InternalServerError("DATA_SHARDS missing".into()))?
        .load(Ordering::Relaxed) as usize;

    let target_parity_shards = PARITY_SHARDS_ATOMIC
        .get()
        .ok_or(AppError::InternalServerError(
            "PARITY_SHARDS missing".into(),
        ))?
        .load(Ordering::Relaxed) as usize;

    let target_stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let num_disks = blob_roots.len();

    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No blob disks configured".into(),
        ));
    }

    let index_roots_config = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;

    let index_roots: Vec<PathBuf> = index_roots_config
        .iter()
        .map(|s| PathBuf::from(s))
        .collect();

    info!(
        "Starting TRUE Redistribution Job (Active & Trash). target={}D+{}P stripe={} bytes blob_disks={} index_roots={} dry_run={}",
        target_data_shards,
        target_parity_shards,
        target_stripe_size,
        num_disks,
        index_roots.len(),
        dry_run
    );

    let mut processed_versions: HashSet<String> = HashSet::new();

    for index_root in &index_roots {
        if should_stop() {
            break;
        }

        if !tokio_fs::try_exists(&index_root).await.unwrap_or(false) {
            warn!("Index root unreachable, skipping: {:?}", index_root);
            continue;
        }

        {
            let mut st = REDIST_STATUS.write().await;
            st.phase = format!("Scanning Root {:?}", index_root);
            ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }

        let mut frag_entries = match tokio_fs::read_dir(&index_root).await {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to read index root {:?}: {}", index_root, e);
                continue;
            }
        };

        while let Ok(Some(frag_entry)) = frag_entries.next_entry().await {
            if should_stop() {
                break;
            }

            let frag_path = frag_entry.path();
            if !frag_path.is_dir() {
                continue;
            }

            let fragment_hash = frag_entry.file_name().to_string_lossy().to_string();

            {
                let mut st = REDIST_STATUS.write().await;
                st.current_fragment = fragment_hash.clone();
                st.total_fragments_scanned += 1;
            }

            let scan_targets = vec![("index", false), (".trash/index", true)];

            for (sub_dir, is_trash) in scan_targets {
                let index_dir = frag_path.join(sub_dir);
                if !tokio_fs::try_exists(&index_dir).await.unwrap_or(false) {
                    continue;
                }

                let (tx, mut rx) = tokio::sync::mpsc::channel::<(PathBuf, bool)>(128);
                let index_dir_clone = index_dir.clone();

                tokio::task::spawn_blocking(move || {
                    let walker = WalkDir::new(index_dir_clone).into_iter();
                    for entry in walker.filter_map(|e| e.ok()) {
                        if entry.file_type().is_file() {
                            let p = entry.path();
                            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");

                            if name.ends_with(METADATA_EXT)
                                && !name.ends_with(PART_META_SUFFIX)
                                && name != QUOTA_FILE
                                && name != RESERVATION_FILE
                            {
                                if tx.blocking_send((p.to_path_buf(), is_trash)).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });

                let mut batch_scanned: u64 = 0;
                const STATUS_BATCH: u64 = 50;

                while let Some((meta_path, is_trash_flag)) = rx.recv().await {
                    if should_stop() {
                        break;
                    }

                    let raw = match tokio_fs::read(&meta_path).await {
                        Ok(d) => d,
                        Err(_) => continue,
                    };

                    let mut meta = match rmp_serde::from_slice::<ObjectMetadata>(&raw) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    if meta.is_delete_marker {
                        continue;
                    }

                    if processed_versions.contains(&meta.version_id) {
                        continue;
                    }
                    processed_versions.insert(meta.version_id.clone());

                    batch_scanned += 1;
                    if batch_scanned >= STATUS_BATCH {
                        let mut st = REDIST_STATUS.write().await;
                        st.total_objects_scanned += batch_scanned;
                        let elapsed = start_time.elapsed().as_secs_f64();
                        st.duration_seconds = elapsed as u64;
                        if elapsed > 0.0 {
                            st.scan_rate_per_sec = st.total_objects_scanned as f64 / elapsed;
                        }
                        ws_manager.broadcast_system_wide(
                            StorageEventPayload::RedistributionUpdate(st.clone()),
                        );
                        batch_scanned = 0;
                    }

                    let needs = check_needs_redistribution(
                        &meta,
                        target_data_shards,
                        target_parity_shards,
                        target_stripe_size,
                        num_disks,
                        &blob_roots,
                        &index_roots,
                        is_trash_flag,
                    )
                    .await;

                    if !needs {
                        continue;
                    }

                    {
                        let mut st = REDIST_STATUS.write().await;
                        st.total_objects_need_redistribution += 1;
                        st.last_object = Some(format!(
                            "{}/{} (v:{}) [Trash:{}]",
                            meta.fragment, meta.object_key, meta.version_id, is_trash_flag
                        ));
                    }

                    if dry_run {
                        info!(
                            "[DRY RUN] Would redistribute {}/{} (v:{}) Trash:{}",
                            meta.fragment, meta.object_key, meta.version_id, is_trash_flag
                        );
                        let mut st = REDIST_STATUS.write().await;
                        st.total_objects_migrated += 1;
                        ws_manager.broadcast_system_wide(
                            StorageEventPayload::RedistributionUpdate(st.clone()),
                        );
                        continue;
                    }

                    match redistribute_object_streaming(
                        &mut meta,
                        target_data_shards,
                        target_parity_shards,
                        target_stripe_size,
                        &blob_roots,
                        &index_roots,
                        is_trash_flag,
                    )
                    .await
                    {
                        Ok(_) => {
                            let mut st = REDIST_STATUS.write().await;
                            st.total_objects_migrated += 1;
                            ws_manager.broadcast_system_wide(
                                StorageEventPayload::RedistributionUpdate(st.clone()),
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Redistribute failed for {}/{} (v:{}): {}",
                                meta.fragment, meta.object_key, meta.version_id, e
                            );
                            let mut st = REDIST_STATUS.write().await;
                            st.total_errors += 1;
                            st.last_error = Some(e.to_string());
                            ws_manager.broadcast_system_wide(
                                StorageEventPayload::RedistributionUpdate(st.clone()),
                            );
                        }
                    }
                }

                if batch_scanned > 0 {
                    let mut st = REDIST_STATUS.write().await;
                    st.total_objects_scanned += batch_scanned;
                    let elapsed = start_time.elapsed().as_secs_f64();
                    st.duration_seconds = elapsed as u64;
                    if elapsed > 0.0 {
                        st.scan_rate_per_sec = st.total_objects_scanned as f64 / elapsed;
                    }
                    ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(
                        st.clone(),
                    ));
                }
            }
        }
    }

    {
        let mut st = REDIST_STATUS.write().await;
        st.is_running = false;
        st.phase = if should_stop() {
            "Stopped by User".to_string()
        } else {
            "Completed".to_string()
        };
        st.duration_seconds = start_time.elapsed().as_secs();
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    info!("TRUE Redistribution Job Ended.");
    Ok(())
}

async fn check_needs_redistribution(
    meta: &ObjectMetadata,
    target_data: usize,
    target_parity: usize,
    target_stripe_size: usize,
    target_num_disks: usize,
    target_blob_roots: &[PathBuf],
    target_index_roots: &[PathBuf],
    is_trash: bool,
) -> bool {
    if meta.parts.is_empty() {
        return false;
    }

    if meta.stripe_size_bytes != target_stripe_size {
        debug!(
            "Needs redistribution: stripe mismatch {} cur={} target={}",
            meta.object_key, meta.stripe_size_bytes, target_stripe_size
        );
        return true;
    }

    let part0 = &meta.parts[0];
    let cur_data = part0.shards.iter().filter(|s| !s.is_parity).count();
    let cur_parity = part0.shards.iter().filter(|s| s.is_parity).count();
    if cur_data != target_data || cur_parity != target_parity {
        debug!(
            "Needs redistribution: EC mismatch {} cur={}D+{}P target={}D+{}P",
            meta.object_key, cur_data, cur_parity, target_data, target_parity
        );
        return true;
    }

    for part in &meta.parts {
        for shard in &part.shards {
            let expected = (part.part_index as usize + shard.shard_index) % target_num_disks;
            if expected >= target_blob_roots.len() {
                return true;
            }

            let target_disk_path = target_blob_roots[expected].to_string_lossy();
            if !shard.absolute_path.starts_with(target_disk_path.as_ref()) {
                debug!("Needs redistribution: Disk mismatch");
                return true;
            }

            let in_trash_path = shard.absolute_path.contains(".trash");
            if is_trash != in_trash_path {
                debug!(
                    "Needs redistribution: Trash status mismatch. IsTrash:{}, Path has trash:{}",
                    is_trash, in_trash_path
                );
                return true;
            }
        }
    }

    let expected_metadata_paths = get_all_metadata_paths_custom(
        target_index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
        is_trash,
    );

    for path in expected_metadata_paths {
        if !tokio_fs::try_exists(&path).await.unwrap_or(false) {
            debug!(
                "Needs redistribution: metadata replica missing at {:?}",
                path
            );
            return true;
        }
    }

    false
}

async fn redistribute_object_streaming(
    meta: &mut ObjectMetadata,
    target_data_shards: usize,
    target_parity_shards: usize,
    target_stripe_size: usize,
    target_blob_roots: &[PathBuf],
    target_index_roots: &[PathBuf],
    is_trash: bool,
) -> Result<(), AppError> {
    if should_stop() {
        return Err(AppError::InternalServerError("Stopped".into()));
    }

    let start_time = Instant::now();
    let num_disks = target_blob_roots.len();
    let total_shards = target_data_shards + target_parity_shards;

    let old_block_size = meta.stripe_size_bytes;
    let new_block_size = target_stripe_size;

    let mut file_cache: HashMap<String, tokio::fs::File> = HashMap::new();

    let old_parts = std::mem::take(&mut meta.parts);
    let mut new_parts: Vec<PartMetadata> = Vec::with_capacity(old_parts.len());
    let mut old_shards_for_gc: Vec<String> = Vec::new();

    for old_part in old_parts {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }

        let old_part_arc = Arc::new(old_part);

        let old_data_count = old_part_arc.shards.iter().filter(|s| !s.is_parity).count() as u64;
        let old_stripe_width = old_block_size as u64 * old_data_count;

        let new_data_width = (new_block_size * target_data_shards) as u64;
        let new_stripe_count = (old_part_arc.size_bytes + new_data_width - 1) / new_data_width;
        let final_shard_len = new_stripe_count * new_block_size as u64;

        let mut writers: Vec<Option<DirectWriter>> = Vec::with_capacity(total_shards);
        let mut commit_list: Vec<(PathBuf, PathBuf, PathBuf)> = Vec::new();
        let mut new_shards: Vec<ShardInfo> = Vec::with_capacity(total_shards);

        for shard_idx in 0..total_shards {
            if should_stop() {
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let is_parity = shard_idx >= target_data_shards;
            let ext = if is_parity {
                PARITY_SUFFIX
            } else {
                SHARD_SUFFIX
            };
            let file_name = format!(
                "part_{}_shard_{}{}",
                old_part_arc.part_index, shard_idx, ext
            );

            let disk_idx = (old_part_arc.part_index as usize + shard_idx) % num_disks;
            let disk_root = &target_blob_roots[disk_idx];

            if !tokio_fs::try_exists(disk_root).await.unwrap_or(false) {
                writers.push(None);
                continue;
            }

            let shard_dir =
                get_shard_dir_custom(disk_root, &meta.fragment, &meta.version_id, is_trash);
            tokio_fs::create_dir_all(&shard_dir).await.ok();

            let final_path = shard_dir.join(&file_name);
            let tmp_path = shard_dir.join(format!("{}{}", file_name, REDIST_TMP_SUFFIX));

            let std_file = match create_direct_file(&tmp_path).await {
                Ok(f) => f,
                Err(_) => {
                    writers.push(None);
                    continue;
                }
            };

            let file_clone = std_file
                .try_clone()
                .map_err(|e| AppError::Io(e, "clone handle".into()))?;
            let len = final_shard_len;
            let ok = tokio::task::spawn_blocking(move || file_clone.set_len(len))
                .await
                .is_ok();
            if !ok {
                writers.push(None);
                continue;
            }

            writers.push(Some(DirectWriter::new(std_file)));
            commit_list.push((tmp_path, final_path.clone(), shard_dir.clone()));

            new_shards.push(ShardInfo {
                shard_index: shard_idx,
                file_name: file_name.clone(),
                absolute_path: final_path.to_string_lossy().to_string(),
                size_bytes: final_shard_len,
                checksum: String::new(),
                is_parity,
            });
        }

        let mut stream_writer = ErasureStreamWriter::new(
            writers,
            target_data_shards,
            target_parity_shards,
            final_shard_len as usize,
            true,
            new_block_size,
        );

        let mut pos = 0u64;
        let mut batch: Vec<u8> = Vec::with_capacity(new_block_size);

        while pos < old_part_arc.size_bytes {
            if should_stop() {
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let stripe_index = pos / old_stripe_width;
            let offset_in_stripe = pos % old_stripe_width;

            let shard_index = (offset_in_stripe / old_block_size as u64) as usize;
            let offset_in_block = offset_in_stripe % old_block_size as u64;
            let file_offset = (stripe_index * old_block_size as u64) + offset_in_block;

            let to_read = ((old_block_size as u64 - offset_in_block)
                .min(old_part_arc.size_bytes - pos)) as usize;
            if to_read == 0 {
                break;
            }

            let chunk = read_or_recover_chunk(
                &mut file_cache,
                Path::new(""),
                old_part_arc.clone(),
                shard_index,
                file_offset,
                to_read,
                old_block_size,
            )
            .await?;

            batch.extend_from_slice(&chunk);

            if batch.len() >= new_block_size || (pos + to_read as u64) >= old_part_arc.size_bytes {
                stream_writer.write(&batch).await?;
                batch.clear();
            }

            pos += to_read as u64;
        }

        stream_writer.finalize().await?;

        let sums = stream_writer.get_shard_checksums();
        let mut sum_map: HashMap<usize, String> = HashMap::new();
        for (i, s) in sums.into_iter().enumerate() {
            sum_map.insert(i, s);
        }
        for s in new_shards.iter_mut() {
            if let Some(v) = sum_map.get(&s.shard_index) {
                s.checksum = v.clone();
            }
        }

        for (tmp, final_p, parent) in &commit_list {
            if should_stop() {
                return Err(AppError::InternalServerError("Stopped".into()));
            }
            if !tokio_fs::try_exists(tmp).await.unwrap_or(false) {
                continue;
            }

            match tokio_fs::rename(tmp, final_p).await {
                Ok(_) => {
                    let _ = sync_dir(parent).await;
                }
                Err(e) => {
                    warn!(
                        "Commit shard rename failed (non-fatal, resumable): {} -> {} : {}",
                        tmp.display(),
                        final_p.display(),
                        e
                    );
                    return Err(AppError::Io(e, "commit shard rename".into()));
                }
            }
        }

        let new_part = PartMetadata {
            part_index: old_part_arc.part_index,
            size_bytes: old_part_arc.size_bytes,
            created_at: old_part_arc.created_at,
            shards: new_shards.clone(),
            checksum: old_part_arc.checksum.clone(),
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
                old_shards_for_gc.push(old_shard.absolute_path.clone());
            }
        }

        file_cache.clear();
    }

    meta.parts = new_parts;
    meta.stripe_size_bytes = new_block_size;

    let bson_data = rmp_serde::to_vec(&*meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize metadata: {}", e))
    })?;

    let meta_paths = get_all_metadata_paths_custom(
        target_index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
        is_trash,
    );

    for p in meta_paths {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }

        if let Some(parent) = p.parent() {
            if !tokio_fs::try_exists(parent).await.unwrap_or(false) {
                tokio_fs::create_dir_all(parent).await.ok();
            }
        }

        let tmp = p.with_extension(META_TMP_EXT);

        let mut f = tokio_fs::File::create(&tmp)
            .await
            .map_err(|e| AppError::Io(e, format!("create meta tmp {}", tmp.display())))?;
        f.write_all(&bson_data)
            .await
            .map_err(|e| AppError::Io(e, "write meta tmp".into()))?;
        let _ = f.sync_all().await;
        drop(f);

        tokio_fs::rename(&tmp, &p)
            .await
            .map_err(|e| AppError::Io(e, "rename meta".into()))?;

        if let Some(parent) = p.parent() {
            let _ = sync_dir(parent).await;
        }
    }

    for old in old_shards_for_gc {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }

        let old_path = PathBuf::from(&old);
        if !tokio_fs::try_exists(&old_path).await.unwrap_or(false) {
            continue;
        }

        let gc_path = PathBuf::from(format!("{}{}", old, GC_SUFFIX));

        if tokio_fs::try_exists(&gc_path).await.unwrap_or(false) {
            continue;
        }

        if let Err(e) = tokio_fs::rename(&old_path, &gc_path).await {
            debug!(
                "GC rename failed (non-fatal): {} -> {} : {}",
                old_path.display(),
                gc_path.display(),
                e
            );
        } else {
            if let Some(parent) = gc_path.parent() {
                let _ = sync_dir(parent).await;
            }
        }
    }

    info!(
        "Redistributed {}/{} (v:{}) [Trash:{}] in {:.2?}.",
        meta.fragment,
        meta.object_key,
        meta.version_id,
        is_trash,
        start_time.elapsed()
    );

    Ok(())
}
