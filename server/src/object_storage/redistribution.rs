use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{
    GC_SUFFIX, METADATA_EXT, PARITY_SUFFIX, PART_META_SUFFIX, QUOTA_FILE, REDIST_TMP_SUFFIX,
    RESERVATION_FILE, SHARD_SUFFIX,
};
use crate::object_storage::disk_manager::{
    default_stripe_size, ObjectMetadata, ObjectPathManager, PartMetadata, Quota, ShardInfo,
    INDEX_STORE_DISKS,
};
use crate::object_storage::ec::ErasureStreamWriter;
use crate::object_storage::fragment_metadata::{read_fragment_metadata, write_fragment_metadata};
use crate::object_storage::prefix_rebuilder::run_prefix_rebuild_job;
use crate::object_storage::recovery::read_or_recover_chunk;
use crate::object_storage::upload::{
    create_direct_file, sync_dir, DirectWriter, DATA_SHARDS_ATOMIC, PARITY_SHARDS_ATOMIC,
    STRIPE_SIZE_ATOMIC,
};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;

use chrono::Utc;
use crc32fast::Hasher as Crc32Hasher;
use serde::Serialize;

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime};

use tokio::fs as tokio_fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, warn};
use ulid::Ulid;
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

pub async fn get_redist_status_api(
    claims: Claims,
) -> Result<Json<RedistributionJobStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(REDIST_STATUS.read().await.clone()))
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
    let ws_manager_clone = ws_manager.clone();

    tokio::spawn(async move {
        if let Err(e) = run_redistribution_job(dry_run, ws_manager).await {
            error!("Redistribution job failed: {}", e);
            let mut st = REDIST_STATUS.write().await;
            st.is_running = false;
            st.phase = "Failed".to_string();
            st.last_error = Some(e.to_string());
            ws_manager_clone
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

pub async fn stop_redistribution_job_api(
    claims: Claims,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
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

async fn replicate_quota_best_effort(fragment_hash: &str, index_roots: &[PathBuf], dry_run: bool) {
    let mut best_bytes: Option<Vec<u8>> = None;
    let mut best_mtime: SystemTime = SystemTime::UNIX_EPOCH;
    let mut best_len: u64 = 0;

    for r in index_roots {
        let p = r.join(fragment_hash).join(QUOTA_FILE);
        let bytes = match tokio_fs::read(&p).await {
            Ok(b) => b,
            Err(_) => continue,
        };
        if rmp_serde::from_slice::<Quota>(&bytes).is_err() {
            continue;
        }
        let meta = tokio_fs::metadata(&p).await.ok();
        let mtime = meta
            .as_ref()
            .and_then(|m| m.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let len = meta.as_ref().map(|m| m.len()).unwrap_or(bytes.len() as u64);

        let take =
            best_bytes.is_none() || (mtime > best_mtime) || (mtime == best_mtime && len > best_len);
        if take {
            best_bytes = Some(bytes);
            best_mtime = mtime;
            best_len = len;
        }
    }

    let Some(canon) = best_bytes else {
        return;
    };

    for r in index_roots {
        let p = r.join(fragment_hash).join(QUOTA_FILE);
        let same = match tokio_fs::read(&p).await {
            Ok(cur) => cur == canon,
            Err(_) => false,
        };
        if same || dry_run {
            continue;
        }
        let _ = durable_atomic_write_bytes(&p, &canon).await;
    }
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

async fn reachable_roots(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for r in roots {
        if tokio_fs::try_exists(r).await.unwrap_or(false) {
            out.push(r.clone());
        }
    }
    out
}

async fn replicate_fragment_metadata_best_effort(fragment_hash: &str) {
    if let Ok(meta) = read_fragment_metadata(fragment_hash).await {
        let _ = write_fragment_metadata(&meta).await;
    }
}

async fn durable_atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("missing parent for {}", path.display()))
    })?;

    tokio_fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;

    let file_name = path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("file")
        .to_string();

    let tmp = parent.join(format!(".{}.tmp.{}", file_name, Ulid::new().to_string()));

    let mut f = tokio_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("create tmp {}", tmp.display())))?;

    f.write_all(bytes)
        .await
        .map_err(|e| AppError::Io(e, format!("write tmp {}", tmp.display())))?;

    let _ = f.sync_all().await;
    drop(f);

    tokio_fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;

    let _ = sync_dir(parent).await;
    Ok(())
}

#[cfg(unix)]
async fn stage_old_shard_gc_with_symlink(old_abs: &str) -> Result<(), AppError> {
    let old_path = PathBuf::from(old_abs);
    if !tokio_fs::try_exists(&old_path).await.unwrap_or(false) {
        return Ok(());
    }

    let gc_path = PathBuf::from(format!("{}{}", old_abs, GC_SUFFIX));
    if tokio_fs::try_exists(&gc_path).await.unwrap_or(false) {
        return Ok(());
    }

    let parent = old_path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("missing shard parent".into()))?
        .to_path_buf();

    tokio_fs::rename(&old_path, &gc_path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename shard -> gc {}", old_path.display())))?;

    let target_name = gc_path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("")
        .to_string();

    let old_path_clone = old_path.clone();
    let parent_clone = parent.clone();

    let symlink_res = tokio::task::spawn_blocking(move || {
        let target = PathBuf::from(target_name);
        std::os::unix::fs::symlink(target, &old_path_clone)
    })
    .await
    .map_err(|_| AppError::InternalServerError("symlink join failed".into()))?;

    if let Err(e) = symlink_res {
        let _ = tokio_fs::rename(&gc_path, &old_path).await;
        return Err(AppError::Io(e, "create gc symlink".into()));
    }

    let _ = sync_dir(&parent_clone).await;
    Ok(())
}

#[cfg(not(unix))]
async fn stage_old_shard_gc_with_symlink(_old_abs: &str) -> Result<(), AppError> {
    Ok(())
}

fn dedupe_id_from_meta_path(path: &Path, is_trash: bool) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    if !name.ends_with(METADATA_EXT) {
        return None;
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    is_trash.hash(&mut h);
    Some(h.finish())
}

async fn list_all_fragments(index_roots: &[PathBuf]) -> Vec<String> {
    let roots = index_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut out = HashSet::<String>::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            let rd = match std::fs::read_dir(&root) {
                Ok(r) => r,
                Err(_) => continue,
            };
            for ent in rd.filter_map(|e| e.ok()) {
                if ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Some(name) = ent.file_name().to_str() {
                        out.insert(name.to_string());
                    }
                }
            }
        }
        let mut v: Vec<String> = out.into_iter().collect();
        v.sort();
        v
    })
    .await
    .unwrap_or_default()
}

struct LiveLayout {
    blob_cfg: Vec<PathBuf>,
    index_cfg: Vec<PathBuf>,
    live_blob: Arc<Vec<PathBuf>>,
    live_index: Arc<Vec<PathBuf>>,
    last_refresh: Instant,
}

impl LiveLayout {
    async fn new(blob_cfg: Vec<PathBuf>, index_cfg: Vec<PathBuf>) -> Self {
        let live_blob = Arc::new(reachable_roots(&blob_cfg).await);
        let live_index = Arc::new(reachable_roots(&index_cfg).await);
        Self {
            blob_cfg,
            index_cfg,
            live_blob,
            live_index,
            last_refresh: Instant::now(),
        }
    }

    async fn refresh_if_needed(&mut self) {
        if self.last_refresh.elapsed() < Duration::from_secs(2) {
            return;
        }
        self.live_blob = Arc::new(reachable_roots(&self.blob_cfg).await);
        self.live_index = Arc::new(reachable_roots(&self.index_cfg).await);
        self.last_refresh = Instant::now();
    }
}

#[cfg(target_os = "linux")]
fn rename_exchange(a: &Path, b: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let a_c = CString::new(a.as_os_str().as_bytes()).unwrap();
    let b_c = CString::new(b.as_os_str().as_bytes()).unwrap();

    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            a_c.as_ptr(),
            libc::AT_FDCWD,
            b_c.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };

    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

async fn publish_tmp_overwrite(
    tmp: &Path,
    final_p: &Path,
    parent: &Path,
    file_name: &str,
) -> Result<(), AppError> {
    let final_exists = tokio_fs::try_exists(final_p).await.unwrap_or(false);

    if !final_exists {
        tokio_fs::rename(tmp, final_p)
            .await
            .map_err(|e| AppError::Io(e, "commit rename".into()))?;
        let _ = sync_dir(parent).await;
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        let a = tmp.to_path_buf();
        let b = final_p.to_path_buf();

        let ex = tokio::task::spawn_blocking(move || rename_exchange(&a, &b))
            .await
            .map_err(|e| AppError::InternalServerError(format!("rename_exchange join: {}", e)))?;

        if ex.is_ok() {
            let gc_name = format!(
                "{}.preexisting.{}{}",
                file_name,
                Ulid::new().to_string(),
                GC_SUFFIX
            );
            let gc_path = parent.join(gc_name);
            let _ = tokio_fs::rename(tmp, &gc_path).await;
            let _ = sync_dir(parent).await;
            return Ok(());
        }
    }

    let gc_name = format!(
        "{}.preexisting.{}{}",
        file_name,
        Ulid::new().to_string(),
        GC_SUFFIX
    );
    let gc_path = parent.join(gc_name);

    tokio_fs::rename(final_p, &gc_path)
        .await
        .map_err(|e| AppError::Io(e, "rename final->gc".into()))?;

    tokio_fs::rename(tmp, final_p)
        .await
        .map_err(|e| AppError::Io(e, "rename tmp->final".into()))?;

    let _ = sync_dir(parent).await;
    Ok(())
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

    let pm = ObjectPathManager::new();
    let blob_roots_cfg = pm.get_blob_roots();

    let index_roots_cfg: Vec<PathBuf> = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?
        .iter()
        .map(PathBuf::from)
        .collect();

    let mut layout = LiveLayout::new(blob_roots_cfg.clone(), index_roots_cfg.clone()).await;

    if layout.live_blob.is_empty() {
        return Err(AppError::InternalServerError(
            "No reachable blob disks".into(),
        ));
    }
    if layout.live_index.is_empty() {
        return Err(AppError::InternalServerError(
            "No reachable index roots".into(),
        ));
    }

    info!(
        "Starting Redistribution Job (Active & Trash). target={}D+{}P stripe={} bytes blob_cfg={} index_cfg={} dry_run={}",
        target_data_shards,
        target_parity_shards,
        target_stripe_size,
        blob_roots_cfg.len(),
        index_roots_cfg.len(),
        dry_run
    );

    let mut touched_fragments: HashSet<String> = HashSet::new();

    let fragments = list_all_fragments(layout.live_index.as_ref()).await;

    {
        let mut st = REDIST_STATUS.write().await;
        st.phase = format!("Preparing (fragments={})", fragments.len());
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    for fragment_hash in fragments {
        if should_stop() {
            break;
        }

        layout.refresh_if_needed().await;
        let live_blob = layout.live_blob.clone();
        let live_index = layout.live_index.clone();

        if live_blob.is_empty() || live_index.is_empty() {
            continue;
        }

        replicate_fragment_metadata_best_effort(&fragment_hash).await;
        replicate_quota_best_effort(&fragment_hash, live_index.as_ref(), dry_run).await;

        {
            let mut st = REDIST_STATUS.write().await;
            st.current_fragment = fragment_hash.clone();
            st.total_fragments_scanned += 1;
            st.phase = format!("Scanning Fragment {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }

        let mut seen_meta_ids: HashSet<u64> = HashSet::new();
        let scan_targets = vec![("index", false), (".trash/index", true)];

        for (sub_dir, is_trash) in scan_targets {
            if should_stop() {
                break;
            }

            let (tx, mut rx) = tokio::sync::mpsc::channel::<(PathBuf, bool)>(256);

            let roots = (*live_index).clone();
            let frag = fragment_hash.clone();
            let sub = sub_dir.to_string();

            tokio::task::spawn_blocking(move || {
                for root in roots {
                    let index_dir = root.join(&frag).join(&sub);
                    if !index_dir.exists() {
                        continue;
                    }
                    let walker = WalkDir::new(index_dir).into_iter();
                    for entry in walker.filter_map(|e| e.ok()) {
                        if !entry.file_type().is_file() {
                            continue;
                        }
                        let p = entry.path();
                        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");

                        if name.ends_with(METADATA_EXT)
                            && !name.ends_with(PART_META_SUFFIX)
                            && name != QUOTA_FILE
                            && name != RESERVATION_FILE
                        {
                            if tx.blocking_send((p.to_path_buf(), is_trash)).is_err() {
                                return;
                            }
                        }
                    }
                }
            });

            let mut batch_scanned: u64 = 0;
            const STATUS_BATCH: u64 = 10;

            while let Some((meta_path, is_trash_flag)) = rx.recv().await {
                if should_stop() {
                    break;
                }

                if let Some(id) = dedupe_id_from_meta_path(&meta_path, is_trash_flag) {
                    if !seen_meta_ids.insert(id) {
                        continue;
                    }
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

                batch_scanned += 1;
                if batch_scanned >= STATUS_BATCH {
                    layout.refresh_if_needed().await;

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
                    batch_scanned = 0;
                }

                let needs = check_needs_redistribution(
                    &meta,
                    target_data_shards,
                    target_parity_shards,
                    target_stripe_size,
                    live_blob.len(),
                    &live_blob,
                    &live_index,
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
                    ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(
                        st.clone(),
                    ));
                }

                if dry_run {
                    let mut st = REDIST_STATUS.write().await;
                    st.total_objects_migrated += 1;
                    ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(
                        st.clone(),
                    ));
                    continue;
                }

                match redistribute_object_streaming(
                    &mut meta,
                    target_data_shards,
                    target_parity_shards,
                    target_stripe_size,
                    &live_blob,
                    &live_index,
                    is_trash_flag,
                )
                .await
                {
                    Ok(_) => {
                        touched_fragments.insert(meta.fragment.clone());
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
                ws_manager
                    .broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
            }
        }
    }

    if !dry_run && !touched_fragments.is_empty() && !should_stop() {
        {
            let mut st = REDIST_STATUS.write().await;
            st.phase = format!(
                "Rebuilding Prefix Index (fragments={})",
                touched_fragments.len()
            );
            ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }

        let mut frags: Vec<String> = touched_fragments.into_iter().collect();
        frags.sort();

        for frag in frags {
            if should_stop() {
                break;
            }
            let _ = run_prefix_rebuild_job(Some(frag), ws_manager.clone()).await;
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

    info!("Redistribution Job Ended.");
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
        return true;
    }

    let part0 = &meta.parts[0];
    let cur_data = part0.shards.iter().filter(|s| !s.is_parity).count();
    let cur_parity = part0.shards.iter().filter(|s| s.is_parity).count();

    if cur_data != target_data || cur_parity != target_parity {
        return true;
    }

    if target_num_disks == 0 {
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
                return true;
            }

            let in_trash_path = shard.absolute_path.contains(".trash");
            if is_trash != in_trash_path {
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
            return true;
        }
    }

    false
}

async fn cleanup_tmp_files(files: &[PathBuf]) {
    for p in files {
        let _ = tokio_fs::remove_file(p).await;
    }
}

async fn crc32_file_hex(path: &Path) -> Result<String, AppError> {
    let mut f = tokio_fs::File::open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open for crc32 {}", path.display())))?;

    let mut hasher = Crc32Hasher::new();
    let mut buf = vec![0u8; 256 * 1024];

    loop {
        let n = f
            .read(&mut buf)
            .await
            .map_err(|e| AppError::Io(e, format!("read crc32 {}", path.display())))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(format!("{:08x}", hasher.finalize()))
}

async fn verify_tmp_shard(
    tmp_path: &Path,
    expected_len: u64,
    expected_crc32: Option<&str>,
) -> Result<(), AppError> {
    let len_ok = tokio_fs::metadata(tmp_path)
        .await
        .map(|m| m.len() == expected_len)
        .unwrap_or(false);

    if !len_ok {
        return Err(AppError::InternalServerError(format!(
            "Shard length mismatch for {}",
            tmp_path.display()
        )));
    }

    if let Some(exp) = expected_crc32.filter(|s| !s.is_empty()) {
        let got = crc32_file_hex(tmp_path).await?;
        if !got.eq_ignore_ascii_case(exp) {
            return Err(AppError::InternalServerError(format!(
                "Shard checksum mismatch for {} (expected {}, got {})",
                tmp_path.display(),
                exp,
                got
            )));
        }
    }

    Ok(())
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

    if num_disks < target_data_shards {
        return Err(AppError::InsufficientStorage);
    }

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
        if old_data_count == 0 {
            return Err(AppError::InternalServerError("invalid old EC".into()));
        }

        let old_stripe_width = old_block_size as u64 * old_data_count;

        let new_data_width = (new_block_size * target_data_shards) as u64;
        if new_data_width == 0 {
            return Err(AppError::InternalServerError(
                "invalid target stripe".into(),
            ));
        }

        let new_stripe_count = (old_part_arc.size_bytes + new_data_width - 1) / new_data_width;
        let final_shard_len = new_stripe_count * new_block_size as u64;

        let mut writers: Vec<Option<DirectWriter>> = Vec::with_capacity(total_shards);
        let mut commit_list: Vec<(PathBuf, PathBuf, PathBuf, String)> = Vec::new();
        let mut new_shards: Vec<ShardInfo> = Vec::with_capacity(total_shards);
        let mut created_tmps: Vec<PathBuf> = Vec::new();

        for shard_idx in 0..total_shards {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
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
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError(
                    "blob disk unreachable".into(),
                ));
            }

            let shard_dir =
                get_shard_dir_custom(disk_root, &meta.fragment, &meta.version_id, is_trash);
            let _ = tokio_fs::create_dir_all(&shard_dir).await;

            let final_path = shard_dir.join(&file_name);

            let tmp_path = shard_dir.join(format!(
                "{}{}{}",
                file_name,
                REDIST_TMP_SUFFIX,
                Ulid::new().to_string()
            ));
            created_tmps.push(tmp_path.clone());

            let std_file = create_direct_file(&tmp_path).await.map_err(|e| {
                AppError::InternalServerError(format!(
                    "create shard tmp {}: {}",
                    tmp_path.display(),
                    e
                ))
            })?;

            let file_clone = std_file
                .try_clone()
                .map_err(|e| AppError::Io(e, "clone direct file".into()))?;

            let len = final_shard_len;
            tokio::task::spawn_blocking(move || file_clone.set_len(len))
                .await
                .map_err(|e| AppError::InternalServerError(format!("set_len join: {}", e)))?
                .map_err(|e| AppError::Io(e, "set_len shard".into()))?;

            writers.push(Some(DirectWriter::new(std_file)));
            commit_list.push((
                tmp_path,
                final_path.clone(),
                shard_dir.clone(),
                file_name.clone(),
            ));

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
        )?;

        let mut pos = 0u64;
        let mut batch: Vec<u8> = Vec::with_capacity(new_block_size);

        while pos < old_part_arc.size_bytes {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
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
        let writers = stream_writer.into_writers();

        for w in writers.into_iter().flatten() {
            if let Err(e) = w.sync_data().await {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::Io(e, "fsync redistribution shard".into()));
            }
        }

        for s in new_shards.iter_mut() {
            if let Some(v) = sum_map.get(&s.shard_index) {
                s.checksum = v.clone();
            }
        }
        let checksum_by_name: HashMap<&str, &str> = new_shards
            .iter()
            .map(|s| (s.file_name.as_str(), s.checksum.as_str()))
            .collect();

        for (tmp, final_p, parent, fname) in &commit_list {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let expected_crc = checksum_by_name.get(fname.as_str()).copied();

            verify_tmp_shard(tmp.as_path(), final_shard_len, expected_crc).await?;

            publish_tmp_overwrite(tmp.as_path(), final_p.as_path(), parent.as_path(), fname)
                .await?;
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

    let bytes = rmp_serde::to_vec(&*meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize metadata: {}", e))
    })?;

    let meta_paths = get_all_metadata_paths_custom(
        target_index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
        is_trash,
    );

    let mut any_meta_ok = false;
    for p in meta_paths {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }
        if durable_atomic_write_bytes(&p, &bytes).await.is_ok() {
            any_meta_ok = true;
        }
    }

    if !any_meta_ok {
        return Err(AppError::InternalServerError(
            "failed to write metadata to any reachable index root".into(),
        ));
    }

    for old in old_shards_for_gc {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }
        let _ = stage_old_shard_gc_with_symlink(&old).await;
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
