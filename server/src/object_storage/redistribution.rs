use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{
    GC_SUFFIX, META_TMP_EXT, METADATA_EXT, PARITY_SUFFIX, PART_META_SUFFIX, PREFIX_INDEX_DIR,
    PREFIX_INDEX_NAMESPACE, PREFIX_INDEX_VERSION, PREFIX_SEGMENT_EXT, QUOTA_FILE,
    REDIST_TMP_SUFFIX, RESERVATION_FILE, SHARD_SUFFIX,
};
use crate::object_storage::disk_manager::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata, PrefixIndexEntry,
    PrefixIndexSegment, ShardInfo, default_stripe_size,
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
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use tokio::fs as tokio_fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

const MAX_META_BYTES: u64 = 16 * 1024 * 1024;

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
    let rebuild_prefix = params
        .get("rebuild_prefix")
        .map(|v| v == "true" || v == "1" || v == "yes")
        .unwrap_or(true);

    let ws_manager = Arc::new(state.ws_manager.clone());
    let ws_manager_clone = ws_manager.clone();

    tokio::spawn(async move {
        if let Err(e) = run_redistribution_job(dry_run, rebuild_prefix, ws_manager).await {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LivenessDomain {
    Live,
    Trash,
}

fn domain_from_flag(is_trash: bool) -> LivenessDomain {
    if is_trash {
        LivenessDomain::Trash
    } else {
        LivenessDomain::Live
    }
}

async fn resolve_domain(
    index_roots: &[PathBuf],
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Option<LivenessDomain> {
    let trash_paths =
        get_all_metadata_paths_custom(index_roots, fragment, object_key, version_id, true);
    for p in trash_paths {
        if tokio_fs::try_exists(&p).await.unwrap_or(false) {
            return Some(LivenessDomain::Trash);
        }
    }

    let live_paths =
        get_all_metadata_paths_custom(index_roots, fragment, object_key, version_id, false);
    for p in live_paths {
        if tokio_fs::try_exists(&p).await.unwrap_or(false) {
            return Some(LivenessDomain::Live);
        }
    }

    None
}

async fn existing_meta_roots(
    index_roots: &[PathBuf],
    fragment: &str,
    object_key: &str,
    version_id: &str,
    is_trash: bool,
) -> Vec<PathBuf> {
    let paths =
        get_all_metadata_paths_custom(index_roots, fragment, object_key, version_id, is_trash);
    let mut out = Vec::new();
    for (root, p) in index_roots.iter().cloned().zip(paths.into_iter()) {
        if tokio_fs::try_exists(&p).await.unwrap_or(false) {
            out.push(root);
        }
    }
    out
}

async fn read_metadata_any_domain(
    index_roots: &[PathBuf],
    fragment: &str,
    object_key: &str,
    version_id: &str,
    domain: LivenessDomain,
) -> Result<ObjectMetadata, AppError> {
    let is_trash = matches!(domain, LivenessDomain::Trash);
    let paths =
        get_all_metadata_paths_custom(index_roots, fragment, object_key, version_id, is_trash);

    for p in paths {
        if !tokio_fs::try_exists(&p).await.unwrap_or(false) {
            continue;
        }

        let m = match tokio_fs::metadata(&p).await {
            Ok(m) => m,
            Err(_) => continue,
        };

        if m.len() == 0 || m.len() > MAX_META_BYTES {
            continue;
        }

        let raw = tokio_fs::read(&p)
            .await
            .map_err(|e| AppError::Io(e, format!("read meta {}", p.display())))?;
        let meta = rmp_serde::from_slice::<ObjectMetadata>(&raw)
            .map_err(|e| AppError::InternalServerError(format!("deserialize meta: {}", e)))?;
        return Ok(meta);
    }

    Err(AppError::NotFound)
}

fn is_missing_io(e: &AppError) -> bool {
    match e {
        AppError::Io(ioe, _) => ioe.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

async fn cleanup_redist_gen_dir(
    target_blob_roots: &[PathBuf],
    fragment: &str,
    version_id: &str,
    is_trash: bool,
    redist_gen: &str,
) {
    for disk_root in target_blob_roots {
        let base = get_shard_dir_custom(disk_root, fragment, version_id, is_trash);
        let gen_dir = base.join(".redist").join(redist_gen);
        let _ = tokio_fs::remove_dir_all(&gen_dir).await;
    }
}

async fn mark_paths_for_gc(paths: &[String]) {
    for p in paths {
        let old_path = PathBuf::from(p);
        if !tokio_fs::try_exists(&old_path).await.unwrap_or(false) {
            continue;
        }

        let gc_path = PathBuf::from(format!("{}{}", p, GC_SUFFIX));
        if tokio_fs::try_exists(&gc_path).await.unwrap_or(false) {
            continue;
        }

        if tokio_fs::rename(&old_path, &gc_path).await.is_ok() {
            if let Some(parent) = gc_path.parent() {
                let _ = sync_dir(parent).await;
            }
        }
    }
}

async fn fsync_path(path: &Path) -> Result<(), AppError> {
    let f = tokio_fs::File::open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open for fsync {}", path.display())))?;
    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("fsync {}", path.display())))?;
    Ok(())
}

async fn try_durable_atomic_write(
    path: &Path,
    tmp_ext: &str,
    data: &[u8],
) -> Result<bool, AppError> {
    if let Some(parent) = path.parent() {
        let _ = tokio_fs::create_dir_all(parent).await;
    }

    let base = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    let tmp = path.with_file_name(format!(".{}.{}.{}", base, tmp_ext, Ulid::new().to_string()));

    let mut f = match tokio_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
    {
        Ok(f) => f,
        Err(_) => return Ok(false),
    };

    if f.write_all(data).await.is_err() {
        let _ = tokio_fs::remove_file(&tmp).await;
        return Ok(false);
    }

    let _ = f.sync_all().await;
    drop(f);

    if tokio_fs::rename(&tmp, path).await.is_ok() {
        if let Some(parent) = path.parent() {
            let _ = sync_dir(parent).await;
        }
        Ok(true)
    } else {
        let _ = tokio_fs::remove_file(&tmp).await;
        Ok(false)
    }
}

async fn find_healthiest_index_root(roots: &[PathBuf]) -> Option<PathBuf> {
    let mut best_root = None;
    let mut max_count = 0u64;

    for root in roots {
        if !tokio_fs::try_exists(root).await.unwrap_or(false) {
            continue;
        }

        let root_clone = root.clone();
        let count = tokio::task::spawn_blocking(move || {
            let mut c = 0u64;
            for entry in WalkDir::new(&root_clone)
                .into_iter()
                .filter_map(|e| e.ok())
                .take(1000)
            {
                if entry.file_type().is_file() {
                    if entry.file_name().to_string_lossy().ends_with(METADATA_EXT) {
                        c += 1;
                    }
                }
            }
            c
        })
        .await
        .unwrap_or(0);

        if best_root.is_none() || count > max_count {
            max_count = count;
            best_root = Some(root.clone());
        }
    }

    if let Some(r) = &best_root {
        info!(
            "Redistribution: Selected healthiest index root for scanning: {:?}",
            r
        );
    } else {
        warn!("Redistribution: No reachable index roots found!");
    }

    best_root
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PrefixSpoolRecord {
    fragment: String,
    prefix: String,
    prefix_hash: String,
    object_key: String,
    version_id: String,
    created_at: i64,
}

fn prefixes_for_key(object_key: &str) -> Vec<String> {
    let mut out = Vec::new();
    let s = object_key.trim_start_matches('/');
    let mut cur = String::new();
    for part in s.split('/') {
        if part.is_empty() {
            continue;
        }
        if cur.is_empty() {
            cur.push_str(part);
        } else {
            cur.push('/');
            cur.push_str(part);
        }
        out.push(cur.clone());
    }
    out
}

fn record_partition(prefix_hash: &str, partitions: usize) -> usize {
    if partitions <= 1 {
        return 0;
    }
    let bytes = prefix_hash.as_bytes();
    let mut v: u32 = 0;
    for b in bytes.iter().take(8) {
        v = v.wrapping_mul(16777619) ^ (*b as u32);
    }
    (v as usize) % partitions
}

struct PrefixSpoolWriter {
    dir: PathBuf,
    partitions: usize,
    open_limit: usize,
    writers: Mutex<HashMap<usize, BufWriter<tokio_fs::File>>>,
    lru: Mutex<VecDeque<usize>>,
}

impl PrefixSpoolWriter {
    async fn new(dir: PathBuf, partitions: usize, open_limit: usize) -> Result<Self, AppError> {
        let partitions = partitions.max(1);
        let open_limit = open_limit.max(1);
        tokio_fs::create_dir_all(&dir)
            .await
            .map_err(|e| AppError::Io(e, "create prefix spool dir".into()))?;
        Ok(Self {
            dir,
            partitions,
            open_limit,
            writers: Mutex::new(HashMap::new()),
            lru: Mutex::new(VecDeque::new()),
        })
    }

    fn spool_path(&self, part: usize) -> PathBuf {
        self.dir.join(format!("pfx_part_{:04}.spool", part))
    }

    async fn open_writer(&self, part: usize) -> Result<BufWriter<tokio_fs::File>, AppError> {
        let path = self.spool_path(part);
        let f = tokio_fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| AppError::Io(e, format!("open prefix spool {}", path.display())))?;
        Ok(BufWriter::new(f))
    }

    async fn touch_lru(&self, part: usize) {
        let mut lru = self.lru.lock().await;
        if let Some(pos) = lru.iter().position(|&x| x == part) {
            lru.remove(pos);
        }
        lru.push_back(part);
    }

    async fn evict_one_if_needed(&self) -> Option<BufWriter<tokio_fs::File>> {
        let mut writers = self.writers.lock().await;
        if writers.len() <= self.open_limit {
            return None;
        }
        let mut lru = self.lru.lock().await;

        while writers.len() > self.open_limit {
            if let Some(p) = lru.pop_front() {
                if let Some(w) = writers.remove(&p) {
                    return Some(w);
                }
            } else {
                break;
            }
        }
        None
    }

    async fn evict_if_needed(&self) {
        while let Some(mut w) = self.evict_one_if_needed().await {
            let _ = w.flush().await;
        }
    }

    async fn append(&self, rec: &PrefixSpoolRecord) -> Result<(), AppError> {
        let bytes = rmp_serde::to_vec(rec).map_err(|e| {
            AppError::InternalServerError(format!("Serialize PrefixSpoolRecord: {}", e))
        })?;
        let len = (bytes.len() as u32).to_le_bytes();

        let part = record_partition(&rec.prefix_hash, self.partitions);

        let w_opt = {
            let mut writers = self.writers.lock().await;
            writers.remove(&part)
        };

        let mut w = match w_opt {
            Some(w) => w,
            None => self.open_writer(part).await?,
        };

        w.write_all(&len)
            .await
            .map_err(|e| AppError::Io(e, "write spool len".into()))?;
        w.write_all(&bytes)
            .await
            .map_err(|e| AppError::Io(e, "write spool bytes".into()))?;

        {
            let mut writers = self.writers.lock().await;
            writers.insert(part, w);
        }

        self.touch_lru(part).await;
        self.evict_if_needed().await;
        Ok(())
    }

    async fn flush_all(&self) {
        let keys: Vec<usize> = {
            let writers = self.writers.lock().await;
            writers.keys().copied().collect()
        };

        for k in keys {
            let w_opt = {
                let mut writers = self.writers.lock().await;
                writers.remove(&k)
            };

            if let Some(mut w) = w_opt {
                let _ = w.flush().await;
                let mut writers = self.writers.lock().await;
                writers.insert(k, w);
            }
        }
    }
}

async fn write_prefix_segment_to_root(
    root: &PathBuf,
    fragment: &str,
    prefix: &str,
    seg: &PrefixIndexSegment,
) -> Result<(), AppError> {
    let bytes = rmp_serde::to_vec(seg)
        .map_err(|e| AppError::InternalServerError(format!("Serialize prefix segment: {}", e)))?;

    let ph = ObjectPathManager::prefix_hash(prefix);
    let p1 = &ph[0..2];
    let p2 = &ph[2..4];

    let dir = root
        .join(fragment)
        .join("index")
        .join(PREFIX_INDEX_DIR)
        .join(PREFIX_INDEX_VERSION)
        .join(PREFIX_INDEX_NAMESPACE)
        .join(p1)
        .join(p2)
        .join(&ph);

    let _ = tokio_fs::create_dir_all(&dir).await;

    let file = format!("seg_{}{}", seg.entries[0].version_id, PREFIX_SEGMENT_EXT);
    let path = dir.join(file);

    let ok = try_durable_atomic_write(&path, META_TMP_EXT, &bytes).await?;
    if !ok {
        return Err(AppError::InternalServerError(format!(
            "Failed to write prefix segment {}",
            path.display()
        )));
    }
    Ok(())
}

async fn mirror_prefix_record(
    index_roots: &[PathBuf],
    rec: &PrefixSpoolRecord,
) -> Result<(), AppError> {
    let seg = PrefixIndexSegment {
        prefix: rec.prefix.clone(),
        entries: vec![PrefixIndexEntry {
            object_key: rec.object_key.clone(),
            version_id: rec.version_id.clone(),
            created_at: rec.created_at,
        }],
    };

    let mut any_ok = false;
    for r in index_roots {
        if write_prefix_segment_to_root(r, &rec.fragment, &rec.prefix, &seg)
            .await
            .is_ok()
        {
            any_ok = true;
        }
    }

    if !any_ok {
        return Err(AppError::InternalServerError(
            "Failed to mirror prefix segment to any index root".into(),
        ));
    }

    Ok(())
}

async fn replay_spool_and_mirror_prefix(
    spool_dir: &PathBuf,
    partitions: usize,
    index_roots: &[PathBuf],
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    for part in 0..partitions {
        if should_stop() {
            break;
        }

        let path = spool_dir.join(format!("pfx_part_{:04}.spool", part));
        if !tokio_fs::try_exists(&path).await.unwrap_or(false) {
            continue;
        }

        {
            let mut st = REDIST_STATUS.write().await;
            st.phase = format!(
                "Rebuilding Prefix Index (partition {}/{})",
                part + 1,
                partitions
            );
            ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }

        let mut f = tokio_fs::File::open(&path)
            .await
            .map_err(|e| AppError::Io(e, "open prefix spool".into()))?;

        loop {
            if should_stop() {
                break;
            }

            let mut len_buf = [0u8; 4];
            match f.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(_) => break,
            }
            let n = u32::from_le_bytes(len_buf) as usize;
            if n == 0 || n > (1024 * 1024) {
                return Err(AppError::InternalServerError(
                    "Invalid prefix spool record length".into(),
                ));
            }
            let mut data = vec![0u8; n];
            f.read_exact(&mut data)
                .await
                .map_err(|e| AppError::Io(e, "read prefix spool record".into()))?;

            let rec = rmp_serde::from_slice::<PrefixSpoolRecord>(&data).map_err(|e| {
                AppError::InternalServerError(format!("Decode PrefixSpoolRecord: {}", e))
            })?;

            mirror_prefix_record(index_roots, &rec).await?;
        }
    }

    Ok(())
}

fn processed_key(meta: &ObjectMetadata, is_trash: bool) -> String {
    format!(
        "{}|{}|{}|{}",
        meta.fragment, meta.object_key, meta.version_id, is_trash
    )
}

pub async fn run_redistribution_job(
    dry_run: bool,
    rebuild_prefix: bool,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();

    {
        let mut st = REDIST_STATUS.write().await;
        *st = RedistributionJobStatus {
            is_running: true,
            phase: format!(
                "Initializing (dry_run={}, rebuild_prefix={})",
                dry_run, rebuild_prefix
            ),
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

    if target_stripe_size % 4096 != 0 {
        return Err(AppError::InternalServerError(format!(
            "stripe_size_bytes must be multiple of 4096 for O_DIRECT (got {})",
            target_stripe_size
        )));
    }

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

    let index_roots: Vec<PathBuf> = index_roots_config.iter().map(PathBuf::from).collect();

    info!(
        "Starting Redistribution (Live + Trash). target={}D+{}P stripe={} bytes blob_disks={} index_roots={} dry_run={} rebuild_prefix={}",
        target_data_shards,
        target_parity_shards,
        target_stripe_size,
        num_disks,
        index_roots.len(),
        dry_run,
        rebuild_prefix
    );

    let processed_versions = Arc::new(Mutex::new(HashSet::<String>::new()));

    let spool_dir =
        std::env::temp_dir().join(format!("n1_prefix_redist_{}", Ulid::new().to_string()));
    let prefix_partitions = 256usize;
    let prefix_open_limit = 32usize;

    let prefix_spool = if rebuild_prefix {
        Some(Arc::new(
            PrefixSpoolWriter::new(spool_dir.clone(), prefix_partitions, prefix_open_limit).await?,
        ))
    } else {
        None
    };

    let best_root_opt = find_healthiest_index_root(&index_roots).await;

    if let Some(index_root) = best_root_opt {
        if !should_stop() {
            if !tokio_fs::try_exists(&index_root).await.unwrap_or(false) {
                warn!("Index root unreachable, skipping: {:?}", index_root);
            } else {
                {
                    let mut st = REDIST_STATUS.write().await;
                    st.phase = format!("Scanning Root {:?}", index_root);
                    ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(
                        st.clone(),
                    ));
                }

                if let Ok(mut frag_entries) = tokio_fs::read_dir(&index_root).await {
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
                            if should_stop() {
                                break;
                            }

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

                                        if p.components()
                                            .any(|c| c.as_os_str() == OsStr::new(PREFIX_INDEX_DIR))
                                        {
                                            continue;
                                        }

                                        let name =
                                            p.file_name().and_then(|n| n.to_str()).unwrap_or("");

                                        if name.ends_with(METADATA_EXT)
                                            && !name.ends_with(PART_META_SUFFIX)
                                            && name != QUOTA_FILE
                                            && name != RESERVATION_FILE
                                        {
                                            if tx
                                                .blocking_send((p.to_path_buf(), is_trash))
                                                .is_err()
                                            {
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

                                let m = match tokio_fs::metadata(&meta_path).await {
                                    Ok(m) => m,
                                    Err(_) => continue,
                                };

                                if m.len() == 0 || m.len() > MAX_META_BYTES {
                                    continue;
                                }

                                let raw = match tokio_fs::read(&meta_path).await {
                                    Ok(d) => d,
                                    Err(_) => continue,
                                };

                                let meta = match rmp_serde::from_slice::<ObjectMetadata>(&raw) {
                                    Ok(m) => m,
                                    Err(_) => continue,
                                };

                                if meta.is_delete_marker {
                                    continue;
                                }

                                let pk = processed_key(&meta, is_trash_flag);
                                {
                                    let mut set = processed_versions.lock().await;
                                    if set.contains(&pk) {
                                        continue;
                                    }
                                    set.insert(pk);
                                }

                                if rebuild_prefix && !is_trash_flag {
                                    if let Some(spool) = prefix_spool.as_ref() {
                                        for prefix in prefixes_for_key(&meta.object_key) {
                                            let ph = ObjectPathManager::prefix_hash(&prefix);
                                            let rec = PrefixSpoolRecord {
                                                fragment: meta.fragment.clone(),
                                                prefix,
                                                prefix_hash: ph,
                                                object_key: meta.object_key.clone(),
                                                version_id: meta.version_id.clone(),
                                                created_at: meta.created_at,
                                            };
                                            let _ = spool.append(&rec).await;
                                        }
                                    }
                                }

                                batch_scanned += 1;
                                if batch_scanned >= STATUS_BATCH {
                                    let mut st = REDIST_STATUS.write().await;
                                    st.total_objects_scanned += batch_scanned;
                                    let elapsed = start_time.elapsed().as_secs_f64();
                                    st.duration_seconds = elapsed as u64;
                                    if elapsed > 0.0 {
                                        st.scan_rate_per_sec =
                                            st.total_objects_scanned as f64 / elapsed;
                                    }
                                    ws_manager.broadcast_system_wide(
                                        StorageEventPayload::RedistributionUpdate(st.clone()),
                                    );
                                    batch_scanned = 0;
                                }

                                let mut meta_for_work = meta;

                                let needs = check_needs_redistribution(
                                    &meta_for_work,
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
                                        meta_for_work.fragment,
                                        meta_for_work.object_key,
                                        meta_for_work.version_id,
                                        is_trash_flag
                                    ));
                                }

                                if dry_run {
                                    let mut st = REDIST_STATUS.write().await;
                                    st.total_objects_migrated += 1;
                                    ws_manager.broadcast_system_wide(
                                        StorageEventPayload::RedistributionUpdate(st.clone()),
                                    );
                                    continue;
                                }

                                match redistribute_object_streaming(
                                    &mut meta_for_work,
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
                                            meta_for_work.fragment,
                                            meta_for_work.object_key,
                                            meta_for_work.version_id,
                                            e
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
                                    st.scan_rate_per_sec =
                                        st.total_objects_scanned as f64 / elapsed;
                                }
                                ws_manager.broadcast_system_wide(
                                    StorageEventPayload::RedistributionUpdate(st.clone()),
                                );
                            }
                        }
                    }
                } else {
                    warn!("Failed to read index root {:?} (inner check)", index_root);
                }
            }
        }
    } else {
        warn!("Redistribution aborted: No healthy index root found.");
        let mut st = REDIST_STATUS.write().await;
        st.last_error = Some("No healthy index root found".to_string());
    }

    if rebuild_prefix {
        if let Some(spool) = prefix_spool.as_ref() {
            spool.flush_all().await;
        }

        if !should_stop() {
            {
                let mut st = REDIST_STATUS.write().await;
                st.phase = "Rebuilding Prefix Index (mirroring segments)".to_string();
                ws_manager
                    .broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
            }
            let _ = replay_spool_and_mirror_prefix(
                &spool_dir,
                prefix_partitions,
                &index_roots,
                ws_manager.clone(),
            )
            .await;
        }

        if !dry_run {
            let _ = tokio_fs::remove_dir_all(&spool_dir).await;
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

    info!("Redistribution job ended.");
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

    if target_stripe_size % 4096 != 0 {
        return Err(AppError::InternalServerError(format!(
            "stripe_size_bytes must be multiple of 4096 for O_DIRECT (got {})",
            target_stripe_size
        )));
    }

    let start_time = Instant::now();
    let num_disks = target_blob_roots.len();

    let start_domain = domain_from_flag(is_trash);
    let cur_domain = resolve_domain(
        target_index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
    )
    .await;

    if cur_domain.is_some() && cur_domain != Some(start_domain) {
        return Ok(());
    }

    let redist_gen = Ulid::new().to_string();

    let old_block_size = meta.stripe_size_bytes;
    let new_block_size = target_stripe_size;

    let mut file_cache: HashMap<String, tokio_fs::File> = HashMap::new();

    let old_parts = std::mem::take(&mut meta.parts);
    let mut new_parts: Vec<PartMetadata> = Vec::with_capacity(old_parts.len());
    let mut old_shards_for_gc: Vec<String> = Vec::new();

    for old_part in old_parts {
        if should_stop() {
            cleanup_redist_gen_dir(
                target_blob_roots,
                &meta.fragment,
                &meta.version_id,
                is_trash,
                &redist_gen,
            )
            .await;
            return Err(AppError::InternalServerError("Stopped".into()));
        }

        let old_part_arc = Arc::new(old_part);

        let old_data_count = old_part_arc.shards.iter().filter(|s| !s.is_parity).count() as u64;
        if old_data_count == 0 {
            continue;
        }

        let old_stripe_width = old_block_size as u64 * old_data_count;

        let new_data_width = (new_block_size * target_data_shards) as u64;
        if new_data_width == 0 {
            cleanup_redist_gen_dir(
                target_blob_roots,
                &meta.fragment,
                &meta.version_id,
                is_trash,
                &redist_gen,
            )
            .await;
            return Err(AppError::InternalServerError(
                "invalid new_data_width".into(),
            ));
        }

        let new_stripe_count = (old_part_arc.size_bytes + new_data_width - 1) / new_data_width;
        let final_shard_len = new_stripe_count * new_block_size as u64;

        let total_shards = target_data_shards + target_parity_shards;

        let mut writers: Vec<Option<DirectWriter>> = Vec::with_capacity(total_shards);
        let mut commit_list: Vec<(PathBuf, PathBuf, PathBuf)> = Vec::new();
        let mut new_shards: Vec<ShardInfo> = Vec::with_capacity(total_shards);

        let mut successful_opens = 0usize;

        for shard_idx in 0..total_shards {
            if should_stop() {
                cleanup_redist_gen_dir(
                    target_blob_roots,
                    &meta.fragment,
                    &meta.version_id,
                    is_trash,
                    &redist_gen,
                )
                .await;
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

            let base_dir =
                get_shard_dir_custom(disk_root, &meta.fragment, &meta.version_id, is_trash);
            let shard_dir = base_dir.join(".redist").join(&redist_gen);
            let _ = tokio_fs::create_dir_all(&shard_dir).await;

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
            let setlen = tokio::task::spawn_blocking(move || file_clone.set_len(len))
                .await
                .map_err(|e| AppError::InternalServerError(format!("set_len task: {}", e)))?;

            if setlen.is_err() {
                let _ = tokio_fs::remove_file(&tmp_path).await;
                writers.push(None);
                continue;
            }

            successful_opens += 1;

            writers.push(Some(DirectWriter::new(std_file)));
            commit_list.push((tmp_path.clone(), final_path.clone(), shard_dir.clone()));

            new_shards.push(ShardInfo {
                shard_index: shard_idx,
                file_name: file_name.clone(),
                absolute_path: final_path.to_string_lossy().to_string(),
                size_bytes: final_shard_len,
                checksum: String::new(),
                is_parity,
            });
        }

        if successful_opens < target_data_shards {
            cleanup_redist_gen_dir(
                target_blob_roots,
                &meta.fragment,
                &meta.version_id,
                is_trash,
                &redist_gen,
            )
            .await;
            return Err(AppError::InternalServerError(format!(
                "Insufficient healthy disks for write quorum. Need {}, got {}.",
                target_data_shards, successful_opens
            )));
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
        let mut block = vec![0u8; new_block_size];
        let mut block_len: usize = 0;

        let target_stream_bytes = new_stripe_count * (new_block_size * target_data_shards) as u64;

        while pos < old_part_arc.size_bytes {
            if should_stop() {
                cleanup_redist_gen_dir(
                    target_blob_roots,
                    &meta.fragment,
                    &meta.version_id,
                    is_trash,
                    &redist_gen,
                )
                .await;
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

            let chunk = match read_or_recover_chunk(
                &mut file_cache,
                Path::new(""),
                old_part_arc.clone(),
                shard_index,
                file_offset,
                to_read,
                old_block_size,
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    if is_missing_io(&e) {
                        let cur = resolve_domain(
                            target_index_roots,
                            &meta.fragment,
                            &meta.object_key,
                            &meta.version_id,
                        )
                        .await;
                        if cur.is_some() && cur != Some(start_domain) {
                            cleanup_redist_gen_dir(
                                target_blob_roots,
                                &meta.fragment,
                                &meta.version_id,
                                is_trash,
                                &redist_gen,
                            )
                            .await;
                            return Ok(());
                        }
                    }
                    cleanup_redist_gen_dir(
                        target_blob_roots,
                        &meta.fragment,
                        &meta.version_id,
                        is_trash,
                        &redist_gen,
                    )
                    .await;
                    return Err(e);
                }
            };

            let mut off = 0usize;
            while off < chunk.len() {
                let space = new_block_size - block_len;
                let take = space.min(chunk.len() - off);
                block[block_len..block_len + take].copy_from_slice(&chunk[off..off + take]);
                block_len += take;
                off += take;

                if block_len == new_block_size {
                    stream_writer.write(&block).await?;
                    block_len = 0;
                }
            }

            pos += to_read as u64;
        }

        let mut remaining_pad = target_stream_bytes.saturating_sub(old_part_arc.size_bytes);
        if remaining_pad > 0 {
            let zero_block = vec![0u8; new_block_size];

            while remaining_pad > 0 {
                if block_len == 0 && remaining_pad >= new_block_size as u64 {
                    stream_writer.write(&zero_block).await?;
                    remaining_pad -= new_block_size as u64;
                    continue;
                }

                let take = (new_block_size as u64).min(remaining_pad) as usize;
                let space = new_block_size - block_len;
                let n = space.min(take);

                block[block_len..block_len + n].fill(0);
                block_len += n;
                remaining_pad -= n as u64;

                if block_len == new_block_size {
                    stream_writer.write(&block).await?;
                    block_len = 0;
                }
            }
        }

        if block_len != 0 {
            block[block_len..].fill(0);
            stream_writer.write(&block).await?;
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

        for (tmp, _final_p, _parent) in &commit_list {
            let _ = fsync_path(tmp.as_path()).await;
        }

        for (tmp, final_p, parent) in &commit_list {
            if should_stop() {
                cleanup_redist_gen_dir(
                    target_blob_roots,
                    &meta.fragment,
                    &meta.version_id,
                    is_trash,
                    &redist_gen,
                )
                .await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            if !tokio_fs::try_exists(tmp).await.unwrap_or(false) {
                cleanup_redist_gen_dir(
                    target_blob_roots,
                    &meta.fragment,
                    &meta.version_id,
                    is_trash,
                    &redist_gen,
                )
                .await;
                return Err(AppError::InternalServerError("missing redist tmp".into()));
            }

            tokio_fs::rename(tmp, final_p).await.map_err(|e| {
                AppError::Io(
                    e,
                    format!(
                        "commit shard rename {} -> {}",
                        tmp.display(),
                        final_p.display()
                    ),
                )
            })?;

            let _ = sync_dir(parent).await;
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

    let cur_domain2 = resolve_domain(
        target_index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
    )
    .await;
    if cur_domain2.is_some() && cur_domain2 != Some(start_domain) {
        cleanup_redist_gen_dir(
            target_blob_roots,
            &meta.fragment,
            &meta.version_id,
            is_trash,
            &redist_gen,
        )
        .await;
        return Ok(());
    }

    let mut fresh = read_metadata_any_domain(
        target_index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
        start_domain,
    )
    .await?;

    if fresh.is_delete_marker {
        cleanup_redist_gen_dir(
            target_blob_roots,
            &meta.fragment,
            &meta.version_id,
            is_trash,
            &redist_gen,
        )
        .await;
        return Ok(());
    }

    fresh.parts = new_parts;
    fresh.stripe_size_bytes = new_block_size;

    let meta_bytes = rmp_serde::to_vec(&fresh)
        .map_err(|e| AppError::InternalServerError(format!("serialize meta: {}", e)))?;

    let roots_that_had_meta = existing_meta_roots(
        target_index_roots,
        &fresh.fragment,
        &fresh.object_key,
        &fresh.version_id,
        is_trash,
    )
    .await;

    let meta_paths = get_all_metadata_paths_custom(
        target_index_roots,
        &fresh.fragment,
        &fresh.object_key,
        &fresh.version_id,
        is_trash,
    );

    let mut wrote_any = false;
    let mut wrote_roots: HashSet<PathBuf> = HashSet::new();

    for (root, p) in target_index_roots
        .iter()
        .cloned()
        .zip(meta_paths.into_iter())
    {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }

        let ok = try_durable_atomic_write(&p, META_TMP_EXT, &meta_bytes).await?;
        if ok {
            wrote_any = true;
            wrote_roots.insert(root);
        }
    }

    if !wrote_any {
        cleanup_redist_gen_dir(
            target_blob_roots,
            &fresh.fragment,
            &fresh.version_id,
            is_trash,
            &redist_gen,
        )
        .await;
        return Err(AppError::InternalServerError(
            "Failed to commit metadata to any index mirror".into(),
        ));
    }

    let mut all_previous_meta_roots_updated = true;
    for r in &roots_that_had_meta {
        if !wrote_roots.contains(r) {
            all_previous_meta_roots_updated = false;
            break;
        }
    }

    if all_previous_meta_roots_updated {
        mark_paths_for_gc(&old_shards_for_gc).await;
    }

    *meta = fresh;

    info!(
        "Redistributed {}/{} (v:{}) [Trash:{}] gen={} in {:.2?} (updated_prev_meta_roots={}).",
        meta.fragment,
        meta.object_key,
        meta.version_id,
        is_trash,
        redist_gen,
        start_time.elapsed(),
        all_previous_meta_roots_updated
    );

    Ok(())
}
