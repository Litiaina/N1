use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use serde::Serialize;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{error, info};
use ulid::Ulid;
use walkdir::WalkDir;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{
    METADATA_EXT, PART_META_SUFFIX, PREFIX_DELIMITER_COMPACTED_FILE,
    PREFIX_DELIMITER_SEGMENT_PREFIX, PREFIX_INDEX_DIR, PREFIX_SEGMENT_EXT, RESERVATION_FILE,
};
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, ObjectSummary, PrefixIndexEntry, PrefixIndexSegment,
};
use crate::n1_core::prefix_delimiter::build_delimiter_compacted_from_entries;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;

const MAX_META_BYTES: u64 = 16 * 1024 * 1024;
const TOMBSTONE_MIN_RETENTION_SECS: i64 = 86_400;

#[derive(Debug, Clone, Serialize, Default)]
pub struct PrefixRebuildJobStatus {
    pub is_running: bool,
    pub phase: String,

    pub total_fragments_scanned: u64,
    pub total_metadata_scanned: u64,
    pub total_objects_indexed: u64,
    pub total_prefixes_written: u64,
    pub total_entries_written: u64,
    pub total_errors: u64,

    pub current_fragment: String,
    pub last_error: Option<String>,

    pub start_timestamp: i64,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
}

static PR_STATUS: LazyLock<Arc<tokio::sync::RwLock<PrefixRebuildJobStatus>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(PrefixRebuildJobStatus::default())));

static STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

#[inline]
fn should_stop() -> bool {
    STOP_SIGNAL.load(Ordering::Relaxed)
}

fn push_update(ws: &WsConnectionManager, st: &PrefixRebuildJobStatus) {
    ws.broadcast_system_wide(StorageEventPayload::PrefixRebuildUpdate(st.clone()));
}

pub async fn get_prefix_rebuild_status_api(claims: Claims) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(axum::Json(PR_STATUS.read().await.clone()))
}

pub async fn trigger_prefix_rebuild_job_api(
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
        let st = PR_STATUS.read().await;
        if st.is_running {
            return Err(AppError::Conflict("Job already running".into()));
        }
    }

    let fragment = params.get("fragment").cloned();
    let ws = Arc::new(state.ws_manager.clone());

    tokio::spawn(async move {
        if let Err(e) = run_prefix_rebuild_job(fragment, ws.clone()).await {
            error!("Prefix rebuild job failed: {}", e);
            let mut st = PR_STATUS.write().await;
            st.is_running = false;
            st.phase = "Failed".to_string();
            st.total_errors += 1;
            st.last_error = Some(e.to_string());
            push_update(&ws, &st);
        }
    });

    Ok((StatusCode::ACCEPTED, "Prefix rebuild initiated.").into_response())
}

pub async fn stop_prefix_rebuild_job_api(
    claims: Claims,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permissions required".into(),
        ));
    }

    STOP_SIGNAL.store(true, Ordering::Relaxed);

    let ws = state.ws_manager.clone();
    let mut st = PR_STATUS.write().await;
    if st.is_running {
        st.phase = "Stopping...".to_string();
        push_update(&ws, &st);
    }

    Ok((StatusCode::OK, "Stop signal sent.").into_response())
}

#[cfg(unix)]
async fn sync_dir(path: &Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;

        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY)
            .open(&path)
            .map_err(|e| AppError::Io(e, format!("open dir for sync: {:?}", path)))?;

        f.sync_all()
            .map_err(|e| AppError::Io(e, format!("sync dir: {:?}", path)))?;

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(windows)]
async fn sync_dir(path: &Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

        let result = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path);

        if let Ok(file) = result {
            let _ = file.sync_all();
        }

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(not(any(unix, windows)))]
async fn sync_dir(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

async fn write_atomic_overwrite(path: &Path, data: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent dir for {}", path.display()))
    })?;

    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, "create parent dir".into()))?;

    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        Ulid::new().to_string()
    ));

    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("open tmp {}", tmp.display())))?;

    f.write_all(data)
        .await
        .map_err(|e| AppError::Io(e, "write tmp".into()))?;

    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync tmp".into()))?;
    drop(f);

    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;
    Ok(())
}

fn canonical_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

fn canonical_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn prefixes_for_key(object_key: &str) -> BTreeSet<String> {
    let key = canonical_key(object_key);
    let mut out = BTreeSet::new();
    out.insert(String::new());

    if key.is_empty() {
        return out;
    }

    let parts: Vec<&str> = key.split('/').filter(|p| !p.is_empty()).collect();
    let mut cur = String::new();

    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            cur.push_str(p);
        } else {
            cur.push('/');
            cur.push_str(p);
        }
        out.insert(cur.clone());
    }

    out
}

fn is_hex2(s: &str) -> bool {
    if s.len() != 2 {
        return false;
    }
    s.as_bytes().iter().all(|b| match b {
        b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F' => true,
        _ => false,
    })
}

async fn list_all_fragments(index_roots: &[PathBuf]) -> Vec<String> {
    let roots = index_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut out = HashSet::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            if let Ok(rd) = std::fs::read_dir(&root) {
                for ent in rd.filter_map(|e| e.ok()) {
                    if ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        let name = ent.file_name().to_string_lossy().to_string();
                        if name.starts_with('.')
                            || name == "System Volume Information"
                            || name == "$RECYCLE.BIN"
                            || name == "lost+found"
                        {
                            continue;
                        }
                        out.insert(name);
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

async fn choose_best_index_dir_for_fragment(
    index_roots: &[PathBuf],
    fragment: &str,
) -> Option<PathBuf> {
    let mut best: Option<(PathBuf, u64, i64)> = None;

    for root in index_roots {
        let idx = root.join(fragment).join("index");
        if !fs::try_exists(&idx).await.unwrap_or(false) {
            continue;
        }

        let mut shard_count = 0u64;
        let mut access_ok = false;

        if let Ok(mut rd) = fs::read_dir(&idx).await {
            access_ok = true;
            while let Ok(Some(ent)) = rd.next_entry().await {
                let name = ent.file_name().to_string_lossy().to_string();
                if is_hex2(&name) {
                    shard_count += 1;
                }
            }
        }

        if !access_ok {
            continue;
        }

        let mut ts = 0i64;
        if let Ok(meta) = fs::metadata(&idx).await {
            if let Ok(m) = meta.modified() {
                ts = m
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
            }
        }

        match &best {
            None => best = Some((idx, shard_count, ts)),
            Some((_p, best_count, best_ts)) => {
                if shard_count > *best_count || (shard_count == *best_count && ts > *best_ts) {
                    best = Some((idx, shard_count, ts));
                }
            }
        }
    }

    best.map(|(p, _, _)| p)
}

async fn scan_latest_by_key(
    scan_index_dir: &Path,
    fragment: &str,
) -> Result<
    (
        u64,
        HashMap<String, PrefixIndexEntry>,
        Vec<(String, String)>,
    ),
    AppError,
> {
    let scan_root = scan_index_dir.to_path_buf();
    let frag = fragment.to_string();
    let now_ts = Utc::now().timestamp();
    let retention_secs = std::cmp::max(
        CONFIG.garbage_collector.retention_seconds as i64,
        TOMBSTONE_MIN_RETENTION_SECS,
    );
    let cutoff_ts = now_ts.saturating_sub(retention_secs);

    tokio::task::spawn_blocking(move || {
        let mut scanned = 0u64;
        let mut latest: HashMap<String, PrefixIndexEntry> = HashMap::new();
        let mut latest_tombstone: HashMap<String, (String, i64)> = HashMap::new();

        let prefix_dir_os = std::ffi::OsStr::new(PREFIX_INDEX_DIR);

        for entry in WalkDir::new(&scan_root).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_dir() {
                continue;
            }

            let p = entry.path();

            if p.components().any(|c| c.as_os_str() == prefix_dir_os) {
                continue;
            }

            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.ends_with(METADATA_EXT) {
                continue;
            }
            if name == RESERVATION_FILE || name.ends_with(PART_META_SUFFIX) {
                continue;
            }

            let md = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            let len = md.len();
            if len == 0 || len > MAX_META_BYTES {
                continue;
            }

            let raw = match std::fs::read(p) {
                Ok(b) => b,
                Err(_) => continue,
            };

            let (meta_frag, entry) = match rmp_serde::from_slice::<ObjectSummary>(&raw) {
                Ok(m) => (m.fragment.clone(), PrefixIndexEntry::from_summary(&m)),
                Err(_) => match rmp_serde::from_slice::<ObjectMetadata>(&raw) {
                    Ok(m) => (m.fragment.clone(), PrefixIndexEntry::from_metadata(&m)),
                    Err(_) => continue,
                },
            };

            if meta_frag != frag {
                continue;
            }

            let key = canonical_key(&entry.object_key);
            let ts = entry.created_at;

            if entry.is_delete_marker {
                match latest_tombstone.get(&key) {
                    None => {
                        latest_tombstone.insert(key, (entry.version_id.clone(), ts));
                    }
                    Some((cur_vid, cur_ts)) => {
                        if ts > *cur_ts || (ts == *cur_ts && &entry.version_id > cur_vid) {
                            latest_tombstone.insert(key, (entry.version_id.clone(), ts));
                        }
                    }
                }
                continue;
            }

            match latest.get(&key) {
                None => {
                    latest.insert(key, entry);
                }
                Some(current) => {
                    if ts > current.created_at
                        || (ts == current.created_at && entry.version_id > current.version_id)
                    {
                        latest.insert(key, entry);
                    }
                }
            }

            scanned += 1;
        }

        let mut stale_tombstones: Vec<(String, String)> = Vec::new();
        for (key, (vid, ts)) in latest_tombstone {
            let overshadowed = match latest.get(&key) {
                None => true,
                Some(current) => ts >= current.created_at,
            };
            if overshadowed && ts <= cutoff_ts {
                stale_tombstones.push((key, vid));
            }
        }

        Ok::<_, AppError>((scanned, latest, stale_tombstones))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("scan task join error: {}", e)))?
}

fn build_per_prefix(
    latest_by_key: HashMap<String, PrefixIndexEntry>,
) -> HashMap<String, Vec<PrefixIndexEntry>> {
    let mut per_prefix: HashMap<String, Vec<PrefixIndexEntry>> = HashMap::new();

    for (object_key, entry) in latest_by_key {
        for pfx in prefixes_for_key(&object_key) {
            per_prefix
                .entry(canonical_prefix(&pfx))
                .or_insert_with(Vec::new)
                .push(entry.clone());
        }
    }

    for (_pfx, v) in per_prefix.iter_mut() {
        v.sort_by(|a, b| match a.object_key.cmp(&b.object_key) {
            std::cmp::Ordering::Equal => a.version_id.cmp(&b.version_id),
            other => other,
        });
        v.dedup_by(|a, b| a.object_key == b.object_key && a.version_id == b.version_id);
    }

    per_prefix
}

async fn write_compacted_prefixes(
    path_manager: &ObjectPathManager,
    fragment: &str,
    per_prefix: HashMap<String, Vec<PrefixIndexEntry>>,
    job_start_time: SystemTime,
) -> Result<(u64, u64), AppError> {
    let mut prefixes_written = 0u64;
    let mut entries_written = 0u64;
    let mut parent_dirs_to_sync = HashSet::<PathBuf>::new();

    for (prefix, entries) in per_prefix {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }

        if entries.is_empty() {
            continue;
        }

        let seg = PrefixIndexSegment {
            prefix: prefix.clone(),
            entries,
        };

        let bytes = rmp_serde::to_vec(&seg).map_err(|e| {
            AppError::InternalServerError(format!("serialize prefix compacted: {}", e))
        })?;
        let delimiter_compacted = build_delimiter_compacted_from_entries(&prefix, &seg.entries);
        let delimiter_bytes = rmp_serde::to_vec(&delimiter_compacted).map_err(|e| {
            AppError::InternalServerError(format!("serialize delimiter prefix compacted: {}", e))
        })?;

        let mut wrote_prefix_any = false;
        let mut wrote_delimiter_any = false;
        let bucket_dirs = path_manager.get_all_prefix_bucket_dirs(fragment, &prefix);

        for p in path_manager.get_all_prefix_compacted_paths(fragment, &prefix) {
            if let Some(parent) = p.parent() {
                let _ = fs::create_dir_all(parent).await;
            }
            if write_atomic_overwrite(&p, &bytes).await.is_ok() {
                if let Some(parent) = p.parent() {
                    parent_dirs_to_sync.insert(parent.to_path_buf());
                }
                wrote_prefix_any = true;
            }
        }

        for p in path_manager.get_all_prefix_delimiter_compacted_paths(fragment, &prefix) {
            if let Some(parent) = p.parent() {
                let _ = fs::create_dir_all(parent).await;
            }
            if write_atomic_overwrite(&p, &delimiter_bytes).await.is_ok() {
                if let Some(parent) = p.parent() {
                    parent_dirs_to_sync.insert(parent.to_path_buf());
                }
                wrote_delimiter_any = true;
            }
        }

        if !wrote_prefix_any || !wrote_delimiter_any {
            return Err(AppError::InternalServerError(format!(
                "failed to write compacted for prefix {}",
                prefix
            )));
        }

        for dir in &bucket_dirs {
            if !fs::try_exists(dir).await.unwrap_or(false) {
                continue;
            }
            if let Ok(mut rd) = fs::read_dir(dir).await {
                while let Ok(Some(entry)) = rd.next_entry().await {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if (name.starts_with("seg_")
                        || name.starts_with(PREFIX_DELIMITER_SEGMENT_PREFIX))
                        && name.ends_with(PREFIX_SEGMENT_EXT)
                    {
                        if let Ok(md) = entry.metadata().await {
                            if let Ok(mtime) = md.modified() {
                                if mtime < job_start_time {
                                    let _ = fs::remove_file(entry.path()).await;
                                }
                            }
                        }
                    }
                }
            }
        }

        for dir in &bucket_dirs {
            if !fs::try_exists(dir).await.unwrap_or(false) {
                continue;
            }
            if fs::try_exists(&dir.join(PREFIX_DELIMITER_COMPACTED_FILE))
                .await
                .unwrap_or(false)
            {
                parent_dirs_to_sync.insert(dir.clone());
            }
        }

        prefixes_written += 1;
        entries_written += seg.entries.len() as u64;
    }

    for dir in parent_dirs_to_sync {
        let _ = sync_dir(&dir).await;
    }

    Ok((prefixes_written, entries_written))
}

async fn cleanup_stale_delete_markers(
    path_manager: &ObjectPathManager,
    fragment: &str,
    stale_tombstones: &[(String, String)],
) {
    for (object_key, version_id) in stale_tombstones {
        for p in path_manager.get_all_metadata_paths(fragment, object_key, version_id) {
            let _ = fs::remove_file(&p).await;
            if let Some(parent) = p.parent() {
                let _ = sync_dir(parent).await;
            }
        }
    }
}

pub async fn run_prefix_rebuild_job(
    fragment: Option<String>,
    ws: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();
    let sys_start_time = SystemTime::now();

    {
        let mut st = PR_STATUS.write().await;
        *st = PrefixRebuildJobStatus {
            is_running: true,
            phase: "Initializing".to_string(),
            start_timestamp: Utc::now().timestamp(),
            ..Default::default()
        };
        push_update(&ws, &st);
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let index_roots: Vec<PathBuf> = index_roots.into_iter().collect();

    let fragments = if let Some(f) = fragment {
        vec![f]
    } else {
        list_all_fragments(&index_roots).await
    };

    {
        let mut st = PR_STATUS.write().await;
        st.phase = format!("Preparing (fragments={})", fragments.len());
        push_update(&ws, &st);
    }

    for frag in fragments {
        if should_stop() {
            break;
        }

        {
            let mut st = PR_STATUS.write().await;
            st.current_fragment = frag.clone();
            st.total_fragments_scanned += 1;
            push_update(&ws, &st);
        }

        let scan_index_dir = match choose_best_index_dir_for_fragment(&index_roots, &frag).await {
            Some(p) => p,
            None => {
                info!("Skipping directory '{}' - no valid N1 index found.", frag);
                continue;
            }
        };

        {
            let mut st = PR_STATUS.write().await;
            st.phase = "Scanning metadata".to_string();
            push_update(&ws, &st);
        }

        let (scanned, latest_by_key, stale_tombstones) =
            match scan_latest_by_key(&scan_index_dir, &frag).await {
                Ok(v) => v,
                Err(e) => {
                    let mut st = PR_STATUS.write().await;
                    st.total_errors += 1;
                    st.last_error = Some(e.to_string());
                    push_update(&ws, &st);
                    continue;
                }
            };

        let elapsed = start_time.elapsed().as_secs_f64();
        {
            let mut st = PR_STATUS.write().await;
            st.total_metadata_scanned += scanned;
            st.total_objects_indexed += latest_by_key.len() as u64;
            st.duration_seconds = elapsed as u64;
            if elapsed > 0.0 {
                st.scan_rate_per_sec = st.total_metadata_scanned as f64 / elapsed;
            }
            st.phase = "Building prefix maps".to_string();
            push_update(&ws, &st);
        }

        let per_prefix = build_per_prefix(latest_by_key);

        {
            let mut st = PR_STATUS.write().await;
            st.phase = "Writing compacted __p__".to_string();
            push_update(&ws, &st);
        }

        match write_compacted_prefixes(&path_manager, &frag, per_prefix, sys_start_time).await {
            Ok((pfx_written, ent_written)) => {
                cleanup_stale_delete_markers(&path_manager, &frag, &stale_tombstones).await;
                let mut st = PR_STATUS.write().await;
                st.total_prefixes_written += pfx_written;
                st.total_entries_written += ent_written;
                st.phase = "Fragment completed".to_string();
                push_update(&ws, &st);
                ws.publish(&frag, StorageEventPayload::StateInvalidated);
            }
            Err(e) => {
                let mut st = PR_STATUS.write().await;
                st.total_errors += 1;
                st.last_error = Some(e.to_string());
                st.phase = "Fragment failed".to_string();
                push_update(&ws, &st);
            }
        }
    }

    {
        let mut st = PR_STATUS.write().await;
        st.is_running = false;
        st.phase = if should_stop() {
            "Stopped by User".to_string()
        } else {
            "Completed".to_string()
        };
        st.duration_seconds = start_time.elapsed().as_secs();
        push_update(&ws, &st);
    }

    info!("Prefix rebuild job ended.");
    Ok(())
}
