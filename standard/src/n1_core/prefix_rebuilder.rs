use std::collections::HashSet;
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

use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::PREFIX_SEGMENT_EXT;
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PrefixIndexEntry, PrefixIndexSegment,
};
use crate::n1_core::index;
use crate::n1_core::prefix_updates::{accelerator_prefix, clear_fragment_accelerator_stale};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;

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
    Query(params): Query<std::collections::BTreeMap<String, String>>,
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

fn build_root_accelerator_from_objects(objects: Vec<ObjectMetadata>) -> PrefixIndexSegment {
    let mut entries: Vec<PrefixIndexEntry> = objects
        .into_iter()
        .map(|metadata| PrefixIndexEntry::from_metadata(&metadata))
        .collect();
    entries.sort_by(|a, b| match canonical_key(&a.object_key).cmp(&canonical_key(&b.object_key)) {
        std::cmp::Ordering::Equal => a.version_id.cmp(&b.version_id),
        other => other,
    });
    entries.dedup_by(|a, b| a.object_key == b.object_key && a.version_id == b.version_id);

    PrefixIndexSegment {
        prefix: accelerator_prefix().to_string(),
        entries,
    }
}

async fn write_root_compacted_prefix(
    path_manager: &ObjectPathManager,
    fragment: &str,
    seg: PrefixIndexSegment,
    job_start_time: SystemTime,
    trashed: bool,
) -> Result<(u64, u64), AppError> {
    let mut parent_dirs_to_sync = HashSet::<PathBuf>::new();
    let bytes = rmp_serde::to_vec(&seg)
        .map_err(|e| AppError::InternalServerError(format!("serialize prefix compacted: {}", e)))?;
    let bucket_dirs = if trashed {
        path_manager.get_all_trash_prefix_bucket_dirs(fragment, accelerator_prefix())
    } else {
        path_manager.get_all_prefix_bucket_dirs(fragment, accelerator_prefix())
    };
    let mut wrote_any = false;

    if should_stop() {
        return Err(AppError::InternalServerError("Stopped".into()));
    }

    let compacted_paths = if trashed {
        path_manager.get_all_trash_prefix_compacted_paths(fragment, accelerator_prefix())
    } else {
        path_manager.get_all_prefix_compacted_paths(fragment, accelerator_prefix())
    };

    for p in compacted_paths {
        if let Some(parent) = p.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        if write_atomic_overwrite(&p, &bytes).await.is_ok() {
            if let Some(parent) = p.parent() {
                parent_dirs_to_sync.insert(parent.to_path_buf());
            }
            wrote_any = true;
        }
    }

    if !wrote_any {
        return Err(AppError::InternalServerError(
            "failed to write root accelerator snapshot".into(),
        ));
    }

    for dir in &bucket_dirs {
        if !fs::try_exists(dir).await.unwrap_or(false) {
            continue;
        }
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("seg_") && name.ends_with(PREFIX_SEGMENT_EXT) {
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

    for dir in parent_dirs_to_sync {
        let _ = sync_dir(&dir).await;
    }

    clear_fragment_accelerator_stale(fragment, trashed).await;

    Ok((1, seg.entries.len() as u64))
}

async fn rebuild_fragment_prefix_accelerator_internal(
    path_manager: &ObjectPathManager,
    _index_roots: &[PathBuf],
    fragment: &str,
    snapshot_time: SystemTime,
) -> Result<(u64, usize, u64, u64), AppError> {
    let live_objects = index::list_fragment_live_objects(fragment).await?;
    let trash_objects = index::list_fragment_trash_objects(fragment).await?;

    let scanned = (live_objects.len() + trash_objects.len()) as u64;
    let indexed_objects = live_objects.len() + trash_objects.len();

    let live_accelerator = build_root_accelerator_from_objects(live_objects);
    let trash_accelerator = build_root_accelerator_from_objects(trash_objects);

    let (live_prefixes_written, live_entries_written) =
        write_root_compacted_prefix(path_manager, fragment, live_accelerator, snapshot_time, false)
            .await?;
    let (trash_prefixes_written, trash_entries_written) =
        write_root_compacted_prefix(path_manager, fragment, trash_accelerator, snapshot_time, true)
            .await?;

    Ok((
        scanned,
        indexed_objects,
        live_prefixes_written + trash_prefixes_written,
        live_entries_written + trash_entries_written,
    ))
}

pub async fn rebuild_fragment_prefix_accelerator(fragment: &str) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let index_roots: Vec<PathBuf> = path_manager.get_all_index_roots().into_iter().collect();
    rebuild_fragment_prefix_accelerator_internal(
        &path_manager,
        &index_roots,
        fragment,
        SystemTime::now(),
    )
    .await
    .map(|_| ())
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

        {
            let mut st = PR_STATUS.write().await;
            st.phase = "Scanning metadata".to_string();
            push_update(&ws, &st);
        }

        match rebuild_fragment_prefix_accelerator_internal(
            &path_manager,
            &index_roots,
            &frag,
            sys_start_time,
        )
        .await
        {
            Ok((scanned, indexed_objects, pfx_written, ent_written)) => {
                let elapsed = start_time.elapsed().as_secs_f64();
                let mut st = PR_STATUS.write().await;
                st.total_metadata_scanned += scanned;
                st.total_objects_indexed += indexed_objects as u64;
                st.total_prefixes_written += pfx_written;
                st.total_entries_written += ent_written;
                st.duration_seconds = elapsed as u64;
                if elapsed > 0.0 {
                    st.scan_rate_per_sec = st.total_metadata_scanned as f64 / elapsed;
                }
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
