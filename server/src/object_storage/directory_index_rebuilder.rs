use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, PART_META_SUFFIX, QUOTA_FILE};
use crate::structs::object_storage_structs::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager,
};

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;

use chrono::Utc;
use futures_util::{StreamExt, stream};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use tokio::fs;
use tokio::sync::{RwLock, Semaphore, mpsc};
use tracing::{error, warn};
use walkdir::WalkDir;

pub const STRUCTURE_ROOT: &str = "structure";

#[derive(Debug, Clone, Serialize, Default)]
pub struct DirIndexRebuildStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_meta_files_seen: u64,
    pub total_meta_parsed_ok: u64,
    pub total_structure_written: u64,
    pub total_structure_failed: u64,
    pub total_structure_deleted: u64,
    pub total_structure_verified: u64,
    pub last_error: Option<String>,
    pub started_at_unix: i64,
    pub duration_seconds: u64,
    pub rate_meta_per_sec: f64,
}

static DIR_INDEX_STATUS: LazyLock<Arc<RwLock<DirIndexRebuildStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(DirIndexRebuildStatus::default())));

static DIR_INDEX_STOP: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

#[inline]
fn should_stop() -> bool {
    DIR_INDEX_STOP.load(Ordering::Relaxed)
}

#[inline]
fn clamp_usize(v: usize, lo: usize, hi: usize) -> usize {
    v.max(lo).min(hi)
}

#[inline]
fn make_comp_key(uid: &str, fragment: &str, object_key: &str) -> String {
    let mut s = String::with_capacity(uid.len() + fragment.len() + object_key.len() + 2);
    s.push_str(uid);
    s.push('\0');
    s.push_str(fragment);
    s.push('\0');
    s.push_str(object_key);
    s
}

fn hash_hex(object_key: &str) -> String {
    let mut h = Sha256::new();
    h.update(object_key.as_bytes());
    hex::encode(h.finalize())
}

pub async fn get_dir_index_rebuild_status() -> impl IntoResponse {
    Json(DIR_INDEX_STATUS.read().await.clone())
}

pub async fn stop_dir_index_rebuild_api() -> impl IntoResponse {
    DIR_INDEX_STOP.store(true, Ordering::Relaxed);
    let mut st = DIR_INDEX_STATUS.write().await;
    if st.is_running {
        st.phase = "Stopping...".to_string();
    }
    (StatusCode::OK, "Stop signal sent.").into_response()
}

pub async fn trigger_dir_index_rebuild_api(
    State(_state): State<Arc<crate::routes::storage_routes::AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    {
        let st = DIR_INDEX_STATUS.read().await;
        if st.is_running {
            return (StatusCode::CONFLICT, "Job already running").into_response();
        }
    }

    let dry_run = params.get("dry_run").map(|v| v == "true").unwrap_or(false);
    let wipe = params.get("wipe").map(|v| v == "true").unwrap_or(false);
    let verify = params.get("verify").map(|v| v == "true").unwrap_or(false);

    let meta_concurrency = clamp_usize(
        params
            .get("meta_concurrency")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(16),
        1,
        128,
    );

    let write_concurrency = clamp_usize(
        params
            .get("write_concurrency")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64),
        1,
        512,
    );

    let batch = clamp_usize(
        params
            .get("batch")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1024),
        64,
        16384,
    );

    tokio::spawn(async move {
        if let Err(e) = run_dir_index_rebuilder(
            dry_run,
            wipe,
            verify,
            meta_concurrency,
            write_concurrency,
            batch,
        )
        .await
        {
            error!("Dir index rebuild failed: {}", e);
            let mut st = DIR_INDEX_STATUS.write().await;
            st.is_running = false;
            st.phase = "Failed".to_string();
            st.last_error = Some(e.to_string());
        }
    });

    (StatusCode::ACCEPTED, "Shadow index rebuild initiated.").into_response()
}

pub async fn run_dir_index_rebuilder(
    dry_run: bool,
    wipe: bool,
    verify: bool,
    meta_concurrency: usize,
    write_concurrency: usize,
    batch_size: usize,
) -> Result<(), AppError> {
    DIR_INDEX_STOP.store(false, Ordering::Relaxed);
    let started = Instant::now();

    {
        let mut st = DIR_INDEX_STATUS.write().await;
        *st = DirIndexRebuildStatus {
            is_running: true,
            phase: format!(
                "Initializing (dry_run={}, wipe={}, verify={}, meta_conc={}, write_conc={}, batch={})",
                dry_run, wipe, verify, meta_concurrency, write_concurrency, batch_size
            ),
            started_at_unix: Utc::now().timestamp(),
            ..Default::default()
        };
    }

    let idx_cfg = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;

    let index_roots: Vec<PathBuf> = idx_cfg
        .iter()
        .map(|s| PathBuf::from(s).join(OBJECT_DATA_ROOT))
        .collect();

    if index_roots.is_empty() {
        let mut st = DIR_INDEX_STATUS.write().await;
        st.is_running = false;
        st.phase = "No index roots configured".to_string();
        return Ok(());
    }

    let pm = ObjectPathManager::new();
    let all_index_roots = pm.get_all_index_roots();

    if wipe {
        {
            let mut st = DIR_INDEX_STATUS.write().await;
            st.phase = "Wiping existing structure/".to_string();
        }
        wipe_structure_roots(&all_index_roots, dry_run).await?;
    }

    {
        let mut st = DIR_INDEX_STATUS.write().await;
        st.phase = "Scanning authoritative metadata".to_string();
    }

    let (auth_map, auth_list) =
        scan_authoritative(index_roots, meta_concurrency, batch_size, started).await?;

    {
        let mut st = DIR_INDEX_STATUS.write().await;
        st.phase = "Reconciling structure/ against authoritative set".to_string();
    }

    reconcile_structure(
        &auth_map,
        &auth_list,
        &all_index_roots,
        dry_run,
        verify,
        write_concurrency,
    )
    .await?;

    let mut st = DIR_INDEX_STATUS.write().await;
    st.is_running = false;
    st.phase = if should_stop() {
        "Stopped".to_string()
    } else {
        "Finished".to_string()
    };
    st.duration_seconds = started.elapsed().as_secs();

    Ok(())
}

async fn wipe_structure_roots(roots: &[PathBuf], dry_run: bool) -> Result<(), AppError> {
    for r in roots {
        if should_stop() {
            break;
        }
        let target = r.join(STRUCTURE_ROOT);
        if !fs::try_exists(&target).await.unwrap_or(false) {
            continue;
        }
        if dry_run {
            continue;
        }
        if let Err(e) = fs::remove_dir_all(&target).await {
            warn!("Failed to wipe {:?}: {}", target, e);
        }
    }
    Ok(())
}

async fn scan_authoritative(
    roots: Vec<PathBuf>,
    meta_concurrency: usize,
    batch_size: usize,
    started: Instant,
) -> Result<
    (
        HashMap<String, String>,
        Vec<(String, String, String, String)>,
    ),
    AppError,
> {
    let (tx, mut rx) = mpsc::channel::<PathBuf>(8192);

    tokio::task::spawn_blocking(move || {
        for root in roots {
            if should_stop() {
                break;
            }
            if !root.exists() {
                continue;
            }

            for entry in WalkDir::new(root)
                .follow_links(false)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if should_stop() {
                    break;
                }
                if !entry.file_type().is_file() {
                    continue;
                }

                let p = entry.path();
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");

                if name == QUOTA_FILE || name == "reservation.bson" {
                    continue;
                }
                if name.ends_with(PART_META_SUFFIX) {
                    continue;
                }
                if !name.ends_with(".bson") {
                    continue;
                }

                let _ = tx.blocking_send(p.to_path_buf());
            }
        }
    });

    let mut batch: Vec<PathBuf> = Vec::with_capacity(batch_size);
    let mut auth_map: HashMap<String, String> = HashMap::new();
    let mut auth_list: Vec<(String, String, String, String)> = Vec::new();

    while let Some(p) = rx.recv().await {
        if should_stop() {
            break;
        }
        batch.push(p);
        if batch.len() >= batch_size {
            apply_auth_batch(
                &batch,
                meta_concurrency,
                &mut auth_map,
                &mut auth_list,
                started,
            )
            .await;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        apply_auth_batch(
            &batch,
            meta_concurrency,
            &mut auth_map,
            &mut auth_list,
            started,
        )
        .await;
    }

    Ok((auth_map, auth_list))
}

async fn apply_auth_batch(
    batch: &[PathBuf],
    meta_concurrency: usize,
    auth_map: &mut HashMap<String, String>,
    auth_list: &mut Vec<(String, String, String, String)>,
    started: Instant,
) {
    let results = stream::iter(batch.iter().cloned())
        .map(|meta_path| async move {
            let seen = 1u64;

            let data = match fs::read(&meta_path).await {
                Ok(d) => d,
                Err(_) => return (seen, 0u64, None),
            };

            let meta = match mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                Ok(m) => m,
                Err(_) => return (seen, 0u64, None),
            };

            let uid = meta.owner_uid;
            let fragment = meta.fragment;
            let object_key = meta.object_key;

            let hash = hash_hex(&object_key);
            let comp = make_comp_key(&uid, &fragment, &object_key);

            (seen, 1u64, Some((comp, uid, fragment, object_key, hash)))
        })
        .buffer_unordered(meta_concurrency)
        .collect::<Vec<(u64, u64, Option<(String, String, String, String, String)>)>>()
        .await;

    let mut seen_total = 0u64;
    let mut parsed_ok_total = 0u64;

    for (seen, ok, item) in results {
        seen_total += seen;
        parsed_ok_total += ok;

        if let Some((comp, uid, fragment, object_key, hash)) = item {
            if !auth_map.contains_key(&comp) {
                auth_map.insert(comp, hash.clone());
                auth_list.push((uid, fragment, object_key, hash));
            }
        }
    }

    {
        let mut st = DIR_INDEX_STATUS.write().await;
        st.total_meta_files_seen += seen_total;
        st.total_meta_parsed_ok += parsed_ok_total;

        let elapsed = started.elapsed().as_secs_f64();
        st.duration_seconds = elapsed as u64;
        if elapsed > 0.0 {
            st.rate_meta_per_sec = st.total_meta_files_seen as f64 / elapsed;
        }
    }
}

async fn reconcile_structure(
    auth_map: &HashMap<String, String>,
    auth_list: &[(String, String, String, String)],
    roots: &[PathBuf],
    dry_run: bool,
    verify: bool,
    write_concurrency: usize,
) -> Result<(), AppError> {
    let write_sem = Arc::new(Semaphore::new(write_concurrency));

    for root in roots {
        if should_stop() {
            break;
        }

        let structure_root = root.join(STRUCTURE_ROOT);
        let mut seen_in_shadow: HashSet<String> = HashSet::new();

        if fs::try_exists(&structure_root).await.unwrap_or(false) {
            for entry in WalkDir::new(&structure_root)
                .follow_links(false)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if should_stop() {
                    break;
                }

                if !entry.file_type().is_file() {
                    continue;
                }

                let rel = match entry.path().strip_prefix(&structure_root) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                let mut it = rel.iter();
                let uid = match it.next().and_then(|p| p.to_str()) {
                    Some(s) if !s.is_empty() => s.to_string(),
                    _ => continue,
                };
                let fragment = match it.next().and_then(|p| p.to_str()) {
                    Some(s) if !s.is_empty() => s.to_string(),
                    _ => continue,
                };

                let mut key_buf = String::from("/");
                let mut first = true;
                for c in it {
                    let s = c.to_string_lossy();
                    if first {
                        first = false;
                    } else {
                        key_buf.push('/');
                    }
                    key_buf.push_str(&s);
                }
                if key_buf == "/" {
                    continue;
                }

                let comp = make_comp_key(&uid, &fragment, &key_buf);
                let expected = auth_map.get(&comp).cloned();

                if expected.is_none() {
                    {
                        let mut st = DIR_INDEX_STATUS.write().await;
                        st.total_structure_deleted += 1;
                    }
                    if !dry_run {
                        let _ = fs::remove_file(entry.path()).await;
                    }
                    continue;
                }

                seen_in_shadow.insert(comp.clone());

                if verify {
                    let expected_hash = expected.unwrap();
                    let content = fs::read_to_string(entry.path()).await.unwrap_or_default();
                    let content_trim = content.trim();

                    let ok = content_trim == expected_hash;

                    {
                        let mut st = DIR_INDEX_STATUS.write().await;
                        st.total_structure_verified += 1;
                        if !ok {
                            st.total_structure_deleted += 1;
                        }
                    }

                    if !ok && !dry_run {
                        let _ = fs::remove_file(entry.path()).await;
                    }
                }
            }
        }

        for (uid, fragment, object_key, hash) in auth_list {
            if should_stop() {
                break;
            }

            let comp = make_comp_key(uid, fragment, object_key);
            if seen_in_shadow.contains(&comp) {
                continue;
            }

            let safe_key = object_key.trim_start_matches('/');
            let path = structure_root.join(uid).join(fragment).join(safe_key);

            if dry_run {
                let mut st = DIR_INDEX_STATUS.write().await;
                st.total_structure_written += 1;
                continue;
            }

            let permit = match write_sem.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    let mut st = DIR_INDEX_STATUS.write().await;
                    st.total_structure_failed += 1;
                    continue;
                }
            };

            let parent_ok = match path.parent() {
                Some(parent) => fs::create_dir_all(parent).await.is_ok(),
                None => false,
            };

            let write_ok = if parent_ok {
                fs::write(&path, hash.as_bytes()).await.is_ok()
            } else {
                false
            };

            drop(permit);

            let mut st = DIR_INDEX_STATUS.write().await;
            if write_ok {
                st.total_structure_written += 1;
            } else {
                st.total_structure_failed += 1;
            }
        }
    }

    Ok(())
}
