use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{
    FRAGMENT_METADATA_FILE, METADATA_EXT, PARITY_SUFFIX, PART_META_SUFFIX, QUOTA_FILE,
    RESERVATION_FILE, SESSION_MANIFEST_FILE, SHARD_SUFFIX,
};
use crate::object_storage::disk_manager::{
    ObjectMetadata, ObjectPathManager, PartMetadata, Quota, INDEX_STORE_DISKS,
};
use crate::object_storage::prefix_rebuilder::run_prefix_rebuild_job;
use crate::object_storage::recovery::{repair_entire_shard, verify_shard_integrity};
use crate::object_storage::upload::UploadSessionManifest;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;

use axum::extract::State;
use axum::{extract::Query, http::StatusCode, response::IntoResponse, Json};
use chrono::Utc;
use futures_util::{stream, StreamExt};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime};
use tokio::fs::{self, File};
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Default)]
pub struct RepairJobStatus {
    pub is_running: bool,
    pub phase: String,

    pub total_scanned: u64,
    pub total_repaired: u64,
    pub corrupt_found: u64,

    pub missing_meta_repaired: u64,
    pub divergent_meta_repaired: u64,
    pub last_disagreement: Option<String>,

    pub current_fragment: String,
    pub last_repaired: Option<String>,
    pub last_error: Option<String>,

    pub start_timestamp: i64,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
}

static REPAIR_STATUS: LazyLock<Arc<RwLock<RepairJobStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(RepairJobStatus::default())));

static STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairCheckMode {
    Availability,
    Scrub,
}

#[derive(Clone, Debug)]
pub struct RepairRunConfig {
    pub dry_run: bool,
    pub check_mode: RepairCheckMode,
    pub meta_concurrency: usize,
    pub shard_repair_concurrency: usize,
    pub meta_batch_size: usize,
}

#[derive(Default, Clone)]
struct BatchDelta {
    scanned: u64,
    repaired_shards: u64,
    corrupt_found: u64,
    missing_meta_repaired: u64,
    divergent_meta_repaired: u64,
    last_repaired: Option<String>,
    last_disagreement: Option<String>,
}

#[inline]
fn should_stop() -> bool {
    STOP_SIGNAL.load(Ordering::Relaxed)
}

#[inline]
fn clamp_usize(v: usize, lo: usize, hi: usize) -> usize {
    v.max(lo).min(hi)
}

fn parse_check_mode(params: &HashMap<String, String>) -> RepairCheckMode {
    match params.get("mode").map(|s| s.as_str()) {
        Some("scrub") | Some("strict") | Some("checksum") => RepairCheckMode::Scrub,
        _ => RepairCheckMode::Availability,
    }
}

#[inline]
fn strip_tmp_attempt_suffix(s: &str) -> &str {
    if let Some(pos) = s.find(".tmp.") {
        &s[..pos]
    } else {
        s
    }
}

#[inline]
fn shard_ext(is_parity: bool) -> &'static str {
    if is_parity {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    }
}

#[inline]
fn stable_shard_file_name(part_index: u64, shard_index: usize, is_parity: bool) -> String {
    format!(
        "part_{}_shard_{}{}",
        part_index,
        shard_index,
        shard_ext(is_parity)
    )
}

fn expected_shard_path(
    path_manager: &ObjectPathManager,
    blob_roots: &[PathBuf],
    fragment: &str,
    version_id: &str,
    part_index: u64,
    shard_index: usize,
    is_parity: bool,
) -> Option<PathBuf> {
    let num_disks = blob_roots.len();
    if num_disks == 0 {
        return None;
    }
    let disk_idx = (part_index as usize + shard_index) % num_disks;
    if disk_idx >= blob_roots.len() {
        return None;
    }
    let disk_root = &blob_roots[disk_idx];
    let shard_dir = path_manager.get_shard_dir_on_disk(disk_root, fragment, version_id);
    let name = stable_shard_file_name(part_index, shard_index, is_parity);
    Some(shard_dir.join(name))
}

async fn normalize_part_paths_for_repair(
    path_manager: &ObjectPathManager,
    blob_roots: &[PathBuf],
    fragment: &str,
    version_id: &str,
    mut part: PartMetadata,
) -> PartMetadata {
    for shard in part.shards.iter_mut() {
        let stable_name =
            stable_shard_file_name(part.part_index, shard.shard_index, shard.is_parity);
        shard.file_name = stable_name;

        let abs = shard.absolute_path.clone();
        let abs_norm = strip_tmp_attempt_suffix(abs.as_str()).to_string();

        let expected = expected_shard_path(
            path_manager,
            blob_roots,
            fragment,
            version_id,
            part.part_index,
            shard.shard_index,
            shard.is_parity,
        );

        let mut chosen: Option<PathBuf> = None;

        if !abs_norm.is_empty() {
            let p = PathBuf::from(&abs_norm);
            if fs::try_exists(&p).await.unwrap_or(false) {
                chosen = Some(p);
            }
        }

        if chosen.is_none() {
            if let Some(exp) = expected.as_ref() {
                if fs::try_exists(exp).await.unwrap_or(false) {
                    chosen = Some(exp.clone());
                }
            }
        }

        if chosen.is_none() {
            if let Some(exp) = expected {
                chosen = Some(exp);
            } else if !abs_norm.is_empty() {
                chosen = Some(PathBuf::from(abs_norm));
            }
        }

        if let Some(p) = chosen {
            shard.absolute_path = p.to_string_lossy().to_string();
        }
    }

    part
}

pub async fn get_repair_status() -> RepairJobStatus {
    REPAIR_STATUS.read().await.clone()
}

pub async fn get_repair_status_api(claims: Claims) -> Result<Json<RepairJobStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(REPAIR_STATUS.read().await.clone()))
}

pub async fn trigger_repair_job_api(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permission required".into(),
        ));
    }

    {
        let status = REPAIR_STATUS.read().await;
        if status.is_running {
            return Err(AppError::Conflict(
                "A repair job is already running.".into(),
            ));
        }
    }

    let cfg = RepairRunConfig {
        dry_run: params.get("dry_run").map(|v| v == "true").unwrap_or(false),
        check_mode: parse_check_mode(&params),
        meta_concurrency: clamp_usize(
            params
                .get("meta_concurrency")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(16),
            1,
            128,
        ),
        shard_repair_concurrency: clamp_usize(
            params
                .get("repair_concurrency")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(4),
            1,
            64,
        ),
        meta_batch_size: clamp_usize(
            params
                .get("batch")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(512),
            32,
            8192,
        ),
    };

    let ws_manager = Arc::new(state.ws_manager.clone());
    let ws_manager_clone = ws_manager.clone();

    tokio::spawn(async move {
        if let Err(e) = run_repair_job(cfg, ws_manager.clone()).await {
            error!("Manual Repair Job Failed: {}", e);
            let mut status = REPAIR_STATUS.write().await;
            status.is_running = false;
            status.phase = "Failed".to_string();
            status.last_error = Some(e.to_string());
            ws_manager_clone
                .broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        "Storage repair job initiated. Check logs or poll /status/repair.",
    )
        .into_response())
}

pub async fn stop_repair_job_api(
    claims: Claims,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permission required".into(),
        ));
    }

    STOP_SIGNAL.store(true, Ordering::Relaxed);
    let mut status = REPAIR_STATUS.write().await;
    if status.is_running {
        status.phase = "Stopping...".to_string();
        state
            .ws_manager
            .broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }
    Ok((StatusCode::OK, "Stop signal sent to repair manager."))
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
                        if let Some(name) = ent.file_name().to_str() {
                            if !name.starts_with('.') {
                                out.insert(name.to_string());
                            }
                        }
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

fn sha256_bytes(b: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out[..]);
    arr
}

async fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    let _ = fs::create_dir_all(parent).await;
    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        Ulid::new().to_string()
    ));
    fs::write(&tmp, data).await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

#[derive(Clone)]
struct RawReplica {
    bytes: Option<Vec<u8>>,
    mtime: SystemTime,
    len: u64,
    sha256: Option<[u8; 32]>,
    valid: bool,
}

async fn read_raw_replica<F>(full: &Path, valid_fn: &F) -> RawReplica
where
    F: Fn(&[u8]) -> bool + Send + Sync,
{
    match fs::read(full).await {
        Ok(bytes) => {
            let meta = fs::metadata(full).await.ok();
            let mtime = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta.as_ref().map(|m| m.len()).unwrap_or(bytes.len() as u64);
            let sha = Some(sha256_bytes(&bytes));
            let valid = valid_fn(&bytes);

            RawReplica {
                bytes: Some(bytes),
                mtime,
                len,
                sha256: sha,
                valid,
            }
        }
        Err(_) => RawReplica {
            bytes: None,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            sha256: None,
            valid: false,
        },
    }
}

fn choose_best_raw_replica(replicas: &[RawReplica]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (i, r) in replicas.iter().enumerate() {
        if !r.valid {
            continue;
        }
        let take = match best {
            None => true,
            Some(cur) => {
                let c = &replicas[cur];
                (r.mtime > c.mtime)
                    || (r.mtime == c.mtime && r.len > c.len)
                    || (r.mtime == c.mtime
                        && r.len == c.len
                        && r.sha256.unwrap_or([0u8; 32]) > c.sha256.unwrap_or([0u8; 32]))
            }
        };
        if take {
            best = Some(i);
        }
    }
    best
}

async fn replicate_bytes_strict(
    paths: &[PathBuf],
    canonical: &[u8],
    dry_run: bool,
) -> Result<(u64, u64), AppError> {
    let canon_sha = sha256_bytes(canonical);
    let mut missing = 0u64;
    let mut divergent = 0u64;

    for p in paths {
        match fs::read(p).await {
            Ok(b) => {
                if sha256_bytes(&b) != canon_sha {
                    divergent += 1;
                    if !dry_run {
                        atomic_write(p, canonical)
                            .await
                            .map_err(|e| AppError::Io(e, "repair mirror file".into()))?;
                    }
                }
            }
            Err(_) => {
                missing += 1;
                if !dry_run {
                    atomic_write(p, canonical)
                        .await
                        .map_err(|e| AppError::Io(e, "repair mirror file".into()))?;
                }
            }
        }
    }

    Ok((missing, divergent))
}

async fn repair_fragment_metadata_mirrors(
    index_roots: &[PathBuf],
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let paths: Vec<PathBuf> = index_roots
        .iter()
        .map(|r| r.join(fragment).join(FRAGMENT_METADATA_FILE))
        .collect();

    let valid = |b: &[u8]| -> bool { rmp_serde::from_slice::<serde_json::Value>(b).is_ok() };

    let mut reps = Vec::with_capacity(paths.len());
    for p in &paths {
        reps.push(read_raw_replica(p, &valid).await);
    }

    let Some(best) = choose_best_raw_replica(&reps) else {
        let any_exists = reps.iter().any(|r| r.bytes.is_some());
        let corrupt = if any_exists { 1 } else { 0 };
        return Ok((0, 0, corrupt));
    };

    let canon = reps[best].bytes.clone().unwrap();
    let (m, d) = replicate_bytes_strict(&paths, &canon, dry_run).await?;
    Ok((m, d, 0))
}

async fn list_all_session_versions(index_roots: &[PathBuf], fragment: &str) -> Vec<String> {
    let mut out = HashSet::<String>::new();

    for root in index_roots {
        let base = root.join(fragment).join("index").join("sessions");
        if !fs::try_exists(&base).await.unwrap_or(false) {
            continue;
        }

        let mut rd = match fs::read_dir(&base).await {
            Ok(r) => r,
            Err(_) => continue,
        };

        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            if !p.is_dir() {
                continue;
            }
            let vid = ent.file_name().to_string_lossy().to_string();
            if !vid.is_empty() {
                out.insert(vid);
            }
        }
    }

    let mut v: Vec<String> = out.into_iter().collect();
    v.sort();
    v
}

async fn list_part_meta_names(session_dirs: &[PathBuf]) -> HashSet<String> {
    let mut out = HashSet::<String>::new();

    for d in session_dirs {
        if !fs::try_exists(d).await.unwrap_or(false) {
            continue;
        }

        let mut rd = match fs::read_dir(d).await {
            Ok(r) => r,
            Err(_) => continue,
        };

        while let Ok(Some(ent)) = rd.next_entry().await {
            let name = ent.file_name().to_string_lossy().to_string();
            if name.ends_with(PART_META_SUFFIX) && name.starts_with("part_") {
                out.insert(name);
            }
        }
    }

    out
}

async fn repair_fragment_sessions_strict(
    fragment: &str,
    index_roots: &[PathBuf],
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let pm = ObjectPathManager::new();
    let versions = list_all_session_versions(index_roots, fragment).await;

    let mut miss = 0u64;
    let mut div = 0u64;
    let mut corrupt = 0u64;

    for version_id in versions {
        if should_stop() {
            break;
        }

        let session_dirs = pm.get_all_session_dirs(fragment, &version_id);
        for d in &session_dirs {
            let _ = fs::create_dir_all(d).await;
        }

        let manifest_paths: Vec<PathBuf> = session_dirs
            .iter()
            .map(|d| d.join(SESSION_MANIFEST_FILE))
            .collect();

        let manifest_valid = |b: &[u8]| -> bool {
            if let Ok(m) = rmp_serde::from_slice::<UploadSessionManifest>(b) {
                m.fragment == fragment && m.version_id == version_id
            } else {
                false
            }
        };

        let mut reps = Vec::with_capacity(manifest_paths.len());
        for p in &manifest_paths {
            reps.push(read_raw_replica(p, &manifest_valid).await);
        }

        let Some(best) = choose_best_raw_replica(&reps) else {
            let any_exists = reps.iter().any(|r| r.bytes.is_some());
            if any_exists {
                corrupt += 1;
            }
            continue;
        };

        let canon_manifest = reps[best].bytes.clone().unwrap();
        let (m1, d1) = replicate_bytes_strict(&manifest_paths, &canon_manifest, dry_run).await?;
        miss += m1;
        div += d1;

        let receipt_paths: Vec<PathBuf> = session_dirs
            .iter()
            .map(|d| d.join(RESERVATION_FILE))
            .collect();
        let receipt_valid = |b: &[u8]| -> bool {
            rmp_serde::from_slice::<crate::object_storage::disk_manager::ReservationReceipt>(b)
                .is_ok()
        };

        let mut reps = Vec::with_capacity(receipt_paths.len());
        for p in &receipt_paths {
            reps.push(read_raw_replica(p, &receipt_valid).await);
        }

        if let Some(best) = choose_best_raw_replica(&reps) {
            let canon_receipt = reps[best].bytes.clone().unwrap();
            let (m2, d2) = replicate_bytes_strict(&receipt_paths, &canon_receipt, dry_run).await?;
            miss += m2;
            div += d2;
        }

        let part_names = list_part_meta_names(&session_dirs).await;
        for name in part_names {
            if should_stop() {
                break;
            }

            let paths: Vec<PathBuf> = session_dirs.iter().map(|d| d.join(&name)).collect();
            let part_valid =
                |b: &[u8]| -> bool { rmp_serde::from_slice::<PartMetadata>(b).is_ok() };

            let mut reps = Vec::with_capacity(paths.len());
            for p in &paths {
                reps.push(read_raw_replica(p, &part_valid).await);
            }

            let Some(best) = choose_best_raw_replica(&reps) else {
                let any_exists = reps.iter().any(|r| r.bytes.is_some());
                if any_exists {
                    corrupt += 1;
                }
                continue;
            };

            let canon_part = reps[best].bytes.clone().unwrap();
            let (m3, d3) = replicate_bytes_strict(&paths, &canon_part, dry_run).await?;
            miss += m3;
            div += d3;
        }
    }

    Ok((miss, div, corrupt))
}

#[derive(Clone)]
struct ReplicaInfo {
    full_path: PathBuf,
    bytes: Option<Vec<u8>>,
    mtime: SystemTime,
    len: u64,
    sha256: Option<[u8; 32]>,
    parsed_ok: bool,
}

async fn read_replica(full: &Path) -> ReplicaInfo {
    match fs::read(full).await {
        Ok(bytes) => {
            let meta = fs::metadata(full).await.ok();
            let mtime = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta.as_ref().map(|m| m.len()).unwrap_or(bytes.len() as u64);
            let sha = Some(sha256_bytes(&bytes));
            let parsed_ok = rmp_serde::from_slice::<ObjectMetadata>(&bytes).is_ok();
            ReplicaInfo {
                full_path: full.to_path_buf(),
                bytes: Some(bytes),
                mtime,
                len,
                sha256: sha,
                parsed_ok,
            }
        }
        Err(_) => ReplicaInfo {
            full_path: full.to_path_buf(),
            bytes: None,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            sha256: None,
            parsed_ok: false,
        },
    }
}

async fn reconcile_object_metadata_strict(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    dry_run: bool,
) -> Result<(Vec<u8>, ObjectMetadata, u64, u64, Option<String>), AppError> {
    let path_manager = ObjectPathManager::new();
    let all_paths = path_manager.get_all_metadata_paths(fragment, object_key, version_id);

    if all_paths.is_empty() {
        return Err(AppError::InternalServerError(
            "No metadata paths configured".into(),
        ));
    }

    let mut replicas: Vec<ReplicaInfo> = Vec::with_capacity(all_paths.len());
    for p in &all_paths {
        replicas.push(read_replica(p).await);
    }

    let mut best_idx: Option<usize> = None;
    let mut best_meta: Option<ObjectMetadata> = None;

    for (idx, ri) in replicas.iter().enumerate() {
        if !ri.parsed_ok {
            continue;
        }
        let Some(bytes) = &ri.bytes else {
            continue;
        };
        let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(bytes) else {
            continue;
        };

        match best_idx {
            None => {
                best_idx = Some(idx);
                best_meta = Some(meta);
            }
            Some(cur) => {
                let cur_ri = &replicas[cur];
                let take = (ri.mtime > cur_ri.mtime)
                    || (ri.mtime == cur_ri.mtime && ri.len > cur_ri.len)
                    || (ri.mtime == cur_ri.mtime
                        && ri.len == cur_ri.len
                        && ri.sha256.unwrap_or([0u8; 32]) > cur_ri.sha256.unwrap_or([0u8; 32]));
                if take {
                    best_idx = Some(idx);
                    best_meta = Some(meta);
                }
            }
        }
    }

    let best_idx = best_idx.ok_or_else(|| {
        AppError::InternalServerError(format!(
            "All replicas unreadable/corrupt msgpack for {}/{}/{}",
            fragment, object_key, version_id
        ))
    })?;

    let canonical_bytes = replicas[best_idx]
        .bytes
        .clone()
        .ok_or_else(|| AppError::InternalServerError("Canonical replica missing bytes".into()))?;

    let canonical_meta = best_meta
        .ok_or_else(|| AppError::InternalServerError("Canonical replica missing meta".into()))?;
    let canon_sha = sha256_bytes(&canonical_bytes);

    let mut missing_repairs = 0u64;
    let mut divergent_repairs = 0u64;
    let mut report_lines: Vec<String> = Vec::new();

    for ri in &replicas {
        match &ri.bytes {
            None => {
                report_lines.push(format!("MISSING path={}", ri.full_path.display()));
                if !dry_run {
                    atomic_write(&ri.full_path, &canonical_bytes)
                        .await
                        .map_err(|e| AppError::Io(e, "repair missing index meta".into()))?;
                    missing_repairs += 1;
                }
            }
            Some(b) => {
                let sha = sha256_bytes(b);
                if sha != canon_sha {
                    report_lines.push(format!(
                        "DIVERGENT path={} len={} canon_path={}",
                        ri.full_path.display(),
                        b.len(),
                        replicas[best_idx].full_path.display()
                    ));
                    if !dry_run {
                        atomic_write(&ri.full_path, &canonical_bytes)
                            .await
                            .map_err(|e| AppError::Io(e, "repair divergent index meta".into()))?;
                        divergent_repairs += 1;
                    }
                }
            }
        }
    }

    let report = if report_lines.is_empty() {
        None
    } else {
        Some(format!(
            "INDEX DISAGREEMENT {}/{}/{}:\n{}",
            fragment,
            object_key,
            version_id,
            report_lines.join("\n")
        ))
    };

    Ok((
        canonical_bytes,
        canonical_meta,
        missing_repairs,
        divergent_repairs,
        report,
    ))
}

async fn repair_fragment_quota_mirrors(
    index_roots: &[PathBuf],
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let paths: Vec<PathBuf> = index_roots
        .iter()
        .map(|r| r.join(fragment).join(QUOTA_FILE))
        .collect();

    let valid = |b: &[u8]| -> bool { rmp_serde::from_slice::<Quota>(b).is_ok() };

    let mut reps = Vec::with_capacity(paths.len());
    for p in &paths {
        reps.push(read_raw_replica(p, &valid).await);
    }

    let Some(best) = choose_best_raw_replica(&reps) else {
        let any_exists = reps.iter().any(|r| r.bytes.is_some());
        let corrupt = if any_exists { 1 } else { 0 };
        return Ok((0, 0, corrupt));
    };

    let canon = reps[best].bytes.clone().unwrap();
    let (m, d) = replicate_bytes_strict(&paths, &canon, dry_run).await?;
    Ok((m, d, 0))
}

async fn process_object_repair(
    meta: ObjectMetadata,
    blob_roots: &Vec<PathBuf>,
    num_disks: usize,
    cfg: RepairRunConfig,
    shard_repair_sem: Arc<Semaphore>,
) -> Result<(usize, usize, Option<String>), AppError> {
    if meta.parts.is_empty() || num_disks == 0 {
        return Ok((0, 0, None));
    }

    let path_manager = ObjectPathManager::new();
    let meta_arc = Arc::new(meta);

    let mut repaired_total = 0usize;
    let mut corrupt_found = 0usize;
    let mut last_repaired: Option<String> = None;

    for part in &meta_arc.parts {
        if should_stop() {
            break;
        }

        let part_fixed = normalize_part_paths_for_repair(
            &path_manager,
            blob_roots,
            &meta_arc.fragment,
            &meta_arc.version_id,
            part.clone(),
        )
        .await;

        let part_arc = Arc::new(part_fixed);
        let mut missing_shards: Vec<usize> = Vec::new();

        for shard in &part_arc.shards {
            let shard_path = PathBuf::from(strip_tmp_attempt_suffix(&shard.absolute_path));
            let mut healthy = false;

            match fs::metadata(&shard_path).await {
                Ok(m) => {
                    if cfg.check_mode == RepairCheckMode::Availability {
                        healthy = m.len() == shard.size_bytes;
                        if !healthy {
                            corrupt_found += 1;
                            warn!("Size mismatch shard (availability): {:?}", shard_path);
                        }
                    } else {
                        if let Ok(mut f) = File::open(&shard_path).await {
                            healthy = verify_shard_integrity(&mut f, &shard.checksum).await;
                            if !healthy {
                                corrupt_found += 1;
                                warn!("Corrupt shard found: {:?}", shard_path);
                            }
                        }
                    }
                }
                Err(_) => {
                    healthy = false;
                }
            }

            if !healthy {
                missing_shards.push(shard.shard_index);
            }
        }

        if missing_shards.is_empty() || cfg.dry_run {
            continue;
        }

        let repairs = stream::iter(missing_shards.into_iter())
            .map(|missing_idx| {
                let part_for_task = part_arc.clone();
                let sem = shard_repair_sem.clone();
                let stripe = meta_arc.stripe_size_bytes;

                async move {
                    let _permit = sem.acquire().await.map_err(|_| {
                        AppError::InternalServerError("Repair semaphore closed".into())
                    })?;
                    repair_entire_shard(part_for_task, missing_idx, stripe).await
                }
            })
            .buffer_unordered(cfg.shard_repair_concurrency)
            .collect::<Vec<Result<(), AppError>>>()
            .await;

        for r in repairs {
            match r {
                Ok(_) => {
                    repaired_total += 1;
                    last_repaired = Some(format!("{}/{}", meta_arc.fragment, meta_arc.object_key));
                }
                Err(e) => {
                    warn!("Shard repair failed (disk might be offline): {}", e);
                }
            }
        }
    }

    Ok((repaired_total, corrupt_found, last_repaired))
}

fn dedupe_id_from_meta_path(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    if !name.ends_with(METADATA_EXT) {
        return None;
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    Some(h.finish())
}

fn should_scan_object_metadata_file(path: &Path) -> bool {
    let name = match path.file_name().and_then(|n| n.to_str()) {
        Some(s) => s,
        None => return false,
    };

    if !name.ends_with(METADATA_EXT) {
        return false;
    }

    if name.ends_with(PART_META_SUFFIX) {
        return false;
    }

    if name == QUOTA_FILE || name == RESERVATION_FILE || name == SESSION_MANIFEST_FILE {
        return false;
    }

    let p = path.to_string_lossy();
    if p.contains("/sessions/") || p.contains("\\sessions\\") {
        return false;
    }

    true
}

pub async fn run_repair_job(
    cfg: RepairRunConfig,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = true;
        status.phase = format!(
            "Initializing (mode={:?}, meta_conc={}, repair_conc={}, batch={}, dry_run={})",
            cfg.check_mode,
            cfg.meta_concurrency,
            cfg.shard_repair_concurrency,
            cfg.meta_batch_size,
            cfg.dry_run
        );
        status.total_scanned = 0;
        status.total_repaired = 0;
        status.corrupt_found = 0;
        status.missing_meta_repaired = 0;
        status.divergent_meta_repaired = 0;
        status.last_disagreement = None;
        status.current_fragment.clear();
        status.last_repaired = None;
        status.last_error = None;
        status.start_timestamp = Utc::now().timestamp();
        status.duration_seconds = 0;
        status.scan_rate_per_sec = 0.0;

        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    let path_manager = ObjectPathManager::new();
    let _index_roots_config = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;

    let index_roots: Vec<PathBuf> = path_manager.get_all_index_roots().into_iter().collect();
    let blob_roots = Arc::new(path_manager.get_blob_roots());
    let num_disks = blob_roots.len();

    let shard_repair_sem = Arc::new(Semaphore::new(cfg.shard_repair_concurrency));
    let fragments = list_all_fragments(&index_roots).await;

    info!(
        "Starting Storage Repair Job (fragments={}, index_roots={}). mode={:?}, dry_run={}",
        fragments.len(),
        index_roots.len(),
        cfg.check_mode,
        cfg.dry_run
    );

    for fragment_hash in fragments {
        if should_stop() {
            break;
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.current_fragment = fragment_hash.clone();
            status.phase = format!("Repairing Fragment {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Repairing fragment metadata {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let (fmiss, fdiv, fcorrupt) =
            repair_fragment_metadata_mirrors(&index_roots, &fragment_hash, cfg.dry_run).await?;

        {
            let mut status = REPAIR_STATUS.write().await;
            status.missing_meta_repaired += fmiss;
            status.divergent_meta_repaired += fdiv;
            status.corrupt_found += fcorrupt;
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Repairing upload sessions {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let (smiss, sdiv, scorrupt) =
            repair_fragment_sessions_strict(&fragment_hash, &index_roots, cfg.dry_run).await?;

        {
            let mut status = REPAIR_STATUS.write().await;
            status.missing_meta_repaired += smiss;
            status.divergent_meta_repaired += sdiv;
            status.corrupt_found += scorrupt;
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let scan_index_dir =
            match choose_best_index_dir_for_fragment(&index_roots, &fragment_hash).await {
                Some(p) => p,
                None => {
                    warn!(
                        "No readable index dir for fragment {}, skipping",
                        fragment_hash
                    );
                    continue;
                }
            };

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Repairing quota {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let (qmiss, qdiv, qcorrupt) =
            repair_fragment_quota_mirrors(&index_roots, &fragment_hash, cfg.dry_run).await?;

        {
            let mut status = REPAIR_STATUS.write().await;
            status.missing_meta_repaired += qmiss;
            status.divergent_meta_repaired += qdiv;
            status.corrupt_found += qcorrupt;
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel::<PathBuf>(1024);
        let scan_dir_clone = scan_index_dir.clone();

        tokio::task::spawn_blocking(move || {
            let walker = WalkDir::new(scan_dir_clone).into_iter();
            for entry in walker.filter_map(|e| e.ok()) {
                if !entry.file_type().is_file() {
                    continue;
                }
                let p = entry.path();
                if should_scan_object_metadata_file(p) {
                    if tx.blocking_send(p.to_owned()).is_err() {
                        break;
                    }
                }
            }
        });

        let mut pending: Vec<PathBuf> = Vec::with_capacity(cfg.meta_batch_size);
        let mut fragment_touched = false;
        let mut seen_ids: HashSet<u64> = HashSet::new();

        while let Some(meta_path) = rx.recv().await {
            if should_stop() {
                break;
            }

            if let Some(id) = dedupe_id_from_meta_path(&meta_path) {
                if !seen_ids.insert(id) {
                    continue;
                }
            }

            pending.push(meta_path);

            if pending.len() >= cfg.meta_batch_size {
                let d = apply_meta_batch(
                    &pending,
                    blob_roots.clone(),
                    num_disks,
                    cfg.clone(),
                    shard_repair_sem.clone(),
                    ws_manager.clone(),
                    start_time,
                )
                .await?;

                if d.repaired_shards > 0
                    || d.missing_meta_repaired > 0
                    || d.divergent_meta_repaired > 0
                {
                    fragment_touched = true;
                }

                pending.clear();
            }
        }

        if !pending.is_empty() {
            let d = apply_meta_batch(
                &pending,
                blob_roots.clone(),
                num_disks,
                cfg.clone(),
                shard_repair_sem.clone(),
                ws_manager.clone(),
                start_time,
            )
            .await?;

            if d.repaired_shards > 0 || d.missing_meta_repaired > 0 || d.divergent_meta_repaired > 0
            {
                fragment_touched = true;
            }

            pending.clear();
        }

        if !cfg.dry_run && fragment_touched && !should_stop() {
            {
                let mut status = REPAIR_STATUS.write().await;
                status.phase = format!("Rebuilding Prefix Index {}", fragment_hash);
                ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
            }

            if let Err(e) =
                run_prefix_rebuild_job(Some(fragment_hash.clone()), ws_manager.clone()).await
            {
                warn!(
                    "Prefix rebuild failed for fragment {}: {}",
                    fragment_hash, e
                );
            }
        }
    }

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = false;
        status.phase = if should_stop() {
            "Stopped by User".to_string()
        } else {
            "Completed".to_string()
        };
        status.duration_seconds = start_time.elapsed().as_secs();
        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));

        info!(
            "Repair Job Ended. Scanned: {}, Repaired: {}, Corrupt: {}, MissingMeta: {}, DivergentMeta: {}",
            status.total_scanned,
            status.total_repaired,
            status.corrupt_found,
            status.missing_meta_repaired,
            status.divergent_meta_repaired
        );
    }

    Ok(())
}

async fn apply_meta_batch(
    batch: &[PathBuf],
    blob_roots: Arc<Vec<PathBuf>>,
    num_disks: usize,
    cfg: RepairRunConfig,
    shard_repair_sem: Arc<Semaphore>,
    ws_manager: Arc<WsConnectionManager>,
    start_time: Instant,
) -> Result<BatchDelta, AppError> {
    if batch.is_empty() {
        return Ok(BatchDelta::default());
    }

    let mut stream = stream::iter(batch.iter().cloned())
        .map(|meta_path| {
            let blob_roots = blob_roots.clone();
            let shard_repair_sem = shard_repair_sem.clone();
            let cfg = cfg.clone();

            async move {
                process_one_meta_path(meta_path, blob_roots, num_disks, cfg, shard_repair_sem).await
            }
        })
        .buffer_unordered(cfg.meta_concurrency);

    let mut batch_agg = BatchDelta::default();
    let mut pending_update = BatchDelta::default();
    let mut last_update = Instant::now();

    while let Some(res) = stream.next().await {
        let delta = match res {
            Ok(d) => d,
            Err(e) => {
                warn!("Meta batch item failed (non-fatal): {}", e);
                continue;
            }
        };

        batch_agg.scanned += delta.scanned;
        batch_agg.repaired_shards += delta.repaired_shards;
        batch_agg.corrupt_found += delta.corrupt_found;
        batch_agg.missing_meta_repaired += delta.missing_meta_repaired;
        batch_agg.divergent_meta_repaired += delta.divergent_meta_repaired;
        if delta.last_repaired.is_some() {
            batch_agg.last_repaired = delta.last_repaired.clone();
        }
        if delta.last_disagreement.is_some() {
            batch_agg.last_disagreement = delta.last_disagreement.clone();
        }

        pending_update.scanned += delta.scanned;
        pending_update.repaired_shards += delta.repaired_shards;
        pending_update.corrupt_found += delta.corrupt_found;
        pending_update.missing_meta_repaired += delta.missing_meta_repaired;
        pending_update.divergent_meta_repaired += delta.divergent_meta_repaired;
        if delta.last_repaired.is_some() {
            pending_update.last_repaired = delta.last_repaired.clone();
        }
        if delta.last_disagreement.is_some() {
            pending_update.last_disagreement = delta.last_disagreement.clone();
        }

        let significant_change = delta.repaired_shards > 0
            || delta.missing_meta_repaired > 0
            || delta.divergent_meta_repaired > 0
            || delta.corrupt_found > 0;

        if significant_change
            || pending_update.scanned >= 10
            || last_update.elapsed().as_millis() > 200
        {
            let mut status = REPAIR_STATUS.write().await;
            status.total_scanned += pending_update.scanned;
            status.total_repaired += pending_update.repaired_shards
                + pending_update.missing_meta_repaired
                + pending_update.divergent_meta_repaired;
            status.corrupt_found += pending_update.corrupt_found;
            status.missing_meta_repaired += pending_update.missing_meta_repaired;
            status.divergent_meta_repaired += pending_update.divergent_meta_repaired;

            if let Some(s) = &pending_update.last_repaired {
                status.last_repaired = Some(s.clone());
            }
            if let Some(s) = &pending_update.last_disagreement {
                status.last_disagreement = Some(s.clone());
            }

            let elapsed = start_time.elapsed().as_secs_f64();
            status.duration_seconds = elapsed as u64;
            if elapsed > 0.0 {
                status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
            }

            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));

            pending_update = BatchDelta::default();
            last_update = Instant::now();
        }
    }

    if pending_update.scanned > 0 {
        let mut status = REPAIR_STATUS.write().await;
        status.total_scanned += pending_update.scanned;
        status.total_repaired += pending_update.repaired_shards
            + pending_update.missing_meta_repaired
            + pending_update.divergent_meta_repaired;
        status.corrupt_found += pending_update.corrupt_found;
        status.missing_meta_repaired += pending_update.missing_meta_repaired;
        status.divergent_meta_repaired += pending_update.divergent_meta_repaired;

        if let Some(s) = &pending_update.last_repaired {
            status.last_repaired = Some(s.clone());
        }
        if let Some(s) = &pending_update.last_disagreement {
            status.last_disagreement = Some(s.clone());
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        status.duration_seconds = elapsed as u64;
        if elapsed > 0.0 {
            status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
        }

        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    Ok(batch_agg)
}

async fn process_one_meta_path(
    meta_path: PathBuf,
    blob_roots: Arc<Vec<PathBuf>>,
    num_disks: usize,
    cfg: RepairRunConfig,
    shard_repair_sem: Arc<Semaphore>,
) -> Result<BatchDelta, AppError> {
    let mut delta = BatchDelta::default();
    if should_stop() {
        return Ok(delta);
    }

    delta.scanned = 1;

    let data = match fs::read(&meta_path).await {
        Ok(d) => d,
        Err(_) => return Ok(delta),
    };

    let meta = match rmp_serde::from_slice::<ObjectMetadata>(&data) {
        Ok(m) => m,
        Err(_) => {
            delta.corrupt_found += 1;
            return Ok(delta);
        }
    };

    if meta.is_delete_marker {
        return Ok(delta);
    }

    let (_canonical_data, canonical_meta, miss_rep, div_rep, report) =
        reconcile_object_metadata_strict(
            &meta.fragment,
            &meta.object_key,
            &meta.version_id,
            cfg.dry_run,
        )
        .await?;

    delta.missing_meta_repaired += miss_rep;
    delta.divergent_meta_repaired += div_rep;

    if let Some(r) = report {
        delta.last_disagreement = Some(r.clone());
        if cfg.dry_run {
            warn!("{}", r);
        }
    }

    let (repaired, corrupt, last_rep) = process_object_repair(
        canonical_meta,
        &blob_roots,
        num_disks,
        cfg,
        shard_repair_sem,
    )
    .await?;

    delta.repaired_shards += repaired as u64;
    delta.corrupt_found += corrupt as u64;
    delta.last_repaired = last_rep;

    Ok(delta)
}
