use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{
    METADATA_EXT, PARITY_SUFFIX, PART_META_SUFFIX, PREFIX_INDEX_DIR, PREFIX_INDEX_NAMESPACE,
    PREFIX_INDEX_VERSION, PREFIX_SEGMENT_EXT, QUOTA_FILE, RESERVATION_FILE, SHARD_SUFFIX,
};
use crate::object_storage::disk_manager::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata, PrefixIndexEntry,
    PrefixIndexSegment, Quota,
};
use crate::object_storage::recovery::{repair_entire_shard, verify_shard_integrity};
use crate::object_storage::upload::sync_dir;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;

use axum::extract::{Query, State};
use axum::{Json, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

const MAX_META_BYTES: u64 = 16 * 1024 * 1024;
const MAX_QUOTA_BYTES: u64 = 1 * 1024 * 1024;

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
    pub prefix_partitions: usize,
    pub prefix_open_limit: usize,
    pub rebuild_prefix: bool,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LivenessDomain {
    Live,
    Trash,
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

fn parse_bool(params: &HashMap<String, String>, k: &str, default_val: bool) -> bool {
    params
        .get(k)
        .map(|v| v == "true" || v == "1" || v == "yes")
        .unwrap_or(default_val)
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

fn path_has_trash_component(p: &Path) -> bool {
    p.components()
        .any(|c| c.as_os_str().to_string_lossy().as_ref() == ".trash")
}

fn domain_from_meta_path(meta_path: &Path) -> LivenessDomain {
    if path_has_trash_component(meta_path) {
        LivenessDomain::Trash
    } else {
        LivenessDomain::Live
    }
}

fn get_shard_dir_domain(
    disk_root: &Path,
    fragment_hash: &str,
    version_id: &str,
    domain: LivenessDomain,
) -> PathBuf {
    match domain {
        LivenessDomain::Live => disk_root.join(fragment_hash).join("blob").join(version_id),
        LivenessDomain::Trash => disk_root
            .join(fragment_hash)
            .join(".trash")
            .join("blob")
            .join(version_id),
    }
}

fn get_all_metadata_paths_domain(
    index_roots: &[PathBuf],
    fragment_hash: &str,
    object_key: &str,
    version_id: &str,
    domain: LivenessDomain,
) -> Vec<PathBuf> {
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
        .map(|root| match domain {
            LivenessDomain::Live => root
                .join(fragment_hash)
                .join("index")
                .join(shard1)
                .join(shard2)
                .join(shard3)
                .join(shard4)
                .join(&filename),
            LivenessDomain::Trash => root
                .join(fragment_hash)
                .join(".trash")
                .join("index")
                .join(shard1)
                .join(shard2)
                .join(shard3)
                .join(shard4)
                .join(&filename),
        })
        .collect()
}

async fn resolve_domain(
    index_roots: &[PathBuf],
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Option<LivenessDomain> {
    let trash_paths = get_all_metadata_paths_domain(
        index_roots,
        fragment,
        object_key,
        version_id,
        LivenessDomain::Trash,
    );
    for p in trash_paths {
        if fs::try_exists(&p).await.unwrap_or(false) {
            return Some(LivenessDomain::Trash);
        }
    }

    let live_paths = get_all_metadata_paths_domain(
        index_roots,
        fragment,
        object_key,
        version_id,
        LivenessDomain::Live,
    );
    for p in live_paths {
        if fs::try_exists(&p).await.unwrap_or(false) {
            return Some(LivenessDomain::Live);
        }
    }

    None
}

fn expected_shard_path(
    blob_roots: &[PathBuf],
    fragment: &str,
    version_id: &str,
    part_index: u64,
    shard_index: usize,
    is_parity: bool,
    domain: LivenessDomain,
) -> Option<PathBuf> {
    let num_disks = blob_roots.len();
    if num_disks == 0 {
        return None;
    }
    let disk_idx = (part_index as usize + shard_index) % num_disks;
    let disk_root = blob_roots.get(disk_idx)?;
    let shard_dir = get_shard_dir_domain(disk_root, fragment, version_id, domain);
    let name = stable_shard_file_name(part_index, shard_index, is_parity);
    Some(shard_dir.join(name))
}

async fn normalize_part_paths_for_repair(
    blob_roots: &[PathBuf],
    fragment: &str,
    version_id: &str,
    mut part: PartMetadata,
    domain: LivenessDomain,
) -> PartMetadata {
    for shard in part.shards.iter_mut() {
        let stable_name =
            stable_shard_file_name(part.part_index, shard.shard_index, shard.is_parity);
        shard.file_name = stable_name;

        let abs = shard.absolute_path.clone();
        let abs_norm = strip_tmp_attempt_suffix(abs.as_str()).to_string();

        let expected = expected_shard_path(
            blob_roots,
            fragment,
            version_id,
            part.part_index,
            shard.shard_index,
            shard.is_parity,
            domain,
        );

        let mut chosen: Option<PathBuf> = None;

        if !abs_norm.is_empty() {
            let p = PathBuf::from(&abs_norm);
            if fs::try_exists(&p).await.unwrap_or(false) {
                let ok = match domain {
                    LivenessDomain::Live => !path_has_trash_component(&p),
                    LivenessDomain::Trash => path_has_trash_component(&p),
                };
                if ok {
                    chosen = Some(p);
                }
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
        dry_run: parse_bool(&params, "dry_run", false),
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
        prefix_partitions: clamp_usize(
            params
                .get("prefix_partitions")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(256),
            16,
            4096,
        ),
        prefix_open_limit: clamp_usize(
            params
                .get("prefix_open_limit")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(32),
            4,
            512,
        ),
        rebuild_prefix: parse_bool(&params, "rebuild_prefix", true),
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

fn sha256_bytes(b: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out[..]);
    arr
}

async fn durable_atomic_write(path: &Path, data: &[u8]) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    let base = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");

    let tmp = path.with_file_name(format!(
        ".{}.{}.{}",
        base,
        METADATA_EXT,
        Ulid::new().to_string()
    ));

    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("open tmp {}", tmp.display())))?;

    f.write_all(data)
        .await
        .map_err(|e| AppError::Io(e, format!("write tmp {}", tmp.display())))?;
    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("fsync tmp {}", tmp.display())))?;
    drop(f);

    if let Err(_) = fs::rename(&tmp, path).await {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        fs::rename(&tmp, path).await.map_err(|e| {
            AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display()))
        })?;
    }

    if let Some(parent) = path.parent() {
        sync_dir(parent).await?;
    }

    Ok(())
}

#[derive(Clone)]
struct ReplicaInfo {
    full_path: PathBuf,
    mtime: SystemTime,
    len: u64,
    bytes: Option<Vec<u8>>,
    sha256: Option<[u8; 32]>,
    meta: Option<ObjectMetadata>,
    oversize_or_empty: bool,
}

async fn read_replica(full: &Path, max: u64) -> ReplicaInfo {
    let meta_fs = fs::metadata(full).await.ok();
    let mtime = meta_fs
        .as_ref()
        .and_then(|m| m.modified().ok())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let len = meta_fs.as_ref().map(|m| m.len()).unwrap_or(0);

    if len == 0 || len > max {
        return ReplicaInfo {
            full_path: full.to_path_buf(),
            mtime,
            len,
            bytes: None,
            sha256: None,
            meta: None,
            oversize_or_empty: true,
        };
    }

    match fs::read(full).await {
        Ok(bytes) => {
            let sha = Some(sha256_bytes(&bytes));
            let meta = rmp_serde::from_slice::<ObjectMetadata>(&bytes).ok();
            ReplicaInfo {
                full_path: full.to_path_buf(),
                mtime,
                len: bytes.len() as u64,
                bytes: Some(bytes),
                sha256: sha,
                meta,
                oversize_or_empty: false,
            }
        }
        Err(_) => ReplicaInfo {
            full_path: full.to_path_buf(),
            mtime,
            len,
            bytes: None,
            sha256: None,
            meta: None,
            oversize_or_empty: false,
        },
    }
}

async fn reconcile_object_metadata_strict(
    index_roots: &[PathBuf],
    fragment: &str,
    object_key: &str,
    version_id: &str,
    domain: LivenessDomain,
    dry_run: bool,
) -> Result<(Vec<u8>, ObjectMetadata, u64, u64, Option<String>), AppError> {
    if resolve_domain(index_roots, fragment, object_key, version_id).await != Some(domain) {
        return Err(AppError::NotFound);
    }

    let all_paths =
        get_all_metadata_paths_domain(index_roots, fragment, object_key, version_id, domain);
    if all_paths.is_empty() {
        return Err(AppError::InternalServerError(
            "No metadata paths configured".into(),
        ));
    }

    let mut replicas: Vec<ReplicaInfo> = Vec::with_capacity(all_paths.len());
    for p in &all_paths {
        replicas.push(read_replica(p, MAX_META_BYTES).await);
    }

    let mut best_idx: Option<usize> = None;

    for (idx, ri) in replicas.iter().enumerate() {
        if ri.meta.is_none() || ri.bytes.is_none() {
            continue;
        }

        match best_idx {
            None => best_idx = Some(idx),
            Some(cur) => {
                let cur_ri = &replicas[cur];
                let take = (ri.mtime > cur_ri.mtime)
                    || (ri.mtime == cur_ri.mtime && ri.len > cur_ri.len)
                    || (ri.mtime == cur_ri.mtime
                        && ri.len == cur_ri.len
                        && ri.sha256.unwrap_or([0u8; 32]) > cur_ri.sha256.unwrap_or([0u8; 32]));
                if take {
                    best_idx = Some(idx);
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

    let canonical_meta = replicas[best_idx]
        .meta
        .clone()
        .ok_or_else(|| AppError::InternalServerError("Canonical replica missing meta".into()))?;

    let canon_sha = sha256_bytes(&canonical_bytes);

    let mut missing_repairs = 0u64;
    let mut divergent_repairs = 0u64;
    let mut report_lines: Vec<String> = Vec::new();

    for ri in &replicas {
        match &ri.bytes {
            None => {
                if ri.oversize_or_empty {
                    report_lines.push(format!(
                        "DIVERGENT path={} len={} (oversize/empty)",
                        ri.full_path.display(),
                        ri.len
                    ));
                    if !dry_run {
                        if let Err(e) = durable_atomic_write(&ri.full_path, &canonical_bytes).await
                        {
                            warn!("Failed to repair divergent meta {:?}: {}", ri.full_path, e);
                        } else {
                            divergent_repairs += 1;
                        }
                    }
                } else {
                    report_lines.push(format!("MISSING path={}", ri.full_path.display()));
                    if !dry_run {
                        if let Err(e) = durable_atomic_write(&ri.full_path, &canonical_bytes).await
                        {
                            warn!("Failed to repair missing meta {:?}: {}", ri.full_path, e);
                        } else {
                            missing_repairs += 1;
                        }
                    }
                }
            }
            Some(b) => {
                if ri.meta.is_none() {
                    report_lines.push(format!(
                        "DIVERGENT path={} (unparseable)",
                        ri.full_path.display()
                    ));
                    if !dry_run {
                        if let Err(e) = durable_atomic_write(&ri.full_path, &canonical_bytes).await
                        {
                            warn!("Failed to repair corrupted meta {:?}: {}", ri.full_path, e);
                        } else {
                            divergent_repairs += 1;
                        }
                    }
                    continue;
                }

                let sha = sha256_bytes(b);
                if sha != canon_sha {
                    report_lines.push(format!(
                        "DIVERGENT path={} len={} canon_path={}",
                        ri.full_path.display(),
                        b.len(),
                        replicas[best_idx].full_path.display()
                    ));
                    if !dry_run {
                        if let Err(e) = durable_atomic_write(&ri.full_path, &canonical_bytes).await
                        {
                            warn!("Failed to repair divergent meta {:?}: {}", ri.full_path, e);
                        } else {
                            divergent_repairs += 1;
                        }
                    }
                }
            }
        }
    }

    let report = if report_lines.is_empty() {
        None
    } else {
        Some(format!(
            "INDEX DISAGREEMENT {}/{}/{} ({:?}):\n{}",
            fragment,
            object_key,
            version_id,
            domain,
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

async fn reconcile_fragment_quota(fragment_hash: &str, dry_run: bool) -> Result<(), AppError> {
    let pm = ObjectPathManager::new();
    let quota_paths = pm.get_all_quota_paths(fragment_hash);

    if quota_paths.is_empty() {
        return Ok(());
    }

    let mut best: Option<(SystemTime, u64, [u8; 32], Vec<u8>)> = None;

    for p in &quota_paths {
        if !fs::try_exists(p).await.unwrap_or(false) {
            continue;
        }

        let m = match fs::metadata(p).await {
            Ok(m) => m,
            Err(_) => continue,
        };

        let len = m.len();
        if len == 0 || len > MAX_QUOTA_BYTES {
            continue;
        }

        let bytes = match fs::read(p).await {
            Ok(b) => b,
            Err(_) => continue,
        };

        if rmp_serde::from_slice::<Quota>(&bytes).is_err() {
            continue;
        }

        let mt = m.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let sha = sha256_bytes(&bytes);

        match &best {
            None => best = Some((mt, len, sha, bytes)),
            Some((bmt, blen, bsha, _)) => {
                let take = (mt > *bmt)
                    || (mt == *bmt && len > *blen)
                    || (mt == *bmt && len == *blen && sha > *bsha);
                if take {
                    best = Some((mt, len, sha, bytes));
                }
            }
        }
    }

    let Some((_mt, _len, _sha, canonical_bytes)) = best else {
        return Ok(());
    };

    if dry_run {
        return Ok(());
    }

    for p in quota_paths {
        if should_stop() {
            break;
        }

        let needs = match fs::read(&p).await {
            Ok(cur) => cur != canonical_bytes,
            Err(_) => true,
        };

        if needs {
            durable_atomic_write(&p, &canonical_bytes).await?;
        }
    }

    Ok(())
}

async fn process_object_repair(
    meta: ObjectMetadata,
    index_roots: &[PathBuf],
    blob_roots: &[PathBuf],
    num_disks: usize,
    cfg: RepairRunConfig,
    shard_repair_sem: Arc<Semaphore>,
    domain: LivenessDomain,
) -> Result<(usize, usize, Option<String>), AppError> {
    if meta.parts.is_empty() || num_disks == 0 {
        return Ok((0, 0, None));
    }

    if resolve_domain(
        index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
    )
    .await
        != Some(domain)
    {
        return Ok((0, 0, None));
    }

    let meta_arc = Arc::new(meta);

    let mut repaired_total = 0usize;
    let mut corrupt_found = 0usize;
    let mut last_repaired: Option<String> = None;

    for part in &meta_arc.parts {
        if should_stop() {
            break;
        }

        let part_fixed = normalize_part_paths_for_repair(
            blob_roots,
            &meta_arc.fragment,
            &meta_arc.version_id,
            part.clone(),
            domain,
        )
        .await;

        let part_arc = Arc::new(part_fixed);
        let mut missing_shards: Vec<usize> = Vec::new();

        for shard in &part_arc.shards {
            let shard_path = PathBuf::from(strip_tmp_attempt_suffix(&shard.absolute_path));

            let (healthy, corrupt_inc) = match cfg.check_mode {
                RepairCheckMode::Availability => match fs::metadata(&shard_path).await {
                    Ok(m) => {
                        let ok = m.len() == shard.size_bytes;
                        (ok, if ok { 0 } else { 1 })
                    }
                    Err(_) => (false, 0),
                },
                RepairCheckMode::Scrub => match File::open(&shard_path).await {
                    Ok(mut f) => match f.metadata().await {
                        Ok(m) => {
                            if m.len() != shard.size_bytes {
                                (false, 1)
                            } else {
                                let ok = verify_shard_integrity(&mut f, &shard.checksum).await;
                                (ok, if ok { 0 } else { 1 })
                            }
                        }
                        Err(_) => (false, 0),
                    },
                    Err(_) => (false, 0),
                },
            };

            corrupt_found += corrupt_inc;

            if !healthy {
                missing_shards.push(shard.shard_index);
            }
        }

        if missing_shards.is_empty() || cfg.dry_run {
            continue;
        }

        let mut rs = stream::iter(missing_shards.into_iter())
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
            .buffer_unordered(cfg.shard_repair_concurrency);

        while let Some(r) = rs.next().await {
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
    writers: Mutex<HashMap<usize, BufWriter<File>>>,
    lru: Mutex<VecDeque<usize>>,
}

impl PrefixSpoolWriter {
    async fn new(dir: PathBuf, partitions: usize, open_limit: usize) -> Result<Self, AppError> {
        let partitions = partitions.max(1);
        let open_limit = open_limit.max(1);
        fs::create_dir_all(&dir)
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

    async fn open_writer(&self, part: usize) -> Result<BufWriter<File>, AppError> {
        let path = self.spool_path(part);
        let f = OpenOptions::new()
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

    async fn evict_one_if_needed(&self) -> Option<BufWriter<File>> {
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

    let _ = fs::create_dir_all(&dir).await;

    let file = format!("seg_{}{}", seg.entries[0].version_id, PREFIX_SEGMENT_EXT);
    let path = dir.join(file);

    durable_atomic_write(&path, &bytes).await?;
    Ok(())
}

async fn mirror_prefix_record(
    index_roots: &[PathBuf],
    rec: &PrefixSpoolRecord,
    dry_run: bool,
) -> Result<(), AppError> {
    if dry_run {
        return Ok(());
    }

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
    dry_run: bool,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    for part in 0..partitions {
        if should_stop() {
            break;
        }

        let path = spool_dir.join(format!("pfx_part_{:04}.spool", part));
        if !fs::try_exists(&path).await.unwrap_or(false) {
            continue;
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!(
                "Rebuilding Prefix Index (partition {}/{})",
                part + 1,
                partitions
            );
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let mut f = File::open(&path)
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

            mirror_prefix_record(index_roots, &rec, dry_run).await?;
        }
    }

    Ok(())
}

async fn count_prefix_files_for_fragment(root: &PathBuf, fragment: &str) -> u64 {
    let dir = root.join(fragment).join("index").join(PREFIX_INDEX_DIR);

    if !fs::try_exists(&dir).await.unwrap_or(false) {
        return 0;
    }

    tokio::task::spawn_blocking(move || -> u64 {
        WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .count() as u64
    })
    .await
    .unwrap_or(0)
}

async fn choose_prefix_source_root_for_fragment(
    index_roots: &[PathBuf],
    fragment: &str,
) -> Option<PathBuf> {
    let mut best: Option<(PathBuf, u64)> = None;

    for r in index_roots {
        if !fs::try_exists(r).await.unwrap_or(false) {
            continue;
        }

        let cnt = count_prefix_files_for_fragment(r, fragment).await;
        if cnt == 0 {
            continue;
        }

        match &best {
            None => best = Some((r.clone(), cnt)),
            Some((_br, bc)) => {
                if cnt > *bc {
                    best = Some((r.clone(), cnt));
                }
            }
        }
    }

    best.map(|(r, _)| r)
}

async fn mirror_prefix_index_across_roots(
    index_roots: &[PathBuf],
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    if index_roots.len() <= 1 {
        return Ok(());
    }

    let mut fragments: HashSet<String> = HashSet::new();
    for root in index_roots {
        if !fs::try_exists(root).await.unwrap_or(false) {
            continue;
        }
        if let Ok(mut rd) = fs::read_dir(root).await {
            while let Ok(Some(ent)) = rd.next_entry().await {
                if ent.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                    let name = ent.file_name().to_string_lossy().to_string();
                    if !name.starts_with('.') {
                        fragments.insert(name);
                    }
                }
            }
        }
    }

    for fragment in fragments {
        if should_stop() {
            break;
        }

        let Some(source_root) =
            choose_prefix_source_root_for_fragment(index_roots, &fragment).await
        else {
            continue;
        };

        let src_pfx = source_root
            .join(&fragment)
            .join("index")
            .join(PREFIX_INDEX_DIR);

        if !fs::try_exists(&src_pfx).await.unwrap_or(false) {
            continue;
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Mirroring Prefix Index (__p__) fragment={}", fragment);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        for target_root in index_roots {
            if should_stop() {
                break;
            }

            if *target_root == source_root {
                continue;
            }

            if !fs::try_exists(target_root).await.unwrap_or(false) {
                continue;
            }

            let dest_index_dir = target_root.join(&fragment).join("index");
            let _ = fs::create_dir_all(&dest_index_dir).await;

            let dest_pfx = dest_index_dir.join(PREFIX_INDEX_DIR);
            let tmp_pfx = dest_index_dir.join(format!(".pfx_tmp_{}_{}", fragment, Ulid::new()));
            let old_pfx = dest_index_dir.join(format!(".pfx_old_{}_{}", fragment, Ulid::new()));

            if fs::try_exists(&tmp_pfx).await.unwrap_or(false) {
                let _ = fs::remove_dir_all(&tmp_pfx).await;
            }
            if fs::try_exists(&old_pfx).await.unwrap_or(false) {
                let _ = fs::remove_dir_all(&old_pfx).await;
            }

            let src = src_pfx.clone();
            let dst = tmp_pfx.clone();

            tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
                std::fs::create_dir_all(&dst)?;
                for entry in WalkDir::new(&src).into_iter().filter_map(|e| e.ok()) {
                    let rel = entry.path().strip_prefix(&src).unwrap_or(entry.path());
                    let out_path = dst.join(rel);

                    if entry.file_type().is_dir() {
                        std::fs::create_dir_all(&out_path)?;
                    } else if entry.file_type().is_file() {
                        if let Some(parent) = out_path.parent() {
                            std::fs::create_dir_all(parent)?;
                        }
                        std::fs::copy(entry.path(), &out_path)?;
                    }
                }
                Ok(())
            })
            .await
            .map_err(|e| AppError::InternalServerError(format!("mirror __p__ join: {}", e)))?
            .map_err(|e| AppError::Io(e, "mirror __p__ copy".into()))?;

            if fs::try_exists(&dest_pfx).await.unwrap_or(false) {
                let _ = fs::rename(&dest_pfx, &old_pfx).await;
                let _ = sync_dir(&dest_index_dir).await;
            }

            fs::rename(&tmp_pfx, &dest_pfx)
                .await
                .map_err(|e| AppError::Io(e, format!("rename tmp->dest {}", dest_pfx.display())))?;

            let _ = sync_dir(&dest_index_dir).await;

            if fs::try_exists(&old_pfx).await.unwrap_or(false) {
                let _ = fs::remove_dir_all(&old_pfx).await;
            }
        }
    }

    Ok(())
}

fn processed_key(meta: &ObjectMetadata, domain: LivenessDomain) -> String {
    format!(
        "{}|{}|{}|{:?}",
        meta.fragment, meta.object_key, meta.version_id, domain
    )
}

async fn process_one_meta_path(
    meta_path: PathBuf,
    index_roots: Arc<Vec<PathBuf>>,
    blob_roots: Arc<Vec<PathBuf>>,
    num_disks: usize,
    cfg: RepairRunConfig,
    processed_versions: Arc<Mutex<HashSet<String>>>,
    shard_repair_sem: Arc<Semaphore>,
    prefix_spool: Option<Arc<PrefixSpoolWriter>>,
) -> Result<BatchDelta, AppError> {
    let mut delta = BatchDelta::default();
    if should_stop() {
        return Ok(delta);
    }

    let domain = domain_from_meta_path(&meta_path);
    delta.scanned = 1;

    let m = match fs::metadata(&meta_path).await {
        Ok(m) => m,
        Err(_) => return Ok(delta),
    };

    if m.len() == 0 || m.len() > MAX_META_BYTES {
        delta.corrupt_found += 1;
        return Ok(delta);
    }

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

    let key = processed_key(&meta, domain);
    {
        let mut set = processed_versions.lock().await;
        if set.contains(&key) {
            return Ok(delta);
        }
        set.insert(key);
    }

    if resolve_domain(
        &index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
    )
    .await
        != Some(domain)
    {
        return Ok(delta);
    }

    let (_canonical_data, canonical_meta, miss_rep, div_rep, report) =
        match reconcile_object_metadata_strict(
            &index_roots,
            &meta.fragment,
            &meta.object_key,
            &meta.version_id,
            domain,
            cfg.dry_run,
        )
        .await
        {
            Ok(v) => v,
            Err(AppError::NotFound) => return Ok(delta),
            Err(e) => return Err(e),
        };

    delta.missing_meta_repaired += miss_rep;
    delta.divergent_meta_repaired += div_rep;

    if let Some(r) = report {
        delta.last_disagreement = Some(r.clone());
        if cfg.dry_run {
            warn!("{}", r);
        }
    }

    if cfg.rebuild_prefix && domain == LivenessDomain::Live {
        if let Some(spool) = prefix_spool.as_ref() {
            let prefixes = prefixes_for_key(&canonical_meta.object_key);
            for prefix in prefixes {
                let ph = ObjectPathManager::prefix_hash(&prefix);
                let rec = PrefixSpoolRecord {
                    fragment: canonical_meta.fragment.clone(),
                    prefix,
                    prefix_hash: ph,
                    object_key: canonical_meta.object_key.clone(),
                    version_id: canonical_meta.version_id.clone(),
                    created_at: canonical_meta.created_at,
                };
                let _ = spool.append(&rec).await;
            }
        }
    }

    let (repaired, corrupt, last_rep) = process_object_repair(
        canonical_meta,
        &index_roots,
        &blob_roots,
        num_disks,
        cfg,
        shard_repair_sem,
        domain,
    )
    .await?;

    delta.repaired_shards += repaired as u64;
    delta.corrupt_found += corrupt as u64;
    delta.last_repaired = last_rep;

    Ok(delta)
}

async fn apply_meta_batch(
    batch: &[PathBuf],
    index_roots: Arc<Vec<PathBuf>>,
    blob_roots: Arc<Vec<PathBuf>>,
    num_disks: usize,
    cfg: RepairRunConfig,
    processed_versions: Arc<Mutex<HashSet<String>>>,
    shard_repair_sem: Arc<Semaphore>,
    ws_manager: Arc<WsConnectionManager>,
    start_time: Instant,
    prefix_spool: Option<Arc<PrefixSpoolWriter>>,
) -> Result<(), AppError> {
    if batch.is_empty() {
        return Ok(());
    }

    let mut agg = BatchDelta::default();

    let mut s = stream::iter(batch.iter().cloned())
        .map(|meta_path| {
            let index_roots = index_roots.clone();
            let blob_roots = blob_roots.clone();
            let processed_versions = processed_versions.clone();
            let shard_repair_sem = shard_repair_sem.clone();
            let cfg = cfg.clone();
            let prefix_spool = prefix_spool.clone();

            async move {
                process_one_meta_path(
                    meta_path,
                    index_roots,
                    blob_roots,
                    num_disks,
                    cfg,
                    processed_versions,
                    shard_repair_sem,
                    prefix_spool,
                )
                .await
            }
        })
        .buffer_unordered(cfg.meta_concurrency);

    while let Some(r) = s.next().await {
        match r {
            Ok(d) => {
                agg.scanned += d.scanned;
                agg.repaired_shards += d.repaired_shards;
                agg.corrupt_found += d.corrupt_found;
                agg.missing_meta_repaired += d.missing_meta_repaired;
                agg.divergent_meta_repaired += d.divergent_meta_repaired;
                if d.last_repaired.is_some() {
                    agg.last_repaired = d.last_repaired;
                }
                if d.last_disagreement.is_some() {
                    agg.last_disagreement = d.last_disagreement;
                }
            }
            Err(e) => {
                warn!("Meta batch item failed (non-fatal): {}", e);
            }
        }
    }

    {
        let mut status = REPAIR_STATUS.write().await;
        status.total_scanned += agg.scanned;
        status.corrupt_found += agg.corrupt_found;
        status.missing_meta_repaired += agg.missing_meta_repaired;
        status.divergent_meta_repaired += agg.divergent_meta_repaired;

        status.total_repaired +=
            agg.repaired_shards + agg.missing_meta_repaired + agg.divergent_meta_repaired;

        if agg.last_repaired.is_some() {
            status.last_repaired = agg.last_repaired.clone();
        }
        if agg.last_disagreement.is_some() {
            status.last_disagreement = agg.last_disagreement.clone();
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        status.duration_seconds = elapsed as u64;
        if elapsed > 0.0 {
            status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
        }

        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    Ok(())
}

async fn find_healthiest_index_root(roots: &[PathBuf]) -> Option<PathBuf> {
    let mut best_root = None;
    let mut max_count = 0u64;

    for root in roots {
        if !fs::try_exists(root).await.unwrap_or(false) {
            continue;
        }

        let root_clone = root.clone();
        let count = tokio::task::spawn_blocking(move || -> u64 {
            let mut c = 0u64;
            let walker = WalkDir::new(root_clone).into_iter();
            for entry in walker.filter_map(|e| e.ok()).take(1000) {
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
            "Repair: Selected healthiest index root for scanning: {:?}",
            r
        );
    } else {
        warn!("Repair: No reachable index roots found!");
    }

    best_root
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
            "Initializing (mode={:?}, meta_conc={}, repair_conc={}, batch={}, dry_run={}, rebuild_prefix={})",
            cfg.check_mode,
            cfg.meta_concurrency,
            cfg.shard_repair_concurrency,
            cfg.meta_batch_size,
            cfg.dry_run,
            cfg.rebuild_prefix
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

    let _ = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;

    let path_manager = ObjectPathManager::new();
    let index_roots = Arc::new(path_manager.get_all_index_roots());
    let blob_roots = Arc::new(path_manager.get_blob_roots());
    let num_disks = blob_roots.len();

    let processed_versions = Arc::new(Mutex::new(HashSet::<String>::new()));
    let processed_fragments = Arc::new(Mutex::new(HashSet::<String>::new()));
    let shard_repair_sem = Arc::new(Semaphore::new(cfg.shard_repair_concurrency));

    let spool_dir =
        std::env::temp_dir().join(format!("n1_prefix_repair_{}", Ulid::new().to_string()));

    let prefix_spool = if cfg.rebuild_prefix {
        Some(Arc::new(
            PrefixSpoolWriter::new(
                spool_dir.clone(),
                cfg.prefix_partitions,
                cfg.prefix_open_limit,
            )
            .await?,
        ))
    } else {
        None
    };

    info!(
        "Starting Storage Repair Job (Scanning healthiest index root). mode={:?}, dry_run={}, rebuild_prefix={}",
        cfg.check_mode, cfg.dry_run, cfg.rebuild_prefix
    );

    let best_root_opt = find_healthiest_index_root(&index_roots).await;

    if let Some(best_root) = best_root_opt.clone() {
        if should_stop() {
            return Ok(());
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Scanning Root {:?}", best_root);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let mut frag_entries = fs::read_dir(&best_root)
            .await
            .map_err(|e| AppError::Io(e, format!("read_dir best_root {}", best_root.display())))?;

        while let Ok(Some(frag_entry)) = frag_entries.next_entry().await {
            if should_stop() {
                break;
            }

            let frag_path = frag_entry.path();
            if !frag_path.is_dir() {
                continue;
            }

            let fragment_hash = frag_entry.file_name().to_string_lossy().to_string();
            if fragment_hash.starts_with('.') {
                continue;
            }

            {
                let mut status = REPAIR_STATUS.write().await;
                status.current_fragment = fragment_hash.clone();
            }

            let should_quota = {
                let mut seen = processed_fragments.lock().await;
                if seen.contains(&fragment_hash) {
                    false
                } else {
                    seen.insert(fragment_hash.clone());
                    true
                }
            };

            if should_quota {
                let _ = reconcile_fragment_quota(&fragment_hash, cfg.dry_run).await;
            }

            let scan_targets = [
                ("index", LivenessDomain::Live),
                (".trash/index", LivenessDomain::Trash),
            ];

            for (sub_dir, domain) in scan_targets {
                if should_stop() {
                    break;
                }

                let index_dir = frag_path.join(sub_dir);
                if !fs::try_exists(&index_dir).await.unwrap_or(false) {
                    continue;
                }

                let (tx, mut rx) = tokio::sync::mpsc::channel::<PathBuf>(1024);
                let index_dir_clone = index_dir.clone();

                tokio::task::spawn_blocking(move || {
                    let walker = WalkDir::new(index_dir_clone).into_iter();
                    for entry in walker.filter_map(|e| e.ok()) {
                        if entry.file_type().is_file() {
                            let p = entry.path().to_owned();

                            if p.components()
                                .any(|c| c.as_os_str() == OsStr::new(PREFIX_INDEX_DIR))
                            {
                                continue;
                            }

                            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                            if name.ends_with(METADATA_EXT)
                                && !name.ends_with(PART_META_SUFFIX)
                                && name != QUOTA_FILE
                                && name != RESERVATION_FILE
                            {
                                if tx.blocking_send(p).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });

                let mut pending: Vec<PathBuf> = Vec::with_capacity(cfg.meta_batch_size);

                while let Some(meta_path) = rx.recv().await {
                    if should_stop() {
                        break;
                    }

                    if domain_from_meta_path(&meta_path) != domain {
                        continue;
                    }

                    pending.push(meta_path);
                    if pending.len() >= cfg.meta_batch_size {
                        apply_meta_batch(
                            &pending,
                            index_roots.clone(),
                            blob_roots.clone(),
                            num_disks,
                            cfg.clone(),
                            processed_versions.clone(),
                            shard_repair_sem.clone(),
                            ws_manager.clone(),
                            start_time,
                            prefix_spool.clone(),
                        )
                        .await?;
                        pending.clear();
                    }
                }

                if !pending.is_empty() {
                    apply_meta_batch(
                        &pending,
                        index_roots.clone(),
                        blob_roots.clone(),
                        num_disks,
                        cfg.clone(),
                        processed_versions.clone(),
                        shard_repair_sem.clone(),
                        ws_manager.clone(),
                        start_time,
                        prefix_spool.clone(),
                    )
                    .await?;
                    pending.clear();
                }
            }
        }
    } else {
        let mut status = REPAIR_STATUS.write().await;
        status.phase = "No healthy index root found to scan.".to_string();
        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    if cfg.rebuild_prefix {
        if let Some(spool) = prefix_spool.as_ref() {
            spool.flush_all().await;
        }

        if !should_stop() {
            {
                let mut status = REPAIR_STATUS.write().await;
                status.phase = "Rebuilding Prefix Index (mirroring segments)".to_string();
                ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
            }

            replay_spool_and_mirror_prefix(
                &spool_dir,
                cfg.prefix_partitions,
                &index_roots,
                cfg.dry_run,
                ws_manager.clone(),
            )
            .await?;
        }

        if !cfg.dry_run && !should_stop() {
            {
                let mut status = REPAIR_STATUS.write().await;
                status.phase = "Mirroring Prefix Index (__p__) across index roots".to_string();
                ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
            }

            mirror_prefix_index_across_roots(&index_roots, ws_manager.clone()).await?;
        }

        if !cfg.dry_run {
            let _ = fs::remove_dir_all(&spool_dir).await;
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
    }

    Ok(())
}
