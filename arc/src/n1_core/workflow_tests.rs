use super::archive::download_selected_archive;
use super::blob_store::{
    free_raw_shards_for_metadatas, open_raw_direct_io, raw_blob_address_exists,
    verify_shard_checksum, write_shard_range,
};
use super::constants::{
    PREFIX_DELIMITER_COMPACTED_FILE, PREFIX_DELIMITER_SEGMENT_PREFIX, PREFIX_SEGMENT_EXT,
};
use super::delete::delete_object;
use super::delete::{
    delete_object_permanently, delete_object_version, empty_trash, recover_object,
    recover_object_version,
};
use super::disk_manager::{
    DownloadArchiveRequest, InitiateUploadParams, ObjectKeyQuery, ObjectMetadata,
    ObjectPathManager, PrefixIndexSegment, RenameObjectRequest, UploadPartQuery,
};
use super::fragment_metadata::init_fragment_metadata;
use super::garbage_collector::{get_gc_status_api, run_garbage_collector};
use super::index_facade as index;
use super::metadata_parser::get_metadata;
use super::modify::rename_object;
use super::object_parser::{
    STREAM_CHUNK_SIZE_ATOMIC, STREAM_LOCALITY_BIAS_ENABLED_ATOMIC, STREAM_MAX_CONCURRENCY,
    STREAM_READAHEAD_BYTES_ATOMIC,
};
use super::quota_manager::{get_quota, init_quota_file, set_storage_used, update_quota};
use super::recovery::{
    RAW_VERIFY_ON_READ_ATOMIC, RepairLogMode, ShardRepairContext, repair_entire_shard,
};
use super::redistribution::{
    RedistributionDurabilityMode, RedistributionExecutionMode, RedistributionRunOptions,
    run_redistribution_job, run_redistribution_job_with_options,
};
use super::repair_manager::{
    RepairCheckMode, RepairRunConfig, RepairRunOptions, run_repair_job, run_repair_job_with_options,
};
use super::retrieve::{
    get_object, get_object_metadata, list_objects, list_trash_objects, stream_object,
};
use super::sync::{ensure_fragment_blob_directories, sync_single_fragment_quota};
use super::upload::{
    DATA_SHARDS_ATOMIC, MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC, PARITY_SHARDS_ATOMIC,
    STRIPE_SIZE_ATOMIC, UPLOAD_IO_GATE_ENABLED_ATOMIC, UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC,
    WRITER_BUFFER_SIZE_ATOMIC, cancel_upload_session,
    cleanup_orphaned_upload_artifacts_for_fragment, finalize_object_upload, get_upload_status,
    initiate_object_upload, object_upload_part,
};
use crate::n1_core::disk_manager::{
    STORAGE_TEST_LOCK as TEST_LOCK, replace_storage_roots_for_tests, version_recency_is_newer,
};
use crate::n1_core::operation_gate::MaintenanceAccessMode;
use crate::routes::storage_routes::AppState;
use crate::settings::constants::CONFIG_FILE;
use crate::settings::create_config::create_default_config_ini;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;

use axum::Json as AxumJson;
use axum::body::{Body, to_bytes};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use axum::response::IntoResponse;
use futures_util::{StreamExt, stream};
use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::fs;
use tokio::time::{Duration, sleep};
use ulid::Ulid;

static TEST_BASE: Lazy<std::path::PathBuf> =
    Lazy::new(|| std::env::temp_dir().join(format!("n1-matrix-{}", std::process::id())));

fn ensure_test_config_file() {
    let config_path = std::path::PathBuf::from(CONFIG_FILE);
    if !config_path.exists() {
        create_default_config_ini(&config_path).expect("create default test config");
    }
}

fn full_claims(fragment: &str) -> Claims {
    Claims {
        fragment: fragment.to_string(),
        read: true,
        write: true,
        modify: true,
        delete: true,
        exp: usize::MAX,
    }
}

async fn ensure_global_storage_config(base: &std::path::Path) {
    let _ = ensure_raw_storage_config(base, 3, 256 * 1024 * 1024).await;
}

async fn ensure_raw_storage_config(
    base: &std::path::Path,
    raw_disk_count: usize,
    raw_disk_bytes: u64,
) -> Vec<std::path::PathBuf> {
    ensure_test_config_file();

    let _ = fs::remove_dir_all(base).await;
    fs::create_dir_all(base)
        .await
        .expect("create raw test base");

    let index_raw_bytes = raw_disk_bytes.max(256 * 1024 * 1024);
    let mut index_roots = Vec::with_capacity(2);
    for idx in 0..2usize {
        let raw_path = base.join(format!("index_{}.raw", idx + 1));
        let raw_path_clone = raw_path.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&raw_path_clone)?;
            file.set_len(index_raw_bytes)
        })
        .await
        .expect("index raw file task")
        .expect("initialize index raw file");
        index_roots.push(raw_path.to_string_lossy().to_string());
    }

    let mut blob_roots = Vec::with_capacity(raw_disk_count);
    let mut raw_paths = Vec::with_capacity(raw_disk_count);
    for idx in 0..raw_disk_count {
        let raw_path = base.join(format!("blob_{}.raw", idx + 1));
        let raw_path_clone = raw_path.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&raw_path_clone)?;
            file.set_len(raw_disk_bytes)
        })
        .await
        .expect("raw file task")
        .expect("initialize raw file");
        blob_roots.push(raw_path.to_string_lossy().to_string());
        raw_paths.push(raw_path);
    }

    replace_storage_roots_for_tests(index_roots, blob_roots, base.to_string_lossy().to_string());
    super::blob_store::clear_raw_runtime_caches_for_tests();
    index::clear_runtime_caches_for_tests().await;
    super::recovery::clear_runtime_state_for_tests();
    super::redistribution::reset_runtime_state_for_tests().await;
    super::repair_manager::reset_runtime_state_for_tests().await;
    crate::n1_core::operation_gate::reset_maintenance_mode_for_tests();

    raw_paths
}

async fn corrupt_shard_bytes(shard: &super::disk_manager::ShardInfo, offset: u64, data: &[u8]) {
    write_shard_range(shard, offset, data)
        .await
        .expect("corrupt shard bytes");
}

async fn shard_checksum_ok(shard: &super::disk_manager::ShardInfo) -> bool {
    verify_shard_checksum(shard)
        .await
        .expect("verify shard checksum")
}

fn ensure_upload_runtime_config() {
    if MAX_IO_SEMAPHORE.get().is_none() {
        MAX_IO_SEMAPHORE.set(128).expect("set MAX_IO_SEMAPHORE");
    }
    if MAX_PART_SIZE_ATOMIC.get().is_none() {
        MAX_PART_SIZE_ATOMIC
            .set(AtomicU64::new(16 * 1024 * 1024))
            .expect("set MAX_PART_SIZE_ATOMIC");
    }
    if DATA_SHARDS_ATOMIC.get().is_none() {
        DATA_SHARDS_ATOMIC
            .set(AtomicUsize::new(1))
            .expect("set DATA_SHARDS_ATOMIC");
    } else {
        DATA_SHARDS_ATOMIC
            .get()
            .expect("DATA_SHARDS_ATOMIC")
            .store(1, Ordering::Relaxed);
    }
    if PARITY_SHARDS_ATOMIC.get().is_none() {
        PARITY_SHARDS_ATOMIC
            .set(AtomicUsize::new(0))
            .expect("set PARITY_SHARDS_ATOMIC");
    } else {
        PARITY_SHARDS_ATOMIC
            .get()
            .expect("PARITY_SHARDS_ATOMIC")
            .store(0, Ordering::Relaxed);
    }
    if WRITER_BUFFER_SIZE_ATOMIC.get().is_none() {
        WRITER_BUFFER_SIZE_ATOMIC
            .set(AtomicUsize::new(64 * 1024))
            .expect("set WRITER_BUFFER_SIZE_ATOMIC");
    } else {
        WRITER_BUFFER_SIZE_ATOMIC
            .get()
            .expect("WRITER_BUFFER_SIZE_ATOMIC")
            .store(64 * 1024, Ordering::Relaxed);
    }
    if UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC.get().is_none() {
        UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC
            .set(AtomicBool::new(true))
            .expect("set UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC");
    } else {
        UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC
            .get()
            .expect("UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC")
            .store(true, Ordering::Relaxed);
    }
    if UPLOAD_IO_GATE_ENABLED_ATOMIC.get().is_none() {
        UPLOAD_IO_GATE_ENABLED_ATOMIC
            .set(AtomicBool::new(true))
            .expect("set UPLOAD_IO_GATE_ENABLED_ATOMIC");
    } else {
        UPLOAD_IO_GATE_ENABLED_ATOMIC
            .get()
            .expect("UPLOAD_IO_GATE_ENABLED_ATOMIC")
            .store(true, Ordering::Relaxed);
    }
    if STRIPE_SIZE_ATOMIC.get().is_none() {
        STRIPE_SIZE_ATOMIC
            .set(AtomicUsize::new(4096))
            .expect("set STRIPE_SIZE_ATOMIC");
    } else {
        STRIPE_SIZE_ATOMIC
            .get()
            .expect("STRIPE_SIZE_ATOMIC")
            .store(4096, Ordering::Relaxed);
    }
    if RAW_VERIFY_ON_READ_ATOMIC.get().is_none() {
        RAW_VERIFY_ON_READ_ATOMIC
            .set(AtomicBool::new(true))
            .expect("set RAW_VERIFY_ON_READ_ATOMIC");
    } else {
        RAW_VERIFY_ON_READ_ATOMIC
            .get()
            .expect("RAW_VERIFY_ON_READ_ATOMIC")
            .store(true, Ordering::Relaxed);
    }
    if STREAM_MAX_CONCURRENCY.get().is_none() {
        STREAM_MAX_CONCURRENCY
            .set(128)
            .expect("set STREAM_MAX_CONCURRENCY");
    }
    if STREAM_CHUNK_SIZE_ATOMIC.get().is_none() {
        STREAM_CHUNK_SIZE_ATOMIC
            .set(AtomicUsize::new(256 * 1024))
            .expect("set STREAM_CHUNK_SIZE_ATOMIC");
    } else {
        STREAM_CHUNK_SIZE_ATOMIC
            .get()
            .expect("STREAM_CHUNK_SIZE_ATOMIC")
            .store(256 * 1024, Ordering::Relaxed);
    }
    if STREAM_READAHEAD_BYTES_ATOMIC.get().is_none() {
        STREAM_READAHEAD_BYTES_ATOMIC
            .set(AtomicUsize::new(512 * 1024))
            .expect("set STREAM_READAHEAD_BYTES_ATOMIC");
    } else {
        STREAM_READAHEAD_BYTES_ATOMIC
            .get()
            .expect("STREAM_READAHEAD_BYTES_ATOMIC")
            .store(512 * 1024, Ordering::Relaxed);
    }
    if STREAM_LOCALITY_BIAS_ENABLED_ATOMIC.get().is_none() {
        STREAM_LOCALITY_BIAS_ENABLED_ATOMIC
            .set(AtomicBool::new(true))
            .expect("set STREAM_LOCALITY_BIAS_ENABLED_ATOMIC");
    } else {
        STREAM_LOCALITY_BIAS_ENABLED_ATOMIC
            .get()
            .expect("STREAM_LOCALITY_BIAS_ENABLED_ATOMIC")
            .store(true, Ordering::Relaxed);
    }
}

fn test_env_usize(name: &str, default: usize, min: usize, max: usize) -> usize {
    match std::env::var(name) {
        Ok(v) => v
            .parse::<usize>()
            .ok()
            .map(|n| n.clamp(min, max))
            .unwrap_or(default),
        Err(_) => default,
    }
}

fn test_env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"),
        Err(_) => default,
    }
}

fn test_env_u64(name: &str, default: u64, min: u64, max: u64) -> u64 {
    match std::env::var(name) {
        Ok(v) => v
            .parse::<u64>()
            .ok()
            .map(|n| n.clamp(min, max))
            .unwrap_or(default),
        Err(_) => default,
    }
}

fn test_env_optional_usize(name: &str, min: usize, max: usize) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|n| n.clamp(min, max))
}

fn test_env_optional_u64(name: &str, min: u64, max: u64) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|n| n.clamp(min, max))
}

#[derive(Clone, Copy)]
struct PerfSnapshot {
    cpu_secs: Option<f64>,
    rss_mib: Option<u64>,
    hwm_mib: Option<u64>,
}

#[derive(Clone, Debug)]
struct PerfStageResult {
    name: &'static str,
    ops: usize,
    bytes: u64,
    wall_secs: f64,
    cpu_secs: Option<f64>,
    rss_before_mib: Option<u64>,
    rss_after_mib: Option<u64>,
    rss_peak_mib: Option<u64>,
    hwm_mib: Option<u64>,
}

#[cfg(unix)]
fn process_cpu_seconds() -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage writes a valid rusage struct on success.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: rc == 0 guarantees `usage` was initialized by getrusage.
    let usage = unsafe { usage.assume_init() };
    let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0;
    let sys = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
    Some(user + sys)
}

#[cfg(not(unix))]
fn process_cpu_seconds() -> Option<f64> {
    None
}

#[cfg(target_os = "linux")]
fn parse_proc_status_kib_field(status: &str, field: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        if !line.starts_with(field) {
            return None;
        }
        line.split_whitespace()
            .nth(1)
            .and_then(|v| v.parse::<u64>().ok())
    })
}

#[cfg(target_os = "linux")]
fn process_rss_hwm_mib() -> (Option<u64>, Option<u64>) {
    let status = match std::fs::read_to_string("/proc/self/status") {
        Ok(v) => v,
        Err(_) => return (None, None),
    };
    let rss = parse_proc_status_kib_field(&status, "VmRSS:").map(|kib| kib / 1024);
    let hwm = parse_proc_status_kib_field(&status, "VmHWM:").map(|kib| kib / 1024);
    (rss, hwm)
}

#[cfg(not(target_os = "linux"))]
fn process_rss_hwm_mib() -> (Option<u64>, Option<u64>) {
    (None, None)
}

fn perf_snapshot() -> PerfSnapshot {
    let (rss_mib, hwm_mib) = process_rss_hwm_mib();
    PerfSnapshot {
        cpu_secs: process_cpu_seconds(),
        rss_mib,
        hwm_mib,
    }
}

fn update_rss_peak(peak: &mut Option<u64>) {
    if let Some(cur) = process_rss_hwm_mib().0 {
        match peak {
            Some(v) => {
                if cur > *v {
                    *v = cur;
                }
            }
            None => *peak = Some(cur),
        }
    }
}

fn cpu_delta_secs(before: Option<f64>, after: Option<f64>) -> Option<f64> {
    match (before, after) {
        (Some(b), Some(a)) if a >= b => Some(a - b),
        (Some(_), Some(_)) => Some(0.0),
        _ => None,
    }
}

fn finish_perf_stage(
    name: &'static str,
    ops: usize,
    bytes: u64,
    started: Instant,
    before: PerfSnapshot,
    rss_peak_mib: Option<u64>,
) -> PerfStageResult {
    let after = perf_snapshot();
    PerfStageResult {
        name,
        ops,
        bytes,
        wall_secs: started.elapsed().as_secs_f64(),
        cpu_secs: cpu_delta_secs(before.cpu_secs, after.cpu_secs),
        rss_before_mib: before.rss_mib,
        rss_after_mib: after.rss_mib,
        rss_peak_mib,
        hwm_mib: after.hwm_mib,
    }
}

fn chunked_payload_for_seed(len: usize, seed: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    for (i, b) in out.iter_mut().enumerate() {
        *b = ((i + seed) % 251) as u8;
    }
    out
}

fn payload_seed_for_epoch(epoch: usize, index: usize) -> usize {
    epoch.saturating_mul(1_000_000).saturating_add(index)
}

async fn stream_object_bytes(fragment: String, object_key: String) -> Vec<u8> {
    let resp = stream_object(
        full_claims(&fragment),
        HeaderMap::new(),
        AxumPath((fragment.clone(), object_key.clone())),
    )
    .await
    .expect("stream object")
    .into_response();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "stream status for {}",
        object_key
    );
    let body = to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("stream response body");
    body.to_vec()
}

fn print_perf_stage(stage: &PerfStageResult) {
    println!(
        "N1 perf stage={} ops={} bytes={} wall_s={:.3} cpu_s={} rss_before_mib={} rss_after_mib={} rss_peak_mib={} hwm_mib={}",
        stage.name,
        stage.ops,
        stage.bytes,
        stage.wall_secs,
        stage
            .cpu_secs
            .map(|v| format!("{:.3}", v))
            .unwrap_or_else(|| "n/a".to_string()),
        stage
            .rss_before_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        stage
            .rss_after_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        stage
            .rss_peak_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        stage
            .hwm_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string())
    );
}

#[derive(Clone, Copy)]
struct SoakProfile {
    rounds: usize,
    ops_per_round: usize,
    client_concurrency: usize,
    inject_disk_down: bool,
}

async fn read_response_json(resp: axum::response::Response) -> serde_json::Value {
    let body = to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read response body");
    serde_json::from_slice(&body).expect("decode JSON response")
}

async fn collect_fragment_versions_streamed(fragment: &str, trashed: bool) -> Vec<ObjectMetadata> {
    let mut out = Vec::new();
    let mut stream = index::stream_fragment_object_versions(fragment.to_string(), trashed);
    while let Some(next_meta) = stream.recv().await {
        out.push(next_meta.expect("stream fragment metadata"));
    }
    out.sort_by(|left, right| {
        left.object_key
            .cmp(&right.object_key)
            .then(left.version_id.cmp(&right.version_id))
    });
    out
}

async fn list_objects_json(
    fragment: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> serde_json::Value {
    list_objects_json_with_internal(fragment, prefix, delimiter, false).await
}

async fn list_objects_json_with_internal(
    fragment: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    include_internal: bool,
) -> serde_json::Value {
    let mut params: HashMap<String, String> = HashMap::new();
    if let Some(p) = prefix {
        params.insert("prefix".to_string(), p.to_string());
    }
    if let Some(d) = delimiter {
        params.insert("delimiter".to_string(), d.to_string());
    }
    if include_internal {
        params.insert("include_internal".to_string(), "true".to_string());
    }

    let resp = list_objects(
        full_claims(fragment),
        AxumPath(fragment.to_string()),
        Query(params),
    )
    .await
    .expect("list objects")
    .into_response();
    assert_eq!(resp.status(), StatusCode::OK);
    read_response_json(resp).await
}

async fn list_objects_json_page(
    fragment: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
) -> serde_json::Value {
    let mut params: HashMap<String, String> = HashMap::new();
    if let Some(p) = prefix {
        params.insert("prefix".to_string(), p.to_string());
    }
    if let Some(d) = delimiter {
        params.insert("delimiter".to_string(), d.to_string());
    }
    params.insert("limit".to_string(), limit.to_string());
    if let Some(tok) = continuation_token {
        params.insert("continuation_token".to_string(), tok);
    }

    let resp = list_objects(
        full_claims(fragment),
        AxumPath(fragment.to_string()),
        Query(params),
    )
    .await
    .expect("list objects page")
    .into_response();
    assert_eq!(resp.status(), StatusCode::OK);
    read_response_json(resp).await
}

async fn list_trash_json_page(
    fragment: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
) -> serde_json::Value {
    let mut params: HashMap<String, String> = HashMap::new();
    if let Some(p) = prefix {
        params.insert("prefix".to_string(), p.to_string());
    }
    if let Some(d) = delimiter {
        params.insert("delimiter".to_string(), d.to_string());
    }
    params.insert("limit".to_string(), limit.to_string());
    if let Some(tok) = continuation_token {
        params.insert("continuation_token".to_string(), tok);
    }

    let resp = list_trash_objects(
        full_claims(fragment),
        AxumPath(fragment.to_string()),
        Query(params),
    )
    .await
    .expect("list trash page")
    .into_response();
    assert_eq!(resp.status(), StatusCode::OK);
    read_response_json(resp).await
}

async fn wait_until_prefix_snapshot_ready(fragment: &str, prefix: &str, timeout_ms: u64) -> bool {
    let path_manager = ObjectPathManager::new();
    let deadline = Instant::now() + std::time::Duration::from_millis(timeout_ms);

    loop {
        let bucket_dirs = path_manager.get_all_prefix_bucket_dirs(fragment, prefix);
        let mut any_bucket = false;
        let mut all_ready = true;

        for bucket in bucket_dirs {
            if !fs::try_exists(&bucket).await.unwrap_or(false) {
                continue;
            }
            any_bucket = true;

            let compacted_exists =
                fs::try_exists(&bucket.join(format!("compacted{}", PREFIX_SEGMENT_EXT)))
                    .await
                    .unwrap_or(false);
            let delimiter_exists = fs::try_exists(&bucket.join(PREFIX_DELIMITER_COMPACTED_FILE))
                .await
                .unwrap_or(false);
            if !(compacted_exists && delimiter_exists) {
                all_ready = false;
                break;
            }
        }

        if any_bucket && all_ready {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn read_object_bytes(claims: Claims, fragment: String, object_key: String) -> Vec<u8> {
    let Claims {
        fragment: claim_fragment,
        read,
        write,
        modify,
        delete,
        exp,
    } = claims;

    let mut last_status = None;
    let mut last_body_err = None;
    let mut last_err = None;
    for attempt in 0..80usize {
        let claims = Claims {
            fragment: claim_fragment.clone(),
            read,
            write,
            modify,
            delete,
            exp,
        };
        let req = Request::builder()
            .uri("/v4/fragment/get")
            .body(Body::empty())
            .expect("build request");
        match get_object(
            claims,
            AxumPath((fragment.clone(), object_key.clone())),
            req,
        )
        .await
        {
            Ok(resp) => {
                last_status = Some(resp.status());
                if resp.status() == StatusCode::OK {
                    match to_bytes(resp.into_body(), usize::MAX).await {
                        Ok(body) => return body.to_vec(),
                        Err(err) => last_body_err = Some(err.to_string()),
                    }
                }
            }
            Err(err) => {
                last_err = Some(format!("{:?}", err));
            }
        }

        if attempt < 79 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            continue;
        }
    }

    let final_metadata = get_metadata(&fragment, &object_key, &fragment)
        .await
        .map(|meta| (meta.version_id, meta.is_delete_marker, meta.parts.len()))
        .ok();
    panic!(
        "read object body failed after retries for {}/{} status={:?} body_err={:?} err={:?} final_metadata={:?}",
        fragment, object_key, last_status, last_body_err, last_err, final_metadata
    );
}

async fn read_object_bytes_eventually(
    fragment: String,
    object_key: String,
    attempts: usize,
) -> Vec<u8> {
    let mut last_status = None;
    let mut last_body_err = None;
    let mut last_err = None;
    for attempt in 0..attempts {
        let req = Request::builder()
            .uri("/v4/fragment/get")
            .body(Body::empty())
            .expect("build request");

        match get_object(
            full_claims(&fragment),
            AxumPath((fragment.clone(), object_key.clone())),
            req,
        )
        .await
        {
            Ok(resp) => {
                last_status = Some(resp.status());
                if resp.status() == StatusCode::OK {
                    match to_bytes(resp.into_body(), usize::MAX).await {
                        Ok(body) => return body.to_vec(),
                        Err(err) => last_body_err = Some(err.to_string()),
                    }
                }
            }
            Err(err) => last_err = Some(format!("{:?}", err)),
        }

        let delay_ms = if attempt < attempts / 3 {
            25
        } else if attempt < (attempts * 2) / 3 {
            50
        } else {
            100
        };
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }

    let final_metadata = get_metadata(&fragment, &object_key, &fragment)
        .await
        .map(|meta| (meta.version_id, meta.is_delete_marker, meta.parts.len()))
        .ok();
    panic!(
        "object {} in fragment {} was not readable after {} attempts status={:?} body_err={:?} err={:?} final_metadata={:?}",
        object_key, fragment, attempts, last_status, last_body_err, last_err, final_metadata
    );
}

async fn get_metadata_eventually(
    fragment: String,
    object_key: String,
    attempts: usize,
) -> super::disk_manager::ObjectMetadata {
    let mut last_err = None;
    for attempt in 0..attempts {
        match get_metadata(&fragment, &object_key, &fragment).await {
            Ok(meta) => return meta,
            Err(err) => last_err = Some(format!("{:?}", err)),
        }

        let delay_ms = if attempt < attempts / 3 {
            25
        } else if attempt < (attempts * 2) / 3 {
            50
        } else {
            100
        };
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }

    panic!(
        "metadata {} in fragment {} was not readable after {} attempts err={:?}",
        object_key, fragment, attempts, last_err
    );
}

async fn setup_fragment(fragment: &str) {
    init_quota_file(fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(fragment).await;
    init_fragment_metadata(fragment, "key")
        .await
        .expect("init metadata");
}

async fn assert_fragment_quota_used(fragment: &str, expected_used: u64) {
    let quota = get_quota(fragment).await.expect("get fragment quota");
    assert_eq!(
        quota.storage_used, expected_used,
        "unexpected quota usage for fragment {}",
        fragment
    );
}

async fn collect_index_tree_hashes(fragment: &str) -> Vec<(String, BTreeMap<String, String>)> {
    let path_manager = super::disk_manager::ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let mut out = Vec::new();

    for root in index_roots {
        let frag_root = root.join(fragment);
        let mut map = BTreeMap::new();

        if !fs::try_exists(&frag_root).await.unwrap_or(false) {
            out.push((root.to_string_lossy().to_string(), map));
            continue;
        }

        let mut prefix_buckets: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();

        for entry in walkdir::WalkDir::new(&frag_root)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }

            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.contains(".tmp.") || name.contains(".n1_health_") {
                continue;
            }

            let rel = path
                .strip_prefix(&frag_root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");

            // `index` is partitioned with owner/replica placement, so it is no longer
            // expected to be byte-identical on every index root.
            if rel.starts_with("index/") {
                continue;
            }

            if rel.starts_with("index/__p__/v1/pfx/") && rel.ends_with(PREFIX_SEGMENT_EXT) {
                if let Some(parent) = std::path::Path::new(&rel).parent() {
                    prefix_buckets.insert(parent.to_string_lossy().replace('\\', "/"));
                }
                continue;
            }

            if let Ok(bytes) = fs::read(path).await {
                let digest = Sha256::digest(&bytes);
                map.insert(rel, hex::encode(digest));
            }
        }

        // Prefix index compaction can produce different physical segment layouts per disk.
        // Canonicalize each prefix bucket to logical "latest entry per object_key" state.
        for bucket_rel in prefix_buckets {
            let bucket_abs = frag_root.join(&bucket_rel);
            let mut latest: std::collections::BTreeMap<String, (String, i64)> =
                std::collections::BTreeMap::new();

            let mut rd = match fs::read_dir(&bucket_abs).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            while let Ok(Some(ent)) = rd.next_entry().await {
                let name = ent.file_name().to_string_lossy().to_string();
                if name != format!("compacted{}", PREFIX_SEGMENT_EXT)
                    && !(name.starts_with("seg_") && name.ends_with(PREFIX_SEGMENT_EXT))
                {
                    continue;
                }
                let Ok(data) = fs::read(ent.path()).await else {
                    continue;
                };
                let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) else {
                    continue;
                };
                for e in seg.entries {
                    match latest.get(&e.object_key) {
                        None => {
                            latest.insert(e.object_key, (e.version_id, e.created_at));
                        }
                        Some((cur_vid, cur_ts)) => {
                            if version_recency_is_newer(
                                e.created_at,
                                &e.version_id,
                                *cur_ts,
                                cur_vid,
                            ) {
                                latest.insert(e.object_key, (e.version_id, e.created_at));
                            }
                        }
                    }
                }
            }

            let mut canonical = Vec::with_capacity(latest.len());
            for (k, (v, ts)) in latest {
                canonical.push((k, v, ts));
            }
            if let Ok(bytes) = rmp_serde::to_vec(&canonical) {
                let digest = Sha256::digest(bytes);
                map.insert(
                    format!("{}/__logical__.sha256", bucket_rel),
                    hex::encode(digest),
                );
            }
        }

        out.push((root.to_string_lossy().to_string(), map));
    }

    out
}

async fn assert_all_index_disks_synchronized(fragment: &str) {
    const MAX_ATTEMPTS: usize = 80;
    const RETRY_DELAY: Duration = Duration::from_millis(50);
    let mut last_snapshots = Vec::new();

    for attempt in 0..MAX_ATTEMPTS {
        let snapshots = collect_index_tree_hashes(fragment).await;
        assert!(
            snapshots.len() >= 2,
            "expected at least 2 index roots, got {}",
            snapshots.len()
        );

        let (_base_root, base_map) = &snapshots[0];
        let converged = snapshots.iter().skip(1).all(|(_root, map)| map == base_map);
        if converged {
            return;
        }

        last_snapshots = snapshots;
        if attempt % 8 == 7 {
            // Quota writes are mirrored asynchronously; trigger bounded reconciliation before
            // declaring a real mirror divergence.
            let _ = sync_single_fragment_quota(fragment.to_string()).await;
        }
        sleep(RETRY_DELAY).await;
    }

    let (base_root, base_map) = &last_snapshots[0];
    for (root, map) in last_snapshots.iter().skip(1) {
        if map != base_map {
            assert_eq!(
                map, base_map,
                "index mirrors diverged after retries: {} vs {}",
                base_root, root
            );
        }
    }
}

async fn simulate_index_root_down(root: &std::path::Path) -> (PathBuf, PathBuf) {
    let down_path = root.with_file_name(format!(
        "{}.down.{}",
        root.file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("index-root"),
        Ulid::new()
    ));
    if fs::try_exists(&down_path).await.unwrap_or(false) {
        match fs::metadata(&down_path).await {
            Ok(meta) if meta.is_dir() => {
                let _ = fs::remove_dir_all(&down_path).await;
            }
            Ok(_) => {
                let _ = fs::remove_file(&down_path).await;
            }
            Err(_) => {}
        }
    }
    fs::rename(root, &down_path)
        .await
        .unwrap_or_else(|e| panic!("simulate index root down {}: {}", root.display(), e));
    fs::write(root, b"n1-index-disk-down-blocker")
        .await
        .unwrap_or_else(|e| panic!("create index down blocker {}: {}", root.display(), e));
    (root.to_path_buf(), down_path)
}

async fn restore_index_root(root: &std::path::Path, down_path: &std::path::Path) {
    if fs::try_exists(root).await.unwrap_or(false) {
        match fs::metadata(root).await {
            Ok(meta) if meta.is_dir() => {
                fs::remove_dir_all(root).await.unwrap_or_else(|e| {
                    panic!("remove recreated index dir {}: {}", root.display(), e)
                });
            }
            Ok(_) => {
                fs::remove_file(root)
                    .await
                    .unwrap_or_else(|e| panic!("remove index blocker {}: {}", root.display(), e));
            }
            Err(e) => panic!("stat index root {}: {}", root.display(), e),
        }
    }
    fs::rename(down_path, root)
        .await
        .unwrap_or_else(|e| panic!("restore index root {}: {}", root.display(), e));
}

async fn count_raw_gc_markers(fragment: &str) -> usize {
    let mut count = 0usize;
    for root in crate::n1_core::blob_store::raw_index_metadata_roots_for_housekeeping()
        .expect("raw index housekeeping roots")
    {
        let marker_dir = root.join(fragment).join("index").join(".raw_gc");
        if !fs::try_exists(&marker_dir).await.unwrap_or(false) {
            continue;
        }
        let mut rd = match fs::read_dir(&marker_dir).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            if entry
                .file_type()
                .await
                .map(|t| t.is_file())
                .unwrap_or(false)
            {
                count += 1;
            }
        }
    }
    count
}

async fn wait_for_gc_to_finish(fragment: &str, previous_start: i64) {
    let mut saw_new_run = false;
    for _ in 0..400 {
        let status = get_gc_status_api(full_claims(fragment))
            .await
            .expect("gc status")
            .0;
        if status.start_timestamp > previous_start {
            saw_new_run = true;
        }
        if saw_new_run && !status.is_running {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("gc did not finish in time for fragment {}", fragment);
}

fn parse_tar_entry_names(tar_bytes: &[u8]) -> Vec<String> {
    let mut names = Vec::new();
    let mut off = 0usize;

    while off + 512 <= tar_bytes.len() {
        let header = &tar_bytes[off..off + 512];
        if header.iter().all(|b| *b == 0) {
            break;
        }

        let name_raw = &header[0..100];
        let name_end = name_raw
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(name_raw.len());
        let name = String::from_utf8_lossy(&name_raw[..name_end]).to_string();
        names.push(name);

        let size_raw = &header[124..136];
        let size_txt = String::from_utf8_lossy(size_raw)
            .trim_matches('\0')
            .trim()
            .to_string();
        let size = u64::from_str_radix(size_txt.trim(), 8).unwrap_or(0) as usize;
        let data_blocks = if size == 0 {
            0
        } else {
            size.div_ceil(512) * 512
        };
        off += 512 + data_blocks;
    }

    names
}

async fn upload_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
    bytes: Vec<u8>,
) -> axum::response::Response {
    upload_part_with_options(fragment, version_id, part_index, bytes, None, None).await
}

async fn upload_part_with_durability(
    fragment: &str,
    version_id: &str,
    part_index: u64,
    bytes: Vec<u8>,
    durability: Option<&str>,
) -> axum::response::Response {
    upload_part_with_options(fragment, version_id, part_index, bytes, durability, None).await
}

async fn upload_part_with_options(
    fragment: &str,
    version_id: &str,
    part_index: u64,
    bytes: Vec<u8>,
    durability: Option<&str>,
    io_gate: Option<bool>,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-length",
        HeaderValue::from_str(&bytes.len().to_string()).expect("content-length"),
    );

    object_upload_part(
        full_claims(fragment),
        headers,
        AxumPath((fragment.to_string(), version_id.to_string(), part_index)),
        Query(UploadPartQuery {
            checksum: None,
            durability: durability.map(|s| s.to_string()),
            strict_durability: None,
            io_gate,
        }),
        Body::from(bytes),
    )
    .await
    .expect("upload part")
    .into_response()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_upload_part_batched_durability_finalize_ok() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "durability/batched.bin";
    let content = (0..(128 * 1024 + 777))
        .map(|i| (i % 239) as u8)
        .collect::<Vec<u8>>();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "test-fragment-key")
        .await
        .expect("init fragment metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let (version_id, session_id) = init_upload(&fragment, key, content.len() as u64, 1).await;
    let put_resp =
        upload_part_with_durability(&fragment, &version_id, 0, content.clone(), Some("batched"))
            .await;
    assert_eq!(put_resp.status(), StatusCode::OK);

    let finalize_resp = finalize_upload(
        state,
        &fragment,
        &version_id,
        &session_id,
        key,
        content.len() as u64,
        1,
    )
    .await;
    assert_eq!(finalize_resp.status(), StatusCode::OK);

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.to_string()).await;
    assert_eq!(got, content);
}

async fn init_upload(
    fragment: &str,
    object_key: &str,
    total_size: u64,
    expected_parts: u64,
) -> (String, String) {
    init_upload_with_plan(fragment, object_key, total_size, expected_parts, None, None).await
}

async fn init_upload_with_plan(
    fragment: &str,
    object_key: &str,
    total_size: u64,
    expected_parts: u64,
    part_size_bytes: Option<u64>,
    part_sizes_bytes: Option<Vec<u64>>,
) -> (String, String) {
    let init_resp = initiate_object_upload(
        full_claims(fragment),
        AxumPath((fragment.to_string(), object_key.to_string())),
        AxumJson(InitiateUploadParams {
            content_type: Some("application/octet-stream".into()),
            total_size_bytes: total_size,
            expected_parts,
            part_size_bytes,
            part_sizes_bytes,
        }),
    )
    .await
    .expect("initiate upload")
    .into_response();
    assert_eq!(init_resp.status(), StatusCode::CREATED);
    let init_json = read_response_json(init_resp).await;
    let version_id = init_json["version_id"]
        .as_str()
        .expect("version_id")
        .to_string();
    let session_id = init_json["session_id"]
        .as_str()
        .expect("session_id")
        .to_string();
    (version_id, session_id)
}

async fn finalize_upload(
    state: Arc<AppState>,
    fragment: &str,
    version_id: &str,
    session_id: &str,
    object_key: &str,
    total_size: u64,
    expected_parts: u64,
) -> axum::response::Response {
    finalize_object_upload(
        State(state),
        full_claims(fragment),
        AxumPath((
            fragment.to_string(),
            version_id.to_string(),
            session_id.to_string(),
        )),
        Query(ObjectKeyQuery {
            object_key: object_key.to_string(),
            version_id: None,
        }),
        AxumJson(InitiateUploadParams {
            content_type: Some("application/octet-stream".into()),
            total_size_bytes: total_size,
            expected_parts,
            part_size_bytes: None,
            part_sizes_bytes: None,
        }),
    )
    .await
    .expect("finalize upload")
    .into_response()
}

async fn run_active_multipart_resume_with_replica_index_disk_loss(rounds: usize) {
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    for round in 0..rounds {
        let part0 =
            chunked_payload_for_seed(8 * 1024 + 3 + round, payload_seed_for_epoch(round, 1));
        let part1 =
            chunked_payload_for_seed(9 * 1024 + 5 + round, payload_seed_for_epoch(round, 2));
        let part2 =
            chunked_payload_for_seed(10 * 1024 + 7 + round, payload_seed_for_epoch(round, 3));
        let total_len = (part0.len() + part1.len() + part2.len()) as u64;

        let key = format!("ha-index/session-resume/r{}/object-0.bin", round);
        let (version_id, session_id) = init_upload(&fragment, &key, total_len, 3).await;
        let index_roots = ObjectPathManager::new().get_all_index_roots();
        assert!(
            index_roots.len() >= 2,
            "expected at least two raw index roots for replica loss test"
        );
        let owner_root = index_roots[0].clone();
        let replica_root = index_roots[1].clone();

        assert_eq!(
            upload_part(&fragment, &version_id, 0, part0.clone())
                .await
                .status(),
            StatusCode::OK
        );

        assert_ne!(
            owner_root, replica_root,
            "replica loss test requires distinct owner and replica roots"
        );

        let (replica_root, replica_down_path) = simulate_index_root_down(&replica_root).await;

        assert_eq!(
            upload_part(&fragment, &version_id, 1, part1.clone())
                .await
                .status(),
            StatusCode::OK
        );

        let status_resp = get_upload_status(
            full_claims(&fragment),
            AxumPath((fragment.clone(), version_id.clone())),
        )
        .await
        .expect("upload status with replica down")
        .into_response();
        assert_eq!(status_resp.status(), StatusCode::OK);
        let status_json = read_response_json(status_resp).await;
        assert_eq!(status_json["uploaded_parts"][0].as_u64(), Some(0));
        assert_eq!(status_json["uploaded_parts"][1].as_u64(), Some(1));

        let session_state = index::load_session(&fragment, &version_id)
            .await
            .expect("load owner-first session state")
            .expect("active session state");
        let committed_parts = session_state.parts.keys().copied().collect::<Vec<_>>();
        assert_eq!(committed_parts, vec![0, 1]);

        let finalize_conflict = finalize_upload(
            state.clone(),
            &fragment,
            &version_id,
            &session_id,
            &key,
            total_len,
            3,
        )
        .await;
        assert_eq!(finalize_conflict.status(), StatusCode::CONFLICT);
        let conflict_json = read_response_json(finalize_conflict).await;
        assert_eq!(conflict_json["missing_parts"][0].as_u64(), Some(2));

        assert_eq!(
            upload_part(&fragment, &version_id, 2, part2.clone())
                .await
                .status(),
            StatusCode::OK
        );

        let finalize_ok = finalize_upload(
            state.clone(),
            &fragment,
            &version_id,
            &session_id,
            &key,
            total_len,
            3,
        )
        .await;
        assert_eq!(finalize_ok.status(), StatusCode::OK);

        let expected = [part0, part1, part2].concat();
        let got = read_object_bytes_eventually(fragment.clone(), key.clone(), 24).await;
        assert_eq!(got, expected, "round {} read mismatch", round);

        restore_index_root(&replica_root, &replica_down_path).await;

        run_repair_job(
            RepairRunConfig {
                dry_run: false,
                check_mode: RepairCheckMode::Scrub,
                meta_concurrency: 8,
                shard_repair_concurrency: 2,
                meta_batch_size: 512,
            },
            Arc::new(WsConnectionManager::new()),
        )
        .await
        .expect("repair after replica restore");

        assert_all_index_disks_synchronized(&fragment).await;
    }
}

async fn put_object(
    state: Arc<AppState>,
    fragment: &str,
    object_key: &str,
    bytes: Vec<u8>,
) -> String {
    let (version_id, session_id) = init_upload(fragment, object_key, bytes.len() as u64, 1).await;
    let put_resp = upload_part(fragment, &version_id, 0, bytes.clone()).await;
    assert_eq!(put_resp.status(), StatusCode::OK);
    let finalize_resp = finalize_upload(
        state,
        fragment,
        &version_id,
        &session_id,
        object_key,
        bytes.len() as u64,
        1,
    )
    .await;
    assert_eq!(finalize_resp.status(), StatusCode::OK);
    version_id
}

async fn put_object_generated_chunked(
    state: Arc<AppState>,
    fragment: &str,
    object_key: &str,
    object_bytes: usize,
    part_bytes: usize,
    seed: usize,
) -> String {
    put_object_generated_chunked_with_options(
        state,
        fragment,
        object_key,
        object_bytes,
        part_bytes,
        seed,
        None,
        None,
    )
    .await
}

async fn put_object_generated_chunked_with_options(
    state: Arc<AppState>,
    fragment: &str,
    object_key: &str,
    object_bytes: usize,
    part_bytes: usize,
    seed: usize,
    durability: Option<&str>,
    io_gate: Option<bool>,
) -> String {
    let expected_parts = object_bytes.div_ceil(part_bytes) as u64;
    let (version_id, session_id) =
        init_upload(fragment, object_key, object_bytes as u64, expected_parts).await;

    for part_index in 0..expected_parts {
        let start = part_index as usize * part_bytes;
        let len = std::cmp::min(part_bytes, object_bytes.saturating_sub(start));
        let mut part = vec![0u8; len];
        for (j, b) in part.iter_mut().enumerate() {
            *b = ((start + j + seed) % 251) as u8;
        }

        let put_resp =
            upload_part_with_options(fragment, &version_id, part_index, part, durability, io_gate)
                .await;
        assert_eq!(put_resp.status(), StatusCode::OK);
    }

    let finalize_resp = finalize_upload(
        state,
        fragment,
        &version_id,
        &session_id,
        object_key,
        object_bytes as u64,
        expected_parts,
    )
    .await;
    assert_eq!(finalize_resp.status(), StatusCode::OK);
    version_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_upload_read_modify_delete_redistribute_repair() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let source_key = "folder/source.bin".to_string();
    let renamed_key = "folder/renamed.bin".to_string();
    let content = (0..(64 * 1024 + 333))
        .map(|i| (i % 251) as u8)
        .collect::<Vec<u8>>();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "test-fragment-key")
        .await
        .expect("init fragment metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let (version_id, session_id) =
        init_upload(&fragment, &source_key, content.len() as u64, 1).await;
    let put_resp = upload_part(&fragment, &version_id, 0, content.clone()).await;
    assert_eq!(put_resp.status(), StatusCode::OK);

    let finalize_resp = finalize_upload(
        state.clone(),
        &fragment,
        &version_id,
        &session_id,
        &source_key,
        content.len() as u64,
        1,
    )
    .await;
    assert_eq!(finalize_resp.status(), StatusCode::OK);

    let fetched_source =
        read_object_bytes(full_claims(&fragment), fragment.clone(), source_key.clone()).await;
    assert_eq!(fetched_source, content);

    let rename_resp = rename_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(RenameObjectRequest {
            source_fragment: fragment.clone(),
            source_key: source_key.clone(),
            destination_fragment: fragment.clone(),
            destination_key: renamed_key.clone(),
        }),
    )
    .await
    .expect("rename object")
    .into_response();
    assert_eq!(rename_resp.status(), StatusCode::OK);

    let old_req = Request::builder()
        .uri("/v4/fragment/get")
        .body(Body::empty())
        .expect("build request");
    assert!(
        get_object(
            full_claims(&fragment),
            AxumPath((fragment.clone(), source_key.clone())),
            old_req,
        )
        .await
        .is_err(),
        "source key should no longer be retrievable after rename"
    );

    let fetched_renamed = read_object_bytes(
        full_claims(&fragment),
        fragment.clone(),
        renamed_key.clone(),
    )
    .await;
    assert_eq!(fetched_renamed, content);

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("run redistribution");

    let redistributed_meta = get_metadata(&fragment, &renamed_key, &fragment)
        .await
        .expect("metadata after redistribution");
    assert_eq!(redistributed_meta.parts.len(), 1);
    let shards = &redistributed_meta.parts[0].shards;
    assert_eq!(shards.len(), 3);
    assert_eq!(shards.iter().filter(|s| !s.is_parity).count(), 2);
    assert_eq!(shards.iter().filter(|s| s.is_parity).count(), 1);

    let target_shard = shards
        .iter()
        .find(|s| !s.is_parity)
        .expect("data shard to corrupt")
        .clone();

    corrupt_shard_bytes(&target_shard, 0, &vec![0xA5; 4096]).await;
    assert!(
        !shard_checksum_ok(&target_shard).await,
        "corruption should break checksum before repair"
    );

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("run repair");

    assert!(
        shard_checksum_ok(&target_shard).await,
        "repair should restore shard checksum"
    );

    let fetched_after_repair = read_object_bytes(
        full_claims(&fragment),
        fragment.clone(),
        renamed_key.clone(),
    )
    .await;
    assert_eq!(fetched_after_repair, content);

    let delete_resp = delete_object(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), renamed_key.clone())),
    )
    .await
    .expect("delete object")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let soft_deleted_meta = get_metadata(&fragment, &renamed_key, &fragment)
        .await
        .expect("metadata should exist as tombstone after soft delete");
    assert!(
        soft_deleted_meta.is_delete_marker,
        "soft delete should leave a delete-marker tombstone"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Arc is flat-list only; prefix listing semantics moved to the registry layer"]
async fn matrix_prefix_listing_survives_rename_delete_after_compaction() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let prev_data_shards = DATA_SHARDS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(1);
    let prev_parity_shards = PARITY_SHARDS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(0);
    let prev_stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(4096);

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(256 * 1024, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key_a = "ward/a.bin".to_string();
    let key_b = "ward/b.bin".to_string();
    let key_a_renamed = "ward/a-renamed.bin".to_string();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    put_object(state.clone(), &fragment, &key_a, vec![1u8; 8 * 1024]).await;
    put_object(state.clone(), &fragment, &key_b, vec![2u8; 8 * 1024]).await;

    let rename_resp = rename_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(RenameObjectRequest {
            source_fragment: fragment.clone(),
            source_key: key_a.clone(),
            destination_fragment: fragment.clone(),
            destination_key: key_a_renamed.clone(),
        }),
    )
    .await
    .expect("rename object")
    .into_response();
    assert_eq!(rename_resp.status(), StatusCode::OK);

    let delete_resp = delete_object(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key_a_renamed.clone())),
    )
    .await
    .expect("delete renamed object")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let listing = list_objects_json(&fragment, Some("ward"), None).await;
    let keys = listing["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    assert!(
        keys.iter().any(|k| k == &key_b),
        "expected unaffected key to stay listed, got {:?}",
        keys
    );
    assert!(
        keys.iter().all(|k| k != &key_a_renamed),
        "deleted key should not stay listed: {:?}",
        keys
    );

    let listing_delim = list_objects_json(&fragment, Some("ward"), Some("/")).await;
    let delim_keys = listing_delim["objects"]
        .as_array()
        .expect("delimiter objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let delim_prefixes = listing_delim["common_prefixes"]
        .as_array()
        .expect("delimiter prefixes array")
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    assert!(
        delim_keys.iter().any(|k| k == &key_b),
        "delimiter listing should keep unaffected key, got {:?}",
        delim_keys
    );
    assert!(
        delim_keys.iter().all(|k| k != &key_a_renamed),
        "delimiter listing should exclude deleted key: {:?}",
        delim_keys
    );
    assert!(
        delim_prefixes.is_empty(),
        "ward delimiter listing should not emit child prefixes: {:?}",
        delim_prefixes
    );

    let root_listing_delim = list_objects_json(&fragment, None, Some("/")).await;
    let root_prefixes = root_listing_delim["common_prefixes"]
        .as_array()
        .expect("root prefixes array")
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        root_prefixes,
        vec!["ward/".to_string()],
        "root delimiter listing should retain only the live ward prefix"
    );

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(prev_data_shards, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(prev_parity_shards, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(prev_stripe_size, Ordering::Relaxed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Arc is flat-list only; delimiter listing moved to the registry layer"]
async fn matrix_delimiter_listing_keeps_exact_prefix_object_and_child_prefixes() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    for (key, fill) in [
        ("study", 1u8),
        ("study/root.bin", 2u8),
        ("study/reports/q1.bin", 3u8),
        ("study/series/a.bin", 4u8),
        ("study/series/b.bin", 5u8),
        ("study-extra.bin", 6u8),
    ] {
        put_object(state.clone(), &fragment, key, vec![fill; 4096]).await;
    }

    let listing = list_objects_json(&fragment, Some("study"), Some("/")).await;
    let mut object_keys = listing["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    object_keys.sort();
    let mut common_prefixes = listing["common_prefixes"]
        .as_array()
        .expect("common prefixes array")
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    common_prefixes.sort();

    assert_eq!(
        object_keys,
        vec!["study".to_string(), "study/root.bin".to_string()]
    );
    assert_eq!(
        common_prefixes,
        vec!["study/reports/".to_string(), "study/series/".to_string()]
    );
    assert!(listing["continuation_token"].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Arc is flat-list only; delimiter pagination moved to the registry layer"]
async fn matrix_delimiter_listing_paginates_without_skipping_rows() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    for (key, fill) in [
        ("page", 1u8),
        ("page/a.bin", 2u8),
        ("page/b.bin", 3u8),
        ("page/sub-a/x.bin", 4u8),
        ("page/sub-b/y.bin", 5u8),
    ] {
        put_object(state.clone(), &fragment, key, vec![fill; 2048]).await;
    }

    let page1 = list_objects_json_page(&fragment, Some("page"), Some("/"), 2, None).await;
    let token1 = page1["continuation_token"]
        .as_str()
        .expect("page1 continuation token")
        .to_string();

    let page2 = list_objects_json_page(&fragment, Some("page"), Some("/"), 2, Some(token1)).await;
    let token2 = page2["continuation_token"]
        .as_str()
        .expect("page2 continuation token")
        .to_string();

    let page3 = list_objects_json_page(&fragment, Some("page"), Some("/"), 2, Some(token2)).await;

    let mut rows = Vec::new();
    for page in [&page1, &page2, &page3] {
        rows.extend(
            page["objects"]
                .as_array()
                .expect("objects array")
                .iter()
                .filter_map(|v| v["object_key"].as_str())
                .map(|s| format!("object:{s}")),
        );
        rows.extend(
            page["common_prefixes"]
                .as_array()
                .expect("common prefixes array")
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| format!("prefix:{s}")),
        );
    }

    assert_eq!(
        rows,
        vec![
            "object:page".to_string(),
            "object:page/a.bin".to_string(),
            "object:page/b.bin".to_string(),
            "prefix:page/sub-a/".to_string(),
            "prefix:page/sub-b/".to_string(),
        ]
    );
    assert!(page3["continuation_token"].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_trash_listing_paginates_without_full_scan_sort_path() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let mut keys = vec![
        "trash-page/c.bin".to_string(),
        "trash-page/a.bin".to_string(),
        "trash-page/e.bin".to_string(),
        "trash-page/b.bin".to_string(),
        "trash-page/d.bin".to_string(),
    ];

    for (idx, key) in keys.iter().enumerate() {
        put_object(state.clone(), &fragment, key, vec![idx as u8 + 1; 2048]).await;
        let delete_resp = delete_object(
            State(state.clone()),
            full_claims(&fragment),
            AxumPath((fragment.clone(), key.clone())),
        )
        .await
        .expect("soft delete object")
        .into_response();
        assert_eq!(delete_resp.status(), StatusCode::OK);
    }

    let page1 = list_trash_json_page(&fragment, None, None, 2, None).await;
    let token1 = page1["continuation_token"]
        .as_str()
        .expect("page1 continuation token")
        .to_string();

    let page2 = list_trash_json_page(&fragment, None, None, 2, Some(token1)).await;
    let token2 = page2["continuation_token"]
        .as_str()
        .expect("page2 continuation token")
        .to_string();

    let page3 = list_trash_json_page(&fragment, None, None, 2, Some(token2)).await;
    assert!(
        page3["continuation_token"].is_null(),
        "last page should not return continuation token"
    );

    let mut listed = Vec::<String>::new();
    for page in [&page1, &page2, &page3] {
        listed.extend(
            page["objects"]
                .as_array()
                .expect("objects array")
                .iter()
                .filter_map(|v| v["object_key"].as_str())
                .filter(|key| key.starts_with("trash-page/"))
                .map(|s| s.to_string()),
        );
    }
    listed.sort();
    keys.sort();
    assert_eq!(listed, keys);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Arc is flat-list only; trash delimiter listing moved to the registry layer"]
async fn matrix_trash_delimiter_listing_paginates_without_full_materialize_path() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    for (idx, key) in [
        "trash-delim",
        "trash-delim/a.bin",
        "trash-delim/b.bin",
        "trash-delim/sub-a/x.bin",
        "trash-delim/sub-b/y.bin",
    ]
    .iter()
    .enumerate()
    {
        put_object(state.clone(), &fragment, key, vec![idx as u8 + 1; 2048]).await;
        let delete_resp = delete_object(
            State(state.clone()),
            full_claims(&fragment),
            AxumPath((fragment.clone(), key.to_string())),
        )
        .await
        .expect("soft delete object")
        .into_response();
        assert_eq!(delete_resp.status(), StatusCode::OK);
    }

    let page1 = list_trash_json_page(&fragment, Some("trash-delim"), Some("/"), 2, None).await;
    let token1 = page1["continuation_token"]
        .as_str()
        .expect("page1 continuation token")
        .to_string();

    let page2 =
        list_trash_json_page(&fragment, Some("trash-delim"), Some("/"), 2, Some(token1)).await;
    let token2 = page2["continuation_token"]
        .as_str()
        .expect("page2 continuation token")
        .to_string();

    let page3 =
        list_trash_json_page(&fragment, Some("trash-delim"), Some("/"), 2, Some(token2)).await;
    assert!(
        page3["continuation_token"].is_null(),
        "last page should not return continuation token"
    );

    let mut rows = Vec::new();
    for page in [&page1, &page2, &page3] {
        rows.extend(
            page["objects"]
                .as_array()
                .expect("objects array")
                .iter()
                .filter_map(|v| v["object_key"].as_str())
                .map(|s| format!("object:{s}")),
        );
        rows.extend(
            page["common_prefixes"]
                .as_array()
                .expect("common prefixes array")
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| format!("prefix:{s}")),
        );
    }

    assert_eq!(
        rows,
        vec![
            "object:trash-delim".to_string(),
            "object:trash-delim/a.bin".to_string(),
            "object:trash-delim/b.bin".to_string(),
            "prefix:trash-delim/sub-a/".to_string(),
            "prefix:trash-delim/sub-b/".to_string(),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_flat_listing_rejects_prefix_and_delimiter_queries() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    for params in [
        HashMap::from([("prefix".to_string(), "x".to_string())]),
        HashMap::from([("delimiter".to_string(), "/".to_string())]),
        HashMap::from([
            ("prefix".to_string(), "x".to_string()),
            ("delimiter".to_string(), "/".to_string()),
        ]),
    ] {
        let err = match list_objects(
            full_claims(&fragment),
            AxumPath(fragment.clone()),
            Query(params),
        )
        .await
        {
            Ok(_) => panic!("flat Arc listing should reject prefix and delimiter"),
            Err(err) => err,
        };
        assert!(
            matches!(err, crate::enums::app_error_enum::AppError::BadRequest(_)),
            "expected bad request, got {:?}",
            err
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_flat_trash_listing_rejects_prefix_and_delimiter_queries() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    for params in [
        HashMap::from([("prefix".to_string(), "x".to_string())]),
        HashMap::from([("delimiter".to_string(), "/".to_string())]),
        HashMap::from([
            ("prefix".to_string(), "x".to_string()),
            ("delimiter".to_string(), "/".to_string()),
        ]),
    ] {
        let err = match list_trash_objects(
            full_claims(&fragment),
            AxumPath(fragment.clone()),
            Query(params),
        )
        .await
        {
            Ok(_) => panic!("flat Arc trash listing should reject prefix and delimiter"),
            Err(err) => err,
        };
        assert!(
            matches!(err, crate::enums::app_error_enum::AppError::BadRequest(_)),
            "expected bad request, got {:?}",
            err
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_multipart_finalize_conflict_then_success() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/multipart.bin".to_string();
    let part0 = vec![1u8; 11 * 1024 + 3];
    let part1 = vec![2u8; 9 * 1024 + 5];
    let combined_len = (part0.len() + part1.len()) as u64;

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let (version_id, session_id) = init_upload(&fragment, &key, combined_len, 2).await;

    let put0 = upload_part(&fragment, &version_id, 0, part0.clone()).await;
    assert_eq!(put0.status(), StatusCode::OK);

    let finalize_conflict = finalize_upload(
        state.clone(),
        &fragment,
        &version_id,
        &session_id,
        &key,
        combined_len,
        2,
    )
    .await;
    assert_eq!(finalize_conflict.status(), StatusCode::CONFLICT);
    let conflict_json = read_response_json(finalize_conflict).await;
    assert_eq!(conflict_json["missing_parts"][0].as_u64(), Some(1));

    let put1 = upload_part(&fragment, &version_id, 1, part1.clone()).await;
    assert_eq!(put1.status(), StatusCode::OK);

    let finalize_ok = finalize_upload(
        state,
        &fragment,
        &version_id,
        &session_id,
        &key,
        combined_len,
        2,
    )
    .await;
    assert_eq!(finalize_ok.status(), StatusCode::OK);

    let expected = [part0, part1].concat();
    let got = read_object_bytes_eventually(fragment.clone(), key.clone(), 160).await;
    assert_eq!(got, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Arc is flat-list only; prefix listing semantics moved to the registry layer"]
async fn matrix_prefix_listing_serves_from_inline_prefix_rows_without_object_metadata_reads() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key_exact = "inline".to_string();
    let key_child = "inline/item.bin".to_string();
    put_object(state.clone(), &fragment, &key_exact, vec![7u8; 1024]).await;
    put_object(state, &fragment, &key_child, vec![9u8; 2048]).await;

    let listing = list_objects_json(&fragment, Some("inline"), None).await;
    let mut keys = listing["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    keys.sort();

    assert_eq!(keys, vec![key_exact.clone(), key_child.clone()]);

    let delimiter_listing = list_objects_json(&fragment, Some("inline"), Some("/")).await;
    let mut delimiter_keys = delimiter_listing["objects"]
        .as_array()
        .expect("delimiter objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    delimiter_keys.sort();

    assert_eq!(delimiter_keys, vec![key_exact, key_child]);
    assert!(
        delimiter_listing["common_prefixes"]
            .as_array()
            .expect("common prefixes array")
            .is_empty(),
        "delimiter listing should not require metadata lookups for direct child rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy prefix snapshot internals are not applicable to current raw-only Arc"]
async fn matrix_prefix_projection_writes_compacted_snapshots_without_delta_segments() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key = "projected/path.bin".to_string();
    put_object(state, &fragment, &key, vec![3u8; 1536]).await;

    assert!(
        wait_until_prefix_snapshot_ready(&fragment, "", 5000).await,
        "root prefix snapshot should be ready"
    );
    assert!(
        wait_until_prefix_snapshot_ready(&fragment, "projected", 5000).await,
        "projected prefix snapshot should be ready"
    );

    let path_manager = ObjectPathManager::new();
    for prefix in ["", "projected"] {
        for bucket in path_manager.get_all_prefix_bucket_dirs(&fragment, prefix) {
            if !fs::try_exists(&bucket).await.unwrap_or(false) {
                continue;
            }

            assert!(
                fs::try_exists(&bucket.join(format!("compacted{}", PREFIX_SEGMENT_EXT)))
                    .await
                    .unwrap_or(false),
                "expected compacted prefix snapshot for prefix '{}' in {:?}",
                prefix,
                bucket
            );
            assert!(
                fs::try_exists(&bucket.join(PREFIX_DELIMITER_COMPACTED_FILE))
                    .await
                    .unwrap_or(false),
                "expected delimiter compacted snapshot for prefix '{}' in {:?}",
                prefix,
                bucket
            );

            let mut entries = fs::read_dir(&bucket).await.expect("read bucket");
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                assert!(
                    !(name.starts_with("seg_") && name.ends_with(PREFIX_SEGMENT_EXT)),
                    "delta prefix segments should not be written by the projector: {:?}",
                    entry.path()
                );
                assert!(
                    !(name.starts_with(PREFIX_DELIMITER_SEGMENT_PREFIX)
                        && name.ends_with(PREFIX_SEGMENT_EXT)),
                    "delta delimiter segments should not be written by the projector: {:?}",
                    entry.path()
                );
            }
        }
    }

    let listing = list_objects_json(&fragment, Some("projected"), None).await;
    let keys = listing["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec![key]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy segmented wal/checkpoint layout test is not applicable to current raw-only Arc"]
async fn matrix_index_object_partition_uses_segmented_wal_and_checkpoints() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 256 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key = "wal/repeated.bin".to_string();
    let mut latest_version = String::new();
    for idx in 0..64u8 {
        latest_version = put_object(
            state.clone(),
            &fragment,
            &key,
            vec![idx; 1024 + usize::from(idx)],
        )
        .await;
    }

    let meta_paths = index::get_object_version_paths(&fragment, &key, &latest_version, false)
        .expect("object version paths");
    assert!(
        !meta_paths.is_empty(),
        "expected at least one index replica path"
    );

    let mut saw_checkpoint = false;
    let mut saw_segmented_wal = false;
    let mut checkpoint_paths = Vec::new();
    for meta_path in meta_paths {
        let partition_dir = meta_path
            .ancestors()
            .nth(4)
            .expect("partition dir ancestor")
            .to_path_buf();
        let wal_dir = partition_dir.join("wal");
        let manifest_path = wal_dir.join("manifest.msgpack");
        let checkpoint_path = partition_dir.join("checkpoints").join("latest.msgpack");
        let segments_dir = wal_dir.join("segments");

        assert!(
            fs::try_exists(&manifest_path).await.unwrap_or(false),
            "expected wal manifest at {}",
            manifest_path.display()
        );
        assert!(
            !fs::try_exists(&wal_dir.join("active.wal"))
                .await
                .unwrap_or(false),
            "legacy single active.wal should not be present"
        );

        checkpoint_paths.push(checkpoint_path);

        let mut segment_count = 0usize;
        if fs::try_exists(&segments_dir).await.unwrap_or(false) {
            let mut entries = fs::read_dir(&segments_dir)
                .await
                .expect("read wal segments");
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry.file_name().to_string_lossy().ends_with(".wal") {
                    segment_count += 1;
                }
            }
        }
        if segment_count >= 1 {
            saw_segmented_wal = true;
        }
    }

    let checkpoint_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < checkpoint_deadline {
        for checkpoint_path in &checkpoint_paths {
            if fs::try_exists(checkpoint_path).await.unwrap_or(false) {
                saw_checkpoint = true;
                break;
            }
        }
        if saw_checkpoint {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(saw_checkpoint, "expected at least one current checkpoint");
    assert!(
        saw_segmented_wal,
        "expected the repeated object partition to persist segmented wal files"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_upload_resume_status_then_finalize_success() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/resume.bin".to_string();
    let part0 = vec![0x11; 8 * 1024 + 7];
    let part1 = vec![0x22; 9 * 1024 + 5];
    let part2 = vec![0x33; 10 * 1024 + 3];
    let total_len = (part0.len() + part1.len() + part2.len()) as u64;

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let (version_id, session_id) = init_upload(&fragment, &key, total_len, 3).await;

    assert_eq!(
        upload_part(&fragment, &version_id, 0, part0.clone())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        upload_part(&fragment, &version_id, 2, part2.clone())
            .await
            .status(),
        StatusCode::OK
    );

    let status_resp = get_upload_status(
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_id.clone())),
    )
    .await
    .expect("upload status")
    .into_response();
    assert_eq!(status_resp.status(), StatusCode::OK);
    let status_json = read_response_json(status_resp).await;
    assert_eq!(status_json["uploaded_parts"][0].as_u64(), Some(0));
    assert_eq!(status_json["uploaded_parts"][1].as_u64(), Some(2));

    let finalize_conflict = finalize_upload(
        state.clone(),
        &fragment,
        &version_id,
        &session_id,
        &key,
        total_len,
        3,
    )
    .await;
    assert_eq!(finalize_conflict.status(), StatusCode::CONFLICT);
    let conflict_json = read_response_json(finalize_conflict).await;
    assert_eq!(conflict_json["uploaded_parts"][0].as_u64(), Some(0));
    assert_eq!(conflict_json["uploaded_parts"][1].as_u64(), Some(2));
    assert_eq!(conflict_json["missing_parts"][0].as_u64(), Some(1));

    assert_eq!(
        upload_part(&fragment, &version_id, 1, part1.clone())
            .await
            .status(),
        StatusCode::OK
    );

    let finalize_ok = finalize_upload(
        state,
        &fragment,
        &version_id,
        &session_id,
        &key,
        total_len,
        3,
    )
    .await;
    assert_eq!(finalize_ok.status(), StatusCode::OK);

    let expected = [part0, part1, part2].concat();
    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_cancel_upload_before_first_part_succeeds() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/cancel-before-part.bin".to_string();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let (version_id, _session_id) = init_upload(&fragment, &key, 4 * 1024 * 1024, 1).await;
    let session_state = index::load_session(&fragment, &version_id)
        .await
        .expect("load session")
        .expect("active session");
    let planned_addresses = session_state.manifest.all_planned_raw_addresses();
    assert!(
        !planned_addresses.is_empty(),
        "single-part uploads should pre-reserve deterministic raw slots"
    );
    for raw in &planned_addresses {
        assert!(
            raw_blob_address_exists(raw)
                .await
                .expect("raw exists check"),
            "planned raw slot should exist before any part upload"
        );
    }

    let cancel_resp = cancel_upload_session(
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_id.clone())),
        Query(ObjectKeyQuery {
            object_key: key.clone(),
            version_id: None,
        }),
    )
    .await
    .expect("cancel upload")
    .into_response();
    assert_eq!(cancel_resp.status(), StatusCode::NO_CONTENT);
    assert!(
        index::load_session(&fragment, &version_id)
            .await
            .expect("load session")
            .is_none(),
        "session should be removed even when no snapshot file was written"
    );
    for raw in &planned_addresses {
        assert!(
            !raw_blob_address_exists(raw)
                .await
                .expect("raw exists check"),
            "cancel should free planned raw slot reservations"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_cancel_upload_after_part_frees_raw_part_extents() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/cancel-after-part.bin".to_string();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let content = vec![0x5Au8; 2 * 1024 * 1024];
    let (version_id, _session_id) = init_upload(&fragment, &key, content.len() as u64, 1).await;
    upload_part(&fragment, &version_id, 0, content).await;

    let uploaded_parts_resp = get_upload_status(
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_id.clone())),
    )
    .await
    .expect("upload status");
    let status_json = read_response_json(uploaded_parts_resp.into_response()).await;
    assert_eq!(status_json["uploaded_parts"][0].as_u64(), Some(0));

    let part_meta =
        super::upload::read_part_meta_any(&ObjectPathManager::new(), &fragment, &version_id, 0)
            .await
            .expect("part meta");
    let raw_addresses: Vec<_> = part_meta
        .shards
        .iter()
        .filter_map(|shard| shard.raw_blob_address.clone())
        .collect();
    assert!(!raw_addresses.is_empty(), "raw addresses recorded");

    let cancel_resp = cancel_upload_session(
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_id.clone())),
        Query(ObjectKeyQuery {
            object_key: key.clone(),
            version_id: None,
        }),
    )
    .await
    .expect("cancel upload")
    .into_response();
    assert_eq!(cancel_resp.status(), StatusCode::NO_CONTENT);

    for raw in raw_addresses {
        assert!(
            !raw_blob_address_exists(&raw)
                .await
                .expect("raw exists check"),
            "cancel should free raw part extent"
        );
    }
    assert!(
        index::load_session(&fragment, &version_id)
            .await
            .expect("load session")
            .is_none(),
        "session should be removed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_planned_multipart_upload_reuses_reserved_raw_slots() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/planned-upload.bin".to_string();
    let part0 = vec![0x41; 8 * 1024 + 17];
    let part1 = vec![0x52; 6 * 1024 + 9];
    let total_len = (part0.len() + part1.len()) as u64;

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let planned_sizes = vec![part0.len() as u64, part1.len() as u64];
    let (version_id, session_id) = init_upload_with_plan(
        &fragment,
        &key,
        total_len,
        2,
        None,
        Some(planned_sizes.clone()),
    )
    .await;

    let session_state = index::load_session(&fragment, &version_id)
        .await
        .expect("load session")
        .expect("session state");
    assert_eq!(session_state.manifest.planned_parts.len(), 2);
    let planned_part0 = session_state
        .manifest
        .planned_part_layout(0)
        .expect("planned part 0")
        .clone();
    let planned_part1 = session_state
        .manifest
        .planned_part_layout(1)
        .expect("planned part 1")
        .clone();
    assert_eq!(planned_part0.size_bytes, planned_sizes[0]);
    assert_eq!(planned_part1.size_bytes, planned_sizes[1]);
    for raw in session_state.manifest.all_planned_raw_addresses() {
        assert!(
            raw_blob_address_exists(&raw)
                .await
                .expect("raw exists check"),
            "planned raw slot should exist after init"
        );
    }

    assert_eq!(
        upload_part(&fragment, &version_id, 0, part0.clone())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        upload_part(&fragment, &version_id, 1, part1.clone())
            .await
            .status(),
        StatusCode::OK
    );

    let committed_part0 =
        super::upload::read_part_meta_any(&ObjectPathManager::new(), &fragment, &version_id, 0)
            .await
            .expect("part 0 metadata");
    let committed_part1 =
        super::upload::read_part_meta_any(&ObjectPathManager::new(), &fragment, &version_id, 1)
            .await
            .expect("part 1 metadata");

    let committed_part0_raws: Vec<_> = committed_part0
        .shards
        .iter()
        .filter_map(|shard| shard.raw_blob_address.clone())
        .collect();
    let committed_part1_raws: Vec<_> = committed_part1
        .shards
        .iter()
        .filter_map(|shard| shard.raw_blob_address.clone())
        .collect();
    let planned_part0_raws: Vec<_> = planned_part0
        .shards
        .iter()
        .map(|shard| shard.raw_blob_address.clone())
        .collect();
    let planned_part1_raws: Vec<_> = planned_part1
        .shards
        .iter()
        .map(|shard| shard.raw_blob_address.clone())
        .collect();

    assert_eq!(committed_part0_raws, planned_part0_raws);
    assert_eq!(committed_part1_raws, planned_part1_raws);

    let finalize_ok = finalize_upload(
        state,
        &fragment,
        &version_id,
        &session_id,
        &key,
        total_len,
        2,
    )
    .await;
    assert_eq!(finalize_ok.status(), StatusCode::OK);

    let expected = [part0, part1].concat();
    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy compact wal cache-drop test is not applicable to current raw-only Arc"]
async fn matrix_planned_part_commit_rehydrates_from_compact_wal_after_cache_drop() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/planned-wal-replay.bin".to_string();
    let part0 = vec![0x61; 9 * 1024 + 7];
    let part1 = vec![0x62; 7 * 1024 + 5];
    let total_len = (part0.len() + part1.len()) as u64;

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let (version_id, session_id) = init_upload_with_plan(
        &fragment,
        &key,
        total_len,
        2,
        None,
        Some(vec![part0.len() as u64, part1.len() as u64]),
    )
    .await;

    assert_eq!(
        upload_part(&fragment, &version_id, 0, part0.clone())
            .await
            .status(),
        StatusCode::OK
    );
    index::clear_runtime_caches_for_tests().await;

    let status_resp = get_upload_status(
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_id.clone())),
    )
    .await
    .expect("upload status after cache drop")
    .into_response();
    assert_eq!(status_resp.status(), StatusCode::OK);
    let status_json = read_response_json(status_resp).await;
    assert_eq!(status_json["uploaded_parts"][0].as_u64(), Some(0));

    assert_eq!(
        upload_part(&fragment, &version_id, 1, part1.clone())
            .await
            .status(),
        StatusCode::OK
    );
    index::clear_runtime_caches_for_tests().await;

    let finalize_ok = finalize_upload(
        state,
        &fragment,
        &version_id,
        &session_id,
        &key,
        total_len,
        2,
    )
    .await;
    assert_eq!(finalize_ok.status(), StatusCode::OK);

    let expected = [part0, part1].concat();
    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_gc_orphaned_part_metadata_frees_raw_extent() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/gc-orphan-part.bin".to_string();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let (version_id, _session_id) = init_upload(&fragment, &key, 4096, 1).await;
    upload_part(&fragment, &version_id, 0, vec![0xAB; 4096]).await;

    let part_meta =
        super::upload::read_part_meta_any(&ObjectPathManager::new(), &fragment, &version_id, 0)
            .await
            .expect("part meta");
    let raw_address = part_meta
        .shards
        .iter()
        .find_map(|shard| shard.raw_blob_address.clone())
        .expect("raw address");

    let _ = index::remove_session(&fragment, &version_id, "test-orphaned-part").await;

    let cleaned = cleanup_orphaned_upload_artifacts_for_fragment(
        &fragment,
        std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(1),
    )
    .await
    .expect("cleanup orphaned upload artifacts");
    assert_eq!(cleaned, 1);
    assert!(
        !raw_blob_address_exists(&raw_address)
            .await
            .expect("raw exists check"),
        "orphaned committed part should be reclaimed by GC helper"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_empty_trash_bulk_clears_multi_version_key_and_refunds_quota() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 128 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key = "trash/bulk.bin".to_string();
    put_object(state.clone(), &fragment, &key, vec![1u8; 1024]).await;
    put_object(state.clone(), &fragment, &key, vec![2u8; 2048]).await;
    put_object(state.clone(), &fragment, &key, vec![3u8; 4096]).await;

    let before_delete_quota = get_quota(&fragment).await.expect("quota before delete");
    assert!(before_delete_quota.storage_used >= 7168);

    let delete_response = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("delete object")
    .into_response();
    assert_eq!(delete_response.status(), StatusCode::OK);

    let trashed_before = index::list_trashed_object_versions_metadata(&fragment, &key)
        .await
        .expect("trash before empty");
    assert_eq!(trashed_before.len(), 3);

    let empty_response = empty_trash(
        State(state),
        full_claims(&fragment),
        AxumPath(fragment.clone()),
    )
    .await
    .expect("empty trash")
    .into_response();
    assert_eq!(empty_response.status(), StatusCode::NO_CONTENT);

    let trashed_after = index::list_trashed_object_versions_metadata(&fragment, &key)
        .await
        .expect("trash after empty");
    assert!(trashed_after.is_empty());
    let fragment_trash_after = index::list_fragment_trash_objects(&fragment)
        .await
        .expect("fragment trash after empty");
    assert!(fragment_trash_after.is_empty());
    let trash_versions_after = collect_fragment_versions_streamed(&fragment, true).await;
    assert!(trash_versions_after.is_empty());

    let quota_after = get_quota(&fragment).await.expect("quota after empty");
    assert_eq!(quota_after.storage_used, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_active_multipart_resume_survives_replica_index_disk_loss() {
    let _guard = TEST_LOCK.lock().await;
    run_active_multipart_resume_with_replica_index_disk_loss(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "index owner-first multipart soak; run manually with N1_INDEX_OWNER_RESUME_SOAK_ROUNDS"]
async fn matrix_index_owner_first_multipart_resume_soak() {
    let _guard = TEST_LOCK.lock().await;
    let rounds = test_env_usize("N1_INDEX_OWNER_RESUME_SOAK_ROUNDS", 5, 1, 128);
    run_active_multipart_resume_with_replica_index_disk_loss(rounds).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_upload_part_retry_size_mismatch_rejected() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/retry-size-mismatch.bin".to_string();
    let accepted = vec![0x7A; 12 * 1024 + 1];
    let mismatch = vec![0x7A; 6 * 1024 + 1];

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let (version_id, session_id) = init_upload(&fragment, &key, accepted.len() as u64, 1).await;

    assert_eq!(
        upload_part(&fragment, &version_id, 0, accepted.clone())
            .await
            .status(),
        StatusCode::OK
    );

    let mismatch_resp = upload_part(&fragment, &version_id, 0, mismatch).await;
    assert_eq!(mismatch_resp.status(), StatusCode::CONFLICT);

    let retry_same = upload_part(&fragment, &version_id, 0, accepted.clone()).await;
    assert_eq!(retry_same.status(), StatusCode::OK);

    let finalize_ok = finalize_upload(
        state,
        &fragment,
        &version_id,
        &session_id,
        &key,
        accepted.len() as u64,
        1,
    )
    .await;
    assert_eq!(finalize_ok.status(), StatusCode::OK);

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, accepted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_same_key_new_upload_finalized_while_old_unfinished() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/same-key-newer-wins.bin".to_string();
    let old_content = vec![0x41; 11 * 1024 + 1];
    let new_content = vec![0x5A; 13 * 1024 + 3];

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let (old_version_id, old_session_id) =
        init_upload(&fragment, &key, old_content.len() as u64, 1).await;
    assert_eq!(
        upload_part(&fragment, &old_version_id, 0, old_content.clone())
            .await
            .status(),
        StatusCode::OK
    );

    let (new_version_id, new_session_id) =
        init_upload(&fragment, &key, new_content.len() as u64, 1).await;
    assert_eq!(
        upload_part(&fragment, &new_version_id, 0, new_content.clone())
            .await
            .status(),
        StatusCode::OK
    );
    let finalize_new = finalize_upload(
        state.clone(),
        &fragment,
        &new_version_id,
        &new_session_id,
        &key,
        new_content.len() as u64,
        1,
    )
    .await;
    assert_eq!(finalize_new.status(), StatusCode::OK);

    let got_new_first =
        read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got_new_first, new_content);

    let finalize_old = finalize_upload(
        state,
        &fragment,
        &old_version_id,
        &old_session_id,
        &key,
        old_content.len() as u64,
        1,
    )
    .await;
    assert_eq!(finalize_old.status(), StatusCode::OK);

    let got_after_old_finalize =
        read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got_after_old_finalize, new_content);

    let latest_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("latest metadata");
    assert_eq!(latest_meta.version_id, new_version_id);
    assert_ne!(latest_meta.version_id, old_version_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_delete_recover_and_permanent_delete_flows() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/delete-flow.bin".to_string();
    let v1 = b"version-one".to_vec();
    let v2 = b"version-two-latest".to_vec();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let (ver1, sess1) = init_upload(&fragment, &key, v1.len() as u64, 1).await;
    assert_eq!(
        upload_part(&fragment, &ver1, 0, v1.clone()).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        finalize_upload(
            state.clone(),
            &fragment,
            &ver1,
            &sess1,
            &key,
            v1.len() as u64,
            1,
        )
        .await
        .status(),
        StatusCode::OK
    );

    let (ver2, sess2) = init_upload(&fragment, &key, v2.len() as u64, 1).await;
    assert_eq!(
        upload_part(&fragment, &ver2, 0, v2.clone()).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        finalize_upload(
            state.clone(),
            &fragment,
            &ver2,
            &sess2,
            &key,
            v2.len() as u64,
            1,
        )
        .await
        .status(),
        StatusCode::OK
    );

    let del_ver = delete_object_version(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), ver1.clone(), key.clone())),
    )
    .await
    .expect("delete version")
    .into_response();
    assert_eq!(del_ver.status(), StatusCode::OK);

    let rec_ver = recover_object_version(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), ver1.clone(), key.clone())),
    )
    .await
    .expect("recover version")
    .into_response();
    assert_eq!(rec_ver.status(), StatusCode::OK);

    let soft_del = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("soft delete object")
    .into_response();
    assert_eq!(soft_del.status(), StatusCode::OK);

    let rec_obj = recover_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("recover object")
    .into_response();
    assert_eq!(rec_obj.status(), StatusCode::OK);

    let perm_del = delete_object_permanently(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("permanent delete")
    .into_response();
    assert_eq!(perm_del.status(), StatusCode::NO_CONTENT);

    let req = Request::builder()
        .uri("/v4/fragment/get")
        .body(Body::empty())
        .expect("build request");
    assert!(
        get_object(
            full_claims(&fragment),
            AxumPath((fragment.clone(), key)),
            req
        )
        .await
        .is_err(),
        "object should not be retrievable after permanent delete"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_recover_object_stays_listed_in_prefix_index() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key_recover = "restore/returned.bin".to_string();
    let key_keep = "restore/keep.bin".to_string();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(
        state.clone(),
        &fragment,
        &key_recover,
        vec![0xA1; 12 * 1024],
    )
    .await;
    put_object(state.clone(), &fragment, &key_keep, vec![0xB2; 9 * 1024]).await;

    let del_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key_recover.clone())),
    )
    .await
    .expect("delete recover key")
    .into_response();
    assert_eq!(del_resp.status(), StatusCode::OK);

    let rec_resp = recover_object(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key_recover.clone())),
    )
    .await
    .expect("recover key")
    .into_response();
    assert_eq!(rec_resp.status(), StatusCode::OK);

    let listing = list_objects_json(&fragment, None, None).await;
    let keys = listing["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .filter(|key| key.starts_with("restore/"))
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    assert!(
        keys.iter().any(|k| k == &key_recover),
        "recovered key missing from flat listing: {:?}",
        keys
    );
    assert!(
        keys.iter().any(|k| k == &key_keep),
        "unaffected key missing from flat listing: {:?}",
        keys
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_raw_blob_crud_delete_recover_and_permanent_delete() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.join(format!("raw-crud-{}", Ulid::new()));
    let _raw_disks = ensure_raw_storage_config(&base, 3, 64 * 1024 * 1024).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "raw/delete-flow.bin".to_string();
    let content = (0..(80 * 1024 + 123))
        .map(|i| (i % 233) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    put_object(state.clone(), &fragment, &key, content.clone()).await;
    assert!(
        !base.join(".n1-raw-stage").exists(),
        "direct raw ingest should not stage shard files on fs storage"
    );
    let meta_before = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("raw metadata before delete");
    assert!(
        meta_before
            .parts
            .iter()
            .flat_map(|part| part.shards.iter())
            .all(|shard| shard.raw_blob_address.is_some()),
        "expected all raw blob addresses to be populated"
    );

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, content);

    let soft_del = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("soft delete raw object")
    .into_response();
    assert_eq!(soft_del.status(), StatusCode::OK);

    let rec_obj = recover_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("recover raw object")
    .into_response();
    assert_eq!(rec_obj.status(), StatusCode::OK);

    let recovered = read_object_bytes_eventually(fragment.clone(), key.clone(), 160).await;
    assert_eq!(recovered, content);

    let perm_del = delete_object_permanently(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("permanent delete raw object")
    .into_response();
    assert_eq!(perm_del.status(), StatusCode::NO_CONTENT);

    assert!(
        get_metadata(&fragment, &key, &fragment).await.is_err(),
        "metadata should be gone after permanent delete"
    );

    for shard in meta_before.parts.iter().flat_map(|part| part.shards.iter()) {
        assert!(
            open_raw_direct_io(shard).is_err(),
            "raw extent should be freed after permanent delete: {}",
            shard.file_name
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_redelete_trashed_version_preserves_latest_live_version() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/version-trash-delete.bin".to_string();
    let v1 = b"older-version".to_vec();
    let v2 = b"latest-version".to_vec();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let (ver1, sess1) = init_upload(&fragment, &key, v1.len() as u64, 1).await;
    assert_eq!(
        upload_part(&fragment, &ver1, 0, v1.clone()).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        finalize_upload(
            state.clone(),
            &fragment,
            &ver1,
            &sess1,
            &key,
            v1.len() as u64,
            1,
        )
        .await
        .status(),
        StatusCode::OK
    );

    let (ver2, sess2) = init_upload(&fragment, &key, v2.len() as u64, 1).await;
    assert_eq!(
        upload_part(&fragment, &ver2, 0, v2.clone()).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        finalize_upload(
            state.clone(),
            &fragment,
            &ver2,
            &sess2,
            &key,
            v2.len() as u64,
            1,
        )
        .await
        .status(),
        StatusCode::OK
    );

    let soft_delete = delete_object_version(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), ver1.clone(), key.clone())),
    )
    .await
    .expect("soft delete older version")
    .into_response();
    assert_eq!(soft_delete.status(), StatusCode::OK);

    let latest_after_soft_delete = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("latest metadata after soft delete");
    assert_eq!(latest_after_soft_delete.version_id, ver2);

    let permanent_delete_trashed = delete_object_version(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), ver1.clone(), key.clone())),
    )
    .await
    .expect("permanently delete trashed version")
    .into_response();
    assert_eq!(permanent_delete_trashed.status(), StatusCode::OK);

    let latest_after_redelete = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("latest metadata after permanent trashed delete");
    assert_eq!(latest_after_redelete.version_id, ver2);

    let current = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(current, v2);

    assert!(
        index::read_trashed_object_version(&fragment, &key, &ver1)
            .await
            .is_err(),
        "trashed version should be gone after second delete"
    );
    assert!(
        recover_object_version(
            State(state),
            full_claims(&fragment),
            AxumPath((fragment.clone(), ver1, key.clone())),
        )
        .await
        .is_err(),
        "permanently deleted trashed version should not be recoverable"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_raw_blob_quota_drift_reconverges_after_redistribution_gc_and_sync() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.join(format!("raw-quota-{}", Ulid::new()));
    let _raw_disks = ensure_raw_storage_config(&base, 3, 96 * 1024 * 1024).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "raw/quota-drift.bin".to_string();
    let content = (0..(96 * 1024 + 333))
        .map(|i| (i % 227) as u8)
        .collect::<Vec<u8>>();
    let content_len = content.len() as u64;

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    put_object(state.clone(), &fragment, &key, content.clone()).await;
    assert_fragment_quota_used(&fragment, content_len).await;

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("raw redistribution should preserve quota");

    assert!(
        count_raw_gc_markers(&fragment).await > 0,
        "expected raw GC markers after redistribution"
    );
    assert_fragment_quota_used(&fragment, content_len).await;

    set_storage_used(&fragment, content_len.saturating_mul(4))
        .await
        .expect("inject raw quota drift");
    assert_fragment_quota_used(&fragment, content_len.saturating_mul(4)).await;

    sync_single_fragment_quota(fragment.clone())
        .await
        .expect("sync raw quota after redistribution");
    assert_fragment_quota_used(&fragment, content_len).await;

    let delete_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("soft delete raw object")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    // Soft delete moves data to trash, so sync must keep counting it until GC empties trash.
    assert_fragment_quota_used(&fragment, content_len).await;
    set_storage_used(&fragment, 0)
        .await
        .expect("inject undercounted raw quota drift");
    assert_fragment_quota_used(&fragment, 0).await;

    sync_single_fragment_quota(fragment.clone())
        .await
        .expect("sync should count raw trash bytes");
    assert_fragment_quota_used(&fragment, content_len).await;

    let previous_gc_start = get_gc_status_api(full_claims(&fragment))
        .await
        .expect("gc status before raw quota test")
        .0
        .start_timestamp;
    let gc_resp = run_garbage_collector(State(state.clone()))
        .await
        .into_response();
    assert_eq!(gc_resp.status(), StatusCode::ACCEPTED);
    wait_for_gc_to_finish(&fragment, previous_gc_start).await;

    assert_eq!(
        count_raw_gc_markers(&fragment).await,
        0,
        "manual GC should drain raw GC markers"
    );
    assert_fragment_quota_used(&fragment, 0).await;

    set_storage_used(&fragment, content_len.saturating_mul(3))
        .await
        .expect("inject post-gc raw quota drift");
    sync_single_fragment_quota(fragment.clone())
        .await
        .expect("sync should zero raw quota after GC");
    assert_fragment_quota_used(&fragment, 0).await;

    let req = Request::builder()
        .uri("/v4/fragment/get")
        .body(Body::empty())
        .expect("build request");
    assert!(
        get_object(
            full_claims(&fragment),
            AxumPath((fragment.clone(), key.clone())),
            req
        )
        .await
        .is_err(),
        "raw object should not be readable after GC removed trash"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_list_objects_hides_internal_keys_unless_requested() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(
        state.clone(),
        &fragment,
        "videos/visible.bin",
        vec![0x11; 1024],
    )
    .await;
    put_object(state, &fragment, "videos/.n1fsd_dir", vec![0x22]).await;

    let listing_default = list_objects_json(&fragment, None, None).await;
    let default_keys = listing_default["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .filter(|key| key.starts_with("videos/"))
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    assert!(
        default_keys.iter().any(|k| k == "videos/visible.bin"),
        "default listing missing visible object: {:?}",
        default_keys
    );
    assert!(
        default_keys.iter().all(|k| k != "videos/.n1fsd_dir"),
        "default listing should hide internal key: {:?}",
        default_keys
    );

    let listing_internal = list_objects_json_with_internal(&fragment, None, None, true).await;
    let internal_keys = listing_internal["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .filter(|key| key.starts_with("videos/"))
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    assert!(
        internal_keys.iter().any(|k| k == "videos/.n1fsd_dir"),
        "include_internal listing should include marker: {:?}",
        internal_keys
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_redistribution_with_one_disk_down() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/disk-down.bin".to_string();
    let content = (0..(32 * 1024 + 17))
        .map(|i| (i % 241) as u8)
        .collect::<Vec<u8>>();

    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let (version_id, session_id) = init_upload(&fragment, &key, content.len() as u64, 1).await;
    assert_eq!(
        upload_part(&fragment, &version_id, 0, content.clone())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        finalize_upload(
            state,
            &fragment,
            &version_id,
            &session_id,
            &key,
            content.len() as u64,
            1,
        )
        .await
        .status(),
        StatusCode::OK
    );

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let blob3 = base.join("blob_3.raw");
    let blob3_down = base.join("blob_3.raw.down");
    if fs::try_exists(&blob3).await.unwrap_or(false) {
        fs::rename(&blob3, &blob3_down)
            .await
            .expect("simulate disk down");
    }

    let redist_res = run_redistribution_job(false, Arc::new(WsConnectionManager::new())).await;

    if fs::try_exists(&blob3_down).await.unwrap_or(false) {
        let _ = fs::rename(&blob3_down, &blob3).await;
    }
    redist_res.expect("redistribution should succeed with one disk down");

    let got = read_object_bytes(full_claims(&fragment), fragment, key).await;
    assert_eq!(got, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_scoped_redistribution_only_touches_requested_fragment() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(262_144, Ordering::Relaxed);

    let fragment_a = {
        let digest = Sha256::digest(format!("fragment-a-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let fragment_b = {
        let digest = Sha256::digest(format!("fragment-b-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key_a = "scope/redist-a.bin".to_string();
    let key_b = "scope/redist-b.bin".to_string();
    let content_a = (0..(20 * 1024 + 11))
        .map(|i| ((i * 3) % 251) as u8)
        .collect::<Vec<u8>>();
    let content_b = (0..(22 * 1024 + 17))
        .map(|i| ((i * 5) % 251) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment_a).await;
    setup_fragment(&fragment_b).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(state.clone(), &fragment_a, &key_a, content_a.clone()).await;
    put_object(state, &fragment_b, &key_b, content_b.clone()).await;

    let before_a = get_metadata(&fragment_a, &key_a, &fragment_a)
        .await
        .expect("metadata for fragment a before redistribution");
    let before_b = get_metadata(&fragment_b, &key_b, &fragment_b)
        .await
        .expect("metadata for fragment b before redistribution");
    assert_eq!(before_a.parts[0].shards.len(), 1);
    assert_eq!(before_b.parts[0].shards.len(), 1);
    assert_eq!(before_a.stripe_size_bytes, 262_144);
    assert_eq!(before_b.stripe_size_bytes, 262_144);

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    run_redistribution_job_with_options(
        false,
        RedistributionRunOptions {
            fast_mode: false,
            durability_mode: RedistributionDurabilityMode::Auto,
            execution_mode: RedistributionExecutionMode::Availability,
            object_concurrency: 4,
            status_batch: 32,
            fragment_filter: Some(fragment_a.clone()),
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("fragment-scoped redistribution");

    let after_a = get_metadata(&fragment_a, &key_a, &fragment_a)
        .await
        .expect("metadata for fragment a after redistribution");
    let after_b = get_metadata(&fragment_b, &key_b, &fragment_b)
        .await
        .expect("metadata for fragment b after redistribution");

    assert_eq!(
        after_a.parts[0].shards.len(),
        3,
        "target fragment should be redistributed to the new shard layout"
    );
    assert_eq!(after_a.stripe_size_bytes, 4096);
    assert_eq!(
        after_b.parts[0].shards.len(),
        1,
        "untargeted fragment should keep its old shard layout"
    );
    assert_eq!(
        after_b.stripe_size_bytes, 262_144,
        "untargeted fragment should keep its old stripe size"
    );

    let got_a = read_object_bytes(full_claims(&fragment_a), fragment_a, key_a).await;
    let got_b = read_object_bytes(full_claims(&fragment_b), fragment_b, key_b).await;
    assert_eq!(got_a, content_a);
    assert_eq!(got_b, content_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_stream_and_metadata_read_paths() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/stream.bin".to_string();
    let content = (0..(24 * 1024 + 19))
        .map(|i| (i % 197) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let (version_id, session_id) = init_upload(&fragment, &key, content.len() as u64, 1).await;
    assert_eq!(
        upload_part(&fragment, &version_id, 0, content.clone())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        finalize_upload(
            state,
            &fragment,
            &version_id,
            &session_id,
            &key,
            content.len() as u64,
            1,
        )
        .await
        .status(),
        StatusCode::OK
    );

    let meta_resp = get_object_metadata(
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("metadata response")
    .into_response();
    assert_eq!(meta_resp.status(), StatusCode::OK);

    let full_stream_resp = stream_object(
        full_claims(&fragment),
        HeaderMap::new(),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("stream full")
    .into_response();
    assert_eq!(full_stream_resp.status(), StatusCode::OK);
    let full_bytes = to_bytes(full_stream_resp.into_body(), usize::MAX)
        .await
        .expect("full stream body");
    assert_eq!(full_bytes.to_vec(), content);

    let mut range_headers = HeaderMap::new();
    range_headers.insert("range", HeaderValue::from_static("bytes=10-99"));
    let range_resp = stream_object(
        full_claims(&fragment),
        range_headers,
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("range stream")
    .into_response();
    assert_eq!(range_resp.status(), StatusCode::PARTIAL_CONTENT);
    let range_bytes = to_bytes(range_resp.into_body(), usize::MAX)
        .await
        .expect("range stream body");
    assert_eq!(range_bytes.to_vec(), content[10..100].to_vec());

    let mut invalid_range_headers = HeaderMap::new();
    invalid_range_headers.insert("range", HeaderValue::from_static("bytes=999999-1000000"));
    let invalid_range_resp = stream_object(
        full_claims(&fragment),
        invalid_range_headers,
        AxumPath((fragment, key)),
    )
    .await
    .expect("invalid range fallback")
    .into_response();
    assert_eq!(invalid_range_resp.status(), StatusCode::OK);
    let invalid_range_bytes = to_bytes(invalid_range_resp.into_body(), usize::MAX)
        .await
        .expect("invalid range body");
    assert_eq!(invalid_range_bytes.to_vec(), content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_bitrot_parity_then_repair() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/bitrot-parity.bin".to_string();
    let content = (0..(96 * 1024 + 307))
        .map(|i| (i % 181) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let (version_id, session_id) = init_upload(&fragment, &key, content.len() as u64, 1).await;
    assert_eq!(
        upload_part(&fragment, &version_id, 0, content.clone())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        finalize_upload(
            state,
            &fragment,
            &version_id,
            &session_id,
            &key,
            content.len() as u64,
            1,
        )
        .await
        .status(),
        StatusCode::OK
    );

    let meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata");
    let parity_shard = meta.parts[0]
        .shards
        .iter()
        .find(|s| s.is_parity)
        .expect("parity shard")
        .clone();

    corrupt_shard_bytes(&parity_shard, 0, &vec![0x6E; 4096]).await;
    assert!(
        !shard_checksum_ok(&parity_shard).await,
        "parity checksum should fail before repair"
    );

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("repair after parity bitrot");

    assert!(
        shard_checksum_ok(&parity_shard).await,
        "parity checksum should pass after repair"
    );

    let got = read_object_bytes(full_claims(&fragment), fragment, key).await;
    assert_eq!(got, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_scoped_repair_only_touches_requested_fragment() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment_a = {
        let digest = Sha256::digest(format!("repair-a-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let fragment_b = {
        let digest = Sha256::digest(format!("repair-b-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key_a = "scope/repair-a.bin".to_string();
    let key_b = "scope/repair-b.bin".to_string();
    let content_a = (0..(64 * 1024 + 9))
        .map(|i| ((i * 7) % 251) as u8)
        .collect::<Vec<u8>>();
    let content_b = (0..(68 * 1024 + 13))
        .map(|i| ((i * 11) % 251) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment_a).await;
    setup_fragment(&fragment_b).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(state.clone(), &fragment_a, &key_a, content_a.clone()).await;
    put_object(state, &fragment_b, &key_b, content_b.clone()).await;

    let meta_a = get_metadata(&fragment_a, &key_a, &fragment_a)
        .await
        .expect("metadata for fragment a");
    let meta_b = get_metadata(&fragment_b, &key_b, &fragment_b)
        .await
        .expect("metadata for fragment b");
    let parity_a = meta_a.parts[0]
        .shards
        .iter()
        .find(|shard| shard.is_parity)
        .expect("parity shard for fragment a")
        .clone();
    let parity_b = meta_b.parts[0]
        .shards
        .iter()
        .find(|shard| shard.is_parity)
        .expect("parity shard for fragment b")
        .clone();

    corrupt_shard_bytes(&parity_a, 0, &vec![0xA3; 4096]).await;
    corrupt_shard_bytes(&parity_b, 0, &vec![0xB7; 4096]).await;
    assert!(
        !shard_checksum_ok(&parity_a).await,
        "target fragment should start corrupted"
    );
    assert!(
        !shard_checksum_ok(&parity_b).await,
        "untargeted fragment should start corrupted"
    );

    run_repair_job_with_options(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        RepairRunOptions {
            fast_mode: false,
            access_mode: MaintenanceAccessMode::Quiesced,
            fragment_filter: Some(fragment_a.clone()),
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("fragment-scoped repair");

    assert!(
        shard_checksum_ok(&parity_a).await,
        "target fragment should be repaired"
    );
    assert!(
        !shard_checksum_ok(&parity_b).await,
        "untargeted fragment should remain unrepaired"
    );

    let got_a = read_object_bytes(full_claims(&fragment_a), fragment_a, key_a).await;
    let got_b = read_object_bytes(full_claims(&fragment_b), fragment_b, key_b).await;
    assert_eq!(got_a, content_a);
    assert_eq!(got_b, content_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy filesystem quota/session mirror repair path is not applicable to current raw-only Arc"]
async fn matrix_repair_synchronizes_all_index_disks() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let k1 = "idx-sync/a.bin".to_string();
    let k2 = "idx-sync/b.bin".to_string();
    let c1 = (0..(24 * 1024 + 5))
        .map(|i| (i % 241) as u8)
        .collect::<Vec<u8>>();
    let c2 = (0..(25 * 1024 + 9))
        .map(|i| (i % 239) as u8)
        .collect::<Vec<u8>>();

    let _v1 = put_object(state.clone(), &fragment, &k1, c1.clone()).await;
    let _v2 = put_object(state.clone(), &fragment, &k2, c2.clone()).await;

    let pm = ObjectPathManager::new();
    let quota_paths = pm.get_all_quota_paths(&fragment);
    assert!(quota_paths.len() >= 2);

    fs::remove_file(&quota_paths[0])
        .await
        .expect("delete one quota mirror");

    let (pending_vid, _pending_sid) = init_upload(&fragment, "idx-sync/pending.bin", 8192, 1).await;
    let pending_chunk = vec![0xAB; 8192];
    let put_pending = upload_part(&fragment, &pending_vid, 0, pending_chunk).await;
    assert_eq!(put_pending.status(), StatusCode::OK);

    let session_paths =
        index::get_session_state_paths(&fragment, &pending_vid).expect("session state paths");
    assert!(
        session_paths.len() >= 2,
        "expected at least two session state replicas"
    );
    fs::write(&session_paths[0], b"corrupt-session-state")
        .await
        .expect("corrupt one session state replica");

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Availability,
            meta_concurrency: 4,
            shard_repair_concurrency: 2,
            meta_batch_size: 256,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("repair should reconcile index mirrors");

    assert_all_index_disks_synchronized(&fragment).await;

    let got1 = read_object_bytes(full_claims(&fragment), fragment.clone(), k1).await;
    let got2 = read_object_bytes(full_claims(&fragment), fragment.clone(), k2).await;
    assert_eq!(got1, c1);
    assert_eq!(got2, c2);

    let pending_status_resp = get_upload_status(
        full_claims(&fragment),
        AxumPath((fragment.clone(), pending_vid.clone())),
    )
    .await
    .expect("pending status after mirror repair")
    .into_response();
    assert_eq!(pending_status_resp.status(), StatusCode::OK);
    let pending_status_json = read_response_json(pending_status_resp).await;
    assert_eq!(pending_status_json["uploaded_parts"][0].as_u64(), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_raw_blob_redistribution_repair_recovered_read_and_gc_markers() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.join(format!("raw-maint-{}", Ulid::new()));
    let _raw_disks = ensure_raw_storage_config(&base, 3, 96 * 1024 * 1024).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "raw/redistribute-repair.bin".to_string();
    let content = (0..(96 * 1024 + 777))
        .map(|i| (i % 211) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(state.clone(), &fragment, &key, content.clone()).await;

    let initial_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("initial raw metadata");
    assert_eq!(initial_meta.parts[0].shards.len(), 1);
    assert!(
        initial_meta.parts[0].shards[0].raw_blob_address.is_some(),
        "expected initial object to live on raw extent"
    );

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("raw redistribution");

    let marker_count = count_raw_gc_markers(&fragment).await;
    assert!(
        marker_count > 0,
        "expected raw GC markers after redistribution, got {}",
        marker_count
    );

    let redistributed_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("redistributed raw metadata");
    assert_eq!(redistributed_meta.parts[0].shards.len(), 3);
    assert!(
        redistributed_meta
            .parts
            .iter()
            .flat_map(|part| part.shards.iter())
            .all(|shard| shard.raw_blob_address.is_some()),
        "expected redistributed shards to remain raw-backed"
    );

    let damaged_shard = redistributed_meta.parts[0]
        .shards
        .iter()
        .find(|shard| !shard.is_parity)
        .expect("raw data shard")
        .clone();
    let overwrite = vec![0x5A; std::cmp::min(4096, damaged_shard.size_bytes as usize).max(1)];
    write_shard_range(&damaged_shard, 0, &overwrite)
        .await
        .expect("corrupt raw shard");
    assert!(
        !verify_shard_checksum(&damaged_shard)
            .await
            .expect("verify corrupted raw shard"),
        "raw shard checksum should fail after corruption"
    );

    let recovered = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(
        recovered, content,
        "recovered read mismatch after raw corruption"
    );

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("raw repair job");

    assert!(
        verify_shard_checksum(&damaged_shard)
            .await
            .expect("verify repaired raw shard"),
        "raw shard checksum should pass after repair"
    );

    let previous_gc_start = get_gc_status_api(full_claims(&fragment))
        .await
        .expect("gc status before run")
        .0
        .start_timestamp;
    let gc_resp = run_garbage_collector(State(state.clone()))
        .await
        .into_response();
    assert_eq!(gc_resp.status(), StatusCode::ACCEPTED);
    wait_for_gc_to_finish(&fragment, previous_gc_start).await;
    assert_eq!(count_raw_gc_markers(&fragment).await, 0);

    let final_read = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(final_read, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_raw_blob_background_repair_relocates_stale_device_id() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.join(format!("raw-stale-device-{}", Ulid::new()));
    let _raw_disks = ensure_raw_storage_config(&base, 3, 96 * 1024 * 1024).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "raw/stale-device-id.bin".to_string();
    let content = (0..(72 * 1024 + 333))
        .map(|i| (i % 197) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(state, &fragment, &key, content.clone()).await;

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("raw redistribution");

    let redistributed_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("redistributed raw metadata");
    let target_idx = redistributed_meta.parts[0]
        .shards
        .iter()
        .position(|shard| !shard.is_parity)
        .expect("raw data shard position");
    let original_shard = redistributed_meta.parts[0].shards[target_idx].clone();
    let original_raw = original_shard
        .raw_blob_address
        .clone()
        .expect("raw blob address");

    let stale_device_id = format!("stale-{}", Ulid::new());
    let mut stale_meta = redistributed_meta.clone();
    let stale_shard = &mut stale_meta.parts[0].shards[target_idx];
    stale_shard
        .raw_blob_address
        .as_mut()
        .expect("stale raw")
        .device_id = stale_device_id.clone();
    stale_shard.absolute_path = format!("raw://{}/{}", stale_device_id, original_raw.allocation_id);

    let stale_bytes =
        rmp_serde::to_vec(&stale_meta).expect("serialize stale metadata with device mismatch");
    let _: ObjectMetadata =
        rmp_serde::from_slice(&stale_bytes).expect("decode stale metadata with device mismatch");
    index::upsert_object_version(&stale_meta)
        .await
        .expect("persist stale metadata with device mismatch");

    let recovered = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(
        recovered, content,
        "recovered read mismatch after stale raw device id"
    );

    let repaired_meta = {
        let mut latest = None;
        for _ in 0..60usize {
            let current = get_metadata(&fragment, &key, &fragment)
                .await
                .expect("metadata after stale-device read");
            let repaired_shard = &current.parts[0].shards[target_idx];
            if let Some(raw) = repaired_shard.raw_blob_address.as_ref() {
                if raw.device_id == original_raw.device_id
                    && raw.allocation_id != original_raw.allocation_id
                {
                    latest = Some(current);
                    break;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        latest.expect("background repair should rewrite stale raw device metadata")
    };

    let repaired_shard = repaired_meta.parts[0].shards[target_idx].clone();
    assert!(
        verify_shard_checksum(&repaired_shard)
            .await
            .expect("verify relocated raw shard"),
        "relocated raw shard checksum should pass"
    );

    let final_read = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(final_read, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_raw_blob_stale_reader_after_relocation_is_idempotent() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.join(format!("raw-stale-reader-{}", Ulid::new()));
    let _raw_disks = ensure_raw_storage_config(&base, 3, 96 * 1024 * 1024).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "raw/stale-reader-idempotent.bin".to_string();
    let content = (0..(96 * 1024 + 777))
        .map(|i| (i % 211) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(state, &fragment, &key, content.clone()).await;

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("raw redistribution");

    let redistributed_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("redistributed raw metadata");
    let target_pos = redistributed_meta.parts[0]
        .shards
        .iter()
        .position(|shard| !shard.is_parity)
        .expect("raw data shard position");
    let target_shard_index = redistributed_meta.parts[0].shards[target_pos].shard_index;
    let original_raw = redistributed_meta.parts[0].shards[target_pos]
        .raw_blob_address
        .clone()
        .expect("raw blob address");

    let stale_device_id = format!("stale-{}", Ulid::new());
    let mut stale_meta = redistributed_meta.clone();
    let stale_shard = &mut stale_meta.parts[0].shards[target_pos];
    stale_shard
        .raw_blob_address
        .as_mut()
        .expect("stale raw")
        .device_id = stale_device_id.clone();
    stale_shard.absolute_path = format!("raw://{}/{}", stale_device_id, original_raw.allocation_id);

    let stale_bytes =
        rmp_serde::to_vec(&stale_meta).expect("serialize stale metadata with device mismatch");
    let _: ObjectMetadata =
        rmp_serde::from_slice(&stale_bytes).expect("decode stale metadata with device mismatch");
    index::upsert_object_version(&stale_meta)
        .await
        .expect("persist stale metadata with device mismatch");

    let recovered = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(
        recovered, content,
        "recovered read mismatch after stale raw device id"
    );

    let repaired_meta = {
        let mut latest = None;
        for _ in 0..60usize {
            let current = get_metadata(&fragment, &key, &fragment)
                .await
                .expect("metadata after stale-device read");
            let repaired_shard = &current.parts[0].shards[target_pos];
            if let Some(raw) = repaired_shard.raw_blob_address.as_ref() {
                if raw.device_id == original_raw.device_id
                    && raw.allocation_id != original_raw.allocation_id
                {
                    latest = Some(current);
                    break;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        latest.expect("background repair should rewrite stale raw device metadata")
    };

    let relocated_raw = repaired_meta.parts[0].shards[target_pos]
        .raw_blob_address
        .clone()
        .expect("relocated raw blob address");
    let stale_part = Arc::new(stale_meta.parts[0].clone());
    let repair_context = ShardRepairContext::new(
        fragment.clone(),
        key.clone(),
        stale_meta.version_id.clone(),
        false,
    );

    repair_entire_shard(
        stale_part,
        target_shard_index,
        4096,
        Some(repair_context),
        RepairLogMode::Verbose,
    )
    .await
    .expect("stale second repair should observe existing relocation");

    let current_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata after stale second repair");
    let current_raw = current_meta.parts[0].shards[target_pos]
        .raw_blob_address
        .clone()
        .expect("current raw blob address");
    assert_eq!(
        current_raw, relocated_raw,
        "stale second repair should not replace the already relocated raw shard"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_raw_blob_manual_repair_relocates_stale_device_id() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.join(format!("raw-manual-repair-stale-{}", Ulid::new()));
    let _raw_disks = ensure_raw_storage_config(&base, 3, 96 * 1024 * 1024).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "raw/manual-repair-stale-device-id.bin".to_string();
    let content = (0..(88 * 1024 + 99))
        .map(|i| (i % 193) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(state, &fragment, &key, content.clone()).await;

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("raw redistribution");

    let redistributed_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("redistributed raw metadata");
    let target_pos = redistributed_meta.parts[0]
        .shards
        .iter()
        .position(|shard| !shard.is_parity)
        .expect("raw data shard position");
    let original_raw = redistributed_meta.parts[0].shards[target_pos]
        .raw_blob_address
        .clone()
        .expect("raw blob address");

    let stale_device_id = format!("stale-{}", Ulid::new());
    let mut stale_meta = redistributed_meta.clone();
    let stale_shard = &mut stale_meta.parts[0].shards[target_pos];
    stale_shard
        .raw_blob_address
        .as_mut()
        .expect("stale raw")
        .device_id = stale_device_id.clone();
    stale_shard.absolute_path = format!("raw://{}/{}", stale_device_id, original_raw.allocation_id);

    let stale_bytes =
        rmp_serde::to_vec(&stale_meta).expect("serialize stale metadata with device mismatch");
    let _: ObjectMetadata =
        rmp_serde::from_slice(&stale_bytes).expect("decode stale metadata with device mismatch");
    index::upsert_object_version(&stale_meta)
        .await
        .expect("persist stale metadata with device mismatch");

    run_repair_job_with_options(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 64,
        },
        RepairRunOptions {
            fast_mode: false,
            access_mode: MaintenanceAccessMode::Quiesced,
            fragment_filter: None,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("manual repair should relocate stale raw shard");

    let repaired_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata after manual repair");
    let repaired_shard = repaired_meta.parts[0].shards[target_pos].clone();
    let repaired_raw = repaired_shard
        .raw_blob_address
        .clone()
        .expect("repaired raw blob address");
    assert_eq!(
        repaired_raw.device_id, original_raw.device_id,
        "manual repair should rewrite stale raw device id back to a configured device"
    );
    assert_ne!(
        repaired_raw.allocation_id, original_raw.allocation_id,
        "manual repair should allocate a fresh raw location after relocation"
    );
    assert!(
        verify_shard_checksum(&repaired_shard)
            .await
            .expect("verify manually relocated raw shard"),
        "manual repair should leave a valid relocated raw shard"
    );

    let final_read = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(final_read, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_raw_blob_manual_availability_repair_relocates_stale_device_id() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.join(format!("raw-manual-avail-stale-{}", Ulid::new()));
    let _raw_disks = ensure_raw_storage_config(&base, 3, 96 * 1024 * 1024).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "raw/manual-availability-repair-stale-device-id.bin".to_string();
    let content = (0..(84 * 1024 + 321))
        .map(|i| (i % 181) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(state, &fragment, &key, content.clone()).await;

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("raw redistribution");

    let redistributed_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("redistributed raw metadata");
    let target_pos = redistributed_meta.parts[0]
        .shards
        .iter()
        .position(|shard| !shard.is_parity)
        .expect("raw data shard position");
    let original_raw = redistributed_meta.parts[0].shards[target_pos]
        .raw_blob_address
        .clone()
        .expect("raw blob address");

    let stale_device_id = format!("stale-{}", Ulid::new());
    let mut stale_meta = redistributed_meta.clone();
    let stale_shard = &mut stale_meta.parts[0].shards[target_pos];
    stale_shard
        .raw_blob_address
        .as_mut()
        .expect("stale raw")
        .device_id = stale_device_id.clone();
    stale_shard.absolute_path = format!("raw://{}/{}", stale_device_id, original_raw.allocation_id);

    let stale_bytes =
        rmp_serde::to_vec(&stale_meta).expect("serialize stale metadata with device mismatch");
    let _: ObjectMetadata =
        rmp_serde::from_slice(&stale_bytes).expect("decode stale metadata with device mismatch");
    index::upsert_object_version(&stale_meta)
        .await
        .expect("persist stale metadata with device mismatch");

    run_repair_job_with_options(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Availability,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 64,
        },
        RepairRunOptions {
            fast_mode: false,
            access_mode: MaintenanceAccessMode::Availability,
            fragment_filter: None,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("manual availability repair should relocate stale raw shard");

    let repaired_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata after manual availability repair");
    let repaired_shard = repaired_meta.parts[0].shards[target_pos].clone();
    let repaired_raw = repaired_shard
        .raw_blob_address
        .clone()
        .expect("repaired raw blob address");
    assert_eq!(
        repaired_raw.device_id, original_raw.device_id,
        "availability repair should rewrite stale raw device id back to a configured device"
    );
    assert_ne!(
        repaired_raw.allocation_id, original_raw.allocation_id,
        "availability repair should allocate a fresh raw location after relocation"
    );
    assert!(
        verify_shard_checksum(&repaired_shard)
            .await
            .expect("verify availability relocated raw shard"),
        "availability repair should leave a valid relocated raw shard"
    );

    let final_read = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(final_read, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy mirrored marker repair path is not applicable to current raw-only Arc"]
async fn matrix_repair_restores_delete_marker_mirror() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key = "idx-sync/delete-marker.bin".to_string();
    let body = (0..(20 * 1024 + 17))
        .map(|i| ((i * 5) % 251) as u8)
        .collect::<Vec<u8>>();
    put_object(state.clone(), &fragment, &key, body).await;

    let del_resp = delete_object(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("soft delete object")
    .into_response();
    assert_eq!(del_resp.status(), StatusCode::OK);

    let marker_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("latest metadata after delete");
    assert!(
        marker_meta.is_delete_marker,
        "latest metadata should be a delete marker"
    );

    let marker_paths =
        index::get_object_version_paths(&fragment, &key, &marker_meta.version_id, false)
            .expect("delete marker paths");
    assert!(
        marker_paths.len() >= 2,
        "expected mirrored marker metadata paths"
    );

    fs::remove_file(&marker_paths[0])
        .await
        .expect("remove one delete marker replica");
    assert!(
        !fs::try_exists(&marker_paths[0]).await.unwrap_or(false),
        "marker precondition: one replica should be missing"
    );

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Availability,
            meta_concurrency: 4,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("repair should restore delete marker metadata mirror");

    assert!(
        fs::try_exists(&marker_paths[0]).await.unwrap_or(false),
        "repair should restore missing delete marker replica"
    );

    let restored = fs::read(&marker_paths[0])
        .await
        .expect("read restored marker replica");
    let canonical = fs::read(&marker_paths[1])
        .await
        .expect("read canonical marker replica");
    assert_eq!(restored, canonical, "marker replicas should match");

    assert_all_index_disks_synchronized(&fragment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_ha_repair_reconverges_index_after_index_disk_down() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let seed_key_a = "ha-index/seed-a.bin".to_string();
    let seed_key_b = "ha-index/seed-b.bin".to_string();
    let seed_bytes_a = (0..(28 * 1024 + 31))
        .map(|i| (i % 233) as u8)
        .collect::<Vec<u8>>();
    let seed_bytes_b = (0..(30 * 1024 + 7))
        .map(|i| ((i * 7) % 251) as u8)
        .collect::<Vec<u8>>();
    put_object(state.clone(), &fragment, &seed_key_a, seed_bytes_a.clone()).await;
    put_object(state.clone(), &fragment, &seed_key_b, seed_bytes_b.clone()).await;

    let down_src = ObjectPathManager::new()
        .get_all_index_roots()
        .into_iter()
        .nth(1)
        .expect("second index root");
    let down_dst = down_src.with_file_name(format!(
        "{}.down",
        down_src
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("index-root")
    ));
    if fs::try_exists(&down_dst).await.unwrap_or(false) {
        match fs::metadata(&down_dst).await {
            Ok(meta) if meta.is_dir() => {
                let _ = fs::remove_dir_all(&down_dst).await;
            }
            Ok(_) => {
                let _ = fs::remove_file(&down_dst).await;
            }
            Err(_) => {}
        }
    }
    fs::rename(&down_src, &down_dst)
        .await
        .expect("simulate index disk down");
    fs::write(&down_src, b"n1-index-disk-down-blocker")
        .await
        .expect("create index down blocker file");

    // Keep serving CRUD-read workload while one index mirror is down.
    for _ in 0..8 {
        let got_a = read_object_bytes_eventually(fragment.clone(), seed_key_a.clone(), 24).await;
        let got_b = read_object_bytes_eventually(fragment.clone(), seed_key_b.clone(), 24).await;
        assert_eq!(got_a, seed_bytes_a);
        assert_eq!(got_b, seed_bytes_b);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("redistribution should tolerate one index disk down");

    fs::remove_file(&down_src)
        .await
        .expect("remove index down blocker file");
    fs::rename(&down_dst, &down_src)
        .await
        .expect("restore index disk");

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 8,
            shard_repair_concurrency: 2,
            meta_batch_size: 512,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("repair after index disk return");

    assert_all_index_disks_synchronized(&fragment).await;

    let got_a = read_object_bytes(full_claims(&fragment), fragment.clone(), seed_key_a).await;
    let got_b = read_object_bytes(full_claims(&fragment), fragment.clone(), seed_key_b).await;
    assert_eq!(got_a, seed_bytes_a);
    assert_eq!(got_b, seed_bytes_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy mirrored-index corruption path is not applicable to current raw-only Arc"]
async fn matrix_redistribution_read_falls_back_when_primary_index_copy_is_corrupt() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key = "ha-index/fallback-after-redist.bin".to_string();
    let content = (0..(40 * 1024 + 73))
        .map(|i| ((i * 13) % 251) as u8)
        .collect::<Vec<u8>>();
    let version_id = put_object(state, &fragment, &key, content.clone()).await;

    let down_src = ObjectPathManager::new()
        .get_all_index_roots()
        .into_iter()
        .next()
        .expect("first index root");
    let down_dst = down_src.with_file_name(format!(
        "{}.down",
        down_src
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("index-root")
    ));
    if fs::try_exists(&down_dst).await.unwrap_or(false) {
        match fs::metadata(&down_dst).await {
            Ok(meta) if meta.is_dir() => {
                let _ = fs::remove_dir_all(&down_dst).await;
            }
            Ok(_) => {
                let _ = fs::remove_file(&down_dst).await;
            }
            Err(_) => {}
        }
    }
    fs::rename(&down_src, &down_dst)
        .await
        .expect("simulate primary index disk down");
    fs::write(&down_src, b"n1-index-disk-down-blocker")
        .await
        .expect("create index down blocker file");

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(262144, Ordering::Relaxed);

    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("redistribution should complete while primary index disk is down");

    fs::remove_file(&down_src)
        .await
        .expect("remove index down blocker file");
    fs::rename(&down_dst, &down_src)
        .await
        .expect("restore primary index disk");

    let meta_paths = index::get_object_version_paths(&fragment, &key, &version_id, false)
        .expect("index metadata paths");
    let mut current_meta = None;
    let mut current_rank = 0usize;
    for path in &meta_paths {
        let Ok(bytes) = fs::read(path).await else {
            continue;
        };
        let Ok(candidate) =
            rmp_serde::from_slice::<crate::n1_core::disk_manager::ObjectMetadata>(&bytes)
        else {
            continue;
        };
        let candidate_rank = candidate.parts.iter().map(|part| part.shards.len()).sum();
        if current_meta.is_none() || candidate_rank > current_rank {
            current_rank = candidate_rank;
            current_meta = Some(candidate);
        }
    }
    let current_meta = current_meta.expect("metadata after redistribution");
    index::upsert_object_version_with_replica_mode(
        &current_meta,
        index::IndexReplicaWriteMode::SyncBestEffort,
    )
    .await
    .expect("restore index replicas after disk return");

    assert!(meta_paths.len() >= 2, "expected mirrored index paths");
    assert!(
        fs::try_exists(&meta_paths[1]).await.unwrap_or(false),
        "secondary index copy should exist after redistribution"
    );

    fs::write(&meta_paths[0], b"n1-corrupt-metadata-copy")
        .await
        .expect("corrupt primary index copy");

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, content);

    let meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata should fall back to healthy mirror");
    assert_eq!(
        meta.parts[0].shards.iter().filter(|s| !s.is_parity).count(),
        1
    );
    assert_eq!(
        meta.parts[0].shards.iter().filter(|s| s.is_parity).count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_ha_crud_during_redistribution_and_repair() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let mut expected: HashMap<String, Vec<u8>> = HashMap::new();
    for i in 0..6u8 {
        let key = format!("ha/base-{}.bin", i);
        let content = (0..(20 * 1024 + (i as usize * 31)))
            .map(|n| ((n + i as usize) % 251) as u8)
            .collect::<Vec<u8>>();
        put_object(state.clone(), &fragment, &key, content.clone()).await;
        expected.insert(key, content);
    }

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    let redist_task = tokio::spawn(run_redistribution_job(
        false,
        Arc::new(WsConnectionManager::new()),
    ));

    let hot_key = "ha/base-0.bin".to_string();
    for _ in 0..8 {
        let got = read_object_bytes_eventually(fragment.clone(), hot_key.clone(), 24).await;
        assert_eq!(
            got,
            expected.get(&hot_key).expect("hot key expected").clone()
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let live_key = "ha/new-during-redist.bin".to_string();
    let live_content = (0..(28 * 1024 + 7))
        .map(|i| (i % 199) as u8)
        .collect::<Vec<u8>>();
    put_object(state.clone(), &fragment, &live_key, live_content.clone()).await;
    expected.insert(live_key.clone(), live_content.clone());

    let del_key = "ha/base-1.bin".to_string();
    let del_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), del_key.clone())),
    )
    .await
    .expect("delete during redistribution")
    .into_response();
    assert_eq!(del_resp.status(), StatusCode::OK);

    let rec_resp = recover_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), del_key.clone())),
    )
    .await
    .expect("recover during redistribution")
    .into_response();
    assert_eq!(rec_resp.status(), StatusCode::OK);

    redist_task
        .await
        .expect("redistribution task join")
        .expect("redistribution should succeed");

    for (key, bytes) in &expected {
        let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
        assert_eq!(got, *bytes, "post-redistribution mismatch for {}", key);
    }

    let meta = get_metadata(&fragment, &hot_key, &fragment)
        .await
        .expect("metadata after redistribution");
    let target_shard = meta.parts[0]
        .shards
        .iter()
        .find(|s| !s.is_parity)
        .expect("data shard")
        .clone();

    corrupt_shard_bytes(&target_shard, 0, &vec![0x3C; 4096]).await;

    let repair_task = tokio::spawn(run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    ));

    for _ in 0..8 {
        let got = read_object_bytes_eventually(fragment.clone(), hot_key.clone(), 24).await;
        assert_eq!(
            got,
            expected.get(&hot_key).expect("hot key expected").clone()
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let renamed_live_key = "ha/new-during-repair.bin".to_string();
    let rename_resp = rename_object(
        State(state),
        full_claims(&fragment),
        AxumJson(RenameObjectRequest {
            source_fragment: fragment.clone(),
            source_key: live_key.clone(),
            destination_fragment: fragment.clone(),
            destination_key: renamed_live_key.clone(),
        }),
    )
    .await
    .expect("rename during repair")
    .into_response();
    assert_eq!(rename_resp.status(), StatusCode::OK);
    let moved = expected.remove(&live_key).expect("old key should exist");
    expected.insert(renamed_live_key.clone(), moved);

    repair_task
        .await
        .expect("repair task join")
        .expect("repair should succeed");

    assert!(
        shard_checksum_ok(&target_shard).await,
        "repair should restore corrupted shard checksum"
    );

    for (key, bytes) in &expected {
        let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
        assert_eq!(got, *bytes, "post-repair mismatch for {}", key);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_ha_redistribution_permanent_delete_no_index_desync() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let mut expected: HashMap<String, Vec<u8>> = HashMap::new();
    for i in 0..10usize {
        let key = format!("ha-redist-del/base-{}.bin", i);
        let content = (0..(16 * 1024 + i * 59))
            .map(|n| ((n * 5 + i) % 251) as u8)
            .collect::<Vec<u8>>();
        put_object(state.clone(), &fragment, &key, content.clone()).await;
        expected.insert(key, content);
    }

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    let target_key = "ha-redist-del/base-3.bin".to_string();
    let redist_task = tokio::spawn(run_redistribution_job(
        false,
        Arc::new(WsConnectionManager::new()),
    ));

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let del_resp = delete_object_permanently(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), target_key.clone())),
    )
    .await
    .expect("permanent delete during redistribution")
    .into_response();
    assert_eq!(del_resp.status(), StatusCode::NO_CONTENT);
    expected.remove(&target_key);

    redist_task
        .await
        .expect("redistribution join")
        .expect("redistribution should succeed after permanent delete");

    let req = Request::builder()
        .uri("/v4/fragment/get")
        .body(Body::empty())
        .expect("build request");
    assert!(
        get_object(
            full_claims(&fragment),
            AxumPath((fragment.clone(), target_key.clone())),
            req
        )
        .await
        .is_err(),
        "permanently deleted key should stay absent after redistribution"
    );

    for (key, bytes) in &expected {
        let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
        assert_eq!(got, *bytes, "survivor mismatch for {}", key);
    }

    assert_all_index_disks_synchronized(&fragment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_ha_repair_permanent_delete_no_index_desync() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let mut expected: HashMap<String, Vec<u8>> = HashMap::new();
    for i in 0..10usize {
        let key = format!("ha-repair-del/base-{}.bin", i);
        let content = (0..(20 * 1024 + i * 43))
            .map(|n| ((n * 7 + i) % 251) as u8)
            .collect::<Vec<u8>>();
        put_object(state.clone(), &fragment, &key, content.clone()).await;
        expected.insert(key, content);
    }

    let repair_hot_key = "ha-repair-del/base-0.bin".to_string();
    let repair_hot_meta = get_metadata(&fragment, &repair_hot_key, &fragment)
        .await
        .expect("repair hot metadata");
    let repair_hot_shard = repair_hot_meta.parts[0]
        .shards
        .iter()
        .find(|s| !s.is_parity)
        .expect("repair hot data shard")
        .clone();

    corrupt_shard_bytes(&repair_hot_shard, 0, &vec![0x6D; 4096]).await;

    let repair_task = tokio::spawn(run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    ));

    let target_key = "ha-repair-del/base-5.bin".to_string();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let del_resp = delete_object_permanently(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), target_key.clone())),
    )
    .await
    .expect("permanent delete during repair")
    .into_response();
    assert_eq!(del_resp.status(), StatusCode::NO_CONTENT);
    expected.remove(&target_key);

    repair_task
        .await
        .expect("repair join")
        .expect("repair should succeed after permanent delete");

    let req = Request::builder()
        .uri("/v4/fragment/get")
        .body(Body::empty())
        .expect("build request");
    assert!(
        get_object(
            full_claims(&fragment),
            AxumPath((fragment.clone(), target_key.clone())),
            req
        )
        .await
        .is_err(),
        "permanently deleted key should stay absent after repair"
    );

    assert!(
        shard_checksum_ok(&repair_hot_shard).await,
        "repair should still restore corrupted survivor shard"
    );

    for (key, bytes) in &expected {
        let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
        assert_eq!(got, *bytes, "survivor mismatch for {}", key);
    }

    assert_all_index_disks_synchronized(&fragment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_ha_concurrent_clients_under_maintenance() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    for i in 0..8usize {
        let key = format!("ha-seed/seed-{}.bin", i);
        let content = (0..(18 * 1024 + i * 23))
            .map(|n| ((n + i) % 239) as u8)
            .collect::<Vec<u8>>();
        put_object(state.clone(), &fragment, &key, content).await;
    }

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    let redist_task = tokio::spawn(run_redistribution_job(
        false,
        Arc::new(WsConnectionManager::new()),
    ));

    let op_count = 10usize;
    let expected_final: Vec<(String, Vec<u8>)> = (0..op_count)
        .map(|i| {
            let final_key = format!("ha-conc/object-{}-renamed.bin", i);
            let bytes = (0..(14 * 1024 + i * 47))
                .map(|n| ((n * 3 + i) % 251) as u8)
                .collect::<Vec<u8>>();
            (final_key, bytes)
        })
        .collect();

    stream::iter(0..op_count)
        .for_each_concurrent(4, |i| {
            let state = state.clone();
            let fragment = fragment.clone();
            async move {
                let key_a = format!("ha-conc/object-{}.bin", i);
                let key_b = format!("ha-conc/object-{}-renamed.bin", i);
                let content = (0..(14 * 1024 + i * 47))
                    .map(|n| ((n * 3 + i) % 251) as u8)
                    .collect::<Vec<u8>>();

                put_object(state.clone(), &fragment, &key_a, content.clone()).await;

                let got = read_object_bytes_eventually(fragment.clone(), key_a.clone(), 28).await;
                assert_eq!(got, content, "initial write mismatch for {}", key_a);

                let rename_resp = rename_object(
                    State(state.clone()),
                    full_claims(&fragment),
                    AxumJson(RenameObjectRequest {
                        source_fragment: fragment.clone(),
                        source_key: key_a.clone(),
                        destination_fragment: fragment.clone(),
                        destination_key: key_b.clone(),
                    }),
                )
                .await
                .expect("rename in concurrent maintenance")
                .into_response();
                assert_eq!(rename_resp.status(), StatusCode::OK);

                let got = read_object_bytes_eventually(fragment.clone(), key_b.clone(), 28).await;
                assert_eq!(got, content, "renamed read mismatch for {}", key_b);

                let del_resp = delete_object(
                    State(state.clone()),
                    full_claims(&fragment),
                    AxumPath((fragment.clone(), key_b.clone())),
                )
                .await
                .expect("soft delete in concurrent maintenance")
                .into_response();
                assert_eq!(del_resp.status(), StatusCode::OK);

                let rec_resp = recover_object(
                    State(state),
                    full_claims(&fragment),
                    AxumPath((fragment.clone(), key_b.clone())),
                )
                .await
                .expect("recover in concurrent maintenance")
                .into_response();
                assert_eq!(rec_resp.status(), StatusCode::OK);

                let got = read_object_bytes_eventually(fragment, key_b.clone(), 24).await;
                assert_eq!(got, content, "recovered read mismatch for {}", key_b);
            }
        })
        .await;

    redist_task
        .await
        .expect("redistribution join")
        .expect("redistribution should succeed during concurrent CRUD");

    let repair_task = tokio::spawn(run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    ));

    for (key, bytes) in &expected_final {
        let got = read_object_bytes_eventually(fragment.clone(), key.clone(), 24).await;
        assert_eq!(got, *bytes, "read mismatch during repair for {}", key);
    }

    repair_task
        .await
        .expect("repair join")
        .expect("repair should succeed after concurrent CRUD");

    for (key, bytes) in &expected_final {
        let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
        assert_eq!(got, *bytes, "final data mismatch for {}", key);

        let meta = get_metadata(&fragment, key, &fragment)
            .await
            .expect("final metadata should exist");
        assert!(
            !meta.is_delete_marker,
            "final object should not be tombstoned: {}",
            key
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_ha_concurrent_clients_with_failure_injection() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let read_attempts = test_env_usize("N1_HA_FAILURE_READ_ATTEMPTS", 64, 8, 256);

    for i in 0..6usize {
        let key = format!("ha-fi/seed-{}.bin", i);
        let content = (0..(20 * 1024 + i * 17))
            .map(|n| ((n + i * 5) % 233) as u8)
            .collect::<Vec<u8>>();
        put_object(state.clone(), &fragment, &key, content).await;
    }

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    let redist_task = tokio::spawn(run_redistribution_job(
        false,
        Arc::new(WsConnectionManager::new()),
    ));

    let op_count = 8usize;
    let expected_final: Vec<(String, Vec<u8>)> = (0..op_count)
        .map(|i| {
            let final_key = format!("ha-fi/object-{}-renamed.bin", i);
            let bytes = (0..(16 * 1024 + i * 53))
                .map(|n| ((n * 7 + i) % 251) as u8)
                .collect::<Vec<u8>>();
            (final_key, bytes)
        })
        .collect();

    stream::iter(0..op_count)
        .for_each_concurrent(4, |i| {
            let state = state.clone();
            let fragment = fragment.clone();
            async move {
                let key_a = format!("ha-fi/object-{}.bin", i);
                let key_b = format!("ha-fi/object-{}-renamed.bin", i);
                let content = (0..(16 * 1024 + i * 53))
                    .map(|n| ((n * 7 + i) % 251) as u8)
                    .collect::<Vec<u8>>();

                put_object(state.clone(), &fragment, &key_a, content.clone()).await;
                let got =
                    read_object_bytes_eventually(fragment.clone(), key_a.clone(), read_attempts)
                        .await;
                assert_eq!(got, content, "initial write mismatch for {}", key_a);

                let rename_resp = rename_object(
                    State(state.clone()),
                    full_claims(&fragment),
                    AxumJson(RenameObjectRequest {
                        source_fragment: fragment.clone(),
                        source_key: key_a.clone(),
                        destination_fragment: fragment.clone(),
                        destination_key: key_b.clone(),
                    }),
                )
                .await
                .expect("rename in failure-injection maintenance")
                .into_response();
                assert_eq!(rename_resp.status(), StatusCode::OK);

                let del_resp = delete_object(
                    State(state.clone()),
                    full_claims(&fragment),
                    AxumPath((fragment.clone(), key_b.clone())),
                )
                .await
                .expect("soft delete in failure-injection maintenance")
                .into_response();
                assert_eq!(del_resp.status(), StatusCode::OK);

                let rec_resp = recover_object(
                    State(state),
                    full_claims(&fragment),
                    AxumPath((fragment.clone(), key_b.clone())),
                )
                .await
                .expect("recover in failure-injection maintenance")
                .into_response();
                assert_eq!(rec_resp.status(), StatusCode::OK);

                let got =
                    read_object_bytes_eventually(fragment, key_b.clone(), read_attempts).await;
                assert_eq!(got, content, "recovered read mismatch for {}", key_b);
            }
        })
        .await;

    redist_task
        .await
        .expect("redistribution join")
        .expect("redistribution should succeed during failure-injection CRUD");

    let target_key = expected_final[0].0.clone();
    let target_meta =
        get_metadata_eventually(fragment.clone(), target_key.clone(), read_attempts).await;
    let target_shard = target_meta.parts[0]
        .shards
        .iter()
        .find(|s| !s.is_parity)
        .expect("target data shard to corrupt")
        .clone();

    corrupt_shard_bytes(&target_shard, 0, &vec![0x9A; 4096]).await;
    assert!(
        !shard_checksum_ok(&target_shard).await,
        "failure injection should break shard checksum before repair"
    );

    let repair_task = tokio::spawn(run_repair_job(
        RepairRunConfig {
            dry_run: false,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    ));

    for (key, bytes) in &expected_final {
        let got = read_object_bytes_eventually(fragment.clone(), key.clone(), read_attempts).await;
        assert_eq!(got, *bytes, "read mismatch during repair for {}", key);
    }

    repair_task
        .await
        .expect("repair join")
        .expect("repair should succeed after failure injection");

    assert!(
        shard_checksum_ok(&target_shard).await,
        "repair should restore shard checksum after failure injection"
    );

    for (key, bytes) in &expected_final {
        let got = read_object_bytes_eventually(fragment.clone(), key.clone(), read_attempts).await;
        assert_eq!(got, *bytes, "final data mismatch for {}", key);

        let meta = get_metadata_eventually(fragment.clone(), key.clone(), read_attempts).await;
        assert!(
            !meta.is_delete_marker,
            "final object should not be tombstoned: {}",
            key
        );
    }
}

async fn run_ha_soak_rounds_under_maintenance(profile: SoakProfile) {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let rounds = profile.rounds;
    let ops_per_round = profile.ops_per_round;
    let client_concurrency = profile.client_concurrency;
    let inject_disk_down = profile.inject_disk_down;
    let read_attempts = test_env_usize("N1_SOAK_READ_ATTEMPTS", 64, 8, 512);

    for round in 0..rounds {
        DATA_SHARDS_ATOMIC
            .get()
            .expect("DATA_SHARDS_ATOMIC")
            .store(2, Ordering::Relaxed);
        PARITY_SHARDS_ATOMIC
            .get()
            .expect("PARITY_SHARDS_ATOMIC")
            .store(1, Ordering::Relaxed);

        let redist_task = tokio::spawn(run_redistribution_job(
            false,
            Arc::new(WsConnectionManager::new()),
        ));

        let expected_round: Vec<(String, Vec<u8>)> = (0..ops_per_round)
            .map(|i| {
                let key = format!("ha-soak/r{}/object-{}-renamed.bin", round, i);
                let bytes = (0..(12 * 1024 + round * 71 + i * 37))
                    .map(|n| ((n * 11 + round + i) % 251) as u8)
                    .collect::<Vec<u8>>();
                (key, bytes)
            })
            .collect();

        stream::iter(0..ops_per_round)
            .for_each_concurrent(client_concurrency, |i| {
                let state = state.clone();
                let fragment = fragment.clone();
                async move {
                    let key_a = format!("ha-soak/r{}/object-{}.bin", round, i);
                    let key_b = format!("ha-soak/r{}/object-{}-renamed.bin", round, i);
                    let content = (0..(12 * 1024 + round * 71 + i * 37))
                        .map(|n| ((n * 11 + round + i) % 251) as u8)
                        .collect::<Vec<u8>>();

                    put_object(state.clone(), &fragment, &key_a, content.clone()).await;
                    let got = read_object_bytes_eventually(
                        fragment.clone(),
                        key_a.clone(),
                        read_attempts,
                    )
                    .await;
                    assert_eq!(
                        got, content,
                        "round {} initial read mismatch {}",
                        round, key_a
                    );

                    let rename_resp = rename_object(
                        State(state.clone()),
                        full_claims(&fragment),
                        AxumJson(RenameObjectRequest {
                            source_fragment: fragment.clone(),
                            source_key: key_a.clone(),
                            destination_fragment: fragment.clone(),
                            destination_key: key_b.clone(),
                        }),
                    )
                    .await
                    .expect("rename in soak round")
                    .into_response();
                    assert_eq!(rename_resp.status(), StatusCode::OK);

                    let del_resp = delete_object(
                        State(state.clone()),
                        full_claims(&fragment),
                        AxumPath((fragment.clone(), key_b.clone())),
                    )
                    .await
                    .expect("delete in soak round")
                    .into_response();
                    assert_eq!(del_resp.status(), StatusCode::OK);

                    let rec_resp = recover_object(
                        State(state),
                        full_claims(&fragment),
                        AxumPath((fragment.clone(), key_b.clone())),
                    )
                    .await
                    .expect("recover in soak round")
                    .into_response();
                    assert_eq!(rec_resp.status(), StatusCode::OK);

                    let got =
                        read_object_bytes_eventually(fragment, key_b.clone(), read_attempts).await;
                    assert_eq!(
                        got, content,
                        "round {} recovered read mismatch {}",
                        round, key_b
                    );
                }
            })
            .await;

        redist_task
            .await
            .expect("redistribution join")
            .expect("redistribution should succeed in soak round");

        if inject_disk_down {
            let blob3 = base.join("blob_3.raw");
            let blob3_down = base.join("blob_3.raw.down");

            if fs::try_exists(&blob3).await.unwrap_or(false) {
                fs::rename(&blob3, &blob3_down)
                    .await
                    .expect("simulate disk down in soak round");
            }

            let redist_res =
                run_redistribution_job(false, Arc::new(WsConnectionManager::new())).await;

            if fs::try_exists(&blob3_down).await.unwrap_or(false) {
                let _ = fs::rename(&blob3_down, &blob3).await;
            }

            redist_res.expect("redistribution should tolerate one disk down in soak round");
        }

        let target_key = expected_round[0].0.clone();
        let target_meta = get_metadata(&fragment, &target_key, &fragment)
            .await
            .expect("target metadata in soak round");
        let target_shard = target_meta.parts[0]
            .shards
            .iter()
            .find(|s| !s.is_parity)
            .expect("target data shard in soak round")
            .clone();

        corrupt_shard_bytes(&target_shard, 0, &vec![0x55; 4096]).await;

        let repair_task = tokio::spawn(run_repair_job(
            RepairRunConfig {
                dry_run: false,
                check_mode: RepairCheckMode::Scrub,
                meta_concurrency: 2,
                shard_repair_concurrency: 2,
                meta_batch_size: 128,
            },
            Arc::new(WsConnectionManager::new()),
        ));

        for (key, bytes) in &expected_round {
            let got =
                read_object_bytes_eventually(fragment.clone(), key.clone(), read_attempts).await;
            assert_eq!(
                got, *bytes,
                "round {} read mismatch during repair {}",
                round, key
            );
        }

        repair_task
            .await
            .expect("repair join")
            .expect("repair should succeed in soak round");

        assert!(
            shard_checksum_ok(&target_shard).await,
            "round {} repair should restore shard checksum",
            round
        );

        for (key, bytes) in &expected_round {
            let got =
                read_object_bytes_eventually(fragment.clone(), key.clone(), read_attempts).await;
            assert_eq!(got, *bytes, "round {} final data mismatch {}", round, key);

            let meta = get_metadata(&fragment, key, &fragment)
                .await
                .expect("final metadata in soak round");
            assert!(
                !meta.is_delete_marker,
                "round {} final object should not be tombstoned: {}",
                round, key
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_ha_soak_rounds_under_maintenance() {
    let profile = SoakProfile {
        rounds: test_env_usize("N1_SOAK_ROUNDS", 3, 1, 64),
        ops_per_round: test_env_usize("N1_SOAK_OPS_PER_ROUND", 4, 1, 128),
        client_concurrency: test_env_usize("N1_SOAK_CLIENT_CONCURRENCY", 4, 1, 64),
        inject_disk_down: test_env_bool("N1_SOAK_DISK_DOWN", false),
    };
    run_ha_soak_rounds_under_maintenance(profile).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "nightly chaos profile; enable with env knobs for long-duration robustness drills"]
async fn matrix_ha_chaos_profile_nightly() {
    // Nightly defaults with optional env overrides.
    let profile = SoakProfile {
        rounds: test_env_usize("N1_SOAK_ROUNDS", 10, 1, 64),
        ops_per_round: test_env_usize("N1_SOAK_OPS_PER_ROUND", 16, 1, 128),
        client_concurrency: test_env_usize("N1_SOAK_CLIENT_CONCURRENCY", 8, 1, 64),
        inject_disk_down: test_env_bool("N1_SOAK_DISK_DOWN", true),
    };
    run_ha_soak_rounds_under_maintenance(profile).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "CRUD CPU/RAM probe; run manually with N1_CRUD_PROBE_* env knobs"]
async fn matrix_crud_cpu_ram_probe() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    let rounds = test_env_usize("N1_CRUD_PROBE_ROUNDS", 3, 1, 64);
    let object_count = test_env_usize("N1_CRUD_PROBE_OBJECTS", 512, 1, 25_000);
    let object_bytes = test_env_usize(
        "N1_CRUD_PROBE_OBJECT_BYTES",
        512 * 1024,
        4096,
        64 * 1024 * 1024,
    );
    let requested_part_bytes = test_env_usize(
        "N1_CRUD_PROBE_PART_BYTES",
        64 * 1024,
        4096,
        64 * 1024 * 1024,
    );
    let part_bytes = requested_part_bytes.min(object_bytes.max(4096));
    let concurrency = test_env_usize("N1_CRUD_PROBE_CONCURRENCY", 16, 1, 256);
    let read_passes = test_env_usize("N1_CRUD_PROBE_READ_PASSES", 2, 1, 16);
    let stream_passes = test_env_usize("N1_CRUD_PROBE_STREAM_PASSES", 1, 1, 16);
    let enable_redistribution = test_env_bool("N1_CRUD_PROBE_ENABLE_REDISTRIBUTION", true);
    let enable_repair = test_env_bool("N1_CRUD_PROBE_ENABLE_REPAIR", true);
    let bitrot_targets = test_env_usize("N1_CRUD_PROBE_BITROT_TARGETS", 16, 0, 4096);
    let repair_meta_concurrency =
        test_env_usize("N1_CRUD_PROBE_REPAIR_META_CONCURRENCY", 8, 1, 256);
    let repair_shard_concurrency =
        test_env_usize("N1_CRUD_PROBE_REPAIR_SHARD_CONCURRENCY", 4, 1, 256);
    let repair_meta_batch = test_env_usize("N1_CRUD_PROBE_REPAIR_META_BATCH", 256, 1, 4096);
    let maintenance_fast_mode = test_env_bool("N1_CRUD_PROBE_MAINT_FAST_MODE", false);
    let redist_fast_mode = test_env_bool("N1_CRUD_PROBE_REDIST_FAST_MODE", maintenance_fast_mode);
    let repair_fast_mode = test_env_bool("N1_CRUD_PROBE_REPAIR_FAST_MODE", maintenance_fast_mode);
    let redist_durability_mode = match std::env::var("N1_CRUD_PROBE_REDIST_DURABILITY") {
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "strict" => RedistributionDurabilityMode::Strict,
            "batched" | "async" | "fast" | "relaxed" => RedistributionDurabilityMode::Batched,
            _ => RedistributionDurabilityMode::Auto,
        },
        Err(_) => RedistributionDurabilityMode::Auto,
    };
    let redist_durability_mode_label = match redist_durability_mode {
        RedistributionDurabilityMode::Auto => "auto",
        RedistributionDurabilityMode::Strict => "strict",
        RedistributionDurabilityMode::Batched => "batched",
    };
    let redist_object_concurrency =
        test_env_optional_usize("N1_CRUD_PROBE_REDIST_OBJECT_CONCURRENCY", 1, 256)
            .unwrap_or_else(|| if redist_fast_mode { 64 } else { 16 });
    let redist_status_batch = test_env_optional_u64("N1_CRUD_PROBE_REDIST_STATUS_BATCH", 1, 8192)
        .unwrap_or(if redist_fast_mode { 256 } else { 64 });
    let max_rss_mib = test_env_u64("N1_CRUD_PROBE_MAX_RSS_MIB", 0, 0, 1_000_000);
    let max_cpu_secs = test_env_u64("N1_CRUD_PROBE_MAX_CPU_SECS", 0, 0, 86_400) as f64;
    let max_wall_secs = test_env_u64("N1_CRUD_PROBE_MAX_WALL_SECS", 0, 0, 86_400) as f64;

    // The CRUD probe keeps multiple immutable versions alive until permanent-delete.
    // Provision raw capacity explicitly so probe failures reflect engine regressions
    // rather than synthetic undersizing of the backing loopback files.
    let projected_live_versions = rounds.saturating_add(1) as u64;
    let projected_live_bytes = (object_count as u64)
        .saturating_mul(object_bytes as u64)
        .saturating_mul(projected_live_versions);
    let redistribution_headroom = if enable_redistribution { 3u64 } else { 2u64 };
    let raw_disk_bytes = projected_live_bytes
        .saturating_mul(redistribution_headroom)
        .div_ceil(3)
        .max(512 * 1024 * 1024);
    ensure_raw_storage_config(&base, 3, raw_disk_bytes).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    MAX_PART_SIZE_ATOMIC
        .get()
        .expect("MAX_PART_SIZE_ATOMIC")
        .store(part_bytes as u64 + 4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let total_payload_bytes = (rounds as u64)
        .saturating_mul(object_count as u64)
        .saturating_mul(object_bytes as u64)
        .saturating_mul(2)
        .max((object_count as u64).saturating_mul(object_bytes as u64));
    let quota_bytes = (object_count as u64)
        .saturating_mul(object_bytes as u64)
        .saturating_mul(8)
        .max(total_payload_bytes)
        .max(64 * 1024 * 1024);
    init_quota_file(&fragment, quota_bytes)
        .await
        .expect("set crud probe quota");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let keys = Arc::new(
        (0..object_count)
            .map(|i| format!("crud-probe/obj-{:05}.bin", i))
            .collect::<Vec<_>>(),
    );

    let mut stages: Vec<PerfStageResult> = Vec::new();
    let mut current_epoch = 0usize;

    let before = perf_snapshot();
    let mut rss_peak = before.rss_mib;
    let started = Instant::now();
    let create_stream = stream::iter(0..object_count)
        .map({
            let state = state.clone();
            let fragment = fragment.clone();
            let keys = keys.clone();
            move |i| {
                let state = state.clone();
                let fragment = fragment.clone();
                let keys = keys.clone();
                async move {
                    let key = keys[i].clone();
                    let seed = payload_seed_for_epoch(current_epoch, i);
                    put_object_generated_chunked(
                        state,
                        &fragment,
                        &key,
                        object_bytes,
                        part_bytes,
                        seed,
                    )
                    .await;
                }
            }
        })
        .buffer_unordered(concurrency);
    tokio::pin!(create_stream);
    while create_stream.next().await.is_some() {
        update_rss_peak(&mut rss_peak);
    }
    stages.push(finish_perf_stage(
        "create",
        object_count,
        (object_count as u64).saturating_mul(object_bytes as u64),
        started,
        before,
        rss_peak,
    ));

    for _round in 0..rounds {
        let before = perf_snapshot();
        let mut rss_peak = before.rss_mib;
        let started = Instant::now();
        for _ in 0..read_passes {
            let read_stream = stream::iter(0..object_count)
                .map({
                    let fragment = fragment.clone();
                    let keys = keys.clone();
                    move |i| {
                        let fragment = fragment.clone();
                        let keys = keys.clone();
                        async move {
                            let key = keys[i].clone();
                            let expected = chunked_payload_for_seed(
                                object_bytes,
                                payload_seed_for_epoch(current_epoch, i),
                            );
                            let got = read_object_bytes(
                                full_claims(&fragment),
                                fragment.clone(),
                                key.clone(),
                            )
                            .await;
                            assert_eq!(got, expected, "get/read mismatch for {}", key);
                        }
                    }
                })
                .buffer_unordered(concurrency);
            tokio::pin!(read_stream);
            while read_stream.next().await.is_some() {
                update_rss_peak(&mut rss_peak);
            }
        }
        stages.push(finish_perf_stage(
            "read_get",
            object_count * read_passes,
            (object_count as u64)
                .saturating_mul(object_bytes as u64)
                .saturating_mul(read_passes as u64),
            started,
            before,
            rss_peak,
        ));

        let before = perf_snapshot();
        let mut rss_peak = before.rss_mib;
        let started = Instant::now();
        for _ in 0..stream_passes {
            let stream_reads = stream::iter(0..object_count)
                .map({
                    let fragment = fragment.clone();
                    let keys = keys.clone();
                    move |i| {
                        let fragment = fragment.clone();
                        let keys = keys.clone();
                        async move {
                            let key = keys[i].clone();
                            let expected = chunked_payload_for_seed(
                                object_bytes,
                                payload_seed_for_epoch(current_epoch, i),
                            );
                            let got = stream_object_bytes(fragment.clone(), key.clone()).await;
                            assert_eq!(got, expected, "stream mismatch for {}", key);
                        }
                    }
                })
                .buffer_unordered(concurrency);
            tokio::pin!(stream_reads);
            while stream_reads.next().await.is_some() {
                update_rss_peak(&mut rss_peak);
            }
        }
        stages.push(finish_perf_stage(
            "read_stream",
            object_count * stream_passes,
            (object_count as u64)
                .saturating_mul(object_bytes as u64)
                .saturating_mul(stream_passes as u64),
            started,
            before,
            rss_peak,
        ));

        let before = perf_snapshot();
        let mut rss_peak = before.rss_mib;
        let started = Instant::now();
        let metadata_reads = stream::iter(0..object_count)
            .map({
                let fragment = fragment.clone();
                let keys = keys.clone();
                move |i| {
                    let fragment = fragment.clone();
                    let keys = keys.clone();
                    async move {
                        let key = keys[i].clone();
                        let resp = get_object_metadata(
                            full_claims(&fragment),
                            AxumPath((fragment.clone(), key.clone())),
                        )
                        .await
                        .expect("metadata response")
                        .into_response();
                        assert_eq!(resp.status(), StatusCode::OK, "metadata status for {}", key);
                    }
                }
            })
            .buffer_unordered(concurrency);
        tokio::pin!(metadata_reads);
        while metadata_reads.next().await.is_some() {
            update_rss_peak(&mut rss_peak);
        }
        stages.push(finish_perf_stage(
            "read_metadata",
            object_count,
            0,
            started,
            before,
            rss_peak,
        ));

        current_epoch = current_epoch.saturating_add(1);

        let before = perf_snapshot();
        let mut rss_peak = before.rss_mib;
        let started = Instant::now();
        let update_stream = stream::iter(0..object_count)
            .map({
                let state = state.clone();
                let fragment = fragment.clone();
                let keys = keys.clone();
                move |i| {
                    let state = state.clone();
                    let fragment = fragment.clone();
                    let keys = keys.clone();
                    async move {
                        let key = keys[i].clone();
                        let seed = payload_seed_for_epoch(current_epoch, i);
                        put_object_generated_chunked(
                            state,
                            &fragment,
                            &key,
                            object_bytes,
                            part_bytes,
                            seed,
                        )
                        .await;
                    }
                }
            })
            .buffer_unordered(concurrency);
        tokio::pin!(update_stream);
        while update_stream.next().await.is_some() {
            update_rss_peak(&mut rss_peak);
        }
        stages.push(finish_perf_stage(
            "update_overwrite",
            object_count,
            (object_count as u64).saturating_mul(object_bytes as u64),
            started,
            before,
            rss_peak,
        ));
    }

    if enable_redistribution {
        let before = perf_snapshot();
        let mut rss_peak = before.rss_mib;
        let started = Instant::now();
        DATA_SHARDS_ATOMIC
            .get()
            .expect("DATA_SHARDS_ATOMIC")
            .store(2, Ordering::Relaxed);
        PARITY_SHARDS_ATOMIC
            .get()
            .expect("PARITY_SHARDS_ATOMIC")
            .store(1, Ordering::Relaxed);
        run_redistribution_job_with_options(
            false,
            RedistributionRunOptions {
                fast_mode: redist_fast_mode,
                durability_mode: redist_durability_mode,
                execution_mode: RedistributionExecutionMode::Availability,
                object_concurrency: redist_object_concurrency,
                status_batch: redist_status_batch,
                fragment_filter: None,
            },
            Arc::new(WsConnectionManager::new()),
        )
        .await
        .expect("run redistribution in CRUD probe");
        update_rss_peak(&mut rss_peak);
        stages.push(finish_perf_stage(
            "redistribute",
            object_count,
            (object_count as u64).saturating_mul(object_bytes as u64),
            started,
            before,
            rss_peak,
        ));
    }

    if enable_repair {
        let corrupt_count = bitrot_targets.min(object_count);
        if corrupt_count > 0 {
            let before = perf_snapshot();
            let mut rss_peak = before.rss_mib;
            let started = Instant::now();
            for i in 0..corrupt_count {
                let key = keys[i].clone();
                let meta = get_metadata(&fragment, &key, &fragment)
                    .await
                    .expect("metadata before repair bitrot");
                let shard = meta
                    .parts
                    .iter()
                    .find_map(|p| p.shards.iter().find(|s| !s.is_parity));
                let Some(shard) = shard else {
                    continue;
                };
                let overwrite = vec![0x5A; std::cmp::min(4096, shard.size_bytes as usize).max(1)];
                corrupt_shard_bytes(shard, 0, &overwrite).await;
                update_rss_peak(&mut rss_peak);
            }
            stages.push(finish_perf_stage(
                "inject_bitrot",
                corrupt_count,
                0,
                started,
                before,
                rss_peak,
            ));
        }

        let before = perf_snapshot();
        let mut rss_peak = before.rss_mib;
        let started = Instant::now();
        run_repair_job_with_options(
            RepairRunConfig {
                dry_run: false,
                check_mode: RepairCheckMode::Scrub,
                meta_concurrency: repair_meta_concurrency,
                shard_repair_concurrency: repair_shard_concurrency,
                meta_batch_size: repair_meta_batch,
            },
            RepairRunOptions {
                fast_mode: repair_fast_mode,
                access_mode: MaintenanceAccessMode::Quiesced,
                fragment_filter: None,
            },
            Arc::new(WsConnectionManager::new()),
        )
        .await
        .expect("run repair in CRUD probe");
        update_rss_peak(&mut rss_peak);
        stages.push(finish_perf_stage(
            "repair",
            object_count,
            0,
            started,
            before,
            rss_peak,
        ));
    }

    let before = perf_snapshot();
    let mut rss_peak = before.rss_mib;
    let started = Instant::now();
    let post_maintenance_reads = stream::iter(0..object_count)
        .map({
            let fragment = fragment.clone();
            let keys = keys.clone();
            move |i| {
                let fragment = fragment.clone();
                let keys = keys.clone();
                async move {
                    let key = keys[i].clone();
                    let expected = chunked_payload_for_seed(
                        object_bytes,
                        payload_seed_for_epoch(current_epoch, i),
                    );
                    let got =
                        read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone())
                            .await;
                    assert_eq!(got, expected, "post-maintenance mismatch for {}", key);
                }
            }
        })
        .buffer_unordered(concurrency);
    tokio::pin!(post_maintenance_reads);
    while post_maintenance_reads.next().await.is_some() {
        update_rss_peak(&mut rss_peak);
    }
    stages.push(finish_perf_stage(
        "verify_post_maintenance",
        object_count,
        (object_count as u64).saturating_mul(object_bytes as u64),
        started,
        before,
        rss_peak,
    ));

    let before = perf_snapshot();
    let mut rss_peak = before.rss_mib;
    let started = Instant::now();
    let soft_delete_stream = stream::iter(0..object_count)
        .map({
            let state = state.clone();
            let fragment = fragment.clone();
            let keys = keys.clone();
            move |i| {
                let state = state.clone();
                let fragment = fragment.clone();
                let keys = keys.clone();
                async move {
                    let key = keys[i].clone();
                    let resp = delete_object(
                        State(state),
                        full_claims(&fragment),
                        AxumPath((fragment.clone(), key.clone())),
                    )
                    .await
                    .expect("soft delete object")
                    .into_response();
                    assert_eq!(
                        resp.status(),
                        StatusCode::OK,
                        "soft delete status for {}",
                        key
                    );
                }
            }
        })
        .buffer_unordered(concurrency);
    tokio::pin!(soft_delete_stream);
    while soft_delete_stream.next().await.is_some() {
        update_rss_peak(&mut rss_peak);
    }
    stages.push(finish_perf_stage(
        "delete_soft",
        object_count,
        0,
        started,
        before,
        rss_peak,
    ));

    let before = perf_snapshot();
    let mut rss_peak = before.rss_mib;
    let started = Instant::now();
    let recover_stream = stream::iter(0..object_count)
        .map({
            let state = state.clone();
            let fragment = fragment.clone();
            let keys = keys.clone();
            move |i| {
                let state = state.clone();
                let fragment = fragment.clone();
                let keys = keys.clone();
                async move {
                    let key = keys[i].clone();
                    let resp = recover_object(
                        State(state),
                        full_claims(&fragment),
                        AxumPath((fragment.clone(), key.clone())),
                    )
                    .await
                    .expect("recover object")
                    .into_response();
                    assert_eq!(resp.status(), StatusCode::OK, "recover status for {}", key);
                }
            }
        })
        .buffer_unordered(concurrency);
    tokio::pin!(recover_stream);
    while recover_stream.next().await.is_some() {
        update_rss_peak(&mut rss_peak);
    }
    stages.push(finish_perf_stage(
        "recover",
        object_count,
        0,
        started,
        before,
        rss_peak,
    ));

    let before = perf_snapshot();
    let mut rss_peak = before.rss_mib;
    let started = Instant::now();
    let permanent_delete_stream = stream::iter(0..object_count)
        .map({
            let state = state.clone();
            let fragment = fragment.clone();
            let keys = keys.clone();
            move |i| {
                let state = state.clone();
                let fragment = fragment.clone();
                let keys = keys.clone();
                async move {
                    let key = keys[i].clone();
                    let resp = delete_object_permanently(
                        State(state),
                        full_claims(&fragment),
                        AxumPath((fragment.clone(), key.clone())),
                    )
                    .await
                    .expect("permanent delete object")
                    .into_response();
                    assert_eq!(
                        resp.status(),
                        StatusCode::NO_CONTENT,
                        "permanent delete for {}",
                        key
                    );
                }
            }
        })
        .buffer_unordered(concurrency);
    tokio::pin!(permanent_delete_stream);
    while permanent_delete_stream.next().await.is_some() {
        update_rss_peak(&mut rss_peak);
    }
    stages.push(finish_perf_stage(
        "delete_permanent",
        object_count,
        0,
        started,
        before,
        rss_peak,
    ));

    let before = perf_snapshot();
    let mut rss_peak = before.rss_mib;
    let started = Instant::now();
    let absent_verify_stream = stream::iter(0..object_count)
        .map({
            let fragment = fragment.clone();
            let keys = keys.clone();
            move |i| {
                let fragment = fragment.clone();
                let keys = keys.clone();
                async move {
                    let key = keys[i].clone();
                    let req = Request::builder()
                        .uri("/v4/fragment/get")
                        .body(Body::empty())
                        .expect("build request");
                    assert!(
                        get_object(
                            full_claims(&fragment),
                            AxumPath((fragment.clone(), key.clone())),
                            req
                        )
                        .await
                        .is_err(),
                        "key should be absent after permanent delete: {}",
                        key
                    );
                }
            }
        })
        .buffer_unordered(concurrency);
    tokio::pin!(absent_verify_stream);
    while absent_verify_stream.next().await.is_some() {
        update_rss_peak(&mut rss_peak);
    }
    stages.push(finish_perf_stage(
        "verify_absent",
        object_count,
        0,
        started,
        before,
        rss_peak,
    ));

    for stage in &stages {
        print_perf_stage(stage);
    }

    let total_wall_secs: f64 = stages.iter().map(|s| s.wall_secs).sum();
    let total_cpu_secs: Option<f64> = if stages.iter().all(|s| s.cpu_secs.is_some()) {
        Some(stages.iter().map(|s| s.cpu_secs.unwrap_or(0.0)).sum())
    } else {
        None
    };
    let peak_rss_mib = stages.iter().filter_map(|s| s.rss_peak_mib).max();
    let peak_hwm_mib = stages.iter().filter_map(|s| s.hwm_mib).max();

    println!(
        "N1 CRUD probe summary: rounds={} objects={} object_bytes={} part_bytes={} concurrency={} read_passes={} stream_passes={} redistribution={} repair={} bitrot_targets={} redist_fast_mode={} redist_durability={} redist_object_concurrency={} redist_status_batch={} repair_fast_mode={} total_wall_s={:.3} total_cpu_s={} peak_rss_mib={} peak_hwm_mib={}",
        rounds,
        object_count,
        object_bytes,
        part_bytes,
        concurrency,
        read_passes,
        stream_passes,
        enable_redistribution,
        enable_repair,
        bitrot_targets,
        redist_fast_mode,
        redist_durability_mode_label,
        redist_object_concurrency,
        redist_status_batch,
        repair_fast_mode,
        total_wall_secs,
        total_cpu_secs
            .map(|v| format!("{:.3}", v))
            .unwrap_or_else(|| "n/a".to_string()),
        peak_rss_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        peak_hwm_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string())
    );

    if max_wall_secs > 0.0 {
        assert!(
            total_wall_secs <= max_wall_secs,
            "CRUD probe wall time exceeded: {:.3}s > {:.3}s",
            total_wall_secs,
            max_wall_secs
        );
    }

    if max_cpu_secs > 0.0 {
        let observed = total_cpu_secs.expect(
            "CPU metric unavailable on this platform; cannot enforce N1_CRUD_PROBE_MAX_CPU_SECS",
        );
        assert!(
            observed <= max_cpu_secs,
            "CRUD probe CPU time exceeded: {:.3}s > {:.3}s",
            observed,
            max_cpu_secs
        );
    }

    if max_rss_mib > 0 {
        let observed = peak_rss_mib.or(peak_hwm_mib).expect(
            "RSS metric unavailable on this platform; cannot enforce N1_CRUD_PROBE_MAX_RSS_MIB",
        );
        assert!(
            observed <= max_rss_mib,
            "CRUD probe RSS exceeded: {} MiB > {} MiB",
            observed,
            max_rss_mib
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "throughput probe for single-NVMe upload saturation; run manually with env knobs"]
async fn matrix_upload_single_nvme_saturation_probe() {
    let _guard = TEST_LOCK.lock().await;
    let workers = test_env_usize("N1_UPLOAD_PROBE_CONCURRENCY", 8, 1, 128);
    let uploads_per_worker = test_env_usize("N1_UPLOAD_PROBE_OBJECTS_PER_WORKER", 2, 1, 1024);
    let object_bytes = test_env_u64(
        "N1_UPLOAD_PROBE_OBJECT_BYTES",
        64 * 1024 * 1024,
        1024 * 1024,
        1024 * 1024 * 1024,
    ) as usize;
    let part_bytes = test_env_usize(
        "N1_UPLOAD_PROBE_PART_BYTES",
        4 * 1024 * 1024,
        256 * 1024,
        64 * 1024 * 1024,
    );
    let min_mib_per_sec = test_env_u64("N1_UPLOAD_PROBE_MIN_MIBPS", 0, 0, 100_000) as f64;
    let max_ram_mib = test_env_u64("N1_UPLOAD_PROBE_MAX_RAM_MIB", 0, 0, 1_000_000);
    let upload_durability = std::env::var("N1_UPLOAD_PROBE_DURABILITY")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .and_then(|v| match v.as_str() {
            "strict" | "sync" | "durable" => Some("strict".to_string()),
            "batched" | "async" | "fast" | "relaxed" => Some("batched".to_string()),
            _ => None,
        });
    let upload_io_gate = std::env::var("N1_UPLOAD_PROBE_IO_GATE").ok().and_then(|v| {
        match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        }
    });

    // Conservative guard:
    // - one part buffer per worker for payload generation
    // - one additional in-flight copy/buffer stage in upload path
    let estimated_working_set_bytes = (workers as u64)
        .saturating_mul(part_bytes as u64)
        .saturating_mul(2);
    let estimated_working_set_mib = estimated_working_set_bytes as f64 / (1024.0 * 1024.0);
    if max_ram_mib > 0 {
        assert!(
            estimated_working_set_mib <= max_ram_mib as f64,
            "estimated in-flight RAM {:.2} MiB exceeds configured cap {} MiB; lower N1_UPLOAD_PROBE_CONCURRENCY or N1_UPLOAD_PROBE_PART_BYTES",
            estimated_working_set_mib,
            max_ram_mib
        );
    }

    let total_uploads = workers * uploads_per_worker;
    let total_bytes = total_uploads as u64 * object_bytes as u64;
    let raw_disk_bytes = total_bytes.saturating_mul(4).max(512 * 1024 * 1024);
    let base = TEST_BASE.join(format!("upload-probe-{}", Ulid::new()));
    ensure_raw_storage_config(&base, 3, raw_disk_bytes).await;
    ensure_upload_runtime_config();

    MAX_PART_SIZE_ATOMIC
        .get()
        .expect("MAX_PART_SIZE_ATOMIC")
        .store(part_bytes as u64 + 4096, Ordering::Relaxed);
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let quota_bytes = total_bytes.saturating_mul(2).max(64 * 1024 * 1024);
    init_quota_file(&fragment, quota_bytes)
        .await
        .expect("set probe quota");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let started = Instant::now();

    stream::iter(0..workers)
        .for_each_concurrent(workers, |w| {
            let state = state.clone();
            let fragment = fragment.clone();
            let upload_durability = upload_durability.clone();
            let upload_io_gate = upload_io_gate;
            async move {
                for i in 0..uploads_per_worker {
                    let key = format!("nvme-probe/w{}/obj-{}.bin", w, i);
                    put_object_generated_chunked_with_options(
                        state.clone(),
                        &fragment,
                        &key,
                        object_bytes,
                        part_bytes,
                        w * 17 + i,
                        upload_durability.as_deref(),
                        upload_io_gate,
                    )
                    .await;
                }
            }
        })
        .await;

    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    let mib_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed;

    println!(
        "N1 upload probe: workers={} uploads_per_worker={} object_bytes={} part_bytes={} durability={} io_gate={} est_working_set_mib={:.2} total_uploads={} total_mib={:.2} elapsed_s={:.3} throughput_mib_s={:.2}",
        workers,
        uploads_per_worker,
        object_bytes,
        part_bytes,
        upload_durability.as_deref().unwrap_or("default"),
        upload_io_gate
            .map(|v| if v { "on" } else { "off" })
            .unwrap_or("default"),
        estimated_working_set_mib,
        total_uploads,
        total_bytes as f64 / (1024.0 * 1024.0),
        elapsed,
        mib_per_sec
    );

    if min_mib_per_sec > 0.0 {
        assert!(
            mib_per_sec >= min_mib_per_sec,
            "throughput below threshold: measured {:.2} MiB/s < required {:.2} MiB/s",
            mib_per_sec,
            min_mib_per_sec
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "sequential stream readahead/locality probe; run manually with env knobs"]
async fn matrix_stream_sequential_readahead_locality_probe() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let object_bytes = test_env_usize(
        "N1_STREAM_PROBE_OBJECT_BYTES",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        2 * 1024 * 1024 * 1024,
    );
    let part_bytes = test_env_usize(
        "N1_STREAM_PROBE_PART_BYTES",
        4 * 1024 * 1024,
        256 * 1024,
        64 * 1024 * 1024,
    );
    let passes = test_env_usize("N1_STREAM_PROBE_PASSES", 2, 1, 8);
    let chunk_bytes = test_env_usize(
        "N1_STREAM_PROBE_CHUNK_BYTES",
        256 * 1024,
        4096,
        4 * 1024 * 1024,
    );
    let readahead_on_bytes = test_env_usize(
        "N1_STREAM_PROBE_READAHEAD_BYTES",
        1024 * 1024,
        64 * 1024,
        8 * 1024 * 1024,
    );
    let readahead_off_bytes = test_env_usize(
        "N1_STREAM_PROBE_READAHEAD_OFF_BYTES",
        64 * 1024,
        64 * 1024,
        8 * 1024 * 1024,
    );
    let min_ratio_permil =
        test_env_u64("N1_STREAM_PROBE_MIN_RATIO_PERMIL", 0, 0, 5000) as f64 / 1000.0;

    MAX_PART_SIZE_ATOMIC
        .get()
        .expect("MAX_PART_SIZE_ATOMIC")
        .store(part_bytes as u64 + 4096, Ordering::Relaxed);
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STREAM_CHUNK_SIZE_ATOMIC
        .get()
        .expect("STREAM_CHUNK_SIZE_ATOMIC")
        .store(chunk_bytes, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "stream-probe/sequential.bin".to_string();

    init_quota_file(&fragment, (object_bytes as u64).saturating_mul(4))
        .await
        .expect("set stream probe quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init fragment metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object_generated_chunked(state.clone(), &fragment, &key, object_bytes, part_bytes, 7).await;

    async fn measure_stream_passes(
        fragment: &str,
        key: &str,
        object_bytes: usize,
        passes: usize,
    ) -> f64 {
        let mut total_elapsed = 0.0f64;
        let mut total_bytes = 0usize;
        for _ in 0..passes {
            let started = Instant::now();
            let resp = stream_object(
                full_claims(fragment),
                HeaderMap::new(),
                AxumPath((fragment.to_string(), key.to_string())),
            )
            .await
            .expect("stream object probe")
            .into_response();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = to_bytes(resp.into_body(), usize::MAX)
                .await
                .expect("stream bytes");
            assert_eq!(body.len(), object_bytes, "streamed size mismatch");
            total_elapsed += started.elapsed().as_secs_f64();
            total_bytes += body.len();
        }
        (total_bytes as f64 / (1024.0 * 1024.0)) / total_elapsed.max(0.001)
    }

    let old_readahead = STREAM_READAHEAD_BYTES_ATOMIC
        .get()
        .expect("STREAM_READAHEAD_BYTES_ATOMIC")
        .load(Ordering::Relaxed);
    let old_locality = STREAM_LOCALITY_BIAS_ENABLED_ATOMIC
        .get()
        .expect("STREAM_LOCALITY_BIAS_ENABLED_ATOMIC")
        .load(Ordering::Relaxed);

    STREAM_READAHEAD_BYTES_ATOMIC
        .get()
        .expect("STREAM_READAHEAD_BYTES_ATOMIC")
        .store(readahead_off_bytes, Ordering::Relaxed);
    STREAM_LOCALITY_BIAS_ENABLED_ATOMIC
        .get()
        .expect("STREAM_LOCALITY_BIAS_ENABLED_ATOMIC")
        .store(false, Ordering::Relaxed);
    let off_mibps = measure_stream_passes(&fragment, &key, object_bytes, passes).await;

    STREAM_READAHEAD_BYTES_ATOMIC
        .get()
        .expect("STREAM_READAHEAD_BYTES_ATOMIC")
        .store(readahead_on_bytes, Ordering::Relaxed);
    STREAM_LOCALITY_BIAS_ENABLED_ATOMIC
        .get()
        .expect("STREAM_LOCALITY_BIAS_ENABLED_ATOMIC")
        .store(true, Ordering::Relaxed);
    let on_mibps = measure_stream_passes(&fragment, &key, object_bytes, passes).await;

    STREAM_READAHEAD_BYTES_ATOMIC
        .get()
        .expect("STREAM_READAHEAD_BYTES_ATOMIC")
        .store(old_readahead, Ordering::Relaxed);
    STREAM_LOCALITY_BIAS_ENABLED_ATOMIC
        .get()
        .expect("STREAM_LOCALITY_BIAS_ENABLED_ATOMIC")
        .store(old_locality, Ordering::Relaxed);

    let ratio = if off_mibps > 0.0 {
        on_mibps / off_mibps
    } else {
        0.0
    };

    println!(
        "N1 stream probe: object_bytes={} part_bytes={} passes={} chunk_bytes={} readahead_off={} readahead_on={} off_mibps={:.2} on_mibps={:.2} ratio={:.3}",
        object_bytes,
        part_bytes,
        passes,
        chunk_bytes,
        readahead_off_bytes,
        readahead_on_bytes,
        off_mibps,
        on_mibps,
        ratio
    );

    if min_ratio_permil > 0.0 {
        assert!(
            ratio >= min_ratio_permil,
            "stream readahead/locality regression: ratio {:.3} < required {:.3}",
            ratio,
            min_ratio_permil
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "empty-trash stage probe; run manually with N1_EMPTY_TRASH_PROBE_* env knobs"]
async fn matrix_empty_trash_fragment_nuke_probe() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let object_count = test_env_usize("N1_EMPTY_TRASH_PROBE_OBJECTS", 2048, 1, 200_000);
    let object_bytes = test_env_usize(
        "N1_EMPTY_TRASH_PROBE_OBJECT_BYTES",
        256 * 1024,
        4 * 1024,
        8 * 1024 * 1024,
    );
    let part_bytes = test_env_usize(
        "N1_EMPTY_TRASH_PROBE_PART_BYTES",
        256 * 1024,
        64 * 1024,
        8 * 1024 * 1024,
    );
    let concurrency = test_env_usize("N1_EMPTY_TRASH_PROBE_CONCURRENCY", 16, 1, 256);

    MAX_PART_SIZE_ATOMIC
        .get()
        .expect("MAX_PART_SIZE_ATOMIC")
        .store(part_bytes as u64 + 4096, Ordering::Relaxed);
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let total_bytes = (object_count as u64).saturating_mul(object_bytes as u64);
    init_quota_file(
        &fragment,
        total_bytes.saturating_mul(4).max(64 * 1024 * 1024),
    )
    .await
    .expect("set empty-trash probe quota");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    stream::iter(0..object_count)
        .for_each_concurrent(concurrency, |i| {
            let state = state.clone();
            let fragment = fragment.clone();
            async move {
                let key = format!("trash-probe/obj-{:06}.bin", i);
                put_object_generated_chunked(state, &fragment, &key, object_bytes, part_bytes, i)
                    .await;
            }
        })
        .await;

    stream::iter(0..object_count)
        .for_each_concurrent(concurrency, |i| {
            let state = state.clone();
            let fragment = fragment.clone();
            async move {
                let key = format!("trash-probe/obj-{:06}.bin", i);
                let resp = delete_object(
                    State(state),
                    full_claims(&fragment),
                    AxumPath((fragment.clone(), key)),
                )
                .await
                .expect("delete probe object")
                .into_response();
                assert_eq!(resp.status(), StatusCode::OK);
            }
        })
        .await;

    let started_wait = Instant::now();
    let trash_versions = loop {
        let versions = collect_fragment_versions_streamed(&fragment, true).await;
        if versions.len() == object_count {
            break versions;
        }
        assert!(
            started_wait.elapsed() < Duration::from_secs(30),
            "probe trash population did not converge: expected {} versions, saw {}",
            object_count,
            versions.len()
        );
        sleep(Duration::from_millis(50)).await;
    };

    let total_refund_size: u64 = trash_versions
        .iter()
        .map(|meta| meta.total_size_bytes)
        .sum();
    let raw_trash_versions = trash_versions
        .iter()
        .filter(|meta| {
            meta.parts
                .iter()
                .flat_map(|part| part.shards.iter())
                .any(|shard| shard.raw_blob_address.is_some())
        })
        .cloned()
        .collect::<Vec<_>>();
    let touched_partitions = trash_versions
        .iter()
        .map(|meta| index::object_partition_id(&meta.fragment, &meta.object_key))
        .collect::<Result<BTreeSet<_>, _>>()
        .expect("derive touched trash partitions");

    let mut stages = Vec::new();
    let mut peak_rss_mib = perf_snapshot().rss_mib;

    let before = perf_snapshot();
    let started = Instant::now();
    free_raw_shards_for_metadatas(&raw_trash_versions)
        .await
        .expect("batch free raw trash");
    update_rss_peak(&mut peak_rss_mib);
    stages.push(finish_perf_stage(
        "raw_free",
        raw_trash_versions.len(),
        total_refund_size,
        started,
        before,
        peak_rss_mib,
    ));

    let before = perf_snapshot();
    let started = Instant::now();
    let touched_partitions =
        index::clear_fragment_trash_in_partitions(&fragment, &touched_partitions)
            .await
            .expect("clear fragment trash");
    update_rss_peak(&mut peak_rss_mib);
    stages.push(finish_perf_stage(
        "index_clear",
        touched_partitions,
        0,
        started,
        before,
        peak_rss_mib,
    ));

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let before = perf_snapshot();
    let started = Instant::now();
    for root in &blob_roots {
        let trash_root = root.join(&fragment).join(".trash");
        if fs::try_exists(&trash_root).await.unwrap_or(false) {
            let _ = fs::remove_dir_all(&trash_root).await;
        }
    }
    update_rss_peak(&mut peak_rss_mib);
    stages.push(finish_perf_stage(
        "blob_trash_rm",
        blob_roots.len(),
        0,
        started,
        before,
        peak_rss_mib,
    ));

    let before = perf_snapshot();
    let started = Instant::now();
    if total_refund_size > 0 {
        update_quota(&fragment, -(total_refund_size as i64))
            .await
            .expect("refund empty trash quota");
    }
    update_rss_peak(&mut peak_rss_mib);
    stages.push(finish_perf_stage(
        "quota_refund",
        1,
        total_refund_size,
        started,
        before,
        peak_rss_mib,
    ));

    let trashed_after = collect_fragment_versions_streamed(&fragment, true).await;
    assert!(
        trashed_after.is_empty(),
        "trash should be empty after probe"
    );
    let quota_after = get_quota(&fragment)
        .await
        .expect("quota after empty-trash probe");
    assert_eq!(quota_after.storage_used, 0);

    let total_wall_secs: f64 = stages.iter().map(|stage| stage.wall_secs).sum();
    let total_cpu_secs = stages
        .iter()
        .filter_map(|stage| stage.cpu_secs)
        .sum::<f64>();
    for stage in &stages {
        print_perf_stage(stage);
    }

    let raw_free_secs = stages
        .iter()
        .find(|stage| stage.name == "raw_free")
        .map(|stage| stage.wall_secs)
        .unwrap_or(0.0);
    let index_clear_secs = stages
        .iter()
        .find(|stage| stage.name == "index_clear")
        .map(|stage| stage.wall_secs)
        .unwrap_or(0.0);
    let blob_trash_rm_secs = stages
        .iter()
        .find(|stage| stage.name == "blob_trash_rm")
        .map(|stage| stage.wall_secs)
        .unwrap_or(0.0);
    let quota_refund_secs = stages
        .iter()
        .find(|stage| stage.name == "quota_refund")
        .map(|stage| stage.wall_secs)
        .unwrap_or(0.0);

    println!(
        "N1 empty trash probe: objects={} object_bytes={} part_bytes={} concurrency={} total_mib={:.2} raw_versions={} raw_free_s={:.3} index_clear_s={:.3} blob_rm_s={:.3} quota_s={:.3} total_wall_s={:.3} total_cpu_s={} peak_rss_mib={} peak_hwm_mib={}",
        object_count,
        object_bytes,
        part_bytes,
        concurrency,
        total_refund_size as f64 / (1024.0 * 1024.0),
        raw_trash_versions.len(),
        raw_free_secs,
        index_clear_secs,
        blob_trash_rm_secs,
        quota_refund_secs,
        total_wall_secs,
        if total_cpu_secs > 0.0 {
            format!("{:.3}", total_cpu_secs)
        } else {
            "n/a".to_string()
        },
        peak_rss_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        perf_snapshot()
            .hwm_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "upload RSS plateau probe; run manually with N1_UPLOAD_RSS_PROBE_* env knobs"]
async fn matrix_upload_rss_plateau_probe() {
    let _guard = TEST_LOCK.lock().await;
    let workers = test_env_usize("N1_UPLOAD_RSS_PROBE_CONCURRENCY", 4, 1, 64);
    let uploads_per_worker = test_env_usize("N1_UPLOAD_RSS_PROBE_OBJECTS_PER_WORKER", 1, 1, 256);
    let object_bytes = test_env_u64(
        "N1_UPLOAD_RSS_PROBE_OBJECT_BYTES",
        256 * 1024 * 1024,
        1024 * 1024,
        16 * 1024 * 1024 * 1024,
    ) as usize;
    let part_bytes = test_env_usize(
        "N1_UPLOAD_RSS_PROBE_PART_BYTES",
        32 * 1024 * 1024,
        256 * 1024,
        512 * 1024 * 1024,
    );
    let cooldown_ms = test_env_u64("N1_UPLOAD_RSS_PROBE_COOLDOWN_MS", 2000, 0, 120_000);
    let max_peak_mib = test_env_u64("N1_UPLOAD_RSS_PROBE_MAX_PEAK_MIB", 0, 0, 1_000_000);
    let max_settle_delta_mib =
        test_env_u64("N1_UPLOAD_RSS_PROBE_MAX_SETTLE_DELTA_MIB", 0, 0, 1_000_000);
    let total_uploads = workers * uploads_per_worker;
    let total_bytes = total_uploads as u64 * object_bytes as u64;
    let raw_disk_bytes = total_bytes.saturating_mul(4).max(512 * 1024 * 1024);
    let base = TEST_BASE.join(format!("upload-rss-probe-{}", Ulid::new()));
    ensure_raw_storage_config(&base, 3, raw_disk_bytes).await;
    ensure_upload_runtime_config();

    MAX_PART_SIZE_ATOMIC
        .get()
        .expect("MAX_PART_SIZE_ATOMIC")
        .store(part_bytes as u64 + 4096, Ordering::Relaxed);
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    init_quota_file(
        &fragment,
        total_bytes.saturating_mul(2).max(64 * 1024 * 1024),
    )
    .await
    .expect("set rss probe quota");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let before = perf_snapshot();
    let rss_peak_atomic = Arc::new(AtomicU64::new(before.rss_mib.unwrap_or(0)));
    let rss_sampler_stop = Arc::new(AtomicBool::new(false));
    let rss_sampler = {
        let rss_peak_atomic = rss_peak_atomic.clone();
        let rss_sampler_stop = rss_sampler_stop.clone();
        tokio::spawn(async move {
            while !rss_sampler_stop.load(Ordering::Relaxed) {
                if let Some(cur) = process_rss_hwm_mib().0 {
                    rss_peak_atomic.fetch_max(cur, Ordering::Relaxed);
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
    };
    let started = Instant::now();

    stream::iter(0..workers)
        .for_each_concurrent(workers, |w| {
            let state = state.clone();
            let fragment = fragment.clone();
            async move {
                for i in 0..uploads_per_worker {
                    let key = format!("rss-probe/w{}/obj-{}.bin", w, i);
                    put_object_generated_chunked(
                        state.clone(),
                        &fragment,
                        &key,
                        object_bytes,
                        part_bytes,
                        w * 97 + i,
                    )
                    .await;
                }
            }
        })
        .await;

    rss_sampler_stop.store(true, Ordering::Relaxed);
    let _ = rss_sampler.await;
    let after = perf_snapshot();
    if cooldown_ms > 0 {
        sleep(Duration::from_millis(cooldown_ms)).await;
    }
    let settled = perf_snapshot();
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    let rss_peak = match rss_peak_atomic.load(Ordering::Relaxed) {
        0 => None,
        peak => Some(peak),
    };

    println!(
        "N1 upload RSS probe: workers={} uploads_per_worker={} object_bytes={} part_bytes={} total_uploads={} total_mib={:.2} elapsed_s={:.3} throughput_mib_s={:.2} rss_before_mib={} rss_after_mib={} rss_peak_mib={} rss_settled_mib={} hwm_mib={}",
        workers,
        uploads_per_worker,
        object_bytes,
        part_bytes,
        total_uploads,
        total_bytes as f64 / (1024.0 * 1024.0),
        elapsed,
        (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed,
        before
            .rss_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        after
            .rss_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        rss_peak
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        settled
            .rss_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        settled
            .hwm_mib
            .map(|v| v.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
    );

    if max_peak_mib > 0 {
        let observed = rss_peak
            .or(settled.hwm_mib)
            .expect("RSS/HWM metric unavailable on this platform");
        assert!(
            observed <= max_peak_mib,
            "upload RSS peak exceeded: {} MiB > {} MiB",
            observed,
            max_peak_mib
        );
    }

    if max_settle_delta_mib > 0 {
        let before_rss = before
            .rss_mib
            .expect("RSS metric unavailable on this platform for before snapshot");
        let settled_rss = settled
            .rss_mib
            .expect("RSS metric unavailable on this platform for settled snapshot");
        let delta = settled_rss.saturating_sub(before_rss);
        assert!(
            delta <= max_settle_delta_mib,
            "upload RSS settled delta exceeded: {} MiB > {} MiB",
            delta,
            max_settle_delta_mib
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_archive_prefix_and_delimiter_tar_stream() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let keys = vec![
        "arch/root-a.bin".to_string(),
        "arch/root-b.bin".to_string(),
        "arch/sub/deep-a.bin".to_string(),
        "arch/sub/deeper/deep-b.bin".to_string(),
        "other/outside.bin".to_string(),
    ];

    for (i, key) in keys.iter().enumerate() {
        let content = (0..(2048 + i * 19))
            .map(|n| ((n + i) % 251) as u8)
            .collect::<Vec<u8>>();
        put_object(state.clone(), &fragment, key, content).await;
    }

    let resp = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        AxumJson(DownloadArchiveRequest {
            keys: vec![],
            prefix: Some("arch".to_string()),
            delimiter: Some("/".to_string()),
            max_keys: Some(1000),
            continuation_token: None,
            format: "tar".to_string(),
            filename: Some("by-prefix".to_string()),
        }),
    )
    .await
    .expect("archive by prefix delimiter")
    .into_response();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("archive bytes");
    let mut names = parse_tar_entry_names(&body);
    names.sort();
    assert_eq!(
        names,
        vec!["arch/root-a.bin".to_string(), "arch/root-b.bin".to_string()]
    );

    let resp_recursive = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        AxumJson(DownloadArchiveRequest {
            keys: vec![],
            prefix: Some("arch".to_string()),
            delimiter: None,
            max_keys: Some(1000),
            continuation_token: None,
            format: "tar".to_string(),
            filename: Some("recursive".to_string()),
        }),
    )
    .await
    .expect("archive recursive prefix")
    .into_response();
    assert_eq!(resp_recursive.status(), StatusCode::OK);
    let body_recursive = to_bytes(resp_recursive.into_body(), usize::MAX)
        .await
        .expect("archive recursive bytes");
    let mut recursive_names = parse_tar_entry_names(&body_recursive);
    recursive_names.sort();
    assert_eq!(
        recursive_names,
        vec![
            "arch/root-a.bin".to_string(),
            "arch/root-b.bin".to_string(),
            "arch/sub/deep-a.bin".to_string(),
            "arch/sub/deeper/deep-b.bin".to_string(),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_archive_prefix_pagination_continuation_token() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let keys = vec![
        "page/a.bin".to_string(),
        "page/b.bin".to_string(),
        "page/c.bin".to_string(),
        "page/d.bin".to_string(),
        "page/e.bin".to_string(),
    ];
    for (i, key) in keys.iter().enumerate() {
        let content = (0..(1024 + i * 7))
            .map(|n| ((n + i) % 251) as u8)
            .collect::<Vec<u8>>();
        put_object(state.clone(), &fragment, key, content).await;
    }

    let resp1 = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        AxumJson(DownloadArchiveRequest {
            keys: vec![],
            prefix: Some("page".to_string()),
            delimiter: None,
            max_keys: Some(2),
            continuation_token: None,
            format: "tar".to_string(),
            filename: Some("p1".to_string()),
        }),
    )
    .await
    .expect("page 1 archive")
    .into_response();
    assert_eq!(resp1.status(), StatusCode::OK);
    let tok1 = resp1
        .headers()
        .get("x-n1-archive-next-token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .expect("page 1 next token");
    let body1 = to_bytes(resp1.into_body(), usize::MAX)
        .await
        .expect("page1 body");
    let mut names1 = parse_tar_entry_names(&body1);
    names1.sort();
    assert_eq!(
        names1,
        vec!["page/a.bin".to_string(), "page/b.bin".to_string()]
    );

    let resp2 = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        AxumJson(DownloadArchiveRequest {
            keys: vec![],
            prefix: Some("page".to_string()),
            delimiter: None,
            max_keys: Some(2),
            continuation_token: Some(tok1),
            format: "tar".to_string(),
            filename: Some("p2".to_string()),
        }),
    )
    .await
    .expect("page 2 archive")
    .into_response();
    assert_eq!(resp2.status(), StatusCode::OK);
    let tok2 = resp2
        .headers()
        .get("x-n1-archive-next-token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .expect("page 2 next token");
    let body2 = to_bytes(resp2.into_body(), usize::MAX)
        .await
        .expect("page2 body");
    let mut names2 = parse_tar_entry_names(&body2);
    names2.sort();
    assert_eq!(
        names2,
        vec!["page/c.bin".to_string(), "page/d.bin".to_string()]
    );

    let resp3 = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        AxumJson(DownloadArchiveRequest {
            keys: vec![],
            prefix: Some("page".to_string()),
            delimiter: None,
            max_keys: Some(2),
            continuation_token: Some(tok2),
            format: "tar".to_string(),
            filename: Some("p3".to_string()),
        }),
    )
    .await
    .expect("page 3 archive")
    .into_response();
    assert_eq!(resp3.status(), StatusCode::OK);
    assert!(
        resp3.headers().get("x-n1-archive-next-token").is_none(),
        "last page should not carry continuation token"
    );
    let body3 = to_bytes(resp3.into_body(), usize::MAX)
        .await
        .expect("page3 body");
    let mut names3 = parse_tar_entry_names(&body3);
    names3.sort();
    assert_eq!(names3, vec!["page/e.bin".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_archive_prefix_unbounded_streams_all_pages_as_tar() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    // Must exceed ARCHIVE_LIST_PAGE_LIMIT (8 under cfg(test)) to prove multi-page unbounded streaming.
    let total = 17usize;
    stream::iter(0..total)
        .map(|i| {
            let state = state.clone();
            let fragment = fragment.clone();
            async move {
                let key = format!("bulk-recover/file-{:04}.bin", i);
                let content = vec![(i % 251) as u8; 128];
                put_object(state, &fragment, &key, content).await;
            }
        })
        .buffer_unordered(24)
        .collect::<Vec<_>>()
        .await;

    let response = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        AxumJson(DownloadArchiveRequest {
            keys: vec![],
            prefix: Some("bulk-recover".to_string()),
            delimiter: None,
            max_keys: None,
            continuation_token: None,
            format: "zip".to_string(),
            filename: Some("full-recovery".to_string()),
        }),
    )
    .await
    .expect("unbounded prefix archive")
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/x-tar"),
        "unbounded prefix mode must force tar"
    );
    assert!(
        response.headers().get("x-n1-archive-next-token").is_none(),
        "unbounded prefix mode should stream all pages in one response"
    );

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("archive body");
    let mut names = parse_tar_entry_names(&body);
    names.sort();
    assert_eq!(names.len(), total, "all prefix keys should be archived");
    assert_eq!(
        names.first().map(String::as_str),
        Some("bulk-recover/file-0000.bin")
    );
    let last_expected = format!("bulk-recover/file-{:04}.bin", total - 1);
    assert_eq!(
        names.last().map(String::as_str),
        Some(last_expected.as_str())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "index mirror HA soak; run manually with N1_INDEX_SOAK_* env knobs"]
async fn matrix_index_mirror_ha_soak_down_recover() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(4096, Ordering::Relaxed);

    let rounds = test_env_usize("N1_INDEX_SOAK_ROUNDS", 5, 1, 128);
    let objects_per_round = test_env_usize("N1_INDEX_SOAK_OBJECTS", 8, 1, 256);
    let enable_redist = test_env_bool("N1_INDEX_SOAK_REDIST", true);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let mut expected: HashMap<String, Vec<u8>> = HashMap::new();
    for i in 0..4usize {
        let key = format!("idx-soak/seed-{}.bin", i);
        let content = (0..(16 * 1024 + i * 31))
            .map(|n| ((n + i) % 251) as u8)
            .collect::<Vec<u8>>();
        put_object(state.clone(), &fragment, &key, content.clone()).await;
        expected.insert(key, content);
    }

    for round in 0..rounds {
        let down_src = base.join("index_2");
        let down_dst = base.join("index_2.down");
        if fs::try_exists(&down_dst).await.unwrap_or(false) {
            let _ = fs::remove_dir_all(&down_dst).await;
        }
        fs::rename(&down_src, &down_dst)
            .await
            .expect("simulate index disk down");
        fs::write(&down_src, b"n1-index-disk-down-blocker")
            .await
            .expect("create index down blocker file");

        for (key, bytes) in &expected {
            let got = read_object_bytes_eventually(fragment.clone(), key.clone(), 24).await;
            assert_eq!(got, *bytes, "round {} read mismatch {}", round, key);
        }

        if enable_redist {
            DATA_SHARDS_ATOMIC
                .get()
                .expect("DATA_SHARDS_ATOMIC")
                .store(2, Ordering::Relaxed);
            PARITY_SHARDS_ATOMIC
                .get()
                .expect("PARITY_SHARDS_ATOMIC")
                .store(1, Ordering::Relaxed);
            run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
                .await
                .expect("redistribution while one index disk down");
        }

        fs::remove_file(&down_src)
            .await
            .expect("remove index down blocker file");
        fs::rename(&down_dst, &down_src)
            .await
            .expect("restore index disk");

        run_repair_job(
            RepairRunConfig {
                dry_run: false,
                check_mode: RepairCheckMode::Scrub,
                meta_concurrency: 8,
                shard_repair_concurrency: 2,
                meta_batch_size: 512,
            },
            Arc::new(WsConnectionManager::new()),
        )
        .await
        .expect("repair after index restore");

        assert_all_index_disks_synchronized(&fragment).await;

        for i in 0..objects_per_round {
            let key = format!("idx-soak/r{}/obj-{}.bin", round, i);
            let content = (0..(10 * 1024 + round * 41 + i * 13))
                .map(|n| ((n * 3 + round + i) % 251) as u8)
                .collect::<Vec<u8>>();
            put_object(state.clone(), &fragment, &key, content.clone()).await;
            expected.insert(key, content);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_repro_delete_multiple_restore_all_listing_blank() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let prefix = "repro-blank";
    let count = 5;
    let mut keys = Vec::new();

    for i in 0..count {
        let key = format!("{}/obj-{}.bin", prefix, i);
        put_object(state.clone(), &fragment, &key, vec![0xAA; 10]).await;
        keys.push(key);
    }

    let initial = list_objects_json(&fragment, None, None).await;
    let initial_keys = initial["objects"]
        .as_array()
        .expect("objects array")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .filter(|key| key.starts_with("repro-blank/"))
        .count();
    assert_eq!(initial_keys, count, "initial listing mismatch");

    for key in &keys {
        let resp = delete_object(
            State(state.clone()),
            full_claims(&fragment),
            AxumPath((fragment.clone(), key.clone())),
        )
        .await
        .expect("delete object")
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let empty = list_objects_json(&fragment, None, None).await;
    let empty_keys = empty["objects"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v["object_key"].as_str())
                .filter(|key| key.starts_with("repro-blank/"))
                .count()
        })
        .unwrap_or(0);
    assert_eq!(empty_keys, 0, "listing should be empty after delete");

    for key in &keys {
        let resp = recover_object(
            State(state.clone()),
            full_claims(&fragment),
            AxumPath((fragment.clone(), key.clone())),
        )
        .await
        .expect("recover object")
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let restored = list_objects_json(&fragment, None, None).await;
    let restored_keys = restored["objects"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v["object_key"].as_str())
                .filter(|key| key.starts_with("repro-blank/"))
                .count()
        })
        .unwrap_or(0);

    assert_eq!(
        restored_keys, count,
        "listing should be restored after recover"
    );
}
