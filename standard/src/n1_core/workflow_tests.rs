use super::archive::{download_manifest_archive, download_selected_archive};
use super::delete::delete_object;
use super::delete::{
    BulkObjectKeysRequest, delete_object_permanently, delete_object_version, delete_objects_bulk,
    recover_object, recover_object_version, recover_objects_bulk,
};
use super::disk_manager::{
    DownloadArchiveManifestQuery, DownloadArchiveRequest, InitiateUploadParams,
    ListObjectsResult, ObjectKeyQuery, ObjectPathManager, RenameObjectRequest, UploadPartQuery,
};
use super::fragment_metadata::init_fragment_metadata;
use super::garbage_collector::{get_gc_status_api, run_garbage_collector};
use super::index;
use super::metadata_parser::get_metadata;
use super::modify::rename_object;
use super::object_parser::{
    STREAM_CHUNK_SIZE_ATOMIC, STREAM_LOCALITY_BIAS_ENABLED_ATOMIC, STREAM_MAX_CONCURRENCY,
    STREAM_READAHEAD_BYTES_ATOMIC,
};
use super::operation_gate::MaintenanceAccessMode;
use super::quota_manager::init_quota_file;
use super::recovery::verify_shard_integrity;
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
use super::storage_capacity_parser::get_capacity_usage;
use super::sync::ensure_fragment_blob_directories;
use super::thumbnail::{ThumbnailQuery, get_object_thumbnail};
use super::upload::{
    DATA_SHARDS_ATOMIC, MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC, PARITY_SHARDS_ATOMIC,
    STRIPE_SIZE_ATOMIC, UPLOAD_IO_GATE_ENABLED_ATOMIC, UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC,
    WRITER_BUFFER_SIZE_ATOMIC, finalize_object_upload, get_upload_status, initiate_object_upload,
    object_upload_part, upload_object_one_shot,
};
use crate::n1_core::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
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
use axum::Json;
use futures_util::{StreamExt, stream};
use image::{ColorType, ImageBuffer, ImageEncoder, Rgba, codecs::png::PngEncoder};
use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_tar::Archive as TarArchive;
use ulid::Ulid;

static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
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
    ensure_test_config_file();

    let index_1 = base.join("index_1");
    let index_2 = base.join("index_2");
    let blob_1 = base.join("blob_1");
    let blob_2 = base.join("blob_2");
    let blob_3 = base.join("blob_3");

    let _ = fs::create_dir_all(&index_1).await;
    let _ = fs::create_dir_all(&index_2).await;
    let _ = fs::create_dir_all(&blob_1).await;
    let _ = fs::create_dir_all(&blob_2).await;
    let _ = fs::create_dir_all(&blob_3).await;

    let index_roots = vec![
        index_1.to_string_lossy().to_string(),
        index_2.to_string_lossy().to_string(),
    ];
    let blob_roots = vec![
        blob_1.to_string_lossy().to_string(),
        blob_2.to_string_lossy().to_string(),
        blob_3.to_string_lossy().to_string(),
    ];

    if INDEX_STORE_DISKS.get().is_none() {
        INDEX_STORE_DISKS
            .set(index_roots)
            .expect("set INDEX_STORE_DISKS");
    }
    if BLOB_STORE_DISKS.get().is_none() {
        BLOB_STORE_DISKS
            .set(blob_roots)
            .expect("set BLOB_STORE_DISKS");
    }
    if FS_STORAGE_ROOT.get().is_none() {
        FS_STORAGE_ROOT
            .set(base.to_string_lossy().to_string())
            .expect("set FS_STORAGE_ROOT");
    }
}

async fn restore_simulated_index_disk(
    down_src: &std::path::Path,
    down_dst: &std::path::Path,
    label: &str,
) {
    if fs::try_exists(down_src).await.unwrap_or(false) {
        match fs::metadata(down_src).await {
            Ok(meta) if meta.is_dir() => {
                let _ = fs::remove_dir_all(down_src).await;
            }
            Ok(_) => {
                let _ = fs::remove_file(down_src).await;
            }
            Err(_) => {}
        }
    }

    fs::rename(down_dst, down_src).await.expect(label);
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

async fn get_thumbnail_response(
    fragment: &str,
    object_key: &str,
    query: ThumbnailQuery,
) -> Result<axum::response::Response, crate::enums::app_error_enum::AppError> {
    get_object_thumbnail(
        full_claims(fragment),
        AxumPath((fragment.to_string(), object_key.to_string())),
        Query(query),
    )
    .await
    .map(IntoResponse::into_response)
}

fn sample_png_bytes(width: u32, height: u32) -> Vec<u8> {
    let mut image = ImageBuffer::<Rgba<u8>, Vec<u8>>::new(width, height);
    for (x, y, pixel) in image.enumerate_pixels_mut() {
        *pixel = Rgba([
            ((x * 17) % 255) as u8,
            ((y * 31) % 255) as u8,
            (((x + y) * 7) % 255) as u8,
            255,
        ]);
    }

    let rgba = image.into_raw();
    let mut out = Cursor::new(Vec::new());
    let encoder = PngEncoder::new(&mut out);
    encoder
        .write_image(&rgba, width, height, ColorType::Rgba8.into())
        .expect("encode sample png");
    out.into_inner()
}

fn print_perf_stage(stage: &PerfStageResult) {
    println!(
        "N1 CRUD probe stage={} ops={} bytes={} wall_s={:.3} cpu_s={} rss_before_mib={} rss_after_mib={} rss_peak_mib={} hwm_mib={}",
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

async fn read_response_bytes(resp: axum::response::Response) -> Vec<u8> {
    to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read response body")
        .to_vec()
}

async fn read_tar_entries(bytes: Vec<u8>) -> BTreeMap<String, Vec<u8>> {
    let (mut writer, reader) = tokio::io::duplex(bytes.len().max(4096));
    tokio::spawn(async move {
        writer
            .write_all(&bytes)
            .await
            .expect("write tar bytes into duplex");
    });
    let mut archive = TarArchive::new(reader);
    let mut entries = archive.entries().expect("tar entries");
    let mut out = BTreeMap::new();

    while let Some(entry) = entries.next().await {
        let mut entry = entry.expect("tar entry");
        let path = entry
            .path()
            .expect("tar path")
            .to_string_lossy()
            .to_string();
        let mut body = Vec::new();
        entry
            .read_to_end(&mut body)
            .await
            .expect("read tar entry body");
        out.insert(path, body);
    }

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

async fn run_manual_gc_and_wait(state: Arc<AppState>) {
    let baseline = get_gc_status_api(full_claims("gc-status"))
        .await
        .expect("gc status baseline")
        .0;
    let baseline_start = baseline.start_timestamp;

    let start = run_garbage_collector(State(state)).await.into_response();
    assert_eq!(start.status(), StatusCode::ACCEPTED, "manual GC must start");

    let deadline = Instant::now() + std::time::Duration::from_secs(20);
    let mut saw_new_run = false;
    loop {
        let status = get_gc_status_api(full_claims("gc-status"))
            .await
            .expect("gc status");
        if status.0.start_timestamp != baseline_start {
            saw_new_run = true;
        }
        if saw_new_run && !status.0.is_running {
            break;
        }
        if Instant::now() >= deadline {
            panic!("manual GC did not finish in time");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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

    for attempt in 0..8usize {
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
        if let Ok(resp) = get_object(
            claims,
            AxumPath((fragment.clone(), object_key.clone())),
            req,
        )
        .await
        {
            if resp.status() == StatusCode::OK {
                if let Ok(body) = to_bytes(resp.into_body(), usize::MAX).await {
                    return body.to_vec();
                }
            }
        }

        if attempt < 7 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            continue;
        }
    }

    panic!(
        "read object body failed after retries for {}/{}",
        fragment, object_key
    );
}

async fn read_object_bytes_eventually(
    fragment: String,
    object_key: String,
    attempts: usize,
) -> Vec<u8> {
    for _ in 0..attempts {
        let req = Request::builder()
            .uri("/v4/fragment/get")
            .body(Body::empty())
            .expect("build request");

        if let Ok(resp) = get_object(
            full_claims(&fragment),
            AxumPath((fragment.clone(), object_key.clone())),
            req,
        )
        .await
        {
            if resp.status() == StatusCode::OK {
                if let Ok(body) = to_bytes(resp.into_body(), usize::MAX).await {
                    return body.to_vec();
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    panic!(
        "object {} in fragment {} was not readable after {} attempts",
        object_key, fragment, attempts
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

            if let Ok(bytes) = fs::read(path).await {
                let digest = Sha256::digest(&bytes);
                map.insert(rel, hex::encode(digest));
            }
        }

        out.push((root.to_string_lossy().to_string(), map));
    }

    out
}

async fn assert_all_index_disks_synchronized(fragment: &str) {
    let snapshots = collect_index_tree_hashes(fragment).await;
    assert!(
        snapshots.len() >= 2,
        "expected at least 2 index roots, got {}",
        snapshots.len()
    );

    let (base_root, base_map) = &snapshots[0];
    for (root, map) in snapshots.iter().skip(1) {
        assert_eq!(
            map, base_map,
            "index mirrors diverged: {} vs {}",
            base_root, root
        );
    }
}

async fn upload_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
    bytes: Vec<u8>,
) -> axum::response::Response {
    upload_part_with_options(fragment, version_id, part_index, bytes, None, None).await
}

async fn upload_one_shot(
    state: Arc<AppState>,
    fragment: &str,
    object_key: &str,
    bytes: Vec<u8>,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-length",
        HeaderValue::from_str(&bytes.len().to_string()).expect("content-length"),
    );
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );

    upload_object_one_shot(
        State(state),
        full_claims(fragment),
        headers,
        AxumPath((fragment.to_string(), object_key.to_string())),
        Query(UploadPartQuery {
            checksum: None,
            durability: Some("batched".to_string()),
            strict_durability: None,
            io_gate: None,
        }),
        Body::from(bytes),
    )
    .await
    .expect("upload one-shot")
    .into_response()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_thumbnail_endpoint_generates_and_reuses_disk_cache() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let key = "images/gallery/sample.png";
    let png = sample_png_bytes(640, 480);
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let upload_resp = upload_one_shot(state, &fragment, key, png).await;
    assert_eq!(upload_resp.status(), StatusCode::CREATED);

    let query = ThumbnailQuery {
        w: Some(128),
        h: Some(128),
        fit: Some("contain".to_string()),
        format: Some("png".to_string()),
        version_id: None,
    };

    let resp1 = get_thumbnail_response(&fragment, key, query.clone())
        .await
        .expect("thumbnail response 1");
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_eq!(
        resp1
            .headers()
            .get("x-n1-thumbnail-cache")
            .and_then(|v| v.to_str().ok()),
        Some("miss")
    );
    assert_eq!(
        resp1.headers().get(header::CONTENT_TYPE),
        Some(&HeaderValue::from_static("image/png"))
    );
    let body1 = to_bytes(resp1.into_body(), usize::MAX)
        .await
        .expect("thumbnail body 1");
    let thumb1 = image::load_from_memory(&body1).expect("decode thumbnail 1");
    assert_eq!(thumb1.width(), 128);
    assert_eq!(thumb1.height(), 96);

    let resp2 = get_thumbnail_response(&fragment, key, query)
        .await
        .expect("thumbnail response 2");
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_eq!(
        resp2
            .headers()
            .get("x-n1-thumbnail-cache")
            .and_then(|v| v.to_str().ok()),
        Some("hit")
    );
    let body2 = to_bytes(resp2.into_body(), usize::MAX)
        .await
        .expect("thumbnail body 2");
    assert_eq!(body1, body2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_thumbnail_endpoint_rejects_non_image_objects() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let key = "docs/readme.txt";
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let upload_resp = upload_one_shot(state, &fragment, key, b"hello thumbnail".to_vec()).await;
    assert_eq!(upload_resp.status(), StatusCode::CREATED);

    let err = get_thumbnail_response(
        &fragment,
        key,
        ThumbnailQuery {
            w: Some(128),
            h: Some(128),
            fit: None,
            format: None,
            version_id: None,
        },
    )
    .await
    .expect_err("non-image thumbnail should fail");

    assert!(matches!(
        err,
        crate::enums::app_error_enum::AppError::BadRequest(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_upload_one_shot_publishes_without_active_session() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "oneshot/small.bin";
    let content = (0..(64 * 1024 + 123))
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

    let resp = upload_one_shot(state, &fragment, key, content.clone()).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let meta_json = read_response_json(resp).await;
    let version_id = meta_json["version_id"]
        .as_str()
        .expect("version_id")
        .to_string();

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.to_string()).await;
    assert_eq!(got, content);

    let meta = index::read_object_version(&fragment, key, &version_id)
        .await
        .expect("read version");
    assert_eq!(meta.total_size_bytes, content.len() as u64);
    assert_eq!(meta.parts.len(), 1);
    assert_eq!(meta.parts[0].part_index, 0);

    let sessions = index::list_active_sessions(&fragment)
        .await
        .expect("list active sessions");
    assert!(sessions.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_capacity_usage_reports_live_filesystem_totals() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 64 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "test-fragment-key")
        .await
        .expect("init fragment metadata");

    let resp = get_capacity_usage(full_claims(&fragment), AxumPath(fragment.clone()))
        .await
        .expect("capacity usage")
        .into_response();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = read_response_json(resp).await;
    let allocated = json["capacity_allocated"]
        .as_u64()
        .expect("capacity_allocated");
    let used = json["capacity_used"].as_u64().expect("capacity_used");
    let available = json["capacity_available"]
        .as_u64()
        .expect("capacity_available");
    let filesystems = json["filesystem_count"].as_u64().expect("filesystem_count");

    assert!(
        allocated > 0,
        "allocated capacity must come from real filesystems"
    );
    assert!(used <= allocated, "used must not exceed allocated");
    assert_eq!(
        allocated.saturating_sub(used),
        available,
        "available should match allocated-used"
    );
    assert!(filesystems >= 1, "must report at least one filesystem");
}

async fn init_upload(
    fragment: &str,
    object_key: &str,
    total_size: u64,
    expected_parts: u64,
) -> (String, String) {
    init_upload_with_part_size(fragment, object_key, total_size, expected_parts, None).await
}

async fn init_upload_with_part_size(
    fragment: &str,
    object_key: &str,
    total_size: u64,
    expected_parts: u64,
    part_size_bytes: Option<u64>,
) -> (String, String) {
    let init_resp = initiate_object_upload(
        full_claims(fragment),
        AxumPath((fragment.to_string(), object_key.to_string())),
        AxumJson(InitiateUploadParams {
            content_type: Some("application/octet-stream".into()),
            total_size_bytes: total_size,
            expected_parts,
            part_size_bytes: part_size_bytes.or_else(|| {
                if expected_parts <= 1 {
                    Some(total_size)
                } else {
                    Some(total_size.div_ceil(expected_parts))
                }
            }),
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
    finalize_upload_with_part_size(
        state,
        fragment,
        version_id,
        session_id,
        object_key,
        total_size,
        expected_parts,
        None,
    )
    .await
}

async fn finalize_upload_with_part_size(
    state: Arc<AppState>,
    fragment: &str,
    version_id: &str,
    session_id: &str,
    object_key: &str,
    total_size: u64,
    expected_parts: u64,
    part_size_bytes: Option<u64>,
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
            part_size_bytes: part_size_bytes.or_else(|| {
                if expected_parts <= 1 {
                    Some(total_size)
                } else {
                    Some(total_size.div_ceil(expected_parts))
                }
            }),
        }),
    )
    .await
    .expect("finalize upload")
    .into_response()
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
    let (version_id, session_id) = init_upload_with_part_size(
        fragment,
        object_key,
        object_bytes as u64,
        expected_parts,
        Some(part_bytes as u64),
    )
    .await;

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

    let finalize_resp = finalize_upload_with_part_size(
        state,
        fragment,
        &version_id,
        &session_id,
        object_key,
        object_bytes as u64,
        expected_parts,
        Some(part_bytes as u64),
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

    let mut damaged_file = fs::OpenOptions::new()
        .write(true)
        .open(&target_shard.absolute_path)
        .await
        .expect("open shard for corruption");
    damaged_file
        .seek(std::io::SeekFrom::Start(0))
        .await
        .expect("seek shard");
    damaged_file
        .write_all(&vec![0xA5; 4096])
        .await
        .expect("corrupt shard");
    damaged_file.flush().await.expect("flush corruption");
    drop(damaged_file);

    let mut before_repair = fs::File::open(&target_shard.absolute_path)
        .await
        .expect("open shard for verify");
    assert!(
        !verify_shard_integrity(&mut before_repair, &target_shard.checksum).await,
        "corruption should break checksum before repair"
    );

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            target_fragment: None,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("run repair");

    let mut after_repair = fs::File::open(&target_shard.absolute_path)
        .await
        .expect("open repaired shard");
    assert!(
        verify_shard_integrity(&mut after_repair, &target_shard.checksum).await,
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

    let soft_deleted_meta = get_metadata(&fragment, &renamed_key, &fragment).await;
    assert!(
        soft_deleted_meta.is_err(),
        "soft delete should remove the live latest metadata"
    );
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
    let part_size = part0.len() as u64;
    let (version_id, session_id) =
        init_upload_with_part_size(&fragment, &key, combined_len, 2, Some(part_size)).await;

    let put0 = upload_part(&fragment, &version_id, 0, part0.clone()).await;
    assert_eq!(put0.status(), StatusCode::OK);

    let finalize_conflict = finalize_upload_with_part_size(
        state.clone(),
        &fragment,
        &version_id,
        &session_id,
        &key,
        combined_len,
        2,
        Some(part_size),
    )
    .await;
    assert_eq!(finalize_conflict.status(), StatusCode::CONFLICT);
    let conflict_json = read_response_json(finalize_conflict).await;
    assert_eq!(conflict_json["missing_parts"][0].as_u64(), Some(1));

    let put1 = upload_part(&fragment, &version_id, 1, part1.clone()).await;
    assert_eq!(put1.status(), StatusCode::OK);

    let finalize_ok = finalize_upload_with_part_size(
        state,
        &fragment,
        &version_id,
        &session_id,
        &key,
        combined_len,
        2,
        Some(part_size),
    )
    .await;
    assert_eq!(finalize_ok.status(), StatusCode::OK);

    let expected = [part0, part1].concat();
    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, expected);
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
    let part0 = vec![0x11; 10 * 1024 + 3];
    let part1 = vec![0x22; 10 * 1024 + 3];
    let part2 = vec![0x33; 7 * 1024 + 5];
    let total_len = (part0.len() + part1.len() + part2.len()) as u64;
    let part_size = part0.len() as u64;

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
    let (version_id, session_id) =
        init_upload_with_part_size(&fragment, &key, total_len, 3, Some(part_size)).await;

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

    let finalize_conflict = finalize_upload_with_part_size(
        state.clone(),
        &fragment,
        &version_id,
        &session_id,
        &key,
        total_len,
        3,
        Some(part_size),
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

    let finalize_ok = finalize_upload_with_part_size(
        state,
        &fragment,
        &version_id,
        &session_id,
        &key,
        total_len,
        3,
        Some(part_size),
    )
    .await;
    assert_eq!(finalize_ok.status(), StatusCode::OK);

    let expected = [part0, part1, part2].concat();
    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_multipart_compact_receipts_present_without_session_part_metadata() {
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
    let key = "matrix/compact-receipts.bin".to_string();
    let part0 = vec![0x41; 9 * 1024 + 7];
    let part1 = vec![0x52; 9 * 1024 + 7];
    let total_len = (part0.len() + part1.len()) as u64;
    let part_size = part0.len() as u64;

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
    let (version_id, session_id) =
        init_upload_with_part_size(&fragment, &key, total_len, 2, Some(part_size)).await;

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

    assert!(
        index::read_session_part(&fragment, &version_id, 0)
            .await
            .expect("read session part 0")
            .is_none(),
        "new multipart flow should not persist full session part metadata"
    );
    assert!(
        index::read_session_part(&fragment, &version_id, 1)
            .await
            .expect("read session part 1")
            .is_none(),
        "new multipart flow should not persist full session part metadata"
    );

    let manifest = index::load_session(&fragment, &version_id)
        .await
        .expect("load session")
        .expect("session exists")
        .manifest;
    let receipts = index::load_session_part_receipts(&fragment, &version_id, &manifest)
        .await
        .expect("load compact receipts")
        .expect("compact receipts exist");
    assert_eq!(receipts.len(), 2);
    assert_eq!(
        receipts[0].as_ref().map(|receipt| receipt.size_bytes),
        Some(part0.len() as u64)
    );
    assert_eq!(
        receipts[1].as_ref().map(|receipt| receipt.size_bytes),
        Some(part1.len() as u64)
    );
    assert_eq!(
        receipts[0]
            .as_ref()
            .map(|receipt| receipt.shard_checksums.len()),
        Some(1)
    );

    let finalize_ok = finalize_upload_with_part_size(
        state,
        &fragment,
        &version_id,
        &session_id,
        &key,
        total_len,
        2,
        Some(part_size),
    )
    .await;
    assert_eq!(finalize_ok.status(), StatusCode::OK);

    let expected = [part0, part1].concat();
    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, expected);
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
async fn matrix_delete_version_again_permanently_removes_only_trashed_version() {
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
    let key = "matrix/delete-version-again.bin".to_string();
    let v1 = b"one".to_vec();
    let v2 = b"two-newest".to_vec();

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

    let first_delete = delete_object_version(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), ver1.clone(), key.clone())),
    )
    .await
    .expect("first delete")
    .into_response();
    assert_eq!(first_delete.status(), StatusCode::OK);

    let second_delete = delete_object_version(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), ver1.clone(), key.clone())),
    )
    .await
    .expect("second delete")
    .into_response();
    assert_eq!(second_delete.status(), StatusCode::NO_CONTENT);

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, v2, "latest live version should stay readable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_live_listing_without_prefix_uses_partition_pagination() {
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

    for (key, fill) in [
        ("alpha.bin", 1u8),
        ("beta.bin", 2u8),
        ("gamma.bin", 3u8),
        ("omega/final.bin", 4u8),
    ] {
        put_object(state.clone(), &fragment, key, vec![fill; 2048]).await;
    }

    let page1 = list_objects_json_page(&fragment, None, None, 2, None).await;
    let token1 = page1["continuation_token"]
        .as_str()
        .expect("page1 continuation")
        .to_string();
    let page2 = list_objects_json_page(&fragment, None, None, 2, Some(token1)).await;

    let mut keys = Vec::new();
    keys.extend(
        page1["objects"]
            .as_array()
            .expect("page1 objects")
            .iter()
            .filter_map(|v| v["object_key"].as_str())
            .map(|s| s.to_string()),
    );
    keys.extend(
        page2["objects"]
            .as_array()
            .expect("page2 objects")
            .iter()
            .filter_map(|v| v["object_key"].as_str())
            .map(|s| s.to_string()),
    );
    keys.sort();
    keys.dedup();

    assert!(
        keys.iter().any(|k| k == "alpha.bin"),
        "missing alpha in paged listing: {:?}",
        keys
    );
    assert!(
        keys.iter().any(|k| k == "beta.bin"),
        "missing beta in paged listing: {:?}",
        keys
    );
    assert!(
        keys.iter().any(|k| k == "gamma.bin"),
        "missing gamma in paged listing: {:?}",
        keys
    );
    assert!(
        keys.iter().any(|k| k == "omega/final.bin"),
        "missing omega/final in paged listing: {:?}",
        keys
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_gc_keeps_live_data_and_cleans_trash_in_partition_layout() {
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

    let key_live = "gc/live.bin".to_string();
    let key_trash = "gc/trash.bin".to_string();
    let live_payload = vec![0x11; 4096];

    put_object(state.clone(), &fragment, &key_live, live_payload.clone()).await;
    put_object(state.clone(), &fragment, &key_trash, vec![0x22; 4096]).await;

    let del_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key_trash.clone())),
    )
    .await
    .expect("soft delete for gc")
    .into_response();
    assert_eq!(del_resp.status(), StatusCode::OK);

    run_manual_gc_and_wait(state).await;

    let live_after = read_object_bytes(full_claims(&fragment), fragment.clone(), key_live).await;
    assert_eq!(live_after, live_payload, "GC must not remove live object");

    let trash_listing = list_trash_json_page(&fragment, None, None, 100, None).await;
    let trashed_keys = trash_listing["objects"]
        .as_array()
        .expect("trash objects")
        .iter()
        .filter_map(|v| v["object_key"].as_str())
        .collect::<Vec<_>>();
    assert!(
        trashed_keys.iter().all(|k| *k != "gc/trash.bin"),
        "manual GC should clear trashed object in this test: {:?}",
        trashed_keys
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

    let blob3 = base.join("blob_3");
    let blob3_down = base.join("blob_3.down");
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
async fn matrix_archive_export_all_tar_respects_internal_filter() {
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

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let visible_key = "archive/visible.bin";
    let internal_key = "archive/.n1fsd_dir";
    let visible_payload = b"visible-bytes".to_vec();
    let internal_payload = b"internal-marker".to_vec();

    put_object(state.clone(), &fragment, visible_key, visible_payload.clone()).await;
    put_object(state, &fragment, internal_key, internal_payload.clone()).await;

    let export_resp = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Json(DownloadArchiveRequest {
            keys: Vec::new(),
            format: "zip".to_string(),
            filename: Some("fragment-export".to_string()),
            export_all: true,
            include_internal: false,
        }),
    )
    .await
    .expect("archive export")
    .into_response();
    assert_eq!(export_resp.status(), StatusCode::OK);
    assert_eq!(
        export_resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/x-tar")
    );

    let entries = read_tar_entries(read_response_bytes(export_resp).await).await;
    assert_eq!(entries.get(visible_key), Some(&visible_payload));
    assert!(
        !entries.contains_key(internal_key),
        "default fragment export should hide internal marker: {:?}",
        entries.keys().collect::<Vec<_>>()
    );

    let export_internal_resp = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment),
        Json(DownloadArchiveRequest {
            keys: Vec::new(),
            format: "tar".to_string(),
            filename: Some("fragment-export-internal".to_string()),
            export_all: true,
            include_internal: true,
        }),
    )
    .await
    .expect("archive export with internal")
    .into_response();
    assert_eq!(export_internal_resp.status(), StatusCode::OK);

    let entries_internal = read_tar_entries(read_response_bytes(export_internal_resp).await).await;
    assert_eq!(entries_internal.get(visible_key), Some(&visible_payload));
    assert_eq!(entries_internal.get(internal_key), Some(&internal_payload));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_archive_export_all_tar_scans_multiple_pages() {
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

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let object_count = 1003usize;
    for i in 0..object_count {
        let key = format!("archive/bulk/object-{i:04}.bin");
        let payload = format!("payload-{i:04}").into_bytes();
        let resp = upload_one_shot(state.clone(), &fragment, &key, payload).await;
        assert_eq!(resp.status(), StatusCode::CREATED, "upload {}", key);
    }

    let export_resp = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment),
        Json(DownloadArchiveRequest {
            keys: Vec::new(),
            format: "tar".to_string(),
            filename: Some("fragment-export-bulk".to_string()),
            export_all: true,
            include_internal: false,
        }),
    )
    .await
    .expect("archive export bulk")
    .into_response();
    assert_eq!(export_resp.status(), StatusCode::OK);

    let entries = read_tar_entries(read_response_bytes(export_resp).await).await;
    assert_eq!(
        entries.len(),
        object_count,
        "archive export should stream every live object across page boundaries"
    );
    assert_eq!(
        entries
            .get("archive/bulk/object-0000.bin")
            .map(Vec::as_slice),
        Some(b"payload-0000".as_slice())
    );
    assert_eq!(
        entries
            .get("archive/bulk/object-1002.bin")
            .map(Vec::as_slice),
        Some(b"payload-1002".as_slice())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_archive_manifest_tar_streams_selected_keys() {
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

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key_a = "manifest/alpha.bin";
    let key_b = "manifest/bravo.bin";
    let key_c = "manifest/charlie.bin";
    let payload_a = b"alpha".to_vec();
    let payload_b = b"bravo".to_vec();

    put_object(state.clone(), &fragment, key_a, payload_a.clone()).await;
    put_object(state.clone(), &fragment, key_b, payload_b.clone()).await;
    put_object(state, &fragment, key_c, b"charlie".to_vec()).await;

    let manifest = format!("{key_b}\nmissing/object.bin\n/{key_a}\n");
    let resp = download_manifest_archive(
        full_claims(&fragment),
        AxumPath(fragment),
        Query(DownloadArchiveManifestQuery {
            format: "tar".to_string(),
            filename: Some("manifest-export".to_string()),
        }),
        Body::from(manifest),
    )
    .await
    .expect("manifest archive export")
    .into_response();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-tar")
    );

    let entries = read_tar_entries(read_response_bytes(resp).await).await;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries.get(key_a), Some(&payload_a));
    assert_eq!(entries.get(key_b), Some(&payload_b));
    assert!(!entries.contains_key("missing/object.bin"));
    assert!(!entries.contains_key(key_c));
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

    let mut parity_file = fs::OpenOptions::new()
        .write(true)
        .open(&parity_shard.absolute_path)
        .await
        .expect("open parity shard");
    parity_file
        .seek(std::io::SeekFrom::Start(0))
        .await
        .expect("seek parity");
    parity_file
        .write_all(&vec![0x6E; 4096])
        .await
        .expect("corrupt parity shard");
    parity_file.flush().await.expect("flush parity corruption");
    drop(parity_file);

    let mut before = fs::File::open(&parity_shard.absolute_path)
        .await
        .expect("open parity for verify");
    assert!(
        !verify_shard_integrity(&mut before, &parity_shard.checksum).await,
        "parity checksum should fail before repair"
    );

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            target_fragment: None,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 2,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("repair after parity bitrot");

    let mut after = fs::File::open(&parity_shard.absolute_path)
        .await
        .expect("open repaired parity");
    assert!(
        verify_shard_integrity(&mut after, &parity_shard.checksum).await,
        "parity checksum should pass after repair"
    );

    let got = read_object_bytes(full_claims(&fragment), fragment, key).await;
    assert_eq!(got, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    let (pending_vid, _pending_sid) = init_upload(&fragment, "idx-sync/pending.bin", 8192, 1).await;
    let pending_chunk = vec![0xAB; 8192];
    let put_pending = upload_part(&fragment, &pending_vid, 0, pending_chunk).await;
    assert_eq!(put_pending.status(), StatusCode::OK);

    let session_dirs = pm.get_all_session_dirs(&fragment, &pending_vid);
    assert!(
        session_dirs.len() >= 2,
        "expected at least two session dirs"
    );
    let manifest0 = session_dirs[0].join("session.manifest.msgpack");
    let reservation1 = session_dirs[1].join("reservation.msgpack");
    let part0_1 = session_dirs[1].join("part_0.meta.msgpack");

    fs::write(&manifest0, b"corrupt-session-manifest")
        .await
        .expect("corrupt one session manifest mirror");
    fs::remove_file(&reservation1)
        .await
        .expect("delete one session reservation mirror");
    if fs::try_exists(&part0_1).await.unwrap_or(false) {
        fs::remove_file(&part0_1)
            .await
            .expect("delete one session part metadata mirror");
    }

    let before = collect_index_tree_hashes(&fragment).await;
    assert_ne!(
        before[0].1, before[1].1,
        "precondition failed: mirrors should be divergent before repair"
    );

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            target_fragment: None,
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
async fn matrix_repair_tolerates_missing_trashed_metadata_replica() {
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

    let marker_meta = index::read_latest_trashed_object(&fragment, &key)
        .await
        .expect("latest trashed metadata after delete");
    assert!(
        !marker_meta.is_delete_marker,
        "trashed metadata should remain a real object record"
    );

    let pm = ObjectPathManager::new();
    let marker_paths = pm.get_all_trashed_metadata_paths(&fragment, &key, &marker_meta.version_id);
    assert!(
        marker_paths.len() >= 2,
        "expected mirrored trashed metadata paths"
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
            target_fragment: None,
            check_mode: RepairCheckMode::Availability,
            meta_concurrency: 4,
            shard_repair_concurrency: 2,
            meta_batch_size: 128,
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("repair should tolerate missing trashed metadata replica");

    let trashed_meta_after = index::read_latest_trashed_object(&fragment, &key)
        .await
        .expect("latest trashed metadata should still be readable");
    assert_eq!(trashed_meta_after.object_key, key);
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
    restore_simulated_index_disk(&down_src, &down_dst, "restore index disk").await;

    run_repair_job(
        RepairRunConfig {
            dry_run: false,
            target_fragment: None,
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

    let down_src = base.join("index_1");
    let down_dst = base.join("index_1.down");
    if fs::try_exists(&down_dst).await.unwrap_or(false) {
        let _ = fs::remove_dir_all(&down_dst).await;
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
    restore_simulated_index_disk(&down_src, &down_dst, "restore primary index disk").await;

    let path_manager = ObjectPathManager::new();
    let meta_paths = path_manager.get_all_metadata_paths(&fragment, &key, &version_id);
    assert!(meta_paths.len() >= 2, "expected mirrored metadata paths");
    assert!(
        fs::try_exists(&meta_paths[1]).await.unwrap_or(false),
        "secondary metadata copy should exist after redistribution"
    );

    fs::write(&meta_paths[0], b"n1-corrupt-metadata-copy")
        .await
        .expect("corrupt primary metadata copy");

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

    let mut damaged = fs::OpenOptions::new()
        .write(true)
        .open(&target_shard.absolute_path)
        .await
        .expect("open shard for corruption");
    damaged
        .seek(std::io::SeekFrom::Start(0))
        .await
        .expect("seek corrupted shard");
    damaged
        .write_all(&vec![0x3C; 4096])
        .await
        .expect("corrupt shard");
    damaged.flush().await.expect("flush shard corruption");
    drop(damaged);

    let repair_task = tokio::spawn(run_repair_job(
        RepairRunConfig {
            dry_run: false,
            target_fragment: None,
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

    let mut repaired = fs::File::open(&target_shard.absolute_path)
        .await
        .expect("open repaired shard");
    assert!(
        verify_shard_integrity(&mut repaired, &target_shard.checksum).await,
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

    let mut damaged = fs::OpenOptions::new()
        .write(true)
        .open(&repair_hot_shard.absolute_path)
        .await
        .expect("open repair-hot shard for corruption");
    damaged
        .seek(std::io::SeekFrom::Start(0))
        .await
        .expect("seek repair-hot shard");
    damaged
        .write_all(&vec![0x6D; 4096])
        .await
        .expect("corrupt repair-hot shard");
    damaged.flush().await.expect("flush repair-hot corruption");
    drop(damaged);

    let repair_task = tokio::spawn(run_repair_job(
        RepairRunConfig {
            dry_run: false,
            target_fragment: None,
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

    let mut repaired = fs::File::open(&repair_hot_shard.absolute_path)
        .await
        .expect("open repaired hot shard");
    assert!(
        verify_shard_integrity(&mut repaired, &repair_hot_shard.checksum).await,
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

                let got = read_object_bytes_eventually(fragment.clone(), key_a.clone(), 20).await;
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

                let got = read_object_bytes_eventually(fragment.clone(), key_b.clone(), 20).await;
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
            target_fragment: None,
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
                let got = read_object_bytes_eventually(fragment.clone(), key_a.clone(), 24).await;
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

                let got = read_object_bytes_eventually(fragment, key_b.clone(), 24).await;
                assert_eq!(got, content, "recovered read mismatch for {}", key_b);
            }
        })
        .await;

    redist_task
        .await
        .expect("redistribution join")
        .expect("redistribution should succeed during failure-injection CRUD");

    let target_key = expected_final[0].0.clone();
    let target_meta = get_metadata(&fragment, &target_key, &fragment)
        .await
        .expect("target metadata after redistribution");
    let target_shard = target_meta.parts[0]
        .shards
        .iter()
        .find(|s| !s.is_parity)
        .expect("target data shard to corrupt")
        .clone();

    let mut damaged = fs::OpenOptions::new()
        .write(true)
        .open(&target_shard.absolute_path)
        .await
        .expect("open target shard for corruption");
    damaged
        .seek(std::io::SeekFrom::Start(0))
        .await
        .expect("seek target shard");
    damaged
        .write_all(&vec![0x9A; 4096])
        .await
        .expect("inject corruption into target shard");
    damaged.flush().await.expect("flush target corruption");
    drop(damaged);

    let mut before = fs::File::open(&target_shard.absolute_path)
        .await
        .expect("open corrupted shard for verify");
    assert!(
        !verify_shard_integrity(&mut before, &target_shard.checksum).await,
        "failure injection should break shard checksum before repair"
    );

    let repair_task = tokio::spawn(run_repair_job(
        RepairRunConfig {
            dry_run: false,
            target_fragment: None,
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
        .expect("repair should succeed after failure injection");

    let mut after = fs::File::open(&target_shard.absolute_path)
        .await
        .expect("open repaired target shard");
    assert!(
        verify_shard_integrity(&mut after, &target_shard.checksum).await,
        "repair should restore shard checksum after failure injection"
    );

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
            let blob3 = base.join("blob_3");
            let blob3_down = base.join("blob_3.down");

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

        let mut damaged = fs::OpenOptions::new()
            .write(true)
            .open(&target_shard.absolute_path)
            .await
            .expect("open target shard in soak round");
        damaged
            .seek(std::io::SeekFrom::Start(0))
            .await
            .expect("seek target shard in soak round");
        damaged
            .write_all(&vec![0x55; 4096])
            .await
            .expect("inject corruption in soak round");
        damaged
            .flush()
            .await
            .expect("flush corruption in soak round");
        drop(damaged);

        let repair_task = tokio::spawn(run_repair_job(
            RepairRunConfig {
                dry_run: false,
                target_fragment: None,
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

        let mut repaired = fs::File::open(&target_shard.absolute_path)
            .await
            .expect("open repaired shard in soak round");
        assert!(
            verify_shard_integrity(&mut repaired, &target_shard.checksum).await,
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
                execution_mode: RedistributionExecutionMode::Availability,
                scope: crate::n1_core::redistribution::RedistributionScope::All,
                target_fragment: None,
                durability_mode: redist_durability_mode,
                object_concurrency: redist_object_concurrency,
                status_batch: redist_status_batch,
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
                let mut damaged = fs::OpenOptions::new()
                    .write(true)
                    .open(&shard.absolute_path)
                    .await
                    .expect("open shard for bitrot");
                damaged
                    .seek(std::io::SeekFrom::Start(0))
                    .await
                    .expect("seek shard for bitrot");
                let overwrite = vec![0x5A; std::cmp::min(4096, shard.size_bytes as usize).max(1)];
                damaged
                    .write_all(&overwrite)
                    .await
                    .expect("write bitrot bytes");
                damaged.flush().await.expect("flush bitrot bytes");
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
                target_fragment: None,
                check_mode: RepairCheckMode::Scrub,
                meta_concurrency: repair_meta_concurrency,
                shard_repair_concurrency: repair_shard_concurrency,
                meta_batch_size: repair_meta_batch,
            },
            RepairRunOptions {
                fast_mode: repair_fast_mode,
                access_mode: MaintenanceAccessMode::Quiesced,
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
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

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
    let total_uploads = workers * uploads_per_worker;
    let total_bytes = total_uploads as u64 * object_bytes as u64;
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
async fn matrix_bulk_delete_and_recover_round_trip() {
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

    let mut keys = Vec::new();
    for i in 0..64usize {
        let key = format!("xray/2025/02-02-2026/doctor/files/images/ppacs/file-{i:04}.dcm");
        put_object(state.clone(), &fragment, &key, vec![(i % 251) as u8; 1024]).await;
        keys.push(key);
    }

    let bulk_delete_resp = delete_objects_bulk(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(BulkObjectKeysRequest {
            object_keys: keys.clone(),
        }),
    )
    .await
    .expect("bulk delete")
    .into_response();
    assert_eq!(bulk_delete_resp.status(), StatusCode::OK);
    let bulk_delete_body = to_bytes(bulk_delete_resp.into_body(), usize::MAX)
        .await
        .expect("bulk delete body");
    let bulk_delete_json: serde_json::Value =
        serde_json::from_slice(&bulk_delete_body).expect("bulk delete json");
    assert_eq!(bulk_delete_json["succeeded"].as_u64(), Some(64));
    assert_eq!(bulk_delete_json["not_found"].as_u64(), Some(0));

    let live_after_delete_resp = list_objects(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Query(HashMap::from([("limit".to_string(), "1000".to_string())])),
    )
    .await
    .expect("live list after bulk delete")
    .into_response();
    let live_after_delete: ListObjectsResult = serde_json::from_slice(
        &to_bytes(live_after_delete_resp.into_body(), usize::MAX)
            .await
            .expect("live delete body"),
    )
    .expect("live delete json");
    assert!(
        live_after_delete.objects.is_empty(),
        "live list should be empty"
    );

    let trash_after_delete_resp = list_trash_objects(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Query(HashMap::from([("limit".to_string(), "1000".to_string())])),
    )
    .await
    .expect("trash list after bulk delete")
    .into_response();
    let trash_after_delete: ListObjectsResult = serde_json::from_slice(
        &to_bytes(trash_after_delete_resp.into_body(), usize::MAX)
            .await
            .expect("trash delete body"),
    )
    .expect("trash delete json");
    assert_eq!(trash_after_delete.objects.len(), 64);

    let bulk_recover_resp = recover_objects_bulk(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(BulkObjectKeysRequest { object_keys: keys }),
    )
    .await
    .expect("bulk recover")
    .into_response();
    assert_eq!(bulk_recover_resp.status(), StatusCode::OK);
    let bulk_recover_body = to_bytes(bulk_recover_resp.into_body(), usize::MAX)
        .await
        .expect("bulk recover body");
    let bulk_recover_json: serde_json::Value =
        serde_json::from_slice(&bulk_recover_body).expect("bulk recover json");
    assert_eq!(bulk_recover_json["succeeded"].as_u64(), Some(64));
    assert_eq!(bulk_recover_json["not_found"].as_u64(), Some(0));

    let live_after_recover_resp = list_objects(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Query(HashMap::from([("limit".to_string(), "1000".to_string())])),
    )
    .await
    .expect("live list after bulk recover")
    .into_response();
    let live_after_recover: ListObjectsResult = serde_json::from_slice(
        &to_bytes(live_after_recover_resp.into_body(), usize::MAX)
            .await
            .expect("live recover body"),
    )
    .expect("live recover json");
    assert_eq!(live_after_recover.objects.len(), 64);

    let trash_after_recover_resp = list_trash_objects(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Query(HashMap::from([("limit".to_string(), "1000".to_string())])),
    )
    .await
    .expect("trash list after bulk recover")
    .into_response();
    let trash_after_recover: ListObjectsResult = serde_json::from_slice(
        &to_bytes(trash_after_recover_resp.into_body(), usize::MAX)
            .await
            .expect("trash recover body"),
    )
    .expect("trash recover json");
    assert!(trash_after_recover.objects.is_empty());
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
        restore_simulated_index_disk(&down_src, &down_dst, "restore index disk").await;

        run_repair_job(
            RepairRunConfig {
                dry_run: false,
                target_fragment: None,
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

    let count = 5;
    let mut keys = Vec::new();

    for i in 0..count {
        let key = format!("repro-blank/obj-{}.bin", i);
        put_object(state.clone(), &fragment, &key, vec![0xAA; 10]).await;
        keys.push(key);
    }

    let initial = list_objects_json(&fragment, None, None).await;
    let initial_keys = initial["objects"].as_array().expect("objects array").len();
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
    let empty_keys = empty["objects"].as_array().map(|v| v.len()).unwrap_or(0);
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
    let restored_keys = restored["objects"].as_array().map(|v| v.len()).unwrap_or(0);

    assert_eq!(
        restored_keys, count,
        "listing should be restored after recover"
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

    let err = list_objects(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Query(HashMap::from([
            ("prefix".to_string(), "patients".to_string()),
            ("delimiter".to_string(), "/".to_string()),
        ])),
    )
    .await
    .err()
    .expect("prefix query should be rejected");
    let resp = err.into_response();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
