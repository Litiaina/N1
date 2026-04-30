use super::archive::{download_manifest_archive, download_selected_archive};
use super::delete::delete_object;
use super::delete::{
    BulkObjectKeysRequest, delete_object_permanently, delete_object_version, delete_objects_bulk,
    delete_objects_bulk_permanently, empty_trash, get_empty_trash_status,
    get_recover_all_trash_status, persistent_trash_job_paths, recover_all_trash, recover_object,
    recover_object_version, recover_objects_bulk, resume_persistent_trash_jobs,
};
use super::disk_manager::{
    DownloadArchiveManifestQuery, DownloadArchiveRequest, InitiateUploadParams, ListObjectsResult,
    ObjectKeyQuery, ObjectMetadata, ObjectPathManager, RenameObjectRequest, UploadPartQuery,
};
use super::fragment_manager::{create_fragment, delete_fragment};
use super::fragment_metadata::{
    DEFAULT_INDEX_PARTITION_COUNT, clear_fragment_partition_count_cache_for_test,
    fragment_index_partition_count, init_fragment_metadata, read_fragment_metadata,
    set_configured_index_partition_count,
};
use super::garbage_collector::{GcJobStatus, get_gc_status_api, run_garbage_collector};
use super::index;
use super::indexdb_store;
use super::maintenance::sync_fragment_metadata;
use super::metadata_parser::get_metadata;
use super::modify::rename_object;
use super::monitoring::get_system_health;
use super::object_parser::{
    STREAM_CHUNK_SIZE_ATOMIC, STREAM_LOCALITY_BIAS_ENABLED_ATOMIC, STREAM_MAX_CONCURRENCY,
    STREAM_READAHEAD_BYTES_ATOMIC,
};
use super::operation_gate::MaintenanceAccessMode;
use super::quota_manager::{get_fragment_blob_usage, get_storage_usage, init_quota_file};
use super::recovery::verify_shard_integrity;
use super::redistribution::{
    RedistributionDurabilityMode, RedistributionExecutionMode, RedistributionRunOptions,
    replay_pending_intents_for_fragment_now, run_redistribution_job,
    run_redistribution_job_with_options,
};
use super::repair_manager::{
    RepairCheckMode, RepairRunConfig, RepairRunOptions, run_repair_job, run_repair_job_with_options,
};
use super::retrieve::{
    get_object, get_object_metadata, list_objects, list_trash_objects, stream_object,
};
use super::storage_capacity_parser::{
    ResizeFragmentQuotaRequest, get_capacity_usage, get_fragment_quota_usage, resize_fragment_quota,
};
use super::sync::ensure_fragment_blob_directories;
use super::upload::{
    DATA_SHARDS_ATOMIC, MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC, PARITY_SHARDS_ATOMIC,
    STRIPE_SIZE_ATOMIC, UPLOAD_IO_GATE_ENABLED_ATOMIC, UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC,
    WRITER_BUFFER_SIZE_ATOMIC, cancel_upload_session, finalize_object_upload, get_upload_status,
    initiate_object_upload, object_upload_part, upload_object_one_shot,
};
use crate::core::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::MASTER_KEY;
use crate::namespace::runtime::global_namespace_runtime;
use crate::routes::storage_routes::AppState;
use crate::settings::constants::CONFIG_FILE;
use crate::settings::create_config::create_default_config_ini;
use crate::structs::auth_structs::Claims;
use crate::test_support::global_test_lock;
use crate::websocket::manager::WsConnectionManager;

use axum::Json as AxumJson;
use axum::Json;
use axum::body::{Body, to_bytes};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use axum::response::IntoResponse;
use bytes::Bytes;
use chrono::Utc;
use futures_util::{StreamExt, stream};
use indexdb::n1 as n1_indexdb;
use indexdb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
#[cfg(target_family = "unix")]
use std::os::unix::fs::symlink as symlink_dir;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::time::{Duration, sleep};
use tokio_tar::Archive as TarArchive;
use ulid::Ulid;

struct SharedTestLock;

impl SharedTestLock {
    async fn lock(&self) -> tokio::sync::MutexGuard<'static, ()> {
        global_test_lock().lock().await
    }
}

static TEST_LOCK: SharedTestLock = SharedTestLock;
static TEST_BASE: Lazy<std::path::PathBuf> =
    Lazy::new(|| std::env::temp_dir().join(format!("n1-matrix-{}", std::process::id())));
const TEST_FRAGMENT_QUOTA_BYTES: u64 = 64 * 1024 * 1024;

fn parse_test_roots(var_name: &str) -> Option<Vec<std::path::PathBuf>> {
    let raw = std::env::var(var_name).ok()?;
    let roots = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
        .collect::<Vec<_>>();
    if roots.is_empty() { None } else { Some(roots) }
}

async fn resolve_test_storage_roots(
    base: &std::path::Path,
    expected_index_root_count: Option<usize>,
    expected_blob_root_count: Option<usize>,
) -> (
    std::path::PathBuf,
    Vec<std::path::PathBuf>,
    Vec<std::path::PathBuf>,
) {
    let env_index_roots = parse_test_roots("N1_TEST_INDEX_ROOTS");
    let env_blob_roots = parse_test_roots("N1_TEST_BLOB_ROOTS");
    match (env_index_roots, env_blob_roots) {
        (Some(index_roots), Some(blob_roots)) => {
            if let Some(expected) = expected_index_root_count {
                assert_eq!(
                    index_roots.len(),
                    expected,
                    "N1_TEST_INDEX_ROOTS must contain exactly {} root(s)",
                    expected
                );
            }
            if let Some(expected) = expected_blob_root_count {
                assert_eq!(
                    blob_roots.len(),
                    expected,
                    "N1_TEST_BLOB_ROOTS must contain exactly {} root(s)",
                    expected
                );
            }

            for root in index_roots.iter().chain(blob_roots.iter()) {
                assert!(
                    root != std::path::Path::new("/mnt/DISK0")
                        && root != std::path::Path::new("/media/administrator/DISK3")
                        && root != std::path::Path::new("/")
                        && root.parent().is_some(),
                    "refusing to use unsafe test root '{}'; point N1_TEST_*_ROOTS at dedicated subdirectories",
                    root.display()
                );
            }

            let fs_root = std::env::var("N1_TEST_FS_ROOT")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| base.join("fs"));
            let _ = fs::remove_dir_all(base).await;
            let _ = fs::create_dir_all(base).await;
            let _ = fs::remove_dir_all(&fs_root).await;
            let _ = fs::create_dir_all(&fs_root).await;

            for root in index_roots.iter().chain(blob_roots.iter()) {
                let _ = fs::remove_dir_all(root).await;
                let _ = fs::create_dir_all(root).await;
            }

            #[cfg(not(target_family = "unix"))]
            compile_error!("hardware-backed workflow test roots currently require unix symlinks");

            let fs_link = base.join("fs");
            let _ = std::fs::remove_file(&fs_link);
            let _ = std::fs::remove_dir_all(&fs_link);
            symlink_dir(&fs_root, &fs_link).expect("symlink fs root into test base");

            let mut linked_index_roots = Vec::new();
            for (idx, target) in index_roots.iter().enumerate() {
                let link = base.join(format!("index_{}", idx + 1));
                let _ = std::fs::remove_file(&link);
                let _ = std::fs::remove_dir_all(&link);
                symlink_dir(target, &link).expect("symlink index root into test base");
                linked_index_roots.push(link);
            }

            let mut linked_blob_roots = Vec::new();
            for (idx, target) in blob_roots.iter().enumerate() {
                let link = base.join(format!("blob_{}", idx + 1));
                let _ = std::fs::remove_file(&link);
                let _ = std::fs::remove_dir_all(&link);
                symlink_dir(target, &link).expect("symlink blob root into test base");
                linked_blob_roots.push(link);
            }

            (fs_link, linked_index_roots, linked_blob_roots)
        }
        (None, None) => {
            let _ = fs::remove_dir_all(base).await;
            let _ = fs::create_dir_all(base).await;

            let index_root_count = expected_index_root_count.unwrap_or(2);
            let blob_root_count = expected_blob_root_count.unwrap_or(3);

            let mut index_roots = Vec::new();
            for idx in 1..=index_root_count {
                let path = base.join(format!("index_{idx}"));
                let _ = fs::create_dir_all(&path).await;
                index_roots.push(path);
            }

            let mut blob_roots = Vec::new();
            for idx in 1..=blob_root_count {
                let path = base.join(format!("blob_{idx}"));
                let _ = fs::create_dir_all(&path).await;
                blob_roots.push(path);
            }

            (base.to_path_buf(), index_roots, blob_roots)
        }
        _ => {
            panic!("set both N1_TEST_INDEX_ROOTS and N1_TEST_BLOB_ROOTS together, or neither");
        }
    }
}

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

fn ensure_master_key_for_tests() {
    if std::env::var("MASTER_KEY").is_err() {
        unsafe { std::env::set_var("MASTER_KEY", "test-master-key") };
    }
}

async fn ensure_global_storage_config(base: &std::path::Path) {
    ensure_test_config_file();

    set_configured_index_partition_count(DEFAULT_INDEX_PARTITION_COUNT)
        .expect("reset configured index partition count");
    clear_fragment_partition_count_cache_for_test();
    indexdb_store::clear_cached_dbs_for_test();
    let (fs_root, index_roots, blob_roots) =
        resolve_test_storage_roots(base, Some(2), Some(3)).await;

    let index_roots = index_roots
        .into_iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    let blob_roots = blob_roots
        .into_iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();

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
            .set(fs_root.to_string_lossy().to_string())
            .expect("set FS_STORAGE_ROOT");
    }
}

async fn ensure_global_storage_config_with_root_counts(
    base: &std::path::Path,
    index_root_count: usize,
    blob_root_count: usize,
) {
    ensure_test_config_file();

    set_configured_index_partition_count(DEFAULT_INDEX_PARTITION_COUNT)
        .expect("reset configured index partition count");
    clear_fragment_partition_count_cache_for_test();
    indexdb_store::clear_cached_dbs_for_test();
    let (fs_root, index_roots, blob_roots) =
        resolve_test_storage_roots(base, Some(index_root_count), Some(blob_root_count)).await;

    let index_roots = index_roots
        .into_iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();

    let blob_roots = blob_roots
        .into_iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();

    if INDEX_STORE_DISKS.get().is_none() {
        INDEX_STORE_DISKS
            .set(index_roots.clone())
            .expect("set INDEX_STORE_DISKS");
    } else {
        assert_eq!(
            INDEX_STORE_DISKS
                .get()
                .expect("INDEX_STORE_DISKS already set"),
            &index_roots,
            "test requires isolated process with matching index roots"
        );
    }

    if BLOB_STORE_DISKS.get().is_none() {
        BLOB_STORE_DISKS
            .set(blob_roots.clone())
            .expect("set BLOB_STORE_DISKS");
    } else {
        assert_eq!(
            BLOB_STORE_DISKS
                .get()
                .expect("BLOB_STORE_DISKS already set"),
            &blob_roots,
            "test requires isolated process with matching blob roots"
        );
    }

    if FS_STORAGE_ROOT.get().is_none() {
        FS_STORAGE_ROOT
            .set(fs_root.to_string_lossy().to_string())
            .expect("set FS_STORAGE_ROOT");
    } else {
        assert_eq!(
            FS_STORAGE_ROOT.get().expect("FS_STORAGE_ROOT already set"),
            &fs_root.to_string_lossy().to_string(),
            "test requires isolated process with matching storage root"
        );
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

async fn park_simulated_disk_with_blocker(
    live_src: &std::path::Path,
    parked_dst: &std::path::Path,
    blocker_contents: &[u8],
    label: &str,
) {
    if fs::try_exists(parked_dst).await.unwrap_or(false) {
        match fs::metadata(parked_dst).await {
            Ok(meta) if meta.is_dir() => {
                let _ = fs::remove_dir_all(parked_dst).await;
            }
            Ok(_) => {
                let _ = fs::remove_file(parked_dst).await;
            }
            Err(_) => {}
        }
    }

    fs::rename(live_src, parked_dst).await.expect(label);
    fs::write(live_src, blocker_contents)
        .await
        .expect("create disk down blocker file");
}

async fn restore_simulated_disk(
    live_src: &std::path::Path,
    parked_dst: &std::path::Path,
    label: &str,
) {
    if fs::try_exists(live_src).await.unwrap_or(false) {
        match fs::metadata(live_src).await {
            Ok(meta) if meta.is_dir() => {
                let _ = fs::remove_dir_all(live_src).await;
            }
            Ok(_) => {
                let _ = fs::remove_file(live_src).await;
            }
            Err(_) => {}
        }
    }

    fs::rename(parked_dst, live_src).await.expect(label);
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

async fn wait_for_trash_job_completion(kind: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let status = if kind == "recover" {
            get_recover_all_trash_status().await
        } else {
            get_empty_trash_status().await
        };
        if !status.is_running {
            assert!(
                !status.phase.eq_ignore_ascii_case("failed"),
                "trash job failed: {:?}",
                status
            );
            break;
        }
        if Instant::now() >= deadline {
            panic!("{} trash job did not finish in time", kind);
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn current_process_rss_kib() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            if let Some(value) = rest
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())
            {
                return value;
            }
        }
    }
    0
}

async fn wait_for_trash_job_completion_with_peak_rss(kind: &str) -> (u64, u64) {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut peak_rss_kib = current_process_rss_kib();
    loop {
        peak_rss_kib = peak_rss_kib.max(current_process_rss_kib());
        let status = if kind == "recover" {
            get_recover_all_trash_status().await
        } else {
            get_empty_trash_status().await
        };
        if !status.is_running {
            assert!(
                !status.phase.eq_ignore_ascii_case("failed"),
                "trash job failed: {:?}",
                status
            );
            return (status.duration_seconds, peak_rss_kib);
        }
        if Instant::now() >= deadline {
            panic!("{} trash job did not finish in time", kind);
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn seed_persistent_trash_job(fragment: &str, operation: &str) {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "operation": operation,
        "fragment": fragment,
        "started_at": 1_i64,
    }))
    .expect("serialize persistent trash job");
    for path in persistent_trash_job_paths(fragment) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .expect("create persistent trash job parent");
        }
        fs::write(&path, &bytes)
            .await
            .expect("write persistent trash job");
    }
}

async fn run_manual_gc_and_wait(state: Arc<AppState>) -> GcJobStatus {
    run_manual_gc_with_target_and_wait(state, None).await
}

async fn run_manual_gc_for_fragment_and_wait(state: Arc<AppState>, fragment: &str) -> GcJobStatus {
    run_manual_gc_with_target_and_wait(state, Some(fragment)).await
}

async fn run_manual_gc_with_target_and_wait(
    state: Arc<AppState>,
    target_fragment: Option<&str>,
) -> GcJobStatus {
    let baseline = get_gc_status_api(full_claims("gc-status"))
        .await
        .expect("gc status baseline")
        .0;
    let baseline_start = baseline.start_timestamp;
    while baseline_start > 0 && Utc::now().timestamp() <= baseline_start {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    let mut params = HashMap::new();
    if let Some(fragment) = target_fragment {
        params.insert("fragment".to_string(), fragment.to_string());
    }
    let start = run_garbage_collector(State(state), Query(params))
        .await
        .into_response();
    assert_eq!(start.status(), StatusCode::ACCEPTED, "manual GC must start");

    let deadline = Instant::now() + std::time::Duration::from_secs(20);
    let mut saw_new_run = false;
    loop {
        let status = get_gc_status_api(full_claims("gc-status"))
            .await
            .expect("gc status");
        let current = status.0;
        let matches_target = target_fragment
            .map(|fragment| current.current_fragment == fragment)
            .unwrap_or(true);
        if matches_target && (current.is_running || current.start_timestamp != baseline_start) {
            saw_new_run = true;
        }
        if target_fragment.is_some()
            && matches_target
            && current.is_manual_run
            && current.phase == "Completed"
        {
            saw_new_run = true;
        }
        if saw_new_run && !current.is_running {
            return current;
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
            .uri("/n1/objects/get")
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
    read_object_bytes_eventually_labeled(fragment, object_key, attempts, "read").await
}

async fn read_object_bytes_eventually_labeled(
    fragment: String,
    object_key: String,
    attempts: usize,
    label: &str,
) -> Vec<u8> {
    for _ in 0..attempts {
        let req = Request::builder()
            .uri("/n1/objects/get")
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

    let metadata_after = get_metadata(&fragment, &object_key, &fragment).await;
    let live_versions_after =
        index::list_live_object_versions_metadata(&fragment, &object_key).await;
    let trash_versions_after =
        index::list_trashed_object_versions_metadata(&fragment, &object_key).await;
    panic!(
        "{}: object {} in fragment {} was not readable after {} attempts; metadata={:?}; live_versions={:?}; trash_versions={:?}",
        label,
        object_key,
        fragment,
        attempts,
        metadata_after,
        live_versions_after,
        trash_versions_after
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

async fn create_fragment_with_quota(quota_bytes: u64) -> String {
    ensure_master_key_for_tests();
    let fragment_name = format!("fragment-quota-{}", Ulid::new());
    let fragment = hex::encode(Sha256::digest(fragment_name.as_bytes()));
    let response = create_fragment(Json(super::sync::CreateFragmentRequest {
        name: fragment_name,
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: Some(quota_bytes),
    }))
    .await
    .expect("create quota fragment")
    .into_response();
    assert_eq!(response.status(), StatusCode::CREATED);
    let meta = indexdb_store::fragment_quota_meta(&fragment)
        .expect("fragment quota meta after create")
        .expect("fragment quota meta");
    assert!(
        meta.quota_project_id.is_some(),
        "fragment creation must assign a stable filesystem quota project id"
    );
    fragment
}

async fn collect_fragment_indexdb_files(fragment: &str) -> Vec<String> {
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let mut out = Vec::new();

    for root in index_roots {
        let frag_root = root.join(fragment);
        if !fs::try_exists(&frag_root).await.unwrap_or(false) {
            continue;
        }

        for entry in walkdir::WalkDir::new(&frag_root)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }
            out.push(entry.path().to_string_lossy().to_string());
        }
    }

    out.sort();
    out
}

fn object_partition_db_paths_owner_first(
    fragment: &str,
    _object_key: &str,
) -> Vec<std::path::PathBuf> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Vec::new();
    }

    let mut hasher = Sha256::new();
    for part in ["fragment", fragment] {
        hasher.update(part.as_bytes());
        hasher.update([0u8]);
    }
    let route_hash: [u8; 32] = hasher.finalize().into();
    let owner_idx =
        usize::from(u16::from(route_hash[0]) << 8 | u16::from(route_hash[1])) % roots.len();
    let mut selected = vec![roots[owner_idx].clone()];
    for (idx, root) in roots.into_iter().enumerate() {
        if idx != owner_idx {
            selected.push(root);
        }
    }

    selected
        .into_iter()
        .map(|root| root.join(fragment).join("indexdb").join("index.ndxdb"))
        .collect()
}

fn fragment_owned_by_index_root(owner_root: &std::path::Path, prefix: &str) -> String {
    for candidate in 0..4096usize {
        let fragment = hex::encode(Sha256::digest(format!("{prefix}-{candidate}").as_bytes()));
        let owner_paths = object_partition_db_paths_owner_first(&fragment, "probe.bin");
        if owner_paths
            .first()
            .map(|path| path.starts_with(owner_root))
            .unwrap_or(false)
        {
            return fragment;
        }
    }

    panic!(
        "failed to find fragment owned by {} with prefix {}",
        owner_root.display(),
        prefix
    );
}

async fn collect_indexdb_replica_hashes(fragment: &str) -> Vec<(String, BTreeMap<String, String>)> {
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
            if !rel.ends_with(".ndxdb") && rel != "fragment.metadata.msgpack" {
                continue;
            }

            if let Ok(bytes) = fs::read(path).await {
                let digest = Sha256::digest(&bytes);
                map.insert(rel, hex::encode(digest));
            }
        }

        out.push((root.to_string_lossy().to_string(), map));
    }

    out
}

fn hash_indexdb_logical_contents(path: &std::path::Path) -> Option<String> {
    std::panic::catch_unwind(|| {
        indexdb_store::evict_cached_db_for_test(path);
        let db = Database::builder().open(path).ok()?;
        let read_txn = db.begin_read().ok()?;
        let mut table_names: Vec<String> = read_txn
            .list_tables()
            .ok()?
            .into_iter()
            .map(|table| table.name().to_owned())
            .collect();
        table_names.sort();

        let mut hasher = Sha256::new();
        for table_name in table_names {
            hasher.update(table_name.as_bytes());
            hasher.update([0u8]);

            let definition: TableDefinition<&str, &[u8]> =
                TableDefinition::new(table_name.as_str());
            let table = read_txn.open_table(definition).ok()?;
            let mut iter = table.iter().ok()?;
            while let Some(entry) = iter.next() {
                let (key, value) = entry.ok()?;
                if table_name == "indexdb_meta"
                    && matches!(
                        key.value(),
                        "created_at_unix" | "last_commit_id" | "replica_generation"
                    )
                {
                    continue;
                }
                hasher.update(key.value().as_bytes());
                hasher.update([0xff]);
                hasher.update(value.value());
                hasher.update([0u8]);
            }
        }
        Some(hex::encode(hasher.finalize()))
    })
    .ok()
    .flatten()
}

async fn collect_indexdb_replica_semantic_hashes(
    fragment: &str,
) -> Vec<(String, BTreeMap<String, String>)> {
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

        let fragment_meta_path = frag_root.join("fragment.metadata.msgpack");
        if let Ok(bytes) = fs::read(&fragment_meta_path).await {
            let digest = Sha256::digest(&bytes);
            map.insert("fragment.metadata.msgpack".to_string(), hex::encode(digest));
        }

        let index_db_path = frag_root.join("indexdb").join("index.ndxdb");
        if let Some(digest) = hash_indexdb_logical_contents(&index_db_path) {
            map.insert("indexdb/index.ndxdb".to_string(), digest);
        } else if fs::try_exists(&index_db_path).await.unwrap_or(false) {
            map.insert("indexdb/index.ndxdb".to_string(), "unreadable".to_string());
        }

        out.push((root.to_string_lossy().to_string(), map));
    }

    out
}

async fn assert_all_indexdb_replicas_synchronized(fragment: &str) {
    let snapshots = collect_indexdb_replica_semantic_hashes(fragment).await;
    assert!(
        snapshots.len() >= 2,
        "expected at least 2 index roots, got {}",
        snapshots.len()
    );

    let (base_root, base_map) = &snapshots[0];
    for (root, map) in snapshots.iter().skip(1) {
        assert_eq!(
            map, base_map,
            "indexdb replicas diverged: {} vs {}",
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
    try_upload_one_shot(state, fragment, object_key, bytes)
        .await
        .expect("upload one-shot")
}

async fn try_upload_one_shot(
    state: Arc<AppState>,
    fragment: &str,
    object_key: &str,
    bytes: Vec<u8>,
) -> Result<axum::response::Response, AppError> {
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
    .map(IntoResponse::into_response)
}

fn generated_streaming_body(object_bytes: usize, chunk_bytes: usize, seed: usize) -> Body {
    let chunk_bytes = chunk_bytes.max(1);
    let chunk_count = object_bytes.div_ceil(chunk_bytes);
    let stream = stream::iter(0..chunk_count).map(move |chunk_index| {
        let start = chunk_index * chunk_bytes;
        let len = std::cmp::min(chunk_bytes, object_bytes.saturating_sub(start));
        let mut chunk = vec![0u8; len];
        for (j, b) in chunk.iter_mut().enumerate() {
            *b = ((start + j + seed) % 251) as u8;
        }
        Ok::<Bytes, std::convert::Infallible>(Bytes::from(chunk))
    });
    Body::from_stream(stream)
}

async fn upload_one_shot_generated_chunked_with_options(
    state: Arc<AppState>,
    fragment: &str,
    object_key: &str,
    object_bytes: usize,
    chunk_bytes: usize,
    seed: usize,
    durability: Option<&str>,
    io_gate: Option<bool>,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-length",
        HeaderValue::from_str(&object_bytes.to_string()).expect("content-length"),
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
            durability: durability.map(str::to_string),
            strict_durability: None,
            io_gate,
        }),
        generated_streaming_body(object_bytes, chunk_bytes, seed),
    )
    .await
    .expect("upload one-shot generated chunks")
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
async fn matrix_fragment_partition_count_is_setup_driven_and_mismatch_is_rejected() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;

    set_configured_index_partition_count(64).expect("set balanced partition count");
    clear_fragment_partition_count_cache_for_test();

    let owner_root = base.join("index_1");
    let fragment = fragment_owned_by_index_root(&owner_root, "fragment-two-disk-cycle");

    init_fragment_metadata(&fragment, "partition-key")
        .await
        .expect("init fragment metadata");
    assert_eq!(
        fragment_index_partition_count(&fragment).expect("fragment partition count"),
        64
    );

    set_configured_index_partition_count(32).expect("set low-hw partition count");
    clear_fragment_partition_count_cache_for_test();

    match fragment_index_partition_count(&fragment) {
        Err(AppError::Conflict(message)) => {
            assert!(
                message.contains("uses 64 index partitions"),
                "unexpected mismatch message: {}",
                message
            );
            assert!(
                message.contains("configured for 32"),
                "unexpected mismatch message: {}",
                message
            );
        }
        other => panic!(
            "expected partition-count mismatch conflict, got {:?}",
            other
        ),
    }

    set_configured_index_partition_count(DEFAULT_INDEX_PARTITION_COUNT)
        .expect("restore default partition count");
    clear_fragment_partition_count_cache_for_test();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_upload_part_batched_durability_finalize_ok() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(262_144, Ordering::Relaxed);

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
async fn matrix_upload_one_shot_publishes_without_active_session() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(262_144, Ordering::Relaxed);

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
    assert_eq!(meta.stripe_size_bytes, 16 * 1024);
    assert_eq!(meta.parts.len(), 1);
    assert_eq!(meta.parts[0].part_index, 0);
    assert_eq!(meta.parts[0].shards[0].size_bytes, 5 * 16 * 1024);

    let sessions = index::list_active_sessions(&fragment)
        .await
        .expect("list active sessions");
    assert!(sessions.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_multipart_upload_persists_dynamic_stripe_in_manifest_and_metadata() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(262_144, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let key = "multipart/dynamic-stripe.bin";
    let part0 = vec![0x11; 16 * 1024];
    let part1 = vec![0x22; 16 * 1024];
    let total = (part0.len() + part1.len()) as u64;

    let (version_id, session_id) =
        init_upload_with_part_size(&fragment, key, total, 2, Some(16 * 1024)).await;
    let session = index::load_session(&fragment, &version_id)
        .await
        .expect("load dynamic stripe session")
        .expect("dynamic stripe session");
    assert_eq!(session.manifest.stripe_size_bytes, 4096);
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(2, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    let put0 = upload_part(&fragment, &version_id, 0, part0.clone()).await;
    assert_eq!(put0.status(), StatusCode::OK);
    let put1 = upload_part(&fragment, &version_id, 1, part1.clone()).await;
    assert_eq!(put1.status(), StatusCode::OK);

    let finalize_resp =
        finalize_upload(state, &fragment, &version_id, &session_id, key, total, 2).await;
    assert_eq!(finalize_resp.status(), StatusCode::OK);

    let meta = index::read_object_version(&fragment, key, &version_id)
        .await
        .expect("read finalized dynamic stripe object");
    assert_eq!(meta.stripe_size_bytes, 4096);
    assert_eq!(meta.parts.len(), 2);
    assert!(meta.parts.iter().all(|part| {
        part.shards.len() == 1 && part.shards.iter().all(|shard| !shard.is_parity)
    }));
    assert!(meta.parts.iter().all(|part| {
        part.shards
            .iter()
            .all(|shard| shard.size_bytes == 16 * 1024)
    }));

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.to_string()).await;
    assert_eq!(got, [part0, part1].concat());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_delete_recreate_same_hash_allows_fresh_upload_without_restart() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment_name = format!("fragment-{}", Ulid::new());
    let fragment = hex::encode(Sha256::digest(fragment_name.as_bytes()));
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "test-fragment-key")
        .await
        .expect("init fragment metadata");
    indexdb_store::ensure_fragment_owner_partition_available(&fragment)
        .expect("materialize fragment owner partition db");

    let first_bytes = b"first upload after fragment create".to_vec();
    let first_resp = upload_one_shot(
        state.clone(),
        &fragment,
        "recreate/first.bin",
        first_bytes.clone(),
    )
    .await;
    assert_eq!(first_resp.status(), StatusCode::CREATED);
    let first_got = read_object_bytes(
        full_claims(&fragment),
        fragment.clone(),
        "recreate/first.bin".to_string(),
    )
    .await;
    assert_eq!(first_got, first_bytes);

    let delete_resp = delete_fragment(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath(fragment.clone()),
    )
    .await
    .expect("delete fragment")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "test-fragment-key")
        .await
        .expect("recreate fragment metadata");
    indexdb_store::ensure_fragment_owner_partition_available(&fragment)
        .expect("materialize recreated fragment owner partition db");
    assert!(
        indexdb_store::has_any_fragment_partition_db(&fragment)
            .expect("check recreated fragment owner db"),
        "recreated fragment should materialize an owner partition db"
    );

    let second_bytes = b"second upload after fragment recreate".to_vec();
    let second_resp = upload_one_shot(
        state,
        &fragment,
        "recreate/second.bin",
        second_bytes.clone(),
    )
    .await;
    assert_eq!(second_resp.status(), StatusCode::CREATED);
    let second_got = read_object_bytes(
        full_claims(&fragment),
        fragment.clone(),
        "recreate/second.bin".to_string(),
    )
    .await;
    assert_eq!(second_got, second_bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_delete_recreate_same_hash_clears_stale_namespace_views() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;

    let fragment_name = format!("fragment-{}", Ulid::new());
    let fragment = hex::encode(Sha256::digest(fragment_name.as_bytes()));
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "test-fragment-key")
        .await
        .expect("init fragment metadata");
    indexdb_store::ensure_fragment_owner_partition_available(&fragment)
        .expect("materialize fragment owner partition db");

    let first_meta = ObjectMetadata {
        object_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        fragment: fragment.clone(),
        version_id: Ulid::new().to_string(),
        upload_session_id: String::new(),
        owner_uid: fragment.clone(),
        content_type: "application/octet-stream".to_string(),
        total_size_bytes: 123,
        created_at: 1_700_000_200,
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: 262_144,
    };
    global_namespace_runtime()
        .finalize_logical_upload(
            &fragment,
            "recreate/first.bin",
            &first_meta.object_key,
            &first_meta,
        )
        .await
        .expect("seed first logical file");

    assert!(
        global_namespace_runtime()
            .entry(&fragment, "recreate/first.bin")
            .await
            .expect("lookup first logical entry")
            .is_some()
    );

    let delete_resp = delete_fragment(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath(fragment.clone()),
    )
    .await
    .expect("delete fragment")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "test-fragment-key")
        .await
        .expect("recreate fragment metadata");
    indexdb_store::ensure_fragment_owner_partition_available(&fragment)
        .expect("materialize recreated fragment owner partition db");

    global_namespace_runtime()
        .mkdir(&fragment, "Images")
        .await
        .expect("mkdir on recreated fragment");

    let second_meta = ObjectMetadata {
        object_key: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        fragment: fragment.clone(),
        version_id: Ulid::new().to_string(),
        upload_session_id: String::new(),
        owner_uid: fragment.clone(),
        content_type: "application/octet-stream".to_string(),
        total_size_bytes: 456,
        created_at: 1_700_000_201,
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: 262_144,
    };
    global_namespace_runtime()
        .finalize_logical_upload(
            &fragment,
            "recreate/second.bin",
            &second_meta.object_key,
            &second_meta,
        )
        .await
        .expect("seed second logical file");

    let root_entries = global_namespace_runtime()
        .list_dir(&fragment, "", false)
        .await
        .expect("list recreated fragment namespace");
    let listed_paths = root_entries
        .into_iter()
        .map(|entry| entry.path)
        .collect::<Vec<_>>();
    assert_eq!(
        listed_paths,
        vec!["Images".to_string(), "recreate".to_string()],
        "recreated fragment should not retain stale namespace entries from the deleted fragment"
    );

    let recreate_entries = global_namespace_runtime()
        .list_dir(&fragment, "recreate", false)
        .await
        .expect("list recreated prefix");
    let recreate_paths = recreate_entries
        .into_iter()
        .map(|entry| entry.path)
        .collect::<Vec<_>>();
    assert_eq!(recreate_paths, vec!["recreate/second.bin".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_create_retry_heals_partial_initialization_instead_of_conflicting() {
    let _guard = TEST_LOCK.lock().await;
    ensure_master_key_for_tests();
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment_name = format!("fragment-{}", Ulid::new());
    let fragment = hex::encode(Sha256::digest(fragment_name.as_bytes()));

    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "test-fragment-key")
        .await
        .expect("seed fragment metadata only");
    assert!(
        !indexdb_store::has_any_fragment_partition_db(&fragment)
            .expect("check missing owner partition db"),
        "precondition failed: partial fragment should not have owner db yet"
    );

    let create_resp = create_fragment(Json(super::sync::CreateFragmentRequest {
        name: fragment_name.clone(),
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: Some(TEST_FRAGMENT_QUOTA_BYTES),
    }))
    .await
    .expect("retry create should heal partial fragment")
    .into_response();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    assert!(
        indexdb_store::has_any_fragment_partition_db(&fragment)
            .expect("check healed owner partition db"),
        "retry create should materialize owner partition db"
    );

    let second_resp = create_fragment(Json(super::sync::CreateFragmentRequest {
        name: fragment_name,
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: Some(TEST_FRAGMENT_QUOTA_BYTES),
    }))
    .await;
    assert!(
        matches!(second_resp, Err(AppError::Conflict(_))),
        "fully initialized fragment should still conflict on create"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_create_rejects_quota_beyond_physical_capacity() {
    let _guard = TEST_LOCK.lock().await;
    ensure_master_key_for_tests();
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let usage = get_storage_usage()
        .await
        .expect("read configured physical capacity");
    let create_err = match create_fragment(Json(super::sync::CreateFragmentRequest {
        name: format!("fragment-{}", Ulid::new()),
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: Some(usage.capacity_allocated.saturating_add(1)),
    }))
    .await
    {
        Ok(_) => panic!("fragment create should reject impossible quota"),
        Err(err) => err,
    };
    assert!(
        matches!(create_err, AppError::InsufficientStorage),
        "expected InsufficientStorage for impossible fragment quota, got {create_err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_create_requires_nonzero_quota() {
    let _guard = TEST_LOCK.lock().await;
    ensure_master_key_for_tests();
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let missing_quota = create_fragment(Json(super::sync::CreateFragmentRequest {
        name: format!("fragment-{}", Ulid::new()),
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: None,
    }))
    .await;
    match missing_quota {
        Err(AppError::BadRequest(_)) => {}
        Err(err) => panic!("expected missing quota BadRequest, got {err:?}"),
        Ok(_) => panic!("expected missing quota to be rejected"),
    }

    let zero_quota = create_fragment(Json(super::sync::CreateFragmentRequest {
        name: format!("fragment-{}", Ulid::new()),
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: Some(0),
    }))
    .await;
    match zero_quota {
        Err(AppError::BadRequest(_)) => {}
        Err(err) => panic!("expected zero quota BadRequest, got {err:?}"),
        Ok(_) => panic!("expected zero quota to be rejected"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_quota_resize_is_admin_validated() {
    let _guard = TEST_LOCK.lock().await;
    ensure_master_key_for_tests();
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = create_fragment_with_quota(2 * 1024 * 1024).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let upload_resp =
        upload_one_shot(state, &fragment, "quota/resize-used.bin", vec![0x42; 4]).await;
    assert_eq!(upload_resp.status(), StatusCode::CREATED);
    let uploaded_meta = index::read_latest_object(&fragment, "quota/resize-used.bin")
        .await
        .expect("read quota resize upload metadata");
    let physical_bytes: u64 = uploaded_meta
        .parts
        .iter()
        .flat_map(|part| part.shards.iter())
        .map(|shard| shard.size_bytes)
        .sum();
    assert!(physical_bytes > 0);
    global_namespace_runtime()
        .finalize_logical_upload(
            &fragment,
            "quota/resize-used.bin",
            "quota/resize-used.bin",
            &uploaded_meta,
        )
        .await
        .expect("bind quota resize object to namespace");

    let grow_resp = resize_fragment_quota(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Json(ResizeFragmentQuotaRequest {
            quota_bytes: 3 * 1024 * 1024,
            secret: (*MASTER_KEY).clone(),
        }),
    )
    .await
    .expect("grow quota")
    .into_response();
    assert_eq!(grow_resp.status(), StatusCode::OK);
    let meta = indexdb_store::fragment_quota_meta(&fragment)
        .expect("quota meta after grow")
        .expect("fragment quota meta");
    assert_eq!(meta.quota_bytes, 3 * 1024 * 1024);
    assert_eq!(meta.committed_bytes, 0);

    let too_small = resize_fragment_quota(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Json(ResizeFragmentQuotaRequest {
            quota_bytes: physical_bytes.saturating_sub(1),
            secret: (*MASTER_KEY).clone(),
        }),
    )
    .await;
    match too_small {
        Err(AppError::Conflict(msg)) => {
            assert!(msg.contains("below current blob filesystem used_bytes"))
        }
        Err(err) => panic!("expected Conflict for shrink below used bytes, got {err:?}"),
        Ok(_) => panic!("expected shrink below used bytes to fail"),
    }
    let meta = indexdb_store::fragment_quota_meta(&fragment)
        .expect("quota meta after rejected shrink")
        .expect("fragment quota meta");
    assert_eq!(meta.quota_bytes, 3 * 1024 * 1024);

    let shrink_resp = resize_fragment_quota(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Json(ResizeFragmentQuotaRequest {
            quota_bytes: physical_bytes,
            secret: (*MASTER_KEY).clone(),
        }),
    )
    .await
    .expect("shrink quota to used bytes")
    .into_response();
    assert_eq!(shrink_resp.status(), StatusCode::OK);
    let meta = indexdb_store::fragment_quota_meta(&fragment)
        .expect("quota meta after valid shrink")
        .expect("fragment quota meta");
    assert_eq!(meta.quota_bytes, physical_bytes);
    assert_eq!(meta.committed_bytes, 0);

    let usage = get_storage_usage().await.expect("physical capacity");
    let impossible = resize_fragment_quota(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Json(ResizeFragmentQuotaRequest {
            quota_bytes: usage.capacity_allocated.saturating_add(1),
            secret: (*MASTER_KEY).clone(),
        }),
    )
    .await;
    match impossible {
        Err(AppError::InsufficientStorage) => {}
        Err(err) => panic!("expected InsufficientStorage for impossible grow, got {err:?}"),
        Ok(_) => panic!("expected impossible quota grow to fail"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_multipart_upload_initiate_does_not_reserve_fragment_quota() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = create_fragment_with_quota(2 * 1024 * 1024).await;
    let init_resp = initiate_object_upload(
        full_claims(&fragment),
        AxumPath((fragment.clone(), "quota/session-a.bin".to_string())),
        AxumJson(InitiateUploadParams {
            content_type: Some("application/octet-stream".into()),
            total_size_bytes: 8,
            expected_parts: 2,
            part_size_bytes: Some(4),
        }),
    )
    .await
    .expect("initiate first upload")
    .into_response();
    assert_eq!(init_resp.status(), StatusCode::CREATED);
    let init_json = read_response_json(init_resp).await;
    let version_id = init_json["version_id"]
        .as_str()
        .expect("version_id")
        .to_string();

    let meta = indexdb_store::fragment_quota_meta(&fragment)
        .expect("quota meta after initiate")
        .expect("fragment quota meta");
    assert_eq!(meta.quota_bytes, 2 * 1024 * 1024);
    assert_eq!(meta.committed_bytes, 0);
    assert_eq!(meta.reserved_bytes, 0);
    assert_eq!(meta.trash_bytes, 0);

    let second_resp = initiate_object_upload(
        full_claims(&fragment),
        AxumPath((fragment.clone(), "quota/session-b.bin".to_string())),
        AxumJson(InitiateUploadParams {
            content_type: Some("application/octet-stream".into()),
            total_size_bytes: 3,
            expected_parts: 1,
            part_size_bytes: Some(3),
        }),
    )
    .await
    .expect("initiate second upload without quota reservation")
    .into_response();
    assert_eq!(second_resp.status(), StatusCode::CREATED);

    index::remove_session(&fragment, &version_id, "quota-test-cancel")
        .await
        .expect("remove quota-bound session");
    let meta = indexdb_store::fragment_quota_meta(&fragment)
        .expect("quota meta after session cancel")
        .expect("fragment quota meta");
    assert_eq!(meta.committed_bytes, 0);
    assert_eq!(meta.reserved_bytes, 0);
    assert_eq!(meta.trash_bytes, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_multipart_cancel_reclaims_physical_upload_bytes() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = create_fragment_with_quota(2 * 1024 * 1024).await;
    let (version_id, _session_id) =
        init_upload_with_part_size(&fragment, "quota/cancel.bin", 8192, 2, Some(4096)).await;
    let put_resp = upload_part(&fragment, &version_id, 0, vec![0x42; 4096]).await;
    assert_eq!(put_resp.status(), StatusCode::OK);

    let usage_before = get_fragment_blob_usage(&fragment)
        .await
        .expect("usage before cancel")
        .used_bytes();
    assert!(
        usage_before > 0,
        "uploaded multipart part should consume physical blob bytes"
    );

    let cancel_resp = cancel_upload_session(
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_id.clone())),
        Query(ObjectKeyQuery {
            object_key: "quota/cancel.bin".to_string(),
            version_id: None,
            filename: None,
        }),
    )
    .await
    .expect("cancel multipart upload")
    .into_response();
    assert_eq!(cancel_resp.status(), StatusCode::NO_CONTENT);

    let usage_after = get_fragment_blob_usage(&fragment)
        .await
        .expect("usage after cancel")
        .used_bytes();
    assert_eq!(
        usage_after, 0,
        "cancel must reclaim physical multipart upload bytes immediately"
    );
    let sessions = index::list_active_sessions(&fragment)
        .await
        .expect("active sessions after cancel");
    assert!(
        sessions
            .iter()
            .all(|session| session.manifest.version_id != version_id),
        "cancelled session must not remain active"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_failed_multipart_part_cleans_partial_attempt_bytes() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = create_fragment_with_quota(2 * 1024 * 1024).await;
    let (version_id, _session_id) =
        init_upload_with_part_size(&fragment, "quota/failed-part.bin", 8192, 1, Some(8192)).await;

    let mut headers = HeaderMap::new();
    headers.insert("content-length", HeaderValue::from_static("8192"));
    let err = match object_upload_part(
        full_claims(&fragment),
        headers,
        AxumPath((fragment.clone(), version_id.clone(), 0)),
        Query(UploadPartQuery {
            checksum: None,
            durability: Some("batched".to_string()),
            strict_durability: None,
            io_gate: None,
        }),
        Body::from(vec![0x99; 4096]),
    )
    .await
    {
        Ok(_) => panic!("short body should fail the part upload"),
        Err(err) => err,
    };
    assert!(
        matches!(err, AppError::BadRequest(_)),
        "expected BadRequest for short body, got {err:?}"
    );

    let usage_after = get_fragment_blob_usage(&fragment)
        .await
        .expect("usage after failed part")
        .used_bytes();
    assert_eq!(
        usage_after, 0,
        "failed part writes must not leave quota-counted temp or stable shard bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_quota_usage_reads_blob_filesystem() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = create_fragment_with_quota(2 * 1024 * 1024).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let object_key = "quota/blob-usage.bin";

    let upload_resp = upload_one_shot(state, &fragment, object_key, vec![0x31; 4]).await;
    assert_eq!(upload_resp.status(), StatusCode::CREATED);
    let metadata = index::read_latest_object(&fragment, object_key)
        .await
        .expect("read uploaded metadata");
    let physical_bytes: u64 = metadata
        .parts
        .iter()
        .flat_map(|part| part.shards.iter())
        .map(|shard| shard.size_bytes)
        .sum();
    assert!(physical_bytes > 0);

    let blob_usage = get_fragment_blob_usage(&fragment)
        .await
        .expect("blob filesystem usage");
    assert_eq!(blob_usage.used_bytes(), physical_bytes);

    let quota_resp = get_fragment_quota_usage(full_claims(&fragment))
        .await
        .expect("quota usage response")
        .into_response();
    assert_eq!(quota_resp.status(), StatusCode::OK);
    let quota_json = read_response_json(quota_resp).await;
    assert_eq!(quota_json["used_bytes"].as_u64(), Some(physical_bytes));
    assert_eq!(quota_json["reserved_bytes"].as_u64(), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_hard_delete_keeps_blob_quota_usage_until_gc() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = create_fragment_with_quota(2 * 1024 * 1024).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let object_key = "quota/one-shot.bin";

    let upload_resp = upload_one_shot(state.clone(), &fragment, object_key, vec![0x31; 4]).await;
    assert_eq!(upload_resp.status(), StatusCode::CREATED);
    let before_delete = get_fragment_blob_usage(&fragment)
        .await
        .expect("blob usage before delete")
        .used_bytes();
    assert!(before_delete > 0);

    let delete_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), object_key.to_string())),
    )
    .await
    .expect("soft delete quota object")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);
    let after_soft_delete = get_fragment_blob_usage(&fragment)
        .await
        .expect("blob usage after soft delete")
        .used_bytes();
    assert_eq!(after_soft_delete, before_delete);

    let recover_resp = recover_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), object_key.to_string())),
    )
    .await
    .expect("recover quota object")
    .into_response();
    assert_eq!(recover_resp.status(), StatusCode::OK);
    let after_recover = get_fragment_blob_usage(&fragment)
        .await
        .expect("blob usage after recover")
        .used_bytes();
    assert_eq!(after_recover, before_delete);

    let purge_resp = delete_object_permanently(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), object_key.to_string())),
    )
    .await
    .expect("permanently delete quota object")
    .into_response();
    assert_eq!(purge_resp.status(), StatusCode::NO_CONTENT);
    let after_hard_delete = get_fragment_blob_usage(&fragment)
        .await
        .expect("blob usage after hard delete")
        .used_bytes();
    assert_eq!(after_hard_delete, before_delete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_create_fails_cleanly_when_index_root_is_blocked() {
    let _guard = TEST_LOCK.lock().await;
    ensure_master_key_for_tests();
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment_name = format!("fragment-{}", Ulid::new());
    let fragment = hex::encode(Sha256::digest(fragment_name.as_bytes()));
    let index_roots = ObjectPathManager::new().get_all_index_roots();
    let blocked_root = index_roots
        .get(0)
        .expect("at least one index root")
        .join(&fragment);
    fs::write(&blocked_root, b"block index root")
        .await
        .expect("write index blocker");

    let create_err = match create_fragment(Json(super::sync::CreateFragmentRequest {
        name: fragment_name.clone(),
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: Some(TEST_FRAGMENT_QUOTA_BYTES),
    }))
    .await
    {
        Ok(_) => panic!("blocked index root should fail fragment create"),
        Err(err) => err,
    };
    assert!(
        matches!(create_err, AppError::ServiceUnavailable(_)),
        "expected ServiceUnavailable, got {create_err:?}"
    );
    assert!(
        read_fragment_metadata(&fragment).await.is_err(),
        "metadata should not be committed when index root preflight fails"
    );
    assert!(
        !indexdb_store::has_any_fragment_partition_db(&fragment)
            .expect("check owner db absence after blocked index create"),
        "owner db should not be created when index root preflight fails"
    );

    fs::remove_file(&blocked_root)
        .await
        .expect("remove index blocker");
    let create_resp = create_fragment(Json(super::sync::CreateFragmentRequest {
        name: fragment_name,
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: Some(TEST_FRAGMENT_QUOTA_BYTES),
    }))
    .await
    .expect("retry create after unblocking index root")
    .into_response();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_fragment_create_fails_cleanly_when_blob_root_is_blocked() {
    let _guard = TEST_LOCK.lock().await;
    ensure_master_key_for_tests();
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment_name = format!("fragment-{}", Ulid::new());
    let fragment = hex::encode(Sha256::digest(fragment_name.as_bytes()));
    let blob_roots = ObjectPathManager::new().get_blob_roots();
    let blocked_root = blob_roots
        .get(0)
        .expect("at least one blob root")
        .join(&fragment);
    fs::write(&blocked_root, b"block blob root")
        .await
        .expect("write blob blocker");

    let create_err = match create_fragment(Json(super::sync::CreateFragmentRequest {
        name: fragment_name.clone(),
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: Some(TEST_FRAGMENT_QUOTA_BYTES),
    }))
    .await
    {
        Ok(_) => panic!("blocked blob root should fail fragment create"),
        Err(err) => err,
    };
    assert!(
        matches!(create_err, AppError::ServiceUnavailable(_)),
        "expected ServiceUnavailable, got {create_err:?}"
    );
    assert!(
        read_fragment_metadata(&fragment).await.is_err(),
        "metadata should not be committed when blob root preflight fails"
    );
    assert!(
        !indexdb_store::has_any_fragment_partition_db(&fragment)
            .expect("check owner db absence after blocked blob create"),
        "owner db should not be created when blob root preflight fails"
    );

    fs::remove_file(&blocked_root)
        .await
        .expect("remove blob blocker");
    let create_resp = create_fragment(Json(super::sync::CreateFragmentRequest {
        name: fragment_name,
        secret: (*MASTER_KEY).clone(),
        key: "test-fragment-key".to_string(),
        quota_bytes: Some(TEST_FRAGMENT_QUOTA_BYTES),
    }))
    .await
    .expect("retry create after unblocking blob root")
    .into_response();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_sync_fragment_metadata_preserves_active_multipart_blob_dirs() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let (version_id, _session_id) =
        init_upload_with_part_size(&fragment, "uploads/pending.bin", 8192, 2, Some(4096)).await;
    let put_resp = upload_part(&fragment, &version_id, 0, vec![0x5A; 4096]).await;
    assert_eq!(put_resp.status(), StatusCode::OK);

    let status_before = get_upload_status(
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_id.clone())),
    )
    .await
    .expect("upload status before sync")
    .into_response();
    assert_eq!(status_before.status(), StatusCode::OK);
    let status_before_json = read_response_json(status_before).await;
    assert_eq!(
        status_before_json["uploaded_parts"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(status_before_json["uploaded_parts"][0].as_u64(), Some(0));

    let sync_resp = sync_fragment_metadata(full_claims(&fragment))
        .await
        .expect("sync fragment metadata")
        .into_response();
    assert_eq!(sync_resp.status(), StatusCode::OK);

    let status_after = get_upload_status(
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_id.clone())),
    )
    .await
    .expect("upload status after sync")
    .into_response();
    assert_eq!(status_after.status(), StatusCode::OK);
    let status_after_json = read_response_json(status_after).await;
    assert_eq!(
        status_after_json["uploaded_parts"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(status_after_json["uploaded_parts"][0].as_u64(), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_sync_fragment_metadata_preserves_recent_unknown_blob_dirs() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let path_manager = ObjectPathManager::new();
    let blob_root = path_manager
        .get_blob_roots()
        .into_iter()
        .next()
        .expect("blob root");
    let unknown_version_id = Ulid::new().to_string();
    let unknown_dir =
        path_manager.get_shard_dir_on_disk(&blob_root, &fragment, &unknown_version_id);
    fs::create_dir_all(&unknown_dir)
        .await
        .expect("create unknown blob dir");
    fs::write(unknown_dir.join("part_0_shard_0.shard"), b"temporary data")
        .await
        .expect("write unknown blob");

    let sync_resp = sync_fragment_metadata(full_claims(&fragment))
        .await
        .expect("sync fragment metadata")
        .into_response();
    assert_eq!(sync_resp.status(), StatusCode::OK);
    assert!(
        fs::try_exists(&unknown_dir)
            .await
            .expect("unknown dir exists"),
        "recent unknown blob dir should not be deleted immediately"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level 2 index + 2 blob disk root configuration"]
async fn matrix_manual_gc_preserves_active_multipart_sessions() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let (version_id, _session_id) =
        init_upload_with_part_size(&fragment, "uploads/gc-pending.bin", 8192, 2, Some(4096)).await;
    let put_resp = upload_part(&fragment, &version_id, 0, vec![0x33; 4096]).await;
    assert_eq!(put_resp.status(), StatusCode::OK);

    run_manual_gc_for_fragment_and_wait(state, &fragment).await;

    let sessions = index::list_active_sessions(&fragment)
        .await
        .expect("list active sessions after manual gc");
    assert!(
        sessions
            .iter()
            .any(|session| session.manifest.version_id == version_id),
        "manual GC must preserve active multipart sessions"
    );

    let status_after = get_upload_status(
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_id.clone())),
    )
    .await
    .expect("upload status after manual gc")
    .into_response();
    assert_eq!(status_after.status(), StatusCode::OK);
    let status_after_json = read_response_json(status_after).await;
    assert_eq!(
        status_after_json["uploaded_parts"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(status_after_json["uploaded_parts"][0].as_u64(), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level 2 index + 2 blob disk root configuration"]
async fn matrix_manual_gc_reclaims_orphaned_multipart_upload_blob_dirs() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let (version_id, _session_id) =
        init_upload_with_part_size(&fragment, "uploads/gc-orphan.bin", 8192, 2, Some(4096)).await;
    let put_resp = upload_part(&fragment, &version_id, 0, vec![0x44; 4096]).await;
    assert_eq!(put_resp.status(), StatusCode::OK);

    let path_manager = ObjectPathManager::new();
    let version_dirs = path_manager
        .get_blob_roots()
        .into_iter()
        .map(|root| path_manager.get_shard_dir_on_disk(&root, &fragment, &version_id))
        .collect::<Vec<_>>();
    assert!(
        version_dirs
            .iter()
            .any(|path| std::fs::metadata(path).is_ok()),
        "multipart upload should create at least one blob version dir"
    );

    index::remove_session(&fragment, &version_id, "test-orphan")
        .await
        .expect("remove multipart session metadata");
    sleep(Duration::from_secs(2)).await;
    run_manual_gc_for_fragment_and_wait(state, &fragment).await;

    for path in &version_dirs {
        assert!(
            !std::fs::metadata(path).is_ok(),
            "manual GC should reclaim orphaned multipart blob dir {}",
            path.display()
        );
    }
    let gc_status = get_gc_status_api(full_claims("gc-status"))
        .await
        .expect("gc status after orphan reclaim")
        .0;
    assert!(
        gc_status.total_orphan_blob_dirs_deleted >= 1,
        "manual GC should report orphaned multipart reclaim, got {}",
        gc_status.total_orphan_blob_dirs_deleted
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_finalize_upload_waits_for_object_operation_lock() {
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
    let key = "uploads/locked-finalize.bin";
    let content = vec![0x7C; 4096];
    let (version_id, session_id) =
        init_upload_with_part_size(&fragment, key, content.len() as u64, 1, Some(4096)).await;
    let put_resp = upload_part(&fragment, &version_id, 0, content.clone()).await;
    assert_eq!(put_resp.status(), StatusCode::OK);

    let object_lock = index::acquire_object_operation_lock(&fragment, key)
        .await
        .expect("acquire object lock");

    let state_clone = state.clone();
    let fragment_clone = fragment.clone();
    let version_id_clone = version_id.clone();
    let session_id_clone = session_id.clone();
    let key_clone = key.to_string();
    let mut finalize_task = tokio::spawn(async move {
        finalize_upload(
            state_clone,
            &fragment_clone,
            &version_id_clone,
            &session_id_clone,
            &key_clone,
            4096,
            1,
        )
        .await
    });

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut finalize_task)
            .await
            .is_err(),
        "finalize should block while another CRUD operation holds the object lock"
    );

    drop(object_lock);

    let finalize_resp = finalize_task.await.expect("finalize join");
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
            filename: None,
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

async fn bind_logical_path_to_physical_object(
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
) -> ObjectMetadata {
    let metadata = index::read_latest_object(fragment, physical_object_id)
        .await
        .expect("read latest physical object metadata");
    global_namespace_runtime()
        .finalize_logical_upload(fragment, logical_path, physical_object_id, &metadata)
        .await
        .expect("bind logical namespace entry");
    metadata
}

async fn seed_live_object_metadata(fragment: &str, object_key: &str, size_bytes: u64) -> String {
    seed_live_object_metadata_at(fragment, object_key, size_bytes, Utc::now().timestamp()).await
}

async fn seed_live_object_metadata_at(
    fragment: &str,
    object_key: &str,
    size_bytes: u64,
    created_at: i64,
) -> String {
    let version_id = Ulid::new().to_string();
    let upload_session_id = Ulid::new().to_string();
    let metadata = ObjectMetadata {
        object_key: object_key.to_string(),
        fragment: fragment.to_string(),
        version_id: version_id.clone(),
        upload_session_id,
        owner_uid: fragment.to_string(),
        content_type: "application/octet-stream".to_string(),
        total_size_bytes: size_bytes,
        created_at,
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: 262_144,
    };
    index::upsert_object_version(&metadata)
        .await
        .expect("seed live object metadata");
    version_id
}

fn bench_namespace_owner_index(fragment: &str, root_count: usize) -> usize {
    if root_count <= 1 {
        return 0;
    }
    let mut hasher = Sha256::new();
    for part in ["fragment", fragment] {
        hasher.update(part.as_bytes());
        hasher.update([0u8]);
    }
    let hash = hasher.finalize();
    usize::from(u16::from(hash[0]) << 8 | u16::from(hash[1])) % root_count
}

fn bench_namespace_db_paths(fragment: &str) -> Vec<std::path::PathBuf> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    if roots.is_empty() {
        return Vec::new();
    }
    let owner = bench_namespace_owner_index(fragment, roots.len());
    let mut ordered = Vec::with_capacity(roots.len());
    ordered.push(
        roots[owner]
            .join(fragment)
            .join("indexdb")
            .join("index.ndxdb"),
    );
    for (idx, root) in roots.into_iter().enumerate() {
        if idx != owner {
            ordered.push(root.join(fragment).join("indexdb").join("index.ndxdb"));
        }
    }
    ordered
}

fn bench_parent_path(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn bench_base_name(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| path.to_string())
}

fn bench_upsert_parent_dirs(
    entries: &mut BTreeMap<String, n1_indexdb::NamespaceEntry>,
    path: &str,
    changed_at: i64,
) {
    let mut current = String::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        if !current.is_empty() {
            current.push('/');
        }
        current.push_str(segment);
        entries
            .entry(current.clone())
            .or_insert_with(|| n1_indexdb::NamespaceEntry {
                path: current.clone(),
                parent_path: bench_parent_path(&current),
                name: bench_base_name(&current),
                kind: n1_indexdb::NamespaceEntryKind::Directory,
                object_id: None,
                head_version_id: None,
                lifecycle: n1_indexdb::NamespaceLifecycle::Live,
                trash_entry_id: None,
                deleted_at_unix: None,
                created_at_unix: changed_at,
                updated_at_unix: changed_at,
            });
    }
}

fn seed_namespace_bench_batch(fragment: &str, object_keys: &[String], size_bytes: u64) {
    let changed_at = Utc::now().timestamp();
    let objects = object_keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                Ulid::new().to_string(),
                Ulid::new().to_string(),
            )
        })
        .collect::<Vec<_>>();
    let mut entries = BTreeMap::new();
    for (object_key, version_id, _) in &objects {
        bench_upsert_parent_dirs(&mut entries, &bench_parent_path(object_key), changed_at);
        entries.insert(
            object_key.clone(),
            n1_indexdb::NamespaceEntry {
                path: object_key.clone(),
                parent_path: bench_parent_path(object_key),
                name: bench_base_name(object_key),
                kind: n1_indexdb::NamespaceEntryKind::File,
                object_id: Some(object_key.clone()),
                head_version_id: Some(version_id.clone()),
                lifecycle: n1_indexdb::NamespaceLifecycle::Live,
                trash_entry_id: None,
                deleted_at_unix: None,
                created_at_unix: changed_at,
                updated_at_unix: changed_at,
            },
        );
    }

    let db_paths = bench_namespace_db_paths(fragment);
    assert!(
        !db_paths.is_empty(),
        "expected index roots for namespace bench"
    );
    for db_path in db_paths {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).expect("create namespace bench indexdb parent");
        }
        indexdb_store::with_partition_commit_lock(&db_path, || {
            let db = indexdb_store::open_n1_indexdb(&db_path)?;
            let write_txn = db.begin_write().expect("begin namespace bench seed");
            for entry in entries.values() {
                write_txn
                    .n1_upsert_namespace_entry(entry)
                    .expect("seed namespace entry");
            }
            for (object_key, version_id, upload_session_id) in &objects {
                let version = n1_indexdb::ObjectVersion {
                    object_id: object_key.clone(),
                    object_key: object_key.clone(),
                    version_id: version_id.clone(),
                    upload_session_id: upload_session_id.clone(),
                    owner_uid: fragment.to_string(),
                    content_type: "application/octet-stream".to_string(),
                    total_size_bytes: size_bytes,
                    created_at_unix: changed_at,
                    committed_at_unix: Some(changed_at),
                    is_delete_marker: false,
                    stripe_size_bytes: 262_144,
                    checksum: None,
                    metadata_json: None,
                    parts: Vec::new(),
                };
                write_txn
                    .n1_upsert_object_version(&version)
                    .expect("seed object version");
                write_txn
                    .n1_set_object_version_state(&n1_indexdb::ObjectVersionState {
                        object_id: object_key.clone(),
                        version_id: version_id.clone(),
                        state: n1_indexdb::ObjectState::Live,
                        generation: 1,
                        updated_at_unix: changed_at,
                    })
                    .expect("seed object version state");
                write_txn
                    .n1_upsert_object_head(&n1_indexdb::ObjectHead {
                        object_id: object_key.clone(),
                        object_key: object_key.clone(),
                        head_version_id: version_id.clone(),
                        owner_uid: fragment.to_string(),
                        content_type: "application/octet-stream".to_string(),
                        total_size_bytes: size_bytes,
                        state: n1_indexdb::ObjectState::Live,
                        generation: 1,
                        created_at_unix: changed_at,
                        updated_at_unix: changed_at,
                    })
                    .expect("seed object head");
            }
            write_txn.commit().expect("commit namespace bench seed");
            Ok(())
        })
        .expect("seed namespace bench db");
    }
}

fn deep_prefix_key_for_index(idx: usize) -> String {
    let mut state = (idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut parts = Vec::with_capacity(11);
    for depth in 1..=10u64 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407u64.wrapping_add(depth));
        parts.push(format!("pre{depth}-{:03}", state % 211));
    }
    parts.push(format!("file-{idx:05}.txt"));
    parts.join("/")
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
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(262_144, Ordering::Relaxed);

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
    let source_meta = get_metadata(&fragment, &source_key, &fragment)
        .await
        .expect("metadata before redistribution");
    assert_eq!(source_meta.stripe_size_bytes, 16 * 1024);

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
        .uri("/n1/objects/get")
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
    assert_eq!(redistributed_meta.stripe_size_bytes, 16 * 1024);
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
        .store(1, Ordering::Relaxed);

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
    let session_layout_before = indexdb_store::session_layout_stats(&fragment, &version_id)
        .expect("session layout before upload")
        .expect("session layout exists");

    assert_eq!(
        upload_part(&fragment, &version_id, 0, part0.clone())
            .await
            .status(),
        StatusCode::OK
    );
    let session_layout_after_part0 = indexdb_store::session_layout_stats(&fragment, &version_id)
        .expect("session layout after part 0")
        .expect("session layout exists after part 0");
    assert_eq!(
        upload_part(&fragment, &version_id, 1, part1.clone())
            .await
            .status(),
        StatusCode::OK
    );
    let session_layout_after_part1 = indexdb_store::session_layout_stats(&fragment, &version_id)
        .expect("session layout after part 1")
        .expect("session layout exists after part 1");

    assert_eq!(
        session_layout_before.session_row_bytes, session_layout_after_part0.session_row_bytes,
        "session row should stay lean across part uploads"
    );
    assert_eq!(
        session_layout_after_part0.session_row_bytes, session_layout_after_part1.session_row_bytes,
        "session row should not grow with receipt fan-out"
    );
    assert_eq!(session_layout_before.receipt_rows, 0);
    assert_eq!(session_layout_after_part0.receipt_rows, 1);
    assert_eq!(session_layout_after_part1.receipt_rows, 2);
    assert_eq!(session_layout_after_part1.part_rows, 0);
    assert!(session_layout_before.owner_db_bytes > 0);
    assert!(session_layout_after_part1.owner_db_bytes > 0);

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
    assert!(
        receipts[0]
            .as_ref()
            .map(|receipt| receipt.shard_size_bytes > 0)
            .unwrap_or(false),
        "compact receipts should persist shard sizing for finalize reconstruction"
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
#[ignore = "benchmark-style local multipart stats"]
async fn matrix_multipart_receipt_indexdb_local_stats() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
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
        .store(64 * 1024, Ordering::Relaxed);
    MAX_PART_SIZE_ATOMIC
        .get()
        .expect("MAX_PART_SIZE_ATOMIC")
        .store(16 * 1024 * 1024, Ordering::Relaxed);
    UPLOAD_IO_GATE_ENABLED_ATOMIC
        .get()
        .expect("UPLOAD_IO_GATE_ENABLED_ATOMIC")
        .store(true, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let key = "matrix/receipt-bench.bin".to_string();
    let part_count = 256u64;
    let part_size = 64 * 1024u64;
    let total_len = part_count * part_size;
    let part_payload = vec![0x5a; part_size as usize];

    init_quota_file(&fragment, 512 * 1024 * 1024)
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
        init_upload_with_part_size(&fragment, &key, total_len, part_count, Some(part_size)).await;

    let layout_before = indexdb_store::session_layout_stats(&fragment, &version_id)
        .expect("session layout before upload")
        .expect("session layout exists");

    let upload_started = std::time::Instant::now();
    for part_index in 0..part_count {
        let response = upload_part(&fragment, &version_id, part_index, part_payload.clone()).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let upload_elapsed = upload_started.elapsed();

    let layout_after_upload = indexdb_store::session_layout_stats(&fragment, &version_id)
        .expect("session layout after upload")
        .expect("session layout exists after upload");

    let finalize_started = std::time::Instant::now();
    let finalize_ok = finalize_upload_with_part_size(
        state,
        &fragment,
        &version_id,
        &session_id,
        &key,
        total_len,
        part_count,
        Some(part_size),
    )
    .await;
    let finalize_elapsed = finalize_started.elapsed();
    assert_eq!(finalize_ok.status(), StatusCode::OK);

    println!(
        "multipart_receipt_indexdb_local_stats parts={} part_size={} total_bytes={} upload_ms={} avg_part_ms={:.3} finalize_ms={} session_row_before={} session_row_after={} receipt_rows={} part_rows={} owner_db_bytes={}",
        part_count,
        part_size,
        total_len,
        upload_elapsed.as_millis(),
        upload_elapsed.as_secs_f64() * 1000.0 / part_count as f64,
        finalize_elapsed.as_millis(),
        layout_before.session_row_bytes,
        layout_after_upload.session_row_bytes,
        layout_after_upload.receipt_rows,
        layout_after_upload.part_rows,
        layout_after_upload.owner_db_bytes
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-style local one-shot upload stats"]
async fn matrix_one_shot_upload_indexdb_local_stats() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
    ensure_upload_runtime_config();

    let workers = test_env_usize("N1_ONESHOT_PROBE_CONCURRENCY", 2, 1, 64);
    let uploads_per_worker = test_env_usize("N1_ONESHOT_PROBE_OBJECTS_PER_WORKER", 2, 1, 1024);
    let object_bytes = test_env_u64(
        "N1_ONESHOT_PROBE_OBJECT_BYTES",
        16 * 1024 * 1024,
        1024 * 1024,
        1024 * 1024 * 1024,
    ) as usize;
    let chunk_bytes = test_env_usize(
        "N1_ONESHOT_PROBE_CHUNK_BYTES",
        1024 * 1024,
        64 * 1024,
        64 * 1024 * 1024,
    );
    let writer_buffer_bytes = test_env_usize(
        "N1_ONESHOT_PROBE_WRITER_BUFFER_BYTES",
        512 * 1024,
        64 * 1024,
        8 * 1024 * 1024,
    );
    let data_shards = test_env_usize("N1_ONESHOT_PROBE_DATA_SHARDS", 1, 1, 16);
    let parity_shards = test_env_usize("N1_ONESHOT_PROBE_PARITY_SHARDS", 1, 0, 16);
    let min_mib_per_sec = test_env_u64("N1_ONESHOT_PROBE_MIN_MIBPS", 0, 0, 100_000) as f64;
    let upload_durability = std::env::var("N1_ONESHOT_PROBE_DURABILITY")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .and_then(|v| match v.as_str() {
            "strict" | "sync" | "durable" => Some("strict".to_string()),
            "batched" | "async" | "fast" | "relaxed" => Some("batched".to_string()),
            _ => None,
        });
    let upload_io_gate = std::env::var("N1_ONESHOT_PROBE_IO_GATE")
        .ok()
        .and_then(|v| match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        });

    MAX_PART_SIZE_ATOMIC
        .get()
        .expect("MAX_PART_SIZE_ATOMIC")
        .store(object_bytes as u64 + 4096, Ordering::Relaxed);
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(data_shards, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(parity_shards, Ordering::Relaxed);
    UPLOAD_IO_GATE_ENABLED_ATOMIC
        .get()
        .expect("UPLOAD_IO_GATE_ENABLED_ATOMIC")
        .store(true, Ordering::Relaxed);
    WRITER_BUFFER_SIZE_ATOMIC
        .get()
        .expect("WRITER_BUFFER_SIZE_ATOMIC")
        .store(writer_buffer_bytes, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    let total_uploads = workers * uploads_per_worker;
    let total_bytes = total_uploads as u64 * object_bytes as u64;
    setup_fragment(&fragment).await;
    init_quota_file(
        &fragment,
        total_bytes.saturating_mul(2).max(64 * 1024 * 1024),
    )
    .await
    .expect("set one-shot probe quota");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    let started = Instant::now();

    stream::iter(0..workers)
        .for_each_concurrent(workers, |w| {
            let state = state.clone();
            let fragment = fragment.clone();
            let upload_durability = upload_durability.clone();
            async move {
                for i in 0..uploads_per_worker {
                    let key = format!("one-shot-probe/w{}/obj-{}.bin", w, i);
                    let response = upload_one_shot_generated_chunked_with_options(
                        state.clone(),
                        &fragment,
                        &key,
                        object_bytes,
                        chunk_bytes,
                        w * 17 + i,
                        upload_durability.as_deref(),
                        upload_io_gate,
                    )
                    .await;
                    assert_eq!(response.status(), StatusCode::CREATED);
                }
            }
        })
        .await;

    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    let mib_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed;

    println!(
        "one_shot_upload_indexdb_local_stats workers={} uploads_per_worker={} object_bytes={} chunk_bytes={} writer_buffer_bytes={} ec={}D+{}P durability={} io_gate={} total_uploads={} total_mib={:.2} elapsed_s={:.3} throughput_mib_s={:.2}",
        workers,
        uploads_per_worker,
        object_bytes,
        chunk_bytes,
        writer_buffer_bytes,
        data_shards,
        parity_shards,
        upload_durability.as_deref().unwrap_or("default"),
        upload_io_gate
            .map(|v| if v { "on" } else { "off" })
            .unwrap_or("default"),
        total_uploads,
        total_bytes as f64 / (1024.0 * 1024.0),
        elapsed,
        mib_per_sec
    );

    if min_mib_per_sec > 0.0 {
        assert!(
            mib_per_sec >= min_mib_per_sec,
            "one-shot throughput too low: {:.2} MiB/s < {:.2} MiB/s",
            mib_per_sec,
            min_mib_per_sec
        );
    }
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
async fn matrix_upload_part_rejects_when_target_blob_disk_becomes_unavailable() {
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
    let key = "uploads/disk-loss.bin";
    let part = vec![0x5Au8; 4096];

    setup_fragment(&fragment).await;
    let (version_id, _session_id) = init_upload(&fragment, key, part.len() as u64, 1).await;

    let blob1 = base.join("blob_1");
    let blob1_down = base.join("blob_1.down");
    if fs::try_exists(&blob1_down).await.unwrap_or(false) {
        let _ = fs::remove_dir_all(&blob1_down).await;
    }
    fs::rename(&blob1, &blob1_down)
        .await
        .expect("simulate target blob disk down");
    fs::write(&blob1, b"offline")
        .await
        .expect("block blob root recreation");

    let mut headers = HeaderMap::new();
    headers.insert(
        "content-length",
        HeaderValue::from_str(&part.len().to_string()).expect("content-length"),
    );

    let upload_result = object_upload_part(
        full_claims(&fragment),
        headers,
        AxumPath((fragment.clone(), version_id.clone(), 0)),
        Query(UploadPartQuery {
            checksum: None,
            durability: None,
            strict_durability: None,
            io_gate: None,
        }),
        Body::from(part),
    )
    .await;

    let status_json = read_response_json(
        get_upload_status(
            full_claims(&fragment),
            AxumPath((fragment.clone(), version_id.clone())),
        )
        .await
        .expect("upload status response")
        .into_response(),
    )
    .await;

    let _ = fs::remove_file(&blob1).await;
    if fs::try_exists(&blob1_down).await.unwrap_or(false) {
        fs::rename(&blob1_down, &blob1)
            .await
            .expect("restore target blob disk");
    }

    match upload_result {
        Err(AppError::ServiceUnavailable(message)) => {
            assert!(
                message.contains("Target blob disk"),
                "unexpected service-unavailable error: {}",
                message
            );
        }
        Err(other) => panic!("expected service unavailable, got {:?}", other),
        Ok(_) => panic!("upload should fail when target blob root is unavailable"),
    }

    assert_eq!(
        status_json["uploaded_parts"]
            .as_array()
            .expect("uploaded_parts array")
            .len(),
        0,
        "failed upload should not commit any part state"
    );
    assert_eq!(status_json["committed_parts"].as_u64().unwrap_or(0), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level disk root configuration"]
async fn matrix_upload_init_rejects_layout_when_blob_roots_are_fewer_than_total_shards() {
    let _guard = TEST_LOCK.lock().await;
    let base = std::env::temp_dir().join(format!("n1-upload-layout-{}", std::process::id()));
    ensure_global_storage_config_with_root_counts(&base, 1, 1).await;
    ensure_upload_runtime_config();
    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let init_result = initiate_object_upload(
        full_claims(&fragment),
        AxumPath((fragment.clone(), "uploads/layout-reject.bin".to_string())),
        AxumJson(InitiateUploadParams {
            content_type: Some("application/octet-stream".into()),
            total_size_bytes: 4096,
            expected_parts: 1,
            part_size_bytes: Some(4096),
        }),
    )
    .await;

    match init_result {
        Err(AppError::ServiceUnavailable(message)) => {
            assert!(
                message.contains("cannot satisfy target layout"),
                "unexpected upload init failure: {}",
                message
            );
        }
        Ok(_) => {
            panic!("expected upload init to reject layout that needs more distinct blob roots")
        }
        Err(other) => panic!("expected service unavailable, got {}", other),
    }

    let _ = fs::remove_dir_all(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_read_survives_single_blob_disk_loss_with_parity() {
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
    let key = "reads/blob-disk-loss.bin".to_string();
    let content = (0..(72 * 1024 + 37))
        .map(|i| ((i * 13) % 251) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(state, &fragment, &key, content.clone()).await;

    let blob_2 = base.join("blob_2");
    let blob_2_parked = base.join("blob_2.down");
    park_simulated_disk_with_blocker(
        &blob_2,
        &blob_2_parked,
        b"n1-blob-disk-down-blocker",
        "park blob_2 for read fallback test",
    )
    .await;

    let got = read_object_bytes_eventually(fragment.clone(), key.clone(), 24).await;
    assert_eq!(got, content);

    let meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata should remain readable with one blob disk down");
    assert_eq!(meta.object_key, key);

    restore_simulated_disk(&blob_2, &blob_2_parked, "restore blob_2 after read test").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level 2 index + 2 blob disk root configuration"]
async fn matrix_missing_fragment_on_one_blob_root_reads_through_and_repair_rebuilds_it() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
    ensure_upload_runtime_config();
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
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key_a = "reads/missing-fragment-root-a.bin".to_string();
    let key_b = "reads/missing-fragment-root-b.bin".to_string();
    let bytes_a = (0..(64 * 1024 + 17))
        .map(|i| ((i * 3) % 251) as u8)
        .collect::<Vec<u8>>();
    let bytes_b = (0..(48 * 1024 + 91))
        .map(|i| ((i * 11 + 7) % 241) as u8)
        .collect::<Vec<u8>>();
    put_object(state.clone(), &fragment, &key_a, bytes_a.clone()).await;
    put_object(state, &fragment, &key_b, bytes_b.clone()).await;

    let meta_a = get_metadata(&fragment, &key_a, &fragment)
        .await
        .expect("metadata for key_a");
    let meta_b = get_metadata(&fragment, &key_b, &fragment)
        .await
        .expect("metadata for key_b");

    let shard_root = |absolute_path: &str| -> std::path::PathBuf {
        std::path::Path::new(absolute_path)
            .ancestors()
            .nth(4)
            .expect("shard path should include blob root")
            .to_path_buf()
    };

    let missing_root = meta_a.parts[0]
        .shards
        .iter()
        .map(|shard| shard_root(&shard.absolute_path))
        .find(|root| {
            meta_b.parts[0]
                .shards
                .iter()
                .any(|shard| shard_root(&shard.absolute_path) == *root)
        })
        .expect("objects should share at least one blob root");
    let missing_fragment_dir = missing_root.join(&fragment);

    let tracked_paths = meta_a.parts[0]
        .shards
        .iter()
        .chain(meta_b.parts[0].shards.iter())
        .filter(|shard| std::path::Path::new(&shard.absolute_path).starts_with(&missing_root))
        .map(|shard| shard.absolute_path.clone())
        .collect::<Vec<_>>();
    assert!(
        !tracked_paths.is_empty(),
        "expected at least one shard path under {:?}",
        missing_root
    );
    assert!(
        fs::try_exists(&missing_fragment_dir).await.unwrap_or(false),
        "fragment dir should exist before deletion: {:?}",
        missing_fragment_dir
    );

    fs::remove_dir_all(&missing_fragment_dir)
        .await
        .expect("delete fragment subtree on one blob root");
    assert!(
        !fs::try_exists(&missing_fragment_dir).await.unwrap_or(false),
        "fragment dir should be gone on deleted blob root"
    );

    let read_a = read_object_bytes_eventually(fragment.clone(), key_a.clone(), 24).await;
    let read_b = read_object_bytes_eventually(fragment.clone(), key_b.clone(), 24).await;
    assert_eq!(
        read_a, bytes_a,
        "read should recover key_a with one fragment root missing"
    );
    assert_eq!(
        read_b, bytes_b,
        "read should recover key_b with one fragment root missing"
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
    .expect("repair should rebuild the missing fragment subtree");

    assert!(
        fs::try_exists(&missing_fragment_dir).await.unwrap_or(false),
        "repair should recreate the missing fragment dir: {:?}",
        missing_fragment_dir
    );
    for path in &tracked_paths {
        assert!(
            fs::try_exists(std::path::Path::new(path))
                .await
                .unwrap_or(false),
            "repair should restore shard path {}",
            path
        );
    }

    let repaired_a = read_object_bytes(full_claims(&fragment), fragment.clone(), key_a).await;
    let repaired_b = read_object_bytes(full_claims(&fragment), fragment.clone(), key_b).await;
    assert_eq!(repaired_a, bytes_a);
    assert_eq!(repaired_b, bytes_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level 2 index + 2 blob disk root configuration"]
async fn matrix_missing_fragment_on_one_blob_root_reads_through_and_redistribution_rebuilds_it() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
    ensure_upload_runtime_config();
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
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key_a = "reads/missing-fragment-redist-a.bin".to_string();
    let key_b = "reads/missing-fragment-redist-b.bin".to_string();
    let bytes_a = (0..(64 * 1024 + 53))
        .map(|i| ((i * 9 + 1) % 251) as u8)
        .collect::<Vec<u8>>();
    let bytes_b = (0..(52 * 1024 + 117))
        .map(|i| ((i * 13 + 5) % 241) as u8)
        .collect::<Vec<u8>>();
    put_object(state.clone(), &fragment, &key_a, bytes_a.clone()).await;
    put_object(state, &fragment, &key_b, bytes_b.clone()).await;

    let meta_a = get_metadata(&fragment, &key_a, &fragment)
        .await
        .expect("metadata for key_a");
    let meta_b = get_metadata(&fragment, &key_b, &fragment)
        .await
        .expect("metadata for key_b");

    let shard_root = |absolute_path: &str| -> std::path::PathBuf {
        std::path::Path::new(absolute_path)
            .ancestors()
            .nth(4)
            .expect("shard path should include blob root")
            .to_path_buf()
    };

    let missing_root = meta_a.parts[0]
        .shards
        .iter()
        .map(|shard| shard_root(&shard.absolute_path))
        .find(|root| {
            meta_b.parts[0]
                .shards
                .iter()
                .any(|shard| shard_root(&shard.absolute_path) == *root)
        })
        .expect("objects should share at least one blob root");
    let missing_fragment_dir = missing_root.join(&fragment);

    let tracked_paths = meta_a.parts[0]
        .shards
        .iter()
        .chain(meta_b.parts[0].shards.iter())
        .filter(|shard| std::path::Path::new(&shard.absolute_path).starts_with(&missing_root))
        .map(|shard| shard.absolute_path.clone())
        .collect::<Vec<_>>();
    assert!(
        !tracked_paths.is_empty(),
        "expected at least one shard path under {:?}",
        missing_root
    );

    fs::remove_dir_all(&missing_fragment_dir)
        .await
        .expect("delete fragment subtree on one blob root");
    assert!(
        !fs::try_exists(&missing_fragment_dir).await.unwrap_or(false),
        "fragment dir should be gone on deleted blob root"
    );

    let read_a = read_object_bytes_eventually(fragment.clone(), key_a.clone(), 24).await;
    let read_b = read_object_bytes_eventually(fragment.clone(), key_b.clone(), 24).await;
    assert_eq!(
        read_a, bytes_a,
        "read should recover key_a with one fragment root missing"
    );
    assert_eq!(
        read_b, bytes_b,
        "read should recover key_b with one fragment root missing"
    );

    run_redistribution_job_with_options(
        false,
        RedistributionRunOptions {
            fast_mode: true,
            execution_mode: RedistributionExecutionMode::Strict,
            durability_mode: RedistributionDurabilityMode::Batched,
            target_fragment: Some(fragment.clone()),
            ..Default::default()
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("redistribution should rebuild the missing fragment subtree");

    assert!(
        fs::try_exists(&missing_fragment_dir).await.unwrap_or(false),
        "redistribution should recreate the missing fragment dir: {:?}",
        missing_fragment_dir
    );
    for path in &tracked_paths {
        assert!(
            fs::try_exists(std::path::Path::new(path))
                .await
                .unwrap_or(false),
            "redistribution should restore shard path {}",
            path
        );
    }

    let rebuilt_a = read_object_bytes(full_claims(&fragment), fragment.clone(), key_a).await;
    let rebuilt_b = read_object_bytes(full_claims(&fragment), fragment.clone(), key_b).await;
    assert_eq!(rebuilt_a, bytes_a);
    assert_eq!(rebuilt_b, bytes_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_metadata_modify_ops_survive_single_blob_disk_loss() {
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
    let source_key = "modify/blob-disk-loss/source.bin".to_string();
    let renamed_key = "modify/blob-disk-loss/renamed.bin".to_string();
    let content = (0..(48 * 1024 + 11))
        .map(|i| ((i * 19 + 7) % 251) as u8)
        .collect::<Vec<u8>>();

    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });
    put_object(state.clone(), &fragment, &source_key, content.clone()).await;

    let blob_2 = base.join("blob_2");
    let blob_2_parked = base.join("blob_2.down");
    park_simulated_disk_with_blocker(
        &blob_2,
        &blob_2_parked,
        b"n1-blob-disk-down-blocker",
        "park blob_2 for metadata modify test",
    )
    .await;

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
    .expect("rename should succeed with one blob disk down")
    .into_response();
    assert_eq!(rename_resp.status(), StatusCode::OK);

    assert!(
        matches!(
            get_metadata(&fragment, &source_key, &fragment).await,
            Err(AppError::NotFound)
        ),
        "source key should no longer exist after rename"
    );

    let renamed_meta = get_metadata(&fragment, &renamed_key, &fragment)
        .await
        .expect("renamed metadata");
    assert_eq!(renamed_meta.object_key, renamed_key);

    let delete_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), renamed_key.clone())),
    )
    .await
    .expect("soft delete should succeed with one blob disk down")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let recover_resp = recover_object(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), renamed_key.clone())),
    )
    .await
    .expect("recover should succeed with one blob disk down")
    .into_response();
    assert_eq!(recover_resp.status(), StatusCode::OK);

    let got = read_object_bytes_eventually(fragment.clone(), renamed_key.clone(), 24).await;
    assert_eq!(got, content);

    restore_simulated_disk(
        &blob_2,
        &blob_2_parked,
        "restore blob_2 after metadata modify test",
    )
    .await;
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
    assert_eq!(del_ver.status(), StatusCode::NO_CONTENT);
    assert!(
        recover_object_version(
            State(state.clone()),
            full_claims(&fragment),
            AxumPath((fragment.clone(), ver1.clone(), key.clone())),
        )
        .await
        .is_err(),
        "directly deleted version should not be recoverable from trash"
    );

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
        .uri("/n1/objects/get")
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
async fn matrix_bulk_permanent_delete_removes_live_and_trashed_objects() {
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

    let live_key = "permanent-bulk/live.bin".to_string();
    let trashed_key = "permanent-bulk/trashed.bin".to_string();
    put_object(state.clone(), &fragment, &live_key, vec![0x41; 2048]).await;
    put_object(state.clone(), &fragment, &trashed_key, vec![0x42; 2048]).await;

    let soft_delete = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), trashed_key.clone())),
    )
    .await
    .expect("soft delete before permanent bulk delete")
    .into_response();
    assert_eq!(soft_delete.status(), StatusCode::OK);

    let resp = delete_objects_bulk_permanently(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(BulkObjectKeysRequest {
            object_keys: vec![live_key.clone(), trashed_key.clone()],
        }),
    )
    .await
    .expect("bulk permanent delete")
    .into_response();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("bulk permanent delete body");
    let parsed: serde_json::Value =
        serde_json::from_slice(&body).expect("bulk permanent delete json");
    assert_eq!(parsed["succeeded"].as_u64(), Some(2));
    assert_eq!(parsed["not_found"].as_u64(), Some(0));

    for key in [live_key, trashed_key] {
        let req = Request::builder()
            .uri("/n1/objects/get")
            .body(Body::empty())
            .expect("build get request");
        assert!(
            get_object(
                full_claims(&fragment),
                AxumPath((fragment.clone(), key.clone())),
                req,
            )
            .await
            .is_err(),
            "permanent bulk delete should remove {}",
            key
        );
    }

    assert!(
        index::list_fragment_trash_objects(&fragment)
            .await
            .expect("trash list after permanent bulk delete")
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_soft_delete_and_recover_are_metadata_only() {
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

    let key = "lifecycle/metadata-only.bin".to_string();
    let bytes = vec![0x6a; 4096];
    let version_id = put_object(state.clone(), &fragment, &key, bytes.clone()).await;

    let delete_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("soft delete")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let path_manager = ObjectPathManager::new();
    for root in path_manager.get_blob_roots() {
        let live_blob_dir = path_manager.get_shard_dir_on_disk(&root, &fragment, &version_id);
        let trash_blob_dir = root
            .join(&fragment)
            .join(".trash")
            .join("blob")
            .join(&version_id);
        assert!(
            fs::try_exists(&live_blob_dir)
                .await
                .expect("live blob exists after soft delete"),
            "soft delete should leave blob in live root"
        );
        assert!(
            !fs::try_exists(&trash_blob_dir)
                .await
                .expect("trash blob exists after soft delete"),
            "soft delete should not move blob into .trash"
        );
    }

    let recover_resp = recover_object(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("recover metadata-only object")
    .into_response();
    assert_eq!(recover_resp.status(), StatusCode::OK);

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key).await;
    assert_eq!(got, bytes);
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
    assert_eq!(first_delete.status(), StatusCode::NO_CONTENT);

    assert!(
        delete_object_version(
            State(state),
            full_claims(&fragment),
            AxumPath((fragment.clone(), ver1.clone(), key.clone())),
        )
        .await
        .is_err(),
        "second direct delete should not find the version anymore"
    );

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
async fn matrix_gc_preserves_recent_unbound_physical_live_object() {
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

    let physical_key = format!("physical-{}", Ulid::new());
    let physical_payload = vec![0x77; 4096];
    let physical_version = put_object(
        state.clone(),
        &fragment,
        &physical_key,
        physical_payload.clone(),
    )
    .await;

    let referenced_physical_key = format!("physical-{}", Ulid::new());
    put_object(
        state.clone(),
        &fragment,
        &referenced_physical_key,
        vec![0x42; 4096],
    )
    .await;
    bind_logical_path_to_physical_object(&fragment, "gc/referenced.bin", &referenced_physical_key)
        .await;

    run_manual_gc_for_fragment_and_wait(state, &fragment).await;

    let bytes = read_object_bytes(full_claims(&fragment), fragment.clone(), physical_key).await;
    assert_eq!(
        bytes, physical_payload,
        "manual GC must not reclaim a recently committed physical object while namespace binding may still be catching up"
    );
    for root in ObjectPathManager::new().get_blob_roots() {
        assert!(
            root.join(&fragment)
                .join("blob")
                .join(&physical_version)
                .exists(),
            "GC removed a protected live blob directory from {}",
            root.display()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_gc_reclaims_stale_unbound_completed_object_when_namespace_is_available() {
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

    let stale_physical_key = format!("physical-{}", Ulid::new());
    let stale_version = seed_live_object_metadata_at(
        &fragment,
        &stale_physical_key,
        4096,
        Utc::now().timestamp() - 7200,
    )
    .await;

    let referenced_physical_key = format!("physical-{}", Ulid::new());
    put_object(
        state.clone(),
        &fragment,
        &referenced_physical_key,
        vec![0x42; 4096],
    )
    .await;
    bind_logical_path_to_physical_object(&fragment, "gc/referenced.bin", &referenced_physical_key)
        .await;

    run_manual_gc_for_fragment_and_wait(state, &fragment).await;

    assert!(
        index::read_object_version(&fragment, &stale_physical_key, &stale_version)
            .await
            .is_err(),
        "manual GC should reclaim stale completed object metadata when namespace reachability is available and no binding references it"
    );
    let referenced = read_object_bytes(
        full_claims(&fragment),
        fragment.clone(),
        referenced_physical_key,
    )
    .await;
    assert_eq!(referenced, vec![0x42; 4096]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_namespace_write_catches_up_stale_owner_after_redistribution_restart() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    global_namespace_runtime()
        .mkdir(&fragment, "redistribution")
        .await
        .expect("seed namespace root dir");

    let paths = object_partition_db_paths_owner_first(&fragment, "redistribution/probe.bin");
    assert!(
        paths.len() >= 2,
        "test requires at least two indexdb replicas"
    );
    let owner_path = paths[0].clone();
    let stale_owner = owner_path.with_file_name(format!("stale-owner-{}.ndxdb", Ulid::new()));
    {
        indexdb_store::evict_cached_db_for_test(&owner_path);
        let owner_db =
            indexdb_store::open_n1_indexdb(&owner_path).expect("open owner for stale backup");
        owner_db
            .backup_to_path(&stale_owner)
            .expect("backup stale owner indexdb");
    }

    global_namespace_runtime()
        .mkdir(&fragment, "redistribution/newer-on-follower")
        .await
        .expect("create newer namespace state");
    assert_all_indexdb_replicas_synchronized(&fragment).await;

    global_namespace_runtime().evict_fragment(&fragment);
    indexdb_store::evict_cached_db_for_test(&owner_path);
    let owner_file_name = owner_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("index.ndxdb")
        .to_string();
    for candidate in [
        owner_path.clone(),
        owner_path.with_file_name(format!("{owner_file_name}-wal")),
        owner_path.with_file_name(format!("{owner_file_name}-shm")),
    ] {
        let _ = std::fs::remove_file(candidate);
    }
    std::fs::rename(&stale_owner, &owner_path).expect("restore stale owner copy");

    global_namespace_runtime()
        .mkdir(&fragment, "redistribution/after-restart")
        .await
        .expect("namespace write must catch up owner from newer follower first");

    let entries = global_namespace_runtime()
        .list_dir(&fragment, "redistribution", false)
        .await
        .expect("list redistribution dir");
    let names = entries
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    assert!(names.contains(&"newer-on-follower".to_string()));
    assert!(names.contains(&"after-restart".to_string()));
    assert_all_indexdb_replicas_synchronized(&fragment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_namespace_indexdb_manual_gc_preserves_trashed_objects_until_empty_trash() {
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

    let physical_object_id = Ulid::new().to_string();
    put_object(
        state.clone(),
        &fragment,
        &physical_object_id,
        vec![0x5a; 4096],
    )
    .await;
    bind_logical_path_to_physical_object(&fragment, "docs/keep.bin", &physical_object_id).await;

    global_namespace_runtime()
        .trash(&fragment, "docs/keep.bin")
        .await
        .expect("trash namespace entry");

    run_manual_gc_and_wait(state).await;

    assert!(
        global_namespace_runtime()
            .trashed_entry(&fragment, "docs/keep.bin")
            .await
            .expect("trashed namespace entry lookup")
            .is_some(),
        "trashed namespace entry must remain recoverable after manual GC"
    );
    assert!(
        index::read_latest_object(&fragment, &physical_object_id)
            .await
            .is_ok(),
        "manual GC must not reclaim namespace-trashed object before empty trash"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_namespace_indexdb_empty_trash_makes_manual_gc_reclaim_physical_objects() {
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

    let physical_object_id = Ulid::new().to_string();
    let version_id = put_object(
        state.clone(),
        &fragment,
        &physical_object_id,
        vec![0x33; 4096],
    )
    .await;
    bind_logical_path_to_physical_object(&fragment, "docs/reclaim.bin", &physical_object_id).await;

    global_namespace_runtime()
        .trash(&fragment, "docs/reclaim.bin")
        .await
        .expect("trash namespace entry");
    global_namespace_runtime()
        .empty_trash(&fragment)
        .await
        .expect("empty namespace trash");

    run_manual_gc_and_wait(state).await;

    assert!(
        global_namespace_runtime()
            .entry(&fragment, "docs/reclaim.bin")
            .await
            .expect("live namespace entry lookup")
            .is_none(),
        "empty trash must remove live namespace entry"
    );
    assert!(
        global_namespace_runtime()
            .trashed_entry(&fragment, "docs/reclaim.bin")
            .await
            .expect("trash namespace entry lookup")
            .is_none(),
        "empty trash must remove trash namespace entry"
    );
    assert!(
        index::read_latest_object(&fragment, &physical_object_id)
            .await
            .is_err(),
        "manual GC must reclaim physical object once namespace no longer references it"
    );
    assert!(
        index::list_live_object_versions_metadata(&fragment, &physical_object_id)
            .await
            .unwrap_or_default()
            .is_empty(),
        "reclaimed object must not keep live version metadata"
    );

    for root in ObjectPathManager::new().get_blob_roots() {
        assert!(
            !root.join(&fragment).join("blob").join(&version_id).exists(),
            "reclaimed object blob directory must be removed from {}",
            root.display()
        );
    }

    let gc_status = get_gc_status_api(full_claims("gc-status"))
        .await
        .expect("gc status after manual reclaim")
        .0;
    assert!(
        gc_status.total_refund_bytes >= 4096,
        "manual GC should report refunded bytes after reclaim, got {}",
        gc_status.total_refund_bytes
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_namespace_indexdb_individual_purge_targeted_gc_reclaims_physical_object() {
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

    let physical_object_id = Ulid::new().to_string();
    let version_id = put_object(
        state.clone(),
        &fragment,
        &physical_object_id,
        vec![0x44; 4096],
    )
    .await;
    bind_logical_path_to_physical_object(&fragment, "purge-target.bin", &physical_object_id).await;

    global_namespace_runtime()
        .trash(&fragment, "purge-target.bin")
        .await
        .expect("trash namespace entry");
    let trash_entry = global_namespace_runtime()
        .trashed_entry(&fragment, "purge-target.bin")
        .await
        .expect("trash namespace entry lookup")
        .expect("trash namespace entry");
    let trash_entry_id = trash_entry
        .trash_entry_id
        .expect("trash namespace entry id");
    global_namespace_runtime()
        .purge_trash_entry_id(&fragment, &trash_entry_id)
        .await
        .expect("purge individual trash entry");

    let gc_status = run_manual_gc_for_fragment_and_wait(state, &fragment).await;

    assert_eq!(
        gc_status.total_fragments, 1,
        "targeted manual GC must process exactly one fragment"
    );
    assert_eq!(
        gc_status.total_fragments_scanned, 1,
        "targeted manual GC must scan exactly one fragment"
    );
    assert!(
        global_namespace_runtime()
            .trashed_entry(&fragment, "purge-target.bin")
            .await
            .expect("trash namespace entry lookup after purge")
            .is_none(),
        "purged namespace entry must leave trash listing"
    );
    assert!(
        index::read_latest_object(&fragment, &physical_object_id)
            .await
            .is_err(),
        "targeted manual GC must reclaim an individually purged object"
    );
    for root in ObjectPathManager::new().get_blob_roots() {
        assert!(
            !root.join(&fragment).join("blob").join(&version_id).exists(),
            "targeted manual GC must remove reclaimed blob directory from {}",
            root.display()
        );
    }
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

    match redist_res {
        Err(AppError::InternalServerError(message)) => {
            assert!(
                message.contains("Insufficient storage"),
                "unexpected redistribution failure: {}",
                message
            );
        }
        other => panic!(
            "expected redistribution to fail when one live blob disk is missing for the target layout, got {:?}",
            other
        ),
    }

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

    put_object(
        state.clone(),
        &fragment,
        visible_key,
        visible_payload.clone(),
    )
    .await;
    put_object(state, &fragment, internal_key, internal_payload.clone()).await;

    let export_resp = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment.clone()),
        Json(DownloadArchiveRequest {
            keys: Vec::new(),
            prefix: None,
            prefixes: Vec::new(),
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
            prefix: None,
            prefixes: Vec::new(),
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
            prefix: None,
            prefixes: Vec::new(),
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

    let version_a = put_object(state.clone(), &fragment, key_a, payload_a.clone()).await;
    let version_b = put_object(state.clone(), &fragment, key_b, payload_b.clone()).await;
    put_object(state, &fragment, key_c, b"charlie".to_vec()).await;

    let manifest = format!(
        "{version_b}\t{key_b}\nmissing/object.bin\n/{key_a}\n{version_a}\tmissing/versioned.bin\n"
    );
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
async fn matrix_archive_tar_streams_selected_prefixes() {
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

    put_object(
        state.clone(),
        &fragment,
        "archive/folder/alpha.bin",
        b"alpha".to_vec(),
    )
    .await;
    put_object(
        state.clone(),
        &fragment,
        "archive/folder/bravo.bin",
        b"bravo".to_vec(),
    )
    .await;
    put_object(
        state,
        &fragment,
        "archive/elsewhere.bin",
        b"elsewhere".to_vec(),
    )
    .await;

    let resp = download_selected_archive(
        full_claims(&fragment),
        AxumPath(fragment),
        Json(DownloadArchiveRequest {
            keys: Vec::new(),
            prefix: None,
            prefixes: vec!["archive/folder/".to_string()],
            format: "tar".to_string(),
            filename: Some("folder-export".to_string()),
            export_all: false,
            include_internal: false,
        }),
    )
    .await
    .expect("prefix archive export")
    .into_response();

    assert_eq!(resp.status(), StatusCode::OK);
    let entries = read_tar_entries(read_response_bytes(resp).await).await;
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries.get("archive/folder/alpha.bin").map(Vec::as_slice),
        Some(b"alpha".as_slice())
    );
    assert_eq!(
        entries.get("archive/folder/bravo.bin").map(Vec::as_slice),
        Some(b"bravo".as_slice())
    );
    assert!(!entries.contains_key("archive/elsewhere.bin"));
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
async fn matrix_repair_synchronizes_all_indexdb_replicas() {
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

    let (pending_vid, _pending_sid) = init_upload(&fragment, "idx-sync/pending.bin", 8192, 1).await;
    let pending_chunk = vec![0xAB; 8192];
    let put_pending = upload_part(&fragment, &pending_vid, 0, pending_chunk).await;
    assert_eq!(put_pending.status(), StatusCode::OK);

    let pm = ObjectPathManager::new();
    let session_db_paths = pm
        .get_all_session_partition_db_paths(&fragment, &pending_vid)
        .expect("session partition db paths");
    assert!(
        session_db_paths.len() >= 2,
        "expected at least two session partition db paths"
    );

    indexdb_store::clear_cached_dbs_for_test();
    fs::write(&session_db_paths[0], b"corrupt-session-partition-db")
        .await
        .expect("corrupt one session partition db replica");

    let before = collect_indexdb_replica_hashes(&fragment).await;
    assert_ne!(
        before[0].1, before[1].1,
        "precondition failed: partition db replicas should be divergent before repair"
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
    .expect("repair should reconcile indexdb replicas");
    indexdb_store::clear_cached_dbs_for_test();

    assert_all_indexdb_replicas_synchronized(&fragment).await;

    let got1 = read_object_bytes(full_claims(&fragment), fragment.clone(), k1).await;
    let got2 = read_object_bytes(full_claims(&fragment), fragment.clone(), k2).await;
    assert_eq!(got1, c1);
    assert_eq!(got2, c2);

    let pending_status_resp = get_upload_status(
        full_claims(&fragment),
        AxumPath((fragment.clone(), pending_vid.clone())),
    )
    .await
    .expect("pending status after indexdb repair")
    .into_response();
    assert_eq!(pending_status_resp.status(), StatusCode::OK);
    let pending_status_json = read_response_json(pending_status_resp).await;
    assert_eq!(pending_status_json["uploaded_parts"][0].as_u64(), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_repair_tolerates_missing_trashed_partition_db_replica() {
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
    let marker_paths = pm
        .get_all_object_partition_db_paths(&fragment, &key)
        .expect("object partition db paths");
    assert!(
        marker_paths.len() >= 2,
        "expected replicated trash partition db paths"
    );

    fs::remove_file(&marker_paths[0])
        .await
        .expect("remove one trash partition db replica");
    assert!(
        !fs::try_exists(&marker_paths[0]).await.unwrap_or(false),
        "precondition: one partition db replica should be missing"
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
    .expect("repair should tolerate missing trash partition db replica");

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

    // Keep serving CRUD-read workload while one indexdb replica root is down.
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

    assert_all_indexdb_replicas_synchronized(&fragment).await;

    let got_a = read_object_bytes(full_claims(&fragment), fragment.clone(), seed_key_a).await;
    let got_b = read_object_bytes(full_claims(&fragment), fragment.clone(), seed_key_b).await;
    assert_eq!(got_a, seed_bytes_a);
    assert_eq!(got_b, seed_bytes_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_repair_after_empty_index_disk_preserves_live_posix_listing() {
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
        .store(1, Ordering::Relaxed);
    STRIPE_SIZE_ATOMIC
        .get()
        .expect("STRIPE_SIZE_ATOMIC")
        .store(64 * 1024, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let videos = vec![
        (
            "repair-empty/live-a.mp4".to_string(),
            (0..(384 * 1024 + 17))
                .map(|i| (i % 251) as u8)
                .collect::<Vec<u8>>(),
        ),
        (
            "repair-empty/live-b.mp4".to_string(),
            (0..(512 * 1024 + 31))
                .map(|i| ((i * 7) % 251) as u8)
                .collect::<Vec<u8>>(),
        ),
    ];

    for (key, bytes) in &videos {
        put_object(state.clone(), &fragment, key, bytes.clone()).await;
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    assert!(
        index_roots.len() >= 2,
        "repair empty-index-disk regression requires two index roots"
    );

    for lost_root in index_roots {
        indexdb_store::clear_cached_dbs_for_test();
        let lost_fragment = lost_root.join(&fragment);
        if fs::try_exists(&lost_fragment).await.unwrap_or(false) {
            fs::remove_dir_all(&lost_fragment)
                .await
                .expect("empty one index disk fragment directory");
        }

        run_repair_job(
            RepairRunConfig {
                dry_run: false,
                target_fragment: Some(fragment.clone()),
                check_mode: RepairCheckMode::Scrub,
                meta_concurrency: 8,
                shard_repair_concurrency: 2,
                meta_batch_size: 512,
            },
            Arc::new(WsConnectionManager::new()),
        )
        .await
        .expect("repair after emptying one index disk");
        indexdb_store::clear_cached_dbs_for_test();
        assert_all_indexdb_replicas_synchronized(&fragment).await;

        let listed = list_objects_json(&fragment, Some("repair-empty"), Some("/")).await;
        let objects = listed["objects"].as_array().expect("objects array");
        let mut listed_keys = objects
            .iter()
            .filter_map(|object| object["object_key"].as_str().map(str::to_string))
            .collect::<Vec<_>>();
        listed_keys.sort();
        assert_eq!(
            listed_keys,
            vec![
                "repair-empty/live-a.mp4".to_string(),
                "repair-empty/live-b.mp4".to_string(),
            ],
            "repair must not lose live POSIX entries after a physical index disk loss"
        );

        for (key, expected) in &videos {
            let got =
                read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
            assert_eq!(got, *expected, "repaired live object mismatch for {}", key);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level disk root configuration"]
async fn matrix_two_live_disks_downgrade_to_one_and_reexpand_cleanly() {
    let _guard = TEST_LOCK.lock().await;
    let base = std::env::temp_dir().join(format!("n1-two-disk-cycle-{}", std::process::id()));
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
    ensure_upload_runtime_config();

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
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key_before = "ha-downgrade/before.bin".to_string();
    let key_before_b = "ha-downgrade/before-b.bin".to_string();
    let before_bytes = (0..(36 * 1024 + 17))
        .map(|i| ((i * 5) % 251) as u8)
        .collect::<Vec<u8>>();
    let before_bytes_b = (0..(19 * 1024 + 101))
        .map(|i| ((i * 7 + 3) % 241) as u8)
        .collect::<Vec<u8>>();
    put_object(state.clone(), &fragment, &key_before, before_bytes.clone()).await;
    put_object(
        state.clone(),
        &fragment,
        &key_before_b,
        before_bytes_b.clone(),
    )
    .await;

    for key in [&key_before, &key_before_b] {
        let meta = get_metadata(&fragment, key, &fragment)
            .await
            .expect("metadata before downgrade");
        assert_eq!(
            meta.parts[0].shards.iter().filter(|s| !s.is_parity).count(),
            1,
            "expected 1 data shard before downgrade for {}",
            key
        );
        assert_eq!(
            meta.parts[0].shards.iter().filter(|s| s.is_parity).count(),
            1,
            "expected 1 parity shard before downgrade for {}",
            key
        );
    }

    let index_2 = base.join("index_2");
    let index_2_parked = base.join("index_2.down");
    let blob_2 = base.join("blob_2");
    let blob_2_parked = base.join("blob_2.down");
    park_simulated_disk_with_blocker(
        &index_2,
        &index_2_parked,
        b"n1-index-disk-down-blocker",
        "park second index disk for downgrade",
    )
    .await;
    park_simulated_disk_with_blocker(
        &blob_2,
        &blob_2_parked,
        b"n1-blob-disk-down-blocker",
        "park second blob disk for downgrade",
    )
    .await;

    let got_before = read_object_bytes_eventually(fragment.clone(), key_before.clone(), 24).await;
    let got_before_b =
        read_object_bytes_eventually(fragment.clone(), key_before_b.clone(), 24).await;
    assert_eq!(got_before, before_bytes);
    assert_eq!(got_before_b, before_bytes_b);

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(0, Ordering::Relaxed);

    run_redistribution_job_with_options(
        false,
        RedistributionRunOptions {
            fast_mode: true,
            execution_mode: RedistributionExecutionMode::Strict,
            durability_mode: RedistributionDurabilityMode::Batched,
            ..Default::default()
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("downgrade redistribution to 1D+0P should succeed");

    // This isolated test parks the second root without changing the configured root set.
    // The exact 2-root -> 1-root -> 2-root policy flip is covered by the deploy-path harness,
    // while this in-process variant stays focused on degraded readability before the root returns.
    for (key, expected) in [
        (&key_before, &before_bytes),
        (&key_before_b, &before_bytes_b),
    ] {
        let got = read_object_bytes_eventually(fragment.clone(), key.to_string(), 24).await;
        assert_eq!(got, *expected);

        let meta = get_metadata(&fragment, key, &fragment)
            .await
            .expect("metadata after downgrade");
        assert_eq!(
            meta.parts[0].shards.iter().filter(|s| !s.is_parity).count(),
            1,
            "expected at least one live data shard during degraded window for {}",
            key
        );
    }

    restore_simulated_disk(&index_2, &index_2_parked, "restore second index disk").await;
    restore_simulated_disk(&blob_2, &blob_2_parked, "restore second blob disk").await;

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    run_redistribution_job_with_options(
        false,
        RedistributionRunOptions {
            fast_mode: true,
            execution_mode: RedistributionExecutionMode::Strict,
            durability_mode: RedistributionDurabilityMode::Batched,
            ..Default::default()
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("re-expand redistribution to 1D+1P should succeed");

    run_repair_job_with_options(
        RepairRunConfig {
            dry_run: false,
            target_fragment: None,
            check_mode: RepairCheckMode::Scrub,
            meta_concurrency: 8,
            shard_repair_concurrency: 2,
            meta_batch_size: 512,
        },
        RepairRunOptions {
            fast_mode: true,
            access_mode: MaintenanceAccessMode::Quiesced,
            ..Default::default()
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect("repair after re-expand should succeed");

    assert_all_indexdb_replicas_synchronized(&fragment).await;

    let key_after = "ha-downgrade/after.bin".to_string();
    let after_bytes = (0..(14 * 1024 + 29))
        .map(|i| ((i * 11 + 7) % 251) as u8)
        .collect::<Vec<u8>>();
    put_object(state.clone(), &fragment, &key_after, after_bytes.clone()).await;
    let got_after = read_object_bytes_eventually(fragment.clone(), key_after.clone(), 24).await;
    assert_eq!(got_after, after_bytes);

    for (key, expected) in [
        (&key_before, &before_bytes),
        (&key_before_b, &before_bytes_b),
        (&key_after, &after_bytes),
    ] {
        let got = read_object_bytes_eventually(fragment.clone(), key.to_string(), 24).await;
        assert_eq!(got, *expected);

        let meta = get_metadata(&fragment, key, &fragment)
            .await
            .expect("metadata after re-expand");
        assert_eq!(
            meta.parts[0].shards.iter().filter(|s| !s.is_parity).count(),
            1,
            "expected 1 data shard after re-expand for {}",
            key
        );
        assert_eq!(
            meta.parts[0].shards.iter().filter(|s| s.is_parity).count(),
            1,
            "expected 1 parity shard after re-expand for {}",
            key
        );
        assert!(
            meta.parts[0]
                .shards
                .iter()
                .any(|shard| { std::path::Path::new(&shard.absolute_path).starts_with(&blob_2) }),
            "re-expanded object {} should use restored blob_2: {:?}",
            key,
            meta.parts[0]
                .shards
                .iter()
                .map(|s| s.absolute_path.clone())
                .collect::<Vec<_>>()
        );
    }

    let _ = fs::remove_dir_all(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level disk root configuration"]
async fn matrix_redistribution_fails_for_orphan_blob_payload_without_index_metadata() {
    let _guard = TEST_LOCK.lock().await;
    let base =
        std::env::temp_dir().join(format!("n1-orphaned-redistribution-{}", std::process::id()));
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
    ensure_upload_runtime_config();

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
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let orphan_version = Ulid::new().to_string();
    let orphan_dir = base
        .join("blob_1")
        .join(&fragment)
        .join("blob")
        .join(&orphan_version);
    fs::create_dir_all(&orphan_dir)
        .await
        .expect("create orphan blob dir");
    fs::write(
        orphan_dir.join("part_0_shard_0.shard"),
        b"orphan shard payload",
    )
    .await
    .expect("write orphan shard");
    sleep(Duration::from_secs(6)).await;

    let err = run_redistribution_job_with_options(
        false,
        RedistributionRunOptions {
            fast_mode: true,
            execution_mode: RedistributionExecutionMode::Strict,
            durability_mode: RedistributionDurabilityMode::Batched,
            ..Default::default()
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await
    .expect_err("orphaned blob payload should fail redistribution");

    let message = err.to_string();
    assert!(
        message.contains("blob payload on disk but no indexed live/trash metadata"),
        "unexpected redistribution error: {}",
        message
    );

    let _ = fs::remove_dir_all(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level disk root configuration"]
async fn matrix_health_reports_degraded_when_blob_copy_is_missing() {
    let _guard = TEST_LOCK.lock().await;
    let base = std::env::temp_dir().join(format!("n1-health-degraded-{}", std::process::id()));
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
    ensure_upload_runtime_config();

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
        .store(4096, Ordering::Relaxed);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key = "health/degraded.bin".to_string();
    let bytes = (0..(24 * 1024 + 33))
        .map(|i| ((i * 13 + 5) % 251) as u8)
        .collect::<Vec<u8>>();
    put_object(state.clone(), &fragment, &key, bytes).await;

    let meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata before health check");
    let missing_shard = meta.parts[0]
        .shards
        .iter()
        .find(|shard| std::path::Path::new(&shard.absolute_path).starts_with(base.join("blob_2")))
        .cloned()
        .expect("expected shard on second blob root");
    fs::remove_file(&missing_shard.absolute_path)
        .await
        .expect("remove shard on second blob root");

    let mut params = HashMap::new();
    params.insert("refresh".to_string(), "true".to_string());
    let response = get_system_health(State(state), full_claims(&fragment), Query(params))
        .await
        .expect("health response")
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("health body");
    let health: serde_json::Value =
        serde_json::from_slice(&body).expect("deserialize health response");

    assert_eq!(
        health.get("status").and_then(|v| v.as_str()),
        Some("degraded")
    );
    assert!(
        health
            .get("summary")
            .and_then(|v| v.as_str())
            .map(|s| s.contains("need repair/redistribution"))
            .unwrap_or(false),
        "unexpected health summary: {}",
        String::from_utf8_lossy(&body)
    );

    let _ = fs::remove_dir_all(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level disk root configuration"]
async fn matrix_redistribution_fails_when_target_layout_needs_more_live_blob_disks_than_available()
{
    let _guard = TEST_LOCK.lock().await;
    let base = std::env::temp_dir().join(format!("n1-redist-live-roots-{}", std::process::id()));
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
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

    let owner_root = base.join("index_1");
    let fragment = fragment_owned_by_index_root(&owner_root, "fragment-redist-missing-blob");
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key = "ha-redist/missing-live-blob.bin".to_string();
    let content = (0..(22 * 1024 + 13))
        .map(|i| ((i * 17 + 5) % 251) as u8)
        .collect::<Vec<u8>>();
    put_object(state, &fragment, &key, content.clone()).await;

    let blob_2 = base.join("blob_2");
    let blob_2_parked = base.join("blob_2.down");
    park_simulated_disk_with_blocker(
        &blob_2,
        &blob_2_parked,
        b"n1-blob-disk-down-blocker",
        "park second blob disk for insufficient-live-root redistribution test",
    )
    .await;

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    let result = run_redistribution_job_with_options(
        false,
        RedistributionRunOptions {
            fast_mode: true,
            execution_mode: RedistributionExecutionMode::Strict,
            durability_mode: RedistributionDurabilityMode::Batched,
            ..Default::default()
        },
        Arc::new(WsConnectionManager::new()),
    )
    .await;

    let status = super::redistribution::get_redist_status().await;

    restore_simulated_disk(
        &blob_2,
        &blob_2_parked,
        "restore second blob disk after insufficient-live-root test",
    )
    .await;

    match result {
        Err(AppError::InternalServerError(message)) => {
            assert!(
                message.contains("Redistribution finished with 1 error(s)")
                    || message.contains("Insufficient storage"),
                "unexpected redistribution failure: {}",
                message
            );
        }
        other => panic!(
            "expected redistribution to fail when target layout needs more live blob roots, got {:?}",
            other
        ),
    }

    assert_eq!(status.phase, "Completed with Errors");
    assert!(
        status.total_errors >= 1,
        "redistribution status should report object errors: {:?}",
        status
    );
    assert!(
        status
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("Insufficient storage"),
        "redistribution status should retain the placement failure: {:?}",
        status
    );

    let got = read_object_bytes_eventually(fragment.clone(), key.clone(), 24).await;
    assert_eq!(got, content);

    let meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata after failed redistribution");
    assert_eq!(
        meta.parts[0].shards.iter().filter(|s| !s.is_parity).count(),
        1
    );
    assert_eq!(
        meta.parts[0].shards.iter().filter(|s| s.is_parity).count(),
        0
    );

    let _ = fs::remove_dir_all(&base).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_redistribution_read_falls_back_when_primary_indexdb_copy_is_corrupt() {
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

    let down_src = base.join("index_1");
    let down_dst = base.join("index_1.down");
    let owner_root = base.join("index_2");
    let fragment = fragment_owned_by_index_root(&owner_root, "fragment-ha-index-fallback");
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key = "ha-index/fallback-after-redist.bin".to_string();
    let content = (0..(40 * 1024 + 73))
        .map(|i| ((i * 13) % 251) as u8)
        .collect::<Vec<u8>>();
    let _version_id = put_object(state, &fragment, &key, content.clone()).await;

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
    index::sync_fragment_partition_replicas(&fragment)
        .await
        .expect("resync restored follower indexdb replica");

    let path_manager = ObjectPathManager::new();
    let partition_db_paths = path_manager
        .get_all_object_partition_db_paths(&fragment, &key)
        .expect("object partition db paths");
    assert!(
        partition_db_paths.len() >= 2,
        "expected replicated indexdb partition paths"
    );
    assert!(
        fs::try_exists(&partition_db_paths[1])
            .await
            .unwrap_or(false),
        "secondary partition db replica should exist after redistribution"
    );

    fs::write(&partition_db_paths[0], b"n1-corrupt-indexdb-copy")
        .await
        .expect("corrupt primary partition db copy");

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, content);

    let meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata should fall back to healthy indexdb replica");
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
async fn matrix_redistribution_defers_cutover_until_owner_index_restored() {
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

    let down_src = base.join("index_2");
    let down_dst = base.join("index_2.down");
    let fragment = fragment_owned_by_index_root(&down_src, "fragment-ha-redist-owner-down");
    setup_fragment(&fragment).await;
    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let key = "ha-redist-owner-down.bin".to_string();
    let content = (0..(48 * 1024 + 19))
        .map(|i| ((i * 7 + 11) % 251) as u8)
        .collect::<Vec<u8>>();
    let version_id = put_object(state, &fragment, &key, content.clone()).await;

    let before_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata before redistribution");
    assert_eq!(
        before_meta.parts[0]
            .shards
            .iter()
            .filter(|s| !s.is_parity)
            .count(),
        1
    );
    assert_eq!(
        before_meta.parts[0]
            .shards
            .iter()
            .filter(|s| s.is_parity)
            .count(),
        0
    );

    if fs::try_exists(&down_dst).await.unwrap_or(false) {
        let _ = fs::remove_dir_all(&down_dst).await;
    }
    fs::rename(&down_src, &down_dst)
        .await
        .expect("simulate owner index disk down");
    fs::write(&down_src, b"n1-index-disk-down-blocker")
        .await
        .expect("create index down blocker file");

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
        .store(262144, Ordering::Relaxed);

    run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
        .await
        .expect("redistribution should finish even when one owner index is down");

    let intent_files = collect_fragment_indexdb_files(&fragment).await;
    assert!(
        intent_files
            .iter()
            .any(|path| { path.contains(&version_id) && path.ends_with(".redist.intent.msgpack") }),
        "failed redistribution should leave a pending intent for replay"
    );

    let during_down = read_object_bytes_eventually(fragment.clone(), key.clone(), 24).await;
    assert_eq!(
        during_down, content,
        "object should remain readable while owner index is down"
    );

    let down_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata should still be readable via replica while owner is down");
    assert_eq!(
        down_meta.parts[0]
            .shards
            .iter()
            .filter(|s| !s.is_parity)
            .count(),
        1,
        "blob layout should not cut over before owner metadata commit is possible"
    );
    assert_eq!(
        down_meta.parts[0]
            .shards
            .iter()
            .filter(|s| s.is_parity)
            .count(),
        0,
        "blob layout should not cut over before owner metadata commit is possible"
    );

    fs::remove_file(&down_src)
        .await
        .expect("remove index down blocker file");
    restore_simulated_index_disk(&down_src, &down_dst, "restore owner index disk").await;

    replay_pending_intents_for_fragment_now(&fragment, true)
        .await
        .expect("replay pending redistribution intents after owner restore");

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
    .expect("repair after owner index restore");

    let remaining_intents = collect_fragment_indexdb_files(&fragment).await;
    assert!(
        !remaining_intents
            .iter()
            .any(|path| { path.contains(&version_id) && path.ends_with(".redist.intent.msgpack") }),
        "successful replay should clear the pending redistribution intent"
    );

    let after_repair = read_object_bytes_eventually(fragment.clone(), key.clone(), 24).await;
    assert_eq!(after_repair, content);

    let repaired_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata after repair");
    assert_eq!(
        repaired_meta.parts[0]
            .shards
            .iter()
            .filter(|s| !s.is_parity)
            .count(),
        2,
        "pending redistribution intent should replay after owner restore"
    );
    assert_eq!(
        repaired_meta.parts[0]
            .shards
            .iter()
            .filter(|s| s.is_parity)
            .count(),
        1,
        "pending redistribution intent should replay after owner restore"
    );

    assert_all_indexdb_replicas_synchronized(&fragment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_reads_prefer_owner_partition_db_over_divergent_replica() {
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
    let key = "owner-read/healthy-owner.bin".to_string();
    let content = (0..(24 * 1024 + 11))
        .map(|i| ((i * 9) % 251) as u8)
        .collect::<Vec<u8>>();
    put_object(state, &fragment, &key, content.clone()).await;

    let partition_db_paths = object_partition_db_paths_owner_first(&fragment, &key);
    assert!(
        partition_db_paths.len() >= 2,
        "expected owner + replica partition db paths"
    );

    fs::write(&partition_db_paths[1], b"n1-divergent-replica-copy")
        .await
        .expect("corrupt follower partition db");

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, content);

    let meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata should come from healthy owner partition db");
    assert_eq!(meta.object_key, key);
    assert_eq!(meta.total_size_bytes as usize, content.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_owner_partition_db_missing_meta_self_heals_on_read() {
    const TEST_PARTITION_META_TABLE: TableDefinition<&str, &[u8]> =
        TableDefinition::new("partition_meta");
    const TEST_PARTITION_META_KEY: &str = "__partition__";

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
    let key = "owner-read/missing-meta-self-heal.bin".to_string();
    let content = (0..(16 * 1024 + 19))
        .map(|i| ((i * 7) % 251) as u8)
        .collect::<Vec<u8>>();
    put_object(state, &fragment, &key, content.clone()).await;

    let partition_db_paths = object_partition_db_paths_owner_first(&fragment, &key);
    assert!(
        !partition_db_paths.is_empty(),
        "expected owner partition db path"
    );
    let owner_path = partition_db_paths[0].clone();

    indexdb_store::evict_cached_db_for_test(&owner_path);
    let owner_db = Database::create(&owner_path).expect("open owner partition db");
    let write_txn = owner_db
        .begin_write()
        .expect("begin write to remove owner partition meta");
    {
        let mut table = write_txn
            .open_table(TEST_PARTITION_META_TABLE)
            .expect("open owner partition meta table");
        let _ = table
            .remove(TEST_PARTITION_META_KEY)
            .expect("remove owner partition meta row");
    }
    write_txn
        .commit()
        .expect("commit owner partition meta removal");
    drop(owner_db);

    if let Some(replica_path) = partition_db_paths.get(1) {
        indexdb_store::evict_cached_db_for_test(replica_path);
        fs::remove_file(replica_path)
            .await
            .expect("remove replica partition db to force owner read");
    }

    let latest = index::read_latest_object(&fragment, &key)
        .await
        .expect("owner read should self-heal missing partition metadata");
    assert_eq!(latest.object_key, key);

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, content);

    indexdb_store::evict_cached_db_for_test(&owner_path);
    let healed_db = Database::create(&owner_path).expect("reopen owner partition db");
    let read_txn = healed_db
        .begin_read()
        .expect("begin read after partition meta self-heal");
    let table = read_txn
        .open_table(TEST_PARTITION_META_TABLE)
        .expect("open healed partition meta table");
    assert!(
        table
            .get(TEST_PARTITION_META_KEY)
            .expect("read healed partition meta row")
            .is_some(),
        "owner partition db should be backfilled with partition metadata"
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
#[ignore = "concurrent permanent-delete maintenance race; run as an isolated HA stress case"]
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
        .uri("/n1/objects/get")
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

    assert_all_indexdb_replicas_synchronized(&fragment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "concurrent permanent-delete maintenance race; run as an isolated HA stress case"]
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
        .uri("/n1/objects/get")
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

    assert_all_indexdb_replicas_synchronized(&fragment).await;
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

                let got = read_object_bytes_eventually_labeled(
                    fragment.clone(),
                    key_a.clone(),
                    20,
                    "ha_conc_initial",
                )
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
                .expect("rename in concurrent maintenance")
                .into_response();
                assert_eq!(rename_resp.status(), StatusCode::OK);

                let got = read_object_bytes_eventually_labeled(
                    fragment.clone(),
                    key_b.clone(),
                    20,
                    "ha_conc_renamed",
                )
                .await;
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

                let got = read_object_bytes_eventually_labeled(
                    fragment,
                    key_b.clone(),
                    24,
                    "ha_conc_recovered",
                )
                .await;
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
        let got = read_object_bytes_eventually_labeled(
            fragment.clone(),
            key.clone(),
            24,
            "ha_conc_repair_read",
        )
        .await;
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
                let got = read_object_bytes_eventually_labeled(
                    fragment.clone(),
                    key_a.clone(),
                    24,
                    "ha_fi_initial",
                )
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

                let got = read_object_bytes_eventually_labeled(
                    fragment,
                    key_b.clone(),
                    24,
                    "ha_fi_recovered",
                )
                .await;
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
        let got = read_object_bytes_eventually_labeled(
            fragment.clone(),
            key.clone(),
            24,
            "ha_fi_repair_read",
        )
        .await;
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

            match redist_res {
                Err(AppError::InternalServerError(message)) => {
                    assert!(
                        message.contains("Insufficient storage"),
                        "unexpected redistribution failure in soak round: {}",
                        message
                    );
                }
                other => panic!(
                    "expected redistribution to fail when one live blob disk is missing for the target soak layout, got {:?}",
                    other
                ),
            }
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
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
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
            .store(1, Ordering::Relaxed);
        PARITY_SHARDS_ATOMIC
            .get()
            .expect("PARITY_SHARDS_ATOMIC")
            .store(1, Ordering::Relaxed);
        run_redistribution_job_with_options(
            false,
            RedistributionRunOptions {
                fast_mode: redist_fast_mode,
                execution_mode: RedistributionExecutionMode::Availability,
                scope: crate::core::redistribution::RedistributionScope::All,
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
                        .uri("/n1/objects/get")
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
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
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
    let writer_buffer_bytes = test_env_usize(
        "N1_UPLOAD_PROBE_WRITER_BUFFER_BYTES",
        512 * 1024,
        64 * 1024,
        8 * 1024 * 1024,
    );
    let min_mib_per_sec = test_env_u64("N1_UPLOAD_PROBE_MIN_MIBPS", 0, 0, 100_000) as f64;
    let max_ram_mib = test_env_u64("N1_UPLOAD_PROBE_MAX_RAM_MIB", 0, 0, 1_000_000);
    let data_shards = test_env_usize("N1_UPLOAD_PROBE_DATA_SHARDS", 1, 1, 16);
    let parity_shards = test_env_usize("N1_UPLOAD_PROBE_PARITY_SHARDS", 1, 0, 16);
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
        .store(data_shards, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(parity_shards, Ordering::Relaxed);
    WRITER_BUFFER_SIZE_ATOMIC
        .get()
        .expect("WRITER_BUFFER_SIZE_ATOMIC")
        .store(writer_buffer_bytes, Ordering::Relaxed);

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
        "N1 upload probe: workers={} uploads_per_worker={} object_bytes={} part_bytes={} writer_buffer_bytes={} ec={}D+{}P durability={} io_gate={} est_working_set_mib={:.2} total_uploads={} total_mib={:.2} elapsed_s={:.3} throughput_mib_s={:.2}",
        workers,
        uploads_per_worker,
        object_bytes,
        part_bytes,
        writer_buffer_bytes,
        data_shards,
        parity_shards,
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
async fn matrix_indexdb_ndxdb_recover_all_and_empty_trash_fast_paths() {
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
        "trash-fast/a.bin".to_string(),
        "trash-fast/b.bin".to_string(),
        "trash-fast/sub/c.bin".to_string(),
        "trash-fast/sub/d.bin".to_string(),
    ];

    for (idx, key) in keys.iter().enumerate() {
        put_object(state.clone(), &fragment, key, vec![idx as u8 + 1; 4096]).await;
    }

    let indexdb_files = collect_fragment_indexdb_files(&fragment).await;
    assert!(
        indexdb_files.iter().any(|path| path.ends_with(".ndxdb")),
        "expected at least one .ndxdb file, found {:?}",
        indexdb_files
    );
    assert!(
        indexdb_files.iter().all(|path| !path.ends_with(".db")),
        "index partition files should only use .ndxdb: {:?}",
        indexdb_files
    );

    let bulk_delete_resp = delete_objects_bulk(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(BulkObjectKeysRequest {
            object_keys: keys.clone(),
        }),
    )
    .await
    .expect("bulk delete for recover-all")
    .into_response();
    assert_eq!(bulk_delete_resp.status(), StatusCode::OK);

    let recover_all_resp = recover_all_trash(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath(fragment.clone()),
    )
    .await
    .expect("recover all trash")
    .into_response();
    assert_eq!(recover_all_resp.status(), StatusCode::ACCEPTED);
    wait_for_trash_job_completion("recover").await;

    let live_after_recover = list_objects_json_page(&fragment, None, None, 100, None).await;
    assert_eq!(
        live_after_recover["objects"]
            .as_array()
            .expect("live after recover")
            .len(),
        keys.len()
    );
    let trash_after_recover = list_trash_json_page(&fragment, None, None, 100, None).await;
    assert!(
        trash_after_recover["objects"]
            .as_array()
            .expect("trash after recover")
            .is_empty()
    );

    let bulk_delete_again_resp = delete_objects_bulk(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(BulkObjectKeysRequest {
            object_keys: keys.clone(),
        }),
    )
    .await
    .expect("bulk delete for empty trash")
    .into_response();
    assert_eq!(bulk_delete_again_resp.status(), StatusCode::OK);

    let empty_resp = empty_trash(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath(fragment.clone()),
    )
    .await
    .expect("empty trash")
    .into_response();
    assert_eq!(empty_resp.status(), StatusCode::ACCEPTED);
    wait_for_trash_job_completion("empty").await;

    let live_after_empty = list_objects_json_page(&fragment, None, None, 100, None).await;
    assert!(
        live_after_empty["objects"]
            .as_array()
            .expect("live after empty")
            .is_empty()
    );
    let trash_after_empty = list_trash_json_page(&fragment, None, None, 100, None).await;
    assert!(
        trash_after_empty["objects"]
            .as_array()
            .expect("trash after empty")
            .is_empty()
    );

    for key in keys {
        let req = Request::builder()
            .uri("/n1/objects/get")
            .body(Body::empty())
            .expect("build get request");
        assert!(
            get_object(
                full_claims(&fragment),
                AxumPath((fragment.clone(), key.clone())),
                req,
            )
            .await
            .is_err(),
            "emptied trash key should not be readable: {}",
            key
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_version_delete_is_direct_and_skips_trash_plane() {
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

    let key = "trash-hidden/series.dcm".to_string();
    let version_a = put_object(state.clone(), &fragment, &key, vec![0x11; 2048]).await;
    let version_b = put_object(state.clone(), &fragment, &key, vec![0x22; 4096]).await;

    let resp = delete_object_version(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), version_a.clone(), key.clone())),
    )
    .await
    .expect("delete older version")
    .into_response();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    assert_eq!(
        index::list_fragment_object_keys_with_trash_versions(&fragment)
            .await
            .expect("trash keys before recover"),
        Vec::<String>::new()
    );
    assert!(
        index::list_trashed_object_versions_metadata(&fragment, &key)
            .await
            .expect("trashed versions before recover")
            .is_empty()
    );
    assert_eq!(
        index::list_live_object_versions_metadata(&fragment, &key)
            .await
            .expect("live versions before recover")
            .len(),
        1
    );

    assert!(
        delete_object_version(
            State(state.clone()),
            full_claims(&fragment),
            AxumPath((fragment.clone(), version_a.clone(), key.clone())),
        )
        .await
        .is_err(),
        "second direct version delete should not find the older version"
    );

    let empty_resp = empty_trash(
        State(state),
        full_claims(&fragment),
        AxumPath(fragment.clone()),
    )
    .await
    .expect("empty direct-version-delete fragment trash")
    .into_response();
    assert_eq!(empty_resp.status(), StatusCode::ACCEPTED);
    wait_for_trash_job_completion("empty").await;

    assert!(
        index::list_trashed_object_versions_metadata(&fragment, &key)
            .await
            .expect("trashed versions after empty")
            .is_empty()
    );
    let live_after_empty = index::list_live_object_versions_metadata(&fragment, &key)
        .await
        .expect("live versions after empty");
    assert_eq!(live_after_empty.len(), 1);
    assert_eq!(live_after_empty[0].version_id, version_b);
    assert!(
        index::list_fragment_object_keys_with_trash_versions(&fragment)
            .await
            .expect("trash keys after empty")
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_rename_allows_destination_with_only_trashed_history() {
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

    let source_key = "rename/live-source.bin".to_string();
    let destination_key = "rename/trashed-destination.bin".to_string();
    let source_bytes = vec![0xAB; 2048];
    let destination_bytes = vec![0xCD; 1024];

    let source_version =
        put_object(state.clone(), &fragment, &source_key, source_bytes.clone()).await;
    let trashed_destination_version = put_object(
        state.clone(),
        &fragment,
        &destination_key,
        destination_bytes,
    )
    .await;

    let delete_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), destination_key.clone())),
    )
    .await
    .expect("soft delete destination")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    assert!(
        index::list_live_object_versions_metadata(&fragment, &destination_key)
            .await
            .expect("live destination versions after soft delete")
            .is_empty(),
        "destination should have no live versions after soft delete"
    );
    assert_eq!(
        index::list_trashed_object_versions_metadata(&fragment, &destination_key)
            .await
            .expect("trashed destination versions after soft delete")
            .len(),
        1,
        "destination should retain one trashed version"
    );

    let rename_resp = rename_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(RenameObjectRequest {
            source_fragment: fragment.clone(),
            source_key: source_key.clone(),
            destination_fragment: fragment.clone(),
            destination_key: destination_key.clone(),
        }),
    )
    .await
    .expect("rename onto trashed destination")
    .into_response();
    assert_eq!(rename_resp.status(), StatusCode::OK);

    assert!(
        get_metadata(&fragment, &source_key, &fragment)
            .await
            .is_err(),
        "source key should disappear after rename"
    );

    let live_destination_versions =
        index::list_live_object_versions_metadata(&fragment, &destination_key)
            .await
            .expect("live destination versions after rename");
    assert_eq!(live_destination_versions.len(), 1);
    assert_eq!(live_destination_versions[0].version_id, source_version);
    let destination_meta = get_metadata(&fragment, &destination_key, &fragment).await;
    assert!(
        destination_meta.is_ok(),
        "destination should have readable live metadata after rename; live={:?}; trashed={:?}",
        live_destination_versions
            .iter()
            .map(|meta| (
                meta.object_key.clone(),
                meta.version_id.clone(),
                meta.is_delete_marker
            ))
            .collect::<Vec<_>>(),
        index::list_trashed_object_versions_metadata(&fragment, &destination_key)
            .await
            .expect("trashed destination versions for debug")
            .iter()
            .map(|meta| (
                meta.object_key.clone(),
                meta.version_id.clone(),
                meta.is_delete_marker
            ))
            .collect::<Vec<_>>()
    );
    let moved = read_object_bytes(
        full_claims(&fragment),
        fragment.clone(),
        destination_key.clone(),
    )
    .await;
    assert_eq!(moved, source_bytes);

    let trashed_destination_versions =
        index::list_trashed_object_versions_metadata(&fragment, &destination_key)
            .await
            .expect("trashed destination versions after rename");
    assert_eq!(trashed_destination_versions.len(), 1);
    assert_eq!(
        trashed_destination_versions[0].version_id,
        trashed_destination_version
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_recover_all_trash_resumes_from_persistent_journal() {
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

    let key = "trash-resume/recover.bin".to_string();
    let bytes = vec![0x4a; 8192];
    put_object(state.clone(), &fragment, &key, bytes.clone()).await;

    let delete_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("soft delete for recover resume")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);
    assert!(
        !index::list_fragment_object_keys_with_trash_versions(&fragment)
            .await
            .expect("trash keys before resume")
            .is_empty()
    );

    seed_persistent_trash_job(&fragment, "recover_all_trash").await;
    let resumed = resume_persistent_trash_jobs(state.clone())
        .await
        .expect("resume persistent recover job");
    assert_eq!(resumed, 1);

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, bytes);
    assert!(
        index::list_fragment_object_keys_with_trash_versions(&fragment)
            .await
            .expect("trash keys after resume")
            .is_empty()
    );
    for path in persistent_trash_job_paths(&fragment) {
        assert!(
            !fs::try_exists(&path)
                .await
                .expect("journal path exists check"),
            "persistent trash job should be cleared: {}",
            path.display()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_empty_trash_resumes_from_persistent_journal() {
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

    let key = "trash-resume/empty.bin".to_string();
    put_object(state.clone(), &fragment, &key, vec![0x9b; 4096]).await;

    let delete_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("soft delete for empty resume")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    seed_persistent_trash_job(&fragment, "empty_trash").await;
    let resumed = resume_persistent_trash_jobs(state.clone())
        .await
        .expect("resume persistent empty job");
    assert_eq!(resumed, 1);

    let req = Request::builder()
        .uri("/n1/objects/get")
        .body(Body::empty())
        .expect("build get request");
    assert!(
        get_object(
            full_claims(&fragment),
            AxumPath((fragment.clone(), key.clone())),
            req,
        )
        .await
        .is_err(),
        "emptied key should not be readable after journal resume"
    );
    assert!(
        index::list_fragment_object_keys_with_trash_versions(&fragment)
            .await
            .expect("trash keys after empty resume")
            .is_empty()
    );
    for path in persistent_trash_job_paths(&fragment) {
        assert!(
            !fs::try_exists(&path)
                .await
                .expect("journal path exists check"),
            "persistent trash job should be cleared: {}",
            path.display()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_recover_object_remains_correct_if_blob_move_happened_before_metadata_restore() {
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

    let key = "trash-recover/interrupted.bin".to_string();
    let bytes = vec![0x5c; 6144];
    let version_id = put_object(state.clone(), &fragment, &key, bytes.clone()).await;

    let delete_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("soft delete for interrupted recover")
    .into_response();
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let path_manager = ObjectPathManager::new();
    for root in path_manager.get_blob_roots() {
        let trash_blob_dir = root
            .join(&fragment)
            .join(".trash")
            .join("blob")
            .join(&version_id);
        let live_blob_dir = path_manager.get_shard_dir_on_disk(&root, &fragment, &version_id);
        if fs::try_exists(&trash_blob_dir)
            .await
            .expect("trash blob exists check")
        {
            if let Some(parent) = live_blob_dir.parent() {
                fs::create_dir_all(parent)
                    .await
                    .expect("create live blob parent");
            }
            fs::rename(&trash_blob_dir, &live_blob_dir)
                .await
                .expect("simulate interrupted recover blob move");
        }
    }

    let recover_resp = recover_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("recover after simulated interrupted move")
    .into_response();
    assert_eq!(recover_resp.status(), StatusCode::OK);

    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, bytes);
    assert!(
        index::list_fragment_object_keys_with_trash_versions(&fragment)
            .await
            .expect("trash keys after interrupted recover")
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn matrix_indexdb_trash_jobs_10k_simulation_stats() {
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

    let object_count = std::env::var("N1_TRASH_BENCH_OBJECTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10_000);
    let object_bytes = std::env::var("N1_TRASH_BENCH_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let seed_rss_kib = current_process_rss_kib();
    let put_started = Instant::now();
    let mut keys = Vec::with_capacity(object_count);
    for idx in 0..object_count {
        let key = format!("trash-bench/{:05}.bin", idx);
        let mut payload = vec![0u8; object_bytes];
        if !payload.is_empty() {
            payload[0] = (idx % 251) as u8;
        }
        put_object(state.clone(), &fragment, &key, payload).await;
        keys.push(key);
    }
    let put_elapsed = put_started.elapsed();
    let after_put_rss_kib = current_process_rss_kib();

    let delete_started = Instant::now();
    let bulk_delete_resp = delete_objects_bulk(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(BulkObjectKeysRequest {
            object_keys: keys.clone(),
        }),
    )
    .await
    .expect("bulk delete for recover-all bench")
    .into_response();
    assert_eq!(bulk_delete_resp.status(), StatusCode::OK);
    let delete_elapsed = delete_started.elapsed();

    let recover_request_started = Instant::now();
    let recover_resp = recover_all_trash(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath(fragment.clone()),
    )
    .await
    .expect("recover all bench")
    .into_response();
    assert_eq!(recover_resp.status(), StatusCode::ACCEPTED);
    let recover_request_elapsed = recover_request_started.elapsed();
    let recover_job_started = Instant::now();
    let (_recover_duration_seconds, recover_peak_rss_kib) =
        wait_for_trash_job_completion_with_peak_rss("recover").await;
    let recover_job_elapsed = recover_job_started.elapsed();

    let bulk_delete_again_resp = delete_objects_bulk(
        State(state.clone()),
        full_claims(&fragment),
        AxumJson(BulkObjectKeysRequest {
            object_keys: keys.clone(),
        }),
    )
    .await
    .expect("bulk delete for empty bench")
    .into_response();
    assert_eq!(bulk_delete_again_resp.status(), StatusCode::OK);

    let empty_request_started = Instant::now();
    let empty_resp = empty_trash(
        State(state),
        full_claims(&fragment),
        AxumPath(fragment.clone()),
    )
    .await
    .expect("empty bench")
    .into_response();
    assert_eq!(empty_resp.status(), StatusCode::ACCEPTED);
    let empty_request_elapsed = empty_request_started.elapsed();
    let empty_job_started = Instant::now();
    let (_empty_duration_seconds, empty_peak_rss_kib) =
        wait_for_trash_job_completion_with_peak_rss("empty").await;
    let empty_job_elapsed = empty_job_started.elapsed();

    let final_rss_kib = current_process_rss_kib();

    println!(
        "N1 trash bench: objects={} bytes={} put_ms={} delete_ms={} recover_request_ms={} recover_job_ms={} empty_request_ms={} empty_job_ms={} seed_rss_kib={} after_put_rss_kib={} recover_peak_rss_kib={} empty_peak_rss_kib={} final_rss_kib={}",
        object_count,
        object_bytes,
        put_elapsed.as_millis(),
        delete_elapsed.as_millis(),
        recover_request_elapsed.as_millis(),
        recover_job_elapsed.as_millis(),
        empty_request_elapsed.as_millis(),
        empty_job_elapsed.as_millis(),
        seed_rss_kib,
        after_put_rss_kib,
        recover_peak_rss_kib,
        empty_peak_rss_kib,
        final_rss_kib
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated process-level 2 index + 2 blob disk root configuration"]
async fn matrix_availability_crud_during_repair_and_redistribution() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
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
    for i in 0..12u8 {
        let key = format!("avail/base-{i}.bin");
        let content = (0..(12 * 1024 + i as usize * 17))
            .map(|n| ((n + i as usize) % 251) as u8)
            .collect::<Vec<u8>>();
        put_object(state.clone(), &fragment, &key, content.clone()).await;
        expected.insert(key, content);
    }

    DATA_SHARDS_ATOMIC
        .get()
        .expect("DATA_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);
    PARITY_SHARDS_ATOMIC
        .get()
        .expect("PARITY_SHARDS_ATOMIC")
        .store(1, Ordering::Relaxed);

    let redist_task = tokio::spawn(run_redistribution_job_with_options(
        false,
        RedistributionRunOptions {
            fast_mode: false,
            execution_mode: RedistributionExecutionMode::Availability,
            scope: crate::core::redistribution::RedistributionScope::All,
            target_fragment: Some(fragment.clone()),
            durability_mode: RedistributionDurabilityMode::Auto,
            object_concurrency: 2,
            status_batch: 8,
        },
        Arc::new(WsConnectionManager::new()),
    ));

    let hot_key = "avail/base-0.bin".to_string();
    let hot_bytes = expected
        .get(&hot_key)
        .expect("hot key should exist")
        .clone();
    let got = read_object_bytes_eventually(fragment.clone(), hot_key.clone(), 24).await;
    assert_eq!(got, hot_bytes);

    let created_key = "avail/new-during-redist.bin".to_string();
    let created_bytes = vec![0x44; 18 * 1024];
    put_object(
        state.clone(),
        &fragment,
        &created_key,
        created_bytes.clone(),
    )
    .await;
    expected.insert(created_key.clone(), created_bytes);

    let recycle_key = "avail/base-1.bin".to_string();
    let del_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), recycle_key.clone())),
    )
    .await
    .expect("delete during availability redistribution")
    .into_response();
    assert_eq!(del_resp.status(), StatusCode::OK);
    let rec_resp = recover_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), recycle_key.clone())),
    )
    .await
    .expect("recover during availability redistribution")
    .into_response();
    assert_eq!(rec_resp.status(), StatusCode::OK);

    redist_task
        .await
        .expect("availability redistribution join")
        .expect("availability redistribution should succeed");

    let repair_task = tokio::spawn(run_repair_job(
        RepairRunConfig {
            dry_run: false,
            target_fragment: Some(fragment.clone()),
            check_mode: RepairCheckMode::Availability,
            meta_concurrency: 2,
            shard_repair_concurrency: 1,
            meta_batch_size: 32,
        },
        Arc::new(WsConnectionManager::new()),
    ));

    let repair_read = read_object_bytes_eventually(fragment.clone(), hot_key.clone(), 24).await;
    assert_eq!(repair_read, hot_bytes);

    let repair_key = "avail/new-during-repair.bin".to_string();
    let repair_bytes = vec![0x55; 10 * 1024 + 3];
    put_object(state.clone(), &fragment, &repair_key, repair_bytes.clone()).await;
    expected.insert(repair_key.clone(), repair_bytes);

    let del_resp = delete_object(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), recycle_key.clone())),
    )
    .await
    .expect("delete during availability repair")
    .into_response();
    assert_eq!(del_resp.status(), StatusCode::OK);
    let rec_resp = recover_object(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), recycle_key.clone())),
    )
    .await
    .expect("recover during availability repair")
    .into_response();
    assert_eq!(rec_resp.status(), StatusCode::OK);

    repair_task
        .await
        .expect("availability repair join")
        .expect("availability repair should succeed");

    for (key, bytes) in &expected {
        let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
        assert_eq!(got, *bytes, "availability maintenance mismatch for {}", key);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "indexdb HA soak; run manually with N1_INDEX_SOAK_* env knobs"]
async fn matrix_indexdb_ha_soak_down_recover() {
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

        assert_all_indexdb_replicas_synchronized(&fragment).await;

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
async fn matrix_live_listing_supports_prefix_and_delimiter_pagination() {
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
async fn matrix_live_listing_rebuilds_partition_namespace_index_when_missing() {
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
        ("rebuild", 1u8),
        ("rebuild/a.bin", 2u8),
        ("rebuild/b.bin", 3u8),
        ("rebuild/sub-a/x.bin", 4u8),
        ("rebuild/sub-b/y.bin", 5u8),
    ] {
        put_object(state.clone(), &fragment, key, vec![fill; 2048]).await;
    }

    indexdb_store::clear_live_namespace_indexes_for_test(&fragment)
        .expect("clear live namespace indexes");
    assert_eq!(
        indexdb_store::count_live_namespace_rows_for_test(&fragment)
            .expect("count cleared namespace rows"),
        0
    );

    let page1 = list_objects_json_page(&fragment, Some("rebuild"), Some("/"), 2, None).await;
    let token1 = page1["continuation_token"]
        .as_str()
        .expect("page1 continuation token")
        .to_string();
    let page2 =
        list_objects_json_page(&fragment, Some("rebuild"), Some("/"), 2, Some(token1)).await;
    let token2 = page2["continuation_token"]
        .as_str()
        .expect("page2 continuation token")
        .to_string();
    let page3 =
        list_objects_json_page(&fragment, Some("rebuild"), Some("/"), 2, Some(token2)).await;

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
            "object:rebuild".to_string(),
            "object:rebuild/a.bin".to_string(),
            "object:rebuild/b.bin".to_string(),
            "prefix:rebuild/sub-a/".to_string(),
            "prefix:rebuild/sub-b/".to_string(),
        ]
    );
    assert!(
        indexdb_store::count_live_namespace_rows_for_test(&fragment)
            .expect("count rebuilt namespace rows")
            > 0,
        "listing should lazily rebuild partition-local live namespace rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_live_listing_with_delimiter_survives_cache_eviction_after_many_renames() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 512 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let total = 240usize;
    for idx in 0..total {
        let source_key =
            format!("idea-2025.3.2/idea-IU-253.30387.90/dir-{idx:04}/file-{idx:04}.bin");
        put_object(
            state.clone(),
            &fragment,
            &source_key,
            vec![(idx % 251) as u8; 32],
        )
        .await;

        let response = rename_object(
            State(state.clone()),
            full_claims(&fragment),
            AxumJson(RenameObjectRequest {
                source_fragment: fragment.clone(),
                source_key: source_key.clone(),
                destination_fragment: fragment.clone(),
                destination_key: format!("sample/{source_key}"),
            }),
        )
        .await
        .expect("rename object")
        .into_response();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "rename failed for {source_key}"
        );
    }

    indexdb_store::clear_cached_dbs_for_test();

    let page = list_objects_json_page(&fragment, Some("sample"), Some("/"), 1000, None).await;
    let prefixes = page["common_prefixes"]
        .as_array()
        .expect("common prefixes array")
        .iter()
        .filter_map(|value| value.as_str())
        .map(str::to_string)
        .collect::<Vec<_>>();

    assert!(
        prefixes
            .iter()
            .any(|prefix| prefix == "sample/idea-2025.3.2/"),
        "expected renamed tree prefix in delimiter listing, got {:?}",
        prefixes
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress"]
async fn matrix_massive_same_fragment_folder_rename_10k_deep_prefixes_has_no_conflicts() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    init_quota_file(&fragment, 2 * 1024 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(&fragment).await;
    init_fragment_metadata(&fragment, "key")
        .await
        .expect("init metadata");

    let state = Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    });

    let total = 10_000usize;
    let mut operations = Vec::with_capacity(total);
    for idx in 0..total {
        let source_key = deep_prefix_key_for_index(idx);
        seed_live_object_metadata(&fragment, &source_key, 64).await;
        operations.push((source_key.clone(), format!("sample/{source_key}")));
    }

    let errors = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    stream::iter(operations.clone())
        .for_each_concurrent(12, {
            let state = state.clone();
            let fragment = fragment.clone();
            let errors = errors.clone();
            move |(source_key, destination_key)| {
                let state = state.clone();
                let fragment = fragment.clone();
                let errors = errors.clone();
                async move {
                    let response = rename_object(
                        State(state),
                        full_claims(&fragment),
                        AxumJson(RenameObjectRequest {
                            source_fragment: fragment.clone(),
                            source_key: source_key.clone(),
                            destination_fragment: fragment.clone(),
                            destination_key: destination_key.clone(),
                        }),
                    )
                    .await;
                    match response {
                        Ok(ok) => {
                            let response = ok.into_response();
                            if response.status() != StatusCode::OK {
                                let mut guard = errors.lock().await;
                                guard.push(format!(
                                    "{} -> {} returned {}",
                                    source_key,
                                    destination_key,
                                    response.status()
                                ));
                            }
                        }
                        Err(err) => {
                            let mut guard = errors.lock().await;
                            guard.push(format!("{source_key} -> {destination_key}: {err}"));
                        }
                    }
                }
            }
        })
        .await;

    let errors = errors.lock().await.clone();
    assert!(
        errors.is_empty(),
        "expected no rename conflicts/errors during 10k folder-style rename, got {} errors; sample={:?}",
        errors.len(),
        errors.iter().take(10).collect::<Vec<_>>()
    );

    let mut continuation = None;
    let mut counted = 0usize;
    loop {
        let page =
            list_objects_json_page(&fragment, Some("sample"), None, 1000, continuation).await;
        counted = counted.saturating_add(page["objects"].as_array().expect("objects array").len());
        continuation = page["continuation_token"].as_str().map(str::to_string);
        if continuation.is_none() {
            break;
        }
    }
    assert_eq!(counted, total, "flat listing count after rename mismatch");

    let delimiter_page =
        list_objects_json_page(&fragment, Some("sample"), Some("/"), 1000, None).await;
    assert!(
        delimiter_page["common_prefixes"]
            .as_array()
            .map(|items| !items.is_empty())
            .unwrap_or(false),
        "expected delimiter listing to produce common prefixes after 10k folder-style rename"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress"]
async fn matrix_namespace_indexdb_single_prefix_rename_and_lifecycle_10k_benchmark() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config_with_root_counts(&base, 2, 2).await;
    ensure_upload_runtime_config();

    let fragment = {
        let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
        hex::encode(digest)
    };
    setup_fragment(&fragment).await;

    let runtime = global_namespace_runtime();
    let object_count = std::env::var("N1_NAMESPACE_BENCH_OBJECTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10_000);
    let mut seed_keys = (0..object_count)
        .map(|idx| format!("images/{}", deep_prefix_key_for_index(idx)))
        .collect::<Vec<_>>();
    let single_source = "images/__bench_single__.bin".to_string();
    seed_keys.push(single_source.clone());

    let seed_rss_kib = current_process_rss_kib();
    let seed_started = Instant::now();
    seed_namespace_bench_batch(&fragment, &seed_keys, 64);
    let seed_elapsed = seed_started.elapsed();
    let after_seed_rss_kib = current_process_rss_kib();

    let import_started = Instant::now();
    let mut import_params = HashMap::new();
    import_params.insert("limit".to_string(), "1".to_string());
    let import_page = runtime
        .list_objects_page(&fragment, &import_params, false)
        .await
        .expect("prime namespace runtime import");
    let import_elapsed = import_started.elapsed();
    assert!(
        !import_page.objects.is_empty() || !import_page.common_prefixes.is_empty(),
        "expected imported namespace to expose seeded content"
    );

    let rename_object_started = Instant::now();
    let rename_object_summary = runtime
        .rename(
            &fragment,
            &single_source,
            "images/__bench_single_renamed__.bin",
        )
        .await
        .expect("rename single logical object");
    let rename_object_elapsed = rename_object_started.elapsed();
    assert_eq!(rename_object_summary.affected_paths, 1);
    assert_eq!(rename_object_summary.affected_objects, 1);

    let rename_prefix_started = Instant::now();
    let rename_prefix_summary = runtime
        .rename(&fragment, "images", "media")
        .await
        .expect("rename logical prefix");
    let rename_prefix_elapsed = rename_prefix_started.elapsed();

    let list_after_rename_started = Instant::now();
    let mut counted = 0usize;
    let mut continuation = None;
    loop {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "1000".to_string());
        params.insert("prefix".to_string(), "media".to_string());
        if let Some(token) = continuation.clone() {
            params.insert("continuation_token".to_string(), token);
        }
        let page = runtime
            .list_objects_page(&fragment, &params, false)
            .await
            .expect("list renamed namespace page");
        counted = counted.saturating_add(page.objects.len());
        continuation = page.continuation_token.clone();
        if continuation.is_none() {
            break;
        }
    }
    let list_after_rename_elapsed = list_after_rename_started.elapsed();
    assert_eq!(counted, object_count + 1);

    let trash_started = Instant::now();
    let trash_summary = runtime
        .trash(&fragment, "media")
        .await
        .expect("trash renamed namespace subtree");
    let trash_elapsed = trash_started.elapsed();
    assert_eq!(trash_summary.affected_paths, 1);
    assert_eq!(trash_summary.affected_objects, 0);

    let recover_started = Instant::now();
    let recover_summary = runtime
        .recover(&fragment, "media")
        .await
        .expect("recover renamed namespace subtree");
    let recover_elapsed = recover_started.elapsed();
    assert_eq!(recover_summary.affected_paths, 1);
    assert_eq!(recover_summary.affected_objects, 0);

    let trash_again_started = Instant::now();
    let trash_again_summary = runtime
        .trash(&fragment, "media")
        .await
        .expect("trash renamed namespace subtree again");
    let trash_again_elapsed = trash_again_started.elapsed();
    assert_eq!(trash_again_summary.affected_paths, 1);
    assert_eq!(trash_again_summary.affected_objects, 0);

    let empty_trash_started = Instant::now();
    let empty_summary = runtime
        .empty_trash(&fragment)
        .await
        .expect("empty namespace lifecycle trash");
    let empty_trash_elapsed = empty_trash_started.elapsed();
    assert_eq!(empty_summary.affected_paths, 1);
    assert_eq!(empty_summary.affected_objects, 0);

    let mut trash_params = HashMap::new();
    trash_params.insert("limit".to_string(), "1000".to_string());
    trash_params.insert("prefix".to_string(), "media".to_string());
    let trash_page = runtime
        .list_objects_page(&fragment, &trash_params, true)
        .await
        .expect("list namespace trash after empty");
    assert!(
        trash_page.objects.is_empty() && trash_page.common_prefixes.is_empty(),
        "empty trash should remove trashed namespace entries from trash listing"
    );

    let final_rss_kib = current_process_rss_kib();

    println!(
        "N1 indexdb POSIX bench: objects={} seed_ms={} prime_ms={} rename_object_ms={} rename_prefix_ms={} list_after_rename_ms={} trash_ms={} recover_ms={} trash_again_ms={} empty_trash_ms={} rename_prefix_paths={} rename_prefix_objects={} trash_paths={} trash_objects={} empty_paths={} empty_objects={} seed_rss_kib={} after_seed_rss_kib={} final_rss_kib={}",
        object_count,
        seed_elapsed.as_millis(),
        import_elapsed.as_millis(),
        rename_object_elapsed.as_millis(),
        rename_prefix_elapsed.as_millis(),
        list_after_rename_elapsed.as_millis(),
        trash_elapsed.as_millis(),
        recover_elapsed.as_millis(),
        trash_again_elapsed.as_millis(),
        empty_trash_elapsed.as_millis(),
        rename_prefix_summary.affected_paths,
        rename_prefix_summary.affected_objects,
        trash_summary.affected_paths,
        trash_summary.affected_objects,
        empty_summary.affected_paths,
        empty_summary.affected_objects,
        seed_rss_kib,
        after_seed_rss_kib,
        final_rss_kib
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_permanent_delete_removes_object_and_allows_recreate_without_tombstone_barrier() {
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
    let key = "tombstone/stale-writer.bin".to_string();
    put_object(state.clone(), &fragment, &key, vec![7u8; 4096]).await;

    let old_meta = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("metadata before delete");

    let resp = delete_object_permanently(
        State(state),
        full_claims(&fragment),
        AxumPath((fragment.clone(), key.clone())),
    )
    .await
    .expect("permanent delete object")
    .into_response();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    assert!(matches!(
        get_metadata(&fragment, &key, &fragment).await,
        Err(AppError::NotFound)
    ));

    index::upsert_object_version(&old_meta)
        .await
        .expect("reinsert historical version after permanent delete");

    let recreated = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("recreated metadata after permanent delete");
    assert_eq!(recreated.version_id, old_meta.version_id);
    assert_eq!(
        index::list_live_object_versions_metadata(&fragment, &key)
            .await
            .expect("list live versions after recreate")
            .len(),
        1,
        "without a tombstone barrier, recreating the object should make it visible again"
    );

    let mut newer_meta = old_meta.clone();
    newer_meta.version_id = Ulid::new().to_string();
    newer_meta.upload_session_id = newer_meta.version_id.clone();
    newer_meta.created_at = old_meta.created_at + 1;
    index::upsert_object_version(&newer_meta)
        .await
        .expect("insert newer version after recreate");

    let latest = index::read_latest_object(&fragment, &key)
        .await
        .expect("newer version should replace recreated historical version");
    assert_eq!(latest.version_id, newer_meta.version_id);

    let listed = list_objects_json(&fragment, None, None).await;
    let objects = listed["objects"].as_array().expect("objects array");
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["object_key"].as_str(), Some(key.as_str()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_direct_deleting_current_version_rolls_back_to_previous_live_version() {
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
    let key = "tombstone/delete-current-version.bin".to_string();
    let v1 = b"old-version".to_vec();
    let v2 = b"new-version".to_vec();
    put_object(state.clone(), &fragment, &key, v1.clone()).await;
    put_object(state.clone(), &fragment, &key, v2.clone()).await;

    let live_versions = index::list_live_object_versions_metadata(&fragment, &key)
        .await
        .expect("list versions before delete");
    assert_eq!(live_versions.len(), 2);
    let current_version_id = live_versions[0].version_id.clone();

    let del_resp = delete_object_version(
        State(state.clone()),
        full_claims(&fragment),
        AxumPath((fragment.clone(), current_version_id.clone(), key.clone())),
    )
    .await
    .expect("delete current version")
    .into_response();
    assert_eq!(del_resp.status(), StatusCode::NO_CONTENT);

    let latest = index::read_latest_object(&fragment, &key)
        .await
        .expect("latest object after deleting current version");
    assert_eq!(latest.object_key, key);
    let got = get_metadata(&fragment, &key, &fragment)
        .await
        .expect("get rolled-back metadata");
    assert_eq!(got.version_id, live_versions[1].version_id);
    assert!(matches!(
        index::read_latest_trashed_object(&fragment, &key).await,
        Err(AppError::NotFound)
    ));
    assert_eq!(
        index::list_live_object_versions_metadata(&fragment, &key)
            .await
            .expect("list versions after direct delete")
            .len(),
        1,
        "deleting the current version should roll back to the previous live version"
    );
    assert!(
        recover_object_version(
            State(state),
            full_claims(&fragment),
            AxumPath((fragment.clone(), current_version_id.clone(), key.clone())),
        )
        .await
        .is_err(),
        "directly deleted current version should not be recoverable from trash"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_live_listing_returns_head_summaries_without_part_arrays() {
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
    let key = "listing/summary-only.bin";
    put_object(state, &fragment, key, vec![3u8; 8192]).await;

    let page = index::scan_fragment_live_objects_page(&fragment, None, None, 10, None, false)
        .await
        .expect("scan live page");
    assert_eq!(page.objects.len(), 1);
    assert!(
        page.objects[0].parts.is_empty(),
        "list responses should come from head summaries, not full version metadata"
    );

    let full = get_metadata(&fragment, key, &fragment)
        .await
        .expect("load full object metadata");
    assert!(
        !full.parts.is_empty(),
        "full metadata reads should still load version details"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matrix_trash_listing_supports_prefix_and_delimiter_pagination() {
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
    assert!(page3["continuation_token"].is_null());

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
