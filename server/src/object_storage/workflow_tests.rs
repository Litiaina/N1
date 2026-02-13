use super::archive::download_selected_archive;
use super::constants::PREFIX_SEGMENT_EXT;
use super::delete::delete_object;
use super::delete::{
    delete_object_permanently, delete_object_version, recover_object, recover_object_version,
};
use super::disk_manager::{
    DownloadArchiveRequest, InitiateUploadParams, ObjectKeyQuery, PrefixIndexSegment,
    RenameObjectRequest, UploadPartQuery,
};
use super::fragment_metadata::init_fragment_metadata;
use super::metadata_parser::get_metadata;
use super::modify::rename_object;
use super::object_parser::{
    STREAM_CHUNK_SIZE_ATOMIC, STREAM_LOCALITY_BIAS_ENABLED_ATOMIC, STREAM_MAX_CONCURRENCY,
    STREAM_READAHEAD_BYTES_ATOMIC,
};
use super::quota_manager::init_quota_file;
use super::recovery::verify_shard_integrity;
use super::retrieve::PREFIX_MAX_DELTA_SEGMENTS_ATOMIC;
use super::redistribution::run_redistribution_job;
use super::repair_manager::{run_repair_job, RepairCheckMode, RepairRunConfig};
use super::retrieve::{get_object, get_object_metadata, stream_object};
use super::sync::ensure_fragment_blob_directories;
use super::upload::{
    finalize_object_upload, initiate_object_upload, object_upload_part, DATA_SHARDS_ATOMIC,
    MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC, PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC,
    WRITER_BUFFER_SIZE_ATOMIC,
};
use crate::object_storage::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
use crate::routes::storage_routes::AppState;
use crate::settings::constants::CONFIG_FILE;
use crate::settings::create_config::create_default_config_ini;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;

use axum::body::{to_bytes, Body};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::response::IntoResponse;
use axum::Json as AxumJson;
use futures_util::{stream, StreamExt};
use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
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
    if PREFIX_MAX_DELTA_SEGMENTS_ATOMIC.get().is_none() {
        PREFIX_MAX_DELTA_SEGMENTS_ATOMIC
            .set(AtomicUsize::new(512))
            .expect("set PREFIX_MAX_DELTA_SEGMENTS_ATOMIC");
    } else {
        PREFIX_MAX_DELTA_SEGMENTS_ATOMIC
            .get()
            .expect("PREFIX_MAX_DELTA_SEGMENTS_ATOMIC")
            .store(512, Ordering::Relaxed);
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
        if let Ok(resp) =
            get_object(claims, AxumPath((fragment.clone(), object_key.clone())), req).await
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

        let mut prefix_buckets: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

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
                            if e.created_at > *cur_ts
                                || (e.created_at == *cur_ts && e.version_id > *cur_vid)
                            {
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
        let data_blocks = if size == 0 { 0 } else { size.div_ceil(512) * 512 };
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
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-length",
        HeaderValue::from_str(&bytes.len().to_string()).expect("content-length"),
    );

    object_upload_part(
        full_claims(fragment),
        headers,
        AxumPath((fragment.to_string(), version_id.to_string(), part_index)),
        Query(UploadPartQuery { checksum: None }),
        Body::from(bytes),
    )
    .await
    .expect("upload part")
    .into_response()
}

async fn init_upload(
    fragment: &str,
    object_key: &str,
    total_size: u64,
    expected_parts: u64,
) -> (String, String) {
    let init_resp = initiate_object_upload(
        full_claims(fragment),
        AxumPath((fragment.to_string(), object_key.to_string())),
        AxumJson(InitiateUploadParams {
            content_type: Some("application/octet-stream".into()),
            total_size_bytes: total_size,
            expected_parts,
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

        let put_resp = upload_part(fragment, &version_id, part_index, part).await;
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

    let soft_deleted_meta = get_metadata(&fragment, &renamed_key, &fragment)
        .await
        .expect("metadata should exist as tombstone after soft delete");
    assert!(
        soft_deleted_meta.is_delete_marker,
        "soft delete should leave a delete-marker tombstone"
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
    let got = read_object_bytes(full_claims(&fragment), fragment.clone(), key.clone()).await;
    assert_eq!(got, expected);
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

    let pm = super::disk_manager::ObjectPathManager::new();
    let quota_paths = pm.get_all_quota_paths(&fragment);
    assert!(quota_paths.len() >= 2);

    fs::remove_file(&quota_paths[0])
        .await
        .expect("delete one quota mirror");

    let (pending_vid, _pending_sid) = init_upload(&fragment, "idx-sync/pending.bin", 8192, 1).await;
    let pending_chunk = vec![0xAB; 8192];
    let put_pending = upload_part(&fragment, &pending_vid, 0, pending_chunk).await;
    assert_eq!(put_pending.status(), StatusCode::OK);

    let session_dirs = pm.get_all_session_dirs(&fragment, &pending_vid);
    assert!(session_dirs.len() >= 2, "expected at least two session dirs");
    let manifest0 = session_dirs[0].join("session.manifest.msgpack");
    let reservation1 = session_dirs[1].join("reservation.msgpack");
    let part0_1 = session_dirs[1].join("part_0.meta.msgpack");

    fs::write(&manifest0, b"corrupt-session-manifest")
        .await
        .expect("corrupt one session manifest mirror");
    fs::remove_file(&reservation1)
        .await
        .expect("delete one session reservation mirror");
    fs::remove_file(&part0_1)
        .await
        .expect("delete one session part metadata mirror");

    let before = collect_index_tree_hashes(&fragment).await;
    assert_ne!(
        before[0].1, before[1].1,
        "precondition failed: mirrors should be divergent before repair"
    );

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

    let pending_status_resp = super::upload::get_upload_status(
        full_claims(&fragment),
        AxumPath((fragment.clone(), pending_vid.clone())),
        Query(ObjectKeyQuery {
            object_key: "idx-sync/pending.bin".to_string(),
            version_id: None,
        }),
    )
    .await
    .expect("pending status after mirror repair")
    .into_response();
    assert_eq!(pending_status_resp.status(), StatusCode::OK);
    let pending_status_json = read_response_json(pending_status_resp).await;
    assert_eq!(pending_status_json["uploaded_parts"][0].as_u64(), Some(0));
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
                    let got =
                        read_object_bytes_eventually(fragment.clone(), key_a.clone(), read_attempts)
                            .await;
                    assert_eq!(got, content, "round {} initial read mismatch {}", round, key_a);

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
                    assert_eq!(got, content, "round {} recovered read mismatch {}", round, key_b);
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

            let redist_res = run_redistribution_job(false, Arc::new(WsConnectionManager::new()))
                .await;

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
        damaged.flush().await.expect("flush corruption in soak round");
        drop(damaged);

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
            let got = read_object_bytes_eventually(fragment.clone(), key.clone(), read_attempts)
                .await;
            assert_eq!(got, *bytes, "round {} read mismatch during repair {}", round, key);
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
                round,
                key
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
#[ignore = "throughput probe for single-NVMe upload saturation; run manually with env knobs"]
async fn matrix_upload_single_nvme_saturation_probe() {
    let _guard = TEST_LOCK.lock().await;
    let base = TEST_BASE.clone();
    ensure_global_storage_config(&base).await;
    ensure_upload_runtime_config();

    let workers = test_env_usize("N1_UPLOAD_PROBE_CONCURRENCY", 8, 1, 128);
    let uploads_per_worker = test_env_usize("N1_UPLOAD_PROBE_OBJECTS_PER_WORKER", 2, 1, 1024);
    let object_bytes =
        test_env_u64("N1_UPLOAD_PROBE_OBJECT_BYTES", 64 * 1024 * 1024, 1024 * 1024, 1024 * 1024 * 1024)
            as usize;
    let part_bytes = test_env_usize(
        "N1_UPLOAD_PROBE_PART_BYTES",
        4 * 1024 * 1024,
        256 * 1024,
        64 * 1024 * 1024,
    );
    let min_mib_per_sec = test_env_u64("N1_UPLOAD_PROBE_MIN_MIBPS", 0, 0, 100_000) as f64;
    let max_ram_mib = test_env_u64("N1_UPLOAD_PROBE_MAX_RAM_MIB", 0, 0, 1_000_000);

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
            async move {
                for i in 0..uploads_per_worker {
                    let key = format!("nvme-probe/w{}/obj-{}.bin", w, i);
                    put_object_generated_chunked(
                        state.clone(),
                        &fragment,
                        &key,
                        object_bytes,
                        part_bytes,
                        w * 17 + i,
                    )
                    .await;
                }
            }
        })
        .await;

    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    let mib_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed;

    println!(
        "N1 upload probe: workers={} uploads_per_worker={} object_bytes={} part_bytes={} est_working_set_mib={:.2} total_uploads={} total_mib={:.2} elapsed_s={:.3} throughput_mib_s={:.2}",
        workers,
        uploads_per_worker,
        object_bytes,
        part_bytes,
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
    put_object_generated_chunked(
        state.clone(),
        &fragment,
        &key,
        object_bytes,
        part_bytes,
        7,
    )
    .await;

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
    let body1 = to_bytes(resp1.into_body(), usize::MAX).await.expect("page1 body");
    let mut names1 = parse_tar_entry_names(&body1);
    names1.sort();
    assert_eq!(names1, vec!["page/a.bin".to_string(), "page/b.bin".to_string()]);

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
    let body2 = to_bytes(resp2.into_body(), usize::MAX).await.expect("page2 body");
    let mut names2 = parse_tar_entry_names(&body2);
    names2.sort();
    assert_eq!(names2, vec!["page/c.bin".to_string(), "page/d.bin".to_string()]);

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
    let body3 = to_bytes(resp3.into_body(), usize::MAX).await.expect("page3 body");
    let mut names3 = parse_tar_entry_names(&body3);
    names3.sort();
    assert_eq!(names3, vec!["page/e.bin".to_string()]);
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
