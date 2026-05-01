use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use tokio::fs;
use ulid::Ulid;

use crate::core::disk_manager::{
    BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS, UploadPartQuery,
};
use crate::core::fragment_metadata::{
    DEFAULT_INDEX_PARTITION_COUNT, clear_fragment_partition_count_cache_for_test,
    init_fragment_metadata, set_configured_index_partition_count,
};
use crate::core::index;
use crate::core::indexdb_store;
use crate::core::object_parser::{
    STREAM_CHUNK_SIZE_ATOMIC, STREAM_LOCALITY_BIAS_ENABLED_ATOMIC, STREAM_MAX_CONCURRENCY,
    STREAM_READAHEAD_BYTES_ATOMIC,
};
use crate::core::quota_manager::init_quota_file;
use crate::core::sync::ensure_fragment_blob_directories;
use crate::core::upload::{
    DATA_SHARDS_ATOMIC, MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC, PARITY_SHARDS_ATOMIC,
    STRIPE_SIZE_ATOMIC, UPLOAD_IO_GATE_ENABLED_ATOMIC, UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC,
    WRITER_BUFFER_SIZE_ATOMIC, upload_object_one_shot,
};
use crate::namespace::runtime::global_namespace_runtime;
use crate::posix::models::PosixNodeKind;
use crate::posix::service;
use crate::routes::storage_routes::AppState;
use crate::settings::constants::CONFIG_FILE;
use crate::settings::create_config::create_default_config_ini;
use crate::structs::auth_structs::Claims;
use crate::test_support::global_test_lock;
use crate::websocket::manager::WsConnectionManager;

struct SharedTestLock;

impl SharedTestLock {
    async fn lock(&self) -> tokio::sync::MutexGuard<'static, ()> {
        global_test_lock().lock().await
    }
}

static TEST_LOCK: SharedTestLock = SharedTestLock;
static TEST_BASE: Lazy<PathBuf> =
    Lazy::new(|| std::env::temp_dir().join(format!("n1-posix-{}", std::process::id())));

fn ensure_test_config_file() {
    let config_path = PathBuf::from(CONFIG_FILE);
    if !config_path.exists() {
        create_default_config_ini(&config_path).expect("create default test config");
    }
}

async fn ensure_global_storage_config(base: &Path) {
    ensure_test_config_file();
    set_configured_index_partition_count(DEFAULT_INDEX_PARTITION_COUNT)
        .expect("reset configured index partition count");
    clear_fragment_partition_count_cache_for_test();
    indexdb_store::clear_cached_dbs_for_test();
    if let (Some(index_roots), Some(blob_roots)) = (INDEX_STORE_DISKS.get(), BLOB_STORE_DISKS.get())
    {
        for root in index_roots.iter().chain(blob_roots.iter()) {
            let path = PathBuf::from(root);
            let _ = fs::remove_dir_all(&path).await;
            let _ = fs::create_dir_all(&path).await;
        }
        return;
    }
    let _ = fs::remove_dir_all(base).await;
    let _ = fs::create_dir_all(base).await;

    let index_roots = vec![base.join("index_1"), base.join("index_2")];
    let blob_roots = vec![
        base.join("blob_1"),
        base.join("blob_2"),
        base.join("blob_3"),
    ];
    for root in index_roots.iter().chain(blob_roots.iter()) {
        let _ = fs::create_dir_all(root).await;
    }

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
    }
    if BLOB_STORE_DISKS.get().is_none() {
        BLOB_STORE_DISKS
            .set(blob_roots.clone())
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

fn app_state() -> Arc<AppState> {
    Arc::new(AppState {
        ws_manager: WsConnectionManager::new(),
    })
}

async fn setup_fragment(fragment: &str) {
    init_quota_file(fragment, 128 * 1024 * 1024)
        .await
        .expect("init quota");
    ensure_fragment_blob_directories(fragment).await;
    init_fragment_metadata(fragment, "test-key")
        .await
        .expect("init fragment metadata");
}

async fn put_object(state: Arc<AppState>, fragment: &str, object_key: &str, payload: Vec<u8>) {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&payload.len().to_string()).expect("content length"),
    );
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    let response = upload_object_one_shot(
        State(state),
        full_claims(fragment),
        headers,
        AxumPath((fragment.to_string(), object_key.to_string())),
        Query(UploadPartQuery {
            checksum: None,
            durability: None,
            strict_durability: None,
            io_gate: None,
        }),
        Body::from(payload),
    )
    .await
    .expect("upload object")
    .into_response();
    assert_eq!(response.status(), StatusCode::CREATED);
    let metadata = index::read_latest_object(fragment, object_key)
        .await
        .expect("uploaded object metadata");
    global_namespace_runtime()
        .finalize_logical_upload(fragment, object_key, object_key, &metadata)
        .await
        .expect("bind POSIX namespace entry");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posix_mkdir_creates_hidden_marker_and_lists_directory() {
    let _guard = TEST_LOCK.lock().await;
    ensure_global_storage_config(&TEST_BASE).await;
    ensure_upload_runtime_config();

    let fragment = hex::encode(Sha256::digest(
        format!("posix-fragment-{}", Ulid::new()).as_bytes(),
    ));
    setup_fragment(&fragment).await;
    let state = app_state();

    let mkdir = service::mkdir_path(
        Arc::clone(&state),
        full_claims(&fragment),
        "reports".to_string(),
    )
    .await
    .expect("mkdir");
    assert_eq!(mkdir.kind, PosixNodeKind::Directory);
    assert_eq!(mkdir.created, Some(true));

    let root = service::list_dir(full_claims(&fragment), String::new(), 100, None, false)
        .await
        .expect("root list");
    assert!(
        root.entries
            .iter()
            .any(|entry| entry.kind == PosixNodeKind::Directory && entry.path == "reports"),
        "root listing missing reports directory: {:?}",
        root.entries
    );

    let stat = service::stat_path(full_claims(&fragment), "reports".to_string(), false)
        .await
        .expect("stat dir");
    assert_eq!(stat.kind, PosixNodeKind::Directory);
    assert!(
        !stat.is_virtual,
        "mkdir should create an explicit namespace directory"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posix_rename_moves_directory_tree_without_touching_core_identity() {
    let _guard = TEST_LOCK.lock().await;
    ensure_global_storage_config(&TEST_BASE).await;
    ensure_upload_runtime_config();

    let fragment = hex::encode(Sha256::digest(
        format!("posix-fragment-{}", Ulid::new()).as_bytes(),
    ));
    setup_fragment(&fragment).await;
    let state = app_state();

    service::mkdir_path(
        Arc::clone(&state),
        full_claims(&fragment),
        "reports".to_string(),
    )
    .await
    .expect("mkdir reports");
    put_object(
        Arc::clone(&state),
        &fragment,
        "reports/a.txt",
        b"alpha".to_vec(),
    )
    .await;
    put_object(
        Arc::clone(&state),
        &fragment,
        "reports/sub/b.txt",
        b"beta".to_vec(),
    )
    .await;

    let rename = service::rename_path(
        Arc::clone(&state),
        full_claims(&fragment),
        "reports".to_string(),
        "archive".to_string(),
    )
    .await
    .expect("rename path");
    assert_eq!(rename.kind, PosixNodeKind::Directory);
    assert_eq!(rename.affected, 1);

    assert!(
        service::stat_path(full_claims(&fragment), "reports".to_string(), false)
            .await
            .is_err(),
        "old path should be gone"
    );

    let archive = service::list_dir(
        full_claims(&fragment),
        "archive".to_string(),
        100,
        None,
        false,
    )
    .await
    .expect("archive list");
    assert!(
        archive
            .entries
            .iter()
            .any(|entry| entry.path == "archive/a.txt"),
        "archive listing missing renamed file: {:?}",
        archive.entries
    );
    assert!(
        archive
            .entries
            .iter()
            .any(|entry| entry.path == "archive/sub"),
        "archive listing missing renamed subdir: {:?}",
        archive.entries
    );

    let renamed_file =
        service::stat_path(full_claims(&fragment), "archive/a.txt".to_string(), false)
            .await
            .expect("renamed file stat");
    assert_eq!(renamed_file.kind, PosixNodeKind::File);
    assert_eq!(renamed_file.size_bytes, Some(5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posix_trash_and_recover_round_trip_for_directory_tree() {
    let _guard = TEST_LOCK.lock().await;
    ensure_global_storage_config(&TEST_BASE).await;
    ensure_upload_runtime_config();

    let fragment = hex::encode(Sha256::digest(
        format!("posix-fragment-{}", Ulid::new()).as_bytes(),
    ));
    setup_fragment(&fragment).await;
    let state = app_state();

    service::mkdir_path(
        Arc::clone(&state),
        full_claims(&fragment),
        "videos".to_string(),
    )
    .await
    .expect("mkdir videos");
    put_object(
        Arc::clone(&state),
        &fragment,
        "videos/demo.mp4",
        b"demo".to_vec(),
    )
    .await;

    let trash = service::trash_path(
        Arc::clone(&state),
        full_claims(&fragment),
        "videos".to_string(),
    )
    .await
    .expect("trash directory");
    assert_eq!(trash.kind, PosixNodeKind::Directory);
    assert_eq!(trash.affected, 1);

    let live_root = service::list_dir(full_claims(&fragment), String::new(), 100, None, false)
        .await
        .expect("live root list");
    assert!(
        live_root.entries.iter().all(|entry| entry.path != "videos"),
        "videos should not remain in live root: {:?}",
        live_root.entries
    );

    let trash_root = service::list_dir(full_claims(&fragment), String::new(), 100, None, true)
        .await
        .expect("trash root list");
    assert!(
        trash_root
            .entries
            .iter()
            .any(|entry| entry.path == "videos"),
        "trash root should expose directory via common prefix: {:?}",
        trash_root.entries
    );

    let recover = service::recover_path(
        Arc::clone(&state),
        full_claims(&fragment),
        "videos".to_string(),
    )
    .await
    .expect("recover directory");
    assert_eq!(recover.kind, PosixNodeKind::Directory);
    assert_eq!(recover.affected, 1);

    let recovered = service::list_dir(
        full_claims(&fragment),
        "videos".to_string(),
        100,
        None,
        false,
    )
    .await
    .expect("recovered list");
    assert!(
        recovered
            .entries
            .iter()
            .any(|entry| entry.path == "videos/demo.mp4" && entry.kind == PosixNodeKind::File),
        "recovered listing missing file: {:?}",
        recovered.entries
    );
}
