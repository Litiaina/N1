//! A retired upload session is not authority to delete committed object data.

use super::*;
use crate::core::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
use crate::core::index_store;
use tokio::sync::Notify;

struct PartPause {
    fragment: String,
    version: String,
    after_receipt: bool,
    entered: Notify,
    release: Notify,
}

static PART_PAUSE: Mutex<Option<Arc<PartPause>>> = Mutex::new(None);

struct PauseGuard(Arc<PartPause>);

impl Drop for PauseGuard {
    fn drop(&mut self) {
        let mut slot = PART_PAUSE.lock().unwrap();
        if slot
            .as_ref()
            .is_some_and(|pause| Arc::ptr_eq(pause, &self.0))
        {
            *slot = None;
        }
        self.0.release.notify_one();
    }
}

pub(super) async fn part_write_boundary(fragment: &str, version: &str, after_receipt: bool) {
    let pause = PART_PAUSE.lock().unwrap().clone();
    if let Some(pause) = pause.filter(|pause| {
        pause.fragment == fragment
            && pause.version == version
            && pause.after_receipt == after_receipt
    }) {
        pause.entered.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(30), pause.release.notified())
            .await
            .expect("test must release paused part writer");
    }
}

fn fixture() -> (String, Claims, Vec<PathBuf>) {
    let base = std::env::temp_dir().join(format!("n1-retired-part-{}", std::process::id()));
    for cell in [&INDEX_STORE_DISKS, &BLOB_STORE_DISKS] {
        for root in cell.get_or_init(|| {
            (0..2)
                .map(|i| base.join(format!("member-{i}")).display().to_string())
                .collect()
        }) {
            std::fs::create_dir_all(root).unwrap();
        }
    }
    let control = FS_STORAGE_ROOT.get_or_init(|| base.join("control").display().to_string());
    let mut enrolled_roots = INDEX_STORE_DISKS
        .get()
        .unwrap()
        .iter()
        .chain(BLOB_STORE_DISKS.get().unwrap())
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    enrolled_roots.sort();
    enrolled_roots.dedup();
    crate::core::root_identity::reconcile_roots_for_test(
        &Path::new(control).join(".n1/topology-roots.json"),
        &enrolled_roots,
    )
    .expect("enroll actual test root identities before strict shard writes");
    MAX_PART_SIZE_ATOMIC.get_or_init(|| AtomicU64::new(1024 * 1024));
    DATA_SHARDS_ATOMIC.get_or_init(|| AtomicUsize::new(1));
    PARITY_SHARDS_ATOMIC.get_or_init(|| AtomicUsize::new(0));
    WRITER_BUFFER_SIZE_ATOMIC.get_or_init(|| AtomicUsize::new(4096));
    STRIPE_SIZE_ATOMIC.get_or_init(|| AtomicUsize::new(4096));
    let manager = ObjectPathManager::new();
    let mut roots = manager.get_all_index_roots();
    roots.extend(manager.get_blob_roots());
    roots.sort();
    roots.dedup();
    let fragment = Ulid::new().to_string();
    let claims = Claims {
        operator: false,
        fragment: fragment.clone(),
        fragment_alias: None,
        read: true,
        write: true,
        modify: true,
        delete: true,
        exp: usize::MAX,
    };
    (fragment, claims, roots)
}

fn ndx_bytes(roots: &[PathBuf], fragment: &str, object: &str) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    for root in roots {
        let dir = index_store::live_object_dir(root, fragment, object).unwrap();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries {
                let path = entry.unwrap().path();
                if path.extension().is_some_and(|extension| extension == "ndx") {
                    files.push((path.clone(), std::fs::read(path).unwrap()));
                }
            }
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    assert!(
        !files.is_empty(),
        "a finalized version must have immutable NDX replicas"
    );
    files
}

async fn finalized_session_retirement_does_not_delete_payload(wrapper: u8) {
    let _lock = crate::test_support::global_test_lock().lock().await;
    for after_receipt in [false, true] {
        let (fragment, claims, roots) = fixture();
        let manager = ObjectPathManager::new();
        let placement = manager.get_blob_roots();
        assert!(!placement.is_empty());
        let payload = (0..8192)
            .map(|i| ((i * 17 + 29) % 251) as u8)
            .collect::<Vec<_>>();
        let manifest = UploadSessionManifest {
            fragment: fragment.clone(),
            object_key: index_store::new_object_id(),
            version_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            total_size_bytes: payload.len() as u64,
            expected_parts: 1,
            part_size_bytes: payload.len() as u64,
            content_type: "application/octet-stream".into(),
            created_at: Utc::now().timestamp(),
            stripe_size_bytes: 4096,
            data_shards: 1,
            parity_shards: u16::from(placement.len() > 1),
            placement_roots: placement
                .iter()
                .take(2)
                .map(|root| root.display().to_string())
                .collect(),
        };
        index_store::open_session(&manifest, None).unwrap();
        let pause = Arc::new(PartPause {
            fragment: fragment.clone(),
            version: manifest.version_id.clone(),
            after_receipt,
            entered: Notify::new(),
            release: Notify::new(),
        });
        *PART_PAUSE.lock().unwrap() = Some(pause.clone());
        let guard = PauseGuard(pause.clone());
        let worker_claims = claims.clone();
        let worker_fragment = fragment.clone();
        let worker_version = manifest.version_id.clone();
        let bytes = payload.clone();
        let mut worker = tokio::spawn(async move {
            let query = UploadPartQuery {
                checksum: None,
                durability: Some("strict".into()),
                strict_durability: Some(true),
                io_gate: Some(false),
            };
            let length = bytes.len() as u64;
            match wrapper {
                0 => {
                    let mut headers = HeaderMap::new();
                    headers.insert(axum::http::header::CONTENT_LENGTH, length.into());
                    object_upload_part(
                        worker_claims,
                        headers,
                        AxumPath((worker_fragment, worker_version, 0)),
                        Query(query),
                        Body::from(bytes),
                    )
                    .await
                    .map(|response| response.into_response().status())
                }
                1 => object_upload_part_stream_direct(
                    worker_claims,
                    worker_fragment,
                    worker_version,
                    0,
                    query,
                    futures_util::stream::iter([Ok(Bytes::from(bytes))]),
                    length,
                )
                .await
                .map(|()| StatusCode::OK),
                _ => object_upload_part_reader_direct(
                    worker_claims,
                    worker_fragment,
                    worker_version,
                    0,
                    query,
                    &mut bytes.as_slice(),
                    length,
                )
                .await
                .map(|()| StatusCode::OK),
            }
        });
        tokio::select! {
            result = &mut worker => panic!("part writer exited before the controlled boundary: {result:?}"),
            entered = tokio::time::timeout(std::time::Duration::from_secs(30), pause.entered.notified()) => {
                entered.expect("actual part writer must reach the post-write pause");
            }
        }
        let params = InitiateUploadParams {
            content_type: Some(manifest.content_type.clone()),
            total_size_bytes: manifest.total_size_bytes,
            expected_parts: manifest.expected_parts,
            part_size_bytes: Some(manifest.part_size_bytes),
        };
        let metadata = match finalize_object_upload_direct(
            claims.clone(),
            fragment.clone(),
            manifest.version_id.clone(),
            manifest.session_id.clone(),
            manifest.object_key.clone(),
            params,
        )
        .await
        .unwrap()
        {
            FinalizeUploadResult::Complete(metadata) => metadata,
            FinalizeUploadResult::Incomplete(_) => panic!("completed shard set must finalize"),
        };
        let logical_key = "documents/finalized-before-part-ack.bin";
        crate::namespace::runtime::global_namespace_runtime()
            .finalize_logical_upload(&fragment, logical_key, &manifest.object_key, &metadata)
            .await
            .unwrap();
        index::remove_session(&fragment, &manifest.version_id, "finalized")
            .await
            .unwrap();
        assert!(
            index::load_session(&fragment, &manifest.version_id)
                .await
                .unwrap()
                .is_none()
        );
        let shards = metadata
            .parts
            .iter()
            .flat_map(|part| &part.shards)
            .map(|shard| {
                let path =
                    crate::core::storage_pool::resolve_address(&shard.absolute_path).unwrap();
                (path.clone(), std::fs::read(path).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            shards.len(),
            manifest.data_shards as usize + manifest.parity_shards as usize
        );
        let ndx = ndx_bytes(&roots, &fragment, &manifest.object_key);
        pause.release.notify_one();
        match tokio::time::timeout(std::time::Duration::from_secs(30), worker)
            .await
            .unwrap()
            .unwrap()
        {
            Ok(status) => assert_eq!(status, StatusCode::CONFLICT),
            Err(error) => assert!(matches!(error, AppError::Conflict(_))),
        }
        for (path, bytes) in &shards {
            assert_eq!(
                &std::fs::read(path).expect("retired part must not delete committed shards"),
                bytes
            );
        }
        assert_eq!(ndx_bytes(&roots, &fragment, &manifest.object_key), ndx);
        let saved =
            index::read_object_version(&fragment, &manifest.object_key, &manifest.version_id)
                .await
                .unwrap();
        assert_eq!(
            rmp_serde::to_vec_named(&saved).unwrap(),
            rmp_serde::to_vec_named(&metadata).unwrap()
        );
        let response = crate::core::download::download_object(
            claims,
            AxumPath(fragment.clone()),
            Query(ObjectKeyQuery {
                object_key: logical_key.into(),
                version_id: Some(manifest.version_id.clone()),
                filename: None,
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let downloaded = axum::body::to_bytes(response.into_body(), payload.len() + 1)
            .await
            .unwrap();
        assert_eq!(downloaded.as_ref(), payload.as_slice());
        drop(guard);
        for root in roots {
            let _ = std::fs::remove_dir_all(index_store::fragment_root_on(&root, &fragment));
        }
    }
}

#[tokio::test]
async fn http_body_part_cannot_delete_a_concurrently_finalized_version() {
    finalized_session_retirement_does_not_delete_payload(0).await;
}

#[tokio::test]
async fn stream_part_cannot_delete_a_concurrently_finalized_version() {
    finalized_session_retirement_does_not_delete_payload(1).await;
}

#[tokio::test]
async fn reader_part_cannot_delete_a_concurrently_finalized_version() {
    finalized_session_retirement_does_not_delete_payload(2).await;
}

async fn cancellation_preserves_data(corrupt: bool, retired: bool) {
    let (fragment, claims, roots) = fixture();
    let payload = vec![0x53; 8192];
    let placement = ObjectPathManager::new().get_blob_roots();
    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: index_store::new_object_id(),
        version_id: Ulid::new().to_string(),
        session_id: Ulid::new().to_string(),
        total_size_bytes: payload.len() as u64,
        expected_parts: 1,
        part_size_bytes: payload.len() as u64,
        content_type: "application/octet-stream".into(),
        created_at: Utc::now().timestamp(),
        stripe_size_bytes: 4096,
        data_shards: 1,
        parity_shards: u16::from(placement.len() > 1),
        placement_roots: placement
            .iter()
            .take(2)
            .map(|root| root.display().to_string())
            .collect(),
    };
    index_store::open_session(&manifest, None).unwrap();
    object_upload_part_reader_direct(
        claims.clone(),
        fragment.clone(),
        manifest.version_id.clone(),
        0,
        UploadPartQuery {
            checksum: None,
            durability: Some("strict".into()),
            strict_durability: Some(true),
            io_gate: Some(false),
        },
        &mut payload.as_slice(),
        payload.len() as u64,
    )
    .await
    .unwrap();
    let metadata = match finalize_object_upload_direct(
        claims.clone(),
        fragment.clone(),
        manifest.version_id.clone(),
        manifest.session_id.clone(),
        manifest.object_key.clone(),
        InitiateUploadParams {
            content_type: Some(manifest.content_type.clone()),
            total_size_bytes: manifest.total_size_bytes,
            expected_parts: manifest.expected_parts,
            part_size_bytes: Some(manifest.part_size_bytes),
        },
    )
    .await
    .unwrap()
    {
        FinalizeUploadResult::Complete(metadata) => metadata,
        FinalizeUploadResult::Incomplete(_) => panic!("fixture must be finalized"),
    };
    let shards = metadata
        .parts
        .iter()
        .flat_map(|part| &part.shards)
        .map(|shard| {
            let path = crate::core::storage_pool::resolve_address(&shard.absolute_path).unwrap();
            (path.clone(), std::fs::read(path).unwrap())
        })
        .collect::<Vec<_>>();
    let logical_key = "documents/already-retired.bin";
    if retired {
        crate::namespace::runtime::global_namespace_runtime()
            .finalize_logical_upload(&fragment, logical_key, &manifest.object_key, &metadata)
            .await
            .unwrap();
        index::remove_session(&fragment, &manifest.version_id, "finalized")
            .await
            .unwrap();
    }
    if corrupt {
        for (path, _) in ndx_bytes(&roots, &fragment, &manifest.object_key) {
            std::fs::write(path, [0xc1]).unwrap();
        }
    }
    let before_ndx = ndx_bytes(&roots, &fragment, &manifest.object_key);
    let outcome = cancel_upload_session_direct(
        claims.clone(),
        fragment.clone(),
        manifest.version_id.clone(),
        if retired {
            logical_key.into()
        } else {
            manifest.object_key.clone()
        },
    )
    .await;
    if corrupt {
        let error = outcome.unwrap_err();
        assert!(
            !matches!(error, AppError::NotFound | AppError::Conflict(_)),
            "metadata decode/read failure must remain visible: {error}"
        );
    } else {
        assert!(matches!(outcome, Err(AppError::Conflict(_))));
    }
    for (path, bytes) in shards {
        assert_eq!(
            std::fs::read(path).expect("failed cancellation must preserve payload"),
            bytes
        );
    }
    assert_eq!(
        ndx_bytes(&roots, &fragment, &manifest.object_key),
        before_ndx
    );
    assert_eq!(
        index::load_session(&fragment, &manifest.version_id)
            .await
            .unwrap()
            .is_some(),
        !retired,
    );
    if retired && !corrupt {
        let response = crate::core::download::download_object(
            claims,
            AxumPath(fragment.clone()),
            Query(ObjectKeyQuery {
                object_key: logical_key.into(),
                version_id: Some(manifest.version_id.clone()),
                filename: None,
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let downloaded = axum::body::to_bytes(response.into_body(), payload.len() + 1)
            .await
            .unwrap();
        assert_eq!(downloaded.as_ref(), payload.as_slice());
    }
    for root in roots {
        let _ = std::fs::remove_dir_all(index_store::fragment_root_on(&root, &fragment));
    }
}

#[tokio::test]
async fn cancellation_does_not_delete_a_committed_or_corrupt_version() {
    let _lock = crate::test_support::global_test_lock().lock().await;
    for corrupt in [false, true] {
        cancellation_preserves_data(corrupt, false).await;
    }
}

#[tokio::test]
async fn cancelling_an_already_retired_session_does_not_reinterpret_a_logical_hint() {
    let _lock = crate::test_support::global_test_lock().lock().await;
    cancellation_preserves_data(false, true).await;
    cancellation_preserves_data(true, true).await;

    let (fragment, claims, roots) = fixture();
    let object = index_store::new_object_id();
    let version = Ulid::new().to_string();
    let payload = b"uncommitted late-writer bytes belong to GC, not a caller hint";
    let manager = ObjectPathManager::new();
    let mut files = Vec::new();
    for root in manager.get_blob_roots() {
        let dir = manager.get_shard_dir_on_disk(&root, &fragment, &object, &version);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("part_0_shard_0.shard");
        std::fs::write(&path, payload).unwrap();
        files.push(path);
    }
    for hint in ["missing/logical-file.bin", object.as_str()] {
        for _ in 0..2 {
            cancel_upload_session_direct(
                claims.clone(),
                fragment.clone(),
                version.clone(),
                hint.into(),
            )
            .await
            .expect("absent-session cancellation must remain idempotent");
            for path in &files {
                assert_eq!(
                    std::fs::read(path).expect("a hint is not deletion authority"),
                    payload
                );
            }
            assert!(
                index::load_session(&fragment, &version)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
    for root in roots {
        let _ = std::fs::remove_dir_all(index_store::fragment_root_on(&root, &fragment));
    }
}

#[tokio::test]
async fn repeated_http_part_consumes_the_body_without_rewriting_shards() {
    use std::os::unix::fs::MetadataExt;

    let _lock = crate::test_support::global_test_lock().lock().await;
    let (fragment, claims, roots) = fixture();
    let payload = vec![0x6a; 8192];
    let manager = ObjectPathManager::new();
    let placement = manager.get_blob_roots();
    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: index_store::new_object_id(),
        version_id: Ulid::new().to_string(),
        session_id: Ulid::new().to_string(),
        total_size_bytes: payload.len() as u64,
        expected_parts: 1,
        part_size_bytes: payload.len() as u64,
        content_type: "application/octet-stream".into(),
        created_at: Utc::now().timestamp(),
        stripe_size_bytes: 4096,
        data_shards: 1,
        parity_shards: u16::from(placement.len() > 1),
        placement_roots: placement
            .iter()
            .take(2)
            .map(|root| root.display().to_string())
            .collect(),
    };
    index_store::open_session(&manifest, None).unwrap();
    let query = UploadPartQuery {
        checksum: None,
        durability: Some("strict".into()),
        strict_durability: Some(true),
        io_gate: Some(false),
    };
    object_upload_part_body_direct(
        claims.clone(),
        fragment.clone(),
        manifest.version_id.clone(),
        0,
        query.clone(),
        Body::from(payload.clone()),
        payload.len() as u64,
    )
    .await
    .unwrap();
    let before_receipts = index::load_session(&fragment, &manifest.version_id)
        .await
        .unwrap()
        .unwrap()
        .part_receipts;
    let shards = shard_specs_for_part(&manager, &manifest, 0)
        .unwrap()
        .into_iter()
        .map(|(_, _, path, _)| {
            let metadata = std::fs::metadata(&path).unwrap();
            (
                path.clone(),
                std::fs::read(path).unwrap(),
                metadata.ino(),
                metadata.mtime(),
                metadata.mtime_nsec(),
            )
        })
        .collect::<Vec<_>>();
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        (payload.len() as u64).into(),
    );
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, io::Error>>(2);
    let consumed = Arc::new(AtomicUsize::new(0));
    let consume_counter = consumed.clone();
    let first_chunk = Arc::new(Notify::new());
    let chunk_notification = first_chunk.clone();
    let body_stream = futures_util::stream::unfold(receiver, move |mut receiver| {
        let consumed = consume_counter.clone();
        let first_chunk = chunk_notification.clone();
        async move {
            receiver.recv().await.map(|chunk| {
                if let Ok(bytes) = &chunk {
                    if consumed.fetch_add(bytes.len(), Ordering::SeqCst) == 0 {
                        first_chunk.notify_one();
                    }
                }
                (chunk, receiver)
            })
        }
    });
    sender
        .send(Ok(Bytes::copy_from_slice(&payload[..1024])))
        .await
        .unwrap();
    let worker_claims = claims.clone();
    let worker_fragment = fragment.clone();
    let worker_version = manifest.version_id.clone();
    let worker_headers = headers.clone();
    let worker_query = query.clone();
    let mut worker = tokio::spawn(async move {
        object_upload_part(
            worker_claims,
            worker_headers,
            AxumPath((worker_fragment, worker_version, 0)),
            Query(worker_query),
            Body::from_stream(body_stream),
        )
        .await
        .map(|response| response.into_response().status())
    });
    tokio::select! {
        result = &mut worker => panic!("replay responded without consuming its body: {result:?}"),
        first = tokio::time::timeout(std::time::Duration::from_secs(30), first_chunk.notified()) => {
            first.expect("replay must start reading the body");
        }
    }
    tokio::select! {
        result = &mut worker => panic!("replay responded before its body completed: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
    }
    assert_eq!(consumed.load(Ordering::SeqCst), 1024);
    for chunk in payload[1024..].chunks(1024) {
        sender
            .send(Ok(Bytes::copy_from_slice(chunk)))
            .await
            .unwrap();
    }
    drop(sender);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(30), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        StatusCode::OK
    );
    assert_eq!(consumed.load(Ordering::SeqCst), payload.len());
    for bad_body in [
        Body::from(payload[..4096].to_vec()),
        Body::from(vec![0x6a; payload.len() + 1]),
        Body::from_stream(futures_util::stream::iter([Err::<Bytes, _>(
            io::Error::from_raw_os_error(libc::EIO),
        )])),
    ] {
        let error = object_upload_part(
            claims.clone(),
            headers.clone(),
            AxumPath((fragment.clone(), manifest.version_id.clone(), 0)),
            Query(query.clone()),
            bad_body,
        )
        .await
        .err()
        .expect("invalid replay body must not get an acknowledgement");
        assert!(matches!(
            error,
            AppError::BadRequest(_) | AppError::Io(_, _)
        ));
    }
    for (path, bytes, inode, mtime, mtime_nsec) in shards {
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        let metadata = std::fs::metadata(path).unwrap();
        assert_eq!(
            (metadata.ino(), metadata.mtime(), metadata.mtime_nsec()),
            (inode, mtime, mtime_nsec)
        );
    }
    assert_eq!(
        index::load_session(&fragment, &manifest.version_id)
            .await
            .unwrap()
            .unwrap()
            .part_receipts,
        before_receipts
    );
    for root in roots {
        let _ = std::fs::remove_dir_all(index_store::fragment_root_on(&root, &fragment));
    }
}
