use super::*;
use crate::namespace::store::lifecycle::*;

#[tokio::test]
async fn read_resolved_before_migration_retries_only_its_unchanged_version() {
    placement_read(true).await;
}

#[tokio::test]
async fn retired_key_without_a_placement_switch_is_not_replayed() {
    placement_read(false).await;
}

async fn placement_read(migrate: bool) {
    use crate::{
        MetadataExecutor,
        object_store::{NativeObjectStore, ObjectStoreConfig},
        service::{MetadataService, ServiceError},
    };
    use n1_link::noa::{Command, Request, Response};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    async fn read(stream: &mut TcpStream) -> Request {
        let size = stream.read_u32().await.unwrap();
        assert!(size < 8192);
        let mut bytes = vec![0; size as usize];
        stream.read_exact(&mut bytes).await.unwrap();
        rmp_serde::from_slice(&bytes).unwrap()
    }
    async fn write(stream: &mut TcpStream, response: Response) {
        let bytes = rmp_serde::to_vec_named(&response).unwrap();
        stream.write_u32(bytes.len() as u32).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
    }
    let (_root, ns) = fixture();
    let handle = reserve(&ns, "racing-read", 10);
    let published = publish(&ns, &handle, 10);
    // Capture the real resolved version before the placement transaction.
    let old = ns.resolve(FRAGMENT, "racing-read", None).unwrap();
    let old_key = old.object_key.clone();
    let destination = upload(20);
    let new_key = destination.object_key.clone();
    if migrate {
        let target = Target {
            fragment: FRAGMENT,
            object: published.object_id,
            version: published.version_id,
        };
        ns.create_move(
            "read-race",
            &MoveWork {
                target: target.clone(),
                source: old_key.clone(),
                upload: destination,
                switched: false,
            },
        )
        .unwrap();
        ns.switch_move("read-race", &target, &stat()).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let objects = NativeObjectStore::new(ObjectStoreConfig {
        native_url: format!("n1://{}", listener.local_addr().unwrap()),
        fragment: "test".into(),
        secret: "test-only-secret".into(),
        max_connections: 1,
        tls: None,
    })
    .unwrap();
    let server = tokio::spawn(async move {
        for (attempt, key) in [old_key, new_key]
            .into_iter()
            .take(if migrate { 2 } else { 1 })
            .enumerate()
        {
            let (mut stream, _) = listener.accept().await.unwrap();
            let auth = read(&mut stream).await;
            assert!(matches!(
                (attempt, auth.command),
                (0, Command::Login { .. }) | (1, Command::Bearer { .. })
            ));
            write(
                &mut stream,
                Response::ok(
                    auth.id,
                    serde_json::json!({"token":"test-token","session":{"exp":9_999_999_999u64}}),
                ),
            )
            .await;
            let request = read(&mut stream).await;
            assert!(
                matches!(request.command, Command::ObjectGet { object_key, offset: 7, length: Some(3) } if object_key == key)
            );
            if attempt == 0 {
                write(&mut stream, Response::error(request.id, 404, "purged")).await;
            } else {
                let mut response = Response::ok(request.id, serde_json::json!({}));
                response.body_len = 3;
                write(&mut stream, response).await;
                stream.write_all(b"abc").await.unwrap();
            }
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "read replayed beyond the permitted placement retry"
        );
    });
    let service = MetadataService::new(MetadataExecutor::new(ns.engine, 2).unwrap(), objects);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        let result = service
            .download_resolved(FRAGMENT, old, 7, Some(3), &mut bytes)
            .await;
        if migrate {
            assert_eq!(result.unwrap(), 3);
            assert_eq!(bytes, b"abc");
        } else {
            assert!(matches!(result, Err(ServiceError::Object(error)) if error.status() == 404));
            assert!(bytes.is_empty());
        }
        server.await.unwrap();
    })
    .await
    .expect("placement read did not terminate");
    service.shutdown().await;
}

#[test]
fn expiry_index_is_ordered_scoped_and_recovery_restarts_ttl() {
    let (_root, ns) = fixture();
    for (name, n, time) in [("new", 10, 300), ("old", 11, 100), ("middle", 12, 200)] {
        let handle = reserve(&ns, name, n);
        publish(&ns, &handle, n);
        ns.trash(FRAGMENT, name, time).unwrap();
    }
    let first = ns.due_trash(None, 200, None, 1).unwrap();
    assert_eq!(first[0].1.deleted_at, 100);
    let second = ns.due_trash(None, 200, Some(&first[0].0), 1).unwrap();
    assert_eq!(second[0].1.deleted_at, 200);
    assert!(
        ns.due_trash(None, 200, Some(&second[0].0), 1)
            .unwrap()
            .is_empty()
    );
    assert!(
        ns.due_trash(Some(PoolId([99; 16])), 999, None, 10)
            .unwrap()
            .is_empty()
    );
    ns.recover(FRAGMENT, &first[0].1.trash).unwrap();
    assert!(!ns.claim_trash(&first[0].1, 999).unwrap());
    ns.trash(FRAGMENT, "old", 500).unwrap();
    assert_eq!(
        ns.due_trash(Some(FRAGMENT), 499, None, 10).unwrap().len(),
        2
    );
}
#[test]
fn claiming_expired_trash_fences_recovery_and_all_versions_until_ack() {
    let (root, ns) = fixture();
    let one = reserve(&ns, "a", 10);
    publish(&ns, &one, 10);
    let two = reserve(&ns, "a", 11);
    publish(&ns, &two, 11);
    let late = reserve(&ns, "a", 12);
    let id = ns.trash(FRAGMENT, "a", 100).unwrap();
    let work = ns.due_trash(None, 100, None, 10).unwrap().remove(0).1;
    assert!(!ns.claim_trash(&work, 99).unwrap());
    assert!(ns.claim_trash(&work, 100).unwrap());
    assert!(ns.recover(FRAGMENT, &id).is_err());
    assert!(
        ns.publish(
            FRAGMENT,
            &late.version_id,
            &late.session_id,
            &ObjectFinalization {
                object_key: upload(12).object_key,
                already_finalized: false,
                total_bytes: 123
            },
            &stat()
        )
        .is_err()
    );
    assert!(ns.due_trash(None, 999, None, 10).unwrap().is_empty());
    assert_eq!(ns.object_blobs(&work, 10).unwrap().len(), 3);
    assert!(!ns.finish_trash(&work, 1).unwrap());
    drop(ns);
    let ns = Namespace::new(MetadataEngine::open(root.path(), crate::Options::default()).unwrap());
    assert_eq!(ns.claimed_trash(None, None, 10).unwrap().len(), 1);
    for blob in ns.object_blobs(&work, 10).unwrap() {
        assert!(ns.prepare_purge(&blob.target, &blob.key).unwrap());
        assert_eq!(
            ns.object_blobs(&work, 10)
                .unwrap()
                .iter()
                .filter(|b| b.key == blob.key)
                .count(),
            1
        );
        assert!(ns.purge_blob(&blob, 1).unwrap());
    }
    assert!(!ns.finish_trash(&work, 1).unwrap());
    assert!(ns.finish_trash(&work, 1).unwrap());
    assert!(ns.trash_page(FRAGMENT, None, 10).unwrap().is_empty());
    assert!(ns.claimed_trash(None, None, 10).unwrap().is_empty());
}
#[test]
fn pins_defer_gc_and_claim_race_has_exactly_one_winner() {
    let (_root, ns) = fixture();
    let handle = reserve(&ns, "a", 10);
    let version = publish(&ns, &handle, 10);
    let target = Target {
        fragment: FRAGMENT,
        object: version.object_id,
        version: version.version_id,
    };
    ns.pin(&target, "export-job").unwrap();
    let id = ns.trash(FRAGMENT, "a", 100).unwrap();
    let work = ns.due_trash(None, 100, None, 10).unwrap().remove(0).1;
    assert!(!ns.claim_trash(&work, 100).unwrap());
    ns.unpin_job("export-job").unwrap();
    let a = ns.clone();
    let b = ns.clone();
    let w = work.clone();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let other = barrier.clone();
    let claim = std::thread::spawn(move || {
        other.wait();
        a.claim_trash(&w, 100).unwrap()
    });
    barrier.wait();
    let recovered = b.recover(FRAGMENT, &id).is_ok();
    let claimed = claim.join().unwrap();
    assert_ne!(recovered, claimed);
    if claimed {
        assert!(ns.pin(&target, "later-export").is_err());
    }
}
#[test]
fn expired_upload_claim_cannot_delete_a_completed_or_live_version() {
    let (_root, ns) = fixture();
    let handle = reserve(&ns, "a", 10);
    publish(&ns, &handle, 10);
    assert!(ns.due_uploads(None, u64::MAX, None, 10).unwrap().is_empty());
    let pending = reserve(&ns, "new", 11);
    let work = ns
        .due_uploads(None, u64::MAX, None, 10)
        .unwrap()
        .remove(0)
        .1;
    assert!(
        ns.claim_upload(&work, work.expires_at - 1)
            .unwrap()
            .is_none()
    );
    let blob = ns.claim_upload(&work, work.expires_at).unwrap().unwrap();
    assert!(ns.pending(FRAGMENT, &pending.version_id, None).is_err());
    assert!(ns.prepare_purge(&blob.target, &blob.key).unwrap());
    assert!(ns.purge_blob(&blob, 1).unwrap());
    assert!(ns.due_uploads(None, u64::MAX, None, 10).unwrap().is_empty());
    assert!(ns.stat(FRAGMENT, "a", None).is_ok());
    assert!(
        ns.engine
            .snapshot()
            .get(Table::Entries, &RecordKey::new(FRAGMENT, "new").unwrap())
            .unwrap()
            .is_none()
    );
}
#[test]
fn migration_switch_is_durable_preserves_version_and_retires_only_old_key() {
    let (root, ns) = fixture();
    let handle = reserve(&ns, "a", 10);
    let before = publish(&ns, &handle, 10);
    let target = Target {
        fragment: FRAGMENT,
        object: before.object_id.clone(),
        version: before.version_id.clone(),
    };
    let source = upload(10).object_key;
    let destination = upload(20);
    let work = MoveWork {
        target: target.clone(),
        source: source.clone(),
        upload: destination.clone(),
        switched: false,
    };
    ns.create_move("job", &work).unwrap();
    ns.trash(FRAGMENT, "a", 100).unwrap();
    let trash = ns.due_trash(None, 100, None, 10).unwrap().remove(0).1;
    assert!(!ns.claim_trash(&trash, 100).unwrap());
    let work = ns.switch_move("job", &target, &stat()).unwrap();
    assert!(work.switched);
    assert_eq!(
        ns.by_identity(FRAGMENT, &before.object_id, &before.version_id)
            .unwrap()
            .object_key,
        destination.object_key
    );
    assert_eq!(
        ns.by_identity(FRAGMENT, &before.object_id, &before.version_id)
            .unwrap()
            .metadata,
        before
    );
    drop(ns);
    let ns = Namespace::new(MetadataEngine::open(root.path(), crate::Options::default()).unwrap());
    assert!(ns.move_work("job", &target).unwrap().unwrap().switched);
    assert!(ns.prepare_purge(&target, &source).unwrap());
    ns.finish_move("job", &work).unwrap();
    assert!(ns.move_work("job", &target).unwrap().is_none());
    assert!(ns.claim_trash(&trash, 100).unwrap());
}
#[test]
fn shared_link_reference_and_concurrent_retirement_are_transactional() {
    let (_root, ns) = fixture();
    let one = reserve(&ns, "a", 10);
    publish(&ns, &one, 10);
    let key = upload(10).object_key;
    ns.engine
        .transact(64, |tx| {
            index_upload(tx, FRAGMENT, &version(999), &version(888), &upload(10))
        })
        .unwrap();
    let target = Target {
        fragment: FRAGMENT,
        object: ns.stat(FRAGMENT, "a", None).unwrap().object_id,
        version: one.version_id,
    };
    assert!(!ns.prepare_purge(&target, &key).unwrap());
    let other = Target {
        fragment: FRAGMENT,
        object: version(999),
        version: version(888),
    };
    assert!(ns.prepare_purge(&other, &key).unwrap());
    assert!(
        ns.engine
            .transact(64, |tx| index_upload(
                tx,
                FRAGMENT,
                &version(1000),
                &version(889),
                &upload(10)
            ))
            .is_err()
    );
}

#[test]
fn idle_gc_seeks_past_ten_thousand_future_records_and_reads_no_namespace() {
    let (_root, ns) = fixture();
    for chunk in 0..100 {
        ns.engine
            .transact(64, |tx| {
                for n in chunk * 100..chunk * 100 + 100 {
                    let work = TrashWork {
                        fragment: FRAGMENT,
                        trash: version(n + 1),
                        object: version(n + 20000),
                        deleted_at: 2000000000,
                    };
                    tx.put(
                        Table::Reclamation,
                        &trash_index(FRAGMENT, &work.trash, work.deleted_at)?,
                        &serde_json::to_vec(&work).unwrap(),
                    )?;
                }
                Ok(())
            })
            .unwrap();
    }
    let started = std::time::Instant::now();
    for _ in 0..1000 {
        assert!(ns.due_trash(None, 1999999999, None, 8).unwrap().is_empty());
    }
    eprintln!(
        "1000 indexed idle passes against 10000 future trash rows: {:?}",
        started.elapsed()
    );
    assert!(
        ns.engine
            .snapshot()
            .scan(Table::Entries, FRAGMENT, "", None, 1)
            .unwrap()
            .is_empty()
    );
}
