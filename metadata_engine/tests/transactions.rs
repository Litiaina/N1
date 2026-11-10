use n1_link::PoolId;
mod common;
use m1::{Error, MetadataEngine, Options, RecordKey, Table};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, Ordering},
};

const FRAGMENT: PoolId = PoolId([7; 16]);

#[test]
#[cfg(unix)]
fn database_credentials_are_private_and_symlink_or_shared_roots_are_rejected() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let parent = common::directory(None);
    let private = parent.path().join("private");
    drop(open(&private));
    assert_eq!(
        std::fs::metadata(&private).unwrap().permissions().mode() & 0o077,
        0
    );
    let shared = parent.path().join("shared");
    std::fs::create_dir(&shared).unwrap();
    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(
        MetadataEngine::open(&shared, Options::default()),
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        std::fs::metadata(&shared).unwrap().permissions().mode() & 0o777,
        0o755
    );
    assert!(std::fs::read_dir(&shared).unwrap().next().is_none());
    let alias = parent.path().join("alias");
    symlink(&private, &alias).unwrap();
    assert!(matches!(
        MetadataEngine::open(&alias, Options::default()),
        Err(Error::Invalid(_))
    ));
    assert!(
        open(&private)
            .snapshot()
            .get(Table::Entries, &key("unchanged"))
            .unwrap()
            .is_none()
    );
}

fn key(name: &str) -> RecordKey {
    RecordKey::new(FRAGMENT, name).unwrap()
}
fn open(path: &std::path::Path) -> MetadataEngine {
    MetadataEngine::open(path, Options::default()).unwrap()
}

#[test]
fn atomic_cross_table_commit_rollback_snapshot_and_restart() {
    let directory = common::directory(None);
    {
        let engine = open(directory.path());
        let before = engine.snapshot();
        let mut abandoned = engine.write().unwrap();
        abandoned
            .put(Table::Entries, &key("abandoned"), b"not committed")
            .unwrap();
        drop(abandoned);
        assert!(
            engine
                .snapshot()
                .get(Table::Entries, &key("abandoned"))
                .unwrap()
                .is_none()
        );
        let mut tx = engine.write().unwrap();
        for table in [
            Table::Entries,
            Table::Versions,
            Table::Uploads,
            Table::Reclamation,
        ] {
            tx.put(table, &key("file/a"), b"first").unwrap();
            assert_eq!(tx.get(table, &key("file/a")).unwrap().unwrap(), b"first");
        }
        tx.commit().unwrap();
        assert!(
            before
                .get(Table::Entries, &key("file/a"))
                .unwrap()
                .is_none()
        );
        let first = engine.snapshot();
        engine
            .transact(1, |tx| {
                assert!(tx.compare_exchange(
                    Table::Entries,
                    &key("file/a"),
                    Some(b"first"),
                    Some(b"second")
                )?);
                tx.remove(Table::Uploads, &key("file/a"))
            })
            .unwrap();
        assert_eq!(
            first.get(Table::Entries, &key("file/a")).unwrap().unwrap(),
            b"first"
        );
        assert!(first.get(Table::Uploads, &key("file/a")).unwrap().is_some());
    }
    let engine = open(directory.path());
    assert_eq!(
        engine
            .snapshot()
            .get(Table::Entries, &key("file/a"))
            .unwrap()
            .unwrap(),
        b"second"
    );
    assert_eq!(
        engine
            .snapshot()
            .get(Table::Versions, &key("file/a"))
            .unwrap()
            .unwrap(),
        b"first"
    );
    assert!(
        engine
            .snapshot()
            .get(Table::Uploads, &key("file/a"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn concurrent_conditional_writers_cannot_claim_the_same_name() {
    let directory = common::directory(None);
    let engine = open(directory.path());
    let barrier = Arc::new(Barrier::new(8));
    let results = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for writer in 0..8 {
            let engine = engine.clone();
            let barrier = barrier.clone();
            workers.push(scope.spawn(move || {
                let mut tx = engine.write().unwrap();
                assert!(
                    tx.compare_exchange(Table::Entries, &key("exclusive"), None, Some(&[writer]))
                        .unwrap()
                );
                tx.put(
                    Table::Versions,
                    &key(&format!("writer-{writer}")),
                    &[writer],
                )
                .unwrap();
                barrier.wait();
                tx.commit()
            }));
        }
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(Error::Conflict)))
            .count(),
        7
    );
    assert_eq!(
        engine
            .snapshot()
            .scan(Table::Versions, FRAGMENT, "", None, 100)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn actual_multiwriter_read_modify_write_has_no_lost_updates() {
    let directory = common::directory(None);
    let engine = open(directory.path());
    let barrier = Arc::new(Barrier::new(6));
    std::thread::scope(|scope| {
        for writer in 0..6 {
            let engine = engine.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                for item in 0..64 {
                    engine
                        .transact(1024, |tx| {
                            let count = tx
                                .get(Table::Entries, &key("counter"))?
                                .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
                                .unwrap_or_default();
                            tx.put(Table::Entries, &key("counter"), &(count + 1).to_be_bytes())?;
                            tx.put(
                                Table::Versions,
                                &key(&format!("{writer}/{item:04}")),
                                &count.to_be_bytes(),
                            )
                        })
                        .unwrap();
                }
            });
        }
    });
    assert_eq!(
        engine
            .snapshot()
            .get(Table::Entries, &key("counter"))
            .unwrap()
            .unwrap(),
        384u64.to_be_bytes()
    );
    assert_eq!(
        engine
            .snapshot()
            .scan(Table::Versions, FRAGMENT, "", None, 1000)
            .unwrap()
            .len(),
        384
    );
}

#[test]
fn snapshots_never_mix_related_records_during_concurrent_mutation() {
    let directory = common::directory(None);
    let engine = open(directory.path());
    let done = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(5));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let engine = engine.clone();
            let done = done.clone();
            let start = start.clone();
            scope.spawn(move || {
                start.wait();
                let mut checks = 0;
                while !done.load(Ordering::Acquire) || checks < 100 {
                    let view = engine.snapshot();
                    assert_eq!(
                        view.get(Table::Entries, &key("same")).unwrap(),
                        view.get(Table::Versions, &key("same")).unwrap()
                    );
                    checks += 1;
                }
            });
        }
        start.wait();
        for revision in 0u64..256 {
            engine
                .transact(1, |tx| {
                    tx.put(Table::Entries, &key("same"), &revision.to_be_bytes())?;
                    tx.put(Table::Versions, &key("same"), &revision.to_be_bytes())
                })
                .unwrap();
        }
        done.store(true, Ordering::Release);
    });
}

#[test]
fn range_predicates_detect_phantoms_and_dont_reject_disjoint_writes() {
    let directory = common::directory(None);
    let engine = open(directory.path());
    let mut predicate = engine.write().unwrap();
    assert!(
        predicate
            .scan(Table::Entries, FRAGMENT, "folder/", None, 10)
            .unwrap()
            .is_empty()
    );
    engine
        .transact(1, |tx| tx.put(Table::Entries, &key("folder/new"), b"new"))
        .unwrap();
    predicate
        .put(Table::Entries, &key("derived"), b"empty")
        .unwrap();
    assert!(matches!(predicate.commit(), Err(Error::Conflict)));
    let mut a = engine.write().unwrap();
    let mut b = engine.write().unwrap();
    assert!(a.get(Table::Entries, &key("a")).unwrap().is_none());
    assert!(b.get(Table::Entries, &key("b")).unwrap().is_none());
    a.put(Table::Entries, &key("a"), b"a").unwrap();
    b.put(Table::Entries, &key("b"), b"b").unwrap();
    a.commit().unwrap();
    b.commit().unwrap();
}

#[test]
fn exact_utf8_keys_pagination_and_fragment_boundaries() {
    let directory = common::directory(None);
    let engine = open(directory.path());
    engine
        .transact(1, |tx| {
            for name in [" tree/a", "tree/a", "tree/ab", "tree/b", "tree/é", "tree0"] {
                tx.put(Table::Entries, &key(name), name.as_bytes())?;
            }
            tx.put(
                Table::Entries,
                &RecordKey::new(PoolId([8; 16]), "tree/a")?,
                b"other",
            )
        })
        .unwrap();
    let view = engine.snapshot();
    let page = view
        .scan(Table::Entries, FRAGMENT, "tree/", None, 2)
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|(key, _)| key.identifier())
            .collect::<Vec<_>>(),
        ["tree/a", "tree/ab"]
    );
    let rest = view
        .scan(Table::Entries, FRAGMENT, "tree/", Some(&page[1].0), 2)
        .unwrap();
    assert_eq!(
        rest.iter()
            .map(|(key, _)| key.identifier())
            .collect::<Vec<_>>(),
        ["tree/b", "tree/é"]
    );
    assert!(
        view.scan(
            Table::Entries,
            FRAGMENT,
            "tree/",
            Some(&key("unrelated")),
            2
        )
        .is_err()
    );
    assert!(view.scan(Table::Entries, FRAGMENT, "", None, 1001).is_err());
    assert!(RecordKey::new(FRAGMENT, "bad\0key").is_err());
    assert!(RecordKey::new(FRAGMENT, "x".repeat(4097)).is_err());
    let bad = serde_json::json!({"fragment":FRAGMENT, "identifier":"bad\0key"});
    assert!(serde_json::from_value::<RecordKey>(bad).is_err());
    assert_eq!(
        serde_json::from_value::<RecordKey>(serde_json::to_value(key("a/b")).unwrap()).unwrap(),
        key("a/b")
    );
}

#[test]
fn rejects_foreign_directories_and_keeps_process_lock_until_last_owner() {
    let directory = common::directory(None);
    std::fs::write(directory.path().join("unrelated"), b"keep").unwrap();
    assert!(matches!(
        MetadataEngine::open(directory.path(), Options::default()),
        Err(Error::Incompatible(_))
    ));
    assert_eq!(
        std::fs::read(directory.path().join("unrelated")).unwrap(),
        b"keep"
    );
    let directory = common::directory(None);
    let engine = open(directory.path());
    let view = engine.snapshot();
    drop(engine);
    assert!(matches!(
        MetadataEngine::open(directory.path(), Options::default()),
        Err(Error::Storage(m1_storage::Error::Locked))
    ));
    drop(view);
    drop(open(directory.path()));
}

#[test]
fn format_mismatch_is_rejected_and_not_rewritten() {
    let directory = common::directory(None);
    drop(open(directory.path()));
    let path = directory.path().join("FORMAT");
    let mut invalid = std::fs::read(&path).unwrap();
    invalid[8..12].copy_from_slice(&999u32.to_be_bytes());
    let crc = crc32c::crc32c(&invalid[..28]);
    invalid[28..32].copy_from_slice(&crc.to_be_bytes());
    std::fs::write(&path, &invalid).unwrap();
    assert!(matches!(
        MetadataEngine::open(directory.path(), Options::default()),
        Err(Error::Incompatible(_))
    ));
    assert_eq!(std::fs::read(path).unwrap(), invalid);
}

#[test]
fn initialized_database_never_silently_recreates_a_missing_generation() {
    let parent = common::directory(None);
    let path = parent.path().join("db");
    let db = open(&path);
    db.transact(1, |tx| tx.put(Table::Versions, &key("existing"), b"keep"))
        .unwrap();
    drop(db);
    let generation = path.join("store-00000000000000000001.m1");
    let saved = parent.path().join("saved-generation");
    std::fs::rename(&generation, &saved).unwrap();
    assert!(MetadataEngine::open(&path, Options::default()).is_err());
    assert!(!generation.exists());
    std::fs::rename(saved, generation).unwrap();
    assert_eq!(
        open(&path)
            .snapshot()
            .get(Table::Versions, &key("existing"))
            .unwrap()
            .unwrap(),
        b"keep"
    );
}

#[test]
fn incomplete_creation_can_finish_but_missing_format_with_data_is_rejected() {
    let directory = common::directory(None);
    drop(open(directory.path()));
    // Complete immutable FORMAT and an empty first generation are sufficient
    // to finish a creation interrupted before CURRENT publication.
    std::fs::remove_file(directory.path().join("CURRENT")).unwrap();
    let db = open(directory.path());
    db.transact(1, |tx| {
        tx.put(Table::Entries, &key("existing"), b"preserve")
    })
    .unwrap();
    drop(db);
    std::fs::remove_file(directory.path().join("FORMAT")).unwrap();
    assert!(matches!(
        MetadataEngine::open(directory.path(), Options::default()),
        Err(Error::Incompatible(_))
    ));
    assert!(!directory.path().join("FORMAT").exists());
}

#[test]
fn missing_current_with_committed_data_fails_without_reinitializing() {
    let directory = common::directory(None);
    let db = open(directory.path());
    db.transact(1, |tx| {
        tx.put(Table::Entries, &key("existing"), b"preserve")
    })
    .unwrap();
    drop(db);
    let generation = directory.path().join("store-00000000000000000001.m1");
    let before = std::fs::read(&generation).unwrap();
    std::fs::remove_file(directory.path().join("CURRENT")).unwrap();
    assert!(MetadataEngine::open(directory.path(), Options::default()).is_err());
    assert_eq!(std::fs::read(generation).unwrap(), before);
    assert!(!directory.path().join("CURRENT").exists());
}

#[test]
fn oversize_write_taints_transaction_and_page_byte_limit_preserves_continuation() {
    let directory = common::directory(None);
    let engine = open(directory.path());
    let mut tx = engine.write().unwrap();
    tx.put(Table::Entries, &key("partial"), b"must roll back")
        .unwrap();
    assert!(
        tx.put(Table::Entries, &key("too-large"), &vec![0; 1024 * 1024 + 1])
            .is_err()
    );
    assert!(matches!(tx.commit(), Err(Error::Invalid(_))));
    assert!(
        engine
            .snapshot()
            .get(Table::Entries, &key("partial"))
            .unwrap()
            .is_none()
    );
    engine
        .transact(1, |tx| {
            for index in 0..5 {
                tx.put(
                    Table::Entries,
                    &key(&format!("large/{index}")),
                    &vec![index; 1024 * 1024],
                )?;
            }
            Ok(())
        })
        .unwrap();
    let view = engine.snapshot();
    let page = view
        .scan(Table::Entries, FRAGMENT, "large/", None, 1000)
        .unwrap();
    assert_eq!(page.len(), 3);
    let rest = view
        .scan(
            Table::Entries,
            FRAGMENT,
            "large/",
            Some(&page.last().unwrap().0),
            1000,
        )
        .unwrap();
    assert_eq!(rest.len(), 2);
    assert_eq!(rest[0].0.identifier(), "large/3");
    assert_eq!(rest[1].0.identifier(), "large/4");
}
