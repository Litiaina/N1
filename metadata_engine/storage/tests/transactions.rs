use m1_storage::{Database, Error, Options};
use std::os::unix::fs::PermissionsExt;

fn directory() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
};

fn key(table: u8, index: u64) -> Vec<u8> {
    let mut key = vec![table];
    key.extend_from_slice(format!("fragment/entry/{index:010}").as_bytes());
    key
}

fn options() -> Options {
    Options {
        cache_bytes: 256 * 1024,
        write_buffer_bytes: 1024 * 1024,
        journal_bytes: 2 * 1024 * 1024,
        background_workers: 1,
    }
}

fn scan(db: &Database) -> BTreeMap<Vec<u8>, Vec<u8>> {
    db.snapshot()
        .range(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect::<Result<BTreeMap<_, _>, _>>()
        .unwrap()
}

#[test]
fn disk_tree_matches_ordered_oracle_through_updates_deletes_checkpoint_and_reopen() {
    let directory = directory();
    let db = Database::open(directory.path(), options()).unwrap();
    let mut expected = BTreeMap::new();
    for batch in 0..80u64 {
        let mut tx = db.write().unwrap();
        for item in 0..128 {
            let index = batch * 128 + item;
            let key = key((index % 4) as u8, index);
            let value = vec![(index % 251) as u8; 128 + (index % 1024) as usize];
            tx.put(&key, &value).unwrap();
            expected.insert(key, value);
        }
        tx.commit().unwrap();
    }
    let before = db.snapshot();
    for batch in 0..40u64 {
        let mut tx = db.write().unwrap();
        for item in 0..128 {
            let index = batch * 128 + item;
            let key = key((index % 4) as u8, index);
            if index % 3 == 0 {
                tx.remove(&key).unwrap();
                expected.remove(&key);
            } else {
                let value = index.to_be_bytes();
                tx.put(&key, &value).unwrap();
                expected.insert(key, value.to_vec());
            }
        }
        tx.commit().unwrap();
    }
    assert_eq!(scan(&db), expected);
    assert_eq!(db.verify().unwrap().records as usize, expected.len());
    db.checkpoint().unwrap();
    assert_eq!(scan(&db), expected);
    assert_eq!(db.verify().unwrap().records as usize, expected.len());
    assert_eq!(
        before
            .range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .count(),
        10240
    );
    assert!(db.cached_bytes() <= options().cache_bytes as usize);
    drop(before);
    drop(db);
    let db = Database::open(directory.path(), options()).unwrap();
    assert_eq!(scan(&db), expected);
}

#[test]
fn unrelated_writers_commit_and_shared_conditional_writes_conflict() {
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let start = Arc::new(Barrier::new(12));
    std::thread::scope(|scope| {
        for writer in 0..12u64 {
            let db = db.clone();
            let start = start.clone();
            scope.spawn(move || {
                let mut tx = db.write().unwrap();
                assert!(tx.get(&key(0, writer)).unwrap().is_none());
                tx.put(&key(0, writer), &writer.to_be_bytes()).unwrap();
                start.wait();
                tx.commit().unwrap();
            });
        }
    });
    let start = Arc::new(Barrier::new(12));
    let successful = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for writer in 0..12u64 {
            let db = db.clone();
            let start = start.clone();
            workers.push(scope.spawn(move || {
                let mut tx = db.write().unwrap();
                assert!(tx.get(&key(1, 0)).unwrap().is_none());
                tx.put(&key(1, 0), &writer.to_be_bytes()).unwrap();
                tx.put(&key(2, writer), b"winner").unwrap();
                start.wait();
                match tx.commit() {
                    Ok(()) => 1,
                    Err(Error::Conflict) => 0,
                    other => panic!("{other:?}"),
                }
            }));
        }
        workers.into_iter().map(|w| w.join().unwrap()).sum::<u64>()
    });
    assert_eq!(successful, 1);
    assert_eq!(scan(&db).len(), 14);
}

#[test]
fn read_sets_prevent_write_skew_and_range_phantoms_without_global_false_conflicts() {
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let mut a = db.write().unwrap();
    let mut b = db.write().unwrap();
    a.get(&key(0, 1)).unwrap();
    a.get(&key(0, 2)).unwrap();
    b.get(&key(0, 1)).unwrap();
    b.get(&key(0, 2)).unwrap();
    a.put(&key(0, 1), b"a").unwrap();
    b.put(&key(0, 2), b"b").unwrap();
    a.commit().unwrap();
    assert!(matches!(b.commit(), Err(Error::Conflict)));
    let mut predicate = db.write().unwrap();
    assert!(
        predicate
            .scan(
                Bound::Included(vec![2]),
                Bound::Excluded(vec![3]),
                10,
                4 * 1024 * 1024
            )
            .unwrap()
            .is_empty()
    );
    let mut insertion = db.write().unwrap();
    insertion.put(&key(2, 5), b"new").unwrap();
    insertion.commit().unwrap();
    predicate.put(&key(0, 0), b"derived").unwrap();
    assert!(matches!(predicate.commit(), Err(Error::Conflict)));
    let mut unrelated = db.write().unwrap();
    unrelated
        .scan(
            Bound::Included(vec![3]),
            Bound::Unbounded,
            10,
            4 * 1024 * 1024,
        )
        .unwrap();
    unrelated.put(&key(3, 0), b"fine").unwrap();
    let mut other = db.write().unwrap();
    other.put(&key(1, 0), b"separate").unwrap();
    other.commit().unwrap();
    unrelated.commit().unwrap();
}

#[test]
fn concurrent_readers_keep_cross_table_snapshots_through_online_checkpoints() {
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(5));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let db = db.clone();
            let done = done.clone();
            let start = start.clone();
            scope.spawn(move || {
                start.wait();
                let mut checks = 0;
                while !done.load(Ordering::Acquire) || checks < 100 {
                    let snapshot = db.snapshot();
                    assert_eq!(
                        snapshot.get(&key(0, 0)).unwrap(),
                        snapshot.get(&key(1, 0)).unwrap()
                    );
                    checks += 1;
                }
            });
        }
        start.wait();
        for revision in 0..200u64 {
            let mut tx = db.write().unwrap();
            tx.put(&key(0, 0), &revision.to_be_bytes()).unwrap();
            tx.put(&key(1, 0), &revision.to_be_bytes()).unwrap();
            tx.commit().unwrap();
            if revision % 20 == 0 {
                db.checkpoint().unwrap();
            }
        }
        done.store(true, Ordering::Release);
    });
}

#[test]
fn snapshots_and_iterators_retain_process_lock_and_generation_files() {
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let mut tx = db.write().unwrap();
    tx.put(&key(0, 0), b"old").unwrap();
    tx.commit().unwrap();
    let snapshot = db.snapshot();
    let old = root.path().join("store-00000000000000000001.m1");
    db.checkpoint().unwrap();
    assert!(old.exists());
    let mut rows = snapshot.range(Bound::Unbounded, Bound::Unbounded).unwrap();
    drop(snapshot);
    drop(db);
    assert!(matches!(
        Database::open(root.path(), options()),
        Err(Error::Locked)
    ));
    assert_eq!(rows.next().unwrap().unwrap().1, b"old");
    drop(rows);
    let db = Database::open(root.path(), options()).unwrap();
    assert_eq!(db.snapshot().get(&key(0, 0)).unwrap().unwrap(), b"old");
    assert!(!old.exists());
}

#[test]
fn transaction_reads_own_writes_and_paginated_range_guards_cover_the_observed_prefix() {
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let mut tx = db.write().unwrap();
    for index in 0..20 {
        tx.put(&key(0, index), &[index as u8]).unwrap();
    }
    tx.commit().unwrap();
    let mut tx = db.write().unwrap();
    tx.remove(&key(0, 1)).unwrap();
    tx.put(&key(0, 0), b"replacement").unwrap();
    let rows = tx
        .scan(
            Bound::Included(vec![0]),
            Bound::Excluded(vec![1]),
            3,
            4 * 1024 * 1024,
        )
        .unwrap();
    assert_eq!(
        rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
        [key(0, 0), key(0, 2), key(0, 3)]
    );
    assert_eq!(rows[0].1, b"replacement");
    let mut late = db.write().unwrap();
    late.put(&key(0, 19), b"outside observed page").unwrap();
    late.commit().unwrap();
    tx.commit().unwrap();
    let mut invalid = db.write().unwrap();
    invalid.put(&key(0, 80), b"discard").unwrap();
    assert!(invalid.put(&key(0, 81), &vec![0; 1024 * 1024 + 1]).is_err());
    assert!(invalid.commit().is_err());
    assert!(db.snapshot().get(&key(0, 80)).unwrap().is_none());
}

#[test]
fn shuffled_variable_length_keys_match_a_reference_map_through_churn_and_reopen() {
    let root = directory();
    let mut db = Database::open(root.path(), options()).unwrap();
    let mut expected = BTreeMap::new();
    let mut random = 0x4d31_7374_6f72_6167u64;
    let mut next = || {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random
    };
    let make_key = |index: u64| {
        let mut key = key((index % 4) as u8, index);
        // Exercise both node item and byte limits, including near-limit keys.
        let suffix = [0, 3, 31, 255, 2048, 4000][index as usize % 6];
        key.extend(std::iter::repeat_n((index % 251) as u8, suffix));
        key
    };
    for batch in 0..320 {
        let mut tx = db.write().unwrap();
        for _ in 0..128 {
            let index = next() % 8192;
            let key = make_key(index);
            assert_eq!(tx.get(&key).unwrap(), expected.get(&key).cloned());
            if next() % 3 == 0 {
                tx.remove(&key).unwrap();
                expected.remove(&key);
            } else {
                let value = next().to_be_bytes().repeat((next() % 64 + 1) as usize);
                tx.put(&key, &value).unwrap();
                expected.insert(key, value);
            }
        }
        tx.commit().unwrap();
        if batch % 20 == 0 {
            db.checkpoint().unwrap();
            assert_eq!(scan(&db), expected, "batch={batch}");
            let a = make_key(next() % 8192);
            let b = make_key(next() % 8192);
            let (start, end) = if a < b { (a, b) } else { (b, a) };
            let actual = db
                .snapshot()
                .range(Bound::Included(start.clone()), Bound::Included(end.clone()))
                .unwrap()
                .take(73)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let oracle: Vec<_> = expected
                .range(start..=end)
                .take(73)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            assert_eq!(actual, oracle, "batch={batch}");
        }
        if batch % 80 == 79 {
            drop(db);
            db = Database::open(root.path(), options()).unwrap();
            assert_eq!(scan(&db), expected, "reopen batch={batch}");
        }
    }
    assert_eq!(db.verify().unwrap().records as usize, expected.len());
    assert!(db.cached_bytes() <= options().cache_bytes as usize);
}

#[test]
fn overwriting_a_hot_key_does_not_fill_the_cache_with_obsolete_pages() {
    let root = directory();
    let db = Database::open(
        root.path(),
        Options {
            journal_bytes: 1024 * 1024 * 1024,
            ..options()
        },
    )
    .unwrap();
    let mut tx = db.write().unwrap();
    tx.put(&key(0, 0), b"original").unwrap();
    tx.commit().unwrap();
    let original = db.snapshot();
    for revision in 0..1024u64 {
        let mut tx = db.write().unwrap();
        tx.put(&key(0, 0), &revision.to_be_bytes()).unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(original.get(&key(0, 0)).unwrap().unwrap(), b"original");
    assert_eq!(
        db.snapshot().get(&key(0, 0)).unwrap().unwrap(),
        1023u64.to_be_bytes()
    );
    assert!(db.cached_bytes() < 1024, "cache={}", db.cached_bytes());
}
