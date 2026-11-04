use super::*;

const FRAGMENT: PoolId = PoolId([23; 16]);
fn version(n: u128) -> String {
    Ulid::from(n).to_string()
}
fn upload(n: u128) -> ObjectUpload {
    ObjectUpload {
        upload_key: format!("N1U1.test-{n}"),
        object_key: format!("N1A1.test-{n}"),
        total_bytes: 123,
        part_bytes: 4096,
        parts: 1,
        expires_at: 9_999_999_999,
        durable: true,
    }
}
fn reserve(namespace: &Namespace, path: &str, n: u128) -> UploadHandle {
    namespace
        .reserve_upload(
            FRAGMENT,
            path,
            &version(n),
            &version(n + 1000),
            &upload(n),
            "application/octet-stream",
            1700000000,
        )
        .unwrap()
}
fn publish(namespace: &Namespace, handle: &UploadHandle, n: u128) -> PublishedVersion {
    let upload = upload(n);
    namespace
        .publish(
            FRAGMENT,
            &handle.version_id,
            &handle.session_id,
            &ObjectFinalization {
                object_key: upload.object_key,
                already_finalized: false,
                total_bytes: 123,
            },
            &stat(),
        )
        .unwrap()
}
fn stat() -> ObjectStat {
    ObjectStat {
        total_bytes: 123,
        data_shards: 1,
        parity_shards: 1,
        finalized_shards: 2,
        integrity_algorithm: "LINK-SHA256-tree-v2".into(),
        integrity_root: "12".repeat(32),
    }
}
fn fixture() -> (tempfile::TempDir, Namespace) {
    let mut builder = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    let root = builder.tempdir().unwrap();
    let db = MetadataEngine::open(root.path(), crate::Options::default()).unwrap();
    (root, Namespace::new(db))
}

#[test]
fn usage_counters_follow_maximum_version_rename_trash_recovery_and_retries() {
    let (_root, namespace) = fixture();
    assert!(namespace.usage(FRAGMENT).unwrap().is_none());
    namespace.recount_usage(FRAGMENT).unwrap();
    let first = reserve(&namespace, "a", 10);
    publish(&namespace, &first, 10);
    assert_eq!(namespace.usage(FRAGMENT).unwrap().unwrap().object_count, 1);
    assert_eq!(namespace.usage(FRAGMENT).unwrap().unwrap().used_bytes, 123);
    publish(&namespace, &first, 10);
    let older = reserve(&namespace, "a", 11);
    let newer = reserve(&namespace, "a", 12);
    publish(&namespace, &newer, 12);
    publish(&namespace, &older, 11);
    assert_eq!(namespace.usage(FRAGMENT).unwrap().unwrap().used_bytes, 123);
    assert_eq!(namespace.usage(FRAGMENT).unwrap().unwrap().object_count, 1);
    namespace.rename(FRAGMENT, "a", "b").unwrap();
    let trash = namespace.trash(FRAGMENT, "b", 1700000001).unwrap();
    assert_eq!(namespace.usage(FRAGMENT).unwrap().unwrap().object_count, 0);
    assert_eq!(namespace.usage(FRAGMENT).unwrap().unwrap().used_bytes, 0);
    namespace.recover(FRAGMENT, &trash).unwrap();
    namespace.recover(FRAGMENT, &trash).unwrap();
    assert_eq!(namespace.usage(FRAGMENT).unwrap().unwrap().object_count, 1);
    assert_eq!(namespace.recount_usage(FRAGMENT).unwrap().used_bytes, 123);
}

#[test]
fn concurrent_usage_counters_match_live_heads_in_every_cross_table_snapshot() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let (_root, namespace) = fixture();
    namespace.recount_usage(FRAGMENT).unwrap();
    let running = Arc::new(AtomicBool::new(true));
    let reader_namespace = namespace.clone();
    let read_running = running.clone();
    let reader = std::thread::spawn(move || {
        let mut checked = 0;
        while read_running.load(Ordering::Acquire) {
            let snapshot = reader_namespace.engine.snapshot();
            let mut actual = Usage::default();
            let mut after = None;
            loop {
                let rows = snapshot
                    .scan(Table::Entries, FRAGMENT, "", after.as_ref(), 1000)
                    .unwrap();
                if rows.is_empty() {
                    break;
                }
                for (key, value) in rows {
                    after = Some(key);
                    let entry: Entry = decode(value).unwrap();
                    if let Some(head) = entry.head {
                        actual.used_bytes += head.total_size_bytes;
                        actual.object_count += 1;
                    }
                }
            }
            let mut counted = Usage::default();
            for (_, value) in snapshot
                .scan(Table::Reclamation, FRAGMENT, "usage/bucket/", None, 256)
                .unwrap()
            {
                let usage: Usage = decode(value).unwrap();
                counted.used_bytes += usage.used_bytes;
                counted.object_count += usage.object_count;
            }
            assert_eq!(actual.used_bytes, counted.used_bytes);
            assert_eq!(actual.object_count, counted.object_count);
            checked += 1;
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        checked
    });
    let mut writers = Vec::new();
    for writer in 0..8u128 {
        let namespace = namespace.clone();
        writers.push(std::thread::spawn(move || {
            for index in 0..64u128 {
                let n = writer * 64 + index + 1;
                let path = format!("objects/{n}");
                let handle = reserve(&namespace, &path, n);
                publish(&namespace, &handle, n);
                // Publication retries, rename and soft deletion must update
                // counts in the same transaction as the visible namespace.
                publish(&namespace, &handle, n);
                let moved = format!("moved/{n}");
                namespace.rename(FRAGMENT, &path, &moved).unwrap();
                if index % 2 == 0 {
                    let trash = namespace.trash(FRAGMENT, &moved, 1700000000).unwrap();
                    if index % 4 == 0 {
                        namespace.recover(FRAGMENT, &trash).unwrap();
                    }
                }
            }
        }));
    }
    for writer in writers {
        writer.join().unwrap();
    }
    running.store(false, Ordering::Release);
    assert!(reader.join().unwrap() > 0);
    let usage = namespace.usage(FRAGMENT).unwrap().unwrap();
    assert_eq!(usage.object_count, 384);
    assert_eq!(usage.used_bytes, 384 * 123);
}

#[test]
fn maximum_ulid_wins_even_when_an_older_upload_finishes_last() {
    let (_root, namespace) = fixture();
    let older = reserve(&namespace, "file", 1);
    let newer = reserve(&namespace, "file", 2);
    assert!(matches!(
        namespace.stat(FRAGMENT, "file", None),
        Err(Error::NotFound)
    ));
    let published = publish(&namespace, &newer, 2);
    let old = publish(&namespace, &older, 1);
    assert_eq!(published.object_id, old.object_id);
    assert_eq!(
        namespace.stat(FRAGMENT, "file", None).unwrap().version_id,
        newer.version_id
    );
    assert_eq!(
        namespace
            .stat(FRAGMENT, "file", Some(&older.version_id))
            .unwrap(),
        old
    );
    assert!(matches!(
        namespace.stat(FRAGMENT, "file", Some(&version(99))),
        Err(Error::NotFound)
    ));
    let response = serde_json::to_string(&published).unwrap();
    assert!(!response.contains("N1A1."));
    assert!(!response.contains("N1U1."));
    assert!(!response.contains("integrity_root"));
}

#[test]
fn concurrent_initial_writers_share_an_identity_without_losing_versions() {
    let (_root, namespace) = fixture();
    let barrier = std::sync::Barrier::new(8);
    let ids = std::thread::scope(|scope| {
        let handles = (1..=8)
            .map(|n| {
                let namespace = &namespace;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let handle = reserve(namespace, "shared", n);
                    publish(namespace, &handle, n).object_id
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(ids.iter().all(|id| id == &ids[0]));
    assert_eq!(
        namespace.stat(FRAGMENT, "shared", None).unwrap().version_id,
        version(8)
    );
    for n in 1..=8 {
        assert_eq!(
            namespace
                .stat(FRAGMENT, "shared", Some(&version(n)))
                .unwrap()
                .version_id,
            version(n)
        );
    }
}

#[test]
fn rename_preserves_version_bytes_and_pending_upload_follows_its_object() {
    let (_root, namespace) = fixture();
    let first = reserve(&namespace, "before", 1);
    let published = publish(&namespace, &first, 1);
    let pending = reserve(&namespace, "before", 2);
    let key = version_key(FRAGMENT, &published.object_id, &first.version_id).unwrap();
    let before = namespace
        .engine
        .snapshot()
        .get(Table::Versions, &key)
        .unwrap();
    namespace.rename(FRAGMENT, "before", "after").unwrap();
    assert!(matches!(
        namespace.stat(FRAGMENT, "before", None),
        Err(Error::NotFound)
    ));
    assert_eq!(namespace.stat(FRAGMENT, "after", None).unwrap(), published);
    assert_eq!(
        namespace
            .engine
            .snapshot()
            .get(Table::Versions, &key)
            .unwrap(),
        before
    );
    publish(&namespace, &pending, 2);
    assert_eq!(
        namespace.stat(FRAGMENT, "after", None).unwrap().version_id,
        version(2)
    );
    assert!(matches!(
        namespace.stat(FRAGMENT, "before", None),
        Err(Error::NotFound)
    ));
}

#[test]
fn trash_is_recoverable_and_old_pending_upload_cannot_resurrect_a_reused_name() {
    let (_root, namespace) = fixture();
    let first = reserve(&namespace, "file", 1);
    let original = publish(&namespace, &first, 1);
    let pending = reserve(&namespace, "file", 2);
    let trash = namespace.trash(FRAGMENT, "file", 1700000001).unwrap();
    assert!(matches!(
        namespace.stat(FRAGMENT, "file", None),
        Err(Error::NotFound)
    ));
    let replacement = reserve(&namespace, "file", 3);
    let replacement = publish(&namespace, &replacement, 3);
    assert_ne!(replacement.object_id, original.object_id);
    let plan = upload(2);
    assert!(matches!(
        namespace.publish(
            FRAGMENT,
            &pending.version_id,
            &pending.session_id,
            &ObjectFinalization {
                object_key: plan.object_key,
                already_finalized: true,
                total_bytes: 123
            },
            &stat()
        ),
        Err(Error::NameConflict(_))
    ));
    assert!(matches!(
        namespace.recover(FRAGMENT, &trash),
        Err(Error::NameConflict(_))
    ));
    let replacement_trash = namespace.trash(FRAGMENT, "file", 1700000002).unwrap();
    namespace.recover(FRAGMENT, &trash).unwrap();
    assert_eq!(namespace.stat(FRAGMENT, "file", None).unwrap(), original);
    namespace.recover(FRAGMENT, &trash).unwrap();
    assert!(matches!(
        namespace.recover(FRAGMENT, &replacement_trash),
        Err(Error::NameConflict(_))
    ));
}

#[test]
fn finalize_retry_is_atomic_and_cannot_rewrite_immutable_version_state() {
    let (_root, namespace) = fixture();
    let handle = reserve(&namespace, "file", 1);
    let first = publish(&namespace, &handle, 1);
    assert_eq!(publish(&namespace, &handle, 1), first);
    let mut wrong = stat();
    wrong.integrity_root = "34".repeat(32);
    assert!(matches!(
        namespace.publish(
            FRAGMENT,
            &handle.version_id,
            &handle.session_id,
            &ObjectFinalization {
                object_key: upload(1).object_key,
                already_finalized: true,
                total_bytes: 123
            },
            &wrong
        ),
        Err(Error::Incompatible(_))
    ));
    assert_eq!(namespace.stat(FRAGMENT, "file", None).unwrap(), first);
    assert!(matches!(
        namespace.pending(
            PoolId([24; 16]),
            &handle.version_id,
            Some(&handle.session_id)
        ),
        Err(Error::NotFound)
    ));
    assert!(matches!(
        namespace.pending(FRAGMENT, &handle.version_id, Some(&version(99))),
        Err(Error::NotFound)
    ));
}

#[test]
fn namespace_and_completion_survive_reopen_without_object_catalogue_in_link() {
    let (root, namespace) = fixture();
    let handle = reserve(&namespace, "a/file", 1);
    let published = publish(&namespace, &handle, 1);
    drop(namespace);
    let namespace =
        Namespace::new(MetadataEngine::open(root.path(), crate::Options::default()).unwrap());
    assert_eq!(namespace.stat(FRAGMENT, "a/file", None).unwrap(), published);
    assert!(
        namespace
            .pending(FRAGMENT, &handle.version_id, None)
            .unwrap()
            .completed
    );
    let page = namespace.list(FRAGMENT, "a/", None, 1).unwrap();
    assert_eq!(page.files.len(), 1);
    assert_eq!(page.files[0].path, "a/file");
    assert!(
        namespace
            .list(FRAGMENT, "a/", page.after.as_deref(), 1)
            .unwrap()
            .files
            .is_empty()
    );
}

#[test]
fn file_and_virtual_prefix_coexist_and_full_length_utf8_keys_remain_valid() {
    let (_root, namespace) = fixture();
    for (path, n) in [("a", 1), ("a/b", 2)] {
        let handle = reserve(&namespace, path, n);
        publish(&namespace, &handle, n);
    }
    let path = "界".repeat(1365) + "a";
    assert_eq!(path.len(), 4096);
    let handle = reserve(&namespace, &path, 3);
    publish(&namespace, &handle, 3);
    assert_eq!(
        namespace.stat(FRAGMENT, &path, None).unwrap().version_id,
        version(3)
    );
    assert_eq!(
        namespace
            .list(FRAGMENT, "a/", None, 100)
            .unwrap()
            .files
            .len(),
        1
    );
    assert_eq!(
        namespace
            .list(FRAGMENT, "a", None, 100)
            .unwrap()
            .files
            .len(),
        2
    );
    assert!(matches!(
        namespace.rename(FRAGMENT, "a", "a/b"),
        Err(Error::NameConflict(_))
    ));
    assert!(namespace.stat(FRAGMENT, "a", None).is_ok());
    assert!(namespace.stat(FRAGMENT, "a/b", None).is_ok());
}

#[test]
fn inherited_path_rules_and_reserved_names_are_preserved() {
    assert_eq!(user_key(" /a//b/ ").unwrap(), "a/b");
    for key in [
        "",
        "/",
        "a/../b",
        "a/./b",
        "a/.uploads/tmp",
        "a/@o_internal",
        "a\0b",
    ] {
        assert!(user_key(key).is_err(), "accepted {key:?}");
    }
    assert!(valid_ulid(&version(31).to_ascii_lowercase()).is_err());
}

#[test]
fn shallow_pagination_seeks_prefixes_without_losing_successor_keys() {
    let (_root, ns) = fixture();
    let keys = [
        "a",
        "a/one",
        "a/two",
        "a/x/deep",
        "a0",
        "a😀/one",
        "a😀0",
        "z",
    ];
    for (index, key) in keys.iter().enumerate() {
        let n = index as u128 + 1;
        let handle = reserve(&ns, key, n);
        publish(&ns, &handle, n);
    }
    let mut paths = Vec::new();
    let mut after = None;
    loop {
        let page = ns.shallow(FRAGMENT, "", "/", after.as_deref(), 1).unwrap();
        paths.extend(page.entries.iter().map(|e| e.path.clone()));
        after = page.after;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(paths, ["a", "a/", "a0", "a😀/", "a😀0", "z"]);
    let children = ns.shallow(FRAGMENT, "a/", "/", None, 10).unwrap();
    assert_eq!(
        children
            .entries
            .iter()
            .map(|e| e.path.as_str())
            .collect::<Vec<_>>(),
        ["a/one", "a/two", "a/x/"]
    );
    assert!(children.after.is_none());
}

#[test]
fn durable_publication_group_rolls_back_every_entry_on_one_bad_object() {
    let (_root, ns) = fixture();
    let reservations = (1..=8)
        .map(|n| Reservation {
            path: format!("group/{n}"),
            version: version(n),
            session: version(n + 1000),
            upload: upload(n),
            content_type: "application/octet-stream".into(),
            created_at: 1700000000,
        })
        .collect::<Vec<_>>();
    let handles = ns.reserve_batch(FRAGMENT, &reservations).unwrap();
    let mut publications = reservations
        .iter()
        .map(|r| Publication {
            version: r.version.clone(),
            session: r.session.clone(),
            finalized: ObjectFinalization {
                object_key: r.upload.object_key.clone(),
                already_finalized: false,
                total_bytes: 123,
            },
            stat: stat(),
        })
        .collect::<Vec<_>>();
    publications[7].stat.total_bytes = 124;
    assert!(matches!(
        ns.publish_batch(FRAGMENT, &publications),
        Err(Error::Incompatible(_))
    ));
    for handle in &handles {
        assert!(matches!(
            ns.stat(FRAGMENT, &handle.object_key, None),
            Err(Error::NotFound)
        ));
        assert!(
            !ns.pending(FRAGMENT, &handle.version_id, None)
                .unwrap()
                .completed
        );
    }
    publications[7].stat = stat();
    let result = ns.publish_batch(FRAGMENT, &publications).unwrap();
    assert_eq!(result.len(), 8);
    assert_eq!(ns.publish_batch(FRAGMENT, &publications).unwrap(), result);
    assert!(
        ns.engine
            .snapshot()
            .scan(Table::Reclamation, FRAGMENT, "pending/", None, 1)
            .unwrap()
            .is_empty()
    );
}
