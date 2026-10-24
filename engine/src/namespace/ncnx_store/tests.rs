use super::*;
use chrono::Utc;

fn store() -> (PathBuf, NcnxStore) {
    let root = std::env::temp_dir().join(format!("n2-ncnx-{}", Ulid::new()));
    (root.clone(), NcnxStore::new("root-a", root))
}

fn record(fragment: &str, object: &str) -> NcnxRecord {
    NcnxRecord::new(
        fragment,
        object,
        Ulid::new().to_string(),
        Utc::now().timestamp_nanos_opt().unwrap(),
    )
    .unwrap()
}

#[test]
fn ncnx_integrity_rejects_corruption() {
    let record = record("fragment-a", "object-a");
    let mut bytes = record.encode().unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x80;
    assert!(NcnxRecord::decode(&bytes).is_err());
}

#[test]
fn immutable_create_is_idempotent_and_rejects_other_pointer() {
    let (root, store) = store();
    let first = record("fragment-a", "object-a").encode().unwrap();
    let second = record("fragment-a", "object-b").encode().unwrap();
    assert!(
        store
            .create_live("fragment-a", "videos/a.mp4", &first)
            .unwrap()
    );
    assert!(
        !store
            .create_live("fragment-a", "videos/a.mp4", &first)
            .unwrap()
    );
    assert!(
        store
            .create_live("fragment-a", "videos/a.mp4", &second)
            .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn delayed_repair_cannot_overwrite_a_recreated_live_generation() {
    let (root, store) = store();
    let old = record("fragment-a", "object-a").encode().unwrap();
    // Even the same object ID can have a different namespace generation.
    let replacement = record("fragment-a", "object-a").encode().unwrap();
    store.create_live("fragment-a", "old/key", &old).unwrap();
    store.create_live("fragment-a", "new/key", &old).unwrap();
    store
        .remove_live_if_exact("fragment-a", "old/key", &old)
        .unwrap();
    store
        .create_live("fragment-a", "old/key", &replacement)
        .unwrap();
    assert!(matches!(
        store.repair_live("fragment-a", "old/key", &old),
        Err(AppError::Conflict(_))
    ));
    assert_eq!(
        store.read_live("fragment-a", "old/key").unwrap().unwrap().1,
        replacement
    );
    assert_eq!(
        store.read_live("fragment-a", "new/key").unwrap().unwrap().1,
        old
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn delayed_repair_cannot_overwrite_a_batched_recreation_or_trash_generation() {
    let (root, store) = store();
    let old = record("fragment-a", "object-a").encode().unwrap();
    let replacement = record("fragment-a", "object-b").encode().unwrap();
    let entries = vec![("old/key".to_string(), old.clone())];
    store.create_live_batch("fragment-a", &entries).unwrap();
    let locator = TrashLocator {
        logical_key: "old/key".into(),
        deleted_at_unix_nanos: 1,
        delete_mutation_id: Ulid::new().to_string(),
    };
    store
        .create_trash_batch("fragment-a", &[(locator.clone(), old.clone())])
        .unwrap();
    store
        .remove_live_batch_if_exact("fragment-a", &entries)
        .unwrap();
    store
        .create_live_batch("fragment-a", &[("old/key".into(), replacement.clone())])
        .unwrap();
    assert!(store.repair_live("fragment-a", "old/key", &old).is_err());
    // A valid trash generation is just as immutable as a live pointer.
    assert!(
        store
            .repair_trash("fragment-a", &locator, &replacement)
            .is_err()
    );
    assert_eq!(
        store.read_live("fragment-a", "old/key").unwrap().unwrap().1,
        replacement
    );
    assert_eq!(
        store.read_trash("fragment-a", &locator).unwrap().unwrap().1,
        old
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn repair_still_restores_missing_and_corrupt_replicas_and_is_idempotent() {
    let (root, store) = store();
    let bytes = record("fragment-a", "object-a").encode().unwrap();
    assert!(store.repair_live("fragment-a", "key", &bytes).unwrap());
    assert!(!store.repair_live("fragment-a", "key", &bytes).unwrap());
    let path = store.live_path("fragment-a", "key").unwrap();
    fs::write(&path, b"corrupt namespace record").unwrap();
    assert!(store.repair_live("fragment-a", "key", &bytes).unwrap());
    assert_eq!(
        store.read_live("fragment-a", "key").unwrap().unwrap().1,
        bytes
    );
    let wrong_fragment = record("fragment-b", "object-a").encode().unwrap();
    assert!(
        store
            .repair_live("fragment-a", "other", &wrong_fragment)
            .is_err()
    );
    assert!(store.read_live("fragment-a", "other").unwrap().is_none());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn batched_live_create_requires_exact_idempotence_and_preflights_conflicts() {
    let (root, store) = store();
    let first = record("fragment-a", "object-a");
    let same_object_new_generation = record("fragment-a", "object-a");
    let conflicting = record("fragment-a", "object-b");
    let entries = vec![
        ("new/a.mp4".to_string(), first.encode().unwrap()),
        (
            "new/b.mp4".to_string(),
            record("fragment-a", "object-c").encode().unwrap(),
        ),
    ];
    assert_eq!(store.create_live_batch("fragment-a", &entries).unwrap(), 2);
    assert_eq!(store.create_live_batch("fragment-a", &entries).unwrap(), 0);
    assert!(
        store
            .create_live_batch(
                "fragment-a",
                &[(
                    "new/a.mp4".to_string(),
                    same_object_new_generation.encode().unwrap(),
                )],
            )
            .is_err()
    );

    let rejected = vec![
        ("new/a.mp4".to_string(), conflicting.encode().unwrap()),
        (
            "new/never-published.mp4".to_string(),
            record("fragment-a", "object-d").encode().unwrap(),
        ),
    ];
    assert!(store.create_live_batch("fragment-a", &rejected).is_err());
    assert!(
        store
            .read_live("fragment-a", "new/never-published.mp4")
            .unwrap()
            .is_none()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn batched_exact_remove_never_removes_a_changed_pointer() {
    let (root, store) = store();
    let first = record("fragment-a", "object-a").encode().unwrap();
    let second = record("fragment-a", "object-b").encode().unwrap();
    store
        .create_live_batch(
            "fragment-a",
            &[
                ("old/a.mp4".to_string(), first.clone()),
                ("old/b.mp4".to_string(), second.clone()),
            ],
        )
        .unwrap();

    assert!(
        store
            .remove_live_batch_if_exact(
                "fragment-a",
                &[
                    ("old/a.mp4".to_string(), first.clone()),
                    ("old/b.mp4".to_string(), first.clone()),
                ],
            )
            .is_err()
    );
    assert!(
        store
            .read_live("fragment-a", "old/a.mp4")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .read_live("fragment-a", "old/b.mp4")
            .unwrap()
            .unwrap()
            .1,
        second
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn exact_remove_never_deletes_another_pointer() {
    let (root, store) = store();
    let first = record("fragment-a", "object-a").encode().unwrap();
    let other = record("fragment-a", "object-b").encode().unwrap();
    store
        .create_live("fragment-a", "videos/a.mp4", &first)
        .unwrap();
    assert!(
        store
            .remove_live_if_exact("fragment-a", "videos/a.mp4", &other)
            .is_err()
    );
    assert_eq!(
        store
            .read_live("fragment-a", "videos/a.mp4")
            .unwrap()
            .unwrap()
            .1,
        first
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn destination_first_copy_delete_is_restart_safe_at_each_stage() {
    let (root, store) = store();
    let bytes = record("fragment-a", "object-a").encode().unwrap();
    let trash = TrashLocator {
        logical_key: "old/a.mp4".into(),
        deleted_at_unix_nanos: 1_234_567,
        delete_mutation_id: Ulid::new().to_string(),
    };
    store
        .create_live("fragment-a", "old/a.mp4", &bytes)
        .unwrap();
    store
        .create_live("fragment-a", "new/a.mp4", &bytes)
        .unwrap();
    assert!(
        store
            .read_live("fragment-a", "old/a.mp4")
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .read_live("fragment-a", "new/a.mp4")
            .unwrap()
            .is_some()
    );

    store.create_trash("fragment-a", &trash, &bytes).unwrap();
    assert!(store.read_trash("fragment-a", &trash).unwrap().is_some());
    store
        .remove_live_if_exact("fragment-a", "old/a.mp4", &bytes)
        .unwrap();
    assert!(
        store
            .read_live("fragment-a", "old/a.mp4")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_live("fragment-a", "new/a.mp4")
            .unwrap()
            .is_some()
    );
    assert!(store.read_trash("fragment-a", &trash).unwrap().is_some());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn batched_trash_and_recovery_keep_copy_before_delete_invariants() {
    let (root, store) = store();
    let first = record("fragment-a", "object-a").encode().unwrap();
    let second = record("fragment-a", "object-b").encode().unwrap();
    let first_trash = TrashLocator {
        logical_key: "photos/a.jpg".into(),
        deleted_at_unix_nanos: 10_000,
        delete_mutation_id: Ulid::new().to_string(),
    };
    let second_trash = TrashLocator {
        logical_key: "photos/b.jpg".into(),
        deleted_at_unix_nanos: 10_000,
        delete_mutation_id: Ulid::new().to_string(),
    };
    let live = vec![
        (first_trash.logical_key.clone(), first.clone()),
        (second_trash.logical_key.clone(), second.clone()),
    ];
    let trash = vec![
        (first_trash.clone(), first.clone()),
        (second_trash.clone(), second.clone()),
    ];
    store.create_live_batch("fragment-a", &live).unwrap();

    // Destination publication is independently durable and idempotent;
    // a restart here leaves both names rather than losing either object.
    assert_eq!(store.create_trash_batch("fragment-a", &trash).unwrap(), 2);
    assert_eq!(store.create_trash_batch("fragment-a", &trash).unwrap(), 0);
    assert!(
        store
            .read_live("fragment-a", &first_trash.logical_key)
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .read_trash("fragment-a", &first_trash)
            .unwrap()
            .is_some()
    );

    store
        .remove_live_batch_if_exact("fragment-a", &live)
        .unwrap();
    assert!(
        store
            .read_live("fragment-a", &first_trash.logical_key)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_trash("fragment-a", &first_trash)
            .unwrap()
            .is_some()
    );

    // Recovery uses the same invariant in reverse: recreate all live
    // pointers first, then remove only the exact trash generations.
    store.create_live_batch("fragment-a", &live).unwrap();
    store
        .remove_trash_batch_if_exact("fragment-a", &trash)
        .unwrap();
    assert!(
        store
            .read_live("fragment-a", &second_trash.logical_key)
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .read_trash("fragment-a", &second_trash)
            .unwrap()
            .is_none()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn batched_trash_remove_never_deletes_a_changed_generation() {
    let (root, store) = store();
    let first = record("fragment-a", "object-a").encode().unwrap();
    let second = record("fragment-a", "object-b").encode().unwrap();
    let locator_a = TrashLocator {
        logical_key: "trash/a.bin".into(),
        deleted_at_unix_nanos: 20_000,
        delete_mutation_id: Ulid::new().to_string(),
    };
    let locator_b = TrashLocator {
        logical_key: "trash/b.bin".into(),
        deleted_at_unix_nanos: 20_000,
        delete_mutation_id: Ulid::new().to_string(),
    };
    store
        .create_trash_batch(
            "fragment-a",
            &[
                (locator_a.clone(), first.clone()),
                (locator_b.clone(), second.clone()),
            ],
        )
        .unwrap();

    assert!(
        store
            .remove_trash_batch_if_exact(
                "fragment-a",
                &[
                    (locator_a.clone(), first),
                    (
                        locator_b.clone(),
                        record("fragment-a", "other").encode().unwrap()
                    ),
                ],
            )
            .is_err()
    );
    assert!(
        store
            .read_trash("fragment-a", &locator_a)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .read_trash("fragment-a", &locator_b)
            .unwrap()
            .unwrap()
            .1,
        second
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn trash_generation_is_zero_decode_and_collision_free() {
    let (root, store) = store();
    let mutation = Ulid::new().to_string();
    let locator = TrashLocator {
        logical_key: "videos/a.mp4".into(),
        deleted_at_unix_nanos: 1_234_567,
        delete_mutation_id: mutation.clone(),
    };
    let bytes = record("fragment-a", "object-a").encode().unwrap();
    store.create_trash("fragment-a", &locator, &bytes).unwrap();
    assert_eq!(
        store
            .trash_path("fragment-a", "videos/a.mp4", 1_234_567, &mutation,)
            .unwrap()
            .strip_prefix(store.trash_root("fragment-a"))
            .unwrap(),
        Path::new(&format!("1234567.{mutation}/videos/a.mp4.ncnx"))
    );
    let listed = store
        .list_trash_names("fragment-a", "videos/", None, 10)
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].trash.as_ref(), Some(&locator));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn repeated_delete_generations_of_the_same_key_page_without_collision() {
    let (root, store) = store();
    let first = TrashLocator {
        logical_key: "videos/a.mp4".into(),
        deleted_at_unix_nanos: 1_000,
        delete_mutation_id: Ulid::new().to_string(),
    };
    let second = TrashLocator {
        logical_key: "videos/a.mp4".into(),
        deleted_at_unix_nanos: 2_000,
        delete_mutation_id: Ulid::new().to_string(),
    };
    let bytes = record("fragment-a", "object-a").encode().unwrap();
    store.create_trash("fragment-a", &second, &bytes).unwrap();
    store.create_trash("fragment-a", &first, &bytes).unwrap();

    let first_page = store
        .list_trash_names("fragment-a", "videos/", None, 1)
        .unwrap();
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].trash.as_ref(), Some(&first));
    let cursor = format!(
        "{}\0{:020}\0{}",
        first.logical_key, first.deleted_at_unix_nanos, first.delete_mutation_id
    );
    let second_page = store
        .list_trash_names("fragment-a", "videos/", Some(&cursor), 1)
        .unwrap();
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].trash.as_ref(), Some(&second));
    assert!(
        store
            .list_trash_names(
                "fragment-a",
                "videos/",
                Some(&format!(
                    "{}\0{:020}\0{}",
                    second.logical_key, second.deleted_at_unix_nanos, second.delete_mutation_id
                )),
                1,
            )
            .unwrap()
            .is_empty()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn one_prefix_delete_generation_contains_unchanged_logical_paths() {
    let (root, store) = store();
    let mutation = Ulid::new().to_string();
    let bytes = record("fragment-a", "object-a").encode().unwrap();
    for logical_key in ["videos/a.mp4", "videos/nested/b.mp4"] {
        store
            .create_trash(
                "fragment-a",
                &TrashLocator {
                    logical_key: logical_key.into(),
                    deleted_at_unix_nanos: 3_000,
                    delete_mutation_id: mutation.clone(),
                },
                &bytes,
            )
            .unwrap();
    }
    let generation = store
        .trash_root("fragment-a")
        .join(format!("3000.{mutation}"));
    assert!(generation.join("videos/a.mp4.ncnx").is_file());
    assert!(generation.join("videos/nested/b.mp4.ncnx").is_file());
    assert_eq!(
        store
            .list_trash_names("fragment-a", "videos/", None, 10)
            .unwrap()
            .into_iter()
            .map(|item| item.logical_key)
            .collect::<Vec<_>>(),
        ["videos/a.mp4", "videos/nested/b.mp4"]
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn removing_last_trash_record_prunes_its_generation() {
    let (root, store) = store();
    let locator = TrashLocator {
        logical_key: "videos/nested/a.mp4".into(),
        deleted_at_unix_nanos: 4_000,
        delete_mutation_id: Ulid::new().to_string(),
    };
    let bytes = record("fragment-a", "object-a").encode().unwrap();
    store.create_trash("fragment-a", &locator, &bytes).unwrap();
    let generation = store.trash_root("fragment-a").join(format!(
        "{}.{}",
        locator.deleted_at_unix_nanos, locator.delete_mutation_id
    ));
    assert!(generation.is_dir());
    assert!(
        store
            .remove_trash_if_exact("fragment-a", &locator, &bytes)
            .unwrap()
    );
    assert!(!generation.exists());
    assert!(store.trash_root("fragment-a").is_dir());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn malformed_or_legacy_trash_layout_fails_closed() {
    let (root, store) = store();
    let legacy = store
        .trash_root("fragment-a")
        .join("videos/a.mp4.ncnx.trashed.1000.01ARZ3NDEKTSV4RRFFQ69G5FAV");
    fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    fs::write(legacy, b"legacy").unwrap();
    assert!(store.list_trash_names("fragment-a", "", None, 10).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn live_listing_uses_names_without_opening_records() {
    let (root, store) = store();
    let path = store.live_path("fragment-a", "videos/broken.mp4").unwrap();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, b"deliberately-not-messagepack").unwrap();
    let listed = store
        .list_live_names("fragment-a", "videos/", None, 10)
        .unwrap();
    assert_eq!(listed[0].logical_key, "videos/broken.mp4");
    assert!(store.read_live("fragment-a", "videos/broken.mp4").is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn lockless_listing_treats_a_concurrently_pruned_branch_as_empty() {
    let root = std::env::temp_dir().join(format!("n2-ncnx-pruned-{}", Ulid::new()));
    let branch = root.join("generation/videos");
    fs::create_dir_all(&branch).unwrap();

    // The parent walk may already hold this path when another GC removes
    // the final NCNX and prunes the branch.
    fs::remove_dir_all(root.join("generation")).unwrap();
    let entries = list_names(&branch, "", None, false, 10).unwrap();
    assert!(entries.is_empty());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn live_listing_pages_exact_keys_and_same_name_prefixes_in_logical_order() {
    let (root, store) = store();
    let expected = ["a", "a-previous", "a/child", "a/child/deep", "a0"];
    for key in expected {
        let path = store.live_path("fragment-a", key).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, b"listing-does-not-decode-this").unwrap();
    }

    let mut actual = Vec::new();
    let mut after = None;
    loop {
        let page = store
            .list_live_names("fragment-a", "a", after.as_deref(), 1)
            .unwrap();
        let Some(item) = page.into_iter().next() else {
            break;
        };
        after = Some(item.logical_key.clone());
        actual.push(item.logical_key);
    }
    assert_eq!(actual, expected);

    let subtree = store
        .list_live_names("fragment-a", "a/", None, 10)
        .unwrap()
        .into_iter()
        .map(|item| item.logical_key)
        .collect::<Vec<_>>();
    assert_eq!(subtree, ["a/child", "a/child/deep"]);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn key_validation_reserves_engine_components() {
    assert!(validate_logical_key("videos/a.mp4").is_ok());
    assert!(validate_logical_key("videos/.trash/a.mp4").is_err());
    assert!(validate_logical_key("../a.mp4").is_err());
    assert!(validate_logical_key("").is_err());
}
