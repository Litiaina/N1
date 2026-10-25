use super::*;

#[tokio::test]
async fn inventory_bounds_empty_pages_and_resumes_past_non_candidate_names() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let store = OwnershipStore::from_runtime(&fixture.runtime).unwrap();
    let uploads =
        physical_fragment_root_on(&fixture.root, &fixture.identity.fragment).join(".uploads");
    fs::create_dir_all(&uploads).unwrap();
    for index in 0..513 {
        fs::write(uploads.join(format!("ignored-{index}")), b"not ownership").unwrap();
    }
    // A plausible ULID symlink must not cause traversal outside the worklist.
    std::os::unix::fs::symlink(
        "/path-that-cannot-be-traversed",
        uploads.join(Ulid::new().to_string()),
    )
    .unwrap();
    let mut expected = Vec::new();
    for index in 0..19 {
        let mut identity = fixture.identity.clone();
        identity.version_id = Ulid::from_parts(index + 1, 1).to_string();
        expected.push(identity.version_id.clone());
        store
            .save(&UploadLifecycleRecord {
                identity,
                state: UploadLifecycleState::Open,
                updated_at_unix: 1,
            })
            .unwrap();
    }
    let mut inventory = UploadInventory::new(&fixture.runtime, &fixture.identity.fragment).unwrap();
    let mut found = Vec::new();
    let mut scanned = 0;
    let mut empty_pages = 0;
    loop {
        let page = inventory.next_page(7).await.unwrap();
        assert!(page.records.len() <= 7);
        assert!(page.scanned_entries <= 7);
        scanned += page.scanned_entries;
        empty_pages += usize::from(page.records.is_empty() && !page.finished);
        found.extend(
            page.records
                .into_iter()
                .map(|record| record.identity.version_id),
        );
        if page.finished {
            break;
        }
        assert!(
            scanned < 600,
            "inventory restarted a previously scanned directory"
        );
    }
    found.sort();
    assert_eq!(found, expected);
    assert!(
        empty_pages > 0,
        "non-candidate-only pages do not mean completion"
    );
    assert!(scanned <= 513 + 1 + 19 * 2 + 6);
    // Returned pages retain only a cookie, not an open directory descriptor.
    #[cfg(target_os = "linux")]
    for fd in fs::read_dir("/proc/self/fd").unwrap().flatten() {
        if let Ok(target) = fs::read_link(fd.path()) {
            assert_ne!(target, uploads);
        }
    }
}

#[tokio::test]
async fn inventory_rejects_replaced_directory_before_resuming_cookie() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    let mut inventory = UploadInventory::new(&fixture.runtime, &fixture.identity.fragment).unwrap();
    assert!(!inventory.next_page(1).await.unwrap().finished);
    let uploads =
        physical_fragment_root_on(&fixture.root, &fixture.identity.fragment).join(".uploads");
    let old = uploads.with_extension("inventory-old");
    fs::rename(&uploads, &old).unwrap();
    fs::create_dir(&uploads).unwrap();
    let result = inventory.next_page(1).await;
    fs::remove_dir(&uploads).unwrap();
    fs::rename(old, uploads).unwrap();
    assert!(
        result.is_err(),
        "a cookie cannot resume in a different directory"
    );
}

#[tokio::test]
async fn inventory_detects_conflicting_replicas_before_deduplication() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    let record = UploadLifecycleRecord {
        identity: fixture.identity.clone(),
        state: UploadLifecycleState::Aborted,
        updated_at_unix: 1,
    };
    let bytes = serde_json::to_vec(&StoredRecord {
        format_version: 1,
        sha256: record_checksum(&record).unwrap(),
        record,
    })
    .unwrap();
    fs::write(
        OwnershipStore::path(&fixture.second, &fixture.identity),
        bytes,
    )
    .unwrap();
    let committed = UploadLifecycleRecord {
        identity: fixture.identity.clone(),
        state: UploadLifecycleState::Committed,
        updated_at_unix: 1,
    };
    let bytes = serde_json::to_vec(&StoredRecord {
        format_version: 1,
        sha256: record_checksum(&committed).unwrap(),
        record: committed,
    })
    .unwrap();
    fs::write(
        OwnershipStore::path(&fixture.root, &fixture.identity),
        bytes,
    )
    .unwrap();
    let mut inventory = UploadInventory::new(&fixture.runtime, &fixture.identity.fragment).unwrap();
    assert!(inventory.next_page(1000).await.is_err());
}

#[tokio::test]
async fn inventory_finds_replica_only_work_and_defers_missing_enrolled_root() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    fs::remove_file(OwnershipStore::path(&fixture.root, &fixture.identity)).unwrap();
    let mut inventory = UploadInventory::new(&fixture.runtime, &fixture.identity.fragment).unwrap();
    let mut found = Vec::new();
    loop {
        let page = inventory.next_page(2).await.unwrap();
        found.extend(page.records);
        if page.finished {
            break;
        }
    }
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].identity, fixture.identity);
    let mut inventory = UploadInventory::new(&fixture.runtime, &fixture.identity.fragment).unwrap();
    let moved = fixture.second.with_extension("inventory-offline");
    fs::rename(&fixture.second, &moved).unwrap();
    let result = inventory.next_page(2).await;
    fs::rename(moved, &fixture.second).unwrap();
    assert!(
        result.is_err(),
        "unavailable root is not an empty completed pass"
    );
}

#[tokio::test]
async fn inventory_resume_tolerates_retiring_returned_candidates() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let store = OwnershipStore::from_runtime(&fixture.runtime).unwrap();
    let mut expected = Vec::new();
    for index in 0..41 {
        let mut identity = fixture.identity.clone();
        identity.version_id = Ulid::from_parts(index + 1, 1).to_string();
        expected.push(identity.version_id.clone());
        store
            .save(&UploadLifecycleRecord {
                identity,
                state: UploadLifecycleState::Open,
                updated_at_unix: 1,
            })
            .unwrap();
    }
    let mut inventory = UploadInventory::new(&fixture.runtime, &fixture.identity.fragment).unwrap();
    let mut found = Vec::new();
    loop {
        let page = inventory.next_page(5).await.unwrap();
        for mut record in page.records {
            found.push(record.identity.version_id.clone());
            record.state = UploadLifecycleState::Committed;
            store.save(&record).unwrap();
        }
        if page.finished {
            break;
        }
        assert!(found.len() <= expected.len());
    }
    found.sort();
    assert_eq!(found, expected);
}
