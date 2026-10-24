use super::*;
use std::fs;
use std::sync::Barrier;
use std::time::{Duration, SystemTime};

fn fragment() -> String {
    // Activity is keyed by stable fragment identity. Use one directly so
    // these tests do not depend on another module bootstrapping alias roots.
    ulid::Ulid::new().to_string()
}

fn assert_released(fragment: &str) {
    assert!(
        !REGISTRY.lock().unwrap().contains_key(fragment),
        "idle activity state leaked"
    );
}

#[test]
fn scans_protect_uploads_before_and_after_snapshot_even_after_completion() {
    let fragment = fragment();
    let early = UploadActivityGuard::begin(&fragment, "early").unwrap();
    let scan = UploadGcScan::begin(&fragment).unwrap();
    drop(early);
    drop(UploadActivityGuard::begin(&fragment, "late").unwrap());
    assert!(scan.try_reclaim("early").unwrap().is_none());
    assert!(scan.try_reclaim("late").unwrap().is_none());
    assert!(scan.try_reclaim("unrelated-orphan").unwrap().is_some());
    drop(scan);
    let next = UploadGcScan::begin(&fragment).unwrap();
    assert!(next.try_reclaim("late").unwrap().is_some());
    drop(next);
    assert_released(&fragment);
}

#[test]
fn old_scan_cannot_delete_new_upload_even_with_zero_ttl_and_clock_rollback() {
    let fragment = fragment();
    let scan = UploadGcScan::begin(&fragment).unwrap();
    // A collector has already observed no namespace entry for this object.
    let guard = UploadActivityGuard::begin(&fragment, "new-object").unwrap();
    let base = std::env::temp_dir().join(&fragment);
    let version = base.join(ulid::Ulid::new().to_string());
    fs::create_dir_all(&version).unwrap();
    fs::write(version.join("shard"), b"must survive").unwrap();
    let now = SystemTime::now();
    fs::File::open(&version)
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(now - Duration::from_secs(86_400)))
        .unwrap();
    drop(guard); // Finished after the namespace snapshot; still protected.
    if let Some(_reclaim) = scan.try_reclaim("new-object").unwrap() {
        crate::core::garbage_collector::reclaim_object_versions(
            &base,
            &HashSet::new(),
            now,
            Duration::ZERO,
            || true,
        )
        .unwrap();
    }
    assert_eq!(fs::read(version.join("shard")).unwrap(), b"must survive");
    drop(scan);
    // This fixture intentionally never committed. A fresh scan can reclaim it.
    let next = UploadGcScan::begin(&fragment).unwrap();
    let reclaim = next.try_reclaim("new-object").unwrap().unwrap();
    crate::core::garbage_collector::reclaim_object_versions(
        &base,
        &HashSet::new(),
        now,
        Duration::ZERO,
        || true,
    )
    .unwrap();
    assert!(!version.exists());
    drop(next); // Also exercise a reclamation guard outliving its scan.
    drop(reclaim);
    assert_released(&fragment);
}

#[test]
fn orphan_fence_excludes_only_its_object_and_survives_overlapping_scans() {
    let fragment = fragment();
    let first = UploadGcScan::begin(&fragment).unwrap();
    let second = UploadGcScan::begin(&fragment).unwrap();
    let reclaim = first.try_reclaim("expired").unwrap().unwrap();
    assert!(matches!(
        UploadActivityGuard::begin(&fragment, "expired"),
        Err(AppError::Conflict(_))
    ));
    assert!(second.try_reclaim("expired").unwrap().is_none());
    drop(UploadActivityGuard::begin(&fragment, "other").unwrap());
    assert!(first.try_reclaim("other").unwrap().is_none());
    assert!(second.try_reclaim("other").unwrap().is_none());
    drop(reclaim);
    drop(UploadActivityGuard::begin(&fragment, "expired").unwrap());
    assert!(second.try_reclaim("expired").unwrap().is_none());
    drop(first);
    drop(second);
    assert_released(&fragment);
}

#[test]
fn scan_memory_is_bounded_and_saturation_defers_deletion_not_uploads() {
    let fragment = fragment();
    let scan = UploadGcScan::begin(&fragment).unwrap();
    for index in 0..=MAX_SCAN_OBJECTS {
        drop(UploadActivityGuard::begin(&fragment, &format!("object-{index}")).unwrap());
    }
    {
        let current = scan.state.lock().unwrap();
        assert_eq!(current.scans[&scan.id].objects.len(), MAX_SCAN_OBJECTS);
        assert!(current.scans[&scan.id].saturated);
        assert!(current.objects.is_empty());
    }
    assert!(scan.try_reclaim("untracked").unwrap().is_none());
    drop(scan);
    assert_released(&fragment);
}

#[test]
fn concurrent_same_object_uploads_keep_their_independent_lifetimes() {
    let fragment = fragment();
    let ready = Arc::new(Barrier::new(17));
    let release = Arc::new(Barrier::new(17));
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let ready = ready.clone();
            let release = release.clone();
            let fragment = &fragment;
            scope.spawn(move || {
                let _upload = UploadActivityGuard::begin(fragment, "same-object").unwrap();
                ready.wait();
                release.wait();
            });
        }
        ready.wait();
        let scan = UploadGcScan::begin(&fragment).unwrap();
        assert!(scan.try_reclaim("same-object").unwrap().is_none());
        release.wait();
        drop(scan);
    });
    assert_released(&fragment);
}

#[test]
fn poisoned_activity_fails_closed_without_poisoning_other_fragments() {
    let fragment = fragment();
    let scan = UploadGcScan::begin(&fragment).unwrap();
    let state = scan.state.clone();
    let _ = std::thread::spawn(move || {
        let _lock = state.lock().unwrap();
        panic!("injected fragment activity failure");
    })
    .join();
    assert!(scan.try_reclaim("object").is_err());
    assert!(UploadActivityGuard::begin(&fragment, "object").is_err());
    let other = self::fragment();
    drop(UploadActivityGuard::begin(&other, "object").unwrap());
    assert_released(&other);
    drop(scan);
    // Only this deliberately poisoned test fixture needs explicit removal.
    REGISTRY.lock().unwrap().remove(&fragment);
}

#[test]
fn prefixed_object_ids_share_one_fence_and_invalid_ids_are_rejected() {
    let fragment = fragment();
    let scan = UploadGcScan::begin(&fragment).unwrap();
    let upload = UploadActivityGuard::begin(&fragment, "/@o_object").unwrap();
    assert!(scan.try_reclaim("object").unwrap().is_none());
    assert!(UploadActivityGuard::active(&fragment, "object").unwrap());
    assert!(UploadActivityGuard::begin(&fragment, "../escape").is_err());
    drop(upload);
    drop(scan);
    assert_released(&fragment);
}

#[tokio::test]
async fn blocking_publisher_keeps_protection_after_awaiting_request_is_cancelled() {
    let fragment = fragment();
    let upload = UploadActivityGuard::begin(&fragment, "object").unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let request = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(upload);
            done_tx.send(()).unwrap();
        })
        .await
        .unwrap();
    });
    started_rx.await.unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let scan = UploadGcScan::begin(&fragment).unwrap();
    assert!(scan.try_reclaim("object").unwrap().is_none());
    release_tx.send(()).unwrap();
    done_rx.await.unwrap();
    assert!(scan.try_reclaim("object").unwrap().is_none());
    drop(scan);
    assert_released(&fragment);
}
