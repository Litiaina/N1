//! Publication, retry and cancellation checks for the shared finalize path.
use super::*;
use std::os::unix::fs::FileExt;

async fn has_completion_seals(store: &Store, plan: n1_link::UploadPlan) -> bool {
    store
        .files(store.pool(plan.identity).unwrap(), plan)
        .await
        .unwrap()
        .iter()
        .flatten()
        .all(|file| file.root.is_some())
}

async fn wait_for_seals(fixture: &Fixture, store: &Store, plan: n1_link::UploadPlan) {
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let sealed = (0..fixture.layout.width()).all(|shard| {
                let member = pool.holder(fp, shard as u16).unwrap();
                let file = std::fs::File::open(member.path.join(fp.staging_path())).unwrap();
                let header =
                    n1_link::shard::ShardHeader::new(&fixture.layout, plan, shard as u16).unwrap();
                let mut seal = [0; n1_link::shard::SEAL_BYTES];
                file.read_exact_at(&mut seal, header.seal_offset()).is_ok()
                    && n1_link::shard::decode_seal(&header, fp, &seal).is_ok()
            });
            if sealed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn thousands_of_out_of_order_parts_finalize_with_a_folded_root_and_conflicting_retry() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let part_bytes = fixture.layout.stripe_bytes();
    let parts = 4101;
    let data = source((part_bytes * parts) as usize - 17);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            part_bytes,
            Durability::Standard,
            3600,
        )
        .await
        .unwrap();
    for group in (0..parts).step_by(8) {
        for part in (group..(group + 8).min(parts)).rev() {
            let start = (part * part_bytes) as usize;
            let end = (start + part_bytes as usize).min(data.len());
            store
                .write_at(
                    SCOPE,
                    init.object_key.clone(),
                    start as u64,
                    std::io::Cursor::new(data[start..end].to_vec()),
                )
                .await
                .unwrap();
        }
    }
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let fp = plan.identity.fingerprint();
    assert_eq!(store.completion_state(fp, plan).unwrap().1, parts);
    // A rejected read-only retry preserves acknowledged immutable bytes and
    // their aggregate. A write failure must still invalidate completion.
    let mut conflict = data[..part_bytes as usize].to_vec();
    conflict[17] ^= 1;
    assert!(matches!(
        store
            .write_at(
                SCOPE,
                init.object_key.clone(),
                0,
                std::io::Cursor::new(conflict)
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(store.completion_state(fp, plan).unwrap().1, parts);
    store
        .write_at(
            SCOPE,
            init.object_key.clone(),
            0,
            std::io::Cursor::new(data[..part_bytes as usize].to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(store.completion_state(fp, plan).unwrap().1, parts);
    store
        .finalize(SCOPE, init.object_key.clone())
        .await
        .unwrap();
    drop(store);
    let store = fixture.reopen();
    assert!(
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .unwrap()
            .already_finalized
    );
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    store.delete(SCOPE, init.object_key).await.unwrap();
    for root in &fixture.paths {
        assert!(!root.join(format!(".uploads/{fp}")).exists());
    }
}

#[tokio::test]
async fn multipart_publication_failure_drains_peers_then_recovers_without_its_cache() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = upload(&store, &data, Durability::Standard).await;
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    let final_paths = (0..fixture.layout.width())
        .map(|shard| {
            let slot = fixture.layout.member_slot(fp, shard as u16).unwrap();
            fixture.paths[slot].join(fp.shard_path(shard as u16))
        })
        .collect::<Vec<_>>();
    // Delay publication until the descriptors and valid seals exist. Installing
    // the fault earlier would exercise initial path validation, not a rename.
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let fence = store.snapshot_gate.clone().write_owned().await;
    let worker = store.clone();
    let key = init.object_key.clone();
    let caller = tokio::spawn(async move { worker.finalize(SCOPE, key).await });
    wait_for_seals(&fixture, &store, plan).await;
    std::fs::create_dir_all(&final_paths[0]).unwrap();
    drop(fence);
    assert!(caller.await.unwrap().is_err());
    for path in &final_paths[1..] {
        assert!(path.is_file(), "publication peer abandoned");
    }
    std::fs::remove_dir(&final_paths[0]).unwrap();
    drop(store);
    let store = fixture.reopen();
    store
        .finalize(SCOPE, init.object_key.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    assert!(
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .unwrap()
            .already_finalized
    );
    store.delete(SCOPE, init.object_key).await.unwrap();
    for path in final_paths {
        assert!(!path.exists());
    }
    for root in &fixture.paths {
        assert!(!root.join(format!(".uploads/{fp}")).exists());
    }
}

#[tokio::test]
async fn cancelled_multipart_finalize_keeps_publication_owned_until_shutdown_drains() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = upload(&store, &data, Durability::Standard).await;
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let fence = store.snapshot_gate.clone().write_owned().await;
    let worker = store.clone();
    let key = init.object_key.clone();
    let caller = tokio::spawn(async move { worker.finalize(SCOPE, key).await });
    wait_for_seals(&fixture, &store, plan).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    store.activity.close().unwrap();
    let activity = store.activity.clone();
    let mut drained = tokio::spawn(async move { activity.wait_idle().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut drained)
            .await
            .is_err()
    );
    drop(fence);
    tokio::time::timeout(std::time::Duration::from_secs(5), drained)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(store);
    let store = fixture.reopen();
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    store.delete(SCOPE, init.object_key).await.unwrap();
}

#[tokio::test]
async fn last_durable_part_seals_privately_and_a_failed_retry_invalidates_its_proof() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let part_bytes = fixture.layout.stripe_bytes();
    let data = source((part_bytes * 9) as usize - 17);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            part_bytes,
            Durability::Durable,
            3600,
        )
        .await
        .unwrap();
    let mut workers = Vec::new();
    for part in (0..9).rev() {
        let store = store.clone();
        let key = init.object_key.clone();
        let start = (part * part_bytes) as usize;
        let bytes = data[start..(start + part_bytes as usize).min(data.len())].to_vec();
        workers.push(tokio::spawn(async move {
            store
                .write_at(SCOPE, key, start as u64, std::io::Cursor::new(bytes))
                .await
        }));
    }
    for worker in workers {
        worker.await.unwrap().unwrap();
    }
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    assert!(has_completion_seals(&store, plan).await);
    wait_for_seals(&fixture, &store, plan).await;
    assert!(matches!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await,
        Err(Error::NotFound)
    ));
    let status = store
        .upload_status(SCOPE, init.object_key.clone(), 0, 9)
        .await
        .unwrap();
    assert!(!status.finalized);
    assert!(status.missing_parts.is_empty());
    // A failed retry may invalidate volatile progress, but the completed
    // immutable bytes and durable proof remain untouched.
    assert!(
        store
            .write_at(
                SCOPE,
                init.object_key.clone(),
                0,
                std::io::Cursor::new(data[..100].to_vec())
            )
            .await
            .is_err()
    );
    assert!(has_completion_seals(&store, plan).await);
    store
        .write_at(
            SCOPE,
            init.object_key.clone(),
            0,
            std::io::Cursor::new(data[..part_bytes as usize].to_vec()),
        )
        .await
        .unwrap();
    assert!(has_completion_seals(&store, plan).await);
    let result = store
        .finalize(SCOPE, init.object_key.clone())
        .await
        .unwrap();
    assert_eq!(result.object_key, init.object_key);
    drop(store);
    let store = fixture.reopen();
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    store.delete(SCOPE, init.object_key).await.unwrap();
}

#[tokio::test]
async fn damaged_completion_seal_recovers_from_a_peer_without_scanning_payload() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = upload(&store, &data, Durability::Durable).await;
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let files = store
        .files(store.pool(plan.identity).unwrap(), plan)
        .await
        .unwrap();
    let file = files[0].as_ref().unwrap();
    assert!(files.iter().flatten().all(|file| file.root.is_some()));
    file.file
        .write_all_at(&[0; 96], file.header.seal_offset())
        .unwrap();
    store
        .finalize(SCOPE, init.object_key.clone())
        .await
        .unwrap();
    // Release the test's retained member descriptors before reopening the pool.
    drop(files);
    drop(store);
    let store = fixture.reopen();
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    store.delete(SCOPE, init.object_key).await.unwrap();
}

#[tokio::test]
async fn durable_completion_survives_cache_loss_and_one_missing_footer() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = upload(&store, &data, Durability::Durable).await;
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let fp = plan.identity.fingerprint();
    let files = store
        .files(store.pool(plan.identity).unwrap(), plan)
        .await
        .unwrap();
    assert!(files.iter().flatten().all(|file| file.root.is_some()));
    for file in files.iter().flatten().skip(1) {
        file.file
            .write_all_at(&[0; 96], file.header.seal_offset())
            .unwrap();
        file.file.sync_all().unwrap();
    }
    drop(files);
    drop(store);
    let store = fixture.reopen();
    assert!(store.completion_state(fp, plan).is_none());
    store
        .finalize(SCOPE, init.object_key.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    store.delete(SCOPE, init.object_key).await.unwrap();
}

#[tokio::test]
async fn standard_cache_loss_requires_explicit_paged_reconciliation() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = upload(&store, &data, Durability::Standard).await;
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    drop(store);
    let store = fixture.reopen();
    assert!(matches!(
        store.finalize(SCOPE, init.object_key.clone()).await,
        Err(Error::Conflict(_))
    ));
    for part in 0..plan.parts() {
        assert!(
            store
                .upload_status(SCOPE, init.object_key.clone(), part, 1)
                .await
                .unwrap()
                .missing_parts
                .is_empty()
        );
    }
    store
        .finalize(SCOPE, init.object_key.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    store.delete(SCOPE, init.object_key).await.unwrap();
}

#[tokio::test]
async fn completed_staged_payload_retries_are_read_only_and_cannot_replace_corrupt_data() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = upload(&store, &data, Durability::Durable).await;
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let files = store
        .files(store.pool(plan.identity).unwrap(), plan)
        .await
        .unwrap();
    let file = files[0].as_ref().unwrap();
    let offset = file.header.frame_offset(0).unwrap() + n1_link::shard::FRAME_HEADER_BYTES as u64;
    file.file.write_all_at(&[251], offset).unwrap();
    let before = std::fs::read(
        file.member
            .path
            .join(plan.identity.fingerprint().staging_path()),
    )
    .unwrap();
    assert!(
        store
            .write_at(
                SCOPE,
                init.object_key.clone(),
                0,
                std::io::Cursor::new(data[..plan.part_length(0).unwrap() as usize].to_vec())
            )
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(
            file.member
                .path
                .join(plan.identity.fingerprint().staging_path())
        )
        .unwrap(),
        before
    );
    drop(files);
    store.delete(SCOPE, init.object_key).await.unwrap();
}

#[tokio::test]
async fn footer_without_a_live_persistence_confirmation_is_refreshed_before_publication() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = upload(&store, &data, Durability::Durable).await;
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let fp = plan.identity.fingerprint();
    let files = store
        .files(store.pool(plan.identity).unwrap(), plan)
        .await
        .unwrap();
    let old_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1);
    for file in files.iter().flatten() {
        assert!(file.completion_proof);
        file.file
            .set_times(std::fs::FileTimes::new().set_modified(old_time))
            .unwrap();
    }
    // A valid footer in page cache does not attest to its own successful flush.
    // A fresh process or failed seal ACK has no live persistence confirmation.
    store.forget(fp);
    store
        .finalize(SCOPE, init.object_key.clone())
        .await
        .unwrap();
    for file in files.iter().flatten() {
        assert_ne!(file.file.metadata().unwrap().modified().unwrap(), old_time);
    }
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    drop(files);
    store.delete(SCOPE, init.object_key).await.unwrap();
}
