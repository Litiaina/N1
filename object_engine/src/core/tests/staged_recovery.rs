//! Repair restores sealed staging without weakening upload retry immutability.
use super::*;
use std::os::unix::fs::FileExt;

#[tokio::test]
async fn explicit_repair_restores_sealed_staging_without_publishing_or_mutating_retries() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = upload(&store, &data, Durability::Durable).await;
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    let member_root = pool.holder(fp, 0).unwrap().path.clone();
    let path = member_root.join(fp.staging_path());
    let header = n1_link::shard::ShardHeader::new(&fixture.layout, plan, 0).unwrap();
    // Keep the key/header, but lose this member's frames and completion seal.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(n1_link::shard::HEADER_BYTES as u64)
        .unwrap();
    drop(pool);
    drop(store);
    let store = fixture.reopen();
    assert!(
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .is_err()
    );
    assert!(!member_root.join(fp.shard_path(0)).exists());
    let before = std::fs::read(&path).unwrap();
    let mut conflict = data[..12288].to_vec();
    conflict[17] ^= 1;
    assert!(
        store
            .write_part(
                SCOPE,
                init.object_key.clone(),
                0,
                std::io::Cursor::new(conflict)
            )
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(
        store
            .write_part(
                SCOPE,
                init.object_key.clone(),
                0,
                std::io::Cursor::new(data[..12288].to_vec())
            )
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let repaired = store.repair(SCOPE, init.object_key.clone()).await.unwrap();
    assert_eq!(repaired.repaired_shards, vec![0]);
    assert!(store.stat(SCOPE, init.object_key.clone()).await.is_err());
    assert!(path.is_file());
    assert!(!member_root.join(fp.shard_path(0)).exists());
    for (part, bytes) in data.chunks(12288).enumerate() {
        store
            .write_part(
                SCOPE,
                init.object_key.clone(),
                part as u64,
                std::io::Cursor::new(bytes.to_vec()),
            )
            .await
            .unwrap();
    }
    // Restored bytes and ACKs survive another reopen before finalization.
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
    // Publication keeps its existing immutability boundary. Even a matching
    // retry must not repair a damaged finalized frame through the upload API.
    let final_path = member_root.join(fp.shard_path(0));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&final_path)
        .unwrap();
    file.write_all_at(
        &[0; n1_link::shard::FRAME_HEADER_BYTES],
        header.frame_offset(0).unwrap(),
    )
    .unwrap();
    file.sync_all().unwrap();
    assert!(
        store
            .write_part(
                SCOPE,
                init.object_key.clone(),
                0,
                std::io::Cursor::new(data[..12288].to_vec())
            )
            .await
            .is_err()
    );
    store.delete(SCOPE, init.object_key.clone()).await.unwrap();
    assert!(matches!(
        store
            .write_part(
                SCOPE,
                init.object_key,
                0,
                std::io::Cursor::new(data[..12288].to_vec())
            )
            .await,
        Err(Error::NotFound)
    ));
}

#[tokio::test]
async fn sealed_staging_without_enough_verified_stripes_refuses_repair_and_retry() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = upload(&store, &data, Durability::Durable).await;
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    let mut paths = Vec::new();
    for shard in 0..fixture.layout.width() {
        let member = pool.holder(fp, shard as u16).unwrap();
        let path = member.path.join(fp.staging_path());
        let header = n1_link::shard::ShardHeader::new(&fixture.layout, plan, shard as u16).unwrap();
        // Preserve every completion seal but remove all proof for stripe zero.
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(
            &[0; n1_link::shard::FRAME_HEADER_BYTES],
            header.frame_offset(0).unwrap(),
        )
        .unwrap();
        file.sync_all().unwrap();
        paths.push((path.clone(), std::fs::read(path).unwrap()));
    }
    drop(pool);
    drop(store);
    let store = fixture.reopen();
    assert!(store.repair(SCOPE, init.object_key.clone()).await.is_err());
    assert!(
        store
            .write_part(
                SCOPE,
                init.object_key,
                0,
                std::io::Cursor::new(data[..12288].to_vec())
            )
            .await
            .is_err()
    );
    for (path, before) in paths {
        assert_eq!(std::fs::read(path).unwrap(), before);
    }
}
