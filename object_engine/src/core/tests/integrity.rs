//! End-to-end enforcement when a frame passes its quick checksum but carries
//! wrong logical bytes or a digest from another object/stripe.
use super::*;
use n1_link::{
    Fingerprint, ShardFormat,
    shard::{self, ShardHeader},
};
use std::os::unix::fs::{FileExt, MetadataExt};
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn checksum_invalid_quorum_reports_integrity_failure_instead_of_admission_pressure() {
    let mut fixture = Fixture::new();
    fixture.layout.data_shards = 1;
    fixture.layout.parity_shards = 0;
    fixture.paths.truncate(1);
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 4096, 4096, Durability::Durable, 3600)
        .await
        .unwrap();
    store
        .put(
            SCOPE,
            init.object_key.clone(),
            std::io::Cursor::new(source(4096)),
        )
        .await
        .unwrap();
    let plan = store
        .inspect_upload(SCOPE, &init.object_key.clone())
        .await
        .unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    let path = pool.holder(fp, 0).unwrap().path.join(fp.shard_path(0));
    let original = std::fs::read(&path).unwrap();
    for offset in [shard::HEADER_BYTES + shard::FRAME_HEADER_BYTES + 17, 8] {
        let mut bytes = original.clone();
        bytes[offset] ^= 0x80;
        std::fs::write(&path, bytes).unwrap();
        let error = store
            .object_health(SCOPE, init.object_key.clone(), true)
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Corrupt(_)),
            "offset {offset}: {error}"
        );
        assert_eq!(error.status(), 422);
    }
}

#[tokio::test]
async fn crc_valid_wrong_source_or_foreign_identity_never_emits_or_repairs_unverified_data() {
    for foreign_digest in [false, true] {
        let mut fixture = Fixture::new();
        fixture.layout.data_shards = 1;
        fixture.layout.parity_shards = 1;
        fixture.paths.truncate(2);
        let store = fixture.enroll();
        let source = source(4096);
        let init = store
            .init(SCOPE, 0, 4096, 4096, Durability::Durable, 3600)
            .await
            .unwrap();
        store
            .put(
                SCOPE,
                init.object_key.clone(),
                std::io::Cursor::new(source.clone()),
            )
            .await
            .unwrap();
        let plan = store
            .inspect_upload(SCOPE, &init.object_key.clone())
            .await
            .unwrap();
        let fp = plan.identity.fingerprint();
        let header = ShardHeader::new(&fixture.layout, plan, 0).unwrap();
        let pool = store.pool(plan.identity).unwrap();
        let path = pool.holder(fp, 0).unwrap().path.join(fp.shard_path(0));
        let before = std::fs::metadata(&path).unwrap();
        let mut payload = source.clone();
        let digest = if foreign_digest {
            header
                .source_hash(Fingerprint([27; 32]), 0, &source)
                .unwrap()
        } else {
            payload[17] ^= 1;
            header.source_hash(fp, 0, &source).unwrap()
        };
        let encoded = shard::encode_frame(&header, fp, 0, digest, &payload).unwrap();
        assert!(shard::decode_frame(&header, fp, 0, &encoded).is_ok());
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&encoded, header.frame_offset(0).unwrap())
            .unwrap();
        file.sync_all().unwrap();
        let mut download = store
            .download_stream(SCOPE, init.object_key.clone(), 0, None)
            .await
            .unwrap();
        let mut returned = Vec::new();
        download.reader.read_to_end(&mut returned).await.unwrap();
        assert!(returned.is_empty(), "unverified source reached the client");
        assert!(matches!(
            download.completion.await.unwrap(),
            Err(Error::Corrupt(_))
        ));
        assert!(matches!(
            store
                .object_health(SCOPE, init.object_key.clone(), true)
                .await,
            Err(Error::Corrupt(_))
        ));
        assert!(matches!(
            store.repair(SCOPE, init.object_key.clone()).await,
            Err(Error::Corrupt(_))
        ));
        assert_eq!(
            std::fs::read(&path).unwrap()[128..128 + encoded.len()],
            encoded
        );
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), before.ino());
        // Purge must remove the corrupt shards and prevent continuation with this key.
        store.delete(SCOPE, init.object_key.clone()).await.unwrap();
        assert!(matches!(
            store.stat(SCOPE, init.object_key.clone()).await,
            Err(Error::NotFound)
        ));
        assert!(
            store
                .put(SCOPE, init.object_key.clone(), std::io::Cursor::new(source))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn sha256_and_hybrid_upload_resume_reconstruction_and_purge_keep_their_pool_format() {
    for format in [ShardFormat::Sha256V1, ShardFormat::Crc32cSha256V2] {
        let mut fixture = Fixture::new();
        fixture.layout.shard_format = format;
        let store = fixture.enroll();
        let data = source(32769);
        let init = store
            .init(
                SCOPE,
                0,
                data.len() as u64,
                12288,
                Durability::Durable,
                3600,
            )
            .await
            .unwrap();
        store
            .write_part(
                SCOPE,
                init.object_key.clone(),
                0,
                std::io::Cursor::new(data[..12288].to_vec()),
            )
            .await
            .unwrap();
        drop(store);
        let store = fixture.reopen();
        let status = store
            .upload_status(SCOPE, init.object_key.clone(), 0, 4096)
            .await
            .unwrap();
        assert_eq!(status.missing_parts, vec![1, 2]);
        for part in [2, 1] {
            let start = part * 12288;
            let end = data.len().min(start + 12288);
            store
                .write_part(
                    SCOPE,
                    init.object_key.clone(),
                    part as u64,
                    std::io::Cursor::new(data[start..end].to_vec()),
                )
                .await
                .unwrap();
        }
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .unwrap();
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .unwrap();
        let plan = store
            .inspect_upload(SCOPE, &init.object_key.clone())
            .await
            .unwrap();
        let fp = plan.identity.fingerprint();
        let pool = store.pool(plan.identity).unwrap();
        let path = pool.holder(fp, 0).unwrap().path.join(fp.shard_path(0));
        std::fs::remove_file(path).unwrap();
        let returned = store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0;
        assert_eq!(returned, data);
        assert_eq!(
            store
                .stat(SCOPE, init.object_key.clone())
                .await
                .unwrap()
                .integrity_algorithm,
            format.integrity_algorithm()
        );
        store.delete(SCOPE, init.object_key.clone()).await.unwrap();
        drop(pool);
        drop(store);
        let store = fixture.reopen();
        assert!(matches!(
            store.stat(SCOPE, init.object_key.clone()).await,
            Err(Error::NotFound)
        ));
        assert!(
            store
                .finalize(SCOPE, init.object_key.clone())
                .await
                .is_err()
        );
    }
}

#[test]
fn changing_the_checksum_format_of_an_enrolled_pool_is_rejected() {
    let mut fixture = Fixture::new();
    let store = fixture.enroll();
    drop(store);
    fixture.layout.shard_format = ShardFormat::Sha256V1;
    assert!(storage_pool::Pool::reopen_fixture(fixture.layout.clone(), &fixture.paths).is_err());
}
