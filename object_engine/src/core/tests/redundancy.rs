//! Exhaustive logical shard-loss sets. Physical filesystem independence is
//! qualified separately by the opt-in process witnesses on two real HDDs.
use super::*;
use std::os::unix::fs::MetadataExt;

#[tokio::test]
async fn every_shard_loss_and_corruption_set_obeys_the_exact_ec_budget() {
    let mut checked = 0;
    for (data_shards, parity_shards) in [(1, 0), (1, 1), (2, 1), (2, 2), (3, 1)] {
        let width = (data_shards + parity_shards) as usize;
        let stripe = data_shards as usize * 4096;
        for length in [0, 1, stripe - 1, stripe, 3 * stripe + 17] {
            let mut fixture = Fixture::new();
            fixture.layout.data_shards = data_shards;
            fixture.layout.parity_shards = parity_shards;
            fixture.paths.truncate(width);
            let store = fixture.enroll();
            let bytes = source(length);
            let upload = store
                .init(
                    SCOPE,
                    0,
                    bytes.len() as u64,
                    (length.max(1).div_ceil(stripe) * stripe) as u64,
                    Durability::Durable,
                    3600,
                )
                .unwrap();
            store
                .put(
                    SCOPE,
                    upload.upload_key.clone(),
                    std::io::Cursor::new(bytes.clone()),
                )
                .await
                .unwrap();
            store
                .finalize(SCOPE, upload.upload_key.clone())
                .await
                .unwrap();
            let plan = store.inspect_upload(SCOPE, &upload.upload_key).unwrap();
            let fingerprint = plan.identity.fingerprint();
            let pool = store.pool(plan.identity).unwrap();
            let paths: Vec<_> = (0..width)
                .map(|shard| {
                    pool.holder(fingerprint, shard as u16)
                        .unwrap()
                        .path
                        .join(fingerprint.shard_path(shard as u16))
                })
                .collect();
            let original: Vec<_> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();
            drop(pool);
            drop(store);
            for corrupt in [false, true] {
                for mask in 1u32..(1 << width) {
                    for (path, bytes) in paths.iter().zip(&original) {
                        std::fs::write(path, bytes).unwrap();
                    }
                    let lost: Vec<_> = (0..width)
                        .filter(|shard| mask & (1 << shard) != 0)
                        .map(|shard| shard as u16)
                        .collect();
                    for &shard in &lost {
                        let path = &paths[usize::from(shard)];
                        if corrupt {
                            let mut bytes = original[usize::from(shard)].clone();
                            bytes[8] ^= 1; // Invalid immutable header, including empty objects.
                            std::fs::write(path, bytes).unwrap();
                        } else {
                            std::fs::remove_file(path).unwrap();
                        }
                    }
                    let after_fault: Vec<_> = paths.iter().map(std::fs::read).collect();
                    let healthy_inodes: Vec<_> = paths
                        .iter()
                        .enumerate()
                        .filter(|(shard, _)| mask & (1 << shard) == 0)
                        .map(|(shard, p)| (shard, p.metadata().unwrap().ino()))
                        .collect();
                    // Reopen removes all upload/progress/decoder cache state.
                    let store = fixture.reopen();
                    let result = store
                        .download(SCOPE, upload.object_key.clone(), 0, None, Vec::new())
                        .await;
                    let context = format!(
                        "{data_shards}+{parity_shards}, {length} bytes, mask {mask:b}, corrupt={corrupt}"
                    );
                    if lost.len() <= usize::from(parity_shards) {
                        assert_eq!(result.unwrap().0, bytes, "{context}");
                        let health = store
                            .object_health(SCOPE, upload.object_key.clone(), true)
                            .await
                            .unwrap();
                        assert_eq!(health.status, "degraded", "{context}");
                        assert_eq!(
                            if corrupt {
                                health.corrupt_shards
                            } else {
                                health.missing_shards
                            },
                            lost,
                            "{context}"
                        );
                        let repaired = store
                            .repair(SCOPE, upload.object_key.clone())
                            .await
                            .unwrap();
                        assert_eq!(repaired.repaired_shards, lost, "{context}");
                        assert_eq!(
                            store
                                .object_health(SCOPE, upload.object_key.clone(), true)
                                .await
                                .unwrap()
                                .status,
                            "healthy",
                            "{context}"
                        );
                        for (path, expected) in paths.iter().zip(&original) {
                            assert_eq!(std::fs::read(path).unwrap(), *expected, "{context}");
                        }
                        for (shard, inode) in &healthy_inodes {
                            assert_eq!(
                                paths[*shard].metadata().unwrap().ino(),
                                *inode,
                                "{context}"
                            );
                        }
                    } else {
                        assert!(
                            result.is_err(),
                            "over-budget loss was acknowledged: {context}"
                        );
                        assert!(
                            store
                                .repair(SCOPE, upload.object_key.clone())
                                .await
                                .is_err(),
                            "over-budget repair succeeded: {context}"
                        );
                        for ((path, expected), shard) in paths.iter().zip(&after_fault).zip(0..) {
                            match expected {
                                Ok(expected) => assert_eq!(
                                    std::fs::read(path).unwrap(),
                                    *expected,
                                    "failed repair rewrote shard {shard}: {context}"
                                ),
                                Err(_) => assert!(
                                    !path.exists(),
                                    "failed repair created shard {shard}: {context}"
                                ),
                            }
                        }
                    }
                    drop(store);
                    checked += 1;
                }
            }
        }
    }
    assert_eq!(checked, 410);
    eprintln!("Exhaustive redundancy matrix: {checked} loss/corruption cases passed");
}
