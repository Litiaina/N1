use super::*;
use crate::core::checksum::{format_sha256, sha256_digest};
use crate::core::disk_manager::ShardInfo;
use simd_erasure_core::ReedSolomonEncoder;
use ulid::Ulid;

fn local_shard(path: &std::path::Path, expected: &[u8]) -> ShardInfo {
    ShardInfo {
        shard_index: 0,
        file_name: "probe.shard".into(),
        absolute_path: path.to_string_lossy().to_string(),
        size_bytes: expected.len() as u64,
        checksum: format_sha256(&sha256_digest(expected)),
        block_checksums: Vec::new(),
        checksum_block_size: 0,
        is_parity: false,
    }
}

#[tokio::test]
async fn shard_probe_distinguishes_missing_corrupt_and_unavailable() {
    let root = std::env::temp_dir().join(format!("n2-shard-probe-{}", Ulid::new()));
    tokio::fs::create_dir_all(&root).await.expect("create root");
    let expected = b"verified-shard";
    let path = root.join("probe.shard");
    let shard = local_shard(&path, expected);

    assert_eq!(shard_probe(&shard, true).await, ShardProbeResult::Missing);

    tokio::fs::write(&path, b"corrupt-shard")
        .await
        .expect("write corrupt shard");
    assert_eq!(
        shard_probe(&shard, true).await,
        ShardProbeResult::ChecksumMismatch
    );

    tokio::fs::write(&path, expected)
        .await
        .expect("write healthy shard");
    assert_eq!(shard_probe(&shard, true).await, ShardProbeResult::Healthy);

    let directory_shard = local_shard(&root, expected);
    assert_eq!(
        shard_probe(&directory_shard, true).await,
        ShardProbeResult::Unavailable
    );
    assert_eq!(
        classify_probe_error(AppError::ServiceUnavailable("peer offline".into())),
        ShardProbeResult::Unavailable
    );

    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn verified_range_reads_only_checksum_blocks_covering_the_slice() {
    let root = std::env::temp_dir().join(format!("n2-range-read-{}", Ulid::new()));
    tokio::fs::create_dir_all(&root).await.expect("create root");
    let data = (0..65_537usize)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let block_size = 4096usize;
    let block_checksums = data
        .chunks(block_size)
        .map(sha256_digest)
        .collect::<Vec<_>>();
    let path = root.join("data.shard");
    tokio::fs::write(&path, &data).await.expect("write shard");
    let shard = ShardInfo {
        shard_index: 0,
        file_name: "data.shard".into(),
        absolute_path: path.to_string_lossy().to_string(),
        size_bytes: data.len() as u64,
        checksum: format_sha256(&sha256_digest(&data)),
        block_checksums,
        checksum_block_size: block_size,
        is_parity: false,
    };

    let mut cache = VerifiedShardCache::default();
    let offset = 123u64;
    let len = 20_000usize;
    let actual = read_verified_shard_slice_cached(&shard, offset, len, &mut cache)
        .await
        .expect("verified range");
    assert_eq!(actual, data[offset as usize..offset as usize + len]);
    assert!(cache.verified.is_empty());
    assert!(!cache.verified_blocks.is_empty());

    tokio::fs::remove_file(&path)
        .await
        .expect("remove backing shard after cache fill");
    let cached_offset = 512u64;
    let cached_len = 1024usize;
    let cached = read_verified_shard_slice_cached(&shard, cached_offset, cached_len, &mut cache)
        .await
        .expect("second range is served from verified block cache");
    assert_eq!(
        cached,
        data[cached_offset as usize..cached_offset as usize + cached_len]
    );
    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn cluster_read_reconstructs_missing_data_from_parity() {
    let root = std::env::temp_dir().join(format!("n2-cluster-read-{}", Ulid::new()));
    tokio::fs::create_dir_all(&root).await.expect("create root");
    let data = b"abcd".to_vec();
    let mut encoder = ReedSolomonEncoder::new(1, 1, data.len()).expect("encoder");
    encoder.add_original_shard(&data).expect("add data");
    let encoded = encoder.encode().expect("encode");
    let parity = encoded.recovery(0).expect("parity").to_vec();
    drop(encoded);
    let parity_path = root.join("part_0_shard_1.parity");
    tokio::fs::write(&parity_path, &parity)
        .await
        .expect("write parity");
    let part = PartMetadata {
        part_index: 0,
        size_bytes: data.len() as u64,
        created_at: 0,
        checksum: None,
        shards: vec![
            ShardInfo {
                shard_index: 0,
                file_name: "part_0_shard_0.shard".into(),
                absolute_path: root
                    .join("missing-data.shard")
                    .to_string_lossy()
                    .to_string(),
                size_bytes: data.len() as u64,
                checksum: format_sha256(&sha256_digest(&data)),
                block_checksums: Vec::new(),
                checksum_block_size: 0,
                is_parity: false,
            },
            ShardInfo {
                shard_index: 1,
                file_name: "part_0_shard_1.parity".into(),
                absolute_path: parity_path.to_string_lossy().to_string(),
                size_bytes: data.len() as u64,
                checksum: format_sha256(&sha256_digest(&parity)),
                block_checksums: Vec::new(),
                checksum_block_size: 0,
                is_parity: true,
            },
        ],
    };

    let restored = read_cluster_data_chunk(&part, 0, 0, data.len())
        .await
        .expect("reconstruct");
    assert_eq!(restored, data);
    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn prefetch_verifies_data_shards_concurrently_and_rejects_corruption() {
    let root = std::env::temp_dir().join(format!("n2-cluster-prefetch-{}", Ulid::new()));
    tokio::fs::create_dir_all(&root).await.expect("create root");
    let expected = [
        b"data-000".to_vec(),
        b"data-111".to_vec(),
        b"data-222".to_vec(),
    ];
    let mut shards = Vec::new();

    for (index, bytes) in expected.iter().enumerate() {
        let path = root.join(format!("part_0_shard_{index}.shard"));
        let stored = if index == 2 {
            b"corrupt!".as_slice()
        } else {
            bytes.as_slice()
        };
        tokio::fs::write(&path, stored).await.expect("write shard");
        shards.push(ShardInfo {
            shard_index: index,
            file_name: format!("part_0_shard_{index}.shard"),
            absolute_path: path.to_string_lossy().to_string(),
            size_bytes: bytes.len() as u64,
            checksum: format_sha256(&sha256_digest(bytes)),
            block_checksums: Vec::new(),
            checksum_block_size: 0,
            is_parity: false,
        });
    }

    let part = PartMetadata {
        part_index: 0,
        size_bytes: expected.iter().map(Vec::len).sum::<usize>() as u64,
        created_at: 0,
        shards,
        checksum: None,
    };
    let mut cache = VerifiedShardCache::default();
    prefetch_verified_data_shards(&part, &mut cache).await;

    assert_eq!(cache.verified.len(), 2);
    assert_eq!(cache.rejected.len(), 1);
    for (index, bytes) in expected.iter().enumerate().take(2) {
        let restored = read_cluster_data_chunk_cached(&part, index, 0, bytes.len(), &mut cache)
            .await
            .expect("read prefetched shard");
        assert_eq!(&restored, bytes);
    }

    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn verified_cluster_stripe_reconstructs_a_corrupt_data_block() {
    let root = std::env::temp_dir().join(format!("n2-block-recover-{}", Ulid::new()));
    tokio::fs::create_dir_all(&root).await.expect("create root");
    let blocks = [b"data".to_vec(), b"more".to_vec()];
    let mut encoder = ReedSolomonEncoder::new(2, 1, 4).expect("encoder");
    for block in &blocks {
        encoder.add_original_shard(block).expect("add data");
    }
    let encoded = encoder.encode().expect("encode");
    let parity = encoded.recovery(0).expect("parity").to_vec();
    drop(encoded);
    let stored = [b"data".to_vec(), b"FAIL".to_vec(), parity.clone()];
    let expected = [blocks[0].clone(), blocks[1].clone(), parity];
    let mut shards = Vec::new();
    for index in 0..3 {
        let path = root.join(format!("part_0_shard_{index}"));
        tokio::fs::write(&path, &stored[index])
            .await
            .expect("write block");
        shards.push(ShardInfo {
            shard_index: index,
            file_name: format!("part_0_shard_{index}"),
            absolute_path: path.to_string_lossy().to_string(),
            size_bytes: 4,
            checksum: format_sha256(&sha256_digest(&expected[index])),
            block_checksums: vec![sha256_digest(&expected[index])],
            checksum_block_size: 4,
            is_parity: index == 2,
        });
    }
    let part = PartMetadata {
        part_index: 0,
        size_bytes: 8,
        created_at: 0,
        shards,
        checksum: None,
    };

    let restored = read_verified_cluster_data_stripe(&part, 0, 4)
        .await
        .expect("reconstruct corrupt block");
    assert_eq!(restored, b"datamore");
    let _ = tokio::fs::remove_dir_all(root).await;
}
