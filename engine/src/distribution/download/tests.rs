use super::*;
use crate::core::checksum::{format_sha256, sha256_digest};
use crate::core::disk_manager::ShardInfo;
use ecs_core::ReedSolomonEncoder;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
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

#[tokio::test]
async fn shard_reconstruction_rejects_zero_blocks_and_invalid_empty_checksum() {
    let part = PartMetadata {
        part_index: 0,
        size_bytes: 0,
        created_at: 0,
        checksum: None,
        shards: vec![local_shard(
            std::path::Path::new("/unused-empty-shard"),
            b"",
        )],
    };
    let mut cache = VerifiedShardCache::for_repair();
    assert!(
        reconstruct_cluster_shard_cached(&part, 0, 0, &mut cache)
            .await
            .expect_err("zero block size must fail instead of looping")
            .to_string()
            .contains("block size is zero")
    );
    assert_eq!(
        reconstruct_cluster_shard_cached(&part, 0, 4096, &mut cache)
            .await
            .expect("empty target still has its recorded SHA"),
        Vec::<u8>::new()
    );
    let mut wrong_checksum = part;
    wrong_checksum.shards[0].checksum = format_sha256(&sha256_digest(b"not empty"));
    assert!(
        reconstruct_cluster_shard_cached(&wrong_checksum, 0, 4096, &mut cache)
            .await
            .expect_err("even zero-length repair must match the immutable checksum")
            .to_string()
            .contains("empty repaired shard checksum is invalid")
    );
}

#[derive(Default)]
struct RepairHashSink {
    checksum: Sha256Stream,
    written: u64,
    largest_write: usize,
    fail_after: Option<u64>,
    mutate_after_first_write: Option<(PathBuf, u64)>,
}

impl AsyncWrite for RepairHashSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.fail_after.is_some_and(|limit| self.written >= limit) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected repair destination failure",
            )));
        }
        self.checksum.update(bytes);
        self.written += bytes.len() as u64;
        self.largest_write = self.largest_write.max(bytes.len());
        if let Some((path, offset)) = self.mutate_after_first_write.take() {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .expect("open verified legacy donor");
            std::io::Seek::seek(&mut file, SeekFrom::Start(offset)).expect("seek donor");
            let mut byte = [0];
            std::io::Read::read_exact(&mut file, &mut byte).expect("read donor byte");
            byte[0] ^= 0x5a;
            std::io::Seek::seek(&mut file, SeekFrom::Start(offset)).expect("seek donor again");
            std::io::Write::write_all(&mut file, &byte).expect("corrupt donor after verification");
        }
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Build large fixtures a stripe at a time: the test itself need not retain
/// whole source or expected target payloads to prove streaming reconstruction.
async fn streaming_fixture(
    root: &Path,
    data_count: usize,
    parity_count: usize,
    block_size: usize,
    blocks: usize,
    block_checksums: bool,
) -> PartMetadata {
    tokio::fs::create_dir_all(root)
        .await
        .expect("create fixture");
    let width = data_count + parity_count;
    let mut files = Vec::new();
    let mut hashes = vec![Sha256Stream::new(); width];
    let mut block_hashes = vec![Vec::new(); width];
    for index in 0..width {
        files.push(
            File::create(root.join(format!("shard-{index}")))
                .await
                .expect("create fixture shard"),
        );
    }
    for block in 0..blocks {
        let encoding_size = block_size + (block_size % 2);
        let mut encoder =
            ReedSolomonEncoder::new(data_count, parity_count, encoding_size).expect("fixture EC");
        for index in 0..data_count {
            let mut bytes = (0..block_size)
                .map(|byte| ((byte * 13 + index * 23 + block * 31) % 251) as u8)
                .collect::<Vec<_>>();
            files[index].write_all(&bytes).await.expect("write data");
            hashes[index].update(&bytes);
            if block_checksums {
                block_hashes[index].push(sha256_digest(&bytes));
            }
            bytes.resize(encoding_size, 0);
            encoder
                .add_original_shard(&bytes)
                .expect("add fixture data");
        }
        let encoded = encoder.encode().expect("encode fixture");
        for parity in 0..parity_count {
            let index = data_count + parity;
            let bytes = &encoded.recovery(parity).expect("fixture parity")[..block_size];
            files[index].write_all(bytes).await.expect("write parity");
            hashes[index].update(bytes);
            if block_checksums {
                block_hashes[index].push(sha256_digest(bytes));
            }
        }
    }
    for file in &mut files {
        file.flush().await.expect("flush fixture");
    }
    drop(files);
    let checksum_block_size = if block_checksums { block_size } else { 0 };
    PartMetadata {
        part_index: 0,
        size_bytes: (block_size * blocks * data_count) as u64,
        created_at: 0,
        checksum: None,
        shards: hashes
            .into_iter()
            .zip(block_hashes)
            .enumerate()
            .map(|(index, (hash, block_checksums))| ShardInfo {
                shard_index: index,
                file_name: format!("shard-{index}"),
                absolute_path: root.join(format!("shard-{index}")).to_string_lossy().into(),
                size_bytes: (block_size * blocks) as u64,
                checksum: format_sha256(&hash.finalize()),
                block_checksums,
                checksum_block_size,
                is_parity: index >= data_count,
            })
            .collect(),
    }
}

#[tokio::test]
async fn large_repair_streams_data_and_parity_with_bounded_payload_cache() {
    let root = std::env::temp_dir().join(format!("n2-stream-repair-{}", Ulid::new()));
    let part = streaming_fixture(&root, 2, 1, 256 * 1024, 129, true).await;
    assert!(part.shards[0].size_bytes > 32 * 1024 * 1024);
    for target in [0, 2] {
        let old = root.join(format!("saved-{target}"));
        tokio::fs::rename(&part.shards[target].absolute_path, &old)
            .await
            .expect("remove exact target");
        let mut cache = VerifiedShardCache::for_repair();
        let mut output = RepairHashSink::default();
        let written =
            reconstruct_cluster_shard_into(&part, target, 256 * 1024, &mut cache, &mut output)
                .await
                .expect("stream exact repair");
        assert_eq!(written, part.shards[target].size_bytes);
        assert_eq!(output.written, written);
        assert_eq!(
            format_sha256(&output.checksum.finalize()),
            part.shards[target].checksum
        );
        assert!(output.largest_write <= MAX_REPAIR_CHUNK_BYTES);
        assert!(cache.verified.is_empty(), "repair retains no whole donors");
        assert!(cache.verified_block_bytes <= REPAIR_CACHE_BYTES);
        assert_eq!(
            cache.verified_block_bytes,
            cache.verified_blocks.values().map(Vec::len).sum::<usize>()
        );
        tokio::fs::rename(old, &part.shards[target].absolute_path)
            .await
            .expect("restore fixture target");
    }
    tokio::fs::remove_dir_all(root)
        .await
        .expect("remove fixture");
}

#[tokio::test]
async fn legacy_repair_streams_without_retaining_full_donors_and_detects_changed_source() {
    let root = std::env::temp_dir().join(format!("n2-legacy-stream-repair-{}", Ulid::new()));
    let part = streaming_fixture(&root, 1, 1, 256 * 1024, 9, false).await;
    tokio::fs::remove_file(&part.shards[0].absolute_path)
        .await
        .expect("remove target");
    let mut cache = VerifiedShardCache::for_repair();
    let mut output = RepairHashSink::default();
    reconstruct_cluster_shard_into(&part, 0, 256 * 1024, &mut cache, &mut output)
        .await
        .expect("legacy donor repair");
    assert!(cache.verified.is_empty());
    assert!(!cache.verified_repair_sources.is_empty());
    assert_eq!(
        format_sha256(&output.checksum.finalize()),
        part.shards[0].checksum
    );

    let mut cache = VerifiedShardCache::for_repair();
    let mut output = RepairHashSink {
        mutate_after_first_write: Some((
            PathBuf::from(&part.shards[1].absolute_path),
            part.shards[1].size_bytes - 17,
        )),
        ..RepairHashSink::default()
    };
    let error = reconstruct_cluster_shard_into(&part, 0, 256 * 1024, &mut cache, &mut output)
        .await
        .expect_err("donor changing after its SHA prepass must not yield successful repair");
    assert!(error.to_string().contains("failed checksum validation"));
    assert!(cache.verified.is_empty());
    tokio::fs::remove_dir_all(root)
        .await
        .expect("remove fixture");
}

#[tokio::test]
async fn streaming_subdivides_large_multi_role_ec_stripes_without_changing_bytes() {
    let root = std::env::temp_dir().join(format!("n2-large-ec-stripe-repair-{}", Ulid::new()));
    let part = streaming_fixture(&root, 3, 2, 2 * MAX_REPAIR_CHUNK_BYTES, 2, true).await;
    for target in [1, 4] {
        let saved = root.join(format!("saved-{target}"));
        tokio::fs::rename(&part.shards[target].absolute_path, &saved)
            .await
            .unwrap();
        let mut cache = VerifiedShardCache::for_repair();
        let mut sink = RepairHashSink::default();
        reconstruct_cluster_shard_into(
            &part,
            target,
            2 * MAX_REPAIR_CHUNK_BYTES,
            &mut cache,
            &mut sink,
        )
        .await
        .expect("subdivided data/parity retain the original EC byte encoding");
        assert_eq!(sink.written, part.shards[target].size_bytes);
        assert_eq!(
            format_sha256(&sink.checksum.finalize()),
            part.shards[target].checksum
        );
        assert!(sink.largest_write <= MAX_REPAIR_CHUNK_BYTES);
        assert!(cache.verified_block_bytes <= REPAIR_CACHE_BYTES);
        tokio::fs::rename(saved, &part.shards[target].absolute_path)
            .await
            .unwrap();
    }
    tokio::fs::remove_dir_all(root).await.unwrap();
}

#[tokio::test]
async fn oversized_recorded_checksum_blocks_are_verified_in_bounded_slices() {
    let root = std::env::temp_dir().join(format!("n2-large-checksum-block-{}", Ulid::new()));
    let block_size = 2 * MAX_REPAIR_CHUNK_BYTES;
    let mut part = streaming_fixture(&root, 1, 1, block_size, 1, true).await;
    tokio::fs::remove_file(&part.shards[0].absolute_path)
        .await
        .expect("remove target");
    // A huge recorded block still denotes the existing final partial block.
    // It must not become an allocation size or be silently trusted.
    part.shards[1].checksum_block_size = usize::MAX;
    let mut output = RepairHashSink::default();
    let mut cache = VerifiedShardCache::for_repair();
    reconstruct_cluster_shard_into(&part, 0, usize::MAX, &mut cache, &mut output)
        .await
        .expect("bounded historical checksum geometry");
    assert_eq!(
        format_sha256(&output.checksum.finalize()),
        part.shards[0].checksum
    );
    assert!(output.largest_write <= MAX_REPAIR_CHUNK_BYTES);
    assert_eq!(cache.verified_block_bytes, 0);
    assert!(cache.verified_blocks.is_empty());
    tokio::fs::remove_dir_all(root)
        .await
        .expect("remove fixture");
}

#[tokio::test]
async fn streaming_repair_checks_geometry_sink_errors_and_final_target_sha() {
    let root = std::env::temp_dir().join(format!("n2-repair-stream-errors-{}", Ulid::new()));
    let part = streaming_fixture(&root, 1, 1, 4096, 4, true).await;
    let mut malformed = part.clone();
    malformed.shards[1].shard_index = 0;
    let mut cache = VerifiedShardCache::for_repair();
    let mut output = RepairHashSink::default();
    assert!(
        reconstruct_cluster_shard_into(&malformed, 0, usize::MAX, &mut cache, &mut output)
            .await
            .is_err()
    );
    assert_eq!(output.written, 0);

    let mut output = RepairHashSink {
        fail_after: Some(4096),
        ..RepairHashSink::default()
    };
    let error = reconstruct_cluster_shard_into(&part, 0, 4096, &mut cache, &mut output)
        .await
        .expect_err("stream destination failure cannot become repair success");
    assert!(error.to_string().contains("stream reconstructed"));
    assert_eq!(output.written, 4096);

    let mut wrong_sha = part.clone();
    wrong_sha.shards[0].checksum = format_sha256(&sha256_digest(b"different committed bytes"));
    let mut cache = VerifiedShardCache::for_repair();
    let mut output = RepairHashSink::default();
    assert!(
        reconstruct_cluster_shard_into(&wrong_sha, 0, 4096, &mut cache, &mut output)
            .await
            .expect_err("block validity cannot replace the full immutable target checksum")
            .to_string()
            .contains("failed checksum validation")
    );
    tokio::fs::remove_dir_all(root)
        .await
        .expect("remove fixture");
}

#[tokio::test]
async fn incremental_probe_detects_late_corruption_truncation_and_appended_bytes() {
    let root = std::env::temp_dir().join(format!("n2-incremental-probe-{}", Ulid::new()));
    let part = streaming_fixture(&root, 1, 1, MAX_REPAIR_CHUNK_BYTES, 3, true).await;
    let shard = &part.shards[0];
    assert_eq!(shard_probe(shard, true).await, ShardProbeResult::Healthy);
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&shard.absolute_path)
        .await
        .unwrap();
    file.set_len(shard.size_bytes + 1).await.unwrap();
    assert_eq!(
        shard_probe(shard, true).await,
        ShardProbeResult::ChecksumMismatch
    );
    file.set_len(shard.size_bytes - 1).await.unwrap();
    assert_eq!(
        shard_probe(shard, true).await,
        ShardProbeResult::ChecksumMismatch
    );
    file.set_len(shard.size_bytes).await.unwrap();
    // Restoring length with a zero byte does not restore the committed SHA.
    assert_eq!(
        shard_probe(shard, true).await,
        ShardProbeResult::ChecksumMismatch
    );
    let mut impossible_size = shard.clone();
    impossible_size.size_bytes = u64::MAX;
    assert_eq!(
        shard_probe(&impossible_size, true).await,
        ShardProbeResult::ChecksumMismatch
    );
    tokio::fs::remove_dir_all(root)
        .await
        .expect("remove fixture");
}

#[tokio::test]
async fn odd_length_parity_stream_still_matches_its_recorded_checksum() {
    let root = std::env::temp_dir().join(format!("n2-odd-parity-repair-{}", Ulid::new()));
    let part = streaming_fixture(&root, 1, 1, 17, 1, false).await;
    tokio::fs::remove_file(&part.shards[1].absolute_path)
        .await
        .unwrap();
    let mut cache = VerifiedShardCache::for_repair();
    let restored = reconstruct_cluster_shard_cached(&part, 1, 17, &mut cache)
        .await
        .expect("odd target parity is padded for EC and trimmed for stored SHA");
    assert_eq!(restored.len(), 17);
    assert_eq!(
        format_sha256(&sha256_digest(&restored)),
        part.shards[1].checksum
    );
    tokio::fs::remove_dir_all(root)
        .await
        .expect("remove fixture");
}

#[test]
fn repair_verified_cache_enforces_byte_and_entry_bounds() {
    let mut cache = VerifiedShardCache::for_repair();
    for index in 0..20 {
        cache.insert_verified_block(index.to_string(), vec![0; MAX_REPAIR_CHUNK_BYTES]);
        assert!(cache.verified_block_bytes <= REPAIR_CACHE_BYTES);
        assert!(cache.verified_blocks.len() <= cache.max_verified_blocks);
    }
    assert_eq!(
        cache.verified_blocks.len(),
        REPAIR_CACHE_BYTES / MAX_REPAIR_CHUNK_BYTES
    );
    cache.clear();
    assert_eq!(cache.verified_block_bytes, 0);
    assert!(cache.verified_blocks.is_empty());
}
