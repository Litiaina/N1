use super::*;
use crate::core::disk_manager::{FS_STORAGE_ROOT, PartMetadata, STORAGE_DISKS, ShardInfo};
use std::collections::HashMap;
use std::sync::Arc;

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[tokio::test]
async fn shard_integrity_rejects_missing_or_non_sha256_checksums() {
    let path = std::env::temp_dir().join(format!("n2-sha256-check-{}", Ulid::new()));
    fs::write(&path, b"verified bytes")
        .await
        .expect("write test shard");
    let mut file = File::open(&path).await.expect("open test shard");
    assert!(!verify_shard_integrity(&mut file, "").await);
    assert!(!verify_shard_integrity(&mut file, "352441c2").await);
    assert!(verify_shard_integrity(&mut file, &sha256_hex(b"verified bytes")).await);
    let _ = fs::remove_file(path).await;
}

#[tokio::test]
async fn read_or_recover_chunk_restores_single_data_shard_for_nine_byte_object() {
    let root = std::env::temp_dir().join(format!("n2-recovery-nine-byte-{}", Ulid::new()));
    let matrix_base = std::env::temp_dir().join(format!("n2-matrix-{}", std::process::id()));
    let index_roots = vec![
        matrix_base.join("index_1").to_string_lossy().to_string(),
        matrix_base.join("index_2").to_string_lossy().to_string(),
    ];
    let blob_roots = vec![
        matrix_base.join("blob_1").to_string_lossy().to_string(),
        matrix_base.join("blob_2").to_string_lossy().to_string(),
        matrix_base.join("blob_3").to_string_lossy().to_string(),
    ];
    for root in index_roots.iter().chain(blob_roots.iter()) {
        fs::create_dir_all(root).await.expect("create suite root");
    }
    if STORAGE_DISKS.get().is_none() {
        STORAGE_DISKS
            .set(index_roots)
            .expect("set suite index roots");
    }
    if STORAGE_DISKS.get().is_none() {
        STORAGE_DISKS.set(blob_roots).expect("set suite blob roots");
    }
    if FS_STORAGE_ROOT.get().is_none() {
        FS_STORAGE_ROOT
            .set(matrix_base.to_string_lossy().to_string())
            .expect("set suite fs root");
    }
    let object_id = "01KRECOVERYOBJECT0000000000";
    let version_id = "01KRECOVERYVERSION000000000";
    let parity_dir = crate::core::disk_manager::ObjectPathManager::new()
        .get_shard_dir_on_disk(&root, "frag", object_id, version_id);
    let missing_dir = crate::core::disk_manager::ObjectPathManager::new().get_shard_dir_on_disk(
        &root.join("missing"),
        "frag",
        object_id,
        version_id,
    );
    fs::create_dir_all(&parity_dir)
        .await
        .expect("create parity dir");

    let mut shard_bytes = vec![0u8; 262_144];
    shard_bytes[..9].copy_from_slice(b"trash-me\n");
    let checksum = sha256_hex(&shard_bytes);

    let parity_path = parity_dir.join("part_0_shard_1.parity");
    fs::write(&parity_path, &shard_bytes)
        .await
        .expect("write parity shard");

    let part = Arc::new(PartMetadata {
        part_index: 0,
        size_bytes: 9,
        created_at: 0,
        checksum: None,
        shards: vec![
            ShardInfo {
                shard_index: 0,
                file_name: "part_0_shard_0.shard".to_string(),
                absolute_path: missing_dir
                    .join("part_0_shard_0.shard")
                    .to_string_lossy()
                    .to_string(),
                size_bytes: 262_144,
                checksum: checksum.clone(),
                block_checksums: Vec::new(),
                checksum_block_size: 0,
                is_parity: false,
            },
            ShardInfo {
                shard_index: 1,
                file_name: "part_0_shard_1.parity".to_string(),
                absolute_path: parity_path.to_string_lossy().to_string(),
                size_bytes: 262_144,
                checksum,
                block_checksums: Vec::new(),
                checksum_block_size: 0,
                is_parity: true,
            },
        ],
    });

    let bytes = read_or_recover_chunk(&mut HashMap::new(), &missing_dir, part, 0, 0, 9, 262_144)
        .await
        .expect("recover missing shard");

    assert_eq!(bytes, b"trash-me\n");

    let bytes = read_or_recover_chunk_streaming(
        &mut HashMap::new(),
        &mut HashMap::new(),
        &missing_dir,
        Arc::new(PartMetadata {
            part_index: 0,
            size_bytes: 9,
            created_at: 0,
            checksum: None,
            shards: vec![
                ShardInfo {
                    shard_index: 0,
                    file_name: "part_0_shard_0.shard".to_string(),
                    absolute_path: missing_dir
                        .join("part_0_shard_0.shard")
                        .to_string_lossy()
                        .to_string(),
                    size_bytes: 262_144,
                    checksum: sha256_hex(&shard_bytes),
                    block_checksums: Vec::new(),
                    checksum_block_size: 0,
                    is_parity: false,
                },
                ShardInfo {
                    shard_index: 1,
                    file_name: "part_0_shard_1.parity".to_string(),
                    absolute_path: parity_path.to_string_lossy().to_string(),
                    size_bytes: 262_144,
                    checksum: sha256_hex(&shard_bytes),
                    block_checksums: Vec::new(),
                    checksum_block_size: 0,
                    is_parity: true,
                },
            ],
        }),
        0,
        0,
        9,
        262_144,
    )
    .await
    .expect("streaming fallback recovers missing shard");

    assert_eq!(bytes, b"trash-me\n");

    let _ = fs::remove_dir_all(&root).await;
}
