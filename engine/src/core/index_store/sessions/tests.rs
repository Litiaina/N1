use super::*;
use crate::core::disk_manager::STORAGE_DISKS;
use crate::test_support::global_test_lock;
use std::os::unix::fs::MetadataExt;

#[tokio::test]
async fn session_cleanup_preserves_terminal_ownership_fences_and_unknown_records_on_every_root() {
    let _global = global_test_lock().lock().await;
    STORAGE_DISKS.get_or_init(|| {
        let root = std::env::temp_dir().join(format!("n2-session-fences-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        vec![root.to_string_lossy().into_owned()]
    });
    let fragment = Ulid::new().to_string();
    for state in ["aborted", "committed"] {
        let manifest = UploadSessionManifest {
            fragment: fragment.clone(),
            object_key: Ulid::new().to_string(),
            packed_ext_u64: 0,
            version_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            total_size_bytes: 1,
            expected_parts: 1,
            part_size_bytes: 1,
            content_type: "application/octet-stream".into(),
            created_at: 1,
            pool_id: "pool-a".into(),
            pool_generation: 1,
            stripe_size_bytes: 4096,
            membership_epoch: 1,
            data_shards: 1,
            parity_shards: 1,
        };
        open_session(&manifest, None).unwrap();
        write_pending_binding(&PendingUploadBinding {
            fragment: fragment.clone(),
            logical_path: "report.bin".into(),
            physical_object_id: manifest.object_key.clone(),
            version_id: manifest.version_id.clone(),
            session_id: manifest.session_id.clone(),
            created_at_unix: 1,
        })
        .unwrap();
        commit_session_part(
            &fragment,
            &manifest.version_id,
            &PartMetadata {
                part_index: 0,
                size_bytes: 1,
                created_at: 1,
                shards: Vec::new(),
                checksum: None,
            },
        )
        .unwrap();
        commit_session_part_receipt(
            &fragment,
            &manifest.version_id,
            &SessionPartReceipt {
                part_index: 0,
                size_bytes: 1,
                shard_size_bytes: 4096,
                created_at: 1,
                checksum: Some([7; 32]),
                shard_checksums: vec![[7; 32], [8; 32]],
            },
        )
        .unwrap();
        let mut fences = Vec::new();
        for root in index_roots() {
            let dir = session_dir(&root, &fragment, &manifest.version_id);
            fs::create_dir_all(&dir).unwrap();
            let fence = dir.join("ownership.n2");
            let bytes = format!("durable-{state}-fence").into_bytes();
            fs::write(&fence, &bytes).unwrap();
            fs::write(
                dir.join("future-lifecycle-record"),
                b"unknown ownership evidence",
            )
            .unwrap();
            fs::create_dir_all(dir.join("unknown-subdirectory")).unwrap();
            fences.push((fence.clone(), fs::metadata(&fence).unwrap().ino(), bytes));
        }
        assert!(remove_session(&fragment, &manifest.version_id).unwrap());
        assert!(
            load_session(&fragment, &manifest.version_id)
                .unwrap()
                .is_none()
        );
        assert!(!remove_session(&fragment, &manifest.version_id).unwrap());
        for (fence, inode, bytes) in fences {
            assert_eq!(fs::read(&fence).unwrap(), bytes);
            assert_eq!(fs::metadata(&fence).unwrap().ino(), inode);
            let dir = fence.parent().unwrap();
            assert_eq!(
                fs::read(dir.join("future-lifecycle-record")).unwrap(),
                b"unknown ownership evidence"
            );
            assert!(dir.join("unknown-subdirectory").is_dir());
            assert!(
                read_dir_names(dir)
                    .unwrap()
                    .iter()
                    .all(|name| !name.ends_with(".receipt")
                        && name != MANIFEST_FILE
                        && name != BINDING_FILE)
            );
        }
    }
    for root in index_roots() {
        fs::remove_dir_all(fragment_root_on(&root, &fragment)).unwrap();
    }
}
