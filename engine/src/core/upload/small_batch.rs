//! Payload preparation for a bounded, filesystem-synchronized small-object group.
//!
//! This module does not publish NDX or NCX, and does not acknowledge durability.
//! The group owner must hold the upload/activity and namespace operation guards,
//! open its filesystem holders before writing, and complete its persistence and
//! metadata publication boundaries before reporting any strict success.

use super::data_plane;
use super::*;

pub(crate) const MAX_OBJECT_BYTES: usize = 1024 * 1024;

pub(crate) struct Plan {
    manifest: UploadSessionManifest,
    bytes: Bytes,
}

impl Plan {
    pub(crate) fn new(
        fragment: &str,
        physical_object_id: &str,
        content_type: &str,
        bytes: Bytes,
    ) -> Result<Self, AppError> {
        if bytes.len() > MAX_OBJECT_BYTES {
            return Err(AppError::PayloadTooLarge(
                "Small-object groups accept objects up to 1 MiB".into(),
            ));
        }
        let object_key = crate::core::index_store::normalize_object_id(physical_object_id)?;
        let version_id = Ulid::new().to_string();
        let (data_shards, parity_shards, placement_roots) =
            data_plane::upload_layout(fragment, &object_key, &version_id)?;
        Ok(Self {
            manifest: UploadSessionManifest {
                fragment: fragment.to_owned(),
                object_key,
                version_id,
                session_id: Ulid::new().to_string(),
                total_size_bytes: bytes.len() as u64,
                expected_parts: 1,
                part_size_bytes: bytes.len() as u64,
                content_type: content_type.to_owned(),
                created_at: Utc::now().timestamp(),
                stripe_size_bytes: data_plane::choose_upload_stripe_size_for_len(bytes.len() as u64),
                data_shards: data_shards as u16,
                parity_shards: parity_shards as u16,
                placement_roots,
            },
            bytes,
        })
    }

    pub(crate) fn roots(&self) -> Result<Vec<PathBuf>, AppError> {
        let roots = self
            .manifest
            .placement_roots
            .iter()
            .map(|root| crate::core::storage_pool::resolve_address(root))
            .collect::<Result<Vec<_>, _>>()?;
        data_plane::ensure_registered_placement_roots(&roots)?;
        Ok(roots)
    }

    /// Complete all payload writes and userspace buffer drains, but do not sync
    /// individual files. The enclosing group supplies strict filesystem barriers.
    pub(crate) async fn write(
        &self,
        progress: Option<IoProgress>,
    ) -> Result<ObjectMetadata, AppError> {
        let manifest = &self.manifest;
        let path_manager = ObjectPathManager::new();
        let stream = futures_util::stream::iter([Ok::<_, AppError>(self.bytes.clone())]);
        let result = data_plane::write_part_blob_to_disk_from_stream(
            &path_manager,
            &manifest.fragment,
            &manifest.object_key,
            &manifest.version_id,
            0,
            manifest.total_size_bytes,
            usize::from(manifest.data_shards),
            usize::from(manifest.parity_shards),
            manifest.stripe_size_bytes,
            Some(&manifest.placement_roots),
            stream,
            None,
            false,
            progress.as_ref(),
        )
        .await;
        let part = match result {
            Ok(part) => part,
            Err(error) => {
                data_plane::cleanup_blob_version_dirs_for_version(
                    &path_manager,
                    &manifest.fragment,
                    &manifest.object_key,
                    &manifest.version_id,
                )
                .await;
                return Err(error);
            }
        };
        Ok(ObjectMetadata {
            object_key: manifest.object_key.clone(),
            fragment: manifest.fragment.clone(),
            version_id: manifest.version_id.clone(),
            upload_session_id: manifest.session_id.clone(),
            owner_uid: manifest.fragment.clone(),
            content_type: manifest.content_type.clone(),
            total_size_bytes: manifest.total_size_bytes,
            created_at: manifest.created_at,
            data_shards: manifest.data_shards,
            parity_shards: manifest.parity_shards,
            placement_generation: 0,
            parts: vec![part],
            is_delete_marker: false,
            stripe_size_bytes: manifest.stripe_size_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_groups_reject_before_looking_up_placement() {
        assert!(matches!(
            Plan::new(
                "unused-fragment",
                &crate::core::index_store::new_object_id(),
                "application/octet-stream",
                Bytes::from(vec![0; MAX_OBJECT_BYTES + 1]),
            ),
            Err(AppError::PayloadTooLarge(_))
        ));
    }

    #[tokio::test]
    async fn grouped_payloads_keep_ec_layout_checksums_and_publish_no_metadata() {
        let _test = crate::test_support::global_test_lock().lock().await;
        use crate::core::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT};

        let base = std::env::temp_dir().join(format!("n1-small-batch-{}", Ulid::new()));
        let roots = BLOB_STORE_DISKS
            .get_or_init(|| {
                (0..2)
                    .map(|index| base.join(format!("member-{index}")).display().to_string())
                    .collect()
            })
            .iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        for root in &roots {
            std::fs::create_dir_all(root).unwrap();
        }
        let control = FS_STORAGE_ROOT.get_or_init(|| base.join("control").display().to_string());
        crate::core::root_identity::reconcile_roots_for_test(
            &Path::new(control).join(".n1/topology-roots.json"),
            &roots,
        )
        .unwrap();

        let fragment = Ulid::new().to_string();
        for length in [0, 1, 4097, 64 * 1024, MAX_OBJECT_BYTES] {
            let bytes = Bytes::from(
                (0..length)
                    .map(|offset| ((offset * 31 + 17) % 251) as u8)
                    .collect::<Vec<_>>(),
            );
            let plan = Plan::new(
                &fragment,
                &crate::core::index_store::new_object_id(),
                "application/octet-stream",
                bytes.clone(),
            )
            .unwrap();
            let planned_roots = plan.roots().unwrap();
            let metadata = plan.write(None).await.unwrap();
            assert_eq!(metadata.total_size_bytes, length as u64);
            assert_eq!(metadata.parts.len(), 1);
            assert_eq!(metadata.parts[0].part_index, 0);
            assert_eq!(metadata.parts[0].size_bytes, length as u64);
            assert!(metadata.parts[0].checksum.is_none());
            assert_eq!(metadata.parts[0].shards.len(), planned_roots.len());
            assert!(Ulid::from_string(&metadata.version_id).is_ok());
            assert!(Ulid::from_string(&metadata.upload_session_id).is_ok());
            let expected_len = data_plane::final_shard_len_for_part(
                length as u64,
                usize::from(metadata.data_shards),
                metadata.stripe_size_bytes,
            );
            let mut pieces = Vec::new();
            for (slot, shard) in metadata.parts[0].shards.iter().enumerate() {
                let path =
                    crate::core::storage_pool::resolve_address(&shard.absolute_path).unwrap();
                assert!(path.starts_with(&planned_roots[slot]));
                assert_eq!(shard.shard_index, slot);
                assert_eq!(shard.is_parity, slot >= usize::from(metadata.data_shards));
                assert_eq!(shard.size_bytes, expected_len);
                assert_eq!(
                    shard.file_name,
                    data_plane::stable_shard_name(0, slot, usize::from(metadata.data_shards))
                );
                let piece = std::fs::read(&path).unwrap();
                assert_eq!(piece.len() as u64, expected_len);
                assert_eq!(shard.checksum, format!("{:08x}", crc32fast::hash(&piece)));
                assert_eq!(
                    std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
                    1
                );
                let object_dir = path.parent().unwrap().parent().unwrap();
                assert_eq!(std::fs::read_dir(object_dir).unwrap().count(), 1);
                pieces.push(piece);
            }
            let stripe = metadata.stripe_size_bytes;
            let mut restored = Vec::new();
            for offset in (0..expected_len as usize).step_by(stripe) {
                for piece in pieces.iter().take(usize::from(metadata.data_shards)) {
                    restored.extend_from_slice(&piece[offset..offset + stripe]);
                }
            }
            restored.truncate(length);
            assert_eq!(restored, bytes);
        }
        for root in &roots {
            let fragment_root =
                crate::core::fragment_identity::physical_fragment_root_on(root, &fragment);
            assert!(!fragment_root.join(".namespace").exists());
            if fragment_root.exists() {
                std::fs::remove_dir_all(fragment_root).unwrap();
            }
        }
    }
}
