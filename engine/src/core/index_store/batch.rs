//! Private NDX preparation for a request-owned small-object group. Callers keep
//! normal key/backup guards and perform the group durability barriers.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use super::{ObjectMetadata, encode, io_err, ndx_file_name, normalize_object_id, object_index_dir};
use crate::core::linux_io::group::StagedFile;
use crate::enums::app_error_enum::AppError;

pub(crate) struct StagedMetadata {
    holders: Vec<StagedFile>,
}

impl StagedMetadata {
    /// The caller has made staged contents and payload durable before this
    /// publication phase. Existing NDX and published holders are never removed,
    /// even when another holder or the subsequent group barrier fails.
    pub(crate) fn publish(&mut self) -> Result<(), AppError> {
        for holder in &mut self.holders {
            holder
                .publish()
                .map_err(|error| io_err(error, "publish grouped N1 metadata"))?;
        }
        Ok(())
    }
}

/// No global root inventory or unrelated-pool cleanup: each target is derived
/// from the already-written, pinned shard addresses. The private NDX remains
/// inside its payload version so ordinary orphan-version reclamation owns it
/// after a crash before publication.
pub(crate) fn stage(metadata: &ObjectMetadata) -> Result<StagedMetadata, AppError> {
    let object_id = normalize_object_id(&metadata.object_key)?;
    if object_id != metadata.object_key || metadata.version_id.parse::<ulid::Ulid>().is_err() {
        return Err(AppError::BadRequest(
            "invalid grouped N1 object identity".into(),
        ));
    }
    if !crate::core::storage_pool::configured().is_empty()
        && crate::core::storage_pool::pool_for_metadata(metadata).is_none()
    {
        return Err(AppError::Conflict(
            "grouped N1 metadata placement does not match its pool".into(),
        ));
    }
    let width = usize::from(metadata.data_shards) + usize::from(metadata.parity_shards);
    if metadata.data_shards == 0 || metadata.parts.is_empty() {
        return Err(AppError::BadRequest(
            "grouped metadata has no complete payload placement".into(),
        ));
    }
    let mut placements = BTreeMap::new();
    for part in &metadata.parts {
        let mut slots = BTreeSet::new();
        if part.shards.len() != width {
            return Err(AppError::Conflict(
                "grouped metadata has incomplete shard placement".into(),
            ));
        }
        for shard in &part.shards {
            if shard.shard_index >= width || !slots.insert(shard.shard_index) {
                return Err(AppError::Conflict(
                    "grouped metadata has invalid shard slots".into(),
                ));
            }
            let shard_path = crate::core::storage_pool::resolve_address(&shard.absolute_path)?;
            let payload_dir = shard_path.parent().ok_or_else(|| {
                AppError::Conflict("grouped shard has no payload directory".into())
            })?;
            let root = payload_dir.ancestors().nth(6).ok_or_else(|| {
                AppError::Conflict("grouped shard is outside its object layout".into())
            })?;
            let object_dir = object_index_dir(root, &metadata.fragment, &metadata.object_key)?;
            if payload_dir != object_dir.join(&metadata.version_id)
                || shard_path.file_name().and_then(|name| name.to_str())
                    != Some(shard.file_name.as_str())
            {
                return Err(AppError::Conflict(
                    "grouped shard does not match its immutable version".into(),
                ));
            }
            let directory = fs::symlink_metadata(payload_dir)
                .map_err(|error| io_err(error, "check grouped N1 payload directory"))?;
            if !directory.is_dir() {
                return Err(AppError::Conflict(
                    "grouped N1 payload directory is not a real directory".into(),
                ));
            }
            placements.insert(object_dir, payload_dir.to_path_buf());
        }
    }
    stage_on_placements(metadata, placements)
}

fn stage_on_placements(
    metadata: &ObjectMetadata,
    placements: BTreeMap<PathBuf, PathBuf>,
) -> Result<StagedMetadata, AppError> {
    let bytes = encode(metadata)?;
    let name = ndx_file_name(metadata);
    let holders = placements
        .into_iter()
        .map(|(object_dir, payload_dir)| {
            StagedFile::create(&object_dir.join(&name), &payload_dir, &bytes)
                .map_err(|error| io_err(error, "stage grouped N1 metadata"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StagedMetadata { holders })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndx_stays_private_in_payload_until_publication_and_uses_existing_encoding() {
        let root = std::env::temp_dir().join(format!("n1-group-ndx-{}", ulid::Ulid::new()));
        let object_dir = root.join("object");
        let version_id = ulid::Ulid::new().to_string();
        let version_dir = object_dir.join(&version_id);
        fs::create_dir_all(&version_dir).unwrap();
        let metadata = ObjectMetadata {
            object_key: ulid::Ulid::new().to_string(),
            fragment: "fragment".into(),
            version_id,
            upload_session_id: ulid::Ulid::new().to_string(),
            owner_uid: "owner".into(),
            content_type: "application/octet-stream".into(),
            total_size_bytes: 0,
            created_at: 1,
            data_shards: 1,
            parity_shards: 0,
            placement_generation: 1,
            parts: Vec::new(),
            is_delete_marker: false,
            stripe_size_bytes: 4096,
        };
        let placements = BTreeMap::from([(object_dir.clone(), version_dir.clone())]);
        let mut staged = stage_on_placements(&metadata, placements).unwrap();
        let final_path = object_dir.join(ndx_file_name(&metadata));
        assert!(!final_path.exists());
        assert_eq!(fs::read_dir(&object_dir).unwrap().count(), 1);
        let private = fs::read_dir(&version_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert!(
            private
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(".n1-batch-")
        );
        assert_eq!(fs::read(&private).unwrap(), encode(&metadata).unwrap());
        // Injected successful first barrier: this test concerns publication,
        // not actual crash persistence (the group tests cover failure ordering).
        staged.publish().unwrap();
        drop(staged);
        assert!(!private.exists());
        assert_eq!(fs::read(final_path).unwrap(), encode(&metadata).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn partial_stage_failure_leaves_no_authoritative_ndx_or_private_file() {
        let root = std::env::temp_dir().join(format!("n1-group-ndx-fail-{}", ulid::Ulid::new()));
        let object_dir = root.join("a-object");
        let version_dir = object_dir.join("version");
        fs::create_dir_all(&version_dir).unwrap();
        let metadata: ObjectMetadata = super::super::decode(
            &super::super::encode(&ObjectMetadata {
                object_key: "object".into(),
                fragment: "fragment".into(),
                version_id: "version".into(),
                upload_session_id: "session".into(),
                owner_uid: "owner".into(),
                content_type: "".into(),
                total_size_bytes: 0,
                created_at: 1,
                data_shards: 1,
                parity_shards: 0,
                placement_generation: 1,
                parts: Vec::new(),
                is_delete_marker: false,
                stripe_size_bytes: 4096,
            })
            .unwrap(),
        )
        .unwrap();
        let placements = BTreeMap::from([
            (object_dir.clone(), version_dir.clone()),
            (root.join("z-missing"), root.join("z-missing/version")),
        ]);
        assert!(stage_on_placements(&metadata, placements).is_err());
        assert!(!object_dir.join(ndx_file_name(&metadata)).exists());
        assert_eq!(fs::read_dir(version_dir).unwrap().count(), 0);
        fs::remove_dir_all(root).unwrap();
    }
}
