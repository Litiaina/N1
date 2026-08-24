use super::*;

pub(super) fn io_err(err: std::io::Error, context: &str) -> AppError {
    AppError::Io(err, context.to_string())
}

#[inline]
pub(super) fn is_not_found(error: &AppError) -> bool {
    matches!(error, AppError::NotFound)
        || matches!(error, AppError::Io(source, _) if source.kind() == std::io::ErrorKind::NotFound)
}

pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, AppError> {
    Ok(rmp_serde::to_vec(value)?)
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, AppError> {
    Ok(rmp_serde::from_slice(bytes)?)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DiskObjectMetadata {
    format_version: u32,
    object_id: String,
    packed_ext_u64: u64,
    fragment: String,
    version_id: String,
    upload_session_id: String,
    owner_uid: String,
    pub(super) content_type: String,
    total_size_bytes: u64,
    created_at: i64,
    pool_id: String,
    pool_generation: u64,
    membership_epoch: u64,
    data_shards: u16,
    parity_shards: u16,
    parts: Vec<crate::core::disk_manager::PartMetadata>,
    is_delete_marker: bool,
    stripe_size_bytes: usize,
    payload_sha256: Sha256Digest,
}

impl From<&ObjectMetadata> for DiskObjectMetadata {
    fn from(meta: &ObjectMetadata) -> Self {
        Self {
            format_version: NCDX_FORMAT_VERSION,
            object_id: meta.object_key.clone(),
            packed_ext_u64: meta.packed_ext_u64,
            fragment: meta.fragment.clone(),
            version_id: meta.version_id.clone(),
            upload_session_id: meta.upload_session_id.clone(),
            owner_uid: meta.owner_uid.clone(),
            content_type: meta.content_type.clone(),
            total_size_bytes: meta.total_size_bytes,
            created_at: meta.created_at,
            pool_id: meta.pool_id.clone(),
            pool_generation: meta.pool_generation,
            membership_epoch: meta.membership_epoch,
            data_shards: meta.data_shards,
            parity_shards: meta.parity_shards,
            parts: meta.parts.clone(),
            is_delete_marker: meta.is_delete_marker,
            stripe_size_bytes: meta.stripe_size_bytes,
            payload_sha256: [0; 32],
        }
    }
}

impl From<DiskObjectMetadata> for ObjectMetadata {
    fn from(meta: DiskObjectMetadata) -> Self {
        Self {
            object_key: meta.object_id,
            packed_ext_u64: meta.packed_ext_u64,
            fragment: meta.fragment,
            version_id: meta.version_id,
            upload_session_id: meta.upload_session_id,
            owner_uid: meta.owner_uid,
            content_type: meta.content_type,
            total_size_bytes: meta.total_size_bytes,
            created_at: meta.created_at,
            pool_id: meta.pool_id,
            pool_generation: meta.pool_generation,
            membership_epoch: meta.membership_epoch,
            data_shards: meta.data_shards,
            parity_shards: meta.parity_shards,
            parts: meta.parts,
            is_delete_marker: meta.is_delete_marker,
            stripe_size_bytes: meta.stripe_size_bytes,
        }
    }
}

pub(super) fn encode_disk_object_metadata(metadata: &ObjectMetadata) -> Result<Vec<u8>, AppError> {
    let mut disk = DiskObjectMetadata::from(metadata);
    disk.validate_fields()?;
    disk.payload_sha256 = disk.expected_sha256()?;
    Ok(rmp_serde::to_vec_named(&disk)?)
}

impl DiskObjectMetadata {
    fn validate_fields(&self) -> Result<(), AppError> {
        if self.format_version != NCDX_FORMAT_VERSION {
            return Err(AppError::Conflict(format!(
                "NCDX format {} is not N2 3.4 format {}",
                self.format_version, NCDX_FORMAT_VERSION
            )));
        }
        if self.object_id.trim().is_empty()
            || self.fragment.trim().is_empty()
            || self.version_id.trim().is_empty()
            || self.pool_id.trim().is_empty()
            || self.pool_generation == 0
            || self.data_shards == 0
            || self.parity_shards == 0
        {
            return Err(AppError::Conflict(
                "NCDX requires object, fragment, version, pool, and static EC identities".into(),
            ));
        }
        for part in &self.parts {
            if let Some(checksum) = part.checksum.as_deref()
                && parse_sha256_hex(checksum).is_none()
            {
                return Err(AppError::Conflict(format!(
                    "NCDX part {} has an invalid SHA-256 checksum",
                    part.part_index
                )));
            }
            if part
                .shards
                .iter()
                .any(|shard| parse_sha256_hex(&shard.checksum).is_none())
            {
                return Err(AppError::Conflict(format!(
                    "NCDX part {} contains a shard without a valid SHA-256 checksum",
                    part.part_index
                )));
            }
        }
        Ok(())
    }

    fn expected_sha256(&self) -> Result<Sha256Digest, AppError> {
        let mut unsigned = self.clone();
        unsigned.payload_sha256 = [0; 32];
        let payload = rmp_serde::to_vec_named(&unsigned)?;
        Ok(sha256_digest(&payload))
    }

    pub(super) fn validate(&self) -> Result<(), AppError> {
        self.validate_fields()?;
        let expected = self.expected_sha256()?;
        if self.payload_sha256 != expected {
            return Err(AppError::Conflict(format!(
                "NCDX integrity mismatch: expected {}, recorded {}",
                format_sha256(&expected),
                format_sha256(&self.payload_sha256)
            )));
        }
        Ok(())
    }
}
