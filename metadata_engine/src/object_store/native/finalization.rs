//! Recover lost volatile completion through N1's public paginated status API.
//! Healthy finalization sends one command. Only N1's explicit reconciliation
//! precondition permits a bounded recovery pass and one finalization retry.
use super::{
    Connection, NativeObjectStore, ObjectFinalization, ObjectStoreError, ObjectStoreResult,
    ObjectUpload, ObjectUploadStatus, check_finalization, decode,
};
use n1_link::noa::Command;
use n1_native_client::NativeClientError;

const STATUS_PAGE: u64 = 4096;
const RECOVERY_PAGE_BYTES: u64 = 64 * 1024 * 1024;
// Match this documented precondition precisely. An arbitrary 409 (expired key,
// conflicting bytes or partial repair) must not start an object-sized scan.
const COMPLETION_UNAVAILABLE: &str =
    "completion state unavailable; query upload status or resume before finalizing";

/// Bound payload validation per native RPC, rather than asking one 4,096-part
/// response to scan hundreds of GiB. At least one declared part must advance.
pub(super) fn status_page_limit(upload: &ObjectUpload, requested: u64) -> ObjectStoreResult<u64> {
    if upload.part_bytes == 0 {
        return Err(ObjectStoreError::Invalid(
            "upload part size must be positive",
        ));
    }
    Ok(requested.min((RECOVERY_PAGE_BYTES / upload.part_bytes).clamp(1, STATUS_PAGE)))
}

impl NativeObjectStore {
    pub async fn finalize(&self, upload: &ObjectUpload) -> ObjectStoreResult<ObjectFinalization> {
        // Own one admitted authenticated exchange across recovery, so shutdown
        // cannot strand an accepted operation between status pages and retry.
        let mut connection = self.connection().await?;
        let result = self.finalize_owned(&mut connection, upload).await;
        if result.is_ok()
            || matches!(&result, Err(ObjectStoreError::Native(NativeClientError::Native { code, .. })) if code != "401")
        {
            connection.release();
        }
        result
    }

    async fn finalize_owned(
        &self,
        connection: &mut Connection,
        upload: &ObjectUpload,
    ) -> ObjectStoreResult<ObjectFinalization> {
        let command = || Command::UploadFinalize {
            object_key: upload.object_key.clone(),
        };
        let response = match connection.client.control(command()).await {
            Err(error)
                if matches!(&error, NativeClientError::Native { code, message }
                    if code == "409" && message == COMPLETION_UNAVAILABLE) =>
            {
                if !self.reconcile_completion(connection, upload).await? {
                    return Err(self.failed(error));
                }
                // No loop: another eviction/error remains visible to the
                // caller. Never replay payloads or allocate another identity.
                connection
                    .client
                    .control(command())
                    .await
                    .map_err(|error| self.failed(error))?
            }
            Err(error) => return Err(self.failed(error)),
            Ok(response) => response,
        };
        let result = decode(response)?;
        check_finalization(upload, &result)?;
        Ok(result)
    }

    async fn reconcile_completion(
        &self,
        connection: &mut Connection,
        upload: &ObjectUpload,
    ) -> ObjectStoreResult<bool> {
        let limit = status_page_limit(upload, STATUS_PAGE)?;
        let mut start = 0;
        while start < upload.parts {
            let report: ObjectUploadStatus = decode(
                connection
                    .client
                    .control(Command::UploadStatus {
                        object_key: upload.object_key.clone(),
                        start,
                        limit,
                    })
                    .await
                    .map_err(|error| self.failed(error))?,
            )?;
            let end = start.saturating_add(limit).min(upload.parts);
            if report.parts != upload.parts
                || report.checked_start != start
                || report.checked_end != end
                || report
                    .missing_parts
                    .iter()
                    .any(|part| *part < start || *part >= end)
            {
                return Err(ObjectStoreError::Contract(
                    "object service returned a different completion status page",
                ));
            }
            if !report.unavailable_members.is_empty() || !report.missing_parts.is_empty() {
                return Ok(false);
            }
            start = end;
        }
        Ok(true)
    }
}
