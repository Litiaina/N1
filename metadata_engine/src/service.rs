//! Application coordination between durable metadata and the public LINK API.
//! NOA authentication/permission adapters must authorize requests before calling
//! this layer. No object-engine implementation dependency is required.
use crate::{
    Error, MetadataExecutor, Result,
    namespace::{Namespace, PendingUpload, PublishedVersion, UploadHandle},
    object_store::{NativeObjectStore, ObjectStoreError, ObjectUploadStatus},
};
use n1_link::PoolId;
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use ulid::Ulid;
pub(crate) mod batch;
#[cfg(test)]
#[path = "service/performance.rs"]
mod performance;

pub struct UploadRequest {
    pub fragment: PoolId,
    pub path: String,
    pub content_type: String,
    pub pool: Option<PoolId>,
    pub total_bytes: u64,
    pub part_bytes: u64,
    pub durable: bool,
    pub ttl_seconds: u64,
}

#[derive(Debug)]
pub enum ServiceError {
    Metadata(Error),
    Object(ObjectStoreError),
}
impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metadata(e) => e.fmt(f),
            Self::Object(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for ServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Metadata(e) => e,
            Self::Object(e) => e,
        })
    }
}
impl From<Error> for ServiceError {
    fn from(e: Error) -> Self {
        Self::Metadata(e)
    }
}
impl From<ObjectStoreError> for ServiceError {
    fn from(e: ObjectStoreError) -> Self {
        Self::Object(e)
    }
}
pub type ServiceResult<T> = std::result::Result<T, ServiceError>;

/// Names and pending transaction state are owned here. The backend sees only
/// the same self-describing object and upload keys used by standalone clients.
#[derive(Clone)]
pub struct MetadataService {
    metadata: MetadataExecutor,
    objects: NativeObjectStore,
    accepting: Arc<AtomicBool>,
    lifecycle: Arc<RwLock<()>>,
}

impl MetadataService {
    pub(crate) async fn recover_publication(
        &self,
        fragment: PoolId,
        version: &str,
        session: &str,
    ) -> ServiceResult<PublishedVersion> {
        let _operation = self.admitted().await?;
        let pending = self.pending(fragment, version, Some(session)).await?;
        let stat = self.objects.stat(&pending.upload.object_key).await?;
        let finalized = crate::object_store::ObjectFinalization {
            object_key: pending.upload.object_key,
            already_finalized: true,
            total_bytes: stat.total_bytes,
        };
        let version = version.to_owned();
        let session = session.to_owned();
        Ok(self
            .metadata
            .execute(move |db| {
                Namespace::new(db).publish(fragment, &version, &session, &finalized, &stat)
            })
            .await?)
    }
    pub(crate) fn objects(&self) -> NativeObjectStore {
        self.objects.clone()
    }
    pub fn new(metadata: MetadataExecutor, objects: NativeObjectStore) -> Self {
        Self {
            metadata,
            objects,
            accepting: Arc::new(AtomicBool::new(true)),
            lifecycle: Arc::new(RwLock::new(())),
        }
    }

    async fn admitted(&self) -> Result<OwnedRwLockReadGuard<()>> {
        let guard = self.lifecycle.clone().read_owned().await;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(guard)
    }

    pub async fn begin_upload(&self, request: UploadRequest) -> ServiceResult<UploadHandle> {
        let _operation = self.admitted().await?;
        // Validate the logical input before any network work. Init has no
        // payload side effects, but cannot reserve an invalid logical key.
        crate::namespace::store::validate_upload(&request.path, &request.content_type)?;
        let upload = self
            .objects
            .init(
                request.pool,
                request.total_bytes,
                request.part_bytes,
                request.durable,
                request.ttl_seconds,
            )
            .await?;
        let version_id = Ulid::new().to_string();
        let session_id = Ulid::new().to_string();
        let created = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Invalid("system time precedes the Unix epoch".into()))?
            .as_secs() as i64;
        Ok(self
            .metadata
            .execute(move |db| {
                Namespace::new(db).reserve_upload(
                    request.fragment,
                    &request.path,
                    &version_id,
                    &session_id,
                    &upload,
                    &request.content_type,
                    created,
                )
            })
            .await?)
    }

    pub(crate) async fn pending(
        &self,
        fragment: PoolId,
        version_id: &str,
        session_id: Option<&str>,
    ) -> Result<PendingUpload> {
        let version = version_id.to_owned();
        let session = session_id.map(str::to_owned);
        self.metadata
            .execute(move |db| Namespace::new(db).pending(fragment, &version, session.as_deref()))
            .await
    }

    pub async fn upload_part<R: AsyncRead + Unpin>(
        &self,
        fragment: PoolId,
        version_id: &str,
        part: u64,
        length: u64,
        source: &mut R,
    ) -> ServiceResult<()> {
        let _operation = self.admitted().await?;
        let pending = self.pending(fragment, version_id, None).await?;
        if pending.completed {
            return Err(Error::NameConflict("upload is already finalized".into()).into());
        }
        self.objects
            .part(&pending.upload, part, length, source)
            .await?;
        Ok(())
    }

    pub async fn upload_status(
        &self,
        fragment: PoolId,
        version_id: &str,
        start: u64,
        limit: u64,
    ) -> ServiceResult<ObjectUploadStatus> {
        let _operation = self.admitted().await?;
        let pending = self.pending(fragment, version_id, None).await?;
        Ok(self.objects.status(&pending.upload, start, limit).await?)
    }

    pub async fn finalize(
        &self,
        fragment: PoolId,
        version_id: &str,
        session_id: &str,
    ) -> ServiceResult<PublishedVersion> {
        let _operation = self.admitted().await?;
        let pending = self.pending(fragment, version_id, Some(session_id)).await?;
        let finalized = self.objects.finalize(&pending.upload).await?;
        let stat = self.objects.stat(&pending.upload.object_key).await?;
        let version = version_id.to_owned();
        let session = session_id.to_owned();
        Ok(self
            .metadata
            .execute(move |db| {
                Namespace::new(db).publish(fragment, &version, &session, &finalized, &stat)
            })
            .await?)
    }

    /// The init handle is durably saved before this stream starts. A lost
    /// response between physical publication and metadata publication is resolved
    /// by finalize with that same handle, without allocating another identity.
    pub async fn put<R: AsyncRead + Unpin>(
        &self,
        fragment: PoolId,
        version_id: &str,
        session_id: &str,
        source: &mut R,
    ) -> ServiceResult<PublishedVersion> {
        let _operation = self.admitted().await?;
        let pending = self.pending(fragment, version_id, Some(session_id)).await?;
        let finalized = self.objects.put(&pending.upload, source).await?;
        let stat = self.objects.stat(&pending.upload.object_key).await?;
        let version = version_id.to_owned();
        let session = session_id.to_owned();
        Ok(self
            .metadata
            .execute(move |db| {
                Namespace::new(db).publish(fragment, &version, &session, &finalized, &stat)
            })
            .await?)
    }

    pub async fn download<W: AsyncWrite + Unpin>(
        &self,
        fragment: PoolId,
        path: &str,
        version_id: Option<&str>,
        offset: u64,
        length: Option<u64>,
        destination: &mut W,
    ) -> ServiceResult<u64> {
        let _operation = self.admitted().await?;
        let path = path.to_owned();
        let version = version_id.map(str::to_owned);
        let stored = self
            .metadata
            .execute(move |db| Namespace::new(db).resolve(fragment, &path, version.as_deref()))
            .await?;
        self.download_resolved(fragment, stored, offset, length, destination)
            .await
    }

    /// A placement switch can retire the old key after name resolution but
    /// before its native read starts. Re-resolve only that immutable version,
    /// only on an absent source and before destination bytes; healthy reads add no RPC.
    pub(crate) async fn download_resolved<W: AsyncWrite + Unpin>(
        &self,
        fragment: PoolId,
        stored: crate::namespace::store::StoredVersion,
        offset: u64,
        length: Option<u64>,
        destination: &mut W,
    ) -> ServiceResult<u64> {
        let mut writer = crate::object_store::DownloadWriter {
            inner: destination,
            started: false,
            failed: false,
        };
        match self
            .objects
            .download(&stored.object_key, offset, length, &mut writer)
            .await
        {
            Err(error) if error.status() == 404 && !writer.started && !writer.failed => {
                let object = stored.metadata.object_id.clone();
                let version = stored.metadata.version_id.clone();
                let current = self
                    .metadata
                    .execute(move |db| Namespace::new(db).by_identity(fragment, &object, &version))
                    .await?;
                if current.object_key == stored.object_key || current.metadata != stored.metadata {
                    return Err(error.into());
                }
                Ok(self
                    .objects
                    .download(&current.object_key, offset, length, &mut writer)
                    .await?)
            }
            result => Ok(result?),
        }
    }

    pub async fn shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        let _drained = self.lifecycle.write().await;
        self.objects.shutdown().await;
        self.metadata.shutdown().await;
    }
}
