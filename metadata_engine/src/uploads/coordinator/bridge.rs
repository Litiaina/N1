//! Verify each external file before forwarding any of its bytes. Keeping the
//! verification and forwarding adjacent bounds the cache working set to a part,
//! rather than walking the entire object twice before N1 can make progress.
use super::*;
use crate::object_store::ObjectStoreError;
use futures_util::{StreamExt, TryStreamExt};
use std::path::Path;

pub(super) async fn verified_file(
    app: &App,
    fragment: PoolId,
    version: &str,
    dir: &Path,
    index: u64,
    capacity: usize,
    memory: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> ApiResult<tokio::fs::File> {
    let key = RecordKey::new(fragment, format!("upload-part/{version}/{index:06}"))?;
    let expected: Part = app
        .db
        .execute(move |db| {
            fragments::decode(
                db.snapshot()
                    .get(Table::Reclamation, &key)?
                    .ok_or(crate::Error::NotFound)?,
            )
        })
        .await?;
    let path = dir.join(format!("{index}.part"));
    tokio::task::spawn_blocking(move || {
        // A cancelled request cannot return this buffer's budget while the
        // owned filesystem worker is still using it, including stalled reads.
        let _memory = memory;
        // One owned worker hashes a complete file without a blocking-pool hop
        // and Tokio File's extra copy on every verification buffer. Rewind the
        // same descriptor, so forwarding uses the file that was just verified.
        let mut file = std::fs::File::open(path)?;
        let mut buffer = vec![0; capacity];
        let mut part = std::io::Read::take(&mut file, expected.bytes.saturating_add(1));
        let mut hash = Sha256::new();
        let mut bytes = 0;
        loop {
            let n = std::io::Read::read(&mut part, &mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
            bytes += n as u64;
        }
        if bytes != expected.bytes || hex::encode(hash.finalize()) != expected.sha256 {
            return Err(ApiError(
                axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                "Staged part failed its checksum".into(),
            ));
        }
        std::io::Seek::rewind(&mut file)?;
        Ok(tokio::fs::File::from_std(file))
    })
    .await
    .map_err(|_| {
        ApiError(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Staged verification worker failed".into(),
        )
    })?
}

pub(super) fn source(
    app: Arc<App>,
    fragment: PoolId,
    version: String,
    dir: PathBuf,
    parts: u64,
    capacity: usize,
    memory: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> impl AsyncRead + Unpin {
    tokio_util::io::StreamReader::new(
        futures_util::stream::iter(0..parts)
            .then(move |index| {
                let app = app.clone();
                let dir = dir.clone();
                let version = version.clone();
                let memory = memory.clone();
                async move {
                    verified_file(&app, fragment, &version, &dir, index, capacity, memory)
                        .await
                        .map(|file| tokio_util::io::ReaderStream::with_capacity(file, capacity))
                        .map_err(std::io::Error::other)
                }
            })
            .try_flatten()
            .boxed(),
    )
}

/// A local checksum/receipt error surfaced through the native source reader is
/// still an M1 error (for example 422), not a generic upstream transport failure.
pub(super) fn forward_error(error: ObjectStoreError) -> ApiError {
    if let ObjectStoreError::Native(n1_native_client::NativeClientError::Io(io)) = &error
        && let Some(source) = io.get_ref().and_then(|e| e.downcast_ref::<ApiError>())
    {
        return ApiError(source.0, source.1.clone());
    }
    error.into()
}
