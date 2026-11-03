//! Adapter for the retained archive builder. Pinned object/version identities
//! resolve through metadata; bytes are read from the public native LINK API.
use super::{BuilderError, store};
use crate::namespace::Namespace;
use tokio::io::AsyncWriteExt;
pub(super) struct Version {
    pub version_id: String,
    pub total_size_bytes: u64,
    pub is_delete_marker: bool,
    key: String,
}
pub(super) async fn read_object_version(
    fragment: &str,
    object: &str,
    version: &str,
) -> Result<Version, BuilderError> {
    let id = ulid::Ulid::from_string(fragment)
        .map_err(|_| BuilderError::InternalServerError("invalid pinned fragment".into()))?;
    let object = object.to_owned();
    let version = version.to_owned();
    let stored = store::context()
        .db
        .execute(move |db| {
            Namespace::new(db).by_identity(n1_link::PoolId(id.to_bytes()), &object, &version)
        })
        .await
        .map_err(|_| BuilderError::InternalServerError("pinned version is unavailable".into()))?;
    Ok(Version {
        version_id: stored.metadata.version_id,
        total_size_bytes: stored.metadata.total_size_bytes,
        is_delete_marker: false,
        key: stored.object_key,
    })
}
pub(super) fn create_multipart_range_reader(
    version: &Version,
    offset: u64,
    length: u64,
) -> impl tokio::io::AsyncRead + Unpin {
    let (mut output, input) = tokio::io::duplex(256 * 1024);
    let objects = store::context().objects.clone();
    let key = version.key.clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<std::io::Error>(1);
    tokio::spawn(async move {
        if objects
            .download(&key, offset, Some(length), &mut output)
            .await
            .is_err()
        {
            let _ = tx
                .send(std::io::Error::other("export object read failed"))
                .await;
        }
        let _ = output.shutdown().await;
    });
    use futures_util::{StreamExt, stream};
    tokio_util::io::StreamReader::new(
        tokio_util::io::ReaderStream::new(input)
            .chain(stream::once(async move {
                match rx.recv().await {
                    Some(error) => Err(error),
                    None => Ok(bytes::Bytes::new()),
                }
            }))
            .boxed(),
    )
}
