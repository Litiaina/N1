//! Bounded durable publication groups reuse LINK's tested batch write path.
use super::{MetadataService, ServiceResult};
use crate::namespace::{
    Namespace, PublishedVersion,
    store::{Publication, Reservation},
};
use futures_util::{StreamExt, TryStreamExt};
use n1_link::PoolId;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};

pub(crate) struct SmallObject {
    pub path: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}
struct Body<'a> {
    objects: &'a [SmallObject],
    index: usize,
    offset: usize,
}
impl AsyncRead for Body<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        while self.index < self.objects.len() && buffer.remaining() > 0 {
            let object = &self.objects[self.index];
            let remaining = &object.bytes[self.offset..];
            if remaining.is_empty() {
                self.index += 1;
                self.offset = 0;
                continue;
            }
            let length = remaining.len().min(buffer.remaining());
            buffer.put_slice(&remaining[..length]);
            self.offset += length;
        }
        Poll::Ready(Ok(()))
    }
}
impl MetadataService {
    pub(crate) async fn put_small_batch(
        &self,
        fragment: PoolId,
        pool: PoolId,
        stripe: u64,
        objects: &[SmallObject],
        durable: bool,
    ) -> ServiceResult<Vec<PublishedVersion>> {
        let _operation = self.admitted().await?;
        if objects.is_empty()
            || objects.len() > 64
            || objects.iter().map(|o| o.bytes.len()).sum::<usize>() > 8 * 1024 * 1024
        {
            return Err(
                crate::Error::Invalid("invalid small-object publication group".into()).into(),
            );
        }
        for object in objects {
            crate::namespace::store::validate_upload(&object.path, &object.content_type)?;
        }
        let created = crate::middleware::auth::now() as i64;
        let inputs = objects
            .iter()
            .map(|o| (o.path.clone(), o.content_type.clone(), o.bytes.len() as u64))
            .collect::<Vec<_>>();
        let client = self.objects.clone();
        let reservations: Vec<Reservation> = futures_util::stream::iter(inputs)
            .map(move |(path, content_type, bytes)| {
                let client = client.clone();
                async move {
                    let part = bytes.max(1).div_ceil(stripe) * stripe;
                    Ok::<_, super::ServiceError>(Reservation {
                        path,
                        content_type,
                        version: ulid::Ulid::new().to_string(),
                        session: ulid::Ulid::new().to_string(),
                        upload: client.init(Some(pool), bytes, part, durable, 86400).await?,
                        created_at: created,
                    })
                }
            })
            .buffered(16)
            .try_collect()
            .await?;
        let plans = reservations
            .iter()
            .map(|r| r.upload.clone())
            .collect::<Vec<_>>();
        let handles = self
            .metadata
            .execute(move |db| Namespace::new(db).reserve_batch(fragment, &reservations))
            .await?;
        let finalized = self
            .objects
            .put_batch(
                &plans,
                &mut Body {
                    objects,
                    index: 0,
                    offset: 0,
                },
            )
            .await?;
        let client = self.objects.clone();
        let publications: Vec<Publication> =
            futures_util::stream::iter(handles.into_iter().zip(finalized))
                .map(move |(handle, finalized)| {
                    let client = client.clone();
                    async move {
                        let stat = client.stat(&finalized.object_key).await?;
                        Ok::<_, super::ServiceError>(Publication {
                            version: handle.version_id,
                            session: handle.session_id,
                            finalized,
                            stat,
                        })
                    }
                })
                .buffered(16)
                .try_collect()
                .await?;
        Ok(self
            .metadata
            .execute(move |db| Namespace::new(db).publish_batch(fragment, &publications))
            .await?)
    }
}
