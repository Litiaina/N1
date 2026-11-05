use super::*;
use lifecycle::{MoveWork, Target};
use sha2::{Digest, Sha256};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct HashReader<R> {
    inner: R,
    hash: Sha256,
}
impl<R: AsyncRead + Unpin> AsyncRead for HashReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        this.hash.update(&buf.filled()[before..]);
        result
    }
}
#[derive(Default)]
struct HashWriter {
    hash: Sha256,
    bytes: u64,
}
impl AsyncWrite for HashWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        this.hash.update(bytes);
        this.bytes += bytes.len() as u64;
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
impl Manager {
    pub(super) async fn repair(&self, job: &mut Job, cancel: &AtomicBool) -> ApiResult<()> {
        while job.pool_cursor < self.pools.len() {
            let pool = self.pools[job.pool_cursor].id;
            if job
                .request
                .source_pool
                .is_some_and(|selected| selected != pool)
            {
                job.pool_cursor += 1;
                continue;
            }
            if self.interrupted(cancel) {
                return Ok(());
            }
            let after = job.cursor.clone();
            let fragment = job.request.fragment;
            let limit = job.request.batch_size;
            let rows = self
                .db
                .execute(move |db| {
                    Namespace::new(db).pool_versions(pool, fragment, after.as_ref(), limit)
                })
                .await?;
            if rows.is_empty() {
                job.pool_cursor += 1;
                job.cursor = None;
                self.save(job).await?;
                continue;
            }
            for (cursor, target) in rows {
                if self.interrupted(cancel) {
                    return Ok(());
                }
                let result = async {
                    let owned = target.clone();
                    let pin = job.id.clone();
                    self.db
                        .execute(move |db| Namespace::new(db).pin(&owned, &pin))
                        .await?;
                    let owned = target.clone();
                    let blob = self
                        .db
                        .execute(move |db| Namespace::new(db).blob(&owned))
                        .await?;
                    if job.request.repair && !job.request.dry_run {
                        self.objects.repair(&blob.key).await?;
                    } else {
                        let health = self.objects.health(&blob.key, true).await?;
                        if health.get("status").and_then(serde_json::Value::as_str)
                            != Some("healthy")
                        {
                            return Err(ApiError::invalid(
                                "object integrity check reported damage",
                            ));
                        }
                    }
                    Ok::<(), ApiError>(())
                }
                .await;
                let pin = job.id.clone();
                self.db
                    .execute(move |db| Namespace::new(db).unpin_job(&pin))
                    .await?;
                job.scanned += 1;
                match result {
                    Ok(()) => job.processed += 1,
                    Err(error) => {
                        job.failures += 1;
                        job.last_error = Some(error.to_string());
                    }
                }
                job.cursor = Some(cursor);
                job.updated_at_unix = crate::middleware::auth::now();
                self.save(job).await?;
                self.pace(job.request.pause_millis).await;
            }
        }
        Ok(())
    }
    pub(super) async fn migrate(&self, job: &mut Job, cancel: &AtomicBool) -> ApiResult<()> {
        // Resolve each durable per-version intent before scanning new work.
        // A stop retires an uncommitted destination, never its source.
        let mut after = None;
        loop {
            let id = job.id.clone();
            let cursor = after.clone();
            let limit = job.request.batch_size;
            let rows = self
                .db
                .execute(move |db| Namespace::new(db).pending_moves(&id, cursor.as_ref(), limit))
                .await?;
            if rows.is_empty() {
                break;
            }
            after = rows.last().map(|row| row.0.clone());
            for (_, work) in rows {
                self.move_one(job, work.target, cancel).await?;
            }
        }
        if self.interrupted(cancel) {
            return Ok(());
        }
        let source = job.request.source_pool.expect("validated source pool");
        loop {
            let cursor = job.cursor.clone();
            let fragment = job.request.fragment;
            let limit = job.request.batch_size;
            let rows = self
                .db
                .execute(move |db| {
                    Namespace::new(db).pool_versions(source, fragment, cursor.as_ref(), limit)
                })
                .await?;
            if rows.is_empty() {
                break;
            }

            let cursor = rows.last().map(|r| r.0.clone());
            let template = job.clone();
            use futures_util::{StreamExt, stream};
            let results = stream::iter(rows.into_iter().map(|(_, target)| {
                let mut local = template.clone();
                local.processed = 0;
                local.bytes_processed = 0;
                async move {
                    if self.interrupted(cancel) {
                        return Ok((0, 0, 0));
                    }
                    let selected = local
                        .request
                        .object
                        .as_ref()
                        .is_none_or(|id| *id == target.object);
                    if !selected {
                        return Ok((0, 0, 0));
                    }
                    if !local.request.dry_run {
                        self.move_one(&mut local, target, cancel).await?;
                    } else {
                        local.processed = 1;
                    }
                    Ok::<_, ApiError>((1, local.processed, local.bytes_processed))
                }
            }))
            .buffer_unordered(job.request.object_concurrency)
            .collect::<Vec<_>>()
            .await;
            for result in results {
                let (scanned, processed, bytes) = result?;
                job.scanned += scanned;
                job.processed += processed;
                job.bytes_processed += bytes;
            }
            if self.interrupted(cancel) {
                return Ok(());
            }
            job.cursor = cursor;
            job.updated_at_unix = crate::middleware::auth::now();
            self.save(job).await?;
            self.pace(job.request.pause_millis).await;
        }
        Ok(())
    }
    async fn move_one(&self, job: &mut Job, target: Target, cancel: &AtomicBool) -> ApiResult<()> {
        let id = job.id.clone();
        let owned = target.clone();
        let mut work = self
            .db
            .execute(move |db| Namespace::new(db).move_work(&id, &owned))
            .await?;
        if let Some(pending) = &work
            && !pending.switched
            && (self.interrupted(cancel)
                || pending.upload.expires_at <= crate::middleware::auth::now())
        {
            // Incomplete uploads may expire while offline. Purge their
            // destination before replacing or forgetting the intent.
            self.objects.purge(&pending.upload.object_key).await?;
            let owned = pending.clone();
            let id = job.id.clone();
            self.db
                .execute(move |db| Namespace::new(db).finish_move(&id, &owned))
                .await?;
            work = None;
        }
        if self.interrupted(cancel) && work.is_none() {
            return Ok(());
        }
        if work.is_none() {
            let owned = target.clone();
            let blob = self
                .db
                .execute(move |db| Namespace::new(db).blob(&owned))
                .await?;
            let source = job.request.source_pool.expect("source pool");
            if n1_link::ObjectKey::inspect(&blob.key)
                .map_err(|_| ApiError::invalid("invalid source identity"))?
                .pool
                != source
            {
                return Ok(());
            }
            let stored = blob
                .stored
                .ok_or_else(|| ApiError::invalid("only published versions can migrate"))?;
            let pool = job.request.target_pool.expect("destination pool");
            let profile = self
                .pools
                .iter()
                .find(|p| p.id == pool)
                .expect("validated destination");
            let stripe = u64::from(profile.block_bytes) * u64::from(profile.data_shards);
            let part_bytes = (8 * 1024 * 1024u64)
                .max(stored.metadata.total_size_bytes.div_ceil(131072))
                .div_ceil(stripe)
                * stripe;
            if part_bytes > self.config.max_part_size {
                return Err(ApiError::invalid(
                    "migration requires a larger configured max_part_size for this topology or object length",
                ));
            }
            let upload = self
                .objects
                .init(
                    Some(pool),
                    stored.metadata.total_size_bytes,
                    part_bytes,
                    true,
                    604800,
                )
                .await?;
            let pending = MoveWork {
                target: target.clone(),
                source: blob.key,
                upload,
                switched: false,
            };
            let owned = pending.clone();
            let id = job.id.clone();
            self.db
                .execute(move |db| Namespace::new(db).create_move(&id, &owned))
                .await?;
            work = Some(pending);
        }
        let mut work = work.expect("durable migration identity");
        if !work.switched {
            let total = work.upload.total_bytes;
            let mut source_hash = Sha256::new();
            // Re-reading each bounded source part also computes a fresh whole
            // payload digest on resume; completed destination parts are verified
            // by the final full destination read, not trusted by their bitmap.
            for part in 0..work.upload.parts {
                if self.interrupted(cancel) {
                    if self.stopping.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    self.objects.purge(&work.upload.object_key).await?;
                    let owned = work.clone();
                    let id = job.id.clone();
                    self.db
                        .execute(move |db| Namespace::new(db).finish_move(&id, &owned))
                        .await?;
                    return Ok(());
                }
                let offset = part * work.upload.part_bytes;
                let length = (total - offset).min(work.upload.part_bytes);
                let status = self.objects.status(&work.upload, part, 1).await;
                let missing = match status {
                    Ok(status) => !status.finalized && status.missing_parts.contains(&part),
                    Err(e) if e.status() == 404 => true,
                    Err(e) => return Err(e.into()),
                };
                let (mut writer, reader) = tokio::io::duplex(256 * 1024);
                let mut reader = HashReader {
                    inner: reader,
                    hash: source_hash,
                };
                let upload = async {
                    if missing {
                        self.objects
                            .part(&work.upload, part, length, &mut reader)
                            .await?;
                    } else {
                        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
                    }
                    Ok::<(), ApiError>(())
                };
                // Closing writer at the producer boundary lets the reader see
                // EOF even for a previously complete part.
                let producer = async {
                    let result = self
                        .objects
                        .download(&work.source, offset, Some(length), &mut writer)
                        .await
                        .map_err(ApiError::from);
                    drop(writer);
                    result
                };
                tokio::try_join!(producer, upload)?;
                source_hash = reader.hash;
                self.pace(
                    (length.saturating_mul(1000)
                        / (job.request.bytes_per_second / job.request.object_concurrency as u64)
                            .max(1))
                    .max(job.request.pause_millis),
                )
                .await;
            }
            if total == 0 {
                self.objects
                    .download(&work.source, 0, Some(0), &mut tokio::io::sink())
                    .await?;
            }
            self.objects.finalize(&work.upload).await?;
            let stat = self.objects.stat(&work.upload.object_key).await?;
            let mut verified = HashWriter::default();
            self.objects
                .download(&work.upload.object_key, 0, Some(total), &mut verified)
                .await?;
            if verified.bytes != total || verified.hash.finalize() != source_hash.finalize() {
                return Err(ApiError::invalid(
                    "migration destination payload digest differs",
                ));
            }
            let id = job.id.clone();
            let owned = target.clone();
            work = self
                .db
                .execute(move |db| Namespace::new(db).switch_move(&id, &owned, &stat))
                .await?;
        }
        let owned = target.clone();
        let key = work.source.clone();
        let retire = self
            .db
            .execute(move |db| Namespace::new(db).prepare_purge(&owned, &key))
            .await?;
        if retire {
            self.objects.purge(&work.source).await?;
        }
        let bytes = work.upload.total_bytes;
        let owned = work;
        let id = job.id.clone();
        self.db
            .execute(move |db| Namespace::new(db).finish_move(&id, &owned))
            .await?;
        job.processed += 1;
        job.bytes_processed += bytes;
        Ok(())
    }
}
