use super::*;
use futures_util::{StreamExt, stream};
use lifecycle::TrashWork;
impl Manager {
    pub(super) async fn gc(&self, job: &mut Job, cancel: &AtomicBool) -> ApiResult<()> {
        let cutoff = crate::middleware::auth::now()
            .saturating_sub(self.config.trash_retention_seconds)
            .min(i64::MAX as u64) as i64;
        // Claimed work is independent of later TTL changes and is retried first.
        for claimed in [true, false] {
            let mut after = None;
            loop {
                if self.interrupted(cancel) {
                    return Ok(());
                }
                let cursor = after.clone();
                let fragment = job.request.fragment;
                let limit = job.request.batch_size;
                let rows = self
                    .db
                    .execute(move |db| {
                        let ns = Namespace::new(db);
                        if claimed {
                            ns.claimed_trash(fragment, cursor.as_ref(), limit)
                        } else {
                            ns.due_trash(fragment, cutoff, cursor.as_ref(), limit)
                        }
                    })
                    .await?;
                if rows.is_empty() {
                    break;
                }
                after = rows.last().map(|row| row.0.clone());
                let request = &job.request;
                let results = stream::iter(rows.into_iter().map(|(_, work)| async move {
                    if self.interrupted(cancel) {
                        return Ok((false, 0, 0));
                    }
                    self.reclaim(work, cutoff, request, cancel).await
                }))
                .buffer_unordered(job.request.object_concurrency)
                .collect::<Vec<_>>()
                .await;
                for result in results {
                    job.scanned += 1;
                    match result {
                        Ok((true, objects, bytes)) => {
                            job.processed += objects;
                            job.bytes_processed += bytes;
                        }
                        Ok(_) => job.deferred += 1,
                        Err(error) => {
                            job.failures += 1;
                            job.last_error = Some(error.to_string());
                        }
                    }
                }
                job.updated_at_unix = crate::middleware::auth::now();
                self.save(job).await?;
                self.pace(job.request.pause_millis).await;
            }
        }
        self.expired_uploads(job, cancel).await?;
        Ok(())
    }
    async fn expired_uploads(&self, job: &mut Job, cancel: &AtomicBool) -> ApiResult<()> {
        let mut after = None;
        let now = crate::middleware::auth::now();
        loop {
            if self.interrupted(cancel) {
                return Ok(());
            }
            let cursor = after.clone();
            let scope = job.request.fragment;
            let limit = job.request.batch_size;
            let rows = self
                .db
                .execute(move |db| {
                    Namespace::new(db).due_uploads(scope, now, cursor.as_ref(), limit)
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            after = rows.last().map(|r| r.0.clone());
            for (_, work) in rows {
                if self.interrupted(cancel) {
                    return Ok(());
                }
                job.scanned += 1;
                if job.request.dry_run {
                    continue;
                }
                // An active writer owns the same lock through file work and
                // its ACK transaction. Defer without occupying object I/O or
                // waiting behind a slow client. The due index retains work.
                let lock = self
                    .upload_locks
                    .get(work.target.fragment, &work.target.version);
                let Ok(_guard) = lock.try_write() else {
                    job.deferred += 1;
                    continue;
                };
                let result: ApiResult<bool> = async {
                    let blob = self
                        .db
                        .execute(move |db| Namespace::new(db).claim_upload(&work, now))
                        .await?;
                    if let Some(blob) = blob {
                        let target = blob.target.clone();
                        let key = blob.key.clone();
                        if self
                            .db
                            .execute(move |db| Namespace::new(db).prepare_purge(&target, &key))
                            .await?
                        {
                            self.objects.purge(&blob.key).await?;
                        }
                        let directory = self
                            .staging
                            .join(blob.target.fragment.to_string())
                            .join(&blob.target.version);
                        match tokio::fs::remove_dir_all(&directory).await {
                            Ok(()) => (),
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                            Err(e) => return Err(e.into()),
                        }
                        loop {
                            let owned = blob.clone();
                            let limit = job.request.batch_size;
                            if self
                                .db
                                .execute(move |db| Namespace::new(db).purge_blob(&owned, limit))
                                .await?
                            {
                                break;
                            }
                            if self.interrupted(cancel) {
                                return Ok(false);
                            }
                            self.pace(job.request.pause_millis).await;
                        }
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
                .await;
                match result {
                    Ok(true) => job.processed += 1,
                    Ok(false) => job.deferred += 1,
                    Err(error) => {
                        job.failures += 1;
                        job.last_error = Some(error.to_string());
                    }
                }
                self.pace(job.request.pause_millis).await;
            }
            self.save(job).await?;
        }
        Ok(())
    }
    async fn reclaim(
        &self,
        work: TrashWork,
        cutoff: i64,
        request: &Request,
        cancel: &AtomicBool,
    ) -> ApiResult<(bool, u64, u64)> {
        if request.dry_run {
            return Ok((true, 1, 0));
        }
        let owned = work.clone();
        if !self
            .db
            .execute(move |db| Namespace::new(db).claim_trash(&owned, cutoff))
            .await?
        {
            return Ok((false, 0, 0));
        }
        let (mut count, mut bytes) = (0, 0);
        loop {
            if self.interrupted(cancel) {
                return Ok((false, count, bytes));
            }
            let owned = work.clone();
            let limit = request.batch_size;
            let blobs = self
                .db
                .execute(move |db| Namespace::new(db).object_blobs(&owned, limit))
                .await?;
            if blobs.is_empty() {
                break;
            }
            for blob in blobs {
                if self.interrupted(cancel) {
                    return Ok((false, count, bytes));
                }
                let lock = self
                    .upload_locks
                    .get(blob.target.fragment, &blob.target.version);
                let Ok(_guard) = lock.try_write() else {
                    return Ok((false, count, bytes));
                };
                let target = blob.target.clone();
                let key = blob.key.clone();
                let retire = self
                    .db
                    .execute(move |db| Namespace::new(db).prepare_purge(&target, &key))
                    .await?;
                if retire {
                    self.objects.purge(&blob.key).await?;
                }
                // This is metadata's own private multipart bridge, never a
                // shard path. Keep the owner/key until cleanup has succeeded.
                let directory = self
                    .staging
                    .join(blob.target.fragment.to_string())
                    .join(&blob.target.version);
                match tokio::fs::remove_dir_all(&directory).await {
                    Ok(()) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Err(e) => return Err(e.into()),
                }
                loop {
                    let owned = blob.clone();
                    let limit = request.batch_size;
                    if self
                        .db
                        .execute(move |db| Namespace::new(db).purge_blob(&owned, limit))
                        .await?
                    {
                        break;
                    }
                    if self.interrupted(cancel) {
                        return Ok((false, count, bytes));
                    }
                    self.pace(request.pause_millis).await;
                }
                count += 1;
                bytes += blob
                    .stored
                    .as_ref()
                    .map_or(0, |s| s.metadata.total_size_bytes);
                self.pace(request.pause_millis).await;
            }
        }
        loop {
            let owned = work.clone();
            let limit = request.batch_size;
            if self
                .db
                .execute(move |db| Namespace::new(db).finish_trash(&owned, limit))
                .await?
            {
                break;
            }
            if self.interrupted(cancel) {
                return Ok((false, count, bytes));
            }
            self.pace(request.pause_millis).await;
        }
        Ok((true, count, bytes))
    }
}
