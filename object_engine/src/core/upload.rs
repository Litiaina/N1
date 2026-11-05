pub(crate) mod activity;
mod direct;
mod small_batch;
mod stream;
use once_cell::sync::OnceCell;
use std::sync::atomic::AtomicUsize;
pub(crate) use stream::write_frames;
pub static MAX_IO_SEMAPHORE: OnceCell<usize> = OnceCell::new();
pub static ADAPTIVE_IO_TARGET_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
use super::io_scheduler::{DiskIoClass, DiskIoGuard, IoProgress, acquire_disk_io};

pub(crate) struct UploadIo {
    guard: Option<DiskIoGuard>,
    progress: IoProgress,
}
impl UploadIo {
    pub fn progress_handle(&self) -> IoProgress {
        self.progress.clone()
    }
    pub fn complete(self) {
        if let Some(guard) = self.guard {
            guard.complete();
        }
    }
}
use super::{
    Error, Result, corrupt,
    ec::StripeCodec,
    store::{FinalizedObject, ShardFile, Store, UploadStatus, blocking, collect, joined, now},
};
use bytes::Bytes;
use n1_link::shard::{self, decode_frame, encode_seal};
use n1_link::{Durability, Fingerprint};
use sha2::Digest;
use std::sync::Arc;
use tokio::io::AsyncRead;

impl Store {
    pub(crate) async fn upload_io(&self, scope: [u8; 16], context: &str) -> Result<UploadIo> {
        let guard = if self.runtime().upload_io_gate_enabled {
            Some(
                acquire_disk_io(
                    DiskIoClass::ForegroundWrite,
                    Some(&hex::encode(scope)),
                    context,
                )
                .await?,
            )
        } else {
            None
        };
        let progress = guard
            .as_ref()
            .map_or_else(IoProgress::untracked, DiskIoGuard::progress_handle);
        Ok(UploadIo { guard, progress })
    }
    pub async fn cancel_upload(self: &Arc<Self>, scope: [u8; 16], key: String) -> Result<usize> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_mutations_allowed()?;
            let identity = store.signer.verify_object(&scope, &key)?;
            let pool = store.pool(identity)?;
            let io = acquire_disk_io(
                DiskIoClass::ForegroundWrite,
                Some(&hex::encode(scope)),
                "cancel upload",
            )
            .await?;
            let _admission = store.admit_metadata(&pool).await?;
            let check = pool.clone();
            blocking(move || check.writable()).await?;
            let fp = identity.fingerprint();
            let lease = store.foreground(fp)?;
            let _guard = lease.state().lifecycle.clone().write_owned().await;
            let probes = (0..pool.layout.width()).map(|shard| {
                let holder = pool.holder(fp, shard as u16);
                blocking(move || {
                    match holder?.open_read(std::path::Path::new(&fp.shard_path(shard as u16))) {
                        Ok(_) => Ok(true),
                        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
                        Err(e) => Err(e),
                    }
                })
            });
            if collect(futures_util::future::join_all(probes).await)?
                .into_iter()
                .any(|exists| exists)
            {
                return Err(Error::Conflict(
                    "finalization has started; resolve finalization or delete with the object key"
                        .into(),
                ));
            }
            store.forget(fp);
            let removals = (0..pool.layout.width()).map(|shard| {
                let holder = pool.holder(fp, shard as u16);
                blocking(move || {
                    let m = holder?;
                    let removed = m.unlink(std::path::Path::new(&fp.staging_path()), true)?;
                    m.retire_empty_stage(fp, true)?;
                    Ok(usize::from(removed))
                })
            });
            let removed = collect(futures_util::future::join_all(removals).await)?
                .into_iter()
                .sum();
            io.complete();
            Ok(removed)
        })
        .await
    }

    pub async fn write_part<R>(
        self: &Arc<Self>,
        scope: [u8; 16],
        key: String,
        part: u64,
        reader: R,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store
                .write_part_owned(scope, &key, part, false, reader)
                .await
        })
        .await
    }

    /// Native continuation uses the immutable LINK key and a byte offset.
    pub async fn write_at<R: AsyncRead + Unpin + Send + 'static>(
        self: &Arc<Self>,
        scope: [u8; 16],
        key: String,
        offset: u64,
        reader: R,
    ) -> Result<()> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store
                .write_part_owned(scope, &key, offset, true, reader)
                .await
        })
        .await
    }

    async fn write_part_owned<R>(
        self: Arc<Self>,
        scope: [u8; 16],
        key: &str,
        position: u64,
        byte_offset: bool,
        mut reader: R,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
    {
        self.operation_gate.ensure_mutations_allowed()?;
        let plan = self.inspect_upload(scope, key).await?;
        let part = if byte_offset {
            if !position.is_multiple_of(plan.part_bytes) {
                return Err(Error::Invalid(
                    "upload offset must align to the declared part size".into(),
                ));
            }
            position / plan.part_bytes
        } else {
            position
        };
        plan.part_length(part)?;
        if now() >= plan.expires_at {
            return Err(Error::Conflict("upload key has expired".into()));
        }
        let pool = self.pool(plan.identity)?;
        let io = self.upload_io(scope, "multipart upload part").await?;
        let _admission = self.admit(&pool).await?;
        let check = pool.clone();
        blocking(move || check.writable()).await?;
        let fp = plan.identity.fingerprint();
        let lease = self.foreground(fp)?;
        let state = lease.state().clone();
        let _lifecycle = state.lifecycle.clone().read_owned().await;
        let _part = state.parts[part as usize % 64].lock().await;
        self.forget_part(fp, part);
        let files = {
            let _prepare = state.prepare.lock().await;
            required(self.files(pool.clone(), plan).await?)?
        };
        let root = write_frames(
            &pool,
            &files,
            plan,
            part,
            &mut reader,
            &io.progress_handle(),
            self.runtime().writer_buffer_size,
        )
        .await?;
        if plan.durability == Durability::Durable {
            persist_files(&files).await?;
            let persisted = self
                .progress
                .lock()
                .ok()
                .and_then(|cache| cache.get(&fp).map(|p| p.parents_persisted))
                .unwrap_or(false);
            if !persisted {
                collect(
                    futures_util::future::join_all(files.iter().map(|f| {
                        let member = f.member.clone();
                        let finalized = f.finalized;
                        blocking(move || {
                            if finalized {
                                member.persist_final_parent(fp)
                            } else {
                                member.persist_stage_parent(fp)
                            }
                        })
                    }))
                    .await,
                )?;
            }
        }
        self.remember_part(fp, plan, part, root);
        if plan.durability == Durability::Durable
            && let Ok(mut cache) = self.progress.lock()
            && let Some(p) = cache.get_mut(&fp)
        {
            p.parents_persisted = true;
        }
        io.complete();
        Ok(())
    }

    pub async fn upload_status(
        self: &Arc<Self>,
        scope: [u8; 16],
        key: String,
        start: u64,
        limit: u64,
    ) -> Result<UploadStatus> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_reads_allowed()?;
            if limit == 0 || limit > 4096 {
                return Err(Error::Invalid("status page size must be 1..=4096".into()));
            }
            let plan = store.inspect_upload(scope, &key).await?;
            let pool = store.pool(plan.identity)?;
            let fp = plan.identity.fingerprint();
            let io = acquire_disk_io(
                DiskIoClass::ForegroundRead,
                Some(&hex::encode(scope)),
                "upload status",
            )
            .await?;
            let _admission = store.admit(&pool).await?;
            let lease = store.lease(fp)?;
            let _guard = lease.state().lifecycle.clone().read_owned().await;
            let files = store.files(pool.clone(), plan).await?;
            if files.iter().all(Option::is_none) {
                return Err(Error::NotFound);
            }
            let finalized_shards = files.iter().flatten().filter(|f| f.finalized).count();
            validate_seal_roots(&files)?;
            let end = start.saturating_add(limit).min(plan.parts());
            if start > plan.parts() {
                return Err(Error::Invalid("status start exceeds part count".into()));
            }
            let mut missing = Vec::new();
            let mut codec = StripeCodec::new(&pool.layout)?;
            for part in start..end {
                io.heartbeat();
                let state = lease.state().clone();
                let _part = state.parts[part as usize % 64].lock().await;
                if verify_part(&files, fp, part, &mut codec).await?.is_none() {
                    missing.push(part);
                }
            }
            io.complete();
            Ok(UploadStatus {
                finalized: finalized_shards == pool.layout.width(),
                finalized_shards,
                parts: plan.parts(),
                checked_start: start,
                checked_end: end,
                missing_parts: missing,
                unavailable_members: pool
                    .members
                    .iter()
                    .enumerate()
                    .filter_map(|(i, m)| m.is_none().then_some(i))
                    .collect(),
            })
        })
        .await
    }

    pub async fn finalize(
        self: &Arc<Self>,
        scope: [u8; 16],
        key: String,
    ) -> Result<FinalizedObject> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_mutations_allowed()?;
            let plan = store.inspect_upload(scope, &key).await?;
            let pool = store.pool(plan.identity)?;
            let check = pool.clone();
            let io = acquire_disk_io(
                DiskIoClass::ForegroundWrite,
                Some(&hex::encode(scope)),
                "finalize upload",
            )
            .await?;
            blocking(move || check.writable()).await?;
            let _admission = store.admit(&pool).await?;
            let fp = plan.identity.fingerprint();
            let lease = store.foreground(fp)?;
            let _guard = lease.state().lifecycle.clone().write_owned().await;
            let files = required(store.files(pool.clone(), plan).await?)?;
            let already_finalized = store.seal_upload(&pool, plan, &files).await?;
            // Repeating finalize also retries failed durability barriers. An
            // existing final filename alone never converts an EIO into success.
            if let Err(e) = persist_files(&files).await {
                store.forget(fp);
                return Err(e);
            }
            let _snapshot = store.snapshot_gate.clone().read_owned().await;
            collect(
                futures_util::future::join_all(files.iter().map(|f| {
                    let member = f.member.clone();
                    let shard = f.header.shard;
                    let finalized = f.finalized;
                    blocking(move || {
                        if !finalized {
                            member.promote(fp, shard, true)?;
                        } else {
                            member.persist_final_parent(fp)?;
                        }
                        member.retire_empty_stage(fp, true)
                    })
                }))
                .await,
            )?;
            store.forget(fp);
            io.complete();
            Ok(FinalizedObject {
                object_key: store
                    .signer
                    .object_key(&scope, plan.identity)
                    .as_str()
                    .into(),
                already_finalized,
                total_bytes: plan.total_bytes,
            })
        })
        .await
    }
    pub(crate) async fn seal_upload(
        &self,
        pool: &super::storage_pool::Pool,
        plan: n1_link::UploadPlan,
        files: &[ShardFile],
    ) -> Result<bool> {
        let fp = plan.identity.fingerprint();
        let optional = files.iter().cloned().map(Some).collect::<Vec<_>>();
        validate_seal_roots(&optional)?;
        let already_finalized = files.iter().all(|f| f.finalized);
        if !already_finalized && now() >= plan.expires_at {
            return Err(Error::Conflict("upload key has expired".into()));
        }
        if !already_finalized {
            let mut codec = StripeCodec::new(&pool.layout)?;
            let mut root =
                shard::object_hasher(pool.layout.shard_format, plan.total_bytes, plan.part_bytes);
            for part in 0..plan.parts() {
                let part_root = match self.cached_root(fp, plan, part) {
                    Some(r) => r,
                    None => verify_part_inner(&optional, fp, part, &mut codec, true)
                        .await?
                        .ok_or_else(|| {
                            Error::Conflict(format!("upload part {part} is incomplete"))
                        })?,
                };
                root.update(part_root);
            }
            let root: [u8; 32] = root.finalize().into();
            if files.iter().any(|f| f.root.is_some_and(|old| old != root)) {
                return Err(corrupt("finalized shard root conflicts with upload data"));
            }
            let writes = files.iter().filter(|f| !f.finalized).map(|f| {
                crate::core::linux_io::write_at_async(
                    f.file.clone(),
                    encode_seal(&f.header, fp, root).to_vec(),
                    f.header.seal_offset(),
                )
            });
            collect(
                futures_util::future::join_all(writes)
                    .await
                    .into_iter()
                    .map(|r| r.map_err(Error::from))
                    .collect(),
            )?;
        }
        Ok(already_finalized)
    }
}

pub(crate) fn required(files: Vec<Option<ShardFile>>) -> Result<Vec<ShardFile>> {
    if files.iter().all(Option::is_none) {
        return Err(Error::NotFound);
    }
    files
        .into_iter()
        .map(|f| f.ok_or_else(|| Error::Conflict("upload is missing required shard data".into())))
        .collect()
}

pub(crate) fn validate_seal_roots(files: &[Option<ShardFile>]) -> Result<()> {
    let mut root = None;
    for file in files.iter().flatten() {
        if let Some(value) = file.root {
            if root.is_some_and(|r| r != value) {
                return Err(corrupt("conflicting finalized shard roots"));
            }
            root = Some(value);
        }
    }
    Ok(())
}

pub(crate) async fn persist_files(files: &[ShardFile]) -> Result<()> {
    collect(
        futures_util::future::join_all(
            files
                .iter()
                .map(|f| crate::core::linux_io::fdatasync_async(f.file.clone())),
        )
        .await
        .into_iter()
        .map(|r| r.map_err(Error::from))
        .collect(),
    )?;
    Ok(())
}

/// A frame whose identity, stripe, geometry and checksum were checked once.
/// Construction stays private; payload slices share the owned read buffer.
#[derive(Clone)]
pub(crate) struct VerifiedFrame {
    bytes: Bytes,
    source_hash: [u8; 32],
}
impl VerifiedFrame {
    fn verify(file: &ShardFile, fp: Fingerprint, stripe: u64, bytes: Bytes) -> Option<Self> {
        let frame = decode_frame(&file.header, fp, stripe, &bytes).ok()?;
        let source_hash = frame.source_hash;
        Some(Self { bytes, source_hash })
    }
    pub fn source_hash(&self) -> [u8; 32] {
        self.source_hash
    }
    pub fn payload(&self) -> Bytes {
        self.bytes.slice(shard::FRAME_HEADER_BYTES..)
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub(crate) async fn read_frame(
    file: &ShardFile,
    fp: Fingerprint,
    stripe: u64,
) -> Result<Option<VerifiedFrame>> {
    if file.header.frame_offset(stripe)? + file.header.record_bytes() > file.readable_bytes {
        return Ok(None);
    }
    match crate::core::linux_io::read_exact_at_async(
        file.file.clone(),
        file.header.record_bytes() as usize,
        file.header.frame_offset(stripe)?,
    )
    .await
    {
        Ok(bytes) => Ok(VerifiedFrame::verify(file, fp, stripe, bytes.into())),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Read adjacent immutable records with one submission. Validation remains per
/// frame, so one corrupt record never hides otherwise usable neighboring data.
pub(crate) async fn read_frames(
    file: &ShardFile,
    fp: Fingerprint,
    stripes: std::ops::Range<u64>,
) -> Result<Vec<Option<VerifiedFrame>>> {
    if stripes.is_empty() {
        return Ok(Vec::new());
    }
    let offset = file.header.frame_offset(stripes.start)?;
    let record = file.header.record_bytes();
    let count = stripes.end - stripes.start;
    let available = ((file.readable_bytes.saturating_sub(offset)) / record).min(count);
    let bytes = if available == 0 {
        Bytes::new()
    } else {
        crate::core::linux_io::read_exact_at_async(
            file.file.clone(),
            (available * record) as usize,
            offset,
        )
        .await?
        .into()
    };
    Ok((0..count)
        .map(|i| {
            if i >= available {
                None
            } else {
                VerifiedFrame::verify(
                    file,
                    fp,
                    stripes.start + i,
                    bytes.slice((i * record) as usize..((i + 1) * record) as usize),
                )
            }
        })
        .collect())
}

pub(crate) async fn verify_part(
    files: &[Option<ShardFile>],
    fp: Fingerprint,
    part: u64,
    codec: &mut StripeCodec,
) -> Result<Option<[u8; 32]>> {
    verify_part_inner(files, fp, part, codec, false).await
}
async fn verify_part_inner(
    files: &[Option<ShardFile>],
    fp: Fingerprint,
    part: u64,
    codec: &mut StripeCodec,
    rewrite: bool,
) -> Result<Option<[u8; 32]>> {
    let Some(first) = files.iter().flatten().next() else {
        return Ok(None);
    };
    let mut root = shard::part_hasher(first.header.format, part);
    if files.iter().any(Option::is_none) {
        return Ok(None);
    }
    for stripe in first.header.part_stripes(part)? {
        let frames = collect(
            futures_util::future::join_all(
                files.iter().flatten().map(|f| read_frame(f, fp, stripe)),
            )
            .await,
        )?;
        if frames.iter().any(Option::is_none) {
            return Ok(None);
        }
        let mut source_hash = None;
        let mut pieces = Vec::new();
        for bytes in &frames {
            let bytes = bytes.as_ref().expect("validated frame");
            if source_hash.is_some_and(|h| h != bytes.source_hash()) {
                return Err(corrupt("EC fragments disagree about source bytes"));
            }
            source_hash = Some(bytes.source_hash());
            pieces.push(Some(bytes.payload()));
        }
        let source = codec.decode(
            &pieces,
            first.header.source_bytes_in_stripe(stripe)? as usize,
        )?;
        let expected = source_hash.expect("nonempty EC stripe");
        if first.header.source_hash(fp, stripe, &source)? != expected {
            return Err(corrupt("decoded stripe checksum mismatch"));
        }
        if rewrite {
            let writes = files
                .iter()
                .flatten()
                .zip(frames)
                .filter(|(file, _)| !file.finalized)
                .map(|(file, bytes)| {
                    crate::core::linux_io::write_at_async(
                        file.file.clone(),
                        bytes.expect("verified frame").bytes().to_vec(),
                        file.header.frame_offset(stripe).expect("validated stripe"),
                    )
                });
            collect(
                futures_util::future::join_all(writes)
                    .await
                    .into_iter()
                    .map(|r| r.map_err(Error::from))
                    .collect(),
            )?;
        }
        root.update(expected);
    }
    Ok(Some(root.finalize().into()))
}
