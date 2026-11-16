pub(crate) mod activity;
mod completion;
mod direct;
mod publication;
mod small_batch;
mod stream;
use completion::{PreparedPart, refresh_completion_seals, write_completion_seals, write_seals};
use once_cell::sync::OnceCell;
use publication::{persist_publication, prepare_publication, publish_files};
use std::sync::atomic::AtomicUsize;
pub(crate) use stream::{WriteOptions, write_frames};
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

    /// The caller must keep this operation in an Activity-owned worker until
    /// all admitted I/O drains. Native streaming owns its socket in that worker;
    /// ordinary callers use the public cancellation-safe wrappers above.
    pub(crate) async fn write_part_owned<R>(
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
        let files = {
            let _prepare = state.prepare.lock().await;
            required(self.files(pool.clone(), plan).await?)?
        };
        let read_only = files.iter().any(|file| file.root.is_some())
            || self
                .completion_state(fp, plan)
                .is_some_and(|(_, prefix)| part < prefix)
            || self.cached_root(fp, plan, part).is_some();
        if !read_only {
            self.forget_part(fp, part);
        }
        let root = write_frames(
            &pool,
            &files,
            plan,
            part,
            &mut reader,
            &io.progress_handle(),
            WriteOptions {
                buffer_size: self.runtime().writer_buffer_size,
                immutable_part: read_only,
            },
        )
        .await?;
        let mut prepared = PreparedPart {
            store: self.clone(),
            fp,
            part,
            acknowledged: read_only,
        };
        if plan.durability == Durability::Durable {
            persist_files(&files).await?;
            let persisted = self
                .progress
                .lock()
                .ok()
                .map(|cache| cache.parents_persisted(fp, plan))
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
        if read_only {
            io.complete();
            return Ok(());
        }
        self.remember_part(fp, plan, part, root);
        if plan.durability == Durability::Durable
            && let Ok(mut cache) = self.progress.lock()
        {
            cache.mark_parents_persisted(fp, plan);
        }
        let seal = if plan.durability == Durability::Durable {
            self.progress
                .lock()
                .ok()
                .and_then(|cache| cache.completion_seal(fp, plan))
        } else {
            None
        };
        // Drain existing writers before establishing durable completion. New
        // writers reopen these files and see immutable payload completion.
        // Release the hashed part lock before upgrading the lifecycle fence.
        if let Some(seal) = seal {
            drop(_part);
            drop(_lifecycle);
            let _completion = state.lifecycle.clone().write_owned().await;
            let current = self
                .progress
                .lock()
                .ok()
                .is_some_and(|cache| cache.seal_is_current(fp, plan, seal));
            if current {
                let files = required(self.files(pool.clone(), plan).await?)?;
                if !files.iter().all(|file| file.root.is_some()) {
                    // All members' payload is stable before the ordered seal
                    // exists. A surviving seal is therefore recovery evidence.
                    write_completion_seals(&files, fp, seal.root).await?;
                    persist_files(&files).await?;
                }
                if let Ok(mut cache) = self.progress.lock() {
                    cache.mark_persisted_seal(fp, plan, seal);
                }
            }
        }
        prepared.acknowledged = true;
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
            let completed = files.iter().flatten().any(|file| file.root.is_some());
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
                if let Some(root) = verify_part(&files, fp, part, &mut codec).await? {
                    let prefix = store
                        .completion_state(fp, plan)
                        .map_or(0, |(_, parts)| parts);
                    if !completed
                        && part >= prefix
                        && store.cached_root(fp, plan, part) != Some(root)
                    {
                        store.remember_part(fp, plan, part, root);
                    }
                } else {
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
            let result = async {
                // Pin filesystem/error-accounting descriptors before seal writes,
                // using the same publication ordering as normal PUT.
                let roots = pool
                    .members
                    .iter()
                    .flatten()
                    .map(|member| member.path.clone())
                    .collect::<Vec<_>>();
                let group = blocking(move || {
                    crate::core::linux_io::group::FilesystemGroup::open(&roots).map_err(Error::from)
                })
                .await?;
                let files = required(store.files(pool.clone(), plan).await?)?;
                let already_finalized = files.iter().all(|f| f.finalized);
                validate_seal_roots(&files.iter().cloned().map(Some).collect::<Vec<_>>())?;
                let durable_root = files.iter().find_map(|file| file.root);
                if !already_finalized && now() >= plan.expires_at {
                    return Err(Error::Conflict("upload key has expired".into()));
                }
                if let Some(root) = durable_root {
                    if files
                        .iter()
                        .any(|file| file.readable_bytes < file.header.seal_offset())
                    {
                        return Err(Error::Conflict(
                            "completed upload requires shard repair".into(),
                        ));
                    }
                    let persisted = store
                        .progress
                        .lock()
                        .ok()
                        .and_then(|cache| cache.persisted_seal(fp, plan))
                        == Some(root);
                    if files.iter().any(|file| file.root.is_none())
                        || (!persisted && !already_finalized)
                    {
                        // A crash during seal publication needs only the fixed
                        // footer restored; the surviving proof covers payload.
                        refresh_completion_seals(&files, fp, root).await?;
                        persist_files(&files).await?;
                    } else if already_finalized {
                        // Retry durability after a lost publication reply.
                        persist_files(&files).await?;
                    }
                } else {
                    // Standard mode consumes its folded RAM completion. Missing
                    // state is reconciled explicitly through paginated status;
                    // finalize never reads payloads or iterates over parts.
                    store.seal_upload(&pool, plan, &files).await?;
                    persist_files(&files).await?;
                }
                let _snapshot = store.snapshot_gate.clone().read_owned().await;
                let directories = prepare_publication(&files, fp).await?;
                let publication = publish_files(&files, fp).await;
                // Every started rename/removal drains and reaches its barrier,
                // including when a peer failed. Success requires both results.
                let persisted = persist_publication(directories).await;
                let bindings = blocking(move || group.check().map_err(Error::from)).await;
                publication?;
                persisted?;
                bindings?;
                Ok(FinalizedObject {
                    object_key: store
                        .signer
                        .object_key(&scope, plan.identity)
                        .as_str()
                        .into(),
                    already_finalized,
                    total_bytes: plan.total_bytes,
                })
            }
            .await;
            if !matches!(result, Err(Error::Conflict(_))) {
                store.forget(fp);
            }
            if result.is_ok() {
                io.complete();
            }
            result
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
            let root = if let Some(root) = files.iter().find_map(|file| file.root) {
                write_seals(files, fp, root).await?;
                return Ok(false);
            } else if plan.parts() == 0 {
                shard::object_hasher(pool.layout.shard_format, plan.total_bytes, plan.part_bytes)
            } else {
                self.completion_state(fp, plan).filter(|(_, parts)| *parts == plan.parts())
                    .map(|(root, _)| root)
                    .ok_or_else(|| Error::Conflict("completion state unavailable; query upload status or resume before finalizing".into()))?
            };
            let root: [u8; 32] = root.finalize().into();
            write_seals(files, fp, root).await?;
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
        root.update(expected);
    }
    Ok(Some(root.finalize().into()))
}
