use super::{
    Error, Result, corrupt,
    ec::StripeCodec,
    io_scheduler::DiskIoGuard,
    object_parser::AdaptiveStreamGuard,
    storage_pool::Pool,
    store::{Admission, Lease, ShardFile, Store, blocking, joined},
    upload::{VerifiedFrame, read_frame, read_frames},
};
use bytes::Bytes;
use n1_link::shard::{self, ShardHeader};
use n1_link::{Fingerprint, Identity};
use serde::Serialize;
use sha2::Digest;
use std::{collections::BTreeSet, path::Path, sync::Arc};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Clone, Debug, Serialize)]
pub struct ObjectInfo {
    pub total_bytes: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub finalized_shards: usize,
    pub integrity_algorithm: &'static str,
    pub integrity_root: String,
}

#[derive(Debug, Serialize)]
pub struct ObjectHealth {
    pub info: ObjectInfo,
    pub status: &'static str,
    pub deep: bool,
    pub verified_bytes: u64,
    pub missing_shards: Vec<u16>,
    pub corrupt_shards: Vec<u16>,
    pub unavailable_shards: Vec<u16>,
}

pub struct DownloadStream {
    pub info: ObjectInfo,
    pub length: u64,
    pub reader: tokio::io::DuplexStream,
    pub completion: DownloadCompletion,
}

/// Awaiting or dropping completion never aborts the worker while member I/O is
/// admitted. Close the reader to stop streaming; the owner drains its I/O.
pub struct DownloadCompletion {
    worker: tokio::task::JoinHandle<Result<()>>,
}
impl std::future::Future for DownloadCompletion {
    type Output = std::result::Result<Result<()>, tokio::task::JoinError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::future::Future::poll(std::pin::Pin::new(&mut self.get_mut().worker), cx)
    }
}

pub(crate) struct OpenObject {
    pub files: Vec<Option<ShardFile>>,
    pub header: ShardHeader,
    pub root: [u8; 32],
    pub missing: Vec<u16>,
    pub corrupt: Vec<u16>,
    pub unavailable: Vec<u16>,
}

/// A validated range and the guards that keep its shards stable while streaming.
/// Native callers keep this plan and their socket in the same activity-owned
/// worker. HTTP uses the existing bounded stream adapter.
pub(crate) struct PreparedDownload {
    store: Arc<Store>,
    pool: Arc<Pool>,
    fp: Fingerprint,
    object: OpenObject,
    offset: u64,
    pub length: u64,
    stream: AdaptiveStreamGuard,
    io: DiskIoGuard,
    admission: Admission,
    lease: Lease,
    guard: tokio::sync::OwnedRwLockReadGuard<()>,
}

pub(crate) struct VerifiedStripe {
    pub source: Bytes,
    pub source_hash: [u8; 32],
    pub bad: Vec<u16>,
    pub unknown: Vec<u16>,
}
impl OpenObject {
    pub fn info(&self) -> ObjectInfo {
        ObjectInfo {
            total_bytes: self.header.total_bytes,
            data_shards: self.header.data_shards,
            parity_shards: self.header.parity_shards,
            finalized_shards: self.files.iter().flatten().count(),
            integrity_algorithm: self.header.format.integrity_algorithm(),
            integrity_root: hex::encode(self.root),
        }
    }
}

impl Store {
    pub(crate) async fn open_object(&self, identity: Identity) -> Result<OpenObject> {
        self.open_fingerprint(self.pool(identity)?, identity.fingerprint())
            .await
    }

    pub(crate) async fn open_fingerprint(
        &self,
        pool: Arc<super::storage_pool::Pool>,
        fp: Fingerprint,
    ) -> Result<OpenObject> {
        let jobs = (0..pool.layout.width()).map(|shard| {
            let holder = pool.holder(fp, shard as u16);
            let layout = pool.layout.clone();
            blocking(move || {
                let member = holder?;
                let path = fp.shard_path(shard as u16);
                let file = match member.open_read(Path::new(&path)) {
                    Ok(file) => file,
                    Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(None);
                    }
                    Err(e) => return Err(e),
                };
                let mut bytes = [0; 128];
                crate::core::linux_io::pread_exact_at(&*file, &mut bytes, 0).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        corrupt("truncated final shard header")
                    } else {
                        e.into()
                    }
                })?;
                let header = ShardHeader::decode(&bytes, &layout, fp, shard as u16)
                    .map_err(|e| corrupt(e.to_string()))?;
                if file.metadata()?.len() != header.file_bytes()? {
                    return Err(corrupt("incorrect final shard file length"));
                }
                let mut seal = [0; 96];
                crate::core::linux_io::pread_exact_at(&*file, &mut seal, header.seal_offset())?;
                let root =
                    shard::decode_seal(&header, fp, &seal).map_err(|e| corrupt(e.to_string()))?;
                let readable_bytes = file.metadata()?.len();
                Ok(Some(ShardFile {
                    completion_proof: shard::decode_completion_seal(&header, fp, &seal).is_ok(),
                    member,
                    file,
                    header,
                    finalized: true,
                    root: Some(root),
                    readable_bytes,
                }))
            })
        });
        let mut files = Vec::new();
        let (mut missing, mut bad, mut unavailable) = (Vec::new(), Vec::new(), Vec::new());
        let results = futures_util::future::join_all(jobs).await;
        for (shard, result) in results.into_iter().enumerate() {
            match result {
                Ok(Some(file)) => files.push(Some(file)),
                Ok(None) => {
                    missing.push(shard as u16);
                    files.push(None)
                }
                Err(Error::Corrupt(_) | Error::Invalid(_)) => {
                    bad.push(shard as u16);
                    files.push(None)
                }
                Err(_) => {
                    unavailable.push(shard as u16);
                    files.push(None)
                }
            }
        }
        let count = files.iter().flatten().count();
        if count == 0 && bad.is_empty() && unavailable.is_empty() {
            return Err(Error::NotFound);
        }
        if count < usize::from(pool.layout.data_shards) {
            if !bad.is_empty() && unavailable.is_empty() {
                return Err(corrupt(
                    "object lacks sufficient checksum-valid finalized shard headers",
                ));
            }
            return Err(Error::Unavailable(
                "object lacks sufficient valid finalized shards".into(),
            ));
        }
        let first = files
            .iter()
            .flatten()
            .next()
            .expect("validated finalized shard");
        let header = first.header.clone();
        let root = first.root.expect("validated seal");
        for f in files.iter().flatten() {
            let mut same = f.header.clone();
            same.shard = header.shard;
            if same != header || f.root != Some(root) {
                return Err(corrupt(
                    "finalized shards disagree about immutable object layout",
                ));
            }
        }
        Ok(OpenObject {
            files,
            header,
            root,
            missing,
            corrupt: bad,
            unavailable,
        })
    }

    pub async fn stat(self: &Arc<Self>, scope: [u8; 16], key: String) -> Result<ObjectInfo> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_reads_allowed()?;
            let id = store.signer.verify_object(&scope, &key)?;
            let pool = store.pool(id)?;
            let io = super::io_scheduler::acquire_disk_io(
                super::io_scheduler::DiskIoClass::ForegroundRead,
                Some(&hex::encode(scope)),
                "object stat",
            )
            .await?;
            let _admission = store.admit_metadata(&pool).await?;
            let lease = store.lease(id.fingerprint())?;
            let _guard = lease.state().lifecycle.clone().read_owned().await;
            let info = store.open_object(id).await?.info();
            io.complete();
            Ok(info)
        })
        .await
    }

    pub async fn download<W>(
        self: &Arc<Self>,
        scope: [u8; 16],
        key: String,
        offset: u64,
        length: Option<u64>,
        mut destination: W,
    ) -> Result<(W, ObjectInfo)>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store
                .download_owned(scope, key, offset, length, &mut destination, None)
                .await
                .map(|info| (destination, info))
        })
        .await
    }

    pub async fn download_stream(
        self: &Arc<Self>,
        scope: [u8; 16],
        key: String,
        offset: u64,
        length: Option<u64>,
    ) -> Result<DownloadStream> {
        let (reader, mut writer) =
            tokio::io::duplex(super::object_parser::stream_readahead_bytes());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let store = self.clone();
        let completion = self.activity.spawn(async move {
            store
                .download_owned(scope, key, offset, length, &mut writer, Some(tx))
                .await
                .map(|_| ())
        })?;
        match rx.await {
            Ok((info, length)) => Ok(DownloadStream {
                info,
                length,
                reader,
                completion: DownloadCompletion { worker: completion },
            }),
            Err(_) => match completion.await {
                Ok(Err(e)) => Err(e),
                _ => Err(Error::Unavailable("download metadata worker failed".into())),
            },
        }
    }

    async fn download_owned<W>(
        self: Arc<Self>,
        scope: [u8; 16],
        key: String,
        offset: u64,
        length: Option<u64>,
        destination: &mut W,
        metadata: Option<tokio::sync::oneshot::Sender<(ObjectInfo, u64)>>,
    ) -> Result<ObjectInfo>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let download = self.prepare_download(scope, key, offset, length).await?;
        if let Some(metadata) = metadata {
            metadata
                .send((download.info(), download.length))
                .map_err(|_| Error::Unavailable("download requester disconnected".into()))?;
        }
        download.write(destination).await
    }

    /// Call only inside an activity-owned operation, retaining its destination
    /// until all admitted member I/O has completed, including on caller failure.
    pub(crate) async fn prepare_download(
        self: &Arc<Self>,
        scope: [u8; 16],
        key: String,
        offset: u64,
        length: Option<u64>,
    ) -> Result<PreparedDownload> {
        let identity = self.signer.verify_object(&scope, &key)?;
        self.operation_gate.ensure_reads_allowed()?;
        let fp = identity.fingerprint();
        let pool = self.pool(identity)?;
        let stream = super::object_parser::acquire_adaptive_stream_guard().await?;
        let io = super::io_scheduler::acquire_disk_io(
            super::io_scheduler::DiskIoClass::ForegroundRead,
            Some(&hex::encode(scope)),
            "LINK download",
        )
        .await?;
        let admission = self.admit_download(&pool).await?;
        let lease = self.foreground(fp)?;
        let guard = lease.state().lifecycle.clone().read_owned().await;
        let object = self.open_object(identity).await?;
        if offset > object.header.total_bytes {
            return Err(Error::Invalid("range starts beyond object length".into()));
        }
        let length = length.unwrap_or(object.header.total_bytes - offset);
        if length > object.header.total_bytes - offset {
            return Err(Error::Invalid("range exceeds object length".into()));
        }
        Ok(PreparedDownload {
            store: self.clone(),
            pool,
            fp,
            object,
            offset,
            length,
            stream,
            io,
            admission,
            lease,
            guard,
        })
    }

    pub async fn object_health(
        self: &Arc<Self>,
        scope: [u8; 16],
        key: String,
        deep: bool,
    ) -> Result<ObjectHealth> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_reads_allowed()?;
            let id = store.signer.verify_object(&scope, &key)?;
            let pool = store.pool(id)?;
            let fp = id.fingerprint();
            let io = super::io_scheduler::acquire_disk_io(
                super::io_scheduler::DiskIoClass::ForegroundRead,
                Some(&hex::encode(scope)),
                "object health",
            )
            .await?;
            let _admission = store.admit(&pool).await?;
            let lease = store.lease(fp)?;
            let _guard = lease.state().lifecycle.clone().read_owned().await;
            let object = store.open_object(id).await?;
            let result = check_health(
                &object,
                &pool.layout,
                fp,
                deep,
                None,
                Some(&io.progress_handle()),
            )
            .await;
            if result.is_ok() {
                io.complete();
            }
            result
        })
        .await
    }
}

impl PreparedDownload {
    pub fn info(&self) -> ObjectInfo {
        self.object.info()
    }

    pub async fn write<W: AsyncWrite + Unpin + Send>(
        self,
        destination: &mut W,
    ) -> Result<ObjectInfo> {
        let Self {
            store,
            pool,
            fp,
            object,
            offset,
            length,
            stream: _stream,
            io,
            admission,
            lease,
            guard,
        } = self;
        let end = offset + length;
        let stripe_bytes = object.header.stripe_bytes();
        let mut codec = StripeCodec::new(&pool.layout)?;
        let mut damaged = !object.missing.is_empty() || !object.corrupt.is_empty();
        if length > 0 {
            let window = if store.runtime().stream_locality_bias_enabled {
                (super::object_parser::stream_readahead_bytes() as u64 / stripe_bytes).clamp(1, 4)
            } else {
                1
            };
            let stop = end.div_ceil(stripe_bytes);
            let mut start = offset / stripe_bytes;
            let mut prefetched =
                read_window(&object, fp, start..(start + window).min(stop), false).await;
            while start < stop {
                let next = (start + window).min(stop);
                // Join the next bounded read window even when emission fails.
                // No read worker or admitted buffer escapes the operation owner.
                let (following, emitted) = tokio::join!(biased;
                    read_window(&object, fp, next..(next + window).min(stop), false),
                    async {
                        for stripe in start..next {
                            let initial = prefetched.initial(&object, fp, stripe).await;
                            let VerifiedStripe { source, bad, unknown, .. } = decode_stripe_inner(
                                &object,
                                fp,
                                stripe,
                                &mut codec,
                                false,
                                false,
                                Some(initial),
                            )
                            .await?;
                            damaged |= !bad.is_empty() || !unknown.is_empty();
                            let from = offset.saturating_sub(stripe * stripe_bytes) as usize;
                            let to = ((end - stripe * stripe_bytes) as usize).min(source.len());
                            for chunk in
                                source[from..to].chunks(super::object_parser::adaptive_emit_bytes())
                            {
                                destination.write_all(chunk).await?;
                                io.record_progress(chunk.len() as u64);
                            }
                        }
                        Ok::<(), Error>(())
                    }
                );
                emitted?;
                prefetched = following;
                start = next;
            }
        }
        destination.flush().await?;
        io.complete();
        let info = object.info();
        drop(guard);
        drop(lease);
        drop(admission);
        if damaged {
            store.queue_read_repair(pool, fp).await;
        }
        Ok(info)
    }
}

// A failed coalesced read falls back to individual records in the consumer.
// This preserves partial-read recovery and precise per-stripe I/O handling.
struct FrameWindow {
    start: u64,
    frames: Vec<Option<Vec<Option<VerifiedFrame>>>>,
}
impl FrameWindow {
    async fn initial(
        &self,
        object: &OpenObject,
        fp: Fingerprint,
        stripe: u64,
    ) -> Vec<Result<Option<VerifiedFrame>>> {
        futures_util::future::join_all(self.frames.iter().enumerate().map(|(i, frames)| {
            let cached = frames
                .as_ref()
                .map(|frames| frames[(stripe - self.start) as usize].clone());
            let file = &object.files[i];
            async move {
                match cached {
                    Some(frame) => Ok(frame),
                    None => match file {
                        Some(file) => read_frame(file, fp, stripe).await,
                        None => Ok(None),
                    },
                }
            }
        }))
        .await
    }
}
async fn read_window(
    object: &OpenObject,
    fp: Fingerprint,
    stripes: std::ops::Range<u64>,
    all: bool,
) -> FrameWindow {
    let start = stripes.start;
    if stripes.is_empty() {
        return FrameWindow {
            start,
            frames: Vec::new(),
        };
    }
    let count = if all {
        object.files.len()
    } else {
        usize::from(object.header.data_shards)
    };
    let frames = futures_util::future::join_all(object.files[..count].iter().map(|file| {
        let stripes = stripes.clone();
        async move {
            match file {
                Some(file) => read_frames(file, fp, stripes).await.ok(),
                None => Some(vec![None; (stripes.end - stripes.start) as usize]),
            }
        }
    }))
    .await;
    FrameWindow { start, frames }
}

pub(crate) async fn decode_stripe(
    object: &OpenObject,
    fp: Fingerprint,
    stripe: u64,
    codec: &mut StripeCodec,
    all: bool,
) -> Result<VerifiedStripe> {
    decode_stripe_inner(object, fp, stripe, codec, all, false, None).await
}

/// Offline activation cannot certify a partial scan through an I/O error.
pub(crate) async fn decode_stripe_strict(
    object: &OpenObject,
    fp: Fingerprint,
    stripe: u64,
    codec: &mut StripeCodec,
) -> Result<VerifiedStripe> {
    decode_stripe_inner(object, fp, stripe, codec, true, true, None).await
}

async fn decode_stripe_inner(
    object: &OpenObject,
    fp: Fingerprint,
    stripe: u64,
    codec: &mut StripeCodec,
    all: bool,
    strict_io: bool,
    initial_frames: Option<Vec<Result<Option<VerifiedFrame>>>>,
) -> Result<VerifiedStripe> {
    let k = usize::from(object.header.data_shards);
    let mut verified = VerifiedPieces {
        pieces: vec![None; object.files.len()],
        source_hash: None,
        bad: Vec::new(),
    };
    let mut unknown = Vec::new();
    let initial = if all { object.files.len() } else { k };
    let reads = (0..initial).map(|i| async move {
        match &object.files[i] {
            Some(f) => read_frame(f, fp, stripe).await,
            None => Ok(None),
        }
    });
    // A failed holder does not defeat a verified read when parity can recover.
    // Upload conflict checks still propagate I/O failures rather than overwrite.
    let frames = match initial_frames {
        Some(frames) => frames,
        None => futures_util::future::join_all(reads).await,
    };
    for (i, bytes) in frames.into_iter().enumerate() {
        match bytes {
            Ok(bytes) => accept_frame(i, bytes, &mut verified)?,
            Err(e) if strict_io => return Err(e),
            Err(_) => unknown.push(i as u16),
        }
    }
    if !all {
        for i in k..object.files.len() {
            if verified.pieces.iter().filter(|p| p.is_some()).count() >= k {
                break;
            }
            let bytes = match &object.files[i] {
                Some(f) => read_frame(f, fp, stripe).await,
                None => Ok(None),
            };
            match bytes {
                Ok(bytes) => accept_frame(i, bytes, &mut verified)?,
                Err(_) => unknown.push(i as u16),
            }
        }
    }
    // A quorum lost to verified bad frames is an integrity failure. Preserve
    // unavailable/unknown for actual read or device failures, rather than
    // reporting checksum damage as transient admission pressure.
    // Private uploads may legitimately have sparse, unwritten frames. Only
    // finalized shards promise that every frame was durably completed.
    if object.files.iter().flatten().all(|file| file.finalized)
        && unknown.is_empty()
        && !verified.bad.is_empty()
        && verified.pieces.iter().flatten().count() < k
    {
        return Err(corrupt("insufficient checksum-valid shard frames"));
    }
    let source = codec.decode(
        &verified.pieces,
        object.header.source_bytes_in_stripe(stripe)? as usize,
    )?;
    let source_hash = object.header.source_hash(fp, stripe, &source)?;
    if Some(source_hash) != verified.source_hash {
        return Err(corrupt(
            "reconstructed source stripe failed integrity verification",
        ));
    }
    Ok(VerifiedStripe {
        source,
        source_hash,
        bad: verified.bad,
        unknown,
    })
}

struct VerifiedPieces {
    pieces: Vec<Option<Bytes>>,
    source_hash: Option<[u8; 32]>,
    bad: Vec<u16>,
}

fn accept_frame(
    i: usize,
    bytes: Option<VerifiedFrame>,
    verified: &mut VerifiedPieces,
) -> Result<()> {
    if let Some(frame) = bytes {
        if verified
            .source_hash
            .is_some_and(|h| h != frame.source_hash())
        {
            return Err(corrupt("source hashes disagree across verified fragments"));
        }
        verified.source_hash = Some(frame.source_hash());
        verified.pieces[i] = Some(frame.payload());
    } else {
        verified.bad.push(i as u16);
    }
    Ok(())
}

pub(crate) async fn check_health(
    object: &OpenObject,
    layout: &n1_link::PoolLayout,
    fp: Fingerprint,
    deep: bool,
    control: Option<&super::maintenance::WorkControl>,
    progress: Option<&super::io_scheduler::IoProgress>,
) -> Result<ObjectHealth> {
    let mut bad = object.corrupt.iter().copied().collect::<BTreeSet<_>>();
    let mut unknown = object.unavailable.iter().copied().collect::<BTreeSet<_>>();
    let mut verified_bytes = 0;
    if deep {
        let mut codec = StripeCodec::new(layout)?;
        let mut root = shard::object_hasher(
            object.header.format,
            object.header.total_bytes,
            object.header.part_bytes,
        );
        for part in 0..object.header.parts() {
            let mut part_root = shard::part_hasher(object.header.format, part);
            let range = object.header.part_stripes(part)?;
            let window = (super::object_parser::stream_readahead_bytes() as u64
                / object.header.stripe_bytes())
            .clamp(1, 4);
            let mut start = range.start;
            while start < range.end {
                let end = (start + window).min(range.end);
                if let Some(control) = control {
                    control
                        .charge(
                            object.header.record_bytes() * layout.width() as u64 * (end - start),
                        )
                        .await?;
                }
                let frames = read_window(object, fp, start..end, true).await;
                for stripe in start..end {
                    let initial = frames.initial(object, fp, stripe).await;
                    let VerifiedStripe {
                        source,
                        source_hash,
                        bad: damaged,
                        unknown: unavailable,
                    } = decode_stripe_inner(
                        object,
                        fp,
                        stripe,
                        &mut codec,
                        true,
                        false,
                        Some(initial),
                    )
                    .await?;
                    unknown.extend(unavailable);
                    part_root.update(source_hash);
                    verified_bytes += source.len() as u64;
                    if let Some(progress) = progress {
                        progress.record(source.len() as u64);
                    }
                    for shard in damaged {
                        if object.files[usize::from(shard)].is_some() {
                            bad.insert(shard);
                        }
                    }
                }
                start = end;
            }
            root.update(part_root.finalize());
        }
        if <[u8; 32]>::from(root.finalize()) != object.root {
            return Err(corrupt(
                "object integrity root differs from finalized seals",
            ));
        }
    }
    let status = if !unknown.is_empty() {
        "unknown"
    } else if !bad.is_empty() || !object.missing.is_empty() {
        "degraded"
    } else if deep {
        "healthy"
    } else {
        "present"
    };
    Ok(ObjectHealth {
        info: object.info(),
        status,
        deep,
        verified_bytes,
        missing_shards: object.missing.clone(),
        corrupt_shards: bad.into_iter().collect(),
        unavailable_shards: unknown.into_iter().collect(),
    })
}
