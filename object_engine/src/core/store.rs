use super::{
    Error, Result, corrupt,
    storage_pool::{Member, Pool},
};
use n1_link::shard::ShardHeader;
use n1_link::{Durability, Fingerprint, Identity, KeySigner, UploadPlan};
use rand::{RngCore, rngs::OsRng};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    path::Path,
    sync::{Arc, Mutex, Weak, atomic::AtomicBool},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex as AsyncMutex, RwLock, Semaphore};

pub(crate) const CACHE_UPLOADS: usize = 64;
pub(crate) const CACHE_PARTS: usize = 4096;

pub struct Store {
    pub(crate) activity: Arc<super::upload::activity::Activity>,
    pub(crate) recovery_control: std::sync::OnceLock<super::backup::control::ServiceControl>,
    pub operation_gate: Arc<super::operation_gate::OperationGate>,
    pub pools: Vec<Arc<Pool>>,
    pub(crate) signer: KeySigner,
    registry: Arc<Mutex<HashMap<Fingerprint, Weak<ActiveObject>>>>,
    pub(crate) progress: Mutex<HashMap<Fingerprint, Progress>>,
    pub(crate) admission: Arc<Semaphore>,
    background_admission: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    memory_bytes: usize,
    pub(crate) repair_manager: Arc<super::repair_manager::RepairManager>,
    pub(crate) read_repair: AtomicBool,
    pub(crate) runtime: std::sync::OnceLock<crate::settings::runtime::RuntimeConfig>,
    pub(crate) monitor: std::sync::Mutex<super::monitor::MonitorStatus>,
    pub(crate) snapshot_gate: Arc<RwLock<()>>,
    pub(crate) backup_slots: Arc<Semaphore>,
}

pub(crate) struct ActiveObject {
    pub lifecycle: Arc<RwLock<()>>,
    pub prepare: AsyncMutex<()>,
    pub parts: [AsyncMutex<()>; 64],
    background: Mutex<Option<Arc<tokio_util::sync::CancellationToken>>>,
}
pub(crate) struct BackgroundOwner {
    state: Arc<ActiveObject>,
    pub cancel: Arc<tokio_util::sync::CancellationToken>,
}
impl ActiveObject {
    pub fn cancel_background(&self) {
        if let Ok(active) = self.background.lock()
            && let Some(token) = &*active
        {
            token.cancel();
        }
    }
    pub fn background(self: &Arc<Self>) -> BackgroundOwner {
        let token = Arc::new(tokio_util::sync::CancellationToken::new());
        if let Ok(mut active) = self.background.lock()
            && let Some(old) = active.replace(token.clone())
        {
            old.cancel();
        }
        BackgroundOwner {
            state: self.clone(),
            cancel: token,
        }
    }
}
impl Drop for BackgroundOwner {
    fn drop(&mut self) {
        if let Ok(mut active) = self.state.background.lock()
            && active
                .as_ref()
                .is_some_and(|token| Arc::ptr_eq(token, &self.cancel))
        {
            active.take();
        }
    }
}

pub(crate) struct Lease {
    state: Option<Arc<ActiveObject>>,
    registry: Arc<Mutex<HashMap<Fingerprint, Weak<ActiveObject>>>>,
    fp: Fingerprint,
}
impl Lease {
    pub fn state(&self) -> &Arc<ActiveObject> {
        self.state.as_ref().expect("live lease")
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        drop(self.state.take());
        if let Ok(mut registry) = self.registry.lock()
            && registry
                .get(&self.fp)
                .is_some_and(|w| w.strong_count() == 0)
        {
            registry.remove(&self.fp);
        }
    }
}

pub(crate) struct Progress {
    pub plan: UploadPlan,
    pub roots: BTreeMap<u64, [u8; 32]>,
    pub touched: Instant,
    pub parents_persisted: bool,
}

#[derive(Debug, Serialize)]
pub struct InitializedUpload {
    pub object_key: String,
    pub total_bytes: u64,
    pub part_bytes: u64,
    pub parts: u64,
    pub expires_at: u64,
    pub durable: bool,
}

#[derive(Debug, Serialize)]
pub struct FinalizedObject {
    pub object_key: String,
    pub already_finalized: bool,
    pub total_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct UploadStatus {
    pub finalized: bool,
    pub finalized_shards: usize,
    pub parts: u64,
    pub checked_start: u64,
    pub checked_end: u64,
    pub missing_parts: Vec<u64>,
    pub unavailable_members: Vec<usize>,
}

#[derive(Clone)]
pub(crate) struct ShardFile {
    pub member: Arc<Member>,
    pub file: Arc<File>,
    pub header: ShardHeader,
    pub finalized: bool,
    pub root: Option<[u8; 32]>,
    pub readable_bytes: u64,
}

impl Store {
    pub fn new(
        pools: Vec<Pool>,
        signing_secret: [u8; 32],
        concurrent_operations: usize,
    ) -> Result<Arc<Self>> {
        Self::with_limits(
            pools,
            signing_secret,
            concurrent_operations,
            256 * 1024 * 1024,
        )
    }
    pub fn with_limits(
        pools: Vec<Pool>,
        signing_secret: [u8; 32],
        concurrent_operations: usize,
        memory_bytes: usize,
    ) -> Result<Arc<Self>> {
        if pools.is_empty() || !(1..=4096).contains(&concurrent_operations) {
            return Err(Error::Invalid(
                "invalid pool/operation configuration".into(),
            ));
        }
        if !(8 * 1024 * 1024..=16 * 1024 * 1024 * 1024usize).contains(&memory_bytes) {
            return Err(Error::Invalid(
                "operation memory budget must be 8 MiB through 16 GiB".into(),
            ));
        }
        let mut identities = std::collections::HashSet::new();
        for p in &pools {
            if working_bytes(p) > memory_bytes {
                return Err(Error::Invalid(
                    "pool stripe working set exceeds operation memory budget".into(),
                ));
            }
            super::ec::StripeCodec::new(&p.layout)?;
            if !identities.insert((p.layout.id, p.layout.generation)) {
                return Err(Error::Invalid("duplicate pool identity/generation".into()));
            }
        }
        Ok(Arc::new(Self {
            activity: super::upload::activity::Activity::new(),
            recovery_control: std::sync::OnceLock::new(),
            operation_gate: Arc::new(super::operation_gate::OperationGate::default()),
            pools: pools.into_iter().map(Arc::new).collect(),
            signer: KeySigner::new(signing_secret),
            registry: Arc::new(Mutex::new(HashMap::new())),
            progress: Mutex::new(HashMap::new()),
            admission: Arc::new(Semaphore::new(concurrent_operations)),
            background_admission: Arc::new(Semaphore::new((concurrent_operations / 4).clamp(1, 8))),
            memory: Arc::new(Semaphore::new(memory_bytes / 65536)),
            memory_bytes,
            repair_manager: super::repair_manager::RepairManager::new(),
            read_repair: AtomicBool::new(true),
            runtime: std::sync::OnceLock::new(),
            monitor: std::sync::Mutex::new(Default::default()),
            snapshot_gate: Arc::new(RwLock::new(())),
            backup_slots: Arc::new(Semaphore::new(1)),
        }))
    }

    pub(crate) async fn admit(&self, pool: &Pool) -> Result<Admission> {
        self.admit_bytes(working_bytes(pool), false).await
    }
    pub(crate) async fn admit_background(&self, pool: &Pool) -> Result<Admission> {
        self.admit_bytes(working_bytes(pool), true).await
    }
    pub(crate) async fn admit_download(&self, pool: &Pool) -> Result<Admission> {
        self.admit_bytes(
            working_bytes(pool) + super::object_parser::stream_readahead_bytes(),
            false,
        )
        .await
    }
    pub(crate) async fn admit_metadata(&self, pool: &Pool) -> Result<Admission> {
        self.admit_bytes(65536 + 4096 * pool.layout.width(), false)
            .await
    }
    async fn admit_bytes(&self, bytes: usize, background: bool) -> Result<Admission> {
        if bytes.div_ceil(65536) > self.memory_bytes / 65536 {
            return Err(Error::Invalid(
                "operation working set exceeds memory budget".into(),
            ));
        }
        let acquire = async {
            let slot = if background {
                &self.background_admission
            } else {
                &self.admission
            }
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Unavailable("engine stopping".into()))?;
            let memory = self
                .memory
                .clone()
                .acquire_many_owned(bytes.div_ceil(65536) as u32)
                .await
                .map_err(|_| Error::Unavailable("engine stopping".into()))?;
            Ok(Admission {
                _slot: slot,
                _memory: memory,
            })
        };
        tokio::time::timeout(crate::io_control::foreground_wait_timeout(), acquire)
            .await
            .map_err(|_| Error::Unavailable("operation admission is full".into()))?
    }

    pub fn configure_read_repair(&self, enabled: bool) {
        self.read_repair
            .store(enabled, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn configure_runtime(
        &self,
        config: crate::settings::runtime::RuntimeConfig,
    ) -> Result<()> {
        config.validate()?;
        self.runtime
            .set(config)
            .map_err(|_| Error::Conflict("object runtime already configured".into()))
    }
    pub(crate) fn runtime(&self) -> &crate::settings::runtime::RuntimeConfig {
        static DEFAULT: std::sync::LazyLock<crate::settings::runtime::RuntimeConfig> =
            std::sync::LazyLock::new(Default::default);
        self.runtime.get().unwrap_or(&DEFAULT)
    }

    fn fresh_plan(
        &self,
        _scope: [u8; 16],
        pool_index: usize,
        total_bytes: u64,
        part_bytes: u64,
        durability: Durability,
        ttl_seconds: u64,
    ) -> Result<UploadPlan> {
        self.operation_gate.ensure_mutations_allowed()?;
        if self
            .runtime()
            .max_file_size
            .is_some_and(|limit| total_bytes > limit)
        {
            return Err(Error::Invalid(
                "object exceeds configured max_file_size".into(),
            ));
        }
        if ttl_seconds == 0 || ttl_seconds > 30 * 24 * 3600 {
            return Err(Error::Invalid(
                "upload lifetime must be 1 second through 30 days".into(),
            ));
        }
        let pool = self
            .pools
            .get(pool_index)
            .ok_or_else(|| Error::Invalid("unknown allocation pool".into()))?;
        pool.writable()?;
        let mut token = [0; 32];
        OsRng.fill_bytes(&mut token);
        let plan = UploadPlan {
            identity: Identity {
                pool: pool.layout.id,
                generation: pool.layout.generation,
                token,
            },
            total_bytes,
            part_bytes,
            expires_at: now() + ttl_seconds,
            durability,
        };
        plan.validate(&pool.layout)?;
        n1_link::shard::ShardHeader::new(&pool.layout, plan, 0)?;
        Ok(plan)
    }

    /// Begin is the sole creation boundary. Continuation only opens existing
    /// incomplete/final files, so deleting them consumes the ability to resume.
    pub async fn init(
        self: &Arc<Self>,
        scope: [u8; 16],
        pool_index: usize,
        total_bytes: u64,
        part_bytes: u64,
        durability: Durability,
        ttl_seconds: u64,
    ) -> Result<InitializedUpload> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            let plan = store.fresh_plan(
                scope,
                pool_index,
                total_bytes,
                part_bytes,
                durability,
                ttl_seconds,
            )?;
            let pool = store.pool(plan.identity)?;
            let io = store.upload_io(scope, "begin upload").await?;
            let _admission = store.admit_metadata(&pool).await?;
            let fp = plan.identity.fingerprint();
            let lease = store.foreground(fp)?;
            let _guard = lease.state().lifecycle.clone().write_owned().await;
            let roots = pool
                .members
                .iter()
                .flatten()
                .map(|m| m.path.clone())
                .collect::<Vec<_>>();
            let group = blocking(move || {
                super::linux_io::group::FilesystemGroup::open(&roots).map_err(Error::from)
            })
            .await?;
            let jobs = (0..pool.layout.width()).map(|shard| {
                let holder = pool.holder(fp, shard as u16);
                let layout = pool.layout.clone();
                blocking(move || {
                    let member = holder?;
                    let header = ShardHeader::new(&layout, plan, shard as u16)?;
                    // Begin allocates only the incomplete header. Full payload
                    // capacity is checked at the first write, as before; a
                    // declared object length is not a disk-space reservation.
                    member.check_capacity(128)?;
                    let (file, new) = match member.open_stage(fp, false, true) {
                        Ok(opened) => opened,
                        Err(error) => {
                            // mkdir can succeed before the shard allocation
                            // fails (for example, ENOSPC). Reclaim only an
                            // empty directory; never remove collision data.
                            member.retire_empty_stage(fp, false)?;
                            return Err(error);
                        }
                    };
                    if !new {
                        return Err(Error::Conflict(
                            "fresh LINK identity already has incomplete data".into(),
                        ));
                    }
                    if let Err(error) =
                        super::linux_io::pwrite_all_at(&*file, &header.encode(fp), 0)
                    {
                        drop(file);
                        member.unlink(Path::new(&fp.staging_path()), false)?;
                        member.retire_empty_stage(fp, false)?;
                        return Err(error.into());
                    }
                    Ok(member)
                })
            });
            let results = futures_util::future::join_all(jobs).await;
            if results.iter().any(Result::is_err) {
                // Every worker has ended. Roll back only files created by this
                // fresh allocation; never remove a pre-existing collision.
                let cleanups = results
                    .iter()
                    .filter_map(|r| r.as_ref().ok())
                    .map(|member| {
                        let member = member.clone();
                        blocking(move || {
                            member.unlink(Path::new(&fp.staging_path()), false)?;
                            member.retire_empty_stage(fp, false)
                        })
                    });
                let cleanup = collect(futures_util::future::join_all(cleanups).await);
                let persisted = blocking(move || group.sync_all().map_err(Error::from)).await;
                cleanup?;
                persisted?;
                collect(results)?;
                unreachable!("failed allocation returns its error")
            }
            // Begin acknowledges allocation, not durable payload. The first
            // durable part persists its header and complete staging ancestry;
            // normal/batch PUT and finalize persist data before publication.
            // An unused allocation may disappear on power loss. It can never
            // be recreated by a continuation request.
            blocking(move || group.check().map_err(Error::from)).await?;
            io.complete();
            Ok(InitializedUpload {
                object_key: store
                    .signer
                    .object_key(&scope, plan.identity)
                    .as_str()
                    .into(),
                total_bytes,
                part_bytes,
                parts: plan.parts(),
                expires_at: plan.expires_at,
                durable: durability == Durability::Durable,
            })
        })
        .await
    }

    pub(crate) fn pool(&self, identity: Identity) -> Result<Arc<Pool>> {
        self.pools
            .iter()
            .find(|p| p.layout.accepts(identity))
            .cloned()
            .ok_or_else(|| Error::Unavailable("LINK pool generation is not configured".into()))
    }

    pub async fn inspect_upload(&self, scope: [u8; 16], key: &str) -> Result<UploadPlan> {
        let identity = self.signer.verify_object(&scope, key)?;
        let pool = self.pool(identity)?;
        let _admission = self.admit_metadata(&pool).await?;
        let lease = self.lease(identity.fingerprint())?;
        let _guard = lease.state().lifecycle.clone().read_owned().await;
        blocking(move || {
            let fp = identity.fingerprint();
            let mut unavailable = false;
            for shard in 0..pool.layout.width() {
                let member = match pool.holder(fp, shard as u16) {
                    Ok(m) => m,
                    Err(Error::Unavailable(_)) => {
                        unavailable = true;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                for path in [fp.shard_path(shard as u16), fp.staging_path()] {
                    let file = match member.open_read(Path::new(&path)) {
                        Ok(f) => f,
                        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(e) => return Err(e),
                    };
                    let mut bytes = [0; 128];
                    super::linux_io::pread_exact_at(&*file, &mut bytes, 0)?;
                    let header = ShardHeader::decode(&bytes, &pool.layout, fp, shard as u16)
                        .map_err(|e| corrupt(e.to_string()))?;
                    let plan = UploadPlan {
                        identity,
                        total_bytes: header.total_bytes,
                        part_bytes: header.part_bytes,
                        expires_at: header.expires_at,
                        durability: header.durability,
                    };
                    plan.validate(&pool.layout)?;
                    return Ok(plan);
                }
            }
            if unavailable {
                Err(Error::Unavailable("upload members are unavailable".into()))
            } else {
                Err(Error::NotFound)
            }
        })
        .await
    }

    pub(crate) fn lease(&self, fp: Fingerprint) -> Result<Lease> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::Unavailable("object coordination poisoned".into()))?;
        let state = registry
            .get(&fp)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let state = Arc::new(ActiveObject {
                    lifecycle: Arc::new(RwLock::new(())),
                    prepare: AsyncMutex::new(()),
                    parts: std::array::from_fn(|_| AsyncMutex::new(())),
                    background: Mutex::new(None),
                });
                registry.insert(fp, Arc::downgrade(&state));
                state
            });
        Ok(Lease {
            state: Some(state),
            registry: self.registry.clone(),
            fp,
        })
    }
    pub(crate) fn foreground(&self, fp: Fingerprint) -> Result<Lease> {
        let lease = self.lease(fp)?;
        lease.state().cancel_background();
        Ok(lease)
    }

    pub(crate) fn cached_root(
        &self,
        fp: Fingerprint,
        plan: UploadPlan,
        part: u64,
    ) -> Option<[u8; 32]> {
        self.progress
            .lock()
            .ok()?
            .get_mut(&fp)
            .filter(|p| p.plan == plan)
            .and_then(|p| {
                p.touched = Instant::now();
                p.roots.get(&part).copied()
            })
    }

    pub(crate) fn forget_part(&self, fp: Fingerprint, part: u64) {
        if let Ok(mut cache) = self.progress.lock()
            && let Some(p) = cache.get_mut(&fp)
        {
            p.roots.remove(&part);
        }
    }

    pub(crate) fn remember_part(
        &self,
        fp: Fingerprint,
        plan: UploadPlan,
        part: u64,
        root: [u8; 32],
    ) {
        if plan.parts() > CACHE_PARTS as u64 {
            return;
        }
        if let Ok(mut cache) = self.progress.lock() {
            if !cache.contains_key(&fp)
                && cache.len() >= CACHE_UPLOADS
                && let Some(oldest) = cache
                    .iter()
                    .min_by_key(|(_, p)| p.touched)
                    .map(|(fp, _)| *fp)
            {
                cache.remove(&oldest);
            }
            let p = cache.entry(fp).or_insert_with(|| Progress {
                plan,
                roots: BTreeMap::new(),
                touched: Instant::now(),
                parents_persisted: false,
            });
            if p.plan == plan {
                p.roots.insert(part, root);
                p.touched = Instant::now();
            }
        }
    }

    pub(crate) fn forget(&self, fp: Fingerprint) {
        if let Ok(mut cache) = self.progress.lock() {
            cache.remove(&fp);
        }
    }

    pub(crate) async fn files(
        &self,
        pool: Arc<Pool>,
        plan: UploadPlan,
    ) -> Result<Vec<Option<ShardFile>>> {
        let fp = plan.identity.fingerprint();
        let jobs = (0..pool.layout.width()).map(|shard| {
            let holder = pool.holder(fp, shard as u16);
            let layout = pool.layout.clone();
            blocking(move || {
                let member = match holder {
                    Ok(m) => m,
                    Err(Error::Unavailable(_)) => return Ok(None),
                    Err(e) => return Err(e),
                };
                let final_path = fp.shard_path(shard as u16);
                let (file, finalized) = match member.open_read(Path::new(&final_path)) {
                    Ok(f) => (f, true),
                    Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                        match member.open_stage(fp, false, false) {
                            Ok((f, _)) => (f, false),
                            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                                return Ok(None);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Err(e) => return Err(e),
                };
                let mut bytes = [0; 128];
                crate::core::linux_io::pread_exact_at(&*file, &mut bytes, 0)?;
                let header = ShardHeader::decode(&bytes, &layout, fp, shard as u16)
                    .map_err(|e| corrupt(e.to_string()))?;
                if !header.matches_plan(plan) {
                    return Err(Error::Conflict(
                        "upload parameters conflict with existing shard".into(),
                    ));
                }
                if file.metadata()?.len() > header.file_bytes()? {
                    return Err(corrupt("shard has unexpected trailing bytes"));
                }
                let root = if finalized {
                    if file.metadata()?.len() != header.file_bytes()? {
                        return Err(corrupt("final shard length differs from format"));
                    }
                    let mut seal = [0; 96];
                    crate::core::linux_io::pread_exact_at(&*file, &mut seal, header.seal_offset())?;
                    Some(
                        n1_link::shard::decode_seal(&header, fp, &seal)
                            .map_err(|e| corrupt(e.to_string()))?,
                    )
                } else {
                    None
                };
                let readable_bytes = file.metadata()?.len();
                Ok(Some(ShardFile {
                    member,
                    file,
                    header,
                    finalized,
                    root,
                    readable_bytes,
                }))
            })
        });
        collect(futures_util::future::join_all(jobs).await)
    }
}

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| Error::Unavailable(format!("filesystem worker failed: {e}")))?
}
pub(crate) async fn joined<T: Send + 'static>(
    activity: Arc<super::upload::activity::Activity>,
    work: impl std::future::Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    // The spawned owner survives caller cancellation and drains all admitted I/O.
    activity.run(work).await
}
pub(crate) fn collect<T>(results: Vec<Result<T>>) -> Result<Vec<T>> {
    results.into_iter().collect()
}

pub(crate) struct Admission {
    _slot: tokio::sync::OwnedSemaphorePermit,
    _memory: tokio::sync::OwnedSemaphorePermit,
}
pub(crate) fn working_bytes(pool: &Pool) -> usize {
    // Source/padding, owned I/O, encoded records and both ECS workspaces. This
    // reserve is charged before allocating a codec, independently of object size.
    16 * pool.layout.width() * pool.layout.block_bytes as usize + 4 * 1024 * 1024
}

#[cfg(test)]
impl Store {
    pub(crate) fn background_active(&self, fp: Fingerprint) -> bool {
        self.registry
            .lock()
            .unwrap()
            .get(&fp)
            .and_then(Weak::upgrade)
            .is_some_and(|state| state.background.lock().unwrap().is_some())
    }
}
