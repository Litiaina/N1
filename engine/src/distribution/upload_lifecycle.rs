//! Durable, holder-local upload ownership. This module does not infer orphan
//! ownership from age or NCNX absence. Coordinators must obtain the independent
//! abort certificate before requesting physical reclamation.
use crate::core::disk_manager::ObjectMetadata;
use crate::core::fragment_identity::physical_fragment_root_on;
use crate::core::index_store;
use crate::core::upload::UploadSessionManifest;
use crate::distribution::membership::ClusterRuntime;
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use ulid::Ulid;

const OWNERSHIP_FILE: &str = "ownership.n2";
const MAX_RECORD_BYTES: u64 = 16 * 1024;
const MAX_PAGE_SIZE: usize = 1000;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct UploadIdentity {
    pub cluster_id: String,
    pub pool_id: String,
    pub pool_generation: u64,
    pub fragment: String,
    pub object_id: String,
    pub version_id: String,
    pub session_id: String,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub created_at_unix: i64,
}

impl UploadIdentity {
    pub fn from_manifest(runtime: &ClusterRuntime, manifest: &UploadSessionManifest) -> Self {
        Self {
            cluster_id: runtime.membership.cluster_id.clone(),
            pool_id: manifest.pool_id.clone(),
            pool_generation: manifest.pool_generation,
            fragment: manifest.fragment.clone(),
            object_id: manifest.object_key.clone(),
            version_id: manifest.version_id.clone(),
            session_id: manifest.session_id.clone(),
            data_shards: manifest.data_shards,
            parity_shards: manifest.parity_shards,
            created_at_unix: manifest.created_at,
        }
    }

    pub fn from_metadata(runtime: &ClusterRuntime, metadata: &ObjectMetadata) -> Self {
        Self {
            cluster_id: runtime.membership.cluster_id.clone(),
            pool_id: metadata.pool_id.clone(),
            pool_generation: metadata.pool_generation,
            fragment: metadata.fragment.clone(),
            object_id: metadata.object_key.clone(),
            version_id: metadata.version_id.clone(),
            session_id: metadata.upload_session_id.clone(),
            data_shards: metadata.data_shards,
            parity_shards: metadata.parity_shards,
            created_at_unix: metadata.created_at,
        }
    }

    fn validate(&self) -> Result<(), AppError> {
        for value in [
            &self.cluster_id,
            &self.pool_id,
            &self.fragment,
            &self.object_id,
        ] {
            if value.is_empty()
                || value.len() > 128
                || !value
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
            {
                return Err(AppError::BadRequest(
                    "invalid upload ownership identity".into(),
                ));
            }
        }
        if self.version_id.len() != 26
            || Ulid::from_string(&self.version_id).is_err()
            || self.session_id.len() != 26
            || Ulid::from_string(&self.session_id).is_err()
            || self.pool_generation == 0
            || self.data_shards == 0
            || self.parity_shards == 0
        {
            return Err(AppError::BadRequest(
                "invalid upload ownership contract".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UploadLifecycleState {
    Open,
    Committed,
    Aborted,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct UploadLifecycleRecord {
    pub identity: UploadIdentity,
    pub state: UploadLifecycleState,
    pub updated_at_unix: i64,
}

#[derive(Deserialize, Serialize)]
struct StoredRecord {
    format_version: u32,
    record: UploadLifecycleRecord,
    sha256: String,
}

// Ownership files are keyed by fragment/version. A caller changing object ID
// must contend on the same lock before the stored immutable identity rejects it.
type VersionKey = (String, String);
static VERSION_LOCKS: LazyLock<Mutex<BTreeMap<VersionKey, Weak<VersionLock>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

struct VersionLock {
    key: VersionKey,
    lock: Arc<RwLock<()>>,
    activity_writer: Mutex<()>,
}

impl Drop for VersionLock {
    fn drop(&mut self) {
        if let Ok(mut locks) = VERSION_LOCKS.lock()
            && locks
                .get(&self.key)
                .is_some_and(|value| std::ptr::eq(value.as_ptr(), self))
        {
            locks.remove(&self.key);
        }
    }
}

fn version_lock(
    fragment: &str,
    _object: &str,
    version: &str,
) -> Result<Arc<VersionLock>, AppError> {
    let key = (fragment.to_string(), version.to_string());
    let mut locks = VERSION_LOCKS.lock().map_err(|_| {
        AppError::ServiceUnavailable("upload ownership registry is poisoned".into())
    })?;
    Ok(locks.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
        let lock = Arc::new(VersionLock {
            key: key.clone(),
            lock: Arc::new(RwLock::new(())),
            activity_writer: Mutex::new(()),
        });
        locks.insert(key, Arc::downgrade(&lock));
        lock
    }))
}

/// Keep this guard until the shard's final rename and durability barrier have
/// completed, including HTTP/native streaming and detached local disk work.
#[derive(Clone)]
pub struct UploadReadGuard {
    _guard: Arc<OwnedRwLockReadGuard<()>>,
    _owner: Arc<VersionLock>,
    pub record: UploadLifecycleRecord,
    store: OwnershipStore,
}

impl UploadReadGuard {
    /// Caller is a synchronous publication worker already holding the binding
    /// read guard. Do not reacquire it here: a queued binding writer could
    /// otherwise deadlock a nested read on a writer-preferring lock.
    pub(crate) fn checkpoint_activity_under_binding(&self) -> Result<(), AppError> {
        if self.record.state != UploadLifecycleState::Open {
            return Ok(());
        }
        let _activity = self._owner.activity_writer.lock().map_err(|_| {
            AppError::ServiceUnavailable("upload activity writer is poisoned".into())
        })?;
        let mut record = self.store.load(&self.record.identity)?.ok_or_else(|| {
            AppError::Conflict("upload ownership disappeared during part completion".into())
        })?;
        if record.state != UploadLifecycleState::Open {
            return Err(AppError::Conflict(
                "upload state changed before part completion".into(),
            ));
        }
        record.updated_at_unix = record.updated_at_unix.max(chrono::Utc::now().timestamp());
        self.store.save(&record)
    }
    /// Checkpoint successful part activity after the final payload publication
    /// and before releasing this read guard. Only tiny state updates serialize;
    /// unrelated parts still stream concurrently and no extra RPC is required.
    pub async fn touch_activity(&self) -> Result<(), AppError> {
        if self.record.state != UploadLifecycleState::Open {
            return Ok(());
        }
        let held = self._guard.clone();
        let owner = self._owner.clone();
        let store = self.store.clone();
        let identity = self.record.identity.clone();
        tokio::task::spawn_blocking(move || {
            let _held = held;
            let _activity = owner.activity_writer.lock().map_err(|_| {
                AppError::ServiceUnavailable("upload activity writer is poisoned".into())
            })?;
            let _binding = store.guard_binding()?;
            let mut record = store.load(&identity)?.ok_or_else(|| {
                AppError::Conflict("upload ownership disappeared during part completion".into())
            })?;
            if record.state != UploadLifecycleState::Open {
                return Err(AppError::Conflict(
                    "upload state changed before part completion".into(),
                ));
            }
            record.updated_at_unix = record.updated_at_unix.max(chrono::Utc::now().timestamp());
            store.save(&record)
        })
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("checkpoint upload activity worker: {e}"))
        })?
    }
}

pub struct UploadWriteGuard {
    // Persistence workers retain an Arc to the exclusive guard. Cancelling an
    // awaiting caller cannot admit a successor around a still-running rename.
    guard: Arc<OwnedRwLockWriteGuard<()>>,
    _owner: Arc<VersionLock>,
    store: OwnershipStore,
    pub record: UploadLifecycleRecord,
}

#[derive(Clone)]
struct OwnershipStore {
    roots: Vec<PathBuf>,
    binding: Option<BindingProof>,
}

#[derive(Clone)]
struct BindingProof {
    cluster_id: String,
    slot_id: String,
    instance_id: String,
    binding_epoch: u64,
    roots: Vec<(String, String, String)>,
}

impl OwnershipStore {
    fn from_runtime(runtime: &ClusterRuntime) -> Result<Self, AppError> {
        if runtime.local_is_reserve || runtime.local_storage_roots.is_empty() {
            return Err(AppError::ServiceUnavailable(
                "no active upload ownership holder".into(),
            ));
        }
        Ok(Self {
            roots: runtime
                .local_storage_roots
                .iter()
                .map(|r| PathBuf::from(&r.path))
                .collect(),
            binding: Some(BindingProof {
                cluster_id: runtime.membership.cluster_id.clone(),
                slot_id: runtime.local_node_id.clone(),
                instance_id: runtime.local_instance_id.clone(),
                binding_epoch: runtime.membership.binding_epoch,
                roots: runtime
                    .local_storage_roots
                    .iter()
                    .map(|r| (r.id.clone(), r.physical_id().to_string(), r.path.clone()))
                    .collect(),
            }),
        })
    }

    fn guard_binding(&self) -> Result<std::sync::RwLockReadGuard<'static, ()>, AppError> {
        let guard = crate::distribution::membership::guard_cluster_binding()?;
        if let Some(expected) = &self.binding {
            match crate::distribution::membership::try_active_cluster_runtime() {
                Some(current) => {
                    let roots = current
                        .local_storage_roots
                        .iter()
                        .map(|r| (r.id.clone(), r.physical_id().to_string(), r.path.clone()))
                        .collect::<Vec<_>>();
                    if current.local_is_reserve
                        || current.membership.cluster_id != expected.cluster_id
                        || current.local_node_id != expected.slot_id
                        || current.local_instance_id != expected.instance_id
                        || current.membership.binding_epoch != expected.binding_epoch
                        || roots != expected.roots
                    {
                        return Err(AppError::Conflict(
                            "upload ownership holder binding changed before disk work".into(),
                        ));
                    }
                    // An unmounted enrolled drive may leave an ordinary empty
                    // mountpoint directory behind. Its apparent absence of
                    // NCDX is not authority to vote abort or write a new fence
                    // onto the OS filesystem beneath that mountpoint.
                    for root in &current.local_storage_roots {
                        crate::core::root_identity::verify_local_root_identity(&current, root)
                            .map_err(|error| match error {
                                error @ AppError::Conflict(_) => error,
                                error => AppError::ServiceUnavailable(format!(
                                    "upload ownership root '{}' is unavailable: {error}",
                                    root.id
                                )),
                            })?;
                    }
                }
                None => {
                    // Unit tests instantiate independent holder snapshots;
                    // production must never act without its active binding.
                    #[cfg(not(test))]
                    return Err(AppError::ServiceUnavailable(
                        "upload ownership runtime is not active".into(),
                    ));
                }
            }
        }
        Ok(guard)
    }

    fn path(root: &Path, identity: &UploadIdentity) -> PathBuf {
        physical_fragment_root_on(root, &identity.fragment)
            .join(".uploads")
            .join(&identity.version_id)
            .join(OWNERSHIP_FILE)
    }

    fn terminal_path(root: &Path, identity: &UploadIdentity) -> PathBuf {
        // ULIDs begin with time, so their leading bytes would concentrate new
        // terminal records. Hash the full version and fan out on trailing hash
        // bytes; ordinary GC never traverses these permanent fences.
        let digest = Sha256::digest(identity.version_id.as_bytes());
        physical_fragment_root_on(root, &identity.fragment)
            .join(".uploads")
            .join(".fences")
            .join(format!("{:02x}", digest[30]))
            .join(format!("{:02x}", digest[31]))
            .join(format!("{}.n2", identity.version_id))
    }

    fn check_roots(&self) -> Result<(), AppError> {
        if self.roots.is_empty() {
            return Err(AppError::ServiceUnavailable(
                "upload ownership roots are unavailable".into(),
            ));
        }
        for root in &self.roots {
            let metadata = fs::metadata(root)
                .map_err(|e| AppError::Io(e, "inspect upload ownership root".into()))?;
            if !metadata.is_dir() {
                return Err(AppError::Conflict(
                    "upload ownership root is not a directory".into(),
                ));
            }
        }
        Ok(())
    }

    fn load(&self, identity: &UploadIdentity) -> Result<Option<UploadLifecycleRecord>, AppError> {
        identity.validate()?;
        self.check_roots()?;
        let mut selected: Option<UploadLifecycleRecord> = None;
        for root in &self.roots {
            for path in [
                Self::path(root, identity),
                Self::terminal_path(root, identity),
            ] {
                let Some(record) = read_record(&path)? else {
                    continue;
                };
                if record.identity != *identity {
                    return Err(AppError::Conflict(
                        "upload ownership identity differs from stored version".into(),
                    ));
                }
                if let Some(current) = selected.as_mut() {
                    if matches!(
                        (current.state, record.state),
                        (
                            UploadLifecycleState::Committed,
                            UploadLifecycleState::Aborted
                        ) | (
                            UploadLifecycleState::Aborted,
                            UploadLifecycleState::Committed
                        )
                    ) {
                        return Err(AppError::Conflict(
                            "upload ownership copies contain conflicting terminal states".into(),
                        ));
                    }
                    if current.state == UploadLifecycleState::Open
                        || record.state != UploadLifecycleState::Open
                    {
                        *current = record;
                    }
                } else {
                    selected = Some(record);
                }
            }
        }
        Ok(selected)
    }

    fn save(&self, record: &UploadLifecycleRecord) -> Result<(), AppError> {
        self.check_roots()?;
        #[cfg(test)]
        tests::pause_save_if_requested(record);
        let bytes = serde_json::to_vec(&StoredRecord {
            format_version: 1,
            record: record.clone(),
            sha256: record_checksum(record)?,
        })
        .map_err(|e| AppError::InternalServerError(format!("encode upload ownership: {e}")))?;
        if record.state != UploadLifecycleState::Open {
            // Terminal authority is durable on every root before any active
            // copy is pruned or any local abort vote is acknowledged.
            for root in &self.roots {
                write_record_at(root, &Self::terminal_path(root, &record.identity), &bytes)?;
            }
        }
        match record.state {
            UploadLifecycleState::Open | UploadLifecycleState::Aborted => {
                // Aborted stays on the active worklist until exact payload
                // cleanup succeeds; a crash can therefore resume that cleanup.
                for root in &self.roots {
                    write_record_at(root, &Self::path(root, &record.identity), &bytes)?;
                }
            }
            UploadLifecycleState::Committed => self.retire_active(&record.identity)?,
        }
        Ok(())
    }

    fn retire_active(&self, identity: &UploadIdentity) -> Result<(), AppError> {
        for root in &self.roots {
            let path = Self::path(root, identity);
            match fs::remove_file(&path) {
                Ok(()) => sync_directory(path.parent().expect("active ownership parent"))?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(AppError::Io(e, "retire active upload ownership".into())),
            }
            let directory = path.parent().expect("active ownership parent");
            match fs::remove_dir(directory) {
                Ok(()) => sync_directory(directory.parent().expect("uploads parent"))?,
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                    ) => {}
                Err(e) => return Err(AppError::Io(e, "retire empty upload work directory".into())),
            }
        }
        Ok(())
    }
}

fn write_record_at(root: &Path, path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().expect("ownership parent");
    create_durable_parents(root, parent)?;
    let tmp = parent.join(format!(".ownership.tmp.{}", Ulid::new()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)
        .map_err(|e| AppError::Io(e, "create upload ownership temporary record".into()))?;
    file.write_all(bytes)
        .map_err(|e| AppError::Io(e, "write upload ownership".into()))?;
    file.sync_all()
        .map_err(|e| AppError::Io(e, "sync upload ownership".into()))?;
    drop(file);
    fs::rename(&tmp, path).map_err(|e| AppError::Io(e, "publish upload ownership".into()))?;
    sync_directory(parent)
}

fn record_checksum(record: &UploadLifecycleRecord) -> Result<String, AppError> {
    let bytes = serde_json::to_vec(record).map_err(|e| {
        AppError::InternalServerError(format!("encode upload ownership checksum: {e}"))
    })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn read_record(path: &Path) -> Result<Option<UploadLifecycleRecord>, AppError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(AppError::Io(e, "read upload ownership".into())),
    };
    if !file
        .metadata()
        .map_err(|e| AppError::Io(e, "stat upload ownership".into()))?
        .is_file()
    {
        return Err(AppError::Conflict(
            "upload ownership is not a regular record".into(),
        ));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| AppError::Io(e, "read upload ownership bytes".into()))?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(AppError::Conflict(
            "upload ownership record exceeds size limit".into(),
        ));
    }
    let stored: StoredRecord = serde_json::from_slice(&bytes)
        .map_err(|e| AppError::Conflict(format!("invalid upload ownership record: {e}")))?;
    stored.record.identity.validate()?;
    if stored.format_version != 1 || stored.sha256 != record_checksum(&stored.record)? {
        return Err(AppError::Conflict(
            "upload ownership checksum/format mismatch".into(),
        ));
    }
    Ok(Some(stored.record))
}

fn sync_directory(path: &Path) -> Result<(), AppError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| AppError::Io(e, "sync upload ownership directory".into()))
}

fn create_durable_parents(root: &Path, parent: &Path) -> Result<(), AppError> {
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| AppError::Conflict("upload ownership path escaped storage root".into()))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(AppError::Conflict(
                "invalid upload ownership path component".into(),
            ));
        }
        let previous = current.clone();
        current.push(component);
        match fs::create_dir(&current) {
            Ok(()) => sync_directory(&previous)?,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&current)
                    .map_err(|e| AppError::Io(e, "inspect upload ownership directory".into()))?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(AppError::Conflict(
                        "upload ownership parent is not a real directory".into(),
                    ));
                }
            }
            Err(e) => return Err(AppError::Io(e, "create upload ownership directory".into())),
        }
    }
    Ok(())
}

fn validate_holder(runtime: &ClusterRuntime, identity: &UploadIdentity) -> Result<(), AppError> {
    identity.validate()?;
    let pool = runtime
        .membership
        .pool(&identity.pool_id)
        .ok_or_else(|| AppError::Conflict("unknown upload ownership pool".into()))?;
    if runtime.membership.cluster_id != identity.cluster_id
        || pool.generation != identity.pool_generation
        || pool.policy.object_data_shards != usize::from(identity.data_shards)
        || pool.policy.object_parity_shards != usize::from(identity.parity_shards)
        || !pool.node_ids.contains(&runtime.local_node_id)
        || runtime.local_is_reserve
    {
        return Err(AppError::Conflict(
            "upload ownership does not match this bound holder".into(),
        ));
    }
    Ok(())
}

async fn lock_version(
    store: OwnershipStore,
    identity: &UploadIdentity,
) -> Result<UploadWriteGuard, AppError> {
    lock_version_mode(store, identity, true).await
}

async fn lock_version_mode(
    store: OwnershipStore,
    identity: &UploadIdentity,
    wait: bool,
) -> Result<UploadWriteGuard, AppError> {
    identity.validate()?;
    let owner = version_lock(
        &identity.fragment,
        &identity.object_id,
        &identity.version_id,
    )?;
    let guard = Arc::new(if wait {
        owner.lock.clone().write_owned().await
    } else {
        owner.lock.clone().try_write_owned().map_err(|_| {
            AppError::ServiceUnavailable("upload is active; background abort deferred".into())
        })?
    });
    let store_copy = store.clone();
    let identity_copy = identity.clone();
    let held = guard.clone();
    let retained_owner = owner.clone();
    let record = tokio::task::spawn_blocking(move || {
        let _held = held;
        let _retained_owner = retained_owner;
        let _binding = store_copy.guard_binding()?;
        store_copy.load(&identity_copy)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("load upload ownership worker: {e}")))??
    .unwrap_or_else(|| UploadLifecycleRecord {
        identity: identity.clone(),
        state: UploadLifecycleState::Open,
        updated_at_unix: identity.created_at_unix,
    });
    Ok(UploadWriteGuard {
        guard,
        _owner: owner,
        store,
        record,
    })
}

impl UploadWriteGuard {
    async fn persist(&mut self, state: UploadLifecycleState) -> Result<(), AppError> {
        let mut record = self.record.clone();
        record.state = state;
        record.updated_at_unix = chrono::Utc::now().timestamp();
        let store = self.store.clone();
        let to_write = record.clone();
        let held = self.guard.clone();
        let retained_owner = self._owner.clone();
        tokio::task::spawn_blocking(move || {
            let _held = held;
            let _retained_owner = retained_owner;
            let _binding = store.guard_binding()?;
            store.save(&to_write)
        })
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("persist upload ownership worker: {e}"))
        })??;
        self.record = record;
        Ok(())
    }

    /// Call only after immutable local NCDX publication and its durability
    /// barriers. The exclusive guard must cover both operations.
    pub async fn mark_committed(&mut self) -> Result<(), AppError> {
        if self.record.state == UploadLifecycleState::Aborted {
            return Err(AppError::Conflict(
                "upload version is permanently aborted".into(),
            ));
        }
        self.persist(UploadLifecycleState::Committed).await
    }

    /// Publish immutable local NCDX and its terminal ownership state in one
    /// cancellation-safe worker. NCDX is durable first; a crash in between is
    /// protected by the NCDX inspection performed before any abort vote.
    pub async fn publish_metadata(&mut self, metadata: ObjectMetadata) -> Result<(), AppError> {
        let id = &self.record.identity;
        if metadata.fragment != id.fragment
            || metadata.object_key != id.object_id
            || metadata.version_id != id.version_id
            || metadata.upload_session_id != id.session_id
            || metadata.pool_id != id.pool_id
            || metadata.pool_generation != id.pool_generation
            || metadata.data_shards != id.data_shards
            || metadata.parity_shards != id.parity_shards
            || self.record.state == UploadLifecycleState::Aborted
        {
            return Err(AppError::Conflict(
                "NCDX publication differs from upload ownership".into(),
            ));
        }
        let mut record = self.record.clone();
        record.state = UploadLifecycleState::Committed;
        record.updated_at_unix = chrono::Utc::now().timestamp();
        let to_write = record.clone();
        let store = self.store.clone();
        let held = self.guard.clone();
        let retained_owner = self._owner.clone();
        tokio::task::spawn_blocking(move || {
            let _held = held;
            let _retained_owner = retained_owner;
            let _binding = store.guard_binding()?;
            store.check_roots()?;
            index_store::upsert_live_version(&metadata)?;
            store.save(&to_write)
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("publish upload NCDX worker: {e}")))??;
        self.record = record;
        Ok(())
    }

    /// Call only after the coordinator has verified an M+1 abort certificate.
    /// The durable Aborted record is retained permanently; only this exact
    /// uncommitted version's payload directory is removed on each local root.
    pub async fn cleanup_aborted_payload(&self) -> Result<u64, AppError> {
        if self.record.state != UploadLifecycleState::Aborted {
            return Err(AppError::Conflict(
                "payload cleanup requires durable aborted ownership".into(),
            ));
        }
        let identity = self.record.identity.clone();
        let store = self.store.clone();
        let held = self.guard.clone();
        let retained_owner = self._owner.clone();
        tokio::task::spawn_blocking(move || {
            let _held = held;
            let _retained_owner = retained_owner;
            let _binding = store.guard_binding()?;
            store.check_roots()?;
            if index_store::read_live_version(
                &identity.fragment,
                &identity.object_id,
                &identity.version_id,
            )?
            .is_some()
            {
                return Err(AppError::Conflict(
                    "committed NCDX still protects this version".into(),
                ));
            }
            let mut removed = 0u64;
            for root in &store.roots {
                let fanout = crate::core::disk_manager::ObjectPathManager::object_fanout_segments(
                    &identity.object_id,
                );
                let target = physical_fragment_root_on(root, &identity.fragment)
                    .join(".objects")
                    .join(&fanout[0])
                    .join(&fanout[1])
                    .join(&fanout[2])
                    .join(&fanout[3])
                    .join(format!("@o_{}", identity.object_id))
                    .join(&identity.version_id);
                let relative = target.strip_prefix(root).map_err(|_| {
                    AppError::Conflict("upload cleanup escaped storage root".into())
                })?;
                let mut current = root.clone();
                let mut absent = false;
                for component in relative.components() {
                    if !matches!(component, std::path::Component::Normal(_)) {
                        return Err(AppError::Conflict("invalid upload cleanup path".into()));
                    }
                    current.push(component);
                    match fs::symlink_metadata(&current) {
                        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                        }
                        Ok(_) => {
                            return Err(AppError::Conflict(
                                "upload cleanup path is not a real directory".into(),
                            ));
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            absent = true;
                            break;
                        }
                        Err(e) => {
                            return Err(AppError::Io(
                                e,
                                "inspect exact aborted upload payload".into(),
                            ));
                        }
                    }
                }
                if absent {
                    continue;
                }
                fs::remove_dir_all(&target)
                    .map_err(|e| AppError::Io(e, "remove exact aborted upload payload".into()))?;
                sync_directory(target.parent().expect("payload parent"))?;
                removed = removed.saturating_add(1);
            }
            store.retire_active(&identity)?;
            Ok(removed)
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("cleanup aborted upload worker: {e}")))?
    }

    async fn protect_local_ncdx(&mut self) -> Result<bool, AppError> {
        let identity = self.record.identity.clone();
        let held = self.guard.clone();
        let retained_owner = self._owner.clone();
        let store = self.store.clone();
        let committed = tokio::task::spawn_blocking(move || {
            let _held = held;
            let _retained_owner = retained_owner;
            let _binding = store.guard_binding()?;
            index_store::read_live_version(
                &identity.fragment,
                &identity.object_id,
                &identity.version_id,
            )
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("inspect local NCDX worker: {e}")))??;
        if let Some(metadata) = committed {
            let identity = &self.record.identity;
            if metadata.pool_id != identity.pool_id
                || metadata.pool_generation != identity.pool_generation
                || metadata.upload_session_id != identity.session_id
                || metadata.data_shards != identity.data_shards
                || metadata.parity_shards != identity.parity_shards
            {
                return Err(AppError::Conflict(
                    "local NCDX differs from upload ownership".into(),
                ));
            }
            self.mark_committed().await?;
            return Ok(true);
        }
        Ok(false)
    }
}

pub async fn ensure_open(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
) -> Result<UploadLifecycleRecord, AppError> {
    validate_holder(runtime, identity)?;
    let mut guard = lock_version(OwnershipStore::from_runtime(runtime)?, identity).await?;
    if guard.record.state == UploadLifecycleState::Aborted {
        return Err(AppError::Conflict(
            "upload version is permanently aborted".into(),
        ));
    }
    if guard.record.state == UploadLifecycleState::Open && !guard.protect_local_ncdx().await? {
        guard.persist(UploadLifecycleState::Open).await?;
    }
    Ok(guard.record)
}

/// Internal trust boundary: the caller MUST first verify K byte-identical NCDX
/// responses from distinct current exact holders. Ordinary upload requests may
/// not call this to invent commit authority. The compact state permits repair
/// of a missing holder without requiring its lost shard to exist first.
pub(crate) async fn bootstrap_committed(
    runtime: &ClusterRuntime,
    metadata: &ObjectMetadata,
) -> Result<UploadLifecycleRecord, AppError> {
    let identity = UploadIdentity::from_metadata(runtime, metadata);
    validate_holder(runtime, &identity)?;
    let mut guard = lock_version(OwnershipStore::from_runtime(runtime)?, &identity).await?;
    if guard.record.state == UploadLifecycleState::Aborted {
        return Err(AppError::Conflict(
            "local aborted ownership cannot be replaced by repair bootstrap".into(),
        ));
    }
    guard.persist(UploadLifecycleState::Committed).await?;
    Ok(guard.record)
}

pub async fn guard_metadata_publication(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
) -> Result<UploadWriteGuard, AppError> {
    validate_holder(runtime, identity)?;
    let store = OwnershipStore::from_runtime(runtime)?;
    let guard = lock_version(store.clone(), identity).await?;
    // Unlike ensure_open, publication cannot invent ownership after receiving
    // a delayed request. It requires the durable pre-shard registration.
    let store_copy = store.clone();
    let identity_copy = identity.clone();
    let held = guard.guard.clone();
    let retained_owner = guard._owner.clone();
    let registered = tokio::task::spawn_blocking(move || {
        let _held = held;
        let _retained_owner = retained_owner;
        let _binding = store_copy.guard_binding()?;
        store_copy.load(&identity_copy)
    })
    .await
    .map_err(|e| {
        AppError::InternalServerError(format!("check upload registration worker: {e}"))
    })??;
    if registered.is_none() {
        return Err(AppError::Conflict(
            "upload ownership was not registered".into(),
        ));
    }
    if guard.record.state == UploadLifecycleState::Aborted {
        return Err(AppError::Conflict(
            "upload version is permanently aborted".into(),
        ));
    }
    Ok(guard)
}

pub async fn guard_shard_write(
    runtime: &ClusterRuntime,
    pool_id: &str,
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<UploadReadGuard, AppError> {
    guard_shard_state(
        runtime,
        pool_id,
        fragment,
        object_id,
        version_id,
        UploadLifecycleState::Open,
    )
    .await
}

/// Repair callers must additionally prove the exact shard checksum from
/// committed immutable NCDX; this guard only excludes concurrent abort/purge.
pub async fn guard_committed_repair(
    runtime: &ClusterRuntime,
    pool_id: &str,
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<UploadReadGuard, AppError> {
    guard_shard_state(
        runtime,
        pool_id,
        fragment,
        object_id,
        version_id,
        UploadLifecycleState::Committed,
    )
    .await
}

async fn guard_shard_state(
    runtime: &ClusterRuntime,
    pool_id: &str,
    fragment: &str,
    object_id: &str,
    version_id: &str,
    required: UploadLifecycleState,
) -> Result<UploadReadGuard, AppError> {
    // Read the stored identity under the version lock, never trust a request's
    // guessed session ID to create a replacement record.
    let store = OwnershipStore::from_runtime(runtime)?;
    let lookup = UploadIdentity {
        cluster_id: runtime.membership.cluster_id.clone(),
        pool_id: pool_id.to_string(),
        pool_generation: runtime
            .membership
            .pool(pool_id)
            .ok_or_else(|| AppError::Conflict("unknown upload pool".into()))?
            .generation,
        fragment: fragment.to_string(),
        object_id: object_id.to_string(),
        version_id: version_id.to_string(),
        session_id: Ulid::nil().to_string(),
        data_shards: 1,
        parity_shards: 1,
        created_at_unix: 0,
    };
    lookup.validate()?;
    let owner = version_lock(fragment, object_id, version_id)?;
    let guard = Arc::new(owner.lock.clone().read_owned().await);
    let held = guard.clone();
    let retained_owner = owner.clone();
    let held_store = store.clone();
    let record = tokio::task::spawn_blocking(move || {
        let _held = held;
        let _retained_owner = retained_owner;
        let _binding = store.guard_binding()?;
        store.check_roots()?;
        let mut identity = None;
        for root in &store.roots {
            for path in [
                OwnershipStore::path(root, &lookup),
                OwnershipStore::terminal_path(root, &lookup),
            ] {
                if let Some(record) = read_record(&path)? {
                    identity = Some(record.identity);
                    break;
                }
            }
            if identity.is_some() {
                break;
            }
        }
        let identity = identity
            .ok_or_else(|| AppError::Conflict("upload ownership was not registered".into()))?;
        store
            .load(&identity)?
            .ok_or_else(|| AppError::Conflict("upload ownership disappeared".into()))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("guard upload shard worker: {e}")))??;
    let identity = &record.identity;
    if identity.pool_id != pool_id
        || identity.fragment != fragment
        || identity.object_id != object_id
        || identity.version_id != version_id
    {
        return Err(AppError::Conflict(
            "upload ownership differs from shard target".into(),
        ));
    }
    validate_holder(runtime, identity)?;
    if record.state != required {
        return Err(AppError::Conflict(format!(
            "shard operation requires {required:?} ownership, found {:?}",
            record.state
        )));
    }
    Ok(UploadReadGuard {
        _guard: guard,
        _owner: owner,
        record,
        store: held_store,
    })
}

/// This is a durable local abort vote, NOT permission to delete payload.
/// Coordinators need M+1 distinct current-holder votes before reclamation.
pub async fn abort_upload(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
) -> Result<UploadLifecycleRecord, AppError> {
    abort_upload_if_idle(runtime, identity, None).await
}

pub async fn abort_upload_if_idle(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
    idle_cutoff_unix: Option<i64>,
) -> Result<UploadLifecycleRecord, AppError> {
    validate_holder(runtime, identity)?;
    let mut guard = lock_version_mode(
        OwnershipStore::from_runtime(runtime)?,
        identity,
        idle_cutoff_unix.is_none(),
    )
    .await?;
    if guard.record.state == UploadLifecycleState::Open
        && idle_cutoff_unix.is_some_and(|cutoff| guard.record.updated_at_unix > cutoff)
    {
        return Err(AppError::Conflict(
            "upload activity advanced after GC observation".into(),
        ));
    }
    if guard.record.state == UploadLifecycleState::Committed || guard.protect_local_ncdx().await? {
        return Err(AppError::Conflict(
            "committed upload ownership cannot be aborted".into(),
        ));
    }
    if guard.record.state != UploadLifecycleState::Aborted {
        guard.persist(UploadLifecycleState::Aborted).await?;
    }
    Ok(guard.record)
}

pub async fn inspect_upload(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
) -> Result<Option<UploadLifecycleRecord>, AppError> {
    validate_holder(runtime, identity)?;
    let owner = version_lock(
        &identity.fragment,
        &identity.object_id,
        &identity.version_id,
    )?;
    let guard = owner.lock.clone().read_owned().await;
    let store = OwnershipStore::from_runtime(runtime)?;
    let identity = identity.clone();
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        let _owner = owner;
        let _binding = store.guard_binding()?;
        store.load(&identity)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("inspect upload ownership worker: {e}")))?
}

/// Exact commit observation under the same version guard as publication and
/// abort. A visible-but-not-yet-durable NCDX cannot be counted while its writer
/// still holds the exclusive guard. Legacy NCDX is accepted only after all old
/// writers have been stopped during upgrade.
pub async fn probe_committed_metadata(
    runtime: &ClusterRuntime,
    metadata: &ObjectMetadata,
) -> Result<bool, AppError> {
    let identity = UploadIdentity::from_metadata(runtime, metadata);
    validate_holder(runtime, &identity)?;
    let owner = version_lock(
        &identity.fragment,
        &identity.object_id,
        &identity.version_id,
    )?;
    let guard = owner.lock.clone().read_owned().await;
    let store = OwnershipStore::from_runtime(runtime)?;
    let expected =
        serde_json::to_vec(metadata).map_err(|e| AppError::InternalServerError(e.to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        let _owner = owner;
        let _binding = store.guard_binding()?;
        if store
            .load(&identity)?
            .is_some_and(|r| r.state == UploadLifecycleState::Aborted)
        {
            return Ok(false);
        }
        let actual = index_store::read_live_version(
            &identity.fragment,
            &identity.object_id,
            &identity.version_id,
        )?;
        match actual {
            Some(actual) => Ok(serde_json::to_vec(&actual)
                .map_err(|e| AppError::InternalServerError(e.to_string()))?
                == expected),
            None => Ok(false),
        }
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("commit probe worker: {e}")))?
}

/// The coordinator must validate its complete abort certificate before this
/// call. This guard alone never authorizes deletion of a Committed version.
pub async fn guard_aborted_cleanup(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
) -> Result<UploadWriteGuard, AppError> {
    validate_holder(runtime, identity)?;
    let guard = lock_version(OwnershipStore::from_runtime(runtime)?, identity).await?;
    if guard.record.state != UploadLifecycleState::Aborted {
        return Err(AppError::Conflict(
            "physical upload cleanup requires durable aborted ownership".into(),
        ));
    }
    Ok(guard)
}

/// Page only the narrow ownership-record directory; never walk `.objects`.
/// Returned records are candidates, not destructive authority. A caller must
/// reacquire a version guard and revalidate state before acting.
pub async fn list_upload_records(
    runtime: &ClusterRuntime,
    fragment: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<UploadLifecycleRecord>, AppError> {
    let store = OwnershipStore::from_runtime(runtime)?;
    if fragment.is_empty()
        || fragment.len() > 128
        || !fragment
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return Err(AppError::BadRequest(
            "invalid upload ownership fragment".into(),
        ));
    }
    let fragment = fragment.to_string();
    let after = after.map(ToOwned::to_owned);
    let limit = limit.clamp(1, MAX_PAGE_SIZE);
    tokio::task::spawn_blocking(move || {
        let _binding = store.guard_binding()?;
        store.check_roots()?;
        let mut selected: BTreeMap<String, UploadLifecycleRecord> = BTreeMap::new();
        for root in &store.roots {
            let uploads = physical_fragment_root_on(root, &fragment).join(".uploads");
            let entries = match fs::read_dir(uploads) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(AppError::Io(e, "list upload ownership candidates".into())),
            };
            for entry in entries {
                let entry =
                    entry.map_err(|e| AppError::Io(e, "read upload ownership candidate".into()))?;
                let Some(version) = entry.file_name().to_str().map(ToOwned::to_owned) else {
                    continue;
                };
                if version.len() != 26
                    || Ulid::from_string(&version).is_err()
                    || after.as_ref().is_some_and(|a| version <= *a)
                    || selected.len() == limit
                        && selected
                            .last_key_value()
                            .is_some_and(|(last, _)| &version > last)
                {
                    continue;
                }
                if !entry
                    .file_type()
                    .map_err(|e| AppError::Io(e, "stat upload ownership candidate".into()))?
                    .is_dir()
                {
                    continue;
                }
                if let Some(record) = read_record(&entry.path().join(OWNERSHIP_FILE))? {
                    if record.identity.fragment != fragment || record.identity.version_id != version
                    {
                        return Err(AppError::Conflict(
                            "upload ownership candidate path differs from identity".into(),
                        ));
                    }
                    let record = store.load(&record.identity)?.ok_or_else(|| {
                        AppError::Conflict("upload ownership candidate disappeared".into())
                    })?;
                    if selected.len() == limit && !selected.contains_key(&version) {
                        selected.pop_last();
                    }
                    selected.insert(version, record);
                }
            }
        }
        Ok(selected.into_values().collect())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("upload ownership listing worker: {e}")))?
}

#[cfg(test)]
#[path = "upload_lifecycle/tests.rs"]
mod tests;
