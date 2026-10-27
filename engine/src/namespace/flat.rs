//! N1 flat namespace. Physical directories organize keys but have no logical
//! identity, mutation authority, or empty-directory semantics.
//!
//! LIST reads names and file types only. HEAD decodes small checksummed NCX
//! pointers. Mutations copy unchanged NCX replicas durably before deleting
//! source replicas. An interrupted copy/delete may leave both names. There is
//! no mutable version head, namespace tombstone or deletion-generation store.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;
use walkdir::WalkDir;

use crate::core::disk_manager::ObjectPathManager;
use crate::core::index_store::{self, TrashKind, TrashRecord};
use crate::core::storage_pool::{self, StoragePool};
use crate::enums::app_error_enum::AppError;

pub(crate) mod batch;

pub const LIVE_DIR: &str = ".namespace";
pub const SUFFIX: &str = ".ncx";
const PREFIX_SUFFIX: &str = ".ncp";
const MAGIC: &[u8] = b"N1NCX4\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pointer {
    pub format: u16,
    pub pool_id: String,
    pub object_id: String,
}

type KeyLocks = HashMap<String, Weak<Mutex<()>>>;
static KEY_LOCKS: LazyLock<Mutex<KeyLocks>> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
pub(super) fn key_locked_for_test(fragment: &str, key: &str) -> bool {
    let fragment = index_store::stable_fragment_id_for_fragment(fragment);
    KEY_LOCKS
        .lock()
        .unwrap()
        .get(&format!("{fragment}\0{key}"))
        .and_then(Weak::upgrade)
        .is_some_and(|lock| lock.try_lock().is_err())
}

/// The registry is held only to obtain exact-key locks. Filesystem work and
/// waiting happen outside it. Dead entries are removed after the last user.
pub fn with_keys<T>(
    fragment: &str,
    keys: &[&str],
    action: impl FnOnce() -> Result<T, AppError>,
) -> Result<T, AppError> {
    let _snapshot_boundary = crate::core::backup::barrier::mutation();
    let fragment = index_store::stable_fragment_id_for_fragment(fragment);
    let names = keys
        .iter()
        .map(|key| format!("{fragment}\0{key}"))
        .collect::<BTreeSet<_>>();
    let locks = {
        let mut registry = KEY_LOCKS
            .lock()
            .map_err(|_| error("namespace lock registry poisoned"))?;
        names
            .iter()
            .map(|name| {
                let lock = registry
                    .get(name)
                    .and_then(Weak::upgrade)
                    .unwrap_or_else(|| Arc::new(Mutex::new(())));
                registry.insert(name.clone(), Arc::downgrade(&lock));
                lock
            })
            .collect::<Vec<_>>()
    };
    let result = (|| {
        let _guards = locks
            .iter()
            .map(|lock| {
                lock.lock()
                    .map_err(|_| error("namespace key lock poisoned"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        action()
    })();
    if let Ok(mut registry) = KEY_LOCKS.lock() {
        for (name, lock) in names.iter().zip(&locks) {
            if Arc::strong_count(lock) == 1 {
                registry.remove(name);
            }
        }
    }
    result
}

fn error(message: impl Into<String>) -> AppError {
    AppError::InternalServerError(message.into())
}
fn io(error: std::io::Error, context: &str) -> AppError {
    AppError::Io(error, context.into())
}

pub fn namespace_root(root: &Path, fragment: &str) -> PathBuf {
    index_store::fragment_root_on(root, fragment).join(LIVE_DIR)
}

pub fn path(root: &Path, fragment: &str, key: &str) -> PathBuf {
    namespace_root(root, fragment).join(encoded_key(key))
}

// Tag every organizer component, not only the leaf. Thus `a`, `a/b`,
// `a.ncx/b` and `a.ncp/b` never compete for the same physical directory/file.
fn encoded_key(key: &str) -> PathBuf {
    let mut parts = key.split('/').peekable();
    let mut path = PathBuf::new();
    while let Some(part) = parts.next() {
        path.push(format!(
            "{part}{}",
            if parts.peek().is_some() {
                PREFIX_SUFFIX
            } else {
                SUFFIX
            }
        ));
    }
    path
}

pub(crate) fn decoded_key(relative: &Path) -> Option<String> {
    let mut parts = relative.components().peekable();
    let mut key = Vec::new();
    while let Some(part) = parts.next() {
        let name = part.as_os_str().to_str()?;
        key.push(name.strip_suffix(if parts.peek().is_some() {
            PREFIX_SUFFIX
        } else {
            SUFFIX
        })?);
    }
    (!key.is_empty()).then(|| key.join("/"))
}

fn roots() -> Vec<PathBuf> {
    ObjectPathManager::new().get_all_index_roots()
}

pub fn ensure_readable() -> Result<(), AppError> {
    for pool in storage_pool::configured() {
        if pool.available_roots().len() < usize::from(pool.data_shards) {
            return Err(AppError::ServiceUnavailable(format!(
                "pool '{}' exceeds its namespace failure budget; absence and complete listing cannot be established",
                pool.id
            )));
        }
    }
    Ok(())
}

fn encode(pointer: &Pointer) -> Result<Vec<u8>, AppError> {
    let body = rmp_serde::to_vec_named(pointer).map_err(|e| error(e.to_string()))?;
    let mut bytes = Vec::with_capacity(MAGIC.len() + 32 + body.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&Sha256::digest(&body));
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

pub fn read(path: &Path) -> Result<Option<Pointer>, AppError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io(e, "read N1 NCX")),
    };
    decode_pointer(path, &bytes).map(Some)
}

fn decode_pointer(path: &Path, bytes: &[u8]) -> Result<Pointer, AppError> {
    let offset = MAGIC.len() + 32;
    if bytes.len() < offset
        || &bytes[..MAGIC.len()] != MAGIC
        || Sha256::digest(&bytes[offset..])[..] != bytes[MAGIC.len()..offset]
    {
        return Err(error(format!(
            "NCX integrity failure at '{}'",
            path.display()
        )));
    }
    let pointer: Pointer = rmp_serde::from_slice(&bytes[offset..])
        .map_err(|e| error(format!("decode NCX '{}': {e}", path.display())))?;
    if pointer.format != 4 || index_store::normalize_object_id(&pointer.object_id).is_err() {
        return Err(error(format!("invalid NCX at '{}'", path.display())));
    }
    Ok(pointer)
}

fn sync_directory(path: &Path) -> Result<(), AppError> {
    crate::core::linux_io::sync_dir_blocking(path).map_err(|e| io(e, "sync namespace directory"))
}

/// Parent creation is durable too: syncing only the leaf parent would lose a
/// newly created prefix chain after a power failure.
fn durable_parents(parent: &Path, root: &Path) -> Result<(), AppError> {
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| error("namespace path escaped storage root"))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(error("invalid namespace parent"));
        }
        let ancestor = current.clone();
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(meta) if meta.is_dir() => (),
            Ok(_) => {
                return Err(AppError::Conflict(
                    "namespace parent is not a directory".into(),
                ));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        if !fs::symlink_metadata(&current)
                            .map_err(|e| io(e, "inspect NCX parent"))?
                            .is_dir()
                        {
                            return Err(error("unsafe namespace parent"));
                        }
                    }
                    Err(e) => return Err(io(e, "create NCX parent")),
                }
                sync_directory(&ancestor)?;
            }
            Err(e) => return Err(io(e, "inspect NCX parent")),
        }
    }
    Ok(())
}

pub fn write(root: &Path, path: &Path, pointer: &Pointer, exclusive: bool) -> Result<(), AppError> {
    if !storage_pool::root_available(root) {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace holder '{}' is unavailable",
            root.display()
        )));
    }
    let parent = path.parent().ok_or_else(|| error("NCX has no parent"))?;
    durable_parents(parent, root)?;
    let temporary = parent.join(format!(".ncx-{}.tmp", Ulid::new()));
    let result = (|| {
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|e| io(e, "create staged NCX"))?;
        crate::core::linux_io::write_bytes_at_blocking(&file, &encode(pointer)?, 0)
            .map_err(|e| io(e, "write NCX"))?;
        crate::core::linux_io::fsync_blocking(&file).map_err(|e| io(e, "sync NCX"))?;
        if exclusive {
            fs::hard_link(&temporary, path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    AppError::Conflict(format!("namespace destination '{}' exists", path.display()))
                } else {
                    io(e, "publish new NCX")
                }
            })?;
            fs::remove_file(&temporary).map_err(|e| io(e, "remove staged NCX"))?;
        } else {
            fs::rename(&temporary, path).map_err(|e| io(e, "publish NCX reference"))?;
        }
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn holders(pointer: &Pointer, fragment: &str, key: &str) -> Result<Vec<PathBuf>, AppError> {
    if pointer.pool_id.is_empty() && storage_pool::configured().is_empty() {
        return Ok(roots());
    }
    let stable_fragment = index_store::stable_fragment_id_for_fragment(fragment);
    Ok(storage_pool::by_id(&pointer.pool_id)?.namespace_roots(&stable_fragment, key))
}

fn check_holders(holders: &[PathBuf]) -> Result<(), AppError> {
    if holders.is_empty()
        || holders
            .iter()
            .any(|root| !storage_pool::root_available(root))
    {
        return Err(AppError::ServiceUnavailable(
            "all exact namespace holders must be available for mutation".into(),
        ));
    }
    Ok(())
}

/// Independent holders may flush in parallel, but a durability phase does not
/// complete until *every* worker has joined. In particular, a failed destination
/// publication must never let a later source-fencing phase start early. Bound
/// fanout even for large pool geometries; this is execution, not a data cache.
pub(crate) fn on_holders<T: Send>(
    roots: &[PathBuf],
    action: impl Fn(&Path) -> Result<T, AppError> + Sync,
) -> Result<Vec<T>, AppError> {
    if roots.len() <= 1 {
        return roots.iter().map(|root| action(root)).collect();
    }
    std::thread::scope(|scope| {
        let workers = roots
            .chunks(roots.len().div_ceil(4))
            .map(|chunk| {
                let action = &action;
                scope.spawn(move || chunk.iter().map(|root| action(root)).collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let mut outcomes = Vec::with_capacity(roots.len());
        for worker in workers {
            match worker.join() {
                Ok(values) => outcomes.extend(values),
                Err(_) => outcomes.push(Err(error("namespace holder worker panicked"))),
            }
        }
        outcomes.into_iter().collect()
    })
}

pub fn lookup(fragment: &str, key: &str) -> Result<Option<Pointer>, AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    ensure_readable()?;
    let mut selected: Option<Pointer> = None;
    let mut corruption = None;
    for root in roots() {
        match read(&path(&root, fragment, key)) {
            Ok(Some(pointer)) => {
                if let Some(current) = &selected {
                    if current != &pointer {
                        return Err(AppError::Conflict(format!(
                            "namespace key '{key}' has conflicting identities"
                        )));
                    }
                }
                selected = Some(pointer);
            }
            Ok(None) => (),
            Err(e) => corruption = Some(e),
        }
    }
    if selected.is_none() {
        if let Some(error) = corruption {
            return Err(error);
        }
    }
    Ok(selected)
}

pub fn publish(
    fragment: &str,
    key: &str,
    object_id: &str,
    pool: Option<&StoragePool>,
) -> Result<Pointer, AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    publish_with_existing(fragment, key, object_id, pool, lookup(fragment, key)?)
}

/// Publish after the caller's fresh lookup while it still holds this exact
/// key's `with_keys` guard. Do not repeat the same all-replica lookup during
/// upload finalization; holder checks and durable publication remain here.
pub(crate) fn publish_with_existing(
    fragment: &str,
    key: &str,
    object_id: &str,
    pool: Option<&StoragePool>,
    existing: Option<Pointer>,
) -> Result<Pointer, AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    if let Some(existing) = existing {
        if existing.object_id != object_id {
            return Err(AppError::Conflict(format!(
                "key '{key}' has another object identity"
            )));
        }
        let exact = holders(&existing, fragment, key)?;
        check_holders(&exact)?;
        on_holders(&exact, |root| {
            let target = path(root, fragment, key);
            match read(&target) {
                Ok(Some(current)) if current == existing => {
                    // NCX is not a version head: unchanged bytes were synced
                    // before publication. Confirm the directory entry even
                    // after an interrupted earlier publication, without
                    // replacing the inode for every new object version.
                    sync_directory(target.parent().ok_or_else(|| error("NCX has no parent"))?)
                }
                Ok(Some(_)) => Err(AppError::Conflict(format!(
                    "key '{key}' has another object identity"
                ))),
                Ok(None) | Err(_) => write(root, &target, &existing, false),
            }
        })?;
        return Ok(existing);
    }
    let pool_id = pool.map(|pool| pool.id.clone()).unwrap_or_default();
    let pointer = Pointer {
        format: 4,
        pool_id,
        object_id: object_id.into(),
    };
    let exact = holders(&pointer, fragment, key)?;
    check_holders(&exact)?;
    on_holders(&exact, |root| {
        write(root, &path(root, fragment, key), &pointer, true)
    })?;
    Ok(pointer)
}

/// Physical filenames are the listing index. No NCX, NDX or payload is opened.
pub fn page(
    fragment: &str,
    prefix: &str,
    delimiter: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<(Vec<(String, bool)>, Option<String>), AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    ensure_readable()?;
    let limit = limit.max(1);
    let capacity = limit.saturating_add(1);
    let mut names = BTreeMap::<String, bool>::new();
    let base_prefix = prefix
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or("");
    for root in roots() {
        let namespace = namespace_root(&root, fragment);
        let base = base_prefix
            .split('/')
            .filter(|part| !part.is_empty())
            .fold(namespace.clone(), |path, part| {
                path.join(format!("{part}{PREFIX_SUFFIX}"))
            });
        if !base.is_dir() {
            continue;
        }
        let mut walker = WalkDir::new(&base)
            .min_depth(1)
            .follow_links(false)
            .into_iter();
        while let Some(entry) = walker.next() {
            let entry = entry.map_err(|e| error(format!("scan flat namespace: {e}")))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&namespace)
                .map_err(|_| error("namespace scan escaped root"))?;
            let Some(key) = decoded_key(relative) else {
                continue;
            };
            let Some(rest) = key.strip_prefix(prefix) else {
                continue;
            };
            let (name, directory) = if !delimiter.is_empty() {
                match rest.find(delimiter) {
                    Some(position) => (
                        format!("{}{}", prefix, &rest[..position + delimiter.len()]),
                        true,
                    ),
                    None => (key.to_string(), false),
                }
            } else {
                (key.to_string(), false)
            };
            if after.is_some_and(|after| name.as_str() <= after) {
                continue;
            }
            names.insert(name, directory);
            // One object proves a common prefix exists. Do not walk millions
            // of siblings merely to report that same virtual prefix again.
            if directory && delimiter == "/" {
                walker.skip_current_dir();
            }
            if names.len() > capacity {
                names.pop_last();
            }
        }
    }
    let more = names.len() > limit;
    if more {
        names.pop_last();
    }
    let next = more
        .then(|| names.last_key_value().map(|(name, _)| name.clone()))
        .flatten();
    Ok((names.into_iter().collect(), next))
}

pub fn keys(fragment: &str, prefix: &str) -> Result<Vec<String>, AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    Ok(page(fragment, prefix, "", None, usize::MAX - 1)?
        .0
        .into_iter()
        .map(|(key, _)| key)
        .collect())
}

/// Linear, bounded-memory metadata enumeration for exports. The lexical first
/// present replica emits each key once; no whole-namespace set or repeatedly
/// rescanned sorted pages are needed. LIST remains the zero-decode path.
pub fn visit(
    fragment: &str,
    prefix: &str,
    mut emit: impl FnMut(&str) -> Result<bool, AppError>,
) -> Result<(), AppError> {
    let mut scan = Scan::new(fragment, false)?;
    scan.prefix = prefix.to_string();
    while let Some(record) = scan.next_record()? {
        if record.key.starts_with(prefix) && !emit(&record.key)? {
            break;
        }
    }
    Ok(())
}

/// An unsorted maintenance walk retains only its directory stack and one NCX.
/// Unlike LIST continuation pages, advancing a batch never restarts the walk.
/// A cursor performs no work while its consumer is processing the last batch.
pub(crate) struct Scan {
    fragment: String,
    roots: Vec<PathBuf>,
    next_root: usize,
    current_root: PathBuf,
    base: PathBuf,
    walker: Option<walkdir::IntoIter>,
    trash: bool,
    prefix: String,
    scanned_entries: u64,
    progress_at: std::time::Instant,
    completed: bool,
    #[cfg(test)]
    authoritative_lookups: usize,
}

pub(crate) struct ScanRecord {
    pub key: String,
    pub pointer: Pointer,
    pub trash: Option<TrashRecord>,
}

impl Scan {
    pub(crate) fn new(fragment: &str, trash: bool) -> Result<Self, AppError> {
        ensure_readable()?;
        Ok(Self::on_roots(
            index_store::stable_fragment_id_for_fragment(fragment),
            roots(),
            trash,
        ))
    }

    fn on_roots(fragment: String, mut roots: Vec<PathBuf>, trash: bool) -> Self {
        // Replica suppression below emits from the lexical first surviving
        // holder. Visit holders in that same order, otherwise a replicated
        // pool can walk an entire non-emitting member before its first frame.
        // This sorts only this cursor's discovery list, never EC member slots.
        roots.sort();
        Self {
            fragment,
            roots,
            next_root: 0,
            current_root: PathBuf::new(),
            base: PathBuf::new(),
            walker: None,
            trash,
            prefix: String::new(),
            scanned_entries: 0,
            progress_at: std::time::Instant::now(),
            completed: false,
            #[cfg(test)]
            authoritative_lookups: 0,
        }
    }

    pub(crate) fn with_prefix(mut self, prefix: String) -> Self {
        self.prefix = prefix;
        self
    }

    pub(crate) fn completed(&self) -> bool {
        self.completed
    }

    pub(crate) fn next_record(&mut self) -> Result<Option<ScanRecord>, AppError> {
        self.next_record_with_progress(|_| Ok(true))
    }

    /// Report physical progress, including non-emitting replica entries. A
    /// stopped consumer releases this walk within 128 entries; progress is
    /// never a claim that the namespace has been completely enumerated.
    pub(crate) fn next_record_with_progress(
        &mut self,
        mut progress: impl FnMut(u64) -> Result<bool, AppError>,
    ) -> Result<Option<ScanRecord>, AppError> {
        if self.completed {
            return Ok(None);
        }
        loop {
            if self.walker.is_none() {
                let Some(root) = self.roots.get(self.next_root) else {
                    ensure_readable()?;
                    self.completed = true;
                    return Ok(None);
                };
                self.next_root += 1;
                self.current_root = root.clone();
                self.base =
                    index_store::fragment_root_on(root, &self.fragment).join(if self.trash {
                        index_store::TRASH_DIR
                    } else {
                        LIVE_DIR
                    });
                if !self.base.is_dir() {
                    continue;
                }
                self.walker = Some(
                    WalkDir::new(&self.base)
                        .min_depth(1)
                        .follow_links(false)
                        .into_iter(),
                );
            }
            let Some(entry) = self.walker.as_mut().unwrap().next() else {
                self.walker = None;
                continue;
            };
            let entry = entry.map_err(|e| error(format!("enumerate flat namespace: {e}")))?;
            self.scanned_entries = self.scanned_entries.saturating_add(1);
            if self.scanned_entries.is_multiple_of(128)
                || self.progress_at.elapsed() >= std::time::Duration::from_secs(1)
            {
                self.progress_at = std::time::Instant::now();
                if !progress(self.scanned_entries)? {
                    return Ok(None);
                }
            }
            if !entry.file_type().is_file()
                || !entry.file_name().to_string_lossy().ends_with(SUFFIX)
            {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&self.base)
                .map_err(|_| error("namespace scan escaped root"))?;
            let (key, pointer, trash) = if self.trash {
                let id = relative.to_string_lossy();
                let Some(record) = trash_record_by_id(&self.fragment, &id)? else {
                    continue;
                };
                let Some(pointer) = trash_pointer(&self.fragment, &record)? else {
                    continue;
                };
                (record.original_path.clone(), pointer, Some(record))
            } else {
                let Some(key) = decoded_key(relative).filter(|key| key.starts_with(&self.prefix))
                else {
                    continue;
                };
                // A healthy duplicate does not need another all-root decode.
                // The canonical entry still performs the authoritative lookup
                // below, so conflicting identities (even on non-holders) are
                // not silently accepted. Corrupt copies must take that same
                // lookup path: the first filename may own a healthy replica's
                // emission even when its own bytes are damaged.
                if let Ok(Some(local)) = read(entry.path()) {
                    if let Ok(first) = self.first_present_holder(&local, &key, None) {
                        if first.as_ref() != Some(&self.current_root) {
                            continue;
                        }
                    }
                }
                #[cfg(test)]
                {
                    self.authoritative_lookups += 1;
                }
                let Some(pointer) = lookup(&self.fragment, &key)? else {
                    continue;
                };
                (key, pointer, None)
            };
            // Fixed holders and surviving names, not a growing de-duplication
            // set. A missing first holder must not hide its surviving replica.
            let first = self.first_present_holder(&pointer, &key, trash.as_ref())?;
            if first.as_ref() == Some(&self.current_root) {
                return Ok(Some(ScanRecord {
                    key,
                    pointer,
                    trash,
                }));
            }
        }
    }

    fn first_present_holder(
        &self,
        pointer: &Pointer,
        key: &str,
        trash: Option<&TrashRecord>,
    ) -> Result<Option<PathBuf>, AppError> {
        Ok(holders(pointer, &self.fragment, key)?
            .into_iter()
            .filter(|root| {
                storage_pool::root_available(root) && {
                    let candidate = match trash {
                        Some(record) => index_store::fragment_root_on(root, &self.fragment)
                            .join(index_store::TRASH_DIR)
                            .join(&record.source_relative_path),
                        None => path(root, &self.fragment, key),
                    };
                    candidate.is_file()
                }
            })
            .min())
    }

    #[cfg(test)]
    pub(crate) fn entries_visited(&self) -> usize {
        self.scanned_entries.try_into().unwrap_or(usize::MAX)
    }

    #[cfg(test)]
    pub(crate) fn authoritative_lookups(&self) -> usize {
        self.authoritative_lookups
    }
}

/// Copy an already committed NCX without changing its contents. A local hard
/// link is the copy: its inode is already durable, so only the new directory
/// entry needs syncing. A holder on another filesystem receives a byte-for-byte
/// staged copy, whose data is synced before its name is published.
fn copy_ncx(
    root: &Path,
    destination: &Path,
    sources: &[PathBuf],
    expected: &Pointer,
) -> Result<(), AppError> {
    if !storage_pool::root_available(root) {
        return Err(AppError::ServiceUnavailable(
            "NCX destination holder unavailable".into(),
        ));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| error("NCX has no parent"))?;
    durable_parents(parent, root)?;
    if let Some(existing) = read(destination)? {
        if &existing != expected {
            return Err(AppError::Conflict(
                "NCX destination has another identity".into(),
            ));
        }
        return sync_directory(parent);
    }
    let source = sources
        .iter()
        .find(|source| source.starts_with(root))
        .or_else(|| sources.first())
        .ok_or_else(|| error("no valid NCX source replica"))?;
    match fs::hard_link(source, destination) {
        Ok(()) => return sync_directory(parent),
        Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => (),
        Err(e) => return Err(io(e, "copy local NCX reference")),
    }
    let temporary = parent.join(format!(".ncx-{}.tmp", Ulid::new()));
    let result = (|| {
        // Never decode/re-encode the reference or read any object-version data.
        let mut input = File::open(source).map_err(|e| io(e, "open NCX copy source"))?;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|e| io(e, "create NCX copy"))?;
        std::io::copy(&mut input, &mut output).map_err(|e| io(e, "copy NCX bytes"))?;
        crate::core::linux_io::fsync_blocking(&output).map_err(|e| io(e, "sync copied NCX"))?;
        fs::hard_link(&temporary, destination).map_err(|e| io(e, "publish copied NCX"))?;
        fs::remove_file(&temporary).map_err(|e| io(e, "remove staged NCX copy"))?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn copy_sources(paths: Vec<PathBuf>, expected: &Pointer) -> Result<Vec<PathBuf>, AppError> {
    let mut sources = Vec::new();
    let mut corruption = None;
    for path in paths {
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_file() => (),
            Ok(_) => return Err(error("NCX copy source is not a regular file")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io(e, "inspect NCX copy source")),
        }
        match read(&path) {
            Ok(Some(pointer)) if &pointer == expected => sources.push(path),
            Ok(Some(_)) => return Err(AppError::Conflict("NCX source identity changed".into())),
            Ok(None) => (),
            Err(e) => corruption = Some(e),
        }
    }
    if sources.is_empty() {
        return Err(corruption.unwrap_or_else(|| error("no valid NCX source replica")));
    }
    Ok(sources)
}

fn remove_source(fragment: &str, key: &str, expected: &Pointer) -> Result<(), AppError> {
    let exact = holders(expected, fragment, key)?;
    check_holders(&exact)?;
    on_holders(&exact, |root| {
        let source = path(root, fragment, key);
        if let Some(current) = read(&source)? {
            if &current != expected {
                return Err(AppError::Conflict(format!(
                    "source '{key}' changed during mutation"
                )));
            }
            fs::remove_file(&source).map_err(|e| io(e, "remove copied NCX source"))?;
            sync_directory(source.parent().unwrap())?;
        }
        Ok(())
    })?;
    Ok(())
}

pub fn move_key(fragment: &str, source: &str, destination: &str) -> Result<bool, AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    with_keys(fragment, &[source, destination], || {
        let Some(pointer) = lookup(fragment, source)? else {
            return Ok(false);
        };
        let _activity =
            crate::core::upload::UploadActivityGuard::begin(fragment, &pointer.object_id)?;
        if let Some(existing) = lookup(fragment, destination)? {
            if existing.object_id != pointer.object_id || existing.pool_id != pointer.pool_id {
                return Err(AppError::Conflict(format!(
                    "destination '{destination}' already exists"
                )));
            }
        }
        let exact = holders(&pointer, fragment, destination)?;
        check_holders(&exact)?;
        let source_holders = holders(&pointer, fragment, source)?;
        check_holders(&source_holders)?;
        let sources = copy_sources(
            source_holders
                .iter()
                .map(|root| path(root, fragment, source))
                .collect(),
            &pointer,
        )?;
        on_holders(&exact, |root| {
            copy_ncx(root, &path(root, fragment, destination), &sources, &pointer)
        })?;
        remove_source(fragment, source, &pointer)?;
        Ok(true)
    })
}

pub(crate) fn trash_key(fragment: &str, key: &str) -> Result<Option<TrashRecord>, AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    with_keys(fragment, &[key], || {
        let Some(pointer) = lookup(fragment, key)? else {
            return Ok(None);
        };
        let _activity =
            crate::core::upload::UploadActivityGuard::begin(fragment, &pointer.object_id)?;
        let exact = holders(&pointer, fragment, key)?;
        check_holders(&exact)?;
        let sources = copy_sources(
            exact.iter().map(|root| path(root, fragment, key)).collect(),
            &pointer,
        )?;
        let nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let generation = format!("{nanos}.d{}.{}", key.split('/').count(), Ulid::new());
        let relative = format!("{generation}/{}", encoded_key(key).display());
        on_holders(&exact, |root| {
            let destination = index_store::fragment_root_on(root, fragment)
                .join(index_store::TRASH_DIR)
                .join(&relative);
            copy_ncx(root, &destination, &sources, &pointer)
        })?;
        remove_source(fragment, key, &pointer)?;
        Ok(Some(TrashRecord {
            trash_entry_id: relative.clone(),
            source_relative_path: relative,
            original_path: key.into(),
            kind: TrashKind::File,
            object_keys: vec![pointer.object_id],
            version_ids: Vec::new(),
            deleted_at_unix: nanos / 1_000_000_000,
        }))
    })
}

pub(crate) fn trash_records(fragment: &str) -> Result<Vec<TrashRecord>, AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    ensure_readable()?;
    let mut records = BTreeMap::new();
    let mut corrupt = BTreeMap::new();
    for root in roots() {
        let trash = index_store::fragment_root_on(&root, fragment).join(index_store::TRASH_DIR);
        if !trash.is_dir() {
            continue;
        }
        for entry in WalkDir::new(&trash).min_depth(1).follow_links(false) {
            let entry = entry.map_err(|e| error(format!("scan flat trash: {e}")))?;
            if !entry.file_type().is_file()
                || !entry.file_name().to_string_lossy().ends_with(SUFFIX)
            {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&trash)
                .map_err(|_| error("trash scan escaped root"))?
                .to_string_lossy()
                .into_owned();
            let Some((generation, key)) = relative.split_once('/') else {
                return Err(error("invalid trash namespace path"));
            };
            let Some(key) = decoded_key(Path::new(key)) else {
                continue;
            };
            let nanos: i64 = generation
                .split('.')
                .next()
                .unwrap_or("")
                .parse()
                .map_err(|_| error("invalid trash generation timestamp"))?;
            let pointer = match read(entry.path()) {
                Ok(Some(pointer)) => pointer,
                Ok(None) => continue,
                Err(e) => {
                    corrupt.insert(relative.clone(), e);
                    continue;
                }
            };
            if records
                .get(&relative)
                .is_some_and(|r: &TrashRecord| r.object_keys != [pointer.object_id.clone()])
            {
                return Err(AppError::Conflict("conflicting trash identity".into()));
            }
            records.entry(relative.clone()).or_insert(TrashRecord {
                trash_entry_id: relative.clone(),
                source_relative_path: relative.clone(),
                original_path: key.into(),
                kind: TrashKind::File,
                object_keys: vec![pointer.object_id],
                version_ids: Vec::new(),
                deleted_at_unix: nanos / 1_000_000_000,
            });
        }
    }
    for (id, error) in corrupt {
        if !records.contains_key(&id) {
            return Err(error);
        }
    }
    Ok(records.into_values().collect())
}

fn trash_pointer(fragment: &str, record: &TrashRecord) -> Result<Option<Pointer>, AppError> {
    let mut pointer = None;
    let mut corrupt = None;
    for root in roots() {
        let source = index_store::fragment_root_on(&root, fragment)
            .join(index_store::TRASH_DIR)
            .join(&record.source_relative_path);
        match read(&source) {
            Ok(Some(p)) => {
                if pointer.as_ref().is_some_and(|old| old != &p) {
                    return Err(AppError::Conflict("conflicting trash identity".into()));
                }
                pointer = Some(p);
            }
            Ok(None) => (),
            Err(e) => corrupt = Some(e),
        }
    }
    if pointer.is_none() {
        if let Some(e) = corrupt {
            return Err(e);
        }
    }
    Ok(pointer)
}

/// An exact trash identity is a bounded point lookup, not a catalog scan.
/// Validate its complete physical encoding before constructing any path from
/// client input. NCX replicas alone authorize recovery; immutable NDX versions
/// are relevant to maintenance/reclaim, not this namespace operation.
pub(crate) fn trash_record_by_id(
    fragment: &str,
    id: &str,
) -> Result<Option<TrashRecord>, AppError> {
    let invalid = || AppError::BadRequest("invalid trash entry identity".into());
    let (generation, encoded) = id.split_once('/').ok_or_else(invalid)?;
    let parts = generation.split('.').collect::<Vec<_>>();
    if parts.len() != 3 || Ulid::from_string(parts[2]).is_err() {
        return Err(invalid());
    }
    let nanos = parts[0].parse::<i64>().map_err(|_| invalid())?;
    let depth = parts[1]
        .strip_prefix('d')
        .and_then(|depth| depth.parse::<usize>().ok())
        .ok_or_else(invalid)?;
    let encoded = Path::new(encoded);
    if encoded
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(invalid());
    }
    let key = decoded_key(encoded).ok_or_else(invalid)?;
    if key.split('/').count() != depth
        || key
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || format!("{generation}/{}", encoded_key(&key).display()) != id
    {
        return Err(invalid());
    }
    index_store::reject_reserved_user_path(&key)?;
    ensure_readable()?;
    let fragment = index_store::stable_fragment_id_for_fragment(fragment);
    let mut record = TrashRecord {
        trash_entry_id: id.into(),
        source_relative_path: id.into(),
        original_path: key,
        kind: TrashKind::File,
        object_keys: Vec::new(),
        version_ids: Vec::new(),
        deleted_at_unix: nanos / 1_000_000_000,
    };
    let Some(pointer) = trash_pointer(&fragment, &record)? else {
        return Ok(None);
    };
    record.object_keys.push(pointer.object_id);
    Ok(Some(record))
}

pub(crate) fn recover(fragment: &str, record: &TrashRecord) -> Result<bool, AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    with_keys(fragment, &[&record.original_path], || {
        let Some(pointer) = trash_pointer(fragment, record)? else {
            return Ok(false);
        };
        let _activity =
            crate::core::upload::UploadActivityGuard::begin(fragment, &pointer.object_id)?;
        let existing = lookup(fragment, &record.original_path)?;
        if existing.as_ref().is_some_and(|p| p != &pointer) {
            return Err(AppError::Conflict(format!(
                "recover destination '{}' exists",
                record.original_path
            )));
        }
        let exact = holders(&pointer, fragment, &record.original_path)?;
        check_holders(&exact)?;
        let sources = copy_sources(
            exact
                .iter()
                .map(|root| {
                    index_store::fragment_root_on(root, fragment)
                        .join(index_store::TRASH_DIR)
                        .join(&record.source_relative_path)
                })
                .collect(),
            &pointer,
        )?;
        on_holders(&exact, |root| {
            copy_ncx(
                root,
                &path(root, fragment, &record.original_path),
                &sources,
                &pointer,
            )
        })?;
        remove_trash_unlocked(fragment, record, false)?;
        Ok(true)
    })
}

fn remove_trash_unlocked(
    fragment: &str,
    record: &TrashRecord,
    prune_organizers: bool,
) -> Result<bool, AppError> {
    // A lost holder can still contain a recoverable trash record. Never expire
    // it or reclaim its payload until the complete recorded set participates.
    let Some(pointer) = trash_pointer(fragment, record)? else {
        return Ok(false);
    };
    let exact = holders(&pointer, fragment, &record.original_path)?;
    if check_holders(&exact).is_err() {
        return Ok(false);
    }
    let removed = on_holders(&exact, |root| {
        let trash = index_store::fragment_root_on(root, fragment).join(index_store::TRASH_DIR);
        remove_trash_replica(
            root,
            &trash,
            Path::new(&record.source_relative_path),
            prune_organizers,
        )
    })?;
    Ok(removed.into_iter().any(|removed| removed))
}

/// Only the just-removed record's known ancestor chain is eligible. Never walk
/// other trash entries, follow redirected parents, or remove the trash root.
/// `remove_dir` itself establishes emptiness, so concurrent/unknown siblings
/// preserve their organizer rather than granting recursive deletion authority.
fn remove_trash_replica(
    root: &Path,
    trash: &Path,
    relative: &Path,
    prune_organizers: bool,
) -> Result<bool, AppError> {
    if relative
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
        || relative.as_os_str().is_empty()
    {
        return Err(error("unsafe trash namespace path"));
    }
    let source = trash.join(relative);
    let parent = source
        .parent()
        .ok_or_else(|| error("trash NCX has no parent"))?;
    let parents = parent
        .strip_prefix(root)
        .map_err(|_| error("trash namespace escaped storage root"))?;
    let mut current = root.to_path_buf();
    for component in parents.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(error("unsafe trash namespace parent"));
        }
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_dir() => (),
            Ok(_) => return Err(error("unsafe trash namespace parent")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(e) => return Err(io(e, "inspect trash namespace parent")),
        }
    }
    let removed = match fs::remove_file(&source) {
        Ok(()) => {
            sync_directory(parent)?;
            true
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(io(e, "remove trash NCX")),
    };
    if !prune_organizers {
        return Ok(removed);
    }
    let mut empty = parent;
    while empty != trash {
        // Both containment and the stop boundary are checked before rmdir;
        // only kernel-confirmed empty directories can be removed.
        if !empty.starts_with(trash) {
            return Err(error("trash organizer escaped trash root"));
        }
        let ancestor = empty
            .parent()
            .ok_or_else(|| error("trash organizer has no parent"))?;
        match fs::remove_dir(empty) {
            Ok(()) => sync_directory(ancestor)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(e) => return Err(io(e, "remove empty trash organizer")),
        }
        empty = ancestor;
    }
    Ok(removed)
}

pub(crate) fn remove_trash(fragment: &str, record: &TrashRecord) -> Result<bool, AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    with_keys(fragment, &[&record.original_path], || {
        remove_trash_unlocked(fragment, record, true)
    })
}

/// GC needs a conservative union of physical references, not a sorted LIST or
/// one chosen namespace holder. Read every NCX once, including misplaced and
/// conflicting surviving copies. Their object IDs all protect data. A corrupt
/// or unreadable copy makes absence uncertain and defers reclamation instead.
/// Root identity/layout proofs are batch-local, never an object-data cache.
pub(crate) struct GcReferenceScan {
    fragment: String,
    roots: Vec<PathBuf>,
    next_scope: usize,
    walker: Option<walkdir::IntoIter>,
    done: bool,
    #[cfg(test)]
    entries_visited: usize,
    #[cfg(test)]
    pointers_read: usize,
}

impl GcReferenceScan {
    pub(crate) fn new(fragment: &str) -> Result<Self, AppError> {
        // Unlike foreground roots(), absence authority may not filter out an
        // unavailable member that could hold the only surviving reference.
        let roots = crate::core::disk_manager::INDEX_STORE_DISKS
            .get()
            .ok_or_else(|| error("namespace roots are not initialized"))?
            .iter()
            .map(PathBuf::from)
            .collect();
        Self::on_roots(fragment, roots)
    }

    fn on_roots(fragment: &str, roots: Vec<PathBuf>) -> Result<Self, AppError> {
        let scan = Self {
            fragment: index_store::stable_fragment_id_for_fragment(fragment),
            roots,
            next_scope: 0,
            walker: None,
            done: false,
            #[cfg(test)]
            entries_visited: 0,
            #[cfg(test)]
            pointers_read: 0,
        };
        scan.validate_roots()?;
        Ok(scan)
    }

    fn validate_roots(&self) -> Result<(), AppError> {
        if self.roots.is_empty() {
            return Err(AppError::ServiceUnavailable(
                "cannot prove namespace references without enrolled roots".into(),
            ));
        }
        for root in &self.roots {
            if !storage_pool::root_available(root) {
                return Err(AppError::ServiceUnavailable(
                    "an enrolled namespace member is unavailable; preserving objects".into(),
                ));
            }
            let fragment =
                crate::core::fragment_identity::physical_fragment_root_on(root, &self.fragment);
            for directory in [
                fragment.clone(),
                fragment.join(LIVE_DIR),
                fragment.join(index_store::TRASH_DIR),
            ] {
                match fs::symlink_metadata(&directory) {
                    Ok(metadata) if metadata.file_type().is_dir() => (),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Ok(_) => {
                        return Err(AppError::Conflict(
                            "redirected namespace authority; preserving objects".into(),
                        ));
                    }
                    Err(e) => return Err(io(e, "inspect GC namespace authority")),
                }
            }
        }
        Ok(())
    }

    /// Bound filesystem entries, not just emitted pointers: a directory-only
    /// namespace also yields. Some(empty) is a valid unfinished work batch;
    /// None means both live/trash scopes on every enrolled root are exhausted.
    pub(crate) fn next_batch(&mut self, limit: usize) -> Result<Option<Vec<String>>, AppError> {
        self.next_batch_while(limit, || true)
    }

    /// Cooperate between physical entries as well as at the count limit. The
    /// cursor and conservative union are unchanged. An individual filesystem
    /// call is not preempted; even a zero budget advances at least one entry.
    pub(crate) fn next_batch_with_budget(
        &mut self,
        limit: usize,
        budget: std::time::Duration,
    ) -> Result<Option<Vec<String>>, AppError> {
        let started = std::time::Instant::now();
        self.next_batch_while(limit, || started.elapsed() < budget)
    }

    fn next_batch_while(
        &mut self,
        limit: usize,
        mut within_budget: impl FnMut() -> bool,
    ) -> Result<Option<Vec<String>>, AppError> {
        if self.done {
            return Ok(None);
        }
        self.validate_roots()?;
        let mut objects = Vec::new();
        for slot in 0..limit.clamp(1, 4096) {
            if slot > 0 && !within_budget() {
                break;
            }
            if self.walker.is_none() {
                if self.next_scope >= self.roots.len().saturating_mul(2) {
                    self.done = true;
                    break;
                }
                let root = &self.roots[self.next_scope / 2];
                let internal = if self.next_scope % 2 == 0 {
                    LIVE_DIR
                } else {
                    index_store::TRASH_DIR
                };
                self.next_scope += 1;
                let base =
                    crate::core::fragment_identity::physical_fragment_root_on(root, &self.fragment)
                        .join(internal);
                match fs::symlink_metadata(&base) {
                    Ok(metadata) if metadata.file_type().is_dir() => {
                        self.walker = Some(
                            WalkDir::new(base)
                                .min_depth(1)
                                .follow_links(false)
                                .into_iter(),
                        );
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Ok(_) => return Err(AppError::Conflict("unsafe namespace traversal".into())),
                    Err(e) => return Err(io(e, "open GC namespace traversal")),
                }
            }
            let Some(entry) = self.walker.as_mut().unwrap().next() else {
                self.walker = None;
                continue;
            };
            let entry = entry.map_err(|e| error(format!("scan GC namespace references: {e}")))?;
            #[cfg(test)]
            {
                self.entries_visited += 1;
            }
            if entry.file_type().is_symlink() {
                return Err(AppError::Conflict(
                    "redirected namespace reference; preserving objects".into(),
                ));
            }
            if !entry.file_name().to_string_lossy().ends_with(SUFFIX) {
                continue;
            }
            if !entry.file_type().is_file() {
                return Err(AppError::Conflict(
                    "unsafe NCX authority type; preserving objects".into(),
                ));
            }
            // Do not follow a leaf swapped to a symlink after enumeration.
            let fd = rustix::fs::openat(
                rustix::fs::CWD,
                entry.path(),
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|e| io(e.into(), "open GC NCX reference"))?;
            let mut file = File::from(fd);
            if !file
                .metadata()
                .map_err(|e| io(e, "inspect GC NCX reference"))?
                .is_file()
            {
                return Err(AppError::Conflict(
                    "unsafe NCX authority type; preserving objects".into(),
                ));
            }
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)
                .map_err(|e| io(e, "read GC NCX reference"))?;
            let pointer = decode_pointer(entry.path(), &bytes)?;
            #[cfg(test)]
            {
                self.pointers_read += 1;
            }
            objects.push(pointer.object_id);
        }
        self.validate_roots()?;
        if self.done && objects.is_empty() {
            Ok(None)
        } else {
            Ok(Some(objects))
        }
    }
}

pub fn referenced_objects(fragment: &str) -> Result<BTreeSet<String>, AppError> {
    let mut objects = BTreeSet::new();
    let mut scan = GcReferenceScan::new(fragment)?;
    while let Some(batch) = scan.next_batch(128)? {
        objects.extend(batch);
    }
    Ok(objects)
}

/// Repair surviving NCX copies only. An interrupted copy/delete may leave both
/// names; repair preserves them instead of inventing deletion authority. The
/// exact-key gate and a fresh read prevent a pre-enumerated, fully deleted name
/// from being recreated by a concurrent repair.
pub fn repair(fragment: &str, dry_run: bool) -> Result<(u64, u64, u64), AppError> {
    let identity = index_store::stable_fragment_id_for_fragment(fragment);
    let fragment = identity.as_str();
    ensure_readable()?;
    let mut report = (0u64, 0u64, 0u64);
    let mut live_scan = Scan::new(fragment, false)?;
    while let Some(record) = live_scan.next_record()? {
        let key = record.key;
        let counts = with_keys(fragment, &[&key], || {
            let mut live = Vec::new();
            let mut corrupt = 0u64;
            for root in roots() {
                match read(&path(&root, fragment, &key)) {
                    Ok(Some(p)) => live.push(p),
                    Ok(None) => (),
                    Err(_) => corrupt += 1,
                }
            }
            if live
                .iter()
                .any(|p| live.first().is_some_and(|first| p != first))
            {
                return Err(AppError::Conflict(format!(
                    "refusing to repair conflicting namespace identity '{key}'"
                )));
            }
            let Some(canonical) = live.first() else {
                if corrupt > 0 {
                    return Err(error(format!(
                        "no valid namespace record survives for '{key}'"
                    )));
                }
                return Ok((0, 0, 0));
            };
            let mut missing = 0;
            let mut divergent = 0;
            for root in holders(canonical, fragment, &key)? {
                if !storage_pool::root_available(&root) {
                    missing += 1;
                    continue;
                }
                let target = path(&root, fragment, &key);
                match read(&target) {
                    Ok(Some(ref p)) if p == canonical => (),
                    Ok(None) => {
                        missing += 1;
                        if !dry_run {
                            write(&root, &target, canonical, true)?;
                        }
                    }
                    _ => {
                        divergent += 1;
                        if !dry_run {
                            write(&root, &target, canonical, false)?;
                        }
                    }
                }
            }
            Ok((missing, divergent, corrupt))
        })?;
        report.0 += counts.0;
        report.1 += counts.1;
        report.2 += counts.2;
    }
    let mut trash_scan = Scan::new(fragment, true)?;
    while let Some(entry) = trash_scan.next_record()? {
        let record = entry
            .trash
            .ok_or_else(|| error("trash cursor returned a live entry"))?;
        let counts = with_keys(fragment, &[&record.original_path], || {
            let mut canonical = None;
            let mut corrupt = 0u64;
            for root in roots() {
                let source = index_store::fragment_root_on(&root, fragment)
                    .join(index_store::TRASH_DIR)
                    .join(&record.source_relative_path);
                match read(&source) {
                    Ok(Some(p)) => {
                        if canonical.as_ref().is_some_and(|old: &Pointer| old != &p) {
                            return Err(AppError::Conflict("conflicting trash identity".into()));
                        }
                        canonical = Some(p);
                    }
                    Ok(None) => (),
                    Err(_) => corrupt += 1,
                }
            }
            let Some(pointer) = canonical else {
                return Ok((0, 0, corrupt));
            };
            let mut missing = 0;
            let mut divergent = 0;
            for root in holders(&pointer, fragment, &record.original_path)? {
                if !storage_pool::root_available(&root) {
                    missing += 1;
                    continue;
                }
                let target = index_store::fragment_root_on(&root, fragment)
                    .join(index_store::TRASH_DIR)
                    .join(&record.source_relative_path);
                match read(&target) {
                    Ok(Some(ref p)) if p == &pointer => (),
                    Ok(None) => {
                        missing += 1;
                        if !dry_run {
                            write(&root, &target, &pointer, true)?;
                        }
                    }
                    _ => {
                        divergent += 1;
                        if !dry_run {
                            write(&root, &target, &pointer, false)?;
                        }
                    }
                }
            }
            Ok((missing, divergent, corrupt))
        })?;
        report.0 += counts.0;
        report.1 += counts.1;
        report.2 += counts.2;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_scan_visits_canonical_holders_before_duplicate_replicas() {
        let configured = vec![
            PathBuf::from("/mnt/member-0"),
            PathBuf::from("/media/member-1"),
            PathBuf::from("/srv/member-2"),
        ];
        for trash in [false, true] {
            let scan = Scan::on_roots("fragment".into(), configured.clone(), trash);
            assert_eq!(scan.roots.first(), configured.iter().min());
            assert!(scan.roots.windows(2).all(|pair| pair[0] <= pair[1]));
            assert_eq!(scan.next_root, 0);
            assert!(scan.walker.is_none());
            assert_eq!(scan.trash, trash);
        }
        assert_eq!(configured[0], PathBuf::from("/mnt/member-0"));
    }

    #[test]
    fn gc_reference_cursor_unions_all_physical_copies_without_holder_filtering() {
        let base = std::env::temp_dir().join(format!("n1-gc-ref-union-{}", Ulid::new()));
        let roots = vec![base.join("member-a"), base.join("member-b")];
        let fragment = Ulid::new().to_string();
        let first = Ulid::new().to_string();
        let other = Ulid::new().to_string();
        for (root, object) in roots.iter().zip([&first, &other]) {
            let live = root.join(format!("@f_{fragment}/{LIVE_DIR}/organizer"));
            fs::create_dir_all(&live).unwrap();
            fs::write(
                live.join("different-holder.ncx"),
                encode(&Pointer {
                    format: 4,
                    pool_id: "not-current-placement".into(),
                    object_id: object.clone(),
                })
                .unwrap(),
            )
            .unwrap();
        }
        let trash_object = Ulid::new().to_string();
        let trash = roots[0].join(format!("@f_{fragment}/.trash/unknown-organizer"));
        fs::create_dir_all(&trash).unwrap();
        fs::write(
            trash.join("retained.ncx"),
            encode(&Pointer {
                format: 4,
                pool_id: String::new(),
                object_id: trash_object.clone(),
            })
            .unwrap(),
        )
        .unwrap();
        let mut scan = GcReferenceScan::on_roots(&fragment, roots.clone()).unwrap();
        let mut seen = BTreeSet::new();
        let mut batches = 0;
        while let Some(batch) = scan.next_batch(1).unwrap() {
            assert!(batch.len() <= 1);
            batches += 1;
            seen.extend(batch);
        }
        assert!(batches >= 3);
        assert_eq!(scan.pointers_read, 3, "each physical NCX must be read once");
        let expected = BTreeSet::from([first, other, trash_object]);
        assert_eq!(seen, expected);

        // A zero work budget deterministically exhausts the time slice after
        // one physical entry. Cursor state must still cover every live/trash
        // replica, including a conflicting or misplaced surviving pointer.
        let mut budgeted = GcReferenceScan::on_roots(&fragment, roots).unwrap();
        let mut seen = BTreeSet::new();
        let mut prior_entries = 0;
        let mut batches = 0;
        while let Some(batch) = budgeted
            .next_batch_with_budget(128, std::time::Duration::ZERO)
            .unwrap()
        {
            assert!(budgeted.entries_visited.saturating_sub(prior_entries) <= 1);
            prior_entries = budgeted.entries_visited;
            batches += 1;
            assert!(batches <= 32, "time slicing must not starve the cursor");
            seen.extend(batch);
        }
        assert_eq!(budgeted.pointers_read, 3);
        assert_eq!(seen, expected);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn gc_reference_cursor_rejects_corruption_symlinks_and_lost_members() {
        let base = std::env::temp_dir().join(format!("n1-gc-ref-safety-{}", Ulid::new()));
        let root = base.join("member");
        let fragment = Ulid::new().to_string();
        let live = root.join(format!("@f_{fragment}/{LIVE_DIR}"));
        fs::create_dir_all(&live).unwrap();
        let pointer_path = live.join("bad.ncx");
        fs::write(&pointer_path, b"corrupt").unwrap();
        let mut scan = GcReferenceScan::on_roots(&fragment, vec![root.clone()]).unwrap();
        assert!(scan.next_batch(128).is_err());
        fs::remove_file(&pointer_path).unwrap();
        std::os::unix::fs::symlink(base.join("external"), &pointer_path).unwrap();
        let mut scan = GcReferenceScan::on_roots(&fragment, vec![root.clone()]).unwrap();
        assert!(scan.next_batch(128).is_err());
        fs::remove_file(&pointer_path).unwrap();
        let mut scan = GcReferenceScan::on_roots(&fragment, vec![root.clone()]).unwrap();
        fs::rename(&root, base.join("disconnected")).unwrap();
        assert!(scan.next_batch(128).is_err());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn removing_real_trash_prunes_nested_organizers_but_keeps_trash_root() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let configured = crate::core::disk_manager::INDEX_STORE_DISKS.get_or_init(|| {
            let base = std::env::temp_dir().join(format!("n1-index-{}", std::process::id()));
            [base.join("index-0"), base.join("index-1")]
                .into_iter()
                .map(|root| root.to_string_lossy().into_owned())
                .collect()
        });
        for root in configured {
            fs::create_dir_all(root).unwrap();
        }
        let fragment = Ulid::new().to_string();
        let object = Ulid::new().to_string();
        let key = "nested/organizer/document.bin";
        publish(&fragment, key, &object, None).unwrap();
        let record = trash_key(&fragment, key).unwrap().unwrap();
        let generation = Path::new(&record.source_relative_path)
            .components()
            .next()
            .unwrap();
        for root in roots() {
            let trash =
                index_store::fragment_root_on(&root, &fragment).join(index_store::TRASH_DIR);
            assert!(trash.join(&record.source_relative_path).is_file());
            assert!(trash.join(generation).is_dir());
        }
        assert!(remove_trash(&fragment, &record).unwrap());
        // Ordinary recovery retains its existing copy/unlink work: pruning
        // belongs to expiration, not the foreground mutation path.
        publish(&fragment, key, &object, None).unwrap();
        let recovered = trash_key(&fragment, key).unwrap().unwrap();
        let recovered_generation = Path::new(&recovered.source_relative_path)
            .components()
            .next()
            .unwrap();
        assert!(recover(&fragment, &recovered).unwrap());
        for root in roots() {
            let fragment_root = index_store::fragment_root_on(&root, &fragment);
            let trash = fragment_root.join(index_store::TRASH_DIR);
            assert!(
                trash.is_dir(),
                "the trash root is not an eligible organizer"
            );
            assert!(!trash.join(generation).exists());
            assert!(trash.join(recovered_generation).is_dir());
            assert!(!trash.join(&recovered.source_relative_path).exists());
            assert_eq!(
                read(&path(&root, &fragment, key))
                    .unwrap()
                    .unwrap()
                    .object_id,
                object
            );
            assert!(fragment_root.join(LIVE_DIR).is_dir());
            fs::remove_dir_all(fragment_root).unwrap();
        }
    }

    #[test]
    fn trash_parent_pruning_preserves_siblings_and_retries_missing_replica() {
        let root = std::env::temp_dir().join(format!("n1-trash-prune-{}", Ulid::new()));
        let trash = root.join(".trash");
        let relative = Path::new("generation/a.ncp/b.ncp/expired.ncx");
        let parent = trash.join(relative).parent().unwrap().to_path_buf();
        fs::create_dir_all(&parent).unwrap();
        fs::write(trash.join(relative), b"expired pointer").unwrap();
        fs::write(parent.join("retained.ncx"), b"retained pointer").unwrap();
        fs::write(parent.join("unknown"), b"unknown data").unwrap();
        assert!(remove_trash_replica(&root, &trash, relative, true).unwrap());
        assert_eq!(
            fs::read(parent.join("retained.ncx")).unwrap(),
            b"retained pointer"
        );
        assert_eq!(fs::read(parent.join("unknown")).unwrap(), b"unknown data");
        assert!(parent.is_dir());
        fs::remove_file(parent.join("retained.ncx")).unwrap();
        // An unknown sibling still prevents pruning, including on retry when
        // this member's NCX was already removed by an interrupted operation.
        assert!(!remove_trash_replica(&root, &trash, relative, true).unwrap());
        assert!(parent.is_dir());
        fs::remove_file(parent.join("unknown")).unwrap();
        assert!(!remove_trash_replica(&root, &trash, relative, true).unwrap());
        assert!(!trash.join("generation").exists());
        assert!(trash.is_dir());
        assert!(root.is_dir());
        assert!(remove_trash_replica(&root, &trash, Path::new("../outside.ncx"), true).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn trash_parent_pruning_rejects_redirected_generation_and_trash_root() {
        use std::os::unix::fs::symlink;
        let root = std::env::temp_dir().join(format!("n1-trash-link-{}", Ulid::new()));
        let outside = root.join("outside");
        let trash = root.join(".trash");
        fs::create_dir_all(&outside).unwrap();
        fs::create_dir(&trash).unwrap();
        fs::write(outside.join("keep.ncx"), b"outside pointer").unwrap();
        symlink(&outside, trash.join("generation")).unwrap();
        assert!(
            remove_trash_replica(&root, &trash, Path::new("generation/keep.ncx"), true).is_err()
        );
        assert_eq!(
            fs::read(outside.join("keep.ncx")).unwrap(),
            b"outside pointer"
        );
        fs::remove_file(trash.join("generation")).unwrap();
        fs::remove_dir(&trash).unwrap();
        symlink(&outside, &trash).unwrap();
        assert!(remove_trash_replica(&root, &trash, Path::new("keep.ncx"), true).is_err());
        assert_eq!(
            fs::read(outside.join("keep.ncx")).unwrap(),
            b"outside pointer"
        );
        fs::remove_file(trash).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn organizer_encoding_is_injective_for_internal_looking_names() {
        let keys = [
            "a",
            "a/b",
            "a.ncx/b",
            "a.ncp/b",
            "a.ncx",
            "a.ncp",
            ".ncx/.ncp",
        ];
        let paths = keys
            .iter()
            .map(|key| encoded_key(key))
            .collect::<BTreeSet<_>>();
        assert_eq!(paths.len(), keys.len());
        for key in keys {
            assert_eq!(decoded_key(&encoded_key(key)).as_deref(), Some(key));
        }
        assert_ne!(encoded_key("a"), encoded_key("a.ncx/b").parent().unwrap());
        assert!(decoded_key(Path::new(".ncx-staged.tmp")).is_none());
    }

    #[test]
    fn ncx_checksums_reject_corruption_and_atomic_publication_rejects_overwrite() {
        let root = std::env::temp_dir().join(format!("n1-ncx-{}", Ulid::new()));
        fs::create_dir_all(&root).unwrap();
        let pointer = Pointer {
            format: 4,
            pool_id: "A".into(),
            object_id: Ulid::new().to_string(),
        };
        let target = root.join("a/b.ncx");
        write(&root, &target, &pointer, true).unwrap();
        assert_eq!(read(&target).unwrap(), Some(pointer.clone()));
        assert!(matches!(
            write(&root, &target, &pointer, true),
            Err(AppError::Conflict(_))
        ));
        let mut bytes = fs::read(&target).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&target, bytes).unwrap();
        assert!(read(&target).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn local_ncx_copy_preserves_bytes_and_inode_without_a_new_generation() {
        use std::os::unix::fs::MetadataExt;
        let root = std::env::temp_dir().join(format!("n1-ncx-copy-{}", Ulid::new()));
        fs::create_dir_all(&root).unwrap();
        let pointer = Pointer {
            format: 4,
            pool_id: "A".into(),
            object_id: Ulid::new().to_string(),
        };
        let source = root.join("source.ncx");
        let destination = root.join("destination.ncx");
        write(&root, &source, &pointer, true).unwrap();
        let before = fs::read(&source).unwrap();
        let inode = fs::metadata(&source).unwrap().ino();
        copy_ncx(&root, &destination, &[source.clone()], &pointer).unwrap();
        assert_eq!(fs::read(&destination).unwrap(), before);
        assert_eq!(fs::metadata(&destination).unwrap().ino(), inode);
        // A crash before source removal leaves two valid names, not lost data.
        assert_eq!(read(&source).unwrap(), read(&destination).unwrap());
        fs::remove_file(&source).unwrap();
        sync_directory(&root).unwrap();
        assert_eq!(read(&destination).unwrap(), Some(pointer));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cross_filesystem_ncx_copy_is_byte_exact_and_does_not_remove_the_source() {
        use std::os::unix::fs::MetadataExt;
        let disk = std::env::temp_dir().join(format!("n1-ncx-xdev-{}", Ulid::new()));
        let second_filesystem = std::env::var_os("N1_TEST_SECOND_FILESYSTEM")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/dev/shm"));
        let ram = second_filesystem.join(format!("n1-ncx-xdev-{}", Ulid::new()));
        fs::create_dir_all(&disk).unwrap();
        fs::create_dir_all(&ram).unwrap();
        assert_ne!(
            fs::metadata(&disk).unwrap().dev(),
            fs::metadata(&ram).unwrap().dev()
        );
        let pointer = Pointer {
            format: 4,
            pool_id: "A".into(),
            object_id: Ulid::new().to_string(),
        };
        let source = disk.join("source.ncx");
        let destination = ram.join("destination.ncx");
        write(&disk, &source, &pointer, true).unwrap();
        let before = fs::read(&source).unwrap();
        copy_ncx(&ram, &destination, &[source.clone()], &pointer).unwrap();
        assert!(source.exists());
        assert_eq!(fs::read(&destination).unwrap(), before);
        assert_eq!(read(&destination).unwrap(), Some(pointer.clone()));
        assert!(fs::read_dir(&ram).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        // An unrelated destination must not be overwritten or lose its source.
        let other = Pointer {
            object_id: Ulid::new().to_string(),
            ..pointer
        };
        assert!(copy_ncx(&ram, &destination, &[source.clone()], &other).is_err());
        assert_eq!(fs::read(&destination).unwrap(), before);
        assert_eq!(fs::read(&source).unwrap(), before);
        fs::remove_dir_all(disk).unwrap();
        fs::remove_dir_all(ram).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn ncx_copy_sources_reject_symlinks_and_conflicting_object_identity() {
        use std::os::unix::fs::symlink;
        let root = std::env::temp_dir().join(format!("n1-ncx-source-{}", Ulid::new()));
        fs::create_dir_all(&root).unwrap();
        let pointer = Pointer {
            format: 4,
            pool_id: "A".into(),
            object_id: Ulid::new().to_string(),
        };
        let source = root.join("source.ncx");
        write(&root, &source, &pointer, true).unwrap();
        let link = root.join("symlink.ncx");
        symlink(&source, &link).unwrap();
        assert!(copy_sources(vec![link], &pointer).is_err());
        let other = Pointer {
            object_id: Ulid::new().to_string(),
            ..pointer
        };
        assert!(copy_sources(vec![source], &other).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn independent_keys_progress_while_another_key_is_busy() {
        let fragment = Ulid::new().to_string();
        let busy_fragment = fragment.clone();
        let (busy_tx, busy_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            with_keys(&busy_fragment, &["a"], || {
                busy_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
        });
        busy_rx.recv().unwrap();
        with_keys(&fragment, &["b"], || Ok(())).unwrap();
        release_tx.send(()).unwrap();
        worker.join().unwrap().unwrap();
        let stable_fragment = index_store::stable_fragment_id_for_fragment(&fragment);
        let unrelated_fragment = Ulid::new().to_string();
        let unrelated_name = format!(
            "{}\0unrelated",
            index_store::stable_fragment_id_for_fragment(&unrelated_fragment)
        );
        // Other tests legitimately hold unrelated namespace locks in parallel.
        // Keep one live here so this assertion cannot depend on global idleness.
        with_keys(&unrelated_fragment, &["unrelated"], || {
            let registry = KEY_LOCKS.lock().unwrap();
            assert!(registry.contains_key(&unrelated_name));
            for key in ["a", "b"] {
                assert!(
                    !registry.contains_key(&format!("{stable_fragment}\0{key}")),
                    "completed test key {key} remained in the registry"
                );
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn holder_phase_joins_slow_replica_even_after_another_replica_fails() {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let release_rx = Mutex::new(release_rx);
            let outcome = on_holders(&[PathBuf::from("first"), PathBuf::from("second")], |root| {
                if root == Path::new("first") {
                    return Err(error("injected replica failure"));
                }
                started_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                Ok(())
            });
            done_tx.send(outcome.is_err()).unwrap();
        });
        started_rx.recv().unwrap();
        let premature = done_rx.try_recv().is_ok();
        release_tx.send(()).unwrap();
        assert!(done_rx.recv().unwrap());
        worker.join().unwrap();
        assert!(
            !premature,
            "failed phase returned with a replica still writing"
        );
    }

    #[test]
    fn holder_phase_bounds_parallelism_for_large_geometries() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let roots = (0..40)
            .map(|i| PathBuf::from(i.to_string()))
            .collect::<Vec<_>>();
        let values = on_holders(&roots, |root| {
            let count = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(count, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(2));
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(root.to_path_buf())
        })
        .unwrap();
        assert_eq!(values, roots);
        assert!(peak.load(Ordering::SeqCst) <= 4);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }
}
