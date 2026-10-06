use super::disk_manager::{
    IndexListEntry, ListObjectsResult, ObjectMetadata, ObjectPathManager, PartMetadata,
    ReservationReceipt,
};
use super::index::{
    IndexSessionState, IndexWriteDurability, SessionPartReceipt, TrashCatalogEntry,
};
use super::upload::UploadSessionManifest;
use crate::core::constants::N1_OBJECTS_DIR;
#[cfg(test)]
use crate::core::disk_manager::BLOB_STORE_DISKS;
use crate::core::fragment_identity::{
    FRAGMENT_ALIAS_DIR, PHYSICAL_FRAGMENT_PREFIX, fragment_alias_dir, fragment_alias_path,
    fragment_alias_target_stable_id, list_fragment_aliases, physical_fragment_dir_name,
    physical_fragment_root_on,
};
use crate::core::linux_io;
use crate::enums::app_error_enum::AppError;
use crate::namespace::paths::{base_name, normalize_path};
use chrono::Utc;
use rustix::fs::{CWD, Dir, Mode, OFlags};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
#[cfg(test)]
use std::io::Read;
use std::os::unix::fs as unix_fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use ulid::Ulid;

pub(crate) const OBJECTS_DIR: &str = N1_OBJECTS_DIR;
pub(crate) const UPLOADS_DIR: &str = ".uploads";
pub(crate) const TRASH_DIR: &str = ".trash";
pub(crate) const TMP_DIR: &str = ".tmp";
pub(crate) const LOCKS_DIR: &str = ".locks";
pub(crate) const GC_DIR: &str = ".gc";
pub(crate) const REPAIR_DIR: &str = ".repair";
const FRAGMENT_META_FILE: &str = ".fragment.ndx";
pub(crate) const N1_ENGINE_FORMAT_VERSION: u16 = 4;
const MANIFEST_FILE: &str = "manifest.ndx";
const BINDING_FILE: &str = "binding.ndx";
const SHARD_RECEIPT_PREFIX: &str = "shard_p";
const PART_RECEIPT_PREFIX: &str = "part_p";
const RECEIPT_SUFFIX: &str = ".receipt";
const PART_META_PREFIX: &str = "part_";
pub(crate) const OBJECT_DIR_PREFIX: &str = "@o_";
pub(crate) const FRAGMENT_DIR_PREFIX: &str = PHYSICAL_FRAGMENT_PREFIX;
const INTENTS_DIR: &str = ".intents";
const TRASH_JOBS_DIR: &str = ".trash-jobs";
#[cfg(test)]
const REPLICA_TMP_PREFIX: &str = ".tmp-replica-";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMeta {
    pub engine_format_version: u16,
    pub stable_fragment_id: String,
    pub state: String,
    pub committed_bytes: u64,
    pub reserved_bytes: u64,
    pub trash_bytes: u64,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
    #[serde(default)]
    pub key_phc: Option<String>,
    #[serde(default)]
    pub nt_hash_hex: Option<String>,
    pub index_partition_count: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FragmentAliasReconciliation {
    pub discovered_fragments: u64,
    pub missing_aliases: u64,
    pub repaired_aliases: u64,
    pub ambiguous_fragments: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingUploadBinding {
    pub fragment: String,
    pub logical_path: String,
    pub physical_object_id: String,
    pub version_id: String,
    pub session_id: String,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TrashKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TrashRecord {
    pub trash_entry_id: String,
    pub original_path: String,
    pub source_relative_path: String,
    pub kind: TrashKind,
    pub object_keys: Vec<String>,
    pub version_ids: Vec<String>,
    pub deleted_at_unix: i64,
}

#[derive(Debug, Clone)]
struct NdxSummary {
    version_id: String,
    size_bytes: u64,
    unix_nanos: i128,
    packed_ext: u64,
}

#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct ReplicaScore {
    meta_present: u64,
    object_records: u64,
    namespace_links: u64,
    upload_records: u64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicaState {
    score: ReplicaScore,
    content_digest: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct ReplicaInspection {
    pub expected_replicas: u64,
    pub present_replicas: u64,
    pub valid_replicas: u64,
    pub missing_replicas: u64,
    pub divergent_replicas: u64,
    pub corrupt_replicas: u64,
    pub healthy: bool,
    pub canonical_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionRow {
    manifest: UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
    last_activity_at: i64,
    finalized_at: Option<i64>,
    aborted_at: Option<i64>,
}

/// Restore rewrites only physical placement; session/object identity is retained.
pub(crate) fn remap_backup_session_manifest(
    bytes: &[u8],
    mut map: impl FnMut(&str) -> Result<String, AppError>,
) -> Result<Vec<u8>, AppError> {
    let mut row: SessionRow = decode(bytes)?;
    for root in &mut row.manifest.placement_roots {
        *root = map(root)?;
    }
    encode(&row)
}

impl SessionRow {
    fn into_state(self) -> IndexSessionState {
        IndexSessionState {
            manifest: self.manifest,
            reservation: self.reservation,
            parts: BTreeMap::new(),
            part_receipts: BTreeMap::new(),
            last_activity_at: self.last_activity_at,
            finalized_at: self.finalized_at,
            aborted_at: self.aborted_at,
        }
    }
}

fn io_err(err: std::io::Error, context: &str) -> AppError {
    AppError::Io(err, context.to_string())
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, AppError> {
    Ok(rmp_serde::to_vec(value)?)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, AppError> {
    Ok(rmp_serde::from_slice(bytes)?)
}

#[cfg(test)]
fn encode_fragment_meta(meta: &FragmentMeta) -> Result<Vec<u8>, AppError> {
    Ok(rmp_serde::to_vec_named(meta)?)
}

fn decode_fragment_meta(bytes: &[u8]) -> Result<FragmentMeta, AppError> {
    let meta = rmp_serde::from_slice::<FragmentMeta>(bytes)?;
    if meta.engine_format_version != N1_ENGINE_FORMAT_VERSION {
        return Err(AppError::Conflict(format!(
            "fragment uses N1 engine format {}, but this binary requires format {}; N1 3.0 does not open or migrate earlier engine formats",
            meta.engine_format_version, N1_ENGINE_FORMAT_VERSION
        )));
    }
    Ok(meta)
}

fn read_physical_fragment_meta(
    root: &Path,
    stable_fragment_id: &str,
) -> Result<Option<FragmentMeta>, AppError> {
    let path = physical_fragment_root_on(root, stable_fragment_id).join(FRAGMENT_META_FILE);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(AppError::Io(
                error,
                format!("read fragment identity metadata {}", path.display()),
            ));
        }
    };
    decode_fragment_meta(&bytes).map(Some)
}

fn validate_physical_fragment_identity(
    root: &Path,
    stable_fragment_id: &str,
) -> Result<bool, AppError> {
    let Some(meta) = read_physical_fragment_meta(root, stable_fragment_id)? else {
        return Ok(false);
    };
    if meta.stable_fragment_id != stable_fragment_id {
        return Err(AppError::Conflict(format!(
            "fragment identity mismatch on {}: directory={}, metadata={}",
            root.display(),
            stable_fragment_id,
            meta.stable_fragment_id
        )));
    }
    Ok(meta.state == "active")
}

/// Enroll global fragment credentials/identity on newly added pool members.
/// This small control catalog is not the per-object namespace. No NCX, NDX
/// object version or payload is moved or copied during expansion.
pub fn enroll_fragment_control_roots() -> Result<(), AppError> {
    let roots = index_roots();
    let aliases = roots
        .iter()
        .flat_map(|root| list_fragment_aliases(root))
        .collect::<BTreeSet<_>>();
    for alias in aliases {
        let mut selected: Option<(FragmentMeta, Vec<u8>)> = None;
        for root in &roots {
            let path = fragment_root_on(root, &alias).join(FRAGMENT_META_FILE);
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io_err(e, "read fragment control catalog")),
            };
            let meta = decode_fragment_meta(&bytes)?;
            if let Some((old, old_bytes)) = &selected {
                if old.stable_fragment_id != meta.stable_fragment_id
                    || (old.updated_at_unix == meta.updated_at_unix && old_bytes != &bytes)
                {
                    return Err(AppError::Conflict(
                        "ambiguous fragment control identity".into(),
                    ));
                }
                if old.updated_at_unix > meta.updated_at_unix {
                    continue;
                }
            }
            selected = Some((meta, bytes));
        }
        let Some((meta, bytes)) = selected else {
            continue;
        };
        if meta.state != "active" {
            continue;
        }
        for root in &roots {
            let physical = physical_fragment_root_on(root, &meta.stable_fragment_id);
            let path = physical.join(FRAGMENT_META_FILE);
            if !path.exists() {
                write_atomic(&path, &bytes)?;
            }
        }
    }
    reconcile_fragment_alias_replicas(&roots, false)?;
    Ok(())
}

/// Restores a missing alias only when surviving replicas unanimously map one
/// alias to one stable physical fragment and the destination already contains
/// matching active fragment metadata. It never creates, copies, or rewrites a
/// physical fragment directory.
pub(crate) fn reconcile_fragment_alias_replicas(
    roots: &[PathBuf],
    dry_run: bool,
) -> Result<FragmentAliasReconciliation, AppError> {
    let mut aliases_by_stable = BTreeMap::<String, BTreeSet<String>>::new();
    let mut stable_ids_by_alias = BTreeMap::<String, BTreeSet<String>>::new();

    for root in roots {
        for alias in list_fragment_aliases(root) {
            let Some(stable_fragment_id) = fragment_alias_target_stable_id(root, &alias) else {
                continue;
            };
            if !validate_physical_fragment_identity(root, &stable_fragment_id)? {
                continue;
            }
            aliases_by_stable
                .entry(stable_fragment_id.clone())
                .or_default()
                .insert(alias.clone());
            stable_ids_by_alias
                .entry(alias)
                .or_default()
                .insert(stable_fragment_id);
        }
    }

    if let Some((alias, stable_ids)) = stable_ids_by_alias
        .iter()
        .find(|(_, stable_ids)| stable_ids.len() > 1)
    {
        return Err(AppError::Conflict(format!(
            "fragment alias '{}' points to multiple stable fragment identities: {}",
            alias,
            stable_ids.iter().cloned().collect::<Vec<_>>().join(", ")
        )));
    }

    let mut report = FragmentAliasReconciliation {
        discovered_fragments: aliases_by_stable.len() as u64,
        ..FragmentAliasReconciliation::default()
    };

    for (stable_fragment_id, aliases) in aliases_by_stable {
        if aliases.len() != 1 {
            report.ambiguous_fragments = report.ambiguous_fragments.saturating_add(1);
            continue;
        }
        let alias = aliases.into_iter().next().expect("one alias was checked");
        let expected_target =
            PathBuf::from("..").join(physical_fragment_dir_name(&stable_fragment_id));

        for root in roots {
            if !validate_physical_fragment_identity(root, &stable_fragment_id)? {
                continue;
            }
            let alias_path = fragment_alias_path(root, &alias);
            match fs::symlink_metadata(&alias_path) {
                Ok(_) => {
                    if fs::read_link(&alias_path).ok().as_deref() != Some(expected_target.as_path())
                    {
                        return Err(AppError::Conflict(format!(
                            "fragment alias {} does not point to {}",
                            alias_path.display(),
                            expected_target.display()
                        )));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    report.missing_aliases = report.missing_aliases.saturating_add(1);
                    if dry_run {
                        continue;
                    }
                    let alias_dir = fragment_alias_dir(root);
                    fs::create_dir_all(&alias_dir).map_err(|error| {
                        AppError::Io(
                            error,
                            format!("create fragment alias directory {}", alias_dir.display()),
                        )
                    })?;
                    match unix_fs::symlink(&expected_target, &alias_path) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                            if fs::read_link(&alias_path).ok().as_deref()
                                != Some(expected_target.as_path())
                            {
                                return Err(AppError::Conflict(format!(
                                    "fragment alias {} changed during automatic reconciliation",
                                    alias_path.display()
                                )));
                            }
                        }
                        Err(error) => {
                            return Err(AppError::Io(
                                error,
                                format!(
                                    "restore fragment alias {} -> {}",
                                    alias_path.display(),
                                    expected_target.display()
                                ),
                            ));
                        }
                    }
                    File::open(&alias_dir)
                        .and_then(|directory| directory.sync_all())
                        .map_err(|error| {
                            AppError::Io(
                                error,
                                format!("sync fragment alias directory {}", alias_dir.display()),
                            )
                        })?;
                    report.repaired_aliases = report.repaired_aliases.saturating_add(1);
                }
                Err(error) => {
                    return Err(AppError::Io(
                        error,
                        format!("inspect fragment alias {}", alias_path.display()),
                    ));
                }
            }
        }
    }

    Ok(report)
}

fn index_roots() -> Vec<PathBuf> {
    ObjectPathManager::new().get_all_index_roots()
}

fn blob_roots() -> Vec<PathBuf> {
    ObjectPathManager::new().get_blob_roots()
}

pub(crate) fn fragment_root_on(root: &Path, fragment: &str) -> PathBuf {
    // All members resolve through the surviving stable identity. Missing one
    // local alias must not redirect namespace repair into an @f_<public-hash>
    // tree or hide a healthy pointer on that member.
    physical_fragment_root_on(root, &stable_fragment_id_for_fragment(fragment))
}

pub(crate) fn stable_fragment_id_for_fragment(fragment: &str) -> String {
    // Physical identity, not a mutable public alias, owns placement and
    // coordination. A missing first member must not hide a surviving alias.
    if let Some(stable) = fragment.strip_prefix(PHYSICAL_FRAGMENT_PREFIX) {
        return stable.to_string();
    }
    if Ulid::from_string(fragment).is_ok() {
        return fragment.to_string();
    }
    for root in index_roots() {
        if let Some(stable) = fragment_alias_target_stable_id(&root, fragment) {
            return stable;
        }
    }
    fragment.to_string()
}

fn blob_fragment_root_on(root: &Path, fragment: &str) -> PathBuf {
    physical_fragment_root_on(root, &stable_fragment_id_for_fragment(fragment))
}

fn fragment_roots(fragment: &str) -> Vec<PathBuf> {
    index_roots()
        .into_iter()
        .map(|root| fragment_root_on(&root, fragment))
        .collect()
}

fn existing_fragment_roots(fragment: &str) -> Vec<PathBuf> {
    fragment_roots(fragment)
        .into_iter()
        .filter(|root| root.exists())
        .collect()
}

#[inline]
fn reserved_user_segment(segment: &str) -> bool {
    segment.starts_with(OBJECT_DIR_PREFIX)
        || segment.starts_with(FRAGMENT_DIR_PREFIX)
        || segment.starts_with(FRAGMENT_ALIAS_DIR)
        || segment.starts_with(OBJECTS_DIR)
        || segment.starts_with(TRASH_DIR)
        || segment.starts_with(UPLOADS_DIR)
        || segment.starts_with(TMP_DIR)
        || segment.starts_with(LOCKS_DIR)
        || segment.starts_with(GC_DIR)
        || segment.starts_with(REPAIR_DIR)
        || segment.starts_with(INTENTS_DIR)
        || segment.starts_with(TRASH_JOBS_DIR)
}

#[cfg(test)]
pub(crate) fn internal_namespace_entry_name(name: &str) -> bool {
    name.starts_with(OBJECT_DIR_PREFIX)
        || name.starts_with(FRAGMENT_DIR_PREFIX)
        || name.starts_with(REPLICA_TMP_PREFIX)
        || matches!(
            name,
            FRAGMENT_ALIAS_DIR
                | OBJECTS_DIR
                | UPLOADS_DIR
                | TRASH_DIR
                | TMP_DIR
                | LOCKS_DIR
                | GC_DIR
                | REPAIR_DIR
                | INTENTS_DIR
                | TRASH_JOBS_DIR
                | FRAGMENT_META_FILE
        )
}

pub(crate) fn reject_reserved_user_path(path: &str) -> Result<(), AppError> {
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        if reserved_user_segment(segment) {
            return Err(AppError::BadRequest(format!(
                "path segment '{}' is reserved by N1",
                segment
            )));
        }
    }
    Ok(())
}

pub(crate) fn normalize_object_key(object_key: &str) -> Result<String, AppError> {
    let key = normalize_path(object_key).map_err(|err| AppError::BadRequest(err.to_string()))?;
    reject_reserved_user_path(&key)?;
    Ok(key)
}

pub(crate) fn object_key_hash(object_key: &str) -> Result<String, AppError> {
    let key = normalize_object_key(object_key)?;
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) fn normalize_object_id(object_id: &str) -> Result<String, AppError> {
    let value = object_id
        .trim()
        .trim_start_matches('/')
        .strip_prefix(OBJECT_DIR_PREFIX)
        .unwrap_or_else(|| object_id.trim().trim_start_matches('/'))
        .to_string();
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(AppError::BadRequest("invalid N1 object id".to_string()));
    }
    Ok(value)
}

pub(crate) fn new_object_id() -> String {
    Ulid::new().to_string()
}

fn object_dir_name_for_id(object_id: &str) -> Result<String, AppError> {
    Ok(format!(
        "{}{}",
        OBJECT_DIR_PREFIX,
        normalize_object_id(object_id)?
    ))
}

fn object_fanout_segments_for_id(object_id: &str) -> Result<(String, String), AppError> {
    let object_id = normalize_object_id(object_id)?;
    let mut hasher = Sha256::new();
    hasher.update(object_id.as_bytes());
    let hash = hex::encode(hasher.finalize());
    Ok((hash[0..2].to_string(), hash[2..4].to_string()))
}

pub(crate) fn object_id_from_dir_name(name: &str) -> Option<String> {
    normalize_object_id(name.strip_prefix(OBJECT_DIR_PREFIX)?).ok()
}

pub(crate) fn object_index_dir(
    root: &Path,
    fragment: &str,
    object_id: &str,
) -> Result<PathBuf, AppError> {
    let (fanout_a, fanout_b) = object_fanout_segments_for_id(object_id)?;
    Ok(blob_fragment_root_on(root, fragment)
        .join(OBJECTS_DIR)
        .join(fanout_a)
        .join(fanout_b)
        .join(object_dir_name_for_id(object_id)?))
}

#[cfg(test)]
pub(crate) fn live_object_dir(
    root: &Path,
    fragment: &str,
    object_id: &str,
) -> Result<PathBuf, AppError> {
    object_index_dir(root, fragment, object_id)
}

#[cfg(test)]
pub(crate) fn read_namespace_symlink_object_id(
    _root: &Path,
    _fragment: &str,
    link_path: &Path,
) -> Result<Option<String>, AppError> {
    if !link_path.file_name().is_some_and(|name| {
        name.to_string_lossy()
            .ends_with(crate::namespace::flat::SUFFIX)
    }) {
        return Ok(None);
    }
    Ok(crate::namespace::flat::read(link_path)?.map(|pointer| pointer.object_id))
}

#[cfg(test)]
pub(crate) fn publish_namespace_link(
    root: &Path,
    fragment: &str,
    logical_path: &str,
    object_id: &str,
) -> Result<(), AppError> {
    publish_namespace_link_with_durability(root, fragment, logical_path, object_id, true)
}

#[cfg(test)]
pub(crate) fn publish_namespace_link_with_durability(
    root: &Path,
    fragment: &str,
    logical_path: &str,
    object_id: &str,
    _strict: bool,
) -> Result<(), AppError> {
    let key = normalize_object_key(logical_path)?;
    let id = normalize_object_id(object_id)?;
    crate::namespace::flat::publish(
        fragment,
        &key,
        &id,
        crate::core::storage_pool::pool_for_root(root),
    )?;
    Ok(())
}

pub(crate) fn object_dir_candidates(
    fragment: &str,
    object_id: &str,
) -> Result<Vec<PathBuf>, AppError> {
    let object_id = normalize_object_id(object_id)?;
    let mut out = Vec::new();
    for root in blob_roots() {
        out.push(object_index_dir(&root, fragment, &object_id)?);
    }
    out.sort();
    out.dedup();
    Ok(out)
}

pub(crate) fn local_object_dirs_under_root(
    root: &Path,
    fragment: &str,
) -> Result<Vec<(PathBuf, String)>, AppError> {
    let objects_root = blob_fragment_root_on(root, fragment).join(OBJECTS_DIR);
    if !objects_root.is_dir() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for fanout_a in read_dir_names(&objects_root)? {
        let fanout_a_dir = objects_root.join(&fanout_a);
        if !fanout_a_dir.is_dir() {
            continue;
        }
        for fanout_b in read_dir_names(&fanout_a_dir)? {
            let fanout_b_dir = fanout_a_dir.join(&fanout_b);
            if !fanout_b_dir.is_dir() {
                continue;
            }
            for name in read_dir_names(&fanout_b_dir)? {
                let Some(object_id) = object_id_from_dir_name(&name) else {
                    continue;
                };
                let object_dir = fanout_b_dir.join(&name);
                if object_dir.is_dir() {
                    out.push((object_dir, object_id));
                }
            }
        }
    }
    Ok(out)
}

fn ensure_fragment_dirs(fragment: &str) -> Result<(), AppError> {
    for root in fragment_roots(fragment) {
        fs::create_dir_all(root.join(UPLOADS_DIR)).map_err(|e| io_err(e, "create uploads dir"))?;
        fs::create_dir_all(root.join(TRASH_DIR)).map_err(|e| io_err(e, "create trash dir"))?;
    }
    Ok(())
}

#[inline(always)]
fn ascii_lower(byte: u8) -> u8 {
    if byte.is_ascii_uppercase() {
        byte + 32
    } else {
        byte
    }
}

#[inline(always)]
fn pack_ext(ext: &[u8]) -> u64 {
    let len = ext.len();
    if len > 7 {
        return 0;
    }

    let mut key = len as u64;
    for (idx, byte) in ext.iter().copied().enumerate() {
        key |= (ascii_lower(byte) as u64) << (8 * (idx + 1));
    }
    key
}

pub(crate) fn packed_extension_key(object_key: &str) -> u64 {
    let name = base_name(object_key);
    let ext = name.rsplit_once('.').map(|(_, ext)| ext).unwrap_or("");
    pack_ext(ext.as_bytes())
}

fn unpack_extension_key(mut packed: u64) -> String {
    let len = (packed & 0xff) as usize;
    packed >>= 8;
    let mut bytes = Vec::new();
    for _ in 0..len.min(7) {
        let byte = (packed & 0xff) as u8;
        bytes.push(byte);
        packed >>= 8;
    }
    String::from_utf8(bytes).unwrap_or_default()
}

fn ndx_file_name(metadata: &ObjectMetadata) -> String {
    let unix_nanos = i128::from(metadata.created_at.max(0)) * 1_000_000_000i128;
    format!(
        "{}.{}.{}.{}.ndx",
        metadata.version_id,
        metadata.total_size_bytes,
        unix_nanos,
        packed_extension_key(&metadata.object_key)
    )
}

fn parse_ndx_file_name(name: &str) -> Option<NdxSummary> {
    let stem = name.strip_suffix(".ndx")?;
    let mut parts = stem.split('.');
    let version_id = parts.next()?.to_string();
    let size_bytes = parts.next()?.parse::<u64>().ok()?;
    let unix_nanos = parts.next()?.parse::<i128>().ok()?;
    let packed_ext = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(NdxSummary {
        version_id,
        size_bytes,
        unix_nanos,
        packed_ext,
    })
}

pub(crate) fn visit_dir_names(
    path: &Path,
    mut visit: impl FnMut(String) -> Result<bool, AppError>,
) -> Result<(), AppError> {
    let fd = rustix::fs::openat(
        CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| {
        if e == rustix::io::Errno::NOENT {
            AppError::NotFound
        } else {
            AppError::InternalServerError(format!("open N1 directory {}: {e}", path.display()))
        }
    })?;
    let mut dir = Dir::new(fd).map_err(|e| {
        AppError::InternalServerError(format!("scan N1 directory {}: {e}", path.display()))
    })?;
    for entry in &mut dir {
        let entry = entry.map_err(|e| {
            AppError::InternalServerError(format!("read N1 directory {}: {e}", path.display()))
        })?;
        let name = entry.file_name().to_string_lossy();
        if name != "." && name != ".." {
            if !visit(name.into_owned())? {
                break;
            }
        }
    }
    Ok(())
}

pub(crate) fn read_dir_names(path: &Path) -> Result<Vec<String>, AppError> {
    let mut names = Vec::new();
    visit_dir_names(path, |name| {
        names.push(name);
        Ok(true)
    })?;
    Ok(names)
}

fn latest_summary_in_object_dir(dir: &Path) -> Result<Option<NdxSummary>, AppError> {
    if !dir.is_dir() {
        return Ok(None);
    }
    let mut latest = None::<NdxSummary>;
    for name in read_dir_names(dir)? {
        let Some(summary) = parse_ndx_file_name(&name) else {
            continue;
        };
        if latest
            .as_ref()
            .is_none_or(|current| summary.version_id > current.version_id)
        {
            latest = Some(summary);
        }
    }
    Ok(latest)
}

fn ndx_path_for_version(dir: &Path, version_id: &str) -> Result<Option<PathBuf>, AppError> {
    if !dir.is_dir() {
        return Ok(None);
    }
    for name in read_dir_names(dir)? {
        if !name.starts_with(version_id) {
            continue;
        }
        if parse_ndx_file_name(&name).is_some() {
            return Ok(Some(dir.join(name)));
        }
    }
    Ok(None)
}

fn session_data_shards(manifest: &UploadSessionManifest) -> usize {
    usize::from(manifest.data_shards)
}

fn session_parity_shards(manifest: &UploadSessionManifest) -> usize {
    usize::from(manifest.parity_shards)
}

fn session_total_shards(manifest: &UploadSessionManifest) -> usize {
    session_data_shards(manifest) + session_parity_shards(manifest)
}

fn session_part_size(manifest: &UploadSessionManifest, part_index: u64) -> Option<u64> {
    if manifest.expected_parts == 0 || part_index >= manifest.expected_parts {
        return None;
    }
    if manifest.expected_parts == 1 {
        return Some(manifest.total_size_bytes);
    }
    let part_size = manifest.part_size_bytes;
    if part_size == 0 {
        return None;
    }
    if part_index + 1 < manifest.expected_parts {
        return Some(part_size);
    }
    let prior_bytes = part_size.saturating_mul(manifest.expected_parts.saturating_sub(1));
    if prior_bytes > manifest.total_size_bytes {
        return None;
    }
    Some(manifest.total_size_bytes - prior_bytes)
}

fn session_shard_size(manifest: &UploadSessionManifest, part_size: u64) -> u64 {
    let data_shards = session_data_shards(manifest);
    let block_size = manifest.stripe_size_bytes;
    if data_shards == 0 || block_size == 0 {
        return 0;
    }
    let stripe_data_width = block_size as u64 * data_shards as u64;
    part_size.div_ceil(stripe_data_width) * block_size as u64
}

fn shard_receipt_name(part_index: u64, shard_index: usize, checksum: u32) -> String {
    format!("shard_p{part_index:020}_s{shard_index:04}.{checksum:08x}{RECEIPT_SUFFIX}")
}

fn part_receipt_name(part_index: u64, checksum: u32) -> String {
    format!("part_p{part_index:020}.{checksum:08x}{RECEIPT_SUFFIX}")
}

fn parse_crc32_field(value: &str) -> Option<u32> {
    if value.len() != 8 {
        return None;
    }
    u32::from_str_radix(value, 16).ok()
}

fn parse_shard_receipt_name(name: &str) -> Option<(u64, usize, u32)> {
    let stem = name.strip_suffix(RECEIPT_SUFFIX)?;
    let (identity, checksum) = stem.rsplit_once('.')?;
    let checksum = parse_crc32_field(checksum)?;
    let rest = identity.strip_prefix(SHARD_RECEIPT_PREFIX)?;
    let (part, shard) = rest.split_once("_s")?;
    Some((part.parse().ok()?, shard.parse().ok()?, checksum))
}

fn parse_part_receipt_name(name: &str) -> Option<(u64, u32)> {
    let stem = name.strip_suffix(RECEIPT_SUFFIX)?;
    let (part, checksum) = stem.rsplit_once('.')?;
    let part = part.strip_prefix(PART_RECEIPT_PREFIX)?;
    Some((part.parse().ok()?, parse_crc32_field(checksum)?))
}

fn remove_receipts_for_part(dir: &Path, part_index: u64) -> Result<(), AppError> {
    if !dir.is_dir() {
        return Ok(());
    }
    for name in read_dir_names(dir)? {
        let is_target = parse_shard_receipt_name(&name)
            .map(|(idx, _, _)| idx == part_index)
            .unwrap_or(false)
            || parse_part_receipt_name(&name)
                .map(|(idx, _)| idx == part_index)
                .unwrap_or(false);
        if is_target {
            let _ = remove_file_if_exists(&dir.join(name))?;
        }
    }
    Ok(())
}

fn push_receipts_from_filename(
    state: &mut IndexSessionState,
    shard_receipts: BTreeMap<u64, BTreeMap<usize, u32>>,
    part_checksums: BTreeMap<u64, u32>,
) {
    let total_shards = session_total_shards(&state.manifest);
    if total_shards == 0 {
        return;
    }
    for (part_index, shard_checksums) in shard_receipts {
        if shard_checksums.len() != total_shards {
            continue;
        }
        let Some(size_bytes) = session_part_size(&state.manifest, part_index) else {
            continue;
        };
        let mut ordered = Vec::with_capacity(total_shards);
        let mut complete = true;
        for shard_index in 0..total_shards {
            if let Some(checksum) = shard_checksums.get(&shard_index) {
                ordered.push(*checksum);
            } else {
                complete = false;
                break;
            }
        }
        if !complete {
            continue;
        }
        let shard_size_bytes = session_shard_size(&state.manifest, size_bytes);
        state.part_receipts.insert(
            part_index,
            SessionPartReceipt {
                part_index,
                size_bytes,
                shard_size_bytes,
                created_at: state.manifest.created_at,
                checksum: part_checksums.get(&part_index).copied(),
                shard_checksums: ordered,
            },
        );
    }
}

fn decode_metadata_file(path: &Path) -> Result<ObjectMetadata, AppError> {
    let bytes = fs::read(path).map_err(|e| io_err(e, "read N1 object metadata"))?;
    decode(&bytes)
}

fn read_authoritative_object_metadata(
    paths: Vec<PathBuf>,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let mut selected: Option<(u64, Vec<u8>, ObjectMetadata)> = None;
    let mut first_error: Option<AppError> = None;

    for path in paths {
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                first_error.get_or_insert_with(|| io_err(error, "read N1 object metadata"));
                continue;
            }
        };
        let mut metadata = match decode::<ObjectMetadata>(&bytes) {
            Ok(metadata) => metadata,
            Err(error) => {
                first_error.get_or_insert(error);
                continue;
            }
        };
        metadata.object_key = object_key.to_string();

        match selected.as_ref() {
            None => selected = Some((metadata.placement_generation, bytes, metadata)),
            Some((generation, selected_bytes, _))
                if metadata.placement_generation > *generation =>
            {
                selected = Some((metadata.placement_generation, bytes, metadata));
            }
            Some((generation, selected_bytes, _))
                if metadata.placement_generation == *generation && bytes != *selected_bytes =>
            {
                return Err(AppError::ServiceUnavailable(format!(
                    "N1 object metadata has same-generation divergent placement for object {} generation {}",
                    object_key, generation
                )));
            }
            _ => {}
        }
    }

    match selected {
        Some((_, _, metadata)) => Ok(Some(metadata)),
        None => match first_error {
            Some(error) => Err(error),
            None => Ok(None),
        },
    }
}

fn summary_metadata(fragment: &str, object_key: String, summary: NdxSummary) -> ObjectMetadata {
    let content_type = mime_guess::from_path(&object_key)
        .first_raw()
        .map(str::to_string)
        .unwrap_or_else(|| {
            let ext = unpack_extension_key(summary.packed_ext);
            if ext.is_empty() {
                "application/octet-stream".to_string()
            } else {
                mime_guess::from_ext(&ext)
                    .first_or_octet_stream()
                    .essence_str()
                    .to_string()
            }
        });
    ObjectMetadata {
        object_key,
        fragment: fragment.to_string(),
        version_id: summary.version_id,
        upload_session_id: String::new(),
        owner_uid: fragment.to_string(),
        content_type,
        total_size_bytes: summary.size_bytes,
        created_at: (summary.unix_nanos / 1_000_000_000i128) as i64,
        data_shards: 0,
        parity_shards: 0,
        placement_generation: 0,
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: super::disk_manager::default_stripe_size(),
    }
}

fn directory_entry(record: String) -> IndexListEntry {
    IndexListEntry {
        record,
        kind: "directory".to_string(),
        size_bytes: 0,
        created_at_unix_nanos: 0,
        packed_ext_u64: None,
        version_ulid: None,
    }
}

pub(crate) fn sync_dir(path: &Path) {
    let _ = linux_io::sync_dir_blocking(path);
}

fn write_atomic_with_durability(
    path: &Path,
    bytes: &[u8],
    strict_durability: bool,
) -> Result<(), AppError> {
    linux_io::atomic_write_blocking_with_durability(path, bytes, strict_durability)
        .map_err(|e| io_err(e, "atomic write N1 file"))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    write_atomic_with_durability(path, bytes, true)
}

pub(crate) fn write_pool_identity(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    write_atomic(path, bytes)
}

fn remove_file_if_exists(path: &Path) -> Result<bool, AppError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(io_err(err, "remove N1 file")),
    }
}

fn create_empty_marker(path: &Path) -> Result<(), AppError> {
    File::create(path)
        .map(|_| ())
        .map_err(|e| io_err(e, "create N1 marker"))
}

fn write_metadata_to_roots_with_durability(
    metadata: &ObjectMetadata,
    strict_durability: bool,
) -> Result<(), AppError> {
    ensure_fragment_dirs(&metadata.fragment)?;
    let bytes = encode(metadata)?;
    let file_name = ndx_file_name(metadata);
    let shard_paths = metadata
        .parts
        .iter()
        .flat_map(|part| part.shards.iter())
        .map(|shard| PathBuf::from(&shard.absolute_path))
        .collect::<Vec<_>>();
    let mut written = 0usize;
    for root in blob_roots() {
        let object_dir = object_index_dir(&root, &metadata.fragment, &metadata.object_key)?;
        let participates = shard_paths
            .iter()
            .any(|shard_path| shard_path.starts_with(&root));
        let payload_dir = object_dir.join(&metadata.version_id);

        if !participates {
            if let Some(path) = ndx_path_for_version(&object_dir, &metadata.version_id)? {
                let _ = remove_file_if_exists(&path)?;
            }
            if payload_dir.is_dir() {
                fs::remove_dir_all(&payload_dir)
                    .map_err(|e| io_err(e, "remove stale N1 object placement"))?;
            }
            if object_dir.is_dir()
                && fs::read_dir(&object_dir)
                    .map_err(|e| io_err(e, "inspect stale N1 object placement"))?
                    .next()
                    .is_none()
            {
                fs::remove_dir(&object_dir)
                    .map_err(|e| io_err(e, "remove empty N1 object placement"))?;
            }
            continue;
        }

        if !payload_dir.is_dir() {
            return Err(AppError::ServiceUnavailable(format!(
                "shard-bearing placement is missing for object {} version {} on {}",
                metadata.object_key,
                metadata.version_id,
                root.display()
            )));
        }
        write_atomic_with_durability(&object_dir.join(&file_name), &bytes, strict_durability)?;
        written += 1;
    }
    if written == 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "no shard-bearing blob placement exists for object {} version {}",
            metadata.object_key, metadata.version_id
        )));
    }
    Ok(())
}

fn write_metadata_to_roots(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_metadata_to_roots_with_durability(metadata, true)
}

pub(crate) fn object_metadata_placements_healthy(
    metadata: &ObjectMetadata,
) -> Result<bool, AppError> {
    let expected_bytes = encode(metadata)?;
    let expected_name = ndx_file_name(metadata);
    let shard_paths = metadata
        .parts
        .iter()
        .flat_map(|part| part.shards.iter())
        .map(|shard| PathBuf::from(&shard.absolute_path))
        .collect::<Vec<_>>();
    let mut expected_placements = 0usize;

    for root in blob_roots() {
        let participates = shard_paths
            .iter()
            .any(|shard_path| shard_path.starts_with(&root));
        let object_dir = object_index_dir(&root, &metadata.fragment, &metadata.object_key)?;
        let metadata_path = object_dir.join(&expected_name);
        let payload_dir = object_dir.join(&metadata.version_id);

        if participates {
            expected_placements += 1;
            if !payload_dir.is_dir() {
                return Ok(false);
            }
            let actual = match fs::read(&metadata_path) {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(err) => return Err(io_err(err, "read N1 colocated object metadata")),
            };
            if actual != expected_bytes {
                return Ok(false);
            }
        } else if metadata_path.exists() || payload_dir.exists() {
            return Ok(false);
        }
    }

    Ok(expected_placements > 0)
}

fn object_dirs_for_all_live(fragment: &str) -> Result<Vec<(PathBuf, String, String)>, AppError> {
    let mut out = Vec::new();
    for key in crate::namespace::flat::keys(fragment, "")? {
        let Some(pointer) = crate::namespace::flat::lookup(fragment, &key)? else {
            continue;
        };
        let dir = object_dir_candidates(fragment, &pointer.object_id)?
            .into_iter()
            .find(|dir| dir.is_dir())
            .unwrap_or_default();
        out.push((dir, key, pointer.object_id));
    }
    Ok(out)
}

fn all_live_version_paths(fragment: &str, object_key: &str) -> Result<Vec<PathBuf>, AppError> {
    let mut paths = Vec::new();
    for dir in object_dir_candidates(fragment, &object_key)? {
        if !dir.is_dir() {
            continue;
        }
        for name in read_dir_names(&dir)? {
            if parse_ndx_file_name(&name).is_some() {
                paths.push(dir.join(name));
            }
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

pub fn upsert_live_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_metadata_to_roots(metadata)
}

pub fn upsert_live_version_with_durability(
    metadata: &ObjectMetadata,
    durability: IndexWriteDurability,
) -> Result<(), AppError> {
    write_metadata_to_roots_with_durability(
        metadata,
        matches!(durability, IndexWriteDurability::Strict),
    )
}

pub fn rebind_unpublished_object_version(
    metadata: &ObjectMetadata,
    destination_object_id: &str,
) -> Result<ObjectMetadata, AppError> {
    let destination_object_id = normalize_object_id(destination_object_id)?;
    let source_object_id = normalize_object_id(&metadata.object_key)?;
    if source_object_id == destination_object_id {
        return Ok(metadata.clone());
    }

    crate::namespace::flat::with_keys(
        &metadata.fragment,
        &[&format!("@rebind:{}", metadata.version_id)],
        || {
            let roots = blob_roots();
            let mut moves = Vec::<(PathBuf, PathBuf)>::new();
            for root in &roots {
                let source_dir = object_index_dir(root, &metadata.fragment, &source_object_id)?;
                let destination_dir =
                    object_index_dir(root, &metadata.fragment, &destination_object_id)?;
                let source_payload = source_dir.join(&metadata.version_id);
                let destination_payload = destination_dir.join(&metadata.version_id);
                if source_payload.is_dir() {
                    if destination_payload.exists() {
                        return Err(AppError::Conflict(format!(
                            "object version {} already exists under destination object {}",
                            metadata.version_id, destination_object_id
                        )));
                    }
                    moves.push((source_payload, destination_payload));
                }
            }
            if moves.is_empty() {
                return Err(AppError::ServiceUnavailable(format!(
                    "unpublished object {} version {} has no shard-bearing placement",
                    source_object_id, metadata.version_id
                )));
            }

            let mut completed = Vec::<(PathBuf, PathBuf)>::new();
            for (source, destination) in &moves {
                if let Some(parent) = destination.parent() {
                    fs::create_dir_all(parent)
                        .map_err(|err| io_err(err, "create rebound N1 object placement"))?;
                }
                if let Err(err) = fs::rename(source, destination) {
                    for (old_source, new_destination) in completed.iter().rev() {
                        let _ = fs::rename(new_destination, old_source);
                    }
                    return Err(io_err(err, "rebind N1 object placement"));
                }
                completed.push((source.clone(), destination.clone()));
            }

            let mut rebound = metadata.clone();
            rebound.placement_generation =
                rebound.placement_generation.checked_add(1).ok_or_else(|| {
                    AppError::InternalServerError(
                        "object placement generation exhausted during rebind".into(),
                    )
                })?;
            rebound.object_key = destination_object_id.clone();
            for part in &mut rebound.parts {
                for shard in &mut part.shards {
                    let Some(root) = roots
                        .iter()
                        .find(|root| Path::new(&shard.absolute_path).starts_with(root))
                    else {
                        for (old_source, new_destination) in completed.iter().rev() {
                            let _ = fs::rename(new_destination, old_source);
                        }
                        return Err(AppError::InternalServerError(format!(
                            "shard path {} does not belong to a configured blob root",
                            shard.absolute_path
                        )));
                    };
                    shard.absolute_path =
                        object_index_dir(root, &metadata.fragment, &destination_object_id)?
                            .join(&metadata.version_id)
                            .join(&shard.file_name)
                            .to_string_lossy()
                            .into_owned();
                }
            }

            if let Err(err) = write_metadata_to_roots(&rebound) {
                for root in &roots {
                    let destination_dir =
                        object_index_dir(root, &metadata.fragment, &destination_object_id)?;
                    if let Some(path) =
                        ndx_path_for_version(&destination_dir, &metadata.version_id)?
                    {
                        let _ = remove_file_if_exists(&path);
                    }
                }
                for (old_source, new_destination) in completed.iter().rev() {
                    let _ = fs::rename(new_destination, old_source);
                }
                return Err(err);
            }

            for root in &roots {
                let source_dir = object_index_dir(root, &metadata.fragment, &source_object_id)?;
                if let Some(path) = ndx_path_for_version(&source_dir, &metadata.version_id)? {
                    let _ = remove_file_if_exists(&path)?;
                }
                if source_dir.is_dir()
                    && fs::read_dir(&source_dir)
                        .map_err(|err| io_err(err, "inspect rebound N1 source object"))?
                        .next()
                        .is_none()
                {
                    fs::remove_dir(&source_dir)
                        .map_err(|err| io_err(err, "remove rebound N1 source object"))?;
                }
            }

            Ok(rebound)
        },
    )
}

pub fn read_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let object_key = normalize_object_key(object_key)?;
    let mut paths = Vec::new();
    for dir in object_dir_candidates(fragment, &object_key)? {
        if let Some(path) = ndx_path_for_version(&dir, version_id)? {
            paths.push(path);
        }
    }
    read_authoritative_object_metadata(paths, &object_key)
}

pub fn read_current_live_head(
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let object_key = normalize_object_key(object_key)?;
    let mut latest = None::<(PathBuf, NdxSummary)>;
    for dir in object_dir_candidates(fragment, &object_key)? {
        let Some(summary) = latest_summary_in_object_dir(&dir)? else {
            continue;
        };
        if latest
            .as_ref()
            .is_none_or(|(_, current)| summary.version_id > current.version_id)
        {
            latest = Some((dir, summary));
        }
    }
    match latest {
        Some((_, summary)) => read_live_version(fragment, &object_key, &summary.version_id),
        None => Ok(None),
    }
}

pub fn list_live_versions(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let object_key = normalize_object_key(object_key)?;
    let mut by_version = BTreeMap::<String, Vec<PathBuf>>::new();
    for path in all_live_version_paths(fragment, &object_key)? {
        if let Some(version) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(parse_ndx_file_name)
            .map(|summary| summary.version_id)
        {
            by_version.entry(version).or_default().push(path);
        }
    }
    let mut out = Vec::with_capacity(by_version.len());
    for paths in by_version.into_values() {
        if let Some(metadata) = read_authoritative_object_metadata(paths, &object_key)? {
            out.push(metadata);
        }
    }
    out.sort_by(|a, b| a.version_id.cmp(&b.version_id));
    Ok(out)
}

pub fn list_live_heads(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut out = Vec::new();
    for (dir, rel, _object_id) in object_dirs_for_all_live(fragment)? {
        if let Some(summary) = latest_summary_in_object_dir(&dir)? {
            out.push(summary_metadata(fragment, rel, summary));
        }
    }
    out.sort_by(|a, b| a.object_key.cmp(&b.object_key));
    Ok(out)
}

fn scan_live_heads_direct_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
) -> Result<ListObjectsResult, AppError> {
    let prefix = prefix.unwrap_or_default();
    reject_reserved_user_path(
        &normalize_path(&prefix).map_err(|e| AppError::BadRequest(e.to_string()))?,
    )?;
    let delimiter = delimiter.unwrap_or_else(|| "/".into());
    let (names, next) = crate::namespace::flat::page(
        fragment,
        &prefix,
        &delimiter,
        continuation_token.as_deref(),
        limit,
    )?;
    let entries = names
        .iter()
        .map(|(key, directory)| IndexListEntry {
            record: key
                .strip_prefix(&prefix)
                .unwrap_or(key)
                .trim_end_matches('/')
                .to_string(),
            kind: if *directory { "directory" } else { "file" }.into(),
            size_bytes: 0,
            created_at_unix_nanos: 0,
            packed_ext_u64: None,
            version_ulid: None,
        })
        .collect();
    let common_prefixes = names
        .into_iter()
        .filter_map(|(key, directory)| directory.then_some(key))
        .collect();
    Ok(ListObjectsResult {
        prefix: Some(prefix),
        limit: Some(limit),
        entries,
        objects: Vec::new(),
        common_prefixes,
        continuation_token: next.clone(),
        next_continuation_token: next,
    })
}

pub fn scan_live_heads_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_live_heads_direct_page(fragment, prefix, delimiter, limit, continuation_token)
}

pub fn scan_live_full_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    let prefix = prefix.unwrap_or_default();
    let delimiter = delimiter.unwrap_or_default();
    let (names, next) = crate::namespace::flat::page(
        fragment,
        &prefix,
        &delimiter,
        continuation_token.as_deref(),
        limit,
    )?;
    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    for (key, directory) in names {
        if directory {
            common_prefixes.push(key);
            continue;
        }
        if let Some(pointer) = crate::namespace::flat::lookup(fragment, &key)? {
            if let Some(metadata) = read_current_live_head(fragment, &pointer.object_id)? {
                objects.push(metadata);
            }
        }
    }
    Ok(ListObjectsResult {
        prefix: Some(prefix),
        limit: Some(limit),
        entries: Vec::new(),
        objects,
        common_prefixes,
        continuation_token: next.clone(),
        next_continuation_token: next,
    })
}

mod replicas;
mod sessions;
mod trash;

pub use replicas::{
    fragment_has_object_index_records, fragment_meta, inspect_fragment_partition_replicas,
    repair_fragment_all_partition_replicas, repair_fragment_partition_replicas,
    sync_fragment_partition_replicas,
};
pub use sessions::{
    commit_session_part, commit_session_part_receipt, list_active_sessions, load_session,
    load_session_part_receipts, open_session, read_session_part, remove_session,
};
pub use trash::{
    delete_object_fast, ensure_fragment_owner_partition_available,
    ensure_fragment_owner_partition_available_strict, ensure_object_owner_partition_available,
    has_any_fragment_index, has_object_index_record, list_active_trash_partition_ids,
    list_object_keys_with_trash_versions, list_partition_trash_entries, list_trash_catalog,
    list_trash_heads, list_trash_versions, list_trashed_object_version_ids_batch,
    move_object_to_trash_fast, move_object_version_to_trash_with_metadata,
    move_objects_to_trash_batch, object_partition_id, publish_trash_version_for_maintenance,
    read_latest_trash, read_trash_version, restore_object_from_trash_fast,
    restore_object_version_from_trash_with_metadata, restore_objects_from_trash_batch,
    restore_objects_from_trash_manifest_batch, scan_trash_full_metadata_page,
    scan_trash_heads_page,
};

pub(crate) use sessions::{
    clear_pending_binding, list_pending_bindings, read_pending_binding, write_pending_binding,
};
#[allow(unused_imports)]
pub(crate) use trash::{
    delete_unreferenced_object_index, delete_unreferenced_object_indexes_batch,
    referenced_object_ids, scan_trash_records, visit_trash_records,
};

use replicas::count_object_index_records;
#[cfg(test)]
use trash::collect_symlink_object_ids_under;

#[cfg(test)]
use replicas::{cleanup_stale_index_plane_on_blob_only_roots, same_root};

#[cfg(test)]
mod tests;
