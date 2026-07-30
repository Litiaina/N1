use super::disk_manager::{
    IndexListEntry, ListObjectsResult, ObjectMetadata, ObjectPathManager, PartMetadata,
    ReservationReceipt,
};
use super::index::{IndexSessionState, SessionPartReceipt, TrashCatalogEntry};
use super::upload::UploadSessionManifest;
use crate::core::checksum::{Sha256Digest, format_sha256, parse_sha256_hex, sha256_digest};
use crate::core::constants::N2_BLOB_DIR;
use crate::core::fragment_identity::{
    FRAGMENT_ALIAS_DIR, PHYSICAL_FRAGMENT_PREFIX, fragment_root_on as resolved_fragment_root_on,
    physical_fragment_root_on, stable_fragment_id_for_root,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::paths::{base_name, normalize_path};
use chrono::Utc;
use rustix::fs::{CWD, Dir, Mode, OFlags, renameat};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use ulid::Ulid;

pub(crate) const OBJECTS_DIR: &str = ".objects";
pub(crate) const UPLOADS_DIR: &str = ".uploads";
pub(crate) const TRASH_DIR: &str = ".trash";
pub(crate) const TMP_DIR: &str = ".tmp";
pub(crate) const LOCKS_DIR: &str = ".locks";
pub(crate) const GC_DIR: &str = ".gc";
pub(crate) const REPAIR_DIR: &str = ".repair";
const FRAGMENT_META_FILE: &str = ".fragment.ncdx";
const MANIFEST_FILE: &str = "manifest.ncdx";
const BINDING_FILE: &str = "binding.ncdx";
const SHARD_RECEIPT_PREFIX: &str = "shard_p";
const PART_RECEIPT_PREFIX: &str = "part_p";
const RECEIPT_SUFFIX: &str = ".receipt";
const PART_META_PREFIX: &str = "part_";
const NCDX_FORMAT_VERSION: u32 = 4;
pub(crate) const OBJECT_DIR_PREFIX: &str = "@o_";
pub(crate) const FRAGMENT_DIR_PREFIX: &str = PHYSICAL_FRAGMENT_PREFIX;
const TRASH_JOBS_DIR: &str = ".trash-jobs";
pub(crate) const BLOB_DIR: &str = N2_BLOB_DIR;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMeta {
    pub fragment_id: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
struct NcdxSummary {
    version_id: String,
    size_bytes: u64,
    unix_nanos: i128,
    packed_ext: u64,
    file_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionRow {
    manifest: UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
    last_activity_at: i64,
    finalized_at: Option<i64>,
    aborted_at: Option<i64>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiskObjectMetadata {
    format_version: u32,
    object_id: String,
    packed_ext_u64: u64,
    fragment: String,
    version_id: String,
    upload_session_id: String,
    owner_uid: String,
    content_type: String,
    total_size_bytes: u64,
    created_at: i64,
    pool_id: String,
    pool_generation: u64,
    membership_epoch: u64,
    data_shards: u16,
    parity_shards: u16,
    parts: Vec<crate::core::disk_manager::PartMetadata>,
    is_delete_marker: bool,
    stripe_size_bytes: usize,
    payload_sha256: Sha256Digest,
}

impl From<&ObjectMetadata> for DiskObjectMetadata {
    fn from(meta: &ObjectMetadata) -> Self {
        Self {
            format_version: NCDX_FORMAT_VERSION,
            object_id: meta.object_key.clone(),
            packed_ext_u64: meta.packed_ext_u64,
            fragment: meta.fragment.clone(),
            version_id: meta.version_id.clone(),
            upload_session_id: meta.upload_session_id.clone(),
            owner_uid: meta.owner_uid.clone(),
            content_type: meta.content_type.clone(),
            total_size_bytes: meta.total_size_bytes,
            created_at: meta.created_at,
            pool_id: meta.pool_id.clone(),
            pool_generation: meta.pool_generation,
            membership_epoch: meta.membership_epoch,
            data_shards: meta.data_shards,
            parity_shards: meta.parity_shards,
            parts: meta.parts.clone(),
            is_delete_marker: meta.is_delete_marker,
            stripe_size_bytes: meta.stripe_size_bytes,
            payload_sha256: [0; 32],
        }
    }
}

impl From<DiskObjectMetadata> for ObjectMetadata {
    fn from(meta: DiskObjectMetadata) -> Self {
        Self {
            object_key: meta.object_id,
            packed_ext_u64: meta.packed_ext_u64,
            fragment: meta.fragment,
            version_id: meta.version_id,
            upload_session_id: meta.upload_session_id,
            owner_uid: meta.owner_uid,
            content_type: meta.content_type,
            total_size_bytes: meta.total_size_bytes,
            created_at: meta.created_at,
            pool_id: meta.pool_id,
            pool_generation: meta.pool_generation,
            membership_epoch: meta.membership_epoch,
            data_shards: meta.data_shards,
            parity_shards: meta.parity_shards,
            parts: meta.parts,
            is_delete_marker: meta.is_delete_marker,
            stripe_size_bytes: meta.stripe_size_bytes,
        }
    }
}

fn encode_disk_object_metadata(metadata: &ObjectMetadata) -> Result<Vec<u8>, AppError> {
    let mut disk = DiskObjectMetadata::from(metadata);
    disk.validate_fields()?;
    disk.payload_sha256 = disk.expected_sha256()?;
    Ok(rmp_serde::to_vec_named(&disk)?)
}

impl DiskObjectMetadata {
    fn validate_fields(&self) -> Result<(), AppError> {
        if self.format_version != NCDX_FORMAT_VERSION {
            return Err(AppError::Conflict(format!(
                "NCDX format {} is not N2 3.3 format {}",
                self.format_version, NCDX_FORMAT_VERSION
            )));
        }
        if self.object_id.trim().is_empty()
            || self.fragment.trim().is_empty()
            || self.version_id.trim().is_empty()
            || self.pool_id.trim().is_empty()
            || self.pool_generation == 0
            || self.data_shards == 0
            || self.parity_shards == 0
        {
            return Err(AppError::Conflict(
                "NCDX requires object, fragment, version, pool, and static EC identities".into(),
            ));
        }
        for part in &self.parts {
            if let Some(checksum) = part.checksum.as_deref()
                && parse_sha256_hex(checksum).is_none()
            {
                return Err(AppError::Conflict(format!(
                    "NCDX part {} has an invalid SHA-256 checksum",
                    part.part_index
                )));
            }
            if part
                .shards
                .iter()
                .any(|shard| parse_sha256_hex(&shard.checksum).is_none())
            {
                return Err(AppError::Conflict(format!(
                    "NCDX part {} contains a shard without a valid SHA-256 checksum",
                    part.part_index
                )));
            }
        }
        Ok(())
    }

    fn expected_sha256(&self) -> Result<Sha256Digest, AppError> {
        let mut unsigned = self.clone();
        unsigned.payload_sha256 = [0; 32];
        let payload = rmp_serde::to_vec_named(&unsigned)?;
        Ok(sha256_digest(&payload))
    }

    fn validate(&self) -> Result<(), AppError> {
        self.validate_fields()?;
        let expected = self.expected_sha256()?;
        if self.payload_sha256 != expected {
            return Err(AppError::Conflict(format!(
                "NCDX integrity mismatch: expected {}, recorded {}",
                format_sha256(&expected),
                format_sha256(&self.payload_sha256)
            )));
        }
        Ok(())
    }
}

fn index_roots() -> Vec<PathBuf> {
    ObjectPathManager::new().get_storage_roots()
}

fn blob_roots() -> Vec<PathBuf> {
    ObjectPathManager::new().get_storage_roots()
}

fn stable_fragment_id_for_fragment(fragment: &str) -> String {
    for root in index_roots() {
        if let Some(stable) = stable_fragment_id_for_root(&root, fragment) {
            return stable;
        }
    }
    fragment.to_string()
}

pub(crate) fn fragment_root_on(root: &Path, fragment: &str) -> PathBuf {
    resolved_fragment_root_on(root, fragment)
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
        || segment == BLOB_DIR
        || segment.starts_with(FRAGMENT_ALIAS_DIR)
        || segment.starts_with(OBJECTS_DIR)
        || segment.starts_with(TRASH_DIR)
        || segment.starts_with(UPLOADS_DIR)
        || segment.starts_with(TMP_DIR)
        || segment.starts_with(LOCKS_DIR)
        || segment.starts_with(GC_DIR)
        || segment.starts_with(REPAIR_DIR)
        || segment.starts_with(TRASH_JOBS_DIR)
}

pub(crate) fn internal_namespace_entry_name(name: &str) -> bool {
    name.starts_with(OBJECT_DIR_PREFIX)
        || name.starts_with(FRAGMENT_DIR_PREFIX)
        || matches!(
            name,
            BLOB_DIR
                | FRAGMENT_ALIAS_DIR
                | UPLOADS_DIR
                | TRASH_DIR
                | TMP_DIR
                | LOCKS_DIR
                | GC_DIR
                | REPAIR_DIR
                | TRASH_JOBS_DIR
                | FRAGMENT_META_FILE
        )
}

pub(crate) fn reject_reserved_user_path(path: &str) -> Result<(), AppError> {
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        if reserved_user_segment(segment) {
            return Err(AppError::BadRequest(format!(
                "path segment '{}' is reserved by N2",
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
        return Err(AppError::BadRequest("invalid N2 object id".to_string()));
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

fn object_fanout_segments_for_id(object_id: &str) -> Result<[String; 4], AppError> {
    let object_id = normalize_object_id(object_id)?;
    Ok(ObjectPathManager::object_fanout_segments(&object_id))
}

pub(crate) fn object_id_from_dir_name(name: &str) -> Option<String> {
    normalize_object_id(name.strip_prefix(OBJECT_DIR_PREFIX)?).ok()
}

pub(crate) fn object_record_dir(
    root: &Path,
    fragment: &str,
    object_id: &str,
) -> Result<PathBuf, AppError> {
    let fanout = object_fanout_segments_for_id(object_id)?;
    Ok(blob_fragment_root_on(root, fragment)
        .join(OBJECTS_DIR)
        .join(&fanout[0])
        .join(&fanout[1])
        .join(&fanout[2])
        .join(&fanout[3])
        .join(object_dir_name_for_id(object_id)?))
}

pub(crate) fn read_namespace_symlink_object_id(
    _root: &Path,
    _fragment: &str,
    link_path: &Path,
) -> Result<Option<String>, AppError> {
    let meta = match fs::symlink_metadata(link_path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(io_err(err, "inspect N2 namespace link")),
    };
    if !meta.file_type().is_symlink() {
        return Ok(None);
    }
    let target = fs::read_link(link_path).map_err(|e| io_err(e, "read N2 namespace link"))?;
    if target.is_absolute() {
        return Err(AppError::InternalServerError(format!(
            "N2 namespace link {} has absolute target",
            link_path.display()
        )));
    }
    if target.components().count() != 1 {
        return Err(AppError::InternalServerError(format!(
            "N2 namespace link {} target is not a direct object pointer",
            link_path.display()
        )));
    }
    let Some(name) = target.file_name().and_then(|name| name.to_str()) else {
        return Ok(None);
    };
    Ok(object_id_from_dir_name(name))
}

pub(crate) fn object_dir_candidates(
    fragment: &str,
    object_id: &str,
) -> Result<Vec<PathBuf>, AppError> {
    let object_id = normalize_object_id(object_id)?;
    let mut out = Vec::new();
    for root in blob_roots() {
        out.push(object_record_dir(&root, fragment, &object_id)?);
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn object_record_dirs_under_root(
    root: &Path,
    fragment: &str,
) -> Result<Vec<(PathBuf, String)>, AppError> {
    let objects_root = blob_fragment_root_on(root, fragment).join(OBJECTS_DIR);
    if !objects_root.is_dir() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    let mut stack = vec![(objects_root, 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        for name in read_dir_names(&dir)? {
            let child = dir.join(&name);
            if !child.is_dir() {
                continue;
            }
            if depth < 4 {
                stack.push((child, depth + 1));
                continue;
            }
            let Some(object_id) = object_id_from_dir_name(&name) else {
                continue;
            };
            out.push((child, object_id));
        }
    }
    Ok(out)
}

fn ensure_fragment_dirs(fragment: &str) -> Result<(), AppError> {
    for root in ObjectPathManager::new().get_all_namespace_roots(fragment) {
        let root = fragment_root_on(&root, fragment);
        fs::create_dir_all(&root).map_err(|e| io_err(e, "create fragment storage root"))?;
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

fn ncdx_file_name(metadata: &ObjectMetadata) -> String {
    let unix_nanos = i128::from(metadata.created_at.max(0)) * 1_000_000_000i128;
    format!(
        "{}.{}.{}.{}.ncdx",
        metadata.version_id, metadata.total_size_bytes, unix_nanos, metadata.packed_ext_u64
    )
}

fn parse_ncdx_file_name(name: &str) -> Option<NcdxSummary> {
    let stem = name.strip_suffix(".ncdx")?;
    let mut parts = stem.split('.');
    let version_id = parts.next()?.to_string();
    let size_bytes = parts.next()?.parse::<u64>().ok()?;
    let unix_nanos = parts.next()?.parse::<i128>().ok()?;
    let packed_ext = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(NcdxSummary {
        version_id,
        size_bytes,
        unix_nanos,
        packed_ext,
        file_name: name.to_string(),
    })
}

pub(crate) fn read_dir_names(path: &Path) -> Result<Vec<String>, AppError> {
    let fd = rustix::fs::openat(
        CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| {
        AppError::InternalServerError(format!("open N2 directory {}: {e}", path.display()))
    })?;
    let mut dir = Dir::new(fd).map_err(|e| {
        AppError::InternalServerError(format!("scan N2 directory {}: {e}", path.display()))
    })?;
    let mut names = Vec::new();
    for entry in &mut dir {
        let entry = entry.map_err(|e| {
            AppError::InternalServerError(format!("read N2 directory {}: {e}", path.display()))
        })?;
        let name = entry.file_name().to_string_lossy();
        if name != "." && name != ".." {
            names.push(name.into_owned());
        }
    }
    Ok(names)
}

fn latest_summary_in_object_dir(dir: &Path) -> Result<Option<NcdxSummary>, AppError> {
    if !dir.is_dir() {
        return Ok(None);
    }
    let mut latest = None::<NcdxSummary>;
    for name in read_dir_names(dir)? {
        let Some(summary) = parse_ncdx_file_name(&name) else {
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

fn ncdx_path_for_version(dir: &Path, version_id: &str) -> Result<Option<PathBuf>, AppError> {
    if !dir.is_dir() {
        return Ok(None);
    }
    for name in read_dir_names(dir)? {
        if !name.starts_with(version_id) {
            continue;
        }
        if parse_ncdx_file_name(&name).is_some() {
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

fn shard_receipt_name(part_index: u64, shard_index: usize, checksum: &Sha256Digest) -> String {
    format!(
        "shard_p{part_index:020}_s{shard_index:04}.{}{RECEIPT_SUFFIX}",
        format_sha256(checksum)
    )
}

fn part_receipt_name(part_index: u64, checksum: &Sha256Digest) -> String {
    format!(
        "part_p{part_index:020}.{}{RECEIPT_SUFFIX}",
        format_sha256(checksum)
    )
}

fn parse_shard_receipt_name(name: &str) -> Option<(u64, usize, Sha256Digest)> {
    let stem = name.strip_suffix(RECEIPT_SUFFIX)?;
    let (identity, checksum) = stem.rsplit_once('.')?;
    let checksum = parse_sha256_hex(checksum)?;
    let rest = identity.strip_prefix(SHARD_RECEIPT_PREFIX)?;
    let (part, shard) = rest.split_once("_s")?;
    Some((part.parse().ok()?, shard.parse().ok()?, checksum))
}

fn parse_part_receipt_name(name: &str) -> Option<(u64, Sha256Digest)> {
    let stem = name.strip_suffix(RECEIPT_SUFFIX)?;
    let (part, checksum) = stem.rsplit_once('.')?;
    let part = part.strip_prefix(PART_RECEIPT_PREFIX)?;
    Some((part.parse().ok()?, parse_sha256_hex(checksum)?))
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
    shard_receipts: BTreeMap<u64, BTreeMap<usize, Sha256Digest>>,
    part_checksums: BTreeMap<u64, Sha256Digest>,
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
    let bytes = fs::read(path).map_err(|e| io_err(e, "read N2 object metadata"))?;
    let metadata = decode::<DiskObjectMetadata>(&bytes)?;
    metadata.validate()?;
    Ok(metadata.into())
}

fn decode_metadata_file_as(path: &Path, object_key: &str) -> Result<ObjectMetadata, AppError> {
    let mut meta = decode_metadata_file(path)?;
    meta.object_key = object_key.to_string();
    Ok(meta)
}

fn record_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn summary_metadata(fragment: &str, object_key: String, summary: NcdxSummary) -> ObjectMetadata {
    let ext = unpack_extension_key(summary.packed_ext);
    ObjectMetadata {
        object_key,
        packed_ext_u64: summary.packed_ext,
        fragment: fragment.to_string(),
        version_id: summary.version_id,
        upload_session_id: String::new(),
        owner_uid: fragment.to_string(),
        content_type: if ext.is_empty() {
            "application/octet-stream".to_string()
        } else {
            mime_guess::from_ext(&ext)
                .first_or_octet_stream()
                .essence_str()
                .to_string()
        },
        total_size_bytes: summary.size_bytes,
        created_at: (summary.unix_nanos / 1_000_000_000i128) as i64,
        // Filename-only summaries are presentation records, never NCDX
        // durability truth. Exact reads decode the immutable NCDX body.
        pool_id: String::new(),
        pool_generation: 0,
        membership_epoch: 0,
        data_shards: 0,
        parity_shards: 0,
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: super::disk_manager::default_stripe_size(),
    }
}

fn summary_entry(record: String, summary: &NcdxSummary) -> IndexListEntry {
    IndexListEntry {
        record,
        kind: "file".to_string(),
        size_bytes: summary.size_bytes,
        created_at_unix_nanos: summary.unix_nanos.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
        packed_ext_u64: Some(summary.packed_ext),
        version_ulid: Some(summary.version_id.clone()),
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

fn sync_dir(path: &Path) {
    if let Ok(dir) = OpenOptions::new().read(true).open(path) {
        let _ = dir.sync_all();
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("N2 path {} has no parent", path.display()))
    })?;
    fs::create_dir_all(parent).map_err(|e| io_err(e, "create N2 parent dir"))?;
    let tmp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("n1"),
        Ulid::new()
    ));
    {
        let mut file = File::create(&tmp).map_err(|e| io_err(e, "create N2 temp file"))?;
        file.write_all(bytes)
            .map_err(|e| io_err(e, "write N2 temp file"))?;
        file.sync_all()
            .map_err(|e| io_err(e, "sync N2 temp file"))?;
    }
    renameat(CWD, &tmp, CWD, path).map_err(|e| {
        AppError::InternalServerError(format!("publish N2 file {}: {e}", path.display()))
    })?;
    sync_dir(parent);
    Ok(())
}

fn write_immutable(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("N2 path {} has no parent", path.display()))
    })?;
    fs::create_dir_all(parent).map_err(|error| io_err(error, "create NCDX parent"))?;
    let temporary = parent.join(format!(".n2-ncdx-publish-{}", Ulid::new()));
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| io_err(error, "create NCDX staging file"))?;
        file.write_all(bytes)
            .map_err(|error| io_err(error, "write NCDX staging file"))?;
        // The immutable file contents and required inode metadata must reach
        // stable storage before publication. The parent-directory fsync below
        // is the durability boundary for the new hard link.
        file.sync_data()
            .map_err(|error| io_err(error, "sync NCDX staging file"))?;
    }
    match fs::hard_link(&temporary, path) {
        Ok(()) => {
            let _ = fs::remove_file(&temporary);
            sync_dir(parent);
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temporary);
            let existing = fs::read(path).map_err(|error| io_err(error, "read existing NCDX"))?;
            if existing == bytes {
                Ok(())
            } else {
                Err(AppError::Conflict(format!(
                    "refused to overwrite divergent immutable NCDX {}",
                    path.display()
                )))
            }
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(io_err(error, "publish immutable NCDX"))
        }
    }
}

fn remove_file_if_exists(path: &Path) -> Result<bool, AppError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(io_err(err, "remove N2 file")),
    }
}

fn create_empty_marker(path: &Path) -> Result<(), AppError> {
    File::create(path)
        .map(|_| ())
        .map_err(|e| io_err(e, "create N2 marker"))
}

fn write_metadata_to_roots(metadata: &ObjectMetadata) -> Result<(), AppError> {
    ensure_fragment_dirs(&metadata.fragment)?;
    let bytes = encode_disk_object_metadata(metadata)?;
    let file_name = ncdx_file_name(metadata);
    let mut written = 0usize;
    for root in blob_roots() {
        let object_dir = object_record_dir(&root, &metadata.fragment, &metadata.object_key)?;
        if !object_dir.join(&metadata.version_id).is_dir() {
            continue;
        }
        write_immutable(&object_dir.join(&file_name), &bytes)?;
        written += 1;
    }
    if written == 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "no local shard-bearing object root exists for {}/{} version {}",
            metadata.fragment, metadata.object_key, metadata.version_id
        )));
    }
    Ok(())
}

fn object_dirs_for_all_live(fragment: &str) -> Result<Vec<(PathBuf, String, String)>, AppError> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for fragment_root in existing_fragment_roots(fragment) {
        let index_root = fragment_root
            .parent()
            .unwrap_or(&fragment_root)
            .to_path_buf();
        let mut stack = vec![(fragment_root.clone(), String::new())];
        while let Some((dir, rel)) = stack.pop() {
            let Ok(names) = read_dir_names(&dir) else {
                continue;
            };
            for name in names {
                if internal_namespace_entry_name(&name) {
                    continue;
                }
                let child = dir.join(&name);
                let child_rel = if rel.is_empty() {
                    name.clone()
                } else {
                    format!("{rel}/{name}")
                };
                if let Some(object_id) =
                    read_namespace_symlink_object_id(&index_root, fragment, &child)?
                {
                    let object_dir = object_dir_candidates(fragment, &object_id)?
                        .into_iter()
                        .find(|dir| dir.is_dir())
                        .unwrap_or_else(|| {
                            object_record_dir(&index_root, fragment, &object_id).unwrap_or_default()
                        });
                    if seen.insert(child_rel.clone()) {
                        out.push((object_dir, child_rel, object_id));
                    }
                } else if child.is_dir() {
                    stack.push((child, child_rel));
                }
            }
        }
    }
    out.sort_by(|a, b| a.1.cmp(&b.1));
    Ok(out)
}

fn all_live_version_paths(fragment: &str, object_key: &str) -> Result<Vec<PathBuf>, AppError> {
    let mut paths = Vec::new();
    for dir in object_dir_candidates(fragment, &object_key)? {
        if !dir.is_dir() {
            continue;
        }
        for name in read_dir_names(&dir)? {
            if parse_ncdx_file_name(&name).is_some() {
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

pub fn read_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let object_key = normalize_object_key(object_key)?;
    for dir in object_dir_candidates(fragment, &object_key)? {
        if let Some(path) = ncdx_path_for_version(&dir, version_id)? {
            return decode_metadata_file_as(&path, &object_key).map(Some);
        }
    }
    Ok(None)
}

pub fn find_live_version_by_id(
    fragment: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let version_id = version_id.trim();
    if version_id.is_empty() {
        return Ok(None);
    }

    for root in blob_roots() {
        for (dir, object_id) in object_record_dirs_under_root(&root, fragment)? {
            if let Some(path) = ncdx_path_for_version(&dir, version_id)? {
                return decode_metadata_file_as(&path, &object_id).map(Some);
            }
        }
    }
    Ok(None)
}

pub fn read_current_live_head(
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let object_key = normalize_object_key(object_key)?;
    let mut latest = None::<(PathBuf, NcdxSummary)>;
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
    latest
        .map(|(dir, summary)| decode_metadata_file_as(&dir.join(summary.file_name), &object_key))
        .transpose()
}

pub fn list_live_versions(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let object_key = normalize_object_key(object_key)?;
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for path in all_live_version_paths(fragment, &object_key)? {
        let meta = decode_metadata_file_as(&path, &object_key)?;
        if seen.insert(meta.version_id.clone()) {
            out.push(meta);
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
    limit: usize,
    continuation_token: Option<String>,
) -> Result<ListObjectsResult, AppError> {
    let prefix = prefix
        .map(|prefix| normalize_object_key(&prefix))
        .transpose()?
        .unwrap_or_default();
    let limit = limit.max(1);
    let mut by_record = BTreeMap::<String, (IndexListEntry, Option<ObjectMetadata>)>::new();
    for fragment_root in existing_fragment_roots(fragment) {
        let index_root = fragment_root
            .parent()
            .unwrap_or(&fragment_root)
            .to_path_buf();
        let dir = fragment_root.join(&prefix);
        if !dir.is_dir() {
            continue;
        }
        for name in read_dir_names(&dir)? {
            if internal_namespace_entry_name(&name) {
                continue;
            }
            let child = dir.join(&name);
            if let Some(object_id) =
                read_namespace_symlink_object_id(&index_root, fragment, &child)?
            {
                if continuation_token
                    .as_ref()
                    .is_some_and(|after| name.as_str() <= after.as_str())
                {
                    continue;
                }
                let Some(object_dir) = object_dir_candidates(fragment, &object_id)?
                    .into_iter()
                    .find(|dir| dir.is_dir())
                else {
                    continue;
                };
                let Some(summary) = latest_summary_in_object_dir(&object_dir)? else {
                    continue;
                };
                let record = record_path(&prefix, &name);
                let entry = summary_entry(name.clone(), &summary);
                let metadata = summary_metadata(fragment, record, summary);
                by_record.entry(name).or_insert((entry, Some(metadata)));
            } else if child.is_dir() {
                if continuation_token
                    .as_ref()
                    .is_some_and(|after| name.as_str() <= after.as_str())
                {
                    continue;
                }
                by_record
                    .entry(name.clone())
                    .or_insert((directory_entry(name), None));
            }
        }
    }

    let mut entries = Vec::new();
    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut next = None;
    for (idx, (_record, (entry, object))) in by_record.into_iter().enumerate() {
        if idx >= limit {
            next = entries
                .last()
                .map(|entry: &IndexListEntry| entry.record.clone());
            break;
        }
        if entry.kind == "directory" {
            common_prefixes.push(entry.record.clone());
        }
        if let Some(object) = object {
            objects.push(object);
        }
        entries.push(entry);
    }
    Ok(ListObjectsResult {
        prefix: Some(prefix),
        limit: Some(limit),
        entries,
        objects,
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
    let _ = delimiter;
    scan_live_heads_direct_page(fragment, prefix, limit, continuation_token)
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
    let limit = limit.max(1);
    let mut candidates = Vec::new();
    let mut objects = Vec::new();
    let mut common_prefixes = BTreeSet::new();
    for (dir, rel, object_id) in object_dirs_for_all_live(fragment)? {
        if !prefix.is_empty() && !rel.starts_with(&prefix) {
            continue;
        }
        if continuation_token
            .as_ref()
            .is_some_and(|after| rel <= *after)
        {
            continue;
        }
        if delimiter == "/" {
            let rest = if prefix.is_empty() {
                rel.as_str()
            } else {
                rel.strip_prefix(&prefix).unwrap_or("")
            };
            let rest = rest.trim_start_matches('/');
            if let Some((first, _)) = rest.split_once('/') {
                let cp = if prefix.is_empty() {
                    format!("{first}/")
                } else {
                    format!("{}/{}", prefix.trim_end_matches('/'), first)
                };
                common_prefixes.insert(cp);
                continue;
            }
        }
        candidates.push((rel, object_id, dir));
    }

    let mut continuation = None;
    if candidates.len() > limit {
        candidates.truncate(limit);
        continuation = candidates.last().map(|(rel, _, _)| rel.clone());
    }

    for (_rel, object_id, dir) in candidates {
        let Some(summary) = latest_summary_in_object_dir(&dir)? else {
            continue;
        };
        objects.push(decode_metadata_file_as(
            &dir.join(summary.file_name),
            &object_id,
        )?);
    }
    Ok(ListObjectsResult {
        prefix: Some(prefix),
        limit: Some(limit),
        entries: Vec::new(),
        objects,
        common_prefixes: common_prefixes.into_iter().collect(),
        continuation_token: continuation.clone(),
        next_continuation_token: continuation,
    })
}

pub fn publish_trash_version_for_maintenance(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_metadata_to_roots(metadata)
}

pub fn read_trash_version(
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    for dir in object_dir_candidates(fragment, object_id)? {
        if let Some(path) = ncdx_path_for_version(&dir, version_id)? {
            return decode_metadata_file(&path).map(Some);
        }
    }
    Ok(None)
}

pub fn read_latest_trash(
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let versions = list_trash_versions(fragment, object_key)?;
    Ok(versions
        .into_iter()
        .max_by(|a, b| a.version_id.cmp(&b.version_id)))
}

pub fn list_trash_versions(
    fragment: &str,
    object_id: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for dir in object_dir_candidates(fragment, object_id)? {
        if !dir.is_dir() {
            continue;
        }
        for name in read_dir_names(&dir)? {
            if parse_ncdx_file_name(&name).is_some() {
                let meta = decode_metadata_file(&dir.join(name))?;
                if seen.insert(meta.version_id.clone()) {
                    out.push(meta);
                }
            }
        }
    }
    out.sort_by(|a, b| a.version_id.cmp(&b.version_id));
    Ok(out)
}

pub(crate) fn scan_trash_records(fragment: &str) -> Result<Vec<TrashRecord>, AppError> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for fragment_root in existing_fragment_roots(fragment) {
        let index_root = fragment_root
            .parent()
            .unwrap_or(&fragment_root)
            .to_path_buf();
        let trash_root = fragment_root.join(TRASH_DIR);
        let mut stack = vec![(trash_root.clone(), String::new())];
        while let Some((dir, rel)) = stack.pop() {
            if !dir.is_dir() {
                continue;
            }
            for name in read_dir_names(&dir)? {
                if internal_namespace_entry_name(&name) && name != OBJECTS_DIR {
                    continue;
                }
                let child = dir.join(&name);
                let child_rel = if rel.is_empty() {
                    name.clone()
                } else {
                    format!("{rel}/{name}")
                };
                if let Some(object_id) =
                    read_namespace_symlink_object_id(&index_root, fragment, &child)?
                {
                    let mut object_keys = BTreeSet::new();
                    let mut version_ids = BTreeSet::new();
                    let mut deleted_at = Utc::now().timestamp();
                    let Some(object_dir) = object_dir_candidates(fragment, &object_id)?
                        .into_iter()
                        .find(|dir| dir.is_dir())
                    else {
                        continue;
                    };
                    for ncdx_name in read_dir_names(&object_dir)? {
                        if parse_ncdx_file_name(&ncdx_name).is_none() {
                            continue;
                        }
                        let meta = decode_metadata_file(&object_dir.join(&ncdx_name))?;
                        deleted_at = meta.created_at;
                        object_keys.insert(object_id.clone());
                        version_ids.insert(meta.version_id);
                    }
                    if seen.insert(child_rel.clone()) {
                        out.push(TrashRecord {
                            trash_entry_id: child_rel.clone(),
                            original_path: child_rel.clone(),
                            source_relative_path: child_rel,
                            kind: TrashKind::File,
                            object_keys: object_keys.into_iter().collect(),
                            version_ids: version_ids.into_iter().collect(),
                            deleted_at_unix: deleted_at,
                        });
                    }
                } else if child.is_dir() {
                    stack.push((child, child_rel));
                }
            }
        }
    }
    out.sort_by(|a, b| a.source_relative_path.cmp(&b.source_relative_path));
    Ok(out)
}

pub fn list_trash_heads(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut out = Vec::new();
    for record in scan_trash_records(fragment)? {
        for object_key in &record.object_keys {
            if let Some(mut meta) = read_latest_trash(fragment, object_key)? {
                meta.object_key = record.original_path.clone();
                out.push(meta);
            }
        }
    }
    Ok(out)
}

pub fn list_trash_catalog(fragment: &str) -> Result<Vec<TrashCatalogEntry>, AppError> {
    Ok(scan_trash_records(fragment)?
        .into_iter()
        .flat_map(|record| {
            record
                .object_keys
                .into_iter()
                .map(move |object_id| TrashCatalogEntry {
                    object_key: object_id,
                    version_ids: record.version_ids.clone(),
                    deleted_at: record.deleted_at_unix,
                })
        })
        .collect())
}

fn scan_trash_page(
    fragment: &str,
    prefix: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    full_metadata: bool,
) -> Result<ListObjectsResult, AppError> {
    let prefix = prefix
        .map(|prefix| normalize_object_key(&prefix))
        .transpose()?
        .unwrap_or_default();
    let limit = limit.max(1);
    let mut by_record = BTreeMap::<String, (IndexListEntry, Option<ObjectMetadata>)>::new();

    for record in scan_trash_records(fragment)? {
        let relative = if prefix.is_empty() {
            record.original_path.as_str()
        } else if record.original_path == prefix {
            if record.kind == TrashKind::Directory {
                continue;
            }
            record.original_path.as_str()
        } else if let Some(rest) = record
            .original_path
            .strip_prefix(prefix.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
        {
            rest
        } else {
            continue;
        };

        if relative.is_empty() {
            continue;
        }

        let (name, is_nested) = relative
            .split_once('/')
            .map(|(head, _)| (head.to_string(), true))
            .unwrap_or_else(|| (relative.to_string(), false));

        if continuation_token
            .as_ref()
            .is_some_and(|after| name.as_str() <= after.as_str())
        {
            continue;
        }

        if is_nested || record.kind == TrashKind::Directory {
            by_record
                .entry(name.clone())
                .or_insert((directory_entry(name), None));
            continue;
        }

        let version_id = record.version_ids.last().cloned().unwrap_or_default();
        let mut metadata = if full_metadata {
            let Some(object_id) = record.object_keys.first() else {
                continue;
            };
            let Some(mut meta) = read_latest_trash(fragment, object_id)? else {
                continue;
            };
            meta.object_key = record.original_path.clone();
            meta
        } else {
            ObjectMetadata {
                object_key: record.original_path.clone(),
                packed_ext_u64: packed_extension_key(&record.original_path),
                fragment: fragment.to_string(),
                version_id,
                upload_session_id: String::new(),
                owner_uid: fragment.to_string(),
                content_type: "application/octet-stream".to_string(),
                total_size_bytes: 0,
                created_at: record.deleted_at_unix,
                // Trash list summaries do not claim an object layout.
                pool_id: String::new(),
                pool_generation: 0,
                membership_epoch: 0,
                data_shards: 0,
                parity_shards: 0,
                parts: Vec::new(),
                is_delete_marker: false,
                stripe_size_bytes: super::disk_manager::default_stripe_size(),
            }
        };
        metadata.object_key = record.original_path.clone();
        let entry = IndexListEntry {
            record: name.clone(),
            kind: "file".to_string(),
            size_bytes: metadata.total_size_bytes,
            created_at_unix_nanos: metadata.created_at.saturating_mul(1_000_000_000),
            packed_ext_u64: Some(packed_extension_key(&metadata.object_key)),
            version_ulid: Some(metadata.version_id.clone()),
        };
        by_record.entry(name).or_insert((entry, Some(metadata)));
    }

    let mut page = by_record.into_iter().collect::<Vec<_>>();
    page.sort_by(|a, b| a.0.cmp(&b.0));
    let continuation = if page.len() > limit {
        page.truncate(limit);
        page.last().map(|(record, _)| record.clone())
    } else {
        None
    };
    let entries = page
        .iter()
        .map(|(_, (entry, _))| entry.clone())
        .collect::<Vec<_>>();
    let mut objects = page
        .into_iter()
        .filter_map(|(_, (_, meta))| meta)
        .collect::<Vec<_>>();
    objects.sort_by(|a, b| a.object_key.cmp(&b.object_key));
    Ok(ListObjectsResult {
        prefix: Some(prefix),
        limit: Some(limit),
        entries,
        objects,
        common_prefixes: Vec::new(),
        continuation_token: continuation.clone(),
        next_continuation_token: continuation,
    })
}

pub fn scan_trash_heads_page(
    fragment: &str,
    prefix: Option<String>,
    _delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_trash_page(fragment, prefix, limit, continuation_token, false)
}

pub fn scan_trash_full_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    _delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_trash_page(fragment, prefix, limit, continuation_token, true)
}

pub fn list_object_keys_with_trash_versions(fragment: &str) -> Result<Vec<String>, AppError> {
    Ok(list_trash_catalog(fragment)?
        .into_iter()
        .map(|entry| entry.object_key)
        .collect())
}

fn collect_symlink_object_ids_under(
    root: &Path,
    fragment: &str,
    base: &Path,
    include_trash_root: bool,
) -> Result<BTreeSet<String>, AppError> {
    let mut out = BTreeSet::new();
    let mut stack = vec![base.to_path_buf()];
    while let Some(dir) = stack.pop() {
        if !dir.is_dir() {
            continue;
        }
        for name in read_dir_names(&dir)? {
            if internal_namespace_entry_name(&name) {
                if !(include_trash_root && name == TRASH_DIR) {
                    continue;
                }
            }
            let child = dir.join(&name);
            if let Some(object_id) = read_namespace_symlink_object_id(root, fragment, &child)? {
                out.insert(object_id);
            } else if child.is_dir() {
                stack.push(child);
            }
        }
    }
    Ok(out)
}

pub(crate) fn referenced_object_ids(fragment: &str) -> Result<BTreeSet<String>, AppError> {
    let mut out = BTreeSet::new();
    for fragment_root in existing_fragment_roots(fragment) {
        let index_root = fragment_root
            .parent()
            .unwrap_or(&fragment_root)
            .to_path_buf();
        out.extend(collect_symlink_object_ids_under(
            &index_root,
            fragment,
            &fragment_root,
            true,
        )?);
    }
    Ok(out)
}

pub(crate) fn delete_unreferenced_object_record(
    fragment: &str,
    object_id: &str,
) -> Result<bool, AppError> {
    let object_id = normalize_object_id(object_id)?;
    if referenced_object_ids(fragment)?.contains(&object_id) {
        return Ok(false);
    }
    let mut removed = false;
    for dir in object_dir_candidates(fragment, &object_id)? {
        if dir.exists() {
            fs::remove_dir_all(&dir).map_err(|e| io_err(e, "delete N2 object record dir"))?;
            removed = true;
            if let Some(parent) = dir.parent() {
                sync_dir(parent);
            }
        }
    }
    Ok(removed)
}

pub(crate) fn cleanup_unreferenced_object_records(fragment: &str) -> Result<usize, AppError> {
    let referenced = referenced_object_ids(fragment)?;
    let mut removed = 0usize;
    let mut seen = HashSet::new();
    for fragment_root in existing_fragment_roots(fragment) {
        let root = fragment_root.parent().unwrap_or(&fragment_root);
        for (_, object_id) in object_record_dirs_under_root(root, fragment)? {
            if referenced.contains(&object_id) || !seen.insert(object_id.clone()) {
                continue;
            }
            if delete_unreferenced_object_record(fragment, &object_id)? {
                removed += 1;
            }
        }
    }
    Ok(removed)
}

pub fn list_trashed_object_version_ids_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let records = scan_trash_records(fragment)?;
    let mut out = HashMap::new();
    for key in object_keys {
        let versions = records
            .iter()
            .find(|record| {
                record.original_path == *key || record.object_keys.iter().any(|id| id == key)
            })
            .map(|record| record.version_ids.clone())
            .unwrap_or_default();
        out.insert(key.clone(), versions);
    }
    Ok(out)
}

pub fn move_object_version_to_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    read_live_version(fragment, object_key, version_id)
}

pub fn restore_object_version_from_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    read_trash_version(fragment, object_key, version_id)
}

pub fn move_object_to_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let versions = list_live_versions(fragment, object_key)?;
    if versions.is_empty() {
        return Ok(None);
    }
    let ids = versions
        .iter()
        .map(|meta| meta.version_id.clone())
        .collect::<Vec<_>>();
    Ok(Some(ids))
}

pub fn move_objects_to_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let mut out = HashMap::new();
    for key in object_keys {
        if let Some(ids) = move_object_to_trash_fast(fragment, key)? {
            out.insert(key.clone(), ids);
        }
    }
    Ok(out)
}

pub fn restore_object_from_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let versions = list_trash_versions(fragment, object_key)?;
    if versions.is_empty() {
        return Ok(None);
    }
    let ids = versions
        .iter()
        .map(|meta| meta.version_id.clone())
        .collect::<Vec<_>>();
    Ok(Some(ids))
}

pub fn restore_objects_from_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let mut out = HashMap::new();
    for key in object_keys {
        if let Some(ids) = restore_object_from_trash_fast(fragment, key)? {
            out.insert(key.clone(), ids);
        }
    }
    Ok(out)
}

pub fn restore_objects_from_trash_manifest_batch(
    fragment: &str,
    object_versions: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let mut out = HashMap::new();
    let records = scan_trash_records(fragment)?;
    for (key, versions) in object_versions {
        let object_ids = records
            .iter()
            .find(|record| {
                record.original_path == *key || record.object_keys.iter().any(|id| id == key)
            })
            .map(|record| record.object_keys.clone())
            .unwrap_or_else(|| vec![key.clone()]);
        let mut restored = Vec::new();
        for version_id in versions {
            for object_id in &object_ids {
                if restore_object_version_from_trash_with_metadata(fragment, object_id, version_id)?
                    .is_some()
                {
                    restored.push(version_id.clone());
                    break;
                }
            }
        }
        if !restored.is_empty() {
            out.insert(key.clone(), restored);
        }
    }
    Ok(out)
}

pub fn delete_all_trash_versions_for_key(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let records = scan_trash_records(fragment)?;
    let object_ids = records
        .iter()
        .find(|record| {
            record.original_path == object_key
                || record.object_keys.iter().any(|id| id == object_key)
        })
        .map(|record| record.object_keys.clone())
        .unwrap_or_else(|| vec![object_key.to_string()]);
    let mut ids = Vec::new();
    for object_id in &object_ids {
        for meta in list_trash_versions(fragment, object_id)? {
            if !ids.contains(&meta.version_id) {
                ids.push(meta.version_id);
            }
        }
    }
    if ids.is_empty() {
        return Ok(None);
    }
    for object_id in object_ids {
        delete_unreferenced_object_record(fragment, &object_id)?;
    }
    Ok(Some(ids))
}

pub fn delete_object_fast(
    fragment: &str,
    object_id: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let mut ids = Vec::new();
    let live_versions = list_live_versions(fragment, object_id)?;
    for meta in live_versions {
        if !ids.contains(&meta.version_id) {
            ids.push(meta.version_id);
        }
    }
    let trash_versions = list_trash_versions(fragment, object_id)?;
    for meta in trash_versions {
        if !ids.contains(&meta.version_id) {
            ids.push(meta.version_id);
        }
    }
    delete_unreferenced_object_record(fragment, object_id)?;
    Ok((!ids.is_empty()).then_some(ids))
}

pub fn delete_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let mut removed = false;
    for dir in object_dir_candidates(fragment, object_key)? {
        if let Some(path) = ncdx_path_for_version(&dir, version_id)? {
            removed |= remove_file_if_exists(&path)?;
        }
        let payload = dir.join(version_id);
        if payload.is_dir() {
            fs::remove_dir_all(&payload)
                .map_err(|e| io_err(e, "delete N2 object version payload"))?;
            removed = true;
        }
        if dir.is_dir() && read_dir_names(&dir)?.is_empty() {
            fs::remove_dir(&dir).map_err(|e| io_err(e, "remove empty N2 object directory"))?;
        }
    }
    Ok(removed)
}

pub fn delete_trash_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    delete_live_version(fragment, object_key, version_id)
}

pub fn clear_fragment_trash(fragment: &str, object_key: Option<&str>) -> Result<(), AppError> {
    if let Some(key) = object_key {
        let _ = delete_unreferenced_object_record(fragment, key)?;
        return Ok(());
    }
    for root in index_roots() {
        let path = fragment_root_on(&root, fragment).join(TRASH_DIR);
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|e| io_err(e, "clear N2 trash object dir"))?;
        }
        fs::create_dir_all(&path).map_err(|e| io_err(e, "recreate N2 trash object dir"))?;
    }
    let _ = cleanup_unreferenced_object_records(fragment)?;
    Ok(())
}

pub fn drop_fragment_trash_namespace(fragment: &str) -> Result<(), AppError> {
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        let path = fragment_root.join(TRASH_DIR);
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|e| io_err(e, "drop N2 trash namespace"))?;
        }
        fs::create_dir_all(&path).map_err(|e| io_err(e, "recreate N2 trash namespace"))?;
        sync_dir(&fragment_root);
    }
    Ok(())
}

pub fn has_object_record(fragment: &str, object_key: &str) -> Result<bool, AppError> {
    Ok(object_dir_candidates(fragment, object_key)?
        .into_iter()
        .any(|path| path.is_dir()))
}

pub fn ensure_fragment_namespace_available(fragment: &str) -> Result<(), AppError> {
    ensure_fragment_dirs(fragment)
}

pub fn has_any_fragment_index(fragment: &str) -> Result<bool, AppError> {
    Ok(existing_fragment_roots(fragment).into_iter().any(|root| {
        root.join(FRAGMENT_META_FILE).exists()
            || root.join(UPLOADS_DIR).exists()
            || count_object_records(root.parent().unwrap_or(&root), fragment) > 0
            || object_dirs_for_all_live(fragment)
                .map(|v| !v.is_empty())
                .unwrap_or(false)
    }))
}

pub fn fragment_has_object_records(fragment: &str, roots: &[PathBuf]) -> bool {
    for root in roots {
        let Ok(object_dirs) = object_record_dirs_under_root(root, fragment) else {
            continue;
        };
        for (object_dir, _) in object_dirs {
            let Ok(records) = fs::read_dir(object_dir) else {
                continue;
            };
            for record in records.filter_map(|entry| entry.ok()) {
                let Some(name) = record.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                if parse_ncdx_file_name(&name).is_some() {
                    return true;
                }
            }
        }
    }
    false
}

fn count_object_records(root: &Path, fragment: &str) -> u64 {
    let Ok(object_dirs) = object_record_dirs_under_root(root, fragment) else {
        return 0;
    };
    let mut count = 0u64;
    for (object_dir, _) in object_dirs {
        let Ok(records) = fs::read_dir(object_dir) else {
            continue;
        };
        for record in records.filter_map(|entry| entry.ok()) {
            if record
                .file_name()
                .to_str()
                .is_some_and(|name| parse_ncdx_file_name(name).is_some())
            {
                count += 1;
            }
        }
    }
    count
}

pub fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    ensure_fragment_dirs(&manifest.fragment)?;
    let now = Utc::now().timestamp();
    let row = SessionRow {
        manifest: manifest.clone(),
        reservation,
        last_activity_at: now,
        finalized_at: None,
        aborted_at: None,
    };
    let bytes = encode(&row)?;
    for dir in session_roots(&manifest.fragment, &manifest.version_id) {
        write_atomic(&dir.join(MANIFEST_FILE), &bytes)?;
    }
    Ok(())
}

fn session_dir(root: &Path, fragment: &str, version_id: &str) -> PathBuf {
    fragment_root_on(root, fragment)
        .join(UPLOADS_DIR)
        .join(version_id)
}

fn session_roots(fragment: &str, version_id: &str) -> Vec<PathBuf> {
    ObjectPathManager::new()
        .get_session_root(fragment, version_id)
        .into_iter()
        .map(|root| session_dir(&root, fragment, version_id))
        .collect()
}

fn load_session_row(fragment: &str, version_id: &str) -> Result<Option<SessionRow>, AppError> {
    for dir in session_roots(fragment, version_id) {
        let path = dir.join(MANIFEST_FILE);
        if path.exists() {
            return decode(&fs::read(path).map_err(|e| io_err(e, "read upload manifest"))?)
                .map(Some);
        }
    }
    Ok(None)
}

pub fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &PartMetadata,
) -> Result<(), AppError> {
    let bytes = encode(part)?;
    for dir in session_roots(fragment, version_id) {
        write_atomic(
            &dir.join(format!("{}{:020}.ncdx", PART_META_PREFIX, part.part_index)),
            &bytes,
        )?;
    }
    Ok(())
}

pub fn commit_session_part_receipt(
    fragment: &str,
    version_id: &str,
    receipt: &SessionPartReceipt,
) -> Result<(), AppError> {
    for dir in session_roots(fragment, version_id) {
        fs::create_dir_all(&dir).map_err(|e| io_err(e, "create upload receipt dir"))?;
        remove_receipts_for_part(&dir, receipt.part_index)?;
        if let Some(checksum) = receipt.checksum {
            create_empty_marker(&dir.join(part_receipt_name(receipt.part_index, &checksum)))?;
        }
        for (shard_index, checksum) in receipt.shard_checksums.iter().copied().enumerate() {
            create_empty_marker(&dir.join(shard_receipt_name(
                receipt.part_index,
                shard_index,
                &checksum,
            )))?;
        }
        sync_dir(&dir);
    }
    Ok(())
}

pub fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    let Some(row) = load_session_row(fragment, version_id)? else {
        return Ok(None);
    };
    let mut state = row.into_state();
    for dir in session_roots(fragment, version_id) {
        if !dir.is_dir() {
            continue;
        }
        let mut shard_receipts = BTreeMap::<u64, BTreeMap<usize, Sha256Digest>>::new();
        let mut part_checksums = BTreeMap::<u64, Sha256Digest>::new();
        for name in read_dir_names(&dir)? {
            let path = dir.join(&name);
            if name.starts_with(PART_META_PREFIX) && name.ends_with(".ncdx") {
                let part: PartMetadata =
                    decode(&fs::read(path).map_err(|e| io_err(e, "read upload part metadata"))?)?;
                state.parts.insert(part.part_index, part);
            } else if let Some((part_index, shard_index, checksum)) =
                parse_shard_receipt_name(&name)
            {
                shard_receipts
                    .entry(part_index)
                    .or_default()
                    .insert(shard_index, checksum);
            } else if let Some((part_index, checksum)) = parse_part_receipt_name(&name) {
                part_checksums.insert(part_index, checksum);
            }
        }
        push_receipts_from_filename(&mut state, shard_receipts, part_checksums);
        break;
    }
    Ok(Some(state))
}

pub fn read_session_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    Ok(load_session(fragment, version_id)?.and_then(|state| state.parts.get(&part_index).cloned()))
}

pub fn load_session_part_receipts(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    let Some(state) = load_session(fragment, version_id)? else {
        return Ok(None);
    };
    let mut out = vec![None; manifest.expected_parts as usize];
    for (idx, receipt) in state.part_receipts {
        if let Some(slot) = out.get_mut(idx as usize) {
            *slot = Some(receipt);
        }
    }
    Ok(Some(out))
}

pub fn remove_session(fragment: &str, version_id: &str) -> Result<bool, AppError> {
    let mut removed = false;
    for dir in session_roots(fragment, version_id) {
        if dir.exists() {
            fs::remove_dir_all(&dir).map_err(|e| io_err(e, "remove upload session"))?;
            removed = true;
        }
    }
    Ok(removed)
}

pub fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for root in existing_fragment_roots(fragment) {
        let uploads = root.join(UPLOADS_DIR);
        if !uploads.is_dir() {
            continue;
        }
        for version_id in read_dir_names(&uploads)? {
            if !seen.insert(version_id.clone()) {
                continue;
            }
            if let Some(state) = load_session(fragment, &version_id)? {
                if state.finalized_at.is_none() && state.aborted_at.is_none() {
                    out.push(state);
                }
            }
        }
    }
    Ok(out)
}

pub(crate) fn write_pending_binding(binding: &PendingUploadBinding) -> Result<(), AppError> {
    let bytes = encode(binding)?;
    for dir in session_roots(&binding.fragment, &binding.version_id) {
        write_atomic(&dir.join(BINDING_FILE), &bytes)?;
    }
    Ok(())
}

pub(crate) fn read_pending_binding(
    fragment: &str,
    version_id: &str,
) -> Result<Option<PendingUploadBinding>, AppError> {
    for dir in session_roots(fragment, version_id) {
        let path = dir.join(BINDING_FILE);
        if path.exists() {
            return decode(&fs::read(path).map_err(|e| io_err(e, "read pending upload binding"))?)
                .map(Some);
        }
    }
    Ok(None)
}

pub(crate) fn list_pending_bindings(fragment: &str) -> Result<Vec<PendingUploadBinding>, AppError> {
    let mut out = Vec::new();
    for session in list_active_sessions(fragment)? {
        if let Some(binding) = read_pending_binding(fragment, &session.manifest.version_id)? {
            out.push(binding);
        }
    }
    Ok(out)
}

pub(crate) fn clear_pending_binding(fragment: &str, version_id: &str) -> Result<(), AppError> {
    for dir in session_roots(fragment, version_id) {
        let _ = remove_file_if_exists(&dir.join(BINDING_FILE))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn metadata() -> ObjectMetadata {
        ObjectMetadata {
            object_key: "01KWOBJECT00000000000000000".into(),
            packed_ext_u64: packed_extension_key("docs/report.pdf"),
            fragment: "01KWFRAGMENT000000000000000".into(),
            version_id: "01KWVERSION0000000000000000".into(),
            upload_session_id: "01KWSESSION0000000000000000".into(),
            owner_uid: "owner".into(),
            content_type: "application/pdf".into(),
            total_size_bytes: 4096,
            created_at: 1_782_864_000,
            pool_id: "pool-a".into(),
            pool_generation: 7,
            membership_epoch: 7,
            data_shards: 2,
            parity_shards: 1,
            parts: Vec::new(),
            is_delete_marker: false,
            stripe_size_bytes: 262_144,
        }
    }

    #[test]
    fn immutable_ncdx_uses_named_fields_without_logical_path() {
        let bytes = encode_disk_object_metadata(&metadata()).expect("encode metadata");
        let value: Value = rmp_serde::from_slice(&bytes).expect("decode metadata");
        assert!(value.is_object());
        assert_eq!(
            value.get("format_version").and_then(Value::as_u64),
            Some(NCDX_FORMAT_VERSION as u64)
        );
        assert!(value.get("payload_sha256").is_some());
        assert!(value.get("object_id").is_some());
        assert!(value.get("object_key").is_none());
        assert!(value.get("logical_path").is_none());
    }

    #[test]
    fn immutable_ncdx_rejects_payload_mutation() {
        let bytes = encode_disk_object_metadata(&metadata()).expect("encode metadata");
        let mut disk: DiskObjectMetadata =
            rmp_serde::from_slice(&bytes).expect("decode disk metadata");
        disk.content_type = "application/corrupt".into();
        assert!(disk.validate().is_err());
    }

    #[test]
    fn packed_extension_is_zero_decode_filename_data() {
        let packed = packed_extension_key("docs/Clip.PDF");
        let expected = 3u64 | (b'p' as u64) << 8 | (b'd' as u64) << 16 | (b'f' as u64) << 24;
        assert_eq!(packed, expected);
        assert_eq!(packed_extension_key("docs/archive.longexts"), 0);
    }

    #[test]
    fn reserved_internal_planes_are_not_user_paths() {
        assert!(normalize_object_key(".trash/video.mp4").is_err());
        assert!(normalize_object_key(".objects/video.mp4").is_err());
        assert!(normalize_object_key("docs/@o_bad/file.txt").is_err());
        assert_eq!(
            normalize_object_key("docs/video.mp4").expect("valid path"),
            "docs/video.mp4"
        );
    }

    #[test]
    fn object_record_path_has_four_hash_fanout_levels() {
        let object_id = "01KWOBJECT00000000000000000";
        let segments = ObjectPathManager::object_fanout_segments(object_id);
        assert_eq!(segments.len(), 4);
        assert!(segments.iter().all(|segment| segment.len() == 2));
        assert_eq!(
            object_dir_name_for_id(object_id).expect("object dir"),
            format!("@o_{object_id}")
        );
    }

    #[test]
    fn ncdx_filename_round_trips_zero_decode_summary() {
        let metadata = metadata();
        let name = ncdx_file_name(&metadata);
        let summary = parse_ncdx_file_name(&name).expect("parse ncdx filename");
        assert_eq!(summary.version_id, metadata.version_id);
        assert_eq!(summary.size_bytes, metadata.total_size_bytes);
        assert_eq!(summary.packed_ext, metadata.packed_ext_u64);
    }
}
