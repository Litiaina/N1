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

mod live;
mod sessions;
mod trash;

pub use live::*;
pub use sessions::*;
pub use trash::*;

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

#[path = "index_store/metadata_codec.rs"]
mod metadata_codec;

use metadata_codec::*;
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
    let roots = ObjectPathManager::new().get_all_namespace_roots(fragment);
    let mut prepared = 0usize;
    let mut failures = Vec::new();
    for root in roots {
        let root = fragment_root_on(&root, fragment);
        match fs::create_dir_all(&root) {
            Ok(()) => prepared = prepared.saturating_add(1),
            Err(error) => failures.push(format!("{}: {}", root.display(), error)),
        }
    }
    if prepared == 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "no local fragment storage root is available for '{fragment}': {}",
            failures.join("; ")
        )));
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
        if e == rustix::io::Errno::NOENT {
            AppError::NotFound
        } else {
            AppError::InternalServerError(format!("open N2 directory {}: {e}", path.display()))
        }
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
    let names = match read_dir_names(dir) {
        Ok(names) => names,
        Err(AppError::NotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    for name in names {
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
    let names = match read_dir_names(dir) {
        Ok(names) => names,
        Err(AppError::NotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    for name in names {
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

fn decode_metadata_file_if_present(path: &Path) -> Result<Option<ObjectMetadata>, AppError> {
    match decode_metadata_file(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if is_not_found(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

fn decode_metadata_file_as_if_present(
    path: &Path,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    match decode_metadata_file_as(path, object_key) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if is_not_found(&error) => Ok(None),
        Err(error) => Err(error),
    }
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
            File::open(parent)
                .and_then(|file| file.sync_all())
                .map_err(|error| io_err(error, "sync immutable NCDX publication directory"))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temporary);
            let existing = fs::read(path).map_err(|error| io_err(error, "read existing NCDX"))?;
            if existing == bytes {
                // A prior request may have linked the exact bytes and then
                // failed its directory barrier. Retry must finish that barrier
                // before this holder can contribute a commit acknowledgement.
                File::open(parent)
                    .and_then(|file| file.sync_all())
                    .map_err(|error| io_err(error, "sync retried NCDX publication directory"))
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
        let names = match read_dir_names(&dir) {
            Ok(names) => names,
            Err(AppError::NotFound) => continue,
            Err(error) => return Err(error),
        };
        for name in names {
            if parse_ncdx_file_name(&name).is_some() {
                paths.push(dir.join(name));
            }
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
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

#[cfg(test)]
#[path = "index_store/tests.rs"]
mod tests;
