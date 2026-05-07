use super::disk_manager::{
    EdgeListEntry, ListObjectsResult, ObjectMetadata, ObjectPathManager, PartMetadata,
    ReservationReceipt,
};
use super::index::{
    IndexReplicaWriteMode, IndexSessionState, SessionPartReceipt, TrashCatalogEntry,
};
use super::upload::UploadSessionManifest;
use crate::core::constants::EDGE_BLOB_DIR;
use crate::enums::app_error_enum::AppError;
use crate::namespace::paths::{base_name, normalize_path};
use chrono::Utc;
use rustix::fs::{CWD, Dir, Mode, OFlags, renameat};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs as unix_fs;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use ulid::Ulid;

pub(crate) const OBJECTS_DIR: &str = ".objects";
pub(crate) const UPLOADS_DIR: &str = ".uploads";
pub(crate) const TRASH_DIR: &str = ".trash";
pub(crate) const TMP_DIR: &str = ".tmp";
pub(crate) const LOCKS_DIR: &str = ".locks";
pub(crate) const GC_DIR: &str = ".gc";
pub(crate) const REPAIR_DIR: &str = ".repair";
const FRAGMENT_META_FILE: &str = ".fragment.ndx";
const MANIFEST_FILE: &str = "manifest.ndx";
const BINDING_FILE: &str = "binding.ndx";
const SHARD_RECEIPT_PREFIX: &str = "shard_p";
const PART_RECEIPT_PREFIX: &str = "part_p";
const RECEIPT_SUFFIX: &str = ".receipt";
const PART_META_PREFIX: &str = "part_";
pub(crate) const OBJECT_DIR_PREFIX: &str = "@o_";
const INTENTS_DIR: &str = ".intents";
const TRASH_JOBS_DIR: &str = ".trash-jobs";
pub(crate) const BLOB_DIR: &str = EDGE_BLOB_DIR;

static EDGE_FS_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
const QUOTA_PROJECT_ID_FLOOR: u32 = 10_000;
const QUOTA_PROJECT_ID_CEILING: u32 = 2_000_000_000;

#[derive(Debug, Clone)]
pub struct FragmentQuotaSpec {
    pub fragment: String,
    pub quota_bytes: u64,
    pub quota_project_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMeta {
    pub fragment_id: String,
    pub state: String,
    pub quota_bytes: u64,
    pub quota_project_id: Option<u32>,
    pub committed_bytes: u64,
    pub reserved_bytes: u64,
    pub trash_bytes: u64,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
    #[serde(default)]
    pub key_phc: Option<String>,
    #[serde(default)]
    pub nt_hash_hex: Option<String>,
    #[serde(default)]
    pub index_partition_count: Option<u16>,
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
pub(crate) enum EdgeTrashKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EdgeTrashRecord {
    pub trash_entry_id: String,
    pub original_path: String,
    pub source_relative_path: String,
    pub kind: EdgeTrashKind,
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

fn index_roots() -> Vec<PathBuf> {
    ObjectPathManager::new().get_all_index_roots()
}

pub(crate) fn fragment_root_on(root: &Path, fragment: &str) -> PathBuf {
    root.join(fragment)
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

fn first_fragment_root(fragment: &str) -> Result<PathBuf, AppError> {
    let roots = fragment_roots(fragment);
    let Some(root) = roots.first() else {
        return Err(AppError::ServiceUnavailable(
            "no index roots configured for N1 Edge".to_string(),
        ));
    };
    Ok(root.clone())
}

#[inline]
fn reserved_user_segment(segment: &str) -> bool {
    segment.starts_with(OBJECT_DIR_PREFIX)
        || segment == BLOB_DIR
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

pub(crate) fn internal_namespace_entry_name(name: &str) -> bool {
    name.starts_with(OBJECT_DIR_PREFIX)
        || matches!(
            name,
            BLOB_DIR
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
                "path segment '{}' is reserved by N1 Edge",
                segment
            )));
        }
    }
    Ok(())
}

pub(crate) fn normalize_edge_key(object_key: &str) -> Result<String, AppError> {
    let key = normalize_path(object_key).map_err(|err| AppError::BadRequest(err.to_string()))?;
    reject_reserved_user_path(&key)?;
    Ok(key)
}

pub(crate) fn object_key_hash(object_key: &str) -> Result<String, AppError> {
    let key = normalize_edge_key(object_key)?;
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
        return Err(AppError::BadRequest(
            "invalid N1 Edge object id".to_string(),
        ));
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

pub(crate) fn object_id_from_dir_name(name: &str) -> Option<String> {
    normalize_object_id(name.strip_prefix(OBJECT_DIR_PREFIX)?).ok()
}

pub(crate) fn object_index_dir(
    root: &Path,
    fragment: &str,
    object_id: &str,
) -> Result<PathBuf, AppError> {
    Ok(root
        .join(fragment)
        .join(OBJECTS_DIR)
        .join(object_dir_name_for_id(object_id)?))
}

pub(crate) fn live_object_dir(
    root: &Path,
    fragment: &str,
    object_id: &str,
) -> Result<PathBuf, AppError> {
    object_index_dir(root, fragment, object_id)
}

pub(crate) fn namespace_path(root: &Path, fragment: &str, logical_path: &str) -> PathBuf {
    root.join(fragment).join(logical_path)
}

fn object_symlink_target(object_id: &str) -> Result<PathBuf, AppError> {
    Ok(PathBuf::from(object_dir_name_for_id(object_id)?))
}

pub(crate) fn read_namespace_symlink_object_id(
    _root: &Path,
    _fragment: &str,
    link_path: &Path,
) -> Result<Option<String>, AppError> {
    let meta = match fs::symlink_metadata(link_path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(io_err(err, "inspect edge namespace link")),
    };
    if !meta.file_type().is_symlink() {
        return Ok(None);
    }
    let target = fs::read_link(link_path).map_err(|e| io_err(e, "read edge namespace link"))?;
    if target.is_absolute() {
        return Err(AppError::InternalServerError(format!(
            "edge namespace link {} has absolute target",
            link_path.display()
        )));
    }
    if target.components().count() != 1 {
        return Err(AppError::InternalServerError(format!(
            "edge namespace link {} target is not a direct object pointer",
            link_path.display()
        )));
    }
    let Some(name) = target.file_name().and_then(|name| name.to_str()) else {
        return Ok(None);
    };
    Ok(object_id_from_dir_name(name))
}

pub(crate) fn publish_namespace_link(
    root: &Path,
    fragment: &str,
    logical_path: &str,
    object_id: &str,
) -> Result<(), AppError> {
    let logical_path = normalize_edge_key(logical_path)?;
    let link = namespace_path(root, fragment, &logical_path);
    let parent = link.parent().ok_or_else(|| {
        AppError::InternalServerError("edge namespace link has no parent".to_string())
    })?;
    fs::create_dir_all(parent).map_err(|e| io_err(e, "create edge namespace parent"))?;
    fs::create_dir_all(object_index_dir(root, fragment, object_id)?)
        .map_err(|e| io_err(e, "create edge object index dir"))?;

    if let Some(existing) = read_namespace_symlink_object_id(root, fragment, &link)? {
        if existing == normalize_object_id(object_id)? {
            return Ok(());
        }
        return Err(AppError::Conflict(format!(
            "logical path '{}' already points to another object",
            logical_path
        )));
    }
    if fs::symlink_metadata(&link).is_ok() {
        return Err(AppError::Conflict(format!(
            "logical path '{}' already exists",
            logical_path
        )));
    }
    let target = object_symlink_target(object_id)?;
    unix_fs::symlink(target, &link).map_err(|e| io_err(e, "publish edge namespace link"))?;
    sync_dir(parent);
    Ok(())
}

pub(crate) fn object_dir_candidates(
    fragment: &str,
    object_id: &str,
) -> Result<Vec<PathBuf>, AppError> {
    let object_id = normalize_object_id(object_id)?;
    let mut out = Vec::new();
    for root in fragment_roots(fragment) {
        out.push(object_index_dir(
            root.parent().unwrap_or(&root),
            fragment,
            &object_id,
        )?);
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn ensure_fragment_dirs(fragment: &str) -> Result<(), AppError> {
    for root in fragment_roots(fragment) {
        fs::create_dir_all(root.join(OBJECTS_DIR)).map_err(|e| io_err(e, "create objects dir"))?;
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
        AppError::InternalServerError(format!("open edge directory {}: {e}", path.display()))
    })?;
    let mut dir = Dir::new(fd).map_err(|e| {
        AppError::InternalServerError(format!("scan edge directory {}: {e}", path.display()))
    })?;
    let mut names = Vec::new();
    for entry in &mut dir {
        let entry = entry.map_err(|e| {
            AppError::InternalServerError(format!("read edge directory {}: {e}", path.display()))
        })?;
        let name = entry.file_name().to_string_lossy();
        if name != "." && name != ".." {
            names.push(name.into_owned());
        }
    }
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

pub(crate) fn latest_summary_metadata_in_object_dir(
    fragment: &str,
    record_key: &str,
    dir: &Path,
) -> Result<Option<ObjectMetadata>, AppError> {
    Ok(latest_summary_in_object_dir(dir)?
        .map(|summary| summary_metadata(fragment, record_key.to_string(), summary)))
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
    let bytes = fs::read(path).map_err(|e| io_err(e, "read edge object metadata"))?;
    decode(&bytes)
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

fn summary_metadata(fragment: &str, object_key: String, summary: NdxSummary) -> ObjectMetadata {
    let ext = unpack_extension_key(summary.packed_ext);
    ObjectMetadata {
        object_key,
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
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: super::disk_manager::default_stripe_size(),
    }
}

fn summary_entry(record: String, summary: &NdxSummary) -> EdgeListEntry {
    EdgeListEntry {
        record,
        kind: "file".to_string(),
        size_bytes: summary.size_bytes,
        created_at_unix_nanos: summary.unix_nanos.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
        packed_ext_u64: Some(summary.packed_ext),
        version_ulid: Some(summary.version_id.clone()),
    }
}

fn directory_entry(record: String) -> EdgeListEntry {
    EdgeListEntry {
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
        AppError::InternalServerError(format!("edge path {} has no parent", path.display()))
    })?;
    fs::create_dir_all(parent).map_err(|e| io_err(e, "create edge parent dir"))?;
    let tmp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("edge"),
        Ulid::new()
    ));
    {
        let mut file = File::create(&tmp).map_err(|e| io_err(e, "create edge temp file"))?;
        file.write_all(bytes)
            .map_err(|e| io_err(e, "write edge temp file"))?;
        file.sync_all()
            .map_err(|e| io_err(e, "sync edge temp file"))?;
    }
    renameat(CWD, &tmp, CWD, path).map_err(|e| {
        AppError::InternalServerError(format!("publish edge file {}: {e}", path.display()))
    })?;
    sync_dir(parent);
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<bool, AppError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(io_err(err, "remove edge file")),
    }
}

fn write_metadata_to_roots(metadata: &ObjectMetadata) -> Result<(), AppError> {
    ensure_fragment_dirs(&metadata.fragment)?;
    let bytes = encode(metadata)?;
    let file_name = ndx_file_name(metadata);
    for root in index_roots() {
        let object_dir = object_index_dir(&root, &metadata.fragment, &metadata.object_key)?;
        write_atomic(&object_dir.join(&file_name), &bytes)?;
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
                    let object_dir = object_index_dir(&index_root, fragment, &object_id)?;
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

pub fn upsert_live_version_with_replica_mode(
    metadata: &ObjectMetadata,
    _mode: IndexReplicaWriteMode,
) -> Result<(), AppError> {
    write_metadata_to_roots(metadata)
}

pub fn read_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let object_key = normalize_edge_key(object_key)?;
    for dir in object_dir_candidates(fragment, &object_key)? {
        if let Some(path) = ndx_path_for_version(&dir, version_id)? {
            return decode_metadata_file_as(&path, &object_key).map(Some);
        }
    }
    Ok(None)
}

pub fn read_current_live_head(
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let object_key = normalize_edge_key(object_key)?;
    for dir in object_dir_candidates(fragment, &object_key)? {
        let Some(summary) = latest_summary_in_object_dir(&dir)? else {
            continue;
        };
        return decode_metadata_file_as(&dir.join(summary.file_name), &object_key).map(Some);
    }
    Ok(None)
}

pub fn list_live_versions(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let object_key = normalize_edge_key(object_key)?;
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
        .map(|prefix| normalize_edge_key(&prefix))
        .transpose()?
        .unwrap_or_default();
    let limit = limit.max(1);
    let mut by_record = BTreeMap::<String, (EdgeListEntry, Option<ObjectMetadata>)>::new();
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
                let object_dir = object_index_dir(&index_root, fragment, &object_id)?;
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
                .map(|entry: &EdgeListEntry| entry.record.clone());
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
        if let Some(path) = ndx_path_for_version(&dir, version_id)? {
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
            if parse_ndx_file_name(&name).is_some() {
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

pub(crate) fn scan_trash_records(fragment: &str) -> Result<Vec<EdgeTrashRecord>, AppError> {
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
                    let object_dir = object_index_dir(&index_root, fragment, &object_id)?;
                    for ndx_name in read_dir_names(&object_dir)? {
                        if parse_ndx_file_name(&ndx_name).is_none() {
                            continue;
                        }
                        let meta = decode_metadata_file(&object_dir.join(&ndx_name))?;
                        deleted_at = meta.created_at;
                        object_keys.insert(object_id.clone());
                        version_ids.insert(meta.version_id);
                    }
                    if seen.insert(child_rel.clone()) {
                        out.push(EdgeTrashRecord {
                            trash_entry_id: child_rel.clone(),
                            original_path: child_rel.clone(),
                            source_relative_path: child_rel,
                            kind: EdgeTrashKind::File,
                            object_keys: object_keys.into_iter().collect(),
                            version_ids: version_ids.into_iter().collect(),
                            deleted_at_unix: deleted_at,
                        });
                    }
                } else if child.is_dir() {
                    let (object_keys, version_ids, deleted_at) = collect_trash_refs_under(&child)?;
                    if !object_keys.is_empty() && seen.insert(child_rel.clone()) {
                        out.push(EdgeTrashRecord {
                            trash_entry_id: child_rel.clone(),
                            original_path: child_rel.clone(),
                            source_relative_path: child_rel.clone(),
                            kind: EdgeTrashKind::Directory,
                            object_keys,
                            version_ids,
                            deleted_at_unix: deleted_at,
                        });
                    }
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

fn collect_trash_refs_under(dir: &Path) -> Result<(Vec<String>, Vec<String>, i64), AppError> {
    let mut object_keys = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    let mut deleted_at = Utc::now().timestamp();
    let mut stack = vec![(dir.to_path_buf(), String::new())];
    let fragment_root = dir
        .ancestors()
        .find(|ancestor| ancestor.file_name().and_then(|name| name.to_str()) == Some(TRASH_DIR))
        .and_then(Path::parent)
        .map(Path::to_path_buf);
    let index_root = fragment_root
        .as_ref()
        .and_then(|root| root.parent())
        .map(Path::to_path_buf);
    let fragment = fragment_root
        .as_ref()
        .and_then(|root| root.file_name())
        .and_then(|name| name.to_str())
        .map(str::to_string);
    while let Some((current, rel)) = stack.pop() {
        if !current.is_dir() {
            continue;
        }
        for name in read_dir_names(&current)? {
            if internal_namespace_entry_name(&name) && name != OBJECTS_DIR {
                continue;
            }
            let child = current.join(&name);
            let child_rel = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };
            if let (Some(root), Some(fragment)) = (index_root.as_ref(), fragment.as_deref()) {
                if let Some(object_id) = read_namespace_symlink_object_id(root, fragment, &child)? {
                    let object_dir = object_index_dir(root, fragment, &object_id)?;
                    for ndx_name in read_dir_names(&object_dir)? {
                        if parse_ndx_file_name(&ndx_name).is_none() {
                            continue;
                        }
                        let meta = decode_metadata_file(&object_dir.join(&ndx_name))?;
                        deleted_at = meta.created_at;
                        object_keys.insert(object_id.clone());
                        version_ids.insert(meta.version_id);
                    }
                    continue;
                }
            }
            if child.is_dir() {
                stack.push((child, child_rel));
            }
        }
    }
    Ok((
        object_keys.into_iter().collect(),
        version_ids.into_iter().collect(),
        deleted_at,
    ))
}

pub fn list_trash_catalog(fragment: &str) -> Result<Vec<TrashCatalogEntry>, AppError> {
    Ok(scan_trash_records(fragment)?
        .into_iter()
        .map(|record| TrashCatalogEntry {
            object_key: record.original_path,
            version_ids: record.version_ids,
            deleted_at: record.deleted_at_unix,
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
        .map(|prefix| normalize_edge_key(&prefix))
        .transpose()?
        .unwrap_or_default();
    let limit = limit.max(1);
    let mut by_record = BTreeMap::<String, (EdgeListEntry, Option<ObjectMetadata>)>::new();

    for record in scan_trash_records(fragment)? {
        let relative = if prefix.is_empty() {
            record.original_path.as_str()
        } else if record.original_path == prefix {
            if record.kind == EdgeTrashKind::Directory {
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

        if is_nested || record.kind == EdgeTrashKind::Directory {
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
                fragment: fragment.to_string(),
                version_id,
                upload_session_id: String::new(),
                owner_uid: fragment.to_string(),
                content_type: "application/octet-stream".to_string(),
                total_size_bytes: 0,
                created_at: record.deleted_at_unix,
                parts: Vec::new(),
                is_delete_marker: false,
                stripe_size_bytes: super::disk_manager::default_stripe_size(),
            }
        };
        metadata.object_key = record.original_path.clone();
        let entry = EdgeListEntry {
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

pub(crate) fn delete_unreferenced_object_index(
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
            fs::remove_dir_all(&dir).map_err(|e| io_err(e, "delete edge object index dir"))?;
            removed = true;
            if let Some(parent) = dir.parent() {
                sync_dir(parent);
            }
        }
    }
    Ok(removed)
}

pub(crate) fn cleanup_unreferenced_object_indexes(fragment: &str) -> Result<usize, AppError> {
    let referenced = referenced_object_ids(fragment)?;
    let mut removed = 0usize;
    let mut seen = HashSet::new();
    for fragment_root in existing_fragment_roots(fragment) {
        let objects_root = fragment_root.join(OBJECTS_DIR);
        if !objects_root.is_dir() {
            continue;
        }
        for name in read_dir_names(&objects_root)? {
            let Some(object_id) = object_id_from_dir_name(&name) else {
                continue;
            };
            if referenced.contains(&object_id) || !seen.insert(object_id.clone()) {
                continue;
            }
            if delete_unreferenced_object_index(fragment, &object_id)? {
                removed += 1;
            }
        }
    }
    Ok(removed)
}

pub fn list_active_trash_partition_ids(_fragment: &str) -> Result<BTreeSet<u16>, AppError> {
    Ok(BTreeSet::from([0]))
}

pub fn list_partition_trash_entries(
    fragment: &str,
    _partition_id: u16,
) -> Result<HashMap<String, Vec<String>>, AppError> {
    Ok(list_trash_catalog(fragment)?
        .into_iter()
        .map(|entry| (entry.object_key, entry.version_ids))
        .collect())
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
        delete_unreferenced_object_index(fragment, &object_id)?;
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
    delete_unreferenced_object_index(fragment, object_id)?;
    Ok((!ids.is_empty()).then_some(ids))
}

pub fn delete_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let mut removed = false;
    for dir in object_dir_candidates(fragment, object_key)? {
        if let Some(path) = ndx_path_for_version(&dir, version_id)? {
            removed |= remove_file_if_exists(&path)?;
        }
    }
    Ok(removed)
}

pub fn delete_trash_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let mut removed = false;
    for dir in object_dir_candidates(fragment, object_key)? {
        if let Some(path) = ndx_path_for_version(&dir, version_id)? {
            removed |= remove_file_if_exists(&path)?;
        }
    }
    Ok(removed)
}

pub fn clear_fragment_trash(fragment: &str, object_key: Option<&str>) -> Result<(), AppError> {
    if let Some(key) = object_key {
        let _ = delete_unreferenced_object_index(fragment, key)?;
        return Ok(());
    }
    for root in index_roots() {
        let path = root.join(fragment).join(TRASH_DIR);
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|e| io_err(e, "clear edge trash object dir"))?;
        }
        fs::create_dir_all(&path).map_err(|e| io_err(e, "recreate edge trash object dir"))?;
    }
    let _ = cleanup_unreferenced_object_indexes(fragment)?;
    Ok(())
}

pub fn drop_fragment_trash_namespace(fragment: &str) -> Result<(), AppError> {
    for root in index_roots() {
        let fragment_root = root.join(fragment);
        let path = fragment_root.join(TRASH_DIR);
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|e| io_err(e, "drop edge trash namespace"))?;
        }
        fs::create_dir_all(&path).map_err(|e| io_err(e, "recreate edge trash namespace"))?;
        sync_dir(&fragment_root);
    }
    Ok(())
}

pub fn clear_fragment_trash_for_partitions(
    fragment: &str,
    _touched_partitions: &BTreeSet<u16>,
) -> Result<usize, AppError> {
    clear_fragment_trash(fragment, None)?;
    Ok(1)
}

pub fn clear_fragment_trash_partition(
    fragment: &str,
    _partition_id: u16,
) -> Result<usize, AppError> {
    clear_fragment_trash(fragment, None)?;
    Ok(1)
}

pub fn object_partition_id(_fragment: &str, object_key: &str) -> Result<u16, AppError> {
    let hash = object_key_hash(object_key)?;
    Ok(u16::from_str_radix(&hash[..4], 16).unwrap_or(0))
}

pub fn has_object_edge_index_record(fragment: &str, object_key: &str) -> Result<bool, AppError> {
    Ok(object_dir_candidates(fragment, object_key)?
        .into_iter()
        .any(|path| path.is_dir()))
}

pub fn ensure_object_owner_partition_available(
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    let root = first_fragment_root(fragment)?;
    fs::create_dir_all(live_object_dir(
        root.parent().unwrap_or(&root),
        fragment,
        object_key,
    )?)
    .map_err(|e| io_err(e, "ensure edge object dir"))
}

pub fn ensure_fragment_owner_partition_available(fragment: &str) -> Result<(), AppError> {
    ensure_fragment_dirs(fragment)
}

pub fn ensure_fragment_owner_partition_available_strict(fragment: &str) -> Result<(), AppError> {
    ensure_fragment_dirs(fragment)
}

pub fn has_any_fragment_edge_index(fragment: &str) -> Result<bool, AppError> {
    Ok(existing_fragment_roots(fragment).into_iter().any(|root| {
        root.join(FRAGMENT_META_FILE).exists()
            || root.join(UPLOADS_DIR).exists()
            || root
                .join(OBJECTS_DIR)
                .read_dir()
                .map(|mut entries| entries.next().is_some())
                .unwrap_or(false)
            || object_dirs_for_all_live(fragment)
                .map(|v| !v.is_empty())
                .unwrap_or(false)
    }))
}

pub fn fragment_has_object_index_records(fragment: &str, roots: &[PathBuf]) -> bool {
    for root in roots {
        let objects_root = root.join(fragment).join(OBJECTS_DIR);
        let Ok(object_dirs) = fs::read_dir(&objects_root) else {
            continue;
        };
        for object_dir in object_dirs.filter_map(|entry| entry.ok()) {
            let Ok(file_type) = object_dir.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let file_name = object_dir.file_name();
            if !file_name
                .to_str()
                .is_some_and(|name| name.starts_with(OBJECT_DIR_PREFIX))
            {
                continue;
            }
            let Ok(records) = fs::read_dir(object_dir.path()) else {
                continue;
            };
            for record in records.filter_map(|entry| entry.ok()) {
                let Some(name) = record.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                if parse_ndx_file_name(&name).is_some() {
                    return true;
                }
            }
        }
    }
    false
}

fn default_project_id(fragment: &str) -> u32 {
    let mut hasher = Sha256::new();
    hasher.update(fragment.as_bytes());
    let hash: [u8; 32] = hasher.finalize().into();
    let span = QUOTA_PROJECT_ID_CEILING - QUOTA_PROJECT_ID_FLOOR;
    QUOTA_PROJECT_ID_FLOOR + (u32::from_be_bytes([hash[0], hash[1], hash[2], hash[3]]) % span)
}

fn read_fragment_meta_from(root: &Path, fragment: &str) -> Result<Option<FragmentMeta>, AppError> {
    let path = root.join(fragment).join(FRAGMENT_META_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|e| io_err(e, "read edge fragment metadata"))?;
    decode(&bytes).map(Some)
}

fn write_fragment_meta(fragment: &str, meta: &FragmentMeta) -> Result<(), AppError> {
    let bytes = encode(meta)?;
    for root in index_roots() {
        write_atomic(&root.join(fragment).join(FRAGMENT_META_FILE), &bytes)?;
    }
    Ok(())
}

pub fn fragment_quota_meta(fragment: &str) -> Result<Option<FragmentMeta>, AppError> {
    for root in index_roots() {
        if let Some(meta) = read_fragment_meta_from(&root, fragment)? {
            return Ok(Some(meta));
        }
    }
    Ok(None)
}

pub fn set_fragment_quota(fragment: &str, quota_bytes: u64) -> Result<(), AppError> {
    ensure_fragment_dirs(fragment)?;
    let now = Utc::now().timestamp();
    let mut meta = fragment_quota_meta(fragment)?.unwrap_or(FragmentMeta {
        fragment_id: fragment.to_string(),
        state: "active".to_string(),
        quota_bytes,
        quota_project_id: Some(default_project_id(fragment)),
        committed_bytes: 0,
        reserved_bytes: 0,
        trash_bytes: 0,
        created_at_unix: now,
        updated_at_unix: now,
        key_phc: None,
        nt_hash_hex: None,
        index_partition_count: None,
    });
    meta.quota_bytes = quota_bytes;
    meta.updated_at_unix = now;
    if meta.quota_project_id.is_none() {
        meta.quota_project_id = Some(default_project_id(fragment));
    }
    write_fragment_meta(fragment, &meta)
}

pub fn ensure_fragment_quota_project_id(fragment: &str) -> Result<u32, AppError> {
    let project_id = fragment_quota_meta(fragment)?
        .and_then(|meta| meta.quota_project_id)
        .unwrap_or_else(|| default_project_id(fragment));
    let quota = fragment_quota_meta(fragment)?
        .map(|meta| meta.quota_bytes)
        .unwrap_or(0);
    let _ = set_fragment_quota(fragment, quota);
    Ok(project_id)
}

pub fn active_fragment_quota_specs() -> Result<Vec<FragmentQuotaSpec>, AppError> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for root in index_roots() {
        if !root.is_dir() {
            continue;
        }
        for name in read_dir_names(&root).unwrap_or_default() {
            if !seen.insert(name.clone()) {
                continue;
            }
            if let Some(meta) = fragment_quota_meta(&name)? {
                if meta.state == "active" && meta.quota_bytes > 0 {
                    out.push(FragmentQuotaSpec {
                        fragment: name,
                        quota_bytes: meta.quota_bytes,
                        quota_project_id: meta
                            .quota_project_id
                            .unwrap_or_else(|| default_project_id(&meta.fragment_id)),
                    });
                }
            }
        }
    }
    Ok(out)
}

pub fn active_fragment_quota_bytes() -> Result<u64, AppError> {
    Ok(active_fragment_quota_specs()?
        .into_iter()
        .map(|spec| spec.quota_bytes)
        .sum())
}

pub fn sync_fragment_partition_replicas(fragment: &str) -> Result<(), AppError> {
    let _guard = EDGE_FS_LOCK
        .lock()
        .map_err(|_| AppError::InternalServerError("edge index sync lock poisoned".to_string()))?;
    let roots = fragment_roots(fragment);
    let Some(source) = roots.iter().find(|root| root.exists()).cloned() else {
        return Ok(());
    };
    for dst in roots {
        if dst == source {
            continue;
        }
        copy_dir_contents(&source, &dst)?;
    }
    Ok(())
}

pub fn repair_fragment_partition_replicas(
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    repair_fragment_all_partition_replicas(fragment, dry_run)
}

pub fn repair_fragment_all_partition_replicas(
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let roots = fragment_roots(fragment);
    let Some(source) = roots.iter().find(|root| root.exists()).cloned() else {
        return Ok((0, 0, 0));
    };
    let mut missing = 0;
    for dst in roots {
        if dst == source {
            continue;
        }
        if !dst.exists() {
            missing += 1;
        }
        if !dry_run {
            copy_dir_contents(&source, &dst)?;
        }
    }
    Ok((missing, 0, 0))
}

fn copy_dir_contents(src: &Path, dst: &Path) -> Result<(), AppError> {
    fs::create_dir_all(dst).map_err(|e| io_err(e, "create edge replica dir"))?;
    for entry in fs::read_dir(src).map_err(|e| io_err(e, "scan edge replica source"))? {
        let entry = entry.map_err(|e| io_err(e, "scan edge replica entry"))?;
        let source = entry.path();
        let target = dst.join(entry.file_name());
        let meta =
            fs::symlink_metadata(&source).map_err(|e| io_err(e, "inspect edge replica source"))?;
        if meta.file_type().is_symlink() {
            if fs::symlink_metadata(&target).is_ok() {
                let _ = fs::remove_file(&target);
            }
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|e| io_err(e, "create edge replica parent"))?;
            }
            let link_target =
                fs::read_link(&source).map_err(|e| io_err(e, "read edge replica symlink"))?;
            unix_fs::symlink(link_target, &target)
                .map_err(|e| io_err(e, "copy edge replica symlink"))?;
        } else if meta.is_dir() {
            copy_dir_contents(&source, &target)?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|e| io_err(e, "create edge replica parent"))?;
            }
            fs::copy(&source, &target).map_err(|e| io_err(e, "copy edge replica file"))?;
        }
    }
    Ok(())
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
    for root in index_roots() {
        write_atomic(
            &root
                .join(&manifest.fragment)
                .join(UPLOADS_DIR)
                .join(&manifest.version_id)
                .join(MANIFEST_FILE),
            &bytes,
        )?;
    }
    Ok(())
}

fn session_dir(root: &Path, fragment: &str, version_id: &str) -> PathBuf {
    root.join(fragment).join(UPLOADS_DIR).join(version_id)
}

fn session_roots(fragment: &str, version_id: &str) -> Vec<PathBuf> {
    index_roots()
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
            &dir.join(format!("{}{:020}.ndx", PART_META_PREFIX, part.part_index)),
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
            write_atomic(
                &dir.join(part_receipt_name(receipt.part_index, checksum)),
                b"",
            )?;
        }
        for (shard_index, checksum) in receipt.shard_checksums.iter().copied().enumerate() {
            write_atomic(
                &dir.join(shard_receipt_name(
                    receipt.part_index,
                    shard_index,
                    checksum,
                )),
                b"",
            )?;
        }
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
        let mut shard_receipts = BTreeMap::<u64, BTreeMap<usize, u32>>::new();
        let mut part_checksums = BTreeMap::<u64, u32>::new();
        for name in read_dir_names(&dir)? {
            let path = dir.join(&name);
            if name.starts_with(PART_META_PREFIX) && name.ends_with(".ndx") {
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
    use crate::core::disk_manager::{INDEX_STORE_DISKS, ObjectMetadata};
    use ulid::Ulid;

    fn ensure_test_index_roots() -> Vec<PathBuf> {
        if let Some(existing) = INDEX_STORE_DISKS.get() {
            let roots = existing.iter().map(PathBuf::from).collect::<Vec<_>>();
            for root in &roots {
                let _ = fs::create_dir_all(root);
            }
            return roots;
        }

        let base = std::env::temp_dir().join(format!("n1-edge-index-{}", std::process::id()));
        let roots = vec![base.join("index-0"), base.join("index-1")];
        for root in &roots {
            fs::create_dir_all(root).expect("create test index root");
        }
        let _ = INDEX_STORE_DISKS.set(
            roots
                .iter()
                .map(|root| root.to_string_lossy().to_string())
                .collect(),
        );
        roots
    }

    fn test_metadata(fragment: &str, object_key: &str, size: u64) -> ObjectMetadata {
        ObjectMetadata {
            object_key: object_key.to_string(),
            fragment: fragment.to_string(),
            version_id: Ulid::new().to_string(),
            upload_session_id: "test-session".to_string(),
            owner_uid: "test-owner".to_string(),
            content_type: "application/octet-stream".to_string(),
            total_size_bytes: size,
            created_at: 1_777_905_000,
            parts: Vec::new(),
            is_delete_marker: false,
            stripe_size_bytes: crate::core::disk_manager::default_stripe_size(),
        }
    }

    fn publish_logical_object(
        roots: &[PathBuf],
        fragment: &str,
        logical_path: &str,
        size: u64,
    ) -> String {
        let object_id = new_object_id();
        upsert_live_version(&test_metadata(fragment, &object_id, size)).expect("publish test ndx");
        for root in roots {
            publish_namespace_link(root, fragment, logical_path, &object_id)
                .expect("publish namespace link");
        }
        object_id
    }

    #[test]
    fn packed_extension_uses_length_prefixed_lowercase_bytes() {
        let packed = packed_extension_key("docs/Clip.PDF");
        let expected = 3u64 | (b'p' as u64) << 8 | (b'd' as u64) << 16 | (b'f' as u64) << 24;
        assert_eq!(packed, expected);
        assert_eq!(packed_extension_key("docs/archive.longexts"), 0);
    }

    #[test]
    fn reserved_user_path_segments_are_rejected() {
        assert!(normalize_edge_key("docs/@o_bad/file.txt").is_err());
        assert!(normalize_edge_key(".trash/video.mp4").is_err());
        assert!(normalize_edge_key(".blobs/video.mp4").is_err());
        assert!(normalize_edge_key("docs/.repair/file.txt").is_err());
        assert_eq!(
            normalize_edge_key("docs/video.mp4").unwrap(),
            "docs/video.mp4"
        );
        assert_eq!(
            normalize_edge_key("blob/video.mp4").unwrap(),
            "blob/video.mp4"
        );
    }

    #[test]
    fn edge_object_id_validation_rejects_posix_file_names() {
        assert!(normalize_object_id("INCDT 122225.mp4").is_err());
        assert!(normalize_object_id("videos/clip.mp4").is_err());
        assert!(normalize_object_id("@o_01KQXRVALIDEDGEID0000000").is_ok());
    }

    #[test]
    fn live_head_listing_reads_filenames_without_decoding_ndx_body() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let roots = ensure_test_index_roots();
        let fragment = format!("edge-list-{}", Ulid::new());
        let object_id = new_object_id();
        let logical_path = "docs/bad-body.mp4";
        let file_name = format!(
            "{}.{}.{}.{}.ndx",
            Ulid::new(),
            12345,
            1_777_905_000_000_000_000i128,
            packed_extension_key(logical_path)
        );
        for root in &roots {
            let object_dir = live_object_dir(root, &fragment, &object_id).expect("object dir");
            fs::create_dir_all(&object_dir).expect("create object dir");
            fs::write(object_dir.join(&file_name), b"not-valid-msgpack").expect("write bad ndx");
            publish_namespace_link(root, &fragment, logical_path, &object_id)
                .expect("publish namespace link");
        }

        let listed =
            scan_live_heads_page(&fragment, Some("docs".to_string()), None, 10, None, true)
                .expect("zero-decode list");
        assert_eq!(listed.entries.len(), 1);
        assert_eq!(listed.objects.len(), 1);
        assert_eq!(listed.objects[0].total_size_bytes, 12345);
        assert_eq!(listed.objects[0].object_key, logical_path);
        assert!(read_current_live_head(&fragment, &object_id).is_err());
    }

    #[test]
    fn live_head_pagination_uses_display_names_not_raw_object_dirs() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let roots = ensure_test_index_roots();
        let fragment = format!("edge-page-{}", Ulid::new());
        for (name, size) in [
            ("docs/a.svg", 10u64),
            ("docs/b.svg", 20u64),
            ("docs/c.svg", 30u64),
        ] {
            publish_logical_object(&roots, &fragment, name, size);
        }

        let first = scan_live_heads_page(&fragment, Some("docs".to_string()), None, 2, None, true)
            .expect("first page");
        assert_eq!(
            first
                .objects
                .iter()
                .map(|meta| meta.object_key.as_str())
                .collect::<Vec<_>>(),
            vec!["docs/a.svg", "docs/b.svg"]
        );
        let second = scan_live_heads_page(
            &fragment,
            Some("docs".to_string()),
            None,
            2,
            first.continuation_token,
            true,
        )
        .expect("second page");
        assert_eq!(
            second
                .objects
                .iter()
                .map(|meta| meta.object_key.as_str())
                .collect::<Vec<_>>(),
            vec!["docs/c.svg"]
        );
    }

    #[test]
    fn full_metadata_scan_uses_current_filesystem_path_after_rename() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let roots = ensure_test_index_roots();
        let fragment = format!("edge-full-meta-{}", Ulid::new());
        let old_key = "docs/old-name.txt";
        let new_key = "docs/new-name.txt";
        let object_id = publish_logical_object(&roots, &fragment, old_key, 42);
        for root in &roots {
            let old_link = root.join(&fragment).join(old_key);
            let new_link = root.join(&fragment).join(new_key);
            fs::rename(&old_link, &new_link).expect("simulate edge namespace rename");
            assert_eq!(
                read_namespace_symlink_object_id(root, &fragment, &new_link)
                    .expect("read moved link"),
                Some(object_id.clone())
            );
        }

        let page =
            scan_live_full_metadata_page(&fragment, Some("docs".to_string()), None, 10, None, true)
                .expect("full metadata page");
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].object_key, object_id);
    }

    #[test]
    fn full_metadata_pagination_advances_by_namespace_path() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let roots = ensure_test_index_roots();
        let fragment = format!("edge-full-page-{}", Ulid::new());
        let first_id = publish_logical_object(&roots, &fragment, "docs/a.txt", 10);
        let second_id = publish_logical_object(&roots, &fragment, "docs/b.txt", 20);
        let third_id = publish_logical_object(&roots, &fragment, "docs/c.txt", 30);

        let first =
            scan_live_full_metadata_page(&fragment, Some("docs".to_string()), None, 2, None, true)
                .expect("first full metadata page");
        assert_eq!(
            first
                .objects
                .iter()
                .map(|meta| meta.object_key.as_str())
                .collect::<Vec<_>>(),
            vec![first_id.as_str(), second_id.as_str()]
        );
        let token = first
            .continuation_token
            .clone()
            .expect("first page continuation");
        assert_eq!(token, "docs/b.txt");

        let second = scan_live_full_metadata_page(
            &fragment,
            Some("docs".to_string()),
            None,
            2,
            first.continuation_token,
            true,
        )
        .expect("second full metadata page");
        assert_eq!(
            second
                .objects
                .iter()
                .map(|meta| meta.object_key.as_str())
                .collect::<Vec<_>>(),
            vec![third_id.as_str()]
        );
        assert!(second.continuation_token.is_none());
    }

    #[test]
    fn multipart_receipts_are_zero_decode_filename_records() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let roots = ensure_test_index_roots();
        let fragment = format!("edge-upload-{}", Ulid::new());
        let version_id = Ulid::new().to_string();
        let manifest = UploadSessionManifest {
            fragment: fragment.clone(),
            object_key: "uploads/video.bin".to_string(),
            version_id: version_id.clone(),
            session_id: Ulid::new().to_string(),
            total_size_bytes: 8 * 1024,
            expected_parts: 8,
            part_size_bytes: 1024,
            content_type: "application/octet-stream".to_string(),
            created_at: 1_777_905_000,
            stripe_size_bytes: 512,
            data_shards: 2,
            parity_shards: 1,
        };
        open_session(&manifest, None).expect("open session");
        let receipt = SessionPartReceipt {
            part_index: 7,
            size_bytes: 1024,
            shard_size_bytes: 512,
            created_at: 1_777_905_001,
            checksum: Some(0x0102_0304),
            shard_checksums: vec![0x1111_1111, 0x2222_2222, 0x3333_3333],
        };
        commit_session_part_receipt(&fragment, &version_id, &receipt).expect("write receipts");

        let dir = roots[0].join(&fragment).join(UPLOADS_DIR).join(&version_id);
        let names = read_dir_names(&dir).expect("receipt names");
        assert!(names.contains(&"part_p00000000000000000007.01020304.receipt".to_string()));
        assert!(names.contains(&"shard_p00000000000000000007_s0000.11111111.receipt".to_string()));
        assert!(names.contains(&"shard_p00000000000000000007_s0001.22222222.receipt".to_string()));
        assert!(names.contains(&"shard_p00000000000000000007_s0002.33333333.receipt".to_string()));

        for name in names.iter().filter(|name| name.ends_with(RECEIPT_SUFFIX)) {
            fs::write(dir.join(name), b"not-msgpack").expect("receipt body is not authority");
        }

        let loaded = load_session_part_receipts(&fragment, &version_id, &manifest)
            .expect("load receipts")
            .expect("session exists");
        let loaded_receipt = loaded[7].as_ref().expect("part receipt");
        assert_eq!(loaded_receipt.part_index, 7);
        assert_eq!(loaded_receipt.size_bytes, 1024);
        assert_eq!(loaded_receipt.shard_size_bytes, 512);
        assert_eq!(loaded_receipt.created_at, manifest.created_at);
        assert_eq!(loaded_receipt.checksum, Some(0x0102_0304));
        assert_eq!(
            loaded_receipt.shard_checksums,
            vec![0x1111_1111, 0x2222_2222, 0x3333_3333]
        );
    }
}
