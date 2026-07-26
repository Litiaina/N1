use crate::core::checksum::{Sha256Digest, format_sha256, sha256_digest};
use crate::core::fragment_identity::physical_fragment_root_on;
use crate::enums::app_error_enum::AppError;
use crate::namespace::paths::{base_name, normalize_path, parent_path};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use ulid::Ulid;

pub const NCNX_FORMAT_VERSION: u32 = 4;
pub const NAMESPACE_DIR: &str = ".namespace";
pub const NAMESPACE_TRASH_DIR: &str = ".trash";
pub const NCNX_SUFFIX: &str = ".ncnx";
const MAX_KEY_BYTES: usize = 4_096;
const MAX_COMPONENT_BYTES: usize = 250;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NcnxRecord {
    pub format_version: u32,
    pub fragment_id: String,
    pub object_id: String,
    pub mutation_id: String,
    pub created_at_unix_nanos: i64,
    pub payload_sha256: Sha256Digest,
}

impl NcnxRecord {
    pub fn new(
        fragment_id: impl Into<String>,
        object_id: impl Into<String>,
        mutation_id: impl Into<String>,
        created_at_unix_nanos: i64,
    ) -> Result<Self, AppError> {
        let mut record = Self {
            format_version: NCNX_FORMAT_VERSION,
            fragment_id: fragment_id.into(),
            object_id: object_id.into(),
            mutation_id: mutation_id.into(),
            created_at_unix_nanos,
            payload_sha256: [0; 32],
        };
        record.validate_fields()?;
        record.payload_sha256 = record.expected_sha256()?;
        Ok(record)
    }

    fn validate_fields(&self) -> Result<(), AppError> {
        if self.format_version != NCNX_FORMAT_VERSION {
            return Err(AppError::Conflict(format!(
                "NCNX format {} is not N2 3.2 format {}",
                self.format_version, NCNX_FORMAT_VERSION
            )));
        }
        if self.fragment_id.trim().is_empty()
            || self.object_id.trim().is_empty()
            || self.mutation_id.trim().is_empty()
            || self.created_at_unix_nanos <= 0
        {
            return Err(AppError::Conflict(
                "NCNX requires fragment, object, mutation, and creation identities".into(),
            ));
        }
        Ok(())
    }

    fn expected_sha256(&self) -> Result<Sha256Digest, AppError> {
        let mut unsigned = self.clone();
        unsigned.payload_sha256 = [0; 32];
        let payload = rmp_serde::to_vec_named(&unsigned).map_err(|err| {
            AppError::InternalServerError(format!("encode NCNX integrity payload: {err}"))
        })?;
        Ok(sha256_digest(&payload))
    }

    pub fn validate(&self) -> Result<(), AppError> {
        self.validate_fields()?;
        let expected = self.expected_sha256()?;
        if expected != self.payload_sha256 {
            return Err(AppError::Conflict(format!(
                "NCNX integrity mismatch: expected {}, recorded {}",
                format_sha256(&expected),
                format_sha256(&self.payload_sha256)
            )));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, AppError> {
        self.validate()?;
        rmp_serde::to_vec_named(self)
            .map_err(|err| AppError::InternalServerError(format!("encode NCNX: {err}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AppError> {
        let record: Self = rmp_serde::from_slice(bytes)
            .map_err(|err| AppError::Conflict(format!("decode NCNX: {err}")))?;
        record.validate()?;
        Ok(record)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrashLocator {
    pub logical_key: String,
    pub deleted_at_unix_nanos: i64,
    pub delete_mutation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NcnxListItem {
    pub logical_key: String,
    pub physical_path: PathBuf,
    pub trash: Option<TrashLocator>,
}

#[derive(Clone, Debug)]
pub struct NcnxStore {
    root_id: String,
    storage_root: PathBuf,
}

impl NcnxStore {
    pub fn new(root_id: impl Into<String>, storage_root: impl Into<PathBuf>) -> Self {
        Self {
            root_id: root_id.into(),
            storage_root: storage_root.into(),
        }
    }

    pub fn root_id(&self) -> &str {
        &self.root_id
    }

    pub fn namespace_root(&self, fragment_id: &str) -> PathBuf {
        physical_fragment_root_on(&self.storage_root, fragment_id).join(NAMESPACE_DIR)
    }

    pub fn trash_root(&self, fragment_id: &str) -> PathBuf {
        physical_fragment_root_on(&self.storage_root, fragment_id).join(NAMESPACE_TRASH_DIR)
    }

    pub fn live_path(&self, fragment_id: &str, logical_key: &str) -> Result<PathBuf, AppError> {
        let key = validate_logical_key(logical_key)?;
        let parent = parent_path(&key);
        let name = base_name(&key);
        Ok(self
            .namespace_root(fragment_id)
            .join(parent)
            .join(format!("{name}{NCNX_SUFFIX}")))
    }

    pub fn trash_path(
        &self,
        fragment_id: &str,
        logical_key: &str,
        deleted_at_unix_nanos: i64,
        delete_mutation_id: &str,
    ) -> Result<PathBuf, AppError> {
        let key = validate_logical_key(logical_key)?;
        let parent = parent_path(&key);
        let name = base_name(&key);
        let generation = trash_generation_name(deleted_at_unix_nanos, delete_mutation_id)?;
        Ok(self
            .trash_root(fragment_id)
            .join(generation)
            .join(parent)
            .join(format!("{name}{NCNX_SUFFIX}")))
    }

    pub fn read_live(
        &self,
        fragment_id: &str,
        logical_key: &str,
    ) -> Result<Option<(NcnxRecord, Vec<u8>)>, AppError> {
        read_record_if_present(&self.live_path(fragment_id, logical_key)?)
    }

    pub fn read_trash(
        &self,
        fragment_id: &str,
        locator: &TrashLocator,
    ) -> Result<Option<(NcnxRecord, Vec<u8>)>, AppError> {
        read_record_if_present(&self.trash_path(
            fragment_id,
            &locator.logical_key,
            locator.deleted_at_unix_nanos,
            &locator.delete_mutation_id,
        )?)
    }

    /// Immutable create. Existing identical bytes make retry idempotent;
    /// existing different bytes are a same-key conflict.
    pub fn create_live(
        &self,
        fragment_id: &str,
        logical_key: &str,
        bytes: &[u8],
    ) -> Result<bool, AppError> {
        let record = NcnxRecord::decode(bytes)?;
        if record.fragment_id != fragment_id {
            return Err(AppError::Conflict(
                "NCNX fragment identity differs from publication target".into(),
            ));
        }
        create_immutable(&self.live_path(fragment_id, logical_key)?, bytes)
    }

    pub fn create_trash(
        &self,
        fragment_id: &str,
        locator: &TrashLocator,
        bytes: &[u8],
    ) -> Result<bool, AppError> {
        let record = NcnxRecord::decode(bytes)?;
        if record.fragment_id != fragment_id {
            return Err(AppError::Conflict(
                "NCNX fragment identity differs from trash target".into(),
            ));
        }
        create_immutable(
            &self.trash_path(
                fragment_id,
                &locator.logical_key,
                locator.deleted_at_unix_nanos,
                &locator.delete_mutation_id,
            )?,
            bytes,
        )
    }

    pub fn remove_live_if_exact(
        &self,
        fragment_id: &str,
        logical_key: &str,
        expected_bytes: &[u8],
    ) -> Result<bool, AppError> {
        remove_if_exact(&self.live_path(fragment_id, logical_key)?, expected_bytes)
    }

    pub fn remove_trash_if_exact(
        &self,
        fragment_id: &str,
        locator: &TrashLocator,
        expected_bytes: &[u8],
    ) -> Result<bool, AppError> {
        let trash_root = self.trash_root(fragment_id);
        let path = self.trash_path(
            fragment_id,
            &locator.logical_key,
            locator.deleted_at_unix_nanos,
            &locator.delete_mutation_id,
        )?;
        let removed = remove_if_exact(&path, expected_bytes)?;
        if removed {
            prune_empty_trash_parents(&path, &trash_root)?;
        }
        Ok(removed)
    }

    pub fn repair_live(
        &self,
        fragment_id: &str,
        logical_key: &str,
        bytes: &[u8],
    ) -> Result<bool, AppError> {
        NcnxRecord::decode(bytes)?;
        replace_durable(&self.live_path(fragment_id, logical_key)?, bytes)
    }

    pub fn repair_trash(
        &self,
        fragment_id: &str,
        locator: &TrashLocator,
        bytes: &[u8],
    ) -> Result<bool, AppError> {
        NcnxRecord::decode(bytes)?;
        replace_durable(
            &self.trash_path(
                fragment_id,
                &locator.logical_key,
                locator.deleted_at_unix_nanos,
                &locator.delete_mutation_id,
            )?,
            bytes,
        )
    }

    /// Streams filesystem names only. NCNX bodies are never opened here.
    pub fn list_live_names(
        &self,
        fragment_id: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NcnxListItem>, AppError> {
        let prefix = validate_prefix(prefix)?;
        list_names(
            &self.namespace_root(fragment_id),
            &prefix,
            after,
            false,
            limit,
        )
    }

    pub fn list_trash_names(
        &self,
        fragment_id: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NcnxListItem>, AppError> {
        let prefix = validate_prefix(prefix)?;
        list_generation_trash_names(&self.trash_root(fragment_id), &prefix, after, limit)
    }
}

fn trash_generation_name(
    deleted_at_unix_nanos: i64,
    delete_mutation_id: &str,
) -> Result<String, AppError> {
    if deleted_at_unix_nanos <= 0 || Ulid::from_string(delete_mutation_id).is_err() {
        return Err(AppError::BadRequest(
            "trash requires a positive deletion time and mutation ULID".into(),
        ));
    }
    Ok(format!("{deleted_at_unix_nanos}.{delete_mutation_id}"))
}

fn parse_trash_generation_name(name: &str) -> Option<(i64, String)> {
    let (deleted_at, mutation_id) = name.split_once('.')?;
    if mutation_id.contains('.') {
        return None;
    }
    let deleted_at_unix_nanos = deleted_at.parse::<i64>().ok()?;
    if deleted_at_unix_nanos <= 0 || Ulid::from_string(mutation_id).is_err() {
        return None;
    }
    Some((deleted_at_unix_nanos, mutation_id.to_string()))
}

pub fn validate_logical_key(key: &str) -> Result<String, AppError> {
    let key = normalize_path(key).map_err(|err| AppError::BadRequest(err.to_string()))?;
    if key.is_empty() {
        return Err(AppError::BadRequest(
            "N2 3.2 object key cannot be empty".into(),
        ));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(AppError::BadRequest(format!(
            "object key exceeds {MAX_KEY_BYTES} bytes"
        )));
    }
    for component in key.split('/') {
        if component.as_bytes().len() > MAX_COMPONENT_BYTES {
            return Err(AppError::BadRequest(format!(
                "object key component exceeds {MAX_COMPONENT_BYTES} bytes"
            )));
        }
        if component.as_bytes().contains(&0)
            || component.starts_with(".n2-")
            || matches!(
                component,
                ".namespace" | ".trash" | ".uploads" | ".objects" | ".fragment"
            )
        {
            return Err(AppError::BadRequest(format!(
                "object key component '{component}' is reserved"
            )));
        }
    }
    Ok(key)
}

fn validate_prefix(prefix: &str) -> Result<String, AppError> {
    if prefix.trim().is_empty() || prefix.trim() == "/" {
        return Ok(String::new());
    }
    let trailing_slash = prefix.trim().ends_with('/');
    let normalized = validate_logical_key(prefix)?;
    Ok(if trailing_slash {
        format!("{normalized}/")
    } else {
        normalized
    })
}

fn read_record_if_present(path: &Path) -> Result<Option<(NcnxRecord, Vec<u8>)>, AppError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(AppError::Io(err, format!("read NCNX {}", path.display()))),
    };
    let record = NcnxRecord::decode(&bytes)?;
    Ok(Some((record, bytes)))
}

fn create_immutable(path: &Path, bytes: &[u8]) -> Result<bool, AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            AppError::Io(err, format!("create NCNX directory {}", parent.display()))
        })?;
    }
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError("NCNX publication path has no parent".into())
    })?;
    let temporary = parent.join(format!(".n2-publish-{}", Ulid::new()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|err| {
            AppError::Io(
                err,
                format!("create NCNX staging file {}", temporary.display()),
            )
        })?;
    if let Err(err) = file.write_all(bytes).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(&temporary);
        return Err(AppError::Io(
            err,
            format!("write NCNX staging file {}", temporary.display()),
        ));
    }
    drop(file);
    match fs::hard_link(&temporary, path) {
        Ok(()) => {
            let _ = fs::remove_file(&temporary);
            sync_parent(path)?;
            Ok(true)
        }
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temporary);
            let existing = fs::read(path).map_err(|err| {
                AppError::Io(err, format!("read existing NCNX {}", path.display()))
            })?;
            if existing == bytes {
                Ok(false)
            } else {
                Err(AppError::Conflict(format!(
                    "NCNX key already exists with a different immutable pointer at {}",
                    path.display()
                )))
            }
        }
        Err(err) => {
            let _ = fs::remove_file(&temporary);
            Err(AppError::Io(
                err,
                format!("publish immutable NCNX {}", path.display()),
            ))
        }
    }
}

fn remove_if_exact(path: &Path, expected_bytes: &[u8]) -> Result<bool, AppError> {
    let mut opened = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(AppError::Io(err, format!("read NCNX {}", path.display()))),
    };
    let opened_metadata = opened
        .metadata()
        .map_err(|err| AppError::Io(err, format!("stat NCNX {}", path.display())))?;
    let mut existing = Vec::new();
    opened
        .read_to_end(&mut existing)
        .map_err(|err| AppError::Io(err, format!("read NCNX {}", path.display())))?;
    if existing != expected_bytes {
        return Err(AppError::Conflict(format!(
            "refused to remove changed NCNX at {}",
            path.display()
        )));
    }
    let parent = path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("NCNX removal path has no parent".into()))?;
    let removed = parent.join(format!(".n2-remove-{}", Ulid::new()));
    match fs::rename(path, &removed) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("isolate NCNX {}", path.display()),
            ));
        }
    }
    let removed_metadata = fs::metadata(&removed)
        .map_err(|err| AppError::Io(err, format!("stat isolated NCNX {}", removed.display())))?;
    #[cfg(unix)]
    let same_file = opened_metadata.dev() == removed_metadata.dev()
        && opened_metadata.ino() == removed_metadata.ino();
    #[cfg(not(unix))]
    let same_file = fs::read(&removed).is_ok_and(|bytes| bytes == expected_bytes);
    if !same_file {
        match fs::hard_link(&removed, path) {
            Ok(()) => {
                fs::remove_file(&removed).map_err(|err| {
                    AppError::Io(
                        err,
                        format!("remove restored NCNX link {}", removed.display()),
                    )
                })?;
                sync_parent(path)?;
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                return Err(AppError::Conflict(format!(
                    "NCNX changed during exact removal; preserved displaced record at {}",
                    removed.display()
                )));
            }
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("restore changed NCNX {}", path.display()),
                ));
            }
        }
        return Err(AppError::Conflict(format!(
            "NCNX changed during exact removal at {}",
            path.display()
        )));
    }
    fs::remove_file(&removed)
        .map_err(|err| AppError::Io(err, format!("remove isolated NCNX {}", removed.display())))?;
    sync_parent(path)?;
    Ok(true)
}

fn replace_durable(path: &Path, bytes: &[u8]) -> Result<bool, AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            AppError::Io(
                err,
                format!("create NCNX repair directory {}", parent.display()),
            )
        })?;
    }
    if fs::read(path).is_ok_and(|existing| existing == bytes) {
        return Ok(false);
    }
    let parent = path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("NCNX repair path has no parent".into()))?;
    let temporary = parent.join(format!(".n2-repair-{}", Ulid::new()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|err| AppError::Io(err, format!("create NCNX repair {}", temporary.display())))?;
    if let Err(err) = file.write_all(bytes).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(&temporary);
        return Err(AppError::Io(
            err,
            format!("write NCNX repair {}", temporary.display()),
        ));
    }
    fs::rename(&temporary, path)
        .map_err(|err| AppError::Io(err, format!("publish NCNX repair {}", path.display())))?;
    sync_parent(path)?;
    Ok(true)
}

fn sync_parent(path: &Path) -> Result<(), AppError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| AppError::Io(err, format!("sync NCNX directory {}", parent.display())))
}

#[derive(Debug)]
enum LexicalEntry {
    File {
        sort_key: String,
        item: NcnxListItem,
    },
    Directory {
        sort_key: String,
        physical_path: PathBuf,
        logical_prefix: String,
    },
}

impl LexicalEntry {
    fn sort_key(&self) -> &str {
        match self {
            Self::File { sort_key, .. } | Self::Directory { sort_key, .. } => sort_key,
        }
    }
}

#[derive(Debug)]
struct LexicalFrame {
    entries: Vec<LexicalEntry>,
}

fn prefixes_overlap(branch: &str, requested: &str) -> bool {
    requested.is_empty() || requested.starts_with(branch) || branch.starts_with(requested)
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return Some(successor);
        }
    }
    None
}

fn branch_can_exceed(branch: &str, after: Option<&str>) -> bool {
    let Some(after) = after else {
        return true;
    };
    prefix_successor(branch.as_bytes()).map_or(true, |upper_bound| {
        after.as_bytes() < upper_bound.as_slice()
    })
}

fn directory_frame(
    physical_directory: &Path,
    logical_parent: &str,
    requested_prefix: &str,
    after: Option<&str>,
    include_after: bool,
) -> Result<LexicalFrame, AppError> {
    let mut entries = Vec::new();
    let directory = fs::read_dir(physical_directory).map_err(|err| {
        AppError::Io(
            err,
            format!("read NCNX directory {}", physical_directory.display()),
        )
    })?;
    for entry in directory {
        let entry = entry.map_err(|err| {
            AppError::Io(
                err,
                format!(
                    "read NCNX directory entry in {}",
                    physical_directory.display()
                ),
            )
        })?;
        let file_name = entry.file_name().into_string().map_err(|name| {
            AppError::Conflict(format!(
                "NCNX path component is not UTF-8: {}",
                PathBuf::from(name).display()
            ))
        })?;
        if file_name.starts_with(".n2-") {
            continue;
        }
        let file_type = entry.file_type().map_err(|err| {
            AppError::Io(err, format!("inspect NCNX path {}", entry.path().display()))
        })?;
        if file_type.is_dir() {
            let logical_prefix = format!("{logical_parent}{file_name}/");
            if prefixes_overlap(&logical_prefix, requested_prefix)
                && branch_can_exceed(&logical_prefix, after)
            {
                entries.push(LexicalEntry::Directory {
                    sort_key: logical_prefix.clone(),
                    physical_path: entry.path(),
                    logical_prefix,
                });
            }
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let relative = format!("{logical_parent}{file_name}");
        let Some(logical_key) = relative.strip_suffix(NCNX_SUFFIX) else {
            continue;
        };
        if !logical_key.starts_with(requested_prefix) {
            continue;
        }
        if let Some(after) = after {
            if logical_key < after || (!include_after && logical_key == after) {
                continue;
            }
        }
        entries.push(LexicalEntry::File {
            sort_key: logical_key.to_string(),
            item: NcnxListItem {
                logical_key: logical_key.to_string(),
                physical_path: entry.path(),
                trash: None,
            },
        });
    }
    // Reverse order lets the streaming walk pop the next lexical entry in O(1).
    entries.sort_unstable_by(|left, right| right.sort_key().cmp(left.sort_key()));
    Ok(LexicalFrame { entries })
}

fn list_names(
    root: &Path,
    prefix: &str,
    after: Option<&str>,
    include_after: bool,
    limit: usize,
) -> Result<Vec<NcnxListItem>, AppError> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let limit = limit.clamp(1, 10_000);
    let mut out = Vec::with_capacity(limit);
    let mut stack = vec![directory_frame(root, "", &prefix, after, include_after)?];
    while let Some(frame) = stack.last_mut() {
        let Some(entry) = frame.entries.pop() else {
            stack.pop();
            continue;
        };
        match entry {
            LexicalEntry::File { item, .. } => {
                out.push(item);
                if out.len() == limit {
                    break;
                }
            }
            LexicalEntry::Directory {
                physical_path,
                logical_prefix,
                ..
            } => stack.push(directory_frame(
                &physical_path,
                &logical_prefix,
                &prefix,
                after,
                include_after,
            )?),
        }
    }
    Ok(out)
}

pub fn parse_trash_relative_path(relative: &str) -> Option<TrashLocator> {
    let (generation, encoded_key) = relative.split_once('/')?;
    let (deleted_at_unix_nanos, delete_mutation_id) = parse_trash_generation_name(generation)?;
    let key = encoded_key.strip_suffix(NCNX_SUFFIX)?;
    let logical_key = validate_logical_key(key).ok()?;
    Some(TrashLocator {
        logical_key,
        deleted_at_unix_nanos,
        delete_mutation_id,
    })
}

fn parse_trash_cursor(after: &str) -> Result<(&str, i64, &str), AppError> {
    let mut fields = after.split('\0');
    let logical_key = fields.next().unwrap_or_default();
    let deleted_at = fields.next();
    let mutation_id = fields.next();
    if fields.next().is_some()
        || logical_key.is_empty()
        || deleted_at.is_none()
        || mutation_id.is_none()
    {
        return Err(AppError::BadRequest(
            "invalid NCNX trash continuation token".into(),
        ));
    }
    let deleted_at_unix_nanos = deleted_at
        .unwrap()
        .parse::<i64>()
        .map_err(|_| AppError::BadRequest("invalid NCNX trash continuation token".into()))?;
    let mutation_id = mutation_id.unwrap();
    validate_logical_key(logical_key)?;
    if deleted_at_unix_nanos <= 0 || Ulid::from_string(mutation_id).is_err() {
        return Err(AppError::BadRequest(
            "invalid NCNX trash continuation token".into(),
        ));
    }
    Ok((logical_key, deleted_at_unix_nanos, mutation_id))
}

fn list_generation_trash_names(
    trash_root: &Path,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<NcnxListItem>, AppError> {
    if !trash_root.exists() {
        return Ok(Vec::new());
    }
    let limit = limit.clamp(1, 10_000);
    let after = after.map(parse_trash_cursor).transpose()?;
    let after_cursor =
        after.map(|(key, nanos, mutation)| format!("{key}\0{nanos:020}\0{mutation}"));
    let mut selected = BTreeMap::<String, NcnxListItem>::new();
    let generations = fs::read_dir(trash_root).map_err(|err| {
        AppError::Io(
            err,
            format!("read NCNX trash root {}", trash_root.display()),
        )
    })?;
    for entry in generations {
        let entry = entry.map_err(|err| {
            AppError::Io(
                err,
                format!("read NCNX trash entry in {}", trash_root.display()),
            )
        })?;
        let generation_name = entry.file_name().into_string().map_err(|name| {
            AppError::Conflict(format!(
                "NCNX trash generation is not UTF-8: {}",
                PathBuf::from(name).display()
            ))
        })?;
        if generation_name.starts_with(".n2-") {
            continue;
        }
        let file_type = entry.file_type().map_err(|err| {
            AppError::Io(
                err,
                format!("inspect NCNX trash generation {}", entry.path().display()),
            )
        })?;
        let Some((deleted_at_unix_nanos, delete_mutation_id)) =
            parse_trash_generation_name(&generation_name)
        else {
            return Err(AppError::Conflict(format!(
                "invalid N2 3.2 trash generation '{}' in {}",
                generation_name,
                trash_root.display()
            )));
        };
        if !file_type.is_dir() {
            return Err(AppError::Conflict(format!(
                "N2 3.2 trash generation is not a directory: {}",
                entry.path().display()
            )));
        }

        let (logical_after, include_after) = match after {
            Some((key, after_nanos, after_mutation)) => {
                let generation_is_after = (deleted_at_unix_nanos, delete_mutation_id.as_str())
                    > (after_nanos, after_mutation);
                (Some(key), generation_is_after)
            }
            None => (None, false),
        };
        for mut item in list_names(&entry.path(), prefix, logical_after, include_after, limit)? {
            let locator = TrashLocator {
                logical_key: item.logical_key.clone(),
                deleted_at_unix_nanos,
                delete_mutation_id: delete_mutation_id.clone(),
            };
            let cursor = format!(
                "{}\0{:020}\0{}",
                locator.logical_key, locator.deleted_at_unix_nanos, locator.delete_mutation_id
            );
            if after_cursor
                .as_ref()
                .is_some_and(|after| cursor.as_str() <= after.as_str())
            {
                continue;
            }
            item.trash = Some(locator);
            selected.insert(cursor, item);
            if selected.len() > limit {
                selected.pop_last();
            }
        }
    }
    Ok(selected.into_values().collect())
}

fn prune_empty_trash_parents(path: &Path, trash_root: &Path) -> Result<(), AppError> {
    let mut current = path.parent().map(Path::to_path_buf);
    while let Some(directory) = current {
        if directory == trash_root {
            break;
        }
        let parent = directory.parent().map(Path::to_path_buf);
        match fs::remove_dir(&directory) {
            Ok(()) => sync_parent(&directory)?,
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::NotFound | ErrorKind::DirectoryNotEmpty
                ) =>
            {
                break;
            }
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("prune empty NCNX trash directory {}", directory.display()),
                ));
            }
        }
        current = parent;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn store() -> (PathBuf, NcnxStore) {
        let root = std::env::temp_dir().join(format!("n2-ncnx-{}", Ulid::new()));
        (root.clone(), NcnxStore::new("root-a", root))
    }

    fn record(fragment: &str, object: &str) -> NcnxRecord {
        NcnxRecord::new(
            fragment,
            object,
            Ulid::new().to_string(),
            Utc::now().timestamp_nanos_opt().unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn ncnx_integrity_rejects_corruption() {
        let record = record("fragment-a", "object-a");
        let mut bytes = record.encode().unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x80;
        assert!(NcnxRecord::decode(&bytes).is_err());
    }

    #[test]
    fn immutable_create_is_idempotent_and_rejects_other_pointer() {
        let (root, store) = store();
        let first = record("fragment-a", "object-a").encode().unwrap();
        let second = record("fragment-a", "object-b").encode().unwrap();
        assert!(
            store
                .create_live("fragment-a", "videos/a.mp4", &first)
                .unwrap()
        );
        assert!(
            !store
                .create_live("fragment-a", "videos/a.mp4", &first)
                .unwrap()
        );
        assert!(
            store
                .create_live("fragment-a", "videos/a.mp4", &second)
                .is_err()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exact_remove_never_deletes_another_pointer() {
        let (root, store) = store();
        let first = record("fragment-a", "object-a").encode().unwrap();
        let other = record("fragment-a", "object-b").encode().unwrap();
        store
            .create_live("fragment-a", "videos/a.mp4", &first)
            .unwrap();
        assert!(
            store
                .remove_live_if_exact("fragment-a", "videos/a.mp4", &other)
                .is_err()
        );
        assert_eq!(
            store
                .read_live("fragment-a", "videos/a.mp4")
                .unwrap()
                .unwrap()
                .1,
            first
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn destination_first_copy_delete_is_restart_safe_at_each_stage() {
        let (root, store) = store();
        let bytes = record("fragment-a", "object-a").encode().unwrap();
        let trash = TrashLocator {
            logical_key: "old/a.mp4".into(),
            deleted_at_unix_nanos: 1_234_567,
            delete_mutation_id: Ulid::new().to_string(),
        };
        store
            .create_live("fragment-a", "old/a.mp4", &bytes)
            .unwrap();
        store
            .create_live("fragment-a", "new/a.mp4", &bytes)
            .unwrap();
        assert!(
            store
                .read_live("fragment-a", "old/a.mp4")
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .read_live("fragment-a", "new/a.mp4")
                .unwrap()
                .is_some()
        );

        store.create_trash("fragment-a", &trash, &bytes).unwrap();
        assert!(store.read_trash("fragment-a", &trash).unwrap().is_some());
        store
            .remove_live_if_exact("fragment-a", "old/a.mp4", &bytes)
            .unwrap();
        assert!(
            store
                .read_live("fragment-a", "old/a.mp4")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .read_live("fragment-a", "new/a.mp4")
                .unwrap()
                .is_some()
        );
        assert!(store.read_trash("fragment-a", &trash).unwrap().is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn trash_generation_is_zero_decode_and_collision_free() {
        let (root, store) = store();
        let mutation = Ulid::new().to_string();
        let locator = TrashLocator {
            logical_key: "videos/a.mp4".into(),
            deleted_at_unix_nanos: 1_234_567,
            delete_mutation_id: mutation.clone(),
        };
        let bytes = record("fragment-a", "object-a").encode().unwrap();
        store.create_trash("fragment-a", &locator, &bytes).unwrap();
        assert_eq!(
            store
                .trash_path("fragment-a", "videos/a.mp4", 1_234_567, &mutation,)
                .unwrap()
                .strip_prefix(store.trash_root("fragment-a"))
                .unwrap(),
            Path::new(&format!("1234567.{mutation}/videos/a.mp4.ncnx"))
        );
        let listed = store
            .list_trash_names("fragment-a", "videos/", None, 10)
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].trash.as_ref(), Some(&locator));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn repeated_delete_generations_of_the_same_key_page_without_collision() {
        let (root, store) = store();
        let first = TrashLocator {
            logical_key: "videos/a.mp4".into(),
            deleted_at_unix_nanos: 1_000,
            delete_mutation_id: Ulid::new().to_string(),
        };
        let second = TrashLocator {
            logical_key: "videos/a.mp4".into(),
            deleted_at_unix_nanos: 2_000,
            delete_mutation_id: Ulid::new().to_string(),
        };
        let bytes = record("fragment-a", "object-a").encode().unwrap();
        store.create_trash("fragment-a", &second, &bytes).unwrap();
        store.create_trash("fragment-a", &first, &bytes).unwrap();

        let first_page = store
            .list_trash_names("fragment-a", "videos/", None, 1)
            .unwrap();
        assert_eq!(first_page.len(), 1);
        assert_eq!(first_page[0].trash.as_ref(), Some(&first));
        let cursor = format!(
            "{}\0{:020}\0{}",
            first.logical_key, first.deleted_at_unix_nanos, first.delete_mutation_id
        );
        let second_page = store
            .list_trash_names("fragment-a", "videos/", Some(&cursor), 1)
            .unwrap();
        assert_eq!(second_page.len(), 1);
        assert_eq!(second_page[0].trash.as_ref(), Some(&second));
        assert!(
            store
                .list_trash_names(
                    "fragment-a",
                    "videos/",
                    Some(&format!(
                        "{}\0{:020}\0{}",
                        second.logical_key, second.deleted_at_unix_nanos, second.delete_mutation_id
                    )),
                    1,
                )
                .unwrap()
                .is_empty()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn one_prefix_delete_generation_contains_unchanged_logical_paths() {
        let (root, store) = store();
        let mutation = Ulid::new().to_string();
        let bytes = record("fragment-a", "object-a").encode().unwrap();
        for logical_key in ["videos/a.mp4", "videos/nested/b.mp4"] {
            store
                .create_trash(
                    "fragment-a",
                    &TrashLocator {
                        logical_key: logical_key.into(),
                        deleted_at_unix_nanos: 3_000,
                        delete_mutation_id: mutation.clone(),
                    },
                    &bytes,
                )
                .unwrap();
        }
        let generation = store
            .trash_root("fragment-a")
            .join(format!("3000.{mutation}"));
        assert!(generation.join("videos/a.mp4.ncnx").is_file());
        assert!(generation.join("videos/nested/b.mp4.ncnx").is_file());
        assert_eq!(
            store
                .list_trash_names("fragment-a", "videos/", None, 10)
                .unwrap()
                .into_iter()
                .map(|item| item.logical_key)
                .collect::<Vec<_>>(),
            ["videos/a.mp4", "videos/nested/b.mp4"]
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn removing_last_trash_record_prunes_its_generation() {
        let (root, store) = store();
        let locator = TrashLocator {
            logical_key: "videos/nested/a.mp4".into(),
            deleted_at_unix_nanos: 4_000,
            delete_mutation_id: Ulid::new().to_string(),
        };
        let bytes = record("fragment-a", "object-a").encode().unwrap();
        store.create_trash("fragment-a", &locator, &bytes).unwrap();
        let generation = store.trash_root("fragment-a").join(format!(
            "{}.{}",
            locator.deleted_at_unix_nanos, locator.delete_mutation_id
        ));
        assert!(generation.is_dir());
        assert!(
            store
                .remove_trash_if_exact("fragment-a", &locator, &bytes)
                .unwrap()
        );
        assert!(!generation.exists());
        assert!(store.trash_root("fragment-a").is_dir());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn malformed_or_legacy_trash_layout_fails_closed() {
        let (root, store) = store();
        let legacy = store
            .trash_root("fragment-a")
            .join("videos/a.mp4.ncnx.trashed.1000.01ARZ3NDEKTSV4RRFFQ69G5FAV");
        fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        fs::write(legacy, b"legacy").unwrap();
        assert!(store.list_trash_names("fragment-a", "", None, 10).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn live_listing_uses_names_without_opening_records() {
        let (root, store) = store();
        let path = store.live_path("fragment-a", "videos/broken.mp4").unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"deliberately-not-messagepack").unwrap();
        let listed = store
            .list_live_names("fragment-a", "videos/", None, 10)
            .unwrap();
        assert_eq!(listed[0].logical_key, "videos/broken.mp4");
        assert!(store.read_live("fragment-a", "videos/broken.mp4").is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn live_listing_pages_exact_keys_and_same_name_prefixes_in_logical_order() {
        let (root, store) = store();
        let expected = ["a", "a-previous", "a/child", "a/child/deep", "a0"];
        for key in expected {
            let path = store.live_path("fragment-a", key).unwrap();
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"listing-does-not-decode-this").unwrap();
        }

        let mut actual = Vec::new();
        let mut after = None;
        loop {
            let page = store
                .list_live_names("fragment-a", "a", after.as_deref(), 1)
                .unwrap();
            let Some(item) = page.into_iter().next() else {
                break;
            };
            after = Some(item.logical_key.clone());
            actual.push(item.logical_key);
        }
        assert_eq!(actual, expected);

        let subtree = store
            .list_live_names("fragment-a", "a/", None, 10)
            .unwrap()
            .into_iter()
            .map(|item| item.logical_key)
            .collect::<Vec<_>>();
        assert_eq!(subtree, ["a/child", "a/child/deep"]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn key_validation_reserves_engine_components() {
        assert!(validate_logical_key("videos/a.mp4").is_ok());
        assert!(validate_logical_key("videos/.trash/a.mp4").is_err());
        assert!(validate_logical_key("../a.mp4").is_err());
        assert!(validate_logical_key("").is_err());
    }
}
