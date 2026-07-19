use crate::core::fragment_identity::physical_fragment_root_on;
use crate::enums::app_error_enum::AppError;
use crate::namespace::paths::{base_name, normalize_path, parent_path};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use ulid::Ulid;

pub const NCNX_FORMAT_VERSION: u32 = 3;
pub const NAMESPACE_DIR: &str = ".namespace";
pub const NAMESPACE_TRASH_DIR: &str = ".trash";
pub const NCNX_SUFFIX: &str = ".ncnx";
const TRASH_SEPARATOR: &str = ".ncnx.trashed.";
const MAX_KEY_BYTES: usize = 4_096;
const MAX_COMPONENT_BYTES: usize = 250;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NcnxRecord {
    pub format_version: u32,
    pub fragment_id: String,
    pub object_id: String,
    pub mutation_id: String,
    pub created_at_unix_nanos: i64,
    pub payload_crc32: u32,
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
            payload_crc32: 0,
        };
        record.validate_fields()?;
        record.payload_crc32 = record.expected_crc32()?;
        Ok(record)
    }

    fn validate_fields(&self) -> Result<(), AppError> {
        if self.format_version != NCNX_FORMAT_VERSION {
            return Err(AppError::Conflict(format!(
                "NCNX format {} is not N2 3.0 format {}",
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

    fn expected_crc32(&self) -> Result<u32, AppError> {
        let mut unsigned = self.clone();
        unsigned.payload_crc32 = 0;
        let payload = rmp_serde::to_vec_named(&unsigned).map_err(|err| {
            AppError::InternalServerError(format!("encode NCNX integrity payload: {err}"))
        })?;
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        Ok(hasher.finalize())
    }

    pub fn validate(&self) -> Result<(), AppError> {
        self.validate_fields()?;
        let expected = self.expected_crc32()?;
        if expected != self.payload_crc32 {
            return Err(AppError::Conflict(format!(
                "NCNX integrity mismatch: expected {expected:08x}, recorded {:08x}",
                self.payload_crc32
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
        if deleted_at_unix_nanos <= 0 || Ulid::from_string(delete_mutation_id).is_err() {
            return Err(AppError::BadRequest(
                "trash requires a positive deletion time and mutation ULID".into(),
            ));
        }
        let parent = parent_path(&key);
        let name = base_name(&key);
        Ok(self.trash_root(fragment_id).join(parent).join(format!(
            "{name}{TRASH_SEPARATOR}{deleted_at_unix_nanos}.{delete_mutation_id}"
        )))
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
        remove_if_exact(
            &self.trash_path(
                fragment_id,
                &locator.logical_key,
                locator.deleted_at_unix_nanos,
                &locator.delete_mutation_id,
            )?,
            expected_bytes,
        )
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
            limit,
            false,
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
        list_names(&self.trash_root(fragment_id), &prefix, after, limit, true)
    }
}

pub fn validate_logical_key(key: &str) -> Result<String, AppError> {
    let key = normalize_path(key).map_err(|err| AppError::BadRequest(err.to_string()))?;
    if key.is_empty() {
        return Err(AppError::BadRequest(
            "N2 3.0 object key cannot be empty".into(),
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
    trash: bool,
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
        let (logical_key, trash_locator) = if trash {
            let Some(locator) = parse_trash_relative_path(&relative) else {
                continue;
            };
            (locator.logical_key.clone(), Some(locator))
        } else {
            let Some(key) = relative.strip_suffix(NCNX_SUFFIX) else {
                continue;
            };
            (key.to_string(), None)
        };
        if !logical_key.starts_with(requested_prefix) {
            continue;
        }
        let sort_key = if let Some(locator) = &trash_locator {
            format!(
                "{}\0{:020}\0{}",
                locator.logical_key, locator.deleted_at_unix_nanos, locator.delete_mutation_id
            )
        } else {
            logical_key.clone()
        };
        if after.is_some_and(|after| sort_key.as_str() <= after) {
            continue;
        }
        entries.push(LexicalEntry::File {
            sort_key,
            item: NcnxListItem {
                logical_key,
                physical_path: entry.path(),
                trash: trash_locator,
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
    limit: usize,
    trash: bool,
) -> Result<Vec<NcnxListItem>, AppError> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let limit = limit.clamp(1, 10_000);
    let mut out = Vec::with_capacity(limit);
    let mut stack = vec![directory_frame(root, "", &prefix, after, trash)?];
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
                trash,
            )?),
        }
    }
    Ok(out)
}

pub fn parse_trash_relative_path(relative: &str) -> Option<TrashLocator> {
    let (key, suffix) = relative.rsplit_once(TRASH_SEPARATOR)?;
    let (deleted_at, mutation_id) = suffix.split_once('.')?;
    let deleted_at_unix_nanos = deleted_at.parse::<i64>().ok()?;
    Ulid::from_string(mutation_id).ok()?;
    Some(TrashLocator {
        logical_key: key.to_string(),
        deleted_at_unix_nanos,
        delete_mutation_id: mutation_id.to_string(),
    })
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
    fn trash_name_is_zero_decode_and_collision_free() {
        let (root, store) = store();
        let mutation = Ulid::new().to_string();
        let locator = TrashLocator {
            logical_key: "videos/a.mp4".into(),
            deleted_at_unix_nanos: 1_234_567,
            delete_mutation_id: mutation.clone(),
        };
        let bytes = record("fragment-a", "object-a").encode().unwrap();
        store.create_trash("fragment-a", &locator, &bytes).unwrap();
        let listed = store
            .list_trash_names("fragment-a", "videos/", None, 10)
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].trash.as_ref(), Some(&locator));
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
