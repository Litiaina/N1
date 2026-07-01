use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const STORE_DIR: &str = ".n2/namespace";
const ENTRY_SUFFIX: &str = ".dentry";
const TRANSACTION_SUFFIX: &str = ".rename";
const LOCK_STRIPES: usize = 256;
const DEFAULT_DIRECTORY_SHARD_BITS: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DentryKey {
    pub fragment: String,
    pub parent_directory_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryLayout {
    pub shard_bits: u8,
    pub epoch: u64,
}

impl Default for DirectoryLayout {
    fn default() -> Self {
        Self {
            shard_bits: DEFAULT_DIRECTORY_SHARD_BITS,
            epoch: 1,
        }
    }
}

impl DirectoryLayout {
    pub fn validate(self) -> Result<Self, AppError> {
        if self.shard_bits > 24 {
            return Err(AppError::BadRequest(
                "namespace directory shard_bits cannot exceed 24".into(),
            ));
        }
        if self.epoch == 0 {
            return Err(AppError::BadRequest(
                "namespace directory layout epoch must be positive".into(),
            ));
        }
        Ok(self)
    }

    pub fn bucket_count(self) -> u32 {
        1u32 << self.shard_bits
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DentryTarget {
    Directory {
        directory_id: String,
        layout: DirectoryLayout,
    },
    Object {
        object_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrashContext {
    pub original_parent_directory_id: String,
    pub original_parent_layout: DirectoryLayout,
    pub original_name: String,
    pub original_path: String,
    pub deleted_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceDentry {
    pub key: DentryKey,
    pub target: DentryTarget,
    pub generation: u64,
    pub last_mutation_id: String,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trash: Option<TrashContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RenameTransaction {
    mutation_id: String,
    source: DentryKey,
    source_layout: DirectoryLayout,
    destination: DentryKey,
    destination_layout: DirectoryLayout,
    expected_generation: u64,
    destination_record: NamespaceDentry,
}

#[derive(Clone)]
pub struct NamespaceDentryStore {
    roots: Arc<Vec<PathBuf>>,
    locks: Arc<Vec<Mutex<()>>>,
}

impl NamespaceDentryStore {
    pub fn new(roots: Vec<PathBuf>) -> Result<Self, AppError> {
        let mut unique = BTreeSet::new();
        let roots = roots
            .into_iter()
            .filter(|root| unique.insert(root.clone()))
            .collect::<Vec<_>>();
        if roots.is_empty() {
            return Err(AppError::BadRequest(
                "N2 namespace requires at least one configured storage device".into(),
            ));
        }
        for root in &roots {
            let namespace_root = root.join(STORE_DIR);
            fs::create_dir_all(namespace_root.join("partitions")).map_err(|err| {
                io_error(
                    err,
                    format!("create namespace root {}", namespace_root.display()),
                )
            })?;
            fs::create_dir_all(namespace_root.join("transactions")).map_err(|err| {
                io_error(
                    err,
                    format!(
                        "create namespace transaction root {}",
                        namespace_root.display()
                    ),
                )
            })?;
        }
        let store = Self {
            roots: Arc::new(roots),
            locks: Arc::new((0..LOCK_STRIPES).map(|_| Mutex::new(())).collect()),
        };
        store.recover_incomplete_renames()?;
        Ok(store)
    }

    pub fn roots(&self) -> &[PathBuf] {
        self.roots.as_slice()
    }

    pub fn get(
        &self,
        key: &DentryKey,
        parent_layout: DirectoryLayout,
    ) -> Result<Option<NamespaceDentry>, AppError> {
        validate_key(key)?;
        parent_layout.validate()?;
        let path = self.entry_path(key, parent_layout);
        read_record(&path, key)
    }

    pub fn create(
        &self,
        key: DentryKey,
        parent_layout: DirectoryLayout,
        target: DentryTarget,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<NamespaceDentry, AppError> {
        validate_key(&key)?;
        validate_target(&target)?;
        parent_layout.validate()?;
        validate_mutation_id(mutation_id)?;
        let stripe = lock_stripe(&key);
        let _guard = self.locks[stripe]
            .lock()
            .map_err(|_| AppError::InternalServerError("namespace lock poisoned".into()))?;
        if let Some(existing) = self.get(&key, parent_layout)? {
            if existing.last_mutation_id == mutation_id && existing.target == target {
                return Ok(existing);
            }
            return Err(AppError::Conflict(format!(
                "namespace entry '{}' already exists",
                key.name
            )));
        }
        let record = NamespaceDentry {
            key: key.clone(),
            target,
            generation: 1,
            last_mutation_id: mutation_id.to_string(),
            created_at_unix: now_unix,
            updated_at_unix: now_unix,
            trash: None,
        };
        write_record_atomic(&self.entry_path(&key, parent_layout), &record)?;
        Ok(record)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rename(
        &self,
        source: DentryKey,
        source_layout: DirectoryLayout,
        destination: DentryKey,
        destination_layout: DirectoryLayout,
        expected_generation: u64,
        mutation_id: &str,
        now_unix: i64,
        trash: Option<TrashContext>,
    ) -> Result<NamespaceDentry, AppError> {
        validate_key(&source)?;
        validate_key(&destination)?;
        source_layout.validate()?;
        destination_layout.validate()?;
        validate_mutation_id(mutation_id)?;
        if source.fragment != destination.fragment {
            return Err(AppError::BadRequest(
                "namespace rename cannot cross fragments".into(),
            ));
        }
        if source == destination {
            return self.get(&source, source_layout)?.ok_or(AppError::NotFound);
        }

        let source_stripe = lock_stripe(&source);
        let destination_stripe = lock_stripe(&destination);
        let (first, second) = if source_stripe <= destination_stripe {
            (source_stripe, destination_stripe)
        } else {
            (destination_stripe, source_stripe)
        };
        let _first_guard = self.locks[first]
            .lock()
            .map_err(|_| AppError::InternalServerError("namespace lock poisoned".into()))?;
        let _second_guard = if second != first {
            Some(
                self.locks[second]
                    .lock()
                    .map_err(|_| AppError::InternalServerError("namespace lock poisoned".into()))?,
            )
        } else {
            None
        };

        if let Some(existing) = self.get(&destination, destination_layout)? {
            if existing.last_mutation_id == mutation_id {
                return Ok(existing);
            }
            return Err(AppError::Conflict(format!(
                "namespace destination '{}' already exists",
                destination.name
            )));
        }
        let existing = self
            .get(&source, source_layout)?
            .ok_or(AppError::NotFound)?;
        if existing.generation != expected_generation {
            return Err(AppError::Conflict(format!(
                "namespace generation changed: expected {}, found {}",
                expected_generation, existing.generation
            )));
        }

        let destination_record = NamespaceDentry {
            key: destination.clone(),
            target: existing.target.clone(),
            generation: existing.generation.saturating_add(1),
            last_mutation_id: mutation_id.to_string(),
            created_at_unix: existing.created_at_unix,
            updated_at_unix: now_unix,
            trash,
        };
        let transaction = RenameTransaction {
            mutation_id: mutation_id.to_string(),
            source,
            source_layout,
            destination,
            destination_layout,
            expected_generation,
            destination_record: destination_record.clone(),
        };
        self.persist_transaction(&transaction)?;
        self.apply_transaction(&transaction)?;
        self.remove_transaction(&transaction)?;
        Ok(destination_record)
    }

    pub fn remove(
        &self,
        key: &DentryKey,
        parent_layout: DirectoryLayout,
        expected_generation: u64,
        mutation_id: &str,
    ) -> Result<NamespaceDentry, AppError> {
        validate_key(key)?;
        validate_mutation_id(mutation_id)?;
        let stripe = lock_stripe(key);
        let _guard = self.locks[stripe]
            .lock()
            .map_err(|_| AppError::InternalServerError("namespace lock poisoned".into()))?;
        let existing = self.get(key, parent_layout)?.ok_or(AppError::NotFound)?;
        if existing.generation != expected_generation {
            return Err(AppError::Conflict(format!(
                "namespace generation changed: expected {}, found {}",
                expected_generation, existing.generation
            )));
        }
        let path = self.entry_path(key, parent_layout);
        fs::remove_file(&path)
            .map_err(|err| io_error(err, format!("remove namespace entry {}", path.display())))?;
        sync_parent(&path)?;
        Ok(existing)
    }

    pub(crate) fn install_prepared_record(
        &self,
        record: NamespaceDentry,
        parent_layout: DirectoryLayout,
    ) -> Result<NamespaceDentry, AppError> {
        validate_key(&record.key)?;
        validate_target(&record.target)?;
        parent_layout.validate()?;
        let stripe = lock_stripe(&record.key);
        let _guard = self.locks[stripe]
            .lock()
            .map_err(|_| AppError::InternalServerError("namespace lock poisoned".into()))?;
        if let Some(existing) = self.get(&record.key, parent_layout)? {
            if existing == record {
                return Ok(existing);
            }
            return Err(AppError::Conflict(format!(
                "namespace entry '{}' changed before prepared install",
                record.key.name
            )));
        }
        write_record_atomic(&self.entry_path(&record.key, parent_layout), &record)?;
        Ok(record)
    }

    pub(crate) fn partition_root(&self, partition: u64) -> &Path {
        self.root_for_partition(partition).as_path()
    }

    pub fn list_parent(
        &self,
        fragment: &str,
        parent_directory_id: &str,
        layout: DirectoryLayout,
    ) -> Result<Vec<NamespaceDentry>, AppError> {
        validate_component("fragment", fragment)?;
        validate_component("parent directory id", parent_directory_id)?;
        layout.validate()?;
        let mut entries = BTreeMap::new();
        for bucket in 0..layout.bucket_count() {
            for record in self.list_bucket(fragment, parent_directory_id, layout, bucket)? {
                entries.insert(record.key.name.clone(), record);
            }
        }
        Ok(entries.into_values().collect())
    }

    pub fn list_bucket(
        &self,
        fragment: &str,
        parent_directory_id: &str,
        layout: DirectoryLayout,
        bucket: u32,
    ) -> Result<Vec<NamespaceDentry>, AppError> {
        validate_component("fragment", fragment)?;
        validate_component("parent directory id", parent_directory_id)?;
        layout.validate()?;
        if bucket >= layout.bucket_count() {
            return Err(AppError::BadRequest(format!(
                "namespace bucket {bucket} is outside directory layout"
            )));
        }
        let partition = virtual_partition_for_bucket(fragment, parent_directory_id, bucket);
        let root = self.root_for_partition(partition);
        let dir = parent_entry_dir(root, partition, fragment, parent_directory_id);
        let read_dir = match fs::read_dir(&dir) {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(io_error(
                    err,
                    format!("list namespace directory {}", dir.display()),
                ));
            }
        };
        let mut entries = BTreeMap::new();
        for child in read_dir {
            let child = child.map_err(|err| {
                io_error(err, format!("read namespace directory {}", dir.display()))
            })?;
            if child.path().extension().and_then(|value| value.to_str()) != Some("dentry") {
                continue;
            }
            let bytes = fs::read(child.path()).map_err(|err| {
                io_error(
                    err,
                    format!("read namespace entry {}", child.path().display()),
                )
            })?;
            let record: NamespaceDentry = rmp_serde::from_slice(&bytes)?;
            if record.key.fragment != fragment
                || record.key.parent_directory_id != parent_directory_id
                || directory_bucket(&record.key.name, layout) != bucket
            {
                return Err(AppError::InternalServerError(format!(
                    "namespace entry {} is stored under the wrong bucket",
                    child.path().display()
                )));
            }
            entries.insert(record.key.name.clone(), record);
        }
        Ok(entries.into_values().collect())
    }

    pub fn recover_incomplete_renames(&self) -> Result<usize, AppError> {
        let mut transactions = BTreeMap::new();
        for root in self.roots.iter() {
            let dir = root.join(STORE_DIR).join("transactions");
            let read_dir = match fs::read_dir(&dir) {
                Ok(read_dir) => read_dir,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(io_error(
                        err,
                        format!("scan namespace transactions {}", dir.display()),
                    ));
                }
            };
            for child in read_dir {
                let child = child.map_err(|err| {
                    io_error(
                        err,
                        format!("scan namespace transactions {}", dir.display()),
                    )
                })?;
                if child.path().extension().and_then(|value| value.to_str()) != Some("rename") {
                    continue;
                }
                let bytes = fs::read(child.path()).map_err(|err| {
                    io_error(
                        err,
                        format!("read namespace transaction {}", child.path().display()),
                    )
                })?;
                let transaction: RenameTransaction = rmp_serde::from_slice(&bytes)?;
                transactions.insert(transaction.mutation_id.clone(), transaction);
            }
        }
        let mut recovered = 0usize;
        for transaction in transactions.into_values() {
            self.apply_transaction(&transaction)?;
            self.remove_transaction(&transaction)?;
            recovered = recovered.saturating_add(1);
        }
        Ok(recovered)
    }

    fn apply_transaction(&self, transaction: &RenameTransaction) -> Result<(), AppError> {
        let destination = self.get(&transaction.destination, transaction.destination_layout)?;
        match destination {
            Some(existing)
                if existing.last_mutation_id == transaction.mutation_id
                    && existing == transaction.destination_record => {}
            Some(_) => {
                return Err(AppError::Conflict(
                    "rename destination changed while replaying transaction".into(),
                ));
            }
            None => {
                let source = self
                    .get(&transaction.source, transaction.source_layout)?
                    .ok_or_else(|| {
                        AppError::InternalServerError(
                            "rename transaction lost both source and destination".into(),
                        )
                    })?;
                if source.generation != transaction.expected_generation {
                    return Err(AppError::Conflict(
                        "rename source generation changed while replaying transaction".into(),
                    ));
                }
                write_record_atomic(
                    &self.entry_path(&transaction.destination, transaction.destination_layout),
                    &transaction.destination_record,
                )?;
            }
        }

        let source_path = self.entry_path(&transaction.source, transaction.source_layout);
        match read_record(&source_path, &transaction.source)? {
            Some(source) if source.generation == transaction.expected_generation => {
                fs::remove_file(&source_path).map_err(|err| {
                    io_error(
                        err,
                        format!("complete namespace rename {}", source_path.display()),
                    )
                })?;
                sync_parent(&source_path)?;
            }
            Some(_) => {
                return Err(AppError::Conflict(
                    "rename source changed while replaying transaction".into(),
                ));
            }
            None => {}
        }
        Ok(())
    }

    fn persist_transaction(&self, transaction: &RenameTransaction) -> Result<(), AppError> {
        let bytes = rmp_serde::to_vec_named(transaction)?;
        for path in self.transaction_paths(transaction) {
            write_bytes_atomic(&path, &bytes)?;
        }
        Ok(())
    }

    fn remove_transaction(&self, transaction: &RenameTransaction) -> Result<(), AppError> {
        for path in self.transaction_paths(transaction) {
            match fs::remove_file(&path) {
                Ok(()) => sync_parent(&path)?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(io_error(
                        err,
                        format!("remove namespace transaction {}", path.display()),
                    ));
                }
            }
        }
        Ok(())
    }

    fn transaction_paths(&self, transaction: &RenameTransaction) -> Vec<PathBuf> {
        let source_partition = virtual_partition(
            &transaction.source.fragment,
            &transaction.source.parent_directory_id,
            &transaction.source.name,
            transaction.source_layout,
        );
        let destination_partition = virtual_partition(
            &transaction.destination.fragment,
            &transaction.destination.parent_directory_id,
            &transaction.destination.name,
            transaction.destination_layout,
        );
        let mut roots = BTreeSet::new();
        roots.insert(self.root_for_partition(source_partition).clone());
        roots.insert(self.root_for_partition(destination_partition).clone());
        let name = format!(
            "{}{}",
            hex::encode(Sha256::digest(transaction.mutation_id.as_bytes())),
            TRANSACTION_SUFFIX
        );
        roots
            .into_iter()
            .map(|root| root.join(STORE_DIR).join("transactions").join(&name))
            .collect()
    }

    fn entry_path(&self, key: &DentryKey, layout: DirectoryLayout) -> PathBuf {
        let partition =
            virtual_partition(&key.fragment, &key.parent_directory_id, &key.name, layout);
        let root = self.root_for_partition(partition);
        parent_entry_dir(root, partition, &key.fragment, &key.parent_directory_id).join(format!(
            "{}{}",
            hex::encode(Sha256::digest(key.name.as_bytes())),
            ENTRY_SUFFIX
        ))
    }

    fn root_for_partition(&self, partition: u64) -> &PathBuf {
        let index = if self.roots.len() == 1 {
            0
        } else {
            let hash = Sha256::digest(partition.to_be_bytes());
            (u64::from_be_bytes(hash[..8].try_into().expect("sha256 prefix")) as usize)
                % self.roots.len()
        };
        &self.roots[index]
    }
}

pub fn root_directory_id(fragment: &str) -> String {
    stable_id(b"n2-namespace-root", fragment)
}

pub fn trash_directory_id(fragment: &str) -> String {
    stable_id(b"n2-namespace-trash", fragment)
}

pub fn new_directory_id() -> String {
    ulid::Ulid::new().to_string()
}

pub fn virtual_partition(
    fragment: &str,
    parent_directory_id: &str,
    name: &str,
    layout: DirectoryLayout,
) -> u64 {
    let bucket = directory_bucket(name, layout);
    virtual_partition_for_bucket(fragment, parent_directory_id, bucket)
}

pub fn directory_bucket(name: &str, layout: DirectoryLayout) -> u32 {
    if layout.shard_bits == 0 {
        return 0;
    }
    let hash = Sha256::digest(name.as_bytes());
    let prefix = u32::from_be_bytes(hash[..4].try_into().expect("sha256 prefix"));
    prefix >> (32 - layout.shard_bits)
}

pub fn virtual_partition_for_bucket(fragment: &str, parent_directory_id: &str, bucket: u32) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-namespace-virtual-partition-v2");
    hasher.update([0]);
    hasher.update(fragment.as_bytes());
    hasher.update([0]);
    hasher.update(parent_directory_id.as_bytes());
    hasher.update([0]);
    hasher.update(bucket.to_be_bytes());
    let hash = hasher.finalize();
    u64::from_be_bytes(hash[..8].try_into().expect("sha256 prefix"))
}

fn parent_entry_dir(
    root: &Path,
    partition: u64,
    fragment: &str,
    parent_directory_id: &str,
) -> PathBuf {
    root.join(STORE_DIR)
        .join("partitions")
        .join(format!("{partition:016x}"))
        .join(hex::encode(Sha256::digest(fragment.as_bytes())))
        .join(parent_directory_id)
}

fn stable_id(domain: &[u8], fragment: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(fragment.as_bytes());
    hex::encode(&hasher.finalize()[..16])
}

fn lock_stripe(key: &DentryKey) -> usize {
    let mut hasher = Sha256::new();
    hasher.update(key.fragment.as_bytes());
    hasher.update([0]);
    hasher.update(key.parent_directory_id.as_bytes());
    hasher.update([0]);
    hasher.update(key.name.as_bytes());
    usize::from(hasher.finalize()[0]) % LOCK_STRIPES
}

fn validate_key(key: &DentryKey) -> Result<(), AppError> {
    validate_component("fragment", &key.fragment)?;
    validate_component("parent directory id", &key.parent_directory_id)?;
    validate_name(&key.name)
}

fn validate_target(target: &DentryTarget) -> Result<(), AppError> {
    match target {
        DentryTarget::Directory {
            directory_id,
            layout,
        } => {
            validate_component("directory id", directory_id)?;
            layout.validate()?;
        }
        DentryTarget::Object { object_id } => validate_component("object id", object_id)?,
    }
    Ok(())
}

fn validate_component(label: &str, value: &str) -> Result<(), AppError> {
    if value.is_empty() || value.len() > 256 || value.contains(['/', '\0']) {
        return Err(AppError::BadRequest(format!("invalid {label}")));
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), AppError> {
    validate_component("namespace entry name", name)?;
    if name == "." || name == ".." {
        return Err(AppError::BadRequest(
            "namespace entry name cannot be '.' or '..'".into(),
        ));
    }
    Ok(())
}

fn validate_mutation_id(mutation_id: &str) -> Result<(), AppError> {
    validate_component("namespace mutation id", mutation_id)
}

fn read_record(path: &Path, expected_key: &DentryKey) -> Result<Option<NamespaceDentry>, AppError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(io_error(
                err,
                format!("read namespace entry {}", path.display()),
            ));
        }
    };
    let record: NamespaceDentry = rmp_serde::from_slice(&bytes)?;
    if &record.key != expected_key {
        return Err(AppError::InternalServerError(format!(
            "namespace key hash collision or misplaced entry at {}",
            path.display()
        )));
    }
    Ok(Some(record))
}

fn write_record_atomic(path: &Path, record: &NamespaceDentry) -> Result<(), AppError> {
    validate_key(&record.key)?;
    validate_target(&record.target)?;
    if record.generation == 0 {
        return Err(AppError::InternalServerError(
            "namespace generation cannot be zero".into(),
        ));
    }
    let bytes = rmp_serde::to_vec_named(record)?;
    write_bytes_atomic(path, &bytes)
}

fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("namespace path {} has no parent", path.display()))
    })?;
    fs::create_dir_all(parent).map_err(|err| {
        io_error(
            err,
            format!("create namespace directory {}", parent.display()),
        )
    })?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("namespace"),
        ulid::Ulid::new()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|err| {
            io_error(
                err,
                format!("create namespace temporary file {}", temporary.display()),
            )
        })?;
    file.write_all(bytes).map_err(|err| {
        io_error(
            err,
            format!("write namespace temporary file {}", temporary.display()),
        )
    })?;
    file.sync_all().map_err(|err| {
        io_error(
            err,
            format!("sync namespace temporary file {}", temporary.display()),
        )
    })?;
    fs::rename(&temporary, path)
        .map_err(|err| io_error(err, format!("publish namespace entry {}", path.display())))?;
    sync_parent(path)
}

fn sync_parent(path: &Path) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("namespace path {} has no parent", path.display()))
    })?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|err| {
            io_error(
                err,
                format!("sync namespace directory {}", parent.display()),
            )
        })
}

fn io_error(err: std::io::Error, context: String) -> AppError {
    AppError::Io(err, context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_roots(label: &str, count: usize) -> Vec<PathBuf> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        (0..count)
            .map(|index| {
                std::env::temp_dir().join(format!(
                    "n2-dentry-{label}-{}-{stamp}-{index}",
                    std::process::id()
                ))
            })
            .collect()
    }

    fn key(parent: &str, name: &str) -> DentryKey {
        DentryKey {
            fragment: "fragment-a".into(),
            parent_directory_id: parent.into(),
            name: name.into(),
        }
    }

    fn object(id: &str) -> DentryTarget {
        DentryTarget::Object {
            object_id: id.into(),
        }
    }

    #[test]
    fn stable_ids_and_partitions_are_deterministic() {
        assert_eq!(root_directory_id("a"), root_directory_id("a"));
        assert_ne!(root_directory_id("a"), root_directory_id("b"));
        assert_ne!(root_directory_id("a"), trash_directory_id("a"));
        let layout = DirectoryLayout {
            shard_bits: 8,
            epoch: 1,
        };
        assert_eq!(
            virtual_partition("a", "parent", "file", layout),
            virtual_partition("a", "parent", "file", layout)
        );
    }

    #[test]
    fn create_is_durable_and_idempotent() {
        let roots = test_roots("create", 2);
        let store = NamespaceDentryStore::new(roots.clone()).expect("store");
        let entry = store
            .create(
                key("root", "a.mp4"),
                DirectoryLayout::default(),
                object("o1"),
                "m1",
                10,
            )
            .expect("create");
        assert_eq!(entry.generation, 1);
        let retry = store
            .create(
                key("root", "a.mp4"),
                DirectoryLayout::default(),
                object("o1"),
                "m1",
                10,
            )
            .expect("retry");
        assert_eq!(entry, retry);
        drop(store);
        let reopened = NamespaceDentryStore::new(roots.clone()).expect("reopen");
        assert_eq!(
            reopened
                .get(&key("root", "a.mp4"), DirectoryLayout::default())
                .expect("read"),
            Some(entry)
        );
        for root in roots {
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn directory_rename_moves_only_one_dentry() {
        let roots = test_roots("rename", 2);
        let store = NamespaceDentryStore::new(roots.clone()).expect("store");
        let directory_id = new_directory_id();
        let source = key("root", "videos");
        let destination = key("root", "media");
        store
            .create(
                source.clone(),
                DirectoryLayout::default(),
                DentryTarget::Directory {
                    directory_id: directory_id.clone(),
                    layout: DirectoryLayout::default(),
                },
                "mkdir",
                10,
            )
            .expect("create dir");
        store
            .create(
                key(&directory_id, "movie.mp4"),
                DirectoryLayout::default(),
                object("object-a"),
                "upload",
                11,
            )
            .expect("create child");
        let renamed = store
            .rename(
                source.clone(),
                DirectoryLayout::default(),
                destination.clone(),
                DirectoryLayout::default(),
                1,
                "rename",
                12,
                None,
            )
            .expect("rename");
        assert_eq!(renamed.generation, 2);
        assert_eq!(
            renamed.target,
            DentryTarget::Directory {
                directory_id: directory_id.clone(),
                layout: DirectoryLayout::default(),
            }
        );
        assert!(
            store
                .get(&source, DirectoryLayout::default())
                .expect("source")
                .is_none()
        );
        assert!(
            store
                .get(&destination, DirectoryLayout::default())
                .expect("destination")
                .is_some()
        );
        assert_eq!(
            store
                .get(&key(&directory_id, "movie.mp4"), DirectoryLayout::default())
                .expect("child")
                .expect("child exists")
                .target,
            object("object-a")
        );
        for root in roots {
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn trash_and_restore_preserve_target_identity() {
        let roots = test_roots("trash", 2);
        let store = NamespaceDentryStore::new(roots.clone()).expect("store");
        let root = root_directory_id("fragment-a");
        let trash_root = trash_directory_id("fragment-a");
        let live = key(&root, "movie.mp4");
        store
            .create(
                live.clone(),
                DirectoryLayout::default(),
                object("o1"),
                "upload",
                10,
            )
            .expect("create");
        let trashed_key = key(&trash_root, "trash-1");
        let trashed = store
            .rename(
                live.clone(),
                DirectoryLayout::default(),
                trashed_key.clone(),
                DirectoryLayout::default(),
                1,
                "trash",
                20,
                Some(TrashContext {
                    original_parent_directory_id: root.clone(),
                    original_parent_layout: DirectoryLayout::default(),
                    original_name: "movie.mp4".into(),
                    original_path: "movie.mp4".into(),
                    deleted_at_unix: 20,
                }),
            )
            .expect("trash");
        assert_eq!(trashed.target, object("o1"));
        assert!(
            store
                .get(&live, DirectoryLayout::default())
                .expect("live")
                .is_none()
        );
        let restored = store
            .rename(
                trashed_key.clone(),
                DirectoryLayout::default(),
                live.clone(),
                DirectoryLayout::default(),
                2,
                "restore",
                30,
                None,
            )
            .expect("restore");
        assert_eq!(restored.target, object("o1"));
        assert!(restored.trash.is_none());
        assert!(
            store
                .get(&trashed_key, DirectoryLayout::default())
                .expect("trash")
                .is_none()
        );
        for root in roots {
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn generation_conflict_cannot_overwrite_destination() {
        let roots = test_roots("generation", 1);
        let store = NamespaceDentryStore::new(roots.clone()).expect("store");
        let source = key("root", "a");
        store
            .create(
                source.clone(),
                DirectoryLayout::default(),
                object("o1"),
                "create",
                1,
            )
            .expect("create");
        let error = store
            .rename(
                source.clone(),
                DirectoryLayout::default(),
                key("root", "b"),
                DirectoryLayout::default(),
                99,
                "rename",
                2,
                None,
            )
            .expect_err("generation conflict");
        assert!(matches!(error, AppError::Conflict(_)));
        assert!(
            store
                .get(&source, DirectoryLayout::default())
                .expect("source")
                .is_some()
        );
        for root in roots {
            let _ = fs::remove_dir_all(root);
        }
    }
}
