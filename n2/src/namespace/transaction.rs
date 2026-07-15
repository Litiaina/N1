use crate::enums::app_error_enum::AppError;
use crate::namespace::dentry::{
    DentryKey, DirectoryLayout, NamespaceDentry, NamespaceDentryStore, virtual_partition,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const NAMESPACE_ROOT: &str = ".n2/namespace";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NamespaceTransactionLeg {
    Insert {
        record: NamespaceDentry,
        parent_layout: DirectoryLayout,
    },
    Remove {
        key: DentryKey,
        parent_layout: DirectoryLayout,
        expected_generation: u64,
    },
}

impl NamespaceTransactionLeg {
    pub fn key(&self) -> &DentryKey {
        match self {
            Self::Insert { record, .. } => &record.key,
            Self::Remove { key, .. } => key,
        }
    }

    pub fn parent_layout(&self) -> DirectoryLayout {
        match self {
            Self::Insert { parent_layout, .. } | Self::Remove { parent_layout, .. } => {
                *parent_layout
            }
        }
    }

    pub fn virtual_partition(&self) -> u64 {
        let key = self.key();
        virtual_partition(
            &key.fragment,
            &key.parent_directory_id,
            &key.name,
            self.parent_layout(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedNamespacePartition {
    pub transaction_id: String,
    pub virtual_partition: u64,
    pub prepared_at_unix: i64,
    pub legs: Vec<NamespaceTransactionLeg>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceCommitDecision {
    pub transaction_id: String,
    pub participant_partitions: Vec<u64>,
    pub mutation_digest: String,
    pub partition_digests: BTreeMap<u64, String>,
    pub committed_at_unix: i64,
}

#[derive(Clone)]
pub struct NamespaceTransactionStore {
    dentries: NamespaceDentryStore,
}

impl NamespaceTransactionStore {
    pub fn new(dentries: NamespaceDentryStore) -> Self {
        Self { dentries }
    }

    pub fn prepare_partition(&self, prepared: &PreparedNamespacePartition) -> Result<(), AppError> {
        validate_prepared(prepared)?;
        let root = self.dentries.partition_root(prepared.virtual_partition);
        let mut claimed = Vec::new();
        for leg in sorted_legs(&prepared.legs) {
            let lock = lock_path(root, prepared.virtual_partition, leg.key());
            match claim_lock(
                root,
                prepared.virtual_partition,
                leg.key(),
                &prepared.transaction_id,
            ) {
                Ok(()) => claimed.push(lock),
                Err(err) => {
                    for path in claimed {
                        let _ = release_lock(&path, &prepared.transaction_id);
                    }
                    return Err(err);
                }
            }
        }

        let validation = prepared
            .legs
            .iter()
            .try_for_each(|leg| self.validate_leg(leg));
        if let Err(err) = validation {
            for leg in &prepared.legs {
                let _ = release_lock(
                    &lock_path(root, prepared.virtual_partition, leg.key()),
                    &prepared.transaction_id,
                );
            }
            return Err(err);
        }

        let bytes = rmp_serde::to_vec_named(prepared)?;
        write_immutable_or_equal(&intent_path(root, prepared), &bytes)
    }

    pub fn commit_partition(
        &self,
        transaction_id: &str,
        virtual_partition: u64,
        expected_digest: &str,
    ) -> Result<PreparedNamespacePartition, AppError> {
        let root = self.dentries.partition_root(virtual_partition);
        let path = intent_path_for(root, transaction_id, virtual_partition);
        let bytes = fs::read(&path).map_err(|err| {
            AppError::Io(err, format!("read namespace intent {}", path.display()))
        })?;
        let prepared: PreparedNamespacePartition = rmp_serde::from_slice(&bytes)?;
        validate_prepared(&prepared)?;
        if prepared.transaction_id != transaction_id
            || prepared.virtual_partition != virtual_partition
        {
            return Err(AppError::InternalServerError(
                "namespace intent identity mismatch".into(),
            ));
        }
        if prepared_partition_digest(&prepared)? != expected_digest {
            return Err(AppError::Conflict(
                "namespace prepared partition does not match commit decision".into(),
            ));
        }

        for leg in prepared
            .legs
            .iter()
            .filter(|leg| matches!(leg, NamespaceTransactionLeg::Insert { .. }))
        {
            if let NamespaceTransactionLeg::Insert {
                record,
                parent_layout,
            } = leg
            {
                self.dentries
                    .install_prepared_record(record.clone(), *parent_layout)?;
            }
        }
        for leg in prepared
            .legs
            .iter()
            .filter(|leg| matches!(leg, NamespaceTransactionLeg::Remove { .. }))
        {
            if let NamespaceTransactionLeg::Remove {
                key,
                parent_layout,
                expected_generation,
            } = leg
            {
                match self.dentries.remove(
                    key,
                    *parent_layout,
                    *expected_generation,
                    transaction_id,
                ) {
                    Ok(_) | Err(AppError::NotFound) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        self.finish_intent(&prepared)?;
        Ok(prepared)
    }

    pub fn commit_prepared_partition(
        &self,
        prepared: &PreparedNamespacePartition,
        expected_digest: &str,
    ) -> Result<PreparedNamespacePartition, AppError> {
        validate_prepared(prepared)?;
        if prepared_partition_digest(prepared)? != expected_digest {
            return Err(AppError::Conflict(
                "namespace replay payload does not match commit decision".into(),
            ));
        }
        if self.partition_is_applied(prepared)? {
            return Ok(prepared.clone());
        }
        self.prepare_partition(prepared)?;
        self.commit_partition(
            &prepared.transaction_id,
            prepared.virtual_partition,
            expected_digest,
        )
    }

    pub fn abort_partition(
        &self,
        transaction_id: &str,
        virtual_partition: u64,
    ) -> Result<(), AppError> {
        let root = self.dentries.partition_root(virtual_partition);
        let path = intent_path_for(root, transaction_id, virtual_partition);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("read namespace intent {}", path.display()),
                ));
            }
        };
        let prepared: PreparedNamespacePartition = rmp_serde::from_slice(&bytes)?;
        self.finish_intent(&prepared)
    }

    pub fn intent_for_key(
        &self,
        key: &DentryKey,
        parent_layout: DirectoryLayout,
    ) -> Result<Option<PreparedNamespacePartition>, AppError> {
        let partition = virtual_partition(
            &key.fragment,
            &key.parent_directory_id,
            &key.name,
            parent_layout,
        );
        let root = self.dentries.partition_root(partition);
        let lock = lock_path(root, partition, key);
        let transaction_id = match fs::read_to_string(&lock) {
            Ok(value) => value,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("read namespace lock {}", lock.display()),
                ));
            }
        };
        let path = intent_path_for(root, transaction_id.trim(), partition);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("read namespace intent {}", path.display()),
                ));
            }
        };
        Ok(Some(rmp_serde::from_slice(&bytes)?))
    }

    pub fn intents_for_partition(
        &self,
        virtual_partition: u64,
    ) -> Result<Vec<PreparedNamespacePartition>, AppError> {
        let root = self.dentries.partition_root(virtual_partition);
        let dir = root.join(NAMESPACE_ROOT).join("intents");
        let read_dir = match fs::read_dir(&dir) {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("scan namespace intents {}", dir.display()),
                ));
            }
        };
        let suffix = format!("-{virtual_partition:016x}.intent");
        let mut prepared = Vec::new();
        for child in read_dir {
            let child = child.map_err(|err| {
                AppError::Io(err, format!("scan namespace intents {}", dir.display()))
            })?;
            let name = child.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(&suffix) {
                continue;
            }
            let bytes = match fs::read(child.path()) {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(AppError::Io(
                        err,
                        format!("read namespace intent {}", child.path().display()),
                    ));
                }
            };
            let intent: PreparedNamespacePartition = rmp_serde::from_slice(&bytes)?;
            validate_prepared(&intent)?;
            prepared.push(intent);
        }
        prepared.sort_by(|left, right| left.transaction_id.cmp(&right.transaction_id));
        Ok(prepared)
    }

    pub fn persist_decision(
        &self,
        virtual_partition: u64,
        decision: &NamespaceCommitDecision,
    ) -> Result<(), AppError> {
        validate_decision(decision)?;
        let root = self.dentries.partition_root(virtual_partition);
        let bytes = rmp_serde::to_vec_named(decision)?;
        write_immutable_or_equal(&decision_path(root, decision), &bytes)
    }

    pub fn read_decision(
        &self,
        virtual_partition: u64,
        transaction_id: &str,
    ) -> Result<Option<NamespaceCommitDecision>, AppError> {
        let root = self.dentries.partition_root(virtual_partition);
        let path = decision_path_for(root, transaction_id);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("read namespace decision {}", path.display()),
                ));
            }
        };
        Ok(Some(rmp_serde::from_slice(&bytes)?))
    }

    fn validate_leg(&self, leg: &NamespaceTransactionLeg) -> Result<(), AppError> {
        match leg {
            NamespaceTransactionLeg::Insert {
                record,
                parent_layout,
            } => match self.dentries.get(&record.key, *parent_layout)? {
                None => Ok(()),
                Some(existing) if existing == *record => Ok(()),
                Some(_) => Err(AppError::Conflict(format!(
                    "namespace destination '{}' already exists",
                    record.key.name
                ))),
            },
            NamespaceTransactionLeg::Remove {
                key,
                parent_layout,
                expected_generation,
            } => match self.dentries.get(key, *parent_layout)? {
                Some(existing) if existing.generation == *expected_generation => Ok(()),
                Some(existing) => Err(AppError::Conflict(format!(
                    "namespace generation changed: expected {}, found {}",
                    expected_generation, existing.generation
                ))),
                None => Err(AppError::NotFound),
            },
        }
    }

    fn partition_is_applied(
        &self,
        prepared: &PreparedNamespacePartition,
    ) -> Result<bool, AppError> {
        for leg in &prepared.legs {
            let applied = match leg {
                NamespaceTransactionLeg::Insert {
                    record,
                    parent_layout,
                } => self.dentries.get(&record.key, *parent_layout)?.as_ref() == Some(record),
                NamespaceTransactionLeg::Remove {
                    key, parent_layout, ..
                } => self.dentries.get(key, *parent_layout)?.is_none(),
            };
            if !applied {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn finish_intent(&self, prepared: &PreparedNamespacePartition) -> Result<(), AppError> {
        let root = self.dentries.partition_root(prepared.virtual_partition);
        let path = intent_path(root, prepared);
        match fs::remove_file(&path) {
            Ok(()) => sync_parent(&path)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("remove namespace intent {}", path.display()),
                ));
            }
        }
        for leg in &prepared.legs {
            release_lock(
                &lock_path(root, prepared.virtual_partition, leg.key()),
                &prepared.transaction_id,
            )?;
        }
        Ok(())
    }
}

pub fn decision_partition(transaction_id: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-namespace-transaction-decision-v2");
    hasher.update([0]);
    hasher.update(transaction_id.as_bytes());
    let hash = hasher.finalize();
    u64::from_be_bytes(hash[..8].try_into().expect("sha256 prefix"))
}

pub fn mutation_digest(partitions: &[PreparedNamespacePartition]) -> Result<String, AppError> {
    let mut canonical = partitions.to_vec();
    canonical.sort_by_key(|prepared| prepared.virtual_partition);
    Ok(hex::encode(Sha256::digest(rmp_serde::to_vec_named(
        &canonical,
    )?)))
}

pub fn prepared_partition_digest(
    prepared: &PreparedNamespacePartition,
) -> Result<String, AppError> {
    Ok(hex::encode(Sha256::digest(rmp_serde::to_vec_named(
        prepared,
    )?)))
}

pub fn prepared_placement_epoch(prepared: &PreparedNamespacePartition) -> Result<u64, AppError> {
    let epochs = prepared
        .legs
        .iter()
        .map(|leg| leg.parent_layout().epoch)
        .collect::<BTreeSet<_>>();
    if epochs.len() != 1 {
        return Err(AppError::BadRequest(
            "namespace partition transaction spans placement epochs".into(),
        ));
    }
    epochs
        .into_iter()
        .next()
        .ok_or_else(|| AppError::BadRequest("namespace transaction has no placement epoch".into()))
}

fn validate_prepared(prepared: &PreparedNamespacePartition) -> Result<(), AppError> {
    if prepared.transaction_id.is_empty() || prepared.legs.is_empty() {
        return Err(AppError::BadRequest(
            "namespace prepared transaction requires identity and legs".into(),
        ));
    }
    let mut keys = BTreeSet::new();
    for leg in &prepared.legs {
        if leg.virtual_partition() != prepared.virtual_partition {
            return Err(AppError::BadRequest(
                "namespace transaction leg belongs to another partition".into(),
            ));
        }
        let key = leg.key();
        if !keys.insert((
            key.fragment.clone(),
            key.parent_directory_id.clone(),
            key.name.clone(),
        )) {
            return Err(AppError::BadRequest(
                "namespace transaction contains duplicate keys".into(),
            ));
        }
    }
    prepared_placement_epoch(prepared)?;
    Ok(())
}

fn validate_decision(decision: &NamespaceCommitDecision) -> Result<(), AppError> {
    if decision.transaction_id.is_empty()
        || decision.participant_partitions.is_empty()
        || decision.mutation_digest.len() != 64
        || decision.partition_digests.len() != decision.participant_partitions.len()
        || decision
            .participant_partitions
            .iter()
            .any(|partition| !decision.partition_digests.contains_key(partition))
    {
        return Err(AppError::BadRequest(
            "invalid namespace transaction decision".into(),
        ));
    }
    Ok(())
}

fn sorted_legs(legs: &[NamespaceTransactionLeg]) -> Vec<&NamespaceTransactionLeg> {
    let mut legs = legs.iter().collect::<Vec<_>>();
    legs.sort_by(|left, right| {
        let left = left.key();
        let right = right.key();
        left.fragment
            .cmp(&right.fragment)
            .then(left.parent_directory_id.cmp(&right.parent_directory_id))
            .then(left.name.cmp(&right.name))
    });
    legs
}

fn lock_path(root: &Path, partition: u64, key: &DentryKey) -> PathBuf {
    root.join(NAMESPACE_ROOT)
        .join("locks")
        .join(format!("{partition:016x}"))
        .join(format!(
            "{}.lock",
            hex::encode(Sha256::digest(
                rmp_serde::to_vec_named(key).unwrap_or_default()
            ))
        ))
}

fn intent_path(root: &Path, prepared: &PreparedNamespacePartition) -> PathBuf {
    intent_path_for(root, &prepared.transaction_id, prepared.virtual_partition)
}

fn intent_path_for(root: &Path, transaction_id: &str, partition: u64) -> PathBuf {
    root.join(NAMESPACE_ROOT).join("intents").join(format!(
        "{}-{partition:016x}.intent",
        hex::encode(Sha256::digest(transaction_id.as_bytes()))
    ))
}

fn decision_path(root: &Path, decision: &NamespaceCommitDecision) -> PathBuf {
    decision_path_for(root, &decision.transaction_id)
}

fn decision_path_for(root: &Path, transaction_id: &str) -> PathBuf {
    root.join(NAMESPACE_ROOT).join("decisions").join(format!(
        "{}.decision",
        hex::encode(Sha256::digest(transaction_id.as_bytes()))
    ))
}

fn claim_lock(
    root: &Path,
    partition: u64,
    key: &DentryKey,
    transaction_id: &str,
) -> Result<(), AppError> {
    let path = lock_path(root, partition, key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            AppError::Io(
                err,
                format!("create namespace lock dir {}", parent.display()),
            )
        })?;
    }

    let mut cleaned_stale_lock = false;
    loop {
        match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(mut file) => {
                file.write_all(transaction_id.as_bytes()).map_err(|err| {
                    AppError::Io(err, format!("write namespace lock {}", path.display()))
                })?;
                file.sync_all().map_err(|err| {
                    AppError::Io(err, format!("sync namespace lock {}", path.display()))
                })?;
                return sync_parent(&path);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let owner = fs::read_to_string(&path).map_err(|read_err| {
                    AppError::Io(read_err, format!("read namespace lock {}", path.display()))
                })?;
                let owner = owner.trim();
                if owner == transaction_id {
                    return Ok(());
                }
                let owner_intent = intent_path_for(root, owner, partition);
                if !cleaned_stale_lock && !owner_intent.exists() {
                    match fs::remove_file(&path) {
                        Ok(()) => {
                            sync_parent(&path)?;
                            cleaned_stale_lock = true;
                            continue;
                        }
                        Err(remove_err) if remove_err.kind() == std::io::ErrorKind::NotFound => {
                            cleaned_stale_lock = true;
                            continue;
                        }
                        Err(remove_err) => {
                            return Err(AppError::Io(
                                remove_err,
                                format!("remove stale namespace lock {}", path.display()),
                            ));
                        }
                    }
                }
                return Err(AppError::Conflict(format!(
                    "namespace key is prepared by transaction {owner}"
                )));
            }
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("claim namespace lock {}", path.display()),
                ));
            }
        }
    }
}

fn release_lock(path: &Path, transaction_id: &str) -> Result<(), AppError> {
    match fs::read_to_string(path) {
        Ok(owner) if owner == transaction_id => {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(err) => {
                    return Err(AppError::Io(
                        err,
                        format!("remove namespace lock {}", path.display()),
                    ));
                }
            }
            sync_parent(path)
        }
        Ok(_) => Err(AppError::Conflict(
            "namespace lock is owned by another transaction".into(),
        )),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(AppError::Io(
            err,
            format!("read namespace lock {}", path.display()),
        )),
    }
}

fn write_immutable_or_equal(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            AppError::Io(
                err,
                format!("create namespace transaction dir {}", parent.display()),
            )
        })?;
    }
    match OpenOptions::new().create_new(true).write(true).open(path) {
        Ok(mut file) => {
            file.write_all(bytes).map_err(|err| {
                AppError::Io(
                    err,
                    format!("write namespace transaction {}", path.display()),
                )
            })?;
            file.sync_all().map_err(|err| {
                AppError::Io(
                    err,
                    format!("sync namespace transaction {}", path.display()),
                )
            })?;
            sync_parent(path)
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = fs::read(path).map_err(|read_err| {
                AppError::Io(
                    read_err,
                    format!("read namespace transaction {}", path.display()),
                )
            })?;
            if existing == bytes {
                Ok(())
            } else {
                Err(AppError::Conflict(
                    "namespace transaction identity has divergent content".into(),
                ))
            }
        }
        Err(err) => Err(AppError::Io(
            err,
            format!("create namespace transaction {}", path.display()),
        )),
    }
}

fn sync_parent(path: &Path) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("path {} has no parent", path.display()))
    })?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|err| AppError::Io(err, format!("sync directory {}", parent.display())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::dentry::DentryTarget;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn store(label: &str) -> (NamespaceDentryStore, Vec<PathBuf>) {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let roots = vec![std::env::temp_dir().join(format!(
            "n2-transaction-{label}-{}-{stamp}",
            std::process::id()
        ))];
        (
            NamespaceDentryStore::new(roots.clone()).expect("store"),
            roots,
        )
    }

    fn key(name: &str) -> DentryKey {
        DentryKey {
            fragment: "fragment".into(),
            parent_directory_id: "root".into(),
            name: name.into(),
        }
    }

    #[test]
    fn prepared_insert_is_invisible_until_commit() {
        let (dentries, roots) = store("prepare");
        let transactions = NamespaceTransactionStore::new(dentries.clone());
        let record = NamespaceDentry {
            key: key("file"),
            target: DentryTarget::Object {
                object_id: "object".into(),
            },
            generation: 1,
            last_mutation_id: "tx".into(),
            created_at_unix: 1,
            updated_at_unix: 1,
            trash: None,
        };
        let partition = virtual_partition(
            &record.key.fragment,
            &record.key.parent_directory_id,
            &record.key.name,
            DirectoryLayout::default(),
        );
        let prepared = PreparedNamespacePartition {
            transaction_id: "tx".into(),
            virtual_partition: partition,
            prepared_at_unix: 1,
            legs: vec![NamespaceTransactionLeg::Insert {
                record: record.clone(),
                parent_layout: DirectoryLayout::default(),
            }],
        };
        transactions.prepare_partition(&prepared).expect("prepare");
        assert!(
            dentries
                .get(&record.key, DirectoryLayout::default())
                .expect("read")
                .is_none()
        );
        transactions
            .commit_partition(
                "tx",
                partition,
                &prepared_partition_digest(&prepared).expect("digest"),
            )
            .expect("commit");
        assert_eq!(
            dentries
                .get(&record.key, DirectoryLayout::default())
                .expect("read"),
            Some(record)
        );
        for root in roots {
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn quorum_certified_payload_repairs_a_missing_intent() {
        let (dentries, roots) = store("replay");
        let transactions = NamespaceTransactionStore::new(dentries.clone());
        let record = NamespaceDentry {
            key: key("replayed"),
            target: DentryTarget::Object {
                object_id: "object-replayed".into(),
            },
            generation: 1,
            last_mutation_id: "tx-replay".into(),
            created_at_unix: 1,
            updated_at_unix: 1,
            trash: None,
        };
        let prepared = PreparedNamespacePartition {
            transaction_id: "tx-replay".into(),
            virtual_partition: virtual_partition(
                &record.key.fragment,
                &record.key.parent_directory_id,
                &record.key.name,
                DirectoryLayout::default(),
            ),
            prepared_at_unix: 1,
            legs: vec![NamespaceTransactionLeg::Insert {
                record: record.clone(),
                parent_layout: DirectoryLayout::default(),
            }],
        };
        let digest = prepared_partition_digest(&prepared).expect("digest");

        transactions
            .commit_prepared_partition(&prepared, &digest)
            .expect("replay missing intent");
        transactions
            .commit_prepared_partition(&prepared, &digest)
            .expect("idempotent replay");
        assert_eq!(
            dentries
                .get(&record.key, DirectoryLayout::default())
                .expect("read"),
            Some(record)
        );
        for root in roots {
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn conflicting_prepare_is_rejected() {
        let (dentries, roots) = store("conflict");
        let transactions = NamespaceTransactionStore::new(dentries);
        let record = NamespaceDentry {
            key: key("file"),
            target: DentryTarget::Object {
                object_id: "object".into(),
            },
            generation: 1,
            last_mutation_id: "tx-a".into(),
            created_at_unix: 1,
            updated_at_unix: 1,
            trash: None,
        };
        let partition = virtual_partition(
            &record.key.fragment,
            &record.key.parent_directory_id,
            &record.key.name,
            DirectoryLayout::default(),
        );
        let mut prepared = PreparedNamespacePartition {
            transaction_id: "tx-a".into(),
            virtual_partition: partition,
            prepared_at_unix: 1,
            legs: vec![NamespaceTransactionLeg::Insert {
                record,
                parent_layout: DirectoryLayout::default(),
            }],
        };
        transactions
            .prepare_partition(&prepared)
            .expect("first prepare");
        prepared.transaction_id = "tx-b".into();
        assert!(matches!(
            transactions.prepare_partition(&prepared),
            Err(AppError::Conflict(_))
        ));
        for root in roots {
            let _ = fs::remove_dir_all(root);
        }
    }
}
