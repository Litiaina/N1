use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use ulid::Ulid;

const INODE_FORMAT: &str = "n2-directory-inode-v2";
const INODE_FILE: &str = ".inode";
const NAMESPACE_DIR: &str = ".namespace";
const DENTRY_DIR: &str = ".dentries";
const CLAIM_DIR: &str = ".claims";
const DECISION_DIR: &str = ".decisions";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InodeReplica {
    pub node_id: String,
    pub root_id: String,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_endpoint: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InodeLayout {
    pub generation: u64,
    pub replicas: Vec<InodeReplica>,
}

impl InodeLayout {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.generation == 0 || self.replicas.len() < 2 {
            return Err(AppError::BadRequest(
                "inode layout requires a positive generation and at least two replicas".into(),
            ));
        }
        let mut nodes = BTreeSet::new();
        let mut roots = BTreeSet::new();
        for replica in &self.replicas {
            if replica.node_id.trim().is_empty()
                || replica.root_id.trim().is_empty()
                || replica.endpoint.trim().is_empty()
                || !nodes.insert(replica.node_id.clone())
                || !roots.insert((replica.node_id.clone(), replica.root_id.clone()))
            {
                return Err(AppError::BadRequest(
                    "inode replicas require distinct node IDs and valid root identities".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectoryInode {
    pub format: String,
    pub fragment_id: String,
    pub inode_id: String,
    pub parent_inode_id: Option<String>,
    pub layout: InodeLayout,
    pub generation: u64,
    pub last_mutation_id: String,
    pub last_mutation_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_certificate: Option<InodeCommitCertificate>,
    pub created_at_unix_nanos: i64,
    pub updated_at_unix_nanos: i64,
}

impl DirectoryInode {
    pub fn new(
        fragment_id: String,
        inode_id: String,
        parent_inode_id: Option<String>,
        layout: InodeLayout,
        now_unix_nanos: i64,
    ) -> Result<Self, AppError> {
        layout.validate()?;
        validate_identity("fragment", &fragment_id)?;
        validate_identity("inode", &inode_id)?;
        if let Some(parent) = parent_inode_id.as_deref() {
            validate_identity("parent inode", parent)?;
            if parent == inode_id {
                return Err(AppError::BadRequest(
                    "directory inode cannot be its own parent".into(),
                ));
            }
        }
        Ok(Self {
            format: INODE_FORMAT.to_string(),
            fragment_id,
            inode_id,
            parent_inode_id,
            layout,
            generation: 0,
            last_mutation_id: String::new(),
            last_mutation_digest: String::new(),
            commit_certificate: None,
            created_at_unix_nanos: now_unix_nanos,
            updated_at_unix_nanos: now_unix_nanos,
        })
    }

    fn validate(&self) -> Result<(), AppError> {
        if self.format != INODE_FORMAT {
            return Err(AppError::Conflict(format!(
                "unsupported directory inode format '{}'",
                self.format
            )));
        }
        self.layout.validate()?;
        validate_identity("fragment", &self.fragment_id)?;
        validate_identity("inode", &self.inode_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InodeDentryTarget {
    Directory {
        inode_id: String,
        layout: InodeLayout,
    },
    Object {
        object_id: String,
    },
    Symlink {
        target: String,
    },
    Fragment {
        stable_fragment_id: String,
        root_inode_id: String,
        root_layout: InodeLayout,
        trash_inode_id: String,
        trash_layout: InodeLayout,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrashOrigin {
    pub parent_inode_id: String,
    pub parent_layout: InodeLayout,
    pub name: String,
    pub path: String,
    pub deleted_at_unix_nanos: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InodeDentry {
    pub name: String,
    pub target: InodeDentryTarget,
    pub generation: u64,
    pub mutation_id: String,
    pub tombstone: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trash: Option<TrashOrigin>,
    pub created_at_unix_nanos: i64,
    pub updated_at_unix_nanos: i64,
}

/// Complete committed state of one directory inode replica.
///
/// Claims and transaction decisions are deliberately excluded: a consistent
/// snapshot can only be read after all durable cross-inode decisions have
/// been finalized, and uncommitted claims are not namespace state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectoryInodeSnapshot {
    pub inode: DirectoryInode,
    pub entries: Vec<InodeDentry>,
}

impl DirectoryInodeSnapshot {
    pub fn validate(&self) -> Result<(), AppError> {
        self.inode.validate()?;
        let mut names = BTreeSet::new();
        for entry in &self.entries {
            validate_name(&entry.name)?;
            validate_target(&entry.target)?;
            if entry.tombstone
                || entry.generation == 0
                || entry.generation > self.inode.generation
                || !names.insert(entry.name.clone())
            {
                return Err(AppError::Conflict(format!(
                    "inode '{}' repair snapshot contains invalid committed dentries",
                    self.inode.inode_id
                )));
            }
        }
        Ok(())
    }
}

impl InodeDentry {
    pub fn live(
        name: String,
        target: InodeDentryTarget,
        created_at_unix_nanos: i64,
    ) -> Result<Self, AppError> {
        validate_name(&name)?;
        validate_target(&target)?;
        Ok(Self {
            name,
            target,
            generation: 0,
            mutation_id: String::new(),
            tombstone: false,
            trash: None,
            created_at_unix_nanos,
            updated_at_unix_nanos: created_at_unix_nanos,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InodeMutationChange {
    Upsert { dentry: InodeDentry },
    Remove { name: String },
}

impl InodeMutationChange {
    fn name(&self) -> &str {
        match self {
            Self::Upsert { dentry } => &dentry.name,
            Self::Remove { name } => name,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PreparedInodeMutation {
    pub fragment_id: String,
    pub inode_id: String,
    pub expected_generation: u64,
    pub target_generation: u64,
    pub mutation_id: String,
    pub mutation_digest: String,
    pub prepared_at_unix_nanos: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_parent_inode_id: Option<String>,
    pub changes: Vec<InodeMutationChange>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InodeCommitCertificate {
    pub mutation_id: String,
    pub mutation_digest: String,
    pub inode_layout_generation: u64,
    pub target_generation: u64,
    pub voter_node_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InodeTransactionParticipant {
    pub fragment_id: String,
    pub inode_id: String,
    pub layout_generation: u64,
    pub expected_generation: u64,
    pub target_generation: u64,
    pub mutation_id: String,
    pub mutation_digest: String,
}

impl InodeTransactionParticipant {
    pub fn new(layout: &InodeLayout, mutation: &PreparedInodeMutation) -> Self {
        Self {
            fragment_id: mutation.fragment_id.clone(),
            inode_id: mutation.inode_id.clone(),
            layout_generation: layout.generation,
            expected_generation: mutation.expected_generation,
            target_generation: mutation.target_generation,
            mutation_id: mutation.mutation_id.clone(),
            mutation_digest: mutation.mutation_digest.clone(),
        }
    }

    fn matches(&self, layout: &InodeLayout, mutation: &PreparedInodeMutation) -> bool {
        self == &Self::new(layout, mutation)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InodeTransactionDecision {
    pub transaction_id: String,
    pub transaction_digest: String,
    pub participants: Vec<InodeTransactionParticipant>,
}

impl InodeTransactionDecision {
    pub fn new(
        transaction_id: String,
        mut participants: Vec<InodeTransactionParticipant>,
    ) -> Result<Self, AppError> {
        validate_identity("transaction", &transaction_id)?;
        if participants.len() < 2 {
            return Err(AppError::BadRequest(
                "cross-inode transaction requires at least two participants".into(),
            ));
        }
        participants.sort_by(|left, right| left.inode_id.cmp(&right.inode_id));
        let mut inode_ids = BTreeSet::new();
        if participants
            .iter()
            .any(|participant| !inode_ids.insert(participant.inode_id.clone()))
        {
            return Err(AppError::BadRequest(
                "cross-inode transaction contains duplicate inode participants".into(),
            ));
        }
        let bytes = rmp_serde::to_vec_named(&(&transaction_id, &participants))?;
        Ok(Self {
            transaction_id,
            transaction_digest: hex::encode(Sha256::digest(bytes)),
            participants,
        })
    }

    pub fn contains(&self, layout: &InodeLayout, mutation: &PreparedInodeMutation) -> bool {
        self.participants
            .iter()
            .any(|participant| participant.matches(layout, mutation))
    }
}

impl InodeCommitCertificate {
    pub fn new(
        mutation: &PreparedInodeMutation,
        layout: &InodeLayout,
        voter_node_ids: Vec<String>,
    ) -> Result<Self, AppError> {
        let certificate = Self {
            mutation_id: mutation.mutation_id.clone(),
            mutation_digest: mutation.mutation_digest.clone(),
            inode_layout_generation: layout.generation,
            target_generation: mutation.target_generation,
            voter_node_ids,
        };
        certificate.validate(mutation, layout)?;
        Ok(certificate)
    }

    pub fn validate(
        &self,
        mutation: &PreparedInodeMutation,
        layout: &InodeLayout,
    ) -> Result<(), AppError> {
        if self.mutation_id != mutation.mutation_id
            || self.mutation_digest != mutation.mutation_digest
            || self.inode_layout_generation != layout.generation
            || self.target_generation != mutation.target_generation
        {
            return Err(AppError::Conflict(
                "inode commit certificate does not match prepared mutation".into(),
            ));
        }
        let eligible = layout
            .replicas
            .iter()
            .map(|replica| replica.node_id.as_str())
            .collect::<BTreeSet<_>>();
        let voters = self
            .voter_node_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let required = (layout.replicas.len() / 2).saturating_add(1);
        if voters.len() < required || !voters.iter().all(|node| eligible.contains(node)) {
            return Err(AppError::Conflict(format!(
                "inode commit certificate has {} valid voter(s), {} required",
                voters.len(),
                required
            )));
        }
        Ok(())
    }
}

impl PreparedInodeMutation {
    pub fn new(
        fragment_id: String,
        inode_id: String,
        expected_generation: u64,
        mutation_id: String,
        prepared_at_unix_nanos: i64,
        changes: Vec<InodeMutationChange>,
    ) -> Result<Self, AppError> {
        Self::build(
            fragment_id,
            inode_id,
            expected_generation,
            mutation_id,
            prepared_at_unix_nanos,
            None,
            changes,
        )
    }

    pub fn new_parent_move(
        fragment_id: String,
        inode_id: String,
        expected_generation: u64,
        mutation_id: String,
        prepared_at_unix_nanos: i64,
        new_parent_inode_id: String,
    ) -> Result<Self, AppError> {
        Self::build(
            fragment_id,
            inode_id,
            expected_generation,
            mutation_id,
            prepared_at_unix_nanos,
            Some(new_parent_inode_id),
            Vec::new(),
        )
    }

    fn build(
        fragment_id: String,
        inode_id: String,
        expected_generation: u64,
        mutation_id: String,
        prepared_at_unix_nanos: i64,
        new_parent_inode_id: Option<String>,
        mut changes: Vec<InodeMutationChange>,
    ) -> Result<Self, AppError> {
        validate_identity("fragment", &fragment_id)?;
        validate_identity("inode", &inode_id)?;
        validate_identity("mutation", &mutation_id)?;
        if changes.is_empty() && new_parent_inode_id.is_none() {
            return Err(AppError::BadRequest(
                "inode mutation requires a dentry or parent change".into(),
            ));
        }
        if let Some(parent) = new_parent_inode_id.as_deref() {
            validate_identity("new parent inode", parent)?;
            if parent == inode_id {
                return Err(AppError::BadRequest(
                    "directory inode cannot become its own parent".into(),
                ));
            }
        }
        changes.sort_by(|left, right| left.name().cmp(right.name()));
        let mut names = BTreeSet::new();
        for change in &changes {
            validate_name(change.name())?;
            if !names.insert(change.name().to_string()) {
                return Err(AppError::BadRequest(format!(
                    "inode mutation changes '{}' more than once",
                    change.name()
                )));
            }
            if let InodeMutationChange::Upsert { dentry } = change {
                validate_target(&dentry.target)?;
            }
        }
        let target_generation = expected_generation
            .checked_add(1)
            .ok_or_else(|| AppError::Conflict("directory inode generation exhausted".into()))?;
        let digest_bytes = rmp_serde::to_vec_named(&(
            &fragment_id,
            &inode_id,
            expected_generation,
            target_generation,
            &mutation_id,
            &new_parent_inode_id,
            &changes,
        ))?;
        let mutation_digest = hex::encode(Sha256::digest(digest_bytes));
        Ok(Self {
            fragment_id,
            inode_id,
            expected_generation,
            target_generation,
            mutation_id,
            mutation_digest,
            prepared_at_unix_nanos,
            new_parent_inode_id,
            changes,
        })
    }
}

#[derive(Clone, Debug)]
pub struct InodeStore {
    root_id: String,
    root: PathBuf,
}

impl InodeStore {
    pub fn new(root_id: String, root: PathBuf) -> Result<Self, AppError> {
        validate_identity("root", &root_id)?;
        if !root.is_absolute() {
            return Err(AppError::BadRequest(
                "inode store root must be an absolute path".into(),
            ));
        }
        let metadata = fs::metadata(&root)
            .map_err(|error| io_error(error, "inspect inode store root", &root))?;
        if !metadata.is_dir() {
            return Err(AppError::BadRequest(format!(
                "inode store root '{}' is not a directory",
                root.display()
            )));
        }
        Ok(Self { root_id, root })
    }

    pub fn root_id(&self) -> &str {
        &self.root_id
    }

    pub fn create_inode(&self, inode: &DirectoryInode) -> Result<(), AppError> {
        inode.validate()?;
        if !inode
            .layout
            .replicas
            .iter()
            .any(|replica| replica.root_id == self.root_id)
        {
            return Err(AppError::Conflict(format!(
                "inode '{}' layout does not include local root '{}'",
                inode.inode_id, self.root_id
            )));
        }
        let directory = self.inode_dir(&inode.fragment_id, &inode.inode_id);
        fs::create_dir_all(directory.join(DENTRY_DIR))
            .map_err(|error| io_error(error, "create inode dentry directory", &directory))?;
        fs::create_dir_all(directory.join(CLAIM_DIR))
            .map_err(|error| io_error(error, "create inode claim directory", &directory))?;
        fs::create_dir_all(directory.join(DECISION_DIR))
            .map_err(|error| io_error(error, "create inode decision directory", &directory))?;
        let path = directory.join(INODE_FILE);
        let bytes = rmp_serde::to_vec_named(inode)?;
        write_immutable_or_equal(&path, &bytes)
    }

    pub fn read_inode(
        &self,
        fragment_id: &str,
        inode_id: &str,
    ) -> Result<DirectoryInode, AppError> {
        let path = self.inode_dir(fragment_id, inode_id).join(INODE_FILE);
        let bytes = fs::read(&path).map_err(|error| io_error(error, "read inode", &path))?;
        let inode: DirectoryInode = rmp_serde::from_slice(&bytes)?;
        inode.validate()?;
        if inode.fragment_id != fragment_id || inode.inode_id != inode_id {
            return Err(AppError::Conflict(format!(
                "inode identity mismatch in '{}'",
                path.display()
            )));
        }
        Ok(inode)
    }

    pub fn read_inode_consistent(
        &self,
        fragment_id: &str,
        inode_id: &str,
    ) -> Result<DirectoryInode, AppError> {
        let decision_root = self.inode_dir(fragment_id, inode_id).join(DECISION_DIR);
        let mut entries = fs::read_dir(&decision_root)
            .map_err(|error| io_error(error, "inspect inode decisions", &decision_root))?;
        if entries.next().is_some() {
            return Err(AppError::ServiceUnavailable(format!(
                "inode '{inode_id}' has a committed cross-inode decision awaiting complete apply"
            )));
        }
        self.read_inode(fragment_id, inode_id)
    }

    pub fn snapshot_inode(
        &self,
        fragment_id: &str,
        inode_id: &str,
    ) -> Result<DirectoryInodeSnapshot, AppError> {
        self.ensure_repair_quiescent(fragment_id, inode_id)?;
        let before = self.read_inode_consistent(fragment_id, inode_id)?;
        let entries = self.list_dentries(fragment_id, inode_id)?;
        let after = self.read_inode_consistent(fragment_id, inode_id)?;
        self.ensure_repair_quiescent(fragment_id, inode_id)?;
        if before.generation != after.generation
            || before.last_mutation_id != after.last_mutation_id
            || before.last_mutation_digest != after.last_mutation_digest
        {
            return Err(AppError::ServiceUnavailable(format!(
                "inode '{inode_id}' changed while its repair snapshot was read"
            )));
        }
        let snapshot = DirectoryInodeSnapshot {
            inode: after,
            entries,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Restore one exact replica from a certified peer snapshot.
    ///
    /// A healthy replica at the same or a newer generation is never
    /// overwritten. On Linux the staged inode directory is exchanged with an
    /// unreadable/stale directory atomically; the old directory is then
    /// discarded. This operation never changes the recorded replica layout.
    pub fn restore_inode_snapshot(
        &self,
        snapshot: &DirectoryInodeSnapshot,
    ) -> Result<bool, AppError> {
        snapshot.validate()?;
        if !snapshot
            .inode
            .layout
            .replicas
            .iter()
            .any(|replica| replica.root_id == self.root_id)
        {
            return Err(AppError::Conflict(format!(
                "inode '{}' repair layout does not include local root '{}'",
                snapshot.inode.inode_id, self.root_id
            )));
        }

        self.ensure_repair_quiescent(&snapshot.inode.fragment_id, &snapshot.inode.inode_id)?;
        match self.snapshot_inode(&snapshot.inode.fragment_id, &snapshot.inode.inode_id) {
            Ok(current) if current == *snapshot => return Ok(false),
            Ok(current) if current.inode.generation >= snapshot.inode.generation => {
                return Err(AppError::Conflict(format!(
                    "inode '{}' has valid divergent generation {}; repair source generation is {}",
                    snapshot.inode.inode_id, current.inode.generation, snapshot.inode.generation
                )));
            }
            Ok(_) | Err(_) => {}
        }

        let target = self.inode_dir(&snapshot.inode.fragment_id, &snapshot.inode.inode_id);
        let parent = target.parent().ok_or_else(|| {
            AppError::InternalServerError("inode repair target has no parent".into())
        })?;
        fs::create_dir_all(parent)
            .map_err(|error| io_error(error, "create inode repair parent", parent))?;
        let staging = parent.join(format!(
            ".@i_{}.repair-{}",
            snapshot.inode.inode_id,
            Ulid::new()
        ));

        let staged = (|| -> Result<(), AppError> {
            fs::create_dir_all(staging.join(DENTRY_DIR))
                .map_err(|error| io_error(error, "create staged inode dentries", &staging))?;
            fs::create_dir_all(staging.join(CLAIM_DIR))
                .map_err(|error| io_error(error, "create staged inode claims", &staging))?;
            fs::create_dir_all(staging.join(DECISION_DIR))
                .map_err(|error| io_error(error, "create staged inode decisions", &staging))?;
            for entry in &snapshot.entries {
                let digest = hex::encode(Sha256::digest(entry.name.as_bytes()));
                let directory = staging.join(DENTRY_DIR).join(format!("{digest}.nref"));
                fs::create_dir_all(&directory)
                    .map_err(|error| io_error(error, "create staged inode dentry", &directory))?;
                write_immutable_or_equal(
                    &directory.join(format!("@g_{}.nref", entry.generation)),
                    &rmp_serde::to_vec_named(entry)?,
                )?;
            }
            write_immutable_or_equal(
                &staging.join(INODE_FILE),
                &rmp_serde::to_vec_named(&snapshot.inode)?,
            )?;
            sync_directory(&staging.join(DENTRY_DIR))?;
            sync_directory(&staging.join(CLAIM_DIR))?;
            sync_directory(&staging.join(DECISION_DIR))?;
            sync_directory(&staging)
        })();
        if let Err(error) = staged {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }

        // Close the repair-vs-mutation race immediately before publication.
        // A stale or corrupt replica cannot participate in a newer mutation,
        // while a replica with an existing claim/decision is always deferred.
        self.ensure_repair_quiescent(&snapshot.inode.fragment_id, &snapshot.inode.inode_id)?;
        match self.snapshot_inode(&snapshot.inode.fragment_id, &snapshot.inode.inode_id) {
            Ok(current) if current == *snapshot => {
                let _ = fs::remove_dir_all(&staging);
                return Ok(false);
            }
            Ok(current) if current.inode.generation >= snapshot.inode.generation => {
                let _ = fs::remove_dir_all(&staging);
                return Err(AppError::Conflict(format!(
                    "inode '{}' advanced to generation {} while repair was staged",
                    snapshot.inode.inode_id, current.inode.generation
                )));
            }
            Ok(_) | Err(_) => {}
        }

        if target.exists() {
            #[cfg(target_os = "linux")]
            {
                use rustix::fs::{CWD, RenameFlags, renameat_with};
                renameat_with(CWD, &staging, CWD, &target, RenameFlags::EXCHANGE).map_err(
                    |error| {
                        io_error(
                            std::io::Error::from_raw_os_error(error.raw_os_error()),
                            "atomically exchange repaired inode",
                            &target,
                        )
                    },
                )?;
                sync_directory(parent)?;
                fs::remove_dir_all(&staging)
                    .map_err(|error| io_error(error, "remove replaced inode replica", &staging))?;
            }
            #[cfg(not(target_os = "linux"))]
            {
                let backup = parent.join(format!(
                    ".@i_{}.replaced-{}",
                    snapshot.inode.inode_id,
                    Ulid::new()
                ));
                fs::rename(&target, &backup)
                    .map_err(|error| io_error(error, "stage replaced inode", &target))?;
                if let Err(error) = fs::rename(&staging, &target) {
                    let _ = fs::rename(&backup, &target);
                    return Err(io_error(error, "publish repaired inode", &target));
                }
                sync_directory(parent)?;
                fs::remove_dir_all(&backup)
                    .map_err(|error| io_error(error, "remove replaced inode replica", &backup))?;
            }
        } else {
            fs::rename(&staging, &target)
                .map_err(|error| io_error(error, "publish missing inode replica", &target))?;
            sync_directory(parent)?;
        }

        let installed =
            self.snapshot_inode(&snapshot.inode.fragment_id, &snapshot.inode.inode_id)?;
        if installed != *snapshot {
            return Err(AppError::Conflict(format!(
                "inode '{}' repair verification differed from its source snapshot",
                snapshot.inode.inode_id
            )));
        }
        Ok(true)
    }

    fn ensure_repair_quiescent(&self, fragment_id: &str, inode_id: &str) -> Result<(), AppError> {
        let inode = self.inode_dir(fragment_id, inode_id);
        for (directory, label) in [
            (inode.join(CLAIM_DIR), "generation claim"),
            (inode.join(DECISION_DIR), "cross-inode decision"),
        ] {
            match fs::read_dir(&directory) {
                Ok(mut entries) => {
                    if entries.next().is_some() {
                        return Err(AppError::ServiceUnavailable(format!(
                            "inode '{inode_id}' has a pending {label}; repair is deferred"
                        )));
                    }
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(io_error(error, "inspect inode repair activity", &directory));
                }
            }
        }
        Ok(())
    }

    pub fn prepare(&self, mutation: &PreparedInodeMutation) -> Result<(), AppError> {
        let inode = self.read_inode(&mutation.fragment_id, &mutation.inode_id)?;
        if inode.generation == mutation.target_generation
            && inode.last_mutation_id == mutation.mutation_id
            && inode.last_mutation_digest == mutation.mutation_digest
        {
            return Ok(());
        }
        if inode.generation != mutation.expected_generation {
            return Err(AppError::Conflict(format!(
                "inode '{}' expected generation {}, found {}",
                mutation.inode_id, mutation.expected_generation, inode.generation
            )));
        }

        let claim_path = self.claim_path(mutation);
        let claim_bytes = rmp_serde::to_vec_named(mutation)?;
        write_immutable_or_equal(&claim_path, &claim_bytes)?;

        for change in &mutation.changes {
            let dentry = materialize_change(change, mutation)?;
            let path = self.dentry_version_path(
                &mutation.fragment_id,
                &mutation.inode_id,
                &dentry.name,
                mutation.target_generation,
            );
            let parent = path.parent().expect("dentry version parent");
            fs::create_dir_all(parent)
                .map_err(|error| io_error(error, "create dentry version directory", parent))?;
            write_immutable_or_equal(&path, &rmp_serde::to_vec_named(&dentry)?)?;
        }
        Ok(())
    }

    pub fn commit(
        &self,
        mutation: &PreparedInodeMutation,
        certificate: &InodeCommitCertificate,
        committed_at_unix_nanos: i64,
    ) -> Result<DirectoryInode, AppError> {
        self.commit_inner(mutation, certificate, committed_at_unix_nanos, true)
    }

    fn commit_inner(
        &self,
        mutation: &PreparedInodeMutation,
        certificate: &InodeCommitCertificate,
        committed_at_unix_nanos: i64,
        release_claim: bool,
    ) -> Result<DirectoryInode, AppError> {
        let mut inode = self.read_inode(&mutation.fragment_id, &mutation.inode_id)?;
        if inode.generation == mutation.target_generation
            && inode.last_mutation_id == mutation.mutation_id
            && inode.last_mutation_digest == mutation.mutation_digest
        {
            certificate.validate(mutation, &inode.layout)?;
            if inode.commit_certificate.as_ref() != Some(certificate) {
                return Err(AppError::Conflict(format!(
                    "inode '{}' was committed with a different certificate",
                    mutation.inode_id
                )));
            }
            return Ok(inode);
        }
        if inode.generation != mutation.expected_generation {
            return Err(AppError::Conflict(format!(
                "inode '{}' commit expected generation {}, found {}",
                mutation.inode_id, mutation.expected_generation, inode.generation
            )));
        }
        certificate.validate(mutation, &inode.layout)?;
        let claim_path = self.claim_path(mutation);
        let claim: PreparedInodeMutation = read_record(&claim_path, "inode generation claim")?;
        if claim != *mutation {
            return Err(AppError::Conflict(format!(
                "inode '{}' generation {} is claimed by another mutation",
                mutation.inode_id, mutation.target_generation
            )));
        }
        for change in &mutation.changes {
            let expected = materialize_change(change, mutation)?;
            let path = self.dentry_version_path(
                &mutation.fragment_id,
                &mutation.inode_id,
                expected.name.as_str(),
                mutation.target_generation,
            );
            let actual: InodeDentry = read_record(&path, "prepared inode dentry")?;
            if actual != expected {
                return Err(AppError::Conflict(format!(
                    "prepared dentry '{}' does not match mutation '{}'",
                    expected.name, mutation.mutation_id
                )));
            }
        }

        inode.generation = mutation.target_generation;
        if let Some(parent_inode_id) = &mutation.new_parent_inode_id {
            inode.parent_inode_id = Some(parent_inode_id.clone());
        }
        inode.last_mutation_id = mutation.mutation_id.clone();
        inode.last_mutation_digest = mutation.mutation_digest.clone();
        inode.commit_certificate = Some(certificate.clone());
        inode.updated_at_unix_nanos = committed_at_unix_nanos;
        let inode_path = self
            .inode_dir(&mutation.fragment_id, &mutation.inode_id)
            .join(INODE_FILE);
        write_atomic(
            &inode_path,
            &rmp_serde::to_vec_named(&inode)?,
            &mutation.mutation_id,
        )?;
        self.compact_changed_dentries(mutation);
        if release_claim {
            let _ = fs::remove_file(&claim_path);
            sync_directory(claim_path.parent().expect("claim parent"))?;
        }
        Ok(inode)
    }

    pub fn record_transaction_decision(
        &self,
        layout: &InodeLayout,
        mutation: &PreparedInodeMutation,
        decision: &InodeTransactionDecision,
    ) -> Result<(), AppError> {
        if !decision.contains(layout, mutation) {
            return Err(AppError::Conflict(
                "transaction decision does not contain this inode mutation".into(),
            ));
        }
        let claim: PreparedInodeMutation =
            read_record(&self.claim_path(mutation), "inode generation claim")?;
        if claim != *mutation {
            return Err(AppError::Conflict(
                "transaction decision cannot bind a different generation claim".into(),
            ));
        }
        let path = self.decision_path(mutation, &decision.transaction_id);
        write_immutable_or_equal(&path, &rmp_serde::to_vec_named(decision)?)
    }

    pub fn commit_transaction_participant(
        &self,
        layout: &InodeLayout,
        mutation: &PreparedInodeMutation,
        certificate: &InodeCommitCertificate,
        decision: &InodeTransactionDecision,
        committed_at_unix_nanos: i64,
    ) -> Result<DirectoryInode, AppError> {
        if !decision.contains(layout, mutation) {
            return Err(AppError::Conflict(
                "transaction decision does not authorize this inode mutation".into(),
            ));
        }
        let recorded: InodeTransactionDecision = read_record(
            &self.decision_path(mutation, &decision.transaction_id),
            "inode transaction decision",
        )?;
        if recorded != *decision {
            return Err(AppError::Conflict(
                "recorded inode transaction decision differs".into(),
            ));
        }
        self.commit_inner(mutation, certificate, committed_at_unix_nanos, false)
    }

    pub fn finalize_transaction_participant(
        &self,
        mutation: &PreparedInodeMutation,
        decision: &InodeTransactionDecision,
    ) -> Result<(), AppError> {
        let inode = self.read_inode(&mutation.fragment_id, &mutation.inode_id)?;
        if inode.generation != mutation.target_generation
            || inode.last_mutation_id != mutation.mutation_id
            || inode.last_mutation_digest != mutation.mutation_digest
        {
            return Err(AppError::Conflict(
                "transaction participant is not committed and cannot be finalized".into(),
            ));
        }
        let decision_path = self.decision_path(mutation, &decision.transaction_id);
        let recorded: InodeTransactionDecision =
            read_record(&decision_path, "inode transaction decision")?;
        if recorded != *decision {
            return Err(AppError::Conflict(
                "transaction finalization decision differs from recorded state".into(),
            ));
        }
        fs::remove_file(&decision_path).map_err(|error| {
            io_error(
                error,
                "remove finalized inode transaction decision",
                &decision_path,
            )
        })?;
        let claim_path = self.claim_path(mutation);
        match fs::remove_file(&claim_path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error, "remove finalized inode claim", &claim_path)),
        }
        sync_directory(decision_path.parent().expect("decision parent"))?;
        sync_directory(claim_path.parent().expect("claim parent"))
    }

    pub fn abort(&self, mutation: &PreparedInodeMutation) -> Result<(), AppError> {
        let inode = self.read_inode(&mutation.fragment_id, &mutation.inode_id)?;
        if inode.generation != mutation.expected_generation {
            return Err(AppError::Conflict(
                "committed inode mutation cannot be aborted".into(),
            ));
        }
        let decision_root = self
            .inode_dir(&mutation.fragment_id, &mutation.inode_id)
            .join(DECISION_DIR);
        if fs::read_dir(&decision_root)
            .map_err(|error| io_error(error, "inspect inode decisions", &decision_root))?
            .next()
            .is_some()
        {
            return Err(AppError::Conflict(
                "inode mutation has a durable transaction decision and cannot be aborted".into(),
            ));
        }
        let claim_path = self.claim_path(mutation);
        let claim: PreparedInodeMutation = read_record(&claim_path, "inode generation claim")?;
        if claim != *mutation {
            return Err(AppError::Conflict(
                "another mutation owns the inode generation claim".into(),
            ));
        }
        for change in &mutation.changes {
            let path = self.dentry_version_path(
                &mutation.fragment_id,
                &mutation.inode_id,
                change.name(),
                mutation.target_generation,
            );
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(io_error(error, "remove aborted dentry", &path)),
            }
        }
        fs::remove_file(&claim_path)
            .map_err(|error| io_error(error, "remove inode generation claim", &claim_path))?;
        sync_directory(claim_path.parent().expect("claim parent"))
    }

    pub fn get_dentry(
        &self,
        fragment_id: &str,
        inode_id: &str,
        name: &str,
    ) -> Result<Option<InodeDentry>, AppError> {
        validate_name(name)?;
        let inode = self.read_inode(fragment_id, inode_id)?;
        let Some(dentry) = self.latest_dentry_at(fragment_id, inode_id, name, inode.generation)?
        else {
            return Ok(None);
        };
        Ok((!dentry.tombstone).then_some(dentry))
    }

    pub fn list_dentries(
        &self,
        fragment_id: &str,
        inode_id: &str,
    ) -> Result<Vec<InodeDentry>, AppError> {
        let inode = self.read_inode(fragment_id, inode_id)?;
        let root = self.inode_dir(fragment_id, inode_id).join(DENTRY_DIR);
        let entries =
            fs::read_dir(&root).map_err(|error| io_error(error, "list inode dentries", &root))?;
        let mut result = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(|error| io_error(error, "read inode dentry entry", &root))?;
            if !entry
                .file_type()
                .map_err(|error| io_error(error, "inspect inode dentry entry", &entry.path()))?
                .is_dir()
            {
                continue;
            }
            let versions = fs::read_dir(entry.path())
                .map_err(|error| io_error(error, "list inode dentry versions", &entry.path()))?;
            let mut selected: Option<InodeDentry> = None;
            for version in versions {
                let version = version
                    .map_err(|error| io_error(error, "read inode dentry version", &entry.path()))?;
                let Some(generation) = version_generation(&version.path()) else {
                    continue;
                };
                if generation > inode.generation
                    || selected
                        .as_ref()
                        .is_some_and(|current| current.generation >= generation)
                {
                    continue;
                }
                let candidate: InodeDentry = read_record(&version.path(), "inode dentry")?;
                selected = Some(candidate);
            }
            if let Some(dentry) = selected.filter(|dentry| !dentry.tombstone) {
                if result.insert(dentry.name.clone(), dentry).is_some() {
                    return Err(AppError::Conflict(
                        "inode contains duplicate logical dentry names".into(),
                    ));
                }
            }
        }
        Ok(result.into_values().collect())
    }

    fn latest_dentry_at(
        &self,
        fragment_id: &str,
        inode_id: &str,
        name: &str,
        generation: u64,
    ) -> Result<Option<InodeDentry>, AppError> {
        let root = self.dentry_name_dir(fragment_id, inode_id, name);
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(error, "list dentry versions", &root)),
        };
        let mut selected: Option<(u64, PathBuf)> = None;
        for entry in entries {
            let entry = entry.map_err(|error| io_error(error, "read dentry version", &root))?;
            let Some(candidate_generation) = version_generation(&entry.path()) else {
                continue;
            };
            if candidate_generation <= generation
                && selected.as_ref().is_none_or(|(selected_generation, _)| {
                    candidate_generation > *selected_generation
                })
            {
                selected = Some((candidate_generation, entry.path()));
            }
        }
        selected
            .map(|(_, path)| read_record(&path, "inode dentry"))
            .transpose()
    }

    fn inode_dir(&self, fragment_id: &str, inode_id: &str) -> PathBuf {
        self.root
            .join(format!("@f_{fragment_id}"))
            .join(NAMESPACE_DIR)
            .join(format!("@i_{inode_id}"))
    }

    fn dentry_name_dir(&self, fragment_id: &str, inode_id: &str, name: &str) -> PathBuf {
        let digest = hex::encode(Sha256::digest(name.as_bytes()));
        self.inode_dir(fragment_id, inode_id)
            .join(DENTRY_DIR)
            .join(format!("{digest}.nref"))
    }

    fn dentry_version_path(
        &self,
        fragment_id: &str,
        inode_id: &str,
        name: &str,
        generation: u64,
    ) -> PathBuf {
        self.dentry_name_dir(fragment_id, inode_id, name)
            .join(format!("@g_{generation}.nref"))
    }

    fn claim_path(&self, mutation: &PreparedInodeMutation) -> PathBuf {
        self.inode_dir(&mutation.fragment_id, &mutation.inode_id)
            .join(CLAIM_DIR)
            .join(format!("@g_{}.claim", mutation.target_generation))
    }

    fn decision_path(&self, mutation: &PreparedInodeMutation, transaction_id: &str) -> PathBuf {
        self.inode_dir(&mutation.fragment_id, &mutation.inode_id)
            .join(DECISION_DIR)
            .join(format!("{transaction_id}.decision"))
    }

    fn compact_changed_dentries(&self, mutation: &PreparedInodeMutation) {
        for change in &mutation.changes {
            let root =
                self.dentry_name_dir(&mutation.fragment_id, &mutation.inode_id, change.name());
            let Ok(entries) = fs::read_dir(root) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if version_generation(&path)
                    .is_some_and(|generation| generation < mutation.target_generation)
                {
                    let _ = fs::remove_file(path);
                }
            }
        }
    }
}

fn materialize_change(
    change: &InodeMutationChange,
    mutation: &PreparedInodeMutation,
) -> Result<InodeDentry, AppError> {
    let mut dentry = match change {
        InodeMutationChange::Upsert { dentry } => dentry.clone(),
        InodeMutationChange::Remove { name } => InodeDentry {
            name: name.clone(),
            target: InodeDentryTarget::Symlink {
                target: String::new(),
            },
            generation: 0,
            mutation_id: String::new(),
            tombstone: true,
            trash: None,
            created_at_unix_nanos: mutation.prepared_at_unix_nanos,
            updated_at_unix_nanos: mutation.prepared_at_unix_nanos,
        },
    };
    dentry.generation = mutation.target_generation;
    dentry.mutation_id = mutation.mutation_id.clone();
    dentry.updated_at_unix_nanos = mutation.prepared_at_unix_nanos;
    Ok(dentry)
}

fn validate_identity(label: &str, value: &str) -> Result<(), AppError> {
    if value.trim().is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value.chars().any(char::is_control)
    {
        return Err(AppError::BadRequest(format!(
            "{label} identity contains an invalid character"
        )));
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), AppError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        return Err(AppError::BadRequest(format!(
            "invalid namespace entry name '{name}'"
        )));
    }
    Ok(())
}

fn validate_target(target: &InodeDentryTarget) -> Result<(), AppError> {
    match target {
        InodeDentryTarget::Directory { inode_id, layout } => {
            validate_identity("directory inode", inode_id)?;
            layout.validate()
        }
        InodeDentryTarget::Object { object_id } => validate_identity("object", object_id),
        InodeDentryTarget::Symlink { target } => {
            if target.contains('\0') {
                Err(AppError::BadRequest(
                    "symbolic-link target contains NUL".into(),
                ))
            } else {
                Ok(())
            }
        }
        InodeDentryTarget::Fragment {
            stable_fragment_id,
            root_inode_id,
            root_layout,
            trash_inode_id,
            trash_layout,
        } => {
            validate_identity("stable fragment", stable_fragment_id)?;
            validate_identity("fragment root inode", root_inode_id)?;
            validate_identity("fragment trash inode", trash_inode_id)?;
            root_layout.validate()?;
            trash_layout.validate()
        }
    }
}

fn version_generation(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .strip_prefix("@g_")?
        .strip_suffix(".nref")?
        .parse()
        .ok()
}

fn read_record<T: for<'de> Deserialize<'de>>(path: &Path, label: &str) -> Result<T, AppError> {
    let bytes = fs::read(path).map_err(|error| io_error(error, &format!("read {label}"), path))?;
    rmp_serde::from_slice(&bytes).map_err(AppError::from)
}

fn write_immutable_or_equal(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError("immutable namespace path has no parent".into())
    })?;
    let temporary = parent.join(format!(".n2-immutable-{}.tmp", Ulid::new()));
    let result = (|| -> Result<(), AppError> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| io_error(error, "create immutable namespace temporary", &temporary))?;
        file.write_all(bytes)
            .map_err(|error| io_error(error, "write immutable namespace temporary", &temporary))?;
        file.sync_all()
            .map_err(|error| io_error(error, "sync immutable namespace temporary", &temporary))?;
        match fs::hard_link(&temporary, path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let existing = fs::read(path)
                    .map_err(|error| io_error(error, "read immutable namespace record", path))?;
                if existing == bytes {
                    Ok(())
                } else {
                    Err(AppError::Conflict(format!(
                        "immutable namespace record '{}' already contains different state",
                        path.display()
                    )))
                }
            }
            Err(error) => Err(io_error(error, "publish immutable namespace record", path)),
        }
    })();
    let _ = fs::remove_file(&temporary);
    result.and_then(|_| sync_directory(parent))
}

fn write_atomic(path: &Path, bytes: &[u8], mutation_id: &str) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError("inode path has no parent directory".into())
    })?;
    let temporary = parent.join(format!(".{INODE_FILE}.{mutation_id}.{}.tmp", Ulid::new()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| io_error(error, "create inode temporary", &temporary))?;
    file.write_all(bytes)
        .map_err(|error| io_error(error, "write inode temporary", &temporary))?;
    file.sync_all()
        .map_err(|error| io_error(error, "sync inode temporary", &temporary))?;
    let result = fs::rename(&temporary, path)
        .map_err(|error| io_error(error, "publish inode", path))
        .and_then(|_| sync_directory(parent));
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn sync_directory(path: &Path) -> Result<(), AppError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error(error, "sync namespace directory", path))
}

fn io_error(error: std::io::Error, action: &str, path: &Path) -> AppError {
    AppError::Io(error, format!("{action} {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use ulid::Ulid;

    fn fixture() -> (PathBuf, InodeStore, DirectoryInode) {
        let root = std::env::temp_dir().join(format!("n2-inode-store-{}", Ulid::new()));
        fs::create_dir_all(&root).expect("create root");
        let replicas = vec![
            InodeReplica {
                node_id: "namespace-a".into(),
                root_id: "ns-root-a".into(),
                endpoint: "https://namespace-a".into(),
                native_endpoint: None,
            },
            InodeReplica {
                node_id: "namespace-b".into(),
                root_id: "ns-root-b".into(),
                endpoint: "https://namespace-b".into(),
                native_endpoint: None,
            },
        ];
        let inode = DirectoryInode::new(
            "fragment-a".into(),
            "root-inode".into(),
            None,
            InodeLayout {
                generation: 1,
                replicas,
            },
            1,
        )
        .expect("inode");
        let store = InodeStore::new("ns-root-a".into(), root.clone()).expect("store");
        store.create_inode(&inode).expect("create inode");
        (root, store, inode)
    }

    fn upsert(
        inode: &DirectoryInode,
        expected_generation: u64,
        mutation_id: &str,
        name: &str,
    ) -> PreparedInodeMutation {
        let dentry = InodeDentry::live(
            name.into(),
            InodeDentryTarget::Object {
                object_id: format!("object-{name}"),
            },
            10,
        )
        .expect("dentry");
        PreparedInodeMutation::new(
            inode.fragment_id.clone(),
            inode.inode_id.clone(),
            expected_generation,
            mutation_id.into(),
            10,
            vec![InodeMutationChange::Upsert { dentry }],
        )
        .expect("mutation")
    }

    fn certificate(
        inode: &DirectoryInode,
        mutation: &PreparedInodeMutation,
    ) -> InodeCommitCertificate {
        InodeCommitCertificate::new(
            mutation,
            &inode.layout,
            vec!["namespace-a".into(), "namespace-b".into()],
        )
        .expect("certificate")
    }

    #[test]
    fn prepared_state_is_invisible_until_inode_commit() {
        let (root, store, inode) = fixture();
        let mutation = upsert(&inode, 0, "mutation-a", "video.mp4");
        store.prepare(&mutation).expect("prepare");
        assert!(
            store
                .get_dentry(&inode.fragment_id, &inode.inode_id, "video.mp4")
                .expect("lookup")
                .is_none()
        );
        store
            .commit(&mutation, &certificate(&inode, &mutation), 11)
            .expect("commit");
        assert!(
            store
                .get_dentry(&inode.fragment_id, &inode.inode_id, "video.mp4")
                .expect("lookup")
                .is_some()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn atomic_generation_claim_allows_only_one_conflicting_mutation() {
        let (root, store, inode) = fixture();
        let store = Arc::new(store);
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for (mutation_id, name) in [("mutation-a", "a"), ("mutation-b", "b")] {
            let store = store.clone();
            let barrier = barrier.clone();
            let mutation = upsert(&inode, 0, mutation_id, name);
            handles.push(thread::spawn(move || {
                barrier.wait();
                store.prepare(&mutation).map(|_| mutation)
            }));
        }
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("join"))
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let winner = results.into_iter().find_map(Result::ok).expect("winner");
        store
            .commit(&winner, &certificate(&inode, &winner), 11)
            .expect("commit winner");
        assert_eq!(
            store
                .list_dentries(&inode.fragment_id, &inode.inode_id)
                .expect("list")
                .len(),
            1
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn same_parent_rename_commits_as_one_inode_generation() {
        let (root, store, inode) = fixture();
        let create = upsert(&inode, 0, "create", "old");
        store.prepare(&create).expect("prepare create");
        store
            .commit(&create, &certificate(&inode, &create), 11)
            .expect("commit create");
        let old = store
            .get_dentry(&inode.fragment_id, &inode.inode_id, "old")
            .expect("lookup")
            .expect("old");
        let mut renamed = old.clone();
        renamed.name = "new".into();
        let rename = PreparedInodeMutation::new(
            inode.fragment_id.clone(),
            inode.inode_id.clone(),
            1,
            "rename".into(),
            12,
            vec![
                InodeMutationChange::Remove { name: "old".into() },
                InodeMutationChange::Upsert { dentry: renamed },
            ],
        )
        .expect("rename");
        store.prepare(&rename).expect("prepare rename");
        store
            .commit(&rename, &certificate(&inode, &rename), 13)
            .expect("commit rename");
        assert!(
            store
                .get_dentry(&inode.fragment_id, &inode.inode_id, "old")
                .expect("old lookup")
                .is_none()
        );
        assert!(
            store
                .get_dentry(&inode.fragment_id, &inode.inode_id, "new")
                .expect("new lookup")
                .is_some()
        );
        let committed = store
            .read_inode(&inode.fragment_id, &inode.inode_id)
            .expect("read inode");
        assert_eq!(committed.generation, 2);
        assert_eq!(
            fs::read_dir(store.dentry_name_dir(&inode.fragment_id, &inode.inode_id, "old"))
                .expect("old versions")
                .count(),
            1
        );
        assert_eq!(
            fs::read_dir(store.dentry_name_dir(&inode.fragment_id, &inode.inode_id, "new"))
                .expect("new versions")
                .count(),
            1
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn aborted_preparation_is_restart_invisible() {
        let (root, store, inode) = fixture();
        let mutation = upsert(&inode, 0, "abort", "uncommitted");
        store.prepare(&mutation).expect("prepare");
        store.abort(&mutation).expect("abort");
        let reopened = InodeStore::new("ns-root-a".into(), root.clone()).expect("reopen");
        assert!(
            reopened
                .get_dentry(&inode.fragment_id, &inode.inode_id, "uncommitted")
                .expect("lookup")
                .is_none()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parent_move_is_a_certified_inode_generation() {
        let (root, store, inode) = fixture();
        let mutation = PreparedInodeMutation::new_parent_move(
            inode.fragment_id.clone(),
            inode.inode_id.clone(),
            0,
            "move-parent".into(),
            20,
            "new-parent".into(),
        )
        .expect("parent mutation");
        store.prepare(&mutation).expect("prepare parent move");
        store
            .commit(&mutation, &certificate(&inode, &mutation), 21)
            .expect("commit parent move");
        let moved = store
            .read_inode(&inode.fragment_id, &inode.inode_id)
            .expect("read moved inode");
        assert_eq!(moved.parent_inode_id.as_deref(), Some("new-parent"));
        assert_eq!(moved.generation, 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cross_inode_decision_blocks_partial_visibility_until_all_participants_finalize() {
        let (root, store, first_inode) = fixture();
        let second_inode = DirectoryInode::new(
            first_inode.fragment_id.clone(),
            "second-inode".into(),
            Some(first_inode.inode_id.clone()),
            first_inode.layout.clone(),
            1,
        )
        .expect("second inode");
        store.create_inode(&second_inode).expect("create second");
        let first = upsert(&first_inode, 0, "tx-first", "from-first");
        let second = upsert(&second_inode, 0, "tx-second", "from-second");
        store.prepare(&first).expect("prepare first");
        store.prepare(&second).expect("prepare second");
        let decision = InodeTransactionDecision::new(
            "transaction-a".into(),
            vec![
                InodeTransactionParticipant::new(&first_inode.layout, &first),
                InodeTransactionParticipant::new(&second_inode.layout, &second),
            ],
        )
        .expect("decision");
        store
            .record_transaction_decision(&first_inode.layout, &first, &decision)
            .expect("record first decision");
        store
            .record_transaction_decision(&second_inode.layout, &second, &decision)
            .expect("record second decision");
        assert!(
            store
                .read_inode_consistent(&first_inode.fragment_id, &first_inode.inode_id)
                .is_err()
        );
        assert!(
            store
                .read_inode_consistent(&second_inode.fragment_id, &second_inode.inode_id)
                .is_err()
        );
        store
            .commit_transaction_participant(
                &first_inode.layout,
                &first,
                &certificate(&first_inode, &first),
                &decision,
                20,
            )
            .expect("commit first");
        assert!(
            store
                .read_inode_consistent(&first_inode.fragment_id, &first_inode.inode_id)
                .is_err(),
            "a committed participant remains hidden behind the durable decision barrier"
        );
        store
            .commit_transaction_participant(
                &second_inode.layout,
                &second,
                &certificate(&second_inode, &second),
                &decision,
                20,
            )
            .expect("commit second");
        store
            .finalize_transaction_participant(&first, &decision)
            .expect("finalize first");
        store
            .finalize_transaction_participant(&second, &decision)
            .expect("finalize second");
        assert!(
            store
                .read_inode_consistent(&first_inode.fragment_id, &first_inode.inode_id)
                .is_ok()
        );
        assert!(
            store
                .read_inode_consistent(&second_inode.fragment_id, &second_inode.inode_id)
                .is_ok()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn complete_inode_snapshot_restores_a_missing_replica() {
        let (root, store, inode) = fixture();
        let mutation = upsert(&inode, 0, "repair-source", "video.mp4");
        store.prepare(&mutation).expect("prepare");
        store
            .commit(&mutation, &certificate(&inode, &mutation), 11)
            .expect("commit");
        let snapshot = store
            .snapshot_inode(&inode.fragment_id, &inode.inode_id)
            .expect("snapshot");
        fs::remove_dir_all(store.inode_dir(&inode.fragment_id, &inode.inode_id))
            .expect("remove replica");

        assert!(
            store
                .restore_inode_snapshot(&snapshot)
                .expect("restore missing replica")
        );
        assert_eq!(
            store
                .snapshot_inode(&inode.fragment_id, &inode.inode_id)
                .expect("restored snapshot"),
            snapshot
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn complete_inode_snapshot_atomically_replaces_corrupt_replica() {
        let (root, store, inode) = fixture();
        let mutation = upsert(&inode, 0, "repair-source", "image.jpg");
        store.prepare(&mutation).expect("prepare");
        store
            .commit(&mutation, &certificate(&inode, &mutation), 11)
            .expect("commit");
        let snapshot = store
            .snapshot_inode(&inode.fragment_id, &inode.inode_id)
            .expect("snapshot");
        fs::write(
            store
                .inode_dir(&inode.fragment_id, &inode.inode_id)
                .join(INODE_FILE),
            b"corrupt",
        )
        .expect("corrupt inode");

        assert!(
            store
                .restore_inode_snapshot(&snapshot)
                .expect("replace corrupt replica")
        );
        assert_eq!(
            store
                .snapshot_inode(&inode.fragment_id, &inode.inode_id)
                .expect("repaired snapshot"),
            snapshot
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn repair_never_replaces_an_inode_with_a_pending_generation_claim() {
        let (root, store, inode) = fixture();
        let snapshot = store
            .snapshot_inode(&inode.fragment_id, &inode.inode_id)
            .expect("snapshot");
        let mutation = upsert(&inode, 0, "active-mutation", "pending");
        store.prepare(&mutation).expect("prepare active mutation");
        fs::write(
            store
                .inode_dir(&inode.fragment_id, &inode.inode_id)
                .join(INODE_FILE),
            b"corrupt",
        )
        .expect("simulate corrupt inode during active mutation");

        assert!(store.restore_inode_snapshot(&snapshot).is_err());
        assert!(store.claim_path(&mutation).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn repair_never_overwrites_a_newer_valid_inode_generation() {
        let (root, store, inode) = fixture();
        let first = upsert(&inode, 0, "first", "one");
        store.prepare(&first).expect("prepare first");
        store
            .commit(&first, &certificate(&inode, &first), 11)
            .expect("commit first");
        let stale = store
            .snapshot_inode(&inode.fragment_id, &inode.inode_id)
            .expect("stale snapshot");
        let second = upsert(&inode, 1, "second", "two");
        store.prepare(&second).expect("prepare second");
        store
            .commit(&second, &certificate(&inode, &second), 12)
            .expect("commit second");

        assert!(store.restore_inode_snapshot(&stale).is_err());
        assert_eq!(
            store
                .read_inode(&inode.fragment_id, &inode.inode_id)
                .expect("current inode")
                .generation,
            2
        );
        let _ = fs::remove_dir_all(root);
    }
}
