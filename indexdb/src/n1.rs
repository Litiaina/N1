use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    Live,
    Trash,
    Deleted,
}

impl ObjectState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Trash => "trash",
            Self::Deleted => "deleted",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "live" => Some(Self::Live),
            "trash" => Some(Self::Trash),
            "deleted" => Some(Self::Deleted),
            _ => None,
        }
    }
}

impl Display for ObjectState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct PartShard {
    pub shard_index: u64,
    pub file_name: String,
    pub absolute_path: String,
    pub relative_path: Option<String>,
    pub blob_root_id: Option<String>,
    pub size_bytes: u64,
    pub checksum: String,
    pub is_parity: bool,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct ObjectPart {
    pub part_index: u64,
    pub size_bytes: u64,
    pub created_at_unix: i64,
    pub checksum: Option<String>,
    pub shards: Vec<PartShard>,
}

#[derive(Debug, Clone)]
pub struct ObjectVersion {
    pub object_id: String,
    pub object_key: String,
    pub version_id: String,
    pub upload_session_id: String,
    pub owner_uid: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub created_at_unix: i64,
    pub committed_at_unix: Option<i64>,
    pub is_delete_marker: bool,
    pub stripe_size_bytes: u64,
    pub checksum: Option<String>,
    pub metadata_json: Option<String>,
    pub parts: Vec<ObjectPart>,
}

#[derive(Debug, Clone)]
pub struct ObjectVersionState {
    pub object_id: String,
    pub version_id: String,
    pub state: ObjectState,
    pub generation: u64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct ObjectHead {
    pub object_id: String,
    pub object_key: String,
    pub head_version_id: String,
    pub owner_uid: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub state: ObjectState,
    pub generation: u64,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceEntryKind {
    File,
    Directory,
}

impl NamespaceEntryKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "file" => Some(Self::File),
            "directory" => Some(Self::Directory),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceLifecycle {
    Live,
    Trashed,
    Retired,
    Purged,
}

impl NamespaceLifecycle {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Trashed => "trashed",
            Self::Retired => "retired",
            Self::Purged => "purged",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "live" => Some(Self::Live),
            "trashed" => Some(Self::Trashed),
            "retired" => Some(Self::Retired),
            "purged" => Some(Self::Purged),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrashEntryState {
    Trashed,
    Retired,
    Purged,
}

impl TrashEntryState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Trashed => "trashed",
            Self::Retired => "retired",
            Self::Purged => "purged",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "trashed" => Some(Self::Trashed),
            "retired" => Some(Self::Retired),
            "purged" => Some(Self::Purged),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NamespaceEntry {
    pub path: String,
    pub parent_path: String,
    pub name: String,
    pub kind: NamespaceEntryKind,
    pub object_id: Option<String>,
    pub head_version_id: Option<String>,
    pub lifecycle: NamespaceLifecycle,
    pub trash_entry_id: Option<String>,
    pub deleted_at_unix: Option<i64>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct NamespaceNode {
    pub node_id: String,
    pub parent_node_id: Option<String>,
    pub name: String,
    pub kind: NamespaceEntryKind,
    pub object_id: Option<String>,
    pub head_version_id: Option<String>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct NamespaceTrashRoot {
    pub trash_entry_id: String,
    pub root_node_id: String,
    pub original_parent_node_id: String,
    pub original_name: String,
    pub original_path: String,
    pub kind: NamespaceEntryKind,
    pub object_id: Option<String>,
    pub head_version_id: Option<String>,
    pub deleted_at_unix: i64,
    pub retired_at_unix: Option<i64>,
    pub purged_at_unix: Option<i64>,
    pub state: TrashEntryState,
}

#[derive(Debug, Clone)]
pub struct NamespaceMutationStats {
    pub affected_paths: usize,
    pub affected_objects: usize,
    pub source_path: Option<String>,
    pub source_kind: NamespaceEntryKind,
    pub source_object_id: Option<String>,
    pub source_head_version_id: Option<String>,
    pub trash_entry_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NamespaceObjectSummary {
    pub path: String,
    pub version_id: String,
    pub upload_session_id: String,
    pub owner_uid: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub created_at_unix: i64,
    pub is_delete_marker: bool,
    pub stripe_size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct TrashEntry {
    pub trash_entry_id: String,
    pub original_path: String,
    pub object_id: Option<String>,
    pub version_id: Option<String>,
    pub kind: NamespaceEntryKind,
    pub deleted_at_unix: i64,
    pub retired_at_unix: Option<i64>,
    pub purged_at_unix: Option<i64>,
    pub state: TrashEntryState,
    pub metadata_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PendingUploadBinding {
    pub logical_path: String,
    pub physical_object_id: String,
    pub version_id: String,
    pub session_id: String,
    pub created_at_unix: i64,
}

#[derive(Debug, Clone)]
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
}
