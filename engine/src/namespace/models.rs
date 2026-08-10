use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NamespaceEntryKind {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleState {
    Live,
    Trashed { changed_at_unix: i64 },
}

impl LifecycleState {
    pub fn live() -> Self {
        Self::Live
    }

    pub fn trashed(changed_at_unix: i64) -> Self {
        Self::Trashed { changed_at_unix }
    }

    pub fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }

    pub fn is_trashed(&self) -> bool {
        matches!(self, Self::Trashed { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceEntry {
    pub fragment: String,
    pub path: String,
    pub parent_path: String,
    pub name: String,
    pub kind: NamespaceEntryKind,
    pub object_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packed_ext_u64: Option<u64>,
    pub lifecycle: LifecycleState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trash_entry_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix: Option<i64>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectHead {
    pub fragment: String,
    pub object_id: String,
    pub current_version_id: String,
    pub storage_object_id: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub lifecycle: LifecycleState,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceFileListing {
    pub path: String,
    pub object_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespacePage {
    pub files: Vec<NamespaceFileListing>,
    pub common_prefixes: Vec<String>,
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceMutationSummary {
    pub operation: String,
    pub kind: NamespaceEntryKind,
    pub affected_paths: usize,
    pub affected_objects: usize,
    pub source_path: Option<String>,
    pub destination_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trash_entry_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NamespaceReachabilitySnapshot {
    pub referenced_object_ids: BTreeSet<String>,
    pub referenced_version_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceError {
    InvalidPath(String),
    Conflict(String),
    NotFound(String),
    Invariant(String),
}

impl std::fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPath(message) => write!(f, "invalid path: {message}"),
            Self::Conflict(message) => write!(f, "conflict: {message}"),
            Self::NotFound(message) => write!(f, "not found: {message}"),
            Self::Invariant(message) => write!(f, "invariant violation: {message}"),
        }
    }
}

impl std::error::Error for NamespaceError {}
