use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PosixNodeKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PosixEntry {
    pub name: String,
    pub path: String,
    pub kind: PosixNodeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_at_unix: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trash_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PosixListResponse {
    pub path: String,
    pub scan_trash: bool,
    pub entries: Vec<PosixEntry>,
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PosixStatResponse {
    pub path: String,
    pub scan_trash: bool,
    pub kind: PosixNodeKind,
    pub is_virtual: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_at_unix: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trash_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct PosixListQuery {
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub continuation_token: Option<String>,
    #[serde(default)]
    pub trash: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct PosixStatQuery {
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub trash: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct PosixPathRequest {
    pub path: String,
    #[serde(default)]
    pub trash_entry_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PosixRenameRequest {
    pub source_path: String,
    pub destination_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PosixMutationResponse {
    pub operation: String,
    pub kind: PosixNodeKind,
    pub affected: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trash_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceOperationState {
    Planning,
    Running,
    Completed,
    Partial,
    Failed,
    Cancelled,
}

impl NamespaceOperationState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Partial | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceOperationStatus {
    pub operation_id: String,
    pub mutation_id: String,
    pub coordinator_node_id: String,
    pub fragment: String,
    pub operation: String,
    pub kind: PosixNodeKind,
    pub state: NamespaceOperationState,
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
    pub discovered: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
    pub completed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub affected_paths: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_path: Option<String>,
    pub cancel_requested: bool,
    pub created_at_unix_nanos: i64,
    pub updated_at_unix_nanos: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_nanos: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trash_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceOperationAccepted {
    pub accepted: bool,
    pub operation_id: String,
    pub mutation_id: String,
    pub coordinator_node_id: String,
    pub operation: String,
    pub kind: PosixNodeKind,
    pub state: NamespaceOperationState,
    pub phase: String,
    pub status_endpoint: String,
    pub cancel_endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum PosixMutationResult {
    Completed(PosixMutationResponse),
    Accepted(NamespaceOperationAccepted),
}
