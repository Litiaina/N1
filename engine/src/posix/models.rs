use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
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
