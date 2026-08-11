use serde::{Deserialize, Serialize};

use crate::core::{redistribution::RedistributionJobStatus, repair_manager::RepairJobStatus};
use crate::namespace::models::{LifecycleState, NamespaceEntryKind, NamespaceMutationSummary};
use crate::posix::models::NamespaceOperationStatus;

#[derive(Deserialize)]
pub struct WsAuthParams {
    pub token: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "op", content = "d")]
pub enum ServerMessage {
    #[serde(rename = "HELLO")]
    Hello { connection_id: String },

    #[serde(rename = "PONG")]
    Pong,

    #[serde(rename = "EVENT")]
    Event(StorageEvent),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", content = "d")]
pub enum ClientMessage {
    #[serde(rename = "PING")]
    Ping,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum StorageEventPayload {
    #[serde(rename = "NAMESPACE_CHANGED")]
    NamespaceChanged(NamespaceChangeEvent),

    #[serde(rename = "NAMESPACE_OPERATION_UPDATED")]
    NamespaceOperationUpdated(NamespaceOperationStatus),

    #[serde(rename = "FRAGMENT_DELETED")]
    FragmentDeleted { fragment: String },

    #[serde(rename = "SYSTEM_MESSAGE")]
    SystemMessage { message: String, severity: String },

    #[serde(rename = "MONITORING_UPDATE")]
    MonitoringUpdate {
        system: serde_json::Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        uploads: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        namespace_runtime: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        maintenance: Option<serde_json::Value>,
        repair_job: Option<serde_json::Value>,
        redist_job: Option<serde_json::Value>,
        object_record_status: String,
        namespace_healthy: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        sampled_at: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },

    #[serde(rename = "REDISTRIBUTION_UPDATE")]
    RedistributionUpdate(RedistributionJobStatus),

    #[serde(rename = "REPAIR_UPDATE")]
    RepairUpdate(RepairJobStatus),
}

#[derive(Debug, Clone, Serialize)]
pub struct NamespaceEntryPatch {
    pub fragment: String,
    pub path: String,
    pub parent_path: String,
    pub name: String,
    pub kind: NamespaceEntryKind,
    pub lifecycle: LifecycleState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_at_unix: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trash_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NamespaceChangeEvent {
    pub fragment: String,
    pub operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<NamespaceEntryKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry: Option<NamespaceEntryPatch>,
    pub affected_paths: usize,
    pub affected_objects: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trash_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

impl NamespaceChangeEvent {
    pub fn from_summary(fragment: impl Into<String>, summary: NamespaceMutationSummary) -> Self {
        let path = summary
            .destination_path
            .clone()
            .or_else(|| summary.source_path.clone());
        Self {
            fragment: fragment.into(),
            operation: summary.operation,
            kind: Some(summary.kind),
            path,
            source_path: summary.source_path,
            destination_path: summary.destination_path,
            entry: None,
            affected_paths: summary.affected_paths,
            affected_objects: summary.affected_objects,
            trash_entry_id: summary.trash_entry_id,
            deleted_at_unix: summary.deleted_at_unix,
            version_id: None,
        }
    }

    pub fn file_upsert(
        fragment: impl Into<String>,
        path: impl Into<String>,
        object_id: impl Into<String>,
        size_bytes: u64,
        version_id: impl Into<String>,
        content_type: impl Into<String>,
        modified_at_unix: i64,
    ) -> Self {
        let fragment = fragment.into();
        let path = path.into();
        let version_id = version_id.into();
        let entry = NamespaceEntryPatch {
            fragment: fragment.clone(),
            parent_path: parent_path(&path),
            name: entry_name(&path),
            path: path.clone(),
            kind: NamespaceEntryKind::File,
            lifecycle: LifecycleState::Live,
            object_id: Some(object_id.into()),
            size_bytes: Some(size_bytes),
            version_id: Some(version_id.clone()),
            content_type: Some(content_type.into()),
            modified_at_unix: Some(modified_at_unix),
            trash_entry_id: None,
            deleted_at_unix: None,
        };
        Self {
            fragment,
            operation: "upsert_file".to_string(),
            kind: Some(NamespaceEntryKind::File),
            path: Some(path),
            source_path: None,
            destination_path: None,
            entry: Some(entry),
            affected_paths: 1,
            affected_objects: 1,
            trash_entry_id: None,
            deleted_at_unix: None,
            version_id: Some(version_id),
        }
    }

    pub fn directory_created(fragment: impl Into<String>, path: impl Into<String>) -> Self {
        let fragment = fragment.into();
        let path = path.into();
        let entry = NamespaceEntryPatch {
            fragment: fragment.clone(),
            parent_path: parent_path(&path),
            name: entry_name(&path),
            path: path.clone(),
            kind: NamespaceEntryKind::Directory,
            lifecycle: LifecycleState::Live,
            object_id: None,
            size_bytes: None,
            version_id: None,
            content_type: None,
            modified_at_unix: None,
            trash_entry_id: None,
            deleted_at_unix: None,
        };
        Self {
            fragment,
            operation: "mkdir".to_string(),
            kind: Some(NamespaceEntryKind::Directory),
            path: Some(path),
            source_path: None,
            destination_path: None,
            entry: Some(entry),
            affected_paths: 1,
            affected_objects: 0,
            trash_entry_id: None,
            deleted_at_unix: None,
            version_id: None,
        }
    }

    pub fn version_removed(
        fragment: impl Into<String>,
        path: impl Into<String>,
        version_id: impl Into<String>,
    ) -> Self {
        let path = path.into();
        Self {
            fragment: fragment.into(),
            operation: "delete_version".to_string(),
            kind: Some(NamespaceEntryKind::File),
            path: Some(path),
            source_path: None,
            destination_path: None,
            entry: None,
            affected_paths: 1,
            affected_objects: 1,
            trash_entry_id: None,
            deleted_at_unix: None,
            version_id: Some(version_id.into()),
        }
    }

    pub fn renamed(
        fragment: impl Into<String>,
        source_path: impl Into<String>,
        destination_path: impl Into<String>,
        kind: NamespaceEntryKind,
        affected_paths: usize,
        affected_objects: usize,
    ) -> Self {
        let destination_path = destination_path.into();
        Self {
            fragment: fragment.into(),
            operation: "rename".to_string(),
            kind: Some(kind),
            path: Some(destination_path.clone()),
            source_path: Some(source_path.into()),
            destination_path: Some(destination_path),
            entry: None,
            affected_paths,
            affected_objects,
            trash_entry_id: None,
            deleted_at_unix: None,
            version_id: None,
        }
    }

    pub fn bulk(
        fragment: impl Into<String>,
        operation: impl Into<String>,
        affected: usize,
    ) -> Self {
        Self {
            fragment: fragment.into(),
            operation: operation.into(),
            kind: Some(NamespaceEntryKind::Directory),
            path: None,
            source_path: None,
            destination_path: None,
            entry: None,
            affected_paths: affected,
            affected_objects: affected,
            trash_entry_id: None,
            deleted_at_unix: None,
            version_id: None,
        }
    }
}

fn parent_path(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn entry_name(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| path.to_string())
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageEvent {
    pub timestamp_ms: u64,
    #[serde(flatten)]
    pub payload: StorageEventPayload,
}

#[derive(serde::Deserialize)]
pub struct BroadcastReq {
    pub message: String,
    pub severity: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{NamespaceChangeEvent, StorageEventPayload};
    use crate::namespace::models::{NamespaceEntryKind, NamespaceMutationSummary};

    #[test]
    fn file_upsert_serializes_as_namespace_delta() {
        let payload = StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::file_upsert(
            "frag",
            "docs/video.mp4",
            "object-123",
            42,
            "01ABC",
            "video/mp4",
            1_777_900_000,
        ));

        let value = serde_json::to_value(payload).expect("serialize namespace event");
        assert_eq!(value["type"], "NAMESPACE_CHANGED");
        assert_eq!(value["data"]["operation"], "upsert_file");
        assert_eq!(value["data"]["path"], "docs/video.mp4");
        assert_eq!(value["data"]["entry"]["parent_path"], "docs");
        assert_eq!(value["data"]["entry"]["name"], "video.mp4");
        assert_eq!(value["data"]["entry"]["object_id"], "object-123");
        assert_eq!(value["data"]["entry"]["version_id"], "01ABC");
    }

    #[test]
    fn mutation_summary_serializes_without_full_refresh_contract() {
        let payload = StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            "frag",
            NamespaceMutationSummary {
                operation: "rename".to_string(),
                kind: NamespaceEntryKind::Directory,
                affected_paths: 1,
                affected_objects: 7,
                source_path: Some("videos".to_string()),
                destination_path: Some("archive/videos".to_string()),
                trash_entry_id: None,
                deleted_at_unix: None,
            },
        ));

        let value = serde_json::to_value(payload).expect("serialize rename event");
        assert_eq!(
            value,
            json!({
                "type": "NAMESPACE_CHANGED",
                "data": {
                    "fragment": "frag",
                    "operation": "rename",
                    "kind": "Directory",
                    "path": "archive/videos",
                    "source_path": "videos",
                    "destination_path": "archive/videos",
                    "affected_paths": 1,
                    "affected_objects": 7
                }
            })
        );
    }
}
