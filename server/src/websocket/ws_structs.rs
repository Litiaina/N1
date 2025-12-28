use serde::{Deserialize, Serialize};

use crate::structs::object_storage_structs::ObjectMetadata;

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
    #[serde(rename = "FILE_UPLOADED")]
    FileUploaded(ObjectMetadata),

    #[serde(rename = "FILE_DELETED")]
    FileDeleted {
        fragment: String,
        object_key: String,
        version_id: Option<String>,
    },

    #[serde(rename = "FILE_RENAMED")]
    FileRenamed {
        source_fragment: String,
        source_key: String,
        destination_fragment: String,
        destination_key: String,
    },

    #[serde(rename = "FRAGMENT_DELETED")]
    FragmentDeleted { fragment: String },

    #[serde(rename = "SYSTEM_MESSAGE")]
    SystemMessage { message: String, severity: String },

    #[serde(rename = "STATE_INVALIDATED")]
    StateInvalidated,
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
