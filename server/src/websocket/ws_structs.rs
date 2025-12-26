use serde::{Deserialize, Serialize};

use crate::structs::object_storage_structs::ObjectMetadata;

#[derive(Deserialize)]
pub struct WsAuthParams {
    pub token: String,
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
    FragmentDeleted {
        fragment: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageEvent {
    #[serde(flatten)]
    pub payload: StorageEventPayload,
}