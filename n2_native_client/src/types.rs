use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

pub const MAX_NATIVE_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_NATIVE_BODY_BYTES: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeRequest {
    pub id: String,
    pub op: String,
    #[serde(default)]
    pub payload: Option<String>,
    #[serde(default)]
    pub body: Option<ByteBuf>,
    #[serde(default)]
    pub body_len: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeResponse {
    pub id: String,
    pub ok: bool,
    #[serde(default)]
    pub status: Option<u16>,
    #[serde(default)]
    pub payload: Option<String>,
    #[serde(default)]
    pub body: Option<ByteBuf>,
    #[serde(default)]
    pub body_len: Option<u64>,
    #[serde(default)]
    pub error: Option<NativeError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct NativeAuthFragmentRequest {
    pub fragment: String,
    pub secret: String,
    pub read: bool,
    pub write: bool,
    pub modify: bool,
    pub delete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NativeBearerRequest {
    pub token: String,
}
