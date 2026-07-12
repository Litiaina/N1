use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

pub const N1_NATIVE_PROTOCOL_VERSION: &str = "n1/0.5";
pub const MAX_NATIVE_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_NATIVE_BODY_BYTES: usize = 1024 * 1024 * 1024;
pub const MAX_NATIVE_FULL_DOWNLOAD_BYTES: usize = 64 * 1024 * 1024;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeOpResponse {
    pub status: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeAuthFragmentRequest {
    pub fragment: String,
    pub secret: String,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub write: bool,
    #[serde(default)]
    pub modify: bool,
    #[serde(default)]
    pub delete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeBearerRequest {
    pub token: String,
}

impl NativeResponse {
    pub fn ok(id: impl Into<String>, payload: impl Serialize) -> Self {
        let payload = serde_json::to_string(&payload).unwrap_or_else(|_| "null".to_string());
        Self {
            id: id.into(),
            ok: true,
            status: None,
            payload: Some(payload),
            body: None,
            body_len: None,
            error: None,
        }
    }

    pub fn operation(id: impl Into<String>, payload: NativeOpResponse, body: Vec<u8>) -> Self {
        let status = payload.status;
        Self {
            id: id.into(),
            ok: status < 400,
            status: Some(status),
            payload: None,
            body_len: if body.is_empty() {
                None
            } else {
                Some(body.len() as u64)
            },
            body: if body.is_empty() {
                None
            } else {
                Some(ByteBuf::from(body))
            },
            error: None,
        }
    }

    pub fn err(id: impl Into<String>, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            ok: false,
            status: None,
            payload: None,
            body: None,
            body_len: None,
            error: Some(NativeError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}
