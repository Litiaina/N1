use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FileSystemClaims {
    pub path: String,
    pub read: bool,
    pub write: bool,
    pub modify: bool,
    pub delete: bool,
    pub exp: usize,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub path: String,
    pub secret: String,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub write: bool,
    #[serde(default)]
    pub modify: bool,
    #[serde(default)]
    pub delete: bool,
    pub expires_in: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub fragment: String,
    pub read: bool,
    pub write: bool,
    pub modify: bool,
    pub delete: bool,
    pub exp: usize,
}

#[derive(Debug, Deserialize)]
pub struct AuthorizationRequest {
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
    pub expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AuthSessionScope {
    Fragment { fragment: String },
    FileSystem { path: String },
}

#[derive(Debug, Serialize)]
pub struct AuthBody {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub refresh_token: Option<String>,
    pub refresh_expires_in: Option<u64>,
}
