use serde::{Deserialize, Serialize};

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

#[derive(Debug, Serialize)]
pub struct AuthBody {
    pub access_token: String,
    pub token_type: String,
}
