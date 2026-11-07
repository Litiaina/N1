//! Restart-safe archive jobs. The original bounded ZIP64/TAR builder is retained;
//! its immutable-version reader now uses metadata and the native object client.
pub(crate) mod archive;
mod builder;
mod manifest;
mod models;
pub(crate) mod routes;
mod source;
pub(crate) mod store;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadArchiveEntry {
    pub object_key: String,
    pub archive_path: String,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub offset_bytes: Option<u64>,
    #[serde(default)]
    pub length_bytes: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadArchiveRequest {
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub entries: Vec<DownloadArchiveEntry>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub prefixes: Vec<String>,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub export_all: bool,
    #[serde(default)]
    pub include_internal: bool,
}
fn default_format() -> String {
    "zip".into()
}
#[derive(Debug)]
pub(super) enum BuilderError {
    ServiceUnavailable(String),
    InternalServerError(String),
    Io(std::io::Error, String),
    Conflict(String),
}
impl std::fmt::Display for BuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServiceUnavailable(s) | Self::InternalServerError(s) | Self::Conflict(s) => {
                f.write_str(s)
            }
            Self::Io(e, s) => write!(f, "{s}: {e}"),
        }
    }
}
impl std::error::Error for BuilderError {}
impl From<BuilderError> for crate::server::error::ApiError {
    fn from(e: BuilderError) -> Self {
        use axum::http::StatusCode;
        let status = match e {
            BuilderError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            BuilderError::Conflict(_) => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self(status, e.to_string())
    }
}
pub async fn initialize(
    app: &std::sync::Arc<crate::server::App>,
) -> crate::server::error::ApiResult<()> {
    store::initialize(app).await
}
pub async fn shutdown() {
    store::shutdown().await
}
#[cfg(test)]
mod builder_tests;
