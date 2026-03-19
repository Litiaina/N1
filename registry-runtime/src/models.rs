use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageDeployment {
    SingleNode,
    MultiNode,
}

impl StorageDeployment {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SingleNode => "single_node",
            Self::MultiNode => "multi_node",
        }
    }

    pub fn parse_db(value: String) -> Result<Self, String> {
        match value.as_str() {
            "single_node" => Ok(Self::SingleNode),
            "multi_node" => Ok(Self::MultiNode),
            other => Err(format!("unknown storage deployment '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub uid: String,
    pub email: String,
    pub quota_limit_bytes: i64,
    pub quota_used_bytes: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct UserAuthRecord {
    pub user: UserRecord,
    pub password_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub owner_uid: String,
    pub fragment: String,
    pub object_key: String,
    pub version_id: String,
    pub status: String,
    pub object_size_bytes: i64,
    pub content_type: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentBindingRecord {
    pub owner_uid: String,
    pub fragment: String,
    pub deployment: StorageDeployment,
    pub engine_url_override: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub sqlite_path: String,
    pub users: i64,
    pub objects: i64,
    pub ready_objects: i64,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub email: String,
    pub password: String,
    pub quota_limit_bytes: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
    pub quota_limit_bytes: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct CreateUserResponse {
    pub user: UserRecord,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub user: UserRecord,
}

#[derive(Debug, Serialize)]
pub struct SignupResponse {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub user: UserRecord,
}

#[derive(Debug, Deserialize)]
pub struct RegisterObjectRequest {
    pub fragment: String,
    pub object_key: String,
    pub expected_size_bytes: Option<i64>,
    pub content_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FinalizeObjectRequest {
    pub fragment: String,
    pub object_key: String,
    pub version_id: String,
    pub object_size_bytes: i64,
    pub content_type: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ObjectMutationResponse {
    pub object: ObjectRecord,
    pub quota_limit_bytes: i64,
    pub quota_used_bytes: i64,
}

#[derive(Debug, Serialize)]
pub struct ObjectDeleteResponse {
    pub removed: bool,
    pub quota_limit_bytes: i64,
    pub quota_used_bytes: i64,
}

#[derive(Debug, Deserialize)]
pub struct ListObjectsQuery {
    pub fragment: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub search: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub include_pending: Option<bool>,
    pub include_trashed: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ResolveObjectQuery {
    pub fragment: String,
    pub object_key: String,
}

#[derive(Debug, Serialize)]
pub struct ListObjectsResponse {
    pub objects: Vec<ObjectRecord>,
    pub common_prefixes: Vec<String>,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Deserialize)]
pub struct SuggestObjectsQuery {
    pub fragment: Option<String>,
    pub prefix: Option<String>,
    pub q: String,
    pub limit: Option<usize>,
    pub include_trashed: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct SuggestObjectsResponse {
    pub suggestions: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ObjectStateRequest {
    pub fragment: String,
    pub object_key: String,
}

#[derive(Debug, Deserialize)]
pub struct FragmentStateRequest {
    pub fragment: String,
}

#[derive(Debug, Deserialize)]
pub struct RenameRegistryObjectRequest {
    pub fragment: String,
    pub source_key: String,
    pub destination_key: String,
}

#[derive(Debug, Deserialize)]
pub struct DeleteObjectQuery {
    pub fragment: String,
    pub object_key: String,
}

#[derive(Debug, Deserialize)]
pub struct LinkFragmentRequest {
    pub fragment: String,
    pub deployment: StorageDeployment,
    pub engine_url_override: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct LinkFragmentResponse {
    pub binding: FragmentBindingRecord,
}

#[derive(Debug, Deserialize)]
pub struct StorageSessionRequest {
    pub fragment: String,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub write: bool,
    #[serde(default)]
    pub modify: bool,
    #[serde(default)]
    pub delete: bool,
}

#[derive(Debug, Serialize)]
pub struct StorageSessionResponse {
    pub fragment: String,
    pub deployment: StorageDeployment,
    pub engine_url: String,
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
}

#[derive(Debug, Serialize)]
pub struct FragmentListResponse {
    pub fragments: Vec<String>,
    pub bindings: Vec<FragmentBindingRecord>,
}

#[derive(Debug, Serialize)]
pub struct BulkObjectMutationResponse {
    pub affected: usize,
    pub quota_limit_bytes: i64,
    pub quota_used_bytes: i64,
}
