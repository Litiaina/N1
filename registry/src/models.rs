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
    pub is_admin: bool,
    pub two_factor_enabled: bool,
    pub quota_limit_bytes: i64,
    pub quota_used_bytes: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct UserAuthRecord {
    pub user: UserRecord,
    pub password_hash: String,
    pub totp_secret: Option<String>,
    pub totp_pending_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub owner_uid: String,
    pub fragment: String,
    pub object_key: String,
    pub storage_object_id: String,
    pub version_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub object_size_bytes: i64,
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trash_entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix: Option<i64>,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ShareDisposition {
    #[default]
    View,
    Stream,
    Download,
}

impl ShareDisposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Stream => "stream",
            Self::Download => "download",
        }
    }

    pub fn parse_db(value: String) -> Result<Self, String> {
        match value.as_str() {
            "view" => Ok(Self::View),
            "stream" => Ok(Self::Stream),
            "download" => Ok(Self::Download),
            other => Err(format!("unknown share disposition '{}'", other)),
        }
    }

    pub fn is_download(self) -> bool {
        matches!(self, Self::Download)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ShareRecord {
    pub share_id: String,
    pub owner_uid: String,
    pub fragment: String,
    pub object_key: String,
    // Retained for internal share persistence parity even though Registry now addresses N1 by logical key.
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub storage_object_id: String,
    pub disposition: ShareDisposition,
    pub expires_at: String,
    pub created_at: String,
    pub revoked_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditEventRecord {
    pub event_id: String,
    pub owner_uid: String,
    pub actor_uid: String,
    pub event_type: String,
    pub fragment: Option<String>,
    pub object_key: Option<String>,
    pub details_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectOverlayRecord {
    pub owner_uid: String,
    pub fragment: String,
    pub object_key: String,
    pub favorite: bool,
    pub tags: Vec<String>,
    pub labels: Vec<String>,
    pub comment_count: usize,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectCommentRecord {
    pub comment_id: String,
    pub owner_uid: String,
    pub actor_uid: String,
    pub fragment: String,
    pub object_key: String,
    pub body: String,
    pub created_at: String,
    pub updated_at: String,
    pub deleted_at: Option<String>,
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
    pub is_admin: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct CreateUserResponse {
    pub user: UserRecord,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
    pub otp: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub refresh_token: String,
    pub refresh_expires_in: u64,
    pub user: UserRecord,
}

#[derive(Debug, Serialize)]
pub struct SignupResponse {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub refresh_token: String,
    pub refresh_expires_in: u64,
    pub user: UserRecord,
}

#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Serialize)]
pub struct RefreshResponse {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub refresh_token: String,
    pub refresh_expires_in: u64,
    pub user: UserRecord,
}

#[derive(Debug, Deserialize)]
pub struct BootstrapAdminRequest {
    pub email: String,
    pub password: String,
    pub master_key: String,
}

#[derive(Debug, Serialize)]
pub struct BootstrapAdminResponse {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub refresh_token: String,
    pub refresh_expires_in: u64,
    pub user: UserRecord,
}

#[derive(Debug, Serialize)]
pub struct AdminUserListResponse {
    pub users: Vec<AdminUserSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminUserSummary {
    pub uid: String,
    pub email: String,
    pub is_admin: bool,
    pub two_factor_enabled: bool,
    pub quota_limit_bytes: i64,
    pub quota_used_bytes: i64,
    pub fragment_count: usize,
    pub fragments: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminStatsTotals {
    pub total_users: usize,
    pub admin_users: usize,
    pub two_factor_enabled_users: usize,
    pub users_using_storage: usize,
    pub total_quota_bytes: i64,
    pub total_used_bytes: i64,
    pub total_fragments: usize,
    pub ready_objects: usize,
    pub trashed_objects: usize,
    pub pending_uploads: usize,
    pub active_sessions: usize,
    pub active_users: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminLargestObjectSummary {
    pub uid: String,
    pub email: String,
    pub fragment: String,
    pub object_key: String,
    pub object_size_bytes: i64,
    pub content_type: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminActivityUserSummary {
    pub uid: String,
    pub email: String,
    pub is_admin: bool,
    pub two_factor_enabled: bool,
    pub active_sessions: usize,
    pub fragment_count: usize,
    pub ready_object_count: usize,
    pub quota_used_bytes: i64,
    pub quota_limit_bytes: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminStatsResponse {
    pub generated_at: String,
    pub totals: AdminStatsTotals,
    pub file_types: Vec<ObjectExtensionStat>,
    pub largest_objects: Vec<AdminLargestObjectSummary>,
    pub active_users: Vec<AdminActivityUserSummary>,
    pub top_storage_users: Vec<AdminActivityUserSummary>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateQuotaRequest {
    pub quota_limit_bytes: i64,
}

#[derive(Debug, Deserialize)]
pub struct UpdateCredentialsRequest {
    pub email: Option<String>,
    pub password: Option<String>,
    pub is_admin: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateEmailRequest {
    pub current_password: String,
    pub new_email: String,
    pub otp: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePasswordRequest {
    pub current_password: String,
    pub new_password: String,
    pub otp: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SetupTwoFactorRequest {
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct EnableTwoFactorRequest {
    pub password: String,
    pub otp: String,
}

#[derive(Debug, Deserialize)]
pub struct DisableTwoFactorRequest {
    pub password: String,
    pub otp: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TwoFactorSetupResponse {
    pub secret: String,
    pub otpauth_uri: String,
    pub qr_data_url: String,
    pub pending: bool,
}

#[derive(Debug, Serialize)]
pub struct UserMutationResponse {
    pub user: UserRecord,
}

#[derive(Debug, Serialize)]
pub struct DeleteUserResponse {
    pub removed: bool,
    pub deleted_fragments: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterObjectRequest {
    pub fragment: String,
    pub object_key: String,
    pub expected_size_bytes: Option<i64>,
    pub content_type: Option<String>,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ArchiveObjectsRequest {
    pub fragment: String,
    #[serde(default)]
    pub keys: Vec<String>,
    pub prefix: Option<String>,
    pub search: Option<String>,
    pub filename: Option<String>,
    pub format: Option<String>,
    pub include_trashed: Option<bool>,
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
pub struct DeleteObjectVersionRequest {
    pub fragment: String,
    pub object_key: String,
    pub version_id: String,
}

#[derive(Debug, Serialize)]
pub struct DeleteObjectVersionResponse {
    pub removed: bool,
    pub object_removed: bool,
    pub object: Option<ObjectRecord>,
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

#[derive(Debug, Deserialize)]
pub struct CreateShareRequest {
    pub fragment: String,
    pub object_key: String,
    pub expires_in: u64,
    #[serde(default)]
    pub disposition: ShareDisposition,
}

#[derive(Debug, Deserialize)]
pub struct ListSharesQuery {
    pub fragment: Option<String>,
    pub include_expired: Option<bool>,
    pub include_revoked: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct CreateShareResponse {
    pub share: ShareRecord,
    pub url: String,
    pub expires_at: String,
}

#[derive(Debug, Serialize)]
pub struct ListSharesResponse {
    pub shares: Vec<ShareRecord>,
}

#[derive(Debug, Deserialize)]
pub struct ActivityQuery {
    pub owner_uid: Option<String>,
    pub fragment: Option<String>,
    pub object_key: Option<String>,
    pub event_type: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ActivityResponse {
    pub events: Vec<AuditEventRecord>,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Deserialize)]
pub struct ObjectOverlayQuery {
    pub fragment: String,
    pub object_key: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateObjectOverlayRequest {
    pub fragment: String,
    pub object_key: String,
    pub favorite: Option<bool>,
    pub tags: Option<Vec<String>>,
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct ObjectOverlayResponse {
    pub overlay: ObjectOverlayRecord,
}

#[derive(Debug, Deserialize)]
pub struct AddObjectCommentRequest {
    pub fragment: String,
    pub object_key: String,
    pub body: String,
}

#[derive(Debug, Deserialize)]
pub struct ListObjectCommentsQuery {
    pub fragment: String,
    pub object_key: String,
    pub include_deleted: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct ObjectCommentsResponse {
    pub comments: Vec<ObjectCommentRecord>,
}

#[derive(Debug, Serialize)]
pub struct DeleteObjectCommentResponse {
    pub removed: bool,
}

#[derive(Debug, Serialize)]
pub struct RevokeShareResponse {
    pub revoked: bool,
}

#[derive(Debug, Serialize)]
pub struct ListObjectsResponse {
    pub objects: Vec<ObjectRecord>,
    pub common_prefixes: Vec<String>,
    pub limit: usize,
    pub offset: usize,
    pub continuation_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ObjectStatsQuery {
    pub fragment: Option<String>,
    pub prefix: Option<String>,
    pub search: Option<String>,
    pub include_pending: Option<bool>,
    pub include_trashed: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectExtensionStat {
    pub extension: String,
    pub object_count: usize,
    pub total_size_bytes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectStatsResponse {
    pub total_objects: usize,
    pub total_size_bytes: i64,
    pub extension_stats: Vec<ObjectExtensionStat>,
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
    #[serde(default)]
    pub trash_entry_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FragmentStateRequest {
    pub fragment: String,
}

#[derive(Debug, Deserialize)]
pub struct BulkObjectKeysRequest {
    pub fragment: String,
    #[serde(default)]
    pub object_keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RenameRegistryObjectRequest {
    pub fragment: String,
    pub source_key: String,
    pub destination_key: String,
}

#[derive(Debug, Deserialize)]
pub struct PrefixObjectMutationRequest {
    pub fragment: String,
    pub prefix: String,
    #[serde(default)]
    pub trash_entry_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RenameRegistryPrefixRequest {
    pub fragment: String,
    pub source_prefix: String,
    pub destination_prefix: String,
}

#[derive(Debug, Deserialize)]
pub struct DeleteObjectQuery {
    pub fragment: String,
    pub object_key: String,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub trash_entry_id: Option<String>,
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
    pub refresh_token: Option<String>,
    pub refresh_expires_in: Option<u64>,
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

#[derive(Debug, Serialize)]
pub struct BulkObjectSelectionResponse {
    pub requested: usize,
    pub unique: usize,
    pub affected: usize,
    pub not_found: usize,
    pub quota_limit_bytes: i64,
    pub quota_used_bytes: i64,
}
