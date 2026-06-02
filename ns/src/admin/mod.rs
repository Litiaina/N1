use crate::enums::app_error_enum::AppError;
use crate::local_auth::nt_hash_hex_for_secret;
use crate::metadata::{
    NasDiskRecord, NasMetadataStore, NasPermission, NasShareRecord, NasUserRecord,
    NasUserShareAccess,
};
use crate::n1_sync::restore_share_file_from_n1;
use crate::settings::config::{NsAdminConfig, NsConfig};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::net::SocketAddr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct AdminRuntime {
    pub addr: SocketAddr,
    pub join: JoinHandle<()>,
}

#[derive(Clone)]
struct AdminState {
    metadata: NasMetadataStore,
    config: Arc<NsConfig>,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    version: &'static str,
    shares: usize,
    users: usize,
    disks: usize,
    sync_enabled: bool,
    smb_enabled: bool,
}

#[derive(Debug, Serialize)]
struct DiskStatusResponse {
    id: String,
    root_path: String,
    enabled: bool,
    exists: bool,
    total_bytes: Option<u64>,
    available_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct UpsertDiskRequest {
    id: String,
    root_path: String,
    #[serde(default = "default_true")]
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct UpsertShareRequest {
    name: String,
    fragment_hash: Option<String>,
    root_path: Option<String>,
    disk_id: Option<String>,
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    n1_base_url: Option<String>,
    #[serde(default)]
    cold_sync_enabled: bool,
    #[serde(default)]
    cold_n1_prefix: String,
}

#[derive(Debug, Deserialize)]
struct UpsertUserRequest {
    username: String,
    password: Option<String>,
    nt_hash_hex: Option<String>,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default)]
    shares: Vec<UserShareAccessRequest>,
}

#[derive(Debug, Deserialize)]
struct RestoreRequest {
    share: String,
    path: String,
}

#[derive(Debug, Serialize)]
struct RestoreResponse {
    share: String,
    path: String,
    restored_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct UserShareAccessRequest {
    share: String,
    #[serde(default = "default_true")]
    read: bool,
    #[serde(default)]
    write: bool,
    #[serde(default)]
    delete: bool,
    #[serde(default)]
    admin: bool,
}

pub async fn start(
    config: Arc<NsConfig>,
    metadata: NasMetadataStore,
) -> Result<Option<AdminRuntime>, AppError> {
    if !config.admin.enabled {
        return Ok(None);
    }
    let addr: SocketAddr = format!("{}:{}", config.admin.bind_addr, config.admin.port)
        .parse()
        .map_err(|err| AppError::BadRequest(format!("invalid NS admin bind address: {}", err)))?;
    let state = AdminState { metadata, config };
    let app = Router::new()
        .route("/ns/admin/status", get(status))
        .route("/ns/admin/shares", get(list_shares).post(upsert_share))
        .route("/ns/admin/disks", get(list_disks).post(upsert_disk))
        .route("/ns/admin/disks/status", get(list_disk_status))
        .route("/ns/admin/users", get(list_users).post(upsert_user))
        .route("/ns/admin/restore", post(restore_file))
        .with_state(state);
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| AppError::Io(err, format!("bind NS admin {}", addr)))?;
    let addr = listener
        .local_addr()
        .map_err(|err| AppError::Io(err, "inspect NS admin listener".to_string()))?;
    let join = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!("NS admin listener exited: {}", err);
        }
    });
    Ok(Some(AdminRuntime { addr, join }))
}

async fn status(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, AppError> {
    require_admin(&state.config.admin, &headers)?;
    Ok(Json(StatusResponse {
        version: env!("CARGO_PKG_VERSION"),
        shares: state.metadata.list_shares()?.len(),
        users: state.metadata.list_users()?.len(),
        disks: state.metadata.list_disks()?.len(),
        sync_enabled: state.config.sync.enabled,
        smb_enabled: state.config.smb.enabled,
    }))
}

async fn list_shares(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<Vec<NasShareRecord>>, AppError> {
    require_admin(&state.config.admin, &headers)?;
    Ok(Json(state.metadata.list_shares()?))
}

async fn list_disks(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<Vec<NasDiskRecord>>, AppError> {
    require_admin(&state.config.admin, &headers)?;
    Ok(Json(state.metadata.list_disks()?))
}

async fn list_disk_status(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<Vec<DiskStatusResponse>>, AppError> {
    require_admin(&state.config.admin, &headers)?;
    let mut out = Vec::new();
    for disk in state.metadata.list_disks()? {
        out.push(disk_status(&disk)?);
    }
    Ok(Json(out))
}

async fn list_users(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<Vec<NasUserRecord>>, AppError> {
    require_admin(&state.config.admin, &headers)?;
    let mut users = state.metadata.list_users()?;
    for user in &mut users {
        user.nt_hash_hex.clear();
    }
    Ok(Json(users))
}

async fn upsert_disk(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<UpsertDiskRequest>,
) -> Result<(StatusCode, Json<NasDiskRecord>), AppError> {
    require_admin(&state.config.admin, &headers)?;
    let id = request.id.trim().to_ascii_lowercase();
    if id.is_empty() || request.root_path.trim().is_empty() {
        return Err(AppError::BadRequest(
            "disk id and root_path are required".to_string(),
        ));
    }
    std::fs::create_dir_all(request.root_path.trim())
        .map_err(|err| AppError::Io(err, format!("mkdir {}", request.root_path)))?;
    let record = NasDiskRecord {
        id,
        root_path: request.root_path.trim().to_string(),
        enabled: request.enabled,
    };
    state.metadata.write_disk_record(&record)?;
    Ok((StatusCode::OK, Json(record)))
}

async fn upsert_share(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<UpsertShareRequest>,
) -> Result<(StatusCode, Json<NasShareRecord>), AppError> {
    require_admin(&state.config.admin, &headers)?;
    let name = request.name.trim().to_ascii_lowercase();
    if name.is_empty() {
        return Err(AppError::BadRequest("share name is required".to_string()));
    }
    let root_path = resolve_admin_share_root(&state.metadata, &request, name.as_str())?;
    std::fs::create_dir_all(root_path.as_str())
        .map_err(|err| AppError::Io(err, format!("mkdir {}", root_path)))?;
    let record = NasShareRecord {
        name,
        fragment_hash: request
            .fragment_hash
            .unwrap_or_else(|| stable_share_hash(request.name.as_str())),
        root_path,
        read_only: request.read_only,
        n1_base_url: request
            .n1_base_url
            .or_else(|| state.config.n1.base_url.clone()),
        cold_sync_enabled: state.config.sync.enabled && request.cold_sync_enabled,
        cold_n1_prefix: request.cold_n1_prefix.trim_matches('/').to_string(),
        n1_access_token: None,
        n1_access_expires_at_ms: None,
        n1_refresh_token: None,
        n1_refresh_expires_at_ms: None,
    };
    state.metadata.write_share_record(&record)?;
    Ok((StatusCode::OK, Json(record)))
}

fn resolve_admin_share_root(
    metadata: &NasMetadataStore,
    request: &UpsertShareRequest,
    share_name: &str,
) -> Result<String, AppError> {
    if let Some(root_path) = request
        .root_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(root_path.to_string());
    }
    let disk_id = request
        .disk_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::BadRequest("root_path or disk_id is required".to_string()))?
        .to_ascii_lowercase();
    let disk = metadata
        .list_disks()?
        .into_iter()
        .find(|disk| disk.id == disk_id)
        .ok_or_else(|| AppError::BadRequest(format!("disk '{}' was not found", disk_id)))?;
    if !disk.enabled {
        return Err(AppError::BadRequest(format!(
            "disk '{}' is disabled",
            disk_id
        )));
    }
    Ok(Path::new(disk.root_path.as_str())
        .join(share_name)
        .display()
        .to_string())
}

async fn upsert_user(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<UpsertUserRequest>,
) -> Result<StatusCode, AppError> {
    require_admin(&state.config.admin, &headers)?;
    let username = request.username.trim().to_ascii_lowercase();
    if username.is_empty() {
        return Err(AppError::BadRequest("username is required".to_string()));
    }
    let nt_hash_hex = request
        .nt_hash_hex
        .map(|value| value.trim().to_ascii_lowercase())
        .or_else(|| {
            request
                .password
                .as_ref()
                .map(|password| nt_hash_hex_for_secret(password))
        })
        .ok_or_else(|| AppError::BadRequest("password or nt_hash_hex is required".to_string()))?;
    let shares = if request.shares.is_empty() {
        vec![NasUserShareAccess {
            share: "*".to_string(),
            permission: NasPermission::read_only(),
        }]
    } else {
        request
            .shares
            .into_iter()
            .map(|share| NasUserShareAccess {
                share: share.share.trim().to_ascii_lowercase(),
                permission: NasPermission {
                    read: share.read || share.admin,
                    write: share.write || share.admin,
                    delete: share.delete || share.admin,
                    admin: share.admin,
                },
            })
            .collect()
    };
    state.metadata.write_user_record(&NasUserRecord {
        username,
        nt_hash_hex,
        enabled: request.enabled,
        shares,
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn restore_file(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<RestoreRequest>,
) -> Result<Json<RestoreResponse>, AppError> {
    require_admin(&state.config.admin, &headers)?;
    let share_name = request.share.trim().to_ascii_lowercase();
    let path = request.path.trim_matches('/').to_string();
    if share_name.is_empty() || path.is_empty() {
        return Err(AppError::BadRequest(
            "share and path are required".to_string(),
        ));
    }
    let share = state
        .metadata
        .share(share_name.as_str())?
        .ok_or(AppError::NotFound)?;
    let restored_bytes = restore_share_file_from_n1(&state.config, &share, path.as_str()).await?;
    Ok(Json(RestoreResponse {
        share: share_name,
        path,
        restored_bytes,
    }))
}

fn require_admin(config: &NsAdminConfig, headers: &HeaderMap) -> Result<(), AppError> {
    let Some(expected) = config.admin_key.as_ref().filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let actual = headers
        .get("x-ns-admin-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if actual == expected {
        return Ok(());
    }
    Err(AppError::Unauthorized)
}

fn stable_share_hash(value: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(value.trim().to_ascii_lowercase().as_bytes());
    hex::encode(hasher.finalize())
}

fn default_true() -> bool {
    true
}

fn disk_status(disk: &NasDiskRecord) -> Result<DiskStatusResponse, AppError> {
    let path = Path::new(disk.root_path.as_str());
    let exists = path.exists();
    let (total_bytes, available_bytes) = if exists {
        statvfs_bytes(path)?
    } else {
        (None, None)
    };
    Ok(DiskStatusResponse {
        id: disk.id.clone(),
        root_path: disk.root_path.clone(),
        enabled: disk.enabled,
        exists,
        total_bytes,
        available_bytes,
    })
}

fn statvfs_bytes(path: &Path) -> Result<(Option<u64>, Option<u64>), AppError> {
    let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        AppError::BadRequest(format!("path contains interior NUL: {}", path.display()))
    })?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(AppError::Io(
            std::io::Error::last_os_error(),
            format!("statvfs {}", path.display()),
        ));
    }
    let stat = unsafe { stat.assume_init() };
    let block_size = stat.f_frsize;
    Ok((
        Some(stat.f_blocks.saturating_mul(block_size)),
        Some(stat.f_bavail.saturating_mul(block_size)),
    ))
}
