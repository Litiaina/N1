use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::structs::auth_structs::AuthSessionScope;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use uuid::Uuid;

const REFRESH_STORE_DIR: &str = ".auth/refresh_sessions";

pub(crate) fn credential_fingerprint(credential: &str) -> String {
    hex::encode(Sha256::digest(credential.as_bytes()))
}

static REFRESH_STORE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SessionPermissions {
    #[serde(default)]
    pub operator: bool,
    pub read: bool,
    pub write: bool,
    pub modify: bool,
    pub delete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RefreshSessionRecord {
    #[serde(default)]
    credential_fingerprint: Option<String>,
    session_id: String,
    scope: AuthSessionScope,
    permissions: SessionPermissions,
    access_expires_in: u64,
    refresh_expires_at: i64,
    current_token_hash: String,
    previous_token_hash: Option<String>,
    rotated_at: Option<i64>,
    created_at: i64,
    last_used_at: i64,
    revoked_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct IssuedRefreshSession {
    pub refresh_token: String,
    pub refresh_expires_in: u64,
}

#[derive(Debug, Clone)]
pub struct RotatedRefreshSession {
    pub scope: AuthSessionScope,
    pub permissions: SessionPermissions,
    pub access_expires_in: u64,
    pub refresh_token: String,
    pub refresh_expires_in: u64,
}

#[derive(Debug)]
pub enum RefreshStoreError {
    InvalidToken,
    TokenExpired,
    TokenReuseDetected,
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl From<std::io::Error> for RefreshStoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for RefreshStoreError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

pub async fn create_refresh_session(
    scope: AuthSessionScope,
    permissions: SessionPermissions,
    access_expires_in: u64,
    refresh_expires_in: u64,
    credential_fingerprint: String,
) -> Result<IssuedRefreshSession, RefreshStoreError> {
    let _guard = REFRESH_STORE_LOCK.lock().await;

    let now = unix_ts();
    let session_id = Uuid::new_v4().to_string();
    let token_secret = random_token_secret();
    let refresh_token = build_refresh_token(&session_id, &token_secret);
    let record = RefreshSessionRecord {
        credential_fingerprint: Some(credential_fingerprint),
        session_id: session_id.clone(),
        scope,
        permissions,
        access_expires_in,
        refresh_expires_at: now + refresh_expires_in as i64,
        current_token_hash: hash_secret(&token_secret),
        previous_token_hash: None,
        rotated_at: None,
        created_at: now,
        last_used_at: now,
        revoked_at: None,
    };

    write_record(&record).await?;

    Ok(IssuedRefreshSession {
        refresh_token,
        refresh_expires_in,
    })
}

pub async fn rotate_refresh_session(
    refresh_token: &str,
) -> Result<RotatedRefreshSession, RefreshStoreError> {
    let _guard = REFRESH_STORE_LOCK.lock().await;

    let (session_id, presented_secret) =
        parse_refresh_token(refresh_token).ok_or(RefreshStoreError::InvalidToken)?;
    let mut record = read_record(&session_id).await?;
    let now = unix_ts();

    if record.revoked_at.is_some() {
        return Err(RefreshStoreError::InvalidToken);
    }

    if now >= record.refresh_expires_at {
        delete_record_if_present(&session_id).await?;
        return Err(RefreshStoreError::TokenExpired);
    }

    let current = match &record.scope {
        AuthSessionScope::Fragment { fragment } => {
            let meta = crate::core::fragment_metadata::read_fragment_metadata(fragment)
                .await
                .map_err(|_| RefreshStoreError::InvalidToken)?;
            credential_fingerprint(if record.permissions.operator {
                crate::middleware::auth_middleware::MASTER_KEY.as_str()
            } else {
                meta.key_phc.as_str()
            })
        }
        AuthSessionScope::FileSystem { .. } => {
            credential_fingerprint(crate::middleware::auth_middleware::AUTH_KEY.as_str())
        }
    };
    if record.credential_fingerprint.as_deref() != Some(current.as_str()) {
        return Err(RefreshStoreError::InvalidToken);
    }

    let presented_hash = hash_secret(&presented_secret);
    if record.previous_token_hash.as_deref() == Some(presented_hash.as_str()) {
        record.revoked_at = Some(now);
        write_record(&record).await?;
        return Err(RefreshStoreError::TokenReuseDetected);
    }

    if record.current_token_hash != presented_hash {
        return Err(RefreshStoreError::InvalidToken);
    }

    let new_secret = random_token_secret();
    let new_refresh_token = build_refresh_token(&session_id, &new_secret);
    let previous_hash = std::mem::replace(&mut record.current_token_hash, hash_secret(&new_secret));
    record.previous_token_hash = Some(previous_hash);
    record.rotated_at = Some(now);
    record.last_used_at = now;

    write_record(&record).await?;

    Ok(RotatedRefreshSession {
        scope: record.scope,
        permissions: record.permissions,
        access_expires_in: record.access_expires_in,
        refresh_token: new_refresh_token,
        refresh_expires_in: (record.refresh_expires_at - now).max(0) as u64,
    })
}

pub async fn revoke_refresh_session(refresh_token: &str) -> Result<(), RefreshStoreError> {
    let _guard = REFRESH_STORE_LOCK.lock().await;

    let (session_id, presented_secret) =
        parse_refresh_token(refresh_token).ok_or(RefreshStoreError::InvalidToken)?;
    let mut record = read_record(&session_id).await?;
    let presented_hash = hash_secret(&presented_secret);

    if record.current_token_hash != presented_hash
        && record.previous_token_hash.as_deref() != Some(presented_hash.as_str())
    {
        return Err(RefreshStoreError::InvalidToken);
    }

    record.revoked_at = Some(unix_ts());
    write_record(&record).await?;
    Ok(())
}

fn refresh_store_root() -> PathBuf {
    if let Ok(root) = std::env::var("N1_REFRESH_STORE_ROOT") {
        return PathBuf::from(root);
    }
    if let Some(root) = FS_STORAGE_ROOT.get() {
        return PathBuf::from(root).join(REFRESH_STORE_DIR);
    }
    std::env::temp_dir()
        .join(format!("litiaina-n1-auth-{}", std::process::id()))
        .join(REFRESH_STORE_DIR)
}

fn session_record_path(session_id: &str) -> PathBuf {
    refresh_store_root().join(format!("{session_id}.json"))
}

async fn ensure_store_dir(root: &Path) -> Result<(), std::io::Error> {
    fs::create_dir_all(root).await
}

async fn write_record(record: &RefreshSessionRecord) -> Result<(), RefreshStoreError> {
    let path = session_record_path(&record.session_id);
    let root = path
        .parent()
        .ok_or_else(|| std::io::Error::other("missing refresh session parent"))?
        .to_path_buf();
    ensure_store_dir(&root).await?;

    let payload = serde_json::to_vec(record)?;
    let tmp_path = root.join(format!("{}.tmp-{}", record.session_id, Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp_path)
        .await?;
    file.write_all(&payload).await?;
    file.sync_all().await?;
    drop(file);
    fs::rename(&tmp_path, &path).await?;
    fs::File::open(&root).await?.sync_all().await?;
    Ok(())
}

async fn read_record(session_id: &str) -> Result<RefreshSessionRecord, RefreshStoreError> {
    let path = session_record_path(session_id);
    let payload = fs::read(path).await.map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => RefreshStoreError::InvalidToken,
        _ => RefreshStoreError::Io(err),
    })?;
    Ok(serde_json::from_slice(&payload)?)
}

async fn delete_record_if_present(session_id: &str) -> Result<(), RefreshStoreError> {
    let path = session_record_path(session_id);
    let root = path
        .parent()
        .ok_or_else(|| std::io::Error::other("missing refresh session parent"))?
        .to_path_buf();
    match fs::remove_file(path).await {
        Ok(()) => {
            fs::File::open(&root).await?.sync_all().await?;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(RefreshStoreError::Io(e)),
    }
}

fn unix_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("unix epoch")
        .as_secs() as i64
}

fn random_token_secret() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn build_refresh_token(session_id: &str, secret: &str) -> String {
    format!("{session_id}.{secret}")
}

fn parse_refresh_token(token: &str) -> Option<(String, String)> {
    let (session_id, secret) = token.split_once('.')?;
    if session_id.is_empty() || secret.is_empty() {
        return None;
    }
    Some((session_id.to_string(), secret.to_string()))
}

fn hash_secret(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
pub async fn clear_refresh_store_for_tests() {
    let _guard = REFRESH_STORE_LOCK.lock().await;
    let _ = fs::remove_dir_all(refresh_store_root()).await;
}
