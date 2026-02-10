use crate::object_storage::disk_manager::ObjectPathManager;
use crate::{enums::app_error_enum::AppError, object_storage::constants::FRAGMENT_METADATA_FILE};

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use chrono::{SecondsFormat, Utc};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use tokio::fs as tokio_fs;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMetadata {
    pub fragment: String,
    pub key_phc: String,
    pub created: String,
    pub updated: Option<String>,
}

fn now_utc_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn fragment_meta_path(root: &Path, fragment_hash: &str) -> PathBuf {
    root.join(fragment_hash).join(FRAGMENT_METADATA_FILE)
}

async fn sync_dir(dir: &Path) -> Result<(), AppError> {
    let f = tokio_fs::File::open(dir)
        .await
        .map_err(|e| AppError::Io(e, format!("open dir {}", dir.display())))?;
    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync dir {}", dir.display())))?;
    Ok(())
}

async fn durable_atomic_write(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("missing parent for {}", path.display()))
    })?;

    tokio_fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;

    let file_name = path.file_name().and_then(|v| v.to_str()).unwrap_or("file");

    let tmp = parent.join(format!(".{}.tmp.{}", file_name, Ulid::new().to_string()));

    let mut f = tokio_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("create tmp {}", tmp.display())))?;

    f.write_all(bytes)
        .await
        .map_err(|e| AppError::Io(e, format!("write tmp {}", tmp.display())))?;

    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync tmp {}", tmp.display())))?;

    drop(f);

    tokio_fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;

    sync_dir(parent).await?;
    Ok(())
}

fn hash_key_to_phc(key: &str) -> Result<String, AppError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    let phc = argon2
        .hash_password(key.as_bytes(), &salt)
        .map_err(|e| AppError::InternalServerError(format!("argon2 hash: {}", e)))?
        .to_string();

    Ok(phc)
}

pub async fn init_fragment_metadata(
    fragment_hash: &str,
    key: &str,
) -> Result<FragmentMetadata, AppError> {
    if key.is_empty() {
        return Err(AppError::BadRequest("key must not be empty".into()));
    }

    let meta = FragmentMetadata {
        fragment: fragment_hash.to_string(),
        key_phc: hash_key_to_phc(key)?,
        created: now_utc_rfc3339(),
        updated: None,
    };

    write_fragment_metadata(&meta).await?;
    Ok(meta)
}

pub async fn read_fragment_metadata(fragment_hash: &str) -> Result<FragmentMetadata, AppError> {
    let pm = ObjectPathManager::new();
    let roots = pm.get_all_index_roots();

    for root in roots {
        let p = fragment_meta_path(&root, fragment_hash);
        if !tokio_fs::try_exists(&p).await.unwrap_or(false) {
            continue;
        }

        let raw = tokio_fs::read(&p)
            .await
            .map_err(|e| AppError::Io(e, format!("read {}", p.display())))?;

        let meta = rmp_serde::from_slice::<FragmentMetadata>(&raw)
            .map_err(|e| AppError::InternalServerError(format!("decode msgpack: {}", e)))?;

        if meta.fragment == fragment_hash {
            return Ok(meta);
        }
    }

    Err(AppError::NotFound)
}

pub async fn write_fragment_metadata(meta: &FragmentMetadata) -> Result<(), AppError> {
    let pm = ObjectPathManager::new();
    let roots = pm.get_all_index_roots();

    let bytes = rmp_serde::to_vec(meta)
        .map_err(|e| AppError::InternalServerError(format!("encode msgpack: {}", e)))?;

    let mut any_ok = false;

    for root in roots {
        let p = fragment_meta_path(&root, &meta.fragment);
        if durable_atomic_write(&p, &bytes).await.is_ok() {
            any_ok = true;
        }
    }

    if !any_ok {
        return Err(AppError::InternalServerError(
            "failed to write fragment metadata to any index root".into(),
        ));
    }

    Ok(())
}

pub async fn rotate_fragment_key(fragment_hash: &str, new_key: &str) -> Result<(), AppError> {
    if new_key.is_empty() {
        return Err(AppError::BadRequest("new_key must not be empty".into()));
    }

    let mut meta = match read_fragment_metadata(fragment_hash).await {
        Ok(m) => m,
        Err(_) => FragmentMetadata {
            fragment: fragment_hash.to_string(),
            key_phc: String::new(),
            created: now_utc_rfc3339(),
            updated: None,
        },
    };

    meta.key_phc = hash_key_to_phc(new_key)?;
    meta.updated = Some(now_utc_rfc3339());
    write_fragment_metadata(&meta).await
}

pub async fn verify_fragment_key(fragment_hash: &str, key: &str) -> Result<bool, AppError> {
    if key.is_empty() {
        return Ok(false);
    }

    let meta = read_fragment_metadata(fragment_hash).await?;

    if meta.key_phc.is_empty() {
        return Ok(false);
    }

    let parsed = PasswordHash::new(&meta.key_phc)
        .map_err(|e| AppError::InternalServerError(format!("bad phc: {}", e)))?;

    Ok(Argon2::default()
        .verify_password(key.as_bytes(), &parsed)
        .is_ok())
}
