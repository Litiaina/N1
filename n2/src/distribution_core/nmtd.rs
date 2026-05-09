use crate::core::modify::sync_dir;
use crate::enums::app_error_enum::AppError;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;
use ulid::Ulid;

pub(crate) const OBJECT_METADATA_FILE: &str = ".nmtd";
pub(crate) const OBJECT_LATEST_FILE: &str = ".nlatest";
pub(crate) const LOCAL_CLUSTER_SHARD_METADATA_FILE: &str = ".nmtd";

pub(crate) fn object_key_hash(object_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    hex::encode(hasher.finalize())
}

pub(crate) fn map_dir_for_root(root: &Path, fragment: &str, key_hash: &str) -> PathBuf {
    root.join(fragment)
        .join("n2")
        .join("objects")
        .join(&key_hash[0..2])
        .join(&key_hash[2..4])
        .join(&key_hash[4..6])
        .join(&key_hash[6..8])
        .join(key_hash)
}

pub(crate) fn map_path_for_root(
    root: &Path,
    fragment: &str,
    key_hash: &str,
    version_id: &str,
) -> PathBuf {
    map_dir_for_root(root, fragment, key_hash)
        .join(version_id)
        .join(OBJECT_METADATA_FILE)
}

pub(crate) fn latest_path_for_root(root: &Path, fragment: &str, key_hash: &str) -> PathBuf {
    map_dir_for_root(root, fragment, key_hash).join(OBJECT_LATEST_FILE)
}

pub(crate) async fn write_atomic(path: &Path, data: &[u8]) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::Io(e, format!("mkdir {:?}", parent)))?;
    }
    let tmp = path.with_extension(format!("tmp.{}", Ulid::new()));
    fs::write(&tmp, data)
        .await
        .map_err(|e| AppError::Io(e, format!("write tmp {:?}", tmp)))?;
    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {:?} -> {:?}", tmp, path)))?;
    if let Some(parent) = path.parent() {
        let _ = sync_dir(parent).await;
    }
    Ok(())
}
