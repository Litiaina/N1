use crate::core::modify::sync_dir;
use crate::enums::app_error_enum::AppError;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;
use ulid::Ulid;

pub(crate) const OBJECT_METADATA_FILE: &str = ".nmtd";
pub(crate) const OBJECT_LATEST_FILE: &str = ".nlatest";
pub(crate) const LOCAL_CLUSTER_SHARD_METADATA_FILE: &str = ".nmtd";
pub(crate) const N2_OBJECTS_DIR: &str = "objects";
pub(crate) const N2_NAMESPACE_DIR: &str = "namespace";

const N2_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS.add(b'%').add(b'/').add(b'\\').add(b'\0');

pub(crate) fn object_key_hash(object_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    hex::encode(hasher.finalize())
}

pub(crate) fn map_dir_for_root(root: &Path, fragment: &str, key_hash: &str) -> PathBuf {
    root.join(fragment)
        .join("n2")
        .join(N2_OBJECTS_DIR)
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

pub(crate) fn normalize_object_key(object_key: &str) -> String {
    object_key
        .trim()
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

pub(crate) fn encode_key_segment(segment: &str) -> String {
    utf8_percent_encode(segment, N2_SEGMENT_ENCODE_SET).to_string()
}

pub(crate) fn decode_key_segment(segment: &str) -> Result<String, AppError> {
    percent_decode_str(segment)
        .decode_utf8()
        .map(|v| v.to_string())
        .map_err(|e| AppError::BadRequest(format!("invalid N2 namespace segment: {}", e)))
}

pub(crate) fn namespace_root_for_root(root: &Path, fragment: &str) -> PathBuf {
    root.join(fragment).join("n2").join(N2_NAMESPACE_DIR)
}

pub(crate) fn namespace_path_for_root(root: &Path, fragment: &str, object_key: &str) -> PathBuf {
    let mut path = namespace_root_for_root(root, fragment);
    for segment in normalize_object_key(object_key).split('/') {
        if !segment.is_empty() {
            path = path.join(encode_key_segment(segment));
        }
    }
    path
}

pub(crate) fn decode_namespace_relative_path(path: &Path) -> Result<String, AppError> {
    let mut out = Vec::new();
    for component in path.components() {
        let Some(raw) = component.as_os_str().to_str() else {
            return Err(AppError::BadRequest(
                "N2 namespace path is not valid UTF-8".to_string(),
            ));
        };
        if raw.is_empty() {
            continue;
        }
        out.push(decode_key_segment(raw)?);
    }
    Ok(out.join("/"))
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
