use crate::core::modify::sync_dir;
use crate::enums::app_error_enum::AppError;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;
use ulid::Ulid;

pub(crate) const OBJECT_METADATA_EXT: &str = "nmtd";
pub(crate) const LOCAL_CLUSTER_SHARD_METADATA_FILE: &str = ".nmtd";
pub(crate) const N2_OBJECTS_DIR: &str = "objects";
pub(crate) const N2_NAMESPACE_DIR: &str = "namespace";
pub(crate) const N2_OBJECT_ID_PREFIX: &str = "@o_";

const N2_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS.add(b'%').add(b'/').add(b'\\').add(b'\0');

pub(crate) fn new_object_id() -> String {
    let mut hasher = Sha256::new();
    hasher.update(Ulid::new().to_string().as_bytes());
    hex::encode(hasher.finalize())
}

pub(crate) fn normalize_object_id(object_id: &str) -> String {
    object_id
        .trim()
        .trim_start_matches(N2_OBJECT_ID_PREFIX)
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

pub(crate) fn object_dir_name(object_id: &str) -> String {
    format!("{}{}", N2_OBJECT_ID_PREFIX, normalize_object_id(object_id))
}

pub(crate) fn object_id_from_dir_name(name: &str) -> Option<String> {
    name.strip_prefix(N2_OBJECT_ID_PREFIX)
        .map(normalize_object_id)
        .filter(|id| id.len() >= 8)
}

fn fanout_prefix(object_id: &str, start: usize) -> &str {
    let end = (start + 2).min(object_id.len());
    if start >= end {
        "00"
    } else {
        &object_id[start..end]
    }
}

pub(crate) fn object_dir_for_root(root: &Path, fragment: &str, object_id: &str) -> PathBuf {
    let object_id = normalize_object_id(object_id);
    root.join(fragment)
        .join("n2")
        .join(N2_OBJECTS_DIR)
        .join(fanout_prefix(&object_id, 0))
        .join(fanout_prefix(&object_id, 2))
        .join(fanout_prefix(&object_id, 4))
        .join(fanout_prefix(&object_id, 6))
        .join(object_dir_name(&object_id))
}

pub(crate) fn record_path_for_root(
    root: &Path,
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> PathBuf {
    object_dir_for_root(root, fragment, object_id).join(format!(
        "{}.{}",
        version_id.trim(),
        OBJECT_METADATA_EXT
    ))
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
