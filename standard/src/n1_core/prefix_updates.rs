use chrono::Utc;
use tokio::fs;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::PREFIX_SEGMENT_EXT;
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PrefixIndexEntry, PrefixIndexSegment,
};
use crate::n1_core::upload::write_atomic;

pub fn accelerator_prefix() -> &'static str {
    ""
}

pub fn object_key_prefix_scopes(object_key: &str) -> Vec<String> {
    let canonical = ObjectPathManager::canonical_prefix(object_key);
    let mut scopes = vec![accelerator_prefix().to_string()];

    if canonical.is_empty() {
        return scopes;
    }

    let mut current = String::new();
    for segment in canonical.split('/').filter(|segment| !segment.is_empty()) {
        if !current.is_empty() {
            current.push('/');
        }
        current.push_str(segment);
        scopes.push(current.clone());
    }

    scopes
}

pub fn object_key_directory_scopes(object_key: &str) -> Vec<String> {
    let mut scopes = object_key_prefix_scopes(object_key);
    if scopes.len() > 1 {
        scopes.pop();
    }
    scopes
}

fn accelerator_stale_marker_name() -> &'static str {
    ".stale"
}

pub fn synthetic_live_entry(metadata: &ObjectMetadata, created_at: i64) -> PrefixIndexEntry {
    PrefixIndexEntry {
        object_key: metadata.object_key.clone(),
        version_id: metadata.version_id.clone(),
        created_at,
        is_delete_marker: false,
        has_inline_summary: true,
        upload_session_id: metadata.upload_session_id.clone(),
        owner_uid: metadata.owner_uid.clone(),
        content_type: metadata.content_type.clone(),
        total_size_bytes: metadata.total_size_bytes,
        stripe_size_bytes: metadata.stripe_size_bytes,
    }
}

pub fn synthetic_delete_marker(metadata: &ObjectMetadata, created_at: i64) -> PrefixIndexEntry {
    PrefixIndexEntry {
        object_key: metadata.object_key.clone(),
        version_id: metadata.version_id.clone(),
        created_at,
        is_delete_marker: true,
        has_inline_summary: false,
        upload_session_id: String::new(),
        owner_uid: String::new(),
        content_type: String::new(),
        total_size_bytes: 0,
        stripe_size_bytes: metadata.stripe_size_bytes,
    }
}

pub fn logical_now() -> i64 {
    Utc::now().timestamp_micros()
}

pub async fn mark_fragment_accelerator_stale(
    fragment: &str,
    trashed: bool,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let bucket_dirs = if trashed {
        path_manager.get_all_trash_prefix_bucket_dirs(fragment, accelerator_prefix())
    } else {
        path_manager.get_all_prefix_bucket_dirs(fragment, accelerator_prefix())
    };

    let mut any_ok = false;
    for bucket in bucket_dirs {
        if fs::create_dir_all(&bucket).await.is_err() {
            continue;
        }
        if fs::write(bucket.join(accelerator_stale_marker_name()), b"")
            .await
            .is_ok()
        {
            any_ok = true;
        }
    }

    if !any_ok {
        return Err(AppError::InternalServerError(
            "failed to mark accelerator stale on index roots".into(),
        ));
    }

    Ok(())
}

pub async fn clear_fragment_accelerator_stale(fragment: &str, trashed: bool) {
    let path_manager = ObjectPathManager::new();
    let bucket_dirs = if trashed {
        path_manager.get_all_trash_prefix_bucket_dirs(fragment, accelerator_prefix())
    } else {
        path_manager.get_all_prefix_bucket_dirs(fragment, accelerator_prefix())
    };

    for bucket in bucket_dirs {
        let _ = fs::remove_file(bucket.join(accelerator_stale_marker_name())).await;
    }
}

pub async fn fragment_accelerator_is_stale(fragment: &str, trashed: bool) -> bool {
    let path_manager = ObjectPathManager::new();
    let bucket_dirs = if trashed {
        path_manager.get_all_trash_prefix_bucket_dirs(fragment, accelerator_prefix())
    } else {
        path_manager.get_all_prefix_bucket_dirs(fragment, accelerator_prefix())
    };

    for bucket in bucket_dirs {
        if fs::try_exists(&bucket.join(accelerator_stale_marker_name()))
            .await
            .unwrap_or(false)
        {
            return true;
        }
    }

    false
}

pub async fn write_prefix_entry_update(
    fragment: &str,
    _object_key: &str,
    segment_id: &str,
    entry: PrefixIndexEntry,
    _child_count_delta: i64,
    trashed: bool,
) -> Result<(), AppError> {
    write_prefix_entries_update(fragment, segment_id, vec![entry], trashed).await
}

pub async fn write_prefix_entries_update(
    fragment: &str,
    segment_id: &str,
    entries: Vec<PrefixIndexEntry>,
    trashed: bool,
) -> Result<(), AppError> {
    write_prefix_entries_update_for_prefix(
        fragment,
        accelerator_prefix(),
        segment_id,
        entries,
        trashed,
    )
    .await
}

pub async fn write_prefix_entries_update_for_prefix(
    fragment: &str,
    prefix: &str,
    segment_id: &str,
    entries: Vec<PrefixIndexEntry>,
    trashed: bool,
) -> Result<(), AppError> {
    if entries.is_empty() {
        return Ok(());
    }

    let scope = ObjectPathManager::canonical_prefix(prefix);
    let path_manager = ObjectPathManager::new();
    let seg = PrefixIndexSegment {
        prefix: scope.clone(),
        entries,
    };
    let bytes = rmp_serde::to_vec(&seg)
        .map_err(|e| AppError::InternalServerError(format!("serialize prefix segment: {}", e)))?;
    let file = format!("seg_{}{}", segment_id, PREFIX_SEGMENT_EXT);

    let bucket_dirs = if trashed {
        path_manager.get_all_trash_prefix_bucket_dirs(fragment, &scope)
    } else {
        path_manager.get_all_prefix_bucket_dirs(fragment, &scope)
    };

    let mut any_ok = false;
    for bucket in bucket_dirs {
        let path = bucket.join(&file);
        if write_atomic(&path, &bytes).await.is_ok() {
            any_ok = true;
        }
    }

    if !any_ok {
        return Err(AppError::InternalServerError(
            "failed to write prefix update on index roots".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{object_key_directory_scopes, object_key_prefix_scopes, synthetic_live_entry};
    use crate::n1_core::disk_manager::ObjectMetadata;

    #[test]
    fn synthetic_live_entry_keeps_inline_summary() {
        let metadata = ObjectMetadata {
            object_key: "patients/a/image.dcm".to_string(),
            fragment: "frag".to_string(),
            version_id: "01".to_string(),
            upload_session_id: "sess".to_string(),
            owner_uid: "owner".to_string(),
            content_type: "application/dicom".to_string(),
            total_size_bytes: 123,
            created_at: 456,
            parts: Vec::new(),
            is_delete_marker: false,
            stripe_size_bytes: 4096,
        };

        let entry = synthetic_live_entry(&metadata, 789);

        assert!(entry.has_inline_summary);
        assert_eq!(entry.object_key, metadata.object_key);
        assert_eq!(entry.version_id, metadata.version_id);
        assert_eq!(entry.total_size_bytes, metadata.total_size_bytes);
    }

    #[test]
    fn object_key_prefix_scopes_include_root_and_parents() {
        let scopes = object_key_prefix_scopes("patients/12-1222/study/series/file.dcm");
        assert_eq!(
            scopes,
            vec![
                "".to_string(),
                "patients".to_string(),
                "patients/12-1222".to_string(),
                "patients/12-1222/study".to_string(),
                "patients/12-1222/study/series".to_string(),
                "patients/12-1222/study/series/file.dcm".to_string(),
            ]
        );
    }

    #[test]
    fn object_key_directory_scopes_exclude_leaf_object() {
        let scopes = object_key_directory_scopes("patients/12-1222/study/series/file.dcm");
        assert_eq!(
            scopes,
            vec![
                "".to_string(),
                "patients".to_string(),
                "patients/12-1222".to_string(),
                "patients/12-1222/study".to_string(),
                "patients/12-1222/study/series".to_string(),
            ]
        );
    }
}
