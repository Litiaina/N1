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

fn accelerator_stale_marker_name() -> &'static str {
    ".stale"
}

pub fn synthetic_live_entry(metadata: &ObjectMetadata, created_at: i64) -> PrefixIndexEntry {
    PrefixIndexEntry {
        object_key: metadata.object_key.clone(),
        version_id: metadata.version_id.clone(),
        created_at,
        is_delete_marker: false,
        has_inline_summary: false,
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
    Utc::now().timestamp()
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
    let path_manager = ObjectPathManager::new();
    let seg = PrefixIndexSegment {
        prefix: accelerator_prefix().to_string(),
        entries: vec![entry],
    };
    let bytes = rmp_serde::to_vec(&seg)
        .map_err(|e| AppError::InternalServerError(format!("serialize prefix segment: {}", e)))?;
    let file = format!("seg_{}{}", segment_id, PREFIX_SEGMENT_EXT);

    let bucket_dirs = if trashed {
        path_manager.get_all_trash_prefix_bucket_dirs(fragment, accelerator_prefix())
    } else {
        path_manager.get_all_prefix_bucket_dirs(fragment, accelerator_prefix())
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
