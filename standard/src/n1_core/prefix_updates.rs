use std::collections::BTreeSet;

use chrono::Utc;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::PREFIX_SEGMENT_EXT;
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PrefixIndexEntry, PrefixIndexSegment,
};
use crate::n1_core::prefix_compactor::{
    schedule_prefix_compaction, schedule_trash_prefix_compaction,
};
use crate::n1_core::prefix_delimiter::{write_delimiter_delta, write_trash_delimiter_delta};
use crate::n1_core::upload::write_atomic;

pub fn prefixes_for_key(object_key: &str) -> BTreeSet<String> {
    let key = ObjectPathManager::canonical_prefix(object_key);
    let mut out = BTreeSet::new();
    out.insert(String::new());

    if key.is_empty() {
        return out;
    }

    let parts: Vec<&str> = key.split('/').filter(|p| !p.is_empty()).collect();
    let mut cur = String::new();

    for (idx, part) in parts.iter().enumerate() {
        if idx == 0 {
            cur.push_str(part);
        } else {
            cur.push('/');
            cur.push_str(part);
        }
        out.insert(cur.clone());
    }

    out
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

pub async fn write_prefix_entry_update(
    fragment: &str,
    object_key: &str,
    segment_id: &str,
    entry: PrefixIndexEntry,
    child_count_delta: i64,
    trashed: bool,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let prefixes = prefixes_for_key(object_key);
    let mut any_ok = false;

    for prefix in &prefixes {
        let seg = PrefixIndexSegment {
            prefix: prefix.clone(),
            entries: vec![entry.clone()],
        };

        let bytes = rmp_serde::to_vec(&seg).map_err(|e| {
            AppError::InternalServerError(format!("serialize prefix segment: {}", e))
        })?;
        let file = format!("seg_{}{}", segment_id, PREFIX_SEGMENT_EXT);

        let bucket_dirs = if trashed {
            path_manager.get_all_trash_prefix_bucket_dirs(fragment, prefix)
        } else {
            path_manager.get_all_prefix_bucket_dirs(fragment, prefix)
        };

        let mut wrote_this_prefix = false;
        for bucket in bucket_dirs {
            let path = bucket.join(&file);
            if write_atomic(&path, &bytes).await.is_ok() {
                wrote_this_prefix = true;
            }
        }

        if wrote_this_prefix {
            any_ok = true;
        }
    }

    if !any_ok {
        return Err(AppError::InternalServerError(
            "failed to write prefix update on index roots".into(),
        ));
    }

    if trashed {
        write_trash_delimiter_delta(
            &path_manager,
            fragment,
            object_key,
            segment_id,
            Some(entry),
            child_count_delta,
        )
        .await?;
        schedule_trash_prefix_compaction(fragment.to_string(), prefixes.into_iter().collect());
    } else {
        write_delimiter_delta(
            &path_manager,
            fragment,
            object_key,
            segment_id,
            Some(entry),
            child_count_delta,
        )
        .await?;
        schedule_prefix_compaction(fragment.to_string(), prefixes.into_iter().collect());
    }

    Ok(())
}
