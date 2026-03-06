use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::{
    ObjectPathManager, PrefixDelimiterChildDelta, PrefixDelimiterChildEntry,
    PrefixDelimiterCompactedSegment, PrefixDelimiterSegment, PrefixIndexEntry,
};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

fn canonical_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

fn canonical_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

pub fn parent_prefix_for_key(object_key: &str) -> String {
    let key = canonical_key(object_key);
    key.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

pub fn ancestor_common_prefix_rows_for_key(object_key: &str) -> Vec<(String, String)> {
    let key = canonical_key(object_key);
    let parts: Vec<&str> = key.split('/').filter(|p| !p.is_empty()).collect();
    if parts.len() < 2 {
        return Vec::new();
    }

    let mut rows = Vec::with_capacity(parts.len().saturating_sub(1));
    let mut current = String::new();

    for (idx, part) in parts.iter().enumerate().take(parts.len() - 1) {
        if idx == 0 {
            current.push_str(part);
            rows.push((String::new(), format!("{}/", current)));
        } else {
            let bucket_prefix = current.clone();
            current.push('/');
            current.push_str(part);
            rows.push((bucket_prefix, format!("{}/", current)));
        }
    }

    rows
}

pub fn is_direct_object_for_prefix(prefix: &str, object_key: &str) -> bool {
    let pfx = canonical_prefix(prefix);
    let key = canonical_key(object_key);
    key == pfx || parent_prefix_for_key(&key) == pfx
}

pub fn immediate_child_common_prefix(prefix: &str, object_key: &str) -> Option<String> {
    let pfx = canonical_prefix(prefix);
    let key = canonical_key(object_key);

    if key == pfx {
        return None;
    }

    let rel = if pfx.is_empty() {
        key
    } else if key.starts_with(&(pfx.clone() + "/")) {
        key[(pfx.len() + 1)..].to_string()
    } else {
        return None;
    };

    let (first, _) = rel.split_once('/')?;
    if first.is_empty() {
        return None;
    }

    Some(if pfx.is_empty() {
        format!("{}/", first)
    } else {
        format!("{}/{}/", pfx, first)
    })
}

pub fn build_delimiter_compacted_from_latest(
    prefix: &str,
    latest: &HashMap<String, (String, i64, bool)>,
) -> PrefixDelimiterCompactedSegment {
    let pfx = canonical_prefix(prefix);
    let mut object_entries = Vec::new();
    let mut child_counts: HashMap<String, u64> = HashMap::new();

    for (object_key, (version_id, created_at, is_delete_marker)) in latest {
        if *is_delete_marker {
            continue;
        }

        if is_direct_object_for_prefix(&pfx, object_key) {
            object_entries.push(PrefixIndexEntry {
                object_key: object_key.clone(),
                version_id: version_id.clone(),
                created_at: *created_at,
                is_delete_marker: false,
            });
            continue;
        }

        if let Some(common_prefix) = immediate_child_common_prefix(&pfx, object_key) {
            *child_counts.entry(common_prefix).or_insert(0) += 1;
        }
    }

    object_entries.sort_by(|a, b| match a.object_key.cmp(&b.object_key) {
        std::cmp::Ordering::Equal => a.version_id.cmp(&b.version_id),
        other => other,
    });

    let mut child_entries: Vec<PrefixDelimiterChildEntry> = child_counts
        .into_iter()
        .map(|(common_prefix, live_count)| PrefixDelimiterChildEntry {
            common_prefix,
            live_count,
        })
        .collect();
    child_entries.sort_by(|a, b| a.common_prefix.cmp(&b.common_prefix));

    PrefixDelimiterCompactedSegment {
        prefix: pfx,
        object_entries,
        child_entries,
    }
}

pub fn build_delimiter_compacted_from_entries(
    prefix: &str,
    entries: &[PrefixIndexEntry],
) -> PrefixDelimiterCompactedSegment {
    let latest: HashMap<String, (String, i64, bool)> = entries
        .iter()
        .map(|entry| {
            (
                entry.object_key.clone(),
                (
                    entry.version_id.clone(),
                    entry.created_at,
                    entry.is_delete_marker,
                ),
            )
        })
        .collect();
    build_delimiter_compacted_from_latest(prefix, &latest)
}

async fn write_atomic(path: &Path, data: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent directory for {}", path.display()))
    })?;

    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, "create delimiter parent".into()))?;

    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        ulid::Ulid::new()
    ));

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("open tmp {}", tmp.display())))?;

    tokio::io::AsyncWriteExt::write_all(&mut file, data)
        .await
        .map_err(|e| AppError::Io(e, "write delimiter tmp".into()))?;
    file.sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync delimiter tmp".into()))?;
    drop(file);

    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))
}

pub async fn write_delimiter_delta(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    segment_id: &str,
    object_entry: Option<PrefixIndexEntry>,
    child_count_delta: i64,
) -> Result<(), AppError> {
    let parent_prefix = parent_prefix_for_key(object_key);
    let mut wrote_any = false;
    if let Some(object_entry) = object_entry {
        let parent_segment = PrefixDelimiterSegment {
            prefix: parent_prefix.clone(),
            object_entries: vec![object_entry],
            child_deltas: Vec::new(),
        };
        let parent_bytes = rmp_serde::to_vec(&parent_segment).map_err(|e| {
            AppError::InternalServerError(format!("serialize delimiter object segment: {}", e))
        })?;

        for path in path_manager.get_all_prefix_delimiter_segment_paths(
            fragment,
            &parent_prefix,
            segment_id,
        ) {
            if write_atomic(&path, &parent_bytes).await.is_ok() {
                wrote_any = true;
            }
        }
    }

    if child_count_delta != 0 {
        for (bucket_prefix, common_prefix) in ancestor_common_prefix_rows_for_key(object_key) {
            let segment = PrefixDelimiterSegment {
                prefix: bucket_prefix.clone(),
                object_entries: Vec::new(),
                child_deltas: vec![PrefixDelimiterChildDelta {
                    common_prefix,
                    delta: child_count_delta,
                }],
            };
            let bytes = rmp_serde::to_vec(&segment).map_err(|e| {
                AppError::InternalServerError(format!("serialize delimiter child segment: {}", e))
            })?;

            for path in path_manager.get_all_prefix_delimiter_segment_paths(
                fragment,
                &bucket_prefix,
                segment_id,
            ) {
                if write_atomic(&path, &bytes).await.is_ok() {
                    wrote_any = true;
                }
            }
        }
    }

    if !wrote_any {
        return Err(AppError::InternalServerError(
            "Failed to write delimiter index segment to any mirror".into(),
        ));
    }

    Ok(())
}
