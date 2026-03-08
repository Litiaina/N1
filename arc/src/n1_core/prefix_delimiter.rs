use crate::n1_core::disk_manager::{
    PrefixDelimiterChildEntry, PrefixDelimiterCompactedSegment, PrefixIndexEntry,
    version_recency_is_newer,
};
use std::collections::HashMap;

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
    latest: &HashMap<String, PrefixIndexEntry>,
) -> PrefixDelimiterCompactedSegment {
    let pfx = canonical_prefix(prefix);
    let mut object_entries = Vec::new();
    let mut child_counts: HashMap<String, u64> = HashMap::new();

    for (object_key, entry) in latest {
        if entry.is_delete_marker {
            continue;
        }

        if is_direct_object_for_prefix(&pfx, object_key) {
            object_entries.push(entry.clone());
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
    let mut latest: HashMap<String, PrefixIndexEntry> = HashMap::new();
    for entry in entries {
        match latest.get(&entry.object_key) {
            None => {
                latest.insert(entry.object_key.clone(), entry.clone());
            }
            Some(current) => {
                if version_recency_is_newer(
                    entry.created_at,
                    &entry.version_id,
                    current.created_at,
                    &current.version_id,
                ) {
                    latest.insert(entry.object_key.clone(), entry.clone());
                }
            }
        }
    }
    build_delimiter_compacted_from_latest(prefix, &latest)
}
