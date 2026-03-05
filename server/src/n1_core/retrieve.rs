use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{
    BROWSER_CACHE_MAX_AGE, PREFIX_DELIMITER_COMPACTED_FILE, PREFIX_SEGMENT_EXT,
};
use crate::n1_core::disk_manager::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, PrefixDelimiterCompactedSegment,
    PrefixIndexEntry, PrefixIndexSegment, Quota, compare_version_recency, version_recency_is_newer,
};
use crate::n1_core::fragment_manager::is_fragment_hash;
use crate::n1_core::index;
use crate::n1_core::metadata_parser::get_metadata;
use crate::n1_core::object_parser::{get_object_logic, stream_object_logic};
use crate::n1_core::quota_manager::get_quota;
use crate::structs::auth_structs::Claims;
use axum::{
    Json as AxumJson,
    body::Body,
    extract::{Path, Query},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
};
use base64::Engine;
use base64::engine::general_purpose;
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};
use tokio::fs;
use tracing::warn;
use walkdir::WalkDir;

#[cfg(test)]
#[derive(Default)]
struct ContinuationCursor {
    root_hint: Option<usize>,
    shard1: String,
    shard2: String,
    shard3: String,
    shard4: String,
    file: String,
}

#[cfg(test)]
fn parse_continuation_token(token: &str) -> Option<ContinuationCursor> {
    let bytes = general_purpose::STANDARD.decode(token).ok()?;
    let s = String::from_utf8(bytes).ok()?;

    let parts_v2: Vec<&str> = s.split('|').collect();
    if parts_v2.len() == 7 && parts_v2[0] == "v2" {
        return Some(ContinuationCursor {
            root_hint: parts_v2[1].parse::<usize>().ok(),
            shard1: parts_v2[2].to_string(),
            shard2: parts_v2[3].to_string(),
            shard3: parts_v2[4].to_string(),
            shard4: parts_v2[5].to_string(),
            file: parts_v2[6].to_string(),
        });
    }

    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 5 {
        Some(ContinuationCursor {
            root_hint: None,
            shard1: parts[0].to_string(),
            shard2: parts[1].to_string(),
            shard3: parts[2].to_string(),
            shard4: parts[3].to_string(),
            file: parts[4].to_string(),
        })
    } else {
        None
    }
}

#[cfg(test)]
fn create_continuation_token(
    shard1: &str,
    shard2: &str,
    shard3: &str,
    shard4: &str,
    filename: &str,
    root_hint: Option<usize>,
) -> String {
    if let Some(root_hint) = root_hint {
        return general_purpose::STANDARD.encode(format!(
            "v2|{}|{}|{}|{}|{}|{}",
            root_hint, shard1, shard2, shard3, shard4, filename
        ));
    }
    general_purpose::STANDARD.encode(format!(
        "{}/{}/{}/{}/{}",
        shard1, shard2, shard3, shard4, filename
    ))
}

fn parse_prefix_continuation_token(token: &str) -> Option<(String, String)> {
    let bytes = general_purpose::STANDARD.decode(token).ok()?;
    let s = String::from_utf8(bytes).ok()?;
    let mut it = s.splitn(3, '|');
    let tag = it.next()?;
    if tag != "pfx" {
        return None;
    }
    let object_key = it.next()?.to_string();
    let version_id = it.next()?.to_string();
    Some((object_key, version_id))
}

fn create_prefix_continuation_token(object_key: &str, version_id: &str) -> String {
    general_purpose::STANDARD.encode(format!("pfx|{}|{}", object_key, version_id))
}

fn parse_delimiter_continuation_token(token: &str) -> Option<String> {
    let bytes = general_purpose::STANDARD.decode(token).ok()?;
    let s = String::from_utf8(bytes).ok()?;
    let mut it = s.splitn(2, '|');
    let tag = it.next()?;
    if tag == "pfxd" {
        return Some(it.next()?.to_string());
    }
    parse_prefix_continuation_token(token).map(|(object_key, _)| canonical_key(&object_key))
}

fn create_delimiter_continuation_token(row_key: &str) -> String {
    general_purpose::STANDARD.encode(format!("pfxd|{}", row_key))
}

fn canonical_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn canonical_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

async fn read_metadata_best(
    _path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Option<ObjectMetadata> {
    index::read_object_version(fragment, object_key, version_id)
        .await
        .ok()
}

async fn read_index_entry_best(
    path_manager: &ObjectPathManager,
    fragment: &str,
    entry: &PrefixIndexEntry,
) -> Option<ObjectMetadata> {
    if let Some(metadata) = entry.to_object_metadata(fragment) {
        return Some(metadata);
    }
    read_metadata_best(path_manager, fragment, &entry.object_key, &entry.version_id).await
}

fn merge_prefix_latest(latest: &mut HashMap<String, PrefixIndexEntry>, entry: PrefixIndexEntry) {
    match latest.get(&entry.object_key) {
        None => {
            latest.insert(entry.object_key.clone(), entry);
        }
        Some(current) => {
            if version_recency_is_newer(
                entry.created_at,
                &entry.version_id,
                current.created_at,
                &current.version_id,
            ) {
                latest.insert(entry.object_key.clone(), entry);
            }
        }
    }
}

async fn try_list_by_prefix_index(
    fragment: &str,
    prefix: &str,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<Option<ListObjectsResult>, AppError> {
    let pfx = canonical_prefix(prefix);
    let pfx_with_slash = if pfx.is_empty() {
        String::new()
    } else {
        format!("{}/", pfx)
    };
    let want_delim = delimiter == Some("/");

    let path_manager = ObjectPathManager::new();
    let mut latest: HashMap<String, PrefixIndexEntry> = HashMap::new();
    let mut had_any_index_data = false;

    let bucket_dirs = path_manager.get_all_prefix_bucket_dirs(fragment, &pfx);
    for d in bucket_dirs {
        if !fs::try_exists(&d).await.unwrap_or(false) {
            continue;
        }

        let compacted_path = d.join(format!("compacted{}", PREFIX_SEGMENT_EXT));
        if fs::try_exists(&compacted_path).await.unwrap_or(false) {
            if let Ok(data) = fs::read(&compacted_path).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    had_any_index_data = true;
                    if canonical_prefix(&seg.prefix) == pfx {
                        for mut e in seg.entries {
                            let ck = canonical_key(&e.object_key);
                            if !pfx.is_empty() && ck != pfx && !ck.starts_with(&pfx_with_slash) {
                                continue;
                            }
                            e.object_key = ck;
                            merge_prefix_latest(&mut latest, e);
                        }
                    }
                }
            }
        }
    }

    if !had_any_index_data {
        return Ok(None);
    }

    if latest.is_empty() {
        return Ok(Some(ListObjectsResult {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            continuation_token: None,
        }));
    }

    let mut entries: Vec<PrefixIndexEntry> = latest.into_values().collect();

    entries.sort_by(|a, b| match a.object_key.cmp(&b.object_key) {
        std::cmp::Ordering::Equal => a.version_id.cmp(&b.version_id),
        other => other,
    });

    let start_after = if let Some(tok) = continuation_token.as_deref() {
        let Some((k, v)) = parse_prefix_continuation_token(tok) else {
            return Ok(None);
        };
        Some((canonical_key(&k), v))
    } else {
        None
    };

    let start_idx = if let Some((k, v)) = start_after.as_ref() {
        match entries.binary_search_by(|entry| match entry.object_key.cmp(k) {
            std::cmp::Ordering::Equal => entry.version_id.cmp(v),
            other => other,
        }) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    } else {
        0
    };

    let mut out_objects: Vec<ObjectMetadata> = Vec::new();
    let mut out_prefixes: Vec<String> = Vec::new();
    let mut seen_prefixes: HashSet<String> = HashSet::new();

    let mut last_pair: Option<(String, String)> = None;
    let mut reached_limit = false;

    for entry in entries.into_iter().skip(start_idx) {
        if out_objects.len() + out_prefixes.len() >= limit {
            break;
        }

        if entry.is_delete_marker {
            continue;
        }

        if !include_internal && is_internal_object_key(entry.object_key.as_str()) {
            continue;
        }

        if !pfx.is_empty()
            && entry.object_key != pfx
            && !entry.object_key.starts_with(&pfx_with_slash)
        {
            continue;
        }

        if want_delim {
            let rel = if pfx.is_empty() {
                entry.object_key.clone()
            } else if entry.object_key == pfx {
                entry.object_key.clone()
            } else if entry.object_key.len() > pfx.len() + 1 {
                entry.object_key[(pfx.len() + 1)..].to_string()
            } else {
                String::new()
            };

            if !pfx.is_empty() && entry.object_key == pfx {
                let Some(meta) = read_index_entry_best(&path_manager, fragment, &entry).await
                else {
                    warn!(
                        "Skipping unreadable prefix-index metadata fragment={} key={} version={}",
                        fragment, entry.object_key, entry.version_id
                    );
                    continue;
                };
                if meta.is_delete_marker {
                    continue;
                }
                out_objects.push(meta);
                last_pair = Some((entry.object_key.clone(), entry.version_id.clone()));
            } else if let Some((first, _rest)) = rel.split_once('/') {
                if !first.is_empty() {
                    let folder = if pfx.is_empty() {
                        format!("{}/", first)
                    } else {
                        format!("{}/{}/", pfx, first)
                    };
                    if seen_prefixes.insert(folder.clone()) {
                        out_prefixes.push(folder);
                        last_pair = Some((entry.object_key.clone(), entry.version_id.clone()));
                    }
                }
            } else {
                let Some(meta) = read_index_entry_best(&path_manager, fragment, &entry).await
                else {
                    warn!(
                        "Skipping unreadable prefix-index metadata fragment={} key={} version={}",
                        fragment, entry.object_key, entry.version_id
                    );
                    continue;
                };
                if meta.is_delete_marker {
                    continue;
                }
                out_objects.push(meta);
                last_pair = Some((entry.object_key.clone(), entry.version_id.clone()));
            }
        } else {
            let Some(meta) = read_index_entry_best(&path_manager, fragment, &entry).await else {
                warn!(
                    "Skipping unreadable prefix-index metadata fragment={} key={} version={}",
                    fragment, entry.object_key, entry.version_id
                );
                continue;
            };
            if meta.is_delete_marker {
                continue;
            }
            out_objects.push(meta);
            last_pair = Some((entry.object_key.clone(), entry.version_id.clone()));
        }

        if out_objects.len() + out_prefixes.len() >= limit {
            reached_limit = true;
            break;
        }
    }

    let next_token = if reached_limit {
        last_pair
            .as_ref()
            .map(|(k, v)| create_prefix_continuation_token(k, v))
    } else {
        None
    };

    Ok(Some(ListObjectsResult {
        objects: out_objects,
        common_prefixes: out_prefixes,
        continuation_token: next_token,
    }))
}

async fn try_list_by_delimiter_index(
    fragment: &str,
    prefix: &str,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<Option<ListObjectsResult>, AppError> {
    #[derive(Clone)]
    struct DelimiterRow {
        row_key: String,
        object_entry: Option<PrefixIndexEntry>,
        common_prefix: Option<String>,
    }

    let pfx = canonical_prefix(prefix);
    let path_manager = ObjectPathManager::new();

    let mut compacted_candidates: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();

    for d in path_manager.get_all_prefix_bucket_dirs(fragment, &pfx) {
        if !fs::try_exists(&d).await.unwrap_or(false) {
            continue;
        }

        let compacted_path = d.join(PREFIX_DELIMITER_COMPACTED_FILE);
        if fs::try_exists(&compacted_path).await.unwrap_or(false) {
            let modified = fs::metadata(&compacted_path)
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::UNIX_EPOCH);
            compacted_candidates.push((modified, compacted_path));
        }
    }

    if compacted_candidates.is_empty() {
        return Ok(None);
    }

    compacted_candidates.sort_by(|a, b| b.0.cmp(&a.0));

    let mut compacted_mtime = None;
    let mut direct_latest: HashMap<String, PrefixIndexEntry> = HashMap::new();
    let mut child_counts: HashMap<String, i64> = HashMap::new();

    for (mtime, path) in compacted_candidates {
        let Ok(data) = fs::read(&path).await else {
            continue;
        };
        let Ok(seg) = rmp_serde::from_slice::<PrefixDelimiterCompactedSegment>(&data) else {
            continue;
        };
        if canonical_prefix(&seg.prefix) != pfx {
            continue;
        }

        for entry in seg.object_entries {
            let mut entry = entry;
            entry.object_key = canonical_key(&entry.object_key);
            merge_prefix_latest(&mut direct_latest, entry);
        }
        for child in seg.child_entries {
            let common_prefix = child.common_prefix.trim_start_matches('/').to_string();
            let live_count = i64::try_from(child.live_count).unwrap_or(i64::MAX);
            child_counts
                .entry(common_prefix)
                .and_modify(|count| *count = count.saturating_add(live_count))
                .or_insert(live_count);
        }

        compacted_mtime = Some(mtime);
        break;
    }

    let Some(_compacted_mtime) = compacted_mtime else {
        return Ok(None);
    };

    let mut rows: Vec<DelimiterRow> = Vec::new();
    for entry in direct_latest.into_values() {
        if entry.is_delete_marker {
            continue;
        }
        if !include_internal && is_internal_object_key(&entry.object_key) {
            continue;
        }
        rows.push(DelimiterRow {
            row_key: entry.object_key.clone(),
            object_entry: Some(entry),
            common_prefix: None,
        });
    }

    for (common_prefix, live_count) in child_counts {
        if live_count <= 0 {
            continue;
        }
        if !include_internal && is_internal_object_key(common_prefix.as_str()) {
            continue;
        }
        rows.push(DelimiterRow {
            row_key: common_prefix.clone(),
            object_entry: None,
            common_prefix: Some(common_prefix),
        });
    }

    rows.sort_by(|a, b| match a.row_key.cmp(&b.row_key) {
        std::cmp::Ordering::Equal => a.object_entry.is_none().cmp(&b.object_entry.is_none()),
        other => other,
    });

    let start_after = if let Some(tok) = continuation_token.as_deref() {
        let Some(row_key) = parse_delimiter_continuation_token(tok) else {
            return Ok(None);
        };
        Some(row_key)
    } else {
        None
    };

    let start_idx = if let Some(start_after) = start_after {
        match rows.binary_search_by(|row| row.row_key.cmp(&start_after)) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    } else {
        0
    };

    let mut out_objects: Vec<ObjectMetadata> = Vec::new();
    let mut out_prefixes: Vec<String> = Vec::new();
    let mut last_row_key: Option<String> = None;
    let mut reached_limit = false;

    for row in rows.into_iter().skip(start_idx) {
        if out_objects.len() + out_prefixes.len() >= limit {
            break;
        }

        if let Some(common_prefix) = row.common_prefix {
            out_prefixes.push(common_prefix);
            last_row_key = Some(row.row_key);
        } else if let Some(entry) = row.object_entry {
            let Some(meta) = read_index_entry_best(&path_manager, fragment, &entry).await else {
                warn!(
                    "Skipping unreadable delimiter-index metadata fragment={} key={} version={}",
                    fragment, entry.object_key, entry.version_id
                );
                continue;
            };
            if meta.is_delete_marker {
                continue;
            }
            out_objects.push(meta);
            last_row_key = Some(row.row_key);
        }

        if out_objects.len() + out_prefixes.len() >= limit {
            reached_limit = true;
            break;
        }
    }

    let continuation_token = if reached_limit {
        last_row_key
            .as_deref()
            .map(create_delimiter_continuation_token)
    } else {
        None
    };

    Ok(Some(ListObjectsResult {
        objects: out_objects,
        common_prefixes: out_prefixes,
        continuation_token,
    }))
}

async fn list_objects_from_index(
    fragment: &str,
    prefix: &str,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
    scan_trash: bool,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    if limit == 0 {
        return Ok(ListObjectsResult {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            continuation_token: None,
        });
    }

    #[derive(Clone)]
    struct DelimiterRow {
        row_key: String,
        object: Option<ObjectMetadata>,
        common_prefix: Option<String>,
    }

    let pfx = canonical_prefix(prefix);
    let pfx_with_slash = if pfx.is_empty() {
        String::new()
    } else {
        format!("{}/", pfx)
    };
    let want_delim = delimiter == Some("/");

    if scan_trash && want_delim {
        let prefix_filter = if pfx.is_empty() {
            None
        } else {
            Some(pfx.as_str())
        };
        let start_after_row = if let Some(tok) = continuation_token.as_deref() {
            let Some(row_key) = parse_delimiter_continuation_token(tok) else {
                return Err(AppError::BadRequest(
                    "invalid continuation_token for delimiter listing".into(),
                ));
            };
            Some(row_key)
        } else {
            None
        };
        let mut scan_cursor = start_after_row
            .as_ref()
            .map(|row_key| (row_key.clone(), String::from("\u{10ffff}")));
        let mut out_objects = Vec::with_capacity(limit);
        let mut out_prefixes = Vec::with_capacity(limit);
        let mut last_output_row: Option<String> = None;
        let mut last_emitted_row = start_after_row.clone();
        let mut reached_limit = false;

        while out_objects.len() + out_prefixes.len() < limit {
            let page_limit = limit
                .saturating_sub(out_objects.len() + out_prefixes.len())
                .max(1)
                .saturating_mul(4)
                .min(1024);

            let (page, page_next) = index::list_fragment_latest_objects_page(
                fragment,
                true,
                prefix_filter,
                scan_cursor
                    .as_ref()
                    .map(|(object_key, version_id)| (object_key.as_str(), version_id.as_str())),
                page_limit,
            )
            .await?;

            if page.is_empty() {
                break;
            }

            for meta in page {
                let ck = canonical_key(&meta.object_key);
                if !include_internal && is_internal_object_key(&ck) {
                    continue;
                }
                if !pfx.is_empty() && ck != pfx && !ck.starts_with(&pfx_with_slash) {
                    continue;
                }

                let rel = if pfx.is_empty() {
                    ck.clone()
                } else if ck == pfx {
                    ck.clone()
                } else if ck.len() > pfx.len() + 1 {
                    ck[(pfx.len() + 1)..].to_string()
                } else {
                    String::new()
                };

                let (row_key, is_common_prefix) = if !pfx.is_empty() && ck == pfx {
                    (ck.clone(), false)
                } else if let Some((first, _rest)) = rel.split_once('/') {
                    if !first.is_empty() {
                        let folder = if pfx.is_empty() {
                            format!("{}/", first)
                        } else {
                            format!("{}/{}/", pfx, first)
                        };
                        (folder, true)
                    } else {
                        (ck.clone(), false)
                    }
                } else {
                    (ck.clone(), false)
                };

                if start_after_row
                    .as_ref()
                    .map(|start| row_key <= *start)
                    .unwrap_or(false)
                {
                    continue;
                }

                if last_emitted_row
                    .as_ref()
                    .map(|last| last == &row_key)
                    .unwrap_or(false)
                {
                    continue;
                }

                if is_common_prefix {
                    out_prefixes.push(row_key.clone());
                } else {
                    out_objects.push(meta);
                }

                last_emitted_row = Some(row_key.clone());
                last_output_row = Some(row_key);

                if out_objects.len() + out_prefixes.len() >= limit {
                    reached_limit = true;
                    break;
                }
            }

            if reached_limit {
                break;
            }

            match page_next {
                Some(next) => scan_cursor = Some(next),
                None => break,
            }
        }

        return Ok(ListObjectsResult {
            objects: out_objects,
            common_prefixes: out_prefixes,
            continuation_token: if reached_limit {
                last_output_row
                    .as_deref()
                    .map(create_delimiter_continuation_token)
            } else {
                None
            },
        });
    }

    if scan_trash && !want_delim {
        let prefix_filter = if pfx.is_empty() {
            None
        } else {
            Some(pfx.as_str())
        };
        let mut scan_cursor = continuation_token
            .as_deref()
            .and_then(parse_prefix_continuation_token);
        let mut out_objects = Vec::with_capacity(limit);
        let mut next_scan_cursor: Option<(String, String)> = None;

        while out_objects.len() < limit {
            let page_limit = limit.saturating_sub(out_objects.len()).max(1);
            let (page, page_next) = index::list_fragment_latest_objects_page(
                fragment,
                true,
                prefix_filter,
                scan_cursor
                    .as_ref()
                    .map(|(object_key, version_id)| (object_key.as_str(), version_id.as_str())),
                page_limit,
            )
            .await?;

            if page.is_empty() {
                next_scan_cursor = None;
                break;
            }

            for meta in page {
                let ck = canonical_key(&meta.object_key);
                if !include_internal && is_internal_object_key(&ck) {
                    continue;
                }
                out_objects.push(meta);
                if out_objects.len() >= limit {
                    break;
                }
            }

            match page_next {
                Some(next) => {
                    next_scan_cursor = Some(next.clone());
                    if out_objects.len() >= limit {
                        break;
                    }
                    scan_cursor = Some(next);
                }
                None => {
                    next_scan_cursor = None;
                    break;
                }
            }
        }

        return Ok(ListObjectsResult {
            objects: out_objects,
            common_prefixes: Vec::new(),
            continuation_token: if next_scan_cursor.is_some() {
                next_scan_cursor.as_ref().map(|(object_key, version_id)| {
                    create_prefix_continuation_token(object_key, version_id)
                })
            } else {
                None
            },
        });
    }

    let objects = if scan_trash {
        if pfx.is_empty() {
            index::list_fragment_trash_objects(fragment).await?
        } else {
            index::list_fragment_trash_objects_for_prefix(fragment, &pfx).await?
        }
    } else {
        if pfx.is_empty() {
            index::list_fragment_objects(fragment).await?
        } else {
            index::list_fragment_objects_for_prefix(fragment, &pfx).await?
        }
    };

    if want_delim {
        let mut rows = Vec::<DelimiterRow>::new();
        let mut seen_prefixes = HashSet::<String>::new();

        for meta in objects {
            let ck = canonical_key(&meta.object_key);
            if !include_internal && is_internal_object_key(&ck) {
                continue;
            }
            if !scan_trash && meta.is_delete_marker {
                continue;
            }
            if !pfx.is_empty() && ck != pfx && !ck.starts_with(&pfx_with_slash) {
                continue;
            }

            let rel = if pfx.is_empty() {
                ck.clone()
            } else if ck == pfx {
                ck.clone()
            } else if ck.len() > pfx.len() + 1 {
                ck[(pfx.len() + 1)..].to_string()
            } else {
                String::new()
            };

            if !pfx.is_empty() && ck == pfx {
                rows.push(DelimiterRow {
                    row_key: ck,
                    object: Some(meta),
                    common_prefix: None,
                });
            } else if let Some((first, _rest)) = rel.split_once('/') {
                if !first.is_empty() {
                    let folder = if pfx.is_empty() {
                        format!("{}/", first)
                    } else {
                        format!("{}/{}/", pfx, first)
                    };
                    if seen_prefixes.insert(folder.clone()) {
                        rows.push(DelimiterRow {
                            row_key: folder.clone(),
                            object: None,
                            common_prefix: Some(folder),
                        });
                    }
                }
            } else {
                rows.push(DelimiterRow {
                    row_key: ck,
                    object: Some(meta),
                    common_prefix: None,
                });
            }
        }

        rows.sort_by(|left, right| {
            left.row_key
                .cmp(&right.row_key)
                .then(left.object.is_none().cmp(&right.object.is_none()))
        });

        let start_after = continuation_token
            .as_deref()
            .and_then(parse_delimiter_continuation_token);
        let start_idx = if let Some(row_key) = start_after {
            match rows.binary_search_by(|row| row.row_key.cmp(&row_key)) {
                Ok(i) => i + 1,
                Err(i) => i,
            }
        } else {
            0
        };

        let mut out_objects = Vec::new();
        let mut out_prefixes = Vec::new();
        let mut last_row_key = None;
        let mut reached_limit = false;

        for row in rows.into_iter().skip(start_idx) {
            if out_objects.len() + out_prefixes.len() >= limit {
                break;
            }

            if let Some(common_prefix) = row.common_prefix {
                out_prefixes.push(common_prefix);
                last_row_key = Some(row.row_key);
            } else if let Some(meta) = row.object {
                out_objects.push(meta);
                last_row_key = Some(row.row_key);
            }

            if out_objects.len() + out_prefixes.len() >= limit {
                reached_limit = true;
                break;
            }
        }

        return Ok(ListObjectsResult {
            objects: out_objects,
            common_prefixes: out_prefixes,
            continuation_token: if reached_limit {
                last_row_key
                    .as_deref()
                    .map(create_delimiter_continuation_token)
            } else {
                None
            },
        });
    }

    let start_after = continuation_token
        .as_deref()
        .and_then(parse_prefix_continuation_token);
    let mut out_objects = Vec::new();
    let mut last_pair = None;
    let mut reached_limit = false;

    for meta in objects {
        if out_objects.len() >= limit {
            break;
        }

        let ck = canonical_key(&meta.object_key);
        if !include_internal && is_internal_object_key(&ck) {
            continue;
        }
        if !scan_trash && meta.is_delete_marker {
            continue;
        }
        if !pfx.is_empty() && ck != pfx && !ck.starts_with(&pfx_with_slash) {
            continue;
        }

        if let Some((start_key, start_version)) = start_after.as_ref() {
            if ck < *start_key || (ck == *start_key && meta.version_id <= *start_version) {
                continue;
            }
        }

        last_pair = Some((ck, meta.version_id.clone()));
        out_objects.push(meta);

        if out_objects.len() >= limit {
            reached_limit = true;
            break;
        }
    }

    Ok(ListObjectsResult {
        objects: out_objects,
        common_prefixes: Vec::new(),
        continuation_token: if reached_limit {
            last_pair
                .as_ref()
                .map(|(key, version)| create_prefix_continuation_token(key, version))
        } else {
            None
        },
    })
}

pub async fn list_objects(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    list_objects_internal(fragment, params, false).await
}

pub async fn list_trash_objects(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    list_objects_internal(fragment, params, true).await
}

async fn list_objects_internal(
    fragment: String,
    params: HashMap<String, String>,
    scan_trash: bool,
) -> Result<impl IntoResponse, AppError> {
    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
        .min(1000);

    let continuation_token = params.get("continuation_token").cloned();
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let include_internal = params
        .get("include_internal")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let delimiter = params
        .get("delimiter")
        .map(|s| s.as_str())
        .filter(|d| *d == "/");
    let prefix_projection_pending = if scan_trash {
        false
    } else {
        index::is_prefix_projection_pending(&fragment).await
    };

    if !scan_trash && !prefix_projection_pending && delimiter.is_some() {
        if let Some(res) = try_list_by_delimiter_index(
            &fragment,
            &prefix,
            limit,
            continuation_token.clone(),
            include_internal,
        )
        .await?
        {
            return Ok(AxumJson(res));
        }
    }

    if !scan_trash && !prefix_projection_pending && (delimiter.is_some() || !prefix.is_empty()) {
        if let Some(res) = try_list_by_prefix_index(
            &fragment,
            &prefix,
            delimiter,
            limit,
            continuation_token.clone(),
            include_internal,
        )
        .await?
        {
            return Ok(AxumJson(res));
        }
    }

    Ok(AxumJson(
        list_objects_from_index(
            &fragment,
            &prefix,
            delimiter,
            limit,
            continuation_token,
            scan_trash,
            include_internal,
        )
        .await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        create_continuation_token, create_delimiter_continuation_token, parse_continuation_token,
        parse_delimiter_continuation_token,
    };

    #[test]
    fn continuation_token_v1_round_trip() {
        let token = create_continuation_token("aa", "bb", "cc", "dd", "file.msgpack", None);
        let parsed = parse_continuation_token(&token).expect("token should parse");
        assert_eq!(parsed.root_hint, None);
        assert_eq!(parsed.shard1, "aa");
        assert_eq!(parsed.shard2, "bb");
        assert_eq!(parsed.shard3, "cc");
        assert_eq!(parsed.shard4, "dd");
        assert_eq!(parsed.file, "file.msgpack");
    }

    #[test]
    fn continuation_token_v2_round_trip() {
        let token = create_continuation_token("0f", "1a", "2b", "3c", "meta.msgpack", Some(7));
        let parsed = parse_continuation_token(&token).expect("token should parse");
        assert_eq!(parsed.root_hint, Some(7));
        assert_eq!(parsed.shard1, "0f");
        assert_eq!(parsed.shard2, "1a");
        assert_eq!(parsed.shard3, "2b");
        assert_eq!(parsed.shard4, "3c");
        assert_eq!(parsed.file, "meta.msgpack");
    }

    #[test]
    fn delimiter_continuation_token_round_trip() {
        let token = create_delimiter_continuation_token("ward-a/");
        let parsed =
            parse_delimiter_continuation_token(&token).expect("delimiter token should parse");
        assert_eq!(parsed, "ward-a/");
    }
}

fn is_internal_object_key(object_key: &str) -> bool {
    let key = canonical_key(object_key);
    key.split('/')
        .any(|segment| segment == ".n1fsd_dir" || segment == ".n1-trash")
}

pub async fn list_object_versions(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let mut versions = index::list_live_object_versions_metadata(&fragment, &object_key).await?;
    versions.sort_by(|a, b| {
        compare_version_recency(b.created_at, &b.version_id, a.created_at, &a.version_id)
    });
    Ok(AxumJson(versions))
}

#[derive(Serialize)]
pub struct FragmentUsage {
    pub fragment: String,
    pub total: u64,
    pub used: u64,
}

pub async fn list_fragments(claims: Claims) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    let fragments = tokio::task::spawn_blocking(move || -> Result<Vec<String>, std::io::Error> {
        let mut fragment_hashes = HashSet::new();

        for root in index_roots {
            if !root.exists() {
                continue;
            }

            for entry in WalkDir::new(&root)
                .min_depth(1)
                .max_depth(1)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        if is_fragment_hash(name) {
                            fragment_hashes.insert(name.to_string());
                        }
                    }
                }
            }
        }

        let mut sorted_fragments: Vec<String> = fragment_hashes.into_iter().collect();
        sorted_fragments.sort();
        Ok(sorted_fragments)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))?
    .map_err(|e| AppError::Io(e, "read storage root".into()))?;

    let mut usage_list = Vec::with_capacity(fragments.len());

    for fragment_hash in fragments {
        let quota = match get_quota(&fragment_hash).await {
            Ok(q) => q,
            Err(_) => Quota {
                allocated_storage: 0,
                storage_used: 0,
            },
        };

        usage_list.push(FragmentUsage {
            fragment: fragment_hash,
            total: quota.allocated_storage,
            used: quota.storage_used,
        });
    }

    Ok(AxumJson(usage_list))
}

pub async fn get_object_metadata(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let metadata = get_metadata(&fragment, &object_key, &claims.fragment).await?;

    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    Ok(AxumJson(metadata))
}

pub async fn get_object(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    req: axum::http::Request<Body>,
) -> Result<Response, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let metadata = get_metadata(&fragment, &object_key, &fragment).await?;

    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    let mut response = get_object_logic(metadata, req).await?.into_response();

    let headers = response.headers_mut();

    let cache_control_val = format!("public, max-age={}", BROWSER_CACHE_MAX_AGE);
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_str(&cache_control_val).unwrap(),
    );

    Ok(response)
}

pub async fn stream_object(
    claims: Claims,
    headers: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let metadata = get_metadata(&fragment, &object_key, &fragment).await?;

    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    stream_object_logic(headers, metadata, "inline").await
}
