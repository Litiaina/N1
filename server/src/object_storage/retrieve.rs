use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{
    BROWSER_CACHE_MAX_AGE, METADATA_EXT, PREFIX_DELIMITER_COMPACTED_FILE,
    PREFIX_DELIMITER_SEGMENT_PREFIX, PREFIX_SEGMENT_EXT, QUOTA_FILE,
};
use crate::object_storage::disk_manager::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, ObjectSummary,
    PrefixDelimiterCompactedSegment, PrefixDelimiterSegment, PrefixIndexSegment, Quota,
};
use crate::object_storage::fragment_manager::is_fragment_hash;
use crate::object_storage::metadata_parser::get_metadata;
use crate::object_storage::object_parser::{get_object_logic, stream_object_logic};
use crate::object_storage::quota_manager::get_quota;
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
use dashmap::DashMap;
use futures_util::StreamExt;
use once_cell::sync::OnceCell;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::fs;
use tracing::{debug, warn};
use walkdir::WalkDir;

pub static PREFIX_MAX_DELTA_SEGMENTS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
static INDEX_ROOT_CURSOR: OnceCell<DashMap<String, AtomicUsize>> = OnceCell::new();

#[derive(Default)]
struct ContinuationCursor {
    root_hint: Option<usize>,
    shard1: String,
    shard2: String,
    shard3: String,
    shard4: String,
    file: String,
}

fn prefix_max_delta_segments() -> usize {
    PREFIX_MAX_DELTA_SEGMENTS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(512)
        .clamp(32, 65_536)
}

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

fn is_hex2(s: &str) -> bool {
    if s.len() != 2 {
        return false;
    }
    s.as_bytes().iter().all(|b| match b {
        b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F' => true,
        _ => false,
    })
}

fn canonical_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn canonical_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

fn index_root_cursor() -> &'static DashMap<String, AtomicUsize> {
    INDEX_ROOT_CURSOR.get_or_init(DashMap::new)
}

fn next_index_root_cursor(key: &str) -> usize {
    if let Some(entry) = index_root_cursor().get(key) {
        return entry.value().fetch_add(1, Ordering::Relaxed);
    }
    index_root_cursor().insert(key.to_string(), AtomicUsize::new(1));
    0
}

async fn read_metadata_best(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Option<ObjectSummary> {
    let paths = path_manager.get_all_metadata_paths(fragment, object_key, version_id);

    let mut best: Option<ObjectSummary> = None;

    for p in paths {
        if !fs::try_exists(&p).await.unwrap_or(false) {
            continue;
        }

        let Ok(data) = fs::read(&p).await else {
            continue;
        };

        let Ok(meta) = rmp_serde::from_slice::<ObjectSummary>(&data) else {
            continue;
        };

        if meta.fragment != fragment
            || meta.object_key != object_key
            || meta.version_id != version_id
        {
            continue;
        }

        match &best {
            None => best = Some(meta),
            Some(cur) => {
                if meta.created_at > cur.created_at
                    || (meta.created_at == cur.created_at && meta.version_id > cur.version_id)
                {
                    best = Some(meta);
                }
            }
        }
    }

    best
}

fn merge_prefix_latest(
    latest: &mut HashMap<String, (String, i64, bool)>,
    object_key: String,
    version_id: String,
    created_at: i64,
    is_delete_marker: bool,
) {
    match latest.get(&object_key) {
        None => {
            latest.insert(object_key, (version_id, created_at, is_delete_marker));
        }
        Some((cur_vid, cur_ts, _)) => {
            if created_at > *cur_ts || (created_at == *cur_ts && version_id > *cur_vid) {
                latest.insert(object_key, (version_id, created_at, is_delete_marker));
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
    let mut latest: HashMap<String, (String, i64, bool)> = HashMap::new();
    let mut had_any_index_data = false;

    let bucket_dirs = path_manager.get_all_prefix_bucket_dirs(fragment, &pfx);
    for d in bucket_dirs {
        if !fs::try_exists(&d).await.unwrap_or(false) {
            continue;
        }

        let compacted_path = d.join(format!("compacted{}", PREFIX_SEGMENT_EXT));
        let mut compacted_mtime: Option<std::time::SystemTime> = None;

        if fs::try_exists(&compacted_path).await.unwrap_or(false) {
            if let Ok(data) = fs::read(&compacted_path).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    had_any_index_data = true;
                    if let Ok(md) = fs::metadata(&compacted_path).await {
                        compacted_mtime = md.modified().ok();
                    }
                    if canonical_prefix(&seg.prefix) == pfx {
                        for e in seg.entries {
                            let ck = canonical_key(&e.object_key);
                            if !pfx.is_empty() && ck != pfx && !ck.starts_with(&pfx_with_slash) {
                                continue;
                            }
                            merge_prefix_latest(
                                &mut latest,
                                ck,
                                e.version_id,
                                e.created_at,
                                e.is_delete_marker,
                            );
                        }
                    }
                }
            }
        }

        let mut seg_files: Vec<(String, std::time::SystemTime, PathBuf)> = Vec::new();
        if let Ok(mut entries) = fs::read_dir(&d).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if !entry
                    .file_type()
                    .await
                    .map(|t| t.is_file())
                    .unwrap_or(false)
                {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("seg_") && name.ends_with(PREFIX_SEGMENT_EXT) {
                    let modified = entry
                        .metadata()
                        .await
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .unwrap_or(std::time::UNIX_EPOCH);
                    seg_files.push((name, modified, entry.path()));
                }
            }
        }
        seg_files.sort_by(|a, b| a.0.cmp(&b.0));

        let recent_delta_count = if let Some(cmp_ts) = compacted_mtime {
            seg_files.iter().filter(|(_, ts, _)| *ts > cmp_ts).count()
        } else {
            seg_files.len()
        };

        let max_deltas = prefix_max_delta_segments();
        if recent_delta_count > max_deltas {
            debug!(
                "Prefix delta fan-in too large for fragment={} prefix='{}' bucket={:?} recent_deltas={} (max={}); fallback to full scan",
                fragment, pfx, d, recent_delta_count, max_deltas
            );
            return Ok(None);
        }

        for (_name, _mtime, path) in seg_files {
            if let Ok(data) = fs::read(&path).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    had_any_index_data = true;
                    if canonical_prefix(&seg.prefix) != pfx {
                        continue;
                    }
                    for e in seg.entries {
                        let ck = canonical_key(&e.object_key);

                        if !pfx.is_empty() && ck != pfx && !ck.starts_with(&pfx_with_slash) {
                            continue;
                        }

                        merge_prefix_latest(
                            &mut latest,
                            ck,
                            e.version_id,
                            e.created_at,
                            e.is_delete_marker,
                        );
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

    let mut entries: Vec<(String, String, bool)> = latest
        .into_iter()
        .map(|(k, (v, _, is_delete_marker))| (k, v, is_delete_marker))
        .collect();

    entries.sort_by(|a, b| match a.0.cmp(&b.0) {
        std::cmp::Ordering::Equal => a.1.cmp(&b.1),
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
        match entries.binary_search_by(|(ek, ev, _)| match ek.cmp(k) {
            std::cmp::Ordering::Equal => ev.cmp(v),
            other => other,
        }) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    } else {
        0
    };

    let mut out_objects: Vec<ObjectSummary> = Vec::new();
    let mut out_prefixes: Vec<String> = Vec::new();
    let mut seen_prefixes: HashSet<String> = HashSet::new();

    let mut last_pair: Option<(String, String)> = None;
    let mut reached_limit = false;

    for (k, v, is_delete_marker) in entries.into_iter().skip(start_idx) {
        if out_objects.len() + out_prefixes.len() >= limit {
            break;
        }

        if is_delete_marker {
            continue;
        }

        if !include_internal && is_internal_object_key(k.as_str()) {
            continue;
        }

        if !pfx.is_empty() && k != pfx && !k.starts_with(&pfx_with_slash) {
            continue;
        }

        if want_delim {
            let rel = if pfx.is_empty() {
                k.clone()
            } else if k == pfx {
                k.clone()
            } else if k.len() > pfx.len() + 1 {
                k[(pfx.len() + 1)..].to_string()
            } else {
                String::new()
            };

            if !pfx.is_empty() && k == pfx {
                let Some(meta) = read_metadata_best(&path_manager, fragment, &k, &v).await else {
                    warn!(
                        "Skipping unreadable prefix-index metadata fragment={} key={} version={}",
                        fragment, k, v
                    );
                    continue;
                };
                if meta.is_delete_marker {
                    continue;
                }
                out_objects.push(meta);
                last_pair = Some((k, v));
            } else if let Some((first, _rest)) = rel.split_once('/') {
                if !first.is_empty() {
                    let folder = if pfx.is_empty() {
                        format!("{}/", first)
                    } else {
                        format!("{}/{}/", pfx, first)
                    };
                    if seen_prefixes.insert(folder.clone()) {
                        out_prefixes.push(folder);
                        last_pair = Some((k, v));
                    }
                }
            } else {
                let Some(meta) = read_metadata_best(&path_manager, fragment, &k, &v).await else {
                    warn!(
                        "Skipping unreadable prefix-index metadata fragment={} key={} version={}",
                        fragment, k, v
                    );
                    continue;
                };
                if meta.is_delete_marker {
                    continue;
                }
                out_objects.push(meta);
                last_pair = Some((k, v));
            }
        } else {
            let Some(meta) = read_metadata_best(&path_manager, fragment, &k, &v).await else {
                warn!(
                    "Skipping unreadable prefix-index metadata fragment={} key={} version={}",
                    fragment, k, v
                );
                continue;
            };
            if meta.is_delete_marker {
                continue;
            }
            out_objects.push(meta);
            last_pair = Some((k, v));
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
        objects: out_objects.into_iter().map(ObjectMetadata::from).collect(),
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
        object_key: Option<String>,
        version_id: Option<String>,
        common_prefix: Option<String>,
    }

    let pfx = canonical_prefix(prefix);
    let path_manager = ObjectPathManager::new();

    let mut compacted_candidates: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    let mut delta_candidates: HashMap<String, Vec<(std::time::SystemTime, PathBuf)>> =
        HashMap::new();

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

        if let Ok(mut entries) = fs::read_dir(&d).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if !entry
                    .file_type()
                    .await
                    .map(|t| t.is_file())
                    .unwrap_or(false)
                {
                    continue;
                }

                let name = entry.file_name().to_string_lossy().to_string();
                if !name.starts_with(PREFIX_DELIMITER_SEGMENT_PREFIX)
                    || !name.ends_with(PREFIX_SEGMENT_EXT)
                {
                    continue;
                }

                let modified = entry
                    .metadata()
                    .await
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .unwrap_or(std::time::UNIX_EPOCH);
                delta_candidates
                    .entry(name)
                    .or_default()
                    .push((modified, entry.path()));
            }
        }
    }

    if compacted_candidates.is_empty() {
        return Ok(None);
    }

    compacted_candidates.sort_by(|a, b| b.0.cmp(&a.0));

    let mut compacted_mtime = None;
    let mut direct_latest: HashMap<String, (String, i64, bool)> = HashMap::new();
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
            merge_prefix_latest(
                &mut direct_latest,
                canonical_key(&entry.object_key),
                entry.version_id,
                entry.created_at,
                entry.is_delete_marker,
            );
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

    let Some(compacted_mtime) = compacted_mtime else {
        return Ok(None);
    };

    let mut delta_segments: Vec<(String, std::time::SystemTime, PrefixDelimiterSegment)> =
        Vec::new();
    for (name, mut candidates) in delta_candidates {
        candidates.sort_by(|a, b| b.0.cmp(&a.0));

        let mut selected = None;
        for (mtime, path) in candidates {
            let Ok(data) = fs::read(&path).await else {
                continue;
            };
            let Ok(seg) = rmp_serde::from_slice::<PrefixDelimiterSegment>(&data) else {
                continue;
            };
            if canonical_prefix(&seg.prefix) != pfx {
                continue;
            }
            selected = Some((mtime, seg));
            break;
        }

        let Some((mtime, seg)) = selected else {
            debug!(
                "Unreadable delimiter delta segment for fragment={} prefix='{}' name={}",
                fragment, pfx, name
            );
            return Ok(None);
        };

        delta_segments.push((name, mtime, seg));
    }

    let recent_delta_count = delta_segments
        .iter()
        .filter(|(_, mtime, _)| *mtime > compacted_mtime)
        .count();
    let max_deltas = prefix_max_delta_segments();
    if recent_delta_count > max_deltas {
        debug!(
            "Delimiter delta fan-in too large for fragment={} prefix='{}' recent_deltas={} (max={}); fallback to full prefix scan",
            fragment, pfx, recent_delta_count, max_deltas
        );
        return Ok(None);
    }

    delta_segments.sort_by(|a, b| a.0.cmp(&b.0));
    for (_name, mtime, seg) in delta_segments {
        if mtime <= compacted_mtime {
            continue;
        }

        for entry in seg.object_entries {
            merge_prefix_latest(
                &mut direct_latest,
                canonical_key(&entry.object_key),
                entry.version_id,
                entry.created_at,
                entry.is_delete_marker,
            );
        }
        for child_delta in seg.child_deltas {
            let common_prefix = child_delta
                .common_prefix
                .trim_start_matches('/')
                .to_string();
            child_counts
                .entry(common_prefix)
                .and_modify(|count| *count = count.saturating_add(child_delta.delta))
                .or_insert(child_delta.delta);
        }
    }

    let mut rows: Vec<DelimiterRow> = Vec::new();
    for (object_key, (version_id, _created_at, is_delete_marker)) in direct_latest {
        if is_delete_marker {
            continue;
        }
        if !include_internal && is_internal_object_key(&object_key) {
            continue;
        }
        rows.push(DelimiterRow {
            row_key: object_key.clone(),
            object_key: Some(object_key),
            version_id: Some(version_id),
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
            object_key: None,
            version_id: None,
            common_prefix: Some(common_prefix),
        });
    }

    rows.sort_by(|a, b| match a.row_key.cmp(&b.row_key) {
        std::cmp::Ordering::Equal => a.object_key.is_none().cmp(&b.object_key.is_none()),
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

    let mut out_objects: Vec<ObjectSummary> = Vec::new();
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
        } else if let (Some(object_key), Some(version_id)) = (row.object_key, row.version_id) {
            let Some(meta) =
                read_metadata_best(&path_manager, fragment, &object_key, &version_id).await
            else {
                warn!(
                    "Skipping unreadable delimiter-index metadata fragment={} key={} version={}",
                    fragment, object_key, version_id
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
        objects: out_objects.into_iter().map(ObjectMetadata::from).collect(),
        common_prefixes: out_prefixes,
        continuation_token,
    }))
}

async fn choose_index_root(
    fragment: &str,
    scan_trash: bool,
    preferred_root_hint: Option<usize>,
) -> Option<(usize, PathBuf)> {
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    let mut candidates: Vec<(usize, PathBuf, u64)> = Vec::new();

    for (root_idx, root) in index_roots.into_iter().enumerate() {
        let candidate = if scan_trash {
            root.join(fragment).join(".trash").join("index")
        } else {
            root.join(fragment).join("index")
        };

        if !fs::try_exists(&candidate).await.unwrap_or(false) {
            continue;
        }

        let mut count = 0u64;
        let mut access_ok = false;

        if let Ok(mut rd) = fs::read_dir(&candidate).await {
            access_ok = true;
            while let Ok(Some(ent)) = rd.next_entry().await {
                let name = ent.file_name().to_string_lossy().to_string();
                if is_hex2(&name) {
                    count += 1;
                }
            }
        }

        if !access_ok {
            continue;
        }

        candidates.push((root_idx, candidate, count));
    }

    if candidates.is_empty() {
        if scan_trash {
            debug!(
                "No readable trash index root found for fragment {}",
                fragment
            );
        } else {
            warn!("No readable index root found for fragment {}", fragment);
        }
        return None;
    }

    if let Some(preferred) = preferred_root_hint {
        if let Some((idx, path, count)) = candidates
            .iter()
            .find(|(idx, _, _)| *idx == preferred)
            .cloned()
        {
            debug!(
                "Selected continuation index root: {:?} (root_id={}, shard_count={}) for fragment {}",
                path, idx, count, fragment
            );
            return Some((idx, path));
        }
    }

    candidates.sort_by_key(|(idx, _, _)| *idx);
    let rr_key = format!("{}:{}", if scan_trash { "trash" } else { "live" }, fragment);
    let pick = next_index_root_cursor(&rr_key) % candidates.len();
    let (idx, path, count) = candidates[pick].clone();

    debug!(
        "Selected balanced index root: {:?} (root_id={}, shard_count={}) for fragment {}",
        path, idx, count, fragment
    );
    Some((idx, path))
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

    if !scan_trash && delimiter.is_some() {
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

    if !scan_trash && (delimiter.is_some() || !prefix.is_empty()) {
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

    let continuation_cursor = continuation_token
        .as_deref()
        .and_then(parse_continuation_token)
        .unwrap_or_default();

    let (selected_root_id, index_root) =
        match choose_index_root(&fragment, scan_trash, continuation_cursor.root_hint).await {
            Some(root) => root,
            None => {
                debug!("No active index root found for fragment {}", fragment);
                return Ok(AxumJson(ListObjectsResult {
                    objects: Vec::new(),
                    common_prefixes: Vec::new(),
                    continuation_token: None,
                }));
            }
        };

    let pfx = canonical_prefix(&prefix);
    let pfx_with_slash = if pfx.is_empty() {
        String::new()
    } else {
        format!("{}/", pfx)
    };

    let mut results: Vec<ObjectSummary> = Vec::new();
    let mut out_prefixes: Vec<String> = Vec::new();
    let mut seen_prefixes: HashSet<String> = HashSet::new();
    let mut next_token = None;
    let mut reached_limit = false;

    let mut s1_entries = read_dir_sorted(&index_root).await?;
    s1_entries.retain(|s| is_hex2(s));

    let s1_start_idx = if !continuation_cursor.shard1.is_empty() {
        s1_entries
            .binary_search(&continuation_cursor.shard1)
            .unwrap_or_else(|x| x)
    } else {
        0
    };

    'outer: for s1 in s1_entries.into_iter().skip(s1_start_idx) {
        let s1_path = index_root.join(&s1);

        let mut s2_entries = read_dir_sorted(&s1_path).await.unwrap_or_default();
        s2_entries.retain(|s| is_hex2(s));

        let s2_start_idx =
            if s1 == continuation_cursor.shard1 && !continuation_cursor.shard2.is_empty() {
                s2_entries
                    .binary_search(&continuation_cursor.shard2)
                    .unwrap_or_else(|x| x)
            } else {
                0
            };

        for s2 in s2_entries.into_iter().skip(s2_start_idx) {
            let s2_path = s1_path.join(&s2);

            let mut s3_entries = read_dir_sorted(&s2_path).await.unwrap_or_default();
            s3_entries.retain(|s| is_hex2(s));

            let s3_start_idx = if s1 == continuation_cursor.shard1
                && s2 == continuation_cursor.shard2
                && !continuation_cursor.shard3.is_empty()
            {
                s3_entries
                    .binary_search(&continuation_cursor.shard3)
                    .unwrap_or_else(|x| x)
            } else {
                0
            };

            for s3 in s3_entries.into_iter().skip(s3_start_idx) {
                let s3_path = s2_path.join(&s3);

                let mut s4_entries = read_dir_sorted(&s3_path).await.unwrap_or_default();
                s4_entries.retain(|s| is_hex2(s));

                let s4_start_idx = if s1 == continuation_cursor.shard1
                    && s2 == continuation_cursor.shard2
                    && s3 == continuation_cursor.shard3
                    && !continuation_cursor.shard4.is_empty()
                {
                    s4_entries
                        .binary_search(&continuation_cursor.shard4)
                        .unwrap_or_else(|x| x)
                } else {
                    0
                };

                for s4 in s4_entries.into_iter().skip(s4_start_idx) {
                    let s4_path = s3_path.join(&s4);

                    let mut file_names = Vec::new();
                    if let Ok(mut entries) = fs::read_dir(&s4_path).await {
                        while let Ok(Some(entry)) = entries.next_entry().await {
                            let fname = entry.file_name().to_string_lossy().to_string();
                            if fname.ends_with(METADATA_EXT) && fname != QUOTA_FILE {
                                file_names.push(fname);
                            }
                        }
                    }
                    file_names.sort();

                    let file_start_idx = if s1 == continuation_cursor.shard1
                        && s2 == continuation_cursor.shard2
                        && s3 == continuation_cursor.shard3
                        && s4 == continuation_cursor.shard4
                        && !continuation_cursor.file.is_empty()
                    {
                        match file_names.binary_search(&continuation_cursor.file) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        }
                    } else {
                        0
                    };

                    let stream_iter = file_names.into_iter().skip(file_start_idx);

                    let mut stream = futures_util::stream::iter(stream_iter)
                        .map(|fname| {
                            let path = s4_path.join(&fname);
                            async move {
                                let data = fs::read(&path).await;
                                (fname, data)
                            }
                        })
                        .buffered(20);

                    let mut current_hash_prefix = String::new();
                    let mut best_meta: Option<ObjectSummary> = None;
                    let mut best_fname: String = String::new();

                    while let Some((fname, read_res)) = stream.next().await {
                        let hash_part = fname.split('#').next().unwrap_or("").to_string();

                        if hash_part != current_hash_prefix {
                            if let Some(meta) = best_meta.take() {
                                let ck = canonical_key(&meta.object_key);
                                let ok_prefix = if pfx.is_empty() {
                                    true
                                } else {
                                    ck == pfx || ck.starts_with(&pfx_with_slash)
                                };

                                if ok_prefix
                                    && (!meta.is_delete_marker || scan_trash)
                                    && (include_internal
                                        || !is_internal_object_key(meta.object_key.as_str()))
                                {
                                    if delimiter.is_some() {
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
                                            results.push(meta);
                                        } else if let Some((first, _rest)) = rel.split_once('/') {
                                            if !first.is_empty() {
                                                let folder = if pfx.is_empty() {
                                                    format!("{}/", first)
                                                } else {
                                                    format!("{}/{}/", pfx, first)
                                                };
                                                if seen_prefixes.insert(folder.clone()) {
                                                    out_prefixes.push(folder);
                                                }
                                            }
                                        } else {
                                            results.push(meta);
                                        }
                                    } else {
                                        results.push(meta);
                                    }
                                }

                                if results.len() + out_prefixes.len() >= limit {
                                    next_token = Some(create_continuation_token(
                                        &s1,
                                        &s2,
                                        &s3,
                                        &s4,
                                        &best_fname,
                                        Some(selected_root_id),
                                    ));
                                    reached_limit = true;
                                    break 'outer;
                                }
                            }

                            current_hash_prefix = hash_part;
                            best_meta = None;
                            best_fname.clear();
                        }

                        match read_res {
                            Ok(data) => {
                                if let Ok(meta) = rmp_serde::from_slice::<ObjectSummary>(&data) {
                                    let take = match &best_meta {
                                        None => true,
                                        Some(cur) => {
                                            meta.created_at > cur.created_at
                                                || (meta.created_at == cur.created_at
                                                    && meta.version_id > cur.version_id)
                                        }
                                    };
                                    if take {
                                        best_fname = fname.clone();
                                        best_meta = Some(meta);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to read metadata file {}: {}", fname, e);
                            }
                        }
                    }

                    if let Some(meta) = best_meta {
                        let ck = canonical_key(&meta.object_key);
                        let ok_prefix = if pfx.is_empty() {
                            true
                        } else {
                            ck == pfx || ck.starts_with(&pfx_with_slash)
                        };

                        if ok_prefix
                            && (!meta.is_delete_marker || scan_trash)
                            && (include_internal
                                || !is_internal_object_key(meta.object_key.as_str()))
                        {
                            if delimiter.is_some() {
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
                                    results.push(meta);
                                } else if let Some((first, _rest)) = rel.split_once('/') {
                                    if !first.is_empty() {
                                        let folder = if pfx.is_empty() {
                                            format!("{}/", first)
                                        } else {
                                            format!("{}/{}/", pfx, first)
                                        };
                                        if seen_prefixes.insert(folder.clone()) {
                                            out_prefixes.push(folder);
                                        }
                                    }
                                } else {
                                    results.push(meta);
                                }
                            } else {
                                results.push(meta);
                            }
                        }
                        if results.len() + out_prefixes.len() >= limit {
                            next_token = Some(create_continuation_token(
                                &s1,
                                &s2,
                                &s3,
                                &s4,
                                &best_fname,
                                Some(selected_root_id),
                            ));
                            reached_limit = true;
                            break 'outer;
                        }
                    }
                }
            }
        }
    }

    Ok(AxumJson(ListObjectsResult {
        objects: results.into_iter().map(ObjectMetadata::from).collect(),
        common_prefixes: out_prefixes,
        continuation_token: if reached_limit { next_token } else { None },
    }))
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

async fn read_dir_sorted(path: &std::path::Path) -> Result<Vec<String>, AppError> {
    let mut entries = fs::read_dir(path)
        .await
        .map_err(|e| AppError::Io(e, "read dir".into()))?;
    let mut names = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        names.push(entry.file_name().to_string_lossy().to_string());
    }
    names.sort();
    Ok(names)
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

    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let hash = hex::encode(hasher.finalize());

    let shard1 = &hash[0..2];
    let shard2 = &hash[2..4];
    let shard3 = &hash[4..6];
    let shard4 = &hash[6..8];

    let index_dirs = path_manager.get_all_index_dirs(&fragment);

    let mut versions: Vec<ObjectSummary> = Vec::new();
    let mut seen_versions = HashSet::new();

    for dir in index_dirs {
        let candidate = dir.join(shard1).join(shard2).join(shard3).join(shard4);
        if !fs::try_exists(&candidate).await.unwrap_or(false) {
            continue;
        }

        let prefix = format!("{}#", hash);
        let mut entries = match fs::read_dir(&candidate).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&prefix) && name.ends_with(METADATA_EXT) {
                if let Some(rest) = name.strip_prefix(&prefix) {
                    if let Some(ver_id) = rest.strip_suffix(METADATA_EXT) {
                        if seen_versions.contains(ver_id) {
                            continue;
                        }

                        if let Ok(data) = fs::read(entry.path()).await {
                            if let Ok(meta) = rmp_serde::from_slice::<ObjectSummary>(&data) {
                                if seen_versions.insert(meta.version_id.clone()) {
                                    versions.push(meta);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    versions.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let response_objects: Vec<ObjectMetadata> =
        versions.into_iter().map(ObjectMetadata::from).collect();

    Ok(AxumJson(response_objects))
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
