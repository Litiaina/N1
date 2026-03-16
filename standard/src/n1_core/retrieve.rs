use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{BROWSER_CACHE_MAX_AGE, METADATA_EXT, PREFIX_SEGMENT_EXT};
use crate::n1_core::disk_manager::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, ObjectSummary, PrefixIndexEntry,
    PrefixIndexSegment,
};
use crate::n1_core::fragment_manager::is_fragment_hash;
use crate::n1_core::index;
use crate::n1_core::metadata_parser::get_metadata;
use crate::n1_core::object_parser::{get_object_logic, stream_object_logic};
use crate::n1_core::prefix_compactor::{compact_prefix_now, compact_trash_prefix_now};
use crate::n1_core::prefix_rebuilder::rebuild_fragment_prefix_accelerator;
use crate::n1_core::prefix_updates::{
    accelerator_prefix, fragment_accelerator_is_stale, synthetic_live_entry,
};
use crate::n1_core::quota_manager::get_storage_usage;
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
    _path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    scan_trash: bool,
) -> Option<ObjectSummary> {
    let metadata = if scan_trash {
        index::read_trashed_object_version(fragment, object_key, version_id).await
    } else {
        index::read_object_version(fragment, object_key, version_id).await
    };

    metadata.ok().map(|m| ObjectSummary {
        object_key: m.object_key,
        version_id: m.version_id,
        fragment: m.fragment,
        upload_session_id: m.upload_session_id,
        owner_uid: m.owner_uid,
        content_type: m.content_type,
        total_size_bytes: m.total_size_bytes,
        created_at: m.created_at,
        _parts_ignored: serde::de::IgnoredAny,
        is_delete_marker: m.is_delete_marker,
        stripe_size_bytes: m.stripe_size_bytes,
    })
}

async fn read_index_entry_best(
    path_manager: &ObjectPathManager,
    fragment: &str,
    entry: &PrefixIndexEntry,
    scan_trash: bool,
) -> Option<ObjectSummary> {
    if let Some(metadata) = entry.to_object_metadata(fragment) {
        return Some(ObjectSummary {
            object_key: metadata.object_key,
            fragment: metadata.fragment,
            version_id: metadata.version_id,
            upload_session_id: metadata.upload_session_id,
            owner_uid: metadata.owner_uid,
            content_type: metadata.content_type,
            total_size_bytes: metadata.total_size_bytes,
            created_at: metadata.created_at,
            _parts_ignored: serde::de::IgnoredAny,
            is_delete_marker: metadata.is_delete_marker,
            stripe_size_bytes: metadata.stripe_size_bytes,
        });
    }

    read_metadata_best(
        path_manager,
        fragment,
        &entry.object_key,
        &entry.version_id,
        scan_trash,
    )
    .await
}

async fn load_best_prefix_compacted_segment(
    fragment: &str,
    prefix: &str,
    scan_trash: bool,
) -> Option<(PrefixIndexSegment, usize)> {
    let pfx = canonical_prefix(prefix);
    let path_manager = ObjectPathManager::new();
    let mut best: Option<(std::time::SystemTime, PrefixIndexSegment)> = None;
    let mut delta_count = 0usize;

    let bucket_dirs = if scan_trash {
        path_manager.get_all_trash_prefix_bucket_dirs(fragment, &pfx)
    } else {
        path_manager.get_all_prefix_bucket_dirs(fragment, &pfx)
    };
    for dir in bucket_dirs {
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            continue;
        }

        let compacted_path = dir.join(format!("compacted{}", PREFIX_SEGMENT_EXT));
        if fs::try_exists(&compacted_path).await.unwrap_or(false) {
            let modified = fs::metadata(&compacted_path)
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::UNIX_EPOCH);
            if let Ok(data) = fs::read(&compacted_path).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    if canonical_prefix(&seg.prefix) == pfx
                        && best
                            .as_ref()
                            .map(|(current, _)| modified > *current)
                            .unwrap_or(true)
                    {
                        best = Some((modified, seg));
                    }
                }
            }
        }

        if let Ok(mut entries) = fs::read_dir(&dir).await {
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
                    delta_count += 1;
                }
            }
        }
    }

    let (_best_mtime, seg) = best?;
    Some((seg, delta_count))
}

fn first_prefix_entry_after(entries: &[PrefixIndexEntry], key: &str, version_id: &str) -> usize {
    entries
        .binary_search_by(
            |entry| match canonical_key(&entry.object_key).as_str().cmp(key) {
                std::cmp::Ordering::Equal => entry.version_id.as_str().cmp(version_id),
                other => other,
            },
        )
        .map(|idx| idx + 1)
        .unwrap_or_else(|idx| idx)
}

fn first_prefix_entry_for_prefix(entries: &[PrefixIndexEntry], prefix: &str) -> usize {
    let start = canonical_prefix(prefix);
    entries
        .binary_search_by(|entry| canonical_key(&entry.object_key).cmp(&start))
        .unwrap_or_else(|idx| idx)
}

async fn list_prefix_compacted_page_fast(
    fragment: &str,
    prefix: &str,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
    mut seg: PrefixIndexSegment,
    scan_trash: bool,
) -> Result<ListObjectsResult, AppError> {
    let pfx = canonical_prefix(prefix);
    let pfx_with_slash = if pfx.is_empty() {
        String::new()
    } else {
        format!("{}/", pfx)
    };
    if !pfx.is_empty() && !seg.entries.iter().any(|entry| canonical_key(&entry.object_key) == pfx) {
        if let Some(exact_entry) = load_exact_prefix_latest_entry(fragment, &pfx, scan_trash).await {
            let insert_at = seg
                .entries
                .binary_search_by(|entry| match canonical_key(&entry.object_key).cmp(&pfx) {
                    std::cmp::Ordering::Equal => entry.version_id.cmp(&exact_entry.version_id),
                    other => other,
                })
                .unwrap_or_else(|idx| idx);
            seg.entries.insert(insert_at, exact_entry);
        }
    }
    let path_manager = ObjectPathManager::new();
    let want_delim = delimiter == Some("/");
    let start_after_delim = if want_delim {
        continuation_token
            .as_deref()
            .and_then(parse_delimiter_continuation_token)
    } else {
        None
    };
    let start_idx = if want_delim {
        if !pfx.is_empty() {
            first_prefix_entry_for_prefix(&seg.entries, &pfx)
        } else {
            0
        }
    } else if let Some(tok) = continuation_token.as_deref() {
        let Some((object_key, version_id)) = parse_prefix_continuation_token(tok) else {
            return Ok(ListObjectsResult {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                continuation_token: None,
            });
        };
        first_prefix_entry_after(&seg.entries, &canonical_key(&object_key), &version_id)
    } else if !pfx.is_empty() {
        first_prefix_entry_for_prefix(&seg.entries, &pfx)
    } else {
        0
    };

    let mut out_objects = Vec::<ObjectMetadata>::new();
    let mut out_prefixes = Vec::<String>::new();
    let mut seen_prefixes = HashSet::<String>::new();
    let mut last_pair = None::<(String, String)>;
    let mut last_row_key = None::<String>;
    let mut reached_limit = false;

    for entry in seg.entries.into_iter().skip(start_idx) {
        if out_objects.len() + out_prefixes.len() >= limit {
            break;
        }

        let key = canonical_key(&entry.object_key);
        if entry.is_delete_marker {
            continue;
        }
        if !include_internal && is_internal_object_key(&key) {
            continue;
        }
        if !pfx.is_empty() && key != pfx && !key.starts_with(&pfx_with_slash) {
            continue;
        }

        if want_delim {
            let rel = if pfx.is_empty() {
                key.clone()
            } else if key == pfx {
                key.clone()
            } else if key.len() > pfx.len() + 1 {
                key[(pfx.len() + 1)..].to_string()
            } else {
                String::new()
            };

            if !pfx.is_empty() && key == pfx {
                if let Some(start_after) = start_after_delim.as_deref() {
                    if key.as_str() <= start_after {
                        continue;
                    }
                }
                let Some(meta) =
                    read_index_entry_best(&path_manager, fragment, &entry, scan_trash).await
                else {
                    warn!(
                        "Skipping unreadable compacted prefix metadata fragment={} key={} version={}",
                        fragment, key, entry.version_id
                    );
                    continue;
                };
                if meta.is_delete_marker {
                    continue;
                }

                last_pair = Some((key.clone(), entry.version_id.clone()));
                last_row_key = Some(key);
                out_objects.push(meta.into());
            } else if let Some((first, _rest)) = rel.split_once('/') {
                if first.is_empty() {
                    continue;
                }
                let common_prefix = if pfx.is_empty() {
                    format!("{}/", first)
                } else {
                    format!("{}/{}/", pfx, first)
                };
                if let Some(start_after) = start_after_delim.as_deref() {
                    if common_prefix.as_str() <= start_after {
                        continue;
                    }
                }
                if seen_prefixes.insert(common_prefix.clone()) {
                    last_row_key = Some(common_prefix.clone());
                    out_prefixes.push(common_prefix);
                }
            } else {
                if let Some(start_after) = start_after_delim.as_deref() {
                    if key.as_str() <= start_after {
                        continue;
                    }
                }
                let Some(meta) =
                    read_index_entry_best(&path_manager, fragment, &entry, scan_trash).await
                else {
                    warn!(
                        "Skipping unreadable compacted prefix metadata fragment={} key={} version={}",
                        fragment, key, entry.version_id
                    );
                    continue;
                };
                if meta.is_delete_marker {
                    continue;
                }

                last_pair = Some((key.clone(), entry.version_id.clone()));
                last_row_key = Some(key);
                out_objects.push(meta.into());
            }
        } else {
            let Some(meta) =
                read_index_entry_best(&path_manager, fragment, &entry, scan_trash).await
            else {
                warn!(
                    "Skipping unreadable compacted prefix metadata fragment={} key={} version={}",
                    fragment, key, entry.version_id
                );
                continue;
            };
            if meta.is_delete_marker {
                continue;
            }

            last_pair = Some((key, entry.version_id.clone()));
            out_objects.push(meta.into());
        }

        if out_objects.len() + out_prefixes.len() >= limit {
            reached_limit = true;
            break;
        }
    }

    Ok(ListObjectsResult {
        objects: out_objects,
        common_prefixes: out_prefixes,
        continuation_token: if reached_limit {
            if want_delim {
                last_row_key
                    .as_deref()
                    .map(create_delimiter_continuation_token)
            } else {
                last_pair
                    .as_ref()
                    .map(|(key, version)| create_prefix_continuation_token(key, version))
            }
        } else {
            None
        },
    })
}

fn merge_prefix_latest(latest: &mut HashMap<String, PrefixIndexEntry>, entry: PrefixIndexEntry) {
    let object_key = canonical_key(&entry.object_key);

    match latest.get(&object_key) {
        None => {
            latest.insert(object_key, entry);
        }
        Some(current) => {
            if entry.created_at > current.created_at
                || (entry.created_at == current.created_at && entry.version_id > current.version_id)
            {
                latest.insert(object_key, entry);
            }
        }
    }
}

async fn load_exact_prefix_latest_entry(
    fragment: &str,
    prefix: &str,
    scan_trash: bool,
) -> Option<PrefixIndexEntry> {
    let res = if scan_trash {
        index::read_latest_trashed_object(fragment, prefix).await
    } else {
        index::read_latest_object(fragment, prefix).await
    };
    match res {
        Ok(meta) if !meta.is_delete_marker => Some(synthetic_live_entry(&meta, meta.created_at)),
        _ => None,
    }
}

async fn try_list_by_prefix_index(
    fragment: &str,
    prefix: &str,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
    scan_trash: bool,
) -> Result<Option<ListObjectsResult>, AppError> {
    let pfx = canonical_prefix(prefix);
    let pfx_with_slash = if pfx.is_empty() {
        String::new()
    } else {
        format!("{}/", pfx)
    };
    let want_delim = delimiter == Some("/");
    let accelerator_prefix = if pfx.is_empty() {
        accelerator_prefix().to_string()
    } else {
        pfx.clone()
    };

    if fragment_accelerator_is_stale(fragment, scan_trash).await {
        let _ = rebuild_fragment_prefix_accelerator(fragment).await;
    }

    if let Some((_, delta_count)) =
        load_best_prefix_compacted_segment(fragment, &accelerator_prefix, scan_trash).await
    {
        if delta_count > prefix_max_delta_segments() {
            let _ = if scan_trash {
                compact_trash_prefix_now(fragment, &accelerator_prefix).await
            } else {
                compact_prefix_now(fragment, &accelerator_prefix).await
            };
        }
    } else {
        let _ = if scan_trash {
            compact_trash_prefix_now(fragment, &accelerator_prefix).await
        } else {
            compact_prefix_now(fragment, &accelerator_prefix).await
        };
    }

    if let Some((seg, delta_count)) =
        load_best_prefix_compacted_segment(fragment, &accelerator_prefix, scan_trash).await
    {
        if delta_count == 0 {
            return Ok(Some(
                list_prefix_compacted_page_fast(
                    fragment,
                    &pfx,
                    delimiter,
                    limit,
                    continuation_token,
                    include_internal,
                    seg,
                    scan_trash,
                )
                .await?,
            ));
        }
    }

    let path_manager = ObjectPathManager::new();
    let mut latest: HashMap<String, PrefixIndexEntry> = HashMap::new();
    let mut had_any_index_data = false;

    let bucket_dirs = if scan_trash {
        path_manager.get_all_trash_prefix_bucket_dirs(fragment, &accelerator_prefix)
    } else {
        path_manager.get_all_prefix_bucket_dirs(fragment, &accelerator_prefix)
    };
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
                    for e in seg.entries {
                        let ck = canonical_key(&e.object_key);
                        if !pfx.is_empty() && ck != pfx && !ck.starts_with(&pfx_with_slash) {
                            continue;
                        }
                        merge_prefix_latest(&mut latest, e);
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
            seg_files.iter().filter(|(_, ts, _)| *ts >= cmp_ts).count()
        } else {
            seg_files.len()
        };

        let max_deltas = prefix_max_delta_segments();
        if recent_delta_count > max_deltas {
            debug!(
                "Prefix delta fan-in high for fragment={} prefix='{}' bucket={:?} recent_deltas={} (max={}); continuing index merge",
                fragment, pfx, d, recent_delta_count, max_deltas
            );
        }

        for (_name, _mtime, path) in seg_files {
            if let Ok(data) = fs::read(&path).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    had_any_index_data = true;
                    for e in seg.entries {
                        let ck = canonical_key(&e.object_key);

                        if !pfx.is_empty() && ck != pfx && !ck.starts_with(&pfx_with_slash) {
                            continue;
                        }

                        merge_prefix_latest(&mut latest, e);
                    }
                }
            }
        }
    }

    if !had_any_index_data {
        let _ = rebuild_fragment_prefix_accelerator(fragment).await;

        if let Some((seg, delta_count)) =
            load_best_prefix_compacted_segment(fragment, &accelerator_prefix, scan_trash).await
        {
            if delta_count == 0 {
                return Ok(Some(
                    list_prefix_compacted_page_fast(
                        fragment,
                        &pfx,
                        delimiter,
                        limit,
                        continuation_token,
                        include_internal,
                        seg,
                        scan_trash,
                    )
                    .await?,
                ));
            }
        }

        return Ok(None);
    }

    if !pfx.is_empty() && !latest.contains_key(&pfx) {
        if let Some(entry) = load_exact_prefix_latest_entry(fragment, &pfx, scan_trash).await {
            latest.insert(pfx.clone(), entry);
        }
    }

    if latest.is_empty() {
        return Ok(Some(ListObjectsResult {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            continuation_token: None,
        }));
    }

    let mut entries: Vec<PrefixIndexEntry> = latest.into_values().collect();

    entries.sort_by(|a, b| match canonical_key(&a.object_key).cmp(&canonical_key(&b.object_key)) {
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
        match entries.binary_search_by(|entry| match canonical_key(&entry.object_key).cmp(k) {
            std::cmp::Ordering::Equal => entry.version_id.cmp(v),
            other => other,
        }) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    } else if !entries.is_empty() && !pfx.is_empty() {
        match entries.binary_search_by(|entry| canonical_key(&entry.object_key).cmp(&pfx)) {
            Ok(i) => i,
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

    for entry in entries.into_iter().skip(start_idx) {
        if out_objects.len() + out_prefixes.len() >= limit {
            break;
        }

        let k = canonical_key(&entry.object_key);
        let v = entry.version_id.clone();

        if entry.is_delete_marker {
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
                let Some(meta) =
                    read_index_entry_best(&path_manager, fragment, &entry, scan_trash).await
                else {
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
                let Some(meta) =
                    read_index_entry_best(&path_manager, fragment, &entry, scan_trash).await
                else {
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
            let Some(meta) = read_index_entry_best(&path_manager, fragment, &entry, scan_trash).await
            else {
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

    Ok(AxumJson(
        list_objects_internal(fragment, params, false).await?,
    ))
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

    Ok(AxumJson(
        list_objects_internal(fragment, params, true).await?,
    ))
}

async fn list_objects_partition_native(
    fragment: &str,
    scan_trash: bool,
    prefix: &str,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    #[derive(Clone)]
    struct Row {
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

    let mut rows: Vec<Row> = Vec::new();
    let mut seen_prefixes = HashSet::<String>::new();

    let metas = if scan_trash {
        index::list_fragment_trash_objects(fragment).await?
    } else {
        index::list_fragment_live_objects(fragment).await?
    };

    for meta in metas {
        if meta.is_delete_marker {
            continue;
        }
        let ck = canonical_key(&meta.object_key);
        if !include_internal && is_internal_object_key(&ck) {
            continue;
        }
        if !pfx.is_empty() && ck != pfx && !ck.starts_with(&pfx_with_slash) {
            continue;
        }

        if want_delim {
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
                rows.push(Row {
                    row_key: format!("o|{}|{}", ck, meta.version_id),
                    object: Some(meta),
                    common_prefix: None,
                });
            } else if let Some((first, _)) = rel.split_once('/') {
                if !first.is_empty() {
                    let folder = if pfx.is_empty() {
                        format!("{}/", first)
                    } else {
                        format!("{}/{}/", pfx, first)
                    };
                    if seen_prefixes.insert(folder.clone()) {
                        rows.push(Row {
                            row_key: format!("p|{}", folder),
                            object: None,
                            common_prefix: Some(folder),
                        });
                    }
                }
            } else {
                rows.push(Row {
                    row_key: format!("o|{}|{}", ck, meta.version_id),
                    object: Some(meta),
                    common_prefix: None,
                });
            }
        } else {
            rows.push(Row {
                row_key: format!("o|{}|{}", ck, meta.version_id),
                object: Some(meta),
                common_prefix: None,
            });
        }
    }

    rows.sort_by(|left, right| left.row_key.cmp(&right.row_key));

    let start_after = continuation_token.as_deref().and_then(|token| {
        parse_delimiter_continuation_token(token).or_else(|| {
            parse_prefix_continuation_token(token).map(|(object_key, version_id)| {
                format!("o|{}|{}", canonical_key(&object_key), version_id)
            })
        })
    });
    let start_idx = if let Some(start_after) = start_after.as_ref() {
        match rows.binary_search_by(|row| row.row_key.cmp(start_after)) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        }
    } else {
        0
    };

    let mut out_objects = Vec::<ObjectMetadata>::new();
    let mut out_prefixes = Vec::<String>::new();
    let mut last_row_key = None::<String>;

    for row in rows.iter().skip(start_idx).take(limit) {
        if let Some(meta) = row.object.clone() {
            out_objects.push(meta);
        } else if let Some(common_prefix) = row.common_prefix.clone() {
            out_prefixes.push(common_prefix);
        }
        last_row_key = Some(row.row_key.clone());
    }

    let has_more = start_idx.saturating_add(out_objects.len() + out_prefixes.len()) < rows.len();
    let continuation_token = if has_more {
        last_row_key
            .as_deref()
            .map(create_delimiter_continuation_token)
    } else {
        None
    };

    Ok(ListObjectsResult {
        objects: out_objects,
        common_prefixes: out_prefixes,
        continuation_token,
    })
}

pub async fn list_objects_direct(
    fragment: &str,
    params: HashMap<String, String>,
    scan_trash: bool,
) -> Result<ListObjectsResult, AppError> {
    list_objects_internal(fragment.to_string(), params, scan_trash).await
}

async fn list_objects_internal(
    fragment: String,
    params: HashMap<String, String>,
    scan_trash: bool,
) -> Result<ListObjectsResult, AppError> {
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

    if let Some(res) = try_list_by_prefix_index(
        &fragment,
        &prefix,
        delimiter,
        limit,
        continuation_token.clone(),
        include_internal,
        scan_trash,
    )
    .await?
    {
        return Ok(res);
    }

    if let Ok(res) = list_objects_partition_native(
        &fragment,
        scan_trash,
        &prefix,
        delimiter,
        limit,
        continuation_token.clone(),
        include_internal,
    )
    .await
    {
        return Ok(res);
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
                return Ok(ListObjectsResult {
                    objects: Vec::new(),
                    common_prefixes: Vec::new(),
                    continuation_token: None,
                });
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
                            if fname.ends_with(METADATA_EXT) {
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

    Ok(ListObjectsResult {
        objects: results.into_iter().map(ObjectMetadata::from).collect(),
        common_prefixes: out_prefixes,
        continuation_token: if reached_limit { next_token } else { None },
    })
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

    Ok(AxumJson(
        index::list_live_object_versions_metadata(&fragment, &object_key).await?,
    ))
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

    let usage = get_storage_usage().await?;
    let mut usage_list = Vec::with_capacity(fragments.len());

    for fragment_hash in fragments {
        usage_list.push(FragmentUsage {
            fragment: fragment_hash,
            total: usage.capacity_allocated,
            used: usage.capacity_used,
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

    Ok(AxumJson(
        get_object_metadata_direct(&fragment, &object_key).await?,
    ))
}

pub async fn get_object_metadata_direct(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let metadata = get_metadata(fragment, object_key, fragment).await?;
    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }
    Ok(metadata)
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
