use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{
    BROWSER_CACHE_MAX_AGE, METADATA_EXT, PREFIX_SEGMENT_EXT, QUOTA_FILE,
};
use crate::object_storage::disk_manager::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, PrefixIndexSegment, Quota,
};
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
use futures_util::StreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use tokio::fs;
use tracing::{debug, warn};
use walkdir::WalkDir;

fn parse_continuation_token(token: &str) -> Option<(String, String, String, String, String)> {
    let bytes = general_purpose::STANDARD.decode(token).ok()?;
    let s = String::from_utf8(bytes).ok()?;
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 5 {
        Some((
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
            parts[3].to_string(),
            parts[4].to_string(),
        ))
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
) -> String {
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

async fn read_metadata_any(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Option<ObjectMetadata> {
    let paths = path_manager.get_all_metadata_paths(fragment, object_key, version_id);
    for p in paths {
        if !fs::try_exists(&p).await.unwrap_or(false) {
            continue;
        }
        if let Ok(data) = fs::read(&p).await {
            if let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(&data) {
                return Some(meta);
            }
        }
    }
    None
}

async fn try_list_by_prefix_index(
    fragment: &str,
    prefix: &str,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
) -> Result<Option<ListObjectsResult>, AppError> {
    let pfx = canonical_prefix(prefix);
    let want_delim = delimiter == Some("/");

    let path_manager = ObjectPathManager::new();

    let bucket_dirs = path_manager.get_all_prefix_bucket_dirs(fragment, &pfx);
    let mut bucket_dir = None;
    for d in bucket_dirs {
        if fs::try_exists(&d).await.unwrap_or(false) {
            bucket_dir = Some(d);
            break;
        }
    }

    let bucket_dir = match bucket_dir {
        Some(d) => d,
        None => return Ok(None),
    };

    let mut segments: Vec<PrefixIndexSegment> = Vec::new();

    for compacted_path in path_manager.get_all_prefix_compacted_paths(fragment, &pfx) {
        if fs::try_exists(&compacted_path).await.unwrap_or(false) {
            if let Ok(data) = fs::read(&compacted_path).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    segments.push(seg);
                    break;
                }
            }
        }
    }

    let mut seg_files = Vec::new();
    if let Ok(mut entries) = fs::read_dir(&bucket_dir).await {
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
                seg_files.push((name, entry.path()));
            }
        }
    }
    seg_files.sort_by(|a, b| a.0.cmp(&b.0));

    for (_name, path) in seg_files {
        if let Ok(data) = fs::read(&path).await {
            if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                segments.push(seg);
            }
        }
    }

    if segments.is_empty() {
        return Ok(None);
    }

    let mut latest: HashMap<String, (String, i64)> = HashMap::new();

    for seg in segments {
        if canonical_prefix(&seg.prefix) != pfx {
            continue;
        }
        for e in seg.entries {
            let ck = canonical_key(&e.object_key);

            if !pfx.is_empty() {
                if ck != pfx && !ck.starts_with(&(pfx.clone() + "/")) {
                    continue;
                }
            }

            match latest.get(&e.object_key) {
                None => {
                    latest.insert(e.object_key.clone(), (e.version_id.clone(), e.created_at));
                }
                Some((cur_vid, cur_ts)) => {
                    if e.created_at > *cur_ts
                        || (e.created_at == *cur_ts && e.version_id > *cur_vid)
                    {
                        latest.insert(e.object_key.clone(), (e.version_id.clone(), e.created_at));
                    }
                }
            }
        }
    }

    if latest.is_empty() {
        return Ok(Some(ListObjectsResult {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            continuation_token: None,
        }));
    }

    let mut entries: Vec<(String, String, i64)> =
        latest.into_iter().map(|(k, (v, ts))| (k, v, ts)).collect();

    entries.sort_by(|a, b| {
        let (ak, av, _ats) = a;
        let (bk, bv, _bts) = b;
        match ak.cmp(bk) {
            std::cmp::Ordering::Equal => av.cmp(bv),
            other => other,
        }
    });

    let mut start_after: Option<(String, String)> = None;
    if let Some(tok) = continuation_token.as_deref() {
        if let Some((k, v)) = parse_prefix_continuation_token(tok) {
            start_after = Some((k, v));
        }
    }

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

    let mut out_objects: Vec<ObjectMetadata> = Vec::new();
    let mut out_prefixes: Vec<String> = Vec::new();
    let mut seen_prefixes: HashSet<String> = HashSet::new();

    let mut last_pair: Option<(String, String)> = None;
    let mut reached_limit = false;

    for (k, v, _ts) in entries.into_iter().skip(start_idx) {
        if out_objects.len() + out_prefixes.len() >= limit {
            break;
        }

        let Some(meta) = read_metadata_any(&path_manager, fragment, &k, &v).await else {
            continue;
        };

        if meta.is_delete_marker {
            continue;
        }

        let ck = canonical_key(&meta.object_key);

        if !pfx.is_empty() {
            if ck != pfx && !ck.starts_with(&(pfx.clone() + "/")) {
                continue;
            }
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
                out_objects.push(meta);
                last_pair = Some((k, v));
            }
        } else {
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
        objects: out_objects,
        common_prefixes: out_prefixes,
        continuation_token: next_token,
    }))
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
    let delimiter = params
        .get("delimiter")
        .map(|s| s.as_str())
        .filter(|d| *d == "/");

    if !scan_trash && (delimiter.is_some() || !prefix.is_empty()) {
        if let Some(res) = try_list_by_prefix_index(
            &fragment,
            &prefix,
            delimiter,
            limit,
            continuation_token.clone(),
        )
        .await?
        {
            return Ok(AxumJson(res));
        }
    }

    if delimiter.is_some() {
        return Ok(AxumJson(ListObjectsResult {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            continuation_token: None,
        }));
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    let mut active_index_root = None;

    for root in index_roots {
        let candidate = if scan_trash {
            root.join(&fragment).join(".trash").join("index")
        } else {
            root.join(&fragment).join("index")
        };

        if fs::try_exists(&candidate).await.unwrap_or(false) {
            active_index_root = Some(candidate);
            break;
        }
    }

    let (start_s1, start_s2, start_s3, start_s4, start_file) = match continuation_token.as_deref() {
        Some(token) => parse_continuation_token(token).unwrap_or_default(),
        None => (
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
        ),
    };

    let index_root = match active_index_root {
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

    debug!("Listing objects in root: {:?}", index_root);

    let pfx = canonical_prefix(&prefix);

    let mut results = Vec::new();
    let mut next_token = None;
    let mut reached_limit = false;

    let mut s1_entries = read_dir_sorted(&index_root).await?;
    s1_entries.retain(|s| is_hex2(s));

    let s1_start_idx = if !start_s1.is_empty() {
        s1_entries.binary_search(&start_s1).unwrap_or_else(|x| x)
    } else {
        0
    };

    'outer: for s1 in s1_entries.into_iter().skip(s1_start_idx) {
        let s1_path = index_root.join(&s1);

        let mut s2_entries = read_dir_sorted(&s1_path).await.unwrap_or_default();
        s2_entries.retain(|s| is_hex2(s));

        let s2_start_idx = if s1 == start_s1 && !start_s2.is_empty() {
            s2_entries.binary_search(&start_s2).unwrap_or_else(|x| x)
        } else {
            0
        };

        for s2 in s2_entries.into_iter().skip(s2_start_idx) {
            let s2_path = s1_path.join(&s2);

            let mut s3_entries = read_dir_sorted(&s2_path).await.unwrap_or_default();
            s3_entries.retain(|s| is_hex2(s));

            let s3_start_idx = if s1 == start_s1 && s2 == start_s2 && !start_s3.is_empty() {
                s3_entries.binary_search(&start_s3).unwrap_or_else(|x| x)
            } else {
                0
            };

            for s3 in s3_entries.into_iter().skip(s3_start_idx) {
                let s3_path = s2_path.join(&s3);

                let mut s4_entries = read_dir_sorted(&s3_path).await.unwrap_or_default();
                s4_entries.retain(|s| is_hex2(s));

                let s4_start_idx =
                    if s1 == start_s1 && s2 == start_s2 && s3 == start_s3 && !start_s4.is_empty() {
                        s4_entries.binary_search(&start_s4).unwrap_or_else(|x| x)
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

                    let file_start_idx = if s1 == start_s1
                        && s2 == start_s2
                        && s3 == start_s3
                        && s4 == start_s4
                        && !start_file.is_empty()
                    {
                        match file_names.binary_search(&start_file) {
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
                    let mut pending_meta: Option<ObjectMetadata> = None;
                    let mut last_fname = String::new();

                    while let Some((fname, read_res)) = stream.next().await {
                        let hash_part = fname.split('#').next().unwrap_or("").to_string();

                        if hash_part != current_hash_prefix {
                            if let Some(meta) = pending_meta.take() {
                                let ok_prefix = if pfx.is_empty() {
                                    true
                                } else {
                                    let ck = canonical_key(&meta.object_key);
                                    ck == pfx || ck.starts_with(&(pfx.clone() + "/"))
                                };

                                if ok_prefix && (!meta.is_delete_marker || scan_trash) {
                                    results.push(meta);
                                }

                                if results.len() >= limit {
                                    next_token = Some(create_continuation_token(
                                        &s1,
                                        &s2,
                                        &s3,
                                        &s4,
                                        &last_fname,
                                    ));
                                    reached_limit = true;
                                    break 'outer;
                                }
                            }
                            current_hash_prefix = hash_part;
                        }

                        match read_res {
                            Ok(data) => match rmp_serde::from_slice::<ObjectMetadata>(&data) {
                                Ok(meta) => {
                                    pending_meta = Some(meta);
                                }
                                Err(e) => {
                                    warn!("Failed to deserialize metadata file {}: {}", fname, e);
                                }
                            },
                            Err(e) => {
                                warn!("Failed to read metadata file {}: {}", fname, e);
                            }
                        }
                        last_fname = fname;
                    }

                    if let Some(meta) = pending_meta {
                        let ok_prefix = if pfx.is_empty() {
                            true
                        } else {
                            let ck = canonical_key(&meta.object_key);
                            ck == pfx || ck.starts_with(&(pfx.clone() + "/"))
                        };

                        if ok_prefix && (!meta.is_delete_marker || scan_trash) {
                            results.push(meta);
                        }
                        if results.len() >= limit {
                            next_token =
                                Some(create_continuation_token(&s1, &s2, &s3, &s4, &last_fname));
                            reached_limit = true;
                            break 'outer;
                        }
                    }
                }
            }
        }
    }

    Ok(AxumJson(ListObjectsResult {
        objects: results,
        common_prefixes: Vec::new(),
        continuation_token: if reached_limit { next_token } else { None },
    }))
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

    let mut active_shard_dir = None;
    for dir in index_dirs {
        let candidate = dir.join(shard1).join(shard2).join(shard3).join(shard4);
        if fs::try_exists(&candidate).await.unwrap_or(false) {
            active_shard_dir = Some(candidate);
            break;
        }
    }

    let shard_dir = active_shard_dir.ok_or(AppError::NotFound)?;

    let prefix = format!("{}#", hash);
    let mut versions = Vec::new();

    let mut entries = fs::read_dir(&shard_dir)
        .await
        .map_err(|e| AppError::Io(e, "read index shard".into()))?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && name.ends_with(METADATA_EXT) {
            if let Ok(data) = fs::read(entry.path()).await {
                if let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(&data) {
                    versions.push(meta);
                }
            }
        }
    }

    versions.sort_by(|a, b| b.created_at.cmp(&a.created_at));

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
                        fragment_hashes.insert(name.to_string());
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
