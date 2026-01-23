use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{BROWSER_CACHE_MAX_AGE, PART_META_SUFFIX};
use crate::object_storage::directory_index::DirectoryIndexManager;
use crate::object_storage::metadata_parser::get_metadata;
use crate::object_storage::object_parser::{get_object_logic, stream_object_logic};
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, ObjectSummary,
};
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
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tokio::fs;
use walkdir::WalkDir;

const TRASH_DIR: &str = ".trash";

fn normalize_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

fn is_trash_key(key: &str) -> bool {
    let k = key.trim_start_matches('/');
    k == TRASH_DIR || k.starts_with(".trash/")
}

fn validate_reserved_paths(object_key: &str) -> Result<(), AppError> {
    if is_trash_key(object_key) {
        return Err(AppError::BadRequest(
            "'.trash' is reserved and cannot be used directly.".into(),
        ));
    }
    Ok(())
}

fn is_legacy_delete_marker(meta: &ObjectMetadata) -> bool {
    meta.content_type == "application/x-delete-marker"
}

fn parse_continuation_token(token: &str) -> Option<(String, String, String)> {
    let bytes = general_purpose::STANDARD.decode(token).ok()?;
    let s = String::from_utf8(bytes).ok()?;
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 3 {
        Some((
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
        ))
    } else {
        None
    }
}

fn create_continuation_token(shard1: &str, shard2: &str, filename: &str) -> String {
    general_purpose::STANDARD.encode(format!("{}/{}/{}", shard1, shard2, filename))
}

fn hash_key(object_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    hex::encode(hasher.finalize())
}

async fn get_latest_summary_in_root(
    index_root: &std::path::PathBuf,
    hash: &str,
) -> Result<ObjectSummary, ()> {
    if hash.len() < 8 {
        return Err(());
    }

    let s1 = &hash[0..2];
    let s2 = &hash[2..4];
    let shard_dir = index_root.join(s1).join(s2);

    let mut entries = fs::read_dir(&shard_dir).await.map_err(|_| ())?;

    let prefix = format!("{}#", hash);
    let mut best_ver: Option<String> = None;
    let mut best_path: Option<std::path::PathBuf> = None;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let fname = entry.file_name().to_string_lossy().to_string();

        if !fname.starts_with(&prefix)
            || !fname.ends_with(".bson")
            || fname.ends_with(PART_META_SUFFIX)
        {
            continue;
        }

        let ver_part = fname.strip_prefix(&prefix).ok_or(())?;
        let ver = ver_part.strip_suffix(".bson").ok_or(())?.to_string();

        let take = match &best_ver {
            None => true,
            Some(curr) => ver > *curr,
        };

        if take {
            best_ver = Some(ver);
            best_path = Some(entry.path());
        }
    }

    let p = best_path.ok_or(())?;
    let data = fs::read(p).await.map_err(|_| ())?;
    mongodb::bson::from_slice::<ObjectSummary>(&data).map_err(|_| ())
}

async fn pick_active_index_root(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
) -> Option<std::path::PathBuf> {
    for dir in path_manager.get_all_index_dirs(uid, fragment) {
        if fs::try_exists(&dir).await.unwrap_or(false) {
            return Some(dir);
        }
    }
    None
}

async fn pick_trash_index_root(
    path_manager: &ObjectPathManager,
    uid: &str,
    fragment: &str,
) -> Option<std::path::PathBuf> {
    for user_dir in path_manager.get_all_user_index_dirs(uid) {
        let candidate = user_dir.join(fragment).join(TRASH_DIR).join("index");
        if fs::try_exists(&candidate).await.unwrap_or(false) {
            return Some(candidate);
        }
    }
    None
}

pub async fn list_objects(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
        .min(1000);

    let mut prefix = params.get("prefix").cloned().unwrap_or_default();
    prefix = normalize_key(&prefix);
    if !prefix.is_empty() {
        validate_reserved_paths(&prefix)?;
    }

    let continuation_token = params.get("continuation_token").cloned();
    let (start_s1, start_s2, start_file) = match continuation_token.as_deref() {
        Some(token) => parse_continuation_token(token).unwrap_or_default(),
        None => ("".to_string(), "".to_string(), "".to_string()),
    };

    if !prefix.is_empty() {
        let index_mgr = DirectoryIndexManager::new();
        if let Ok(mut hashes) = index_mgr
            .list_hashes_by_prefix(&uid, &fragment, &prefix)
            .await
        {
            if !hashes.is_empty() {
                hashes.sort();
                hashes.dedup();

                let index_root = match pick_active_index_root(&path_manager, &uid, &fragment).await
                {
                    Some(r) => r,
                    None => {
                        return Ok(AxumJson(ListObjectsResult {
                            objects: Vec::new(),
                            continuation_token: None,
                        }));
                    }
                };

                let start_hash = start_file.split('#').next().unwrap_or("").to_string();

                let start_idx = if !start_hash.is_empty() {
                    match hashes.binary_search(&start_hash) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    }
                } else {
                    0
                };

                let mut results: Vec<ObjectMetadata> = Vec::new();
                let mut reached_limit = false;

                let mut last_s1 = String::new();
                let mut last_s2 = String::new();
                let mut last_fname = String::new();

                for hash in hashes.into_iter().skip(start_idx) {
                    if results.len() >= limit {
                        reached_limit = true;
                        break;
                    }

                    if hash.len() < 4 {
                        continue;
                    }

                    if let Ok(summary) = get_latest_summary_in_root(&index_root, &hash).await {
                        let meta: ObjectMetadata = summary.into();

                        if is_legacy_delete_marker(&meta) {
                            continue;
                        }

                        if meta.object_key.starts_with(&prefix) {
                            results.push(meta.clone());

                            last_s1 = hash[0..2].to_string();
                            last_s2 = hash[2..4].to_string();
                            last_fname = format!("{}#{}.bson", hash, meta.version_id);

                            if results.len() >= limit {
                                reached_limit = true;
                                break;
                            }
                        }
                    }
                }

                if !results.is_empty() {
                    let next = if reached_limit
                        && !last_s1.is_empty()
                        && !last_s2.is_empty()
                        && !last_fname.is_empty()
                    {
                        Some(create_continuation_token(&last_s1, &last_s2, &last_fname))
                    } else {
                        None
                    };

                    return Ok(AxumJson(ListObjectsResult {
                        objects: results,
                        continuation_token: next,
                    }));
                }
            }
        }
    }

    let index_root = match pick_active_index_root(&path_manager, &uid, &fragment).await {
        Some(r) => r,
        None => {
            return Ok(AxumJson(ListObjectsResult {
                objects: Vec::new(),
                continuation_token: None,
            }));
        }
    };

    let mut s1_entries = Vec::new();
    let mut entries = fs::read_dir(&index_root)
        .await
        .map_err(|e| AppError::Io(e, "read index".into()))?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        s1_entries.push(entry.file_name().to_string_lossy().to_string());
    }
    s1_entries.sort();

    let s1_start_idx = if !start_s1.is_empty() {
        s1_entries.binary_search(&start_s1).unwrap_or_else(|x| x)
    } else {
        0
    };

    let mut results = Vec::new();
    let mut next_token = None;
    let mut reached_limit = false;

    'outer: for s1 in s1_entries.into_iter().skip(s1_start_idx) {
        let s1_path = index_root.join(&s1);

        let mut s2_entries = Vec::new();
        let mut entries = match fs::read_dir(&s1_path).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            s2_entries.push(entry.file_name().to_string_lossy().to_string());
        }
        s2_entries.sort();

        let s2_start_idx = if s1 == start_s1 && !start_s2.is_empty() {
            s2_entries.binary_search(&start_s2).unwrap_or_else(|x| x)
        } else {
            0
        };

        for s2 in s2_entries.into_iter().skip(s2_start_idx) {
            let s2_path = s1_path.join(&s2);

            let mut file_names = Vec::new();
            let mut entries = match fs::read_dir(&s2_path).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let fname = entry.file_name().to_string_lossy().to_string();
                if fname.ends_with(".bson") && !fname.ends_with(PART_META_SUFFIX) {
                    file_names.push(fname);
                }
            }

            file_names.sort();

            let file_start_idx = if s1 == start_s1 && s2 == start_s2 && !start_file.is_empty() {
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
                    let path = s2_path.join(&fname);
                    async move {
                        let data = fs::read(&path).await;
                        (fname, data)
                    }
                })
                .buffered(20);

            let mut current_hash_prefix = String::new();
            let mut pending_meta: Option<ObjectMetadata> = None;
            let mut last_fname_processed = String::new();

            while let Some((fname, read_result)) = stream.next().await {
                let hash_part = fname.split('#').next().unwrap_or("").to_string();

                if hash_part != current_hash_prefix {
                    if let Some(meta) = pending_meta.take() {
                        if !is_legacy_delete_marker(&meta) {
                            if prefix.is_empty() || meta.object_key.starts_with(&prefix) {
                                results.push(meta);
                            }
                        }

                        if results.len() >= limit {
                            next_token =
                                Some(create_continuation_token(&s1, &s2, &last_fname_processed));
                            reached_limit = true;
                            break 'outer;
                        }
                    }
                    current_hash_prefix = hash_part;
                }

                if let Ok(data) = read_result {
                    if let Ok(summary) = mongodb::bson::from_slice::<ObjectSummary>(&data) {
                        pending_meta = Some(summary.into());
                    }
                }
                last_fname_processed = fname;
            }

            if let Some(meta) = pending_meta {
                if !is_legacy_delete_marker(&meta) {
                    if prefix.is_empty() || meta.object_key.starts_with(&prefix) {
                        results.push(meta);
                    }
                }

                if results.len() >= limit {
                    next_token = Some(create_continuation_token(&s1, &s2, &last_fname_processed));
                    reached_limit = true;
                    break 'outer;
                }
            }
        }
    }

    Ok(AxumJson(ListObjectsResult {
        objects: results,
        continuation_token: if reached_limit { next_token } else { None },
    }))
}

pub async fn list_deleted_objects(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
        .min(1000);

    let mut prefix = params.get("prefix").cloned().unwrap_or_default();
    prefix = normalize_key(&prefix);
    if !prefix.is_empty() {
        validate_reserved_paths(&prefix)?;
    }

    let index_root = match pick_trash_index_root(&path_manager, &uid, &fragment).await {
        Some(r) => r,
        None => {
            return Ok(AxumJson(ListObjectsResult {
                objects: Vec::new(),
                continuation_token: None,
            }));
        }
    };

    let continuation_token = params.get("continuation_token").cloned();
    let (start_s1, start_s2, start_file) = match continuation_token.as_deref() {
        Some(token) => parse_continuation_token(token).unwrap_or_default(),
        None => ("".to_string(), "".to_string(), "".to_string()),
    };

    let mut s1_entries = Vec::new();
    let mut entries = fs::read_dir(&index_root)
        .await
        .map_err(|e| AppError::Io(e, "read trash index".into()))?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        s1_entries.push(entry.file_name().to_string_lossy().to_string());
    }
    s1_entries.sort();

    let s1_start_idx = if !start_s1.is_empty() {
        s1_entries.binary_search(&start_s1).unwrap_or_else(|x| x)
    } else {
        0
    };

    let mut results = Vec::new();
    let mut next_token = None;
    let mut reached_limit = false;

    'outer: for s1 in s1_entries.into_iter().skip(s1_start_idx) {
        let s1_path = index_root.join(&s1);

        let mut s2_entries = Vec::new();
        let mut entries = match fs::read_dir(&s1_path).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            s2_entries.push(entry.file_name().to_string_lossy().to_string());
        }
        s2_entries.sort();

        let s2_start_idx = if s1 == start_s1 && !start_s2.is_empty() {
            s2_entries.binary_search(&start_s2).unwrap_or_else(|x| x)
        } else {
            0
        };

        for s2 in s2_entries.into_iter().skip(s2_start_idx) {
            let s2_path = s1_path.join(&s2);

            let mut file_names = Vec::new();
            let mut entries = match fs::read_dir(&s2_path).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let fname = entry.file_name().to_string_lossy().to_string();
                if fname.ends_with(".bson") && !fname.ends_with(PART_META_SUFFIX) {
                    file_names.push(fname);
                }
            }

            file_names.sort();

            let file_start_idx = if s1 == start_s1 && s2 == start_s2 && !start_file.is_empty() {
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
                    let path = s2_path.join(&fname);
                    async move {
                        let data = fs::read(&path).await;
                        (fname, data)
                    }
                })
                .buffered(20);

            let mut current_hash_prefix = String::new();
            let mut pending_meta: Option<ObjectMetadata> = None;
            let mut last_fname_processed = String::new();

            while let Some((fname, read_result)) = stream.next().await {
                let hash_part = fname.split('#').next().unwrap_or("").to_string();

                if hash_part != current_hash_prefix {
                    if let Some(meta) = pending_meta.take() {
                        if prefix.is_empty() || meta.object_key.starts_with(&prefix) {
                            results.push(meta);
                        }

                        if results.len() >= limit {
                            next_token =
                                Some(create_continuation_token(&s1, &s2, &last_fname_processed));
                            reached_limit = true;
                            break 'outer;
                        }
                    }
                    current_hash_prefix = hash_part;
                }

                if let Ok(data) = read_result {
                    if let Ok(summary) = mongodb::bson::from_slice::<ObjectSummary>(&data) {
                        pending_meta = Some(summary.into());
                    }
                }
                last_fname_processed = fname;
            }

            if let Some(meta) = pending_meta {
                if prefix.is_empty() || meta.object_key.starts_with(&prefix) {
                    results.push(meta);
                }

                if results.len() >= limit {
                    next_token = Some(create_continuation_token(&s1, &s2, &last_fname_processed));
                    reached_limit = true;
                    break 'outer;
                }
            }
        }
    }

    Ok(AxumJson(ListObjectsResult {
        objects: results,
        continuation_token: if reached_limit { next_token } else { None },
    }))
}

pub async fn list_object_versions(
    claims: Claims,
    Path((fragment, object_key_raw)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let path_manager = ObjectPathManager::new();
    let hash = hash_key(&object_key);

    let shard1 = &hash[0..2];
    let shard2 = &hash[2..4];

    let mut active_shard_dir = None;
    for dir in path_manager.get_all_index_dirs(&uid, &fragment) {
        let candidate = dir.join(shard1).join(shard2);
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
        if name.starts_with(&prefix) && name.ends_with(".bson") {
            if let Ok(data) = fs::read(entry.path()).await {
                if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                    if !is_legacy_delete_marker(&meta) {
                        versions.push(meta);
                    }
                }
            }
        }
    }

    versions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Ok(AxumJson(versions))
}

pub async fn list_deleted_object_versions(
    claims: Claims,
    Path((fragment, object_key_raw)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let path_manager = ObjectPathManager::new();
    let hash = hash_key(&object_key);

    let shard1 = &hash[0..2];
    let shard2 = &hash[2..4];

    let mut trash_shard_dir = None;
    for user_dir in path_manager.get_all_user_index_dirs(&uid) {
        let candidate = user_dir
            .join(&fragment)
            .join(TRASH_DIR)
            .join("index")
            .join(shard1)
            .join(shard2);
        if fs::try_exists(&candidate).await.unwrap_or(false) {
            trash_shard_dir = Some(candidate);
            break;
        }
    }

    let shard_dir = trash_shard_dir.ok_or(AppError::NotFound)?;
    let prefix = format!("{}#", hash);

    let mut versions = Vec::new();

    let mut entries = fs::read_dir(&shard_dir)
        .await
        .map_err(|e| AppError::Io(e, "read trash shard".into()))?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && name.ends_with(".bson") {
            if let Ok(data) = fs::read(entry.path()).await {
                if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                    versions.push(meta);
                }
            }
        }
    }

    versions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Ok(AxumJson(versions))
}

pub async fn list_fragments(claims: Claims) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let user_storage_roots = path_manager.get_all_user_index_dirs(&uid);

    let fragments = tokio::task::spawn_blocking(move || -> Result<Vec<String>, std::io::Error> {
        let mut active_root = None;
        for root in user_storage_roots {
            if root.exists() {
                active_root = Some(root);
                break;
            }
        }

        let user_storage_root = match active_root {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };

        let mut fragments = Vec::new();

        for entry in WalkDir::new(&user_storage_root)
            .min_depth(1)
            .max_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    fragments.push(name.to_string());
                }
            }
        }

        fragments.sort();
        Ok(fragments)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))?
    .map_err(|e| AppError::Io(e, "read user storage root".into()))?;

    Ok(AxumJson(fragments))
}

pub async fn get_object_metadata(
    claims: Claims,
    Path((fragment, object_key_raw)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let metadata = get_metadata(&fragment, &object_key, &claims.uid).await?;
    if is_legacy_delete_marker(&metadata) {
        return Err(AppError::NotFound);
    }

    Ok(AxumJson(metadata))
}

pub async fn get_deleted_object_metadata(
    claims: Claims,
    Path((fragment, object_key_raw)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let path_manager = ObjectPathManager::new();
    let hash = hash_key(&object_key);

    let index_root = pick_trash_index_root(&path_manager, &uid, &fragment)
        .await
        .ok_or(AppError::NotFound)?;

    let summary = get_latest_summary_in_root(&index_root, &hash)
        .await
        .map_err(|_| AppError::NotFound)?;
    let meta: ObjectMetadata = summary.into();

    Ok(AxumJson(meta))
}

pub async fn get_object(
    claims: Claims,
    Path((fragment, object_key_raw)): Path<(String, String)>,
    req: axum::http::Request<Body>,
) -> Result<Response, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let metadata = get_metadata(&fragment, &object_key, &uid).await?;
    if is_legacy_delete_marker(&metadata) {
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
    Path((fragment, object_key_raw)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = normalize_key(&object_key_raw);
    validate_reserved_paths(&object_key)?;

    let metadata = get_metadata(&fragment, &object_key, &uid).await?;
    if is_legacy_delete_marker(&metadata) {
        return Err(AppError::NotFound);
    }

    stream_object_logic(headers, metadata, "inline").await
}
