use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::get_metadata_with_cache;
use crate::object_storage::constants::{BROWSER_CACHE_MAX_AGE, OBJECT_DATA_ROOT};
use crate::object_storage::object_parser::{get_object_logic, stream_object_logic};
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ListObjectsResult, ObjectMetadata, ObjectPathManager, ObjectSummary};
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

fn parse_continuation_token(token: &str) -> Option<(String, String, String)> {
    let bytes = general_purpose::STANDARD.decode(token).ok()?;
    let s = String::from_utf8(bytes).ok()?;
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 3 {
        Some((parts[0].to_string(), parts[1].to_string(), parts[2].to_string()))
    } else {
        None
    }
}

fn create_continuation_token(shard1: &str, shard2: &str, filename: &str) -> String {
    general_purpose::STANDARD.encode(format!("{}/{}/{}", shard1, shard2, filename))
}

pub async fn list_objects(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let index_root = path_manager.get_index_dir(&uid, &fragment);

    let limit: usize = params.get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
        .min(1000);
        
    let include_deleted = params.get("include_deleted")
        .map(|v| v == "true")
        .unwrap_or(false);

    let continuation_token = params.get("continuation_token").cloned();
    let (start_s1, start_s2, start_file) = match continuation_token.as_deref() {
        Some(token) => parse_continuation_token(token).unwrap_or_default(),
        None => ("".to_string(), "".to_string(), "".to_string()),
    };

    if !fs::try_exists(&index_root).await.unwrap_or(false) {
        return Ok(AxumJson(ListObjectsResult { objects: Vec::new(), continuation_token: None }));
    }

    let mut s1_entries = Vec::new();
    let mut entries = fs::read_dir(&index_root).await.map_err(|e| AppError::Io(e, "read index".into()))?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        s1_entries.push(entry.file_name().to_string_lossy().to_string());
    }
    s1_entries.sort();

    let s1_start_idx = if !start_s1.is_empty() {
        s1_entries.binary_search(&start_s1).unwrap_or_else(|x| x)
    } else { 0 };

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
        } else { 0 };

        for s2 in s2_entries.into_iter().skip(s2_start_idx) {
            let s2_path = s1_path.join(&s2);
            
            let mut file_names = Vec::new();
            let mut entries = match fs::read_dir(&s2_path).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let fname = entry.file_name().to_string_lossy().to_string();
                if fname.ends_with(".bson") {
                    file_names.push(fname);
                }
            }

            file_names.sort();

            let file_start_idx = if s1 == start_s1 && s2 == start_s2 && !start_file.is_empty() {
                match file_names.binary_search(&start_file) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                }
            } else { 0 };

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
                        if !meta.is_delete_marker || include_deleted {
                            results.push(meta);
                        }
                        
                        if results.len() >= limit {
                            next_token = Some(create_continuation_token(&s1, &s2, &last_fname_processed));
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
                 if !meta.is_delete_marker || include_deleted {
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
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    
    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let hash = hex::encode(hasher.finalize());
    
    let shard1 = &hash[0..2];
    let shard2 = &hash[2..4];
    
    let shard_dir = path_manager.get_index_dir(&uid, &fragment)
        .join(shard1)
        .join(shard2);

    if !fs::try_exists(&shard_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

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
    let user_storage_root = path_manager.root.join(OBJECT_DATA_ROOT).join(&uid);

    let fragments = tokio::task::spawn_blocking(move || -> Result<Vec<String>, std::io::Error> {
        if !user_storage_root.exists() {
            return Ok(Vec::new());
        }

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
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let metadata = get_metadata_with_cache(&fragment, &object_key, &claims.uid).await?;

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
    let uid = claims.uid;

    let metadata = get_metadata_with_cache(&fragment, &object_key, &uid).await?;

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
    let uid = claims.uid;
    let metadata = get_metadata_with_cache(&fragment, &object_key, &uid).await?;

    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    stream_object_logic(headers, metadata, "inline").await
}
