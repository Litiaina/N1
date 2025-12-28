use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::{get_metadata_with_cache, get_object_keys_with_cache};
use crate::object_storage::constants::{BROWSER_CACHE_MAX_AGE, OBJECT_DATA_ROOT};
use crate::object_storage::object_parser::{get_object_logic, stream_object_logic};
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use axum::{
    Json as AxumJson,
    body::Body,
    extract::{Path, Query},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tokio::fs;
use walkdir::WalkDir;

pub async fn list_objects(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let keys_arc = get_object_keys_with_cache(&uid, &fragment).await?;
    let keys = keys_arc.as_ref(); 

    let start_index = if let Some(marker) = params.get("marker") {
        match keys.binary_search(marker) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    } else {
        0
    };

    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
        .min(1000);

    let target_keys = keys
        .iter()
        .skip(start_index)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();

    let include_deleted = params.get("include_deleted").map(|v| v == "true").unwrap_or(false);

    let mut objects = stream::iter(target_keys)
        .map(|key| {
            let frag = &fragment;
            let u = &uid;
            async move { get_metadata_with_cache(&frag, &key, &u).await }
        })
        .buffer_unordered(64)
        .filter_map(|res| async move {
            match res {
                Ok(meta) => {
                    if !include_deleted && meta.is_delete_marker {
                        None
                    } else {
                        Some(meta)
                    }
                }
                Err(_) => None,
            }
        })
        .collect::<Vec<_>>()
        .await;

    objects.sort_by(|a, b| a.object_key.cmp(&b.object_key));

    Ok(AxumJson(objects))
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
