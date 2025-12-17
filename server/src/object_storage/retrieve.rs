use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::get_metadata_with_cache;
use crate::object_storage::constants::{BROWSER_CACHE_MAX_AGE, OBJECT_DATA_ROOT};
use crate::object_storage::object_parser::{get_object_logic, stream_object_logic};
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::ObjectPathManager;
use axum::{
    Json as AxumJson,
    body::Body,
    extract::{Path, Query},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, stream};
use std::collections::HashMap;
use walkdir::WalkDir;

pub async fn list_objects(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let fragment_dir = path_manager
        .root
        .join(OBJECT_DATA_ROOT)
        .join(&uid)
        .join(&fragment);

    let keys = tokio::task::spawn_blocking(move || -> Result<Vec<String>, std::io::Error> {
        if !fragment_dir.exists() {
            return Ok(Vec::new());
        }

        let mut collected_keys = Vec::with_capacity(1000);

        for entry in WalkDir::new(&fragment_dir)
            .min_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_dir() {
                if let Ok(relative) = entry.path().strip_prefix(&fragment_dir) {
                    let key = relative.to_string_lossy().replace("\\", "/");
                    collected_keys.push(key);
                }
            }
        }

        collected_keys.sort();
        Ok(collected_keys)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))?
    .map_err(|e| AppError::Io(e, "list objects scan".into()))?;

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
        .unwrap_or(100);
    let limit = limit.min(1000);

    let target_keys = keys
        .into_iter()
        .skip(start_index)
        .take(limit)
        .collect::<Vec<_>>();

    let objects = stream::iter(target_keys)
        .map(|key| {
            let frag = fragment.clone();
            let u = uid.clone();
            async move { get_metadata_with_cache(&frag, &key, &u).await }
        })
        .buffer_unordered(64)
        .filter_map(|res| async { res.ok() })
        .collect::<Vec<_>>()
        .await;

    Ok(AxumJson(objects))
}

pub async fn list_fragments(
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
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

    Ok(AxumJson(metadata))
}

pub async fn get_object(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    req: axum::http::Request<Body>,
) -> Result<Response, AppError> {
    let uid = claims.uid;

    let metadata = get_metadata_with_cache(&fragment, &object_key, &uid).await?;

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

    stream_object_logic(headers, metadata).await
}
