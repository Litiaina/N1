use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::get_metadata_with_cache;
use crate::object_storage::object_parser::{get_object_logic, stream_object_logic};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::ObjectPathManager;
use axum::{
    Json as AxumJson,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
};
use futures_util::future::join_all;
use std::collections::HashMap;
use std::sync::Arc;

const BROWSER_CACHE_MAX_AGE: u64 = 3600;

pub async fn list_objects(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let fragment_dir = path_manager.root.join("storage").join(&uid).join(&fragment);

    let keys = tokio::task::spawn_blocking(move || -> Result<Vec<String>, std::io::Error> {
        if !fragment_dir.exists() {
            return Ok(Vec::new());
        }

        let mut collected_keys = Vec::with_capacity(1000);
        let mut stack = vec![(fragment_dir.clone(), String::new())];

        while let Some((current_dir, prefix)) = stack.pop() {
            let entries = match std::fs::read_dir(&current_dir) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for entry in entries.flatten() {
                let file_type = match entry.file_type() {
                    Ok(ft) => ft,
                    Err(_) => continue,
                };

                if file_type.is_dir() {
                    let name = entry.file_name().to_string_lossy().to_string();

                    let new_prefix = if prefix.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", prefix, name)
                    };
                    collected_keys.push(new_prefix.clone());

                    stack.push((entry.path(), new_prefix));
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

    let mut tasks = Vec::with_capacity(target_keys.len());

    for key in target_keys {
        let frag = fragment.clone();
        let u = uid.clone();
        tasks.push(async move { get_metadata_with_cache(&frag, &key, &u).await });
    }

    let results = join_all(tasks).await;
    let mut objects = Vec::with_capacity(results.len());

    for res in results {
        if let Ok(meta) = res {
            objects.push(meta);
        }
    }

    Ok(AxumJson(objects))
}

pub async fn get_object_metadata(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let metadata = get_metadata_with_cache(&fragment, &object_key, &claims.uid).await?;
    Ok(AxumJson(metadata))
}

pub async fn get_object(
    State(_state): State<Arc<AppState>>,
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
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    headers: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;

    let metadata = get_metadata_with_cache(&fragment, &object_key, &uid).await?;

    stream_object_logic(headers, metadata).await
}
