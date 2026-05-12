use crate::core::constants::BROWSER_CACHE_MAX_AGE;
use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata, ObjectPathManager};
use crate::core::fragment_manager::is_fragment_hash;
use crate::core::metadata_parser::get_metadata;
use crate::core::object_parser::{get_object_logic, stream_object_logic};
use crate::core::quota_manager::get_storage_usage;
use crate::core::{edge_index_store, index};
use crate::enums::app_error_enum::AppError;
use crate::namespace::runtime::{global_namespace_runtime, logical_not_found};
use crate::structs::auth_structs::Claims;
use axum::{
    Json as AxumJson,
    body::Body,
    extract::{Path, Query},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use walkdir::WalkDir;

async fn list_objects_flat_page(
    fragment: &str,
    scan_trash: bool,
    _prefix: Option<String>,
    _delimiter: Option<String>,
    limit: usize,
    _continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    let mut params = HashMap::new();
    params.insert("limit".to_string(), limit.to_string());
    if let Some(prefix) = _prefix {
        params.insert("prefix".to_string(), prefix);
    }
    if let Some(delimiter) = _delimiter {
        params.insert("delimiter".to_string(), delimiter);
    }
    if let Some(token) = _continuation_token {
        params.insert("continuation_token".to_string(), token);
    }
    if _include_internal {
        params.insert("include_internal".to_string(), "true".to_string());
    }
    global_namespace_runtime()
        .list_objects_page(fragment, &params, scan_trash)
        .await
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
    let prefix = params.get("prefix").cloned();
    let delimiter = params.get("delimiter").cloned();
    let include_internal = params
        .get("include_internal")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);

    list_objects_flat_page(
        &fragment,
        scan_trash,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
    )
    .await
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

    let physical_object_id = resolve_read_object_id(&fragment, &object_key, false).await?;
    let versions = index::list_live_object_versions_metadata(&fragment, &physical_object_id)
        .await?
        .into_iter()
        .map(|mut metadata| {
            metadata.object_key = object_key.clone();
            metadata.without_internal_layout()
        })
        .collect::<Vec<_>>();

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
        get_object_metadata_direct(&fragment, &object_key)
            .await?
            .without_internal_layout(),
    ))
}

pub async fn get_object_metadata_direct(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let physical_object_id = resolve_read_object_id(fragment, object_key, false).await?;
    let mut metadata = get_metadata(fragment, &physical_object_id, fragment).await?;
    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }
    metadata.object_key = object_key.trim_start_matches('/').to_string();
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

    let metadata = get_object_metadata_direct(&fragment, &object_key).await?;

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

    let metadata = get_object_metadata_direct(&fragment, &object_key).await?;

    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    stream_object_logic(headers, metadata, "inline", None).await
}

async fn resolve_read_object_id(
    fragment: &str,
    object_key: &str,
    include_trash: bool,
) -> Result<String, AppError> {
    let normalized = object_key.trim_start_matches('/').trim().to_string();
    let resolved = if include_trash {
        global_namespace_runtime()
            .resolve_any_object_id(fragment, &normalized)
            .await?
    } else {
        global_namespace_runtime()
            .resolve_live_object_id(fragment, &normalized)
            .await?
    };
    match resolved {
        Some(object_id) => Ok(object_id),
        None => edge_index_store::normalize_object_id(&normalized).map_err(|_| logical_not_found()),
    }
}
