use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::time::timeout;
use tracing::{error, info, warn};

use crate::{
    core::{
        disk_manager::ObjectPathManager,
        edge_index_store::{
            active_fragment_quota_bytes, ensure_fragment_owner_partition_available_strict,
            ensure_fragment_quota_project_id, has_any_fragment_edge_index, set_fragment_quota,
        },
        fragment_metadata::{
            init_fragment_metadata_strict, read_fragment_metadata, rotate_fragment_key,
            write_fragment_metadata,
        },
        quota_manager::{
            apply_fragment_project_quota, fragment_quota_enforcement_enabled, get_storage_usage,
        },
        sync::{
            CreateFragmentRequest, CreateFragmentResponse, ensure_fragment_blob_directories_strict,
        },
    },
    enums::app_error_enum::AppError,
    middleware::auth_middleware::MASTER_KEY,
    namespace::runtime::global_namespace_runtime,
    routes::storage_routes::AppState,
    structs::auth_structs::Claims,
    validation::fragment_validation::does_fragment_exist,
    websocket::ws_structs::StorageEventPayload,
};

const FRAGMENT_CREATE_IO_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
pub struct UpdateFragmentRequest {
    pub new_name: Option<String>,
    pub new_key: Option<String>,
    pub secret: Option<String>,
}

#[derive(Serialize)]
pub struct UpdateFragmentResponse {
    pub fragment_hash: String,
    pub name: Option<String>,
    pub message: String,
}

pub fn is_fragment_hash(name: &str) -> bool {
    if name.len() != 64 {
        return false;
    }
    name.as_bytes()
        .iter()
        .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F'))
}

async fn prepare_fragment_index_roots_strict(fragment: &str) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    if index_roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "no index roots configured for fragment provisioning".into(),
        ));
    }

    for root in index_roots {
        let index_dir = root.join(fragment);
        timeout(FRAGMENT_CREATE_IO_TIMEOUT, fs::create_dir_all(&index_dir))
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable(format!(
                    "timed out preparing index root {}",
                    index_dir.display()
                ))
            })?
            .map_err(|e| {
                AppError::ServiceUnavailable(format!(
                    "failed to prepare index root {}: {}",
                    index_dir.display(),
                    e
                ))
            })?;

        let metadata = timeout(FRAGMENT_CREATE_IO_TIMEOUT, fs::metadata(&index_dir))
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable(format!(
                    "timed out verifying index root {}",
                    index_dir.display()
                ))
            })?
            .map_err(|e| {
                AppError::ServiceUnavailable(format!(
                    "failed to verify index root {}: {}",
                    index_dir.display(),
                    e
                ))
            })?;

        if !metadata.is_dir() {
            return Err(AppError::ServiceUnavailable(format!(
                "index root {} is not a directory",
                index_dir.display()
            )));
        }
    }

    Ok(())
}

async fn enforce_fragment_quota_request(requested_quota: u64) -> Result<(), AppError> {
    if requested_quota == 0 {
        return Ok(());
    }
    let usage = get_storage_usage().await?;
    let active_quota = tokio::task::spawn_blocking(active_fragment_quota_bytes)
        .await
        .map_err(|err| AppError::InternalServerError(format!("quota scan task: {err}")))??;
    if active_quota.saturating_add(requested_quota) > usage.capacity_allocated {
        return Err(AppError::InsufficientStorage);
    }
    Ok(())
}

pub async fn create_fragment(
    Json(payload): Json<CreateFragmentRequest>,
) -> Result<impl IntoResponse, AppError> {
    if payload.secret != *MASTER_KEY {
        return Err(AppError::Unauthorized);
    }

    if payload.key.is_empty() {
        return Err(AppError::BadRequest("key must not be empty".into()));
    }

    let mut hasher = Sha256::new();
    hasher.update(payload.name.as_bytes());
    let hash = hex::encode(hasher.finalize());

    global_namespace_runtime().evict_fragment(&hash);

    let metadata_exists = read_fragment_metadata(&hash).await.is_ok();
    let edge_index_exists = has_any_fragment_edge_index(&hash)?;
    if metadata_exists && edge_index_exists {
        return Err(AppError::Conflict(format!(
            "Fragment already exists: {}",
            hash
        )));
    }

    let requested_quota = payload.quota_bytes.ok_or_else(|| {
        AppError::BadRequest(
            "quota_bytes is required when creating a fragment; fragments are quota-bound virtual partitions"
                .to_string(),
        )
    })?;
    if requested_quota == 0 {
        return Err(AppError::BadRequest(
            "quota_bytes must be greater than 0".to_string(),
        ));
    }
    enforce_fragment_quota_request(requested_quota).await?;

    prepare_fragment_index_roots_strict(&hash).await?;
    ensure_fragment_blob_directories_strict(&hash).await?;
    ensure_fragment_owner_partition_available_strict(&hash)?;
    let quota_project_id = ensure_fragment_quota_project_id(&hash)?;
    apply_fragment_project_quota(&hash, requested_quota, quota_project_id).await?;
    set_fragment_quota(&hash, requested_quota)?;
    if !metadata_exists {
        let _ = init_fragment_metadata_strict(&hash, &payload.key).await?;
    }

    if metadata_exists || edge_index_exists {
        info!(
            "Recovered partial fragment '{}' with hash: {}",
            payload.name, hash
        );
    } else {
        info!(
            "Created new fragment '{}' with hash: {}",
            payload.name, hash
        );
    }
    if fragment_quota_enforcement_enabled() {
        info!(
            "Applied filesystem project quota for fragment {} with project_id={} quota_bytes={}",
            hash, quota_project_id, requested_quota
        );
    }

    Ok((
        StatusCode::CREATED,
        Json(CreateFragmentResponse {
            fragment_hash: hash,
            name: payload.name,
        }),
    ))
}

pub async fn update_fragment(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment_hash): Path<String>,
    Json(payload): Json<UpdateFragmentRequest>,
) -> Result<impl IntoResponse, AppError> {
    let is_admin = payload
        .secret
        .as_ref()
        .map(|s| s == &*MASTER_KEY)
        .unwrap_or(false);

    if !is_admin && claims.fragment != fragment_hash {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    if payload.new_name.is_some() && !is_admin {
        return Err(AppError::Forbidden(
            "Master Secret required to rename fragment".into(),
        ));
    }

    let wants_key = payload.new_key.is_some();

    if wants_key && !is_admin && !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required to update fragment settings".into(),
        ));
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    let mut current_hash = fragment_hash.clone();
    let mut new_name_display = None;

    if let Some(new_name) = &payload.new_name {
        let mut hasher = Sha256::new();
        hasher.update(new_name.as_bytes());
        let new_hash = hex::encode(hasher.finalize());

        if new_hash != current_hash {
            let existing_meta = read_fragment_metadata(&current_hash).await.ok();

            for root in &index_roots {
                if root.join(&new_hash).exists() {
                    return Err(AppError::Conflict(format!(
                        "Fragment '{}' (Hash: {}) already exists",
                        new_name, new_hash
                    )));
                }
            }

            for root in &index_roots {
                let src = root.join(&current_hash);
                let dst = root.join(&new_hash);
                if src.exists() {
                    if let Err(e) = fs::rename(&src, &dst).await {
                        error!("Failed to rename index root {:?} -> {:?}: {}", src, dst, e);
                        return Err(AppError::InternalServerError(
                            "Failed to rename fragment on disk".into(),
                        ));
                    }
                }
            }

            for root in &blob_roots {
                let src = root.join(&current_hash);
                let dst = root.join(&new_hash);
                if src.exists() {
                    if let Err(e) = fs::rename(&src, &dst).await {
                        error!("Failed to rename blob root {:?} -> {:?}: {}", src, dst, e);
                    }
                }
            }

            let mut meta =
                existing_meta.unwrap_or_else(|| crate::core::fragment_metadata::FragmentMetadata {
                    fragment: new_hash.clone(),
                    key_phc: String::new(),
                    nt_hash_hex: None,
                    index_partition_count:
                        crate::core::fragment_metadata::configured_index_partition_count(),
                    created: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    updated: None,
                });

            meta.fragment = new_hash.clone();
            write_fragment_metadata(&meta).await?;

            info!(
                "Renamed fragment {} -> {} ({})",
                current_hash, new_name, new_hash
            );

            state.emit(
                &current_hash,
                StorageEventPayload::FragmentDeleted {
                    fragment: current_hash.clone(),
                },
            );

            current_hash = new_hash;
            new_name_display = Some(new_name.clone());
        }
    }

    if let Some(new_key) = payload.new_key {
        rotate_fragment_key(&current_hash, &new_key).await?;
    }

    Ok((
        StatusCode::OK,
        Json(UpdateFragmentResponse {
            fragment_hash: current_hash,
            name: new_name_display,
            message: "Fragment updated successfully".to_string(),
        }),
    ))
}

pub async fn delete_fragment(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required to delete fragment".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();
    let fragment_owned = fragment.clone();

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        global_namespace_runtime().evict_fragment(&fragment_owned);

        for root in index_roots.iter() {
            let fragment_path = root.join(&fragment_owned);
            if fragment_path.exists() {
                if let Err(e) = std::fs::remove_dir_all(&fragment_path) {
                    warn!("Failed to delete fragment index {:?}: {}", fragment_path, e);
                }
            }
        }

        for root in blob_roots.iter() {
            let fragment_path = root.join(&fragment_owned);
            if fragment_path.exists() {
                if let Err(e) = std::fs::remove_dir_all(&fragment_path) {
                    warn!(
                        "Failed to delete fragment blob dir {:?}: {}",
                        fragment_path, e
                    );
                }
            }
        }

        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))??;

    info!("Deleted fragment '{}'", fragment);

    state.emit(
        &fragment,
        StorageEventPayload::FragmentDeleted {
            fragment: fragment.clone(),
        },
    );

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "success",
            "message": "Fragment deleted successfully"
        })),
    ))
}

#[derive(Deserialize)]
pub struct FragmentCheckRequest {
    pub fragment: String,
}

pub async fn get_fragment_hash(
    Json(request): Json<FragmentCheckRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mut hasher = Sha256::new();
    hasher.update(&request.fragment.as_bytes());
    let hash = hex::encode(hasher.finalize());

    if !does_fragment_exist(&hash).await {
        return Err(AppError::NotFound);
    };

    Ok((
        StatusCode::OK,
        Json(serde_json::json!(
            {
                "fragment": format!("{}", &request.fragment),
                "fragment_hash": hash
            }
        )),
    ))
}

pub async fn verify_fragment_existence(
    Json(request): Json<FragmentCheckRequest>,
) -> Result<impl IntoResponse, AppError> {
    if does_fragment_exist(&request.fragment).await {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({ "fragment": &request.fragment })),
        ));
    }

    Err(AppError::NotFound)
}
