use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::{error, info, warn};

use crate::{
    enums::app_error_enum::AppError,
    middleware::auth_middleware::MASTER_KEY,
    core::{
        disk_manager::ObjectPathManager,
        fragment_metadata::{
            init_fragment_metadata, read_fragment_metadata, rotate_fragment_key,
            write_fragment_metadata,
        },
        sync::{CreateFragmentRequest, CreateFragmentResponse, ensure_fragment_blob_directories},
    },
    routes::storage_routes::AppState,
    structs::auth_structs::Claims,
    validation::fragment_validation::does_fragment_exist,
    websocket::ws_structs::StorageEventPayload,
};

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

pub async fn list_public_fragments() -> Result<impl IntoResponse, AppError> {
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    let fragments = tokio::task::spawn_blocking(move || -> Result<Vec<String>, std::io::Error> {
        let mut fragment_hashes = std::collections::HashSet::new();

        for root in index_roots {
            if !root.exists() {
                continue;
            }

            if let Ok(entries) = std::fs::read_dir(root) {
                for entry in entries.filter_map(|e| e.ok()) {
                    if let Ok(file_type) = entry.file_type() {
                        if file_type.is_dir() {
                            if let Some(name) = entry.file_name().to_str() {
                                if is_fragment_hash(name) {
                                    fragment_hashes.insert(name.to_string());
                                }
                            }
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

    if fragments.is_empty() {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "message": "No fragments found",
                "fragments": []
            })),
        ));
    }

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "fragments": fragments
        })),
    ))
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

    if does_fragment_exist(&hash).await {
        return Err(AppError::Conflict(format!(
            "Fragment already exists: {}",
            hash
        )));
    }

    ensure_fragment_blob_directories(&hash).await;
    let _ = init_fragment_metadata(&hash, &payload.key).await?;

    info!(
        "Created new fragment '{}' with hash: {}",
        payload.name, hash
    );

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

            let mut meta = match read_fragment_metadata(&new_hash).await {
                Ok(m) => m,
                Err(_) => {
                    let old_meta = read_fragment_metadata(&current_hash).await.ok();
                    old_meta.unwrap_or_else(|| {
                        crate::core::fragment_metadata::FragmentMetadata {
                            fragment: new_hash.clone(),
                            key_phc: String::new(),
                            nt_hash_hex: None,
                            created: chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                            updated: None,
                        }
                    })
                }
            };

            meta.fragment = new_hash.clone();
            write_fragment_metadata(&meta).await.ok();

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
        state.emit(&current_hash, StorageEventPayload::StateInvalidated);
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
