use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::info;

use crate::{
    core::{
        fragment_metadata::{init_fragment_metadata, rotate_fragment_key},
        quota_manager::init_quota_file,
        raw_index,
        sync::{CreateFragmentRequest, CreateFragmentResponse, ensure_fragment_blob_directories},
    },
    enums::app_error_enum::AppError,
    middleware::auth_middleware::MASTER_KEY,
    routes::storage_routes::AppState,
    structs::auth_structs::Claims,
    validation::fragment_validation::does_fragment_exist,
    websocket::ws_structs::StorageEventPayload,
};

#[derive(Deserialize)]
pub struct UpdateFragmentRequest {
    pub new_name: Option<String>,
    pub new_quota: Option<u64>,
    pub new_key: Option<String>,
    pub secret: Option<String>,
}

#[derive(Serialize)]
pub struct UpdateFragmentResponse {
    pub fragment_hash: String,
    pub name: Option<String>,
    pub message: String,
}

pub async fn list_public_fragments() -> Result<impl IntoResponse, AppError> {
    let mut fragments = raw_index::list_fragments().await?;
    fragments.sort();
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

    init_quota_file(&hash, payload.quota).await?;
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
            quota: payload.quota,
        }),
    ))
}

pub async fn update_fragment(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment_hash): Path<String>,
    Json(payload): Json<UpdateFragmentRequest>,
) -> Result<impl IntoResponse, AppError> {
    if payload.new_name.is_some() {
        return Err(AppError::BadRequest(
            "fragment rename is not supported in Arc raw-index mode".into(),
        ));
    }

    let is_admin = payload
        .secret
        .as_ref()
        .map(|s| s == &*MASTER_KEY)
        .unwrap_or(false);
    if !is_admin && claims.fragment != fragment_hash {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let wants_quota = payload.new_quota.is_some();
    let wants_key = payload.new_key.is_some();
    if (wants_quota || wants_key) && !is_admin && !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required to update fragment settings".into(),
        ));
    }

    if let Some(quota) = payload.new_quota {
        init_quota_file(&fragment_hash, quota).await?;
        state.emit(&fragment_hash, StorageEventPayload::StateInvalidated);
    }
    if let Some(new_key) = payload.new_key {
        rotate_fragment_key(&fragment_hash, &new_key).await?;
        state.emit(&fragment_hash, StorageEventPayload::StateInvalidated);
    }

    Ok((
        StatusCode::OK,
        Json(UpdateFragmentResponse {
            fragment_hash,
            name: None,
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

    raw_index::delete_fragment_state(&fragment).await?;
    info!("Deleted fragment '{}' from raw index state", fragment);
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
