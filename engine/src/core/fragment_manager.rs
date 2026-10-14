use std::collections::BTreeSet;
use std::path::PathBuf;
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
use tracing::info;

use crate::{
    core::{
        disk_manager::ObjectPathManager,
        fragment_identity::{
            ensure_fragment_identity_on_roots, fragment_alias_path, fragment_root_on,
            new_stable_fragment_id, remove_fragment_aliases_for_stable_id,
        },
        fragment_metadata::{
            init_fragment_metadata_strict, read_fragment_metadata, resolve_stable_fragment_id,
            rotate_fragment_key,
        },
        index_store::{ensure_fragment_owner_partition_available_strict, has_any_fragment_index},
        operation_gate::ensure_user_mutations_allowed,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeleteFragmentRequest {
    pub secret: String,
    pub confirm_fragment: String,
}

#[derive(Serialize)]
pub struct UpdateFragmentResponse {
    pub fragment_hash: String,
    pub name: Option<String>,
    pub message: String,
}

fn validate_delete_fragment_request(
    claims: &Claims,
    stable_fragment_id: &str,
    requested_fragment: &str,
    payload: &DeleteFragmentRequest,
    master_key: &str,
) -> Result<(), AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden(
            "Delete permission required to delete fragment".into(),
        ));
    }
    if claims.fragment != stable_fragment_id {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    if payload.secret != master_key {
        return Err(AppError::Unauthorized);
    }
    if payload.confirm_fragment != requested_fragment
        && payload.confirm_fragment != stable_fragment_id
    {
        return Err(AppError::BadRequest(
            "confirm_fragment must exactly match the fragment in the URL or the stable fragment id"
                .into(),
        ));
    }
    Ok(())
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
        let index_dir = fragment_root_on(&root, fragment);
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

pub async fn create_fragment(
    Json(payload): Json<CreateFragmentRequest>,
) -> Result<axum::response::Response, AppError> {
    // The owned task retains admission even if the requesting client leaves.
    tokio::spawn(async move {
        let _boundary = crate::core::backup::barrier::mutation_async().await;
        create_fragment_inner(payload)
            .await
            .map(IntoResponse::into_response)
    })
    .await
    .map_err(|e| AppError::InternalServerError(e.to_string()))?
}

async fn create_fragment_inner(
    payload: CreateFragmentRequest,
) -> Result<impl IntoResponse, AppError> {
    ensure_user_mutations_allowed()?;
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

    let existing_meta = read_fragment_metadata(&hash).await.ok();
    let metadata_exists = existing_meta.is_some();
    let index_exists = has_any_fragment_index(&hash)?;
    if metadata_exists && index_exists {
        return Err(AppError::Conflict(format!(
            "Fragment already exists: {}",
            hash
        )));
    }

    let path_manager = ObjectPathManager::new();
    let identity_roots = path_manager.get_all_index_roots();
    let stable_fragment_id = existing_meta
        .as_ref()
        .map(|meta| meta.stable_fragment_id.clone())
        .unwrap_or_else(new_stable_fragment_id);
    ensure_fragment_identity_on_roots(&identity_roots, &hash, &stable_fragment_id).await?;

    prepare_fragment_index_roots_strict(&hash).await?;
    ensure_fragment_blob_directories_strict(&hash).await?;
    ensure_fragment_owner_partition_available_strict(&hash)?;
    if !metadata_exists {
        let _ = init_fragment_metadata_strict(&stable_fragment_id, &payload.key).await?;
    }

    if metadata_exists || index_exists {
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
) -> Result<axum::response::Response, AppError> {
    tokio::spawn(async move {
        let _boundary = crate::core::backup::barrier::mutation_async().await;
        update_fragment_inner(state, claims, fragment_hash, payload)
            .await
            .map(IntoResponse::into_response)
    })
    .await
    .map_err(|e| AppError::InternalServerError(e.to_string()))?
}

async fn update_fragment_inner(
    state: Arc<AppState>,
    claims: Claims,
    fragment_hash: String,
    payload: UpdateFragmentRequest,
) -> Result<impl IntoResponse, AppError> {
    ensure_user_mutations_allowed()?;
    let is_admin = payload
        .secret
        .as_ref()
        .map(|s| s == &*MASTER_KEY)
        .unwrap_or(false);

    let current_stable = resolve_stable_fragment_id(&fragment_hash).await?;

    if !is_admin && claims.fragment != current_stable {
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

    let mut current_hash = fragment_hash.clone();
    let mut new_name_display = None;

    if let Some(new_name) = &payload.new_name {
        let mut hasher = Sha256::new();
        hasher.update(new_name.as_bytes());
        let new_hash = hex::encode(hasher.finalize());

        if new_hash != current_hash {
            let existing_meta = read_fragment_metadata(&current_hash).await.ok();
            let stable_fragment_id = existing_meta
                .as_ref()
                .map(|meta| meta.stable_fragment_id.clone())
                .ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "fragment {} has no stable fragment identity",
                        current_hash
                    ))
                })?;

            for root in &index_roots {
                if fragment_alias_path(root, &new_hash).exists() {
                    return Err(AppError::Conflict(format!(
                        "Fragment '{}' (Hash: {}) already exists",
                        new_name, new_hash
                    )));
                }
            }

            ensure_fragment_identity_on_roots(&index_roots, &new_hash, &stable_fragment_id).await?;

            for root in &index_roots {
                let old_alias = fragment_alias_path(root, &current_hash);
                if fs::try_exists(&old_alias).await.unwrap_or(false) {
                    let _ = fs::remove_file(&old_alias).await;
                }
            }

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
    Json(payload): Json<DeleteFragmentRequest>,
) -> Result<impl IntoResponse, AppError> {
    ensure_user_mutations_allowed()?;
    // Resolve aliases without requiring metadata that a previous partial
    // deletion may already have removed. The JWT, master key and confirmation
    // below still authorize only this exact stable fragment.
    let stable_fragment_id = crate::core::index_store::stable_fragment_id_for_fragment(&fragment);
    validate_delete_fragment_request(
        &claims,
        &stable_fragment_id,
        &fragment,
        &payload,
        &MASTER_KEY,
    )?;

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();
    let stable_owned = stable_fragment_id.clone();
    let lifecycle_guard = crate::core::fragment_lifecycle::acquire(&stable_owned).await?;

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        let _snapshot_boundary = crate::core::backup::barrier::mutation();
        // Keep exclusion in the worker even if the HTTP request is cancelled.
        let _lifecycle_guard = lifecycle_guard;
        global_namespace_runtime().evict_fragment(&stable_owned);

        remove_fragment_storage(&stable_owned, index_roots.into_iter().chain(blob_roots))
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

fn remove_fragment_storage(
    fragment: &str,
    roots: impl IntoIterator<Item = PathBuf>,
) -> Result<(), AppError> {
    let roots = roots.into_iter().collect::<BTreeSet<_>>();
    for root in &roots {
        let path = fragment_root_on(root, fragment);
        match std::fs::remove_dir_all(&path) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => {
                return Err(AppError::Io(
                    e,
                    format!("delete fragment {}", path.display()),
                ));
            }
        }
    }
    // Keep aliases on failure so an authorized caller can retry. Never announce
    // deletion until every distinct member and its aliases have been removed.
    for root in &roots {
        remove_fragment_aliases_for_stable_id(root, fragment)?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_fragment_removal_preserves_aliases_and_can_be_retried() {
        let fragment = ulid::Ulid::new().to_string();
        let base = std::env::temp_dir().join(format!("n1-delete-error-{fragment}"));
        let roots = vec![base.join("a"), base.join("b")];
        for root in &roots {
            std::fs::create_dir_all(root.join(".fragment")).unwrap();
            std::os::unix::fs::symlink(
                format!("../@f_{fragment}"),
                root.join(".fragment/public-alias"),
            )
            .unwrap();
        }
        let malformed = fragment_root_on(&roots[0], &fragment);
        std::fs::write(&malformed, b"not a directory").unwrap();
        let healthy = fragment_root_on(&roots[1], &fragment);
        std::fs::create_dir_all(&healthy).unwrap();
        std::fs::write(healthy.join("fixture"), b"payload").unwrap();
        assert!(matches!(
            remove_fragment_storage(&fragment, roots.clone()),
            Err(AppError::Io(_, _))
        ));
        assert!(healthy.join("fixture").exists());
        assert!(std::fs::symlink_metadata(roots[0].join(".fragment/public-alias")).is_ok());
        std::fs::remove_file(malformed).unwrap();
        // Missing members and duplicate index/blob roots are valid on retry.
        remove_fragment_storage(
            &fragment,
            roots.iter().cloned().chain(roots.iter().cloned()),
        )
        .unwrap();
        assert!(!healthy.exists());
        assert!(std::fs::symlink_metadata(roots[0].join(".fragment/public-alias")).is_err());
        remove_fragment_storage(&fragment, roots).unwrap();
        std::fs::remove_dir_all(base).unwrap();
    }

    fn claims(fragment: &str, delete: bool, modify: bool) -> Claims {
        Claims {
            operator: false,
            fragment: fragment.to_string(),
            fragment_alias: None,
            read: true,
            write: true,
            modify,
            delete,
            exp: usize::MAX,
        }
    }

    fn delete_payload(fragment: &str, secret: &str) -> DeleteFragmentRequest {
        DeleteFragmentRequest {
            secret: secret.to_string(),
            confirm_fragment: fragment.to_string(),
        }
    }

    #[test]
    fn fragment_delete_requires_delete_permission_not_modify() {
        let fragment = "fragment-hash";
        let payload = delete_payload(fragment, "master");

        let result = validate_delete_fragment_request(
            &claims(fragment, false, true),
            fragment,
            fragment,
            &payload,
            "master",
        );
        assert!(matches!(result, Err(AppError::Forbidden(_))));

        validate_delete_fragment_request(
            &claims(fragment, true, false),
            fragment,
            fragment,
            &payload,
            "master",
        )
        .expect("delete permission is sufficient when confirmation and master key match");
    }

    #[test]
    fn fragment_delete_requires_master_key_and_exact_confirmation() {
        let fragment = "fragment-hash";
        let mut payload = delete_payload(fragment, "wrong");
        let result = validate_delete_fragment_request(
            &claims(fragment, true, false),
            fragment,
            fragment,
            &payload,
            "master",
        );
        assert!(matches!(result, Err(AppError::Unauthorized)));

        payload.secret = "master".to_string();
        payload.confirm_fragment = "other-fragment".to_string();
        let result = validate_delete_fragment_request(
            &claims(fragment, true, false),
            fragment,
            fragment,
            &payload,
            "master",
        );
        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }
}
