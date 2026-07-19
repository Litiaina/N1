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
            new_stable_fragment_id, physical_fragment_root_on,
            remove_fragment_aliases_for_stable_id,
        },
        fragment_metadata::{
            FragmentMetadata, init_fragment_metadata_strict, read_fragment_metadata,
            resolve_stable_fragment_id, rotate_fragment_key, write_fragment_metadata_strict,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FragmentControlSnapshot {
    pub fragment_hash: String,
    pub metadata: FragmentMetadata,
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

async fn prepare_fragment_storage_roots_strict(stable_fragment_id: &str) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let storage_roots = path_manager.get_storage_roots();

    if storage_roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "no storage devices configured for fragment provisioning".into(),
        ));
    }

    for root in storage_roots {
        let fragment_root = physical_fragment_root_on(&root, stable_fragment_id);
        timeout(
            FRAGMENT_CREATE_IO_TIMEOUT,
            fs::create_dir_all(&fragment_root),
        )
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "timed out preparing storage fragment root {}",
                fragment_root.display()
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "failed to prepare storage fragment root {}: {}",
                fragment_root.display(),
                e
            ))
        })?;
    }

    Ok(())
}

pub(crate) async fn provision_fragment_local(
    fragment_hash: &str,
    stable_fragment_id: &str,
    key: &str,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let mut identity_roots = path_manager.get_storage_roots();
    identity_roots.sort();
    identity_roots.dedup();
    ensure_fragment_identity_on_roots(&identity_roots, fragment_hash, stable_fragment_id).await?;
    prepare_fragment_storage_roots_strict(stable_fragment_id).await?;
    ensure_fragment_blob_directories_strict(stable_fragment_id).await?;
    if read_fragment_metadata(fragment_hash).await.is_err() {
        let _ = init_fragment_metadata_strict(stable_fragment_id, key).await?;
    }
    Ok(())
}

pub(crate) async fn apply_fragment_control_snapshot_local(
    snapshot: &FragmentControlSnapshot,
) -> Result<(), AppError> {
    if !is_fragment_hash(&snapshot.fragment_hash) {
        return Err(AppError::BadRequest(
            "fragment control snapshot contains an invalid alias hash".into(),
        ));
    }
    let stable_fragment_id = snapshot.metadata.fragment.trim();
    if stable_fragment_id.is_empty() {
        return Err(AppError::BadRequest(
            "fragment control snapshot has no stable fragment identity".into(),
        ));
    }

    let path_manager = ObjectPathManager::new();
    let mut identity_roots = path_manager.get_storage_roots();
    identity_roots.sort();
    identity_roots.dedup();
    ensure_fragment_identity_on_roots(&identity_roots, &snapshot.fragment_hash, stable_fragment_id)
        .await?;
    prepare_fragment_storage_roots_strict(stable_fragment_id).await?;
    ensure_fragment_blob_directories_strict(stable_fragment_id).await?;
    write_fragment_metadata_strict(&snapshot.metadata).await?;
    Ok(())
}

pub(crate) async fn local_fragment_control_snapshot()
-> Result<Vec<FragmentControlSnapshot>, AppError> {
    let path_manager = ObjectPathManager::new();
    let mut aliases = std::collections::BTreeSet::new();
    for root in path_manager.get_storage_roots() {
        aliases.extend(crate::core::fragment_identity::list_fragment_aliases(&root));
    }

    let mut snapshots = Vec::with_capacity(aliases.len());
    for fragment_hash in aliases {
        snapshots.push(FragmentControlSnapshot {
            metadata: read_fragment_metadata(&fragment_hash).await?,
            fragment_hash,
        });
    }
    Ok(snapshots)
}

pub(crate) async fn rename_fragment_alias_local(
    current_hash: &str,
    new_hash: &str,
    stable_fragment_id: &str,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let mut roots = path_manager.get_storage_roots();
    roots.sort();
    roots.dedup();
    ensure_fragment_identity_on_roots(&roots, new_hash, stable_fragment_id).await?;
    for root in roots {
        let old_alias = fragment_alias_path(&root, current_hash);
        if fs::try_exists(&old_alias).await.unwrap_or(false) {
            fs::remove_file(&old_alias).await.map_err(|err| {
                AppError::Io(
                    err,
                    format!("remove old fragment alias {}", old_alias.display()),
                )
            })?;
        }
    }
    Ok(())
}

pub(crate) async fn rotate_fragment_key_local(
    fragment_hash: &str,
    key: &str,
) -> Result<(), AppError> {
    rotate_fragment_key(fragment_hash, key).await.map(|_| ())
}

pub(crate) fn delete_fragment_local(stable_fragment_id: &str) -> Result<(), AppError> {
    global_namespace_runtime().evict_fragment(stable_fragment_id);
    let path_manager = ObjectPathManager::new();
    let mut roots = path_manager.get_storage_roots();
    roots.sort();
    roots.dedup();
    for root in roots {
        let fragment_path = fragment_root_on(&root, stable_fragment_id);
        if fragment_path.exists() {
            std::fs::remove_dir_all(&fragment_path).map_err(|err| {
                AppError::Io(
                    err,
                    format!("delete fragment root {}", fragment_path.display()),
                )
            })?;
        }
        remove_fragment_aliases_for_stable_id(&root, stable_fragment_id);
    }
    for root in path_manager.get_storage_roots() {
        let fragment_path = physical_fragment_root_on(&root, stable_fragment_id);
        if fragment_path.exists() {
            std::fs::remove_dir_all(&fragment_path).map_err(|err| {
                AppError::Io(
                    err,
                    format!("delete blob fragment root {}", fragment_path.display()),
                )
            })?;
        }
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

    let existing_meta = read_fragment_metadata(&hash).await.ok();
    if existing_meta.is_some() {
        return Err(AppError::Conflict(format!(
            "Fragment already exists: {}",
            hash
        )));
    }

    let stable_fragment_id = new_stable_fragment_id();
    crate::distribution::peer::replicate_fragment_control(
        crate::distribution::peer::PeerFragmentControlRequest::Provision {
            fragment_hash: hash.clone(),
            stable_fragment_id: stable_fragment_id.clone(),
            key: payload.key.clone(),
        },
    )
    .await?;
    info!(
        "Created new fragment '{}' with hash: {}",
        payload.name, hash
    );
    Ok((
        StatusCode::CREATED,
        Json(CreateFragmentResponse {
            fragment_hash: hash,
            stable_fragment_id,
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
                .map(|meta| meta.fragment.clone())
                .ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "fragment {} has no stable fragment identity",
                        current_hash
                    ))
                })?;

            crate::distribution::peer::replicate_fragment_control(
                crate::distribution::peer::PeerFragmentControlRequest::Rename {
                    current_hash: current_hash.clone(),
                    new_hash: new_hash.clone(),
                    stable_fragment_id: stable_fragment_id.clone(),
                },
            )
            .await?;

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
        crate::distribution::peer::replicate_fragment_control(
            crate::distribution::peer::PeerFragmentControlRequest::RotateKey {
                fragment_hash: current_hash.clone(),
                key: new_key,
            },
        )
        .await?;
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
    let stable_fragment_id = resolve_stable_fragment_id(&fragment).await?;
    validate_delete_fragment_request(
        &claims,
        &stable_fragment_id,
        &fragment,
        &payload,
        &MASTER_KEY,
    )?;

    crate::distribution::peer::replicate_fragment_control(
        crate::distribution::peer::PeerFragmentControlRequest::Delete {
            stable_fragment_id: stable_fragment_id.clone(),
        },
    )
    .await?;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(fragment: &str, delete: bool, modify: bool) -> Claims {
        Claims {
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
