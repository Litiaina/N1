use std::sync::Arc;

use axum::extract::State;
use axum::{Json as AxumJson, http::StatusCode, response::IntoResponse};
use tracing::info;

use crate::core::disk_manager::{ObjectPathManager, RenameObjectRequest};
use crate::core::fragment_metadata::resolve_stable_fragment_id;
use crate::core::index;
use crate::enums::app_error_enum::AppError;
use crate::namespace::runtime::global_namespace_runtime;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};

async fn acquire_rename_locks(
    fragment: &str,
    source_key: &str,
    destination_key: &str,
) -> Result<Vec<index::ObjectOperationGuard>, AppError> {
    let mut targets = vec![
        ObjectPathManager::canonical_object_key(source_key),
        ObjectPathManager::canonical_object_key(destination_key),
    ];
    targets.sort();
    targets.dedup();
    let mut guards = Vec::with_capacity(targets.len());
    for object_key in targets {
        guards.push(index::acquire_object_operation_lock(fragment, &object_key).await?);
    }
    Ok(guards)
}

pub async fn rename_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(mut payload): AxumJson<RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }

    payload.source_fragment = resolve_stable_fragment_id(&payload.source_fragment).await?;
    payload.destination_fragment =
        resolve_stable_fragment_id(&payload.destination_fragment).await?;

    if claims.fragment != payload.source_fragment {
        return Err(AppError::Forbidden(
            "Access denied to source fragment".into(),
        ));
    }
    if payload.source_fragment != payload.destination_fragment {
        return Err(AppError::BadRequest(
            "N2 rename is a namespace-only mutation within one fragment; copy the object to move it between fragments"
                .into(),
        ));
    }
    if payload.source_key == payload.destination_key {
        return Ok(StatusCode::OK);
    }

    let _locks = acquire_rename_locks(
        &payload.source_fragment,
        &payload.source_key,
        &payload.destination_key,
    )
    .await?;
    let summary = global_namespace_runtime()
        .rename(
            &payload.source_fragment,
            &payload.source_key,
            &payload.destination_key,
        )
        .await?;

    info!(
        "Renamed namespace path from {}/{} to {}/{}",
        payload.source_fragment,
        payload.source_key,
        payload.destination_fragment,
        payload.destination_key
    );
    state.emit(
        &payload.source_fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &payload.source_fragment,
            summary,
        )),
    );
    Ok(StatusCode::OK)
}
