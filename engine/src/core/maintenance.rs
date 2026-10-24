use crate::core::index;
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use tracing::info;

async fn perform_fragment_cleanup(fragment: &str) -> Result<FragmentCleanupResponse, AppError> {
    // This endpoint is deliberately read-only. A local HEAD/session inventory
    // is not proof that a distributed version is unreferenced: historical
    // versions, trash, peers and durable export pins can still retain it.
    // In particular, `.objects` children are hash-fanout directories, NOT
    // version IDs. Never infer deletion authority from their names or age.
    let live_objects = index::list_fragment_live_objects(fragment).await?;
    let total_usage = live_objects
        .iter()
        .filter(|meta| !meta.is_delete_marker)
        .try_fold(0u64, |total, meta| {
            total.checked_add(meta.total_size_bytes).ok_or_else(|| {
                AppError::InternalServerError("local metadata usage exceeds u64".into())
            })
        })?;

    Ok(FragmentCleanupResponse {
        fragment: fragment.to_string(),
        usage_bytes: total_usage,
        usage_scope: "local_recorded_live_heads",
        orphan_blob_dirs_removed: 0,
        orphan_reclamation_status: "deferred",
        message: "Read-only metadata inspection complete; no blob directories were removed. \
                  Use guarded garbage collection for expired trash and upload-session expiry. \
                  Distributed orphan shard reclamation is deferred.",
    })
}

#[derive(Serialize)]
struct FragmentCleanupResponse {
    fragment: String,
    usage_bytes: u64,
    usage_scope: &'static str,
    orphan_blob_dirs_removed: i32,
    orphan_reclamation_status: &'static str,
    message: &'static str,
}

pub async fn sync_fragment_metadata(claims: Claims) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permissions required to clean fragment metadata".into(),
        ));
    }

    let response = perform_fragment_cleanup(&claims.fragment).await?;
    info!(
        fragment = claims.fragment,
        local_usage_bytes = response.usage_bytes,
        "Fragment metadata inspected read-only; orphan shard reclamation deferred"
    );

    Ok((StatusCode::OK, Json(response)))
}

#[cfg(test)]
#[path = "maintenance/tests.rs"]
mod tests;
