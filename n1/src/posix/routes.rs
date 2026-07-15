use std::sync::Arc;

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceEntryKind;
use crate::namespace::runtime::global_namespace_runtime;
use crate::posix::models::{
    PosixEntry, PosixListQuery, PosixListResponse, PosixMutationResponse, PosixNodeKind,
    PosixPathRequest, PosixRenameRequest, PosixStatQuery, PosixStatResponse,
};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};

async fn posix_list(
    claims: Claims,
    Query(query): Query<PosixListQuery>,
) -> Result<Json<PosixListResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let scan_trash = query.trash.unwrap_or(false);
    let path = query.path.unwrap_or_default();
    let limit = query.limit.unwrap_or(200).clamp(1, 1000);
    let (entries, continuation_token) = global_namespace_runtime()
        .list_dir_page(
            &claims.fragment,
            &path,
            scan_trash,
            limit,
            query.continuation_token.as_deref(),
        )
        .await?;
    let mut mapped = entries
        .into_iter()
        .map(|entry| PosixEntry {
            size_bytes: entry.size_bytes,
            name: entry.name,
            path: entry.path,
            kind: match entry.kind {
                NamespaceEntryKind::File => PosixNodeKind::File,
                NamespaceEntryKind::Directory => PosixNodeKind::Directory,
            },
            modified_at_unix: Some(entry.updated_at_unix),
            content_type: None,
            version_id: entry.version_id,
            trash_entry_id: entry.trash_entry_id,
            deleted_at_unix: entry.deleted_at_unix,
        })
        .collect::<Vec<_>>();
    mapped.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(Json(PosixListResponse {
        path,
        scan_trash,
        entries: mapped,
        continuation_token,
    }))
}

async fn posix_stat(
    claims: Claims,
    Query(query): Query<PosixStatQuery>,
) -> Result<Json<PosixStatResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let path = query.path.unwrap_or_default();
    let scan_trash = query.trash.unwrap_or(false);
    let entry = if scan_trash {
        global_namespace_runtime()
            .trashed_entry(&claims.fragment, &path)
            .await?
    } else {
        global_namespace_runtime()
            .entry(&claims.fragment, &path)
            .await?
    }
    .ok_or(AppError::NotFound)?;
    let kind = match entry.kind {
        NamespaceEntryKind::File => PosixNodeKind::File,
        NamespaceEntryKind::Directory => PosixNodeKind::Directory,
    };
    let is_virtual = false;
    let (size_bytes, content_type, version_id) = if entry.kind == NamespaceEntryKind::File {
        let head = global_namespace_runtime()
            .head_for_path(&claims.fragment, &entry.path)
            .await?;
        (
            head.as_ref().map(|value| value.total_size_bytes),
            head.as_ref().map(|value| value.content_type.clone()),
            head.as_ref().map(|value| value.current_version_id.clone()),
        )
    } else {
        (None, None, None)
    };
    let _ = scan_trash;
    Ok(Json(PosixStatResponse {
        path: entry.path,
        scan_trash,
        kind,
        is_virtual,
        size_bytes,
        modified_at_unix: Some(entry.updated_at_unix),
        content_type,
        version_id,
        trash_entry_id: entry.trash_entry_id,
        deleted_at_unix: entry.deleted_at_unix,
    }))
}

async fn posix_mkdir(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Json(req): Json<PosixPathRequest>,
) -> Result<Json<PosixMutationResponse>, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    let path = req.path;
    let created = global_namespace_runtime()
        .mkdir(&claims.fragment, &path)
        .await?;
    if created {
        state.emit(
            &claims.fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::directory_created(
                &claims.fragment,
                &path,
            )),
        );
    }
    Ok(Json(PosixMutationResponse {
        operation: "mkdir".to_string(),
        kind: PosixNodeKind::Directory,
        affected: usize::from(created),
        path: Some(path),
        source_path: None,
        destination_path: None,
        created: Some(created),
        trash_entry_id: None,
        deleted_at_unix: None,
    }))
}

async fn posix_rename(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Json(req): Json<PosixRenameRequest>,
) -> Result<Json<PosixMutationResponse>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    let summary = global_namespace_runtime()
        .rename(&claims.fragment, &req.source_path, &req.destination_path)
        .await?;
    state.emit(
        &claims.fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &claims.fragment,
            summary.clone(),
        )),
    );
    Ok(Json(PosixMutationResponse {
        operation: summary.operation,
        kind: match summary.kind {
            NamespaceEntryKind::File => PosixNodeKind::File,
            NamespaceEntryKind::Directory => PosixNodeKind::Directory,
        },
        affected: summary.affected_paths,
        path: None,
        source_path: summary.source_path,
        destination_path: summary.destination_path,
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    }))
}

async fn posix_trash(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Json(req): Json<PosixPathRequest>,
) -> Result<Json<PosixMutationResponse>, AppError> {
    let summary = global_namespace_runtime()
        .trash(&claims.fragment, &req.path)
        .await?;
    state.emit(
        &claims.fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &claims.fragment,
            summary.clone(),
        )),
    );
    Ok(Json(PosixMutationResponse {
        operation: summary.operation,
        kind: match summary.kind {
            NamespaceEntryKind::File => PosixNodeKind::File,
            NamespaceEntryKind::Directory => PosixNodeKind::Directory,
        },
        affected: summary.affected_paths,
        path: summary.source_path,
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    }))
}

async fn posix_recover(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Json(req): Json<PosixPathRequest>,
) -> Result<Json<PosixMutationResponse>, AppError> {
    let summary = match req.trash_entry_id.as_deref().map(str::trim) {
        Some(trash_entry_id) if !trash_entry_id.is_empty() => {
            global_namespace_runtime()
                .recover_trash_entry_id(&claims.fragment, trash_entry_id)
                .await?
        }
        _ => {
            global_namespace_runtime()
                .recover(&claims.fragment, &req.path)
                .await?
        }
    };
    state.emit(
        &claims.fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &claims.fragment,
            summary.clone(),
        )),
    );
    Ok(Json(PosixMutationResponse {
        operation: summary.operation,
        kind: match summary.kind {
            NamespaceEntryKind::File => PosixNodeKind::File,
            NamespaceEntryKind::Directory => PosixNodeKind::Directory,
        },
        affected: summary.affected_paths,
        path: summary.source_path,
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    }))
}

pub fn build_posix_read_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/noa/v1/posix/list", get(posix_list))
        .route("/noa/v1/posix/stat", get(posix_stat))
}

pub fn build_posix_write_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/noa/v1/posix/mkdir", post(posix_mkdir))
        .route("/noa/v1/posix/rename", post(posix_rename))
        .route("/noa/v1/posix/move", post(posix_rename))
        .route("/noa/v1/posix/trash", post(posix_trash))
        .route("/noa/v1/posix/recover", post(posix_recover))
}
