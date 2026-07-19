use std::sync::Arc;

use crate::distribution::namespace::{
    NamespaceMutationKind, PeerNamespaceApplyRequest, commit_namespace_created,
    commit_namespace_summary,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceEntryKind;
use crate::namespace::runtime::global_namespace_runtime;
use crate::posix::models::{
    PosixEntry, PosixListResponse, PosixMutationResponse, PosixNodeKind, PosixStatResponse,
};
use crate::posix::paths::normalize_posix_path;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};

fn posix_kind_from_namespace(kind: NamespaceEntryKind) -> PosixNodeKind {
    match kind {
        NamespaceEntryKind::File => PosixNodeKind::File,
        NamespaceEntryKind::Directory => PosixNodeKind::Directory,
    }
}

pub async fn list_dir(
    claims: Claims,
    path: String,
    limit: usize,
    continuation_token: Option<String>,
    scan_trash: bool,
) -> Result<PosixListResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let fragment = claims.fragment.clone();
    let path = normalize_posix_path(Some(path.as_str()))?;
    if !path.is_empty() {
        let entry = if scan_trash {
            global_namespace_runtime()
                .trashed_entry(&fragment, &path)
                .await?
        } else {
            global_namespace_runtime().entry(&fragment, &path).await?
        }
        .ok_or(AppError::NotFound)?;
        if entry.kind == NamespaceEntryKind::File {
            return Err(AppError::BadRequest(
                "POSIX path is a file, not a directory".to_string(),
            ));
        }
    }
    let (namespace_entries, continuation_token) = global_namespace_runtime()
        .list_dir_page(
            &fragment,
            &path,
            scan_trash,
            limit.clamp(1, 1000),
            continuation_token.as_deref(),
        )
        .await?;
    let entries = namespace_entries
        .into_iter()
        .map(|entry| PosixEntry {
            name: entry.name,
            path: entry.path,
            kind: posix_kind_from_namespace(entry.kind),
            object_id: entry.object_id,
            size_bytes: entry.size_bytes,
            modified_at_unix: Some(entry.updated_at_unix),
            content_type: None,
            version_id: entry.version_id,
            trash_entry_id: entry.trash_entry_id,
            deleted_at_unix: entry.deleted_at_unix,
        })
        .collect::<Vec<_>>();

    Ok(PosixListResponse {
        path,
        scan_trash,
        entries,
        continuation_token,
    })
}

pub async fn stat_path(
    claims: Claims,
    path: String,
    scan_trash: bool,
) -> Result<PosixStatResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let fragment = claims.fragment.clone();
    let path = normalize_posix_path(Some(path.as_str()))?;
    let entry = if scan_trash {
        global_namespace_runtime()
            .trashed_entry(&fragment, &path)
            .await?
    } else {
        global_namespace_runtime().entry(&fragment, &path).await?
    };
    match entry {
        None if path.is_empty() => Ok(PosixStatResponse {
            path,
            scan_trash,
            kind: PosixNodeKind::Directory,
            is_virtual: true,
            object_id: None,
            size_bytes: None,
            modified_at_unix: None,
            content_type: None,
            version_id: None,
            trash_entry_id: None,
            deleted_at_unix: None,
        }),
        Some(entry) => {
            let kind = posix_kind_from_namespace(entry.kind.clone());
            let head = if entry.kind == NamespaceEntryKind::File
                && entry.size_bytes.is_none()
                && entry.version_id.is_none()
            {
                match entry.object_id.as_ref() {
                    Some(object_id) => global_namespace_runtime()
                        .object_heads(&fragment, std::slice::from_ref(object_id))
                        .await?
                        .remove(object_id),
                    None => None,
                }
            } else {
                None
            };
            Ok(PosixStatResponse {
                path: entry.path,
                scan_trash,
                kind,
                is_virtual: false,
                object_id: entry.object_id,
                size_bytes: entry
                    .size_bytes
                    .or_else(|| head.as_ref().map(|value| value.total_size_bytes)),
                modified_at_unix: Some(entry.updated_at_unix),
                content_type: head.as_ref().map(|value| value.content_type.clone()),
                version_id: entry
                    .version_id
                    .or_else(|| head.as_ref().map(|value| value.current_version_id.clone())),
                trash_entry_id: entry.trash_entry_id,
                deleted_at_unix: entry.deleted_at_unix,
            })
        }
        None => Err(AppError::NotFound),
    }
}

pub async fn mkdir_path(
    state: Arc<AppState>,
    claims: Claims,
    path: String,
) -> Result<PosixMutationResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    let fragment = claims.fragment.clone();
    let path = normalize_posix_path(Some(path.as_str()))?;
    if path.is_empty() {
        return Err(AppError::BadRequest(
            "root path cannot be created".to_string(),
        ));
    }

    let created = commit_namespace_created(PeerNamespaceApplyRequest {
        mutation_id: None,
        kind: NamespaceMutationKind::Mkdir,
        fragment: fragment.clone(),
        source_path: Some(path.clone()),
        destination_path: None,
        object_id: None,
        version_id: None,
        metadata: None,
    })
    .await?;
    if created {
        state.emit(
            &fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::directory_created(
                &fragment, &path,
            )),
        );
    }

    Ok(PosixMutationResponse {
        operation: "mkdir".to_string(),
        kind: PosixNodeKind::Directory,
        affected: usize::from(created),
        path: Some(path),
        source_path: None,
        destination_path: None,
        created: Some(created),
        trash_entry_id: None,
        deleted_at_unix: None,
    })
}

pub async fn rename_path(
    state: Arc<AppState>,
    claims: Claims,
    source_path: String,
    destination_path: String,
) -> Result<PosixMutationResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    let fragment = claims.fragment.clone();
    let source_path = normalize_posix_path(Some(source_path.as_str()))?;
    let destination_path = normalize_posix_path(Some(destination_path.as_str()))?;
    if source_path.is_empty() || destination_path.is_empty() {
        return Err(AppError::BadRequest(
            "root path cannot be renamed".to_string(),
        ));
    }
    if source_path == destination_path {
        let entry = global_namespace_runtime()
            .entry(&fragment, &source_path)
            .await?
            .ok_or(AppError::NotFound)?;
        let kind = posix_kind_from_namespace(entry.kind);
        return Ok(PosixMutationResponse {
            operation: "rename".to_string(),
            kind,
            affected: 0,
            path: None,
            source_path: Some(source_path),
            destination_path: Some(destination_path),
            created: None,
            trash_entry_id: None,
            deleted_at_unix: None,
        });
    }

    let summary = commit_namespace_summary(PeerNamespaceApplyRequest {
        mutation_id: None,
        kind: NamespaceMutationKind::Rename,
        fragment: fragment.clone(),
        source_path: Some(source_path.clone()),
        destination_path: Some(destination_path.clone()),
        object_id: None,
        version_id: None,
        metadata: None,
    })
    .await?;
    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment,
            summary.clone(),
        )),
    );
    Ok(PosixMutationResponse {
        operation: summary.operation,
        kind: posix_kind_from_namespace(summary.kind),
        affected: summary.affected_paths,
        path: None,
        source_path: Some(source_path),
        destination_path: Some(destination_path),
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    })
}

pub async fn trash_path(
    state: Arc<AppState>,
    claims: Claims,
    path: String,
) -> Result<PosixMutationResponse, AppError> {
    let path = normalize_posix_path(Some(path.as_str()))?;
    if path.is_empty() {
        return Err(AppError::BadRequest(
            "root path cannot be trashed".to_string(),
        ));
    }
    let fragment = claims.fragment.clone();
    let summary = commit_namespace_summary(PeerNamespaceApplyRequest {
        mutation_id: None,
        kind: NamespaceMutationKind::Trash,
        fragment: fragment.clone(),
        source_path: Some(path.clone()),
        destination_path: None,
        object_id: None,
        version_id: None,
        metadata: None,
    })
    .await?;
    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment,
            summary.clone(),
        )),
    );
    Ok(PosixMutationResponse {
        operation: summary.operation,
        kind: posix_kind_from_namespace(summary.kind),
        affected: summary.affected_paths,
        path: Some(path),
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    })
}

pub async fn recover_path(
    state: Arc<AppState>,
    claims: Claims,
    path: String,
) -> Result<PosixMutationResponse, AppError> {
    let path = normalize_posix_path(Some(path.as_str()))?;
    if path.is_empty() {
        return Err(AppError::BadRequest(
            "root path cannot be recovered".to_string(),
        ));
    }
    let fragment = claims.fragment.clone();
    let summary = commit_namespace_summary(PeerNamespaceApplyRequest {
        mutation_id: None,
        kind: NamespaceMutationKind::Recover,
        fragment: fragment.clone(),
        source_path: Some(path.clone()),
        destination_path: None,
        object_id: None,
        version_id: None,
        metadata: None,
    })
    .await?;
    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment,
            summary.clone(),
        )),
    );
    Ok(PosixMutationResponse {
        operation: summary.operation,
        kind: posix_kind_from_namespace(summary.kind),
        affected: summary.affected_paths,
        path: Some(path),
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    })
}
