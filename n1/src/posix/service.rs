use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::IntoResponse;

use crate::core::delete::{delete_object, delete_object_permanently, recover_object};
use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata, UploadPartQuery};
use crate::core::index;
use crate::core::metadata_parser::get_metadata;
use crate::core::modify::rename_object;
use crate::core::retrieve::{get_object_metadata_direct, list_objects_direct};
use crate::core::upload::upload_object_one_shot;
use crate::enums::app_error_enum::AppError;
use crate::posix::models::{
    PosixEntry, PosixListResponse, PosixMutationResponse, PosixNodeKind, PosixStatResponse,
};
use crate::posix::paths::{
    DIR_MARKER_NAME, child_name, directory_prefix, marker_key, normalize_posix_path,
};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;

#[derive(Debug, Clone)]
enum PathClassification {
    Root,
    File(ObjectMetadata),
    Directory {
        marker: Option<ObjectMetadata>,
        sample_child: Option<ObjectMetadata>,
    },
    Conflict,
    Missing,
}

fn latest_trashed_version(metas: Vec<ObjectMetadata>) -> Option<ObjectMetadata> {
    metas.into_iter().max_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.version_id.cmp(&b.version_id))
    })
}

fn clone_claims(claims: &Claims) -> Claims {
    Claims {
        fragment: claims.fragment.clone(),
        read: claims.read,
        write: claims.write,
        modify: claims.modify,
        delete: claims.delete,
        exp: claims.exp,
    }
}

async fn exact_metadata(
    fragment: &str,
    object_key: &str,
    scan_trash: bool,
) -> Result<Option<ObjectMetadata>, AppError> {
    if scan_trash {
        Ok(latest_trashed_version(
            index::list_trashed_object_versions_metadata(fragment, object_key).await?,
        ))
    } else {
        match get_object_metadata_direct(fragment, object_key).await {
            Ok(meta) => Ok(Some(meta)),
            Err(AppError::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

async fn internal_marker_metadata(
    fragment: &str,
    path: &str,
    scan_trash: bool,
) -> Result<Option<ObjectMetadata>, AppError> {
    let Some(marker) = marker_key(path) else {
        return Ok(None);
    };
    if scan_trash {
        Ok(latest_trashed_version(
            index::list_trashed_object_versions_metadata(fragment, &marker).await?,
        ))
    } else {
        match get_metadata(fragment, &marker, fragment).await {
            Ok(meta) if !meta.is_delete_marker => Ok(Some(meta)),
            Ok(_) => Ok(None),
            Err(AppError::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

fn list_params(
    prefix: Option<String>,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> HashMap<String, String> {
    let mut params = HashMap::new();
    params.insert("limit".to_string(), limit.to_string());
    if let Some(prefix) = prefix {
        params.insert("prefix".to_string(), prefix);
    }
    if let Some(delimiter) = delimiter {
        params.insert("delimiter".to_string(), delimiter.to_string());
    }
    if let Some(token) = continuation_token {
        params.insert("continuation_token".to_string(), token);
    }
    if include_internal {
        params.insert("include_internal".to_string(), "true".to_string());
    }
    params
}

async fn list_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<&str>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
    scan_trash: bool,
) -> Result<ListObjectsResult, AppError> {
    list_objects_direct(
        fragment,
        list_params(
            prefix,
            delimiter,
            limit,
            continuation_token,
            include_internal,
        ),
        scan_trash,
    )
    .await
}

async fn list_all_objects_under_prefix(
    fragment: &str,
    prefix: &str,
    scan_trash: bool,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut out = Vec::new();
    let mut continuation = None;
    loop {
        let page = list_page(
            fragment,
            Some(prefix.to_string()),
            None,
            1000,
            continuation.take(),
            true,
            scan_trash,
        )
        .await?;
        out.extend(page.objects);
        continuation = page.continuation_token;
        if continuation.is_none() {
            break;
        }
    }
    Ok(out)
}

async fn classify_path(
    fragment: &str,
    path: &str,
    scan_trash: bool,
) -> Result<PathClassification, AppError> {
    if path.is_empty() {
        return Ok(PathClassification::Root);
    }

    let exact_file = exact_metadata(fragment, path, scan_trash).await?;
    let marker = internal_marker_metadata(fragment, path, scan_trash).await?;
    let child_prefix = directory_prefix(path);
    let sample_child = if let Some(prefix) = child_prefix.as_deref() {
        list_page(
            fragment,
            Some(prefix.to_string()),
            None,
            2,
            None,
            true,
            scan_trash,
        )
        .await?
        .objects
        .into_iter()
        .find(|meta| meta.object_key != format!("{prefix}{DIR_MARKER_NAME}"))
    } else {
        None
    };

    let is_dir = marker.is_some() || sample_child.is_some();
    match (exact_file, is_dir) {
        (Some(file), false) => Ok(PathClassification::File(file)),
        (None, true) => Ok(PathClassification::Directory {
            marker,
            sample_child,
        }),
        (Some(_), true) => Ok(PathClassification::Conflict),
        (None, false) => Ok(PathClassification::Missing),
    }
}

fn map_listing_entries(path: &str, page: ListObjectsResult) -> Result<Vec<PosixEntry>, AppError> {
    let mut entries = BTreeMap::<String, PosixEntry>::new();

    for object in page.objects {
        if let Some(marker_path) = object
            .object_key
            .strip_suffix(&format!("/{}", DIR_MARKER_NAME))
        {
            if marker_path == path.trim_matches('/') {
                continue;
            }
            let Some(name) = child_name(path, marker_path) else {
                continue;
            };
            let entry = PosixEntry {
                name: name.clone(),
                path: marker_path.to_string(),
                kind: PosixNodeKind::Directory,
                size_bytes: None,
                modified_at_unix: Some(object.created_at),
                content_type: None,
                version_id: Some(object.version_id.clone()),
                trash_entry_id: None,
                deleted_at_unix: None,
            };
            match entries.get(&name) {
                Some(existing) if existing.kind == PosixNodeKind::Directory => continue,
                Some(_) => {
                    return Err(AppError::Conflict(format!(
                        "POSIX facade encountered conflicting file and directory entry '{}'",
                        name
                    )));
                }
                None => {
                    entries.insert(name, entry);
                }
            }
        } else {
            let Some(name) = child_name(path, &object.object_key) else {
                continue;
            };
            let entry = PosixEntry {
                name: name.clone(),
                path: object.object_key.clone(),
                kind: PosixNodeKind::File,
                size_bytes: Some(object.total_size_bytes),
                modified_at_unix: Some(object.created_at),
                content_type: Some(object.content_type.clone()),
                version_id: Some(object.version_id.clone()),
                trash_entry_id: None,
                deleted_at_unix: None,
            };
            match entries.get(&name) {
                Some(existing) if existing.kind == PosixNodeKind::File => continue,
                Some(_) => {
                    return Err(AppError::Conflict(format!(
                        "POSIX facade encountered conflicting file and directory entry '{}'",
                        name
                    )));
                }
                None => {
                    entries.insert(name, entry);
                }
            }
        }
    }

    for prefix in page.common_prefixes {
        let normalized = prefix.trim_end_matches('/');
        let Some(name) = child_name(path, normalized) else {
            continue;
        };
        let entry = PosixEntry {
            name: name.clone(),
            path: normalized.to_string(),
            kind: PosixNodeKind::Directory,
            size_bytes: None,
            modified_at_unix: None,
            content_type: None,
            version_id: None,
            trash_entry_id: None,
            deleted_at_unix: None,
        };
        match entries.get(&name) {
            Some(existing) if existing.kind == PosixNodeKind::Directory => continue,
            Some(_) => {
                return Err(AppError::Conflict(format!(
                    "POSIX facade encountered conflicting file and directory entry '{}'",
                    name
                )));
            }
            None => {
                entries.insert(name, entry);
            }
        }
    }

    Ok(entries.into_values().collect())
}

async fn rename_exact_object(
    state: Arc<AppState>,
    claims: Claims,
    source_key: String,
    destination_key: String,
) -> Result<(), AppError> {
    let fragment = claims.fragment.clone();
    let response = rename_object(
        State(state),
        claims,
        AxumJson(crate::core::disk_manager::RenameObjectRequest {
            source_fragment: fragment.clone(),
            source_key,
            destination_fragment: fragment,
            destination_key,
        }),
    )
    .await?
    .into_response();

    if response.status() == StatusCode::OK {
        Ok(())
    } else {
        Err(AppError::InternalServerError(format!(
            "rename returned unexpected status {}",
            response.status()
        )))
    }
}

async fn soft_delete_exact_object(
    state: Arc<AppState>,
    claims: Claims,
    object_key: String,
) -> Result<(), AppError> {
    let fragment = claims.fragment.clone();
    let response = delete_object(State(state), claims, AxumPath((fragment, object_key)))
        .await?
        .into_response();
    if response.status() == StatusCode::OK {
        Ok(())
    } else {
        Err(AppError::InternalServerError(format!(
            "trash returned unexpected status {}",
            response.status()
        )))
    }
}

async fn recover_exact_object(
    state: Arc<AppState>,
    claims: Claims,
    object_key: String,
) -> Result<(), AppError> {
    let fragment = claims.fragment.clone();
    let response = recover_object(State(state), claims, AxumPath((fragment, object_key)))
        .await?
        .into_response();
    if response.status() == StatusCode::OK {
        Ok(())
    } else {
        Err(AppError::InternalServerError(format!(
            "recover returned unexpected status {}",
            response.status()
        )))
    }
}

async fn permanently_delete_exact_object(
    state: Arc<AppState>,
    claims: Claims,
    object_key: String,
) -> Result<(), AppError> {
    let fragment = claims.fragment.clone();
    let response =
        delete_object_permanently(State(state), claims, AxumPath((fragment, object_key)))
            .await?
            .into_response();
    if matches!(response.status(), StatusCode::OK | StatusCode::NO_CONTENT) {
        Ok(())
    } else {
        Err(AppError::InternalServerError(format!(
            "permanent delete returned unexpected status {}",
            response.status()
        )))
    }
}

async fn create_directory_marker(
    state: Arc<AppState>,
    claims: Claims,
    path: &str,
) -> Result<(), AppError> {
    let marker = marker_key(path).ok_or_else(|| {
        AppError::BadRequest("root path cannot be created as a directory".to_string())
    })?;
    let fragment = claims.fragment.clone();
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-n1-dir-marker"),
    );
    let response = upload_object_one_shot(
        State(state),
        claims,
        headers,
        AxumPath((fragment, marker)),
        Query(UploadPartQuery {
            checksum: None,
            durability: None,
            strict_durability: None,
            io_gate: None,
        }),
        Body::empty(),
    )
    .await?
    .into_response();

    if response.status() == StatusCode::CREATED {
        Ok(())
    } else {
        Err(AppError::InternalServerError(format!(
            "mkdir marker upload returned unexpected status {}",
            response.status()
        )))
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

    match classify_path(&fragment, &path, scan_trash).await? {
        PathClassification::File(_) => {
            return Err(AppError::BadRequest(
                "POSIX path is a file, not a directory".to_string(),
            ));
        }
        PathClassification::Conflict => {
            return Err(AppError::Conflict(
                "path exists as both a file and a directory in flat N1 storage".to_string(),
            ));
        }
        PathClassification::Missing if !path.is_empty() => return Err(AppError::NotFound),
        _ => {}
    }

    let page = list_page(
        &fragment,
        directory_prefix(&path),
        Some("/"),
        limit.clamp(1, 1000),
        continuation_token,
        true,
        scan_trash,
    )
    .await?;
    let continuation_token = page.continuation_token.clone();
    let entries = map_listing_entries(&path, page)?;

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

    match classify_path(&fragment, &path, scan_trash).await? {
        PathClassification::Root => Ok(PosixStatResponse {
            path,
            scan_trash,
            kind: PosixNodeKind::Directory,
            is_virtual: true,
            size_bytes: None,
            modified_at_unix: None,
            content_type: None,
            version_id: None,
            trash_entry_id: None,
            deleted_at_unix: None,
        }),
        PathClassification::File(meta) => Ok(PosixStatResponse {
            path,
            scan_trash,
            kind: PosixNodeKind::File,
            is_virtual: false,
            size_bytes: Some(meta.total_size_bytes),
            modified_at_unix: Some(meta.created_at),
            content_type: Some(meta.content_type),
            version_id: Some(meta.version_id),
            trash_entry_id: None,
            deleted_at_unix: None,
        }),
        PathClassification::Directory {
            marker,
            sample_child,
        } => {
            let modified_at_unix = marker
                .as_ref()
                .map(|meta| meta.created_at)
                .or_else(|| sample_child.as_ref().map(|meta| meta.created_at));
            Ok(PosixStatResponse {
                path,
                scan_trash,
                kind: PosixNodeKind::Directory,
                is_virtual: marker.is_none(),
                size_bytes: None,
                modified_at_unix,
                content_type: None,
                version_id: marker.map(|meta| meta.version_id),
                trash_entry_id: None,
                deleted_at_unix: None,
            })
        }
        PathClassification::Conflict => Err(AppError::Conflict(
            "path exists as both a file and a directory in flat N1 storage".to_string(),
        )),
        PathClassification::Missing => Err(AppError::NotFound),
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

    match classify_path(&fragment, &path, false).await? {
        PathClassification::Root | PathClassification::Directory { .. } => {
            return Ok(PosixMutationResponse {
                operation: "mkdir".to_string(),
                kind: PosixNodeKind::Directory,
                affected: 0,
                path: Some(path),
                source_path: None,
                destination_path: None,
                created: Some(false),
                trash_entry_id: None,
                deleted_at_unix: None,
            });
        }
        PathClassification::File(_) | PathClassification::Conflict => {
            return Err(AppError::Conflict(
                "cannot create directory because the POSIX path already exists".to_string(),
            ));
        }
        PathClassification::Missing => {}
    }

    create_directory_marker(state, claims, &path).await?;

    Ok(PosixMutationResponse {
        operation: "mkdir".to_string(),
        kind: PosixNodeKind::Directory,
        affected: 1,
        path: Some(path),
        source_path: None,
        destination_path: None,
        created: Some(true),
        trash_entry_id: None,
        deleted_at_unix: None,
    })
}

async fn collect_directory_rename_plan(
    fragment: &str,
    source_path: &str,
    destination_path: &str,
) -> Result<Vec<(String, String)>, AppError> {
    let source_prefix = directory_prefix(source_path).expect("non-root source prefix");
    let destination_prefix =
        directory_prefix(destination_path).expect("non-root destination prefix");
    let mut objects = list_all_objects_under_prefix(fragment, &source_prefix, false).await?;
    objects.sort_by(|a, b| a.object_key.cmp(&b.object_key));

    if objects.is_empty() {
        return Err(AppError::NotFound);
    }

    let mut plan = Vec::with_capacity(objects.len());
    for object in objects {
        let suffix = object
            .object_key
            .strip_prefix(&source_prefix)
            .ok_or_else(|| {
                AppError::InternalServerError(format!(
                    "directory rename source key '{}' did not match source prefix '{}'",
                    object.object_key, source_prefix
                ))
            })?;
        plan.push((
            object.object_key.clone(),
            format!("{destination_prefix}{suffix}"),
        ));
    }
    Ok(plan)
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
        let kind = match classify_path(&fragment, &source_path, false).await? {
            PathClassification::File(_) => PosixNodeKind::File,
            PathClassification::Directory { .. } | PathClassification::Root => {
                PosixNodeKind::Directory
            }
            PathClassification::Conflict => {
                return Err(AppError::Conflict(
                    "source POSIX path exists as both a file and a directory".to_string(),
                ));
            }
            PathClassification::Missing => return Err(AppError::NotFound),
        };
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

    let source_kind = classify_path(&fragment, &source_path, false).await?;
    let destination_kind = classify_path(&fragment, &destination_path, false).await?;
    if !matches!(destination_kind, PathClassification::Missing) {
        return Err(AppError::Conflict(
            "destination POSIX path already exists".to_string(),
        ));
    }

    match source_kind {
        PathClassification::File(_) => {
            rename_exact_object(state, claims, source_path.clone(), destination_path.clone())
                .await?;
            Ok(PosixMutationResponse {
                operation: "rename".to_string(),
                kind: PosixNodeKind::File,
                affected: 1,
                path: None,
                source_path: Some(source_path),
                destination_path: Some(destination_path),
                created: None,
                trash_entry_id: None,
                deleted_at_unix: None,
            })
        }
        PathClassification::Directory { .. } => {
            let source_prefix = directory_prefix(&source_path).expect("source prefix");
            if destination_path == source_path
                || destination_path.starts_with(&(source_prefix.clone()))
            {
                return Err(AppError::BadRequest(
                    "cannot move a directory into itself".to_string(),
                ));
            }

            let plan =
                collect_directory_rename_plan(&fragment, &source_path, &destination_path).await?;
            let mut completed = Vec::new();
            for (from, to) in &plan {
                if let Err(err) = rename_exact_object(
                    Arc::clone(&state),
                    clone_claims(&claims),
                    from.clone(),
                    to.clone(),
                )
                .await
                {
                    for (rolled_from, rolled_to) in completed.into_iter().rev() {
                        let _ = rename_exact_object(
                            Arc::clone(&state),
                            clone_claims(&claims),
                            rolled_to,
                            rolled_from,
                        )
                        .await;
                    }
                    return Err(err);
                }
                completed.push((from.clone(), to.clone()));
            }
            Ok(PosixMutationResponse {
                operation: "rename".to_string(),
                kind: PosixNodeKind::Directory,
                affected: plan.len(),
                path: None,
                source_path: Some(source_path),
                destination_path: Some(destination_path),
                created: None,
                trash_entry_id: None,
                deleted_at_unix: None,
            })
        }
        PathClassification::Conflict => Err(AppError::Conflict(
            "source POSIX path exists as both a file and a directory".to_string(),
        )),
        PathClassification::Missing | PathClassification::Root => Err(AppError::NotFound),
    }
}

async fn batch_apply_over_prefix(
    state: Arc<AppState>,
    claims: Claims,
    path: &str,
    scan_trash: bool,
    mut op: impl FnMut(
        Arc<AppState>,
        Claims,
        String,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send>>,
) -> Result<(PosixNodeKind, usize), AppError> {
    let fragment = claims.fragment.clone();
    match classify_path(&fragment, path, scan_trash).await? {
        PathClassification::File(_) => {
            op(state, claims, path.to_string()).await?;
            Ok((PosixNodeKind::File, 1))
        }
        PathClassification::Directory { .. } => {
            let prefix = directory_prefix(path).expect("directory prefix");
            let objects = list_all_objects_under_prefix(&fragment, &prefix, scan_trash).await?;
            if objects.is_empty() {
                return Err(AppError::NotFound);
            }
            let count = objects.len();
            for object in objects {
                op(
                    Arc::clone(&state),
                    clone_claims(&claims),
                    object.object_key.clone(),
                )
                .await?;
            }
            Ok((PosixNodeKind::Directory, count))
        }
        PathClassification::Conflict => Err(AppError::Conflict(
            "path exists as both a file and a directory in flat N1 storage".to_string(),
        )),
        PathClassification::Missing | PathClassification::Root => Err(AppError::NotFound),
    }
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
    let (kind, affected) =
        batch_apply_over_prefix(state, claims, &path, false, |state, claims, object_key| {
            Box::pin(soft_delete_exact_object(state, claims, object_key))
        })
        .await?;
    Ok(PosixMutationResponse {
        operation: "trash".to_string(),
        kind,
        affected,
        path: Some(path),
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: None,
        deleted_at_unix: None,
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
    let (kind, affected) =
        batch_apply_over_prefix(state, claims, &path, true, |state, claims, object_key| {
            Box::pin(recover_exact_object(state, claims, object_key))
        })
        .await?;
    Ok(PosixMutationResponse {
        operation: "recover".to_string(),
        kind,
        affected,
        path: Some(path),
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: None,
        deleted_at_unix: None,
    })
}

pub async fn permanently_delete_path(
    state: Arc<AppState>,
    claims: Claims,
    path: String,
) -> Result<PosixMutationResponse, AppError> {
    let path = normalize_posix_path(Some(path.as_str()))?;
    if path.is_empty() {
        return Err(AppError::BadRequest(
            "root path cannot be permanently deleted".to_string(),
        ));
    }
    let (kind, affected) =
        batch_apply_over_prefix(state, claims, &path, false, |state, claims, object_key| {
            Box::pin(permanently_delete_exact_object(state, claims, object_key))
        })
        .await?;
    Ok(PosixMutationResponse {
        operation: "permanent_delete".to_string(),
        kind,
        affected,
        path: Some(path),
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: None,
        deleted_at_unix: None,
    })
}
