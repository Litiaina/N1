use std::collections::BTreeSet;

use crate::core::archive::PreparedArchiveEntry;
use crate::core::disk_manager::DownloadArchiveRequest;
use crate::core::retrieve::get_object_metadata_direct;
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceEntryKind;
use crate::namespace::paths::normalize_path;
use crate::namespace::runtime::global_namespace_runtime;

pub async fn prepare_logical_archive_entries(
    fragment: &str,
    req: &DownloadArchiveRequest,
) -> Result<Option<Vec<PreparedArchiveEntry>>, AppError> {
    if req.export_all {
        return Ok(None);
    }

    let has_prefixes = req
        .prefix
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || !req.prefixes.is_empty();
    if req.keys.is_empty() && !has_prefixes {
        return Ok(None);
    }

    let mut prepared = Vec::new();
    let mut seen_paths = BTreeSet::new();

    for logical_key in &req.keys {
        if let Some(entry) = prepare_logical_archive_entry(fragment, logical_key).await? {
            if seen_paths.insert(entry.archive_path.clone()) {
                prepared.push(entry);
            }
        }
    }

    for prefix in archive_prefixes(req)? {
        for logical_path in
            collect_logical_file_paths(fragment, &prefix, req.include_internal).await?
        {
            if let Some(entry) = prepare_logical_archive_entry(fragment, &logical_path).await? {
                if seen_paths.insert(entry.archive_path.clone()) {
                    prepared.push(entry);
                }
            }
        }
    }

    Ok(Some(prepared))
}

fn archive_prefixes(req: &DownloadArchiveRequest) -> Result<Vec<String>, AppError> {
    let mut prefixes = Vec::new();
    let mut seen = BTreeSet::new();

    for prefix in req
        .prefix
        .iter()
        .cloned()
        .chain(req.prefixes.iter().cloned())
    {
        let canonical = normalize_archive_prefix(&prefix)?;
        if canonical.is_empty() {
            continue;
        }
        if seen.insert(canonical.clone()) {
            prefixes.push(canonical);
        }
    }

    Ok(prefixes)
}

async fn collect_logical_file_paths(
    fragment: &str,
    prefix: &str,
    include_internal: bool,
) -> Result<Vec<String>, AppError> {
    let mut pending_dirs = vec![prefix.to_string()];
    let mut files = Vec::new();
    let mut seen_dirs = BTreeSet::new();

    while let Some(dir) = pending_dirs.pop() {
        if !seen_dirs.insert(dir.clone()) {
            continue;
        }
        let entries = global_namespace_runtime()
            .list_dir(fragment, &dir, false)
            .await?;
        for entry in entries {
            if !include_internal && is_internal_namespace_path(&entry.path) {
                continue;
            }
            match entry.kind {
                NamespaceEntryKind::File if entry.lifecycle.is_live() => files.push(entry.path),
                NamespaceEntryKind::Directory if entry.lifecycle.is_live() => {
                    pending_dirs.push(entry.path);
                }
                _ => {}
            }
        }
    }

    files.sort();
    files.dedup();
    Ok(files)
}

async fn prepare_logical_archive_entry(
    fragment: &str,
    logical_path: &str,
) -> Result<Option<PreparedArchiveEntry>, AppError> {
    let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
    if normalized.is_empty() {
        return Ok(None);
    }

    let Some(entry) = global_namespace_runtime()
        .entry(fragment, &normalized)
        .await?
    else {
        return Ok(None);
    };
    if entry.kind != NamespaceEntryKind::File || !entry.lifecycle.is_live() {
        return Ok(None);
    }
    let Some(object_id) = entry.object_id else {
        return Err(AppError::InternalServerError(format!(
            "logical file '{}' is missing physical object id",
            normalized
        )));
    };

    let metadata = match get_object_metadata_direct(fragment, &object_id).await {
        Ok(metadata) => metadata,
        Err(AppError::NotFound) => return Ok(None),
        Err(err) => return Err(err),
    };

    Ok(Some(PreparedArchiveEntry {
        archive_path: normalized,
        metadata,
    }))
}

fn normalize_archive_prefix(prefix: &str) -> Result<String, AppError> {
    normalize_path(prefix).map_err(namespace_error_to_app)
}

fn is_internal_namespace_path(path: &str) -> bool {
    path.split('/')
        .any(|segment| segment == ".n1fsd_dir" || segment == ".n1-trash")
}

fn namespace_error_to_app(error: crate::namespace::models::NamespaceError) -> AppError {
    match error {
        crate::namespace::models::NamespaceError::InvalidPath(message) => {
            AppError::BadRequest(message)
        }
        crate::namespace::models::NamespaceError::Conflict(message) => AppError::Conflict(message),
        crate::namespace::models::NamespaceError::NotFound(_) => AppError::NotFound,
        crate::namespace::models::NamespaceError::Invariant(message) => {
            AppError::InternalServerError(message)
        }
    }
}
