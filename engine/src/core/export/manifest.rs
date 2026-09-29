use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

use crate::core::disk_manager::DownloadArchiveRequest;
use crate::core::index;
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceEntryKind;
use crate::namespace::paths::normalize_path;
use crate::namespace::runtime::{NamespaceObjectRef, global_namespace_runtime};

use super::builder::ActiveProgress;
use super::models::ExportManifestEntry;
use super::store::{manifest_partial_path, manifest_path};

const MANIFEST_PAGE_SIZE: usize = 1000;
const MAX_ARCHIVE_PATH_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy)]
pub(super) struct ManifestSummary {
    pub objects_total: u64,
    pub source_bytes_total: u64,
}

struct ManifestSourceRef {
    object_ref: NamespaceObjectRef,
    archive_path: String,
    offset_bytes: u64,
    length_bytes: Option<u64>,
}

impl ManifestSourceRef {
    fn whole_object(object_ref: NamespaceObjectRef) -> Self {
        Self {
            archive_path: object_ref.path.clone(),
            object_ref,
            offset_bytes: 0,
            length_bytes: None,
        }
    }
}

pub(super) fn validate_archive_path(path: &str) -> Result<(), AppError> {
    if path.is_empty()
        || path.len() > MAX_ARCHIVE_PATH_BYTES
        || path.starts_with('/')
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(AppError::BadRequest(format!(
            "object path '{path}' cannot be represented safely in an archive"
        )));
    }
    Ok(())
}

fn normalized_key(value: &str) -> Result<Option<String>, AppError> {
    let normalized =
        normalize_path(value).map_err(|error| AppError::BadRequest(error.to_string()))?;
    if normalized.is_empty() {
        Ok(None)
    } else {
        validate_archive_path(&normalized)?;
        Ok(Some(normalized))
    }
}

fn normalized_prefixes(request: &DownloadArchiveRequest) -> Result<Vec<String>, AppError> {
    if request.export_all {
        return Ok(vec![String::new()]);
    }
    let mut prefixes = request
        .prefix
        .iter()
        .chain(request.prefixes.iter())
        .filter_map(|value| normalized_key(value).transpose())
        .collect::<Result<Vec<_>, _>>()?;
    prefixes.sort();
    prefixes.dedup();

    let mut roots: Vec<String> = Vec::new();
    'candidate: for prefix in prefixes {
        for root in &roots {
            if prefix == *root
                || prefix
                    .strip_prefix(root)
                    .is_some_and(|rest| rest.starts_with('/'))
            {
                continue 'candidate;
            }
        }
        roots.push(prefix);
    }
    Ok(roots)
}

async fn explicit_object_ref(fragment: &str, path: &str) -> Result<NamespaceObjectRef, AppError> {
    let entry = global_namespace_runtime()
        .entry(fragment, path)
        .await?
        .ok_or(AppError::NotFound)?;
    if entry.kind != NamespaceEntryKind::File || !entry.lifecycle.is_live() {
        return Err(AppError::NotFound);
    }
    let object_id = entry.object_id.ok_or_else(|| {
        AppError::InternalServerError(format!(
            "N2 namespace export entry '{path}' has no object identity"
        ))
    })?;
    let version_id = entry.version_id.ok_or_else(|| {
        AppError::InternalServerError(format!(
            "N2 namespace export entry '{path}' has no live version identity"
        ))
    })?;
    Ok(NamespaceObjectRef {
        path: path.to_string(),
        object_id,
        version_id,
    })
}

async fn validate_object_ref(
    fragment: String,
    source: ManifestSourceRef,
) -> Result<ExportManifestEntry, AppError> {
    validate_archive_path(&source.archive_path)?;
    let metadata = index::read_object_version(
        &fragment,
        &source.object_ref.object_id,
        &source.object_ref.version_id,
    )
    .await?;
    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }
    if metadata.version_id != source.object_ref.version_id {
        return Err(AppError::InternalServerError(format!(
            "N2 export version changed while capturing '{}'",
            source.archive_path
        )));
    }
    let length_bytes = source.length_bytes.unwrap_or_else(|| {
        metadata
            .total_size_bytes
            .saturating_sub(source.offset_bytes)
    });
    let end = source
        .offset_bytes
        .checked_add(length_bytes)
        .ok_or_else(|| {
            AppError::BadRequest("mapped export byte range overflows u64".to_string())
        })?;
    if source.offset_bytes > metadata.total_size_bytes || end > metadata.total_size_bytes {
        return Err(AppError::BadRequest(format!(
            "mapped export range for '{}' exceeds its immutable object version",
            source.archive_path
        )));
    }
    Ok(ExportManifestEntry {
        archive_path: source.archive_path,
        object_id: source.object_ref.object_id,
        version_id: source.object_ref.version_id,
        source_offset_bytes: source.offset_bytes,
        size_bytes: length_bytes,
    })
}

async fn append_manifest_entry(
    file: &mut File,
    entry: &ExportManifestEntry,
    summary: &mut ManifestSummary,
) -> Result<(), AppError> {
    let mut encoded = serde_json::to_vec(entry).map_err(|error| {
        AppError::InternalServerError(format!("serialize N2 export manifest entry: {error}"))
    })?;
    encoded.push(b'\n');
    file.write_all(&encoded)
        .await
        .map_err(|error| AppError::Io(error, "write N2 export manifest".to_string()))?;
    summary.objects_total = summary.objects_total.saturating_add(1);
    summary.source_bytes_total = summary.source_bytes_total.saturating_add(entry.size_bytes);
    Ok(())
}

async fn collect_validated_entry(
    tasks: &mut JoinSet<Result<ExportManifestEntry, AppError>>,
    file: &mut File,
    summary: &mut ManifestSummary,
) -> Result<(), AppError> {
    let entry = tasks
        .join_next()
        .await
        .ok_or_else(|| {
            AppError::InternalServerError("N2 export validation task disappeared".to_string())
        })?
        .map_err(|error| {
            AppError::InternalServerError(format!("N2 export validation task failed: {error}"))
        })??;
    append_manifest_entry(file, &entry, summary).await
}

async fn submit_validation(
    tasks: &mut JoinSet<Result<ExportManifestEntry, AppError>>,
    fragment: &str,
    source: ManifestSourceRef,
    concurrency: usize,
    file: &mut File,
    summary: &mut ManifestSummary,
) -> Result<(), AppError> {
    while tasks.len() >= concurrency.max(1) {
        collect_validated_entry(tasks, file, summary).await?;
    }
    let fragment = fragment.to_string();
    tasks.spawn(async move { validate_object_ref(fragment, source).await });
    Ok(())
}

async fn sync_parent(path: &Path) -> Result<(), AppError> {
    let parent = path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("export manifest has no parent".to_string()))?
        .to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(parent).and_then(|directory| directory.sync_all())
    })
    .await
    .map_err(|error| AppError::InternalServerError(format!("sync export manifest task: {error}")))?
    .map_err(|error| AppError::Io(error, "sync N2 export manifest parent".to_string()))
}

pub(super) async fn capture_manifest(
    id: &str,
    fragment: &str,
    request: &DownloadArchiveRequest,
    validation_concurrency: usize,
    progress: Arc<ActiveProgress>,
) -> Result<ManifestSummary, AppError> {
    progress.ensure_running()?;
    let partial_path = manifest_partial_path(id)?;
    let final_path = manifest_path(id)?;
    if let Some(parent) = partial_path.parent() {
        fs::create_dir_all(parent).await.map_err(|error| {
            AppError::Io(error, "create N2 export manifest directory".to_string())
        })?;
    }
    let _ = fs::remove_file(&partial_path).await;
    let mut file = File::create(&partial_path)
        .await
        .map_err(|error| AppError::Io(error, "create N2 export manifest".to_string()))?;
    let mut summary = ManifestSummary {
        objects_total: 0,
        source_bytes_total: 0,
    };

    let mut validation_tasks = JoinSet::new();
    if request.entries.is_empty() {
        let mut explicit_paths = Vec::new();
        let mut explicit_set = HashSet::new();
        if !request.export_all {
            for value in &request.keys {
                let Some(path) = normalized_key(value)? else {
                    continue;
                };
                if explicit_set.insert(path.clone()) {
                    explicit_paths.push(path);
                }
            }
        }
        let prefixes = normalized_prefixes(request)?;
        if explicit_paths.is_empty() && prefixes.is_empty() {
            return Err(AppError::BadRequest(
                "export requires object keys, mapped entries, prefixes, or export_all=true"
                    .to_string(),
            ));
        }

        for path in explicit_paths {
            progress.ensure_running()?;
            let object_ref = explicit_object_ref(fragment, &path).await?;
            submit_validation(
                &mut validation_tasks,
                fragment,
                ManifestSourceRef::whole_object(object_ref),
                validation_concurrency,
                &mut file,
                &mut summary,
            )
            .await?;
        }

        for prefix in prefixes {
            progress.ensure_running()?;
            let query_prefix = if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            };
            let mut cursor = None;
            loop {
                progress.ensure_running()?;
                let page = global_namespace_runtime()
                    .list_live_object_refs_page(
                        fragment,
                        if query_prefix.is_empty() {
                            None
                        } else {
                            Some(query_prefix.as_str())
                        },
                        cursor.as_deref(),
                        MANIFEST_PAGE_SIZE,
                    )
                    .await?;
                for object_ref in page.entries {
                    if explicit_set.contains(&object_ref.path) {
                        continue;
                    }
                    submit_validation(
                        &mut validation_tasks,
                        fragment,
                        ManifestSourceRef::whole_object(object_ref),
                        validation_concurrency,
                        &mut file,
                        &mut summary,
                    )
                    .await?;
                }
                let Some(next) = page.continuation_token else {
                    break;
                };
                cursor = Some(next);
            }
        }
    } else {
        let mut archive_paths = HashSet::new();
        for entry in &request.entries {
            progress.ensure_running()?;
            let source_path = normalized_key(&entry.object_key)?.ok_or_else(|| {
                AppError::BadRequest("mapped export object_key cannot be empty".to_string())
            })?;
            let archive_path = normalized_key(&entry.archive_path)?.ok_or_else(|| {
                AppError::BadRequest("mapped export archive_path cannot be empty".to_string())
            })?;
            if !archive_paths.insert(archive_path.clone()) {
                return Err(AppError::BadRequest(format!(
                    "mapped export contains duplicate archive path '{archive_path}'"
                )));
            }
            let mut object_ref = explicit_object_ref(fragment, &source_path).await?;
            if let Some(version_id) = entry
                .version_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                object_ref.version_id = version_id.to_string();
            }
            submit_validation(
                &mut validation_tasks,
                fragment,
                ManifestSourceRef {
                    object_ref,
                    archive_path,
                    offset_bytes: entry.offset_bytes.unwrap_or(0),
                    length_bytes: entry.length_bytes,
                },
                validation_concurrency,
                &mut file,
                &mut summary,
            )
            .await?;
        }
    }

    while !validation_tasks.is_empty() {
        progress.ensure_running()?;
        collect_validated_entry(&mut validation_tasks, &mut file, &mut summary).await?;
    }

    file.flush()
        .await
        .map_err(|error| AppError::Io(error, "flush N2 export manifest".to_string()))?;
    file.sync_all()
        .await
        .map_err(|error| AppError::Io(error, "sync N2 export manifest".to_string()))?;
    drop(file);
    fs::rename(&partial_path, &final_path)
        .await
        .map_err(|error| AppError::Io(error, "publish N2 export manifest".to_string()))?;
    sync_parent(&final_path).await?;
    Ok(summary)
}
