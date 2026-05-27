use axum::{
    Json,
    body::Body,
    extract::{Path as AxumPath, Query},
    http::header,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::Stream;
use tokio::{
    fs,
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use tokio_util::io::{ReaderStream, StreamReader};

use async_zip::{Compression, ZipEntryBuilder, tokio::write::ZipFileWriter};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_tar::{Builder as TarBuilder, Header as TarHeader};
use uuid::Uuid;

use crate::{
    core::disk_manager::{
        DownloadArchiveManifestQuery, DownloadArchiveRequest, ObjectMetadata, PartMetadata,
        ShardInfo,
    },
    core::{
        index,
        metadata_parser::{get_metadata, get_version_metadata},
        recovery::read_or_recover_chunk,
        retrieve::get_object_metadata_direct,
    },
    enums::app_error_enum::AppError,
    namespace::{
        models::NamespaceEntryKind,
        paths::normalize_path,
        runtime::{NamespaceObjectRef, global_namespace_runtime},
    },
    structs::auth_structs::Claims,
};

const MAX_ARCHIVE_KEYS: usize = 10_000;
const ARCHIVE_STREAM_CHANNEL: usize = 32;
const ARCHIVE_DUPLEX_BYTES: usize = 1024 * 1024;
const ARCHIVE_CHUNK_REQ_SIZE: usize = 256 * 1024;
const ARCHIVE_SCAN_PAGE_LIMIT: usize = 1000;

enum ArchiveSelection {
    Explicit {
        keys: Vec<String>,
        prefixes: Vec<String>,
        include_internal: bool,
    },
    FragmentAll {
        include_internal: bool,
    },
    ManifestFile(PathBuf),
    LogicalNamespace {
        keys: Vec<String>,
        prefixes: Vec<String>,
        include_internal: bool,
    },
}

pub async fn download_selected_archive(
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
    Json(req): Json<DownloadArchiveRequest>,
) -> Result<impl IntoResponse, AppError> {
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let filename = req.filename.clone().unwrap_or_else(|| "export".to_string());
    let selection = resolve_archive_selection(&req)?;

    let selected_key_count = match &selection {
        ArchiveSelection::Explicit { keys, prefixes, .. } if prefixes.is_empty() => {
            Some(keys.len())
        }
        ArchiveSelection::Explicit { .. } => None,
        ArchiveSelection::FragmentAll { .. } => None,
        ArchiveSelection::ManifestFile(_) => None,
        ArchiveSelection::LogicalNamespace { prefixes, keys, .. } if prefixes.is_empty() => {
            Some(keys.len())
        }
        ArchiveSelection::LogicalNamespace { .. } => None,
    };

    let (effective_format, content_type, disposition) =
        if selected_key_count.is_none() || selected_key_count.unwrap_or_default() > 1000 {
            (
                "tar",
                "application/x-tar",
                archive_content_disposition(&filename, ".tar"),
            )
        } else {
            match req.format.as_str() {
                "tar" => (
                    "tar",
                    "application/x-tar",
                    archive_content_disposition(&filename, ".tar"),
                ),
                _ => (
                    "zip",
                    "application/zip",
                    archive_content_disposition(&filename, ".zip"),
                ),
            }
        };

    let stream = stream_archive(fragment, selection, effective_format.to_string()).await?;

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, disposition)
        .body(Body::from_stream(stream))
        .unwrap())
}

pub async fn download_logical_archive(
    fragment: String,
    req: DownloadArchiveRequest,
) -> Result<Response, AppError> {
    let filename = req.filename.clone().unwrap_or_else(|| "export".to_string());
    let (keys, prefixes) = resolve_logical_archive_selection(&req)?;
    let selected_key_count = if prefixes.is_empty() {
        Some(keys.len())
    } else {
        None
    };

    let (effective_format, content_type, disposition) =
        if selected_key_count.is_none() || selected_key_count.unwrap_or_default() > 1000 {
            (
                "tar",
                "application/x-tar",
                archive_content_disposition(&filename, ".tar"),
            )
        } else {
            match req.format.as_str() {
                "tar" => (
                    "tar",
                    "application/x-tar",
                    archive_content_disposition(&filename, ".tar"),
                ),
                _ => (
                    "zip",
                    "application/zip",
                    archive_content_disposition(&filename, ".zip"),
                ),
            }
        };

    let stream = stream_archive(
        fragment,
        ArchiveSelection::LogicalNamespace {
            keys,
            prefixes,
            include_internal: req.include_internal,
        },
        effective_format.to_string(),
    )
    .await?;

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, disposition)
        .body(Body::from_stream(stream))
        .unwrap())
}

pub async fn download_manifest_archive(
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
    Query(query): Query<DownloadArchiveManifestQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    if query.format != "tar" {
        return Err(AppError::BadRequest(
            "manifest archive currently supports tar only".into(),
        ));
    }

    let filename = query.filename.unwrap_or_else(|| "export".to_string());
    let manifest_path = persist_manifest_body(body).await?;
    let stream = stream_archive(
        fragment,
        ArchiveSelection::ManifestFile(manifest_path),
        "tar".to_string(),
    )
    .await?;

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "application/x-tar")
        .header(
            header::CONTENT_DISPOSITION,
            archive_content_disposition(&filename, ".tar"),
        )
        .body(Body::from_stream(stream))
        .unwrap())
}

fn canonical_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

fn archive_content_disposition(filename: &str, extension: &str) -> String {
    let filename = filename.trim();
    let filename = if filename.is_empty() {
        "export"
    } else {
        filename
    };
    let with_extension = if filename.to_lowercase().ends_with(&extension.to_lowercase()) {
        filename.to_string()
    } else {
        format!("{filename}{extension}")
    };
    format!("attachment; filename=\"{}\"", with_extension)
}

fn canonical_archive_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim().trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}/")
    }
}

async fn persist_manifest_body(body: Body) -> Result<PathBuf, AppError> {
    let manifest_path =
        std::env::temp_dir().join(format!("n2-archive-manifest-{}.lst", Uuid::new_v4()));
    let mut file = File::create(&manifest_path)
        .await
        .map_err(|e| AppError::Io(e, "create manifest temp file".into()))?;
    let mut stream = body.into_data_stream();
    use tokio_stream::StreamExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            AppError::InternalServerError(format!("read archive manifest body: {}", e))
        })?;
        file.write_all(&chunk)
            .await
            .map_err(|e| AppError::Io(e, "write manifest temp file".into()))?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, "flush manifest temp file".into()))?;
    Ok(manifest_path)
}

fn resolve_archive_selection(req: &DownloadArchiveRequest) -> Result<ArchiveSelection, AppError> {
    if req.export_all {
        return Ok(ArchiveSelection::FragmentAll {
            include_internal: req.include_internal,
        });
    }

    let mut seen = HashSet::new();
    let mut keys = Vec::new();

    for key in &req.keys {
        let canonical = canonical_key(key);
        if canonical.is_empty() {
            continue;
        }
        if seen.insert(canonical.clone()) {
            keys.push(canonical);
        }
        if keys.len() > MAX_ARCHIVE_KEYS {
            return Err(AppError::BadRequest(format!(
                "selected archive key count exceeds {}; use export_all tar mode for large exports",
                MAX_ARCHIVE_KEYS
            )));
        }
    }

    let mut seen_prefixes = HashSet::new();
    let mut prefixes = Vec::new();
    for prefix in req
        .prefix
        .iter()
        .cloned()
        .chain(req.prefixes.iter().cloned())
    {
        let canonical = canonical_archive_prefix(&prefix);
        if canonical.is_empty() {
            continue;
        }
        if seen_prefixes.insert(canonical.clone()) {
            prefixes.push(canonical);
        }
    }

    if keys.is_empty() && prefixes.is_empty() {
        return Err(AppError::BadRequest(
            "request must include object keys or prefixes, or set export_all=true".into(),
        ));
    }

    Ok(ArchiveSelection::Explicit {
        keys,
        prefixes,
        include_internal: req.include_internal,
    })
}

fn normalize_logical_archive_key(key: &str) -> Result<Option<String>, AppError> {
    let normalized = normalize_path(key).map_err(namespace_error_to_app)?;
    if normalized.is_empty() {
        Ok(None)
    } else {
        Ok(Some(normalized))
    }
}

fn resolve_logical_archive_selection(
    req: &DownloadArchiveRequest,
) -> Result<(Vec<String>, Vec<String>), AppError> {
    let mut seen_keys = HashSet::new();
    let mut keys = Vec::new();
    for key in &req.keys {
        let Some(normalized) = normalize_logical_archive_key(key)? else {
            continue;
        };
        if seen_keys.insert(normalized.clone()) {
            keys.push(normalized);
        }
        if keys.len() > MAX_ARCHIVE_KEYS {
            return Err(AppError::BadRequest(format!(
                "selected archive key count exceeds {}; use export_all tar mode for large exports",
                MAX_ARCHIVE_KEYS
            )));
        }
    }

    let mut seen_prefixes = HashSet::new();
    let mut prefixes = Vec::new();
    for prefix in req
        .prefix
        .iter()
        .cloned()
        .chain(req.prefixes.iter().cloned())
    {
        let Some(normalized) = normalize_logical_archive_key(&prefix)? else {
            continue;
        };
        if seen_prefixes.insert(normalized.clone()) {
            prefixes.push(normalized);
        }
    }

    prefixes.sort();
    let mut roots: Vec<String> = Vec::new();
    'outer: for prefix in prefixes {
        for root in &roots {
            if prefix == *root
                || prefix
                    .strip_prefix(root)
                    .is_some_and(|rest| rest.starts_with('/'))
            {
                continue 'outer;
            }
        }
        roots.push(prefix);
    }

    if keys.is_empty() && roots.is_empty() {
        return Err(AppError::BadRequest(
            "request must include object keys or prefixes, or set export_all=true".into(),
        ));
    }

    Ok((keys, roots))
}

fn is_internal_namespace_path(path: &str) -> bool {
    path.split('/')
        .any(|segment| segment == ".n2fsd_dir" || segment == ".n2-trash")
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

async fn stream_archive(
    fragment: String,
    selection: ArchiveSelection,
    format: String,
) -> Result<impl Stream<Item = Result<Bytes, AppError>>, AppError> {
    let (tx, rx) = mpsc::channel::<Result<Bytes, AppError>>(ARCHIVE_STREAM_CHANNEL);

    tokio::spawn(async move {
        if let Err(e) = write_archive_stream(fragment, selection, format, tx.clone()).await {
            let _ = tx.send(Err(e)).await;
        }
    });

    Ok(ReceiverStream::new(rx))
}

async fn write_archive_stream(
    fragment: String,
    selection: ArchiveSelection,
    format: String,
    tx: mpsc::Sender<Result<Bytes, AppError>>,
) -> Result<(), AppError> {
    let (reader, writer) = tokio::io::duplex(ARCHIVE_DUPLEX_BYTES);

    tokio::spawn(async move {
        let mut rs = ReaderStream::new(reader);
        use tokio_stream::StreamExt;
        while let Some(chunk) = rs.next().await {
            match chunk {
                Ok(b) => {
                    if tx.send(Ok(b)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(AppError::Io(e, "streaming archive".into())))
                        .await;
                    break;
                }
            }
        }
    });

    match format.as_str() {
        "tar" => write_tar_archive(&fragment, selection, writer).await?,
        _ => write_zip_archive(&fragment, selection, writer).await?,
    }

    Ok(())
}

async fn load_archive_metadata_for_key(
    fragment: &str,
    key: &str,
) -> Result<Option<(String, ObjectMetadata)>, AppError> {
    let clean_key = key.trim_start_matches('/').to_string();
    if let Some(found) = load_archive_metadata_for_logical_path(fragment, &clean_key, true).await? {
        return Ok(Some(found));
    }
    let metadata = match get_metadata(fragment, &clean_key, fragment).await {
        Ok(m) if !m.is_delete_marker => m,
        _ => return Ok(None),
    };
    Ok(Some((clean_key, metadata)))
}

async fn load_archive_metadata_for_manifest_entry(
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<Option<(String, ObjectMetadata)>, AppError> {
    let clean_key = canonical_key(object_key);
    if clean_key.is_empty() {
        return Ok(None);
    }

    let physical_key = global_namespace_runtime()
        .resolve_any_object_id(fragment, &clean_key)
        .await?
        .unwrap_or_else(|| clean_key.clone());

    let metadata = match version_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(version_id) => {
            match get_version_metadata(fragment, &physical_key, version_id, fragment).await {
                Ok(m) if !m.is_delete_marker => m,
                _ => return Ok(None),
            }
        }
        None => match load_archive_metadata_for_logical_path(fragment, &clean_key, true).await? {
            Some((_, metadata)) => metadata,
            None => match get_metadata(fragment, &clean_key, fragment).await {
                Ok(m) if !m.is_delete_marker => m,
                _ => return Ok(None),
            },
        },
    };

    Ok(Some((clean_key, metadata)))
}

async fn write_physical_tar_entries(
    fragment: &str,
    keys: Vec<String>,
    prefixes: Vec<String>,
    include_internal: bool,
    tar: &mut TarBuilder<DuplexStream>,
) -> Result<(), AppError> {
    let mut emitted_paths = HashSet::new();
    for key in keys {
        let Some((archive_path, metadata)) = load_archive_metadata_for_key(fragment, &key).await?
        else {
            continue;
        };
        if !include_internal && is_internal_namespace_path(&archive_path) {
            continue;
        }
        if !emitted_paths.insert(archive_path.clone()) {
            continue;
        }
        append_tar_metadata(tar, archive_path, &metadata).await?;
    }

    for prefix in prefixes {
        let mut continuation_token = None;
        loop {
            let mut params = HashMap::from([
                ("prefix".to_string(), prefix.clone()),
                ("limit".to_string(), ARCHIVE_SCAN_PAGE_LIMIT.to_string()),
                ("include_internal".to_string(), include_internal.to_string()),
            ]);
            if let Some(token) = continuation_token.take() {
                params.insert("continuation_token".to_string(), token);
            }
            let page = global_namespace_runtime()
                .list_objects_page(fragment, &params, false)
                .await?;

            for metadata in page.objects {
                let archive_path = canonical_key(&metadata.object_key);
                if !include_internal && is_internal_namespace_path(&archive_path) {
                    continue;
                }
                let Some((_, full_metadata)) =
                    load_archive_metadata_for_key(fragment, &archive_path).await?
                else {
                    continue;
                };
                if !emitted_paths.insert(archive_path.clone()) {
                    continue;
                }
                append_tar_metadata(tar, archive_path, &full_metadata).await?;
            }

            if page.continuation_token.is_none() {
                break;
            }
            continuation_token = page.continuation_token;
        }
    }

    Ok(())
}

async fn write_fragment_namespace_tar_entries(
    fragment: &str,
    include_internal: bool,
    tar: &mut TarBuilder<DuplexStream>,
) -> Result<(), AppError> {
    write_logical_namespace_tar_entries(
        fragment,
        Vec::new(),
        vec![String::new()],
        include_internal,
        tar,
    )
    .await
}

async fn load_archive_metadata_for_logical_path(
    fragment: &str,
    logical_path: &str,
    include_internal: bool,
) -> Result<Option<(String, ObjectMetadata)>, AppError> {
    let Some(normalized) = normalize_logical_archive_key(logical_path)? else {
        return Ok(None);
    };
    if !include_logical_archive_path(&normalized, include_internal) {
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

    Ok(Some((normalized, metadata)))
}

async fn load_archive_metadata_for_object_ref(
    fragment: &str,
    entry: &NamespaceObjectRef,
) -> Result<Option<(String, ObjectMetadata)>, AppError> {
    let mut metadata =
        match index::read_object_version(fragment, &entry.object_id, &entry.version_id).await {
            Ok(metadata) if !metadata.is_delete_marker => metadata,
            Ok(_) | Err(AppError::NotFound) => return Ok(None),
            Err(err) => return Err(err),
        };
    metadata.object_key = entry.path.clone();
    Ok(Some((entry.path.clone(), metadata)))
}

fn include_logical_archive_path(path: &str, include_internal: bool) -> bool {
    include_internal || !is_internal_namespace_path(path)
}

fn parse_manifest_line(line: &str) -> Option<(Option<&str>, &str)> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some((version_id, object_key)) = trimmed.split_once('\t') {
        return Some((Some(version_id), object_key));
    }
    Some((None, trimmed))
}

async fn write_zip_archive(
    fragment: &str,
    selection: ArchiveSelection,
    writer: DuplexStream,
) -> Result<(), AppError> {
    let mut zip = ZipFileWriter::with_tokio(writer);

    match selection {
        ArchiveSelection::Explicit {
            keys,
            prefixes,
            include_internal: _,
        } => {
            if !prefixes.is_empty() {
                return Err(AppError::BadRequest(
                    "zip export does not support folder selections; use tar".into(),
                ));
            }
            for key in keys {
                let Some((archive_path, metadata)) =
                    load_archive_metadata_for_key(fragment, &key).await?
                else {
                    continue;
                };

                let mut reader = create_multipart_reader(&metadata);
                let builder = ZipEntryBuilder::new(archive_path.into(), Compression::Stored);

                let mut entry_writer = zip.write_entry_stream(builder).await.map_err(|e| {
                    AppError::Io(
                        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                        "zip entry create".into(),
                    )
                })?;
                let mut compat_writer = (&mut entry_writer).compat_write();

                tokio::io::copy(&mut reader, &mut compat_writer)
                    .await
                    .map_err(|e| AppError::Io(e, "zip copy".into()))?;

                drop(compat_writer);

                entry_writer.close().await.map_err(|e| {
                    AppError::Io(
                        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                        "zip entry close".into(),
                    )
                })?;
            }
        }
        ArchiveSelection::FragmentAll { .. } => {
            return Err(AppError::BadRequest(
                "zip export does not support export_all; use tar for fragment-scale archives"
                    .into(),
            ));
        }
        ArchiveSelection::ManifestFile(_) => {
            return Err(AppError::BadRequest(
                "zip export does not support manifest mode; use tar".into(),
            ));
        }
        ArchiveSelection::LogicalNamespace {
            keys,
            prefixes,
            include_internal,
        } => {
            if !prefixes.is_empty() {
                return Err(AppError::BadRequest(
                    "zip export does not support folder selections; use tar".into(),
                ));
            }
            for key in keys {
                let Some((archive_path, metadata)) =
                    load_archive_metadata_for_logical_path(fragment, &key, include_internal)
                        .await?
                else {
                    continue;
                };

                let mut reader = create_multipart_reader(&metadata);
                let builder = ZipEntryBuilder::new(archive_path.into(), Compression::Stored);

                let mut entry_writer = zip.write_entry_stream(builder).await.map_err(|e| {
                    AppError::Io(
                        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                        "zip entry create".into(),
                    )
                })?;
                let mut compat_writer = (&mut entry_writer).compat_write();

                tokio::io::copy(&mut reader, &mut compat_writer)
                    .await
                    .map_err(|e| AppError::Io(e, "zip copy".into()))?;

                drop(compat_writer);

                entry_writer.close().await.map_err(|e| {
                    AppError::Io(
                        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                        "zip entry close".into(),
                    )
                })?;
            }
        }
    }

    zip.close().await.map_err(|e| {
        AppError::Io(
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            "zip finish".into(),
        )
    })?;
    Ok(())
}

async fn append_tar_metadata(
    tar: &mut TarBuilder<DuplexStream>,
    archive_path: String,
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    let mut header = TarHeader::new_gnu();
    header.set_size(metadata.total_size_bytes);
    header.set_mode(0o644);

    let reader = create_multipart_reader(metadata);
    tar.append_data(&mut header, archive_path, reader)
        .await
        .map_err(|e| AppError::Io(e, "tar append".into()))
}

async fn write_logical_namespace_tar_entries(
    fragment: &str,
    keys: Vec<String>,
    prefixes: Vec<String>,
    include_internal: bool,
    tar: &mut TarBuilder<DuplexStream>,
) -> Result<(), AppError> {
    let mut emitted_paths = HashSet::new();
    for key in keys {
        let Some((archive_path, metadata)) =
            load_archive_metadata_for_logical_path(fragment, &key, include_internal).await?
        else {
            continue;
        };
        if !emitted_paths.insert(archive_path.clone()) {
            continue;
        }
        append_tar_metadata(tar, archive_path, &metadata).await?;
    }

    for prefix in prefixes {
        let mut continuation_token: Option<String> = None;
        loop {
            let prefix_query = if prefix.is_empty() {
                String::new()
            } else {
                canonical_archive_prefix(&prefix)
            };
            let page = global_namespace_runtime()
                .list_live_object_refs_page(
                    fragment,
                    if prefix_query.is_empty() {
                        None
                    } else {
                        Some(prefix_query.as_str())
                    },
                    continuation_token.as_deref(),
                    ARCHIVE_SCAN_PAGE_LIMIT,
                )
                .await?;

            for entry in page.entries {
                let archive_path = canonical_key(&entry.path);
                if !include_logical_archive_path(&archive_path, include_internal) {
                    continue;
                }
                if !emitted_paths.insert(archive_path.clone()) {
                    continue;
                }
                let Some((_, metadata)) =
                    load_archive_metadata_for_object_ref(fragment, &entry).await?
                else {
                    continue;
                };
                append_tar_metadata(tar, archive_path, &metadata).await?;
            }

            let Some(next) = page.continuation_token else {
                break;
            };
            continuation_token = Some(next);
        }
    }

    Ok(())
}

async fn write_tar_archive(
    fragment: &str,
    selection: ArchiveSelection,
    writer: DuplexStream,
) -> Result<(), AppError> {
    let mut tar = TarBuilder::new(writer);

    match selection {
        ArchiveSelection::Explicit {
            keys,
            prefixes,
            include_internal,
        } => {
            write_physical_tar_entries(fragment, keys, prefixes, include_internal, &mut tar)
                .await?;
        }
        ArchiveSelection::FragmentAll { include_internal } => {
            write_fragment_namespace_tar_entries(fragment, include_internal, &mut tar).await?;
        }
        ArchiveSelection::ManifestFile(path) => {
            let result: Result<(), AppError> = async {
                let file = File::open(&path)
                    .await
                    .map_err(|e| AppError::Io(e, "open archive manifest".into()))?;
                let mut lines = BufReader::new(file).lines();
                while let Some(line) = lines
                    .next_line()
                    .await
                    .map_err(|e| AppError::Io(e, "read archive manifest".into()))?
                {
                    let Some((version_id, key)) = parse_manifest_line(&line) else {
                        continue;
                    };
                    let Some((archive_path, metadata)) =
                        load_archive_metadata_for_manifest_entry(fragment, key, version_id).await?
                    else {
                        continue;
                    };

                    let mut header = TarHeader::new_gnu();
                    header.set_size(metadata.total_size_bytes);
                    header.set_mode(0o644);

                    let reader = create_multipart_reader(&metadata);
                    tar.append_data(&mut header, archive_path, reader)
                        .await
                        .map_err(|e| AppError::Io(e, "tar append".into()))?;
                }
                Ok(())
            }
            .await;
            let _ = fs::remove_file(&path).await;
            result?;
        }
        ArchiveSelection::LogicalNamespace {
            keys,
            prefixes,
            include_internal,
        } => {
            write_logical_namespace_tar_entries(
                fragment,
                keys,
                prefixes,
                include_internal,
                &mut tar,
            )
            .await?;
        }
    }

    tar.finish()
        .await
        .map_err(|e| AppError::Io(e, "tar finish".into()))?;
    Ok(())
}

fn create_multipart_reader(
    metadata: &ObjectMetadata,
) -> StreamReader<impl Stream<Item = std::io::Result<Bytes>> + Unpin, Bytes> {
    let parts: std::collections::VecDeque<PartMetadata> = metadata.parts.clone().into();
    let stripe_size_bytes = metadata.stripe_size_bytes;

    struct ReaderState {
        parts_queue: std::collections::VecDeque<PartMetadata>,
        blob_dir: PathBuf,
        cursor_in_part: u64,
        file_cache: HashMap<String, File>,
        stripe_size: usize,
    }

    let initial_state = ReaderState {
        parts_queue: parts,
        blob_dir: PathBuf::new(),
        cursor_in_part: 0,
        file_cache: HashMap::new(),
        stripe_size: stripe_size_bytes,
    };

    let stream = futures_util::stream::unfold(initial_state, move |mut state| async move {
        let chunk_req_size = ARCHIVE_CHUNK_REQ_SIZE;
        let mut buf = Vec::with_capacity(chunk_req_size);

        loop {
            let part = match state.parts_queue.front() {
                Some(p) => p,
                None => {
                    if !buf.is_empty() {
                        return Some((Ok(Bytes::from(buf)), state));
                    }
                    return None;
                }
            };

            if state.cursor_in_part >= part.size_bytes {
                state.parts_queue.pop_front();
                state.cursor_in_part = 0;
                state.file_cache.clear();
                continue;
            }

            let bytes_left_in_part = part.size_bytes - state.cursor_in_part;
            let bytes_to_fill = chunk_req_size - buf.len();
            let bytes_to_read = std::cmp::min(bytes_left_in_part, bytes_to_fill as u64) as usize;

            if bytes_to_read == 0 {
                state.parts_queue.pop_front();
                state.cursor_in_part = 0;
                state.file_cache.clear();
                continue;
            }

            let data_shards_info: Vec<&ShardInfo> =
                part.shards.iter().filter(|s| !s.is_parity).collect();
            let data_shards_count = data_shards_info.len() as u64;

            if data_shards_count == 0 {
                return Some((Err(std::io::Error::other("Corrupt metadata")), state));
            }

            let mut temp_bytes_read = 0;
            while temp_bytes_read < bytes_to_read {
                let stripe_block_size = state.stripe_size as u64;
                let stripe_size = stripe_block_size * data_shards_count;

                let offset_in_stripe = state.cursor_in_part % stripe_size;
                let shard_index = (offset_in_stripe / stripe_block_size) as usize;
                let offset_in_block = offset_in_stripe % stripe_block_size;

                let stripe_index = state.cursor_in_part / stripe_size;
                let file_offset = (stripe_index * stripe_block_size) + offset_in_block;

                let bytes_left_in_block = stripe_block_size - offset_in_block;
                let remaining_req = bytes_to_read - temp_bytes_read;
                let chunk_len = std::cmp::min(remaining_req as u64, bytes_left_in_block) as usize;

                let part_arc = Arc::new(part.clone());

                match read_or_recover_chunk(
                    &mut state.file_cache,
                    &state.blob_dir,
                    part_arc,
                    shard_index,
                    file_offset,
                    chunk_len,
                    state.stripe_size,
                )
                .await
                {
                    Ok(data) => {
                        buf.extend_from_slice(&data);
                        state.cursor_in_part += chunk_len as u64;
                        temp_bytes_read += chunk_len;
                    }
                    Err(e) => {
                        return Some((Err(std::io::Error::other(e.to_string())), state));
                    }
                }
            }

            if buf.len() >= chunk_req_size {
                return Some((Ok(Bytes::from(buf)), state));
            }
        }
    });

    StreamReader::new(Box::pin(stream))
}
