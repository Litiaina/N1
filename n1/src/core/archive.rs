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
    },
    enums::app_error_enum::AppError,
    structs::auth_structs::Claims,
};

const MAX_ARCHIVE_KEYS: usize = 10_000;
const ARCHIVE_STREAM_CHANNEL: usize = 32;
const ARCHIVE_DUPLEX_BYTES: usize = 1024 * 1024;
const ARCHIVE_CHUNK_REQ_SIZE: usize = 256 * 1024;
const ARCHIVE_SCAN_PAGE_LIMIT: usize = 1000;

#[derive(Clone)]
pub struct PreparedArchiveEntry {
    pub archive_path: String,
    pub metadata: ObjectMetadata,
}

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
    PreparedEntries(Vec<PreparedArchiveEntry>),
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
        ArchiveSelection::PreparedEntries(entries) => Some(entries.len()),
    };

    let (effective_format, content_type, disposition) =
        if selected_key_count.is_none() || selected_key_count.unwrap_or_default() > 1000 {
            (
                "tar",
                "application/x-tar",
                format!("attachment; filename=\"{}.tar\"", filename),
            )
        } else {
            match req.format.as_str() {
                "tar" => (
                    "tar",
                    "application/x-tar",
                    format!("attachment; filename=\"{}.tar\"", filename),
                ),
                _ => (
                    "zip",
                    "application/zip",
                    format!("attachment; filename=\"{}.zip\"", filename),
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

pub async fn download_prepared_archive(
    entries: Vec<PreparedArchiveEntry>,
    req: DownloadArchiveRequest,
) -> Result<Response, AppError> {
    let filename = req.filename.clone().unwrap_or_else(|| "export".to_string());
    let selected_key_count = Some(entries.len());

    let (effective_format, content_type, disposition) =
        if selected_key_count.is_none() || selected_key_count.unwrap_or_default() > 1000 {
            (
                "tar",
                "application/x-tar",
                format!("attachment; filename=\"{}.tar\"", filename),
            )
        } else {
            match req.format.as_str() {
                "tar" => (
                    "tar",
                    "application/x-tar",
                    format!("attachment; filename=\"{}.tar\"", filename),
                ),
                _ => (
                    "zip",
                    "application/zip",
                    format!("attachment; filename=\"{}.zip\"", filename),
                ),
            }
        };

    let stream = stream_archive(
        String::new(),
        ArchiveSelection::PreparedEntries(entries),
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
            format!("attachment; filename=\"{}.tar\"", filename),
        )
        .body(Body::from_stream(stream))
        .unwrap())
}

fn canonical_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
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
        std::env::temp_dir().join(format!("n1-archive-manifest-{}.lst", Uuid::new_v4()));
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

    let metadata = match version_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(version_id) => {
            match get_version_metadata(fragment, &clean_key, version_id, fragment).await {
                Ok(m) if !m.is_delete_marker => m,
                _ => return Ok(None),
            }
        }
        None => match get_metadata(fragment, &clean_key, fragment).await {
            Ok(m) if !m.is_delete_marker => m,
            _ => return Ok(None),
        },
    };

    Ok(Some((clean_key, metadata)))
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
        ArchiveSelection::PreparedEntries(entries) => {
            for entry in entries {
                let mut reader = create_multipart_reader(&entry.metadata);
                let builder = ZipEntryBuilder::new(entry.archive_path.into(), Compression::Stored);

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
            let mut emitted_paths = HashSet::new();
            for key in keys {
                let Some((archive_path, metadata)) =
                    load_archive_metadata_for_key(fragment, &key).await?
                else {
                    continue;
                };
                if !emitted_paths.insert(archive_path.clone()) {
                    continue;
                }

                let mut header = TarHeader::new_gnu();
                header.set_size(metadata.total_size_bytes);
                header.set_mode(0o644);

                let reader = create_multipart_reader(&metadata);

                tar.append_data(&mut header, archive_path, reader)
                    .await
                    .map_err(|e| AppError::Io(e, "tar append".into()))?;
            }

            for prefix in prefixes {
                let mut continuation_token = None;
                loop {
                    let page = index::scan_fragment_live_metadata_page(
                        fragment,
                        Some(prefix.clone()),
                        None,
                        ARCHIVE_SCAN_PAGE_LIMIT,
                        continuation_token.clone(),
                        include_internal,
                    )
                    .await?;

                    for metadata in page.objects {
                        if metadata.is_delete_marker {
                            continue;
                        }

                        let archive_path = canonical_key(&metadata.object_key);
                        if !emitted_paths.insert(archive_path.clone()) {
                            continue;
                        }

                        let mut header = TarHeader::new_gnu();
                        header.set_size(metadata.total_size_bytes);
                        header.set_mode(0o644);

                        let reader = create_multipart_reader(&metadata);

                        tar.append_data(&mut header, archive_path, reader)
                            .await
                            .map_err(|e| AppError::Io(e, "tar append".into()))?;
                    }

                    if page.continuation_token.is_none() {
                        break;
                    }
                    continuation_token = page.continuation_token;
                }
            }
        }
        ArchiveSelection::FragmentAll { include_internal } => {
            let mut continuation_token = None;

            loop {
                let page = index::scan_fragment_live_metadata_page(
                    fragment,
                    None,
                    None,
                    ARCHIVE_SCAN_PAGE_LIMIT,
                    continuation_token.clone(),
                    include_internal,
                )
                .await?;

                for metadata in page.objects {
                    if metadata.is_delete_marker {
                        continue;
                    }

                    let mut header = TarHeader::new_gnu();
                    header.set_size(metadata.total_size_bytes);
                    header.set_mode(0o644);

                    let archive_path = canonical_key(&metadata.object_key);
                    let reader = create_multipart_reader(&metadata);

                    tar.append_data(&mut header, archive_path, reader)
                        .await
                        .map_err(|e| AppError::Io(e, "tar append".into()))?;
                }

                if page.continuation_token.is_none() {
                    break;
                }
                continuation_token = page.continuation_token;
            }
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
        ArchiveSelection::PreparedEntries(entries) => {
            for entry in entries {
                let mut header = TarHeader::new_gnu();
                header.set_size(entry.metadata.total_size_bytes);
                header.set_mode(0o644);

                let reader = create_multipart_reader(&entry.metadata);
                tar.append_data(&mut header, entry.archive_path, reader)
                    .await
                    .map_err(|e| AppError::Io(e, "tar append".into()))?;
            }
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
