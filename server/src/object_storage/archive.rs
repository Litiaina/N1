use axum::{
    body::Body,
    extract::Path,
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use futures_util::Stream;
use tokio::{io::DuplexStream, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use tokio_util::io::{ReaderStream, StreamReader};

use async_zip::{tokio::write::ZipFileWriter, Compression, ZipEntryBuilder};
use base64::engine::general_purpose;
use base64::Engine;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio_tar::{Builder as TarBuilder, Header as TarHeader};
use walkdir::WalkDir;

use crate::{
    enums::app_error_enum::AppError,
    object_storage::constants::{METADATA_EXT, PART_META_SUFFIX, QUOTA_FILE, RESERVATION_FILE},
    object_storage::disk_manager::{
        DownloadArchiveRequest, ObjectMetadata, ObjectPathManager, ObjectSummary, PartMetadata,
        ShardInfo,
    },
    object_storage::{metadata_parser::get_metadata, recovery::read_or_recover_chunk},
    structs::auth_structs::Claims,
};

const MAX_ARCHIVE_KEYS: usize = 100_000;
const DEFAULT_ARCHIVE_KEYS: usize = 10_000;
const ARCHIVE_STREAM_CHANNEL: usize = 8;
const ARCHIVE_DUPLEX_BYTES: usize = 64 * 1024;
const ARCHIVE_CHUNK_REQ_SIZE: usize = 64 * 1024;
const ARCHIVE_NEXT_TOKEN_HEADER: &str = "x-n1-archive-next-token";

pub async fn download_selected_archive(
    claims: Claims,
    Path(fragment): Path<String>,
    Json(req): Json<DownloadArchiveRequest>,
) -> Result<impl IntoResponse, AppError> {
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let selection = resolve_archive_keys(&fragment, &req).await?;
    let keys = selection.keys;
    if keys.is_empty() {
        return Err(AppError::BadRequest(
            "No object keys resolved from request".into(),
        ));
    }

    let filename = req.filename.clone().unwrap_or_else(|| "export".to_string());

    let (effective_format, content_type, disposition) = if keys.len() > 1000 {
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

    let stream = stream_selected_objects(fragment, keys, effective_format.to_string()).await?;

    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, disposition);

    if let Some(next) = selection.next_continuation_token {
        builder = builder.header(ARCHIVE_NEXT_TOKEN_HEADER, next);
    }

    Ok(builder.body(Body::from_stream(stream)).unwrap())
}

#[derive(Debug, Clone)]
struct ArchiveSelection {
    keys: Vec<String>,
    next_continuation_token: Option<String>,
}

fn canonical_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

fn canonical_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn normalize_delimiter(delimiter: Option<&str>) -> Option<&str> {
    delimiter.filter(|d| *d == "/")
}

fn matches_prefix_with_delimiter(key: &str, prefix: &str, delimiter: Option<&str>) -> bool {
    let ck = canonical_key(key);
    let pfx = canonical_prefix(prefix);

    if pfx.is_empty() {
        return true;
    }

    if delimiter != Some("/") {
        return ck == pfx || ck.starts_with(&(pfx + "/"));
    }

    if ck == pfx {
        return true;
    }

    if !ck.starts_with(&(pfx.clone() + "/")) {
        return false;
    }

    let rel = &ck[(pfx.len() + 1)..];
    !rel.contains('/')
}

async fn resolve_keys_from_prefix_index(
    fragment: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
) -> Result<Vec<String>, AppError> {
    let path_manager = ObjectPathManager::new();
    let pfx = canonical_prefix(prefix);
    let mut latest: HashMap<String, (String, i64)> = HashMap::new();
    let mut had_any_index_data = false;

    let bucket_dirs = path_manager.get_all_prefix_bucket_dirs(fragment, &pfx);
    for d in bucket_dirs {
        if !tokio::fs::try_exists(&d).await.unwrap_or(false) {
            continue;
        }

        let compacted_path = d.join(format!(
            "compacted{}",
            crate::object_storage::constants::PREFIX_SEGMENT_EXT
        ));
        if tokio::fs::try_exists(&compacted_path)
            .await
            .unwrap_or(false)
        {
            if let Ok(data) = tokio::fs::read(&compacted_path).await {
                if let Ok(seg) = rmp_serde::from_slice::<
                    crate::object_storage::disk_manager::PrefixIndexSegment,
                >(&data)
                {
                    had_any_index_data = true;
                    if canonical_prefix(&seg.prefix) == pfx {
                        for e in seg.entries {
                            let ck = canonical_key(&e.object_key);
                            if !matches_prefix_with_delimiter(&ck, &pfx, delimiter) {
                                continue;
                            }
                            match latest.get(&ck) {
                                None => {
                                    latest.insert(ck, (e.version_id.clone(), e.created_at));
                                }
                                Some((cur_vid, cur_ts)) => {
                                    if e.created_at > *cur_ts
                                        || (e.created_at == *cur_ts && e.version_id > *cur_vid)
                                    {
                                        latest.insert(ck, (e.version_id.clone(), e.created_at));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut seg_files = Vec::new();
        if let Ok(mut entries) = tokio::fs::read_dir(&d).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let is_file = entry
                    .file_type()
                    .await
                    .map(|t| t.is_file())
                    .unwrap_or(false);
                if !is_file {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("seg_")
                    && name.ends_with(crate::object_storage::constants::PREFIX_SEGMENT_EXT)
                {
                    seg_files.push((name, entry.path()));
                }
            }
        }
        seg_files.sort_by(|a, b| a.0.cmp(&b.0));

        for (_name, path) in seg_files {
            if let Ok(data) = tokio::fs::read(&path).await {
                if let Ok(seg) = rmp_serde::from_slice::<
                    crate::object_storage::disk_manager::PrefixIndexSegment,
                >(&data)
                {
                    had_any_index_data = true;
                    if canonical_prefix(&seg.prefix) != pfx {
                        continue;
                    }
                    for e in seg.entries {
                        let ck = canonical_key(&e.object_key);
                        if !matches_prefix_with_delimiter(&ck, &pfx, delimiter) {
                            continue;
                        }
                        match latest.get(&ck) {
                            None => {
                                latest.insert(ck, (e.version_id.clone(), e.created_at));
                            }
                            Some((cur_vid, cur_ts)) => {
                                if e.created_at > *cur_ts
                                    || (e.created_at == *cur_ts && e.version_id > *cur_vid)
                                {
                                    latest.insert(ck, (e.version_id.clone(), e.created_at));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if !had_any_index_data {
        return Ok(Vec::new());
    }

    let mut entries: Vec<(String, String, i64)> =
        latest.into_iter().map(|(k, (v, ts))| (k, v, ts)).collect();
    entries.sort_by(|a, b| match a.0.cmp(&b.0) {
        std::cmp::Ordering::Equal => a.1.cmp(&b.1),
        other => other,
    });

    let mut out = Vec::new();
    for (k, v, _ts) in entries {
        if out.len() >= max_keys {
            break;
        }
        if let Ok(meta) = get_metadata(fragment, &k, fragment).await {
            if !meta.is_delete_marker && meta.version_id == v {
                out.push(k);
            }
        }
    }
    Ok(out)
}

async fn resolve_keys_from_prefix_scan(
    fragment: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
) -> Result<Vec<String>, AppError> {
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();
    let mut latest: HashMap<String, (String, i64)> = HashMap::new();

    for root in index_roots {
        let idx = root.join(fragment).join("index");
        if !tokio::fs::try_exists(&idx).await.unwrap_or(false) {
            continue;
        }

        for entry in WalkDir::new(&idx).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }
            let p = entry.path();
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.ends_with(METADATA_EXT) {
                continue;
            }
            if name == QUOTA_FILE || name == RESERVATION_FILE || name.ends_with(PART_META_SUFFIX) {
                continue;
            }

            let Ok(raw) = std::fs::read(p) else {
                continue;
            };
            let Ok(meta) = rmp_serde::from_slice::<ObjectSummary>(&raw) else {
                continue;
            };
            if meta.fragment != fragment || meta.is_delete_marker {
                continue;
            }
            let ck = canonical_key(&meta.object_key);
            if !matches_prefix_with_delimiter(&ck, prefix, delimiter) {
                continue;
            }

            match latest.get(&ck) {
                None => {
                    latest.insert(ck, (meta.version_id, meta.created_at));
                }
                Some((cur_vid, cur_ts)) => {
                    if meta.created_at > *cur_ts
                        || (meta.created_at == *cur_ts && meta.version_id > *cur_vid)
                    {
                        latest.insert(ck, (meta.version_id, meta.created_at));
                    }
                }
            }
        }
    }

    let mut keys: Vec<String> = latest.into_keys().collect();
    keys.sort();
    keys.truncate(max_keys);
    Ok(keys)
}

fn parse_archive_continuation_token(token: &str) -> Result<String, AppError> {
    let raw = general_purpose::STANDARD
        .decode(token)
        .map_err(|_| AppError::BadRequest("Invalid archive continuation token".into()))?;
    let s = String::from_utf8(raw)
        .map_err(|_| AppError::BadRequest("Invalid archive continuation token".into()))?;
    let mut it = s.splitn(3, '|');
    let tag = it.next().unwrap_or("");
    let key = it.next().unwrap_or("");
    if tag != "archpfx" || key.is_empty() {
        return Err(AppError::BadRequest(
            "Invalid archive continuation token".into(),
        ));
    }
    Ok(canonical_key(key))
}

fn create_archive_continuation_token(last_key: &str) -> String {
    general_purpose::STANDARD.encode(format!("archpfx|{}", canonical_key(last_key)))
}

fn apply_archive_pagination(
    mut keys: Vec<String>,
    max_keys: usize,
    continuation_token: Option<&str>,
) -> Result<ArchiveSelection, AppError> {
    keys.sort();
    keys.dedup();

    let start_idx = if let Some(tok) = continuation_token {
        let after_key = parse_archive_continuation_token(tok)?;
        match keys.binary_search(&after_key) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    } else {
        0
    };

    if start_idx >= keys.len() {
        return Ok(ArchiveSelection {
            keys: Vec::new(),
            next_continuation_token: None,
        });
    }

    let end_idx = (start_idx + max_keys).min(keys.len());
    let page = keys[start_idx..end_idx].to_vec();
    let next = if end_idx < keys.len() {
        page.last().map(|k| create_archive_continuation_token(k))
    } else {
        None
    };

    Ok(ArchiveSelection {
        keys: page,
        next_continuation_token: next,
    })
}

async fn resolve_archive_keys(
    fragment: &str,
    req: &DownloadArchiveRequest,
) -> Result<ArchiveSelection, AppError> {
    let max_keys = req
        .max_keys
        .unwrap_or(DEFAULT_ARCHIVE_KEYS)
        .clamp(1, MAX_ARCHIVE_KEYS);

    if !req.keys.is_empty() {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for k in &req.keys {
            let ck = canonical_key(k);
            if ck.is_empty() {
                continue;
            }
            if seen.insert(ck.clone()) {
                out.push(ck);
            }
            if out.len() >= max_keys {
                break;
            }
        }
        return Ok(ArchiveSelection {
            keys: out,
            next_continuation_token: None,
        });
    }

    let prefix = req
        .prefix
        .as_ref()
        .map(|p| canonical_prefix(p))
        .unwrap_or_default();
    if prefix.is_empty() {
        return Err(AppError::BadRequest(
            "Request must include non-empty keys or prefix".into(),
        ));
    }
    let delimiter = normalize_delimiter(req.delimiter.as_deref());

    let mut keys =
        resolve_keys_from_prefix_index(fragment, &prefix, delimiter, MAX_ARCHIVE_KEYS).await?;
    if keys.is_empty() {
        keys =
            resolve_keys_from_prefix_scan(fragment, &prefix, delimiter, MAX_ARCHIVE_KEYS).await?;
    }

    apply_archive_pagination(keys, max_keys, req.continuation_token.as_deref())
}

async fn stream_selected_objects(
    fragment: String,
    keys: Vec<String>,
    format: String,
) -> Result<impl Stream<Item = Result<Bytes, AppError>>, AppError> {
    let (tx, rx) = mpsc::channel::<Result<Bytes, AppError>>(ARCHIVE_STREAM_CHANNEL);

    tokio::spawn(async move {
        if let Err(e) = write_archive_stream(fragment, keys, format, tx.clone()).await {
            let _ = tx.send(Err(e)).await;
        }
    });

    Ok(ReceiverStream::new(rx))
}

async fn write_archive_stream(
    fragment: String,
    keys: Vec<String>,
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
        "tar" => write_tar_archive(&fragment, keys, writer).await?,
        _ => write_zip_archive(&fragment, keys, writer).await?,
    }

    Ok(())
}

async fn write_zip_archive(
    fragment: &str,
    keys: Vec<String>,
    writer: DuplexStream,
) -> Result<(), AppError> {
    let mut zip = ZipFileWriter::with_tokio(writer);

    for key in keys {
        let clean_key = key.trim_start_matches('/');

        let metadata = match get_metadata(fragment, clean_key, fragment).await {
            Ok(m) if !m.is_delete_marker => m,
            _ => continue,
        };

        let mut reader = create_multipart_reader(&metadata);

        let builder = ZipEntryBuilder::new(clean_key.into(), Compression::Stored);

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
    keys: Vec<String>,
    writer: DuplexStream,
) -> Result<(), AppError> {
    let mut tar = TarBuilder::new(writer);

    for key in keys {
        let clean_key = key.trim_start_matches('/');

        let metadata = match get_metadata(fragment, clean_key, fragment).await {
            Ok(m) if !m.is_delete_marker => m,
            _ => continue,
        };

        let mut header = TarHeader::new_gnu();
        header
            .set_path(clean_key)
            .map_err(|e| AppError::Io(e, "tar path".into()))?;
        header.set_size(metadata.total_size_bytes);
        header.set_mode(0o644);
        header.set_cksum();

        let reader = create_multipart_reader(&metadata);

        tar.append_data(&mut header, clean_key, reader)
            .await
            .map_err(|e| AppError::Io(e, "tar append".into()))?;
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
                return Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Corrupt metadata",
                    )),
                    state,
                ));
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
                        return Some((
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            )),
                            state,
                        ));
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
