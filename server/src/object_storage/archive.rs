use axum::{
    extract::Path,
    http::header,
    response::{IntoResponse, Response},
    Json, body::Body,
};
use bytes::Bytes;
use futures_util::Stream;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, DuplexStream},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use async_zip::{tokio::write::ZipFileWriter, Compression, ZipEntryBuilder};
use tokio_tar::{Builder as TarBuilder, Header as TarHeader};
use std::io::SeekFrom;
use std::path::{Path as StdPath, PathBuf};
use reed_solomon_simd::ReedSolomonDecoder;
use std::collections::HashMap;

use crate::{
    enums::app_error_enum::AppError,
    object_storage::{
        caching::get_metadata_with_cache, ec::STRIPE_BLOCK_SIZE,
    },
    structs::{
        auth_structs::Claims,
        object_storage_structs::{
             DownloadArchiveRequest, 
            ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo
        },
    },
};

pub fn default_format() -> String { "zip".to_string() }

pub async fn download_selected_archive(
    claims: Claims,
    Path(fragment): Path<String>,
    Json(req): Json<DownloadArchiveRequest>,
) -> Result<impl IntoResponse, AppError> {

    if req.keys.is_empty() {
        return Err(AppError::BadRequest("No object keys provided".into()));
    }

    if req.keys.len() > 10_000 {
        return Err(AppError::BadRequest("Too many objects requested".into()));
    }

    let filename = req.filename.clone().unwrap_or_else(|| "export".to_string());

    let stream = stream_selected_objects(
        claims.uid.clone(),
        fragment,
        req.keys.clone(),
        req.format.clone(),
    ).await?;

    let content_type = match req.format.as_str() {
        "tar" => "application/x-tar",
        _ => "application/zip",
    };

    let disposition = match req.format.as_str() {
        "tar" => format!("attachment; filename=\"{}.tar\"", filename),
        _ => format!("attachment; filename=\"{}.zip\"", filename),
    };

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, disposition)
        .body(Body::from_stream(stream))
        .unwrap())
}

async fn stream_selected_objects(
    uid: String,
    fragment: String,
    keys: Vec<String>,
    format: String,
) -> Result<impl Stream<Item = Result<Bytes, AppError>>, AppError> {
    let (tx, rx) = mpsc::channel::<Result<Bytes, AppError>>(8);

    tokio::spawn(async move {
        if let Err(e) = write_archive_stream(uid, fragment, keys, format, tx.clone()).await {
            let _ = tx.send(Err(e)).await;
        }
    });

    Ok(ReceiverStream::new(rx))
}

async fn write_archive_stream(
    uid: String,
    fragment: String,
    keys: Vec<String>,
    format: String,
    tx: mpsc::Sender<Result<Bytes, AppError>>,
) -> Result<(), AppError> {

    let (reader, writer) = tokio::io::duplex(64 * 1024);

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
                    let _ = tx.send(Err(AppError::Io(e, "streaming archive".into()))).await;
                    break;
                }
            }
        }
    });

    match format.as_str() {
        "tar" => write_tar_archive(&uid, &fragment, keys, writer).await?,
        _ => write_zip_archive(&uid, &fragment, keys, writer).await?,
    }

    Ok(())
}

async fn write_zip_archive(
    uid: &str,
    fragment: &str,
    keys: Vec<String>,
    writer: DuplexStream,
) -> Result<(), AppError> {
    let mut zip = ZipFileWriter::with_tokio(writer);

    for key in keys {
        let clean_key = key.trim_start_matches('/');

        let metadata = match get_metadata_with_cache(fragment, clean_key, uid).await {
            Ok(m) if !m.is_delete_marker => m,
            _ => continue,
        };

        let mut reader = create_multipart_reader(&metadata);
        
        let builder = ZipEntryBuilder::new(clean_key.into(), Compression::Stored);
        
        let mut entry_writer = zip.write_entry_stream(builder)
            .await
            .map_err(|e| AppError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()), "zip entry create".into()))?;

        let mut compat_writer = (&mut entry_writer).compat_write();

        tokio::io::copy(&mut reader, &mut compat_writer)
            .await
            .map_err(|e| AppError::Io(e, "zip copy".into()))?;
        
        drop(compat_writer);

        entry_writer.close()
            .await
            .map_err(|e| AppError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()), "zip entry close".into()))?;
    }

    zip.close().await.map_err(|e| AppError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()), "zip finish".into()))?;
    Ok(())
}

async fn write_tar_archive(
    uid: &str,
    fragment: &str,
    keys: Vec<String>,
    writer: DuplexStream,
) -> Result<(), AppError> {
    let mut tar = TarBuilder::new(writer);

    for key in keys {
        let clean_key = key.trim_start_matches('/');

        let metadata = match get_metadata_with_cache(fragment, clean_key, uid).await {
            Ok(m) if !m.is_delete_marker => m,
            _ => continue,
        };

        let mut header = TarHeader::new_gnu();
        header.set_path(clean_key).map_err(|e| AppError::Io(e, "tar path".into()))?;
        header.set_size(metadata.total_size_bytes);
        header.set_mode(0o644);
        header.set_cksum();

        let reader = create_multipart_reader(&metadata);

        tar.append_data(&mut header, clean_key, reader)
            .await
            .map_err(|e| AppError::Io(e, "tar append".into()))?;
    }

    tar.finish().await.map_err(|e| AppError::Io(e, "tar finish".into()))?;
    Ok(())
}

fn create_multipart_reader(metadata: &ObjectMetadata) -> StreamReader<impl Stream<Item = std::io::Result<Bytes>> + Unpin, Bytes> {
    let pm = ObjectPathManager::new();
    let blob_dir = pm.get_blob_dir(&metadata.owner_uid, &metadata.fragment, &metadata.version_id);
    let parts: std::collections::VecDeque<PartMetadata> = metadata.parts.clone().into();

    struct ReaderState {
        parts_queue: std::collections::VecDeque<PartMetadata>,
        blob_dir: PathBuf,
        cursor_in_part: u64,
        file_cache: HashMap<String, File>,
    }

    let initial_state = ReaderState {
        parts_queue: parts,
        blob_dir,
        cursor_in_part: 0,
        file_cache: HashMap::new(),
    };

    let stream = futures_util::stream::unfold(
        initial_state,
        move |mut state| async move {
            let chunk_req_size = 64 * 1024; 
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
                    continue;
                }

                let bytes_left_in_part = part.size_bytes - state.cursor_in_part;
                let bytes_to_fill = chunk_req_size - buf.len();
                let bytes_to_read = std::cmp::min(bytes_left_in_part, bytes_to_fill as u64) as usize;

                if bytes_to_read == 0 {
                    state.parts_queue.pop_front();
                    state.cursor_in_part = 0;
                    continue;
                }

                let data_shards_info: Vec<&ShardInfo> = part.shards.iter().filter(|s| !s.is_parity).collect();
                let data_shards_count = data_shards_info.len() as u64;

                if data_shards_count == 0 {
                     return Some((Err(std::io::Error::new(std::io::ErrorKind::Other, "Corrupt metadata")), state));
                }

                let mut temp_bytes_read = 0;
                while temp_bytes_read < bytes_to_read {
                    let stripe_block_size = STRIPE_BLOCK_SIZE as u64;
                    let stripe_size = stripe_block_size * data_shards_count;
                    let stripe_index = state.cursor_in_part / stripe_size;
                    let offset_in_stripe = state.cursor_in_part % stripe_size;
                    
                    let shard_index = (offset_in_stripe / stripe_block_size) as usize;
                    let offset_in_block = offset_in_stripe % stripe_block_size;
                    
                    let file_offset = (stripe_index * stripe_block_size) + offset_in_block;

                    let bytes_left_in_block = stripe_block_size - offset_in_block;
                    let remaining_req = bytes_to_read - temp_bytes_read;
                    let chunk_len = std::cmp::min(remaining_req as u64, bytes_left_in_block) as usize;

                    if let Some(shard_info) = data_shards_info.get(shard_index) {
                         match read_stripe_chunk_internal(
                            &mut state.file_cache,
                            &state.blob_dir,
                            part,
                            shard_info,
                            file_offset,
                            chunk_len,
                            data_shards_count as usize
                        ).await {
                            Ok(data) => {
                                buf.extend_from_slice(&data);
                                state.cursor_in_part += chunk_len as u64;
                                temp_bytes_read += chunk_len;
                            },
                            Err(e) => {
                                return Some((Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())), state));
                            }
                        }
                    } else {
                        return Some((Err(std::io::Error::new(std::io::ErrorKind::Other, "Shard index out of bounds")), state));
                    }
                }

                if buf.len() >= chunk_req_size {
                    return Some((Ok(Bytes::from(buf)), state));
                }
            }
        }
    );

    StreamReader::new(Box::pin(stream))
}

async fn read_stripe_chunk_internal(
    file_cache: &mut HashMap<String, File>,
    version_dir: &StdPath,
    part: &PartMetadata,
    target_shard: &ShardInfo,
    offset: u64,
    len: usize,
    data_shards_count: usize,
) -> Result<Vec<u8>, AppError> {
    
    let mut direct_success = false;
    let mut buf = vec![0u8; len];

    {
        if !file_cache.contains_key(&target_shard.file_name) {
             let path = version_dir.join(&target_shard.file_name);
             if let Ok(f) = File::open(&path).await {
                 file_cache.insert(target_shard.file_name.clone(), f);
             }
        }

        if let Some(file) = file_cache.get_mut(&target_shard.file_name) {
            if file.seek(SeekFrom::Start(offset)).await.is_ok() {
                if file.read_exact(&mut buf).await.is_ok() {
                    direct_success = true;
                }
            }
        }
    }

    if direct_success {
        return Ok(buf);
    }

    let parity_count = part.shards.len() - data_shards_count;
    let mut decoder = ReedSolomonDecoder::new(data_shards_count, parity_count, len)
        .map_err(|e| AppError::InternalServerError(format!("RS Init: {}", e)))?;

    let mut shards_found = 0;

    for s_info in &part.shards {
        if shards_found >= data_shards_count { break; }
        if s_info.shard_index == target_shard.shard_index { continue; }

        if !file_cache.contains_key(&s_info.file_name) {
             let s_path = version_dir.join(&s_info.file_name);
             if let Ok(f) = File::open(&s_path).await {
                 file_cache.insert(s_info.file_name.clone(), f);
             }
        }

        if let Some(f) = file_cache.get_mut(&s_info.file_name) {
            if let Ok(metadata) = f.metadata().await {
                if metadata.len() < (offset + len as u64) {
                    continue; 
                }
            }

            if f.seek(SeekFrom::Start(offset)).await.is_ok() {
                let mut chunk = vec![0u8; len];
                if f.read_exact(&mut chunk).await.is_ok() {
                    let res = if s_info.is_parity {
                        decoder.add_recovery_shard(s_info.shard_index - data_shards_count, chunk)
                    } else {
                        decoder.add_original_shard(s_info.shard_index, chunk)
                    };
                    
                    if res.is_ok() {
                        shards_found += 1;
                    }
                }
            }
        }
    }

    if shards_found < data_shards_count {
        return Err(AppError::InternalServerError("Data chunk lost: Insufficient shards".into()));
    }

    let result = decoder.decode().map_err(|e| AppError::InternalServerError(format!("RS Decode: {}", e)))?;
    
    if let Some(restored) = result.restored_original_iter().find(|(id, _)| *id == target_shard.shard_index) {
        Ok(restored.1.to_vec())
    } else {
        Err(AppError::InternalServerError("Failed to reconstruct target chunk".into()))
    }
}
