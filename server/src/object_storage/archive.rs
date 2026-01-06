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
    io::{AsyncReadExt, DuplexStream},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use async_zip::{tokio::write::ZipFileWriter, Compression, ZipEntryBuilder};
use tokio_tar::{Builder as TarBuilder, Header as TarHeader};
use std::io::Cursor;
use std::path::PathBuf;

use crate::{
    enums::app_error_enum::AppError,
    object_storage::caching::get_metadata_with_cache,
    object_storage::object_parser::reconstruct_shard,
    structs::{
        auth_structs::Claims,
        object_storage_structs::{
             DownloadArchiveRequest, 
            ObjectMetadata, ObjectPathManager, PartMetadata
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
    let parts = metadata.parts.clone();

    struct ReaderState {
        parts_iter: std::vec::IntoIter<PartMetadata>,
        current_part: Option<PartMetadata>,
        current_shard_idx: usize,
        current_file: Option<File>,
        current_cursor: Option<Cursor<Vec<u8>>>,
        blob_dir: PathBuf,
    }

    let initial_state = ReaderState {
        parts_iter: parts.into_iter(),
        current_part: None,
        current_shard_idx: 0,
        current_file: None,
        current_cursor: None,
        blob_dir,
    };

    let stream = futures_util::stream::unfold(
        initial_state,
        move |mut state| async move {
            loop {
                let mut buf = vec![0u8; 64 * 1024];
                let mut bytes_read = 0;
                let mut has_active_reader = false;

                if let Some(file) = state.current_file.as_mut() {
                    match file.read(&mut buf).await {
                        Ok(n) => {
                            bytes_read = n;
                            has_active_reader = true;
                        },
                        Err(e) => return Some((Err(e), state)),
                    }
                } else if let Some(cursor) = state.current_cursor.as_mut() {
                    match cursor.read(&mut buf).await {
                        Ok(n) => {
                            bytes_read = n;
                            has_active_reader = true;
                        },
                        Err(e) => return Some((Err(e), state)),
                    }
                }

                if has_active_reader {
                    if bytes_read > 0 {
                        buf.truncate(bytes_read);
                        return Some((Ok(Bytes::from(buf)), state));
                    } else {
                        state.current_file = None;
                        state.current_cursor = None;
                        state.current_shard_idx += 1;
                    }
                }

                if state.current_part.is_none() {
                    state.current_part = state.parts_iter.next();
                    state.current_shard_idx = 0;
                }

                let part = match state.current_part.as_ref() {
                    Some(p) => p,
                    None => return None,
                };

                let shard_info_opt = part.shards.iter().find(|s| s.shard_index == state.current_shard_idx && !s.is_parity);

                match shard_info_opt {
                    Some(shard_info) => {
                        let path = if !shard_info.absolute_path.is_empty() {
                            PathBuf::from(&shard_info.absolute_path)
                        } else {
                            state.blob_dir.join(&shard_info.file_name)
                        };

                        match File::open(&path).await {
                            Ok(f) => {
                                state.current_file = Some(f);
                            }
                            Err(_) => {
                                match reconstruct_shard(part, shard_info.shard_index, &state.blob_dir).await {
                                    Ok(data) => {
                                        state.current_cursor = Some(Cursor::new(data));
                                    }
                                    Err(e) => {
                                        return Some((Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())), state));
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    None => {
                        state.current_part = None;
                        continue;
                    }
                }
            }
        }
    );

    StreamReader::new(Box::pin(stream))
}
