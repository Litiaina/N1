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

use crate::{
    enums::app_error_enum::AppError,
    object_storage::caching::get_metadata_with_cache,
    structs::{
        auth_structs::Claims,
        object_storage_structs::{
             DownloadArchiveRequest, 
            ObjectMetadata, ObjectPathManager
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

    let stream = futures_util::stream::unfold(
        (parts.into_iter(), None::<File>, blob_dir),
        move |(mut parts, mut current_file, blob_dir)| async move {
            loop {
                if let Some(mut file) = current_file.take() {
                    let mut buf = vec![0u8; 64 * 1024];
                    match file.read(&mut buf).await {
                        Ok(0) => {
                        }
                        Ok(n) => {
                            buf.truncate(n);
                            return Some((
                                Ok::<Bytes, std::io::Error>(Bytes::from(buf)), 
                                (parts, Some(file), blob_dir)
                            ));
                        }
                        Err(e) => {
                            return Some((Err(e), (parts, None, blob_dir)));
                        }
                    }
                }

                match parts.next() {
                    Some(part) => {
                        let path = blob_dir.join(part.file_name);
                        match File::open(path).await {
                            Ok(f) => {
                                current_file = Some(f);
                                continue;
                            }
                            Err(e) => return Some((Err(e), (parts, None, blob_dir))),
                        }
                    }
                    None => return None,
                }
            }
        }
    );

    StreamReader::new(Box::pin(stream))
}
