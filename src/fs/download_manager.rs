use axum::body::{Body, Bytes};
use std::path::PathBuf;
use std::pin::Pin;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::Stream;
use mime_guess::from_path;
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};
use crate::fs::fs_constants::DEFAULT_BUFFER_SIZE;
use crate::utils::directory_validation::sanitize_path;
use crate::utils::header_extractor::extract_uid_from_header;

pub async fn download_file(header: HeaderMap, Path(path): Path<String>) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = match extract_uid_from_header(&header).await {
        Ok(email) => email,
        Err(code) => return Err(code),
    };
    let sanitized_path = sanitize_path(&path);
    let file_path = PathBuf::from(format!("storage-files/{}/{}", validated_uid, sanitized_path));
    let file = fs::File::open(&file_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;

    let file_metadata = file
        .metadata()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mime_type = from_path(&file_path).first_or_octet_stream();

    let file_name = file_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");

    let mut reader = BufReader::new(file);
    let mut buffer = vec![0; DEFAULT_BUFFER_SIZE];

    let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>> =
        Box::pin(async_stream::stream! {
            loop {
                match reader.read(&mut buffer).await {
                    Ok(read_size) if read_size == 0 => break,
                    Ok(read_size) => yield Ok(Bytes::copy_from_slice(&buffer[..read_size])),
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }
        });

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", file_name),
        )
        .header("Content-Type", mime_type.as_ref())
        .header("Content-Length", file_metadata.len().to_string())
        .body(Body::from_stream(stream))
        .unwrap()
        .into_response())
}