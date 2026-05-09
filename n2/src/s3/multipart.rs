use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::s3::headers::X_AMZ_VERSION_ID;
use crate::s3::xml::{unix_to_rfc3339, xml_escape, xml_response};
use axum::body::{Body, to_bytes};
use axum::http::{
    HeaderValue, StatusCode,
    header::{CONTENT_LENGTH, ETAG},
};
use axum::response::Response;
use bytes::Bytes;
use chrono::Utc;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ulid::Ulid;

const COMPLETE_BODY_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct S3MultipartSession {
    bucket: String,
    key: String,
    upload_id: String,
    content_type: Option<String>,
    initiated_at: i64,
}

#[derive(Debug)]
pub(crate) struct CompletedMultipartBody {
    pub body: Body,
    pub content_type: Option<String>,
    pub upload_id: String,
}

#[derive(Clone, Debug)]
struct StoredPart {
    part_number: u32,
    size_bytes: u64,
    etag: String,
    path: PathBuf,
}

struct PartBodyStreamState {
    pending: VecDeque<PathBuf>,
    current: Option<fs::File>,
}

fn multipart_root() -> PathBuf {
    FS_STORAGE_ROOT
        .get()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(STORAGE_ROOT))
        .join("n2")
        .join("s3-multipart")
}

fn validate_upload_id(upload_id: &str) -> Result<(), AppError> {
    let ok = !upload_id.is_empty()
        && upload_id.len() <= 80
        && upload_id
            .as_bytes()
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_');
    if ok {
        Ok(())
    } else {
        Err(AppError::BadRequest("invalid S3 uploadId".to_string()))
    }
}

fn validate_part_number(part_number: u32) -> Result<(), AppError> {
    if (1..=10_000).contains(&part_number) {
        Ok(())
    } else {
        Err(AppError::BadRequest(
            "partNumber must be between 1 and 10000".to_string(),
        ))
    }
}

fn session_dir(upload_id: &str) -> Result<PathBuf, AppError> {
    validate_upload_id(upload_id)?;
    Ok(multipart_root().join(upload_id))
}

fn session_file(dir: &Path) -> PathBuf {
    dir.join("session.nmtd")
}

fn parse_part_name(name: &str, dir: &Path) -> Option<StoredPart> {
    let rest = name.strip_prefix("part_")?;
    let rest = rest.strip_suffix(".part")?;
    let mut fields = rest.split('.');
    let part_number = fields.next()?.parse::<u32>().ok()?;
    let size_bytes = fields.next()?.parse::<u64>().ok()?;
    let etag = fields.next()?.to_string();
    if fields.next().is_some() || etag.is_empty() {
        return None;
    }
    Some(StoredPart {
        part_number,
        size_bytes,
        etag,
        path: dir.join(name),
    })
}

fn body_from_parts(parts: Vec<StoredPart>) -> Body {
    let state = PartBodyStreamState {
        pending: parts.into_iter().map(|part| part.path).collect(),
        current: None,
    };

    Body::from_stream(futures_util::stream::unfold(
        state,
        |mut state| async move {
            loop {
                if state.current.is_none() {
                    let Some(path) = state.pending.pop_front() else {
                        return None;
                    };
                    match fs::File::open(path).await {
                        Ok(file) => state.current = Some(file),
                        Err(err) => return Some((Err(err), state)),
                    }
                }

                let mut buffer = vec![0u8; 1024 * 1024];
                let read = state
                    .current
                    .as_mut()
                    .expect("current part file")
                    .read(&mut buffer)
                    .await;
                match read {
                    Ok(0) => {
                        state.current = None;
                    }
                    Ok(n) => {
                        buffer.truncate(n);
                        return Some((Ok::<Bytes, std::io::Error>(Bytes::from(buffer)), state));
                    }
                    Err(err) => return Some((Err(err), state)),
                }
            }
        },
    ))
}

async fn read_session(upload_id: &str) -> Result<(S3MultipartSession, PathBuf), AppError> {
    let dir = session_dir(upload_id)?;
    let bytes = fs::read(session_file(&dir))
        .await
        .map_err(|e| AppError::Io(e, format!("read S3 multipart session {}", upload_id)))?;
    let session = serde_json::from_slice::<S3MultipartSession>(&bytes).map_err(|e| {
        AppError::BadRequest(format!("invalid S3 multipart session {}: {}", upload_id, e))
    })?;
    Ok((session, dir))
}

async fn list_parts_in_dir(dir: &Path) -> Result<Vec<StoredPart>, AppError> {
    let mut entries = fs::read_dir(dir)
        .await
        .map_err(|e| AppError::Io(e, format!("read S3 multipart dir {:?}", dir)))?;
    let mut parts = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| AppError::Io(e, format!("read S3 multipart entry {:?}", dir)))?
    {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(part) = parse_part_name(name, dir) {
            parts.push(part);
        }
    }
    parts.sort_by_key(|part| part.part_number);
    Ok(parts)
}

async fn remove_existing_part(dir: &Path, part_number: u32) -> Result<(), AppError> {
    let prefix = format!("part_{:05}.", part_number);
    let mut entries = fs::read_dir(dir)
        .await
        .map_err(|e| AppError::Io(e, format!("read S3 multipart dir {:?}", dir)))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| AppError::Io(e, format!("read S3 multipart entry {:?}", dir)))?
    {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(prefix.as_str()) && name.ends_with(".part") {
            let _ = fs::remove_file(entry.path()).await;
        }
    }
    Ok(())
}

fn ensure_session_target(
    session: &S3MultipartSession,
    bucket: &str,
    key: &str,
) -> Result<(), AppError> {
    if session.bucket == bucket && session.key == key {
        Ok(())
    } else {
        Err(AppError::Conflict(
            "S3 multipart uploadId belongs to a different object".to_string(),
        ))
    }
}

pub(crate) async fn create_multipart_upload(
    bucket: &str,
    key: &str,
    content_type: Option<String>,
) -> Result<Response, AppError> {
    let upload_id = Ulid::new().to_string();
    let dir = session_dir(upload_id.as_str())?;
    fs::create_dir_all(&dir)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir S3 multipart {:?}", dir)))?;
    let session = S3MultipartSession {
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id: upload_id.clone(),
        content_type,
        initiated_at: Utc::now().timestamp(),
    };
    let tmp = dir.join("session.nmtd.tmp");
    let bytes = serde_json::to_vec(&session)
        .map_err(|e| AppError::InternalServerError(format!("encode S3 multipart: {}", e)))?;
    fs::write(&tmp, bytes)
        .await
        .map_err(|e| AppError::Io(e, format!("write S3 multipart {:?}", tmp)))?;
    fs::rename(&tmp, session_file(&dir))
        .await
        .map_err(|e| AppError::Io(e, format!("publish S3 multipart {:?}", dir)))?;

    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id.as_str())
    );
    Ok(xml_response(body))
}

pub(crate) async fn upload_part(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: u32,
    body: Body,
) -> Result<Response, AppError> {
    validate_part_number(part_number)?;
    let (session, dir) = read_session(upload_id).await?;
    ensure_session_target(&session, bucket, key)?;

    let tmp = dir.join(format!("part_{:05}.{}.tmp", part_number, Ulid::new()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("create S3 multipart part {:?}", tmp)))?;
    let mut stream = body.into_data_stream();
    let mut hasher = Sha256::new();
    let mut size_bytes = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read S3 multipart part body".to_string(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        hasher.update(chunk.as_ref());
        file.write_all(chunk.as_ref())
            .await
            .map_err(|e| AppError::Io(e, format!("write S3 multipart part {:?}", tmp)))?;
        size_bytes = size_bytes.saturating_add(chunk.len() as u64);
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, format!("flush S3 multipart part {:?}", tmp)))?;
    drop(file);

    let etag = hex::encode(hasher.finalize());
    remove_existing_part(&dir, part_number).await?;
    let final_path = dir.join(format!(
        "part_{:05}.{}.{}.part",
        part_number, size_bytes, etag
    ));
    fs::rename(&tmp, &final_path)
        .await
        .map_err(|e| AppError::Io(e, format!("publish S3 multipart part {:?}", final_path)))?;

    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    if let Ok(v) = HeaderValue::from_str(format!("\"{}\"", etag).as_str()) {
        response.headers_mut().insert(ETAG, v);
    }
    if let Ok(v) = HeaderValue::from_str(size_bytes.to_string().as_str()) {
        response.headers_mut().insert(CONTENT_LENGTH, v);
    }
    Ok(response)
}

fn parse_complete_part_numbers(xml: &str) -> Vec<u32> {
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<PartNumber>") {
        rest = &rest[start + "<PartNumber>".len()..];
        let Some(end) = rest.find("</PartNumber>") else {
            break;
        };
        if let Ok(n) = rest[..end].trim().parse::<u32>() {
            out.push(n);
        }
        rest = &rest[end + "</PartNumber>".len()..];
    }
    out
}

pub(crate) async fn prepare_complete_upload(
    bucket: &str,
    key: &str,
    upload_id: &str,
    body: Body,
) -> Result<CompletedMultipartBody, AppError> {
    let (session, dir) = read_session(upload_id).await?;
    ensure_session_target(&session, bucket, key)?;

    let complete_body = to_bytes(body, COMPLETE_BODY_LIMIT)
        .await
        .map_err(|e| AppError::BadRequest(format!("read CompleteMultipartUpload XML: {}", e)))?;
    let requested = std::str::from_utf8(complete_body.as_ref())
        .map(parse_complete_part_numbers)
        .unwrap_or_default();
    let mut parts = list_parts_in_dir(&dir).await?;
    if parts.is_empty() {
        return Err(AppError::BadRequest(
            "CompleteMultipartUpload requires at least one uploaded part".to_string(),
        ));
    }
    if !requested.is_empty() {
        let mut selected = Vec::with_capacity(requested.len());
        for part_number in requested {
            let Some(part) = parts.iter().find(|p| p.part_number == part_number).cloned() else {
                return Err(AppError::BadRequest(format!(
                    "missing uploaded part {} for CompleteMultipartUpload",
                    part_number
                )));
            };
            selected.push(part);
        }
        parts = selected;
    }

    Ok(CompletedMultipartBody {
        body: body_from_parts(parts),
        content_type: session.content_type,
        upload_id: upload_id.to_string(),
    })
}

pub(crate) async fn finish_complete_upload(
    bucket: &str,
    key: &str,
    upload_id: &str,
    version_id: &str,
) -> Result<Response, AppError> {
    let dir = session_dir(upload_id)?;
    let _ = fs::remove_dir_all(&dir).await;
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>/s3/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>&quot;{}&quot;</ETag></CompleteMultipartUploadResult>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(version_id)
    );
    let mut response = xml_response(body);
    if let Ok(v) = HeaderValue::from_str(version_id) {
        response.headers_mut().insert(X_AMZ_VERSION_ID, v);
    }
    Ok(response)
}

pub(crate) async fn abort_multipart_upload(
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<StatusCode, AppError> {
    let (session, dir) = read_session(upload_id).await?;
    ensure_session_target(&session, bucket, key)?;
    let _ = fs::remove_dir_all(&dir).await;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn list_uploaded_parts(
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Response, AppError> {
    let (session, dir) = read_session(upload_id).await?;
    ensure_session_target(&session, bucket, key)?;
    let parts = list_parts_in_dir(&dir).await?;
    let mut parts_xml = String::new();
    for part in parts {
        parts_xml.push_str(
            format!(
                "<Part><PartNumber>{}</PartNumber><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size></Part>",
                part.part_number,
                unix_to_rfc3339(session.initiated_at),
                xml_escape(part.etag.as_str()),
                part.size_bytes
            )
            .as_str(),
        );
    }
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId><StorageClass>STANDARD</StorageClass><IsTruncated>false</IsTruncated>{}</ListPartsResult>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
        parts_xml
    );
    Ok(xml_response(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::disk_manager::FS_STORAGE_ROOT;
    use axum::body::to_bytes;

    fn ensure_test_root() {
        let root = std::env::temp_dir().join(format!("n2-s3-multipart-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&root);
        let _ = FS_STORAGE_ROOT.set(root.to_string_lossy().to_string());
    }

    fn upload_id_from_xml(xml: &str) -> String {
        let start = xml
            .find("<UploadId>")
            .expect("upload id start")
            .saturating_add("<UploadId>".len());
        let end = xml[start..]
            .find("</UploadId>")
            .expect("upload id end")
            .saturating_add(start);
        xml[start..end].to_string()
    }

    #[tokio::test]
    async fn multipart_helpers_stream_parts_in_order() {
        ensure_test_root();
        let init = create_multipart_upload("frag", "docs/video.bin", Some("video/mp4".to_string()))
            .await
            .expect("create upload");
        let init_body = to_bytes(init.into_body(), usize::MAX)
            .await
            .expect("read init xml");
        let upload_id = upload_id_from_xml(std::str::from_utf8(init_body.as_ref()).expect("xml"));

        let first = upload_part(
            "frag",
            "docs/video.bin",
            upload_id.as_str(),
            1,
            Body::from("hello "),
        )
        .await
        .expect("part 1");
        assert_eq!(first.status(), StatusCode::OK);
        let second = upload_part(
            "frag",
            "docs/video.bin",
            upload_id.as_str(),
            2,
            Body::from("world"),
        )
        .await
        .expect("part 2");
        assert_eq!(second.status(), StatusCode::OK);

        let complete_xml = Body::from(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part><Part><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>",
        );
        let completed =
            prepare_complete_upload("frag", "docs/video.bin", upload_id.as_str(), complete_xml)
                .await
                .expect("complete body");
        let bytes = to_bytes(completed.body, usize::MAX)
            .await
            .expect("read combined body");
        assert_eq!(bytes.as_ref(), b"hello world");
        let finished =
            finish_complete_upload("frag", "docs/video.bin", upload_id.as_str(), "01TEST")
                .await
                .expect("finish");
        assert_eq!(finished.status(), StatusCode::OK);
        assert!(
            read_session(upload_id.as_str()).await.is_err(),
            "finished upload must remove the session"
        );
    }
}
