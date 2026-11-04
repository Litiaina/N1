use super::namespace::version_json;
use crate::{
    middleware::auth,
    namespace::Namespace,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};

pub(super) async fn metadata(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let id = claims.id();
    let version = q.get("version_id").cloned();
    let output_path = path.clone();
    let v = app
        .db
        .execute(move |db| Namespace::new(db).stat(id, &path, version.as_deref()))
        .await?;
    Ok(Json(version_json(&output_path, &v)))
}
pub(super) async fn versions(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let id = claims.id();
    let output_path = path.clone();
    let versions = app
        .db
        .execute(move |db| Namespace::new(db).versions(id, &path))
        .await?;
    Ok(Json(json!(
        versions
            .iter()
            .map(|v| version_json(&output_path, v))
            .collect::<Vec<_>>()
    )))
}
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    stream(
        app,
        claims.id(),
        path,
        q.get("version_id").cloned(),
        headers,
        None,
        false,
    )
    .await
}
pub(super) async fn download(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let path = q
        .get("object_key")
        .ok_or_else(|| ApiError::invalid("object_key is required"))?
        .clone();
    stream(
        app,
        claims.id(),
        path,
        q.get("version_id").cloned(),
        headers,
        None,
        true,
    )
    .await
}
pub(super) async fn chunk(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let path = q
        .get("object_key")
        .ok_or_else(|| ApiError::invalid("object_key is required"))?
        .clone();
    let parse = |name: &str, default: u64| -> ApiResult<u64> {
        q.get(name)
            .map(|v| {
                v.parse()
                    .map_err(|_| ApiError::invalid(format!("invalid {name}")))
            })
            .unwrap_or(Ok(default))
    };
    let offset = parse("offset", 0)?;
    let bytes = parse("length", parse("chunk_size", 1024 * 1024)?)?;
    if bytes == 0 {
        return Err(ApiError::invalid("chunk length must be positive"));
    }
    if bytes > 8 * 1024 * 1024 {
        return Err(ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            "chunk length exceeds 8 MiB".into(),
        ));
    }
    stream(
        app,
        claims.id(),
        path,
        q.get("version_id").cloned(),
        headers,
        Some((offset, bytes)),
        false,
    )
    .await
}
pub(crate) async fn stream(
    app: Arc<App>,
    fragment: n1_link::PoolId,
    path: String,
    version: Option<String>,
    headers: HeaderMap,
    chunk: Option<(u64, u64)>,
    attachment: bool,
) -> ApiResult<Response> {
    let lookup = path.clone();
    let stored = app
        .db
        .execute(move |db| Namespace::new(db).resolve(fragment, &lookup, version.as_deref()))
        .await?;
    let total = stored.metadata.total_size_bytes;
    let mut partial = false;
    let (offset, length) = if let Some((offset, length)) = chunk {
        if offset >= total {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        (offset, length.min(total - offset))
    } else if let Some(range) = headers.get(header::RANGE) {
        partial = true;
        parse_range(
            range
                .to_str()
                .map_err(|_| ApiError::invalid("invalid Range header"))?,
            total,
        )?
    } else {
        (0, total)
    };
    let service = app.service.clone();
    let content_type = stored
        .metadata
        .content_type
        .parse::<axum::http::HeaderValue>()
        .map_err(|_| ApiError::invalid("invalid stored content type"))?;
    let etag = format!("\"{}\"", stored.metadata.version_id)
        .parse::<axum::http::HeaderValue>()
        .expect("version ETag");
    let body = if length == 0 {
        // Content-Length: 0 completes the client exchange at the headers.
        // Validate the native read before publishing those headers: a later
        // body error cannot retract an empty HTTP or named-native success.
        service
            .download_resolved(fragment, stored, offset, Some(0), &mut tokio::io::sink())
            .await?;
        Body::empty()
    } else {
        let (mut writer, reader) = tokio::io::duplex(256 * 1024);
        let (error_tx, mut error_rx) = tokio::sync::mpsc::channel::<std::io::Error>(1);
        tokio::spawn(async move {
            if service
                .download_resolved(fragment, stored, offset, Some(length), &mut writer)
                .await
                .is_err()
            {
                let _ = error_tx
                    .send(std::io::Error::other(
                        "object download failed integrity or availability checks",
                    ))
                    .await;
            }
        });
        // Surface a truncated/corrupt backend stream as a body error. A successful
        // HTTP header alone is never used as evidence of a complete download.
        use futures_util::{StreamExt, stream};
        let source = tokio_util::io::ReaderStream::new(reader).chain(stream::once(async move {
            match error_rx.recv().await {
                Some(error) => Err(error),
                None => Ok(bytes::Bytes::new()),
            }
        }));
        Body::from_stream(source)
    };
    let mut response = body.into_response();
    *response.status_mut() = if partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let h = response.headers_mut();
    h.insert(header::CONTENT_TYPE, content_type);
    h.insert(
        header::CONTENT_LENGTH,
        length.to_string().parse().expect("length header"),
    );
    h.insert(
        header::ACCEPT_RANGES,
        "bytes".parse().expect("static header"),
    );
    h.insert(header::ETAG, etag);
    if partial {
        h.insert(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{total}", offset, offset + length - 1)
                .parse()
                .expect("range header"),
        );
    }
    if attachment {
        let filename = path.rsplit('/').next().unwrap_or("download");
        let encoded =
            percent_encoding::utf8_percent_encode(filename, percent_encoding::NON_ALPHANUMERIC);
        h.insert(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename*=UTF-8''{encoded}")
                .parse()
                .map_err(|_| ApiError::invalid("invalid filename"))?,
        );
    }
    Ok(response)
}
pub(crate) fn parse_range(value: &str, total: u64) -> ApiResult<(u64, u64)> {
    let error = || {
        ApiError(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "Invalid or unsatisfiable byte range".into(),
        )
    };
    let (a, b) = value
        .strip_prefix("bytes=")
        .and_then(|s| s.split_once('-'))
        .ok_or_else(error)?;
    if total == 0 || b.contains(',') {
        return Err(error());
    }
    if a.is_empty() {
        let count = b.parse::<u64>().map_err(|_| error())?;
        if count == 0 {
            return Err(error());
        }
        let length = count.min(total);
        return Ok((total - length, length));
    }
    let start = a.parse::<u64>().map_err(|_| error())?;
    let end = if b.is_empty() {
        total - 1
    } else {
        b.parse::<u64>().map_err(|_| error())?.min(total - 1)
    };
    if start >= total || end < start {
        return Err(error());
    }
    Ok((start, end - start + 1))
}
#[derive(Clone, Serialize, Deserialize)]
struct Grant {
    purpose: String,
    fragment: String,
    epoch: u64,
    path: String,
    version: String,
    exp: u64,
}
pub(super) async fn signed_url(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let path = body
        .get("object_key")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::invalid("object_key is required"))?
        .to_owned();
    let version = body
        .get("version_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let id = claims.id();
    let lookup = path.clone();
    let stored = app
        .db
        .execute(move |db| Namespace::new(db).stat(id, &lookup, version.as_deref()))
        .await?;
    let ttl = body
        .get("expires_in")
        .or_else(|| body.get("ttl_seconds"))
        .and_then(Value::as_u64)
        .unwrap_or(300)
        .min(3600);
    if ttl == 0 {
        return Err(ApiError::invalid("signed URL lifetime must be positive"));
    }
    let grant = Grant {
        purpose: "n1.named.read".into(),
        fragment: claims.fragment,
        epoch: claims.epoch,
        path,
        version: stored.version_id,
        exp: auth::now() + ttl,
    };
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &grant,
        &jsonwebtoken::EncodingKey::from_secret(app.config.jwt_secret.as_bytes()),
    )
    .map_err(|_| ApiError::invalid("cannot sign read URL"))?;
    let path = format!("/noa/v1/objects/signed-read?token={token}");
    let base = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let scheme = if app.config.cert_path.is_some() {
        "https"
    } else {
        "http"
    };
    let url = if base.is_empty() {
        path
    } else {
        format!("{scheme}://{base}{path}")
    };
    Ok(Json(
        json!({"url":url,"signed_url":url,"expires_at":grant.exp,"expires_in":ttl}),
    ))
}
pub(super) async fn signed_read(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let token = q.get("token").ok_or_else(ApiError::unauthorized)?;
    let mut validation = jsonwebtoken::Validation::default();
    validation.leeway = 0;
    let grant = jsonwebtoken::decode::<Grant>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(app.config.jwt_secret.as_bytes()),
        &validation,
    )
    .map_err(|_| ApiError::unauthorized())?
    .claims;
    if grant.purpose != "n1.named.read" {
        return Err(ApiError::unauthorized());
    }
    let identity = grant.fragment;
    let record = app
        .db
        .execute(move |db| crate::fragments::resolve(&db, &identity))
        .await?;
    if record.epoch != grant.epoch {
        return Err(ApiError::unauthorized());
    }
    stream(
        app,
        record.id,
        grant.path,
        Some(grant.version),
        headers,
        None,
        true,
    )
    .await
}
