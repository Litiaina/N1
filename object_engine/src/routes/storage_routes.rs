//! HTTP is an adapter to the same NOA service and LINK streaming engine.
use crate::{
    core::{Error, Result},
    middleware::auth_middleware::Claims,
    protocol::ops::Service,
};
use axum::{
    Extension, Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures_util::StreamExt;
use n1_link::noa::{Command, MAX_BODY_BYTES, MAX_PREAUTH_BYTES, Request, Response as NoaResponse};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio_util::io::{ReaderStream, StreamReader};

pub fn router(service: Arc<Service>) -> Router {
    Router::new()
        .route("/health", get(ping))
        .route("/noa/signed", get(signed_download))
        .route("/noa/backup/receive/{repository}", get(receive_backup))
        .route(
            "/noa",
            post(control).layer(DefaultBodyLimit::max(MAX_PREAUTH_BYTES)),
        )
        .route(
            "/noa/stream",
            post(streamed).layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/noa/upload/{object_key}/bytes/{offset}",
            put(part).layer(DefaultBodyLimit::disable()),
        )
        .route("/noa/object/{object_key}", get(download).head(head_object))
        .layer(Extension(Arc::new(tokio::sync::Semaphore::new(1024))))
        .layer(axum::middleware::from_fn_with_state(
            service.clone(),
            rate_limit_peer,
        ))
        .with_state(service)
}
async fn rate_limit_peer(
    State(service): State<Arc<Service>>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|peer| peer.0.ip())
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    if let Err(e) = service.limits.peer(peer, false) {
        return error(e);
    }
    request.extensions_mut().insert(peer);
    next.run(request).await
}
fn claims(service: &Service, headers: &HeaderMap) -> Result<Claims> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(Error::Unauthorized)?;
    service.auth.verify(value)
}
fn error(e: Error) -> Response {
    (
        StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(json!({"error":e.to_string()})),
    )
        .into_response()
}
async fn ping(State(service): State<Arc<Service>>) -> Json<serde_json::Value> {
    Json(service.ping())
}
async fn control(
    State(service): State<Arc<Service>>,
    Extension(peer): Extension<std::net::IpAddr>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> Response {
    let id = request.id;
    let result = async {
        if request.body_len != 0 {
            return Err(Error::Invalid(
                "use the raw streaming endpoint for object bytes".into(),
            ));
        }
        if matches!(
            &request.command,
            Command::Login { .. }
                | Command::Refresh { .. }
                | Command::Bearer { .. }
                | Command::Logout
        ) {
            service.limits.peer(peer, true)?;
        }
        match request.command {
            Command::Ping => Ok(service.ping()),
            Command::Login {
                fragment,
                secret,
                permissions,
                ttl_seconds,
            } => {
                let login = service
                    .auth
                    .login(fragment, secret, permissions, ttl_seconds)
                    .await?;
                Ok(json!(login))
            }
            Command::Refresh { refresh_token } => {
                Ok(json!(service.auth.refresh(&refresh_token).await?))
            }
            Command::Bearer { token } => Ok(json!({"session":service.auth.verify(&token)?})),
            Command::Logout => {
                service.auth.logout(&claims(&service, &headers)?).await?;
                Ok(json!({"authenticated":false}))
            }
            command => {
                let claims = claims(&service, &headers)?;
                service.authorize(&claims, &command)?;
                service.limits.operation(&claims, &command)?;
                service.control(&claims, command).await
            }
        }
    }
    .await;
    let response = match result {
        Ok(value) => NoaResponse::ok(id, value),
        Err(e) => NoaResponse::error(id, e.status(), e.to_string()),
    };
    (
        StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(response),
    )
        .into_response()
}
async fn part(
    State(service): State<Arc<Service>>,
    Path((key, offset)): Path<(String, u64)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let result = async {
        let claims = claims(&service, &headers)?;
        let expected = service.upload_length(&claims, &key, offset).await?;
        service.limits.operation(
            &claims,
            &Command::UploadPart {
                object_key: key.clone(),
                offset,
            },
        )?;
        if expected > MAX_BODY_BYTES {
            return Err(Error::Invalid("part exceeds transport body limit".into()));
        }
        let length = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| Error::Invalid("part requires an exact Content-Length".into()))?;
        if length != expected {
            return Err(Error::Invalid(
                "Content-Length differs from part length".into(),
            ));
        }
        let reader = body_reader(body);
        service
            .store
            .write_at(claims.sub.0, key, offset, reader)
            .await?;
        Ok(Json(json!({"offset":offset,"complete":true})).into_response())
    }
    .await;
    result.unwrap_or_else(error)
}

fn body_reader(body: Body) -> impl tokio::io::AsyncRead + Unpin + Send + 'static {
    let stream = tokio_stream::StreamExt::timeout(
        body.into_data_stream(),
        std::time::Duration::from_secs(30),
    );
    StreamReader::new(Box::pin(stream.map(|r| match r {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(e)) => Err(std::io::Error::other(e)),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "HTTP upload source stalled",
        )),
    })))
}

/// HTTP streaming uploads carry the identical length-prefixed MessagePack
/// control followed by raw bytes. Authentication stays in the HTTP header.
async fn streamed(
    State(service): State<Arc<Service>>,
    Extension(memory): Extension<Arc<tokio::sync::Semaphore>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let result = async {
        let claims = claims(&service, &headers)?;
        let total = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| Error::Invalid("stream requires an exact Content-Length".into()))?;
        let mut reader = body_reader(body);
        let length = reader.read_u32().await? as usize;
        if length == 0 || length > MAX_PREAUTH_BYTES {
            return Err(Error::Invalid("HTTP stream control exceeds limit".into()));
        }
        let _permit = memory
            .try_acquire_owned()
            .map_err(|_| Error::Unavailable("HTTP stream admission is full".into()))?;
        let mut bytes = vec![0; length];
        reader.read_exact(&mut bytes).await?;
        let request: Request = rmp_serde::from_slice(&bytes)
            .map_err(|e| Error::Invalid(format!("invalid NOA control: {e}")))?;
        let expected = service.body_length(&claims, &request.command).await?;
        service.limits.operation(&claims, &request.command)?;
        let max = if matches!(&request.command, Command::UploadPut { .. }) {
            i64::MAX as u64
        } else {
            MAX_BODY_BYTES
        };
        if expected != request.body_len || expected > max || total != 4 + length as u64 + expected {
            return Err(Error::Invalid(
                "HTTP stream declaration differs from upload plan".into(),
            ));
        }
        let result = service.upload_body(&claims, request.command, reader).await;
        let response = match result {
            Ok(value) => NoaResponse::ok(request.id, value),
            Err(e) => NoaResponse::error(request.id, e.status(), e.to_string()),
        };
        Ok((
            StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(response),
        )
            .into_response())
    }
    .await;
    result.unwrap_or_else(error)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Range {
    #[serde(default)]
    offset: u64,
    length: Option<u64>,
}
async fn download(
    State(service): State<Arc<Service>>,
    Path(key): Path<String>,
    Query(range): Query<Range>,
    headers: HeaderMap,
) -> Response {
    match claims(&service, &headers) {
        Ok(claims) => download_authorized(service, claims, key, range, headers).await,
        Err(e) => error(e),
    }
}
async fn head_object(
    State(service): State<Arc<Service>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let claims = claims(&service, &headers)?;
        let command = Command::ObjectStat {
            object_key: key.clone(),
        };
        service.authorize(&claims, &command)?;
        service.limits.operation(&claims, &command)?;
        let info = service.store.stat(claims.sub.0, key).await?;
        let etag = format!("\"{}\"", info.integrity_root);
        if let Some(response) = read_preconditions(&headers, &etag) {
            return Ok(response);
        }
        Response::builder()
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, info.total_bytes)
            .header(header::ACCEPT_RANGES, "bytes")
            .header(header::ETAG, etag)
            .header(header::CACHE_CONTROL, "private, no-store")
            .body(Body::empty())
            .map_err(|e| Error::Invalid(e.to_string()))
    }
    .await;
    result.unwrap_or_else(error)
}

fn read_preconditions(headers: &HeaderMap, etag: &str) -> Option<Response> {
    let matches = |name, weak: bool| {
        headers.get_all(name).iter().any(|value| {
            value.to_str().is_ok_and(|value| {
                value.trim() == "*"
                    || value.split(',').any(|candidate| {
                        let candidate = candidate.trim();
                        let candidate = if weak {
                            candidate.strip_prefix("W/").unwrap_or(candidate)
                        } else {
                            candidate
                        };
                        candidate == etag
                    })
            })
        })
    };
    let status = if headers.contains_key(header::IF_MATCH) && !matches(header::IF_MATCH, false) {
        StatusCode::PRECONDITION_FAILED
    } else if headers.contains_key(header::IF_NONE_MATCH) && matches(header::IF_NONE_MATCH, true) {
        StatusCode::NOT_MODIFIED
    } else {
        return None;
    };
    Some(
        (
            status,
            [
                (header::ETAG, etag),
                (header::CACHE_CONTROL, "private, no-store"),
            ],
        )
            .into_response(),
    )
}
async fn signed_download(
    State(service): State<Arc<Service>>,
    Query(grant): Query<n1_link::noa::ReadGrant>,
    headers: HeaderMap,
) -> Response {
    match service.auth.signed_claims(&grant) {
        Ok(claims) => {
            download_authorized(
                service,
                claims,
                grant.object_key,
                Range {
                    offset: grant.offset,
                    length: grant.length,
                },
                headers,
            )
            .await
        }
        Err(e) => error(e),
    }
}
async fn download_authorized(
    service: Arc<Service>,
    claims: Claims,
    key: String,
    mut range: Range,
    headers: HeaderMap,
) -> Response {
    let result = async {
        service.authorize(
            &claims,
            &Command::ObjectGet {
                object_key: key.clone(),
                offset: range.offset,
                length: range.length,
            },
        )?;
        service.limits.operation(
            &claims,
            &Command::ObjectGet {
                object_key: key.clone(),
                offset: range.offset,
                length: range.length,
            },
        )?;
        let mut header_range_applied = false;
        if headers.contains_key(header::RANGE)
            || headers.contains_key(header::IF_NONE_MATCH)
            || headers.contains_key(header::IF_MATCH)
        {
            let info = service.store.stat(claims.sub.0, key.clone()).await?;
            let etag = format!("\"{}\"", info.integrity_root);
            if let Some(response) = read_preconditions(&headers, &etag) {
                return Ok(response);
            }
            if let Some(requested) = headers.get(header::RANGE) {
                if range.offset != 0 || range.length.is_some() {
                    return Err(Error::Invalid(
                        "choose the signed/query range or the Range header".into(),
                    ));
                }
                // No Last-Modified validator is stored for immutable objects.
                // Only an exact strong ETag can satisfy If-Range. Empty objects
                // use an ordinary empty response rather than an inclusive range.
                let apply_range = info.total_bytes != 0
                    && (!headers.contains_key(header::IF_RANGE)
                        || headers.get(header::IF_RANGE).and_then(|v| v.to_str().ok())
                            == Some(etag.as_str()));
                if apply_range {
                    match crate::core::header_parser::parse_range_header(
                        requested.to_str().ok(),
                        info.total_bytes,
                    ) {
                        Some((start, end)) => {
                            header_range_applied = true;
                            range.offset = start;
                            range.length = Some(end - start + 1);
                        }
                        None => {
                            return Ok((
                                StatusCode::RANGE_NOT_SATISFIABLE,
                                [(
                                    header::CONTENT_RANGE,
                                    format!("bytes */{}", info.total_bytes),
                                )],
                            )
                                .into_response());
                        }
                    }
                }
            }
        }
        let stream = service
            .store
            .download_stream(claims.sub.0, key, range.offset, range.length)
            .await?;
        let body = ReaderStream::new(stream.reader).chain(futures_util::stream::once(async move {
            match stream.completion.await {
                Ok(Ok(())) => Ok(bytes::Bytes::new()),
                Ok(Err(e)) => Err(std::io::Error::other(e.to_string())),
                Err(e) => Err(std::io::Error::other(e.to_string())),
            }
        }));
        let total = stream.info.total_bytes;
        let etag = format!("\"{}\"", stream.info.integrity_root);
        let partial = stream.length > 0
            && (header_range_applied || range.offset > 0 || stream.length != total);
        let mut response = Response::builder()
            .status(if partial {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            })
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, stream.length)
            .header(header::ACCEPT_RANGES, "bytes")
            .header(header::ETAG, etag)
            .header(header::CACHE_CONTROL, "private, no-store");
        if partial {
            response = response.header(
                header::CONTENT_RANGE,
                format!(
                    "bytes {}-{}/{}",
                    range.offset,
                    range.offset + stream.length - 1,
                    total
                ),
            );
        }
        response
            .body(Body::from_stream(body))
            .map_err(|e| Error::Invalid(e.to_string()))
    }
    .await;
    result.unwrap_or_else(error)
}

async fn receive_backup(
    State(service): State<Arc<Service>>,
    Path(alias): Path<String>,
    headers: HeaderMap,
    upgrade: axum::extract::ws::WebSocketUpgrade,
) -> Response {
    let result = async {
        let claims = claims(&service, &headers)?;
        service.authorize(
            &claims,
            &Command::BackupReceive {
                repository: alias.clone(),
            },
        )?;
        let permit = service
            .transfers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Unavailable("replication admission is full".into()))?;
        let repo = service.open_repository(&alias, true).await?;
        Ok(upgrade
            .max_message_size(64 * 1024)
            .max_frame_size(64 * 1024)
            .on_upgrade(move |socket| async move {
                let _permit = permit;
                let (bridge, mut protocol) = tokio::io::duplex(128 * 1024);
                let (received, forwarded) = tokio::join!(
                    async move { crate::core::backup::network::receive(&mut protocol, repo).await },
                    crate::core::backup::network::websocket_server(socket, bridge)
                );
                if let Err(e) = received {
                    tracing::debug!("HTTP replication incomplete: {e}");
                }
                if let Err(e) = forwarded {
                    tracing::debug!("HTTP replication adapter ended: {e}");
                }
            })
            .into_response())
    }
    .await;
    result.unwrap_or_else(error)
}
