//! Named NOA uses the original n1/0.5 frames. Transport dispatch shares the
//! domain handlers; no HTTP loopback or metadata dependency is added to LINK.
mod dispatch;
pub mod frame;
pub mod types;
use super::{App, error::ApiError};
use crate::middleware::auth;
use axum::{body::Body, http::HeaderMap};
use futures_util::TryStreamExt;
use serde_json::{Value, json};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use types::{MAX_NATIVE_FRAME_BYTES, NativeResponse};

pub async fn serve(
    listener: TcpListener,
    app: Arc<App>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    stop: CancellationToken,
) -> io::Result<()> {
    let slots = Arc::new(tokio::sync::Semaphore::new(
        app.config.native_max_connections,
    ));
    let mut workers = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            done = workers.join_next(), if !workers.is_empty() => {
                let _ = done;
            }
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    drop(socket);
                    continue;
                };
                socket.set_nodelay(true)?;
                let app = app.clone();
                let tls = tls.clone();
                workers.spawn(async move {
                    let _permit = permit;
                    if let Some(tls) = tls {
                        if let Ok(Ok(socket)) =
                            tokio::time::timeout(Duration::from_secs(15), tls.accept(socket)).await
                        {
                            let _ = connection(socket, app).await;
                        }
                    } else {
                        let _ = connection(socket, app).await;
                    }
                });
            }
        }
    }
    if tokio::time::timeout(Duration::from_secs(30), async {
        while workers.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }
    Ok(())
}
async fn connection<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    socket: S,
    app: Arc<App>,
) -> io::Result<()> {
    let (mut input, mut output) = tokio::io::split(socket);
    let router = super::router(app.clone());
    let mut token: Option<String> = None;
    loop {
        let limit = if token.is_some() {
            MAX_NATIVE_FRAME_BYTES
        } else {
            64 * 1024
        };
        let Some(request) = tokio::time::timeout(
            Duration::from_secs(if token.is_some() { 120 } else { 15 }),
            frame::read_request_control_with_limit(&mut input, limit),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "native control stalled"))??
        else {
            return Ok(());
        };
        let body_len = request.body_len.unwrap_or(0);
        if body_len > types::MAX_NATIVE_BODY_BYTES as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native body exceeds limit",
            ));
        }
        let payload: Value = match request
            .payload
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
        {
            Ok(value) => value.unwrap_or_else(|| json!({})),
            Err(_) => {
                frame::write_response(
                    &mut output,
                    &NativeResponse::err(request.id, "bad_request", "invalid JSON payload"),
                )
                .await?;
                if body_len > 0 {
                    return Ok(());
                }
                continue;
            }
        };
        if matches!(
            request.op.as_str(),
            "ping" | "auth.fragment" | "auth.bearer" | "session.info" | "auth.clear" | "logout"
        ) {
            if body_len > 0 || request.body.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "control operation cannot carry bytes",
                ));
            }
            let result = match request.op.as_str() {
                "ping" => Ok(json!({
                    "protocol": types::N1_NATIVE_PROTOCOL_VERSION,
                    "server": "Litiaina M1",
                    "transport": "n1://",
                    "object_api_version": 2
                })),
                "auth.clear" | "logout" => {
                    token = None;
                    Ok(json!({"authenticated":false}))
                }
                "auth.fragment" => match serde_json::from_value::<auth::LoginRequest>(payload) {
                    Ok(request) => match auth::login(&app, request).await {
                        Ok(response) => {
                            token = response["access_token"].as_str().map(str::to_owned);
                            Ok(response)
                        }
                        Err(e) => Err(e),
                    },
                    Err(_) => Err(ApiError::invalid("invalid fragment credentials")),
                },
                "auth.bearer" => {
                    let candidate = payload["token"].as_str().unwrap_or("").to_owned();
                    let mut headers = HeaderMap::new();
                    if let Ok(value) = format!("Bearer {candidate}").parse() {
                        headers.insert("authorization", value);
                    }
                    match auth::authorize(&app, &headers, "session").await {
                        Ok(claims) => {
                            token = Some(candidate);
                            Ok(session_json(&claims))
                        }
                        Err(e) => Err(e),
                    }
                }
                "session.info" => {
                    if let Some(token) = &token {
                        let mut headers = HeaderMap::new();
                        headers.insert(
                            "authorization",
                            format!("Bearer {token}")
                                .parse()
                                .map_err(io::Error::other)?,
                        );
                        auth::authorize(&app, &headers, "session")
                            .await
                            .map(|claims| session_json(&claims))
                    } else {
                        Ok(json!({"authenticated":false}))
                    }
                }
                _ => unreachable!(),
            };
            let response = match result {
                Ok(value) => NativeResponse::ok(request.id, value),
                Err(error) => {
                    token = None;
                    NativeResponse::err(request.id, error.0.as_u16().to_string(), error.1)
                }
            };
            frame::write_response(&mut output, &response).await?;
            continue;
        }
        if body_len > 0 || request.body.is_some() {
            let mut headers = HeaderMap::new();
            if let Some(token) = &token {
                headers.insert(
                    "authorization",
                    format!("Bearer {token}")
                        .parse()
                        .map_err(io::Error::other)?,
                );
            }
            if auth::authorize(&app, &headers, "write").await.is_err() {
                frame::write_response(
                    &mut output,
                    &NativeResponse::err(
                        request.id,
                        "401",
                        "authenticate with write permission before sending bytes",
                    ),
                )
                .await?;
                return Ok(());
            }
            if !matches!(
                request.op.as_str(),
                "upload.one_shot"
                    | "object.upload.one_shot"
                    | "multipart.put_part"
                    | "upload.multipart.put_part"
            ) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "operation does not accept bytes",
                ));
            }
        }
        let response = if let Some(bytes) = request.body.clone() {
            dispatch::dispatch(
                router.clone(),
                &request,
                payload,
                token.as_deref(),
                Body::from(bytes.into_vec()),
                None,
            )
            .await
        } else if request.body_len.is_some() {
            let (reader, mut writer) = tokio::io::duplex(256 * 1024);
            let body = Body::from_stream(tokio_util::io::ReaderStream::new(reader));
            let router = router.clone();
            let request_copy = request.clone();
            let token = token.clone();
            let work = tokio::spawn(async move {
                dispatch::dispatch(
                    router,
                    &request_copy,
                    payload,
                    token.as_deref(),
                    body,
                    Some(body_len),
                )
                .await
            });
            let copied = tokio::io::copy(&mut (&mut input).take(body_len), &mut writer).await;
            drop(writer);
            let result = work.await.map_err(io::Error::other)?;
            if copied? != body_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "native body ended early",
                ));
            }
            result
        } else {
            dispatch::dispatch(
                router.clone(),
                &request,
                payload,
                token.as_deref(),
                Body::empty(),
                None,
            )
            .await
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                frame::write_response(
                    &mut output,
                    &NativeResponse::err(request.id, error.0.as_u16().to_string(), error.1),
                )
                .await?;
                continue;
            }
        };
        let status = response.status().as_u16();
        let length = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        let mut source = tokio_util::io::StreamReader::new(
            response
                .into_body()
                .into_data_stream()
                .map_err(io::Error::other),
        );
        if let Some(length) = length {
            let response = NativeResponse {
                id: request.id,
                ok: status < 400,
                status: Some(status),
                payload: None,
                body: None,
                body_len: Some(length),
                error: None,
            };
            frame::write_response(&mut output, &response).await?;
            let n = tokio::io::copy(&mut source, &mut output).await?;
            if n != length {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "native response length mismatch",
                ));
            }
            output.flush().await?;
        } else {
            let mut bytes = Vec::new();
            (&mut source)
                .take(8 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .await?;
            if bytes.len() > 8 * 1024 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native control response exceeds bound",
                ));
            }
            frame::write_response(
                &mut output,
                &NativeResponse::operation(request.id, types::NativeOpResponse { status }, bytes),
            )
            .await?;
        }
    }
}
fn session_json(claims: &auth::Claims) -> Value {
    json!({
        "authenticated": true,
        "fragment": claims.fragment,
        "read": claims.read,
        "write": claims.write,
        "modify": claims.modify,
        "delete": claims.delete,
        "operator": claims.operator,
        "expires_at": claims.exp
    })
}
