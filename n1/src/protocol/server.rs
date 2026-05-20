use crate::protocol::auth::NativeSession;
use crate::protocol::frame::{read_request_body, read_request_control, write_response};
use crate::protocol::ops::{NativeExecutionError, execute_native_op, execute_native_streaming_op};
use crate::protocol::types::{
    MAX_NATIVE_BODY_BYTES, N1_NATIVE_PROTOCOL_VERSION, NativeAuthFragmentRequest,
    NativeBearerRequest, NativeOpResponse, NativeRequest, NativeResponse,
};
use crate::routes::storage_routes::AppState;
use axum::body::Body;
use serde_json::{Value, json};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::io::ReaderStream;
use tracing::{debug, info, warn};

const NATIVE_UPLOAD_PIPE_BYTES: usize = 2 * 1024 * 1024;
const NATIVE_UPLOAD_COPY_BUFFER_BYTES: usize = 256 * 1024;

pub async fn bind_native_protocol(addr: SocketAddr) -> io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

pub async fn serve_native_protocol(listener: TcpListener, addr: SocketAddr, state: Arc<AppState>) {
    info!("Native protocol => n1://{}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                if let Err(err) = stream.set_nodelay(true) {
                    warn!(
                        "N1 native connection {} failed to set TCP_NODELAY: {}",
                        peer, err
                    );
                }
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, peer, state).await {
                        debug!("N1 native connection {} closed: {}", peer, err);
                    }
                });
            }
            Err(err) => warn!("N1 native accept failed: {}", err),
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    state: Arc<AppState>,
) -> Result<(), String> {
    let mut session = NativeSession::default();
    loop {
        let Some(request) = read_request_control(&mut stream)
            .await
            .map_err(|err| format!("read frame failed: {err}"))?
        else {
            return Ok(());
        };

        let response = handle_request(&state, &mut session, peer, request, &mut stream).await;
        write_response(&mut stream, &response)
            .await
            .map_err(|err| format!("write frame failed: {err}"))?;
    }
}

async fn handle_request(
    state: &Arc<AppState>,
    session: &mut NativeSession,
    _peer: SocketAddr,
    request: NativeRequest,
    stream: &mut TcpStream,
) -> NativeResponse {
    let id = request.id.clone();
    match request.op.as_str() {
        "ping" => NativeResponse::ok(
            id,
            json!({
                "protocol": N1_NATIVE_PROTOCOL_VERSION,
                "server": "Litiaina N1",
                "transport": "n1://",
            }),
        ),
        "auth.clear" | "logout" => {
            session.clear();
            NativeResponse::ok(id, json!({"authenticated": false}))
        }
        "auth.bearer" => match payload_as::<NativeBearerRequest>(request.payload.as_deref()) {
            Ok(payload) => match session.set_bearer(payload.token) {
                Ok(claims) => NativeResponse::ok(
                    id,
                    json!({
                        "authenticated": true,
                        "fragment": claims.fragment,
                        "read": claims.read,
                        "write": claims.write,
                        "modify": claims.modify,
                        "delete": claims.delete,
                        "expires_at": claims.exp,
                    }),
                ),
                Err(err) => NativeResponse::err(id, "auth.invalid_token", err),
            },
            Err(err) => NativeResponse::err(id, "bad_request", err.to_string()),
        },
        "auth.fragment" => {
            match payload_as::<NativeAuthFragmentRequest>(request.payload.as_deref()) {
                Ok(payload) => auth_fragment(session, &id, payload).await,
                Err(err) => NativeResponse::err(id, "bad_request", err),
            }
        }
        "session.info" => {
            let payload = match session.claims() {
                Some(claims) => json!({
                    "authenticated": true,
                    "fragment": claims.fragment,
                    "read": claims.read,
                    "write": claims.write,
                    "modify": claims.modify,
                    "delete": claims.delete,
                    "expires_at": claims.exp,
                }),
                None => json!({"authenticated": false}),
            };
            NativeResponse::ok(id, payload)
        }
        op => match payload_value(request.payload.as_deref()) {
            Ok(payload) => {
                let result = if request.body.is_none()
                    && request.body_len.unwrap_or(0) > 0
                    && native_op_accepts_streaming_body(op)
                {
                    execute_native_streaming_body(
                        state.clone(),
                        session.clone(),
                        op.to_string(),
                        payload,
                        request.body_len.unwrap_or(0),
                        stream,
                    )
                    .await
                } else {
                    let body = if let Some(body) = request.body {
                        Some(body.into_vec())
                    } else if let Some(body_len) = request.body_len.filter(|value| *value > 0) {
                        match read_request_body(stream, body_len).await {
                            Ok(body) => Some(body),
                            Err(err) => {
                                return NativeResponse::err(
                                    id,
                                    "bad_request",
                                    format!("read native body failed: {err}"),
                                );
                            }
                        }
                    } else {
                        None
                    };
                    execute_native_op(state.clone(), session, op, payload, body).await
                };

                match result {
                    Ok(Some((payload, body))) => NativeResponse::operation(id, payload, body),
                    Ok(None) => {
                        NativeResponse::err(id, "unknown_op", format!("unsupported op '{op}'"))
                    }
                    Err(err) => NativeResponse::operation(id, err.0, err.1),
                }
            }
            Err(err) => NativeResponse::err(id, "bad_request", err),
        },
    }
}

async fn execute_native_streaming_body(
    state: Arc<AppState>,
    session: NativeSession,
    op: String,
    payload: Value,
    body_len: u64,
    stream: &mut TcpStream,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, NativeExecutionError> {
    if body_len as u128 > MAX_NATIVE_BODY_BYTES as u128 {
        return Err(native_protocol_error(
            413,
            format!("native body exceeds {MAX_NATIVE_BODY_BYTES} bytes"),
        ));
    }

    let (pipe_reader, pipe_writer) = duplex(NATIVE_UPLOAD_PIPE_BYTES);
    let body = Body::from_stream(ReaderStream::new(pipe_reader));
    let op_for_task = op.clone();
    let upload_task = tokio::spawn(async move {
        execute_native_streaming_op(state, &session, &op_for_task, payload, body, body_len).await
    });

    if let Err(err) = pump_native_body(stream, pipe_writer, body_len).await {
        upload_task.abort();
        return Err(native_protocol_error(
            400,
            format!("read native upload body failed: {err}"),
        ));
    }

    match upload_task.await {
        Ok(result) => result,
        Err(err) => Err(native_protocol_error(
            500,
            format!("native upload task failed: {err}"),
        )),
    }
}

async fn pump_native_body<R>(
    reader: &mut R,
    mut writer: DuplexStream,
    body_len: u64,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut remaining = body_len;
    let mut writer_open = true;
    let mut buffer = vec![0u8; NATIVE_UPLOAD_COPY_BUFFER_BYTES];

    while remaining > 0 {
        let wanted = (remaining as usize).min(buffer.len());
        let read = reader.read(&mut buffer[..wanted]).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "native upload body ended early",
            ));
        }
        remaining -= read as u64;

        if writer_open {
            match writer.write_all(&buffer[..read]).await {
                Ok(()) => {}
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                    ) =>
                {
                    writer_open = false;
                }
                Err(err) => return Err(err),
            }
        }
    }

    if writer_open {
        writer.shutdown().await?;
    }
    Ok(())
}

fn native_op_accepts_streaming_body(op: &str) -> bool {
    matches!(
        op,
        "upload.one_shot"
            | "object.upload.one_shot"
            | "multipart.put_part"
            | "upload.multipart.put_part"
    )
}

fn native_protocol_error(status: u16, error: impl std::fmt::Display) -> NativeExecutionError {
    NativeExecutionError(
        crate::protocol::ops::op_response(status),
        crate::protocol::ops::native_body_bytes(&json!({ "error": error.to_string() })),
    )
}

fn payload_value(payload: Option<&str>) -> Result<Value, String> {
    match payload {
        Some(payload) if !payload.trim().is_empty() => {
            serde_json::from_str(payload).map_err(|err| format!("invalid payload json: {err}"))
        }
        _ => Ok(Value::Null),
    }
}

fn payload_as<T>(payload: Option<&str>) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(payload_value(payload)?).map_err(|err| err.to_string())
}

async fn auth_fragment(
    session: &mut NativeSession,
    id: &str,
    payload: NativeAuthFragmentRequest,
) -> NativeResponse {
    let body = match crate::middleware::auth_middleware::authorize_fragment_core(
        crate::structs::auth_structs::AuthorizationRequest {
            fragment: payload.fragment,
            secret: payload.secret,
            read: payload.read,
            write: payload.write,
            modify: payload.modify,
            delete: payload.delete,
            expires_in: payload.expires_in,
        },
    )
    .await
    {
        Ok(body) => body,
        Err(err) => {
            return NativeResponse::operation(
                id,
                crate::protocol::ops::auth_error_response(&err),
                crate::protocol::ops::native_body_bytes(&json!({
                    "error": crate::protocol::ops::auth_error_message(&err)
                })),
            );
        }
    };
    let token = body.access_token.clone();
    if let Err(err) = session.set_bearer(token.to_string()) {
        return NativeResponse::err(id, "auth.invalid_token", err);
    }
    NativeResponse::operation(
        id,
        crate::protocol::ops::op_response(200),
        crate::protocol::ops::native_body_bytes(&body),
    )
}
