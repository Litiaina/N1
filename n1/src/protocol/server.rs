use crate::core::upload::{AdaptiveIoGuard, acquire_upload_io_guard_for_query};
use crate::enums::app_error_enum::AppError;
use crate::protocol::auth::NativeSession;
use crate::protocol::frame::{read_request_body, read_request_control, write_response};
use crate::protocol::ops::{
    NativeExecutionError, execute_native_op, execute_native_streaming_reader,
    prepare_native_download_stream, stream_native_download_plan,
};
use crate::protocol::types::{
    MAX_NATIVE_BODY_BYTES, N1_NATIVE_PROTOCOL_VERSION, NativeAuthFragmentRequest,
    NativeBearerRequest, NativeOpResponse, NativeRequest, NativeResponse,
};
use crate::routes::storage_routes::AppState;
use serde_json::{Value, json};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

const NATIVE_UPLOAD_DRAIN_BUFFER_BYTES: usize = 128 * 1024;

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

        match handle_request(&state, &mut session, peer, request, &mut stream).await {
            NativeRequestOutcome::Response(response) => {
                write_response(&mut stream, &response)
                    .await
                    .map_err(|err| format!("write frame failed: {err}"))?;
            }
            NativeRequestOutcome::Written => {}
        }
    }
}

enum NativeRequestOutcome {
    Response(NativeResponse),
    Written,
}

async fn handle_request(
    state: &Arc<AppState>,
    session: &mut NativeSession,
    _peer: SocketAddr,
    request: NativeRequest,
    stream: &mut TcpStream,
) -> NativeRequestOutcome {
    let id = request.id.clone();
    let response = match request.op.as_str() {
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
                if request.body.is_none()
                    && request.body_len.unwrap_or(0) == 0
                    && native_op_streams_response_body(op)
                {
                    return match execute_native_streaming_response_body(
                        session.clone(),
                        op,
                        payload,
                        stream,
                        &id,
                    )
                    .await
                    {
                        Ok(()) => NativeRequestOutcome::Written,
                        Err(err) => NativeRequestOutcome::Response(NativeResponse::operation(
                            id, err.0, err.1,
                        )),
                    };
                }

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
                        &id,
                    )
                    .await
                } else {
                    let body = if let Some(body) = request.body {
                        Some(body.into_vec())
                    } else if let Some(body_len) = request.body_len.filter(|value| *value > 0) {
                        match read_request_body(stream, body_len).await {
                            Ok(body) => Some(body),
                            Err(err) => {
                                return NativeRequestOutcome::Response(NativeResponse::err(
                                    id,
                                    "bad_request",
                                    format!("read native body failed: {err}"),
                                ));
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
    };
    NativeRequestOutcome::Response(response)
}

async fn execute_native_streaming_body(
    state: Arc<AppState>,
    session: NativeSession,
    op: String,
    mut payload: Value,
    body_len: u64,
    stream: &mut TcpStream,
    request_id: &str,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, NativeExecutionError> {
    if body_len as u128 > MAX_NATIVE_BODY_BYTES as u128 {
        return Err(native_protocol_error(
            413,
            format!("native body exceeds {MAX_NATIVE_BODY_BYTES} bytes"),
        ));
    }

    let pre_gate = if bool_value(payload.get("expect_continue")).unwrap_or(false) {
        let guard = acquire_native_upload_continue_gate(&session, &op, &payload).await?;
        if guard.is_some() {
            if let Value::Object(ref mut map) = payload {
                map.insert("io_gate".to_string(), Value::Bool(false));
            }
        }
        let continue_response = NativeResponse::operation(
            request_id.to_string(),
            NativeOpResponse { status: 100 },
            Vec::new(),
        );
        write_response(stream, &continue_response)
            .await
            .map_err(|err| {
                native_protocol_error(500, format!("native continue write failed: {err}"))
            })?;
        guard
    } else {
        None
    };

    let (result, remaining) = {
        let mut body_reader = NativeSocketReader {
            stream,
            remaining: body_len,
        };
        let result = execute_native_streaming_reader(
            state,
            &session,
            &op,
            payload,
            &mut body_reader,
            body_len,
        )
        .await;
        (result, body_reader.remaining)
    };

    if remaining > 0 {
        if let Err(err) = drain_native_remaining(stream, remaining).await {
            return Err(native_protocol_error(
                400,
                format!("drain native upload body failed: {err}"),
            ));
        }
    }

    drop(pre_gate);
    result
}

async fn execute_native_streaming_response_body(
    session: NativeSession,
    op: &str,
    payload: Value,
    stream: &mut TcpStream,
    request_id: &str,
) -> Result<(), NativeExecutionError> {
    match op {
        "object.download_stream" | "download.stream" | "object.view_stream" => {
            let plan = prepare_native_download_stream(&session, payload)
                .await
                .map_err(native_app_error_for_continue)?;
            let response = NativeResponse {
                id: request_id.to_string(),
                ok: true,
                status: Some(200),
                payload: Some(serde_json::to_string(&plan.payload).unwrap_or_else(|_| "{}".into())),
                body: None,
                body_len: Some(plan.body_len),
                error: None,
            };
            write_response(stream, &response).await.map_err(|err| {
                native_protocol_error(500, format!("native download control write failed: {err}"))
            })?;
            stream_native_download_plan(stream, plan)
                .await
                .map_err(native_app_error_for_continue)?;
            Ok(())
        }
        _ => Err(native_protocol_error(
            400,
            format!("op '{op}' does not stream a native response body"),
        )),
    }
}

struct NativeSocketReader<'a> {
    stream: &'a mut TcpStream,
    remaining: u64,
}

impl AsyncRead for NativeSocketReader<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return std::task::Poll::Ready(Ok(()));
        }

        let cap = buf.remaining();
        if cap == 0 {
            return std::task::Poll::Ready(Ok(()));
        }

        let allowed = this.remaining.min(cap as u64) as usize;
        let mut limited = buf.take(allowed);
        match Pin::new(&mut *this.stream).poll_read(cx, &mut limited) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Ok(())) => {
                let read = limited.filled().len();
                if read == 0 {
                    return std::task::Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "native upload body ended early",
                    )));
                }
                // The limited child ReadBuf filled bytes inside the parent unfilled
                // region. Reflect that initialization before advancing the parent.
                unsafe {
                    buf.assume_init(read);
                }
                buf.advance(read);
                this.remaining = this.remaining.saturating_sub(read as u64);
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(Err(err)) => std::task::Poll::Ready(Err(err)),
        }
    }
}

async fn drain_native_remaining(stream: &mut TcpStream, mut remaining: u64) -> io::Result<()> {
    let mut buffer = vec![0u8; NATIVE_UPLOAD_DRAIN_BUFFER_BYTES];
    while remaining > 0 {
        let wanted = (remaining as usize).min(buffer.len());
        let read = stream.read(&mut buffer[..wanted]).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "native upload body ended early",
            ));
        }
        remaining = remaining.saturating_sub(read as u64);
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

fn native_op_streams_response_body(op: &str) -> bool {
    matches!(
        op,
        "object.download_stream" | "download.stream" | "object.view_stream"
    )
}

async fn acquire_native_upload_continue_gate(
    session: &NativeSession,
    op: &str,
    payload: &Value,
) -> Result<Option<AdaptiveIoGuard>, NativeExecutionError> {
    let Some(claims) = session.claims() else {
        return Err(native_protocol_error(
            401,
            "native upload requires authentication",
        ));
    };
    if !claims.write {
        return Err(native_protocol_error(403, "Write permission required"));
    }

    let io_gate_override = bool_value(payload.get("io_gate"));
    if matches!(io_gate_override, Some(false)) {
        return Ok(None);
    }

    let context = match op {
        "upload.one_shot" | "object.upload.one_shot" => {
            let object_key =
                string_value(payload.get("object_key")).unwrap_or_else(|| "<missing>".to_string());
            format!(
                "native continue fragment={}, object_key={}",
                claims.fragment, object_key
            )
        }
        "multipart.put_part" | "upload.multipart.put_part" => {
            let version_id =
                string_value(payload.get("version_id")).unwrap_or_else(|| "<missing>".to_string());
            let part_index = payload
                .get("part_index")
                .and_then(Value::as_u64)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<missing>".to_string());
            format!(
                "native continue fragment={}, version={}, part={}",
                claims.fragment, version_id, part_index
            )
        }
        _ => return Ok(None),
    };

    acquire_upload_io_guard_for_query(io_gate_override, &claims.fragment, &context)
        .await
        .map_err(native_app_error_for_continue)
}

fn native_app_error_for_continue(err: AppError) -> NativeExecutionError {
    let status = match err {
        AppError::Forbidden(_) => 403,
        AppError::Unauthorized => 401,
        AppError::BadRequest(_) => 400,
        AppError::PayloadTooLarge(_) => 413,
        AppError::Conflict(_) => 409,
        AppError::NotFound => 404,
        AppError::ServiceUnavailable(_) => 503,
        _ => 500,
    };
    native_protocol_error(status, err)
}

fn native_protocol_error(status: u16, error: impl std::fmt::Display) -> NativeExecutionError {
    NativeExecutionError(
        crate::protocol::ops::op_response(status),
        crate::protocol::ops::native_body_bytes(&json!({ "error": error.to_string() })),
    )
}

fn bool_value(value: Option<&Value>) -> Option<bool> {
    match value? {
        Value::Bool(value) => Some(*value),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        },
        Value::Number(value) => value.as_u64().map(|value| value != 0),
        _ => None,
    }
}

fn string_value(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
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
