use crate::core::io_scheduler::IoProgress;
use crate::core::upload::{AdaptiveIoGuard, acquire_upload_io_guard_for_query};
use crate::enums::app_error_enum::AppError;
use crate::protocol::auth::NativeSession;
use crate::protocol::frame::{read_request_control_with_limit, write_response};
use crate::protocol::ops::{
    NativeExecutionError, execute_native_op, execute_native_streaming_reader,
    prepare_native_download_stream, stream_native_download_plan,
};
use crate::protocol::types::{
    MAX_NATIVE_BODY_BYTES, MAX_NATIVE_FRAME_BYTES, N1_NATIVE_PROTOCOL_VERSION,
    NativeAuthFragmentRequest, NativeBearerRequest, NativeOpResponse, NativeRequest,
    NativeResponse,
};
use crate::routes::storage_routes::AppState;
use serde_json::{Value, json};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

const NATIVE_UPLOAD_DRAIN_BUFFER_BYTES: usize = 128 * 1024;
const PREAUTH_FRAME_LIMIT: usize = 64 * 1024;

pub fn native_connection_limit(value: Option<&str>) -> Result<usize, String> {
    let limit = value
        .unwrap_or("32")
        .parse::<usize>()
        .map_err(|_| "native_max_connections must be an integer from 1 to 4096")?;
    if !(1..=4096).contains(&limit) {
        return Err("native_max_connections must be from 1 to 4096".into());
    }
    Ok(limit)
}

pub async fn bind_native_protocol(addr: SocketAddr) -> io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

pub async fn serve_native_protocol(
    listener: TcpListener,
    addr: SocketAddr,
    state: Arc<AppState>,
    max_connections: usize,
) {
    info!(
        "Native protocol => n1://{} ({} connections)",
        addr, max_connections
    );
    let connections = Arc::new(Semaphore::new(max_connections));

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let Ok(permit) = connections.clone().try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                if let Err(err) = stream.set_nodelay(true) {
                    warn!(
                        "N1 native connection {} failed to set TCP_NODELAY: {}",
                        peer, err
                    );
                }
                let state = state.clone();
                tokio::spawn(async move {
                    let _permit = permit;
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
        let authenticated = session.claims().is_some();
        let timeout = if authenticated { 120 } else { 15 };
        let frame_limit = if authenticated {
            MAX_NATIVE_FRAME_BYTES
        } else {
            PREAUTH_FRAME_LIMIT
        };
        let Some(request) = tokio::time::timeout(
            Duration::from_secs(timeout),
            read_request_control_with_limit(&mut stream, frame_limit),
        )
        .await
        .map_err(|_| "native control frame timed out".to_string())?
        .map_err(|err| format!("read frame failed: {err}"))?
        else {
            return Ok(());
        };

        if let Err(error) = validate_request_body(&session, &request) {
            write_response(
                &mut stream,
                &NativeResponse::err(request.id.clone(), "bad_request", error),
            )
            .await
            .map_err(|err| err.to_string())?;
            // Unconsumed body bytes must never become another control frame.
            return Ok(());
        }

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

fn validate_request_body(session: &NativeSession, request: &NativeRequest) -> Result<(), String> {
    if request.body.is_some() && request.body_len.is_some() {
        return Err("use an inline body or a separate raw body, not both".into());
    }
    let length = request
        .body
        .as_ref()
        .map_or(request.body_len.unwrap_or(0), |body| body.len() as u64);
    if length > MAX_NATIVE_BODY_BYTES as u64 {
        return Err("native body exceeds limit".into());
    }
    if length > 0
        || (native_op_accepts_streaming_body(&request.op)
            && (request.body.is_some() || request.body_len.is_some()))
    {
        if !native_op_accepts_streaming_body(&request.op) {
            return Err("this operation does not accept a raw body".into());
        }
        let claims = session
            .claims()
            .ok_or("authenticate before sending upload bytes")?;
        if !claims.write {
            return Err("write permission required before sending upload bytes".into());
        }
    }
    Ok(())
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
                        "operator": claims.operator,
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
                    "operator": claims.operator,
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
                    && request.body_len.is_some()
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

    let progress = pre_gate.as_ref().map(AdaptiveIoGuard::progress_handle);
    let (result, remaining) = {
        let mut body_reader = NativeSocketReader {
            stream,
            remaining: body_len,
            progress,
            idle: Box::pin(tokio::time::sleep(Duration::from_secs(30))),
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

    if result.is_ok() && remaining == 0 {
        if let Some(guard) = pre_gate {
            guard.complete();
        }
    } else {
        drop(pre_gate);
    }
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
    progress: Option<IoProgress>,
    idle: Pin<Box<tokio::time::Sleep>>,
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
            std::task::Poll::Pending => {
                if this.idle.as_mut().poll(cx).is_ready() {
                    std::task::Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "native upload body stalled",
                    )))
                } else {
                    std::task::Poll::Pending
                }
            }
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
                this.idle
                    .as_mut()
                    .reset(tokio::time::Instant::now() + Duration::from_secs(30));
                if let Some(progress) = &this.progress {
                    progress.record(read as u64);
                }
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
        let read =
            tokio::time::timeout(Duration::from_secs(30), stream.read(&mut buffer[..wanted]))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "native rejected body stalled")
                })??;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn native_admission_is_configurable_and_bounded() {
        assert_eq!(native_connection_limit(None).unwrap(), 32);
        assert_eq!(native_connection_limit(Some("256")).unwrap(), 256);
        for invalid in ["0", "4097", "-1", "garbage"] {
            assert!(native_connection_limit(Some(invalid)).is_err());
        }
    }

    #[tokio::test]
    async fn preauth_frame_limit_rejects_length_before_reading_body() {
        let mut control = std::io::Cursor::new(((PREAUTH_FRAME_LIMIT + 1) as u32).to_be_bytes());
        assert_eq!(
            read_request_control_with_limit(&mut control, PREAUTH_FRAME_LIMIT)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(control.position(), 4);
    }

    #[tokio::test]
    async fn unauthenticated_declared_gigabyte_is_rejected_without_reading_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connection(
                stream,
                peer,
                Arc::new(AppState {
                    ws_manager: crate::websocket::manager::WsConnectionManager::new(),
                }),
            )
            .await
            .unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        let request = NativeRequest {
            id: "bounded".into(),
            op: "unknown".into(),
            payload: None,
            body: None,
            body_len: Some(MAX_NATIVE_BODY_BYTES as u64),
        };
        let control = rmp_serde::to_vec(&request).unwrap();
        client
            .write_all(&(control.len() as u32).to_be_bytes())
            .await
            .unwrap();
        client.write_all(&control).await.unwrap();
        // Send no raw bytes: old code would allocate 1 GiB and block here.
        let length = tokio::time::timeout(Duration::from_secs(2), client.read_u32())
            .await
            .unwrap()
            .unwrap();
        let mut reply = vec![0; length as usize];
        client.read_exact(&mut reply).await.unwrap();
        assert!(!rmp_serde::from_slice::<NativeResponse>(&reply).unwrap().ok);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn zero_byte_uploads_also_require_auth_and_body_encoding_is_unambiguous() {
        let session = NativeSession::default();
        let mut request = NativeRequest {
            id: "empty".into(),
            op: "upload.one_shot".into(),
            payload: None,
            body: None,
            body_len: Some(0),
        };
        assert!(validate_request_body(&session, &request).is_err());
        request.op = "ping".into();
        request.body = Some(serde_bytes::ByteBuf::from(Vec::new()));
        assert!(validate_request_body(&session, &request).is_err());
        request.body = None;
        assert!(validate_request_body(&session, &request).is_ok());
    }
}
