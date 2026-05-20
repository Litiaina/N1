use crate::protocol::auth::NativeSession;
use crate::protocol::frame::{read_request, write_response};
use crate::protocol::ops::execute_native_op;
use crate::protocol::types::{
    N1_NATIVE_PROTOCOL_VERSION, NativeAuthFragmentRequest, NativeBearerRequest, NativeRequest,
    NativeResponse,
};
use crate::routes::storage_routes::AppState;
use serde_json::{Value, json};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

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
        let Some(request) = read_request(&mut stream)
            .await
            .map_err(|err| format!("read frame failed: {err}"))?
        else {
            return Ok(());
        };

        let response = handle_request(&state, &mut session, peer, request).await;
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
                match execute_native_op(
                    state.clone(),
                    session,
                    op,
                    payload,
                    request.body.map(|body| body.into_vec()),
                )
                .await
                {
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
