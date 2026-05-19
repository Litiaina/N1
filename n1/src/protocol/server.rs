use crate::protocol::auth::NativeSession;
use crate::protocol::dispatch::NativeApiDispatcher;
use crate::protocol::frame::{read_request, write_response};
use crate::protocol::ops::typed_api_request;
use crate::protocol::types::{
    N1_NATIVE_PROTOCOL_VERSION, NativeApiRequest, NativeAuthFragmentRequest, NativeBearerRequest,
    NativeRequest, NativeResponse,
};
use axum::Router;
use serde_json::{Value, json};
use std::io;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

pub async fn bind_native_protocol(addr: SocketAddr) -> io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

pub async fn serve_native_protocol(listener: TcpListener, addr: SocketAddr, router: Router) {
    info!("Native protocol => n1://{}", addr);

    let dispatcher = NativeApiDispatcher::new(router);
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                if let Err(err) = stream.set_nodelay(true) {
                    warn!(
                        "N1 native connection {} failed to set TCP_NODELAY: {}",
                        peer, err
                    );
                }
                let dispatcher = dispatcher.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, peer, dispatcher).await {
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
    dispatcher: NativeApiDispatcher,
) -> Result<(), String> {
    let mut session = NativeSession::default();
    loop {
        let Some(request) = read_request(&mut stream)
            .await
            .map_err(|err| format!("read frame failed: {err}"))?
        else {
            return Ok(());
        };

        let response = handle_request(&dispatcher, &mut session, peer, request).await;
        write_response(&mut stream, &response)
            .await
            .map_err(|err| format!("write frame failed: {err}"))?;
    }
}

async fn handle_request(
    dispatcher: &NativeApiDispatcher,
    session: &mut NativeSession,
    peer: SocketAddr,
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
                Ok(payload) => auth_fragment(dispatcher, session, peer, &id, payload).await,
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
        "api.request" => match payload_as::<NativeApiRequest>(request.payload.as_deref()) {
            Ok(api_request) => {
                dispatch_api(dispatcher, session, peer, id, api_request, request.body).await
            }
            Err(err) => NativeResponse::err(id, "bad_request", err),
        },
        op => match payload_value(request.payload.as_deref())
            .and_then(|payload| typed_api_request(op, &payload).map_err(|err| err.to_string()))
        {
            Ok(Some(api_request)) => {
                dispatch_api(dispatcher, session, peer, id, api_request, request.body).await
            }
            Ok(None) => NativeResponse::err(id, "unknown_op", format!("unsupported op '{op}'")),
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
    dispatcher: &NativeApiDispatcher,
    session: &mut NativeSession,
    peer: SocketAddr,
    id: &str,
    payload: NativeAuthFragmentRequest,
) -> NativeResponse {
    let api_request = NativeApiRequest {
        method: "POST".to_string(),
        path: "/n1/auth/fragment".to_string(),
        headers: Default::default(),
        json: Some(serde_json::to_value(payload).unwrap_or(Value::Null)),
    };
    let empty_session = NativeSession::default();
    let (api_response, body) = match dispatcher
        .dispatch(&empty_session, peer, api_request, None)
        .await
    {
        Ok(response) => response,
        Err(err) => return NativeResponse::err(id, "dispatch_failed", err),
    };
    if api_response.status >= 400 {
        return NativeResponse::api(id, api_response, body);
    }
    let Ok(value) = serde_json::from_slice::<Value>(&body) else {
        return NativeResponse::err(id, "auth.invalid_response", "auth response was not JSON");
    };
    let Some(token) = value.get("access_token").and_then(Value::as_str) else {
        return NativeResponse::err(
            id,
            "auth.invalid_response",
            "auth response did not include access_token",
        );
    };
    if let Err(err) = session.set_bearer(token.to_string()) {
        return NativeResponse::err(id, "auth.invalid_token", err);
    }
    NativeResponse::api(id, api_response, body)
}

async fn dispatch_api(
    dispatcher: &NativeApiDispatcher,
    session: &NativeSession,
    peer: SocketAddr,
    id: String,
    request: NativeApiRequest,
    body: Option<Vec<u8>>,
) -> NativeResponse {
    match dispatcher.dispatch(session, peer, request, body).await {
        Ok((payload, body)) => NativeResponse::api(id, payload, body),
        Err(err) => NativeResponse::err(id, "dispatch_failed", err),
    }
}
