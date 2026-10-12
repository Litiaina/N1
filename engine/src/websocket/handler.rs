use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::{
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{Validation, decode};
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};
use uuid::Uuid;

use crate::middleware::auth_middleware::JWT_KEYS;
use crate::websocket::ws_structs::BroadcastReq;
use crate::{
    routes::storage_routes::AppState,
    structs::auth_structs::Claims,
    websocket::ws_structs::{ClientMessage, ServerMessage, WsAuthParams},
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<WsAuthParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let decoding_key = &JWT_KEYS.decoding;
    let mut validation = Validation::default();
    validation.validate_exp = true;

    let claims = match decode::<Claims>(&params.token, decoding_key, &validation) {
        Ok(c) => c.claims,
        Err(e) => {
            warn!("Websocket auth failed: {}", e);
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };
    if !claims.read {
        return StatusCode::FORBIDDEN.into_response();
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state, claims))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, claims: Claims) {
    let fragment = claims.fragment.clone();
    let expiry = tokio::time::sleep(Duration::from_secs(
        (claims.exp as u64).saturating_sub(chrono::Utc::now().timestamp().max(0) as u64),
    ));
    let conn_id = Uuid::new_v4().to_string();

    info!(fragment = %fragment, conn_id = %conn_id, "Websocket client connected");

    let (mut sender, mut receiver) = socket.split();

    let (tx, mut rx) = mpsc::channel::<ServerMessage>(100);

    let mut broadcast_rx = state.ws_manager.subscribe(&fragment);

    let _ = tx
        .send(ServerMessage::Hello {
            connection_id: conn_id.clone(),
        })
        .await;

    let mut write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = match serde_json::to_string(&msg) {
                Ok(j) => j,
                Err(_) => continue,
            };

            if sender.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
        let _ = sender.close().await;
    });

    let tx_logic = tx.clone();
    let mut logic_task = tokio::spawn(async move {
        tokio::pin!(expiry);
        let mut hb_interval = interval(HEARTBEAT_INTERVAL);
        hb_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut last_heartbeat = Instant::now();

        loop {
            tokio::select! {
                _ = &mut expiry => break,
                event_res = broadcast_rx.recv() => {
                    match event_res {
                        Ok(event) => {
                            match tx_logic.try_send(ServerMessage::Event(event)) {
                                Ok(_) => {},
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!("Websocket client {} buffer full (slow consumer). Disconnecting.", fragment);
                                    break;
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => break,
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            warn!("Websocket client {} lagged behind namespace events. Disconnecting.", fragment);
                            break;
                        }
                        Err(_) => break,
                    }
                }

                _ = hb_interval.tick() => {
                    if last_heartbeat.elapsed() > CLIENT_TIMEOUT {
                        warn!("Websocket client timed out");
                        break;
                    }
                }

                msg = receiver.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            last_heartbeat = Instant::now();
                            if let Ok(ClientMessage::Ping) = serde_json::from_str::<ClientMessage>(&text) {
                                let _ = tx_logic.try_send(ServerMessage::Pong);
                            }
                        }
                        Some(Ok(Message::Close(_))) => break,
                        Some(Err(_)) => break,
                        None => break,
                        _ => { last_heartbeat = Instant::now(); }
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = &mut write_task => logic_task.abort(),
        _ = &mut logic_task => write_task.abort(),
    }

    info!("Websocket client disconnected");
    state.ws_manager.cleanup_connection(&claims.fragment);
}

pub async fn system_broadcast_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BroadcastReq>,
) -> StatusCode {
    state
        .ws_manager
        .broadcast_announcement(&payload.message, &payload.severity);
    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn websocket_requires_read_and_closes_when_token_expires() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        crate::middleware::auth_middleware::tests::ensure_test_env();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new()
            .route("/ws", axum::routing::get(ws_handler))
            .with_state(Arc::new(AppState {
                ws_manager: crate::websocket::manager::WsConnectionManager::new(),
            }));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        for read in [false, true] {
            let claims = Claims {
                operator: false,
                fragment: "ws-test".into(),
                fragment_alias: None,
                read,
                write: false,
                modify: false,
                delete: false,
                exp: (chrono::Utc::now().timestamp() + 2) as usize,
            };
            let token = jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &claims,
                &JWT_KEYS.encoding,
            )
            .unwrap();
            let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
            client.write_all(format!("GET /ws?token={token} HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").as_bytes()).await.unwrap();
            let mut header = Vec::new();
            tokio::time::timeout(Duration::from_secs(2), async {
                while !header.ends_with(b"\r\n\r\n") {
                    header.push(client.read_u8().await.unwrap());
                    assert!(header.len() <= 4096);
                }
            })
            .await
            .unwrap();
            let header = String::from_utf8(header).unwrap();
            if read {
                assert!(header.starts_with("HTTP/1.1 101"), "{header}");
                // A valid masked empty ping doesn't extend token authority.
                client.write_all(&[0x89, 0x80, 0, 0, 0, 0]).await.unwrap();
                let mut frames = Vec::new();
                tokio::time::timeout(Duration::from_secs(4), client.read_to_end(&mut frames))
                    .await
                    .expect("expired socket must close before heartbeat inactivity timeout")
                    .unwrap();
            } else {
                assert!(header.starts_with("HTTP/1.1 403"), "{header}");
            }
        }
        server.abort();
    }
}
