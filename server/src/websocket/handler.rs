use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::{
    extract::{Query, State, WebSocketUpgrade, ws::{Message, WebSocket}},
    http::StatusCode,
    response::IntoResponse,
};
use futures_util::{StreamExt, SinkExt};
use jsonwebtoken::{Validation, decode};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};
use uuid::Uuid;

use crate::websocket::ws_structs::BroadcastReq;
use crate::{
    middleware::auth_middleware::JWT_KEYS,
    routes::storage_routes::AppState,
    structs::auth_structs::Claims,
    websocket::{
        ws_structs::{WsAuthParams, ServerMessage, ClientMessage, StorageEventPayload},
    },
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

    ws.on_upgrade(move |socket| handle_socket(socket, state, claims))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, claims: Claims) {
    let uid = claims.uid.clone();
    let conn_id = Uuid::new_v4().to_string();
    
    info!(uid = %uid, conn_id = %conn_id, "Websocket Client connected");

    let (mut sender, mut receiver) = socket.split();

    let mut rx = state.ws_manager.subscribe(&uid);

    let hello = ServerMessage::Hello { connection_id: conn_id.clone() };
    if let Ok(msg) = serde_json::to_string(&hello) {
        if let Err(e) = sender.send(Message::Text(msg.into())).await {
            warn!(uid = %uid, "Websocket Failed to send hello: {}", e);
            return;
        }
    }

    let mut hb_interval = interval(HEARTBEAT_INTERVAL);
    hb_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_heartbeat = Instant::now();
    let (socket_tx, mut socket_rx) = tokio::sync::mpsc::channel::<ServerMessage>(100);

    let write_task = tokio::spawn(async move {
        while let Some(msg) = socket_rx.recv().await {
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

    loop {
        tokio::select! {
            event_result = rx.recv() => {
                match event_result {
                    Ok(event) => {
                        let _ = socket_tx.send(ServerMessage::Event(event)).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(uid = %uid, skipped = %skipped, "WS: Client lagged. Sending StateInvalidated.");
                        let _ = socket_tx.send(ServerMessage::Event(crate::websocket::ws_structs::StorageEvent {
                            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                            payload: StorageEventPayload::StateInvalidated
                        })).await;
                    }
                    Err(_) => break,
                }
            }

            _ = hb_interval.tick() => {
                if last_heartbeat.elapsed() > CLIENT_TIMEOUT {
                    warn!(uid = %uid, "Websocket Client timed out (no heartbeat). Disconnecting.");
                    break; 
                }
            }

            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        last_heartbeat = Instant::now();

                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(client_msg) => {
                                match client_msg {
                                    ClientMessage::Ping => {
                                        let _ = socket_tx.send(ServerMessage::Pong).await;
                                    }
                                }
                            },
                            Err(e) => {
                                warn!(uid = %uid, "Websocket Invalid payload: {}", e);
                                let _ = socket_tx.send(ServerMessage::Error {
                                    code: "INVALID_PAYLOAD".to_string(),
                                    message: "Message could not be parsed. Expected JSON object with 'op' and 'd'.".to_string(),
                                }).await;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) => break,
                    Some(Err(e)) => {
                        warn!(uid = %uid, "Websocket Error receiving: {}", e);
                        break;
                    }
                    None => break,
                    _ => {
                        last_heartbeat = Instant::now();
                    } 
                }
            }
        }
    }

    info!(uid = %uid, "Websocket Client disconnected");
    write_task.abort();
}

pub async fn system_broadcast_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BroadcastReq>,
) -> StatusCode {
    state.ws_manager.broadcast_announcement(&payload.message, &payload.severity);
    StatusCode::OK
}
