use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};

use crate::args::ServeArgs;
use crate::install::{CommandOutcome, InstallSettings, InstallShell, LinuxHost, OutputKind};

const INDEX_HTML: &str = include_str!("../web/index.html");
const SHELL_CSS: &str = include_str!("../web/shell.css");
const SHELL_JS: &str = include_str!("../web/shell.js");

#[derive(Clone)]
struct AppState {
    setup_token: Arc<str>,
    shell: Arc<InstallShell>,
    command_lock: Arc<Mutex<()>>,
    hostname: Arc<str>,
}

#[derive(Debug, Deserialize)]
struct TokenQuery {
    token: String,
}

#[derive(Debug, Deserialize)]
struct ClientCommand {
    id: String,
    line: String,
    confirmation: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Hello {
        hostname: String,
        version: &'static str,
        prompt_state: String,
    },
    Output {
        id: String,
        kind: OutputKind,
        text: String,
    },
    Completed {
        id: String,
        outcome: CommandOutcome,
    },
    ProtocolError {
        message: String,
    },
}

pub async fn run(args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let settings = InstallSettings::from_serve_args(&args)
        .map_err(|error| std::io::Error::other(format!("invalid setup configuration: {error}")))?;
    let address = args.bind.parse::<SocketAddr>().map_err(|error| {
        std::io::Error::other(format!("invalid --bind address '{}': {error}", args.bind))
    })?;
    let configured_token = args
        .setup_token
        .clone()
        .or_else(|| std::env::var("N1_SHELL_SETUP_TOKEN").ok());
    if configured_token
        .as_deref()
        .is_some_and(|token| token.len() < 16)
    {
        return Err(
            std::io::Error::other("setup token must contain at least 16 characters").into(),
        );
    }
    let setup_token = configured_token.unwrap_or_else(generate_setup_token);
    let hostname = hostname::get()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let shell = Arc::new(InstallShell::new(settings, Arc::new(LinuxHost)));
    let state = AppState {
        setup_token: Arc::from(setup_token.clone()),
        shell,
        command_lock: Arc::new(Mutex::new(())),
        hostname: Arc::from(hostname),
    };

    let router = Router::new()
        .route("/", get(index))
        .route("/shell.css", get(css))
        .route("/shell.js", get(javascript))
        .route("/ws", get(websocket))
        .fallback(not_found)
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(address).await?;

    println!("N1 Shell installation terminal");
    println!("  address: http://{address}");
    println!("  setup token: {setup_token}");
    println!("  install directory: {}", args.install_dir);
    println!("  N1 remains disabled until you explicitly run 'n1 start'.");
    if !address.ip().is_loopback() {
        println!(
            "  warning: plain HTTP is intended only for a trusted setup LAN; stop this server after provisioning."
        );
    }

    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn index() -> Response {
    no_store_response(Html(INDEX_HTML).into_response(), "text/html; charset=utf-8")
}

async fn css() -> Response {
    no_store_response(SHELL_CSS.into_response(), "text/css; charset=utf-8")
}

async fn javascript() -> Response {
    no_store_response(SHELL_JS.into_response(), "text/javascript; charset=utf-8")
}

fn no_store_response(mut response: Response, content_type: &'static str) -> Response {
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; connect-src 'self' ws: wss:; script-src 'self'; style-src 'self'; img-src 'none'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    response
}

async fn not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "not found")
}

async fn websocket(
    State(state): State<AppState>,
    Query(query): Query<TokenQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    if !constant_time_equal(query.token.as_bytes(), state.setup_token.as_bytes()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    upgrade.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    if send_message(
        &mut sender,
        &ServerMessage::Hello {
            hostname: state.hostname.to_string(),
            version: env!("CARGO_PKG_VERSION"),
            prompt_state: state.shell.current_prompt_state(),
        },
    )
    .await
    .is_err()
    {
        return;
    }

    while let Some(Ok(message)) = receiver.next().await {
        let Message::Text(text) = message else {
            if matches!(message, Message::Close(_)) {
                break;
            }
            continue;
        };
        let command: ClientCommand = match serde_json::from_str(&text) {
            Ok(command) => command,
            Err(error) => {
                if send_message(
                    &mut sender,
                    &ServerMessage::ProtocolError {
                        message: format!("invalid command message: {error}"),
                    },
                )
                .await
                .is_err()
                {
                    break;
                }
                continue;
            }
        };
        if command.line.contains(['\n', '\r']) || command.line.len() > 8192 {
            if send_message(
                &mut sender,
                &ServerMessage::ProtocolError {
                    message: "commands must be one line and no longer than 8192 bytes".into(),
                },
            )
            .await
            .is_err()
            {
                break;
            }
            continue;
        }

        let command_guard = state.command_lock.clone().lock_owned().await;
        let shell = state.shell.clone();
        let line = command.line.clone();
        let confirmation = command.confirmation.clone();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();
        let mut task = tokio::task::spawn_blocking(move || {
            let _command_guard = command_guard;
            shell.execute(&line, confirmation.as_deref(), &mut |kind, text| {
                let _ = output_tx.send((kind, text));
            })
        });

        let outcome = loop {
            tokio::select! {
                output = output_rx.recv() => {
                    if let Some((kind, text)) = output
                        && send_message(&mut sender, &ServerMessage::Output {
                            id: command.id.clone(),
                            kind,
                            text,
                        }).await.is_err() {
                        return;
                    }
                }
                result = &mut task => {
                    break match result {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            let _ = send_message(&mut sender, &ServerMessage::Output {
                                id: command.id.clone(),
                                kind: OutputKind::Error,
                                text: format!("command task failed: {error}"),
                            }).await;
                            CommandOutcome {
                                success: false,
                                prompt_state: state.shell.current_prompt_state(),
                                ..CommandOutcome::default()
                            }
                        }
                    };
                }
            }
        };
        while let Ok((kind, text)) = output_rx.try_recv() {
            if send_message(
                &mut sender,
                &ServerMessage::Output {
                    id: command.id.clone(),
                    kind,
                    text,
                },
            )
            .await
            .is_err()
            {
                return;
            }
        }
        if send_message(
            &mut sender,
            &ServerMessage::Completed {
                id: command.id,
                outcome,
            },
        )
        .await
        .is_err()
        {
            return;
        }
    }
}

async fn send_message(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    message: &ServerMessage,
) -> Result<(), ()> {
    let encoded = serde_json::to_string(message).map_err(|_| ())?;
    sender
        .send(Message::Text(encoded.into()))
        .await
        .map_err(|_| ())
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn generate_setup_token() -> String {
    let mut bytes = [0_u8; 24];
    OsRng.fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison_requires_exact_bytes() {
        assert!(constant_time_equal(b"same-token", b"same-token"));
        assert!(!constant_time_equal(b"same-token", b"same-tokee"));
        assert!(!constant_time_equal(b"short", b"longer"));
    }

    #[test]
    fn generated_tokens_are_long_and_not_reused() {
        let first = generate_setup_token();
        let second = generate_setup_token();
        assert_eq!(first.len(), 48);
        assert_ne!(first, second);
    }
}
