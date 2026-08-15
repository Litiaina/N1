use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Json, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::{SinkExt, StreamExt};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};

use crate::ServeArgs;
use crate::install::{CommandOutcome, InstallSettings, LinuxHost, OutputKind};
use crate::provision::{
    AgentCommand, AgentController, AgentIdentity, AgentResponse, PairRequest, PairResponse,
    ProvisionShell,
};

const INDEX_HTML: &str = include_str!("../web/index.html");
const SHELL_CSS: &str = include_str!("../web/shell.css");
const SHELL_JS: &str = include_str!("../web/shell.js");

#[derive(Clone)]
struct AppState {
    setup_token: Arc<str>,
    shell: Arc<ProvisionShell>,
    agent: Arc<AgentController>,
    command_lock: Arc<Mutex<()>>,
    agent_lock: Arc<Mutex<()>>,
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
        .or_else(|| std::env::var("N2_SHELL_SETUP_TOKEN").ok());
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
    let host = Arc::new(LinuxHost);
    let agent = Arc::new(
        AgentController::new(settings.clone(), &setup_token, host).map_err(|error| {
            std::io::Error::other(format!("initialize provisioning agent: {error}"))
        })?,
    );
    let shell = Arc::new(
        ProvisionShell::new(settings, args.allow_shared_failure_domain)
            .map_err(|error| std::io::Error::other(format!("initialize commissioner: {error}")))?,
    );
    let state = AppState {
        setup_token: Arc::from(setup_token.clone()),
        shell,
        agent,
        command_lock: Arc::new(Mutex::new(())),
        agent_lock: Arc::new(Mutex::new(())),
        hostname: Arc::from(hostname),
    };

    let router = build_router(state.clone());
    let listener = tokio::net::TcpListener::bind(address).await?;

    println!("N2 Shell installation terminal");
    println!("  address: http://{address}");
    println!("  setup token: {setup_token}");
    println!("  install directory: {}", args.install_dir);
    println!("  N2 remains disabled until you explicitly run 'cluster start'.");
    println!("  agent id: {}", state.agent.identity().agent_id);
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

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/shell.css", get(css))
        .route("/shell.js", get(javascript))
        .route("/ws", get(websocket))
        .route("/agent/v1/identity", get(agent_identity))
        .route("/agent/v1/pair", post(agent_pair))
        .route("/agent/v1/command", post(agent_command))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .fallback(not_found)
        .with_state(state)
}

async fn agent_identity(State(state): State<AppState>) -> Json<AgentIdentity> {
    Json(state.agent.identity())
}

async fn agent_pair(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PairRequest>,
) -> Result<Json<PairResponse>, (StatusCode, String)> {
    let _guard = state.agent_lock.lock().await;
    let token = headers
        .get("x-n2-setup-token")
        .and_then(|value| value.to_str().ok())
        .ok_or((StatusCode::UNAUTHORIZED, "missing agent setup token".into()))?;
    state
        .agent
        .pair(token, request)
        .map(Json)
        .map_err(|error| (StatusCode::CONFLICT, error))
}

async fn agent_command(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(command): Json<AgentCommand>,
) -> Result<Json<AgentResponse>, (StatusCode, String)> {
    let credential = bearer_credential(&headers)
        .ok_or((StatusCode::UNAUTHORIZED, "missing agent credential".into()))?;
    let pair = state
        .agent
        .authenticate(credential)
        .map_err(|error| (StatusCode::UNAUTHORIZED, error))?;
    let guard = state.agent_lock.clone().lock_owned().await;
    let agent = state.agent.clone();
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        agent.execute(&pair, command)
    })
    .await
    .map(Json)
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("agent command task failed: {error}"),
        )
    })
}

fn bearer_credential(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
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
    use crate::install::{BlockDisk, Emit, FormatResult, HostOps, ServiceAction};
    use std::collections::BTreeSet;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex as StdMutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Default)]
    struct FakeHost {
        disks: StdMutex<Vec<BlockDisk>>,
        active: StdMutex<BTreeSet<String>>,
    }

    impl HostOps for FakeHost {
        fn require_privileged(&self) -> Result<(), String> {
            Ok(())
        }

        fn check_prerequisites(&self) -> Result<Vec<String>, String> {
            Ok(vec!["test tools".into()])
        }

        fn list_disks(&self) -> Result<Vec<BlockDisk>, String> {
            Ok(self.disks.lock().unwrap().clone())
        }

        fn format_disk(
            &self,
            _disk: &BlockDisk,
            _label: &str,
            _settings: &InstallSettings,
            _emit: Emit<'_>,
        ) -> Result<FormatResult, String> {
            Err("not used by distributed commissioning test".into())
        }

        fn mount_disk(
            &self,
            _disk: &BlockDisk,
            _label: &str,
            _settings: &InstallSettings,
            _emit: Emit<'_>,
        ) -> Result<FormatResult, String> {
            Err("not used by distributed commissioning test".into())
        }

        fn service_action(&self, service: &str, action: ServiceAction) -> Result<String, String> {
            match action {
                ServiceAction::Start | ServiceAction::Restart => {
                    self.active.lock().unwrap().insert(service.into());
                }
                ServiceAction::Stop => {
                    self.active.lock().unwrap().remove(service);
                }
                ServiceAction::Enable | ServiceAction::Disable => {}
            }
            Ok(String::new())
        }

        fn service_is_active(&self, service: &str) -> bool {
            self.active.lock().unwrap().contains(service)
        }

        fn reload_systemd(&self) -> Result<(), String> {
            Ok(())
        }

        fn identity_exists(&self, _user: &str, _group: &str) -> Result<(), String> {
            Ok(())
        }

        fn apply_ownership(&self, _path: &Path, _user: &str, _group: &str) -> Result<(), String> {
            Ok(())
        }

        fn recent_logs(&self, service: &str, _lines: usize) -> Result<String, String> {
            Ok(format!("logs for {service}"))
        }
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_physical_agents_pair_validate_and_commit_identical_membership() {
        let root = test_root("distributed-http");
        let agent_one_root = root.join("agent-one");
        let agent_two_root = root.join("agent-two");
        let commissioner_root = root.join("commissioner");
        let settings_one = test_settings(&agent_one_root);
        let settings_two = test_settings(&agent_two_root);
        let commissioner_settings = test_settings(&commissioner_root);
        prepare_files(&settings_one);
        prepare_files(&settings_two);
        prepare_files(&commissioner_settings);

        let token_one = "agent-one-token-0123456789";
        let token_two = "agent-two-token-0123456789";
        let (url_one, task_one) = spawn_agent(
            settings_one.clone(),
            token_one,
            "N2DISK001",
            "uuid-one",
            "serial-one",
        )
        .await;
        let (url_two, task_two) = spawn_agent(
            settings_two.clone(),
            token_two,
            "N2DISK002",
            "uuid-two",
            "serial-two",
        )
        .await;

        let certificate = commissioner_root.join("tls/cert.pem");
        let private_key = commissioner_root.join("tls/key.pem");
        fs::create_dir_all(certificate.parent().unwrap()).unwrap();
        fs::write(&certificate, b"certificate").unwrap();
        fs::write(&private_key, b"private-key").unwrap();
        let settings_one_for_failure = settings_one.clone();
        let settings_two_for_failure = settings_two.clone();
        tokio::task::spawn_blocking(move || {
            let shell = ProvisionShell::new(commissioner_settings, true).unwrap();
            assert!(
                shell_command(&shell, "cluster init cluster-test", None)
                    .0
                    .success
            );
            assert!(
                shell_command(
                    &shell,
                    &format!("host pair host-001 {url_one} {token_one}"),
                    None
                )
                .0
                .success
            );
            assert!(
                shell_command(
                    &shell,
                    &format!("host pair host-002 {url_two} {token_two}"),
                    None
                )
                .0
                .success
            );
            assert!(
                shell_command(
                    &shell,
                    "node create node-001 host-001 10.0.0.1 50001 50002",
                    None
                )
                .0
                .success
            );
            assert!(
                shell_command(
                    &shell,
                    "node create node-002 host-002 10.0.0.2 50001 50002",
                    None
                )
                .0
                .success
            );
            assert!(
                shell_command(&shell, "disk assign node-001 N2DISK001", None)
                    .0
                    .success
            );
            assert!(
                shell_command(&shell, "disk assign node-002 N2DISK002", None)
                    .0
                    .success
            );
            assert!(
                shell_command(&shell, "pool create pool-001 1 1 1", None)
                    .0
                    .success
            );
            assert!(
                shell_command(&shell, "pool enroll pool-001 node-001 node-002", None)
                    .0
                    .success
            );
            assert!(
                shell_command(
                    &shell,
                    &format!(
                        "config tls {} {}",
                        certificate.display(),
                        private_key.display()
                    ),
                    None
                )
                .0
                .success
            );
            let (validated, output) = shell_command(&shell, "cluster validate", None);
            assert!(validated.success, "{}", output.join("\n"));
            assert!(output.join("\n").contains("Physical hosts: 2"));

            let (pending, _) = shell_command(&shell, "cluster commit", None);
            let expected = pending.confirmation.unwrap().expected;
            let plan_hash = expected.split_whitespace().last().unwrap().to_string();
            fs::remove_file(&settings_two_for_failure.engine_binary).unwrap();
            let (failed, output) = shell_command(&shell, "cluster commit", Some(&expected));
            assert!(!failed.success);
            assert!(output.join("\n").contains("prepare failed"));
            assert!(
                !settings_one_for_failure
                    .setup_root
                    .join("staged")
                    .join(&plan_hash)
                    .exists(),
                "a successful peer stage must be aborted when another physical node fails prepare"
            );
            fs::write(&settings_two_for_failure.engine_binary, b"test binary").unwrap();
            let mut permissions = fs::metadata(&settings_two_for_failure.engine_binary)
                .unwrap()
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&settings_two_for_failure.engine_binary, permissions).unwrap();
            let (committed, output) = shell_command(&shell, "cluster commit", Some(&expected));
            assert!(committed.success, "{}", output.join("\n"));
            assert!(shell_command(&shell, "cluster evolve 2", None).0.success);
            assert!(
                shell_command(&shell, "cluster discard-draft", None)
                    .0
                    .success
            );
            assert!(shell.current_prompt_state().ends_with(":committed"));
        })
        .await
        .unwrap();

        let membership_one = fs::read(
            settings_one
                .node_dir("node-001")
                .join("state/n2_membership.json"),
        )
        .unwrap();
        let membership_two = fs::read(
            settings_two
                .node_dir("node-002")
                .join("state/n2_membership.json"),
        )
        .unwrap();
        assert_eq!(membership_one, membership_two);
        assert!(settings_one.setup_root.join("committed.sha256").exists());
        assert!(settings_two.setup_root.join("committed.sha256").exists());

        task_one.abort();
        task_two.abort();
        fs::remove_dir_all(root).unwrap();
    }

    async fn spawn_agent(
        settings: InstallSettings,
        token: &str,
        label: &str,
        uuid: &str,
        serial: &str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let mount = settings.mount_path(label);
        fs::create_dir_all(&mount).unwrap();
        let disk = BlockDisk {
            name: label.to_ascii_lowercase(),
            path: format!("/dev/{}", label.to_ascii_lowercase()),
            stable_id: format!("/dev/disk/by-id/{serial}"),
            model: "test disk".into(),
            serial: serial.into(),
            size_bytes: 1024,
            read_only: false,
            removable: false,
            system: false,
            mountpoints: vec![mount.to_string_lossy().into_owned()],
            filesystem: Some("xfs".into()),
            label: Some(label.into()),
            uuid: Some(uuid.into()),
            partition_path: Some(format!("/dev/{}1", label.to_ascii_lowercase())),
        };
        let host = Arc::new(FakeHost {
            disks: StdMutex::new(vec![disk]),
            ..FakeHost::default()
        });
        let agent = Arc::new(AgentController::new(settings.clone(), token, host).unwrap());
        let shell = Arc::new(ProvisionShell::new(settings, true).unwrap());
        let state = AppState {
            setup_token: Arc::from(token.to_string()),
            shell,
            agent,
            command_lock: Arc::new(Mutex::new(())),
            agent_lock: Arc::new(Mutex::new(())),
            hostname: Arc::from("test-node"),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, build_router(state)).await.unwrap();
        });
        (format!("http://{address}"), task)
    }

    fn shell_command(
        shell: &ProvisionShell,
        command: &str,
        confirmation: Option<&str>,
    ) -> (CommandOutcome, Vec<String>) {
        let mut output = Vec::new();
        let result = shell.execute(command, confirmation, &mut |_, text| output.push(text));
        (result, output)
    }

    fn test_root(name: &str) -> PathBuf {
        let base = std::env::var_os("N2_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = base.join(format!("n2-web-{name}-{}-{unique}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn test_settings(root: &Path) -> InstallSettings {
        InstallSettings {
            engine_binary: root.join("bin/litiaina-n2"),
            gateway_binary: root.join("bin/litiaina-n2-gateway"),
            runtime_root: root.join("runtime/n2"),
            gateway_runtime: root.join("runtime/gateway"),
            env_path: root.join("etc/n2.env"),
            setup_root: root.join("setup"),
            mount_root: root.join("mnt"),
            fstab_path: root.join("fstab"),
            service_name: "n2-test".into(),
            gateway_service_name: "n2-gateway-test".into(),
            systemd_unit_dir: root.join("systemd"),
            run_user: "test".into(),
            run_group: "test".into(),
        }
    }

    fn prepare_files(settings: &InstallSettings) {
        fs::create_dir_all(settings.engine_binary.parent().unwrap()).unwrap();
        for binary in [&settings.engine_binary, &settings.gateway_binary] {
            fs::write(binary, b"test binary").unwrap();
            let mut permissions = fs::metadata(binary).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(binary, permissions).unwrap();
        }
    }
}
