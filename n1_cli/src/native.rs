use std::io::{self, Write};
use std::time::Duration;

use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use reqwest::{Method, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_bytes::ByteBuf;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use cli_core::client::{CreateFragmentRequest, DynError, get_secret, print_output};
use cli_core::models::{
    GcJobStatus, HealthResponse, ListUploadsResponse, RedistributionJobStatus, RepairJobStatus,
    UploadStatusResponse,
};

use crate::args::{
    Cli, Commands, FragmentCommands, JobAction, NamespaceCommands, ObjectCommands,
    RedistributeAction, RepairAction, UploadCommands,
};
use crate::client::{
    FragmentHashLookupRequest, FragmentHashLookupResponse, ResizeFragmentQuotaRequest,
    build_redistribution_start_query, build_repair_start_query, looks_like_fragment_hash,
    resolve_cli_auth_fragment_hint,
};
use crate::output::{
    format_gc_status, format_health, format_health_watch_line, format_redistribution_status,
    format_repair_status, format_upload_list, format_upload_status,
};

const MAX_NATIVE_FRAME_BYTES: usize = 128 * 1024 * 1024;

#[derive(Serialize)]
struct NativeRequest {
    id: String,
    op: String,
    payload: Option<String>,
    body: Option<ByteBuf>,
    body_len: Option<u64>,
}

#[derive(serde::Deserialize)]
struct NativeResponse {
    id: String,
    ok: bool,
    status: Option<u16>,
    payload: Option<String>,
    body: Option<ByteBuf>,
    body_len: Option<u64>,
    error: Option<NativeError>,
}

#[derive(serde::Deserialize)]
struct NativeError {
    code: String,
    message: String,
}

#[derive(Serialize)]
struct NativeAuthFragmentRequest {
    fragment: String,
    secret: String,
    read: bool,
    write: bool,
    modify: bool,
    delete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_in: Option<u64>,
}

struct NativeN1Client {
    stream: TcpStream,
    next_id: u64,
}

struct NativeOpResult {
    status: StatusCode,
    body: Vec<u8>,
}

pub async fn run_native(cli: Cli) -> Result<(), DynError> {
    let native_url = resolve_native_url(&cli)?;
    let secret = get_secret(cli.secret.clone())?;
    let mut client = NativeN1Client::connect(&native_url).await?;

    match &cli.command {
        Commands::Console => {
            authenticate_native_cli(&mut client, &cli, &secret).await?;
            native_console(&mut client, &cli).await
        }
        Commands::Fragment {
            command: FragmentCommands::Create(args),
        } => {
            let response = client
                .mapped_request(
                    Method::POST,
                    "/n1/fragments",
                    Some(serde_json::to_value(CreateFragmentRequest {
                        name: args.name.clone(),
                        key: args.key.clone(),
                        secret,
                        quota_bytes: Some(args.quota_bytes),
                    })?),
                )
                .await?;
            print_native_response(response)
        }
        _ => {
            authenticate_native_cli(&mut client, &cli, &secret).await?;
            handle_native_authenticated_command(&mut client, &cli).await
        }
    }
}

fn resolve_native_url(cli: &Cli) -> Result<String, DynError> {
    if let Some(url) = cli
        .native_url
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        return Ok(url.to_string());
    }
    if let Ok(url) = std::env::var("N1_NATIVE_URL")
        && !url.trim().is_empty()
    {
        return Ok(url.trim().to_string());
    }
    if let Some(url) =
        native_url_from_config("n1_config.ini").or_else(|| native_url_from_config("config.ini"))
    {
        return Ok(url);
    }
    Ok("n1://127.0.0.1:50002".to_string())
}

fn native_url_from_config(path: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut in_server = false;
    let mut host = "127.0.0.1".to_string();
    let mut port = "50002".to_string();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_server = line == "[server]";
            continue;
        }
        if !in_server {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key.trim() {
            "host" => {
                let value = value.trim();
                host = if value == "0.0.0.0" {
                    "127.0.0.1"
                } else {
                    value
                }
                .to_string();
            }
            "native_port" => port = value.trim().to_string(),
            _ => {}
        }
    }
    Some(format!("n1://{host}:{port}"))
}

async fn authenticate_native_cli(
    client: &mut NativeN1Client,
    cli: &Cli,
    secret: &str,
) -> Result<(), DynError> {
    let auth_hint = resolve_cli_auth_fragment_hint(cli)?;
    let auth_fragment = resolve_n1_auth_fragment_native(client, &auth_hint).await?;
    client.auth_fragment(&auth_fragment, secret).await
}

async fn resolve_n1_auth_fragment_native(
    client: &mut NativeN1Client,
    fragment_hint: &str,
) -> Result<String, DynError> {
    if looks_like_fragment_hash(fragment_hint) {
        return Ok(fragment_hint.to_string());
    }
    let response: FragmentHashLookupResponse = client
        .mapped_json(
            Method::GET,
            "/n1/fragments/hash",
            Some(serde_json::to_value(FragmentHashLookupRequest {
                fragment: fragment_hint.to_string(),
            })?),
        )
        .await?;
    Ok(response.fragment_hash)
}

async fn handle_native_authenticated_command(
    client: &mut NativeN1Client,
    cli: &Cli,
) -> Result<(), DynError> {
    match &cli.command {
        Commands::Console => native_console(client, cli).await,
        Commands::Fragment { command } => {
            handle_native_fragment_command(client, cli, command).await
        }
        Commands::Health { watch, deep_scan } => {
            let path = format!("/n1/maintenance/health?deep_scan={deep_scan}");
            if *watch {
                native_watch_json::<HealthResponse, _, _>(
                    client,
                    &path,
                    cli.poll_ms,
                    cli.json,
                    format_health_watch_line,
                    |_| false,
                )
                .await
            } else {
                let health: HealthResponse = client.mapped_json(Method::GET, &path, None).await?;
                print_output(cli.json, &health, &format_health(&health))
            }
        }
        Commands::Namespace { command } => match command {
            NamespaceCommands::List { path, limit } => {
                let value: Value = client
                    .mapped_json(
                        Method::GET,
                        &format!(
                            "/n1/posix/list?path={}&limit={}",
                            encode_query(path),
                            (*limit).clamp(1, 1000)
                        ),
                        None,
                    )
                    .await?;
                print_json_value(cli.json, &value)
            }
            NamespaceCommands::Stat { path } => {
                let value: Value = client
                    .mapped_json(
                        Method::GET,
                        &format!("/n1/posix/stat?path={}", encode_query(path)),
                        None,
                    )
                    .await?;
                print_json_value(cli.json, &value)
            }
        },
        Commands::Object { command } => match command {
            ObjectCommands::Meta {
                object_key,
                version_id,
            } => {
                let mut payload = json!({"object_key": object_key});
                if let Some(version_id) = version_id {
                    payload["version_id"] = Value::String(version_id.clone());
                }
                let value: Value = client.operation_json("object.meta", payload, None).await?;
                print_json_value(cli.json, &value)
            }
            ObjectCommands::Versions { object_key } => {
                let value: Value = client
                    .operation_json("object.versions", json!({"object_key": object_key}), None)
                    .await?;
                print_json_value(cli.json, &value)
            }
            ObjectCommands::Status { object_key, mode } => {
                let value: Value = client
                    .mapped_json(
                        Method::GET,
                        &format!(
                            "/n1/objects/status/{}?mode={}",
                            encode_path(object_key),
                            encode_query(mode)
                        ),
                        None,
                    )
                    .await?;
                print_json_value(cli.json, &value)
            }
            ObjectCommands::Repair { object_key, mode } => {
                let value: Value = client
                    .mapped_json(
                        Method::POST,
                        &format!(
                            "/n1/objects/repair/{}?mode={}",
                            encode_path(object_key),
                            encode_query(mode)
                        ),
                        None,
                    )
                    .await?;
                print_json_value(cli.json, &value)
            }
        },
        Commands::Uploads { command } => match command {
            UploadCommands::List { limit, offset } => {
                let uploads: ListUploadsResponse = client
                    .mapped_json(
                        Method::GET,
                        &format!("/n1/upload/multipart?limit={limit}&offset={offset}"),
                        None,
                    )
                    .await?;
                print_output(cli.json, &uploads, &format_upload_list(&uploads))
            }
            UploadCommands::Status { version_id } => {
                let status: UploadStatusResponse = client
                    .mapped_json(
                        Method::GET,
                        &format!("/n1/upload/multipart/{}", encode_path(version_id)),
                        None,
                    )
                    .await?;
                print_output(cli.json, &status, &format_upload_status(&status))
            }
        },
        Commands::Gc { action } => match action {
            JobAction::Start => {
                let response = client
                    .mapped_request(Method::POST, "/n1/maintenance/gc/start", None)
                    .await?;
                print_native_response(response)?;
                native_watch_json::<GcJobStatus, _, _>(
                    client,
                    "/n1/maintenance/gc/status",
                    cli.poll_ms,
                    cli.json,
                    format_gc_status,
                    |status| !status.is_running,
                )
                .await
            }
            JobAction::Status => {
                let status: GcJobStatus = client
                    .mapped_json(Method::GET, "/n1/maintenance/gc/status", None)
                    .await?;
                print_output(cli.json, &status, &format_gc_status(&status))
            }
            JobAction::Stop => print_native_response(
                client
                    .mapped_request(Method::POST, "/n1/maintenance/gc/stop", None)
                    .await?,
            ),
            JobAction::Watch => {
                native_watch_json::<GcJobStatus, _, _>(
                    client,
                    "/n1/maintenance/gc/status",
                    cli.poll_ms,
                    cli.json,
                    format_gc_status,
                    |_| false,
                )
                .await
            }
        },
        Commands::Repair { action } => match action {
            RepairAction::Start(args) => {
                let query = build_repair_start_query(
                    args.dry_run,
                    cli.fragment.as_deref(),
                    args.mode,
                    args.fast_mode,
                    args.read_only_fast,
                    args.meta_concurrency,
                    args.repair_concurrency,
                    args.batch,
                );
                let path = path_with_query("/n1/maintenance/repair/start", &query);
                print_native_response(client.mapped_request(Method::POST, &path, None).await?)?;
                native_watch_json::<RepairJobStatus, _, _>(
                    client,
                    "/n1/maintenance/repair/status",
                    cli.poll_ms,
                    cli.json,
                    format_repair_status,
                    |status| !status.is_running,
                )
                .await
            }
            RepairAction::Status => {
                let status: RepairJobStatus = client
                    .mapped_json(Method::GET, "/n1/maintenance/repair/status", None)
                    .await?;
                print_output(cli.json, &status, &format_repair_status(&status))
            }
            RepairAction::Stop => print_native_response(
                client
                    .mapped_request(Method::POST, "/n1/maintenance/repair/stop", None)
                    .await?,
            ),
            RepairAction::Watch => {
                native_watch_json::<RepairJobStatus, _, _>(
                    client,
                    "/n1/maintenance/repair/status",
                    cli.poll_ms,
                    cli.json,
                    format_repair_status,
                    |_| false,
                )
                .await
            }
        },
        Commands::Redistribute { action } => match action {
            RedistributeAction::Start(args) => {
                let query = build_redistribution_start_query(
                    args.dry_run,
                    cli.fragment.as_deref(),
                    args.mode,
                    args.fast_mode,
                    args.read_only_fast,
                    args.durability,
                    args.scope,
                    args.object_concurrency,
                    args.status_batch,
                );
                let path = path_with_query("/n1/maintenance/redistribution/start", &query);
                print_native_response(client.mapped_request(Method::POST, &path, None).await?)?;
                native_watch_json::<RedistributionJobStatus, _, _>(
                    client,
                    "/n1/maintenance/redistribution/status",
                    cli.poll_ms,
                    cli.json,
                    format_redistribution_status,
                    |status| !status.is_running,
                )
                .await
            }
            RedistributeAction::Status => {
                let status: RedistributionJobStatus = client
                    .mapped_json(Method::GET, "/n1/maintenance/redistribution/status", None)
                    .await?;
                print_output(cli.json, &status, &format_redistribution_status(&status))
            }
            RedistributeAction::Stop => print_native_response(
                client
                    .mapped_request(Method::POST, "/n1/maintenance/redistribution/stop", None)
                    .await?,
            ),
            RedistributeAction::Watch => {
                native_watch_json::<RedistributionJobStatus, _, _>(
                    client,
                    "/n1/maintenance/redistribution/status",
                    cli.poll_ms,
                    cli.json,
                    format_redistribution_status,
                    |_| false,
                )
                .await
            }
        },
    }
}

async fn handle_native_fragment_command(
    client: &mut NativeN1Client,
    cli: &Cli,
    command: &FragmentCommands,
) -> Result<(), DynError> {
    match command {
        FragmentCommands::Create(_) => unreachable!("fragment create is handled before auth"),
        FragmentCommands::List => {
            let value: Value = client
                .mapped_json(Method::GET, "/n1/fragments", None)
                .await?;
            print_json_value(cli.json, &value)
        }
        FragmentCommands::Hash { fragment } => {
            let fragment = fragment
                .as_deref()
                .or(cli.fragment.as_deref())
                .ok_or_else(|| {
                    io::Error::other("pass --fragment or fragment hash --fragment <name>")
                })?;
            let value: Value = client
                .mapped_json(
                    Method::GET,
                    "/n1/fragments/hash",
                    Some(serde_json::to_value(FragmentHashLookupRequest {
                        fragment: fragment.to_string(),
                    })?),
                )
                .await?;
            print_json_value(cli.json, &value)
        }
        FragmentCommands::Quota => {
            let value: Value = client
                .mapped_json(Method::GET, "/n1/capacity/quota", None)
                .await?;
            print_json_value(cli.json, &value)
        }
        FragmentCommands::ResizeQuota(args) => {
            let target = cli
                .fragment
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    io::Error::other("Pass --fragment <name-or-hash> for quota resize.")
                })?;
            let fragment = resolve_n1_auth_fragment_native(client, target).await?;
            let response = client
                .mapped_request(
                    Method::PATCH,
                    &format!("/n1/fragments/{}/quota", encode_path(&fragment)),
                    Some(serde_json::to_value(ResizeFragmentQuotaRequest {
                        quota_bytes: args.quota_bytes,
                        secret: args.master_secret.clone(),
                    })?),
                )
                .await?;
            print_native_response(response)
        }
    }
}

async fn native_console(client: &mut NativeN1Client, cli: &Cli) -> Result<(), DynError> {
    println!("N1 native console connected. Type 'help' for commands, 'exit' to quit.");
    let mut line = String::new();
    loop {
        print!("n1> ");
        io::stdout().flush()?;
        line.clear();
        if io::stdin().read_line(&mut line)? == 0 {
            break;
        }
        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if matches!(input, "exit" | "quit") {
            break;
        }
        if input == "help" {
            print_console_help();
            continue;
        }
        if let Err(err) = handle_console_line(client, cli, input).await {
            eprintln!("error: {err}");
        }
    }
    Ok(())
}

async fn handle_console_line(
    client: &mut NativeN1Client,
    cli: &Cli,
    input: &str,
) -> Result<(), DynError> {
    let parts = input.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        ["health"] => {
            let health: HealthResponse = client
                .mapped_json(Method::GET, "/n1/maintenance/health", None)
                .await?;
            print_output(cli.json, &health, &format_health(&health))
        }
        ["quota"] => {
            let value: Value = client
                .mapped_json(Method::GET, "/n1/capacity/quota", None)
                .await?;
            print_json_value(cli.json, &value)
        }
        ["fragments"] => {
            let value: Value = client
                .mapped_json(Method::GET, "/n1/fragments", None)
                .await?;
            print_json_value(cli.json, &value)
        }
        ["list"] => {
            let value: Value = client
                .mapped_json(Method::GET, "/n1/posix/list?path=&limit=200", None)
                .await?;
            print_json_value(cli.json, &value)
        }
        ["list", path] => {
            let value: Value = client
                .mapped_json(
                    Method::GET,
                    &format!("/n1/posix/list?path={}&limit=200", encode_query(path)),
                    None,
                )
                .await?;
            print_json_value(cli.json, &value)
        }
        ["stat", path] => {
            let value: Value = client
                .mapped_json(
                    Method::GET,
                    &format!("/n1/posix/stat?path={}", encode_query(path)),
                    None,
                )
                .await?;
            print_json_value(cli.json, &value)
        }
        ["uploads"] => {
            let uploads: ListUploadsResponse = client
                .mapped_json(Method::GET, "/n1/upload/multipart?limit=50&offset=0", None)
                .await?;
            print_output(cli.json, &uploads, &format_upload_list(&uploads))
        }
        ["upload", version_id] => {
            let status: UploadStatusResponse = client
                .mapped_json(
                    Method::GET,
                    &format!("/n1/upload/multipart/{}", encode_path(version_id)),
                    None,
                )
                .await?;
            print_output(cli.json, &status, &format_upload_status(&status))
        }
        ["gc"] | ["gc", "status"] => {
            let status: GcJobStatus = client
                .mapped_json(Method::GET, "/n1/maintenance/gc/status", None)
                .await?;
            print_output(cli.json, &status, &format_gc_status(&status))
        }
        ["gc", "start"] => print_native_response(
            client
                .mapped_request(Method::POST, "/n1/maintenance/gc/start", None)
                .await?,
        ),
        ["repair"] | ["repair", "status"] => {
            let status: RepairJobStatus = client
                .mapped_json(Method::GET, "/n1/maintenance/repair/status", None)
                .await?;
            print_output(cli.json, &status, &format_repair_status(&status))
        }
        ["repair", "start"] => print_native_response(
            client
                .mapped_request(Method::POST, "/n1/maintenance/repair/start", None)
                .await?,
        ),
        ["redistribute"] | ["redistribute", "status"] => {
            let status: RedistributionJobStatus = client
                .mapped_json(Method::GET, "/n1/maintenance/redistribution/status", None)
                .await?;
            print_output(cli.json, &status, &format_redistribution_status(&status))
        }
        ["redistribute", "start"] => print_native_response(
            client
                .mapped_request(Method::POST, "/n1/maintenance/redistribution/start", None)
                .await?,
        ),
        ["object", "status", object_key] => {
            let value: Value = client
                .mapped_json(
                    Method::GET,
                    &format!(
                        "/n1/objects/status/{}?mode=availability",
                        encode_path(object_key)
                    ),
                    None,
                )
                .await?;
            print_json_value(cli.json, &value)
        }
        ["object", "meta", object_key] => {
            let value: Value = client
                .operation_json("object.meta", json!({"object_key": object_key}), None)
                .await?;
            print_json_value(cli.json, &value)
        }
        ["object", "versions", object_key] => {
            let value: Value = client
                .operation_json("object.versions", json!({"object_key": object_key}), None)
                .await?;
            print_json_value(cli.json, &value)
        }
        ["object", "repair", object_key] => {
            let value: Value = client
                .mapped_json(
                    Method::POST,
                    &format!("/n1/objects/repair/{}?mode=strict", encode_path(object_key)),
                    None,
                )
                .await?;
            print_json_value(cli.json, &value)
        }
        ["session"] => {
            let value = client.raw_json("session.info", json!({}), None).await?;
            print_json_value(cli.json, &value)
        }
        _ => {
            println!("unknown command. Type 'help'.");
            Ok(())
        }
    }
}

fn print_console_help() {
    println!(
        "commands:\n  health\n  quota\n  fragments\n  list [path]\n  stat <path>\n  uploads\n  upload <version_id>\n  gc status|start\n  repair status|start\n  redistribute status|start\n  object meta <object_key>\n  object versions <object_key>\n  object status <object_key>\n  object repair <object_key>\n  session\n  exit"
    );
}

async fn native_watch_json<T, F, D>(
    client: &mut NativeN1Client,
    path: &str,
    poll_ms: u64,
    json_output: bool,
    format: F,
    done: D,
) -> Result<(), DynError>
where
    T: DeserializeOwned + Serialize,
    F: Fn(&T) -> String,
    D: Fn(&T) -> bool,
{
    loop {
        let value: T = client.mapped_json(Method::GET, path, None).await?;
        print_output(json_output, &value, &format(&value))?;
        if done(&value) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(poll_ms)).await;
    }
}

fn print_json_value(json_output: bool, value: &Value) -> Result<(), DynError> {
    if json_output {
        println!("{}", serde_json::to_string(value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(value)?);
    }
    Ok(())
}

fn print_native_response(response: NativeOpResult) -> Result<(), DynError> {
    if let Ok(value) = rmp_serde::from_slice::<Value>(&response.body) {
        println!("{}", serde_json::to_string(&value)?);
    } else if let Ok(value) = serde_json::from_slice::<Value>(&response.body) {
        println!("{}", serde_json::to_string(&value)?);
    } else if !response.body.is_empty() {
        println!("{}", String::from_utf8_lossy(&response.body));
    } else {
        println!("{{\"status\":{}}}", response.status.as_u16());
    }
    Ok(())
}

fn path_with_query(path: &str, query: &[(String, String)]) -> String {
    if query.is_empty() {
        return path.to_string();
    }
    let query = query
        .iter()
        .map(|(key, value)| format!("{}={}", encode_query(key), encode_query(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{query}")
}

fn encode_query(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn encode_path(value: &str) -> String {
    encode_query(value).replace("%2F", "/")
}

impl NativeN1Client {
    async fn connect(url: &str) -> Result<Self, DynError> {
        let address = parse_native_address(url)?;
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream, next_id: 0 })
    }

    async fn auth_fragment(&mut self, fragment: &str, secret: &str) -> Result<(), DynError> {
        let response = self
            .round_trip(
                "auth.fragment",
                json!(NativeAuthFragmentRequest {
                    fragment: fragment.to_string(),
                    secret: secret.to_string(),
                    read: true,
                    write: true,
                    modify: true,
                    delete: true,
                    expires_in: Some(21_600),
                }),
                None,
            )
            .await?;
        ensure_native_ok(&response)?;
        Ok(())
    }

    async fn mapped_json<T: DeserializeOwned>(
        &mut self,
        method: Method,
        path: &str,
        json_body: Option<Value>,
    ) -> Result<T, DynError> {
        let response = self.mapped_request(method, path, json_body).await?;
        Ok(rmp_serde::from_slice(&response.body)?)
    }

    async fn mapped_request(
        &mut self,
        method: Method,
        path: &str,
        json_body: Option<Value>,
    ) -> Result<NativeOpResult, DynError> {
        let (op, payload) = native_op_from_mapped_call(method, path, json_body)?;
        let response = self.round_trip(&op, payload, None).await?;
        ensure_native_ok(&response)?;
        let status = StatusCode::from_u16(response.status.unwrap_or(500))?;
        let body = response
            .body
            .map(|body| body.into_vec())
            .unwrap_or_default();
        if status.is_client_error() || status.is_server_error() {
            return Err(io::Error::other(format!(
                "native operation failed: status={} body={}",
                status,
                String::from_utf8_lossy(&body)
            ))
            .into());
        }
        Ok(NativeOpResult { status, body })
    }

    async fn raw_json(
        &mut self,
        op: &str,
        payload: Value,
        body: Option<Vec<u8>>,
    ) -> Result<Value, DynError> {
        let response = self.round_trip(op, payload, body).await?;
        ensure_native_ok(&response)?;
        let payload = response.payload.unwrap_or_else(|| "null".to_string());
        Ok(serde_json::from_str(&payload)?)
    }

    async fn operation_json<T: DeserializeOwned>(
        &mut self,
        op: &str,
        payload: Value,
        body: Option<Vec<u8>>,
    ) -> Result<T, DynError> {
        let response = self.round_trip(op, payload, body).await?;
        ensure_native_ok(&response)?;
        let body = response
            .body
            .as_ref()
            .map(|body| body.as_slice())
            .unwrap_or_default();
        Ok(rmp_serde::from_slice(body)?)
    }

    async fn round_trip(
        &mut self,
        op: &str,
        payload: Value,
        body: Option<Vec<u8>>,
    ) -> Result<NativeResponse, DynError> {
        self.next_id = self.next_id.saturating_add(1);
        let id = format!("n1-cli-{}", self.next_id);
        let request = NativeRequest {
            id: id.clone(),
            op: op.to_string(),
            payload: Some(serde_json::to_string(&payload)?),
            body_len: body.as_ref().map(|body| body.len() as u64),
            body: None,
        };
        let bytes = rmp_serde::to_vec(&request)?;
        if bytes.is_empty() || bytes.len() > MAX_NATIVE_FRAME_BYTES {
            return Err(io::Error::other(format!(
                "native request frame size {} is outside the supported bound",
                bytes.len()
            ))
            .into());
        }
        self.stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await?;
        self.stream.write_all(&bytes).await?;
        if let Some(body) = body {
            self.stream.write_all(&body).await?;
        }
        self.stream.flush().await?;

        let mut len = [0u8; 4];
        self.stream.read_exact(&mut len).await?;
        let len = u32::from_be_bytes(len) as usize;
        if len == 0 || len > MAX_NATIVE_FRAME_BYTES {
            return Err(
                io::Error::other(format!("native response frame size {len} is invalid")).into(),
            );
        }
        let mut frame = vec![0u8; len];
        self.stream.read_exact(&mut frame).await?;
        let mut response = rmp_serde::from_slice::<NativeResponse>(&frame)?;
        if response.body.is_none() {
            if let Some(body_len) = response.body_len {
                let mut body = vec![0u8; body_len as usize];
                if !body.is_empty() {
                    self.stream.read_exact(&mut body).await?;
                }
                response.body = Some(ByteBuf::from(body));
            }
        }
        if response.id != id {
            return Err(io::Error::other(format!(
                "native response id mismatch: got {} expected {id}",
                response.id
            ))
            .into());
        }
        Ok(response)
    }
}

fn parse_native_address(url: &str) -> Result<String, DynError> {
    let Some(rest) = url.strip_prefix("n1://") else {
        return Err(io::Error::other("native URL must start with n1://").into());
    };
    let address = rest.trim_end_matches('/');
    if address.is_empty() {
        return Err(io::Error::other("native URL did not include host:port").into());
    }
    Ok(address.to_string())
}

fn ensure_native_ok(response: &NativeResponse) -> Result<(), DynError> {
    if response.ok {
        return Ok(());
    }
    let error = response
        .error
        .as_ref()
        .map(|err| format!("{}: {}", err.code, err.message))
        .unwrap_or_else(|| "unknown native error".to_string());
    Err(io::Error::other(error).into())
}

fn native_op_from_mapped_call(
    method: Method,
    path: &str,
    json_body: Option<Value>,
) -> Result<(String, Value), DynError> {
    let (path, query) = split_mapped_path(path)?;
    let mut payload = query;

    let merge_json = |payload: &mut Value| {
        if let Some(body) = json_body.clone()
            && let (Some(dst), Some(src)) = (payload.as_object_mut(), body.as_object())
        {
            for (key, value) in src {
                dst.insert(key.clone(), value.clone());
            }
        } else if let Some(body) = json_body.clone() {
            *payload = body;
        }
    };

    let method_for_error = method.clone();
    let op = match (method, path.as_str()) {
        (Method::POST, "/n1/fragments") => {
            merge_json(&mut payload);
            "fragment.create"
        }
        (Method::GET, "/n1/fragments") => "fragment.list",
        (Method::GET, "/n1/fragments/hash") => {
            merge_json(&mut payload);
            "fragment.hash.public"
        }
        (Method::GET, "/n1/capacity/quota") => "fragment.quota",
        (Method::GET, "/n1/maintenance/health") => "maintenance.health",
        (Method::GET, "/n1/posix/list") => "namespace.list",
        (Method::GET, "/n1/posix/stat") => "namespace.stat",
        (Method::GET, "/n1/upload/multipart") => "uploads.list",
        (Method::POST, "/n1/maintenance/gc/start") => "maintenance.gc.start",
        (Method::GET, "/n1/maintenance/gc/status") => "maintenance.gc.status",
        (Method::POST, "/n1/maintenance/gc/stop") => "maintenance.gc.stop",
        (Method::POST, "/n1/maintenance/repair/start") => "maintenance.repair.start",
        (Method::GET, "/n1/maintenance/repair/status") => "maintenance.repair.status",
        (Method::POST, "/n1/maintenance/repair/stop") => "maintenance.repair.stop",
        (Method::POST, "/n1/maintenance/redistribution/start") => {
            "maintenance.redistribution.start"
        }
        (Method::GET, "/n1/maintenance/redistribution/status") => {
            "maintenance.redistribution.status"
        }
        (Method::POST, "/n1/maintenance/redistribution/stop") => "maintenance.redistribution.stop",
        (Method::PATCH, path) if path.starts_with("/n1/fragments/") && path.ends_with("/quota") => {
            let fragment = path
                .trim_start_matches("/n1/fragments/")
                .trim_end_matches("/quota");
            payload["fragment"] = Value::String(fragment.to_string());
            merge_json(&mut payload);
            "fragment.resize_quota"
        }
        (Method::GET, path) if path.starts_with("/n1/upload/multipart/") => {
            payload["version_id"] =
                Value::String(path.trim_start_matches("/n1/upload/multipart/").to_string());
            "uploads.status"
        }
        (Method::GET, path) if path.starts_with("/n1/objects/status/") => {
            payload["object_key"] =
                Value::String(path.trim_start_matches("/n1/objects/status/").to_string());
            "object.status"
        }
        (Method::POST, path) if path.starts_with("/n1/objects/repair/") => {
            payload["object_key"] =
                Value::String(path.trim_start_matches("/n1/objects/repair/").to_string());
            "object.repair"
        }
        _ => {
            return Err(io::Error::other(format!(
                "native CLI has no direct n1:// mapping for {} {}",
                method_for_error, path
            ))
            .into());
        }
    };
    Ok((op.to_string(), payload))
}

fn split_mapped_path(path: &str) -> Result<(String, Value), DynError> {
    let normalized = if path.starts_with("/n1/") {
        path.to_string()
    } else {
        let url = Url::parse(path)?;
        let mut out = url.path().to_string();
        if let Some(query) = url.query() {
            out.push('?');
            out.push_str(query);
        }
        out
    };
    let (path, query) = normalized.split_once('?').unwrap_or((&normalized, ""));
    let path = percent_decode_str(path).decode_utf8_lossy().to_string();
    let mut payload = serde_json::Map::new();
    for pair in query.split('&').filter(|item| !item.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode_str(key).decode_utf8_lossy().to_string();
        let value = percent_decode_str(value).decode_utf8_lossy().to_string();
        payload.insert(key, Value::String(value));
    }
    Ok((path, Value::Object(payload)))
}
