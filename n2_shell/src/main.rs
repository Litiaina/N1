use clap::{Args, Parser, Subcommand};
use n2_native_client::{N2NativeClient, NativeClientError, NativeClientResult};
use serde_json::{Value, json};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

const ONE_SHOT_LIMIT: u64 = 16 * 1024 * 1024;
const DEFAULT_PART_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Parser)]
#[command(name = "litiaina-n2-shell", version)]
#[command(about = "Native N2 operator shell")]
#[command(
    long_about = "Native N2 operator shell for cluster health, namespace inspection, object repair, upload/download, and repair/rebalance control. Direct commands run once. The shell command opens a persistent n2:// session."
)]
#[command(arg_required_else_help = false, next_line_help = true)]
struct Cli {
    #[arg(
        long,
        global = true,
        default_value = "n2://127.0.0.1:50002",
        help = "Native N2 endpoint, for example n2://127.0.0.1:50002."
    )]
    native_url: String,

    #[arg(
        short,
        long,
        global = true,
        help = "Fragment name or hash used for authentication and fragment-scoped operations."
    )]
    fragment: Option<String>,

    #[arg(short, long, global = true, help = "Fragment secret.")]
    secret: Option<String>,

    #[arg(long, global = true, default_value_t = false, help = "Print compact JSON.")]
    json: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Ping the native N2 endpoint.")]
    Ping,
    #[command(about = "Open a persistent operator shell.")]
    Shell,
    #[command(about = "Authenticate and print the native session.")]
    Auth(AuthArgs),
    #[command(about = "Print the current native session.")]
    Session,
    #[command(about = "Run a raw native operation.")]
    Raw {
        op: String,
        #[arg(long, default_value = "{}")]
        payload: String,
    },
    #[command(about = "Show local and storage-system health.")]
    Health,
    #[command(about = "Show cluster membership.")]
    Membership,
    #[command(about = "Show resolved cluster layout.")]
    Layout,
    #[command(about = "Show cluster usage. This is monitoring-only.")]
    Usage {
        #[arg(long)]
        target_fragment: Option<String>,
    },
    #[command(about = "List a namespace directory.")]
    List(ListArgs),
    #[command(about = "Stat a namespace path.")]
    Stat(PathArgs),
    #[command(about = "Create a namespace folder.")]
    Mkdir(PathArgs),
    #[command(about = "Rename or move a namespace path.")]
    Rename(RenameArgs),
    #[command(about = "Move a path into namespace trash.")]
    Trash(PathArgs),
    #[command(about = "Recover a path from namespace trash.")]
    Recover(PathArgs),
    #[command(about = "Purge a path from namespace trash.")]
    Purge(PathArgs),
    #[command(about = "Empty namespace trash for the active fragment.")]
    EmptyTrash,
    #[command(about = "Object metadata, versions, status, repair, and download.")]
    Object {
        #[command(subcommand)]
        command: ObjectCommand,
    },
    #[command(about = "Upload a file. Uses multipart automatically for large files.")]
    Upload(UploadArgs),
    #[command(about = "Download an object through native streaming.")]
    Download(DownloadArgs),
    #[command(about = "Inspect multipart upload sessions.")]
    Uploads {
        #[command(subcommand)]
        command: UploadsCommand,
    },
    #[command(about = "Cluster repair controls.")]
    Repair(JobArgs),
    #[command(about = "Cluster rebalance controls.")]
    Rebalance(JobArgs),
}

#[derive(Args)]
struct AuthArgs {
    #[arg(long)]
    fragment: String,
    #[arg(long)]
    secret: String,
}

#[derive(Args)]
struct ListArgs {
    #[arg(long, default_value = "")]
    path: String,
    #[arg(long, default_value_t = 200)]
    limit: usize,
    #[arg(long, default_value_t = false)]
    trash: bool,
}

#[derive(Args)]
struct PathArgs {
    #[arg(long)]
    path: String,
    #[arg(long, default_value_t = false)]
    trash: bool,
}

#[derive(Args)]
struct RenameArgs {
    #[arg(long)]
    source_path: String,
    #[arg(long)]
    destination_path: String,
}

#[derive(Subcommand)]
enum ObjectCommand {
    Meta {
        #[arg(long)]
        object_key: String,
        #[arg(long)]
        version_id: Option<String>,
    },
    Versions {
        #[arg(long)]
        object_key: String,
    },
    Status {
        #[arg(long)]
        object_key: String,
        #[arg(long, default_value = "availability")]
        mode: String,
    },
    Repair {
        #[arg(long)]
        object_key: String,
        #[arg(long, default_value = "strict")]
        mode: String,
    },
}

#[derive(Args)]
struct UploadArgs {
    #[arg(long)]
    object_key: String,
    #[arg(long)]
    file: PathBuf,
    #[arg(long)]
    content_type: Option<String>,
    #[arg(long, default_value_t = DEFAULT_PART_SIZE)]
    part_size: u64,
}

#[derive(Args)]
struct DownloadArgs {
    #[arg(long)]
    object_key: String,
    #[arg(long)]
    file: PathBuf,
    #[arg(long)]
    version_id: Option<String>,
}

#[derive(Subcommand)]
enum UploadsCommand {
    List {
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    Status {
        #[arg(long)]
        version_id: String,
    },
    Cancel {
        #[arg(long)]
        version_id: String,
        #[arg(long)]
        object_key: Option<String>,
    },
}

#[derive(Args)]
struct JobArgs {
    #[command(subcommand)]
    action: JobAction,
}

#[derive(Subcommand)]
enum JobAction {
    Plan,
    Start {
        #[arg(long)]
        target_fragment: Option<String>,
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    Status,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("n2 shell error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> NativeClientResult<()> {
    let mut cli = Cli::parse();
    let command = cli.command.take().unwrap_or(Command::Shell);
    let mut client = N2NativeClient::connect(&cli.native_url)
        .await?
        .with_request_prefix("n2-shell");

    match command {
        Command::Ping => print_value(client.raw_json("ping", &json!({}), None).await?, cli.json),
        Command::Shell => {
            authenticate_from_cli(&mut client, &cli).await?;
            interactive_shell(&mut client, &cli).await
        }
        Command::Auth(auth) => {
            client.auth_fragment(&auth.fragment, &auth.secret).await?;
            print_value(client.raw_json("session.info", &json!({}), None).await?, cli.json)
        }
        Command::Session => print_value(client.raw_json("session.info", &json!({}), None).await?, cli.json),
        Command::Raw { op, payload } => {
            authenticate_if_configured(&mut client, &cli).await?;
            let payload = parse_json(&payload)?;
            print_response(client.operation(&op, &payload, None).await?, cli.json)
        }
        command => {
            authenticate_from_cli(&mut client, &cli).await?;
            run_authenticated_command(&mut client, &cli, command).await
        }
    }
}

async fn run_authenticated_command(
    client: &mut N2NativeClient,
    cli: &Cli,
    command: Command,
) -> NativeClientResult<()> {
    match command {
        Command::Health => print_op(client, "maintenance.health", json!({}), cli.json).await,
        Command::Membership => print_op(client, "cluster.membership", json!({}), cli.json).await,
        Command::Layout => print_op(client, "cluster.layout", json!({}), cli.json).await,
        Command::Usage { target_fragment } => {
            print_op(client, "cluster.usage", json!({ "fragment": target_fragment }), cli.json).await
        }
        Command::List(args) => {
            print_op(
                client,
                "namespace.list",
                json!({ "path": args.path, "limit": args.limit, "trash": args.trash }),
                cli.json,
            )
            .await
        }
        Command::Stat(args) => {
            print_op(
                client,
                "namespace.stat",
                json!({ "path": args.path, "trash": args.trash }),
                cli.json,
            )
            .await
        }
        Command::Mkdir(args) => {
            print_op(client, "namespace.mkdir", json!({ "path": args.path }), cli.json).await
        }
        Command::Rename(args) => {
            print_op(
                client,
                "namespace.rename",
                json!({ "source_path": args.source_path, "destination_path": args.destination_path }),
                cli.json,
            )
            .await
        }
        Command::Trash(args) => {
            print_op(client, "namespace.trash", json!({ "path": args.path }), cli.json).await
        }
        Command::Recover(args) => {
            print_op(client, "namespace.recover", json!({ "path": args.path }), cli.json).await
        }
        Command::Purge(args) => {
            print_op(client, "namespace.purge", json!({ "path": args.path }), cli.json).await
        }
        Command::EmptyTrash => print_op(client, "trash.empty", json!({}), cli.json).await,
        Command::Object { command } => run_object_command(client, command, cli.json).await,
        Command::Upload(args) => upload_file(client, args, cli.json).await,
        Command::Download(args) => download_file(client, args).await,
        Command::Uploads { command } => run_uploads_command(client, command, cli.json).await,
        Command::Repair(args) => run_job_command(client, "cluster.repair", args, &cli.fragment, cli.json).await,
        Command::Rebalance(args) => {
            run_job_command(client, "cluster.rebalance", args, &cli.fragment, cli.json).await
        }
        Command::Ping | Command::Shell | Command::Auth(_) | Command::Session | Command::Raw { .. } => Ok(()),
    }
}

async fn run_object_command(
    client: &mut N2NativeClient,
    command: ObjectCommand,
    json_output: bool,
) -> NativeClientResult<()> {
    match command {
        ObjectCommand::Meta {
            object_key,
            version_id,
        } => {
            print_op(
                client,
                "object.meta",
                json!({ "object_key": object_key, "version_id": version_id }),
                json_output,
            )
            .await
        }
        ObjectCommand::Versions { object_key } => {
            print_op(client, "object.versions", json!({ "object_key": object_key }), json_output)
                .await
        }
        ObjectCommand::Status { object_key, mode } => {
            print_op(
                client,
                "object.status",
                json!({ "object_key": object_key, "mode": mode }),
                json_output,
            )
            .await
        }
        ObjectCommand::Repair { object_key, mode } => {
            print_op(
                client,
                "object.repair",
                json!({ "object_key": object_key, "mode": mode }),
                json_output,
            )
            .await
        }
    }
}

async fn run_uploads_command(
    client: &mut N2NativeClient,
    command: UploadsCommand,
    json_output: bool,
) -> NativeClientResult<()> {
    match command {
        UploadsCommand::List { limit } => {
            print_op(client, "uploads.list", json!({ "limit": limit }), json_output).await
        }
        UploadsCommand::Status { version_id } => {
            print_op(client, "uploads.status", json!({ "version_id": version_id }), json_output).await
        }
        UploadsCommand::Cancel {
            version_id,
            object_key,
        } => {
            print_op(
                client,
                "multipart.cancel",
                json!({ "version_id": version_id, "object_key": object_key }),
                json_output,
            )
            .await
        }
    }
}

async fn run_job_command(
    client: &mut N2NativeClient,
    prefix: &str,
    args: JobArgs,
    auth_fragment: &Option<String>,
    json_output: bool,
) -> NativeClientResult<()> {
    match args.action {
        JobAction::Plan => print_op(client, &format!("{prefix}.plan"), json!({}), json_output).await,
        JobAction::Status => print_op(client, &format!("{prefix}.status"), json!({}), json_output).await,
        JobAction::Start {
            target_fragment,
            prefix: object_prefix,
            limit,
            dry_run,
        } => {
            let fragment = target_fragment
                .or_else(|| auth_fragment.clone())
                .ok_or_else(|| NativeClientError::protocol("job start requires --fragment or --target-fragment"))?;
            print_op(
                client,
                &format!("{prefix}.start"),
                json!({
                    "fragment": fragment,
                    "prefix": object_prefix,
                    "limit": limit,
                    "dry_run": dry_run,
                }),
                json_output,
            )
            .await
        }
    }
}

async fn upload_file(
    client: &mut N2NativeClient,
    args: UploadArgs,
    json_output: bool,
) -> NativeClientResult<()> {
    let mut file = File::open(&args.file).await?;
    let size = file.metadata().await?.len();
    let content_type = args
        .content_type
        .unwrap_or_else(|| content_type_from_path(&args.file));

    if size <= ONE_SHOT_LIMIT {
        let response = client
            .operation_reader(
                "upload.one_shot",
                &json!({
                    "object_key": args.object_key,
                    "content_type": content_type,
                }),
                &mut file,
                size,
            )
            .await?;
        return print_response(response, json_output);
    }

    let part_size = args.part_size.max(1024 * 1024);
    let expected_parts = size.div_ceil(part_size);
    let init: Value = client
        .operation_json(
            "multipart.init",
            &json!({
                "object_key": args.object_key,
                "content_type": content_type,
                "total_size_bytes": size,
                "expected_parts": expected_parts,
                "part_size_bytes": part_size,
            }),
            None,
        )
        .await?;
    let version_id = required_response_string(&init, "version_id")?;
    let session_id = required_response_string(&init, "session_id")?;

    for part_index in 0..expected_parts {
        let start = part_index.saturating_mul(part_size);
        let part_len = part_size.min(size.saturating_sub(start));
        file.seek(std::io::SeekFrom::Start(start)).await?;
        let mut part_reader = (&mut file).take(part_len);
        client
            .operation_reader(
                "multipart.put_part",
                &json!({
                    "version_id": version_id,
                    "part_index": part_index,
                }),
                &mut part_reader,
                part_len,
            )
            .await?;
        eprintln!(
            "uploaded part {}/{} ({})",
            part_index + 1,
            expected_parts,
            human_bytes(start.saturating_add(part_len))
        );
    }

    let finalized = client
        .operation(
            "multipart.finalize",
            &json!({
                "object_key": args.object_key,
                "content_type": content_type,
                "total_size_bytes": size,
                "expected_parts": expected_parts,
                "part_size_bytes": part_size,
                "version_id": version_id,
                "session_id": session_id,
            }),
            None,
        )
        .await?;
    print_response(finalized, json_output)
}

async fn download_file(client: &mut N2NativeClient, args: DownloadArgs) -> NativeClientResult<()> {
    let mut output = std::fs::File::create(&args.file)?;
    let payload = json!({
        "object_key": args.object_key,
        "version_id": args.version_id,
    });
    let (response, bytes) = client
        .stream_response("object.download_stream", &payload, |chunk| {
            output.write_all(chunk)?;
            Ok(())
        })
        .await?;
    if !response.ok {
        let (code, message) = response
            .error
            .map(|err| (err.code, err.message))
            .unwrap_or_else(|| ("native_error".to_string(), "unknown native error".to_string()));
        return Err(NativeClientError::Native { code, message });
    }
    eprintln!("downloaded {} to {}", human_bytes(bytes), args.file.display());
    Ok(())
}

async fn interactive_shell(client: &mut N2NativeClient, cli: &Cli) -> NativeClientResult<()> {
    println!("N2 shell connected over n2://. Type 'help' for commands, 'exit' to quit.");
    let mut line = String::new();
    loop {
        print!("n2> ");
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
            print_shell_help();
            continue;
        }
        if let Err(err) = run_shell_line(client, cli, input).await {
            eprintln!("error: {err}");
        }
    }
    Ok(())
}

async fn run_shell_line(
    client: &mut N2NativeClient,
    cli: &Cli,
    input: &str,
) -> NativeClientResult<()> {
    let parts = input.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        ["ping"] => print_value(client.raw_json("ping", &json!({}), None).await?, cli.json),
        ["session"] => print_value(client.raw_json("session.info", &json!({}), None).await?, cli.json),
        ["health"] => print_op(client, "maintenance.health", json!({}), cli.json).await,
        ["cluster"] | ["cluster", "health"] => {
            print_op(client, "cluster.health", json!({}), cli.json).await
        }
        ["membership"] => print_op(client, "cluster.membership", json!({}), cli.json).await,
        ["layout"] => print_op(client, "cluster.layout", json!({}), cli.json).await,
        ["usage"] => print_op(client, "cluster.usage", json!({}), cli.json).await,
        ["list"] => print_op(client, "namespace.list", json!({"path": "", "limit": 200}), cli.json).await,
        ["list", path] => {
            print_op(client, "namespace.list", json!({"path": path, "limit": 200}), cli.json).await
        }
        ["trash-list"] => {
            print_op(client, "namespace.list", json!({"path": "", "limit": 200, "trash": true}), cli.json).await
        }
        ["stat", path] => print_op(client, "namespace.stat", json!({"path": path}), cli.json).await,
        ["mkdir", path] => print_op(client, "namespace.mkdir", json!({"path": path}), cli.json).await,
        ["rename", source, destination] | ["move", source, destination] => {
            print_op(
                client,
                "namespace.rename",
                json!({"source_path": source, "destination_path": destination}),
                cli.json,
            )
            .await
        }
        ["trash", path] => print_op(client, "namespace.trash", json!({"path": path}), cli.json).await,
        ["recover", path] => {
            print_op(client, "namespace.recover", json!({"path": path}), cli.json).await
        }
        ["purge", path] => print_op(client, "namespace.purge", json!({"path": path}), cli.json).await,
        ["empty-trash"] => print_op(client, "trash.empty", json!({}), cli.json).await,
        ["uploads"] => print_op(client, "uploads.list", json!({"limit": 50}), cli.json).await,
        ["upload-status", version_id] => {
            print_op(client, "uploads.status", json!({"version_id": version_id}), cli.json).await
        }
        ["object", "meta", object_key] => {
            print_op(client, "object.meta", json!({"object_key": object_key}), cli.json).await
        }
        ["object", "versions", object_key] => {
            print_op(client, "object.versions", json!({"object_key": object_key}), cli.json).await
        }
        ["object", "status", object_key] => {
            print_op(client, "object.status", json!({"object_key": object_key}), cli.json).await
        }
        ["object", "repair", object_key] => {
            print_op(client, "object.repair", json!({"object_key": object_key, "mode": "strict"}), cli.json).await
        }
        ["repair"] | ["repair", "status"] => {
            print_op(client, "cluster.repair.status", json!({}), cli.json).await
        }
        ["repair", "plan"] => print_op(client, "cluster.repair.plan", json!({}), cli.json).await,
        ["repair", "start"] => {
            let fragment = cli.fragment.clone().ok_or_else(|| {
                NativeClientError::protocol("repair start requires global --fragment")
            })?;
            print_op(client, "cluster.repair.start", json!({"fragment": fragment}), cli.json).await
        }
        ["rebalance"] | ["rebalance", "status"] => {
            print_op(client, "cluster.rebalance.status", json!({}), cli.json).await
        }
        ["rebalance", "plan"] => {
            print_op(client, "cluster.rebalance.plan", json!({}), cli.json).await
        }
        ["rebalance", "start"] => {
            let fragment = cli.fragment.clone().ok_or_else(|| {
                NativeClientError::protocol("rebalance start requires global --fragment")
            })?;
            print_op(client, "cluster.rebalance.start", json!({"fragment": fragment}), cli.json).await
        }
        _ => {
            println!("unknown command. Type 'help'.");
            Ok(())
        }
    }
}

fn print_shell_help() {
    println!(
        "commands:\n  ping\n  session\n  health\n  cluster [health]\n  membership\n  layout\n  usage\n  list [path]\n  trash-list\n  stat <path>\n  mkdir <path>\n  rename <source> <destination>\n  trash <path>\n  recover <path>\n  purge <path>\n  empty-trash\n  uploads\n  upload-status <version_id>\n  object meta <object_key>\n  object versions <object_key>\n  object status <object_key>\n  object repair <object_key>\n  repair status|plan|start\n  rebalance status|plan|start\n  exit"
    );
}

async fn authenticate_from_cli(client: &mut N2NativeClient, cli: &Cli) -> NativeClientResult<()> {
    let fragment = cli.fragment.as_deref().ok_or_else(|| {
        NativeClientError::protocol("pass --fragment <name-or-hash> for authenticated commands")
    })?;
    let secret = cli.secret.as_deref().ok_or_else(|| {
        NativeClientError::protocol("pass --secret <fragment-secret> for authenticated commands")
    })?;
    client.auth_fragment(fragment, secret).await
}

async fn authenticate_if_configured(
    client: &mut N2NativeClient,
    cli: &Cli,
) -> NativeClientResult<()> {
    match (cli.fragment.as_deref(), cli.secret.as_deref()) {
        (Some(fragment), Some(secret)) => client.auth_fragment(fragment, secret).await,
        (None, None) => Ok(()),
        _ => Err(NativeClientError::protocol(
            "--fragment and --secret must be provided together",
        )),
    }
}

async fn print_op(
    client: &mut N2NativeClient,
    op: &str,
    payload: Value,
    json_output: bool,
) -> NativeClientResult<()> {
    let response = client.operation(op, &payload, None).await?;
    print_response(response, json_output)
}

fn print_response(
    response: n2_native_client::NativeOperationResponse,
    json_output: bool,
) -> NativeClientResult<()> {
    if response.body.is_empty() {
        println!("null");
        return Ok(());
    }
    if let Ok(value) = rmp_serde::from_slice::<Value>(&response.body) {
        return print_value(value, json_output);
    }
    if let Ok(value) = serde_json::from_slice::<Value>(&response.body) {
        return print_value(value, json_output);
    }
    println!("{}", String::from_utf8_lossy(&response.body));
    Ok(())
}

fn print_value(value: Value, json_output: bool) -> NativeClientResult<()> {
    if json_output {
        println!("{}", serde_json::to_string(&value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    Ok(())
}

fn parse_json(raw: &str) -> NativeClientResult<Value> {
    serde_json::from_str(raw).map_err(Into::into)
}

fn required_response_string(value: &Value, field: &str) -> NativeClientResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| NativeClientError::protocol(format!("native response missing {field}")))
}

fn content_type_from_path(path: &Path) -> String {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("html") => "text/html",
        Some("txt") => "text/plain",
        Some("json") => "application/json",
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("mp4") => "video/mp4",
        Some("webm") => "video/webm",
        Some("zip") => "application/zip",
        _ => "application/octet-stream",
    }
    .to_string()
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}
