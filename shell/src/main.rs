mod install;
mod provision;
mod web;

#[path = "main/interactive.rs"]
mod interactive;
#[path = "main/output.rs"]
mod output;
#[path = "main/profile.rs"]
mod profile;
#[path = "main/commands.rs"]
mod shell_commands;
#[path = "main/transfer.rs"]
mod transfer;

use interactive::*;
use output::*;
use profile::*;
use shell_commands::*;
use transfer::*;

use clap::{Args, Parser, Subcommand};
use n2_native_client::{N2NativeClient, NativeClientError, NativeClientResult, ensure_native_ok};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};

const DEFAULT_NATIVE_URL: &str = "n2://127.0.0.1:50002";
const ONE_SHOT_LIMIT: u64 = 16 * 1024 * 1024;
const DEFAULT_PART_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Args, Clone)]
pub struct ServeArgs {
    #[arg(long, default_value = "0.0.0.0:50902")]
    pub bind: String,
    #[arg(long, default_value = "/opt/litiaina/n2")]
    pub install_dir: String,
    #[arg(long, default_value = "/usr/local/bin/litiaina-n2")]
    pub engine_binary: String,
    #[arg(long, default_value = "/usr/local/bin/litiaina-n2-gateway")]
    pub gateway_binary: String,
    #[arg(long, default_value = "/var/lib/litiaina/n2")]
    pub runtime_root: String,
    #[arg(long, default_value = "/var/lib/litiaina/n2-gateway")]
    pub gateway_runtime: String,
    #[arg(long, default_value = "/etc/litiaina/n2/n2.env")]
    pub env_file: String,
    #[arg(long, default_value = "/var/lib/litiaina/n2-setup")]
    pub setup_root: String,
    #[arg(long, default_value = "/mnt")]
    pub mount_root: String,
    #[arg(long, default_value = "/etc/fstab")]
    pub fstab: String,
    #[arg(long, default_value = "litiaina-n2")]
    pub service_name: String,
    #[arg(long, default_value = "litiaina-n2-gateway")]
    pub gateway_service_name: String,
    #[arg(long, default_value = "/etc/systemd/system", hide = true)]
    pub systemd_unit_dir: String,
    #[arg(long, default_value = "litiaina")]
    pub run_user: String,
    #[arg(long, default_value = "litiaina")]
    pub run_group: String,
    #[arg(long)]
    pub setup_token: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Laboratory only: permit multiple N2 nodes with the same machine fingerprint."
    )]
    pub allow_shared_failure_domain: bool,
}

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
        default_value = DEFAULT_NATIVE_URL,
        help = "Native N2 endpoint, for example n2://127.0.0.1:50002."
    )]
    native_url: String,

    #[arg(
        long,
        global = true,
        help = "Load native URL, fragment, and optional secret from a saved shell profile."
    )]
    profile: Option<String>,

    #[arg(
        short,
        long,
        global = true,
        help = "Fragment hash or stable fragment id used for authentication and fragment-scoped operations."
    )]
    fragment: Option<String>,

    #[arg(short, long, global = true, help = "Fragment secret.")]
    secret: Option<String>,

    #[arg(
        long,
        global = true,
        default_value_t = false,
        help = "Print compact JSON."
    )]
    json: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Serve the guarded browser installation terminal.")]
    Serve(Box<ServeArgs>),
    #[command(about = "Ping the native N2 endpoint.")]
    Ping,
    #[command(about = "Open a persistent operator shell.")]
    Shell,
    #[command(about = "Manage saved shell profiles.")]
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
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
    #[command(about = "Show detailed cluster monitor snapshot.")]
    Monitor(MonitorArgs),
    #[command(about = "Show a compact operator dashboard.")]
    Dashboard(DashboardArgs),
    #[command(about = "Show cluster membership.")]
    Membership,
    #[command(about = "Show resolved cluster layout.")]
    Layout,
    #[command(about = "Show cluster usage. This is monitoring-only.")]
    Usage {
        #[arg(long)]
        target_fragment: Option<String>,
        #[arg(long, default_value_t = false)]
        refresh: bool,
    },
    #[command(about = "Fragment administration.")]
    Fragment {
        #[command(subcommand)]
        command: FragmentCommand,
    },
    #[command(about = "List a namespace directory.")]
    List(ListArgs),
    #[command(about = "Stat a namespace path.")]
    Stat(PathArgs),
    #[command(about = "Rename or move a namespace path.")]
    Rename(RenameArgs),
    #[command(about = "Move a path into namespace trash.")]
    Trash(PathArgs),
    #[command(about = "Recover a path from namespace trash.")]
    Recover(PathArgs),
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
    #[command(about = "Local maintenance GC/scrub controls.")]
    Gc(JobArgs),
    #[command(about = "Cluster repair controls.")]
    Repair(JobArgs),
    #[command(about = "Cluster rebalance controls.")]
    Rebalance(JobArgs),
}

#[derive(Subcommand)]
enum ProfileCommand {
    #[command(about = "List saved shell profiles.")]
    List,
    #[command(about = "Show the profile store path.")]
    Path,
    #[command(about = "Save the current --native-url/--fragment profile.")]
    Save {
        #[arg(long)]
        name: String,
        #[arg(
            long,
            default_value_t = false,
            help = "Store --secret in the local profile file. Off by default."
        )]
        store_secret: bool,
    },
    #[command(about = "Remove a saved shell profile.")]
    Remove {
        #[arg(long)]
        name: String,
    },
}

#[derive(Args)]
struct AuthArgs {
    #[arg(long)]
    fragment: String,
    #[arg(long)]
    secret: String,
}

#[derive(Args)]
struct DashboardArgs {
    #[arg(long)]
    target_fragment: Option<String>,
    #[arg(long, default_value_t = 64)]
    object_limit: usize,
    #[arg(long, default_value_t = false)]
    probe_objects: bool,
    #[arg(long, default_value_t = false)]
    refresh: bool,
}

#[derive(Args)]
struct MonitorArgs {
    #[arg(long)]
    target_fragment: Option<String>,
    #[arg(long)]
    prefix: Option<String>,
    #[arg(long, default_value_t = 64)]
    object_limit: usize,
    #[arg(long, default_value_t = false)]
    probe_objects: bool,
    #[arg(long, default_value_t = false)]
    refresh: bool,
    #[arg(long, default_value_t = false)]
    deep_scan: bool,
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
    Redistribute {
        #[arg(long)]
        object_key: String,
        #[arg(long)]
        version_id: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value = "availability")]
        mode: String,
    },
}

#[derive(Subcommand)]
enum FragmentCommand {
    List,
    Check {
        #[arg(long)]
        fragment: String,
    },
    Hash {
        #[arg(long)]
        fragment: String,
    },
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        key: String,
        #[arg(long)]
        master_key: String,
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
    #[arg(
        long,
        default_value_t = 1,
        help = "Multipart parts to upload concurrently. Use >1 only against a direct coordinator or sticky native endpoint."
    )]
    parallel_parts: usize,
    #[arg(
        long,
        default_value_t = false,
        help = "Disable the server-side upload IO gate for this upload."
    )]
    no_io_gate: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Calculate and send SHA-256 for each uploaded body/part."
    )]
    sha256: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Use restart-proof receipt-backed multipart instead of speed-first standard multipart."
    )]
    durable: bool,
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
        #[arg(long, default_value_t = false)]
        durable: bool,
    },
    Status {
        #[arg(long)]
        version_id: String,
        #[arg(long, default_value_t = false)]
        durable: bool,
    },
    Cancel {
        #[arg(long)]
        version_id: String,
        #[arg(long)]
        object_key: Option<String>,
        #[arg(long, default_value_t = false)]
        durable: bool,
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
        #[arg(long, default_value_t = true)]
        mismatched_only: bool,
        #[arg(long)]
        max_duration_seconds: Option<u64>,
        #[arg(long)]
        max_objects: Option<u64>,
        #[arg(long)]
        max_directories: Option<u64>,
        #[arg(long)]
        max_bytes: Option<u64>,
        #[arg(long)]
        continuation_token: Option<String>,
        #[arg(long, default_value = "low")]
        priority: String,
    },
    Status,
    Watch {
        #[arg(long, default_value_t = 5)]
        interval_seconds: u64,
    },
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShellProfile {
    native_url: String,
    #[serde(default)]
    fragment: Option<String>,
    #[serde(default)]
    secret: Option<String>,
}

#[tokio::main]
async fn main() {
    let mut cli = Cli::parse();
    if let Some(args) = take_serve_command(&mut cli) {
        if let Err(error) = web::run(*args).await {
            eprintln!("n2 setup shell error: {error}");
            std::process::exit(1);
        }
        return;
    }
    if let Err(err) = run(cli).await {
        eprintln!("n2 shell error: {err}");
        std::process::exit(1);
    }
}

fn take_serve_command(cli: &mut Cli) -> Option<Box<ServeArgs>> {
    if !matches!(cli.command, Some(Command::Serve(_))) {
        return None;
    }
    let Some(Command::Serve(args)) = cli.command.take() else {
        unreachable!("serve command was already matched");
    };
    Some(args)
}

async fn run(mut cli: Cli) -> NativeClientResult<()> {
    if let Some(command) = take_profile_command(&mut cli) {
        return run_profile_command(command, &cli);
    }
    apply_profile(&mut cli)?;
    let command = cli.command.take().unwrap_or(Command::Shell);
    let mut client = N2NativeClient::connect(&cli.native_url)
        .await?
        .with_request_prefix("n2-shell");

    match command {
        Command::Serve(_) => unreachable!("serve is handled before native connection"),
        Command::Ping => print_value(client.raw_json("ping", &json!({}), None).await?, cli.json),
        Command::Shell => {
            authenticate_from_cli(&mut client, &cli).await?;
            interactive_shell(&mut client, &cli).await
        }
        Command::Profile { .. } => Ok(()),
        Command::Auth(auth) => {
            client.auth_fragment(&auth.fragment, &auth.secret).await?;
            print_value(
                client.raw_json("session.info", &json!({}), None).await?,
                cli.json,
            )
        }
        Command::Session => print_value(
            client.raw_json("session.info", &json!({}), None).await?,
            cli.json,
        ),
        Command::Raw { op, payload } => {
            authenticate_if_configured(&mut client, &cli).await?;
            let payload = parse_json(&payload)?;
            print_response(client.operation(&op, &payload, None).await?, cli.json)
        }
        Command::Fragment { command } if is_public_fragment_command(&command) => {
            run_fragment_command(&mut client, command, cli.json).await
        }
        command => {
            authenticate_from_cli(&mut client, &cli).await?;
            run_authenticated_command(&mut client, &cli, command).await
        }
    }
}

fn take_profile_command(cli: &mut Cli) -> Option<ProfileCommand> {
    if matches!(cli.command, Some(Command::Profile { .. })) {
        let Some(Command::Profile { command }) = cli.command.take() else {
            unreachable!("profile command was already matched");
        };
        return Some(command);
    }
    None
}

fn is_public_fragment_command(command: &FragmentCommand) -> bool {
    matches!(
        command,
        FragmentCommand::Create { .. }
            | FragmentCommand::Check { .. }
            | FragmentCommand::Hash { .. }
    )
}

async fn run_authenticated_command(
    client: &mut N2NativeClient,
    cli: &Cli,
    command: Command,
) -> NativeClientResult<()> {
    match command {
        Command::Serve(_) => unreachable!("serve is handled before native connection"),
        Command::Health => print_op(client, "maintenance.health", json!({}), cli.json).await,
        Command::Monitor(args) => {
            print_op(
                client,
                "cluster.monitor",
                monitor_payload(args, &cli.fragment),
                cli.json,
            )
            .await
        }
        Command::Dashboard(args) => print_dashboard(client, args, cli).await,
        Command::Membership => print_op(client, "cluster.membership", json!({}), cli.json).await,
        Command::Layout => print_op(client, "cluster.layout", json!({}), cli.json).await,
        Command::Usage {
            target_fragment,
            refresh,
        } => {
            print_op(
                client,
                "cluster.usage",
                json!({ "fragment": target_fragment, "refresh": refresh }),
                cli.json,
            )
            .await
        }
        Command::Fragment { command } => run_fragment_command(client, command, cli.json).await,
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
        Command::Rename(args) => print_op(
            client,
            "namespace.rename",
            json!({ "source_path": args.source_path, "destination_path": args.destination_path }),
            cli.json,
        )
        .await,
        Command::Trash(args) => {
            print_op(
                client,
                "namespace.trash",
                json!({ "path": args.path }),
                cli.json,
            )
            .await
        }
        Command::Recover(args) => {
            print_op(
                client,
                "namespace.recover",
                json!({ "path": args.path }),
                cli.json,
            )
            .await
        }
        Command::Object { command } => run_object_command(client, command, cli.json).await,
        Command::Upload(args) => upload_file(client, args, cli, cli.json).await,
        Command::Download(args) => download_file(client, args).await,
        Command::Uploads { command } => run_uploads_command(client, command, cli.json).await,
        Command::Gc(args) => run_gc_command(client, args, cli.json).await,
        Command::Repair(args) => {
            run_job_command(client, "cluster.repair", args, &cli.fragment, cli.json).await
        }
        Command::Rebalance(args) => {
            run_job_command(client, "cluster.rebalance", args, &cli.fragment, cli.json).await
        }
        Command::Ping
        | Command::Shell
        | Command::Profile { .. }
        | Command::Auth(_)
        | Command::Session
        | Command::Raw { .. } => Ok(()),
    }
}

fn dashboard_payload(args: DashboardArgs, auth_fragment: &Option<String>) -> Value {
    json!({
        "fragment": args.target_fragment.or_else(|| auth_fragment.clone()),
        "object_limit": args.object_limit,
        "probe_objects": args.probe_objects,
        "refresh": args.refresh,
    })
}

#[cfg(test)]
#[path = "main/tests.rs"]
mod tests;
