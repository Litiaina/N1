use clap::{Args, Parser, Subcommand};
use crc32fast::Hasher;
use n2_native_client::{N2NativeClient, NativeClientError, NativeClientResult};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
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
        help = "Calculate and send CRC32 for each uploaded body/part."
    )]
    crc32: bool,
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
    if let Err(err) = run().await {
        eprintln!("n2 shell error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> NativeClientResult<()> {
    let mut cli = Cli::parse();
    if let Some(command) = take_profile_command(&mut cli) {
        return run_profile_command(command, &cli);
    }
    apply_profile(&mut cli)?;
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
        Command::Dashboard(args) => print_dashboard(client, args, &cli).await,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_precheck_does_not_discard_normal_commands() {
        let mut cli = Cli::try_parse_from([
            "litiaina-n2-shell",
            "--native-url",
            "n2://127.0.0.1:50002",
            "--fragment",
            "frag",
            "--secret",
            "secret",
            "upload",
            "--object-key",
            "bench/test.bin",
            "--file",
            "/tmp/test.bin",
        ])
        .expect("upload command parses");

        assert!(take_profile_command(&mut cli).is_none());
        assert!(matches!(cli.command, Some(Command::Upload(_))));
    }

    #[test]
    fn profile_precheck_takes_profile_commands_only() {
        let mut cli = Cli::try_parse_from(["litiaina-n2-shell", "profile", "path"])
            .expect("profile command parses");

        assert!(matches!(
            take_profile_command(&mut cli),
            Some(ProfileCommand::Path)
        ));
        assert!(cli.command.is_none());
    }

    #[test]
    fn fragment_create_check_and_hash_are_public_commands() {
        assert!(is_public_fragment_command(&FragmentCommand::Create {
            name: "frag".to_string(),
            key: "key".to_string(),
            master_key: "master".to_string(),
        }));
        assert!(is_public_fragment_command(&FragmentCommand::Check {
            fragment: "frag".to_string(),
        }));
        assert!(is_public_fragment_command(&FragmentCommand::Hash {
            fragment: "frag".to_string(),
        }));
        assert!(!is_public_fragment_command(&FragmentCommand::List));
    }

    #[test]
    fn flat_namespace_shell_has_no_manual_purge_or_empty_directory_commands() {
        for command in ["mkdir", "purge", "empty-trash"] {
            assert!(
                Cli::try_parse_from(["litiaina-n2-shell", command]).is_err(),
                "retired command '{command}' unexpectedly parsed"
            );
        }
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

fn run_profile_command(command: ProfileCommand, cli: &Cli) -> NativeClientResult<()> {
    match command {
        ProfileCommand::Path => {
            println!("{}", profile_store_path()?.display());
            Ok(())
        }
        ProfileCommand::List => {
            let profiles = load_profiles()?;
            if profiles.is_empty() {
                println!("No N2 shell profiles saved.");
                return Ok(());
            }
            for (name, profile) in profiles {
                let fragment = profile.fragment.unwrap_or_else(|| "-".to_string());
                let secret = if profile.secret.is_some() {
                    "stored"
                } else {
                    "not stored"
                };
                println!(
                    "{name}\n  native_url: {}\n  fragment:   {fragment}\n  secret:     {secret}",
                    profile.native_url
                );
            }
            Ok(())
        }
        ProfileCommand::Save { name, store_secret } => {
            let mut profiles = load_profiles()?;
            let profile = ShellProfile {
                native_url: cli.native_url.clone(),
                fragment: cli.fragment.clone(),
                secret: if store_secret {
                    cli.secret.clone()
                } else {
                    None
                },
            };
            profiles.insert(name.clone(), profile);
            save_profiles(&profiles)?;
            println!("saved N2 shell profile '{name}'");
            Ok(())
        }
        ProfileCommand::Remove { name } => {
            let mut profiles = load_profiles()?;
            if profiles.remove(&name).is_some() {
                save_profiles(&profiles)?;
                println!("removed N2 shell profile '{name}'");
            } else {
                println!("profile '{name}' was not found");
            }
            Ok(())
        }
    }
}

fn apply_profile(cli: &mut Cli) -> NativeClientResult<()> {
    let Some(name) = cli.profile.clone() else {
        return Ok(());
    };
    let profiles = load_profiles()?;
    let profile = profiles.get(&name).ok_or_else(|| {
        NativeClientError::protocol(format!("N2 shell profile '{name}' not found"))
    })?;
    if cli.native_url == DEFAULT_NATIVE_URL {
        cli.native_url = profile.native_url.clone();
    }
    if cli.fragment.is_none() {
        cli.fragment = profile.fragment.clone();
    }
    if cli.secret.is_none() {
        cli.secret = profile.secret.clone();
    }
    Ok(())
}

fn load_profiles() -> NativeClientResult<BTreeMap<String, ShellProfile>> {
    let path = profile_store_path()?;
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = fs::read(&path)?;
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn save_profiles(profiles: &BTreeMap<String, ShellProfile>) -> NativeClientResult<()> {
    let path = profile_store_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(profiles)?;
    fs::write(path, body)?;
    Ok(())
}

fn profile_store_path() -> NativeClientResult<PathBuf> {
    if let Ok(config_home) = env::var("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(config_home).join("litiaina/n2/shell-profiles.json"));
    }
    let home = env::var("HOME").map_err(|_| {
        NativeClientError::protocol("HOME is not set; cannot resolve N2 shell profile path")
    })?;
    Ok(PathBuf::from(home).join(".config/litiaina/n2/shell-profiles.json"))
}

fn monitor_payload(args: MonitorArgs, auth_fragment: &Option<String>) -> Value {
    json!({
        "fragment": args.target_fragment.or_else(|| auth_fragment.clone()),
        "prefix": args.prefix,
        "object_limit": args.object_limit,
        "probe_objects": args.probe_objects,
        "refresh": args.refresh,
        "deep_scan": args.deep_scan,
    })
}

async fn run_fragment_command(
    client: &mut N2NativeClient,
    command: FragmentCommand,
    json_output: bool,
) -> NativeClientResult<()> {
    match command {
        FragmentCommand::List => print_op(client, "fragment.list", json!({}), json_output).await,
        FragmentCommand::Check { fragment } => {
            print_op(
                client,
                "fragment.check",
                json!({ "fragment": fragment }),
                json_output,
            )
            .await
        }
        FragmentCommand::Hash { fragment } => {
            print_op(
                client,
                "fragment.hash.public",
                json!({ "fragment": fragment }),
                json_output,
            )
            .await
        }
        FragmentCommand::Create {
            name,
            key,
            master_key,
        } => {
            print_op(
                client,
                "fragment.create",
                json!({
                    "name": name,
                    "key": key,
                    "secret": master_key,
                }),
                json_output,
            )
            .await
        }
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
            print_op(
                client,
                "object.versions",
                json!({ "object_key": object_key }),
                json_output,
            )
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
        ObjectCommand::Redistribute {
            object_key,
            version_id,
            dry_run,
            force,
            mode,
        } => {
            print_op(
                client,
                "object.redistribute",
                json!({
                    "object_key": object_key,
                    "version_id": version_id,
                    "dry_run": dry_run,
                    "force": force,
                    "mode": mode,
                }),
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
            print_op(
                client,
                "uploads.list",
                json!({ "limit": limit }),
                json_output,
            )
            .await
        }
        UploadsCommand::Status { version_id } => {
            print_op(
                client,
                "uploads.status",
                json!({ "version_id": version_id }),
                json_output,
            )
            .await
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

async fn run_gc_command(
    client: &mut N2NativeClient,
    args: JobArgs,
    json_output: bool,
) -> NativeClientResult<()> {
    match args.action {
        JobAction::Plan | JobAction::Status => {
            print_op(client, "maintenance.gc.status", json!({}), json_output).await
        }
        JobAction::Watch { interval_seconds } => {
            watch_job(
                client,
                "maintenance.gc",
                interval_seconds.max(1),
                json_output,
            )
            .await
        }
        JobAction::Stop => print_op(client, "maintenance.gc.stop", json!({}), json_output).await,
        JobAction::Start {
            target_fragment: _,
            prefix: _,
            limit: _,
            dry_run: _,
            mismatched_only: _,
            max_duration_seconds: _,
            max_objects: _,
            max_directories: _,
            max_bytes: _,
            continuation_token: _,
            priority: _,
        } => {
            print_op(
                client,
                "maintenance.gc.start",
                json!({ "mode": "scrub", "fast_mode": false }),
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
        JobAction::Plan => {
            print_op(client, &format!("{prefix}.plan"), json!({}), json_output).await
        }
        JobAction::Status => {
            print_op(client, &format!("{prefix}.status"), json!({}), json_output).await
        }
        JobAction::Watch { interval_seconds } => {
            watch_job(client, prefix, interval_seconds.max(1), json_output).await
        }
        JobAction::Stop => {
            print_op(client, &format!("{prefix}.stop"), json!({}), json_output).await
        }
        JobAction::Start {
            target_fragment,
            prefix: object_prefix,
            limit,
            dry_run,
            mismatched_only,
            max_duration_seconds,
            max_objects,
            max_directories,
            max_bytes,
            continuation_token,
            priority,
        } => {
            let fragment = target_fragment
                .or_else(|| auth_fragment.clone())
                .ok_or_else(|| {
                    NativeClientError::protocol(
                        "job start requires --fragment or --target-fragment",
                    )
                })?;
            print_op(
                client,
                &format!("{prefix}.start"),
                json!({
                    "fragment": fragment,
                    "prefix": object_prefix,
                    "limit": limit,
                    "dry_run": dry_run,
                    "mismatched_only": mismatched_only,
                    "max_duration_seconds": max_duration_seconds,
                    "max_objects": max_objects,
                    "max_directories": max_directories,
                    "max_bytes": max_bytes,
                    "continuation_token": continuation_token,
                    "priority": priority,
                }),
                json_output,
            )
            .await
        }
    }
}

async fn watch_job(
    client: &mut N2NativeClient,
    prefix: &str,
    interval_seconds: u64,
    json_output: bool,
) -> NativeClientResult<()> {
    loop {
        let value = client
            .operation_json(&format!("{prefix}.status"), &json!({}), None)
            .await?;
        if json_output {
            println!("{}", serde_json::to_string(&value)?);
        } else {
            print_job_status_line(prefix, &value);
        }
        if !value
            .get("running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            break;
        }
        sleep(Duration::from_secs(interval_seconds)).await;
    }
    Ok(())
}

async fn upload_file(
    client: &mut N2NativeClient,
    args: UploadArgs,
    cli: &Cli,
    json_output: bool,
) -> NativeClientResult<()> {
    let mut file = File::open(&args.file).await?;
    let size = file.metadata().await?.len();
    let content_type = args
        .content_type
        .unwrap_or_else(|| content_type_from_path(&args.file));

    if size <= ONE_SHOT_LIMIT {
        let checksum = if args.crc32 {
            Some(crc32_range(&mut file, 0, size).await?)
        } else {
            None
        };
        file.seek(std::io::SeekFrom::Start(0)).await?;
        let response = client
            .operation_reader(
                "upload.one_shot",
                &json!({
                    "object_key": args.object_key,
                    "content_type": content_type,
                    "io_gate": !args.no_io_gate,
                    "checksum": checksum,
                }),
                &mut file,
                size,
            )
            .await?;
        return print_response(response, json_output);
    }

    let part_size = args.part_size.max(1024 * 1024);
    let expected_parts = size.div_ceil(part_size);
    let parallel_parts = args
        .parallel_parts
        .max(1)
        .min(expected_parts.max(1) as usize);
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

    if parallel_parts == 1 {
        for part_index in 0..expected_parts {
            let start = part_index.saturating_mul(part_size);
            let part_len = part_size.min(size.saturating_sub(start));
            let checksum = if args.crc32 {
                Some(crc32_range(&mut file, start, part_len).await?)
            } else {
                None
            };
            file.seek(std::io::SeekFrom::Start(start)).await?;
            let mut part_reader = (&mut file).take(part_len);
            client
                .operation_reader(
                    "multipart.put_part",
                    &json!({
                        "version_id": version_id,
                        "part_index": part_index,
                        "io_gate": !args.no_io_gate,
                        "checksum": checksum,
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
    } else {
        let fragment = cli
            .fragment
            .clone()
            .ok_or_else(|| NativeClientError::protocol("parallel upload requires --fragment"))?;
        let secret = cli
            .secret
            .clone()
            .ok_or_else(|| NativeClientError::protocol("parallel upload requires --secret"))?;
        let native_url = cli.native_url.clone();
        let file_path = args.file.clone();
        let mut next_part = 0u64;
        let mut completed_parts = 0u64;
        let mut workers = JoinSet::new();

        while next_part < expected_parts || !workers.is_empty() {
            while next_part < expected_parts && workers.len() < parallel_parts {
                let part_index = next_part;
                next_part = next_part.saturating_add(1);
                workers.spawn(upload_part_with_new_client(
                    native_url.clone(),
                    fragment.clone(),
                    secret.clone(),
                    file_path.clone(),
                    version_id.clone(),
                    part_index,
                    part_size,
                    size,
                    args.crc32,
                    !args.no_io_gate,
                ));
            }

            let Some(joined) = workers.join_next().await else {
                continue;
            };
            let (part_index, uploaded_bytes) = joined.map_err(|err| {
                NativeClientError::protocol(format!("upload worker failed: {err}"))
            })??;
            completed_parts = completed_parts.saturating_add(1);
            eprintln!(
                "uploaded part {}/{} ({})",
                part_index + 1,
                expected_parts,
                human_bytes(uploaded_bytes)
            );
        }

        if completed_parts != expected_parts {
            return Err(NativeClientError::protocol(format!(
                "uploaded {completed_parts} parts, expected {expected_parts}"
            )));
        }
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

async fn upload_part_with_new_client(
    native_url: String,
    fragment: String,
    secret: String,
    file_path: PathBuf,
    version_id: String,
    part_index: u64,
    part_size: u64,
    total_size: u64,
    crc32: bool,
    io_gate: bool,
) -> NativeClientResult<(u64, u64)> {
    let mut client = N2NativeClient::connect(&native_url)
        .await?
        .with_request_prefix(format!("n2-shell-part-{part_index}"));
    client.auth_fragment(&fragment, &secret).await?;

    let start = part_index.saturating_mul(part_size);
    let part_len = part_size.min(total_size.saturating_sub(start));
    let mut file = File::open(&file_path).await?;
    let checksum = if crc32 {
        Some(crc32_range(&mut file, start, part_len).await?)
    } else {
        None
    };
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut part_reader = (&mut file).take(part_len);
    client
        .operation_reader(
            "multipart.put_part",
            &json!({
                "version_id": version_id,
                "part_index": part_index,
                "io_gate": io_gate,
                "checksum": checksum,
            }),
            &mut part_reader,
            part_len,
        )
        .await?;
    Ok((part_index, start.saturating_add(part_len)))
}

async fn crc32_range(file: &mut File, offset: u64, len: u64) -> NativeClientResult<String> {
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut remaining = len;
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut hasher = Hasher::new();
    while remaining > 0 {
        let take = buffer.len().min(remaining as usize);
        let read = file.read(&mut buffer[..take]).await?;
        if read == 0 {
            return Err(NativeClientError::protocol(format!(
                "file ended while calculating CRC32: expected {len} bytes from offset {offset}"
            )));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(format!("{:08x}", hasher.finalize()))
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
            .unwrap_or_else(|| {
                (
                    "native_error".to_string(),
                    "unknown native error".to_string(),
                )
            });
        return Err(NativeClientError::Native { code, message });
    }
    eprintln!(
        "downloaded {} to {}",
        human_bytes(bytes),
        args.file.display()
    );
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
        ["session"] => print_value(
            client.raw_json("session.info", &json!({}), None).await?,
            cli.json,
        ),
        ["health"] => print_op(client, "maintenance.health", json!({}), cli.json).await,
        ["cluster"] | ["cluster", "health"] => {
            print_op(client, "cluster.health", json!({}), cli.json).await
        }
        ["membership"] => print_op(client, "cluster.membership", json!({}), cli.json).await,
        ["layout"] => print_op(client, "cluster.layout", json!({}), cli.json).await,
        ["usage"] => print_op(client, "cluster.usage", json!({}), cli.json).await,
        ["usage", "refresh"] | ["usage", "--refresh"] => {
            print_op(client, "cluster.usage", json!({"refresh": true}), cli.json).await
        }
        ["monitor"] => {
            print_op(
                client,
                "cluster.monitor",
                json!({"fragment": cli.fragment.clone(), "object_limit": 64}),
                cli.json,
            )
            .await
        }
        ["monitor", "deep"] => {
            print_op(
                client,
                "cluster.monitor",
                json!({"fragment": cli.fragment.clone(), "object_limit": 64, "deep_scan": true}),
                cli.json,
            )
            .await
        }
        ["monitor", "probe"] => print_op(
            client,
            "cluster.monitor",
            json!({"fragment": cli.fragment.clone(), "object_limit": 64, "probe_objects": true}),
            cli.json,
        )
        .await,
        ["dashboard"] => {
            print_dashboard(
                client,
                DashboardArgs {
                    target_fragment: cli.fragment.clone(),
                    object_limit: 64,
                    probe_objects: false,
                    refresh: false,
                },
                cli,
            )
            .await
        }
        ["dashboard", "probe"] => {
            print_dashboard(
                client,
                DashboardArgs {
                    target_fragment: cli.fragment.clone(),
                    object_limit: 64,
                    probe_objects: true,
                    refresh: false,
                },
                cli,
            )
            .await
        }
        ["fragments"] => print_op(client, "fragment.list", json!({}), cli.json).await,
        ["fragment", "check", fragment] => {
            print_op(
                client,
                "fragment.check",
                json!({"fragment": fragment}),
                cli.json,
            )
            .await
        }
        ["fragment", "hash", fragment] => {
            print_op(
                client,
                "fragment.hash",
                json!({"fragment": fragment}),
                cli.json,
            )
            .await
        }
        ["list"] => {
            print_op(
                client,
                "namespace.list",
                json!({"path": "", "limit": 200}),
                cli.json,
            )
            .await
        }
        ["list", path] => {
            print_op(
                client,
                "namespace.list",
                json!({"path": path, "limit": 200}),
                cli.json,
            )
            .await
        }
        ["trash-list"] => {
            print_op(
                client,
                "namespace.list",
                json!({"path": "", "limit": 200, "trash": true}),
                cli.json,
            )
            .await
        }
        ["stat", path] => print_op(client, "namespace.stat", json!({"path": path}), cli.json).await,
        ["rename", source, destination] | ["move", source, destination] => {
            print_op(
                client,
                "namespace.rename",
                json!({"source_path": source, "destination_path": destination}),
                cli.json,
            )
            .await
        }
        ["trash", path] => {
            print_op(client, "namespace.trash", json!({"path": path}), cli.json).await
        }
        ["recover", path] => {
            print_op(client, "namespace.recover", json!({"path": path}), cli.json).await
        }
        ["uploads"] => print_op(client, "uploads.list", json!({"limit": 50}), cli.json).await,
        ["upload-status", version_id] => {
            print_op(
                client,
                "uploads.status",
                json!({"version_id": version_id}),
                cli.json,
            )
            .await
        }
        ["object", "meta", object_key] => {
            print_op(
                client,
                "object.meta",
                json!({"object_key": object_key}),
                cli.json,
            )
            .await
        }
        ["object", "versions", object_key] => {
            print_op(
                client,
                "object.versions",
                json!({"object_key": object_key}),
                cli.json,
            )
            .await
        }
        ["object", "status", object_key] => {
            print_op(
                client,
                "object.status",
                json!({"object_key": object_key}),
                cli.json,
            )
            .await
        }
        ["object", "repair", object_key] => {
            print_op(
                client,
                "object.repair",
                json!({"object_key": object_key, "mode": "strict"}),
                cli.json,
            )
            .await
        }
        ["object", "redistribute", object_key] => {
            print_op(
                client,
                "object.redistribute",
                json!({"object_key": object_key}),
                cli.json,
            )
            .await
        }
        ["object", "redistribute", object_key, "dry-run"]
        | ["object", "redistribute", object_key, "--dry-run"] => {
            print_op(
                client,
                "object.redistribute",
                json!({"object_key": object_key, "dry_run": true}),
                cli.json,
            )
            .await
        }
        ["repair"] | ["repair", "status"] => {
            print_op(client, "cluster.repair.status", json!({}), cli.json).await
        }
        ["repair", "plan"] => print_op(client, "cluster.repair.plan", json!({}), cli.json).await,
        ["repair", "stop"] => print_op(client, "cluster.repair.stop", json!({}), cli.json).await,
        ["repair", "watch"] => watch_job(client, "cluster.repair", 5, cli.json).await,
        ["repair", "start"] => {
            let fragment = cli.fragment.clone().ok_or_else(|| {
                NativeClientError::protocol("repair start requires global --fragment")
            })?;
            print_op(
                client,
                "cluster.repair.start",
                json!({"fragment": fragment}),
                cli.json,
            )
            .await
        }
        ["rebalance"] | ["rebalance", "status"] => {
            print_op(client, "cluster.rebalance.status", json!({}), cli.json).await
        }
        ["rebalance", "plan"] => {
            print_op(client, "cluster.rebalance.plan", json!({}), cli.json).await
        }
        ["rebalance", "stop"] => {
            print_op(client, "cluster.rebalance.stop", json!({}), cli.json).await
        }
        ["rebalance", "watch"] => watch_job(client, "cluster.rebalance", 5, cli.json).await,
        ["rebalance", "start"] => {
            let fragment = cli.fragment.clone().ok_or_else(|| {
                NativeClientError::protocol("rebalance start requires global --fragment")
            })?;
            print_op(
                client,
                "cluster.rebalance.start",
                json!({"fragment": fragment, "mismatched_only": true}),
                cli.json,
            )
            .await
        }
        ["gc"] | ["gc", "status"] => {
            print_op(client, "maintenance.gc.status", json!({}), cli.json).await
        }
        ["gc", "scrub"] | ["gc", "start"] => {
            print_op(
                client,
                "maintenance.gc.start",
                json!({"mode": "scrub", "fast_mode": false}),
                cli.json,
            )
            .await
        }
        ["gc", "stop"] => print_op(client, "maintenance.gc.stop", json!({}), cli.json).await,
        ["gc", "watch"] => watch_job(client, "maintenance.gc", 5, cli.json).await,
        _ => {
            println!("unknown command. Type 'help'.");
            Ok(())
        }
    }
}

fn print_shell_help() {
    println!(
        "commands:\n  ping\n  session\n  health\n  cluster [health]\n  membership\n  layout\n  dashboard [probe]\n  monitor [deep|probe]\n  usage [refresh]\n  fragments\n  fragment check <fragment>\n  fragment hash <fragment>\n  list [path]\n  trash-list\n  stat <path>\n  rename <source> <destination>\n  trash <path>\n  recover <path>\n  uploads\n  upload-status <version_id>\n  object meta <object_key>\n  object versions <object_key>\n  object status <object_key>\n  object repair <object_key>\n  object redistribute <object_key> [dry-run]\n  repair status|plan|start|watch|stop\n  rebalance status|plan|start|watch|stop\n  gc status|scrub|watch|stop\n  exit"
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

async fn print_dashboard(
    client: &mut N2NativeClient,
    args: DashboardArgs,
    cli: &Cli,
) -> NativeClientResult<()> {
    let value = client
        .operation_json(
            "cluster.monitor",
            &dashboard_payload(args, &cli.fragment),
            None,
        )
        .await?;
    if cli.json {
        return print_value(value, true);
    }

    println!("N2 Operator Dashboard");
    println!("---------------------");
    println!(
        "cluster: {} | access: {} | epoch: {}",
        value_str(&value, &["cluster", "status"])
            .or_else(|| value_str(&value, &["status"]))
            .unwrap_or("unknown"),
        value_str(&value, &["cluster", "access_mode"])
            .or_else(|| value_str(&value, &["access_mode"]))
            .unwrap_or("unknown"),
        value_u64(&value, &["cluster", "membership_epoch"])
            .or_else(|| value_u64(&value, &["membership", "epoch"]))
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );

    if let Some(layout) = value.get("topology").or_else(|| value.get("layout")) {
        println!(
            "layout: data={} parity={} active_nodes={} old_layout={} current_layout={}",
            value_u64(layout, &["data_shards"]).unwrap_or(0),
            value_u64(layout, &["parity_shards"]).unwrap_or(0),
            value_u64(layout, &["active_write_nodes"]).unwrap_or_else(|| {
                layout
                    .get("active_nodes")
                    .and_then(Value::as_array)
                    .map(|nodes| nodes.len() as u64)
                    .unwrap_or(0)
            }),
            value_u64(layout, &["old_layout_objects"]).unwrap_or(0),
            value_u64(layout, &["current_layout_objects"]).unwrap_or(0),
        );
    }

    if let Some(nodes) = value.get("nodes").and_then(Value::as_array) {
        let online = nodes
            .iter()
            .filter(|node| {
                value_str(node, &["status"]) == Some("online")
                    || value_str(node, &["state"]) == Some("online")
            })
            .count();
        println!("nodes: {online}/{} online", nodes.len());
        for node in nodes {
            println!(
                "  {} {:<10} rss={} rtt={}ms disk_free={}",
                value_str(node, &["id"])
                    .or_else(|| value_str(node, &["node_id"]))
                    .unwrap_or("node"),
                value_str(node, &["status"])
                    .or_else(|| value_str(node, &["state"]))
                    .unwrap_or("unknown"),
                value_u64(node, &["rss_bytes"])
                    .map(human_bytes)
                    .unwrap_or_else(|| "-".to_string()),
                value_u64(node, &["rtt_ms"]).unwrap_or(0),
                value_u64(node, &["disk_free_bytes"])
                    .map(human_bytes)
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
    }

    if let Some(health) = value.get("objects").or_else(|| value.get("object_health")) {
        println!(
            "objects: healthy={} degraded={} unavailable={} repairable={}",
            value_u64(health, &["healthy"]).unwrap_or(0),
            value_u64(health, &["degraded"]).unwrap_or(0),
            value_u64(health, &["unavailable"]).unwrap_or(0),
            value_u64(health, &["repairable"]).unwrap_or(0),
        );
    }

    if let Some(jobs) = value.get("jobs") {
        print_job_status_line("repair", jobs.get("repair").unwrap_or(&Value::Null));
        print_job_status_line("rebalance", jobs.get("rebalance").unwrap_or(&Value::Null));
    }

    if let Some(io_control) = value.get("io_control") {
        println!(
            "io: peer_read_pressure={} peer_write_pressure={} stalled_peers={}",
            value_u64(io_control, &["peer_read_pressure"]).unwrap_or(0),
            value_u64(io_control, &["peer_write_pressure"]).unwrap_or(0),
            value_u64(io_control, &["stalled_peers"]).unwrap_or(0),
        );
    }

    Ok(())
}

fn print_job_status_line(label: &str, value: &Value) {
    let running = value
        .get("running")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let job = value_str(value, &["job"]).unwrap_or(label);
    let scanned = value_u64(value, &["scanned"]).unwrap_or(0);
    let rewritten = value_u64(value, &["rewritten"]).unwrap_or(0);
    let skipped = value_u64(value, &["skipped"])
        .or_else(|| value_u64(value, &["skipped_current_layout"]))
        .unwrap_or(0);
    let failed = value_u64(value, &["failed"]).unwrap_or(0);
    let last_error = value_str(value, &["last_error"]).unwrap_or("-");
    println!(
        "{job}: running={running} scanned={scanned} rewritten={rewritten} skipped={skipped} failed={failed} last_error={last_error}"
    );
}

fn value_str<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_str()
}

fn value_u64(value: &Value, path: &[&str]) -> Option<u64> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_u64()
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
