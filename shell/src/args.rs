use clap::{Args, Parser, Subcommand};
use n1_link::PoolId;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "litiaina-n1-shell",
    version,
    about = "Native NOA 2 client for LINK objects"
)]
pub(crate) struct Cli {
    #[arg(long, global = true)]
    pub(crate) endpoint: Option<String>,
    #[arg(long, global = true)]
    pub(crate) credentials: Option<PathBuf>,
    #[arg(long, global = true)]
    pub(crate) ca: Option<PathBuf>,
    #[command(subcommand)]
    pub(crate) command: Action,
}
#[derive(Subcommand)]
pub(crate) enum Action {
    /// Run explicit offline pool maintenance with the matching engine binary.
    Pool(OfflineArgs),
    Ping,
    Put {
        file: PathBuf,
        #[arg(long)]
        pool: Option<PoolId>,
        #[arg(long)]
        part_bytes: Option<u64>,
        #[arg(long)]
        durable_parts: bool,
    },
    Batch {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[arg(long)]
        pool: Option<PoolId>,
    },
    Init {
        #[arg(long)]
        bytes: u64,
        #[arg(long)]
        part_bytes: u64,
        #[arg(long)]
        pool: Option<PoolId>,
        #[arg(long)]
        durable: bool,
        #[arg(long, default_value_t = 86400)]
        ttl_seconds: u64,
    },
    Part {
        #[arg(long)]
        upload_key: String,
        #[arg(long)]
        part: u64,
        file: PathBuf,
    },
    Status {
        #[arg(long)]
        upload_key: String,
        #[arg(long, default_value_t = 0)]
        start: u64,
        #[arg(long, default_value_t = 4096)]
        limit: u64,
    },
    Finalize {
        #[arg(long)]
        upload_key: String,
    },
    Cancel {
        #[arg(long)]
        upload_key: String,
    },
    Get {
        #[arg(long)]
        key: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long)]
        length: Option<u64>,
    },
    Stat {
        #[arg(long)]
        key: String,
    },
    Health {
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        deep: bool,
    },
    Repair {
        #[arg(long)]
        key: String,
    },
    Delete {
        #[arg(long, required = true)]
        key: Vec<String>,
    },
    Pools,
    Gc,
    Scrub {
        #[arg(long)]
        repair: bool,
        #[arg(long,default_value_t=16*1024*1024)]
        bytes_per_second: u64,
        #[arg(long, default_value_t = 3600)]
        window_seconds: u64,
    },
    Job,
    Stop {
        #[arg(long)]
        job_id: String,
    },
    /// Operate on configured recovery repository aliases.
    Backup {
        #[command(subcommand)]
        command: BackupAction,
    },
}
#[derive(Subcommand)]
pub(crate) enum BackupAction {
    /// Serve the Console's dedicated operator API on the repository host.
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    /// Generate an owner-only operator API token without printing it.
    OperatorKeygen {
        #[arg(long)]
        token_file: PathBuf,
    },
    /// Run offline backup/recovery without opening a native connection.
    Offline(OfflineArgs),
    Capture {
        #[arg(long)]
        repository: String,
        #[arg(long, default_value_t = 30)]
        boundary_seconds: u64,
    },
    List {
        #[arg(long)]
        repository: String,
        #[arg(long, default_value = "")]
        after: String,
        #[arg(long, default_value_t = 64)]
        limit: usize,
    },
    Verify {
        #[arg(long)]
        repository: String,
        #[arg(long)]
        snapshot: String,
    },
    Replicate {
        #[arg(long)]
        repository: String,
        #[arg(long)]
        destination: String,
        #[arg(long)]
        snapshot: String,
    },
    Prune {
        #[arg(long)]
        repository: String,
        #[arg(long)]
        keep_last: usize,
        #[arg(long)]
        protect: String,
    },
    Gc {
        #[arg(long)]
        repository: String,
        #[arg(long)]
        discard_incomplete: bool,
    },
    Job,
    Stop {
        #[arg(long)]
        job_id: String,
    },
}

#[derive(Args, Debug, Clone)]
pub struct OfflineArgs {
    /// Matching N1 engine. Defaults to the engine beside this shell binary.
    #[arg(long)]
    pub engine_binary: Option<PathBuf>,
    /// Engine-side operation and flags, for example: verify --config /etc/n1.json.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
    pub arguments: Vec<std::ffi::OsString>,
}
