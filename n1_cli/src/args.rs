use clap::{Args, Parser, Subcommand, ValueEnum};

const CLI_LONG_ABOUT: &str = "\
Operator-first CLI for N1 3.0.

Use this CLI to inspect health, track upload pressure, and run maintenance jobs
without guessing the HTTP routes behind them. Global flags apply to every
subcommand, so set the server URL, fragment, and secret once and keep the rest
focused on the operation you need.

N1 authentication is fragment-scoped. For protected commands, pass either
--fragment <name-or-hash> or --auth-fragment <name-or-hash>.";

const CLI_AFTER_HELP: &str = "\
Examples:
  litiaina-n1-cli --url https://n1.local:50001 --secret <secret> --fragment mainframe health
  litiaina-n1-cli --url https://n1.local:50001 --secret <secret> --fragment mainframe health --watch
  litiaina-n1-cli --url https://n1.local:50001 --secret <secret> --fragment mainframe uploads list
  litiaina-n1-cli --url https://n1.local:50001 --secret <secret> --fragment mainframe repair start --mode availability
  litiaina-n1-cli --url https://n1.local:50001 --secret <secret> --fragment mainframe redistribute start --mode strict --durability strict
  litiaina-n1-cli --url https://n1.local:50001 --secret <secret> --fragment mainframe gc watch";

#[derive(Parser, Debug)]
#[command(
    name = "litiaina-n1-cli",
    version,
    about = "Operator CLI for N1 3.0",
    long_about = CLI_LONG_ABOUT,
    subcommand_required = true,
    arg_required_else_help = true,
    next_line_help = true,
    after_help = CLI_AFTER_HELP
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(
        short,
        long,
        global = true,
        help_heading = "Connection",
        help = "Fragment secret used for authenticated maintenance requests."
    )]
    pub secret: Option<String>,

    #[arg(
        short,
        long,
        global = true,
        help_heading = "Connection",
        help = "Target fragment name or hash. Also used for authentication when --auth-fragment is omitted."
    )]
    pub fragment: Option<String>,

    #[arg(
        long,
        global = true,
        help_heading = "Connection",
        help = "Override the fragment name or hash used only for authentication."
    )]
    pub auth_fragment: Option<String>,

    #[arg(
        long,
        global = true,
        help_heading = "Connection",
        help = "Base server URL, for example https://127.0.0.1:50001."
    )]
    pub url: Option<String>,

    #[arg(
        long,
        global = true,
        default_value_t = true,
        help_heading = "Connection",
        help = "Allow invalid TLS certificates. Keep this on only for trusted lab or LAN deployments."
    )]
    pub insecure: bool,

    #[arg(
        long,
        global = true,
        default_value_t = 1000,
        help_heading = "Output",
        help = "Polling interval in milliseconds for watch-style commands."
    )]
    pub poll_ms: u64,

    #[arg(
        long,
        global = true,
        default_value_t = false,
        help_heading = "Output",
        help = "Emit raw JSON instead of formatted operator output."
    )]
    pub json: bool,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    #[command(
        about = "Manage engine fragments.",
        long_about = "Create or inspect fragment-level access scopes used by N1."
    )]
    Fragment {
        #[command(subcommand)]
        command: FragmentCommands,
    },

    #[command(
        about = "Show engine health and maintenance debt.",
        long_about = "Fetch the unified N1 health snapshot. This includes disk state, upload pressure, process memory, maintenance activity, and redistribution debt."
    )]
    Health {
        #[arg(
            long,
            default_value_t = false,
            help = "Continuously refresh the health snapshot using --poll-ms."
        )]
        watch: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Ask the engine to perform a deeper partition scan instead of using the lighter cached view."
        )]
        deep_scan: bool,
    },

    #[command(
        about = "Inspect active multipart uploads.",
        long_about = "List active upload sessions or inspect one specific session to see committed parts, committed bytes, and manifest context."
    )]
    Uploads {
        #[command(subcommand)]
        command: UploadCommands,
    },

    #[command(
        about = "Control garbage collection.",
        long_about = "Start, stop, or watch the garbage collector. GC is responsible for trash expiry, orphan cleanup, and abandoned multipart cleanup."
    )]
    Gc {
        #[command(subcommand)]
        action: JobAction,
    },

    #[command(
        about = "Control repair jobs.",
        long_about = "Repair scans metadata and blob state, then repairs or reports corruption depending on the selected mode."
    )]
    Repair {
        #[command(subcommand)]
        action: RepairAction,
    },

    #[command(
        about = "Control redistribution jobs.",
        long_about = "Redistribution realigns placement after disk or policy changes. Use dry-run first when you want to understand the work before moving data."
    )]
    Redistribute {
        #[command(subcommand)]
        action: RedistributeAction,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum FragmentCommands {
    #[command(about = "Create a fragment with a caller-provided name and key.")]
    Create(FragmentCreateArgs),

    #[command(about = "Resize a fragment quota as an explicit admin operation.")]
    ResizeQuota(FragmentResizeQuotaArgs),
}

#[derive(Args, Debug, Clone)]
pub struct FragmentCreateArgs {
    #[arg(long, help = "Human-readable fragment name.")]
    pub name: String,

    #[arg(long, help = "Secret key that protects the fragment.")]
    pub key: String,

    #[arg(long, help = "Fragment quota in bytes.")]
    pub quota_bytes: u64,
}

#[derive(Args, Debug, Clone)]
pub struct FragmentResizeQuotaArgs {
    #[arg(long, help = "New fragment quota in bytes.")]
    pub quota_bytes: u64,

    #[arg(
        long,
        help = "N1 master secret authorizing the virtual partition resize."
    )]
    pub master_secret: String,
}

#[derive(Subcommand, Debug, Clone)]
pub enum UploadCommands {
    #[command(about = "List active upload sessions for the selected fragment.")]
    List {
        #[arg(
            long,
            default_value_t = 50,
            help = "Maximum number of sessions to return."
        )]
        limit: usize,
        #[arg(long, default_value_t = 0, help = "Pagination offset.")]
        offset: usize,
    },

    #[command(about = "Inspect one upload session by version id.")]
    Status {
        #[arg(long, help = "Version id of the multipart upload session.")]
        version_id: String,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum JobAction {
    #[command(about = "Start the job.")]
    Start,
    #[command(about = "Fetch the current job status once.")]
    Status,
    #[command(about = "Request the running job to stop.")]
    Stop,
    #[command(about = "Continuously watch job status using --poll-ms.")]
    Watch,
}

#[derive(Subcommand, Debug, Clone)]
pub enum RepairAction {
    #[command(about = "Start a repair job with the chosen mode and tunings.")]
    Start(RepairStartArgs),
    #[command(about = "Fetch current repair status once.")]
    Status,
    #[command(about = "Request the running repair job to stop.")]
    Stop,
    #[command(about = "Continuously watch repair status using --poll-ms.")]
    Watch,
}

#[derive(Args, Debug, Clone)]
pub struct RepairStartArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Scan only. Do not mutate data or metadata."
    )]
    pub dry_run: bool,

    #[arg(
        long,
        help = "Repair mode. availability is safest for live traffic; scrub is the deepest pass."
    )]
    pub mode: Option<RepairModeArg>,

    #[arg(
        long,
        default_value_t = false,
        help = "Faster pacing with less durability margin during the maintenance pass."
    )]
    pub fast_mode: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Legacy alias that promotes availability mode to read-only fast maintenance."
    )]
    pub read_only_fast: bool,

    #[arg(long, help = "Metadata concurrency override.")]
    pub meta_concurrency: Option<usize>,

    #[arg(long, help = "Repair worker concurrency override.")]
    pub repair_concurrency: Option<usize>,

    #[arg(
        long,
        help = "Batch size override for maintenance status flushing and work grouping."
    )]
    pub batch: Option<usize>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum RedistributeAction {
    #[command(about = "Start a redistribution job with the chosen scope and durability.")]
    Start(RedistributeStartArgs),
    #[command(about = "Fetch current redistribution status once.")]
    Status,
    #[command(about = "Request the running redistribution job to stop.")]
    Stop,
    #[command(about = "Continuously watch redistribution status using --poll-ms.")]
    Watch,
}

#[derive(Args, Debug, Clone)]
pub struct RedistributeStartArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Scan only. Do not move data or rewrite placement."
    )]
    pub dry_run: bool,

    #[arg(
        long,
        help = "Execution mode. strict and scrub tighten maintenance access during the run."
    )]
    pub mode: Option<ExecutionModeArg>,

    #[arg(
        long,
        default_value_t = false,
        help = "Faster pacing for redistribution work."
    )]
    pub fast_mode: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Legacy alias that promotes availability mode to read-only fast maintenance."
    )]
    pub read_only_fast: bool,

    #[arg(long, help = "Durability strategy for redistribution writes.")]
    pub durability: Option<DurabilityArg>,

    #[arg(
        long,
        help = "Limit redistribution to all data, blob only, or index only."
    )]
    pub scope: Option<RedistributeScopeArg>,

    #[arg(long, help = "Object concurrency override.")]
    pub object_concurrency: Option<usize>,

    #[arg(
        long,
        help = "How many objects to process before flushing status updates."
    )]
    pub status_batch: Option<u64>,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum RepairModeArg {
    Availability,
    Checksum,
    Strict,
    Scrub,
}

impl RepairModeArg {
    pub fn as_query_value(self) -> &'static str {
        match self {
            Self::Availability => "availability",
            Self::Checksum => "checksum",
            Self::Strict => "strict",
            Self::Scrub => "scrub",
        }
    }
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum ExecutionModeArg {
    Availability,
    Strict,
    Scrub,
}

impl ExecutionModeArg {
    pub fn as_query_value(self) -> &'static str {
        match self {
            Self::Availability => "availability",
            Self::Strict => "strict",
            Self::Scrub => "scrub",
        }
    }
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum DurabilityArg {
    Auto,
    Strict,
    Batched,
}

impl DurabilityArg {
    pub fn as_query_value(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Strict => "strict",
            Self::Batched => "batched",
        }
    }
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum RedistributeScopeArg {
    All,
    Blob,
    Index,
}

impl RedistributeScopeArg {
    pub fn as_query_value(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Blob => "blob",
            Self::Index => "index",
        }
    }
}
