use clap::{Args, Parser, Subcommand, ValueEnum};

const CLI_LONG_ABOUT: &str = "\
Operator-first shell for N1.

Use this shell to inspect health, track upload pressure, list namespace state,
and run maintenance jobs without guessing the protocol routes behind them.
When no HTTPS --url is provided, the shell prefers the native n1:// transport
from n1_config.ini or n1://127.0.0.1:50002.

N1 authentication is fragment-scoped. For protected commands, pass either
--fragment <name-or-hash> or --auth-fragment <name-or-hash>.";

const CLI_AFTER_HELP: &str = "\
Examples:
  litiaina-n1-shell --secret <secret> --fragment mainframe shell
  litiaina-n1-shell --secret <secret> --fragment mainframe health
  litiaina-n1-shell --secret <secret> --fragment mainframe namespace list --path docs
  litiaina-n1-shell --secret <secret> --fragment mainframe uploads list
  litiaina-n1-shell --secret <secret> --fragment mainframe repair --none --strict --fast-mode
  litiaina-n1-shell --secret <secret> --fragment mainframe repair --scrub
  litiaina-n1-shell --secret <secret> --fragment mainframe gc --none --scrub --fast-mode
  litiaina-n1-shell --secret <secret> --fragment mainframe redistribute --none --strict --durability strict
  litiaina-n1-shell --url https://n1.local:50001 --secret <secret> --fragment mainframe health";

#[derive(Parser, Debug)]
#[command(
    name = "litiaina-n1-shell",
    version,
    about = "Operator shell for N1",
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
        help = "Compatibility HTTPS API URL, for example https://127.0.0.1:50001. If omitted, the shell prefers native n1://."
    )]
    pub url: Option<String>,

    #[arg(
        long,
        global = true,
        help_heading = "Connection",
        help = "Native N1 URL, for example n1://127.0.0.1:50002. Overrides config/default native endpoint."
    )]
    pub native_url: Option<String>,

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
        about = "Open a stateful native N1 operator shell.",
        long_about = "Open a persistent n1:// session for monitoring, listing, object status/repair, GC, repair, redistribution, and quota checks."
    )]
    Shell,

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
        about = "Read namespace state.",
        long_about = "Read-only namespace commands for listing and stat. The shell intentionally does not expose namespace rename/delete/update operations."
    )]
    Namespace {
        #[command(subcommand)]
        command: NamespaceCommands,
    },

    #[command(
        about = "Inspect or repair a single object.",
        long_about = "Object status and repair commands. Repair is object-scoped and does not lock the whole fragment."
    )]
    Object {
        #[command(subcommand)]
        command: ObjectCommands,
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
        long_about = "Direct GC control. Use --none for whole-N1 GC or --fragment <name-or-hash> for one fragment. Status, stop, and watch are single flags."
    )]
    Gc(GcArgs),

    #[command(
        about = "Control repair jobs.",
        long_about = "Direct repair control. Use --none for whole-N1 repair or --fragment <name-or-hash> for one fragment. Mode flags are direct: --availability, --checksum, --strict, or --scrub."
    )]
    Repair(RepairArgs),

    #[command(
        about = "Control redistribution jobs.",
        long_about = "Direct redistribution control. Use --none for whole-N1 redistribution or --fragment <name-or-hash> for one fragment. Redistribution is for placement realignment after disk or policy changes."
    )]
    Redistribute(RedistributeArgs),
}

#[derive(Subcommand, Debug, Clone)]
pub enum FragmentCommands {
    #[command(about = "Create a fragment with a caller-provided name and key.")]
    Create(FragmentCreateArgs),

    #[command(about = "List fragments visible to the server.")]
    List,

    #[command(about = "Resolve a fragment name to its hash.")]
    Hash {
        #[arg(long, help = "Fragment name or hash to resolve.")]
        fragment: Option<String>,
    },

    #[command(about = "Show quota/usage for the authenticated fragment.")]
    Quota,

    #[command(about = "Resize a fragment quota as an explicit admin operation.")]
    ResizeQuota(FragmentResizeQuotaArgs),
}

#[derive(Subcommand, Debug, Clone)]
pub enum NamespaceCommands {
    #[command(about = "List one namespace directory.")]
    List {
        #[arg(long, default_value = "", help = "Namespace path to list.")]
        path: String,
        #[arg(long, default_value_t = 200, help = "Maximum entries to return.")]
        limit: usize,
    },

    #[command(about = "Stat one namespace path.")]
    Stat {
        #[arg(long, help = "Namespace path to inspect.")]
        path: String,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum ObjectCommands {
    #[command(about = "Read object metadata.")]
    Meta {
        #[arg(long, help = "Object key / namespace path.")]
        object_key: String,
        #[arg(long, help = "Optional exact version id.")]
        version_id: Option<String>,
    },

    #[command(about = "List committed versions for one object.")]
    Versions {
        #[arg(long, help = "Object key / namespace path.")]
        object_key: String,
    },

    #[command(about = "Check one object status.")]
    Status {
        #[arg(long, help = "Object key / namespace path.")]
        object_key: String,
        #[arg(
            long,
            default_value = "availability",
            help = "availability, checksum, strict, or scrub."
        )]
        mode: String,
    },

    #[command(about = "Repair one object.")]
    Repair {
        #[arg(long, help = "Object key / namespace path.")]
        object_key: String,
        #[arg(
            long,
            default_value = "strict",
            help = "availability, checksum, strict, or scrub."
        )]
        mode: String,
    },
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

#[derive(Args, Debug, Clone)]
pub struct GcArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Run GC across all N1 fragments."
    )]
    pub none: bool,

    #[arg(long, default_value_t = false, help = "Fetch current GC status once.")]
    pub status: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Request the running GC job to stop."
    )]
    pub stop: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Continuously watch GC status using --poll-ms."
    )]
    pub watch: bool,

    #[arg(long, default_value_t = false, help = "Use availability mode.")]
    pub availability: bool,

    #[arg(long, default_value_t = false, help = "Use strict maintenance mode.")]
    pub strict: bool,

    #[arg(long, default_value_t = false, help = "Use scrub maintenance mode.")]
    pub scrub: bool,

    #[arg(long, default_value_t = false, help = "Use fast-mode GC timing.")]
    pub fast_mode: bool,
}

#[derive(Args, Debug, Clone)]
pub struct RepairArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Repair across all N1 fragments."
    )]
    pub none: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Fetch current repair status once."
    )]
    pub status: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Request the running repair job to stop."
    )]
    pub stop: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Continuously watch repair status using --poll-ms."
    )]
    pub watch: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Scan only. Do not mutate data or metadata."
    )]
    pub dry_run: bool,

    #[arg(long, default_value_t = false, help = "Use availability mode.")]
    pub availability: bool,

    #[arg(long, default_value_t = false, help = "Use checksum mode.")]
    pub checksum: bool,

    #[arg(long, default_value_t = false, help = "Use strict mode.")]
    pub strict: bool,

    #[arg(long, default_value_t = false, help = "Use scrub mode.")]
    pub scrub: bool,

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

#[derive(Args, Debug, Clone)]
pub struct RedistributeArgs {
    #[arg(
        long,
        default_value_t = false,
        help = "Redistribute across all N1 fragments."
    )]
    pub none: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Fetch current redistribution status once."
    )]
    pub status: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Request the running redistribution job to stop."
    )]
    pub stop: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Continuously watch redistribution status using --poll-ms."
    )]
    pub watch: bool,

    #[arg(
        long,
        default_value_t = false,
        help = "Scan only. Do not move data or rewrite placement."
    )]
    pub dry_run: bool,

    #[arg(long, default_value_t = false, help = "Use availability mode.")]
    pub availability: bool,

    #[arg(long, default_value_t = false, help = "Use strict mode.")]
    pub strict: bool,

    #[arg(long, default_value_t = false, help = "Use scrub mode.")]
    pub scrub: bool,

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
