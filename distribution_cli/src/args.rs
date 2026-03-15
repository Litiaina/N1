use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "litiaina-n2-cli")]
#[command(version, about = "Dedicated admin CLI for N2 distribution runtime")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    #[arg(short, long, global = true)]
    pub secret: Option<String>,
    #[arg(short, long, global = true)]
    pub fragment: Option<String>,
    #[arg(long, global = true)]
    pub auth_fragment: Option<String>,
    #[arg(long, global = true)]
    pub url: Option<String>,
    #[arg(long, global = true, default_value_t = true)]
    pub insecure: bool,
    #[arg(long, global = true, default_value_t = 1000)]
    pub poll_ms: u64,
    #[arg(long, global = true, default_value_t = false)]
    pub json: bool,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    Fragment {
        #[command(subcommand)]
        command: FragmentCommands,
    },
    Health {
        #[arg(long, default_value_t = false)]
        watch: bool,
        #[arg(long, default_value_t = false)]
        deep_scan: bool,
    },
    Uploads {
        #[command(subcommand)]
        command: UploadCommands,
    },
    Gc {
        #[command(subcommand)]
        action: JobAction,
    },
    Repair {
        #[command(subcommand)]
        action: RepairAction,
    },
    Redistribute {
        #[command(subcommand)]
        action: RedistributeAction,
    },
    PrefixRebuild {
        #[command(subcommand)]
        action: PrefixRebuildAction,
    },
    ClusterView {
        #[arg(long, default_value_t = false)]
        public: bool,
        #[arg(long)]
        expected_epoch: Option<u64>,
    },
    Membership {
        #[command(subcommand)]
        action: MembershipAction,
    },
    Placement(PlacementArgs),
}

#[derive(Subcommand, Debug, Clone)]
pub enum FragmentCommands {
    Create(FragmentCreateArgs),
}

#[derive(Args, Debug, Clone)]
pub struct FragmentCreateArgs {
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub key: String,
}

#[derive(Subcommand, Debug, Clone)]
pub enum UploadCommands {
    List {
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long, default_value_t = 0)]
        offset: usize,
    },
    Status {
        #[arg(long)]
        version_id: String,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum JobAction {
    Start,
    Status,
    Stop,
    Watch,
}

#[derive(Subcommand, Debug, Clone)]
pub enum RepairAction {
    Start(RepairStartArgs),
    Status,
    Stop,
    Watch,
}

#[derive(Args, Debug, Clone)]
pub struct RepairStartArgs {
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long)]
    pub mode: Option<RepairModeArg>,
    #[arg(long, default_value_t = false)]
    pub fast_mode: bool,
    #[arg(long, default_value_t = false)]
    pub read_only_fast: bool,
    #[arg(long)]
    pub meta_concurrency: Option<usize>,
    #[arg(long)]
    pub repair_concurrency: Option<usize>,
    #[arg(long)]
    pub batch: Option<usize>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum RedistributeAction {
    Start(RedistributeStartArgs),
    Status,
    Stop,
    Watch,
}

#[derive(Args, Debug, Clone)]
pub struct RedistributeStartArgs {
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long)]
    pub mode: Option<ExecutionModeArg>,
    #[arg(long, default_value_t = false)]
    pub fast_mode: bool,
    #[arg(long, default_value_t = false)]
    pub read_only_fast: bool,
    #[arg(long)]
    pub durability: Option<DurabilityArg>,
    #[arg(long)]
    pub scope: Option<RedistributeScopeArg>,
    #[arg(long)]
    pub object_concurrency: Option<usize>,
    #[arg(long)]
    pub status_batch: Option<u64>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum PrefixRebuildAction {
    Start {
        #[arg(long)]
        fragment: Option<String>,
    },
    Status,
    Stop,
    Watch,
}

#[derive(Subcommand, Debug, Clone)]
pub enum MembershipAction {
    Get,
    Apply {
        #[arg(long)]
        file: String,
    },
    Rollback {
        #[arg(long)]
        epoch: u64,
    },
}

#[derive(Args, Debug, Clone)]
pub struct PlacementArgs {
    #[arg(long)]
    pub fragment: String,
    #[arg(long)]
    pub object_key: String,
    #[arg(long)]
    pub uid: Option<String>,
    #[arg(long)]
    pub version_id: Option<String>,
    #[arg(long)]
    pub kc: Option<u8>,
    #[arg(long)]
    pub mc: Option<u8>,
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
