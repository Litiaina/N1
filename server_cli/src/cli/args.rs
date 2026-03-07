use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "Litiaina N1 CLI")]
#[command(version, about = "Litiaina N1 Storage Management Interface", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
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
    #[arg(long, global = true, default_value_t = 500)]
    pub poll_ms: u64,
    #[arg(long, global = true, default_value_t = false)]
    pub json: bool,
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
pub enum OpsGcAction {
    Start,
    Status,
    Stop,
    Watch,
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

#[derive(Subcommand, Debug, Clone)]
pub enum OpsRepairAction {
    Start {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long)]
        mode: Option<RepairModeArg>,
        #[arg(long, default_value_t = false)]
        fast_mode: bool,
        #[arg(long, default_value_t = false)]
        read_only_fast: bool,
        #[arg(long)]
        meta_concurrency: Option<usize>,
        #[arg(long)]
        repair_concurrency: Option<usize>,
        #[arg(long)]
        batch: Option<usize>,
    },
    Status,
    Stop,
    Watch,
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

#[derive(Subcommand, Debug, Clone)]
pub enum OpsRedistributeAction {
    Start {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long)]
        mode: Option<ExecutionModeArg>,
        #[arg(long, default_value_t = false)]
        fast_mode: bool,
        #[arg(long, default_value_t = false)]
        read_only_fast: bool,
        #[arg(long)]
        durability: Option<DurabilityArg>,
        #[arg(long)]
        scope: Option<RedistributeScopeArg>,
        #[arg(long)]
        object_concurrency: Option<usize>,
        #[arg(long)]
        status_batch: Option<u64>,
    },
    Status,
    Stop,
    Watch,
}

#[derive(Subcommand, Debug, Clone)]
pub enum OpsPrefixRebuildAction {
    Start {
        #[arg(long)]
        fragment: Option<String>,
    },
    Status,
    Stop,
    Watch,
}

#[derive(Subcommand, Debug, Clone)]
pub enum OpsCommands {
    Gc {
        #[command(subcommand)]
        action: OpsGcAction,
    },
    Repair {
        #[command(subcommand)]
        action: OpsRepairAction,
    },
    Redistribute {
        #[command(subcommand)]
        action: OpsRedistributeAction,
    },
    PrefixRebuild {
        #[command(subcommand)]
        action: OpsPrefixRebuildAction,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    Server,
    FormatBlobDevice {
        #[arg(long)]
        path: String,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value_t = false)]
        prepare: bool,
        #[arg(long)]
        nonce: Option<String>,
        #[arg(long)]
        fingerprint: Option<String>,
    },
    FormatIndexDevice {
        #[arg(long)]
        path: String,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value_t = false)]
        prepare: bool,
        #[arg(long)]
        nonce: Option<String>,
        #[arg(long)]
        fingerprint: Option<String>,
    },
    CreateFragment {
        #[arg(short, long)]
        name: String,
        #[arg(short, long, default_value_t = 10_737_418_240)]
        quota: u64,
    },
    Gc,
    Repair {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long)]
        mode: Option<RepairModeArg>,
        #[arg(long, default_value_t = false)]
        fast_mode: bool,
        #[arg(long, default_value_t = false)]
        read_only_fast: bool,
        #[arg(long)]
        meta_concurrency: Option<usize>,
        #[arg(long)]
        repair_concurrency: Option<usize>,
        #[arg(long)]
        batch: Option<usize>,
    },
    Redistribute {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long)]
        mode: Option<ExecutionModeArg>,
        #[arg(long, default_value_t = false)]
        fast_mode: bool,
        #[arg(long, default_value_t = false)]
        read_only_fast: bool,
        #[arg(long)]
        durability: Option<DurabilityArg>,
        #[arg(long)]
        scope: Option<RedistributeScopeArg>,
        #[arg(long)]
        object_concurrency: Option<usize>,
        #[arg(long)]
        status_batch: Option<u64>,
    },
    PrefixRebuild,
    Health {
        #[arg(long, default_value_t = false)]
        watch: bool,
        #[arg(long, default_value_t = false)]
        deep_scan: bool,
    },
    Uploads {
        #[command(subcommand)]
        cmd: UploadCommands,
    },
    Ops {
        #[command(subcommand)]
        cmd: OpsCommands,
    },
}
