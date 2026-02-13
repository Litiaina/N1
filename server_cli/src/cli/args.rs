use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "Litiaina N1 CLI")]
#[command(version, about = "Litiaina N1 Storage Management Interface", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
    #[arg(short, long, global = true)]
    pub secret: Option<String>,
    #[arg(short, long, global = true, default_value = "admin")]
    pub fragment: String,
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
pub enum JobAction {
    Start,
    Status,
    Stop,
    Watch,
}

#[derive(Subcommand, Debug, Clone)]
pub enum OpsCommands {
    Gc {
        #[command(subcommand)]
        action: JobAction,
    },
    Repair {
        #[command(subcommand)]
        action: JobAction,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    Redistribute {
        #[command(subcommand)]
        action: JobAction,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    PrefixRebuild {
        #[command(subcommand)]
        action: JobAction,
        #[arg(long)]
        fragment: Option<String>,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    Server,
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
    },
    Redistribute {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    PrefixRebuild,
    Health {
        #[arg(long, default_value_t = false)]
        watch: bool,
        #[arg(long, default_value_t = false)]
        deep_scan: bool,
    },
    Ops {
        #[command(subcommand)]
        cmd: OpsCommands,
    },
}
