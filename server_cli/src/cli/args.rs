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
    Health,
}
