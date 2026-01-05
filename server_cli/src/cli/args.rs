use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "Litiaina N1")]
#[command(version, about = "Litiaina Cloud Server & CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    #[arg(short, long, global = true)]
    pub email: Option<String>,

    #[arg(short, long, global = true)]
    pub password: Option<String>,

    #[arg(long, global = true)]
    pub url: Option<String>,

    #[arg(long, global = true, default_value_t = true)]
    pub insecure: bool,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    Server,
    Gc,
    Sync,
}
