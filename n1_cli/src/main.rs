mod args;
mod client;
mod output;

use clap::Parser;

use args::Cli;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(error) = client::run(cli).await {
        eprintln!("n1 cli error: {error}");
        std::process::exit(1);
    }
}
