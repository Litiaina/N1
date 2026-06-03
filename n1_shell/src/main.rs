mod args;
mod client;
mod native;
mod output;

use clap::Parser;

use args::Cli;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(error) = client::run(cli).await {
        eprintln!("n1 shell error: {error}");
        std::process::exit(1);
    }
}
