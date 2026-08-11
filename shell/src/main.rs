mod args;
mod client;
mod install;
mod native;
mod output;
mod web;

use clap::Parser;

use args::Cli;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let result = match &cli.command {
        args::Commands::Serve(arguments) => web::run(arguments.clone()).await,
        _ => client::run(cli).await,
    };
    if let Err(error) = result {
        eprintln!("n1 shell error: {error}");
        std::process::exit(1);
    }
}
