use axum_server::Handle;
use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt};
use tokio::sync::Mutex;
use tracing::info;

use crate::cli::commands::handle_command;
use crate::cli::parser::CliCommand;

pub async fn start_cli<F, Fut>(
    shutdown_handle: Arc<Mutex<Option<(Arc<Handle>, tokio::task::JoinHandle<()>)>>>,
    clean_callback: F,
) 
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    info!("Type /help to see available commands.");
    print_prompt();

    while let Ok(Some(line)) = lines.next_line().await {
        let cmd = CliCommand::parse(&line);
        let keep_running = handle_command(cmd, Arc::clone(&shutdown_handle), &clean_callback).await;

        if !keep_running {
            break;
        }

        print_prompt();
    }

    info!("CLI closed.");
}

fn print_prompt() {
    use std::io::Write;
    std::io::stdout().flush().unwrap();
}
