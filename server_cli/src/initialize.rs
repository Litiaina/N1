use axum_server::Handle;
use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt};
use tokio::sync::Mutex;
use tracing::info;

use crate::commands::handle_command;
use crate::parser::CliCommand;

pub async fn start_cli(
    shutdown_handle: Arc<Mutex<Option<(Arc<Handle>, tokio::task::JoinHandle<()>)>>>,
) {
    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    info!("Type /help to see available commands.");

    print_prompt();

    while let Ok(Some(line)) = lines.next_line().await {
        let cmd = CliCommand::parse(&line);
        let keep_running = handle_command(cmd, Arc::clone(&shutdown_handle)).await;

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
