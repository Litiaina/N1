use axum_server::Handle;
use std::{process, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::parser::CliCommand;

pub async fn handle_command(
    cmd: CliCommand,
    shutdown_handle: Arc<Mutex<Option<(Arc<Handle>, JoinHandle<()>)>>>,
) -> bool {
    match cmd {
        CliCommand::Exit => {
            info!("Shutdown requested via CLI");
            let maybe_pair = {
                let mut lock = shutdown_handle.lock().await;
                lock.take()
            };

            if let Some((handle, join)) = maybe_pair {
                handle.graceful_shutdown(Some(Duration::from_secs(10)));
                info!("Waiting for server to shut down...");

                match join.await {
                    Ok(_) => {
                        info!("╔═════════════════════════════════════════════════════╗");
                        info!("║         S E R V E R  H A S  S H U T D O W N         ║");
                        info!("╚═════════════════════════════════════════════════════╝");
                    }
                    Err(e) => error!("Server task ended with error: {:?}", e),
                }

                process::exit(0);
            } else {
                warn!("No active server handle (already shutting down?)");
            }
            return false;
        }

        CliCommand::Status => {
            info!("╔═════════════════════════════╗");
            info!("║         S T A T U S         ║");
            info!("╚═════════════════════════════╝");
            //The implementation must ping microservices to check their availability and health status.
        }

        CliCommand::Help => {
            info!("╔═════════════════════════════════╗");
            info!("║         C O M M A N D S         ║");
            info!("╚═════════════════════════════════╝");
            info!("• /help    → Show this help menu");
            info!("• /status  → Display server status");
            info!("• /exit    → Shut down the server");
        }

        CliCommand::Unknown(cmd) => {
            info!("Unknown command: {} (try /help)", cmd);
        }
    }

    true
}
