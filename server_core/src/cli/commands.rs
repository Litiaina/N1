use axum_server::Handle;
use std::{process, sync::Arc, time::Duration};
use sysinfo::{Pid, System};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::cli::parser::{CliCommand, clear_screen};

pub async fn handle_command<F, Fut>(
    cmd: CliCommand,
    shutdown_handle: Arc<Mutex<Option<(Arc<Handle>, JoinHandle<()>)>>>,
    clean_callback: F,
) -> bool
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = ()> + Send,
{
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

            false
        }
        CliCommand::Status => {
            let mut sys = System::new_all();
            sys.refresh_all();

            let pid = Pid::from(std::process::id() as usize);

            if let Some(process) = sys.process(pid) {
                let disk = process.disk_usage();
                let uptime_secs = process.run_time();
                let hours = uptime_secs / 3600;
                let minutes = (uptime_secs % 3600) / 60;
                let seconds = uptime_secs % 60;
                let uptime_str = format!("{:02}h {:02}m {:02}s", hours, minutes, seconds);

                info!("╔═════════════════════════════╗");
                info!("║         S T A T U S         ║");
                info!("╚═════════════════════════════╝");
                info!("│ Status             : ONLINE");
                info!("│ Uptime             : {}", uptime_str);
                info!("│ CPU usage          : {:.2} %", process.cpu_usage());
                info!("│ Disk usage (read)  : {} bytes", disk.read_bytes);
                info!("│ Disk usage (write) : {} bytes", disk.written_bytes);
                info!("│ Disk usage (total) : {} bytes",disk.total_read_bytes + disk.total_written_bytes);
                info!("│ Memory usage       : {} bytes", process.memory());
                info!("│ PID                : {}", std::process::id());
                info!("│ Hostname           : {}",hostname::get().unwrap_or_default().to_string_lossy());
                info!("│ Working Dir        : {}",std::env::current_dir().unwrap_or_default().display());
            } else {
                error!("Process not found!");
            }

            true
        }
        CliCommand::Help => {
            info!("╔═════════════════════════════════╗");
            info!("║         C O M M A N D S         ║");
            info!("╚═════════════════════════════════╝");
            info!("• /help    → Show this help menu");
            info!("• /status  → Display server status");
            info!("• /exit    → Shut down the server");
            info!("• /clear   → Clear screen");
            info!("• /clean   → Clean unfinished object and chunks");

            true
        }
        CliCommand::Clear => {
            clear_screen();
            true
        }
        CliCommand::Clean => {
            info!("╔═══════════════════════════════╗");
            info!("║         C L E A N E R         ║");
            info!("╚═══════════════════════════════╝");

            clean_callback().await;

            true
        }
        CliCommand::Unknown(cmd) => {
            info!("Unknown command: {} (try /help)", cmd);
            true
        }
    }
}
