use crate::fs::cleanup_manager::interval_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::handlers::error_handlers::handler_404;
use crate::object_storage::sync::sync_quota;
use crate::routes::storage_routes::serve_storage_routes;
use crate::structs::object_storage_structs::{FS_STORAGE_ROOT, OBJECT_STORAGE_ROOT};
use crate::utils::date_time_formatter::LocalTimer;
use crate::validation::enforce_init_settings::{ensure_config_file, ensure_env_file};
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use chrono::Local;
use clap::Parser;
use dotenv::dotenv;
use ini::Ini;
use server_cli::cli::args::{Cli, Commands};
use server_cli::cli::client::execute_command;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, fmt};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod database;
mod enums;
mod fs;
mod handlers;
mod middleware;
mod object_storage;
mod routes;
mod settings;
mod statics;
mod structs;
mod utils;
mod validation;
mod websocket;

pub fn init_tracing() -> Result<WorkerGuard, Box<dyn std::error::Error>> {
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());

    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info,litiaina_n1=trace,server_cli=trace,axum=info,hyper=info".into()
            }),
        )
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_target(false)
                .with_file(false)
                .with_line_number(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_ansi(true)
                .with_timer(LocalTimer)
        )
        .try_init()?;

    Ok(guard)
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install 'ring' as the default TLS provider.");

    let _guard = init_tracing().expect("Failed to initialize tracing subscriber");

    let args = Cli::parse();

    match args.command {
        Some(cmd) if matches!(cmd, Commands::Gc | Commands::Sync) => {
            dotenv().ok();

            let target_url = args.url.or_else(|| {
                Ini::load_from_file("config.ini").ok().and_then(|conf| {
                    conf.section(Some("server")).map(|section| {
                        let host = section.get("host").unwrap_or("127.0.0.1");
                        let port = section.get("https_port").unwrap_or("50001");
                        let effective_host = if host == "0.0.0.0" { "127.0.0.1" } else { host };
                        format!("https://{}:{}", effective_host, port)
                    })
                })
            });
            
            let result = execute_command(
                &cmd,          
                args.email,    
                args.password, 
                target_url,   
                args.insecure  
            ).await;

            drop(_guard);

            if let Err(e) = result {
                eprintln!("CLI Execution Error: {}", e);
                std::process::exit(1);
            }
            return;
        },
        _ => {}
    }

    if let Err(e) = ensure_env_file() {
        tracing::error!("Failed to ensure .env file: {}", e);
        return;
    }

    dotenv().ok();
    tracing::info!("Loaded environment variables from .env file.");

    if let Err(e) = ensure_config_file() {
        tracing::error!("Failed to ensure config file: {}", e);
        return;
    }

    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    tracing::info!("╔══════════════════════════════════════╗");
    tracing::info!("║         L I T I A I N A  N 1         ║");
    tracing::info!("╚══════════════════════════════════════╝");
    tracing::info!("│ Version        : {}", env!("CARGO_PKG_VERSION"));
    tracing::info!("│ Status         : ONLINE");
    tracing::info!("│ Start Time     : {}", current_time);
    tracing::info!("│ PID            : {}", std::process::id());
    tracing::info!("│ Hostname       : {}", hostname::get().unwrap_or_default().to_string_lossy());
    tracing::info!("│ Working Dir    : {}", std::env::current_dir().unwrap_or_default().display());
    tracing::info!("╞═════════ ACTIVE CONNECTIONS ═════════╡");

    run_server().await
}

pub async fn run_server() {
    let config = Ini::load_from_file("config.ini").expect("Failed to load config.ini");

    let server_section = config.section(Some("server")).unwrap_or_else(|| {
        panic!("Missing [server] section in config.ini");
    });

    let host = server_section.get("host").unwrap_or("127.0.0.1");

    let ip: IpAddr = IpAddr::from_str(host).unwrap_or_else(|_| {
        panic!("Invalid host IP address in config.ini: {}", host);
    });

    let https_port = server_section
        .get("https_port")
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(50001);

    let cert_path = server_section
        .get("cert_path")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "cert.pem".to_string());

    let key_path = server_section
        .get("key_path")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "key.pem".to_string());

    let object_storage_path = config
        .section(Some("disk_config"))
        .and_then(|s| s.get("object_storage_path"))
        .unwrap_or("object-storage-path");

    let fs_storage_path = config
        .section(Some("disk_config"))
        .and_then(|s| s.get("fs_storage_path"))
        .unwrap_or("fs-storage-path");

    OBJECT_STORAGE_ROOT
        .set(object_storage_path.to_string())
        .expect("Double initialization of config!");

    FS_STORAGE_ROOT
        .set(fs_storage_path.to_string())
        .expect("Double initialization of STORAGE_ROOT!");

    let cert = RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("Failed to load TLS certificates");

    let storage_addr = SocketAddr::from((ip, https_port));
    let (storage_router, app_state) = serve_storage_routes().await;
    let serve_storage = storage_router.fallback(handler_404);

    let fs_root = FS_STORAGE_ROOT.get().unwrap_or(&STORAGE_ROOT.to_string()).to_string();
    tokio::spawn(async move {
        interval_cleanup(&fs_root).await;
    });

    let db_client = Arc::clone(&app_state.db_client);
    tokio::spawn(async move {
        if let Err(e) = sync_quota(db_client).await {
            tracing::error!("Failed to setup database locks/quota: {}", e);
        }
    });

    serve(storage_addr, serve_storage, cert).await;
}

async fn serve(
    socket: SocketAddr,
    app: Router,
    cert: RustlsConfig,
) {
    tracing::info!("Secured port => {} type: https", socket);
    let handle = Arc::new(axum_server::Handle::new());
    
    tokio::spawn(shutdown_signal(handle.clone()));
    
    let mut server = axum_server::bind_rustls(socket, cert).handle((*handle).clone());
    server.http_builder().http2().enable_connect_protocol();

    server
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .unwrap();
}

pub async fn shutdown_signal(handle: Arc<axum_server::Handle<SocketAddr>>) {
    #[cfg(unix)]
    let terminator = async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut interrupt = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        
        tokio::select! {
            _ = terminate.recv() => {},
            _ = interrupt.recv() => {},
        }
    };

    #[cfg(not(unix))]
    let terminator = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c event");
    };

    terminator.await;
    
    tracing::info!("Shutdown signal received. Stopping server...");
    handle.graceful_shutdown(Some(Duration::from_secs(10)));
}
