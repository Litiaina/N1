use crate::fs::cleanup_manager::interval_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::handlers::error_handlers::handler_404;
use crate::object_storage::upload::{DATA_SHARDS_ATOMIC, MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC, PARITY_SHARDS_ATOMIC, WRITER_BUFFER_SIZE_ATOMIC};
use crate::routes::storage_routes::serve_storage_routes;
use crate::structs::object_storage_structs::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
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
use std::sync::atomic::{AtomicU64, AtomicUsize};
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

            if let Err(_) = result {
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

    let disk_section = config.section(Some("disk_config"));

    let fs_storage_path = disk_section
        .and_then(|s| s.get("fs_storage_path"))
        .unwrap_or("fs-storage-path");

    FS_STORAGE_ROOT
        .set(fs_storage_path.to_string())
        .expect("Double initialization of STORAGE_ROOT!");

    let index_store_str = disk_section
        .and_then(|s| s.get("index_store_disks"))
        .expect("Missing index_store_disks in [disk_config]");

    let index_disks: Vec<String> = index_store_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if index_disks.is_empty() {
        panic!("At least one index store disk must be configured in index_store_disks!");
    }

    INDEX_STORE_DISKS
        .set(index_disks)
        .expect("Double initialization of INDEX_STORE_DISKS!");

    let blob_store_str = disk_section
        .and_then(|s| s.get("blob_store_disks"))
        .expect("Missing blob_store_disks in [disk_config]");

    let blob_disks: Vec<String> = blob_store_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if blob_disks.is_empty() {
        panic!("At least one blob store disk must be configured in blob_store_disks!");
    }

    BLOB_STORE_DISKS
        .set(blob_disks)
        .expect("Double initialization of BLOB_STORE_DISKS!");

    let upload_section = config.section(Some("upload_config"));

    let max_io_semaphore = upload_section
        .and_then(|s| s.get("max_concurrent_io"))
        .unwrap_or("16384");

    let max_io_semaphore: usize = max_io_semaphore
        .parse()
        .expect("max_concurrent_io does not fit into usize");

    MAX_IO_SEMAPHORE
        .set(max_io_semaphore)
        .expect("Double initialization of MAX_IO_SEMAPHORE");

    let max_part_size = upload_section
        .and_then(|s| s.get("max_part_size"))
        .unwrap_or("52428800");

    let max_part_size: AtomicU64 = AtomicU64::new(
        max_part_size
            .parse::<u64>()
            .expect("max_part_size does not fit into u64")
    );
    
    MAX_PART_SIZE_ATOMIC
        .set(max_part_size)
        .expect("Double initialization of MAX_PART_SIZE!");
    
    let data_shards = upload_section
        .and_then(|s| s.get("data_shards"))
        .unwrap_or("1");

    let data_shards: AtomicUsize = AtomicUsize::new(
        data_shards
            .parse::<usize>()
            .expect("data_shards does not fit into usize")
    );

    DATA_SHARDS_ATOMIC
        .set(data_shards)
        .expect("Double initialization of DATA_SHARDS!");
    
    let parity_shards = upload_section
        .and_then(|s| s.get("parity_shards"))
        .unwrap_or("0");

    let parity_shards: AtomicUsize = AtomicUsize::new(
        parity_shards
            .parse::<usize>()
            .expect("parity_shards does not fit into usize")
    );

    PARITY_SHARDS_ATOMIC
        .set(parity_shards)
        .expect("Double initialization of PARITY_SHARDS!");

    let writer_buffer_size = upload_section
        .and_then(|s| s.get("writer_buffer_size"))
        .unwrap_or("262144");

    let writer_buffer_size: AtomicUsize = AtomicUsize::new(
        writer_buffer_size
            .parse::<usize>()
            .expect("writer_buffer_size does not fit into usize")
    );
    
    WRITER_BUFFER_SIZE_ATOMIC
        .set(writer_buffer_size)
        .expect("Double initialization of WRITER_BUFFER_SIZE!");

    let cert = RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("Failed to load TLS certificates");

    let storage_addr = SocketAddr::from((ip, https_port));
    let storage_router = serve_storage_routes().await;
    let serve_storage = storage_router.fallback(handler_404);

    let fs_root = FS_STORAGE_ROOT.get().unwrap_or(&STORAGE_ROOT.to_string()).to_string();
    tokio::spawn(async move {
        interval_cleanup(&fs_root).await;
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
