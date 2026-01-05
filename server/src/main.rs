use crate::fs::cleanup_manager::{interval_cleanup, start_cleanup};
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::handlers::error_handlers::handler_404;
use crate::object_storage::garbage_collector::run_garbage_collector;
use crate::object_storage::sync::sync_quota;
use crate::routes::storage_routes::serve_storage_routes;
use crate::structs::object_storage_structs::OBJECT_STORAGE_ROOT;
use crate::utils::date_time_formatter::LocalTimer;
use crate::validation::enforce_init_settings::{ensure_config_file, ensure_env_file};
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use chrono::Local;
use dotenv::dotenv;
use ini::Ini;
use server_cli::{self, server_login, start_cli};
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
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

pub fn init_tracing() -> Result<(), tracing_subscriber::util::TryInitError> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info,litiaina_n1=trace,server_cli=trace,axum=info,hyper=info".into()
            }),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_target(true)
                .with_ansi(true)
                .with_timer(LocalTimer),
        )
        .try_init()
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install 'ring' as the default TLS provider.");

    init_tracing().expect("Failed to initialize tracing subscriber");

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

    let bypass_login = env::var("ENABLE_LOCK").unwrap_or("true".to_string()) == "false";
    let user = if bypass_login {
        tracing::warn!("SECURITY LOCK BYPASSED: ENABLE_LOCK is false in .env");
        "unknown".to_string()
    } else {
        loop {
            match server_login().await {
                Ok(access_id) => break access_id,
                Err(e) => {
                    tracing::error!("Login failed: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    };

    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    tracing::info!("╔══════════════════════════════════════╗");
    tracing::info!("║         L I T I A I N A  N 1         ║");
    tracing::info!("╚══════════════════════════════════════╝");
    tracing::info!("│ Status         : ONLINE");
    tracing::info!("│ Start Time     : {}", current_time);
    tracing::info!("│ Administrator  : {}", user);
    tracing::info!("│ PID            : {}", std::process::id());
    tracing::info!("│ Hostname       : {}", hostname::get().unwrap_or_default().to_string_lossy());
    tracing::info!("│ Working Dir    : {}", std::env::current_dir().unwrap_or_default().display());
    tracing::info!("│ Command        : /exit to terminate the session");
    tracing::info!("╞════════════════════════ ACTIVE CONNECTIONS ════════════════════════╡");

    run_server().await
}

pub async fn run_server() {
    tokio::spawn(async {
        interval_cleanup(STORAGE_ROOT).await;
    });

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

    let storage_path = config
        .section(Some("disk_config"))
        .and_then(|s| s.get("storage_path"))
        .unwrap_or("object-storage-array");

    OBJECT_STORAGE_ROOT
        .set(storage_path.to_string())
        .expect("Double initialization of config!");

    let cert = RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("Failed to load TLS certificates");

    let storage_addr = SocketAddr::from((ip, https_port));
    let (storage_router, app_state) = serve_storage_routes().await;
    let serve_storage = storage_router.fallback(handler_404);

    let shutdown_handle: Arc<
        Mutex<Option<(Arc<axum_server::Handle<SocketAddr>>, tokio::task::JoinHandle<()>)>>,
    > = Arc::new(Mutex::new(None));

    let shutdown_handle_clone = Arc::clone(&shutdown_handle);

    sync_quota()
        .await
        .expect("Failed to setup database locks");

    {
        tokio::spawn(async move {
            let (handle, join) = serve(storage_addr, serve_storage, cert).await;
            let mut lock = shutdown_handle_clone.lock().await;
            *lock = Some((handle, join));
        });
    }

    {
        let app_state = Arc::clone(&app_state);
        tokio::spawn(async move {
            start_cli(shutdown_handle, move || {
                let app_state = Arc::clone(&app_state);
                async move {
                    run_garbage_collector(app_state).await;
                    start_cleanup(STORAGE_ROOT).await;
                }
            })
            .await;
        });
    }

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for CTRL+C");
}

async fn serve(
    socket: SocketAddr,
    app: Router,
    cert: RustlsConfig,
) -> (Arc<axum_server::Handle<SocketAddr>>, tokio::task::JoinHandle<()>) {
    tracing::info!("Secured port => {} type: https", socket);

    let handle = Arc::new(axum_server::Handle::new());

    let mut server = axum_server::bind_rustls(socket, cert).handle((*handle).clone());
    server.http_builder().http2().enable_connect_protocol();

    let join = tokio::spawn(async move {
        server
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .unwrap();
    });

    (handle, join)
}

pub async fn shutdown_signal(handle: Arc<axum_server::Handle<SocketAddr>>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install signal handler");

        terminate.recv().await;
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c event");
    }

    handle.graceful_shutdown(Some(Duration::from_secs(10)));
}
