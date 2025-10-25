use crate::fs::cleanup_manager::{interval_cleanup, start_cleanup};
use crate::fs::fs_constants::{CONFIG_FILE, ENV_FILE, STORAGE_ROOT};
use crate::handlers::error_handlers::handler_404;
use crate::object_storage::garbage_collector::run_garbage_collector;
use crate::object_storage::setup::setup_database_locks;
use crate::routes::storage_routes::{AppState, serve_storage_routes};
use crate::settings::create_config::create_default_config_ini;
use crate::settings::create_env::create_env_file;
use crate::statics::database_statics::DATABASE_URL;
use crate::structs::port_struct::Ports;
use crate::utils::date_time_formatter::LocalTimer;
use axum::handler::HandlerWithoutStateExt;
use axum::http::uri::Authority;
use axum::http::{StatusCode, Uri};
use axum::response::Redirect;
use axum::{BoxError, Router};
use axum_extra::extract::Host;
use axum_server::tls_rustls::RustlsConfig;
use chrono::Local;
use dotenv::dotenv;
use ini::Ini;
use mongodb::Client;
use mongodb::options::ClientOptions;
use server_core::{self, server_login, start_cli};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
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

pub fn init_tracing() -> Result<(), tracing_subscriber::util::TryInitError> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info,litiaina_n2=trace,server_cli=trace,axum=info,hyper=info".into()
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

    if !Path::new(ENV_FILE).exists() {
        match create_env_file(ENV_FILE) {
            Ok(_) => {
                tracing::info!(
                    "Created .env file. Please configure it with your environment variables."
                );
            }
            Err(e) => {
                eprintln!("Failed to create .env file: {}", e);
                return;
            }
        }
    } else {
        dotenv().ok();
        tracing::info!("Loaded environment variables from .env file.");
    }

    if !Path::new(CONFIG_FILE).exists() {
        match create_default_config_ini(CONFIG_FILE) {
            Ok(_) => {
                tracing::info!(
                    "Created default '{}' file with standard settings.",
                    CONFIG_FILE
                );
            }
            Err(e) => {
                tracing::error!("FATAL: Failed to create '{}' file: {}", CONFIG_FILE, e);
                return;
            }
        }
    } else {
        tracing::info!("Loaded configuration from '{}' file.", CONFIG_FILE);
    }

    let user = loop {
        match server_login().await {
            Ok(access_id) => break access_id,
            Err(e) => tracing::error!("{}", e),
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
    tracing::info!(
        "│ Hostname       : {}",
        hostname::get().unwrap_or_default().to_string_lossy()
    );
    tracing::info!(
        "│ Working Dir    : {}",
        std::env::current_dir().unwrap_or_default().display()
    );
    tracing::info!("│ Command        : /exit to terminate the session");
    tracing::info!("╞════════════════════════ ACTIVE CONNECTIONS ════════════════════════╡");

    setup_database_locks()
        .await
        .expect("Failed to setup database locks");

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

    let http_port = server_section
        .get("http_port")
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(51000);

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

    let storage_ports = Ports {
        http: http_port,
        https: https_port,
    };

    let cert = RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("Failed to load TLS certificates");

    let storage_addr = SocketAddr::from((ip, storage_ports.https));
    let serve_storage = serve_storage_routes().await.fallback(handler_404);

    let shutdown_handle: Arc<
        Mutex<Option<(Arc<axum_server::Handle>, tokio::task::JoinHandle<()>)>>,
    > = Arc::new(Mutex::new(None));

    let shutdown_handle_clone = Arc::clone(&shutdown_handle);

    {
        tokio::spawn(async move {
            let (handle, join) = serve(storage_addr, serve_storage, storage_ports, cert).await;
            let mut lock = shutdown_handle_clone.lock().await;
            *lock = Some((handle, join));
        });
    }

    {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let client_options = ClientOptions::parse(&*DATABASE_URL)
            .await
            .expect("Failed to parse MongoDB URI");
        let db_client = Arc::new(
            Client::with_options(client_options).expect("Failed to create MongoDB client"),
        );
        let app_state = Arc::new(AppState {
            db_client: Arc::clone(&db_client),
        });

        tokio::spawn(async move {
            start_cli(shutdown_handle, move || {
                let app_state = Arc::clone(&app_state);
                async move {
                    run_garbage_collector(Arc::clone(&app_state.db_client)).await;
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
    ports: Ports,
    cert: RustlsConfig,
) -> (Arc<axum_server::Handle>, tokio::task::JoinHandle<()>) {
    tracing::info!("Secured port => {} type: https", socket);

    let handle = Arc::new(axum_server::Handle::new());

    let handle_clone_for_redirect = Arc::clone(&handle);
    tokio::spawn(async move {
        redirect_http_to_https(
            ports,
            shutdown_signal(Arc::clone(&handle_clone_for_redirect)),
        )
        .await;
    });

    let mut server = axum_server::bind_rustls(socket, cert).handle((*handle).clone());
    server.http_builder().http2().enable_connect_protocol();

    let join = tokio::spawn(async move {
        server.serve(app.into_make_service()).await.unwrap();
    });

    (handle, join)
}

async fn redirect_http_to_https<F>(ports: Ports, signal: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    fn make_https(host: &str, uri: Uri, https_port: u16) -> Result<Uri, BoxError> {
        let mut parts = uri.into_parts();

        parts.scheme = Some(axum::http::uri::Scheme::HTTPS);

        if parts.path_and_query.is_none() {
            parts.path_and_query = Some("/".parse().unwrap());
        }

        let authority: Authority = host.parse()?;
        let bare_host = match authority.port() {
            Some(port_struct) => authority
                .as_str()
                .strip_suffix(port_struct.as_str())
                .unwrap()
                .strip_suffix(':')
                .unwrap(),
            None => authority.as_str(),
        };

        parts.authority = Some(format!("{bare_host}:{https_port}").parse()?);

        Ok(Uri::from_parts(parts)?)
    }

    let redirect = move |Host(host): Host, uri: Uri| async move {
        match make_https(&host, uri, ports.https) {
            Ok(uri) => Ok(Redirect::permanent(&uri.to_string())),
            Err(error) => {
                tracing::warn!(%error, "failed to convert URI to HTTPS");
                Err(StatusCode::BAD_REQUEST)
            }
        }
    };

    let addr = SocketAddr::from(([127, 0, 0, 1], ports.http));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::info!("Port => {} type: http", addr);
    axum::serve(listener, redirect.into_make_service())
        .with_graceful_shutdown(signal)
        .await
        .unwrap();
}

pub async fn shutdown_signal(handle: Arc<axum_server::Handle>) {
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
