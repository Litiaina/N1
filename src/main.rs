use crate::fs::cleanup_manager::cleanup_stale_uploads;
use crate::fs::fs_constants::ENV_FILE;
use crate::handlers::error_handlers::handler_404;
use crate::routes::storage_routes::serve_storage_routes;
use crate::structs::port_struct::Ports;
use axum::handler::HandlerWithoutStateExt;
use axum::http::uri::Authority;
use axum::http::{StatusCode, Uri};
use axum::response::Redirect;
use axum::{BoxError, Router};
use axum_extra::extract::Host;
use axum_server::tls_rustls::RustlsConfig;
use chrono::Local;
use dotenv::dotenv;
use std::env;
use std::fs::File;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod database;
mod enums;
mod fs;
mod handlers;
mod middleware;
mod routes;
mod statics;
mod structs;
mod utils;
mod validation;
mod object_storage;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=trace", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    tracing::info!("╔═════════════════════════════════════════════════════════════╗");
    tracing::info!("║         L I T I A I N A  S T O R A G E  S E R V E R         ║");
    tracing::info!("╚═════════════════════════════════════════════════════════════╝");
    tracing::info!("{} - Status: ONLINE", current_time);
    tracing::info!("Press CTRL+C to terminate");
    tracing::info!("══════════════ ACTIVE CONNECTIONS ══════════════");
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
    server().await
}

async fn server() {
    tokio::spawn(async {
        cleanup_stale_uploads().await;
    });

    let storage_ports = Ports {
        http: 51000,
        https: 50001,
    };

    let exe_dir = env::current_exe()
        .expect("Failed to find executable path")
        .parent()
        .expect("Failed to get executable directory")
        .to_path_buf();

    let config = RustlsConfig::from_pem_file(exe_dir.join("cert.pem"), exe_dir.join("key.pem"))
        .await
        .unwrap();

    let storage_port = SocketAddr::from(([127, 0, 0, 1], storage_ports.https));
    let serve_storage = serve_storage_routes().await.fallback(handler_404);

    let file_system_task = tokio::spawn(serve(
        storage_port,
        serve_storage,
        Arc::from("storage-api".to_string()),
        storage_ports,
        config,
    ));

    let _ = tokio::join!(file_system_task,);
}

async fn serve(
    socket: SocketAddr,
    app: Router,
    name: Arc<String>,
    ports: Ports,
    cert: RustlsConfig,
) {
    tracing::info!("{} on port => {} type: https", name, socket);

    let handle = axum_server::Handle::new();

    let server_name_for_task = Arc::clone(&name);
    let shutdown_future = shutdown_signal(Arc::clone(&server_name_for_task), handle.clone());
    tokio::spawn(redirect_http_to_https(
        server_name_for_task,
        ports,
        shutdown_future,
    ));

    let mut server = axum_server::bind_rustls(socket, cert).handle(handle);
    server.http_builder().http2().enable_connect_protocol();
    server.serve(app.into_make_service()).await.unwrap();
}

async fn redirect_http_to_https<F>(name: Arc<String>, ports: Ports, signal: F)
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
    tracing::info!("{} on port => {} type: http", name, addr);
    axum::serve(listener, redirect.into_make_service())
        .with_graceful_shutdown(signal)
        .await
        .unwrap();
}

async fn shutdown_signal(name: Arc<String>, handle: axum_server::Handle) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("{} => shutdown received", name);
    handle.graceful_shutdown(Some(Duration::from_secs(10)));
}

fn create_env_file(path: &str) -> io::Result<()> {
    let mut file = File::create(path)?;
    writeln!(file, "JWT_SECRET=enter_jwt_secret_here")?;
    Ok(())
}
