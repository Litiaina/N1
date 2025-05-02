use std::net::SocketAddr;
use std::sync::Arc;
use axum::Router;
use chrono::Local;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::Notify;
use tower_http::trace::TraceLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use crate::handlers::error_handlers::handler_404;
use crate::routes::file_system_routes;

mod handlers;
mod routes;
mod structs;
mod types;
mod utils;
mod fs;

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
    tracing::info!("╔════════════════════════════════════════════════════════════════════╗");
    tracing::info!("║         L I T I A I N A  F I L E  S Y S T E M  S E R V E R         ║");
    tracing::info!("╚════════════════════════════════════════════════════════════════════╝");
    tracing::info!("{} - Status: ONLINE", current_time);
    tracing::info!("Press CTRL+C to terminate");
    tracing::info!("══════════════ ACTIVE CONNECTIONS ══════════════");
    server().await
}

async fn server() {
    let file_system_port = SocketAddr::from(([127, 0, 0, 1], 50001));
    let file_system_listener = TcpListener::bind(file_system_port).await.unwrap();
    let serve_file_system = file_system_routes::serve_file_system_routes().fallback(handler_404);
    let shutdown_notify = Arc::new(Notify::new());

    let file_system_task = tokio::spawn(serve(
        file_system_listener,
        serve_file_system,
        "file_system",
        shutdown_notify.clone(),
    ));

    let _ = tokio::join!(
        file_system_task
    );
}

async fn serve(listener: TcpListener, app: Router, name: &str, shutdown_notify: Arc<Notify>) {
    tracing::info!("{} => {}", name, listener.local_addr().unwrap());
    if let Err(err) = axum::serve(listener, app.layer(TraceLayer::new_for_http()))
        .with_graceful_shutdown(shutdown_signal(shutdown_notify))
        .await
    {
        eprintln!("Error serving {}: {}", name, err);
    }
    println!("{} => service terminated", name);
}

async fn shutdown_signal(shutdown_notify: Arc<Notify>) {
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

    shutdown_notify.notify_one();
}
