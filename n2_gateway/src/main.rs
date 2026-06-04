mod config;
mod native;
mod node;
mod proxy;
mod routes;
mod state;

use axum_server::tls_rustls::RustlsConfig;
use config::GatewayConfig;
use state::GatewayState;
use std::net::SocketAddr;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install 'ring' as the default TLS provider.");

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "n2_gateway=info,tower_http=info".into()),
        )
        .init();

    tracing::info!("╔════════════════════════════════════════════════╗");
    tracing::info!("║         L I T I A I N A  N 2  G A T E        ║");
    tracing::info!("╚════════════════════════════════════════════════╝");

    let config = GatewayConfig::load().map_err(std::io::Error::other)?;
    let addr: SocketAddr = format!("{}:{}", config.host, config.https_port).parse()?;
    let tls_config = RustlsConfig::from_pem_file(&config.cert_path, &config.key_path).await?;
    let state = GatewayState::new(config)
        .await
        .map_err(std::io::Error::other)?;

    spawn_health_monitor(state.clone());

    info!("N2 Gateway HTTPS => https://{}", addr);
    let app = routes::router(state);
    axum_server::bind_rustls(addr, tls_config)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

fn spawn_health_monitor(state: GatewayState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(state.config.health_interval);
        loop {
            interval.tick().await;
            state.nodes.probe_once().await;
        }
    });
}
