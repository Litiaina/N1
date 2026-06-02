mod admin;
mod core;
mod enums;
mod fragment_auth;
mod fuse;
mod local_auth;
mod metadata;
mod n1_sync;
mod namespace;
mod routes;
mod settings;
mod smb;
mod structs;
mod validation;
mod websocket;

use crate::metadata::NasMetadataStore;
use crate::routes::storage_routes::{AppState, init_global_app_state};
use crate::settings::config::load_config;
use crate::settings::constants::{CONFIG_FILE, ENV_FILE};
use crate::validation::enforce_init_settings::{ensure_config_file, ensure_env_file};
use chrono::Local;
use std::sync::Arc;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

fn init_tracing() {
    tracing_subscriber::registry()
        .with(fmt::layer().with_target(false))
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
}

#[tokio::main]
async fn main() {
    let _ = dotenv::from_filename(ENV_FILE);
    let _ = ensure_env_file();
    let _ = ensure_config_file();
    init_tracing();

    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    tracing::info!("╔═════════════════════════════════╗");
    tracing::info!("║      L I T I A I N A  N S       ║");
    tracing::info!("╚═════════════════════════════════╝");
    tracing::info!("│ Version        : {}", env!("CARGO_PKG_VERSION"));
    tracing::info!("│ Status         : ONLINE");
    tracing::info!("│ Start Time     : {}", current_time);
    tracing::info!("│ PID            : {}", std::process::id());
    tracing::info!(
        "│ Hostname       : {}",
        hostname::get().unwrap_or_default().to_string_lossy()
    );
    tracing::info!(
        "│ Working Dir    : {}",
        std::env::current_dir().unwrap_or_default().display()
    );

    let config = match load_config() {
        Ok(config) => Arc::new(config),
        Err(err) => {
            tracing::error!("failed to load {}: {}", CONFIG_FILE, err);
            return;
        }
    };

    let metadata = match NasMetadataStore::open(&config.storage.metadata_db_path) {
        Ok(store) => store,
        Err(err) => {
            tracing::error!("failed to open NAS metadata store: {}", err);
            return;
        }
    };
    if let Err(err) = metadata.seed_from_config(&config) {
        tracing::error!("failed to seed NAS metadata store: {}", err);
        return;
    }
    match metadata.list_shares() {
        Ok(shares) => tracing::info!("│ Active Shares  : {}", shares.len()),
        Err(err) => {
            tracing::error!("failed to inspect active NAS shares: {}", err);
            return;
        }
    }

    let app_state = Arc::new(AppState::new(
        metadata.clone(),
        config.storage.staging_root.clone(),
        Arc::clone(&config),
    ));
    if let Err(err) = init_global_app_state(Arc::clone(&app_state)) {
        tracing::error!("failed to initialize global NS state: {}", err);
        return;
    }

    let _sync_join = n1_sync::spawn_sync_worker(Arc::clone(&config), metadata);

    let admin_runtime = match admin::start(Arc::clone(&config), app_state.metadata.clone()).await {
        Ok(Some(runtime)) => {
            tracing::info!("NS admin listener online at {}", runtime.addr);
            Some(runtime)
        }
        Ok(None) => {
            tracing::info!("NS admin listener disabled");
            None
        }
        Err(err) => {
            tracing::error!("failed to start NS admin listener: {}", err);
            return;
        }
    };

    let smb_runtime = match smb::start(config.smb.clone()).await {
        Ok(Some(runtime)) => {
            tracing::info!("smb listener online at {}", runtime.addr);
            Some(runtime)
        }
        Ok(None) => {
            tracing::warn!("SMB listener disabled in configuration");
            None
        }
        Err(err) => {
            tracing::error!("failed to start SMB listener: {}", err);
            return;
        }
    };

    if let Some(runtime) = smb_runtime {
        let mut join = runtime.join;
        let mut admin_join = admin_runtime.map(|runtime| runtime.join);
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown signal received");
                join.abort();
                if let Some(admin_join) = admin_join.as_mut() {
                    admin_join.abort();
                }
            }
            _ = &mut join => {
                tracing::warn!("SMB listener exited");
            }
            _ = async {
                if let Some(admin_join) = admin_join.as_mut() {
                    let _ = admin_join.await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                tracing::warn!("NS admin listener exited");
            }
        }
    } else if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!("failed to wait for shutdown signal: {}", err);
    }
}
