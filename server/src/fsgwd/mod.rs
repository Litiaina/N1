pub mod config;
pub mod server;

use self::config::FsgwdConfig;
use self::server::spawn_listener;
use std::io;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct FsgwdRuntime {
    pub addr: std::net::SocketAddr,
    pub join: JoinHandle<()>,
}

pub async fn start(config: FsgwdConfig) -> io::Result<Option<FsgwdRuntime>> {
    if !config.enabled {
        return Ok(None);
    }

    let (listener_addr, join) = spawn_listener(config).await?;
    Ok(Some(FsgwdRuntime {
        addr: listener_addr,
        join,
    }))
}
