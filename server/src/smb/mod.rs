pub mod config;
pub mod server;

use self::config::SmbConfig;
use self::server::spawn_listener;
use std::io;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct SmbRuntime {
    pub addr: std::net::SocketAddr,
    pub join: JoinHandle<()>,
}

pub async fn start(config: SmbConfig) -> io::Result<Option<SmbRuntime>> {
    if !config.enabled {
        return Ok(None);
    }

    let (listener_addr, join) = spawn_listener(config).await?;
    Ok(Some(SmbRuntime {
        addr: listener_addr,
        join,
    }))
}
