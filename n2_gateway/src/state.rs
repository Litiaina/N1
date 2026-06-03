use crate::config::GatewayConfig;
use crate::node::NodeTable;
use crate::session_db::GatewaySessionDb;
use std::sync::Arc;

#[derive(Clone)]
pub struct GatewayState {
    pub config: Arc<GatewayConfig>,
    pub nodes: NodeTable,
    pub sessions: GatewaySessionDb,
    pub http: reqwest::Client,
}

impl GatewayState {
    pub async fn new(config: GatewayConfig) -> Result<Self, String> {
        let sessions = GatewaySessionDb::load(config.state_path.clone()).await?;
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .timeout(config.request_timeout)
            .build()
            .map_err(|err| format!("failed to build HTTP client: {err}"))?;
        Ok(Self {
            nodes: NodeTable::new(config.nodes.clone()),
            config: Arc::new(config),
            sessions,
            http,
        })
    }
}
