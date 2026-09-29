use crate::config::GatewayConfig;
use crate::node::NodeTable;
use std::sync::Arc;

#[derive(Clone)]
pub struct GatewayState {
    pub config: Arc<GatewayConfig>,
    pub nodes: NodeTable,
    pub http: reqwest::Client,
    pub export_http: reqwest::Client,
}

impl GatewayState {
    pub async fn new(config: GatewayConfig) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .timeout(config.request_timeout)
            .build()
            .map_err(|err| format!("failed to build HTTP client: {err}"))?;
        // Export bodies can legitimately outlive the ordinary request budget.
        // Bound connection/idle reads, not the lifetime of a progressing body.
        let export_http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .connect_timeout(config.request_timeout)
            .read_timeout(config.request_timeout)
            .build()
            .map_err(|err| format!("failed to build export HTTP client: {err}"))?;
        Ok(Self {
            nodes: NodeTable::new(config.nodes.clone()),
            config: Arc::new(config),
            http,
            export_http,
        })
    }
}
