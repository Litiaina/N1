use crate::config::NodeConfig;
use crate::session_db::now_unix;
use n2_native_client::N2NativeClient;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Unknown,
    Online,
    Offline,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeSnapshot {
    pub id: String,
    pub http_url: String,
    pub native_url: String,
    pub weight: u32,
    pub status: NodeStatus,
    pub last_seen_at: Option<i64>,
    pub rtt_ms: Option<u64>,
    pub failure_count: u64,
    pub routed_requests: u64,
}

#[derive(Debug, Clone)]
pub struct NodeRuntime {
    pub config: NodeConfig,
    pub status: NodeStatus,
    pub last_seen_at: Option<i64>,
    pub rtt_ms: Option<u64>,
    pub failure_count: u64,
    pub routed_requests: u64,
}

impl NodeRuntime {
    fn new(config: NodeConfig) -> Self {
        Self {
            config,
            status: NodeStatus::Unknown,
            last_seen_at: None,
            rtt_ms: None,
            failure_count: 0,
            routed_requests: 0,
        }
    }

    fn snapshot(&self) -> NodeSnapshot {
        NodeSnapshot {
            id: self.config.id.clone(),
            http_url: self.config.http_url.clone(),
            native_url: self.config.native_url.clone(),
            weight: self.config.weight,
            status: self.status.clone(),
            last_seen_at: self.last_seen_at,
            rtt_ms: self.rtt_ms,
            failure_count: self.failure_count,
            routed_requests: self.routed_requests,
        }
    }
}

#[derive(Clone)]
pub struct NodeTable {
    nodes: Arc<RwLock<HashMap<String, NodeRuntime>>>,
}

impl NodeTable {
    pub fn new(configs: Vec<NodeConfig>) -> Self {
        let nodes = configs
            .into_iter()
            .map(|config| (config.id.clone(), NodeRuntime::new(config)))
            .collect();
        Self {
            nodes: Arc::new(RwLock::new(nodes)),
        }
    }

    pub async fn snapshots(&self) -> Vec<NodeSnapshot> {
        let mut snapshots = self
            .nodes
            .read()
            .await
            .values()
            .map(NodeRuntime::snapshot)
            .collect::<Vec<_>>();
        snapshots.sort_by(|a, b| a.id.cmp(&b.id));
        snapshots
    }

    pub async fn get(&self, id: &str) -> Option<NodeConfig> {
        self.nodes
            .read()
            .await
            .get(id)
            .map(|node| node.config.clone())
    }

    pub async fn is_available(&self, id: &str) -> bool {
        self.nodes
            .read()
            .await
            .get(id)
            .is_some_and(|node| !matches!(node.status, NodeStatus::Offline))
    }

    pub async fn select_best(&self) -> Option<NodeConfig> {
        let mut nodes = self.nodes.write().await;
        let mut candidates = nodes.values_mut().collect::<Vec<_>>();
        candidates.retain(|node| !matches!(node.status, NodeStatus::Offline));
        candidates.sort_by(|a, b| {
            let a_score = node_score(a);
            let b_score = node_score(b);
            a_score
                .cmp(&b_score)
                .then_with(|| a.config.id.cmp(&b.config.id))
        });
        let node = candidates.into_iter().next()?;
        node.routed_requests = node.routed_requests.saturating_add(1);
        Some(node.config.clone())
    }

    #[cfg(test)]
    pub async fn mark_online_for_test(&self, id: &str, rtt_ms: u64) {
        if let Some(node) = self.nodes.write().await.get_mut(id) {
            node.status = NodeStatus::Online;
            node.rtt_ms = Some(rtt_ms);
        }
    }

    #[cfg(test)]
    pub async fn mark_unknown_for_test(&self, id: &str) {
        if let Some(node) = self.nodes.write().await.get_mut(id) {
            node.status = NodeStatus::Unknown;
            node.rtt_ms = None;
        }
    }

    #[cfg(test)]
    pub async fn mark_offline_for_test(&self, id: &str) {
        self.mark_failure(id).await;
    }

    pub async fn mark_route(&self, id: &str) {
        if let Some(node) = self.nodes.write().await.get_mut(id) {
            node.routed_requests = node.routed_requests.saturating_add(1);
        }
    }

    pub async fn mark_failure(&self, id: &str) {
        if let Some(node) = self.nodes.write().await.get_mut(id) {
            node.status = NodeStatus::Offline;
            node.failure_count = node.failure_count.saturating_add(1);
        }
    }

    pub async fn probe_once(&self) {
        let ids = self.nodes.read().await.keys().cloned().collect::<Vec<_>>();
        for id in ids {
            let Some(config) = self.get(&id).await else {
                continue;
            };
            let started = Instant::now();
            let result = async {
                let mut client = N2NativeClient::connect(&config.native_url).await?;
                client.raw_json("ping", &json!({}), None).await?;
                Ok::<(), n2_native_client::NativeClientError>(())
            }
            .await;
            let mut nodes = self.nodes.write().await;
            let Some(node) = nodes.get_mut(&id) else {
                continue;
            };
            match result {
                Ok(()) => {
                    node.status = NodeStatus::Online;
                    node.last_seen_at = Some(now_unix());
                    node.rtt_ms = Some(started.elapsed().as_millis() as u64);
                    debug!("N2 gateway node {} online", id);
                }
                Err(err) => {
                    node.status = NodeStatus::Offline;
                    node.failure_count = node.failure_count.saturating_add(1);
                    warn!("N2 gateway node {} offline: {}", id, err);
                }
            }
        }
    }
}

fn node_score(node: &NodeRuntime) -> u64 {
    let health_penalty = match node.status {
        NodeStatus::Online => 0u64,
        NodeStatus::Unknown => 10_000,
        NodeStatus::Offline => 1_000_000,
    };
    let default_rtt = match node.status {
        NodeStatus::Online => 0u64,
        NodeStatus::Unknown => 10_000,
        NodeStatus::Offline => 1_000_000,
    };
    let routed = node.routed_requests / node.config.weight.max(1) as u64;
    health_penalty + node.rtt_ms.unwrap_or(default_rtt) + routed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, weight: u32) -> NodeConfig {
        NodeConfig {
            id: id.to_string(),
            http_url: format!("https://{id}.example.test:50001"),
            native_url: format!("n2://{id}.example.test:50002"),
            weight,
        }
    }

    #[tokio::test]
    async fn selection_prefers_online_nodes_over_unknown_nodes() {
        let table = NodeTable::new(vec![node("node-a", 1), node("node-b", 1)]);
        table.mark_unknown_for_test("node-a").await;
        table.mark_online_for_test("node-b", 5).await;

        let selected = table.select_best().await.expect("selected node");
        assert_eq!(selected.id, "node-b");
    }

    #[tokio::test]
    async fn selection_excludes_offline_nodes() {
        let table = NodeTable::new(vec![node("node-a", 1), node("node-b", 1)]);
        table.mark_online_for_test("node-a", 1).await;
        table.mark_online_for_test("node-b", 1).await;
        table.mark_offline_for_test("node-a").await;

        let selected = table.select_best().await.expect("selected node");
        assert_eq!(selected.id, "node-b");
    }

    #[tokio::test]
    async fn selection_uses_node_id_as_deterministic_tie_breaker() {
        let table = NodeTable::new(vec![node("node-b", 1), node("node-a", 1)]);
        table.mark_online_for_test("node-a", 1).await;
        table.mark_online_for_test("node-b", 1).await;

        let selected = table.select_best().await.expect("selected node");
        assert_eq!(selected.id, "node-a");
    }
}
