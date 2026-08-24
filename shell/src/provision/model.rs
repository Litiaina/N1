use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentBinding {
    pub url: String,
    pub agent_id: String,
    pub machine_fingerprint: String,
    pub mount_root: String,
    pub credential: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostSpec {
    pub id: String,
    pub agent: AgentBinding,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeSpec {
    pub id: String,
    pub host_id: String,
    pub advertised_host: String,
    pub https_port: u16,
    pub native_port: u16,
    pub disks: BTreeMap<String, DiskSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskSpec {
    pub label: String,
    pub filesystem_uuid: String,
    pub stable_id: String,
    pub serial: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolSpec {
    pub id: String,
    pub generation: u64,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub node_ids: Vec<String>,
    pub writable: bool,
}

impl PoolSpec {
    pub(super) fn width(&self) -> usize {
        self.data_shards.saturating_add(self.parity_shards)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewaySpec {
    pub id: String,
    pub node_id: String,
    pub bind_host: String,
    pub https_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSpec {
    pub format_version: u32,
    pub cluster_id: String,
    pub topology_epoch: u64,
    pub hosts: BTreeMap<String, HostSpec>,
    pub nodes: BTreeMap<String, NodeSpec>,
    pub pools: BTreeMap<String, PoolSpec>,
    pub reserve_nodes: BTreeSet<String>,
    pub gateways: BTreeMap<String, GatewaySpec>,
}

impl ClusterSpec {
    pub(super) fn validate(&self) -> Result<(), String> {
        if self.format_version != PLAN_FORMAT_VERSION {
            return Err(format!(
                "unsupported Shell plan format {}",
                self.format_version
            ));
        }
        validate_id(&self.cluster_id, "cluster ID")?;
        if self.topology_epoch == 0 {
            return Err("topology epoch must be positive".into());
        }
        if self.pools.is_empty() {
            return Err("at least one server pool is required".into());
        }
        if self.hosts.is_empty() {
            return Err("at least one physical host is required".into());
        }
        let mut agent_ids = BTreeSet::new();
        for (host_key, host) in &self.hosts {
            if host_key != &host.id {
                return Err(format!(
                    "host map key '{host_key}' does not match host ID '{}'",
                    host.id
                ));
            }
            validate_id(&host.id, "host ID")?;
            if !agent_ids.insert(host.agent.agent_id.clone()) {
                return Err(format!(
                    "physical provisioning agent '{}' is enrolled more than once",
                    host.agent.agent_id
                ));
            }
        }
        let mut claimed = BTreeSet::new();
        let mut generations = BTreeSet::new();
        for (pool_key, pool) in &self.pools {
            if pool_key != &pool.id {
                return Err(format!(
                    "pool map key '{pool_key}' does not match pool ID '{}'",
                    pool.id
                ));
            }
            validate_id(&pool.id, "pool ID")?;
            if pool.generation == 0 || !generations.insert(pool.generation) {
                return Err("every server pool requires a unique positive generation".into());
            }
            if pool.data_shards == 0 || pool.parity_shards == 0 {
                return Err(format!("pool '{}' requires K >= 1 and M >= 1", pool.id));
            }
            if pool.node_ids.len() != pool.width() {
                return Err(format!(
                    "pool '{}' EC {}+{} requires exactly {} nodes, but has {}",
                    pool.id,
                    pool.data_shards,
                    pool.parity_shards,
                    pool.width(),
                    pool.node_ids.len()
                ));
            }
            for node_id in &pool.node_ids {
                if !self.nodes.contains_key(node_id) {
                    return Err(format!(
                        "pool '{}' references unknown node '{node_id}'",
                        pool.id
                    ));
                }
                if !claimed.insert(node_id.clone()) {
                    return Err(format!("node '{node_id}' is assigned more than once"));
                }
            }
        }
        for node_id in &self.reserve_nodes {
            if !self.nodes.contains_key(node_id) {
                return Err(format!("reserve set references unknown node '{node_id}'"));
            }
            if !claimed.insert(node_id.clone()) {
                return Err(format!(
                    "node '{node_id}' cannot be both active and reserve"
                ));
            }
        }
        if claimed.len() != self.nodes.len() {
            let unassigned = self
                .nodes
                .keys()
                .filter(|node| !claimed.contains(*node))
                .cloned()
                .collect::<Vec<_>>();
            return Err(format!(
                "nodes must belong to exactly one server pool or the reserve set; unassigned: {}",
                unassigned.join(", ")
            ));
        }
        let mut labels = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for (node_key, node) in &self.nodes {
            if node_key != &node.id {
                return Err(format!(
                    "node map key '{node_key}' does not match node ID '{}'",
                    node.id
                ));
            }
            validate_id(&node.id, "node ID")?;
            validate_host(&node.advertised_host)?;
            if !self.hosts.contains_key(&node.host_id) {
                return Err(format!(
                    "node '{}' references unknown physical host '{}'",
                    node.id, node.host_id
                ));
            }
            for port in [node.https_port, node.native_port] {
                if port == 0 || !endpoints.insert((node.advertised_host.clone(), port)) {
                    return Err(format!(
                        "node '{}' has an invalid or colliding advertised endpoint",
                        node.id
                    ));
                }
            }
            if node.disks.is_empty() {
                return Err(format!("node '{}' has no assigned storage disk", node.id));
            }
            for (label, disk) in &node.disks {
                if !valid_ndisk_label(label) || !labels.insert(label.clone()) {
                    return Err(format!("disk label '{label}' is invalid or duplicated"));
                }
                if disk.label != *label || disk.filesystem_uuid.is_empty() {
                    return Err(format!(
                        "disk binding '{label}' has inconsistent identity metadata"
                    ));
                }
            }
        }
        let mut gateway_hosts = BTreeSet::new();
        for (gateway_key, gateway) in &self.gateways {
            if gateway_key != &gateway.id {
                return Err(format!(
                    "gateway map key '{gateway_key}' does not match gateway ID '{}'",
                    gateway.id
                ));
            }
            if !self.nodes.contains_key(&gateway.node_id) {
                return Err(format!(
                    "gateway '{}' references unknown node '{}'",
                    gateway.id, gateway.node_id
                ));
            }
            validate_host(&gateway.bind_host)?;
            let node = &self.nodes[&gateway.node_id];
            if gateway.https_port == 0
                || self.nodes_for_host(&node.host_id).iter().any(|local| {
                    gateway.https_port == local.https_port
                        || gateway.https_port == local.native_port
                })
            {
                return Err(format!(
                    "gateway '{}' port collides with a node on its physical host",
                    gateway.id
                ));
            }
            if !gateway_hosts.insert(node.host_id.clone()) {
                return Err(format!(
                    "physical host '{}' owns more than one gateway",
                    node.host_id
                ));
            }
        }
        Ok(())
    }

    pub(super) fn nodes_for_host(&self, host_id: &str) -> Vec<&NodeSpec> {
        self.nodes
            .values()
            .filter(|node| node.host_id == host_id)
            .collect()
    }

    pub(super) fn host_for_node(&self, node: &NodeSpec) -> &HostSpec {
        self.hosts.get(&node.host_id).expect("validated node host")
    }

    pub(super) fn gateway_for_host(&self, host_id: &str) -> Option<&GatewaySpec> {
        self.gateways.values().find(|gateway| {
            self.nodes
                .get(&gateway.node_id)
                .is_some_and(|node| node.host_id == host_id)
        })
    }

    pub(super) fn sanitized(&self) -> Self {
        let mut clone = self.clone();
        for host in clone.hosts.values_mut() {
            host.agent.credential.clear();
        }
        clone
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentEnvelope {
    pub spec: ClusterSpec,
    pub env: String,
    pub certificate: Vec<u8>,
    pub private_key: Vec<u8>,
    pub plan_hash: String,
}

impl DeploymentEnvelope {
    pub(super) fn new(
        spec: ClusterSpec,
        env: String,
        certificate: Vec<u8>,
        private_key: Vec<u8>,
    ) -> Result<Self, String> {
        let spec = spec.sanitized();
        let plan_hash = deployment_hash(&spec, &env, &certificate, &private_key)?;
        Ok(Self {
            spec,
            env,
            certificate,
            private_key,
            plan_hash,
        })
    }

    pub(super) fn validate_hash(&self) -> Result<(), String> {
        let actual = deployment_hash(&self.spec, &self.env, &self.certificate, &self.private_key)?;
        if actual != self.plan_hash {
            return Err("deployment content does not match its canonical SHA-256 hash".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CommissionerPlan {
    pub(super) spec: ClusterSpec,
    pub(super) commissioner_id: String,
    pub(super) tls_certificate_path: Option<String>,
    pub(super) tls_private_key_path: Option<String>,
    pub(super) env: String,
    pub(super) committed_hash: Option<String>,
}
