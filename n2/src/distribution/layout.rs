use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterEcLayout {
    pub data_shards: usize,
    pub parity_shards: usize,
}

impl ClusterEcLayout {
    pub fn total_shards(self) -> usize {
        self.data_shards.saturating_add(self.parity_shards)
    }

    fn validate(self) -> Result<Self, String> {
        if self.data_shards == 0 {
            return Err("cluster data shards must be greater than zero".to_string());
        }
        if self.parity_shards == 0 {
            return Err("N2 3.0 server pools require at least one parity shard".to_string());
        }
        Ok(self)
    }
}

/// Static EC contract of the newest writable server pool. It is derived from
/// membership at startup; N2 never adapts it to a smaller live topology.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterLayoutPolicy {
    pub data_shards: usize,
    pub parity_shards: usize,
}

impl Default for ClusterLayoutPolicy {
    fn default() -> Self {
        Self {
            data_shards: 1,
            parity_shards: 1,
        }
    }
}

impl ClusterLayoutPolicy {
    pub fn layout(&self) -> Result<ClusterEcLayout, String> {
        ClusterEcLayout {
            data_shards: self.data_shards,
            parity_shards: self.parity_shards,
        }
        .validate()
    }
}

pub fn resolve_layout(
    available_nodes: usize,
    policy: &ClusterLayoutPolicy,
) -> Result<ClusterEcLayout, String> {
    let layout = policy.layout()?;
    if layout.total_shards() > available_nodes {
        return Err(format!(
            "cluster layout {}+{} requires {} nodes, but only {} are available",
            layout.data_shards,
            layout.parity_shards,
            layout.total_shards(),
            available_nodes
        ));
    }
    Ok(layout)
}

pub fn resolve_configured_layout(
    configured_nodes: usize,
    policy: &ClusterLayoutPolicy,
) -> Result<ClusterEcLayout, String> {
    resolve_layout(configured_nodes, policy)
}

pub fn resolve_write_layout(
    available_nodes: usize,
    configured_nodes: usize,
    policy: &ClusterLayoutPolicy,
) -> Result<ClusterEcLayout, String> {
    if available_nodes == 0 {
        return Err("cluster has no writable nodes".to_string());
    }
    let configured_layout = resolve_configured_layout(configured_nodes, policy)?;
    if available_nodes < configured_layout.total_shards() {
        return Err(format!(
            "configured object EC {}+{} requires {} writable server-pool nodes, but only {} are available",
            configured_layout.data_shards,
            configured_layout.parity_shards,
            configured_layout.total_shards(),
            available_nodes
        ));
    }
    Ok(configured_layout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_layout_refuses_under_width_writes_without_adapting() {
        let policy = ClusterLayoutPolicy {
            data_shards: 2,
            parity_shards: 1,
        };
        assert_eq!(
            resolve_write_layout(3, 3, &policy).unwrap(),
            ClusterEcLayout {
                data_shards: 2,
                parity_shards: 1,
            }
        );
        assert!(resolve_write_layout(2, 3, &policy).is_err());
        assert!(resolve_write_layout(1, 3, &policy).is_err());
    }

    #[test]
    fn static_layout_rejects_invalid_or_underprovisioned_pool() {
        assert!(resolve_write_layout(0, 2, &ClusterLayoutPolicy::default()).is_err());
        assert!(resolve_configured_layout(1, &ClusterLayoutPolicy::default()).is_err());
        assert!(
            ClusterLayoutPolicy {
                data_shards: 1,
                parity_shards: 0,
            }
            .layout()
            .is_err()
        );
    }
}
