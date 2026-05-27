use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterLayoutMode {
    Auto,
    Fixed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterEcLayout {
    pub data_shards: usize,
    pub parity_shards: usize,
}

impl ClusterEcLayout {
    pub fn total_shards(self) -> usize {
        self.data_shards.saturating_add(self.parity_shards)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterLayoutPolicy {
    pub mode: ClusterLayoutMode,
    pub fixed_data_shards: Option<usize>,
    pub fixed_parity_shards: Option<usize>,
}

impl Default for ClusterLayoutPolicy {
    fn default() -> Self {
        Self {
            mode: ClusterLayoutMode::Auto,
            fixed_data_shards: None,
            fixed_parity_shards: None,
        }
    }
}

pub fn derive_auto_layout(active_nodes: usize) -> ClusterEcLayout {
    match active_nodes {
        0 | 1 => ClusterEcLayout {
            data_shards: 1,
            parity_shards: 0,
        },
        nodes => ClusterEcLayout {
            data_shards: nodes.saturating_sub(1),
            parity_shards: 1,
        },
    }
}

pub fn resolve_layout(
    active_nodes: usize,
    policy: &ClusterLayoutPolicy,
) -> Result<ClusterEcLayout, String> {
    let layout = match policy.mode {
        ClusterLayoutMode::Auto => derive_auto_layout(active_nodes),
        ClusterLayoutMode::Fixed => ClusterEcLayout {
            data_shards: policy
                .fixed_data_shards
                .ok_or_else(|| "fixed cluster layout requires data shards".to_string())?,
            parity_shards: policy
                .fixed_parity_shards
                .ok_or_else(|| "fixed cluster layout requires parity shards".to_string())?,
        },
    };

    if layout.data_shards == 0 {
        return Err("cluster data shards must be greater than zero".to_string());
    }
    if layout.total_shards() > active_nodes.max(1) {
        return Err(format!(
            "cluster layout {}+{} requires {} nodes, but only {} are active",
            layout.data_shards,
            layout.parity_shards,
            layout.total_shards(),
            active_nodes
        ));
    }
    Ok(layout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_layout_treats_nodes_as_cluster_shards() {
        assert_eq!(
            derive_auto_layout(1),
            ClusterEcLayout {
                data_shards: 1,
                parity_shards: 0
            }
        );
        assert_eq!(
            derive_auto_layout(2),
            ClusterEcLayout {
                data_shards: 1,
                parity_shards: 1
            }
        );
        assert_eq!(
            derive_auto_layout(4),
            ClusterEcLayout {
                data_shards: 3,
                parity_shards: 1
            }
        );
    }

    #[test]
    fn fixed_layout_validates_against_active_nodes() {
        let policy = ClusterLayoutPolicy {
            mode: ClusterLayoutMode::Fixed,
            fixed_data_shards: Some(2),
            fixed_parity_shards: Some(1),
        };
        assert!(resolve_layout(3, &policy).is_ok());
        assert!(resolve_layout(2, &policy).is_err());
    }
}
