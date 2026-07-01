use serde::{Deserialize, Serialize};

pub const DEFAULT_AUTO_MAX_DATA_SHARDS: usize = 8;
pub const DEFAULT_AUTO_MAX_PARITY_SHARDS: usize = 3;

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
    #[serde(default = "default_auto_max_data_shards")]
    pub auto_max_data_shards: usize,
    #[serde(default = "default_auto_max_parity_shards")]
    pub auto_max_parity_shards: usize,
}

pub fn default_auto_max_data_shards() -> usize {
    DEFAULT_AUTO_MAX_DATA_SHARDS
}

pub fn default_auto_max_parity_shards() -> usize {
    DEFAULT_AUTO_MAX_PARITY_SHARDS
}

impl Default for ClusterLayoutPolicy {
    fn default() -> Self {
        Self {
            mode: ClusterLayoutMode::Auto,
            fixed_data_shards: None,
            fixed_parity_shards: None,
            auto_max_data_shards: DEFAULT_AUTO_MAX_DATA_SHARDS,
            auto_max_parity_shards: DEFAULT_AUTO_MAX_PARITY_SHARDS,
        }
    }
}

pub fn derive_auto_layout(active_nodes: usize) -> ClusterEcLayout {
    derive_auto_layout_with_bounds(
        active_nodes,
        DEFAULT_AUTO_MAX_DATA_SHARDS,
        DEFAULT_AUTO_MAX_PARITY_SHARDS,
    )
}

pub fn derive_auto_layout_with_bounds(
    active_nodes: usize,
    max_data_shards: usize,
    max_parity_shards: usize,
) -> ClusterEcLayout {
    let max_data_shards = max_data_shards.max(1);
    match active_nodes {
        0 | 1 => ClusterEcLayout {
            data_shards: 1,
            parity_shards: 0,
        },
        nodes => {
            let max_total = max_data_shards
                .saturating_add(max_parity_shards)
                .max(max_data_shards);
            let target_total = nodes.min(max_total).max(1);
            if max_parity_shards == 0 {
                return ClusterEcLayout {
                    data_shards: target_total,
                    parity_shards: 0,
                };
            }
            let parity_shards = target_total
                .saturating_sub(max_data_shards)
                .max(1)
                .min(max_parity_shards)
                .min(target_total.saturating_sub(1));
            ClusterEcLayout {
                data_shards: target_total.saturating_sub(parity_shards).max(1),
                parity_shards,
            }
        }
    }
}

pub fn resolve_layout(
    active_nodes: usize,
    policy: &ClusterLayoutPolicy,
) -> Result<ClusterEcLayout, String> {
    let layout = match policy.mode {
        ClusterLayoutMode::Auto => derive_auto_layout_with_bounds(
            active_nodes,
            policy.auto_max_data_shards,
            policy.auto_max_parity_shards,
        ),
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

pub fn resolve_write_layout(
    available_nodes: usize,
    configured_nodes: usize,
    policy: &ClusterLayoutPolicy,
) -> Result<ClusterEcLayout, String> {
    if available_nodes == 0 {
        return Err("cluster has no writable nodes".to_string());
    }

    let configured_layout = resolve_layout(configured_nodes.max(available_nodes), policy)?;
    if available_nodes >= configured_layout.total_shards() {
        return Ok(configured_layout);
    }

    // Preserve as much of the configured failure tolerance as the live node set
    // permits. Reducing data width instead of parity turns fixed 2+1 into 1+1
    // during a one-node outage while keeping the chosen layout immutable per object.
    let parity_shards = configured_layout
        .parity_shards
        .min(available_nodes.saturating_sub(1));
    Ok(ClusterEcLayout {
        data_shards: available_nodes.saturating_sub(parity_shards).max(1),
        parity_shards,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_layout_uses_bounded_cluster_shards() {
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
        assert_eq!(
            derive_auto_layout(10),
            ClusterEcLayout {
                data_shards: 8,
                parity_shards: 2
            }
        );
        assert_eq!(
            derive_auto_layout(1_000),
            ClusterEcLayout {
                data_shards: 8,
                parity_shards: 3
            }
        );
    }

    #[test]
    fn auto_layout_uses_configured_bounds() {
        assert_eq!(
            derive_auto_layout_with_bounds(32, 4, 2),
            ClusterEcLayout {
                data_shards: 4,
                parity_shards: 2
            }
        );
        assert_eq!(
            derive_auto_layout_with_bounds(32, 6, 0),
            ClusterEcLayout {
                data_shards: 6,
                parity_shards: 0
            }
        );
    }

    #[test]
    fn fixed_layout_validates_against_active_nodes() {
        let policy = ClusterLayoutPolicy {
            mode: ClusterLayoutMode::Fixed,
            fixed_data_shards: Some(2),
            fixed_parity_shards: Some(1),
            ..ClusterLayoutPolicy::default()
        };
        assert!(resolve_layout(3, &policy).is_ok());
        assert!(resolve_layout(2, &policy).is_err());
    }

    #[test]
    fn fixed_layout_degrades_per_object_without_changing_configured_layout() {
        let policy = ClusterLayoutPolicy {
            mode: ClusterLayoutMode::Fixed,
            fixed_data_shards: Some(2),
            fixed_parity_shards: Some(1),
            ..ClusterLayoutPolicy::default()
        };

        assert_eq!(
            resolve_write_layout(3, 3, &policy).unwrap(),
            ClusterEcLayout {
                data_shards: 2,
                parity_shards: 1,
            }
        );
        assert_eq!(
            resolve_write_layout(2, 3, &policy).unwrap(),
            ClusterEcLayout {
                data_shards: 1,
                parity_shards: 1,
            }
        );
        assert_eq!(
            resolve_write_layout(1, 3, &policy).unwrap(),
            ClusterEcLayout {
                data_shards: 1,
                parity_shards: 0,
            }
        );
    }

    #[test]
    fn degraded_layout_preserves_configured_parity_before_data_width() {
        let policy = ClusterLayoutPolicy {
            mode: ClusterLayoutMode::Fixed,
            fixed_data_shards: Some(5),
            fixed_parity_shards: Some(3),
            ..ClusterLayoutPolicy::default()
        };

        assert_eq!(
            resolve_write_layout(7, 8, &policy).unwrap(),
            ClusterEcLayout {
                data_shards: 4,
                parity_shards: 3,
            }
        );
        assert_eq!(
            resolve_write_layout(2, 8, &policy).unwrap(),
            ClusterEcLayout {
                data_shards: 1,
                parity_shards: 1,
            }
        );
    }

    #[test]
    fn write_layout_rejects_an_empty_live_set() {
        assert!(resolve_write_layout(0, 3, &ClusterLayoutPolicy::default()).is_err());
    }
}
