use crate::distribution::layout::ClusterEcLayout;
use crate::distribution::membership::ClusterNode;
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterShardRole {
    Data,
    Parity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterShardPlacement {
    pub shard_index: usize,
    pub role: ClusterShardRole,
    pub node_id: String,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_endpoint: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterPlacementPlan {
    pub fragment: String,
    pub object_key: String,
    pub version_id: String,
    pub membership_epoch: u64,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub placements: Vec<ClusterShardPlacement>,
    pub adaptive_without_rebalance: bool,
}

fn placement_score(fragment: &str, object_key: &str, version_id: &str, node_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(fragment.as_bytes());
    hasher.update([0]);
    hasher.update(object_key.as_bytes());
    hasher.update([0]);
    hasher.update(version_id.as_bytes());
    hasher.update([0]);
    hasher.update(node_id.as_bytes());
    hasher.finalize().into()
}

pub fn build_placement_plan(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    membership_epoch: u64,
    layout: ClusterEcLayout,
    candidates: &[ClusterNode],
) -> Result<ClusterPlacementPlan, AppError> {
    let required = layout.total_shards();
    if candidates.len() < required {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 cluster layout {}+{} requires {} writable node(s), but only {} are available",
            layout.data_shards,
            layout.parity_shards,
            required,
            candidates.len()
        )));
    }

    let mut scored = candidates
        .iter()
        .map(|node| {
            (
                placement_score(fragment, object_key, version_id, &node.id),
                node,
            )
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| right.0.cmp(&left.0).then(left.1.id.cmp(&right.1.id)));

    let placements = scored
        .into_iter()
        .take(required)
        .enumerate()
        .map(|(shard_index, (_, node))| ClusterShardPlacement {
            shard_index,
            role: if shard_index < layout.data_shards {
                ClusterShardRole::Data
            } else {
                ClusterShardRole::Parity
            },
            node_id: node.id.clone(),
            endpoint: node.endpoint.clone(),
            native_endpoint: node.native_endpoint.clone(),
        })
        .collect::<Vec<_>>();

    Ok(ClusterPlacementPlan {
        fragment: fragment.to_string(),
        object_key: object_key.to_string(),
        version_id: version_id.to_string(),
        membership_epoch,
        data_shards: layout.data_shards,
        parity_shards: layout.parity_shards,
        placements,
        adaptive_without_rebalance: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::membership::NodeState;

    fn node(id: &str) -> ClusterNode {
        ClusterNode {
            id: id.to_string(),
            endpoint: format!("https://{id}:50001"),
            native_endpoint: Some(format!("n2://{id}:50002")),
            state: Some(NodeState::Online),
            weight: 1,
            index_roots: vec![],
            blob_roots: vec![],
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        }
    }

    #[test]
    fn placement_is_deterministic_and_uses_required_nodes_only() {
        let nodes = vec![node("a"), node("b"), node("c"), node("d")];
        let layout = ClusterEcLayout {
            data_shards: 2,
            parity_shards: 1,
        };
        let left = build_placement_plan("frag", "obj", "ver", 7, layout, &nodes).unwrap();
        let right = build_placement_plan("frag", "obj", "ver", 7, layout, &nodes).unwrap();
        assert_eq!(left, right);
        assert_eq!(left.placements.len(), 3);
        assert_eq!(left.placements[0].role, ClusterShardRole::Data);
        assert_eq!(left.placements[2].role, ClusterShardRole::Parity);
    }
}
