use crate::distribution::membership::ClusterRuntime;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterRepairPolicy {
    pub object_repair_scope: String,
    pub fragment_repair_scope: String,
    pub byte_payload_consensus: bool,
    pub namespace_consensus_required: bool,
}

pub fn repair_policy(_runtime: &ClusterRuntime) -> ClusterRepairPolicy {
    ClusterRepairPolicy {
        object_repair_scope: "object placement record and missing shard/parity holders".to_string(),
        fragment_repair_scope: "namespace ledger, object records, and local node storage health"
            .to_string(),
        byte_payload_consensus: false,
        namespace_consensus_required: true,
    }
}
