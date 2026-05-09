use crate::distribution_core::config::n2_runtime_config;
use crate::distribution_core::model::{N2ClusterShardRef, NodeDescriptor};
use crate::distribution_core::peer::normalize_endpoint;
use crate::middleware::auth_middleware::ADMIN_KEY;
use crate::structs::auth_structs::Claims;

pub(crate) fn is_admin_claim(claims: &Claims) -> bool {
    claims.fragment == *ADMIN_KEY
}

pub(crate) fn is_local_target(target: &NodeDescriptor) -> bool {
    let cfg = n2_runtime_config();
    target.node_id == cfg.node_id
        || normalize_endpoint(&target.endpoint) == normalize_endpoint(&cfg.node_endpoint)
}

pub(crate) fn shard_matches_target(shard: &N2ClusterShardRef, target: &NodeDescriptor) -> bool {
    shard.node_id == target.node_id
        || normalize_endpoint(&shard.endpoint) == normalize_endpoint(&target.endpoint)
}
