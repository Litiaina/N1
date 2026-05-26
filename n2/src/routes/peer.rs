use crate::distribution_core::api::{
    post_n2_internal_governance_abort, post_n2_internal_governance_append,
    post_n2_internal_governance_vote,
};
use crate::distribution_core::health::{get_n2_internal_node_health, get_n2_internal_shard_health};
use crate::distribution_core::peer_api::{
    n2_internal_abort_shard, n2_internal_begin_shard, n2_internal_commit_shard,
    n2_internal_delete_object_record, n2_internal_delete_shard, n2_internal_read_object_record,
    n2_internal_read_shard, n2_internal_rename_namespace, n2_internal_store_shard,
    n2_internal_upsert_object_record, n2_internal_write_shard_chunk,
};
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::routes::AppState;
use axum::Router;
use axum::routing::{delete, get, post, put};
use std::sync::Arc;

pub(crate) fn internal_n2_routes(layers: &RateLimitLayers) -> Router<Arc<AppState>> {
    let routes = Router::<Arc<AppState>>::new()
        .route(
            "/n2/internal/session/begin/{:fragment}/{:version_id}/{:cluster_shard_index}",
            post(n2_internal_begin_shard),
        )
        .route(
            "/n2/internal/session/chunk/{:session_id}",
            put(n2_internal_write_shard_chunk),
        )
        .route(
            "/n2/internal/session/commit/{:session_id}",
            post(n2_internal_commit_shard),
        )
        .route(
            "/n2/internal/session/abort/{:session_id}",
            post(n2_internal_abort_shard),
        )
        .route(
            "/n2/internal/store/{:fragment}/{:version_id}/{:cluster_shard_index}",
            post(n2_internal_store_shard),
        )
        .route(
            "/n2/internal/read/{:fragment}/{:version_id}/{:cluster_shard_index}",
            get(n2_internal_read_shard),
        )
        .route(
            "/n2/internal/delete/{:fragment}/{:version_id}/{:cluster_shard_index}",
            delete(n2_internal_delete_shard),
        )
        .route(
            "/n2/internal/object-record/upsert",
            post(n2_internal_upsert_object_record),
        )
        .route(
            "/n2/internal/object-record/{:fragment}",
            get(n2_internal_read_object_record),
        )
        .route(
            "/n2/internal/object-record/{:fragment}",
            delete(n2_internal_delete_object_record),
        )
        .route(
            "/n2/internal/namespace/rename",
            post(n2_internal_rename_namespace),
        )
        .route("/n2/internal/health/node", get(get_n2_internal_node_health))
        .route(
            "/n2/internal/health/shard/{:fragment}/{:version_id}/{:cluster_shard_index}",
            get(get_n2_internal_shard_health),
        )
        .route(
            "/n2/internal/governance/request-vote",
            post(post_n2_internal_governance_vote),
        )
        .route(
            "/n2/internal/governance/append",
            post(post_n2_internal_governance_append),
        )
        .route(
            "/n2/internal/governance/abort",
            post(post_n2_internal_governance_abort),
        );

    layers.layer_global(routes)
}
