use crate::distribution_core::api::{
    post_n2_internal_governance_abort, post_n2_internal_governance_append,
    post_n2_internal_governance_vote,
};
use crate::distribution_core::peer_api::{
    n2_internal_abort_shard, n2_internal_begin_shard, n2_internal_commit_shard,
    n2_internal_delete_map, n2_internal_delete_shard, n2_internal_read_map, n2_internal_read_shard,
    n2_internal_store_shard, n2_internal_upsert_map, n2_internal_write_shard_chunk,
};
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::routes::AppState;
use axum::Router;
use axum::routing::{delete, get, post, put};
use std::sync::Arc;

pub(crate) fn internal_n2_routes(layers: &RateLimitLayers) -> Router<Arc<AppState>> {
    let routes = Router::<Arc<AppState>>::new()
        .route(
            "/v4/n2/internal/session/begin/{:fragment}/{:version_id}/{:cluster_shard_index}",
            post(n2_internal_begin_shard),
        )
        .route(
            "/v4/n2/internal/session/chunk/{:session_id}",
            put(n2_internal_write_shard_chunk),
        )
        .route(
            "/v4/n2/internal/session/commit/{:session_id}",
            post(n2_internal_commit_shard),
        )
        .route(
            "/v4/n2/internal/session/abort/{:session_id}",
            post(n2_internal_abort_shard),
        )
        .route(
            "/v4/n2/internal/store/{:fragment}/{:version_id}/{:cluster_shard_index}",
            post(n2_internal_store_shard),
        )
        .route(
            "/v4/n2/internal/read/{:fragment}/{:version_id}/{:cluster_shard_index}",
            get(n2_internal_read_shard),
        )
        .route(
            "/v4/n2/internal/delete/{:fragment}/{:version_id}/{:cluster_shard_index}",
            delete(n2_internal_delete_shard),
        )
        .route("/v4/n2/internal/map/upsert", post(n2_internal_upsert_map))
        .route("/v4/n2/internal/map/{:fragment}", get(n2_internal_read_map))
        .route(
            "/v4/n2/internal/map/{:fragment}",
            delete(n2_internal_delete_map),
        )
        .route(
            "/v4/n2/internal/governance/request-vote",
            post(post_n2_internal_governance_vote),
        )
        .route(
            "/v4/n2/internal/governance/append",
            post(post_n2_internal_governance_append),
        )
        .route(
            "/v4/n2/internal/governance/abort",
            post(post_n2_internal_governance_abort),
        );

    layers.layer_global(routes)
}
