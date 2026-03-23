use crate::middleware::auth_middleware::validate_access;
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::core::operation_gate::{
    enforce_quiesced_mode_on_reads, enforce_read_only_fast_mode_on_mutations,
};
use crate::distribution_core::api::{
    get_n2_cluster_view, get_n2_cluster_view_public, get_n2_governance_state, get_n2_membership,
    post_n2_internal_governance_abort, post_n2_internal_governance_append,
    post_n2_internal_governance_vote, put_n2_membership, resolve_n2_placement,
    rollback_n2_membership,
};
use crate::distribution_core::data_plane::{
    delete_n2_object, get_n2_object, list_n2_object_versions, list_n2_objects,
    n2_internal_abort_shard, n2_internal_begin_shard, n2_internal_commit_shard,
    n2_internal_delete_map, n2_internal_delete_shard, n2_internal_read_map, n2_internal_read_shard,
    n2_internal_store_shard, n2_internal_upsert_map, n2_internal_write_shard_chunk, put_n2_object,
    reconcile_n2_fragment, rename_n2_object,
};
use axum::Router;
use axum::middleware::from_fn;
use axum::routing::{delete, get, post, put};
pub use n1::routes::storage_routes::AppState;
use n1::routes::storage_routes::{
    StorageRouteOptions, apply_runtime_http_layers, build_storage_routes_unbound_with_options,
};
use std::sync::Arc;

fn public_n2_routes(layers: &RateLimitLayers) -> Router<Arc<AppState>> {
    layers.layer_global(Router::<Arc<AppState>>::new().route(
        "/v4/n2/cluster/view/public",
        get(get_n2_cluster_view_public),
    ))
}

fn protected_n2_routes(layers: &RateLimitLayers) -> Router<Arc<AppState>> {
    let read_ops = Router::<Arc<AppState>>::new()
        .route("/v4/n2/cluster/view", get(get_n2_cluster_view))
        .route("/v4/n2/governance/state", get(get_n2_governance_state))
        .route("/v4/n2/membership", get(get_n2_membership))
        .route(
            "/v4/n2/placement/{:fragment}/{*object_key}",
            get(resolve_n2_placement),
        )
        .route("/v4/n2/object/list/{:fragment}", get(list_n2_objects))
        .route(
            "/v4/n2/object/versions/{:fragment}/{*object_key}",
            get(list_n2_object_versions),
        )
        .route(
            "/v4/n2/object/{:fragment}/{*object_key}",
            get(get_n2_object),
        );

    let read_ops = layers
        .layer_view(read_ops)
        .layer(from_fn(enforce_quiesced_mode_on_reads));

    let write_ops = Router::<Arc<AppState>>::new()
        .route("/v4/n2/membership", put(put_n2_membership))
        .route(
            "/v4/n2/membership/rollback/{:epoch}",
            post(rollback_n2_membership),
        )
        .route(
            "/v4/n2/object/{:fragment}/{*object_key}",
            post(put_n2_object),
        )
        .route(
            "/v4/n2/object/{:fragment}/{*object_key}",
            delete(delete_n2_object),
        )
        .route("/v4/n2/object/rename", post(rename_n2_object))
        .route("/v4/n2/reconcile/{:fragment}", post(reconcile_n2_fragment));

    let write_ops = layers
        .layer_upload(write_ops)
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    Router::<Arc<AppState>>::new()
        .merge(read_ops)
        .merge(write_ops)
        .layer(from_fn(validate_access))
}

fn internal_n2_routes(layers: &RateLimitLayers) -> Router<Arc<AppState>> {
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

pub async fn serve_storage_routes() -> Router {
    let layers = RateLimitLayers::new();
    let (base_router, app_state) = build_storage_routes_unbound_with_options(StorageRouteOptions {
        include_object_storage: false,
        include_legacy_fs: false,
    });

    apply_runtime_http_layers(
        base_router
            .merge(public_n2_routes(&layers))
            .merge(protected_n2_routes(&layers))
            .merge(internal_n2_routes(&layers)),
    )
    .with_state(app_state)
}
