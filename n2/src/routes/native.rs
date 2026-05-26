use crate::core::operation_gate::{
    enforce_quiesced_mode_on_reads, enforce_read_only_fast_mode_on_mutations,
};
use crate::distribution_core::api::{
    get_n2_cluster_view, get_n2_cluster_view_public, get_n2_governance_state, get_n2_membership,
    put_n2_membership, resolve_n2_placement, rollback_n2_membership,
};
use crate::distribution_core::data_plane::{
    delete_n2_object, get_n2_object, list_n2_object_versions, list_n2_objects, put_n2_object,
    reconcile_n2_fragment, rename_n2_object,
};
use crate::distribution_core::health::{
    get_n2_cluster_health, get_n2_fragment_health, get_n2_node_health, get_n2_object_health,
    repair_n2_object,
};
use crate::middleware::auth_middleware::validate_access;
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::routes::AppState;
use axum::Router;
use axum::middleware::from_fn;
use axum::routing::{delete, get, post, put};
use std::sync::Arc;

pub(crate) fn public_n2_routes(layers: &RateLimitLayers) -> Router<Arc<AppState>> {
    layers.layer_global(
        Router::<Arc<AppState>>::new()
            .route("/n2/cluster/view/public", get(get_n2_cluster_view_public)),
    )
}

pub(crate) fn protected_n2_routes(layers: &RateLimitLayers) -> Router<Arc<AppState>> {
    let read_ops = Router::<Arc<AppState>>::new()
        .route("/n2/cluster/view", get(get_n2_cluster_view))
        .route("/n2/health/cluster", get(get_n2_cluster_health))
        .route("/n2/health/node", get(get_n2_node_health))
        .route(
            "/n2/health/fragment/{:fragment}",
            get(get_n2_fragment_health),
        )
        .route(
            "/n2/health/object/{:fragment}/{*object_key}",
            get(get_n2_object_health),
        )
        .route("/n2/governance/state", get(get_n2_governance_state))
        .route("/n2/membership", get(get_n2_membership))
        .route(
            "/n2/placement/{:fragment}/{*object_key}",
            get(resolve_n2_placement),
        )
        .route("/n2/object/list/{:fragment}", get(list_n2_objects))
        .route(
            "/n2/object/versions/{:fragment}/{*object_key}",
            get(list_n2_object_versions),
        )
        .route("/n2/object/{:fragment}/{*object_key}", get(get_n2_object));

    let read_ops = layers
        .layer_view(read_ops)
        .layer(from_fn(enforce_quiesced_mode_on_reads));

    let write_ops = Router::<Arc<AppState>>::new()
        .route("/n2/membership", put(put_n2_membership))
        .route(
            "/n2/membership/rollback/{:epoch}",
            post(rollback_n2_membership),
        )
        .route("/n2/object/{:fragment}/{*object_key}", post(put_n2_object))
        .route(
            "/n2/object/{:fragment}/{*object_key}",
            delete(delete_n2_object),
        )
        .route("/n2/object/rename", post(rename_n2_object))
        .route(
            "/n2/repair/object/{:fragment}/{*object_key}",
            post(repair_n2_object),
        )
        .route("/n2/reconcile/{:fragment}", post(reconcile_n2_fragment));

    let write_ops = layers
        .layer_upload(write_ops)
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    Router::<Arc<AppState>>::new()
        .merge(read_ops)
        .merge(write_ops)
        .layer(from_fn(validate_access))
}
