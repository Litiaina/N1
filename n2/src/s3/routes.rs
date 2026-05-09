use crate::core::operation_gate::{
    enforce_quiesced_mode_on_reads, enforce_read_only_fast_mode_on_mutations,
};
use crate::middleware::auth_middleware::validate_access;
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::routes::AppState;
use crate::s3::{
    s3_delete_object, s3_get_object, s3_head_bucket, s3_head_object, s3_list_buckets,
    s3_list_objects_v2, s3_post_object, s3_put_object,
};
use axum::Router;
use axum::middleware::from_fn;
use axum::routing::{get, head, put};
use std::sync::Arc;

pub(crate) fn protected_s3_routes(layers: &RateLimitLayers) -> Router<Arc<AppState>> {
    let read_ops = Router::<Arc<AppState>>::new()
        .route("/s3", get(s3_list_buckets))
        .route(
            "/s3/{:bucket}",
            head(s3_head_bucket).get(s3_list_objects_v2),
        )
        .route(
            "/s3/{:bucket}/{*key}",
            get(s3_get_object).head(s3_head_object),
        );

    let read_ops = layers
        .layer_view(read_ops)
        .layer(from_fn(enforce_quiesced_mode_on_reads));

    let write_ops = Router::<Arc<AppState>>::new().route(
        "/s3/{:bucket}/{*key}",
        put(s3_put_object)
            .post(s3_post_object)
            .delete(s3_delete_object),
    );

    let write_ops = layers
        .layer_upload(write_ops)
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    Router::<Arc<AppState>>::new()
        .merge(read_ops)
        .merge(write_ops)
        .layer(from_fn(validate_access))
}
