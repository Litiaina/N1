use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::routes::native::{protected_n2_routes, public_n2_routes};
use crate::routes::peer::internal_n2_routes;
use crate::s3::routes::protected_s3_routes;
use axum::Router;
use n1::routes::storage_routes::{
    StorageRouteOptions, apply_runtime_http_layers, build_storage_routes_unbound_with_options,
};

pub async fn serve_storage_routes() -> Router {
    let layers = RateLimitLayers::new();
    let (base_router, app_state) = build_storage_routes_unbound_with_options(StorageRouteOptions {
        include_object_storage: false,
    });

    apply_runtime_http_layers(
        base_router
            .merge(public_n2_routes(&layers))
            .merge(protected_n2_routes(&layers))
            .merge(protected_s3_routes(&layers))
            .merge(internal_n2_routes(&layers)),
    )
    .with_state(app_state)
}
