mod auth;
pub(crate) mod cursor;
mod maintenance;
mod namespace;
pub(crate) mod read;
mod upload;
use super::App;
use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{delete, get, patch, post, put},
};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route(
            "/noa/v1/ping",
            get(|| async {
                axum::Json(serde_json::json!({
                    "status": "ok",
                    "engine": "m1",
                    "version": env!("CARGO_PKG_VERSION")
                }))
            }),
        )
        .route("/noa/v1/auth/fragment", post(auth::login))
        .route("/noa/v1/auth/refresh", post(auth::refresh))
        .route("/noa/v1/auth/logout", post(auth::logout))
        .route("/noa/v1/fragments", get(auth::list).post(auth::create))
        .route("/noa/v1/fragments/check", get(auth::check))
        .route("/noa/v1/fragments/hash", get(auth::hash))
        .route(
            "/noa/v1/fragments/{fragment}",
            patch(auth::update).delete(auth::remove),
        )
        .route("/noa/v1/objects", get(namespace::objects))
        .route("/noa/v1/posix/list", get(namespace::list))
        .route("/noa/v1/posix/stat", get(namespace::stat))
        .route("/noa/v1/posix/mkdir", post(namespace::mkdir))
        .route("/noa/v1/posix/rename", post(namespace::rename))
        .route("/noa/v1/posix/move", post(namespace::rename))
        .route("/noa/v1/objects/rename", post(namespace::rename))
        .route("/noa/v1/posix/trash", post(namespace::trash))
        .route("/noa/v1/posix/recover", post(namespace::recover))
        .route("/noa/v1/objects/bulk-delete", post(namespace::bulk_delete))
        .route(
            "/noa/v1/objects/bulk-recover",
            post(namespace::bulk_recover),
        )
        .route("/noa/v1/posix/purge", post(namespace::retired))
        .route(
            "/noa/v1/objects/permanent/bulk-delete",
            post(namespace::retired),
        )
        .route(
            "/noa/v1/objects/version/{version}/{*key}",
            delete(namespace::retired),
        )
        .route(
            "/noa/v1/objects/permanent/{*key}",
            delete(namespace::retired),
        )
        .route("/noa/v1/objects/{*key}", delete(namespace::delete))
        .route(
            "/noa/v1/objects/recover/{*key}",
            post(namespace::recover_key),
        )
        .route("/noa/v1/trash", get(namespace::trash_list))
        .route("/noa/v1/trash/recover-all", post(namespace::recover_all))
        .route("/noa/v1/namespace/snapshot", get(namespace::snapshot))
        .route("/noa/v1/namespace/search", get(namespace::search))
        .route("/noa/v1/objects/meta/{*key}", get(read::metadata))
        .route("/noa/v1/objects/versions/{*key}", get(read::versions))
        .route("/noa/v1/objects/get/{*key}", get(read::get))
        .route("/noa/v1/objects/stream/{*key}", get(read::get))
        .route("/noa/v1/objects/download", get(read::download))
        .route("/noa/v1/objects/download/chunk", get(read::chunk))
        .route("/noa/v1/objects/signed-url", post(read::signed_url))
        .route("/noa/v1/objects/signed-read", get(read::signed_read))
        .route(
            "/noa/v1/objects/archive",
            post(crate::exports::archive::selected).layer(DefaultBodyLimit::max(4 * 1024 * 1024)),
        )
        .route(
            "/noa/v1/objects/archive/manifest",
            post(crate::exports::archive::manifest),
        )
        .route("/noa/v1/upload/one-shot/{*key}", post(upload::one_shot))
        .route(
            "/noa/v1/upload/batch",
            post(upload::batch).layer(DefaultBodyLimit::max(5 * 1024 * 1024 * 1024)),
        )
        .route("/noa/v1/upload/multipart/init/{*key}", post(upload::init))
        .route(
            "/noa/v1/upload/multipart/durable/init/{*key}",
            post(upload::durable_init),
        )
        .route("/noa/v1/upload/multipart", get(upload::list))
        .route(
            "/noa/v1/upload/multipart/durable",
            get(upload::durable_list),
        )
        .route(
            "/noa/v1/upload/multipart/{version}",
            get(upload::status).delete(upload::cancel),
        )
        .route(
            "/noa/v1/upload/multipart/durable/{version}",
            get(upload::durable_status).delete(upload::durable_cancel),
        )
        .route(
            "/noa/v1/upload/multipart/{version}/{part}",
            put(upload::part),
        )
        .route(
            "/noa/v1/upload/multipart/durable/{version}/{part}",
            put(upload::durable_part),
        )
        .route(
            "/noa/v1/upload/multipart/{version}/{session}/finalize",
            post(upload::finalize),
        )
        .route(
            "/noa/v1/upload/multipart/durable/{version}/{session}/finalize",
            post(upload::durable_finalize),
        )
        .route("/noa/v1/maintenance/health", get(maintenance::health))
        .route(
            "/noa/v1/maintenance/metadata/backups",
            get(super::backups::list).post(super::backups::create),
        )
        .route(
            "/noa/v1/maintenance/metadata/backups/{id}/download",
            get(super::backups::download),
        )
        .route(
            "/noa/v1/maintenance/metadata/backups/{id}/signed-url",
            post(super::backups::signed_url),
        )
        .route(
            "/noa/v1/maintenance/metadata/signed-backup",
            get(super::backups::signed_download),
        )
        .route(
            "/noa/v1/maintenance/fragments/{fragment}/health",
            get(maintenance::fragment_health),
        )
        .route("/noa/v1/maintenance/pools/health", get(maintenance::pools))
        .route("/noa/v1/capacity/usage", get(maintenance::capacity))
        .route(
            "/noa/v1/capacity/fragment",
            get(maintenance::fragment_capacity),
        )
        .route(
            "/noa/v1/objects/status/{*key}",
            get(maintenance::object_status),
        )
        .route(
            "/noa/v1/objects/repair/{*key}",
            post(maintenance::object_repair),
        )
        .route("/noa/v1/maintenance/{kind}/start", post(maintenance::start))
        .route(
            "/noa/v1/maintenance/fragments/{fragment}/{kind}/start",
            post(maintenance::scoped_start),
        )
        .route(
            "/noa/v1/maintenance/objects/redistribution/start/{*key}",
            post(maintenance::migrate_object),
        )
        .route(
            "/noa/v1/maintenance/{kind}/status",
            get(maintenance::status),
        )
        .route("/noa/v1/maintenance/{kind}/stop", post(maintenance::stop))
        .route(
            "/noa/v1/maintenance/fragment/init",
            post(maintenance::fragment_init),
        )
        .route(
            "/noa/v1/maintenance/fragment/cleanup",
            post(maintenance::fragment_init),
        )
        .route("/noa/v1/ws", get(auth::ws))
        .route("/noa/v1/exports", post(crate::exports::routes::create))
        .route(
            "/noa/v1/exports/{id}",
            get(crate::exports::routes::status).delete(crate::exports::routes::cancel),
        )
        .route(
            "/noa/v1/exports/{id}/download",
            get(crate::exports::routes::download),
        )
        .route(
            "/noa/v1/exports/{id}/signed-url",
            post(crate::exports::routes::signed_url),
        )
        .route(
            "/noa/v1/signed-exports/download",
            get(crate::exports::routes::signed_download),
        )
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            crate::middleware::rate_limiter::admit,
        ))
        .layer(DefaultBodyLimit::max(4 * 1024 * 1024))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
                .expose_headers(Any),
        )
        .with_state(app)
}
