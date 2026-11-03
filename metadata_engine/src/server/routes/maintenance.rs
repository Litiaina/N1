use crate::{
    middleware::auth,
    namespace::Namespace,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
};
use n1_link::noa::{Command, ScrubOptions};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
pub(super) async fn health(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    auth::authorize(&app, &headers, "read").await?;
    let mut health = app.service.objects().control(Command::Health).await?;
    health["metadata"] = json!({"engine":"procyon-engine","namespace":true});
    Ok(Json(health))
}
pub(super) async fn pools(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    auth::authorize(&app, &headers, "read").await?;
    Ok(Json(app.service.objects().control(Command::Pools).await?))
}
pub(super) async fn capacity(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    auth::authorize(&app, &headers, "read").await?;
    let value = app.service.objects().control(Command::Health).await?;
    Ok(Json(value["capacity"].clone()))
}
pub(super) async fn fragment_capacity(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let id = claims.id();
    let (mut bytes, mut objects) = (0, 0);
    let mut after = None;
    loop {
        let cursor = after.clone();
        let page = app
            .db
            .execute(move |db| Namespace::new(db).list(id, "", cursor.as_deref(), 1000))
            .await?;
        if page.files.is_empty() {
            break;
        }
        after = page.after;
        for file in page.files {
            bytes += file.version.total_size_bytes;
            objects += 1;
        }
    }
    Ok(Json(
        json!({"fragment":claims.fragment,"used_bytes":bytes,"object_count":objects}),
    ))
}
pub(super) async fn object_status(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let id = claims.id();
    let stored = app
        .db
        .execute(move |db| Namespace::new(db).resolve(id, &path, None))
        .await?;
    Ok(Json(
        app.service
            .objects()
            .control(Command::ObjectHealth {
                object_key: stored.object_key,
                deep: q
                    .get("mode")
                    .is_some_and(|s| s == "checksum" || s == "deep"),
            })
            .await?,
    ))
}
pub(super) async fn object_repair(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "maintenance").await?;
    let id = claims.id();
    let stored = app
        .db
        .execute(move |db| Namespace::new(db).resolve(id, &path, None))
        .await?;
    Ok(Json(
        app.service
            .objects()
            .control(Command::ObjectRepair {
                object_key: stored.object_key,
            })
            .await?,
    ))
}
pub(super) async fn start(
    State(app): State<Arc<App>>,
    Path(kind): Path<String>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "maintenance").await?;
    claims.require("operator")?;
    let command = match kind.as_str() {
        "gc" => Command::GarbageCollect,
        "repair" => Command::Scrub {
            options: ScrubOptions {
                repair: true,
                bytes_per_second: q
                    .get("bytes_per_second")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(16 * 1024 * 1024),
                window_seconds: q
                    .get("window_seconds")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(3600),
            },
        },
        "redistribution" => {
            return Err(ApiError::invalid(
                "immutable LINK pools are expanded by enrolling a complete new pool; existing objects retain their pool",
            ));
        }
        _ => return Err(ApiError::invalid("unknown maintenance job")),
    };
    Ok(Json(app.service.objects().control(command).await?))
}
pub(super) async fn status(
    State(app): State<Arc<App>>,
    Path(kind): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    auth::authorize(&app, &headers, "maintenance").await?;
    if !["gc", "repair", "redistribution"].contains(&kind.as_str()) {
        return Err(ApiError::invalid("unknown maintenance job"));
    }
    let mut value = app
        .service
        .objects()
        .control(Command::MaintenanceJob)
        .await?;
    let running = value
        .get("state")
        .and_then(Value::as_str)
        .is_some_and(|s| s == "running" || s == "stopping");
    value["running"] = json!(running);
    Ok(Json(value))
}
pub(super) async fn stop(
    State(app): State<Arc<App>>,
    Path(_kind): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "maintenance").await?;
    claims.require("operator")?;
    let state = app
        .service
        .objects()
        .control(Command::MaintenanceJob)
        .await?;
    let id = state
        .get("id")
        .or_else(|| state.get("job_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::invalid("no maintenance job is active"))?;
    Ok(Json(
        app.service
            .objects()
            .control(Command::MaintenanceStop { job_id: id.into() })
            .await?,
    ))
}
pub(super) async fn fragment_init(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    auth::authorize(&app, &headers, "maintenance").await?;
    Ok(Json(
        json!({"initialized":true,"message":"Logical fragment metadata is transactionally initialized; LINK has no per-fragment directories"}),
    ))
}
