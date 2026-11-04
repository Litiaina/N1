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
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    if q.get("deep_scan").is_some_and(|s| s == "true") {
        app.diagnostics.start(app.clone(), claims.id(), None)?;
    }
    let mut value = super::super::monitoring::health(&app).await;
    // Explicit diagnostics may request live in-memory counters. Routine Console
    // polling never takes this path or bypasses filesystem observation caching.
    if q.get("live_counters").is_some_and(|s| s == "true") {
        let live = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            app.monitor_objects.control(Command::Health),
        )
        .await
        .map_err(|_| ApiError::invalid("live object counters timed out"))??;
        value["io"] = live["io"].clone();
        value["operations"] = live["operations"].clone();
    }
    Ok(Json(value))
}
pub(super) async fn fragment_health(
    State(app): State<Arc<App>>,
    Path(fragment): Path<String>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let target = app
        .db
        .execute(move |db| crate::fragments::resolve(&db, &fragment))
        .await?;
    if target.id != claims.id() && !claims.operator {
        return Err(ApiError::forbidden());
    }
    if q.get("deep_scan").is_some_and(|s| s == "true") {
        app.diagnostics.start(app.clone(), target.id, None)?;
    }
    let mut health = super::super::monitoring::health(&app).await;
    health["fragment"] = json!(ulid::Ulid::from_bytes(target.id.0).to_string());
    Ok(Json(health))
}
pub(super) async fn pools(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let observed = super::super::monitoring::object(&app).await;
    let object = observed.value.as_ref().cloned().unwrap_or(Value::Null);
    let mut pools = object["pools"].as_array().cloned().unwrap_or_default();
    if let Some(id) = q.get("pool").filter(|s| !s.is_empty()) {
        pools.retain(|pool| pool["id"].as_str() == Some(id));
        if pools.is_empty() && object["pools"].is_array() {
            return Err(ApiError::invalid("unknown pool"));
        }
    }
    if q.get("deep_scan").is_some_and(|s| s == "true") {
        app.diagnostics
            .start(app.clone(), claims.id(), q.get("pool").cloned())?;
    }
    for pool in &mut pools {
        if let Some(integrity) = app
            .diagnostics
            .status(claims.id(), pool["id"].as_str().unwrap_or(""))
        {
            pool["integrity"] = integrity;
        }
    }
    Ok(Json(
        json!({"architecture":"LINK","fragment":claims.fragment,"pools":pools,"observation":observed,
        "object_status":object["status"],"error":object["error"],
        "allocation_policy":"newest complete unsealed pool","metadata_store":"procyon-engine"}),
    ))
}
pub(super) async fn capacity(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    auth::authorize(&app, &headers, "read").await?;
    let observed = super::super::monitoring::object(&app).await;
    Ok(Json(
        observed
            .value
            .map(|v| v["capacity"].clone())
            .unwrap_or(Value::Null),
    ))
}
pub(super) async fn fragment_capacity(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let id = claims.id();
    let usage = app
        .db
        .execute(move |db| Namespace::new(db).usage(id))
        .await?;
    let observed = super::super::monitoring::object(&app).await;
    let mut value = observed
        .value
        .map(|v| v["capacity"].clone())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    value["architecture"] = json!("LINK");
    value["fragment"] = json!(claims.fragment);
    value["used_bytes"] = json!(usage.as_ref().map(|s| s.used_bytes));
    value["object_count"] = json!(usage.as_ref().map(|s| s.object_count));
    value["measurement"] = json!(if usage.is_some() {
        "transaction_counters"
    } else {
        "not_initialized"
    });
    value["objects_scanned"] = json!(0);
    Ok(Json(value))
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
    let value = app
        .service
        .objects()
        .control(Command::MaintenanceJob)
        .await?;
    let matches = match kind.as_str() {
        "repair" => value["kind"] == "scrub",
        "gc" => value["kind"] == "gc",
        _ => false,
    };
    if !matches {
        return Ok(Json(json!({"kind":kind,"running":false,"phase":"idle"})));
    }
    let running = value
        .get("phase")
        .and_then(Value::as_str)
        .is_some_and(|s| s == "running" || s == "stopping");
    let mut value = value;
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
