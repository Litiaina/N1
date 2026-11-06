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
use n1_link::noa::Command;
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
    Ok(Json(json!({
        "architecture": "LINK",
        "fragment": claims.fragment,
        "pools": pools,
        "observation": observed,
        "object_status": object["status"],
        "error": object["error"],
        "allocation_policy": "newest complete unsealed pool",
        "metadata_store": "procyon-engine"
    })))
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

async fn scope(
    app: &App,
    claims: &auth::Claims,
    q: &HashMap<String, String>,
) -> ApiResult<Option<n1_link::PoolId>> {
    if q.get("all_fragments").is_some_and(|v| v == "true") {
        claims.require("operator")?;
        return Ok(None);
    }
    let Some(identity) = q.get("fragment") else {
        return Ok(Some(claims.id()));
    };
    let identity = identity.clone();
    let target = app
        .db
        .execute(move |db| crate::fragments::resolve(&db, &identity))
        .await?;
    if target.id != claims.id() {
        claims.require("operator")?;
    }
    Ok(Some(target.id))
}
fn number(q: &HashMap<String, String>, name: &str, default: u64) -> ApiResult<u64> {
    q.get(name)
        .map(|v| {
            v.parse()
                .map_err(|_| ApiError::invalid(format!("invalid {name}")))
        })
        .unwrap_or(Ok(default))
}
fn boolean(q: &HashMap<String, String>, name: &str, default: bool) -> ApiResult<bool> {
    match q.get(name).map(String::as_str) {
        None => Ok(default),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        _ => Err(ApiError::invalid(format!("invalid {name}"))),
    }
}
pub(super) async fn start(
    State(app): State<Arc<App>>,
    Path(kind): Path<String>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    use crate::maintenance::Kind;
    let claims = auth::authorize(&app, &headers, "maintenance").await?;
    let kind = Kind::parse(&kind)?;
    if boolean(&q, "fast_mode", false)? {
        return Err(ApiError::invalid(
            "fast_mode is not supported; use the configured bounded maintenance controls",
        ));
    }
    if q.get("durability").is_some_and(|value| value != "strict") {
        return Err(ApiError::invalid(
            "pool migration always requires strict durability",
        ));
    }
    let mut request = app
        .maintenance
        .default_request(scope(&app, &claims, &q).await?);
    request.dry_run = boolean(&q, "dry_run", false)?;
    request.repair = q.get("mode").is_none_or(|mode| mode != "check") && !request.dry_run;
    request.batch_size = number(&q, "batch", request.batch_size as u64)? as usize;
    request.object_concurrency =
        number(&q, "object_concurrency", request.object_concurrency as u64)? as usize;
    request.pause_millis = number(&q, "pause_millis", request.pause_millis)?;
    request.bytes_per_second = number(&q, "bytes_per_second", request.bytes_per_second)?;
    for (field, destination) in [
        ("source_pool", &mut request.source_pool),
        ("target_pool", &mut request.target_pool),
    ] {
        *destination = q
            .get(field)
            .map(|v| {
                v.parse()
                    .map_err(|_| ApiError::invalid(format!("invalid {field}")))
            })
            .transpose()?;
    }
    if let Some(path) = q.get("object_key") {
        let fragment = request
            .fragment
            .ok_or_else(|| ApiError::invalid("object selection requires a fragment"))?;
        let path = path.clone();
        request.object = Some(
            app.db
                .execute(move |db| Ok(Namespace::new(db).stat(fragment, &path, None)?.object_id))
                .await?,
        );
    }
    let job = app.maintenance.start(kind, request).await?;
    Ok(Json(serde_json::to_value(job).map_err(|_| {
        ApiError::invalid("invalid maintenance response")
    })?))
}
pub(super) async fn status(
    State(app): State<Arc<App>>,
    Path(kind): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "maintenance").await?;
    let value = app
        .maintenance
        .status(crate::maintenance::Kind::parse(&kind)?)
        .await?;
    if !claims.operator
        && value.get("request").is_some()
        && value["request"]["fragment"].as_str() != Some(&claims.id().to_string())
    {
        return Err(ApiError::forbidden());
    }
    Ok(Json(value))
}
pub(super) async fn stop(
    State(app): State<Arc<App>>,
    Path(kind): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "maintenance").await?;
    claims.require("operator")?;
    let kind = crate::maintenance::Kind::parse(&kind)?;
    app.maintenance.stop_job(kind).await?;
    Ok(Json(app.maintenance.status(kind).await?))
}
pub(super) async fn scoped_start(
    State(app): State<Arc<App>>,
    Path((fragment, kind)): Path<(String, String)>,
    headers: HeaderMap,
    Query(mut q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    if q.get("all_fragments").is_some_and(|v| v == "true") {
        return Err(ApiError::invalid(
            "fragment route cannot request all fragments",
        ));
    }
    q.insert("fragment".into(), fragment);
    start(State(app), Path(kind), headers, Query(q)).await
}
pub(super) async fn migrate_object(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Query(mut q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    q.insert("object_key".into(), path);
    start(State(app), Path("redistribution".into()), headers, Query(q)).await
}
pub(super) async fn fragment_init(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    auth::authorize(&app, &headers, "maintenance").await?;
    Ok(Json(json!({
        "initialized": true,
        "message": "Logical fragment metadata is transactionally initialized; LINK has no per-fragment directories"
    })))
}
