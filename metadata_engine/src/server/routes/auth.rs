use crate::{
    fragments,
    middleware::auth,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use axum::{
    Json,
    extract::{Path, Query, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};

pub(super) async fn login(
    State(app): State<Arc<App>>,
    Json(request): Json<auth::LoginRequest>,
) -> ApiResult<Json<Value>> {
    Ok(Json(auth::login(&app, request).await?))
}
#[derive(Deserialize)]
pub(super) struct RefreshRequest {
    refresh_token: String,
}
pub(super) async fn refresh(
    State(app): State<Arc<App>>,
    Json(request): Json<RefreshRequest>,
) -> ApiResult<Json<Value>> {
    Ok(Json(
        auth::refresh(&app, request.refresh_token, false).await?,
    ))
}
pub(super) async fn logout(
    State(app): State<Arc<App>>,
    Json(request): Json<RefreshRequest>,
) -> ApiResult<Json<Value>> {
    Ok(Json(
        auth::refresh(&app, request.refresh_token, true).await?,
    ))
}
#[derive(Deserialize)]
pub(super) struct Create {
    name: String,
    secret: String,
    key: String,
}
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Json(request): Json<Create>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    if !auth::same_secret(&request.secret, &app.config.master_key) {
        return Err(ApiError::unauthorized());
    }
    let hash = auth::hash_secret(&app, request.key).await?;
    let record = app
        .db
        .execute(move |db| fragments::create(&db, &request.name, &hash))
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"fragment_hash":record.alias,"fragment_id":ulid::Ulid::from_bytes(record.id.0).to_string(),"name":record.name,"message":"Fragment created"}),
        ),
    ))
}
pub(super) async fn list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let identity = claims.fragment.clone();
    let record = app
        .db
        .execute(move |db| fragments::resolve(&db, &identity))
        .await?;
    Ok(Json(json!([record.alias])))
}
pub(super) async fn hash(Query(query): Query<HashMap<String, String>>) -> ApiResult<Json<Value>> {
    let name = query
        .get("name")
        .or_else(|| query.get("fragment"))
        .ok_or_else(|| ApiError::invalid("fragment name is required"))?;
    Ok(Json(json!({"fragment_hash":fragments::alias(name)})))
}
pub(super) async fn check(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let name = query
        .get("fragment")
        .or_else(|| query.get("name"))
        .ok_or_else(|| ApiError::invalid("fragment is required"))?
        .clone();
    let exists = app
        .db
        .execute(move |db| match fragments::resolve(&db, &name) {
            Ok(_) => Ok(true),
            Err(crate::Error::NotFound) => Ok(false),
            Err(e) => Err(e),
        })
        .await?;
    Ok(Json(json!({"exists":exists})))
}
#[derive(Deserialize)]
pub(super) struct Update {
    new_name: Option<String>,
    new_key: Option<String>,
    secret: Option<String>,
}
pub(super) async fn update(
    State(app): State<Arc<App>>,
    Path(identity): Path<String>,
    headers: HeaderMap,
    Json(request): Json<Update>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "session").await?;
    let record = app
        .db
        .execute(move |db| fragments::resolve(&db, &identity))
        .await?;
    let administrator = request
        .secret
        .as_deref()
        .is_some_and(|s| auth::same_secret(s, &app.config.master_key));
    if !administrator && record.id != claims.id() {
        return Err(ApiError::forbidden());
    }
    if request.new_name.is_some() && !administrator {
        return Err(ApiError::forbidden());
    }
    if request.new_key.is_some() && !administrator {
        claims.require("modify")?;
    }
    let key_hash = match request.new_key {
        Some(key) => Some(auth::hash_secret(&app, key).await?),
        None => None,
    };
    let record = app
        .db
        .execute(move |db| fragments::update(&db, &record, request.new_name, key_hash, false))
        .await?;
    Ok(Json(
        json!({"fragment_hash":record.alias,"name":record.name,"message":"Fragment updated"}),
    ))
}
#[derive(Deserialize)]
pub(super) struct Remove {
    secret: String,
    confirm_fragment: String,
}
pub(super) async fn remove(
    State(app): State<Arc<App>>,
    Path(identity): Path<String>,
    headers: HeaderMap,
    Json(request): Json<Remove>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "delete").await?;
    if !auth::same_secret(&request.secret, &app.config.master_key) {
        return Err(ApiError::unauthorized());
    }
    if request.confirm_fragment != identity && request.confirm_fragment != claims.fragment {
        return Err(ApiError::invalid(
            "confirm_fragment must exactly match the URL fragment or stable fragment id",
        ));
    }
    let record = app
        .db
        .execute(move |db| fragments::resolve(&db, &identity))
        .await?;
    if record.id != claims.id() {
        return Err(ApiError::forbidden());
    }
    app.db
        .execute(move |db| fragments::update(&db, &record, None, None, true).map(|_| ()))
        .await?;
    Ok(Json(json!({"deleted":true})))
}
pub(super) async fn ws(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    socket: WebSocketUpgrade,
) -> ApiResult<Response> {
    let mut headers = headers;
    if let Some(token) = query.get("token") {
        headers.insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .map_err(|_| ApiError::unauthorized())?,
        );
    }
    let claims = auth::authorize(&app, &headers, "read").await?;
    let mut events = app.events.subscribe();
    Ok(socket.on_upgrade(move |mut socket| async move {
        let expires = tokio::time::sleep(std::time::Duration::from_secs(claims.exp.saturating_sub(auth::now()))); tokio::pin!(expires);
        loop { tokio::select! {
            _ = &mut expires => break,
            message = socket.recv() => { if message.is_none() || matches!(message, Some(Err(_)) | Some(Ok(axum::extract::ws::Message::Close(_)))) { break; } },
            event = events.recv() => match event {
                Ok(event) if event["fragment"].as_str() == Some(claims.fragment.as_str()) => { if socket.send(axum::extract::ws::Message::Text(json!({"type":"EVENT","payload":event}).to_string().into())).await.is_err() { break; } },
                Ok(_) => (), Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => { let _ = socket.send(axum::extract::ws::Message::Text(json!({"type":"EVENT","payload":{"event":"NAMESPACE_CHANGED","fragment":claims.fragment}}).to_string().into())).await; }, Err(_) => break,
            }
        } }
    }))
}
