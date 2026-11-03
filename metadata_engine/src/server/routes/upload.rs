use super::namespace::version_json;
use crate::{
    middleware::auth,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
    service::UploadRequest,
    uploads::coordinator as uploads,
};
use axum::{
    Json,
    body::Body,
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use futures_util::TryStreamExt;
use n1_link::PoolId;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
type Q = HashMap<String, String>;
fn strict(app: &App, q: &Q) -> ApiResult<bool> {
    match q.get("durability").map(String::as_str) {
        None => Ok(app.config.strict_parts),
        Some("strict") => Ok(true),
        Some("batched") => Ok(false),
        _ => Err(ApiError::invalid("durability must be strict or batched")),
    }
}
fn reader(
    body: Body,
) -> tokio_util::io::StreamReader<
    impl futures_util::Stream<Item = std::io::Result<bytes::Bytes>> + Unpin,
    bytes::Bytes,
> {
    tokio_util::io::StreamReader::new(body.into_data_stream().map_err(std::io::Error::other))
}
pub(super) async fn one_shot(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Query(q): Query<Q>,
    body: Body,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let claims = auth::authorize(&app, &headers, "write").await?;
    let id = claims.id();
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    crate::namespace::store::validate_upload(&path, &content_type)?;
    let durable = strict(&app, &q)?;
    let length = headers
        .get("content-length")
        .map(|v| {
            v.to_str()
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or_else(|| ApiError::invalid("invalid content length"))
        })
        .transpose()?;
    let _permit = app
        .upload_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::invalid("upload service is shutting down"))?;
    let mut source = reader(body);
    let result = if let Some(length) = length {
        put_stream(
            &app,
            id,
            path.clone(),
            content_type,
            length,
            durable,
            &mut source,
        )
        .await?
    } else {
        let temp = app
            .config
            .staging_dir
            .join(format!("one-shot-{}.tmp", ulid::Ulid::new()));
        let mut options = tokio::fs::OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temp).await?;
        let result = async {
            // Stream unknown-length bodies to disk; an absent policy cap does
            // not change memory admission or LINK's supported source length.
            let maximum = app.config.max_file_size.unwrap_or(i64::MAX as u64);
            let copied = tokio::io::copy(&mut (&mut source).take(maximum + 1), &mut file).await?;
            app.config.check_object_size(copied)?;
            file.flush().await?;
            use tokio::io::AsyncSeekExt;
            file.rewind().await?;
            put_stream(
                &app,
                id,
                path.clone(),
                content_type,
                copied,
                durable,
                &mut file,
            )
            .await
        }
        .await;
        let _ = tokio::fs::remove_file(temp).await;
        result?
    };
    app.emit(id);
    Ok((StatusCode::CREATED, Json(version_json(&path, &result))))
}
pub(crate) async fn put_stream<R: AsyncRead + Unpin>(
    app: &App,
    id: PoolId,
    path: String,
    content_type: String,
    length: u64,
    durable: bool,
    source: &mut R,
) -> ApiResult<crate::namespace::PublishedVersion> {
    app.config.check_object_size(length)?;
    let pool = app
        .pools
        .iter()
        .rev()
        .find(|p| !p.sealed)
        .ok_or_else(|| ApiError::invalid("all object pools are sealed"))?;
    let stripe = u64::from(pool.data_shards) * u64::from(pool.block_bytes);
    let max_part = (app.config.max_part_size / stripe).max(1) * stripe;
    let part = if length <= max_part {
        length.max(1).div_ceil(stripe) * stripe
    } else {
        max_part
    };
    let upload = app
        .service
        .begin_upload(UploadRequest {
            fragment: id,
            path,
            content_type,
            pool: Some(pool.id),
            total_bytes: length,
            part_bytes: part,
            durable,
            ttl_seconds: 86400,
        })
        .await?;
    if upload.expected_parts <= 1 {
        return Ok(app
            .service
            .put(id, &upload.version_id, &upload.session_id, source)
            .await?);
    }
    for index in 0..upload.expected_parts {
        let bytes = length.saturating_sub(index * part).min(part);
        app.service
            .upload_part(
                id,
                &upload.version_id,
                index,
                bytes,
                &mut (&mut *source).take(bytes),
            )
            .await?;
    }
    Ok(app
        .service
        .finalize(id, &upload.version_id, &upload.session_id)
        .await?)
}
pub(super) async fn batch(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<Q>,
    mut multipart: Multipart,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let claims = auth::authorize(&app, &headers, "write").await?;
    let id = claims.id();
    let durable = strict(&app, &q)?;
    let concurrency = q
        .get("concurrency")
        .map(|v| {
            v.parse::<usize>()
                .map_err(|_| ApiError::invalid("invalid batch concurrency"))
        })
        .transpose()?
        .unwrap_or(8);
    if !(1..=128).contains(&concurrency) {
        return Err(ApiError::invalid("batch concurrency must be 1 through 128"));
    }
    let mut results = Vec::new();
    let mut group = Vec::new();
    let mut group_bytes = 0;
    let mut count = 0;
    let group_budget = app.config.upload_memory_budget.min(8 * 1024 * 1024);
    let units = app
        .config
        .upload_memory_budget
        .min(9 * 1024 * 1024)
        .div_ceil(65536);
    let _memory = app
        .upload_memory
        .clone()
        .acquire_many_owned(units as u32)
        .await
        .map_err(|_| ApiError::invalid("upload memory admission closed"))?;
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|_| ApiError::invalid("invalid batch multipart body"))?
    {
        if !group.is_empty() && (group.len() >= 64 || group_bytes + 1024 * 1024 > group_budget) {
            results.extend(
                batch_group(
                    app.clone(),
                    id,
                    std::mem::take(&mut group),
                    durable,
                    concurrency.min(16),
                )
                .await,
            );
            group_bytes = 0;
        }
        count += 1;
        if count > 4096 {
            return Err(ApiError::invalid("batch contains more than 4096 objects"));
        }
        if field.name() != Some("object") {
            return Err(ApiError::invalid("batch fields must be named object"));
        }
        let name = field
            .file_name()
            .ok_or_else(|| ApiError::invalid("batch object filename is required"))?;
        let path = percent_encoding::percent_decode_str(name)
            .decode_utf8()
            .map_err(|_| ApiError::invalid("invalid encoded object key"))?
            .into_owned();
        let content_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_owned();
        let mut bytes = Vec::new();
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|_| ApiError::invalid("batch part stream failed"))?
        {
            if bytes.len() + chunk.len() > 1024 * 1024 {
                return Err(ApiError(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "batch objects must not exceed 1 MiB".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if !group.is_empty() && (group.len() >= 64 || group_bytes + bytes.len() > 8 * 1024 * 1024) {
            results.extend(
                batch_group(
                    app.clone(),
                    id,
                    std::mem::take(&mut group),
                    durable,
                    concurrency.min(16),
                )
                .await,
            );
            group_bytes = 0;
        }
        group_bytes += bytes.len();
        group.push((path, content_type, bytes));
    }
    results.extend(batch_group(app.clone(), id, group, durable, concurrency.min(16)).await);
    app.emit(id);
    let successful = results
        .iter()
        .filter(|v| v["status"].as_u64() == Some(201))
        .count();
    let status = if successful == results.len() {
        StatusCode::CREATED
    } else {
        StatusCode::MULTI_STATUS
    };
    Ok((
        status,
        Json(json!({"results":results,"uploaded":successful,"failed":count-successful})),
    ))
}
async fn batch_group(
    app: Arc<App>,
    id: PoolId,
    group: Vec<(String, String, Vec<u8>)>,
    durable: bool,
    _concurrency: usize,
) -> Vec<Value> {
    let mut results = Vec::new();
    let mut objects = Vec::new();
    for (path, content_type, bytes) in group {
        if let Err(error) = app.config.check_object_size(bytes.len() as u64) {
            results.push(json!({"object_key":path,"status":error.0.as_u16(),"error":error.1}));
            continue;
        }
        match crate::namespace::store::validate_upload(&path, &content_type) {
            Ok(_) => objects.push(crate::service::batch::SmallObject {
                path,
                content_type,
                bytes,
            }),
            Err(error) => {
                results.push(json!({"object_key":path,"status":400,"error":error.to_string()}))
            }
        }
    }
    if objects.is_empty() {
        return results;
    }
    let _permit = match app.upload_slots.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            for object in objects {
                results.push(json!({"object_key":object.path,"status":503,"error":"Upload admission closed"}));
            }
            return results;
        }
    };
    let Some(pool) = app.pools.iter().rev().find(|p| !p.sealed) else {
        for object in objects {
            results.push(json!({"object_key":object.path,"status":503,"error":"All object pools are sealed"}));
        }
        return results;
    };
    let stripe = u64::from(pool.data_shards) * u64::from(pool.block_bytes);
    match app
        .service
        .put_small_batch(id, pool.id, stripe, &objects, durable)
        .await
    {
        Ok(versions) => {
            for (object, version) in objects.iter().zip(versions) {
                let mut value = version_json(&object.path, &version);
                value["status"] = json!(201);
                value["success"] = json!(true);
                results.push(value);
            }
        }
        Err(error) => {
            let error: ApiError = error.into();
            for object in objects {
                results.push(
                    json!({"object_key":object.path,"status":error.0.as_u16(),"error":error.1}),
                );
            }
        }
    }
    results
}
async fn do_init(
    app: Arc<App>,
    path: String,
    headers: HeaderMap,
    q: Q,
    request: uploads::InitRequest,
    durable: bool,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "write").await?;
    let strict = strict(&app, &q)?;
    let handle = uploads::init(
        &app,
        claims.id(),
        path,
        request,
        durable,
        q.get("fresh").is_some_and(|v| v == "true"),
        strict,
    )
    .await?;
    Ok(Json(
        serde_json::to_value(handle).expect("upload handle JSON"),
    ))
}
pub(super) async fn init(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Query(q): Query<Q>,
    Json(body): Json<uploads::InitRequest>,
) -> ApiResult<Json<Value>> {
    do_init(app, path, headers, q, body, false).await
}
pub(super) async fn durable_init(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Query(q): Query<Q>,
    Json(body): Json<uploads::InitRequest>,
) -> ApiResult<Json<Value>> {
    do_init(app, path, headers, q, body, true).await
}
async fn do_part(
    app: Arc<App>,
    version: String,
    index: u64,
    headers: HeaderMap,
    q: Q,
    body: Body,
    durable: bool,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "write").await?;
    let strict = strict(&app, &q)?;
    let saved = uploads::plan(&app, claims.id(), &version, durable).await?;
    let expected = saved
        .handle
        .total_size_bytes
        .saturating_sub(index.saturating_mul(saved.handle.part_size_bytes))
        .min(saved.handle.part_size_bytes);
    let length = headers
        .get("content-length")
        .map(|v| {
            v.to_str()
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or_else(|| ApiError::invalid("invalid part Content-Length"))
        })
        .transpose()?
        .unwrap_or(expected);
    let _permit = app
        .upload_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::invalid("upload admission closed"))?;
    let mut input = reader(body);
    uploads::part(
        &app,
        uploads::PartRequest {
            fragment: claims.id(),
            version: &version,
            index,
            length,
            durable,
            strict,
        },
        &mut input,
    )
    .await?;
    if input.read(&mut [0u8; 1]).await? != 0 {
        return Err(ApiError::invalid("part body exceeds its declared shape"));
    }
    Ok(Json(
        json!({"version_id":version,"part_index":index,"uploaded":true}),
    ))
}
pub(super) async fn part(
    State(app): State<Arc<App>>,
    Path((version, index)): Path<(String, u64)>,
    headers: HeaderMap,
    Query(q): Query<Q>,
    body: Body,
) -> ApiResult<Json<Value>> {
    do_part(app, version, index, headers, q, body, false).await
}
pub(super) async fn durable_part(
    State(app): State<Arc<App>>,
    Path((version, index)): Path<(String, u64)>,
    headers: HeaderMap,
    Query(q): Query<Q>,
    body: Body,
) -> ApiResult<Json<Value>> {
    do_part(app, version, index, headers, q, body, true).await
}
async fn do_status(
    app: Arc<App>,
    version: String,
    headers: HeaderMap,
    durable: bool,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "write").await?;
    Ok(Json(
        uploads::status(&app, claims.id(), &version, durable).await?,
    ))
}
pub(super) async fn status(
    State(app): State<Arc<App>>,
    Path(version): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    do_status(app, version, headers, false).await
}
pub(super) async fn durable_status(
    State(app): State<Arc<App>>,
    Path(version): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    do_status(app, version, headers, true).await
}
async fn do_finalize(
    app: Arc<App>,
    version: String,
    session: String,
    headers: HeaderMap,
    body: Value,
    durable: bool,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "write").await?;
    let id = claims.id();
    let handle = uploads::plan(&app, id, &version, durable).await?.handle;
    let v = uploads::finalize(app, id, version, session, durable, body).await?;
    Ok(Json(version_json(&handle.object_key, &v)))
}
pub(super) async fn finalize(
    State(app): State<Arc<App>>,
    Path((version, session)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    do_finalize(app, version, session, headers, body, false).await
}
pub(super) async fn durable_finalize(
    State(app): State<Arc<App>>,
    Path((version, session)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    do_finalize(app, version, session, headers, body, true).await
}
async fn do_cancel(
    app: Arc<App>,
    version: String,
    headers: HeaderMap,
    durable: bool,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "write").await?;
    uploads::cancel(&app, claims.id(), &version, durable).await?;
    Ok(Json(json!({"cancelled":true})))
}
pub(super) async fn cancel(
    State(app): State<Arc<App>>,
    Path(version): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    do_cancel(app, version, headers, false).await
}
pub(super) async fn durable_cancel(
    State(app): State<Arc<App>>,
    Path(version): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    do_cancel(app, version, headers, true).await
}
async fn do_list(app: Arc<App>, headers: HeaderMap, q: Q, durable: bool) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "write").await?;
    let id = claims.id();
    let limit = q.get("limit").and_then(|v| v.parse().ok()).unwrap_or(25);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::invalid("invalid upload page limit"));
    }
    let scope = format!("uploads/{id}/{durable}");
    let after = super::cursor::decode(
        &app,
        &scope,
        q.get("continuation_token").map(String::as_str),
    )?;
    let (sessions, next) = app
        .db
        .execute(move |db| uploads::list(db, id, durable, after.as_deref(), limit, auth::now()))
        .await?;
    let next = next.map(|version| super::cursor::encode(&app, &scope, &version));
    Ok(Json(json!({"sessions":sessions,"continuation_token":next})))
}
pub(super) async fn list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<Q>,
) -> ApiResult<Json<Value>> {
    do_list(app, headers, q, false).await
}
pub(super) async fn durable_list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<Q>,
) -> ApiResult<Json<Value>> {
    do_list(app, headers, q, true).await
}
