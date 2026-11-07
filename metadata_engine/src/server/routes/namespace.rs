use super::cursor;
use crate::{
    middleware::auth,
    namespace::{Namespace, PublishedVersion},
    posix::models::*,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use n1_link::PoolId;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

fn normalize(path: &str) -> ApiResult<String> {
    crate::namespace::paths::normalize_path(path).map_err(|e| ApiError::invalid(e.to_string()))
}
pub(super) async fn retired(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    auth::authorize(&app, &headers, "session").await?;
    Err(ApiError(
        StatusCode::GONE,
        "Manual permanent or version deletion is retired; trash retention owns final reclamation"
            .into(),
    ))
}
pub(super) fn version_json(path: &str, v: &PublishedVersion) -> Value {
    json!({
        "object_key": path,
        "object_id": v.object_id,
        "version_id": v.version_id,
        "content_type": v.content_type,
        "total_size_bytes": v.total_size_bytes,
        "created_at": v.created_at_unix,
        "created_at_unix": v.created_at_unix
    })
}
fn file_entry(path: String, v: PublishedVersion) -> PosixEntry {
    PosixEntry {
        name: path.rsplit('/').next().unwrap_or("").into(),
        path,
        kind: PosixNodeKind::File,
        size_bytes: Some(v.total_size_bytes),
        modified_at_unix: Some(v.created_at_unix),
        content_type: Some(v.content_type),
        version_id: Some(v.version_id),
        trash_entry_id: None,
        deleted_at_unix: None,
    }
}
pub(super) async fn list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<PosixListQuery>,
) -> ApiResult<Json<PosixListResponse>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let fragment = claims.id();
    let path = normalize(q.path.as_deref().unwrap_or(""))?;
    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    };
    let limit = q.limit.unwrap_or(200);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::invalid("limit must be 1 through 1000"));
    }
    let trash = q.trash.unwrap_or(false);
    let scope = format!("posix/{fragment}/{trash}/{path}");
    let after = cursor::decode(&app, &scope, q.continuation_token.as_deref())?;
    let (entries, after) = if trash {
        let rows = app
            .db
            .execute(move |db| Namespace::new(db).trash_page(fragment, after.as_deref(), limit))
            .await?;
        let after = rows.last().map(|r| r.0.clone());
        let mut entries = Vec::new();
        for (key, t, v) in rows {
            if !t.restored && t.path.starts_with(&prefix) {
                let mut entry = file_entry(t.path, v);
                entry.trash_entry_id = Some(key.trim_start_matches("trash/").into());
                entry.deleted_at_unix = Some(t.deleted_at_unix);
                entries.push(entry);
            }
        }
        (entries, after)
    } else {
        let scan_prefix = prefix.clone();
        let page = app
            .db
            .execute(move |db| {
                Namespace::new(db).shallow(fragment, &scan_prefix, "/", after.as_deref(), limit)
            })
            .await?;
        let entries = page
            .entries
            .into_iter()
            .map(|row| {
                if row.directory {
                    let path = row.path.trim_end_matches('/').to_owned();
                    PosixEntry {
                        name: path.rsplit('/').next().unwrap_or("").into(),
                        path,
                        kind: PosixNodeKind::Directory,
                        size_bytes: None,
                        modified_at_unix: None,
                        content_type: None,
                        version_id: None,
                        trash_entry_id: None,
                        deleted_at_unix: None,
                    }
                } else {
                    file_entry(row.path, row.version.expect("file has a committed version"))
                }
            })
            .collect();
        (entries, page.after)
    };
    Ok(Json(PosixListResponse {
        path,
        scan_trash: trash,
        entries,
        continuation_token: after.map(|a| cursor::encode(&app, &scope, &a)),
    }))
}
pub(super) async fn stat(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<PosixStatQuery>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let id = claims.id();
    let path = normalize(q.path.as_deref().unwrap_or(""))?;
    let lookup = path.clone();
    let file = app
        .db
        .execute(move |db| Namespace::new(db).stat(id, &lookup, None))
        .await;
    match file {
        Ok(v) => Ok(Json(json!({
            "path": path,
            "kind": "file",
            "is_virtual": false,
            "scan_trash": false,
            "size_bytes": v.total_size_bytes,
            "version_id": v.version_id,
            "content_type": v.content_type,
            "modified_at_unix": v.created_at_unix
        }))),
        Err(crate::Error::NotFound) => {
            let prefix = if path.is_empty() {
                String::new()
            } else {
                format!("{path}/")
            };
            let page = app
                .db
                .execute(move |db| Namespace::new(db).list(id, &prefix, None, 1))
                .await?;
            if !path.is_empty() && page.files.is_empty() {
                return Err(crate::Error::NotFound.into());
            }
            Ok(Json(
                json!({"path":path,"kind":"directory","is_virtual":true,"scan_trash":false}),
            ))
        }
        Err(e) => Err(e.into()),
    }
}
pub(super) async fn mkdir(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    auth::authorize(&app, &headers, "write").await?;
    Err(ApiError::invalid(
        "N1 uses virtual prefixes; empty directories are not persisted",
    ))
}
pub(super) async fn objects(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let id = claims.id();
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let delimiter = q.get("delimiter").cloned().unwrap_or_else(|| "/".into());
    if delimiter.len() > 64 {
        return Err(ApiError::invalid("delimiter is too long"));
    }
    let include_metadata = q.get("include_metadata").is_some_and(|v| v == "true");
    let limit = q
        .get("limit")
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(|_| ApiError::invalid("invalid limit"))?
        .unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::invalid("limit must be 1 through 1000"));
    }
    let scope = format!("objects/{id}/{prefix}/{delimiter}/{include_metadata}");
    let after = cursor::decode(
        &app,
        &scope,
        q.get("continuation_token").map(String::as_str),
    )?;
    let query_prefix = prefix.clone();
    let page = app
        .db
        .execute(move |db| {
            Namespace::new(db).shallow(id, &query_prefix, &delimiter, after.as_deref(), limit)
        })
        .await?;
    let mut entries = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut objects = Vec::new();
    for row in page.entries {
        entries.push(json!({
            "record": row.path.strip_prefix(&prefix).unwrap_or(&row.path).trim_end_matches('/'),
            "kind": if row.directory{
                "directory"
            }else{
                "file"
            },
            "size_bytes": 0,
            "created_at_unix_nanos": 0
        }));
        if row.directory {
            common_prefixes.push(row.path);
        } else if include_metadata {
            objects.push(version_json(
                &row.path,
                &row.version.expect("committed file version"),
            ));
        }
    }
    let next = page.after.map(|a| cursor::encode(&app, &scope, &a));
    Ok(Json(json!({
        "prefix": prefix,
        "limit": limit,
        "entries": entries,
        "objects": objects,
        "common_prefixes": common_prefixes,
        "continuation_token": next,
        "next_continuation_token": next
    })))
}

async fn matching(app: &App, id: PoolId, path: &str) -> ApiResult<(bool, Vec<String>)> {
    let clean = normalize(path)?;
    let lookup = clean.clone();
    let exact = if path.ends_with('/') {
        false
    } else {
        match app
            .db
            .execute(move |db| Namespace::new(db).stat(id, &lookup, None))
            .await
        {
            Ok(_) => true,
            Err(crate::Error::NotFound) => false,
            Err(e) => return Err(e.into()),
        }
    };
    if exact {
        return Ok((false, vec![clean]));
    }
    let prefix = if clean.is_empty() {
        String::new()
    } else {
        format!("{clean}/")
    };
    // Mutations process one bounded page at a time; callers advance by the last
    // original source key while names move or disappear.
    let page = app
        .db
        .execute(move |db| Namespace::new(db).list(id, &prefix, None, 1000))
        .await?;
    if page.files.is_empty() {
        return Err(crate::Error::NotFound.into());
    }
    Ok((true, page.files.into_iter().map(|f| f.path).collect()))
}
pub(super) async fn rename(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "modify").await?;
    let id = claims.id();
    let source = body
        .get("source_path")
        .or_else(|| body.get("source_key"))
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::invalid("source_path is required"))?;
    let destination = body
        .get("destination_path")
        .or_else(|| body.get("destination_key"))
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::invalid("destination_path is required"))?;
    if body
        .get("destination_fragment")
        .and_then(Value::as_str)
        .is_some_and(|s| s != claims.fragment && s != claims.fragment_alias)
    {
        return Err(ApiError::invalid("cross-fragment rename is not supported"));
    }
    let from = normalize(source)?;
    let to = normalize(destination)?;
    crate::namespace::store::validate_upload(&to, "application/octet-stream")?;
    let (directory, mut paths) = matching(&app, id, source).await?;
    if directory && (to == from || to.starts_with(&format!("{from}/"))) {
        return Err(ApiError::invalid(
            "prefix destination must be outside its source",
        ));
    }
    let mut affected = 0;
    let mut conflicts = Vec::new();
    let mut failures = Vec::new();
    let mut after = None;
    loop {
        for path in paths {
            after = Some(path.clone());
            let target = if directory {
                format!(
                    "{to}/{}",
                    path.strip_prefix(&format!("{from}/")).unwrap_or(&path)
                )
            } else {
                to.clone()
            };
            let original = path.clone();
            match app
                .db
                .execute(move |db| Namespace::new(db).rename(id, &path, &target))
                .await
            {
                Ok(()) => affected += 1,
                Err(crate::Error::NameConflict(_)) => conflicts.push(original),
                Err(e) => {
                    if !directory {
                        return Err(e.into());
                    }
                    failures.push(original);
                }
            }
        }
        if !directory {
            break;
        }
        let prefix = format!("{from}/");
        let cursor = after.clone();
        let page = app
            .db
            .execute(move |db| Namespace::new(db).list(id, &prefix, cursor.as_deref(), 1000))
            .await?;
        if page.files.is_empty() {
            break;
        }
        paths = page.files.into_iter().map(|f| f.path).collect();
    }
    app.emit(id);
    Ok(Json(json!({
        "operation": "rename",
        "kind": if directory{
            "directory"
        }else{
            "file"
        },
        "affected": affected,
        "affected_paths": affected,
        "source_path": from,
        "destination_path": to,
        "conflicted_paths": conflicts,
        "failed_paths": failures
    })))
}
pub(super) async fn trash(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(body): Json<PosixPathRequest>,
) -> ApiResult<Json<Value>> {
    trash_path(&app, &headers, &body.path).await.map(Json)
}
async fn trash_path(app: &App, headers: &HeaderMap, path: &str) -> ApiResult<Value> {
    let claims = auth::authorize(app, headers, "delete").await?;
    let id = claims.id();
    let (directory, mut paths) = matching(app, id, path).await?;
    let prefix = format!("{}/", normalize(path)?);
    let mut affected = 0;
    let mut last_id = None;
    let mut failures = Vec::new();
    let mut after = None;
    loop {
        for path in paths {
            after = Some(path.clone());
            let original = path.clone();
            match app
                .db
                .execute(move |db| Namespace::new(db).trash(id, &path, auth::now() as i64))
                .await
            {
                Ok(trash_id) => {
                    affected += 1;
                    last_id = Some(trash_id);
                }
                Err(e) => {
                    if !directory {
                        return Err(e.into());
                    }
                    failures.push(original);
                }
            }
        }
        if !directory {
            break;
        }
        let p = prefix.clone();
        let cursor = after.clone();
        let page = app
            .db
            .execute(move |db| Namespace::new(db).list(id, &p, cursor.as_deref(), 1000))
            .await?;
        if page.files.is_empty() {
            break;
        }
        paths = page.files.into_iter().map(|f| f.path).collect();
    }
    app.emit(id);
    Ok(json!({
        "operation": "trash",
        "kind": if directory{
            "directory"
        }else{
            "file"
        },
        "affected": affected,
        "affected_paths": affected,
        "trash_entry_id": last_id,
        "failed_paths": failures,
        "conflicted_paths": []
    }))
}
pub(super) async fn delete(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    trash_path(&app, &headers, &path).await.map(Json)
}
pub(super) async fn recover(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(body): Json<PosixPathRequest>,
) -> ApiResult<Json<Value>> {
    recover_path(
        &app,
        &headers,
        &body.path,
        body.trash_entry_id.as_deref(),
        false,
    )
    .await
    .map(Json)
}
pub(super) async fn recover_key(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    recover_path(
        &app,
        &headers,
        &path,
        q.get("trash_entry_id").map(String::as_str),
        false,
    )
    .await
    .map(Json)
}
async fn recover_path(
    app: &App,
    headers: &HeaderMap,
    path: &str,
    trash_id: Option<&str>,
    all: bool,
) -> ApiResult<Value> {
    let claims = auth::authorize(app, headers, "modify").await?;
    let id = claims.id();
    let clean = normalize(path)?;
    let mut candidates = BTreeMap::new();
    let mut after = None;
    loop {
        let cursor = after.clone();
        let rows = app
            .db
            .execute(move |db| Namespace::new(db).trash_page(id, cursor.as_deref(), 1000))
            .await?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|r| r.0.clone());
        for (key, t, _) in rows {
            let entry_id = key.trim_start_matches("trash/").to_owned();
            if t.restored {
                continue;
            }
            let matches = if let Some(requested) = trash_id {
                entry_id == requested
            } else {
                all || t.path == clean || t.path.starts_with(&format!("{clean}/"))
            };
            if matches {
                candidates.insert(t.path, (entry_id, t.deleted_at_unix));
            }
        }
    }
    if candidates.is_empty() {
        return Err(crate::Error::NotFound.into());
    }
    let mut affected = 0;
    let mut conflicts = Vec::new();
    let mut failures = Vec::new();
    for (path, (trash_id, _)) in candidates {
        match app
            .db
            .execute(move |db| Namespace::new(db).recover(id, &trash_id))
            .await
        {
            Ok(_) => affected += 1,
            Err(crate::Error::NameConflict(_)) => conflicts.push(path),
            Err(_) => failures.push(path),
        }
    }
    app.emit(id);
    Ok(json!({
        "operation": "recover",
        "affected": affected,
        "affected_paths": affected,
        "conflicted_paths": conflicts,
        "failed_paths": failures
    }))
}
pub(super) async fn recover_all(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    recover_path(&app, &headers, "", None, true).await.map(Json)
}
pub(super) async fn trash_list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let id = claims.id();
    let scope = format!("trash/{id}");
    let after = cursor::decode(
        &app,
        &scope,
        q.get("continuation_token").map(String::as_str),
    )?;
    let limit = q.get("limit").and_then(|s| s.parse().ok()).unwrap_or(200);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::invalid("invalid trash page limit"));
    }
    let rows = app
        .db
        .execute(move |db| Namespace::new(db).trash_page(id, after.as_deref(), limit))
        .await?;
    let next = rows.last().map(|r| cursor::encode(&app, &scope, &r.0));
    let entries: Vec<Value> = rows
        .into_iter()
        .filter(|r| !r.1.restored)
        .map(|(key, t, v)| {
            json!({
                "object_key": t.path,
                "path": t.path,
                "kind": "file",
                "trash_entry_id": key.trim_start_matches("trash/"),
                "deleted_at_unix": t.deleted_at_unix,
                "size_bytes": v.total_size_bytes,
                "version_id": v.version_id
            })
        })
        .collect();
    Ok(Json(
        json!({"entries":entries,"objects":entries,"continuation_token":next}),
    ))
}
pub(super) async fn bulk_delete(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    bulk(app, headers, body, false).await
}
pub(super) async fn bulk_recover(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    bulk(app, headers, body, true).await
}
async fn bulk(
    app: Arc<App>,
    headers: HeaderMap,
    body: Value,
    recover: bool,
) -> ApiResult<Json<Value>> {
    let keys = body
        .get("object_keys")
        .or_else(|| body.get("keys"))
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::invalid("object_keys is required"))?;
    if keys.len() > 4096 {
        return Err(ApiError::invalid("too many object keys"));
    }
    let mut affected = 0;
    let mut failed = Vec::new();
    for key in keys {
        let key = key
            .as_str()
            .ok_or_else(|| ApiError::invalid("invalid object key"))?;
        let result = if recover {
            recover_path(&app, &headers, key, None, false).await
        } else {
            trash_path(&app, &headers, key).await
        };
        match result {
            Ok(v) => affected += v["affected"].as_u64().unwrap_or(0),
            Err(e) => failed.push(json!({"object_key":key,"status":e.0.as_u16(),"error":e.1})),
        }
    }
    Ok(Json(json!({"affected":affected,"failed":failed})))
}
pub(super) async fn search(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> ApiResult<Response> {
    auth::authorize(&app, &headers, "read").await?;
    app.accel.search(&headers, query.as_deref()).await
}
pub(super) async fn snapshot(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let id = claims.id();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);
    tokio::spawn(async move {
        let mut after = None;
        let mut count = 0;
        loop {
            let cursor = after.clone();
            let page = app
                .db
                .execute(move |db| Namespace::new(db).list(id, "", cursor.as_deref(), 1000))
                .await;
            match page {
                Ok(page) => {
                    after = page.after;
                    if page.files.is_empty() {
                        let _ = tx
                            .send(Ok(bytes::Bytes::from(format!(
                                "{}\n",
                                json!({"kind":"complete","entry_count":count})
                            ))))
                            .await;
                        break;
                    }
                    for row in page.files {
                        count += 1;
                        let mut v = version_json(&row.path, &row.version);
                        v["kind"] = json!("entry");
                        if tx
                            .send(Ok(bytes::Bytes::from(format!("{v}\n"))))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(_) => {
                    let _ = tx
                        .send(Err(std::io::Error::other("namespace scan failed")))
                        .await;
                    break;
                }
            }
        }
    });
    Ok((
        [("content-type", "application/x-ndjson")],
        Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)),
    )
        .into_response())
}
