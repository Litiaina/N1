//! Authenticated search with an always-available native metadata path.
use super::{cursor, namespace::version_json};
use crate::{
    middleware::auth,
    namespace::{Namespace, SearchField, SearchMatch, SearchSpec},
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use axum::{
    Json,
    extract::{Query, RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;

const NATIVE_CURSOR: &str = "m1:";

#[derive(Deserialize)]
pub(super) struct SearchQuery {
    q: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    field: SearchField,
    #[serde(default, rename = "match")]
    matching: SearchMatch,
    limit: Option<usize>,
    continuation_token: Option<String>,
    #[serde(default)]
    refresh: bool,
}

pub(super) async fn search(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    Query(query): Query<SearchQuery>,
) -> ApiResult<Response> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    let spec = SearchSpec::new(
        &query.q,
        &query.scope,
        query.field,
        query.matching,
        query.limit.unwrap_or(100),
    )?;
    if query.refresh && query.continuation_token.is_some() {
        return Err(ApiError::invalid(
            "refresh cannot be combined with a continuation token",
        ));
    }
    let native_page = query
        .continuation_token
        .as_deref()
        .is_some_and(|s| s.starts_with(NATIVE_CURSOR));
    // Accel's existing projection folds scope names. Scoped M1 queries retain
    // the namespace's exact path case through the native reader instead.
    if app.accel.searches_enabled()
        && !native_page
        && (!spec.is_scoped() || query.continuation_token.is_some())
    {
        match app.accel.search(&headers, raw.as_deref()).await {
            Ok(response) if response.status() == StatusCode::OK => return Ok(response),
            Ok(response) if query.continuation_token.is_some() => return Ok(response),
            Err(error) if query.continuation_token.is_some() => {
                return Err(error);
            }
            _ => {} // A fresh unavailable acceleration attempt falls back to M1.
        }
    }
    let binding = hex::encode(Sha256::digest(
        serde_json::to_vec(&spec).expect("search spec JSON"),
    ));
    let scope = format!("search/{}/{binding}", claims.fragment);
    let token = query
        .continuation_token
        .as_deref()
        .map(|token| {
            token.strip_prefix(NATIVE_CURSOR).ok_or_else(|| {
                ApiError::invalid(
                    "continuation token belongs to another search source; restart search",
                )
            })
        })
        .transpose()?;
    let after = cursor::decode(&app, &scope, token)?;
    let id = claims.id();
    let page = app
        .db
        .execute(move |db| Namespace::new(db).search(id, &spec, after.as_deref()))
        .await?;
    let next = page
        .after
        .as_ref()
        .map(|key| format!("{NATIVE_CURSOR}{}", cursor::encode(&app, &scope, key)));
    let results = page
        .files
        .iter()
        .map(|file| version_json(&file.path, &file.version))
        .collect::<Vec<_>>();
    Ok((
        [("cache-control", "no-store"), ("x-m1-search", "native")],
        Json(json!({
            "results": results,
            "total_matches": page.total_matches,
            "continuation_token": next,
            "scanned_entries": page.scanned_entries,
            "source": "m1",
            "consistency": "live_keyset"
        })),
    )
        .into_response())
}
