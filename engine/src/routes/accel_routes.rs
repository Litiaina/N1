use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use axum::body::Body;
use axum::extract::RawQuery;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Response};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use std::env;
use std::sync::LazyLock;
use std::time::Duration;

const DEFAULT_ACCEL_SEARCH_BACKEND: &str = "n1-primary";

static ACCEL_SEARCH_CLIENT: LazyLock<Result<reqwest::Client, String>> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(32)
        .build()
        .map_err(|error| error.to_string())
});

fn build_target_url(
    base: &str,
    backend_id: &str,
    raw_query: Option<&str>,
) -> Result<String, AppError> {
    let base = base.trim().trim_end_matches('/');
    if base.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "N1 namespace search accelerator is not configured".to_string(),
        ));
    }
    let backend_id = backend_id.trim();
    if backend_id.is_empty() {
        return Err(AppError::InternalServerError(
            "N1 namespace search backend identity is empty".to_string(),
        ));
    }
    let mut target = reqwest::Url::parse(&format!(
        "{base}/v1/noa/{}/namespace/search",
        utf8_percent_encode(backend_id, NON_ALPHANUMERIC)
    ))
    .map_err(|error| {
        AppError::InternalServerError(format!(
            "N1 namespace search accelerator URL is invalid: {error}"
        ))
    })?;
    target.set_query(raw_query.filter(|query| !query.is_empty()));
    Ok(target.to_string())
}

pub async fn proxy_namespace_search(
    claims: Claims,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Response<Body>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    let authorization = headers
        .get(AUTHORIZATION)
        .cloned()
        .ok_or(AppError::Unauthorized)?;
    let base = env::var("N1_ACCEL_SEARCH_URL").map_err(|_| {
        AppError::ServiceUnavailable(
            "N1 namespace search accelerator is not configured".to_string(),
        )
    })?;
    let backend_id = env::var("N1_ACCEL_SEARCH_BACKEND_ID")
        .unwrap_or_else(|_| DEFAULT_ACCEL_SEARCH_BACKEND.to_string());
    let target = build_target_url(&base, &backend_id, raw_query.as_deref())?;
    let client = ACCEL_SEARCH_CLIENT.as_ref().map_err(|error| {
        AppError::InternalServerError(format!(
            "N1 namespace search HTTP client could not initialize: {error}"
        ))
    })?;
    let upstream = client
        .get(target)
        .header(AUTHORIZATION, authorization)
        .send()
        .await
        .map_err(|error| {
            AppError::ServiceUnavailable(format!(
                "N1 namespace search accelerator is unavailable: {error}"
            ))
        })?;

    let status = upstream.status();
    let content_type = upstream.headers().get(CONTENT_TYPE).cloned();
    let cache_state = upstream.headers().get("x-accel-search").cloned();
    let mut response = Response::builder().status(status);
    if let Some(content_type) = content_type {
        response = response.header(CONTENT_TYPE, content_type);
    }
    if let Some(cache_state) = cache_state {
        response = response.header("x-accel-search", cache_state);
    }
    response
        .header("cache-control", HeaderValue::from_static("no-store"))
        .body(Body::from_stream(upstream.bytes_stream()))
        .map_err(|error| {
            AppError::InternalServerError(format!(
                "N1 namespace search proxy response failed: {error}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::build_target_url;

    #[test]
    fn search_target_is_fixed_to_the_configured_backend_and_preserves_query() {
        let target = build_target_url(
            "http://127.0.0.1:53001/",
            "n1 primary",
            Some("q=video.mp4&field=filename&match=exact"),
        )
        .expect("search target");
        assert_eq!(
            target,
            "http://127.0.0.1:53001/v1/noa/n1%20primary/namespace/search?q=video.mp4&field=filename&match=exact"
        );
    }

    #[test]
    fn search_target_rejects_an_empty_accelerator_url() {
        assert!(build_target_url(" ", "n1-primary", Some("q=a")).is_err());
    }
}
