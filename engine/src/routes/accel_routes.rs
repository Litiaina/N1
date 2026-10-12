use crate::core::disk_manager::ObjectMetadata;
use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::JWT_KEYS;
use crate::structs::auth_structs::Claims;
use axum::body::Body;
use axum::extract::RawQuery;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Response};
use futures_util::StreamExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use std::env;
use std::sync::LazyLock;
use std::time::Duration;

static ACCEL_READ_PROXY: LazyLock<Option<ReadProxy>> = LazyLock::new(|| {
    let base = env::var("N1_ACCEL_READ_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())?;
    let backend = env::var("N1_ACCEL_READ_BACKEND_ID")
        .unwrap_or_else(|_| DEFAULT_ACCEL_SEARCH_BACKEND.into());
    match ReadProxy::new(&base, &backend) {
        Ok(proxy) => Some(proxy),
        Err(error) => {
            tracing::warn!("Accel read configuration rejected; using authoritative reads: {error}");
            None
        }
    }
});

struct ReadProxy {
    client: reqwest::Client,
    target: reqwest::Url,
}

struct ReadTarget<'a> {
    fragment: &'a str,
    object: &'a str,
    version: &'a str,
    size: u64,
}

impl ReadProxy {
    fn new(base: &str, backend: &str) -> Result<Self, String> {
        if backend.is_empty()
            || !backend
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err("invalid Accel read backend ID".into());
        }
        let base = reqwest::Url::parse(base.trim()).map_err(|_| "invalid Accel read URL")?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(
                "Accel read URL must be an absolute HTTP(S) origin without credentials or query"
                    .into(),
            );
        }
        let target = base
            .join(&format!("/v1/noa/{backend}/objects/download"))
            .map_err(|_| "invalid Accel read URL")?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "cannot create Accel read client")?;
        Ok(Self { client, target })
    }

    async fn read(
        &self,
        target: ReadTarget<'_>,
        headers: &HeaderMap,
        disposition: &str,
        filename: Option<&str>,
        signer: &EncodingKey,
    ) -> Option<Response<Body>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs();
        // Internal read-only authority for an already-authorized stream. Its
        // lifetime covers long video/download streams and it is never exposed.
        let claims = Claims {
            fragment: target.fragment.into(),
            fragment_alias: None,
            operator: false,
            read: true,
            write: false,
            modify: false,
            delete: false,
            exp: now as usize + 6 * 60 * 60,
        };
        let token = encode(&Header::default(), &claims, signer).ok()?;
        let mut url = self.target.clone();
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("object_key", target.object)
                .append_pair("version_id", target.version)
                .append_pair(
                    "disposition",
                    if disposition == "attachment" {
                        "attachment"
                    } else {
                        "inline"
                    },
                );
            if let Some(filename) = filename {
                query.append_pair("filename", filename);
            }
        }
        let mut request = self.client.get(url).bearer_auth(token);
        for name in ["range", "if-range", "if-none-match"] {
            if let Some(value) = headers.get(name) {
                request = request.header(name, value);
            }
        }
        // Fail open only before client headers. Never splice/restart a partially delivered body.
        let response = tokio::time::timeout(Duration::from_secs(3), request.send())
            .await
            .ok()?
            .ok()?;
        let status = response.status();
        if !matches!(status.as_u16(), 200 | 206 | 304 | 416) {
            return None;
        }
        if status.as_u16() != 416
            && response.headers().get("etag")?.to_str().ok()? != format!("\"{}\"", target.version)
        {
            return None;
        }
        if status.as_u16() == 200 && response.content_length() != Some(target.size) {
            return None;
        }
        if status.as_u16() == 206 {
            let (start, end) = crate::core::header_parser::parse_range_header(
                headers.get("range").and_then(|value| value.to_str().ok()),
                target.size,
            )?;
            let expected = format!("bytes {start}-{end}/{}", target.size);
            if response.headers().get("content-range")?.to_str().ok()? != expected
                || response.content_length() != Some(end - start + 1)
            {
                return None;
            }
        }
        if status.as_u16() == 304 && !headers.contains_key("if-none-match") {
            return None;
        }
        if status.as_u16() == 416
            && response.headers().get("content-range")?.to_str().ok()?
                != format!("bytes */{}", target.size)
        {
            return None;
        }
        let mut outgoing = Response::builder().status(status);
        for name in [
            "content-type",
            "content-length",
            "content-range",
            "accept-ranges",
            "etag",
            "last-modified",
            "content-disposition",
            "x-litiaina-checksum",
            "x-accel-cache",
        ] {
            if let Some(value) = response.headers().get(name) {
                outgoing = outgoing.header(name, value);
            }
        }
        let stream =
            futures_util::stream::try_unfold(response.bytes_stream(), |mut stream| async move {
                let chunk = tokio::time::timeout(Duration::from_secs(30), stream.next())
                    .await
                    .map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::TimedOut, "Accel read body stalled")
                    })?;
                match chunk {
                    None => Ok(None),
                    Some(Ok(bytes)) => Ok(Some((bytes, stream))),
                    Some(Err(error)) => Err(std::io::Error::other(error)),
                }
            });
        outgoing
            .header("cache-control", "private, no-store")
            .body(Body::from_stream(stream))
            .ok()
    }
}

/// Called only after normal N1 authorization and authoritative version resolution.
pub(crate) async fn try_cached_read(
    headers: &HeaderMap,
    metadata: &ObjectMetadata,
    disposition: &str,
    filename: Option<&str>,
) -> Option<Response<Body>> {
    if headers
        .get("x-litiaina-accel-origin")
        .is_some_and(|value| value == "1")
    {
        return None;
    }
    let proxy = ACCEL_READ_PROXY.as_ref()?;
    proxy
        .read(
            ReadTarget {
                fragment: &metadata.fragment,
                object: &metadata.object_key,
                version: &metadata.version_id,
                size: metadata.total_size_bytes,
            },
            headers,
            disposition,
            filename,
            &JWT_KEYS.encoding,
        )
        .await
}

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
    use super::*;
    use axum::{Router, extract::Query, routing::get};
    use std::collections::HashMap;

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

    #[test]
    fn read_proxy_rejects_unsafe_configuration() {
        for base in [
            "",
            "file:///tmp/bytes",
            "http://user:password@localhost",
            "http://localhost?q=secret",
            "http://localhost/#fragment",
        ] {
            assert!(ReadProxy::new(base, "n1-primary").is_err());
        }
        assert!(ReadProxy::new("http://localhost", "../admin").is_err());
    }

    async fn test_cache(
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> Response<Body> {
        let token = headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .strip_prefix("Bearer ")
            .unwrap();
        let claims = jsonwebtoken::decode::<Claims>(
            token,
            &jsonwebtoken::DecodingKey::from_secret(b"proxy-test-secret"),
            &jsonwebtoken::Validation::default(),
        )
        .unwrap()
        .claims;
        assert!(claims.read);
        assert!(!claims.operator && !claims.write && !claims.modify && !claims.delete);
        assert_eq!(claims.fragment, "stable-fragment");
        assert_eq!(query["version_id"], "immutable-version");
        assert_eq!(query["disposition"], "inline");
        if query["object_key"] == "unavailable" {
            return Response::builder().status(503).body(Body::empty()).unwrap();
        }
        let mut response = Response::builder()
            .header(
                "etag",
                if query["object_key"] == "wrong-version" {
                    "\"wrong\""
                } else {
                    "\"immutable-version\""
                },
            )
            .header("cache-control", "public, max-age=999")
            .header("x-accel-cache", "HIT");
        if headers.contains_key("range") {
            response = response.status(206).header("content-range", "bytes 0-1/3");
            return response
                .header("content-length", 2)
                .body(Body::from("ab"))
                .unwrap();
        }
        response
            .header("content-length", 3)
            .body(Body::from("abc"))
            .unwrap()
    }

    #[tokio::test]
    async fn read_proxy_pins_read_only_authority_preserves_ranges_and_rejects_bad_origins() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/noa/n1-primary/objects/download", get(test_cache)),
            )
            .await
            .unwrap();
        });
        let proxy = ReadProxy::new(&format!("http://{address}"), "n1-primary").unwrap();
        let signer = EncodingKey::from_secret(b"proxy-test-secret");
        let target = |object| ReadTarget {
            fragment: "stable-fragment",
            object,
            version: "immutable-version",
            size: 3,
        };
        let response = proxy
            .read(target("object"), &HeaderMap::new(), "inline", None, &signer)
            .await
            .unwrap();
        assert_eq!(response.headers()["cache-control"], "private, no-store");
        assert_eq!(response.headers()["x-accel-cache"], "HIT");
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 3).await.unwrap(),
            "abc"
        );
        let headers = HeaderMap::from_iter([(
            axum::http::header::RANGE,
            HeaderValue::from_static("bytes=0-1"),
        )]);
        let response = proxy
            .read(target("object"), &headers, "inline", None, &signer)
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 3).await.unwrap(),
            "ab"
        );
        for object in ["wrong-version", "unavailable"] {
            assert!(
                proxy
                    .read(target(object), &HeaderMap::new(), "inline", None, &signer)
                    .await
                    .is_none()
            );
        }
        server.abort();
    }
}
