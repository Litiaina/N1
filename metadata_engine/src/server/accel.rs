//! Optional private Accel integration, retained from N1's named service.
//! Authorization and immutable-version resolution always happen in M1 first.
use super::error::{ApiError, ApiResult};
use crate::middleware::auth;
use axum::{
    body::Body,
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use futures_util::StreamExt;
use std::{collections::BTreeMap, time::Duration};

#[derive(Clone, Default)]
pub(crate) struct Config {
    read: Option<reqwest::Url>,
    search: Option<reqwest::Url>,
}

fn endpoint(base: &str, backend: &str, operation: &str) -> ApiResult<reqwest::Url> {
    if backend.is_empty()
        || !backend
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(ApiError::invalid("invalid Accel backend identity"));
    }
    let base =
        reqwest::Url::parse(base.trim()).map_err(|_| ApiError::invalid("invalid Accel URL"))?;
    if !matches!(base.scheme(), "http" | "https")
        || base.host_str().is_none()
        || !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
    {
        return Err(ApiError::invalid(
            "Accel URL requires HTTP(S), without credentials, query or fragment",
        ));
    }
    reqwest::Url::parse(&format!(
        "{}/v1/noa/{backend}/{operation}",
        base.as_str().trim_end_matches('/')
    ))
    .map_err(|_| ApiError::invalid("invalid Accel endpoint"))
}

impl Config {
    pub(crate) fn load(env: &BTreeMap<String, String>) -> ApiResult<Self> {
        let get = |name: &str| std::env::var(name).ok().or_else(|| env.get(name).cloned());
        let target = |kind: &str, operation: &str| -> ApiResult<Option<reqwest::Url>> {
            let Some(base) = get(&format!("N1_ACCEL_{kind}_URL")).filter(|s| !s.trim().is_empty())
            else {
                return Ok(None);
            };
            let backend =
                get(&format!("N1_ACCEL_{kind}_BACKEND_ID")).unwrap_or_else(|| "n1-primary".into());
            endpoint(&base, &backend, operation).map(Some)
        };
        Ok(Self {
            read: target("READ", "objects/download")?,
            search: target("SEARCH", "namespace/search")?,
        })
    }
}

pub(crate) struct Accel {
    config: Config,
    client: Option<reqwest::Client>,
}

impl Accel {
    pub(crate) fn reads_enabled(&self) -> bool {
        self.config.read.is_some()
    }

    pub(crate) fn searches_enabled(&self) -> bool {
        self.config.search.is_some()
    }

    pub(crate) fn new(config: &Config) -> ApiResult<Self> {
        let client = if config.read.is_some() || config.search.is_some() {
            Some(
                reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(1))
                    .pool_idle_timeout(Duration::from_secs(90))
                    .pool_max_idle_per_host(32)
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .map_err(|_| ApiError::invalid("cannot initialize Accel client"))?,
            )
        } else {
            None
        };
        Ok(Self {
            config: config.clone(),
            client,
        })
    }

    pub(crate) async fn search(
        &self,
        headers: &HeaderMap,
        query: Option<&str>,
    ) -> ApiResult<Response> {
        let mut url = self.config.search.clone().ok_or_else(|| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Namespace search accelerator is not configured".into(),
            )
        })?;
        url.set_query(query.filter(|q| !q.is_empty()));
        let authorization = headers
            .get(header::AUTHORIZATION)
            .cloned()
            .ok_or_else(ApiError::unauthorized)?;
        let upstream = tokio::time::timeout(
            Duration::from_secs(3),
            self.client
                .as_ref()
                .expect("configured client")
                .get(url)
                .header(header::AUTHORIZATION, authorization)
                .send(),
        )
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Namespace search accelerator timed out".into(),
            )
        })?
        .map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Namespace search accelerator is unavailable".into(),
            )
        })?;
        let mut response = Response::builder().status(upstream.status());
        for name in ["content-type", "x-accel-search"] {
            if let Some(value) = upstream.headers().get(name) {
                response = response.header(name, value);
            }
        }
        response
            .header("cache-control", "no-store")
            .body(body(upstream))
            .map_err(|_| ApiError::invalid("invalid search response headers"))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read(
        &self,
        app: &super::App,
        fragment: n1_link::PoolId,
        object: &str,
        version: &str,
        size: u64,
        headers: &HeaderMap,
        attachment: bool,
        filename: &str,
    ) -> Option<Response> {
        if headers
            .get("x-litiaina-accel-origin")
            .is_some_and(|v| v == "1")
        {
            return None;
        }
        let mut url = self.config.read.clone()?;
        let identity = ulid::Ulid::from_bytes(fragment.0).to_string();
        let record = app
            .db
            .execute(move |db| crate::fragments::resolve(&db, &identity))
            .await
            .ok()?;
        let claims = auth::Claims {
            fragment: ulid::Ulid::from_bytes(fragment.0).to_string(),
            fragment_alias: record.alias,
            epoch: record.epoch,
            operator: false,
            read: true,
            write: false,
            modify: false,
            delete: false,
            exp: auth::now() + 6 * 3600,
        };
        // This private read-only credential is never returned to the client.
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(app.config.jwt_secret.as_bytes()),
        )
        .ok()?;
        // Keep the public named lookup boundary on cache callbacks. The pinned
        // version prevents reading a newer head. A concurrent rename may miss
        // at the origin, in which case M1 uses its already-resolved LINK key.
        url.query_pairs_mut()
            .append_pair("object_key", object)
            .append_pair("version_id", version)
            .append_pair(
                "disposition",
                if attachment { "attachment" } else { "inline" },
            )
            .append_pair("filename", filename);
        let mut request = self.client.as_ref()?.get(url).bearer_auth(token);
        for name in ["range", "if-range", "if-none-match"] {
            if let Some(value) = headers.get(name) {
                request = request.header(name, value);
            }
        }
        // Fallback is allowed only before any client response is published.
        let upstream = tokio::time::timeout(Duration::from_secs(3), request.send())
            .await
            .ok()?
            .ok()?;
        let status = upstream.status();
        if !matches!(status.as_u16(), 200 | 206 | 304) {
            return None;
        }
        if upstream.headers().get("etag")?.to_str().ok()? != format!("\"{version}\"") {
            return None;
        }
        match status.as_u16() {
            200 if headers.contains_key(header::RANGE)
                || upstream.content_length() != Some(size) =>
            {
                return None;
            }
            206 => {
                let (offset, length) =
                    super::routes::read::parse_range(headers.get("range")?.to_str().ok()?, size)
                        .ok()?;
                if upstream.content_length() != Some(length)
                    || upstream.headers().get("content-range")?.to_str().ok()?
                        != format!("bytes {offset}-{}/{size}", offset + length - 1)
                {
                    return None;
                }
            }
            304 if !headers.contains_key(header::IF_NONE_MATCH) => return None,
            _ => {}
        }
        let mut response = Response::builder().status(status);
        for name in [
            "content-length",
            "content-range",
            "accept-ranges",
            "etag",
            "last-modified",
            "x-accel-cache",
        ] {
            if let Some(value) = upstream.headers().get(name) {
                response = response.header(name, value);
            }
        }
        if attachment {
            let name =
                percent_encoding::utf8_percent_encode(filename, percent_encoding::NON_ALPHANUMERIC);
            response = response.header(
                header::CONTENT_DISPOSITION,
                format!("attachment; filename*=UTF-8''{name}"),
            );
        }
        response
            .header("cache-control", "private, no-store")
            .body(body(upstream))
            .ok()
    }
}

fn body(upstream: reqwest::Response) -> Body {
    Body::from_stream(futures_util::stream::try_unfold(
        upstream.bytes_stream(),
        |mut stream| async move {
            let chunk = tokio::time::timeout(Duration::from_secs(30), stream.next())
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "Accel response stalled")
                })?;
            match chunk {
                None => Ok(None),
                Some(Ok(bytes)) => Ok(Some((bytes, stream))),
                Some(Err(error)) => Err(std::io::Error::other(error)),
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoints_reject_credentials_redirect_injection_and_ambiguous_configuration() {
        for base in [
            "file:///tmp/cache",
            "http://user:secret@host",
            "http://host/?token=x",
            "http://host/#x",
            "relative",
        ] {
            assert!(endpoint(base, "n1-primary", "namespace/search").is_err());
        }
        for backend in ["", "../private", "a/b", "a?token=x"] {
            assert!(endpoint("http://cache", backend, "namespace/search").is_err());
        }
        assert_eq!(
            endpoint("https://cache/private/", "test", "namespace/search")
                .unwrap()
                .as_str(),
            "https://cache/private/v1/noa/test/namespace/search"
        );
    }
}
