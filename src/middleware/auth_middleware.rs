use std::collections::HashSet;
use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use axum::http::Method;
use once_cell::sync::Lazy;
use tracing::{error, warn};


static VALID_API_KEYS: Lazy<HashSet<String>> = Lazy::new(|| {
    let raw = std::env::var("API_KEYS").expect("Missing API_KEYS environment variable");

    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
});

pub async fn auth(req: Request, next: Next) -> Response {
    let uri = req.uri();
    let path = uri.path();
    let method = req.method();
    let headers = req.headers();

    if matches!(method, &Method::OPTIONS | &Method::HEAD) {
        return next.run(req).await;
    }

    let api_key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|auth| {
            let auth_trimmed = auth.trim();
            auth_trimmed
                .get(0..7)
                .filter(|prefix| prefix.eq_ignore_ascii_case("Bearer "))
                .map(|_| auth_trimmed[7..].trim().to_string())
        });

    let api_key = match api_key {
        Some(key) if !key.is_empty() => key,
        Some(_) => {
            warn!("[AUTH] Empty API key provided for request to '{}'", path);
            return unauthorized_response();
        }
        None => {
            error!("[AUTH] Missing API key in request to '{}'", path);
            return unauthorized_response();
        }
    };

    if VALID_API_KEYS.contains(&api_key) {
        next.run(req).await
    } else {
        warn!("[AUTH] Unauthorized access attempt to '{}'", path);
        unauthorized_response()
    }
}

fn unauthorized_response() -> Response {
    (StatusCode::UNAUTHORIZED, "Unauthorized").into_response()
}
