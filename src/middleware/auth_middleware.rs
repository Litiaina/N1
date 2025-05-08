use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use axum::http::Method;
use tracing::{error, warn};
use crate::validation::database_validation::validate_api_key;

pub async fn auth(req: Request, next: Next) -> Response {
    let uri = req.uri();
    let path = uri.path();
    let method = req.method();
    let headers = req.headers();

    if matches!(method, &Method::OPTIONS | &Method::HEAD) {
        return next.run(req).await;
    }
    
    let api_key = headers
        .iter()
        .find(|(k, _)| k.as_str().eq_ignore_ascii_case("x-api-key"))
        .and_then(|(_, v)| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| {
            uri.query().and_then(|query| {
                url::form_urlencoded::parse(query.as_bytes())
                    .find(|(k, _)| k == "api_key")
                    .map(|(_, v)| v.into_owned())
            })
        });

    let api_key = match api_key {
        Some(key) if !key.is_empty() => key,
        Some(_) => {
            warn!("[AUTH] Empty API key provided for request to '{}'", path);
            return unauthorized_response("API key cannot be empty");
        }
        None => {
            error!("[AUTH] Missing API key in request to '{}'", path);
            return unauthorized_response("Missing API key in request headers or query");
        }
    };

    match validate_api_key(&api_key).await {
        Ok(true) => {
            next.run(req).await
        }
        Ok(false) => {
            warn!("[AUTH] Unauthorized access attempt to '{}'", path);
            unauthorized_response("Invalid API key")
        }
        Err(e) => {
            error!("[AUTH] Database error during API key validation: {e}");
            internal_server_error("Database error while validating API key")
        }
    }
}

fn unauthorized_response(reason: &str) -> Response {
    (StatusCode::UNAUTHORIZED, json_error("Unauthorized", reason)).into_response()
}

fn internal_server_error(reason: &str) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, json_error("Internal Server Error", reason)).into_response()
}

fn json_error(error: &str, message: &str) -> String {
    format!(r#"{{"error": "{}", "message": "{}"}}"#, error, message)
}
