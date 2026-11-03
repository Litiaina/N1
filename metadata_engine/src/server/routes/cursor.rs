use crate::{
    middleware::auth::now,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
#[derive(Serialize, Deserialize)]
struct Cursor {
    scope: String,
    after: String,
    expires: u64,
}
pub(super) fn encode(app: &App, scope: &str, after: &str) -> String {
    let bytes = serde_json::to_vec(&Cursor {
        scope: scope.into(),
        after: after.into(),
        expires: now() + 86400,
    })
    .expect("cursor JSON");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(app.config.jwt_secret.as_bytes()).expect("HMAC key");
    mac.update(&bytes);
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(&bytes),
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}
pub(super) fn decode(app: &App, scope: &str, token: Option<&str>) -> ApiResult<Option<String>> {
    let Some(token) = token else { return Ok(None) };
    if token.len() > 8192 {
        return Err(ApiError::invalid("invalid continuation token"));
    }
    let (body, signature) = token
        .split_once('.')
        .ok_or_else(|| ApiError::invalid("invalid continuation token"))?;
    let body = URL_SAFE_NO_PAD
        .decode(body)
        .map_err(|_| ApiError::invalid("invalid continuation token"))?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| ApiError::invalid("invalid continuation token"))?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(app.config.jwt_secret.as_bytes()).expect("HMAC key");
    mac.update(&body);
    mac.verify_slice(&signature)
        .map_err(|_| ApiError::invalid("invalid continuation token"))?;
    let cursor: Cursor = serde_json::from_slice(&body)
        .map_err(|_| ApiError::invalid("invalid continuation token"))?;
    if cursor.scope != scope || cursor.expires <= now() {
        return Err(ApiError::invalid(
            "continuation token is expired or belongs to another query",
        ));
    }
    Ok(Some(cursor.after))
}
