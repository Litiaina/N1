use crate::n1_core::fragment_metadata::{
    backfill_fragment_nt_hash_if_missing, verify_fragment_key,
};
use crate::structs::auth_structs::{AuthBody, AuthorizationRequest, Claims};
use crate::validation::fragment_validation::does_fragment_exist;
use axum::body::Body;
use axum::extract::{FromRequestParts, Request};
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::{
    Json, RequestPartsExt,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use headers::Authorization;
use headers::authorization::Bearer;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde_json::json;
use std::env;
use std::fmt::Display;
use std::sync::LazyLock;
use std::time::SystemTime;
use tracing::{info, warn};

pub static JWT_KEYS: LazyLock<Keys> = LazyLock::new(|| {
    let secret = env::var("JWT_SECRET").expect("JWT_SECRET must be set");
    Keys::new(secret.as_bytes())
});

pub static MASTER_KEY: LazyLock<String> =
    LazyLock::new(|| env::var("MASTER_KEY").expect("MASTER_KEY must be set for authentication"));

pub async fn validate_access(req: Request<Body>, next: Next) -> Result<Response, AuthError> {
    let header = req.headers().get("authorization");
    let token = match header {
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|auth_str| auth_str.strip_prefix("Bearer ")),
        None => None,
    };

    if let Some(token) = token {
        let decoding_key = &JWT_KEYS.decoding;
        let validation = Validation::default();

        match decode::<Claims>(token, decoding_key, &validation) {
            Ok(_) => Ok(next.run(req).await),
            Err(_) => Err(AuthError::InvalidToken),
        }
    } else {
        Err(AuthError::InvalidToken)
    }
}

pub async fn authorize_fragment(
    Json(payload): Json<AuthorizationRequest>,
) -> Result<Json<AuthBody>, AuthError> {
    if payload.secret.is_empty() || payload.fragment.is_empty() {
        return Err(AuthError::MissingCredentials);
    }

    if payload.secret != *MASTER_KEY {
        match does_fragment_exist(&payload.fragment).await {
            true => {}
            false => return Err(AuthError::FragmentNotFound),
        }

        let ok = verify_fragment_key(&payload.fragment, &payload.secret)
            .await
            .map_err(|_| AuthError::InternalError)?;

        if !ok {
            return Err(AuthError::WrongCredentials);
        }

        match backfill_fragment_nt_hash_if_missing(&payload.fragment, &payload.secret).await {
            Ok(true) => {
                info!(
                    "backfilled nt_hash_hex for fragment {} after successful auth",
                    payload.fragment
                );
            }
            Ok(false) => {}
            Err(e) => {
                warn!(
                    "failed to backfill nt_hash_hex for fragment {} after successful auth: {}",
                    payload.fragment, e
                );
            }
        }
    }

    let duration = payload.expires_in.unwrap_or(24 * 60 * 60);

    let exp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + duration;

    let claims = Claims {
        fragment: payload.fragment,
        read: payload.read,
        write: payload.write,
        modify: payload.modify,
        delete: payload.delete,
        exp: exp as usize,
    };

    let token = encode(&Header::default(), &claims, &JWT_KEYS.encoding)
        .map_err(|_| AuthError::InternalError)?;

    Ok(Json(AuthBody::new(token)))
}

impl Display for Claims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fragment: {}, read: {}, write: {}, modify: {}, delete: {},  exp: {}",
            self.fragment, self.read, self.write, self.modify, self.delete, self.exp
        )
    }
}

impl AuthBody {
    fn new(access_token: String) -> Self {
        Self {
            access_token,
            token_type: "Bearer".to_string(),
        }
    }
}

impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| AuthError::InvalidToken)?;

        let token_data =
            decode::<Claims>(bearer.token(), &JWT_KEYS.decoding, &Validation::default())
                .map_err(|_| AuthError::InvalidToken)?;

        Ok(token_data.claims)
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AuthError::WrongCredentials => (StatusCode::UNAUTHORIZED, "Wrong credentials"),
            AuthError::MissingCredentials => (StatusCode::BAD_REQUEST, "Missing credentials"),
            AuthError::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid token"),
            AuthError::InternalError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "An internal error occurred",
            ),
            AuthError::FragmentNotFound => (StatusCode::BAD_REQUEST, "Fragment not found"),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}

pub struct Keys {
    pub encoding: EncodingKey,
    pub decoding: DecodingKey,
}

impl Keys {
    fn new(secret: &[u8]) -> Self {
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
        }
    }
}

pub enum AuthError {
    WrongCredentials,
    MissingCredentials,
    InvalidToken,
    InternalError,
    FragmentNotFound,
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::Validation;
    use std::sync::{Mutex, OnceLock};

    fn env_guard() -> &'static Mutex<()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| Mutex::new(()))
    }

    fn ensure_test_env() {
        if std::env::var("JWT_SECRET").is_err() {
            unsafe { std::env::set_var("JWT_SECRET", "test-jwt-secret") };
        }
        if std::env::var("AUTH_KEY").is_err() {
            unsafe { std::env::set_var("AUTH_KEY", "test-auth-key") };
        }
        if std::env::var("MASTER_KEY").is_err() {
            unsafe { std::env::set_var("MASTER_KEY", "test-master-key") };
        }
    }

    #[tokio::test]
    async fn master_key_accepts_any_auth_fragment_and_mints_claim() {
        let _guard = env_guard().lock().expect("env guard");
        ensure_test_env();

        let requested_fragment = "test-admin-context-fragment".to_string();
        let payload = AuthorizationRequest {
            fragment: requested_fragment.clone(),
            secret: MASTER_KEY.clone(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            expires_in: Some(60),
        };

        let Json(body) = match authorize_fragment(Json(payload)).await {
            Ok(v) => v,
            Err(_) => panic!("admin auth should succeed"),
        };

        let decoded = decode::<Claims>(
            &body.access_token,
            &JWT_KEYS.decoding,
            &Validation::default(),
        )
        .expect("decode token");
        assert_eq!(decoded.claims.fragment, requested_fragment);
        assert!(decoded.claims.read);
        assert!(decoded.claims.write);
        assert!(decoded.claims.modify);
        assert!(decoded.claims.delete);
    }

    #[tokio::test]
    async fn empty_fragment_is_rejected_before_auth_mode_selection() {
        let _guard = env_guard().lock().expect("env guard");
        ensure_test_env();

        let payload = AuthorizationRequest {
            fragment: String::new(),
            secret: "not-master".to_string(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            expires_in: Some(60),
        };

        let res = authorize_fragment(Json(payload)).await;
        assert!(matches!(res, Err(AuthError::MissingCredentials)));
    }
}
