use crate::structs::auth_structs::{
    AuthBody, AuthorizationRequest, Claims, FileSystemClaims, LoginRequest,
};
use crate::validation::auth_validation::validate_auth;
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
use tracing::info;
use std::env;
use std::fmt::Display;
use std::sync::LazyLock;
use std::time::SystemTime;

pub static JWT_KEYS: LazyLock<Keys> = LazyLock::new(|| {
    let secret = env::var("JWT_SECRET").expect("JWT_SECRET must be set");
    Keys::new(secret.as_bytes())
});

pub static AUTH_KEY: LazyLock<String> =
    LazyLock::new(|| env::var("AUTH_KEY").expect("AUTH_KEY must be set for authentication"));

pub static MASTER_KEY: LazyLock<String> =
    LazyLock::new(|| env::var("MASTER_KEY").expect("MASTER_KEY must be set for authentication"));

pub static ADMIN_KEY: LazyLock<String> =
    LazyLock::new(|| env::var("ADMIN_KEY").expect("ADMIN_KEY must be set for authentication"));

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

    if payload.secret != *AUTH_KEY {
        return Err(AuthError::WrongCredentials);
    }

    if payload.fragment != *ADMIN_KEY {
        match does_fragment_exist(&payload.fragment).await {
            true => { info!("Access granted on fragment: {}", &payload.fragment) }
            false => return Err(AuthError::FragmentNotFound),
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

pub async fn auth(req: Request<Body>, next: Next) -> Result<Response, AuthError> {
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

        match decode::<FileSystemClaims>(token, decoding_key, &validation) {
            Ok(_) => Ok(next.run(req).await),
            Err(_) => Err(AuthError::InvalidToken),
        }
    } else {
        Err(AuthError::InvalidToken)
    }
}

pub async fn authorize(Json(payload): Json<LoginRequest>) -> Result<Json<AuthBody>, AuthError> {
    if payload.email.is_empty() || payload.password.is_empty() {
        return Err(AuthError::MissingCredentials);
    }

    match validate_auth(&payload.email, &payload.password).await {
        Ok(Some(uid)) => {
            let exp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 24 * 60 * 60;

            let claims = FileSystemClaims {
                email: payload.email.to_owned(),
                uid,
                exp: exp as usize,
            };

            let token = encode(&Header::default(), &claims, &JWT_KEYS.encoding)
                .map_err(|_| AuthError::InternalError)?;

            Ok(Json(AuthBody::new(token)))
        }
        Ok(None) => Err(AuthError::WrongCredentials),
        Err(_) => Err(AuthError::InternalError),
    }
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

impl Display for FileSystemClaims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "email: {}, uid: {}", self.email, self.uid)
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

impl<S> FromRequestParts<S> for FileSystemClaims
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
            decode::<FileSystemClaims>(bearer.token(), &JWT_KEYS.decoding, &Validation::default())
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
