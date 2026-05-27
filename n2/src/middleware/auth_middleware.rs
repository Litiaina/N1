use crate::core::fragment_metadata::{backfill_fragment_nt_hash_if_missing, verify_fragment_key};
use crate::middleware::refresh_store::{
    RefreshStoreError, SessionPermissions, create_refresh_session, revoke_refresh_session,
    rotate_refresh_session,
};
use crate::structs::auth_structs::{
    AuthBody, AuthSessionScope, AuthorizationRequest, Claims, FileSystemClaims, LoginRequest,
    RefreshRequest,
};
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

pub static AUTH_KEY: LazyLock<String> =
    LazyLock::new(|| env::var("AUTH_KEY").expect("AUTH_KEY must be set for authentication"));

pub static MASTER_KEY: LazyLock<String> =
    LazyLock::new(|| env::var("MASTER_KEY").expect("MASTER_KEY must be set for authentication"));

pub static ADMIN_KEY: LazyLock<String> =
    LazyLock::new(|| env::var("ADMIN_KEY").expect("ADMIN_KEY must be set for authentication"));

pub static REFRESH_TOKEN_TTL_SECS: LazyLock<u64> = LazyLock::new(|| {
    env::var("REFRESH_TOKEN_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(7 * 24 * 60 * 60)
});

pub static ACCESS_TOKEN_DEFAULT_TTL_SECS: LazyLock<u64> = LazyLock::new(|| {
    env::var("ACCESS_TOKEN_DEFAULT_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(15 * 60)
});

pub static ACCESS_TOKEN_MAX_TTL_SECS: LazyLock<u64> = LazyLock::new(|| {
    env::var("ACCESS_TOKEN_MAX_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60 * 60)
});

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
    Ok(Json(authorize_fragment_core(payload).await?))
}

pub async fn authorize_fragment_core(payload: AuthorizationRequest) -> Result<AuthBody, AuthError> {
    if payload.secret.is_empty() || payload.fragment.is_empty() {
        return Err(AuthError::MissingCredentials);
    }

    match does_fragment_exist(&payload.fragment).await {
        true => {}
        false => return Err(AuthError::FragmentNotFound),
    }

    if payload.secret != *MASTER_KEY {
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

    mint_fragment_auth_body(
        payload.fragment,
        payload.read,
        payload.write,
        payload.modify,
        payload.delete,
        resolve_access_token_ttl(payload.expires_in),
    )
    .await
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
    if payload.secret.is_empty() || payload.path.is_empty() {
        return Err(AuthError::MissingCredentials);
    }

    if payload.secret != *AUTH_KEY {
        return Err(AuthError::WrongCredentials);
    }

    let body = mint_fs_auth_body(
        payload.path,
        payload.read,
        payload.write,
        payload.modify,
        payload.delete,
        resolve_access_token_ttl(payload.expires_in),
    )
    .await?;

    Ok(Json(body))
}

pub async fn refresh_access_token(
    Json(payload): Json<RefreshRequest>,
) -> Result<Json<AuthBody>, AuthError> {
    if payload.refresh_token.trim().is_empty() {
        return Err(AuthError::MissingCredentials);
    }

    let rotated = rotate_refresh_session(&payload.refresh_token)
        .await
        .map_err(map_refresh_error)?;

    let body = match rotated.scope {
        AuthSessionScope::Fragment { fragment } => AuthBody::new(
            mint_fragment_access_token(
                fragment,
                rotated.permissions.read,
                rotated.permissions.write,
                rotated.permissions.modify,
                rotated.permissions.delete,
                rotated.access_expires_in,
            )?,
            rotated.access_expires_in,
            None,
            None,
        )
        .with_refresh(rotated.refresh_token, rotated.refresh_expires_in),
        AuthSessionScope::FileSystem { path } => AuthBody::new(
            mint_fs_access_token(
                path,
                rotated.permissions.read,
                rotated.permissions.write,
                rotated.permissions.modify,
                rotated.permissions.delete,
                rotated.access_expires_in,
            )?,
            rotated.access_expires_in,
            None,
            None,
        )
        .with_refresh(rotated.refresh_token, rotated.refresh_expires_in),
    };

    Ok(Json(body))
}

pub async fn logout_refresh_token(
    Json(payload): Json<RefreshRequest>,
) -> Result<StatusCode, AuthError> {
    if payload.refresh_token.trim().is_empty() {
        return Err(AuthError::MissingCredentials);
    }

    revoke_refresh_session(&payload.refresh_token)
        .await
        .map_err(map_refresh_error)?;
    Ok(StatusCode::NO_CONTENT)
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
        write!(
            f,
            "path: {}, read: {}, write: {}, modify: {}, delete: {},  exp: {}",
            self.path, self.read, self.write, self.modify, self.delete, self.exp
        )
    }
}

impl AuthBody {
    fn new(
        access_token: String,
        expires_in: u64,
        refresh_token: Option<String>,
        refresh_expires_in: Option<u64>,
    ) -> Self {
        Self {
            access_token,
            token_type: "Bearer".to_string(),
            expires_in,
            refresh_token,
            refresh_expires_in,
        }
    }

    fn with_refresh(mut self, refresh_token: String, refresh_expires_in: u64) -> Self {
        self.refresh_token = Some(refresh_token);
        self.refresh_expires_in = Some(refresh_expires_in);
        self
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

#[derive(Debug)]
pub enum AuthError {
    WrongCredentials,
    MissingCredentials,
    InvalidToken,
    InternalError,
    FragmentNotFound,
}

async fn mint_fragment_auth_body(
    fragment: String,
    read: bool,
    write: bool,
    modify: bool,
    delete: bool,
    access_expires_in: u64,
) -> Result<AuthBody, AuthError> {
    let token = mint_fragment_access_token(
        fragment.clone(),
        read,
        write,
        modify,
        delete,
        access_expires_in,
    )?;

    let refresh = create_refresh_session(
        AuthSessionScope::Fragment { fragment },
        SessionPermissions {
            read,
            write,
            modify,
            delete,
        },
        access_expires_in,
        *REFRESH_TOKEN_TTL_SECS,
    )
    .await
    .map_err(map_refresh_error)?;

    Ok(AuthBody::new(
        token,
        access_expires_in,
        Some(refresh.refresh_token),
        Some(refresh.refresh_expires_in),
    ))
}

async fn mint_fs_auth_body(
    path: String,
    read: bool,
    write: bool,
    modify: bool,
    delete: bool,
    access_expires_in: u64,
) -> Result<AuthBody, AuthError> {
    let token = mint_fs_access_token(path.clone(), read, write, modify, delete, access_expires_in)?;

    let refresh = create_refresh_session(
        AuthSessionScope::FileSystem { path },
        SessionPermissions {
            read,
            write,
            modify,
            delete,
        },
        access_expires_in,
        *REFRESH_TOKEN_TTL_SECS,
    )
    .await
    .map_err(map_refresh_error)?;

    Ok(AuthBody::new(
        token,
        access_expires_in,
        Some(refresh.refresh_token),
        Some(refresh.refresh_expires_in),
    ))
}

fn expiry_ts(duration_secs: u64) -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + duration_secs
}

fn resolve_access_token_ttl(requested: Option<u64>) -> u64 {
    let default_ttl = (*ACCESS_TOKEN_DEFAULT_TTL_SECS).max(60);
    let max_ttl = (*ACCESS_TOKEN_MAX_TTL_SECS).max(default_ttl);
    requested.unwrap_or(default_ttl).clamp(60, max_ttl)
}

fn mint_fragment_access_token(
    fragment: String,
    read: bool,
    write: bool,
    modify: bool,
    delete: bool,
    access_expires_in: u64,
) -> Result<String, AuthError> {
    let claims = Claims {
        fragment,
        read,
        write,
        modify,
        delete,
        exp: expiry_ts(access_expires_in) as usize,
    };

    encode(&Header::default(), &claims, &JWT_KEYS.encoding).map_err(|_| AuthError::InternalError)
}

fn mint_fs_access_token(
    path: String,
    read: bool,
    write: bool,
    modify: bool,
    delete: bool,
    access_expires_in: u64,
) -> Result<String, AuthError> {
    let claims = FileSystemClaims {
        path,
        write,
        modify,
        read,
        delete,
        exp: expiry_ts(access_expires_in) as usize,
    };

    encode(&Header::default(), &claims, &JWT_KEYS.encoding).map_err(|_| AuthError::InternalError)
}

fn map_refresh_error(error: RefreshStoreError) -> AuthError {
    match error {
        RefreshStoreError::InvalidToken
        | RefreshStoreError::TokenExpired
        | RefreshStoreError::TokenReuseDetected => AuthError::InvalidToken,
        RefreshStoreError::Io(_) | RefreshStoreError::Json(_) => AuthError::InternalError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::disk_manager::{BLOB_STORE_DISKS, INDEX_STORE_DISKS};
    use crate::middleware::refresh_store::clear_refresh_store_for_tests;
    use crate::test_support::global_test_lock;
    use jsonwebtoken::Validation;

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
        if std::env::var("REFRESH_TOKEN_TTL_SECS").is_err() {
            unsafe { std::env::set_var("REFRESH_TOKEN_TTL_SECS", "3600") };
        }
        let isolated_refresh_root =
            std::env::temp_dir().join(format!("n2-auth-refresh-tests-{}", std::process::id()));
        unsafe {
            std::env::set_var("N2_REFRESH_STORE_ROOT", isolated_refresh_root);
        }
        let index_root =
            std::env::temp_dir().join(format!("n2-auth-index-tests-{}", std::process::id()));
        let blob_root =
            std::env::temp_dir().join(format!("n2-auth-blob-tests-{}", std::process::id()));
        std::fs::create_dir_all(&index_root).expect("create auth index root");
        std::fs::create_dir_all(&blob_root).expect("create auth blob root");
        if INDEX_STORE_DISKS.get().is_none() {
            INDEX_STORE_DISKS
                .set(vec![index_root.to_string_lossy().to_string()])
                .expect("set auth index roots");
        }
        if BLOB_STORE_DISKS.get().is_none() {
            BLOB_STORE_DISKS
                .set(vec![blob_root.to_string_lossy().to_string()])
                .expect("set auth blob roots");
        }
    }

    async fn ensure_auth_test_fragment(fragment: &str) {
        crate::core::fragment_metadata::init_fragment_metadata(fragment, "fragment-key")
            .await
            .expect("init fragment metadata");
    }

    #[tokio::test]
    async fn master_key_accepts_existing_fragment_and_mints_claim() {
        let _guard = global_test_lock().lock().await;
        ensure_test_env();

        let requested_fragment = "test-admin-context-fragment".to_string();
        ensure_auth_test_fragment(&requested_fragment).await;
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

        assert!(body.refresh_token.is_some());
        assert_eq!(body.refresh_expires_in, Some(3600));

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
    async fn master_key_rejects_missing_fragment() {
        let _guard = global_test_lock().lock().await;
        ensure_test_env();

        let payload = AuthorizationRequest {
            fragment: "missing-admin-context-fragment".to_string(),
            secret: MASTER_KEY.clone(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            expires_in: Some(60),
        };

        let res = authorize_fragment(Json(payload)).await;
        assert!(matches!(res, Err(AuthError::FragmentNotFound)));
    }

    #[tokio::test]
    async fn empty_fragment_is_rejected_before_auth_mode_selection() {
        let _guard = global_test_lock().lock().await;
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

    #[tokio::test]
    async fn refresh_rotates_token_and_logout_revokes_session() {
        let _guard = global_test_lock().lock().await;
        ensure_test_env();
        clear_refresh_store_for_tests().await;
        ensure_auth_test_fragment("refresh-fragment").await;

        let payload = AuthorizationRequest {
            fragment: "refresh-fragment".to_string(),
            secret: MASTER_KEY.clone(),
            read: true,
            write: false,
            modify: false,
            delete: false,
            expires_in: Some(120),
        };

        let Json(initial) = authorize_fragment(Json(payload))
            .await
            .expect("initial auth");
        let initial_refresh = initial
            .refresh_token
            .clone()
            .expect("refresh token should be present");

        let Json(rotated) = refresh_access_token(Json(RefreshRequest {
            refresh_token: initial_refresh.clone(),
        }))
        .await
        .expect("refresh should succeed");

        let rotated_refresh = rotated
            .refresh_token
            .clone()
            .expect("rotated refresh token");
        assert_ne!(initial_refresh, rotated_refresh);

        let logout_status = logout_refresh_token(Json(RefreshRequest {
            refresh_token: rotated_refresh.clone(),
        }))
        .await
        .expect("logout should succeed");
        assert_eq!(logout_status, StatusCode::NO_CONTENT);

        let post_logout = refresh_access_token(Json(RefreshRequest {
            refresh_token: rotated_refresh,
        }))
        .await;
        assert!(matches!(post_logout, Err(AuthError::InvalidToken)));
    }

    #[tokio::test]
    async fn refresh_reuse_detection_revokes_rotated_session() {
        let _guard = global_test_lock().lock().await;
        ensure_test_env();
        clear_refresh_store_for_tests().await;
        ensure_auth_test_fragment("refresh-fragment").await;

        let payload = AuthorizationRequest {
            fragment: "refresh-fragment".to_string(),
            secret: MASTER_KEY.clone(),
            read: true,
            write: false,
            modify: false,
            delete: false,
            expires_in: Some(120),
        };

        let Json(initial) = authorize_fragment(Json(payload))
            .await
            .expect("initial auth");
        let initial_refresh = initial
            .refresh_token
            .clone()
            .expect("refresh token should be present");

        let Json(rotated) = refresh_access_token(Json(RefreshRequest {
            refresh_token: initial_refresh.clone(),
        }))
        .await
        .expect("refresh should succeed");

        let rotated_refresh = rotated
            .refresh_token
            .clone()
            .expect("rotated refresh token");

        let reuse_attempt = refresh_access_token(Json(RefreshRequest {
            refresh_token: initial_refresh,
        }))
        .await;
        assert!(matches!(reuse_attempt, Err(AuthError::InvalidToken)));

        let post_reuse = refresh_access_token(Json(RefreshRequest {
            refresh_token: rotated_refresh,
        }))
        .await;
        assert!(matches!(post_reuse, Err(AuthError::InvalidToken)));
    }

    #[tokio::test]
    async fn omitted_expiration_uses_short_default_and_requested_ttl_is_clamped() {
        let _guard = global_test_lock().lock().await;
        ensure_test_env();
        clear_refresh_store_for_tests().await;
        ensure_auth_test_fragment("default-ttl-fragment").await;
        ensure_auth_test_fragment("clamped-ttl-fragment").await;

        let Json(default_body) = authorize_fragment(Json(AuthorizationRequest {
            fragment: "default-ttl-fragment".to_string(),
            secret: MASTER_KEY.clone(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            expires_in: None,
        }))
        .await
        .expect("default ttl auth");

        assert_eq!(default_body.expires_in, 15 * 60);

        let Json(clamped_body) = authorize_fragment(Json(AuthorizationRequest {
            fragment: "clamped-ttl-fragment".to_string(),
            secret: MASTER_KEY.clone(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            expires_in: Some(24 * 60 * 60),
        }))
        .await
        .expect("clamped ttl auth");

        assert_eq!(clamped_body.expires_in, 60 * 60);
    }
}
