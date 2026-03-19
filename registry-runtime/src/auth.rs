use std::time::{SystemTime, UNIX_EPOCH};

use argon2::Argon2;
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use axum::http::HeaderMap;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

use crate::error::RegistryError;
use crate::models::UserRecord;

#[derive(Clone, Debug)]
pub struct AuthSettings {
    pub jwt_secret: String,
    pub master_key: String,
    pub access_token_ttl_secs: u64,
    pub refresh_token_ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryClaims {
    pub sub: String,
    pub email: String,
    pub is_admin: bool,
    pub exp: usize,
    pub iat: usize,
}

pub fn hash_password(password: &str) -> Result<String, RegistryError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| RegistryError::internal(format!("password hash failed: {}", err)))
}

pub fn verify_password(password: &str, password_hash: &str) -> Result<(), RegistryError> {
    let parsed = PasswordHash::new(password_hash)
        .map_err(|_| RegistryError::unauthorized("invalid credentials"))?;
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .map_err(|_| RegistryError::unauthorized("invalid credentials"))
}

pub fn issue_access_token(
    user: &UserRecord,
    settings: &AuthSettings,
) -> Result<(String, u64), RegistryError> {
    let now = now_unix();
    let claims = RegistryClaims {
        sub: user.uid.clone(),
        email: user.email.clone(),
        is_admin: user.is_admin,
        iat: now as usize,
        exp: now.saturating_add(settings.access_token_ttl_secs) as usize,
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(settings.jwt_secret.as_bytes()),
    )
    .map_err(|err| RegistryError::internal(format!("jwt issue failed: {}", err)))?;
    Ok((token, settings.access_token_ttl_secs))
}

pub fn hash_refresh_token(token: &str) -> String {
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest;
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn claims_from_headers(
    headers: &HeaderMap,
    settings: &AuthSettings,
) -> Result<RegistryClaims, RegistryError> {
    let Some(value) = headers.get(axum::http::header::AUTHORIZATION) else {
        return Err(RegistryError::unauthorized("missing bearer token"));
    };
    let value = value
        .to_str()
        .map_err(|_| RegistryError::unauthorized("invalid authorization header"))?;
    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err(RegistryError::unauthorized("missing bearer token"));
    };
    decode::<RegistryClaims>(
        token,
        &DecodingKey::from_secret(settings.jwt_secret.as_bytes()),
        &Validation::new(Algorithm::HS256),
    )
    .map(|data| data.claims)
    .map_err(|_| RegistryError::unauthorized("invalid or expired token"))
}

pub fn require_master_key(
    headers: &HeaderMap,
    settings: &AuthSettings,
) -> Result<(), RegistryError> {
    let Some(value) = headers.get("x-registry-master-key") else {
        return Err(RegistryError::forbidden("missing registry master key"));
    };
    let value = value
        .to_str()
        .map_err(|_| RegistryError::forbidden("invalid registry master key"))?;
    if value != settings.master_key {
        return Err(RegistryError::forbidden("invalid registry master key"));
    }
    Ok(())
}

pub fn require_admin_claims(
    headers: &HeaderMap,
    settings: &AuthSettings,
) -> Result<RegistryClaims, RegistryError> {
    let claims = claims_from_headers(headers, settings)?;
    if !claims.is_admin {
        return Err(RegistryError::forbidden("administrator access is required"));
    }
    Ok(claims)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
