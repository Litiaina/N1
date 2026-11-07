//! The original four independent permissions and master-only operator grant.
use crate::{
    fragments::{self, Fragment},
    server::{
        App,
        error::{ApiError, ApiResult},
    },
};
use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::http::HeaderMap;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use n1_link::PoolId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub fragment: String,
    pub fragment_alias: String,
    pub operator: bool,
    pub read: bool,
    pub write: bool,
    pub modify: bool,
    pub delete: bool,
    pub exp: u64,
    pub epoch: u64,
}
impl Claims {
    pub fn id(&self) -> PoolId {
        PoolId(
            Ulid::from_string(&self.fragment)
                .expect("verified stable fragment")
                .to_bytes(),
        )
    }
    pub fn require(&self, permission: &str) -> ApiResult<()> {
        let allowed = match permission {
            "session" => true,
            "read" => self.read,
            "write" => self.write,
            "modify" => self.modify,
            "delete" => self.delete,
            "maintenance" => self.write && self.modify && self.delete,
            "operator" => self.operator,
            _ => false,
        };
        if allowed {
            Ok(())
        } else {
            Err(ApiError::forbidden())
        }
    }
}
#[derive(Deserialize)]
pub struct LoginRequest {
    pub fragment: String,
    pub secret: String,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub write: bool,
    #[serde(default)]
    pub modify: bool,
    #[serde(default)]
    pub delete: bool,
    pub expires_in: Option<u64>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Refresh {
    claims: Claims,
    family: String,
    expires: u64,
    used: bool,
}
pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub fn same_secret(a: &str, b: &str) -> bool {
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<Sha256>::new_from_slice(b"N1 credential comparison").expect("HMAC key");
    mac.update(a.as_bytes());
    let expected = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(b"N1 credential comparison").expect("HMAC key");
    mac.update(b.as_bytes());
    mac.verify_slice(&expected).is_ok()
}
pub async fn hash_secret(app: &App, secret: String) -> ApiResult<String> {
    if secret.is_empty() || secret.len() > 4096 {
        return Err(ApiError::invalid("invalid fragment access key"));
    }
    let permit = app
        .auth_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::unauthorized())?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        Argon2::default()
            .hash_password(secret.as_bytes(), &SaltString::generate(&mut OsRng))
            .map(|v| v.to_string())
            .map_err(|_| ApiError::invalid("cannot hash access key"))
    })
    .await
    .map_err(|_| ApiError::unauthorized())?
}
pub async fn login(app: &App, request: LoginRequest) -> ApiResult<serde_json::Value> {
    let identity = request.fragment.clone();
    let fragment = app
        .db
        .execute(move |db| fragments::resolve(&db, &identity))
        .await
        .map_err(|_| ApiError::unauthorized())?;
    let operator = same_secret(&request.secret, &app.config.master_key);
    if !operator {
        let hash = fragment.key_hash.clone();
        let secret = request.secret;
        let permit = app
            .auth_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::unauthorized())?;
        let valid = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            PasswordHash::new(&hash).ok().is_some_and(|hash| {
                Argon2::default()
                    .verify_password(secret.as_bytes(), &hash)
                    .is_ok()
            })
        })
        .await
        .map_err(|_| ApiError::unauthorized())?;
        if !valid {
            return Err(ApiError::unauthorized());
        }
    }
    let ttl = request
        .expires_in
        .unwrap_or(app.config.access_ttl)
        .min(app.config.access_max_ttl);
    if ttl == 0 {
        return Err(ApiError::invalid("access token lifetime must be positive"));
    }
    let claims = Claims {
        fragment: Ulid::from_bytes(fragment.id.0).to_string(),
        fragment_alias: fragment.alias,
        operator,
        read: request.read,
        write: request.write,
        modify: request.modify,
        delete: request.delete,
        exp: now() + ttl,
        epoch: fragment.epoch,
    };
    issue(app, claims, None).await
}
async fn issue(app: &App, claims: Claims, family: Option<String>) -> ApiResult<serde_json::Value> {
    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(app.config.jwt_secret.as_bytes()),
    )
    .map_err(|_| ApiError::unauthorized())?;
    let refresh_token = format!("{}.{}", Ulid::new(), Ulid::new());
    let record = Refresh {
        claims: claims.clone(),
        family: family.unwrap_or_else(|| Ulid::new().to_string()),
        expires: now() + app.config.refresh_ttl,
        used: false,
    };
    let key = fragments::key(format!(
        "refresh/{}",
        hex::encode(Sha256::digest(refresh_token.as_bytes()))
    ))?;
    let bytes = fragments::encode(&record)?;
    app.db
        .execute(move |db| db.transact(64, |tx| tx.put(crate::Table::Reclamation, &key, &bytes)))
        .await?;
    Ok(serde_json::json!({
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": claims.exp.saturating_sub(now()),
        "refresh_token": refresh_token,
        "refresh_expires_in": app.config.refresh_ttl
    }))
}
pub async fn refresh(app: &App, token: String, logout: bool) -> ApiResult<serde_json::Value> {
    let key = fragments::key(format!(
        "refresh/{}",
        hex::encode(Sha256::digest(token.as_bytes()))
    ))?;
    // Rotation and reuse detection are one durable transaction. On reuse the
    // family is revoked before the caller receives its authentication failure.
    let record = app
        .db
        .execute(move |db| {
            db.transact(64, |tx| {
                let mut record: Refresh = fragments::decode(
                    tx.get(crate::Table::Reclamation, &key)?
                        .ok_or(crate::Error::NotFound)?,
                )?;
                let family_key = fragments::key(format!("refresh-family/{}", record.family))?;
                if record.used || logout {
                    tx.put(crate::Table::Reclamation, &family_key, b"true")?;
                }
                let invalid = record.used
                    || logout
                    || record.expires <= now()
                    || tx.get(crate::Table::Reclamation, &family_key)?.is_some();
                record.used = true;
                tx.put(
                    crate::Table::Reclamation,
                    &key,
                    &fragments::encode(&record)?,
                )?;
                Ok(if invalid { None } else { Some(record) })
            })
        })
        .await
        .map_err(|_| ApiError::unauthorized())?;
    if logout {
        return Ok(serde_json::json!({"logged_out":true}));
    }
    let mut record = record.ok_or_else(ApiError::unauthorized)?;
    let identity = record.claims.fragment.clone();
    let fragment = app
        .db
        .execute(move |db| fragments::resolve(&db, &identity))
        .await
        .map_err(|_| ApiError::unauthorized())?;
    if fragment.epoch != record.claims.epoch {
        return Err(ApiError::unauthorized());
    }
    record.claims.fragment_alias = fragment.alias;
    record.claims.exp = now() + app.config.access_ttl;
    issue(app, record.claims, Some(record.family)).await
}
pub(crate) fn decode_claims(app: &App, headers: &HeaderMap) -> ApiResult<Claims> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(ApiError::unauthorized)?;
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 0;
    Ok(decode::<Claims>(
        token,
        &DecodingKey::from_secret(app.config.jwt_secret.as_bytes()),
        &validation,
    )
    .map_err(|_| ApiError::unauthorized())?
    .claims)
}
pub async fn authorize(app: &App, headers: &HeaderMap, permission: &str) -> ApiResult<Claims> {
    let claims = decode_claims(app, headers)?;
    let identity = claims.fragment.clone();
    let fragment: Fragment = app
        .db
        .execute(move |db| fragments::resolve(&db, &identity))
        .await
        .map_err(|_| ApiError::unauthorized())?;
    if claims.epoch != fragment.epoch {
        return Err(ApiError::unauthorized());
    }
    claims.require(permission)?;
    Ok(claims)
}
