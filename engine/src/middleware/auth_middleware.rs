//! Fragment authentication is control state, independent of physical objects.
//! LINK authentication binds each supplied key to the stable fragment ID.
use crate::{
    core::{Error, Result},
    settings::config::Fragment,
};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use n1_link::{PoolId, noa::Permissions};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub session_id: String,
    pub sub: PoolId,
    pub permissions: Permissions,
    pub operator: bool,
    pub exp: u64,
    pub iat: u64,
    pub iss: String,
    pub aud: String,
}
impl Claims {
    pub fn live(&self) -> Result<()> {
        if self.exp <= now() {
            Err(Error::Unauthorized)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
pub struct Auth {
    activity: Arc<crate::core::upload::activity::Activity>,
    pub signed_reads: crate::url::signed_read::SignedReads,
    session_root: PathBuf,
    fragments: Arc<Vec<Fragment>>,
    encoding: EncodingKey,
    decoding: DecodingKey,
    logins: Arc<Semaphore>,
}

impl Auth {
    pub fn new(fragments: Vec<Fragment>, secret: &[u8; 32]) -> Self {
        Self::with_session_root(
            fragments,
            secret,
            std::env::temp_dir().join(format!("n1-auth-{}", ulid::Ulid::new())),
        )
    }
    pub fn with_session_root(
        fragments: Vec<Fragment>,
        secret: &[u8; 32],
        session_root: PathBuf,
    ) -> Self {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("valid key");
        mac.update(b"Litiaina/N1/NOA/auth/v2\0");
        let key = mac.finalize().into_bytes();
        Self {
            activity: crate::core::upload::activity::Activity::new(),
            signed_reads: crate::url::signed_read::SignedReads::new(secret),
            session_root,
            fragments: Arc::new(fragments),
            encoding: EncodingKey::from_secret(&key),
            decoding: DecodingKey::from_secret(&key),
            logins: Arc::new(Semaphore::new(4)),
        }
    }
    pub(crate) fn with_activity(
        mut self,
        activity: Arc<crate::core::upload::activity::Activity>,
    ) -> Self {
        self.activity = activity;
        self
    }
    pub async fn login(
        &self,
        alias: String,
        secret: String,
        requested: Option<Permissions>,
        ttl: Option<u64>,
    ) -> Result<Login> {
        let auth = self.clone();
        self.activity
            .run(async move { auth.login_owned(alias, secret, requested, ttl).await })
            .await
    }
    async fn login_owned(
        &self,
        alias: String,
        secret: String,
        requested: Option<Permissions>,
        ttl: Option<u64>,
    ) -> Result<Login> {
        if alias.len() > 256 || secret.len() > 4096 {
            return Err(Error::Unauthorized);
        }
        let permit = self
            .logins
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Unavailable("authentication admission is full".into()))?;
        let fragments = self.fragments.clone();
        let fragment = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let fragment = fragments
                .iter()
                .find(|f| f.alias == alias)
                .ok_or(Error::Unauthorized)?;
            let hash = PasswordHash::new(&fragment.secret_hash).map_err(|_| Error::Unauthorized)?;
            Argon2::default()
                .verify_password(secret.as_bytes(), &hash)
                .map_err(|_| Error::Unauthorized)?;
            Ok::<_, Error>(fragment.clone())
        })
        .await
        .map_err(|_| Error::Unavailable("authentication worker failed".into()))??;
        let ttl = ttl.unwrap_or(3600);
        if !(1..=86400).contains(&ttl) {
            return Err(Error::Invalid(
                "token lifetime must be 1..=86400 seconds".into(),
            ));
        }
        let permissions = requested
            .unwrap_or(fragment.permissions)
            .intersect(fragment.permissions);
        let operator = fragment.operator && requested.is_none();
        let session = super::refresh_store::create_refresh_session(
            &self.session_root,
            fragment.id,
            super::refresh_store::SessionPermissions {
                operator,
                read: permissions.read,
                write: permissions.write,
                delete: permissions.delete,
                repair: permissions.repair,
            },
            ttl,
            7 * 86400,
            super::refresh_store::credential_fingerprint(&fragment.secret_hash),
        )
        .await
        .map_err(refresh_error)?;
        let claims = Claims {
            session_id: session.session_id,
            sub: fragment.id,
            permissions,
            operator,
            exp: now() + ttl,
            iat: now(),
            iss: "Litiaina N1".into(),
            aud: "NOA2".into(),
        };
        let token = jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)
            .map_err(|_| Error::Unavailable("token encoding failed".into()))?;
        Ok(Login {
            token,
            session: claims,
            refresh_token: session.refresh_token,
            refresh_expires_in: session.refresh_expires_in,
        })
    }
    pub async fn refresh(&self, token: &str) -> Result<Login> {
        if token.len() > 128 {
            return Err(Error::Unauthorized);
        }
        let auth = self.clone();
        let token = token.to_owned();
        self.activity
            .run(async move { auth.refresh_owned(&token).await })
            .await
    }
    async fn refresh_owned(&self, token: &str) -> Result<Login> {
        let rotated = super::refresh_store::rotate_refresh_session(
            &self.session_root,
            &self.fragments,
            token,
        )
        .await
        .map_err(refresh_error)?;
        let fragment = self
            .fragments
            .iter()
            .find(|f| f.id == rotated.scope)
            .ok_or(Error::Unauthorized)?;
        let permissions = Permissions {
            read: rotated.permissions.read,
            write: rotated.permissions.write,
            delete: rotated.permissions.delete,
            repair: rotated.permissions.repair,
        }
        .intersect(fragment.permissions);
        let claims = Claims {
            session_id: rotated.session_id,
            sub: rotated.scope,
            permissions,
            operator: rotated.permissions.operator && fragment.operator,
            exp: now() + rotated.access_expires_in,
            iat: now(),
            iss: "Litiaina N1".into(),
            aud: "NOA2".into(),
        };
        let token = jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)
            .map_err(|_| Error::Unavailable("token encoding failed".into()))?;
        Ok(Login {
            token,
            session: claims,
            refresh_token: rotated.refresh_token,
            refresh_expires_in: rotated.refresh_expires_in,
        })
    }
    pub async fn logout(&self, claims: &Claims) -> Result<()> {
        let auth = self.clone();
        let session = claims.session_id.clone();
        self.activity
            .run(async move {
                super::refresh_store::revoke_session(&auth.session_root, &session)
                    .await
                    .map_err(refresh_error)
            })
            .await
    }
    pub fn verify(&self, token: &str) -> Result<Claims> {
        if token.len() > 8192 {
            return Err(Error::Unauthorized);
        }
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 0;
        validation.set_issuer(&["Litiaina N1"]);
        validation.set_audience(&["NOA2"]);
        let mut claims = jsonwebtoken::decode::<Claims>(token, &self.decoding, &validation)
            .map_err(|_| Error::Unauthorized)?
            .claims;
        claims.live()?;
        let fragment = self
            .fragments
            .iter()
            .find(|f| f.id == claims.sub)
            .ok_or(Error::Unauthorized)?;
        claims.permissions = claims.permissions.intersect(fragment.permissions);
        claims.operator &= fragment.operator;
        Ok(claims)
    }
    pub fn signed_claims(&self, grant: &n1_link::noa::ReadGrant) -> Result<Claims> {
        self.signed_reads.verify(grant)?;
        let fragment = self
            .fragments
            .iter()
            .find(|f| f.id == grant.fragment)
            .ok_or(Error::Unauthorized)?;
        if !fragment.permissions.read {
            return Err(Error::Forbidden);
        }
        Ok(Claims {
            sub: fragment.id,
            permissions: Permissions {
                read: true,
                ..Default::default()
            },
            operator: false,
            exp: grant.expires_at as u64,
            iat: now(),
            iss: "Litiaina N1".into(),
            aud: "NOA2".into(),
            session_id: String::new(),
        })
    }
}
#[derive(Serialize)]
pub struct Login {
    pub token: String,
    pub session: Claims,
    pub refresh_token: String,
    pub refresh_expires_in: u64,
}
fn refresh_error(error: super::refresh_store::RefreshStoreError) -> Error {
    match error {
        super::refresh_store::RefreshStoreError::Io(error) => Error::Io(error),
        super::refresh_store::RefreshStoreError::Json(error) => {
            Error::Corrupt(format!("invalid refresh session: {error}"))
        }
        _ => Error::Unauthorized,
    }
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests;
