use crate::middleware::auth_middleware::JWT_KEYS;
use crate::structs::auth_structs::Claims;
use jsonwebtoken::{Validation, decode};
use std::time::{SystemTime, UNIX_EPOCH};

fn unix_timestamp_seconds() -> usize {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(usize::MAX)
}

#[derive(Debug, Clone, Default)]
pub struct NativeSession {
    bearer_token: Option<String>,
    claims: Option<Claims>,
}

impl NativeSession {
    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    pub fn claims(&self) -> Option<&Claims> {
        self.claims
            .as_ref()
            .filter(|claims| claims.exp > unix_timestamp_seconds())
    }

    pub fn set_bearer(&mut self, token: String) -> Result<&Claims, String> {
        let token_data = decode::<Claims>(&token, &JWT_KEYS.decoding, &Validation::default())
            .map_err(|_| "invalid bearer token".to_string())?;
        self.bearer_token = Some(token);
        self.claims = Some(token_data.claims);
        Ok(self.claims.as_ref().expect("claims were just set"))
    }

    pub fn clear(&mut self) {
        self.bearer_token = None;
        self.claims = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(exp: usize) -> Claims {
        Claims {
            fragment: "native-auth-test".to_string(),
            fragment_alias: None,
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp,
        }
    }

    #[test]
    fn cached_native_claims_stop_authorizing_after_expiry() {
        let now = unix_timestamp_seconds();
        let expired = NativeSession {
            bearer_token: Some("expired".to_string()),
            claims: Some(claims(now.saturating_sub(1))),
        };
        assert!(expired.claims().is_none());

        let current = NativeSession {
            bearer_token: Some("current".to_string()),
            claims: Some(claims(now.saturating_add(60))),
        };
        assert!(current.claims().is_some());
    }
}
