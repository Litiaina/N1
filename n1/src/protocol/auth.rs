use crate::middleware::auth_middleware::JWT_KEYS;
use crate::structs::auth_structs::Claims;
use jsonwebtoken::{Validation, decode};

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
        self.claims.as_ref()
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
