use crate::enums::app_error_enum::AppError;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::env;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

type SignedUrlHmac = Hmac<Sha256>;

static SIGNED_URL_SECRET: LazyLock<Vec<u8>> = LazyLock::new(|| {
    env::var("SIGNED_READ_URL_SECRET")
        .ok()
        .filter(|secret| !secret.trim().is_empty())
        .or_else(|| {
            env::var("JWT_SECRET")
                .ok()
                .filter(|secret| !secret.trim().is_empty())
        })
        .expect("non-empty SIGNED_READ_URL_SECRET or JWT_SECRET must be set for signed urls")
        .into_bytes()
});

pub(crate) fn now_unix() -> Result<i64, AppError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| AppError::InternalServerError(format!("clock drift detected: {error}")))?;
    i64::try_from(now.as_secs())
        .map_err(|_| AppError::InternalServerError("system clock exceeds supported range".into()))
}

pub(crate) fn sign(payload: &[u8]) -> String {
    sign_with_secret(SIGNED_URL_SECRET.as_slice(), payload)
}

pub(crate) fn verify(payload: &[u8], signature: &str) -> Result<(), AppError> {
    verify_with_secret(SIGNED_URL_SECRET.as_slice(), payload, signature)
}

pub(crate) fn sign_with_secret(secret: &[u8], payload: &[u8]) -> String {
    let mut mac = SignedUrlHmac::new_from_slice(secret).expect("valid HMAC key");
    mac.update(payload);
    hex::encode(mac.finalize().into_bytes())
}

pub(crate) fn verify_with_secret(
    secret: &[u8],
    payload: &[u8],
    signature: &str,
) -> Result<(), AppError> {
    let signature = hex::decode(signature.trim()).map_err(|_| AppError::Unauthorized)?;
    let mut mac = SignedUrlHmac::new_from_slice(secret).expect("valid HMAC key");
    mac.update(payload);
    mac.verify_slice(&signature)
        .map_err(|_| AppError::Unauthorized)
}

#[cfg(test)]
mod tests {
    use super::{sign_with_secret, verify_with_secret};

    #[test]
    fn signatures_are_domain_payload_bound() {
        let signature = sign_with_secret(b"test-secret", b"n2\npayload-a");
        assert!(verify_with_secret(b"test-secret", b"n2\npayload-a", &signature).is_ok());
        assert!(verify_with_secret(b"test-secret", b"n2\npayload-b", &signature).is_err());
        assert!(verify_with_secret(b"test-secret", b"n1\npayload-a", &signature).is_err());
        assert!(verify_with_secret(b"other-secret", b"n2\npayload-a", &signature).is_err());
    }
}
