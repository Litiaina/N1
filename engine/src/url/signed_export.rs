use crate::core::export::{download_export_with_signed_authority, signed_export_context};
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use crate::url::signing;
use axum::Json;
use axum::extract::{Path, Query};
use axum::http::HeaderMap;
use axum::response::Response;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};

const SIGNED_EXPORT_DEFAULT_TTL_SECONDS: u64 = 6 * 60 * 60;
const SIGNED_EXPORT_MAX_TTL_SECONDS: u64 = 24 * 60 * 60;

#[derive(Debug, Default, Deserialize)]
pub struct SignedExportUrlRequest {
    #[serde(default)]
    pub expires_in: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct SignedExportUrlResponse {
    pub export_id: String,
    pub path: String,
    pub url: String,
    pub expires_at: i64,
}

#[derive(Debug, Deserialize)]
pub struct SignedExportDownloadQuery {
    pub fragment: String,
    pub export_id: String,
    pub expires: i64,
    pub signature: String,
}

fn signed_export_payload(fragment: &str, export_id: &str, expires: i64) -> String {
    format!("n1-export-download\n{fragment}\n{export_id}\n{expires}")
}

fn sign_export(fragment: &str, export_id: &str, expires: i64) -> String {
    signing::sign(signed_export_payload(fragment, export_id, expires).as_bytes())
}

fn validate_export_signature(query: &SignedExportDownloadQuery, now: i64) -> Result<(), AppError> {
    if query.fragment.trim().is_empty() || query.export_id.trim().is_empty() {
        return Err(AppError::BadRequest(
            "signed export URL is missing its fragment or export identity".to_string(),
        ));
    }
    if query.expires < now {
        return Err(AppError::Unauthorized);
    }
    signing::verify(
        signed_export_payload(&query.fragment, &query.export_id, query.expires).as_bytes(),
        &query.signature,
    )
}

fn signed_export_path(fragment: &str, export_id: &str, expires: i64, signature: &str) -> String {
    format!(
        "/noa/v1/signed-exports/download?fragment={}&export_id={}&expires={}&signature={}",
        utf8_percent_encode(fragment, NON_ALPHANUMERIC),
        utf8_percent_encode(export_id, NON_ALPHANUMERIC),
        expires,
        utf8_percent_encode(signature, NON_ALPHANUMERIC),
    )
}

pub async fn mint_signed_export_url(
    claims: Claims,
    Path(export_id): Path<String>,
    Json(request): Json<SignedExportUrlRequest>,
) -> Result<Json<SignedExportUrlResponse>, AppError> {
    let now = signing::now_unix()?;
    let (fragment, artifact_expires_at) = signed_export_context(&claims, &export_id).await?;
    let ttl = request
        .expires_in
        .unwrap_or(SIGNED_EXPORT_DEFAULT_TTL_SECONDS)
        .clamp(1, SIGNED_EXPORT_MAX_TTL_SECONDS);
    let expires_at = now.saturating_add(ttl as i64).min(artifact_expires_at);
    if expires_at <= now {
        return Err(AppError::NotFound);
    }
    let signature = sign_export(&fragment, &export_id, expires_at);
    let path = signed_export_path(&fragment, &export_id, expires_at, &signature);
    Ok(Json(SignedExportUrlResponse {
        export_id,
        path: path.clone(),
        url: path,
        expires_at,
    }))
}

pub async fn download_signed_export(
    headers: HeaderMap,
    Query(query): Query<SignedExportDownloadQuery>,
) -> Result<Response, AppError> {
    validate_export_signature(&query, signing::now_unix()?)?;
    download_export_with_signed_authority(&query.fragment, &query.export_id, &headers).await
}

#[cfg(test)]
mod tests {
    use super::{
        SignedExportDownloadQuery, signed_export_path, signed_export_payload,
        validate_export_signature,
    };
    use crate::url::signing;

    #[test]
    fn signed_export_identity_is_tamper_evident_and_expiring() {
        let expires = 1_900_000_000;
        let payload = signed_export_payload("fragment-a", "export-a", expires);
        let signature = signing::sign_with_secret(b"unit-test-secret", payload.as_bytes());
        let valid = SignedExportDownloadQuery {
            fragment: "fragment-a".to_string(),
            export_id: "export-a".to_string(),
            expires,
            signature: signature.clone(),
        };
        assert!(
            signing::verify_with_secret(b"unit-test-secret", payload.as_bytes(), &signature)
                .is_ok()
        );
        assert!(validate_export_signature(&valid, expires + 1).is_err());

        let changed_payload = signed_export_payload("fragment-a", "export-b", expires);
        assert!(
            signing::verify_with_secret(
                b"unit-test-secret",
                changed_payload.as_bytes(),
                &signature,
            )
            .is_err()
        );
    }

    #[test]
    fn signed_export_path_encodes_authority_fields() {
        let path = signed_export_path("fragment/value", "export value", 123, "aa:bb");
        assert!(path.contains("fragment=fragment%2Fvalue"));
        assert!(path.contains("export_id=export%20value"));
        assert!(path.contains("signature=aa%3Abb"));
    }
}
