use crate::core::disk_manager::{ObjectKeyQuery, ObjectPathManager};
use crate::core::download::download_object as download_object_impl;
use crate::core::metadata_parser::get_version_metadata;
use crate::core::retrieve::{get_object_metadata_direct, stream_object as stream_object_impl};
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use axum::Json as AxumJson;
use axum::extract::{Path, Query};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use hmac::{Hmac, Mac};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::env;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

const SIGNED_READ_URL_DEFAULT_TTL_SECS: u64 = 60 * 60;
const SIGNED_READ_URL_MAX_TTL_SECS: u64 = 6 * 60 * 60;

type SignedReadHmac = Hmac<Sha256>;

static SIGNED_READ_URL_SECRET: LazyLock<Vec<u8>> = LazyLock::new(|| {
    env::var("SIGNED_READ_URL_SECRET")
        .or_else(|_| env::var("JWT_SECRET"))
        .expect("SIGNED_READ_URL_SECRET or JWT_SECRET must be set for signed read urls")
        .into_bytes()
});

#[derive(Debug, Deserialize)]
pub struct SignedReadUrlRequest {
    pub object_key: String,
    #[serde(default)]
    pub disposition: Option<String>,
    #[serde(default)]
    pub expires_in: Option<u64>,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SignedReadUrlResponse {
    pub url: String,
    pub path: String,
    pub fragment: String,
    pub object_key: String,
    pub disposition: String,
    pub expires_at: i64,
    pub version_id: Option<String>,
    pub filename: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SignedReadObjectQuery {
    pub fragment: String,
    pub object_key: String,
    pub expires: i64,
    pub signature: String,
    #[serde(default)]
    pub disposition: Option<String>,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
}

#[derive(Debug, Clone)]
struct ValidatedSignedRead {
    disposition: String,
    version_id: Option<String>,
    filename: Option<String>,
}

fn current_unix_timestamp_secs() -> Result<i64, AppError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| AppError::InternalServerError(format!("clock drift detected: {}", e)))?;
    i64::try_from(now.as_secs())
        .map_err(|_| AppError::InternalServerError("system clock exceeds supported range".into()))
}

fn normalize_signed_read_disposition(value: Option<&str>) -> Result<&'static str, AppError> {
    let normalized = value.unwrap_or("view").trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" | "view" | "inline" => Ok("view"),
        "stream" => Ok("stream"),
        "download" | "attachment" => Ok("download"),
        other => Err(AppError::BadRequest(format!(
            "Unsupported signed read disposition '{}'",
            other
        ))),
    }
}

fn normalize_optional_non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn signed_read_ttl_secs(requested: Option<u64>) -> u64 {
    requested
        .unwrap_or(SIGNED_READ_URL_DEFAULT_TTL_SECS)
        .clamp(1, SIGNED_READ_URL_MAX_TTL_SECS)
}

fn signed_read_payload(
    fragment: &str,
    object_key: &str,
    expires: i64,
    disposition: &str,
    version_id: Option<&str>,
    filename: Option<&str>,
) -> String {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        fragment,
        object_key,
        expires,
        disposition,
        version_id.unwrap_or(""),
        filename.unwrap_or("")
    )
}

fn sign_signed_read_with_secret(
    secret: &[u8],
    fragment: &str,
    object_key: &str,
    expires: i64,
    disposition: &str,
    version_id: Option<&str>,
    filename: Option<&str>,
) -> String {
    let payload = signed_read_payload(
        fragment,
        object_key,
        expires,
        disposition,
        version_id,
        filename,
    );
    let mut mac = SignedReadHmac::new_from_slice(secret).expect("valid HMAC key");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn sign_signed_read(
    fragment: &str,
    object_key: &str,
    expires: i64,
    disposition: &str,
    version_id: Option<&str>,
    filename: Option<&str>,
) -> String {
    sign_signed_read_with_secret(
        SIGNED_READ_URL_SECRET.as_slice(),
        fragment,
        object_key,
        expires,
        disposition,
        version_id,
        filename,
    )
}

fn validate_signed_read_query_with_secret(
    secret: &[u8],
    query: &SignedReadObjectQuery,
    now: i64,
) -> Result<ValidatedSignedRead, AppError> {
    let object_key = ObjectPathManager::canonical_object_key(&query.object_key);
    if query.fragment.trim().is_empty() || object_key.is_empty() {
        return Err(AppError::BadRequest(
            "Signed read URL is missing a fragment or object key".into(),
        ));
    }

    if query.expires < now {
        return Err(AppError::Unauthorized);
    }

    let disposition = normalize_signed_read_disposition(query.disposition.as_deref())?;
    let version_id = normalize_optional_non_empty(query.version_id.as_deref());
    let filename = normalize_optional_non_empty(query.filename.as_deref());
    if version_id.is_some() && disposition != "download" {
        return Err(AppError::BadRequest(
            "Signed version reads are only supported for download disposition".into(),
        ));
    }

    let payload = signed_read_payload(
        &query.fragment,
        &object_key,
        query.expires,
        disposition,
        version_id.as_deref(),
        filename.as_deref(),
    );

    let signature = hex::decode(query.signature.trim()).map_err(|_| AppError::Unauthorized)?;
    let mut mac = SignedReadHmac::new_from_slice(secret).expect("valid HMAC key");
    mac.update(payload.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| AppError::Unauthorized)?;

    Ok(ValidatedSignedRead {
        disposition: disposition.to_string(),
        version_id,
        filename,
    })
}

fn validate_signed_read_query(
    query: &SignedReadObjectQuery,
) -> Result<ValidatedSignedRead, AppError> {
    validate_signed_read_query_with_secret(
        SIGNED_READ_URL_SECRET.as_slice(),
        query,
        current_unix_timestamp_secs()?,
    )
}

fn build_signed_read_path(
    fragment: &str,
    object_key: &str,
    expires: i64,
    disposition: &str,
    signature: &str,
    version_id: Option<&str>,
    filename: Option<&str>,
) -> String {
    let fragment = utf8_percent_encode(fragment, NON_ALPHANUMERIC).to_string();
    let object_key = utf8_percent_encode(object_key, NON_ALPHANUMERIC).to_string();
    let disposition = utf8_percent_encode(disposition, NON_ALPHANUMERIC).to_string();
    let signature = utf8_percent_encode(signature, NON_ALPHANUMERIC).to_string();

    let mut path = format!(
        "/noa/v1/objects/signed-read?fragment={}&object_key={}&expires={}&disposition={}&signature={}",
        fragment, object_key, expires, disposition, signature
    );

    if let Some(version_id) = version_id {
        path.push_str("&version_id=");
        path.push_str(&utf8_percent_encode(version_id, NON_ALPHANUMERIC).to_string());
    }

    if let Some(filename) = filename {
        path.push_str("&filename=");
        path.push_str(&utf8_percent_encode(filename, NON_ALPHANUMERIC).to_string());
    }

    path
}

async fn ensure_signed_read_target_exists(
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<(), AppError> {
    if let Some(version_id) = version_id {
        get_version_metadata(fragment, object_key, version_id, fragment).await?;
    } else {
        get_object_metadata_direct(fragment, object_key).await?;
    }
    Ok(())
}

pub async fn mint_signed_read_url(
    claims: Claims,
    AxumJson(req): AxumJson<SignedReadUrlRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    let object_key = ObjectPathManager::canonical_object_key(&req.object_key);
    if object_key.is_empty() {
        return Err(AppError::BadRequest(
            "object_key is required for signed read urls".into(),
        ));
    }

    let disposition = normalize_signed_read_disposition(req.disposition.as_deref())?;
    let version_id = normalize_optional_non_empty(req.version_id.as_deref());
    let filename = normalize_optional_non_empty(req.filename.as_deref());
    if version_id.is_some() && disposition != "download" {
        return Err(AppError::BadRequest(
            "Signed version reads are only supported for download disposition".into(),
        ));
    }

    ensure_signed_read_target_exists(&claims.fragment, &object_key, version_id.as_deref()).await?;

    let expires_at = current_unix_timestamp_secs()? + signed_read_ttl_secs(req.expires_in) as i64;
    let signature = sign_signed_read(
        &claims.fragment,
        &object_key,
        expires_at,
        disposition,
        version_id.as_deref(),
        filename.as_deref(),
    );
    let path = build_signed_read_path(
        &claims.fragment,
        &object_key,
        expires_at,
        disposition,
        &signature,
        version_id.as_deref(),
        filename.as_deref(),
    );

    Ok(AxumJson(SignedReadUrlResponse {
        url: path.clone(),
        path,
        fragment: claims.fragment,
        object_key,
        disposition: disposition.to_string(),
        expires_at,
        version_id,
        filename,
    }))
}

pub async fn read_object_via_signed_url(
    headers: HeaderMap,
    Query(query): Query<SignedReadObjectQuery>,
) -> Result<impl IntoResponse, AppError> {
    let validated = validate_signed_read_query(&query)?;
    let object_key = ObjectPathManager::canonical_object_key(&query.object_key);
    let claims = Claims {
        fragment: query.fragment.clone(),
        fragment_alias: None,
        read: true,
        write: false,
        modify: false,
        delete: false,
        exp: usize::try_from(query.expires.max(0)).unwrap_or(usize::MAX),
    };

    match validated.disposition.as_str() {
        "download" => {
            let response = download_object_impl(
                claims,
                Path(query.fragment),
                Query(ObjectKeyQuery {
                    object_key,
                    version_id: validated.version_id,
                    filename: validated.filename,
                }),
                headers,
            )
            .await?;
            Ok(response.into_response())
        }
        "stream" | "view" => {
            let response =
                stream_object_impl(claims, headers, Path((query.fragment, object_key))).await?;
            Ok(response.into_response())
        }
        _ => Err(AppError::BadRequest(
            "Unsupported signed read disposition".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SIGNED_READ_URL_DEFAULT_TTL_SECS, SIGNED_READ_URL_MAX_TTL_SECS, SignedReadObjectQuery,
        build_signed_read_path, normalize_signed_read_disposition, sign_signed_read_with_secret,
        signed_read_ttl_secs, validate_signed_read_query_with_secret,
    };
    use crate::enums::app_error_enum::AppError;
    use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

    #[test]
    fn ttl_is_clamped_to_supported_range() {
        assert_eq!(signed_read_ttl_secs(None), SIGNED_READ_URL_DEFAULT_TTL_SECS);
        assert_eq!(signed_read_ttl_secs(Some(0)), 1);
        assert_eq!(
            signed_read_ttl_secs(Some(SIGNED_READ_URL_MAX_TTL_SECS + 5)),
            SIGNED_READ_URL_MAX_TTL_SECS
        );
    }

    #[test]
    fn disposition_normalization_supports_expected_aliases() {
        assert_eq!(
            normalize_signed_read_disposition(Some("inline")).expect("inline disposition"),
            "view"
        );
        assert_eq!(
            normalize_signed_read_disposition(Some("attachment")).expect("attachment disposition"),
            "download"
        );
        assert!(matches!(
            normalize_signed_read_disposition(Some("weird")),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn signed_read_validation_rejects_tampering_and_expiry() {
        let secret = b"unit-test-secret";
        let fragment = "frag";
        let object_key = "videos/demo.mp4";
        let expires = 1_900_000_000i64;
        let signature = sign_signed_read_with_secret(
            secret, fragment, object_key, expires, "stream", None, None,
        );

        let valid = SignedReadObjectQuery {
            fragment: fragment.to_string(),
            object_key: object_key.to_string(),
            expires,
            signature: signature.clone(),
            disposition: Some("stream".to_string()),
            version_id: None,
            filename: None,
        };
        let validated =
            validate_signed_read_query_with_secret(secret, &valid, expires - 10).expect("valid");
        assert_eq!(validated.disposition, "stream");
        assert!(validated.version_id.is_none());

        let tampered = SignedReadObjectQuery {
            object_key: "videos/other.mp4".to_string(),
            ..valid
        };
        assert!(matches!(
            validate_signed_read_query_with_secret(secret, &tampered, expires - 10),
            Err(AppError::Unauthorized)
        ));

        let expired = SignedReadObjectQuery {
            fragment: fragment.to_string(),
            object_key: object_key.to_string(),
            expires,
            signature,
            disposition: Some("stream".to_string()),
            version_id: None,
            filename: None,
        };
        assert!(matches!(
            validate_signed_read_query_with_secret(secret, &expired, expires + 1),
            Err(AppError::Unauthorized)
        ));
    }

    #[test]
    fn signed_version_reads_require_download_disposition() {
        let secret = b"unit-test-secret";
        let fragment = "frag";
        let object_key = "archive.tar";
        let expires = 1_900_000_000i64;
        let version_id = "01TESTVERSION";
        let signature = sign_signed_read_with_secret(
            secret,
            fragment,
            object_key,
            expires,
            "download",
            Some(version_id),
            None,
        );

        let valid = SignedReadObjectQuery {
            fragment: fragment.to_string(),
            object_key: object_key.to_string(),
            expires,
            signature,
            disposition: Some("download".to_string()),
            version_id: Some(version_id.to_string()),
            filename: None,
        };
        assert!(validate_signed_read_query_with_secret(secret, &valid, expires - 5).is_ok());

        let signature = sign_signed_read_with_secret(
            secret,
            fragment,
            object_key,
            expires,
            "stream",
            Some(version_id),
            None,
        );
        let invalid = SignedReadObjectQuery {
            fragment: fragment.to_string(),
            object_key: object_key.to_string(),
            expires,
            signature,
            disposition: Some("stream".to_string()),
            version_id: Some(version_id.to_string()),
            filename: None,
        };
        assert!(matches!(
            validate_signed_read_query_with_secret(secret, &invalid, expires - 5),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn signed_read_path_contains_encoded_values() {
        let encoded_object_key =
            utf8_percent_encode("folder/video clip.mp4", NON_ALPHANUMERIC).to_string();
        let encoded_version_id = utf8_percent_encode("ver/1", NON_ALPHANUMERIC).to_string();
        let encoded_filename = utf8_percent_encode("video clip.mp4", NON_ALPHANUMERIC).to_string();
        let path = build_signed_read_path(
            "frag hash",
            "folder/video clip.mp4",
            12345,
            "stream",
            "abc123",
            Some("ver/1"),
            Some("video clip.mp4"),
        );
        assert!(path.starts_with("/noa/v1/objects/signed-read?"));
        assert!(path.contains("fragment=frag%20hash"));
        assert!(path.contains(&format!("object_key={}", encoded_object_key)));
        assert!(path.contains(&format!("version_id={}", encoded_version_id)));
        assert!(path.contains(&format!("filename={}", encoded_filename)));
    }
}
