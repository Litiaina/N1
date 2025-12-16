use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::get_metadata_with_cache;
use crate::object_storage::download::stream_object;
use crate::object_storage::metadata_parser::find_latest_metadata_on_disk;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    CreateSignedUrlRequest, SignedUrlDisposition, SignedUrlResponse,
};
use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use ini::Ini;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use sha2::Sha256;
use std::collections::HashMap;
use std::env;
use std::sync::LazyLock;

static URL_SIGNING_SECRET: LazyLock<String> =
    LazyLock::new(|| env::var("URL_SIGNING_SECRET").expect("URL_SIGNING_SECRET must be set"));

static BASE_URL: LazyLock<String> = LazyLock::new(|| {
    let config = Ini::load_from_file("config.ini").unwrap_or_else(|_| Ini::new());
    let server_section = config.section(Some("server"));

    server_section
        .and_then(|sec| sec.get("sharable_url"))
        .unwrap_or("https://127.0.0.1:50001")
        .to_string()
});

const MAX_EXPIRATION_SECONDS: i64 = 60 * 60 * 24 * 7;

const QUERY_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    .add(b'`');

pub async fn create_signed_url(
    claims: Claims,
    AxumJson(payload): AxumJson<CreateSignedUrlRequest>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let clean_key = payload.object_key.trim_start_matches('/');

    let metadata = find_latest_metadata_on_disk(&uid, &payload.fragment, clean_key).await?;

    if payload.expires_in as i64 > MAX_EXPIRATION_SECONDS {
        return Err(AppError::BadRequest(format!(
            "Expiration cannot exceed {} seconds.",
            MAX_EXPIRATION_SECONDS
        )));
    }

    let expires_in = Duration::seconds(payload.expires_in as i64);
    let expires_at = Utc::now() + expires_in;
    let expires_timestamp = expires_at.timestamp();

    let disposition_str = match payload.disposition {
        SignedUrlDisposition::View => "view",
        SignedUrlDisposition::Stream => "stream",
        SignedUrlDisposition::Download => "download",
    };

    let data_to_sign = format!(
        "{}:{}:{}:{}:{}",
        metadata.owner_uid,
        metadata.fragment,
        metadata.object_key,
        expires_timestamp,
        disposition_str
    );

    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(URL_SIGNING_SECRET.as_bytes())
        .map_err(|_| AppError::Conflict("Invalid signing key configuration.".to_string()))?;
    mac.update(data_to_sign.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    let url = format!(
        "{}/v4/public/share?owner_uid={}&fragment={}&object_key={}&expires={}&signature={}&disposition={}",
        BASE_URL.to_string(),
        utf8_percent_encode(&metadata.owner_uid, QUERY_ENCODE_SET),
        utf8_percent_encode(&payload.fragment, QUERY_ENCODE_SET),
        utf8_percent_encode(&metadata.object_key, QUERY_ENCODE_SET),
        expires_timestamp,
        signature,
        utf8_percent_encode(disposition_str, QUERY_ENCODE_SET)
    );

    Ok(AxumJson(SignedUrlResponse {
        url,
        expires_at: expires_at.to_rfc3339(),
    }))
}

struct SignedUrlQuery {
    owner_uid: String,
    fragment: String,
    object_key: String,
    expires_timestamp: i64,
    provided_signature: String,
    disposition: String,
}

impl SignedUrlQuery {
    fn from_params(params: &Query<HashMap<String, String>>) -> Result<Self, AppError> {
        let owner_uid = params
            .get("owner_uid")
            .ok_or_else(|| AppError::BadRequest("Missing owner_uid.".to_string()))?
            .to_string();

        let fragment = params
            .get("fragment")
            .ok_or_else(|| AppError::BadRequest("Missing fragment.".to_string()))?
            .to_string();

        let object_key = params
            .get("object_key")
            .ok_or_else(|| AppError::BadRequest("Missing object_key.".to_string()))?
            .to_string();

        let expires_str = params
            .get("expires")
            .ok_or_else(|| AppError::BadRequest("Missing expiration.".to_string()))?;

        let provided_signature = params
            .get("signature")
            .ok_or_else(|| AppError::BadRequest("Missing signature.".to_string()))?
            .to_string();

        let expires_timestamp = expires_str
            .parse::<i64>()
            .map_err(|_| AppError::BadRequest("Invalid expiration format.".to_string()))?;

        let disposition = params
            .get("disposition")
            .cloned()
            .unwrap_or_else(|| "view".to_string());

        Ok(Self {
            owner_uid,
            fragment,
            object_key,
            expires_timestamp,
            provided_signature,
            disposition,
        })
    }
}

pub async fn get_shared_object(
    params: Query<HashMap<String, String>>,
    headers: HeaderMap,
    _req: axum::http::Request<Body>,
) -> Result<Response, AppError> {
    let query = SignedUrlQuery::from_params(&params)?;

    if Utc::now().timestamp() > query.expires_timestamp {
        return Err(AppError::NotFound);
    }

    let clean_key = query.object_key.trim_start_matches('/');

    let metadata = get_metadata_with_cache(&query.fragment, clean_key, &query.owner_uid).await?;

    let data_to_sign = format!(
        "{}:{}:{}:{}:{}",
        metadata.owner_uid,
        metadata.fragment,
        metadata.object_key,
        query.expires_timestamp,
        query.disposition
    );

    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(URL_SIGNING_SECRET.as_bytes())
        .map_err(|_| AppError::Conflict("Invalid signing key configuration.".to_string()))?;
    mac.update(data_to_sign.as_bytes());

    mac.verify_slice(
        &hex::decode(&query.provided_signature)
            .map_err(|_| AppError::BadRequest("Invalid signature format.".to_string()))?,
    )
    .map_err(|_| AppError::BadRequest("Invalid signature.".to_string()))?;

    let disposition_type = match query.disposition.as_str() {
        "view" | "stream" => "inline",
        "download" => "attachment",
        _ => "inline",
    };

    let response = stream_object(metadata, headers, disposition_type).await?;
    Ok(response.into_response())
}
