use crate::database::database_constants::OBJECT_STORAGE_DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::object_storage::download::download_object_logic;
use crate::object_storage::metadata_parser::find_latest_object_metadata;
use crate::object_storage::object_parser::{get_object_logic, stream_object_logic};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    CreateSignedUrlRequest, ObjectMetadata, SignedUrlDisposition, SignedUrlResponse,
};
use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use ini::Ini;
use mongodb::bson::doc;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode, utf8_percent_encode};
use sha2::Sha256;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, LazyLock};

static URL_SIGNING_SECRET: LazyLock<String> =
    LazyLock::new(|| env::var("URL_SIGNING_SECRET").expect("URL_SIGNING_SECRET must be set"));

static BASE_URL: LazyLock<String> = LazyLock::new(|| {
    let config = Ini::load_from_file("config.ini").unwrap_or_else(|_| Ini::new());
    let server_section = config.section(Some("server"));

    server_section
        .and_then(|sec| sec.get("sharable_url"))
        .unwrap_or("https://127.0.0.1:50001").to_string()
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
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<CreateSignedUrlRequest>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let metadata = find_latest_object_metadata(
        &state.db_client,
        &payload.fragment,
        &payload.object_key,
        &uid,
    )
    .await?;

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
        metadata.owner_uid, metadata.fragment, metadata.key, expires_timestamp, disposition_str
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
        utf8_percent_encode(&payload.object_key, QUERY_ENCODE_SET),
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
        let owner_uid_encoded = params
            .get("owner_uid")
            .ok_or_else(|| AppError::BadRequest("Missing owner_uid.".to_string()))?;
        let fragment_encoded = params
            .get("fragment")
            .ok_or_else(|| AppError::BadRequest("Missing fragment.".to_string()))?;
        let object_key_encoded = params
            .get("object_key")
            .ok_or_else(|| AppError::BadRequest("Missing object_key.".to_string()))?;
        let expires_str = params
            .get("expires")
            .ok_or_else(|| AppError::BadRequest("Missing expiration.".to_string()))?;
        let provided_signature = params
            .get("signature")
            .ok_or_else(|| AppError::BadRequest("Missing signature.".to_string()))?;

        let owner_uid = percent_decode(owner_uid_encoded.as_bytes())
            .decode_utf8()
            .map_err(|_| AppError::BadRequest("Invalid owner_uid encoding.".to_string()))?
            .to_string();
        let fragment = percent_decode(fragment_encoded.as_bytes())
            .decode_utf8()
            .map_err(|_| AppError::BadRequest("Invalid fragment encoding.".to_string()))?
            .to_string();
        let object_key = percent_decode(object_key_encoded.as_bytes())
            .decode_utf8()
            .map_err(|_| AppError::BadRequest("Invalid object_key encoding.".to_string()))?
            .to_string();

        let expires_timestamp = expires_str
            .parse::<i64>()
            .map_err(|_| AppError::BadRequest("Invalid expiration format.".to_string()))?;

        let disposition = match params.get("disposition") {
            Some(encoded_val) => percent_decode(encoded_val.as_bytes())
                .decode_utf8()
                .map_err(|_| AppError::BadRequest("Invalid disposition encoding.".to_string()))?
                .to_string(),
            None => "view".to_string(),
        };

        Ok(Self {
            owner_uid,
            fragment,
            object_key,
            expires_timestamp,
            provided_signature: provided_signature.clone(),
            disposition,
        })
    }
}

pub async fn get_shared_object(
    State(state): State<Arc<AppState>>,
    params: Query<HashMap<String, String>>,
    headers: HeaderMap,
    req: axum::http::Request<Body>,
) -> Result<Response, AppError> {
    let query = SignedUrlQuery::from_params(&params)?;

    if Utc::now().timestamp() > query.expires_timestamp {
        return Err(AppError::NotFound);
    }

    let collection = state
        .db_client
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    let metadata = collection
        .find_one(doc! { "owner_uid": &query.owner_uid, "fragment": &query.fragment, "key": &query.object_key, "is_latest": true })
        .await
        .map_err(|_| AppError::Conflict("Database query failed.".to_string()))?
        .ok_or(AppError::NotFound)?;

    let data_to_sign = format!(
        "{}:{}:{}:{}:{}",
        metadata.owner_uid,
        metadata.fragment,
        metadata.key,
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

    match query.disposition.as_str() {
        "view" => {
            let response = get_object_logic(metadata, req).await?;
            Ok(response.into_response())
        }
        "stream" => {
            let response = stream_object_logic(headers, metadata).await?;
            Ok(response.into_response())
        }
        "download" => {
            let response = download_object_logic(req, metadata).await?;
            Ok(response.into_response())
        }
        _ => Err(AppError::BadRequest("Invalid disposition".to_string())),
    }
}
