use crate::enums::app_error_enum::AppError;
use crate::object_storage::retrieve::stream_object_logic;
use crate::object_storage::metadata_parser::find_latest_object_metadata;
use crate::routes::storage_routes::AppState;
use crate::structs::object_storage_structs::{
    CreateSignedUrlRequest, ObjectMetadata, SignedUrlDisposition, SignedUrlResponse,
};
use crate::utils::header_extractor::extract_uid_from_fragmented_header;
use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use mongodb::bson::doc;
use sha2::Sha256;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, LazyLock};
use crate::object_storage::download::download_object_logic;

static URL_SIGNING_SECRET: LazyLock<String> =
    LazyLock::new(|| env::var("URL_SIGNING_SECRET").expect("URL_SIGNING_SECRET must be set"));

pub async fn create_signed_url(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    AxumJson(payload): AxumJson<CreateSignedUrlRequest>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let metadata = find_latest_object_metadata(
        &state.db_client,
        &payload.fragment,
        &payload.object_key,
        &uid,
    )
    .await?;

    let expires_in = Duration::seconds(payload.expires_in as i64);
    let expires_at = Utc::now() + expires_in;
    let expires_timestamp = expires_at.timestamp();

    let disposition_str = match payload.disposition {
        SignedUrlDisposition::Inline => "inline",
        SignedUrlDisposition::Attachment => "attachment",
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

    let base_url = "https://storage.litiaina.com";
    let url = format!(
        "{}/v4/public/share?fragment={}&object_key={}&expires={}&signature={}&disposition={}",
        base_url,
        urlencoding::encode(&payload.fragment),
        urlencoding::encode(&payload.object_key),
        expires_timestamp,
        signature,
        disposition_str
    );

    Ok(AxumJson(SignedUrlResponse {
        url,
        expires_at: expires_at.to_rfc3339(),
    }))
}

pub async fn get_shared_object(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    req: axum::http::Request<Body>,
) -> Result<Response, AppError> {
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
    let disposition_str = params
        .get("disposition")
        .cloned()
        .unwrap_or_else(|| "inline".to_string());

    let fragment = urlencoding::decode(fragment_encoded)
        .map_err(|_| AppError::BadRequest("Invalid fragment encoding.".to_string()))?
        .to_string();
    let object_key = urlencoding::decode(object_key_encoded)
        .map_err(|_| AppError::BadRequest("Invalid object_key encoding.".to_string()))?
        .to_string();

    let expires_timestamp = expires_str
        .parse::<i64>()
        .map_err(|_| AppError::BadRequest("Invalid expiration format.".to_string()))?;

    if Utc::now().timestamp() > expires_timestamp {
        return Err(AppError::BadRequest("URL has expired.".to_string()));
    }

    let collection = state
        .db_client
        .database("litiaina-storage")
        .collection::<ObjectMetadata>("objects");
    let metadata = collection
        .find_one(doc! { "fragment": &fragment, "key": &object_key, "is_latest": true })
        .await
        .map_err(|_| AppError::Conflict("Database query failed.".to_string()))?
        .ok_or(AppError::BadRequest("Object not found.".to_string()))?;

    let data_to_sign = format!(
        "{}:{}:{}:{}:{}",
        metadata.owner_uid, metadata.fragment, metadata.key, expires_timestamp, disposition_str
    );
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(URL_SIGNING_SECRET.as_bytes())
        .map_err(|_| AppError::Conflict("Invalid signing key configuration.".to_string()))?;
    mac.update(data_to_sign.as_bytes());

    mac.verify_slice(
        &hex::decode(provided_signature)
            .map_err(|_| AppError::BadRequest("Invalid signature format.".to_string()))?,
    )
    .map_err(|_| AppError::BadRequest("Invalid signature.".to_string()))?;

    match disposition_str.as_str() {
        "attachment" => {
            let response = download_object_logic(req, metadata).await?;
            Ok(response.into_response())
        }
        _ => {
            let response = stream_object_logic(headers, metadata).await?;
            Ok(response.into_response())
        }
    }
}
