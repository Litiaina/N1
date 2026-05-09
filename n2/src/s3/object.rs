use crate::distribution_core::data_plane::{delete_n2_object, get_n2_object, put_n2_object};
use crate::distribution_core::map_store::load_n2_object_map;
use crate::distribution_core::model::{DeleteN2ObjectQuery, GetObjectQuery, PutObjectQuery};
use crate::enums::app_error_enum::AppError;
use crate::s3::auth::ensure_bucket_access;
use crate::s3::headers::X_AMZ_VERSION_ID;
use crate::s3::multipart::{
    abort_multipart_upload, create_multipart_upload, finish_complete_upload, list_uploaded_parts,
    prepare_complete_upload, upload_part,
};
use crate::s3::query::S3ObjectQuery;
use crate::s3::xml::unix_to_rfc3339;
use crate::structs::auth_structs::Claims;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED},
};
use axum::response::{IntoResponse, Response};

pub async fn s3_put_object(
    claims: Claims,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<S3ObjectQuery>,
    body: Body,
) -> Result<Response, AppError> {
    ensure_bucket_access(&claims, bucket.as_str(), false, true, false)?;
    if let (Some(upload_id), Some(part_number)) = (query.upload_id.as_deref(), query.part_number) {
        return upload_part(bucket.as_str(), key.as_str(), upload_id, part_number, body).await;
    }
    if query.upload_id.is_some() || query.part_number.is_some() {
        return Err(AppError::BadRequest(
            "S3 UploadPart requires uploadId and partNumber".to_string(),
        ));
    }
    let put = put_n2_object(
        claims,
        headers,
        Path((bucket, key)),
        Query(PutObjectQuery {
            uid: None,
            version_id: None,
            expected_epoch: None,
            kc: None,
            mc: None,
            kd: None,
            md: None,
            stripe_block_size: None,
            content_type: None,
        }),
        body,
    )
    .await?
    .0;
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    if let Ok(v) = HeaderValue::from_str(format!("\"{}\"", put.version_id).as_str()) {
        response.headers_mut().insert(ETAG, v);
    }
    if let Ok(v) = HeaderValue::from_str(put.version_id.as_str()) {
        response.headers_mut().insert(X_AMZ_VERSION_ID, v);
    }
    Ok(response)
}

pub async fn s3_post_object(
    claims: Claims,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<S3ObjectQuery>,
    body: Body,
) -> Result<Response, AppError> {
    ensure_bucket_access(&claims, bucket.as_str(), false, true, false)?;
    if query.uploads.is_some() {
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);
        return create_multipart_upload(bucket.as_str(), key.as_str(), content_type).await;
    }
    let Some(upload_id) = query.upload_id.as_deref() else {
        return Err(AppError::BadRequest(
            "S3 POST object requires uploads or uploadId".to_string(),
        ));
    };
    let completed = prepare_complete_upload(bucket.as_str(), key.as_str(), upload_id, body).await?;
    let put_result = put_n2_object(
        claims,
        HeaderMap::new(),
        Path((bucket.clone(), key.clone())),
        Query(PutObjectQuery {
            uid: None,
            version_id: None,
            expected_epoch: None,
            kc: None,
            mc: None,
            kd: None,
            md: None,
            stripe_block_size: None,
            content_type: completed.content_type,
        }),
        completed.body,
    )
    .await;
    let put = put_result?.0;
    finish_complete_upload(
        bucket.as_str(),
        key.as_str(),
        completed.upload_id.as_str(),
        put.version_id.as_str(),
    )
    .await
}

pub async fn s3_get_object(
    claims: Claims,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<S3ObjectQuery>,
) -> Result<Response, AppError> {
    ensure_bucket_access(&claims, bucket.as_str(), true, false, false)?;
    if let Some(upload_id) = query.upload_id.as_deref() {
        return list_uploaded_parts(bucket.as_str(), key.as_str(), upload_id).await;
    }
    Ok(get_n2_object(
        claims,
        headers,
        Path((bucket, key)),
        Query(GetObjectQuery {
            version_id: query.version_id,
        }),
    )
    .await?
    .into_response())
}

pub async fn s3_head_object(
    claims: Claims,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<S3ObjectQuery>,
) -> Result<Response, AppError> {
    ensure_bucket_access(&claims, bucket.as_str(), true, false, false)?;
    let map =
        load_n2_object_map(bucket.as_str(), key.as_str(), query.version_id.as_deref()).await?;
    let mut response = Response::new(Body::empty());
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_str(map.content_type.as_str())
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    if let Ok(v) = HeaderValue::from_str(map.total_size_bytes.to_string().as_str()) {
        response.headers_mut().insert(CONTENT_LENGTH, v);
    }
    if let Ok(v) = HeaderValue::from_str(format!("\"{}\"", map.object.version_id).as_str()) {
        response.headers_mut().insert(ETAG, v);
    }
    if let Ok(v) = HeaderValue::from_str(map.object.version_id.as_str()) {
        response.headers_mut().insert(X_AMZ_VERSION_ID, v);
    }
    if let Ok(v) = HeaderValue::from_str(unix_to_rfc3339(map.committed_at).as_str()) {
        response.headers_mut().insert(LAST_MODIFIED, v);
    }
    Ok(response)
}

pub async fn s3_delete_object(
    claims: Claims,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<S3ObjectQuery>,
) -> Result<StatusCode, AppError> {
    ensure_bucket_access(&claims, bucket.as_str(), false, false, true)?;
    if let Some(upload_id) = query.upload_id.as_deref() {
        return abort_multipart_upload(bucket.as_str(), key.as_str(), upload_id).await;
    }
    let _ = delete_n2_object(
        claims,
        Path((bucket, key)),
        Query(DeleteN2ObjectQuery {
            version_id: query.version_id,
            expected_epoch: None,
            purge_shards: Some(true),
        }),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
