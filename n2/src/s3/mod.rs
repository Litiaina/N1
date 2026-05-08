use crate::distribution_core::data_plane::{
    delete_n2_object, get_n2_object, list_n2_objects, load_n2_object_map, put_n2_object,
};
use crate::distribution_core::model::{
    DeleteN2ObjectQuery, GetObjectQuery, ListN2ObjectsQuery, PutObjectQuery,
};
use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::ADMIN_KEY;
use crate::structs::auth_structs::Claims;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{
    HeaderMap, HeaderName, HeaderValue, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED},
};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::Deserialize;

const X_AMZ_VERSION_ID: HeaderName = HeaderName::from_static("x-amz-version-id");

#[derive(Clone, Debug, Deserialize)]
pub struct S3BucketQuery {
    #[serde(rename = "list-type")]
    pub list_type: Option<u8>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    #[serde(rename = "max-keys")]
    pub max_keys: Option<usize>,
    #[serde(rename = "continuation-token")]
    pub continuation_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct S3ObjectQuery {
    #[serde(rename = "versionId")]
    pub version_id: Option<String>,
}

fn is_admin_claim(claims: &Claims) -> bool {
    claims.fragment == *ADMIN_KEY
}

fn ensure_bucket_access(
    claims: &Claims,
    bucket: &str,
    read: bool,
    write: bool,
    delete: bool,
) -> Result<(), AppError> {
    if read && !claims.read {
        return Err(AppError::Forbidden(
            "S3 read permission required".to_string(),
        ));
    }
    if write && !claims.write {
        return Err(AppError::Forbidden(
            "S3 write permission required".to_string(),
        ));
    }
    if delete && !claims.delete {
        return Err(AppError::Forbidden(
            "S3 delete permission required".to_string(),
        ));
    }
    if !is_admin_claim(claims) && claims.fragment != bucket {
        return Err(AppError::Forbidden("S3 bucket access denied".to_string()));
    }
    Ok(())
}

fn xml_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

fn unix_to_rfc3339(secs: i64) -> String {
    DateTime::<Utc>::from_timestamp(secs, 0)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn xml_response(body: String) -> Response {
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/xml; charset=utf-8"),
    );
    response
}

pub async fn s3_list_buckets(claims: Claims) -> Result<Response, AppError> {
    let bucket = xml_escape(claims.fragment.as_str());
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>{bucket}</ID><DisplayName>{bucket}</DisplayName></Owner><Buckets><Bucket><Name>{bucket}</Name><CreationDate>{}</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>"#,
        unix_to_rfc3339(Utc::now().timestamp())
    );
    Ok(xml_response(body))
}

pub async fn s3_head_bucket(
    claims: Claims,
    Path(bucket): Path<String>,
) -> Result<StatusCode, AppError> {
    ensure_bucket_access(&claims, bucket.as_str(), true, false, false)?;
    Ok(StatusCode::OK)
}

pub async fn s3_list_objects_v2(
    claims: Claims,
    Path(bucket): Path<String>,
    Query(query): Query<S3BucketQuery>,
) -> Result<Response, AppError> {
    ensure_bucket_access(&claims, bucket.as_str(), true, false, false)?;
    let limit = query.max_keys.unwrap_or(1000).clamp(1, 1000);
    let list_type = query.list_type.unwrap_or(2);
    if list_type != 2 {
        return Err(AppError::BadRequest(
            "N2 S3 compatibility currently serves ListObjectsV2; use list-type=2".to_string(),
        ));
    }
    let prefix = query.prefix.unwrap_or_default();
    let delimiter = query.delimiter.unwrap_or_default();
    let listed = list_n2_objects(
        claims,
        Path(bucket.clone()),
        Query(ListN2ObjectsQuery {
            limit: Some(limit.saturating_mul(8).clamp(limit, 10_000)),
            continuation: query.continuation_token.clone(),
        }),
    )
    .await?
    .0;

    let mut contents = String::new();
    let mut common_prefixes = Vec::<String>::new();
    let mut emitted = 0usize;
    for object in listed.objects {
        if !prefix.is_empty() && !object.object_key.starts_with(prefix.as_str()) {
            continue;
        }
        if !delimiter.is_empty() {
            let rest = object
                .object_key
                .strip_prefix(prefix.as_str())
                .unwrap_or(object.object_key.as_str());
            if let Some(pos) = rest.find(delimiter.as_str()) {
                let common = format!("{}{}", prefix, &rest[..pos + delimiter.len()]);
                if !common_prefixes.iter().any(|v| v == &common) {
                    common_prefixes.push(common);
                }
                continue;
            }
        }
        if emitted >= limit {
            break;
        }
        emitted = emitted.saturating_add(1);
        let key = xml_escape(object.object_key.as_str());
        let last_modified = unix_to_rfc3339(object.committed_at);
        contents.push_str(
            format!(
                "<Contents><Key>{key}</Key><LastModified>{last_modified}</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                xml_escape(object.latest_version_id.as_str()),
                object.total_size_bytes
            )
            .as_str(),
        );
    }

    let common_xml = common_prefixes
        .into_iter()
        .map(|p| {
            format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(p.as_str())
            )
        })
        .collect::<String>();
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{}</MaxKeys><IsTruncated>{}</IsTruncated>{}{}</ListBucketResult>"#,
        xml_escape(bucket.as_str()),
        xml_escape(prefix.as_str()),
        emitted,
        limit,
        if listed.has_more { "true" } else { "false" },
        contents,
        common_xml
    );
    Ok(xml_response(body))
}

pub async fn s3_put_object(
    claims: Claims,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
    body: Body,
) -> Result<Response, AppError> {
    ensure_bucket_access(&claims, bucket.as_str(), false, true, false)?;
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

pub async fn s3_get_object(
    claims: Claims,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<S3ObjectQuery>,
) -> Result<Response, AppError> {
    ensure_bucket_access(&claims, bucket.as_str(), true, false, false)?;
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
