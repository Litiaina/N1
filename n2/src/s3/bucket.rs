use crate::distribution_core::data_plane::list_n2_objects;
use crate::distribution_core::model::ListN2ObjectsQuery;
use crate::enums::app_error_enum::AppError;
use crate::s3::auth::ensure_bucket_access;
use crate::s3::query::S3BucketQuery;
use crate::s3::xml::{unix_to_rfc3339, xml_escape, xml_response};
use crate::structs::auth_structs::Claims;
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::Response;
use chrono::Utc;

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
            limit: Some(limit),
            continuation: query.continuation_token.clone(),
            prefix: if prefix.is_empty() {
                None
            } else {
                Some(prefix.clone())
            },
            delimiter: if delimiter.is_empty() {
                None
            } else {
                Some(delimiter.clone())
            },
        }),
    )
    .await?
    .0;

    let mut contents = String::new();
    let mut emitted = 0usize;
    for object in listed.objects {
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

    let common_xml = listed
        .common_prefixes
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
