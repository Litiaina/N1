use super::{ApiError, types::NativeRequest};
use axum::{Router, body::Body, http::Request};
use serde_json::Value;
use tower::ServiceExt;
fn escaped(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}
pub(super) async fn dispatch(
    router: Router,
    request: &NativeRequest,
    payload: Value,
    token: Option<&str>,
    body: Body,
    body_len: Option<u64>,
) -> Result<axum::response::Response, ApiError> {
    let field = |name: &str| -> Result<String, ApiError> {
        payload[name]
            .as_str()
            .map(escaped)
            .ok_or_else(|| ApiError::invalid(format!("{name} is required")))
    };
    let multipart = if payload["durable"].as_bool().unwrap_or(false) {
        "/noa/v1/upload/multipart/durable"
    } else {
        "/noa/v1/upload/multipart"
    };
    let (method, mut uri) = match request.op.as_str() {
        "namespace.list" | "posix.list" => ("GET", "/noa/v1/posix/list".into()),
        "namespace.stat" | "posix.stat" => ("GET", "/noa/v1/posix/stat".into()),
        "namespace.search" => ("GET", "/noa/v1/namespace/search".into()),
        "namespace.snapshot" => ("GET", "/noa/v1/namespace/snapshot".into()),
        "namespace.mkdir" | "posix.mkdir" => ("POST", "/noa/v1/posix/mkdir".into()),
        "namespace.rename" | "posix.rename" | "posix.move" => {
            ("POST", "/noa/v1/posix/rename".into())
        }
        "namespace.trash" | "posix.trash" => ("POST", "/noa/v1/posix/trash".into()),
        "namespace.recover" | "posix.recover" => ("POST", "/noa/v1/posix/recover".into()),
        "object.list" => ("GET", "/noa/v1/objects".into()),
        "trash.list" => ("GET", "/noa/v1/trash".into()),
        "trash.recover_all" => ("POST", "/noa/v1/trash/recover-all".into()),
        "object.metadata" | "object.meta" | "object.stat" => (
            "GET",
            format!("/noa/v1/objects/meta/{}", field("object_key")?),
        ),
        "object.versions" => (
            "GET",
            format!("/noa/v1/objects/versions/{}", field("object_key")?),
        ),
        "object.get" | "object.stream" | "object.download" => (
            "GET",
            format!("/noa/v1/objects/get/{}", field("object_key")?),
        ),
        "object.download_chunk" => ("GET", "/noa/v1/objects/download/chunk".into()),
        "object.delete" => (
            "DELETE",
            format!("/noa/v1/objects/{}", field("object_key")?),
        ),
        "object.recover" => (
            "POST",
            format!("/noa/v1/objects/recover/{}", field("object_key")?),
        ),
        "object.rename" => ("POST", "/noa/v1/objects/rename".into()),
        "object.archive" => ("POST", "/noa/v1/objects/archive".into()),
        "object.archive_manifest" => ("POST", "/noa/v1/objects/archive/manifest".into()),
        "export.create" => ("POST", "/noa/v1/exports".into()),
        "export.status" => ("GET", format!("/noa/v1/exports/{}", field("export_id")?)),
        "export.download" => (
            "GET",
            format!("/noa/v1/exports/{}/download", field("export_id")?),
        ),
        "export.cancel" => ("DELETE", format!("/noa/v1/exports/{}", field("export_id")?)),
        "export.signed_url" => (
            "POST",
            format!("/noa/v1/exports/{}/signed-url", field("export_id")?),
        ),
        "object.bulk_delete" | "object.bulk.delete" => {
            ("POST", "/noa/v1/objects/bulk-delete".into())
        }
        "object.bulk_recover" | "object.bulk.recover" => {
            ("POST", "/noa/v1/objects/bulk-recover".into())
        }
        "upload.one_shot" | "object.upload.one_shot" => (
            "POST",
            format!("/noa/v1/upload/one-shot/{}", field("object_key")?),
        ),
        "multipart.init" | "upload.multipart.init" => {
            ("POST", format!("{multipart}/init/{}", field("object_key")?))
        }
        "multipart.put_part" | "upload.multipart.put_part" => (
            "PUT",
            format!(
                "{multipart}/{}/{}",
                field("version_id")?,
                payload["part_index"]
                    .as_u64()
                    .ok_or_else(|| ApiError::invalid("part_index is required"))?
            ),
        ),
        "multipart.finalize" | "upload.multipart.finalize" => (
            "POST",
            format!(
                "{multipart}/{}/{}/finalize",
                field("version_id")?,
                field("session_id")?
            ),
        ),
        "multipart.status" | "upload.multipart.status" => {
            ("GET", format!("{multipart}/{}", field("version_id")?))
        }
        "multipart.cancel" | "upload.multipart.cancel" => {
            ("DELETE", format!("{multipart}/{}", field("version_id")?))
        }
        "multipart.list" | "upload.multipart.list" => ("GET", multipart.into()),
        "fragment.create" => ("POST", "/noa/v1/fragments".into()),
        "fragment.list" => ("GET", "/noa/v1/fragments".into()),
        "fragment.check" => ("GET", "/noa/v1/fragments/check".into()),
        "fragment.hash.public" => ("GET", "/noa/v1/fragments/hash".into()),
        "fragment.update" => ("PATCH", format!("/noa/v1/fragments/{}", field("fragment")?)),
        "fragment.delete" => (
            "DELETE",
            format!("/noa/v1/fragments/{}", field("fragment")?),
        ),
        "auth.refresh" => ("POST", "/noa/v1/auth/refresh".into()),
        "auth.logout" => ("POST", "/noa/v1/auth/logout".into()),
        "maintenance.health" => ("GET", "/noa/v1/maintenance/health".into()),
        "maintenance.pools.health" => ("GET", "/noa/v1/maintenance/pools/health".into()),
        "metadata.backups.list" => ("GET", "/noa/v1/maintenance/metadata/backups".into()),
        "metadata.backups.create" => ("POST", "/noa/v1/maintenance/metadata/backups".into()),
        "metadata.backups.download" => (
            "GET",
            format!(
                "/noa/v1/maintenance/metadata/backups/{}/download",
                field("snapshot")?
            ),
        ),
        "capacity.usage" => ("GET", "/noa/v1/capacity/usage".into()),
        "capacity.fragment" => ("GET", "/noa/v1/capacity/fragment".into()),
        "object.status" => (
            "GET",
            format!("/noa/v1/objects/status/{}", field("object_key")?),
        ),
        "object.repair" => (
            "POST",
            format!("/noa/v1/objects/repair/{}", field("object_key")?),
        ),
        op if op.starts_with("maintenance.") => {
            let parts: Vec<_> = op.split('.').collect();
            if parts.len() != 3 {
                return Err(ApiError::invalid("unsupported native operation"));
            }
            (
                if parts[2] == "status" { "GET" } else { "POST" },
                format!("/noa/v1/maintenance/{}/{}", parts[1], parts[2]),
            )
        }
        _ => return Err(ApiError::invalid("unsupported native operation")),
    };
    if (method == "GET" || body_len.is_some() || request.op.starts_with("maintenance."))
        && let Some(fields) = payload.as_object()
    {
        let query: Vec<_> = fields
            .iter()
            .filter(|(_, v)| !v.is_object() && !v.is_array() && !v.is_null())
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    escaped(k),
                    escaped(
                        &v.as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| v.to_string())
                    )
                )
            })
            .collect();
        if !query.is_empty() {
            uri.push('?');
            uri.push_str(&query.join("&"));
        }
    }
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let body = if let Some(length) = body_len {
        builder = builder.header("content-length", length).header(
            "content-type",
            payload["content_type"]
                .as_str()
                .unwrap_or("application/octet-stream"),
        );
        body
    } else if request.body.is_some() {
        let length = request.body.as_ref().map_or(0, |b| b.len());
        builder = builder.header("content-length", length).header(
            "content-type",
            payload["content_type"]
                .as_str()
                .unwrap_or("application/octet-stream"),
        );
        body
    } else {
        builder = builder.header("content-type", "application/json");
        Body::from(payload.to_string())
    };
    Ok(router
        .oneshot(
            builder
                .body(body)
                .map_err(|_| ApiError::invalid("invalid native request fields"))?,
        )
        .await
        .expect("infallible router"))
}
