use crate::core::checksum::{format_sha256, parse_sha256_hex};
use crate::enums::app_error_enum::AppError;
use axum::http::HeaderMap;

fn normalize_sha256(value: &str) -> Result<String, AppError> {
    let value = value.trim();
    let value = value
        .split_once(':')
        .map(|(algorithm, digest)| {
            if algorithm.eq_ignore_ascii_case("sha256") {
                Ok(digest.trim())
            } else {
                Err(AppError::BadRequest(
                    "N2 3.4 supports SHA-256 as its only upload checksum algorithm".into(),
                ))
            }
        })
        .transpose()?
        .unwrap_or(value);
    parse_sha256_hex(value)
        .map(|digest| format_sha256(&digest))
        .ok_or_else(|| {
            AppError::BadRequest(
                "N2 upload checksum must be SHA-256 as 64 hex characters or sha256:<hex>".into(),
            )
        })
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub fn upload_sha256_checksum(
    headers: &HeaderMap,
    query_checksum: Option<&str>,
) -> Result<Option<String>, AppError> {
    if let Some(value) = query_checksum
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return normalize_sha256(value).map(Some);
    }
    if let Some(value) = header_value(headers, "x-litiaina-checksum")
        .or_else(|| header_value(headers, "x-noa-checksum-sha256"))
        .or_else(|| header_value(headers, "x-n2-checksum-sha256"))
    {
        return normalize_sha256(&value).map(Some);
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn accepts_sha256_query_prefix() {
        let headers = HeaderMap::new();
        let parsed = upload_sha256_checksum(
            &headers,
            Some("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        )
        .expect("parse")
        .expect("checksum");
        assert_eq!(
            parsed,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn normalizes_uppercase_sha256_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-noa-checksum-sha256",
            "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD"
                .parse()
                .unwrap(),
        );
        let parsed = upload_sha256_checksum(&headers, None)
            .expect("parse")
            .expect("checksum");
        assert_eq!(
            parsed,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn rejects_crc32_and_unknown_checksum_algorithms() {
        let headers = HeaderMap::new();
        assert!(upload_sha256_checksum(&headers, Some("352441c2")).is_err());
        assert!(upload_sha256_checksum(&headers, Some("crc32:352441c2")).is_err());
    }
}
