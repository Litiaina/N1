use crate::enums::app_error_enum::AppError;
use axum::http::HeaderMap;
use base64::Engine;

fn normalize_hex_crc32(value: &str) -> Result<String, AppError> {
    let value = value.trim();
    let value = value
        .strip_prefix("crc32:")
        .or_else(|| value.strip_prefix("CRC32:"))
        .unwrap_or(value)
        .trim();
    if value.len() == 8 && value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(value.to_ascii_lowercase());
    }
    Err(AppError::BadRequest(
        "N2 upload checksum must be CRC32 as 8 hex characters or crc32:<hex>".into(),
    ))
}

fn decode_base64_crc32(value: &str) -> Result<String, AppError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value.trim())
        .map_err(|err| AppError::BadRequest(format!("invalid base64 CRC32 checksum: {err}")))?;
    if bytes.len() != 4 {
        return Err(AppError::BadRequest(
            "base64 CRC32 checksum must decode to exactly 4 bytes".into(),
        ));
    }
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3]
    ))
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub fn upload_crc32_checksum(
    headers: &HeaderMap,
    query_checksum: Option<&str>,
) -> Result<Option<String>, AppError> {
    if let Some(value) = query_checksum
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return normalize_hex_crc32(value).map(Some);
    }
    if let Some(value) = header_value(headers, "x-litiaina-checksum")
        .or_else(|| header_value(headers, "x-n2-checksum-crc32"))
    {
        return normalize_hex_crc32(&value).map(Some);
    }
    if let Some(value) = header_value(headers, "x-amz-checksum-crc32") {
        return decode_base64_crc32(&value).map(Some);
    }
    if header_value(headers, "x-amz-checksum-sha256").is_some()
        || header_value(headers, "x-n2-checksum-sha256").is_some()
    {
        return Err(AppError::BadRequest(
            "N2 cluster upload currently validates CRC32 checksums; provide x-n2-checksum-crc32 or checksum=crc32:<hex>"
                .into(),
        ));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn accepts_crc32_query_prefix() {
        let headers = HeaderMap::new();
        let parsed = upload_crc32_checksum(&headers, Some("crc32:DEADBEEF"))
            .expect("parse")
            .expect("checksum");
        assert_eq!(parsed, "deadbeef");
    }
}
