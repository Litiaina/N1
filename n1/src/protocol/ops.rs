use crate::protocol::dispatch::api_request;
use crate::protocol::types::NativeApiRequest;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::{Value, json};

pub fn typed_api_request(op: &str, payload: &Value) -> Result<Option<NativeApiRequest>, String> {
    let request = match op {
        "namespace.list" | "posix.list" => {
            let path = string_field(payload, "path").unwrap_or_default();
            let limit = usize_field(payload, "limit").unwrap_or(200).clamp(1, 1000);
            let trash = bool_field(payload, "trash").unwrap_or(false);
            let continuation = string_field(payload, "continuation_token");
            let mut query = vec![
                ("path", path),
                ("limit", limit.to_string()),
                ("trash", trash.to_string()),
            ];
            if let Some(continuation) = continuation {
                query.push(("continuation_token", continuation));
            }
            api_request(
                "GET",
                format!("/n1/posix/list?{}", query_string(query)),
                None,
            )
        }
        "namespace.stat" | "posix.stat" => {
            let path = required_string(payload, "path")?;
            let trash = bool_field(payload, "trash").unwrap_or(false);
            api_request(
                "GET",
                format!(
                    "/n1/posix/stat?{}",
                    query_string(vec![("path", path), ("trash", trash.to_string())])
                ),
                None,
            )
        }
        "namespace.mkdir" | "posix.mkdir" => api_request(
            "POST",
            "/n1/posix/mkdir",
            Some(json!({"path": required_string(payload, "path")?})),
        ),
        "namespace.rename" | "posix.rename" | "posix.move" => api_request(
            "POST",
            "/n1/posix/rename",
            Some(json!({
                "source_path": required_string(payload, "source_path")?,
                "destination_path": required_string(payload, "destination_path")?,
            })),
        ),
        "namespace.trash" | "posix.trash" => api_request(
            "POST",
            "/n1/posix/trash",
            Some(path_payload(payload, true)?),
        ),
        "namespace.recover" | "posix.recover" => api_request(
            "POST",
            "/n1/posix/recover",
            Some(path_payload(payload, true)?),
        ),
        "namespace.purge" | "posix.purge" => api_request(
            "POST",
            "/n1/posix/purge",
            Some(path_payload(payload, true)?),
        ),
        "object.list" => {
            let prefix = string_field(payload, "prefix");
            let delimiter = string_field(payload, "delimiter");
            let limit = usize_field(payload, "limit").unwrap_or(100).clamp(1, 1000);
            let continuation = string_field(payload, "continuation_token");
            let mut query = vec![("limit", limit.to_string())];
            if let Some(prefix) = prefix {
                query.push(("prefix", prefix));
            }
            if let Some(delimiter) = delimiter {
                query.push(("delimiter", delimiter));
            }
            if let Some(continuation) = continuation {
                query.push(("continuation_token", continuation));
            }
            api_request("GET", format!("/n1/objects?{}", query_string(query)), None)
        }
        "trash.list" => {
            let prefix = string_field(payload, "prefix");
            let limit = usize_field(payload, "limit").unwrap_or(100).clamp(1, 1000);
            let continuation = string_field(payload, "continuation_token");
            let mut query = vec![("limit", limit.to_string())];
            if let Some(prefix) = prefix {
                query.push(("prefix", prefix));
            }
            if let Some(continuation) = continuation {
                query.push(("continuation_token", continuation));
            }
            api_request("GET", format!("/n1/trash?{}", query_string(query)), None)
        }
        "trash.empty" => api_request("DELETE", "/n1/trash/empty", None),
        "trash.recover_all" => api_request("POST", "/n1/trash/recover-all", None),
        "object.status" => {
            let object_key = required_string(payload, "object_key")?;
            let mode = string_field(payload, "mode").unwrap_or_else(|| "availability".to_string());
            api_request(
                "GET",
                format!(
                    "/n1/objects/status/{}?{}",
                    encode_path(&object_key),
                    query_string(vec![("mode", mode)])
                ),
                None,
            )
        }
        "object.repair" => {
            let object_key = required_string(payload, "object_key")?;
            let mode = string_field(payload, "mode").unwrap_or_else(|| "strict".to_string());
            api_request(
                "POST",
                format!(
                    "/n1/objects/repair/{}?{}",
                    encode_path(&object_key),
                    query_string(vec![("mode", mode)])
                ),
                None,
            )
        }
        "object.delete" => api_request(
            "DELETE",
            format!(
                "/n1/objects/{}",
                encode_path(&required_string(payload, "object_key")?)
            ),
            None,
        ),
        "object.recover" => api_request(
            "POST",
            format!(
                "/n1/objects/recover/{}",
                encode_path(&required_string(payload, "object_key")?)
            ),
            None,
        ),
        "object.purge" => api_request(
            "DELETE",
            format!(
                "/n1/objects/permanent/{}",
                encode_path(&required_string(payload, "object_key")?)
            ),
            None,
        ),
        "multipart.init" => {
            let object_key = required_string(payload, "object_key")?;
            let params = payload
                .get("params")
                .cloned()
                .unwrap_or_else(|| payload_without(payload, &["object_key"]));
            api_request(
                "POST",
                format!("/n1/upload/multipart/init/{}", encode_path(&object_key)),
                Some(params),
            )
        }
        "multipart.status" => api_request(
            "GET",
            format!(
                "/n1/upload/multipart/{}",
                encode_path(&required_string(payload, "version_id")?)
            ),
            None,
        ),
        "multipart.part" => {
            let version_id = required_string(payload, "version_id")?;
            let part_index = required_string(payload, "part_index")?;
            api_request(
                "PUT",
                format!(
                    "/n1/upload/multipart/{}/{}?{}",
                    encode_path(&version_id),
                    encode_path(&part_index),
                    query_string(upload_part_query(payload)?)
                ),
                None,
            )
        }
        "multipart.finalize" => {
            let version_id = required_string(payload, "version_id")?;
            let session_id = required_string(payload, "session_id")?;
            let object_key = required_string(payload, "object_key")?;
            let params = payload.get("params").cloned().unwrap_or_else(|| {
                payload_without(payload, &["version_id", "session_id", "object_key"])
            });
            api_request(
                "POST",
                format!(
                    "/n1/upload/multipart/{}/{}/finalize?{}",
                    encode_path(&version_id),
                    encode_path(&session_id),
                    query_string(vec![("object_key", object_key)])
                ),
                Some(params),
            )
        }
        "maintenance.health" => api_request("GET", "/n1/maintenance/health", None),
        "maintenance.repair.status" => api_request("GET", "/n1/maintenance/repair/status", None),
        "maintenance.repair.start" => api_request(
            "POST",
            maintenance_fragment_path(payload, "/n1/maintenance/fragments", "/repair/start")?,
            None,
        ),
        "maintenance.gc.status" => api_request("GET", "/n1/maintenance/gc/status", None),
        "maintenance.gc.start" => {
            if let Some(fragment) = string_field(payload, "fragment") {
                api_request(
                    "POST",
                    format!(
                        "/n1/maintenance/fragments/{}/gc/start",
                        encode_path(&fragment)
                    ),
                    None,
                )
            } else {
                api_request("POST", "/n1/maintenance/gc/start", None)
            }
        }
        "maintenance.redistribution.status" => {
            api_request("GET", "/n1/maintenance/redistribution/status", None)
        }
        "maintenance.redistribution.start" => api_request(
            "POST",
            maintenance_fragment_path(
                payload,
                "/n1/maintenance/fragments",
                "/redistribution/start",
            )?,
            None,
        ),
        _ => return Ok(None),
    };
    Ok(Some(request))
}

fn upload_part_query(payload: &Value) -> Result<Vec<(&'static str, String)>, String> {
    let mut query = Vec::new();
    for key in [
        "durability",
        "expected_checksum",
        "content_type",
        "checksum",
        "size_bytes",
    ] {
        if let Some(value) = string_field(payload, key) {
            query.push((key, value));
        }
    }
    Ok(query)
}

fn maintenance_fragment_path(
    payload: &Value,
    prefix: &str,
    suffix: &str,
) -> Result<String, String> {
    let fragment = required_string(payload, "fragment")?;
    let mode = string_field(payload, "mode").unwrap_or_else(|| "scrub".to_string());
    let fast = bool_field(payload, "fast_mode").unwrap_or(true);
    Ok(format!(
        "{}/{}/{}?{}",
        prefix.trim_end_matches('/'),
        encode_path(&fragment),
        suffix.trim_start_matches('/'),
        query_string(vec![("mode", mode), ("fast_mode", fast.to_string())])
    ))
}

fn path_payload(payload: &Value, include_trash_id: bool) -> Result<Value, String> {
    let mut out = json!({"path": required_string(payload, "path")?});
    if include_trash_id {
        if let Some(value) = string_field(payload, "trash_entry_id") {
            out["trash_entry_id"] = Value::String(value);
        }
    }
    Ok(out)
}

fn payload_without(payload: &Value, keys: &[&str]) -> Value {
    let Some(map) = payload.as_object() else {
        return Value::Null;
    };
    let mut out = serde_json::Map::new();
    for (key, value) in map {
        if !keys.contains(&key.as_str()) {
            out.insert(key.clone(), value.clone());
        }
    }
    Value::Object(out)
}

fn required_string(payload: &Value, field: &str) -> Result<String, String> {
    string_field(payload, field).ok_or_else(|| format!("missing string field '{field}'"))
}

fn string_field(payload: &Value, field: &str) -> Option<String> {
    match payload.get(field)? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn usize_field(payload: &Value, field: &str) -> Option<usize> {
    payload
        .get(field)
        .and_then(|value| value.as_u64())
        .and_then(|value| usize::try_from(value).ok())
}

fn bool_field(payload: &Value, field: &str) -> Option<bool> {
    payload.get(field).and_then(Value::as_bool)
}

fn query_string(values: Vec<(&str, String)>) -> String {
    values
        .into_iter()
        .map(|(key, value)| format!("{}={}", encode_query(key), encode_query(&value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn encode_query(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn encode_path(value: &str) -> String {
    value
        .split('/')
        .map(|segment| utf8_percent_encode(segment, NON_ALPHANUMERIC).to_string())
        .collect::<Vec<_>>()
        .join("/")
}
