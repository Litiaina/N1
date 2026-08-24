use super::*;

pub(super) async fn best_node(state: &GatewayState) -> Result<NodeConfig, Response<Body>> {
    state.nodes.select_best().await.ok_or_else(|| {
        text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway has no available N2 nodes",
        )
    })
}

pub(super) async fn coordinator_node(
    state: &GatewayState,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
) -> Result<NodeConfig, Response<Body>> {
    let coordinator_node_id = coordinator_node_id(headers, query).ok_or_else(|| {
        text_response(
            StatusCode::BAD_REQUEST,
            "coordinator_node_id is required for coordinator-owned gateway routing",
        )
    })?;
    state.nodes.mark_route(&coordinator_node_id).await;
    let configured = state.nodes.get(&coordinator_node_id).await.ok_or_else(|| {
        text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "coordinator node is not configured",
        )
    })?;
    if !state.nodes.is_available(&coordinator_node_id).await
        && !state.nodes.probe_node(&coordinator_node_id).await
    {
        return Err(text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "coordinator node is unavailable",
        ));
    }
    Ok(configured)
}

pub(super) fn coordinator_node_id(
    headers: &HeaderMap,
    query: &HashMap<String, String>,
) -> Option<String> {
    headers
        .get("x-n2-coordinator-node")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            ["coordinator_node_id", "coordinator", "node_id"]
                .into_iter()
                .find_map(|key| {
                    query
                        .get(key)
                        .map(String::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                })
        })
}

pub(super) async fn upstream_failed(
    state: &GatewayState,
    node: &NodeConfig,
    err: String,
) -> Response<Body> {
    // A failed upload stream is not proof that its coordinator died: browser
    // cancellation closes the request body intentionally. Confirm node liveness
    // through an independent native ping before changing gateway availability.
    state.nodes.probe_node(&node.id).await;
    text_response(
        StatusCode::SERVICE_UNAVAILABLE,
        format!("gateway upstream {} failed: {err}", node.id),
    )
}

pub(super) async fn native_call_failed(
    state: &GatewayState,
    node: &NodeConfig,
    error: NativeCallError,
) -> Response<Body> {
    if let Some(status) = error.status {
        return text_response(
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            error.message,
        );
    }
    upstream_failed(state, node, error.message).await
}

pub(super) fn query_to_json(query: HashMap<String, String>) -> Value {
    let mut map = serde_json::Map::new();
    for (key, value) in query {
        if matches!(
            key.as_str(),
            "coordinator_node_id" | "coordinator" | "node_id"
        ) {
            continue;
        }
        if let Ok(parsed) = value.parse::<u64>() {
            map.insert(key, Value::Number(parsed.into()));
        } else if value == "true" || value == "false" {
            map.insert(key, Value::Bool(value == "true"));
        } else {
            map.insert(key, Value::String(value));
        }
    }
    Value::Object(map)
}

pub(super) fn decode_path(path: &str) -> String {
    percent_decode_str(path).decode_utf8_lossy().to_string()
}

pub(super) fn json_response(status: u16, value: Value) -> Response<Body> {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    json_response_value(status, value)
}

pub(super) fn json_response_value(status: StatusCode, value: Value) -> Response<Body> {
    if status == StatusCode::NO_CONTENT || status == StatusCode::NOT_MODIFIED {
        return Response::builder()
            .status(status)
            .body(Body::empty())
            .expect("valid empty response");
    }
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec()),
        ))
        .expect("valid json response")
}

pub(super) async fn post_json_to_node(
    state: &GatewayState,
    node: &NodeConfig,
    path: &str,
    payload: &Value,
) -> Result<(StatusCode, Value), String> {
    let upstream = state
        .http
        .post(format!("{}{}", node.http_url, path))
        .json(payload)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    let status = upstream.status();
    let bytes = upstream.bytes().await.map_err(|err| err.to_string())?;
    let body = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(&bytes).unwrap_or_else(
            |_| json!({ "error": String::from_utf8_lossy(&bytes).trim().to_string() }),
        )
    };
    Ok((status, body))
}
