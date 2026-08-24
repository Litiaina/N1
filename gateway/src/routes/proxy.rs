use super::*;

pub(super) async fn one_shot_upload(
    State(state): State<Arc<GatewayState>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Body,
) -> Response<Body> {
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    proxy_selected_http(&state, node, Method::POST, uri, headers, body).await
}

#[derive(Debug, Deserialize)]
pub(super) struct DownloadQuery {
    #[serde(rename = "object_key")]
    pub(super) _object_key: String,
    #[serde(default)]
    version_id: Option<String>,
}

pub(super) async fn download_object(
    State(state): State<Arc<GatewayState>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(query): Query<DownloadQuery>,
) -> Response<Body> {
    let node = match node_for_download(&state, query.version_id.as_deref()).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    proxy_selected_http(&state, node, Method::GET, uri, headers, Body::empty()).await
}

pub(super) async fn proxy_selected_http(
    state: &GatewayState,
    node: NodeConfig,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response<Body> {
    let mut request = axum::extract::Request::new(body);
    *request.method_mut() = method;
    *request.uri_mut() = uri;
    *request.headers_mut() = headers;
    match proxy_http(state, node.clone(), request).await {
        Ok(response) => response,
        Err(response) => {
            // Client cancellation is not node failure. Probe independently so
            // the coordinator does not get fenced by an abandoned body.
            state.nodes.probe_node(&node.id).await;
            response
        }
    }
}

pub(super) async fn proxy_fallback(
    State(state): State<Arc<GatewayState>>,
    request: axum::extract::Request,
) -> Response<Body> {
    let path = request.uri().path().to_string();
    let retry_template = replay_safe_proxy_template(&request);
    let query = proxy_query_parameters(request.uri().query());
    let mut node = match if path.starts_with("/noa/v1/namespace/operations/")
        && coordinator_node_id(request.headers(), &query).is_some()
    {
        coordinator_node(&state, request.headers(), &query).await
    } else {
        node_for_proxy_path(&state, &path).await
    } {
        Ok(node) => node,
        Err(response) => return response,
    };
    let mut request = request;
    loop {
        match proxy_http(&state, node.clone(), request).await {
            Ok(response) => return response,
            Err(response) => {
                state.nodes.mark_failure(&node.id).await;
                let Some(template) = retry_template.as_ref() else {
                    return response;
                };
                node = match best_node(&state).await {
                    Ok(node) => node,
                    Err(_) => return response,
                };
                request = replay_safe_proxy_request(template);
            }
        }
    }
}

pub(super) fn proxy_query_parameters(query: Option<&str>) -> HashMap<String, String> {
    let mut values = HashMap::new();
    for pair in query.unwrap_or_default().split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        let key = percent_decode_str(key).decode_utf8_lossy().to_string();
        let value = percent_decode_str(value).decode_utf8_lossy().to_string();
        values.insert(key, value);
    }
    values
}

#[derive(Clone)]
pub(super) struct ReplaySafeProxyTemplate {
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
}

pub(super) fn replay_safe_proxy_template(
    request: &axum::extract::Request,
) -> Option<ReplaySafeProxyTemplate> {
    matches!(*request.method(), Method::GET | Method::HEAD).then(|| ReplaySafeProxyTemplate {
        method: request.method().clone(),
        uri: request.uri().clone(),
        headers: request.headers().clone(),
    })
}

pub(super) fn replay_safe_proxy_request(
    template: &ReplaySafeProxyTemplate,
) -> axum::extract::Request {
    let mut request = axum::extract::Request::new(Body::empty());
    *request.method_mut() = template.method.clone();
    *request.uri_mut() = template.uri.clone();
    *request.headers_mut() = template.headers.clone();
    request
}

async fn preferred_node(
    state: &GatewayState,
    preferred_id: &str,
) -> Result<NodeConfig, Response<Body>> {
    if state.nodes.is_available(preferred_id).await {
        if let Some(node) = state.nodes.get(preferred_id).await {
            state.nodes.mark_route(preferred_id).await;
            return Ok(node);
        }
    }
    best_node(state).await
}

async fn node_for_download(
    state: &GatewayState,
    _version_id: Option<&str>,
) -> Result<NodeConfig, Response<Body>> {
    preferred_node(state, read_preferred_node_id().as_str()).await
}

async fn node_for_proxy_path(
    state: &GatewayState,
    path: &str,
) -> Result<NodeConfig, Response<Body>> {
    if path == "/noa/v1/objects/signed-url"
        || path == "/noa/v1/objects/signed-read"
        || path.starts_with("/noa/v1/objects/status/")
        || path.starts_with("/noa/v1/objects/repair/")
        || path.starts_with("/noa/v1/objects/get/")
        || path.starts_with("/noa/v1/objects/stream/")
        || path == "/noa/v1/trash"
        || path == "/noa/v1/objects"
        || path.starts_with("/noa/v1/posix/")
        || path.starts_with("/noa/v1/objects/versions/")
        || path.starts_with("/noa/v1/objects/meta/")
    {
        return preferred_node(state, read_preferred_node_id().as_str()).await;
    }

    best_node(state).await
}

fn read_preferred_node_id() -> String {
    std::env::var("N2_GATEWAY_READ_NODE_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "node-a".to_string())
}
