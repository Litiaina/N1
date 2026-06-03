use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, Response, StatusCode, header};
use futures_util::TryStreamExt;

use crate::config::NodeConfig;
use crate::state::GatewayState;

pub async fn proxy_http(
    state: &GatewayState,
    node: NodeConfig,
    request: Request,
) -> Result<Response<Body>, Response<Body>> {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let body = request.into_body();
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or(uri.path());
    let upstream_url = format!("{}{}", node.http_url, path_and_query);

    let stream = body
        .into_data_stream()
        .map_err(|err| std::io::Error::other(err.to_string()));
    let mut builder = state
        .http
        .request(method, upstream_url)
        .body(reqwest::Body::wrap_stream(stream));
    builder = copy_request_headers(builder, &headers);

    let upstream = builder.send().await.map_err(|err| {
        text_response(
            StatusCode::BAD_GATEWAY,
            format!("gateway upstream failed: {err}"),
        )
    })?;

    let status = upstream.status();
    let mut response = Response::builder().status(status);
    for (name, value) in upstream.headers() {
        if should_copy_response_header(name) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from_stream(upstream.bytes_stream()))
        .map_err(|err| text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

pub fn text_response(status: StatusCode, message: impl Into<String>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(message.into()))
        .expect("valid text response")
}

fn copy_request_headers(
    mut builder: reqwest::RequestBuilder,
    headers: &HeaderMap,
) -> reqwest::RequestBuilder {
    for (name, value) in headers {
        if should_copy_request_header(name) {
            builder = builder.header(name, value);
        }
    }
    builder
}

fn should_copy_request_header(name: &HeaderName) -> bool {
    !matches!(
        *name,
        header::HOST
            | header::CONNECTION
            | header::UPGRADE
            | header::PROXY_AUTHORIZATION
            | header::PROXY_AUTHENTICATE
    )
}

fn should_copy_response_header(name: &HeaderName) -> bool {
    !matches!(*name, header::CONNECTION | header::UPGRADE)
}
