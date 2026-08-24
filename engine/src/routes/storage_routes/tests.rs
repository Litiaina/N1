use super::{
    STANDARD_BATCH_MAX_CONCURRENCY, batch_error_status, decode_standard_batch_object_key,
    default_standard_batch_concurrency, ping,
};
use crate::enums::app_error_enum::AppError;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use axum::routing::get;
use tower::ServiceExt;

#[tokio::test]
async fn canonical_noa_ping_is_present_and_retired_n2_ping_is_absent() {
    let router = Router::new().route("/noa/v1/ping", get(ping));

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/noa/v1/ping")
                .body(Body::empty())
                .expect("NOA request"),
        )
        .await
        .expect("NOA response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 16).await.expect("pong body"),
        "pong"
    );

    let legacy = router
        .oneshot(
            Request::builder()
                .uri("/n2/ping")
                .body(Body::empty())
                .expect("legacy request"),
        )
        .await
        .expect("legacy response");
    assert_eq!(legacy.status(), StatusCode::NOT_FOUND);
}

#[test]
fn standard_batch_object_keys_are_explicit_and_percent_decoded() {
    assert_eq!(
        decode_standard_batch_object_key(Some("object"), Some("images%2F2026%2Fscan%2001.png"))
            .expect("decode batch object key"),
        "images/2026/scan 01.png"
    );
    assert!(decode_standard_batch_object_key(Some("files"), Some("a.bin")).is_err());
    assert!(decode_standard_batch_object_key(Some("object"), Some("%2F")).is_err());
    assert!(decode_standard_batch_object_key(Some("object"), None).is_err());
}

#[test]
fn standard_batch_concurrency_is_bounded_independently_of_cluster_size() {
    assert!((1..=STANDARD_BATCH_MAX_CONCURRENCY).contains(&default_standard_batch_concurrency()));
}

#[test]
fn standard_batch_preserves_survival_and_capacity_statuses() {
    assert_eq!(
        batch_error_status(&AppError::ServiceUnavailable(
            "fewer than K participants are writable".to_string()
        )),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        batch_error_status(&AppError::InsufficientStorage),
        StatusCode::INSUFFICIENT_STORAGE
    );
    assert_eq!(
        batch_error_status(&AppError::Conflict("namespace collision".to_string())),
        StatusCode::CONFLICT
    );
}
