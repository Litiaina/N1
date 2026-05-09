use axum::http::HeaderName;

pub(super) const X_AMZ_VERSION_ID: HeaderName = HeaderName::from_static("x-amz-version-id");
