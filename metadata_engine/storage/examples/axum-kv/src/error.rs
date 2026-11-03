use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Axum(#[from] axum::Error),

    #[error(transparent)]
    ProcyonEngine(#[from] procyon_engine::Error),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        self.to_string().into_response()
    }
}
