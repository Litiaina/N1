use actix_web::{HttpResponse, ResponseError};

#[derive(Debug)]
pub struct MyError(procyon_engine::Error);

impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl ResponseError for MyError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().body("Internal Server Error")
    }
}

impl From<procyon_engine::Error> for MyError {
    fn from(value: procyon_engine::Error) -> Self {
        Self(value)
    }
}

pub type RouteResult<T> = Result<T, MyError>;
