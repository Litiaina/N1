use axum::extract::multipart::MultipartError;
use tokio::io;

#[derive(Debug)]
pub enum AppError {
    Io(io::Error, &'static str),
    Timeout(&'static str),
    BadRequest(&'static str),
    Conflict(&'static str),
    PayloadTooLarge(&'static str),
    Unauthorized,
    Multipart(MultipartError),
}