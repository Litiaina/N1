//! Native NOA 2 client. Control frames and raw object streams travel directly
//! over n1:// or certificate-verified n1s://, without an HTTP dependency.
mod client;
mod error;
mod frame;
mod url;
pub use client::N1NativeClient;
pub use error::{NativeClientError, NativeClientResult};
pub use n1_link::noa::{Command, Permissions, Request, Response};
pub use url::{NativeAddress, parse_native_address};
