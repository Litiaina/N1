#[cfg(not(target_os = "linux"))]
compile_error!("N1 engine is Linux-only. Build and run N1 on Linux.");

pub mod core;
pub mod enums;
pub mod fs;
pub mod handlers;
pub mod io_control;
pub mod middleware;
pub mod namespace;
pub mod posix;
pub mod protocol;
pub mod routes;
pub mod settings;
pub mod statics;
pub mod structs;
#[cfg(test)]
pub mod test_support;
pub mod url;
pub mod utils;
pub mod validation;
pub mod websocket;
