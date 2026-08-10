use crate::error::{NativeClientError, NativeClientResult};

pub fn parse_native_address(url: &str) -> NativeClientResult<String> {
    let Some(rest) = url.strip_prefix("n1://") else {
        return Err(NativeClientError::protocol(
            "native URL must start with n1://",
        ));
    };
    let address = rest.trim_end_matches('/');
    if address.is_empty() {
        return Err(NativeClientError::protocol(
            "native URL did not include host:port",
        ));
    }
    Ok(address.to_string())
}
