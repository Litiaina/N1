use crate::error::{NativeClientError, NativeClientResult};
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeAddress {
    pub authority: String,
    pub host: String,
    pub tls: bool,
}
pub fn parse_native_address(url: &str) -> NativeClientResult<NativeAddress> {
    let (tls, rest) = if let Some(s) = url.strip_prefix("n1://") {
        (false, s)
    } else if let Some(s) = url.strip_prefix("n1s://") {
        (true, s)
    } else {
        return Err(NativeClientError::protocol(
            "native URL requires n1:// or n1s://",
        ));
    };
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    if authority.is_empty()
        || authority
            .chars()
            .any(|c| c.is_whitespace() || "/?#@\\".contains(c))
    {
        return Err(NativeClientError::protocol(
            "native URL must contain only a host and port",
        ));
    }
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| NativeClientError::protocol("native URL requires an explicit port"))?;
    if port.parse::<u16>().ok().is_none_or(|p| p == 0) {
        return Err(NativeClientError::protocol("invalid native port"));
    }
    let host = if host.starts_with('[') && host.ends_with(']') {
        let raw = &host[1..host.len() - 1];
        raw.parse::<std::net::Ipv6Addr>()
            .map_err(|_| NativeClientError::protocol("invalid bracketed IPv6 host"))?;
        raw
    } else {
        if host.is_empty() || host.contains([':', '[', ']']) {
            return Err(NativeClientError::protocol("invalid native host"));
        }
        host
    };
    Ok(NativeAddress {
        authority: authority.into(),
        host: host.into(),
        tls,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn addresses_are_strict_and_tls_is_explicit() {
        assert_eq!(
            parse_native_address("n1s://[::1]:9443/").unwrap(),
            NativeAddress {
                authority: "[::1]:9443".into(),
                host: "::1".into(),
                tls: true
            }
        );
        assert!(!parse_native_address("n1://127.0.0.1:19001").unwrap().tls);
        for url in [
            "http://localhost:1",
            "n1://",
            "n1://localhost",
            "n1://localhost:0",
            "n1://user@host:1",
            "n1://host:1/path",
            "n1://host:1?x",
            "n1://::1:1",
            "n1://host:1//",
            "n1://host:65536",
            "n1://host:1#x",
        ] {
            assert!(parse_native_address(url).is_err(), "{url}");
        }
    }
}
