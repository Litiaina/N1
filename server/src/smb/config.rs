use std::env;

#[derive(Clone, Debug)]
pub struct SmbConfig {
    pub enabled: bool,
    pub bind_addr: String,
    pub smb_port: u16,
    pub max_sessions: usize,
    pub max_frame_bytes: usize,
    pub async_close_commit: bool,
    pub async_close_threshold_bytes: u64,
    pub auth_username: Option<String>,
    pub auth_password: Option<String>,
    pub auth_domain: Option<String>,
    pub require_signing: bool,
}

impl Default for SmbConfig {
    fn default() -> Self {
        let enabled = env::var("N1_SMB_ENABLED")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        let bind_addr = env::var("N1_SMB_BIND_ADDR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "0.0.0.0".to_string());

        let smb_port = env::var("N1_SMB_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(445);

        let max_sessions = env::var("N1_SMB_MAX_SESSIONS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4096)
            .clamp(64, 65_536);
        let max_frame_bytes = env::var("N1_SMB_MAX_FRAME_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(16 * 1024 * 1024)
            .clamp(4 * 1024, 64 * 1024 * 1024);
        let async_close_commit = env::var("N1_SMB_ASYNC_CLOSE_COMMIT")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);
        let async_close_threshold_bytes = env::var("N1_SMB_ASYNC_CLOSE_THRESHOLD_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(64 * 1024 * 1024)
            .clamp(1 * 1024 * 1024, 1024 * 1024 * 1024 * 1024);

        let auth_username = env::var("N1_SMB_AUTH_USERNAME")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let auth_password = env::var("N1_SMB_AUTH_PASSWORD")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let auth_domain = env::var("N1_SMB_AUTH_DOMAIN")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let require_signing = env::var("N1_SMB_REQUIRE_SIGNING")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        Self {
            enabled,
            bind_addr,
            smb_port,
            max_sessions,
            max_frame_bytes,
            async_close_commit,
            async_close_threshold_bytes,
            auth_username,
            auth_password,
            auth_domain,
            require_signing,
        }
    }
}

impl SmbConfig {
    pub fn socket_addr(&self) -> Result<std::net::SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.bind_addr, self.smb_port).parse()
    }
}
