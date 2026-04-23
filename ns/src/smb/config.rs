use std::env;

const SMB_QUERY_DIRECTORY_PAGE_LIMIT_MIN: usize = 16;
const SMB_QUERY_DIRECTORY_PAGE_LIMIT_MAX: usize = 1000;
const SMB_QUERY_DIRECTORY_MAX_BUFFERED_ENTRIES_MIN: usize = 32;
const SMB_QUERY_DIRECTORY_MAX_BUFFERED_ENTRIES_MAX: usize = 8192;
const SMB_QUERY_DIRECTORY_MAX_SEEDED_ENTRIES_MIN: usize = 8;
const SMB_QUERY_DIRECTORY_MAX_SEEDED_ENTRIES_MAX: usize = 2048;

pub fn clamp_query_directory_page_limit(value: usize) -> usize {
    value.clamp(
        SMB_QUERY_DIRECTORY_PAGE_LIMIT_MIN,
        SMB_QUERY_DIRECTORY_PAGE_LIMIT_MAX,
    )
}

pub fn clamp_query_directory_max_buffered_entries(value: usize) -> usize {
    value.clamp(
        SMB_QUERY_DIRECTORY_MAX_BUFFERED_ENTRIES_MIN,
        SMB_QUERY_DIRECTORY_MAX_BUFFERED_ENTRIES_MAX,
    )
}

pub fn clamp_query_directory_max_seeded_entries(
    value: usize,
    max_buffered_entries: usize,
) -> usize {
    value
        .clamp(
            SMB_QUERY_DIRECTORY_MAX_SEEDED_ENTRIES_MIN,
            SMB_QUERY_DIRECTORY_MAX_SEEDED_ENTRIES_MAX,
        )
        .min(max_buffered_entries)
}

#[derive(Clone, Debug)]
pub struct SmbConfig {
    pub enabled: bool,
    pub bind_addr: String,
    pub smb_port: u16,
    pub max_sessions: usize,
    pub max_frame_bytes: usize,
    pub memory_budget_bytes: usize,
    pub async_close_commit: bool,
    pub async_close_threshold_bytes: u64,
    pub require_signing: bool,
    pub auth_domain: String,
    pub allow_guest: bool,
    pub query_directory_page_limit: usize,
    pub query_directory_max_buffered_entries: usize,
    pub query_directory_max_seeded_entries: usize,
}

impl Default for SmbConfig {
    fn default() -> Self {
        let enabled = env::var("NS_SMB_ENABLED")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(true);

        let bind_addr = env::var("NS_SMB_BIND_ADDR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "0.0.0.0".to_string());

        let smb_port = env::var("NS_SMB_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(445);

        let max_sessions = env::var("NS_SMB_MAX_SESSIONS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4096)
            .clamp(64, 65_536);
        let max_frame_bytes = env::var("NS_SMB_MAX_FRAME_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(16 * 1024 * 1024)
            .clamp(4 * 1024, 64 * 1024 * 1024);
        let memory_budget_bytes = env::var("NS_SMB_MEMORY_BUDGET_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(128 * 1024 * 1024)
            .clamp(8 * 1024 * 1024, 4 * 1024 * 1024 * 1024usize)
            .max(max_frame_bytes);
        let async_close_commit = env::var("NS_SMB_ASYNC_CLOSE_COMMIT")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);
        let async_close_threshold_bytes = env::var("NS_SMB_ASYNC_CLOSE_THRESHOLD_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(64 * 1024 * 1024)
            .clamp(1 * 1024 * 1024, 1024 * 1024 * 1024 * 1024);

        let require_signing = env::var("NS_SMB_REQUIRE_SIGNING")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        let auth_domain = env::var("NS_SMB_AUTH_DOMAIN")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "LITI".to_string());

        let allow_guest = env::var("NS_SMB_ALLOW_GUEST")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        let query_directory_page_limit = env::var("NS_SMB_QUERY_DIRECTORY_PAGE_LIMIT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(clamp_query_directory_page_limit)
            .unwrap_or(1000);

        let query_directory_max_buffered_entries =
            env::var("NS_SMB_QUERY_DIRECTORY_MAX_BUFFERED_ENTRIES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .map(clamp_query_directory_max_buffered_entries)
                .unwrap_or(4096);

        let query_directory_max_seeded_entries =
            env::var("NS_SMB_QUERY_DIRECTORY_MAX_SEEDED_ENTRIES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .map(|v| {
                    clamp_query_directory_max_seeded_entries(
                        v,
                        query_directory_max_buffered_entries,
                    )
                })
                .unwrap_or(512);

        Self {
            enabled,
            bind_addr,
            smb_port,
            max_sessions,
            max_frame_bytes,
            memory_budget_bytes,
            async_close_commit,
            async_close_threshold_bytes,
            require_signing,
            auth_domain,
            allow_guest,
            query_directory_page_limit,
            query_directory_max_buffered_entries,
            query_directory_max_seeded_entries,
        }
    }
}

impl SmbConfig {
    pub fn socket_addr(&self) -> Result<std::net::SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.bind_addr, self.smb_port).parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_directory_page_limit_is_clamped() {
        assert_eq!(clamp_query_directory_page_limit(1), 16);
        assert_eq!(clamp_query_directory_page_limit(256), 256);
        assert_eq!(clamp_query_directory_page_limit(10_000), 1000);
    }

    #[test]
    fn query_directory_max_buffered_entries_is_clamped() {
        assert_eq!(clamp_query_directory_max_buffered_entries(1), 32);
        assert_eq!(clamp_query_directory_max_buffered_entries(512), 512);
        assert_eq!(clamp_query_directory_max_buffered_entries(99_999), 8192);
    }

    #[test]
    fn query_directory_max_seeded_entries_is_clamped_and_bounded_by_buffered() {
        assert_eq!(clamp_query_directory_max_seeded_entries(1, 512), 8);
        assert_eq!(clamp_query_directory_max_seeded_entries(128, 512), 128);
        assert_eq!(clamp_query_directory_max_seeded_entries(4000, 512), 512);
        assert_eq!(clamp_query_directory_max_seeded_entries(200, 64), 64);
    }
}
