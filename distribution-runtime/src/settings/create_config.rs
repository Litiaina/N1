use ini::Ini;
use std::{io, path::Path};

pub fn create_default_config_ini<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let mut conf = Ini::new();

    conf.with_section(Some("server"))
        .set("host", "127.0.0.1")
        .set("https_port", "50001")
        .set("cert_path", "")
        .set("key_path", "")
        .set("sharable_url", "");

    conf.with_section(Some("disk_config"))
        .set("index_store_disks", "")
        .set("blob_store_disks", "")
        .set("fs_storage_path", "");

    conf.with_section(Some("garbage_collector"))
        .set("cleanup_interval_seconds", "3600")
        .set("file_ttl_seconds", "3600")
        .set("retention_seconds", "2592000");

    conf.with_section(Some("set_limits"))
        .set("max_file_size", "5368709120");

    conf.with_section(Some("jwt_token_config"))
        .set("login_token_expiration", "900");

    conf.with_section(Some("rate_limiter_config"))
        .set("enable_rate_limiting", "true")
        .set("auth_limit_per_second", "2")
        .set("auth_limit_burst_size", "5")
        .set("view_limit_per_second", "30")
        .set("view_limit_burst_size", "100")
        .set("upload_limit_per_second", "300")
        .set("upload_limit_burst_size", "3000")
        .set("sharing_limit_per_second", "50")
        .set("sharing_limit_burst_size", "100")
        .set("global_limit_per_second", "200")
        .set("global_limit_burst_size", "300");

    conf.with_section(Some("upload_config"))
        .set("max_concurrent_io", "auto")
        .set("adaptive_concurrency_enabled", "true")
        .set("adaptive_concurrency_interval_ms", "auto")
        .set("writer_buffer_size", "auto")
        .set("upload_memory_budget_bytes", "auto")
        .set("upload_part_strict_durability_default", "false")
        .set("upload_io_gate_enabled", "true")
        .set("stream_max_concurrency", "auto")
        .set("stream_chunk_size", "auto")
        .set("stream_readahead_bytes", "auto")
        .set("stream_locality_bias_enabled", "true")
        .set("monitor_interval_seconds", "auto")
        .set("monitor_deep_scan_interval_seconds", "auto")
        .set("max_part_size", "52428800")
        .set("data_shards", "1")
        .set("parity_shards", "0")
        .set("stripe_block_size", "262144");

    conf.with_section(Some("n2"))
        .set("node_id", "node-1")
        .set("node_endpoint", "https://127.0.0.1:50001")
        .set("cluster_membership_file", "n2_cluster_membership.json")
        .set("internal_token", "change-me-n2-internal-token")
        .set("auto_cross_params", "true")
        .set("cluster_stripe_block_size", "262144")
        .set("remote_request_timeout_ms", "60000")
        .set("kc", "1")
        .set("mc", "0")
        .set("kd", "1")
        .set("md", "0");

    conf.with_section(Some("smb"))
        .set("enabled", "false")
        .set("bind_addr", "0.0.0.0")
        .set("smb_port", "445")
        .set("max_sessions", "4096")
        .set("max_frame_bytes", "16777216")
        .set("async_close_commit", "false")
        .set("async_close_threshold_bytes", "67108864")
        .set("require_signing", "false")
        .set("query_directory_page_limit", "1000")
        .set("query_directory_max_buffered_entries", "4096")
        .set("query_directory_max_seeded_entries", "512");

    conf.write_to_file(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn default_n2_config_includes_n2_section() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "n2-default-config-{}-{}.ini",
            std::process::id(),
            unique
        ));

        create_default_config_ini(&path).expect("create default config");
        let ini = Ini::load_from_file(&path).expect("load generated config");
        let n2 = ini.section(Some("n2")).expect("n2 section");

        assert_eq!(n2.get("node_id"), Some("node-1"));
        assert_eq!(n2.get("node_endpoint"), Some("https://127.0.0.1:50001"));
        assert_eq!(
            n2.get("cluster_membership_file"),
            Some("n2_cluster_membership.json")
        );
        assert_eq!(
            n2.get("internal_token"),
            Some("change-me-n2-internal-token")
        );
        assert_eq!(n2.get("auto_cross_params"), Some("true"));
        assert_eq!(n2.get("cluster_stripe_block_size"), Some("262144"));
        assert_eq!(n2.get("remote_request_timeout_ms"), Some("60000"));
        assert_eq!(n2.get("kc"), Some("1"));
        assert_eq!(n2.get("mc"), Some("0"));
        assert_eq!(n2.get("kd"), Some("1"));
        assert_eq!(n2.get("md"), Some("0"));

        let _ = fs::remove_file(path);
    }
}
