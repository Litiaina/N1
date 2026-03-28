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

    conf.with_section(Some("index_config"))
        .set("partition_count", "64");

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
    fn default_config_includes_smb_directory_tuning_keys() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "n1-default-config-{}-{}.ini",
            std::process::id(),
            unique
        ));

        create_default_config_ini(&path).expect("create default config");
        let ini = Ini::load_from_file(&path).expect("load generated config");
        let smb = ini.section(Some("smb")).expect("smb section");

        assert_eq!(smb.get("query_directory_page_limit"), Some("1000"));
        assert_eq!(
            smb.get("query_directory_max_buffered_entries"),
            Some("4096")
        );
        assert_eq!(smb.get("query_directory_max_seeded_entries"), Some("512"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn default_config_excludes_n2_section() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "n1-default-config-no-n2-{}-{}.ini",
            std::process::id(),
            unique
        ));

        create_default_config_ini(&path).expect("create default config");
        let ini = Ini::load_from_file(&path).expect("load generated config");
        assert!(
            ini.section(Some("n2")).is_none(),
            "N1 config should not generate an [n2] section"
        );

        let _ = fs::remove_file(path);
    }
}
