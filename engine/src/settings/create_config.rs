use ini::Ini;
use std::{io, path::Path};

pub fn create_default_config_ini<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let mut conf = Ini::new();

    conf.with_section(Some("server"))
        .set("host", "127.0.0.1")
        .set("https_port", "50001")
        .set("native_port", "50002")
        .set("cert_path", "")
        .set("key_path", "");

    conf.with_section(Some("disk_config"))
        .set("fs_storage_path", "");

    conf.with_section(Some("storage_pool.A"))
        .set("generation", "1")
        .set("roots", "")
        .set("data_shards", "3")
        .set("parity_shards", "1")
        .set("sealed", "false");

    conf.with_section(Some("index_config"))
        .set("partition_count", "64");

    conf.with_section(Some("garbage_collector"))
        .set("cleanup_interval_seconds", "3600")
        .set("file_ttl_seconds", "3600")
        .set("trash_retention_seconds", "2592000");

    conf.with_section(Some("export"))
        .set("retention_seconds", "86400")
        .set("cleanup_interval_seconds", "60")
        .set("max_concurrent_jobs", "2")
        .set("max_queued_jobs", "128")
        .set("manifest_validation_concurrency", "8")
        .set("object_read_concurrency", "8")
        .set("artifact_chunk_size_bytes", "16777216")
        .set("artifact_write_concurrency", "4")
        .set("max_buffered_bytes_per_job", "134217728")
        .set("max_buffered_bytes_global", "268435456")
        .set("max_concurrent_downloads", "32")
        .set("max_retained_bytes", "0");

    conf.with_section(Some("auto_healing"))
        .set("enabled", "true")
        .set("topology_check_interval_seconds", "30")
        .set("topology_recovery_stability_seconds", "10")
        .set("initial_scan_delay_seconds", "60")
        .set("availability_scan_interval_seconds", "3600")
        .set("checksum_scrub_interval_seconds", "604800")
        .set("object_scan_concurrency", "1")
        .set("shard_repair_concurrency", "1")
        .set("metadata_batch_size", "128");

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
        .set("stripe_block_size", "262144");

    conf.with_section(Some("io_control"))
        .set("foreground_wait_timeout_ms", "30000")
        .set("stall_threshold_ms", "20000")
        .set("slow_io_threshold_ms", "1000")
        .set("latency_samples_per_class", "1024");

    conf.write_to_file(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

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
        let index = ini
            .section(Some("index_config"))
            .expect("index_config section");
        assert_eq!(index.get("partition_count"), Some("64"));
        let io_control = ini.section(Some("io_control")).expect("io_control section");
        assert_eq!(io_control.get("foreground_wait_timeout_ms"), Some("30000"));
        assert_eq!(io_control.get("stall_threshold_ms"), Some("20000"));
        let gc = ini
            .section(Some("garbage_collector"))
            .expect("garbage_collector section");
        assert_eq!(gc.get("trash_retention_seconds"), Some("2592000"));
        assert!(gc.get("retention_seconds").is_none());
        let export = ini.section(Some("export")).expect("export section");
        assert_eq!(export.get("retention_seconds"), Some("86400"));
        assert_eq!(export.get("manifest_validation_concurrency"), Some("8"));
        assert_eq!(export.get("artifact_chunk_size_bytes"), Some("16777216"));
        assert_eq!(export.get("max_buffered_bytes_per_job"), Some("134217728"));
        assert_eq!(export.get("max_buffered_bytes_global"), Some("268435456"));
        assert_eq!(export.get("max_concurrent_downloads"), Some("32"));
        assert_eq!(export.get("max_retained_bytes"), Some("0"));
        let auto_healing = ini
            .section(Some("auto_healing"))
            .expect("auto_healing section");
        assert_eq!(auto_healing.get("enabled"), Some("true"));
        assert_eq!(
            auto_healing.get("availability_scan_interval_seconds"),
            Some("3600")
        );
        assert_eq!(
            auto_healing.get("checksum_scrub_interval_seconds"),
            Some("604800")
        );

        let _ = fs::remove_file(path);
    }
}
