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
        .set("index_store_disks", "")
        .set("blob_store_disks", "")
        .set("fs_storage_path", "");

    conf.with_section(Some("cluster_config"))
        .set("node_id", "node-1")
        .set("node_endpoint", "https://127.0.0.1:50001")
        .set("native_endpoint", "n2://127.0.0.1:50002")
        .set("membership_file", "n2_membership.json")
        .set("cluster_layout_mode", "auto")
        .set("fixed_data_shards", "")
        .set("fixed_parity_shards", "")
        .set("auto_rebalance", "false");

    conf.with_section(Some("quota_config"))
        .set("fragment_quota_backend", "disabled");

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

    conf.write_to_file(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn default_config_includes_cluster_section() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "n2-default-config-no-n2-{}-{}.ini",
            std::process::id(),
            unique
        ));

        create_default_config_ini(&path).expect("create default config");
        let ini = Ini::load_from_file(&path).expect("load generated config");
        let cluster = ini
            .section(Some("cluster_config"))
            .expect("cluster_config section");
        assert_eq!(cluster.get("cluster_layout_mode"), Some("auto"));
        assert_eq!(cluster.get("membership_file"), Some("n2_membership.json"));
        let index = ini
            .section(Some("index_config"))
            .expect("index_config section");
        assert_eq!(index.get("partition_count"), Some("64"));

        let _ = fs::remove_file(path);
    }
}
