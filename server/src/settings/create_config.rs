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
        .set("login_token_expiration", "600");

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
        .set("prefix_max_delta_segments", "auto")
        .set("prefix_compact_trigger_segments", "auto")
        .set("prefix_compact_min_interval_seconds", "auto")
        .set("monitor_interval_seconds", "auto")
        .set("monitor_deep_scan_interval_seconds", "auto")
        .set("max_part_size", "52428800")
        .set("data_shards", "1")
        .set("parity_shards", "0")
        .set("stripe_block_size", "262144");

    conf.write_to_file(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}
