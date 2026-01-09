use ini::Ini;
use std::{io, path::Path};

pub fn create_default_config_ini<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let mut conf = Ini::new();

    //Server configuration
    conf.with_section(Some("server"))
        .set("host", "127.0.0.1")
        .set("https_port", "50001")
        .set("cert_path", "certificates/cert.pem")
        .set("key_path", "certificates/key.pem")
        .set("sharable_url", "https://127.0.0.1:50001");

    //Storage disk configuration
    conf.with_section(Some("disk_config"))
        //Root storage path
        .set("object_storage_path", "object-storage-path")
        .set("fs_storage_path", "fs-storage-path");

    //Garbage collector
    conf.with_section(Some("garbage_collector"))
        .set("cleanup_interval_seconds", "3600")
        .set("file_ttl_seconds", "3600")
        .set("retention_days", "30");

    //Upload & Storage limits
    conf.with_section(Some("set_limits"))
        .set("max_file_size", "5368709120"); // 5GB

    //JWT Token settings
    conf.with_section(Some("jwt_token_config"))
        .set("login_token_expiration", "600"); // 10 minutes

    //Rate limiter config
    conf.with_section(Some("rate_limiter_config"))
        // Enable or Disable rate limiter
        .set("enable_rate_limiting", "true")
        // Auth: Strict. Human input speed only.
        .set("auth_limit_per_second", "2")
        .set("auth_limit_burst_size", "5")
        // View: Accommodates ThumbnailLoader queue (4 concurrent)
        .set("view_limit_per_second", "30")
        .set("view_limit_burst_size", "100") 
        // Upload: Accommodates 6 parallel workers on high-speed connections
        .set("upload_limit_per_second", "300")
        .set("upload_limit_burst_size", "3000")
        // Transcode: Very CPU heavy, strict limit
        .set("transcode_limit_per_second", "1")
        .set("transcode_limit_burst_size", "2")
        // Sharing: Browser image fetching (max ~6 concurrent connections)
        .set("sharing_limit_per_second", "50")
        .set("sharing_limit_burst_size", "100")
        // Global: Safety net (View + Upload + Sharing overhead)
        .set("global_limit_per_second", "200") 
        .set("global_limit_burst_size", "300");

    //Upload erasure coding config
    conf.with_section(Some("upload_config"))
        // Max concurrent IO
        .set("max_concurrent_io", "16384")
        // Max writer buffer size
        .set("writer_buffer_size", "262144")
        // Max part size of an object
        .set("max_part_size", "52428800")
        // Total data shards during erasure coding
        .set("data_shards", "4")
        // Total parity shards during erasure coding
        .set("parity_shards", "2");

    //Write config file
    conf.write_to_file(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}
