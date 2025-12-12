use ini::Ini;
use std::{io, path::Path};

pub fn create_default_config_ini<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let mut conf = Ini::new();

    //Server configuration
    conf.with_section(Some("server"))
        .set("host", "127.0.0.1")
        .set("http_port", "51000")
        .set("https_port", "50001")
        .set("cert_path", "certificates/cert.pem")
        .set("key_path", "certificates/key.pem")
        .set("sharable_url", "https://127.0.0.1:50001");

    //Garbage collector
    conf.with_section(Some("garbage_collector"))
        .set("cleanup_interval_seconds", "86400")
        .set("file_ttl_seconds", "86400");

    //Upload & Storage limits
    conf.with_section(Some("set_limits"))
        .set("max_file_size", "5368709120"); // 5GB

    //JWT Token settings
    conf.with_section(Some("jwt_token_config"))
        .set("login_token_expiration", "600"); // 10 minutes

    //Storage config
    conf.with_section(Some("garbage_collector"))
        .set("cleanup_interval_seconds", "86400")
        .set("file_ttl_seconds", "86400");

    //Rate limiter config
    conf.with_section(Some("rate_limiter_config"))
        // Enable or Disable rate limiter
        .set("enable_rate_limiting", "true")
        // Auth
        .set("auth_limit_per_second", "1")
        .set("auth_limit_burst_size", "5")
        // View operations
        .set("view_limit_per_second", "100")
        .set("view_limit_burst_size", "2000")
        // Upload operations
        .set("upload_limit_per_second", "300")
        .set("upload_limit_burst_size", "3000")
        // Transcode operations
        .set("transcode_limit_per_second", "5")
        .set("transcode_limit_burst_size", "10")
        // Global fallback
        .set("global_limit_per_second", "200")
        .set("global_limit_burst_size", "2000");

    //Write config file
    conf.write_to_file(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}
