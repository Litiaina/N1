use ini::Ini;
use std::{io, path::Path};

pub fn create_default_config_ini<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let mut conf = Ini::new();

    //Server Configuration
    conf.with_section(Some("server"))
        .set("host", "127.0.0.1")
        .set("http_port", "51000")
        .set("https_port", "50001")
        .set("cert_path", "certificates/cert.pem")
        .set("key_path", "certificates/key.pem");

    //Garbage Collector
    conf.with_section(Some("garbage_collector"))
        .set("cleanup_interval_seconds", "86400")
        .set("file_ttl_seconds", "86400");

    //Upload & Storage Limits
    conf.with_section(Some("set_limits"))
        .set("max_file_size", "5368709120"); // 5GB

    //JWT Token Settings
    conf.with_section(Some("jwt_token_config"))
        .set("login_token_expiration", "600"); // 10 minutes

    //Storage Config
    conf.with_section(Some("garbage_collector"))
        .set("cleanup_interval_seconds", "86400")
        .set("file_ttl_seconds", "86400");

    //Write Config File
    conf.write_to_file(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}
