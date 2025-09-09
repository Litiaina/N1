use ini::Ini;
use std::io;

pub fn create_default_config_ini(path: &str) -> io::Result<()> {
    let mut conf = Ini::new();
    conf.with_section(Some("garbage_collector"))
        .set("cleanup_interval_seconds", "86400")
        .set("file_ttl_seconds", "86400");
    conf.write_to_file(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}
