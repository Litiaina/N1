use ini::Ini;
use std::sync::LazyLock;

use crate::settings::constants::CONFIG_FILE;

pub struct GarbageCollectorConfig {
    pub cleanup_interval_seconds: u64,
    pub file_ttl_seconds: u64,
    pub trash_retention_seconds: u64,
}

pub struct Config {
    pub garbage_collector: GarbageCollectorConfig,
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    let conf = Ini::load_from_file(CONFIG_FILE)
        .expect("Failed to load config.ini. Make sure the file exists and is readable.");

    let gc_section = conf
        .section(Some("garbage_collector"))
        .expect("Missing [garbage_collector] section in config.ini");

    let cleanup_interval_seconds: u64 = gc_section
        .get("cleanup_interval_seconds")
        .expect("Missing 'cleanup_interval_seconds' in [garbage_collector] section")
        .parse()
        .expect("'cleanup_interval_seconds' must be a valid positive integer.");

    let file_ttl_seconds: u64 = gc_section
        .get("file_ttl_seconds")
        .expect("Missing 'file_ttl_seconds' in [garbage_collector] section")
        .parse()
        .expect("'file_ttl_seconds' must be a valid positive integer.");

    let trash_retention_seconds: u64 = gc_section
        .get("trash_retention_seconds")
        .expect("Missing 'trash_retention_seconds' in [garbage_collector] section")
        .parse()
        .expect("'trash_retention_seconds' must be a valid non-negative integer.");

    Config {
        garbage_collector: GarbageCollectorConfig {
            cleanup_interval_seconds,
            file_ttl_seconds,
            trash_retention_seconds,
        },
    }
});
