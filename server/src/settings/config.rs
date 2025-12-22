use ini::Ini;
use std::sync::LazyLock;

pub struct GarbageCollectorConfig {
    pub cleanup_interval_seconds: u64,
    pub file_ttl_seconds: u64,
    pub retention_days: u64,
}

pub struct Config {
    pub garbage_collector: GarbageCollectorConfig,
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    let conf = Ini::load_from_file("config.ini")
        .expect("Failed to load config.ini. Make sure the file exists and is readable.");

    let gc_section = conf.section(Some("garbage_collector"))
        .expect("Missing [garbage_collector] section in config.ini");

    let cleanup_interval_seconds: u64 = gc_section.get("cleanup_interval_seconds")
        .expect("Missing 'cleanup_interval_seconds' in [garbage_collector] section")
        .parse()
        .expect("'cleanup_interval_seconds' must be a valid positive integer.");

    let file_ttl_seconds: u64 = gc_section.get("file_ttl_seconds")
        .expect("Missing 'file_ttl_seconds' in [garbage_collector] section")
        .parse()
        .expect("'file_ttl_seconds' must be a valid positive integer.");

    let retention_days: u64 = gc_section.get("retention_days")
        .expect("Missing 'retention_days' in [garbage_collector] section")
        .parse()
        .expect("'retention_days' must be a valid positive integer.");

    Config {
        garbage_collector: GarbageCollectorConfig {
            cleanup_interval_seconds,
            file_ttl_seconds,
            retention_days,
        },
    }
});
