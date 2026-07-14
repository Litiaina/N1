use ini::Ini;
use std::sync::LazyLock;

use crate::settings::constants::CONFIG_FILE;

pub struct GarbageCollectorConfig {
    pub cleanup_interval_seconds: u64,
    pub file_ttl_seconds: u64,
    pub trash_retention_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct AutoHealingConfig {
    pub enabled: bool,
    pub topology_check_interval_seconds: u64,
    pub topology_recovery_stability_seconds: u64,
    pub initial_scan_delay_seconds: u64,
    pub availability_scan_interval_seconds: u64,
    pub checksum_scrub_interval_seconds: u64,
    pub object_scan_concurrency: usize,
    pub shard_repair_concurrency: usize,
    pub metadata_batch_size: usize,
}

pub struct Config {
    pub garbage_collector: GarbageCollectorConfig,
    pub auto_healing: AutoHealingConfig,
}

fn required_value<'a>(section: &'a ini::Properties, key: &str) -> &'a str {
    section
        .get(key)
        .unwrap_or_else(|| panic!("Missing '{key}' in [auto_healing] section"))
        .trim()
}

fn parse_u64(section: &ini::Properties, key: &str) -> u64 {
    required_value(section, key)
        .parse::<u64>()
        .unwrap_or_else(|_| panic!("'{key}' in [auto_healing] must be an unsigned integer"))
}

fn parse_usize(section: &ini::Properties, key: &str) -> usize {
    required_value(section, key)
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("'{key}' in [auto_healing] must be an unsigned integer"))
}

fn parse_bool(section: &ini::Properties, key: &str) -> bool {
    match required_value(section, key) {
        "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON" => true,
        "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF" => false,
        _ => panic!("'{key}' in [auto_healing] must be a boolean"),
    }
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

    let auto_healing_section = conf
        .section(Some("auto_healing"))
        .expect("Missing [auto_healing] section in config.ini");

    Config {
        garbage_collector: GarbageCollectorConfig {
            cleanup_interval_seconds,
            file_ttl_seconds,
            trash_retention_seconds,
        },
        auto_healing: AutoHealingConfig {
            enabled: parse_bool(auto_healing_section, "enabled"),
            topology_check_interval_seconds: parse_u64(
                auto_healing_section,
                "topology_check_interval_seconds",
            )
            .max(1),
            topology_recovery_stability_seconds: parse_u64(
                auto_healing_section,
                "topology_recovery_stability_seconds",
            ),
            initial_scan_delay_seconds: parse_u64(
                auto_healing_section,
                "initial_scan_delay_seconds",
            ),
            availability_scan_interval_seconds: parse_u64(
                auto_healing_section,
                "availability_scan_interval_seconds",
            )
            .max(1),
            checksum_scrub_interval_seconds: parse_u64(
                auto_healing_section,
                "checksum_scrub_interval_seconds",
            )
            .max(1),
            object_scan_concurrency: parse_usize(auto_healing_section, "object_scan_concurrency")
                .clamp(1, 64),
            shard_repair_concurrency: parse_usize(auto_healing_section, "shard_repair_concurrency")
                .clamp(1, 32),
            metadata_batch_size: parse_usize(auto_healing_section, "metadata_batch_size")
                .clamp(32, 4096),
        },
    }
});
