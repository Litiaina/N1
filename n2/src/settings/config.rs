use ini::Ini;
use std::sync::LazyLock;

use crate::settings::constants::CONFIG_FILE;

pub struct GarbageCollectorConfig {
    pub cleanup_interval_seconds: u64,
    pub file_ttl_seconds: u64,
    pub retention_seconds: u64,
}

#[derive(Clone, Debug)]
pub struct AutoHealingConfig {
    pub enabled: bool,
    pub initial_delay_seconds: u64,
    pub scan_interval_seconds: u64,
    pub coordinator_nodes: usize,
    pub max_parallel_fragments: usize,
    pub max_objects_per_fragment_cycle: u64,
    pub max_duration_seconds: u64,
    pub lease_seconds: u64,
    pub checksum_scan_every_cycles: u64,
}

impl Default for AutoHealingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay_seconds: 30,
            scan_interval_seconds: 900,
            coordinator_nodes: 0,
            max_parallel_fragments: 2,
            max_objects_per_fragment_cycle: 256,
            max_duration_seconds: 30,
            lease_seconds: 90,
            checksum_scan_every_cycles: 24,
        }
    }
}

pub struct Config {
    pub garbage_collector: GarbageCollectorConfig,
    pub auto_healing: AutoHealingConfig,
}

fn parse_bool(value: Option<&str>, default: bool) -> bool {
    match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some("1" | "true" | "yes" | "on") => true,
        Some("0" | "false" | "no" | "off") => false,
        _ => default,
    }
}

fn parse_or<T>(value: Option<&str>, default: T, key: &str) -> T
where
    T: std::str::FromStr,
{
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "auto")
        .map(|value| {
            value
                .parse::<T>()
                .unwrap_or_else(|_| panic!("'{key}' has an invalid value"))
        })
        .unwrap_or(default)
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

    let retention_seconds: u64 = gc_section
        .get("retention_seconds")
        .expect("Missing 'retention_seconds' in [garbage_collector] section")
        .parse()
        .expect("'retention_seconds' must be a valid positive integer.");

    let defaults = AutoHealingConfig::default();
    let healing = conf.section(Some("auto_healing"));
    let max_duration_seconds = parse_or(
        healing.and_then(|section| section.get("max_duration_seconds")),
        defaults.max_duration_seconds,
        "max_duration_seconds",
    )
    .clamp(5, 3600);
    let lease_seconds = parse_or(
        healing.and_then(|section| section.get("lease_seconds")),
        defaults.lease_seconds,
        "lease_seconds",
    )
    .max(max_duration_seconds.saturating_mul(2).saturating_add(5));
    let auto_healing = AutoHealingConfig {
        enabled: parse_bool(
            healing.and_then(|section| section.get("enabled")),
            defaults.enabled,
        ),
        initial_delay_seconds: parse_or(
            healing.and_then(|section| section.get("initial_delay_seconds")),
            defaults.initial_delay_seconds,
            "initial_delay_seconds",
        ),
        scan_interval_seconds: parse_or(
            healing.and_then(|section| section.get("scan_interval_seconds")),
            defaults.scan_interval_seconds,
            "scan_interval_seconds",
        )
        .max(1),
        coordinator_nodes: parse_or(
            healing.and_then(|section| section.get("coordinator_nodes")),
            defaults.coordinator_nodes,
            "coordinator_nodes",
        ),
        max_parallel_fragments: parse_or(
            healing.and_then(|section| section.get("max_parallel_fragments")),
            defaults.max_parallel_fragments,
            "max_parallel_fragments",
        )
        .clamp(1, 32),
        max_objects_per_fragment_cycle: parse_or(
            healing.and_then(|section| section.get("max_objects_per_fragment_cycle")),
            defaults.max_objects_per_fragment_cycle,
            "max_objects_per_fragment_cycle",
        )
        .max(1),
        max_duration_seconds,
        lease_seconds,
        checksum_scan_every_cycles: parse_or(
            healing.and_then(|section| section.get("checksum_scan_every_cycles")),
            defaults.checksum_scan_every_cycles,
            "checksum_scan_every_cycles",
        )
        .max(1),
    };

    Config {
        garbage_collector: GarbageCollectorConfig {
            cleanup_interval_seconds,
            file_ttl_seconds,
            retention_seconds,
        },
        auto_healing,
    }
});
