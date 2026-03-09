use crate::handlers::error_handlers::handler_404;
use crate::n1_core::adaptive_concurrency::{
    ADAPTIVE_CONCURRENCY_ENABLED_ATOMIC, ADAPTIVE_CONCURRENCY_INTERVAL_MS_ATOMIC,
};
use crate::n1_core::blob_store::{
    format_raw_blob_device_with_intent, format_raw_index_device_with_intent,
    prepare_raw_blob_device_format, prepare_raw_index_device_format,
    validate_configured_raw_blob_disks,
};
use crate::n1_core::disk_manager::initialize_storage_roots;
use crate::n1_core::index_facade::{IndexBackendMode, set_index_backend_mode};
use crate::n1_core::monitoring::{
    MONITOR_DEEP_SCAN_INTERVAL_SECS_ATOMIC, MONITOR_INTERVAL_SECS_ATOMIC,
};
use crate::n1_core::object_parser::{
    ADAPTIVE_STREAM_TARGET_ATOMIC, STREAM_CHUNK_SIZE_ATOMIC, STREAM_LOCALITY_BIAS_ENABLED_ATOMIC,
    STREAM_MAX_CONCURRENCY, STREAM_READAHEAD_BYTES_ATOMIC,
};
use crate::n1_core::recovery::RAW_VERIFY_ON_READ_ATOMIC;
use crate::n1_core::upload::{
    ADAPTIVE_IO_TARGET_ATOMIC, DATA_SHARDS_ATOMIC, MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC,
    PARITY_SHARDS_ATOMIC, RAW_WRITER_INFLIGHT_LIMIT_ATOMIC, STRIPE_SIZE_ATOMIC,
    UPLOAD_IO_GATE_ENABLED_ATOMIC, UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC,
    WRITER_BUFFER_SIZE_ATOMIC,
};
use crate::routes::storage_routes::serve_storage_routes;
use crate::settings::constants::{CONFIG_FILE, ENV_FILE};
use crate::utils::date_time_formatter::LocalTimer;
use crate::validation::enforce_init_settings::{ensure_config_file, ensure_env_file};
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use chrono::Local;
use clap::Parser;
use dotenv::dotenv;
use ini::Ini;
use server_cli::cli::args::{Cli, Commands};
use server_cli::cli::client::execute_command;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::time::Duration;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

mod enums;
mod fuse;
mod handlers;
mod middleware;
mod n1_core;
mod routes;
mod settings;
mod smb;
mod structs;
mod utils;
mod validation;
mod websocket;

fn detect_cpu_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn align_up_4k(v: usize) -> usize {
    ((v.max(4096) + 4095) / 4096) * 4096
}

fn ram_gib(ram_bytes: Option<u64>) -> u64 {
    ram_bytes
        .map(|b| b / (1024 * 1024 * 1024))
        .unwrap_or(0)
        .max(1)
}

fn adaptive_max_concurrent_io(
    cpu_cores: usize,
    blob_disks: usize,
    ram_bytes: Option<u64>,
) -> usize {
    let core_cap = (cpu_cores.saturating_mul(6)).clamp(6, 192);
    let disk_cap = (blob_disks.max(1).saturating_mul(12)).clamp(6, 192);
    let ram_cap = match ram_gib(ram_bytes) {
        0..=4 => 8,
        5..=8 => 16,
        9..=16 => 24,
        17..=32 => 32,
        33..=64 => 48,
        _ => 64,
    };
    core_cap.min(disk_cap).min(ram_cap).clamp(4, 192)
}

fn adaptive_writer_buffer_size(
    cpu_cores: usize,
    ram_bytes: Option<u64>,
    blob_disks: usize,
    total_shards: usize,
) -> usize {
    let ram = ram_gib(ram_bytes);
    let mut bytes = match ram_gib(ram_bytes) {
        0..=4 => 128 * 1024,
        5..=8 => 512 * 1024,
        9..=64 => 1024 * 1024,
        _ => 2 * 1024 * 1024,
    };

    if cpu_cores <= 2 {
        bytes = bytes.min(128 * 1024);
    } else if cpu_cores <= 4 {
        bytes = bytes.min(512 * 1024);
    } else if cpu_cores <= 8 {
        bytes = bytes.min(1024 * 1024);
    }

    if total_shards >= 8 {
        bytes = bytes.min(256 * 1024);
    } else if total_shards >= 6 {
        bytes = bytes.min(512 * 1024);
    } else if total_shards >= 4 {
        bytes = bytes.min(768 * 1024);
    }

    if blob_disks <= 1 {
        let single_disk_cap = if cpu_cores <= 2 || ram <= 4 {
            256 * 1024
        } else if cpu_cores <= 4 || ram <= 8 {
            1024 * 1024
        } else if cpu_cores <= 8 || ram <= 16 {
            2 * 1024 * 1024
        } else {
            4 * 1024 * 1024
        };
        bytes = bytes.min(single_disk_cap);
    } else if blob_disks == 2 {
        bytes = bytes.min(512 * 1024);
    }

    align_up_4k(bytes.clamp(64 * 1024, 4 * 1024 * 1024))
}

fn adaptive_raw_writer_inflight_limit(
    cpu_cores: usize,
    ram_bytes: Option<u64>,
    blob_disks: usize,
    max_concurrent_io: usize,
    writer_buffer_size: usize,
    total_shards: usize,
) -> usize {
    let ram = ram_gib(ram_bytes);
    let mut limit = match ram {
        0..=4 => 2,
        5..=8 => 4,
        9..=16 => 8,
        17..=32 => 12,
        33..=64 => 16,
        _ => 24,
    };

    if cpu_cores <= 2 {
        limit = limit.min(2);
    } else if cpu_cores <= 4 {
        limit = limit.min(4);
    } else if cpu_cores <= 8 {
        limit = limit.min(8);
    } else if cpu_cores <= 16 {
        limit = limit.min(16);
    } else {
        limit = limit.min(24);
    }

    if writer_buffer_size >= 4 * 1024 * 1024 {
        limit = limit.min(8);
    } else if writer_buffer_size >= 2 * 1024 * 1024 {
        limit = limit.min(12);
    } else if writer_buffer_size >= 1024 * 1024 {
        limit = limit.min(16);
    }

    if blob_disks <= 1 {
        limit = limit.min(match ram {
            0..=8 => 4,
            9..=16 => 6,
            17..=32 => 8,
            33..=64 => 12,
            _ => 16,
        });
    } else if blob_disks == 2 {
        limit = limit.min(16);
    } else {
        limit = limit.min(24);
    }

    let per_upload_cap = max_concurrent_io
        .saturating_mul(2)
        .saturating_div(total_shards.max(1))
        .clamp(4, 32);
    limit.min(per_upload_cap).clamp(2, 32)
}

fn detect_total_ram_bytes() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kib = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok())?;
            return Some(kib.saturating_mul(1024));
        }
    }
    None
}

fn adaptive_upload_memory_budget_bytes(
    cpu_cores: usize,
    ram_bytes: Option<u64>,
    blob_disks: usize,
    total_shards: usize,
) -> usize {
    let mut budget: usize = match ram_gib(ram_bytes) {
        0..=4 => 32usize * 1024 * 1024,
        5..=8 => 64usize * 1024 * 1024,
        9..=16 => 96usize * 1024 * 1024,
        17..=32 => 128usize * 1024 * 1024,
        33..=64 => 192usize * 1024 * 1024,
        _ => 256usize * 1024 * 1024,
    };

    if cpu_cores >= 24 {
        budget = budget.saturating_add(32usize * 1024 * 1024);
    }
    if total_shards >= 4 {
        budget = budget.saturating_add(32usize * 1024 * 1024);
    }

    if blob_disks <= 1 {
        budget = budget.min(96usize * 1024 * 1024);
    } else if blob_disks == 2 {
        budget = budget.min(160usize * 1024 * 1024);
    }

    budget.clamp(32usize * 1024 * 1024, 384usize * 1024 * 1024)
}

fn estimate_upload_part_working_set_bytes(
    data_shards: usize,
    parity_shards: usize,
    writer_buffer_size: usize,
    raw_writer_inflight_limit: usize,
    stripe_block_size: usize,
) -> usize {
    let total_shards = data_shards.saturating_add(parity_shards).max(1);
    let _ = writer_buffer_size;
    let raw_block_bytes = stripe_block_size.max(4096);
    let writer_buffers_per_shard = raw_writer_inflight_limit.saturating_add(1).max(1);
    let writer_bytes = total_shards
        .saturating_mul(raw_block_bytes)
        .saturating_mul(writer_buffers_per_shard);
    let ec_ingest = data_shards
        .max(1)
        .saturating_mul(stripe_block_size.max(4096));
    let framing_overhead = 512 * 1024usize;
    writer_bytes
        .saturating_add(ec_ingest.saturating_mul(2))
        .saturating_add(framing_overhead)
        .max(1)
}

fn adaptive_stream_chunk_size(cpu_cores: usize, ram_bytes: Option<u64>) -> usize {
    let bytes = if cpu_cores <= 2 || ram_gib(ram_bytes) <= 4 {
        128 * 1024
    } else if cpu_cores <= 4 || ram_gib(ram_bytes) <= 8 {
        256 * 1024
    } else if cpu_cores <= 8 || ram_gib(ram_bytes) <= 16 {
        512 * 1024
    } else {
        1024 * 1024
    };
    align_up_4k(bytes)
}

fn adaptive_stream_readahead_bytes(chunk_size: usize, ram_bytes: Option<u64>) -> usize {
    let mul = match ram_gib(ram_bytes) {
        0..=4 => 2,
        5..=8 => 2,
        9..=16 => 3,
        17..=32 => 4,
        _ => 6,
    };
    align_up_4k((chunk_size.saturating_mul(mul)).clamp(64 * 1024, 8 * 1024 * 1024))
}

fn adaptive_stream_memory_budget_bytes(ram_bytes: Option<u64>) -> usize {
    match ram_gib(ram_bytes) {
        0..=4 => 48 * 1024 * 1024,
        5..=8 => 64 * 1024 * 1024,
        9..=16 => 96 * 1024 * 1024,
        17..=32 => 128 * 1024 * 1024,
        33..=64 => 192 * 1024 * 1024,
        _ => 256 * 1024 * 1024,
    }
}

fn adaptive_stream_max_concurrency(
    cpu_cores: usize,
    blob_disks: usize,
    ram_bytes: Option<u64>,
    chunk_size: usize,
    readahead_bytes: usize,
) -> usize {
    let core_cap = (cpu_cores.saturating_mul(12)).clamp(12, 768);
    let disk_cap = (blob_disks.max(1).saturating_mul(64)).clamp(16, 768);
    let per_stream = chunk_size
        .saturating_add(readahead_bytes)
        .saturating_add(64 * 1024)
        .max(1);
    let mem_cap = (adaptive_stream_memory_budget_bytes(ram_bytes) / per_stream).clamp(8, 768);
    core_cap.min(disk_cap).min(mem_cap).clamp(8, 768)
}

fn parse_index_backend_mode(raw: Option<&str>) -> IndexBackendMode {
    let Some(value) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return IndexBackendMode::RawOnly;
    };
    match value.to_ascii_lowercase().as_str() {
        "raw" | "raw-only" | "raw_only" => IndexBackendMode::RawOnly,
        "raw-shadow" | "raw_shadow" | "shadow" => IndexBackendMode::RawShadow,
        _ => IndexBackendMode::Fs,
    }
}

#[cfg(unix)]
fn storage_identity_for_metadata(dev: u64, rdev: u64, is_block_device: bool) -> u64 {
    if is_block_device && rdev != 0 {
        rdev
    } else {
        dev
    }
}

#[cfg(unix)]
fn detect_unique_storage_devices(paths: &[String]) -> usize {
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::fs::MetadataExt;

    let mut dev_ids: HashSet<u64> = HashSet::new();
    let mut fallback_paths: HashSet<PathBuf> = HashSet::new();

    for raw in paths {
        let p = PathBuf::from(raw);
        let resolved = std::fs::canonicalize(&p).unwrap_or(p);
        fallback_paths.insert(resolved.clone());

        if let Ok(meta) = std::fs::metadata(&resolved) {
            dev_ids.insert(storage_identity_for_metadata(
                meta.dev(),
                meta.rdev(),
                meta.file_type().is_block_device(),
            ));
        }
    }

    if !dev_ids.is_empty() {
        dev_ids.len().max(1)
    } else {
        fallback_paths.len().max(1)
    }
}

#[cfg(not(unix))]
fn detect_unique_storage_devices(paths: &[String]) -> usize {
    let mut unique: HashSet<PathBuf> = HashSet::new();
    for raw in paths {
        let p = PathBuf::from(raw);
        let resolved = std::fs::canonicalize(&p).unwrap_or(p);
        unique.insert(resolved);
    }
    unique.len().max(1)
}

fn adaptive_concurrency_interval_ms(cpu_cores: usize, ram_bytes: Option<u64>) -> usize {
    if cpu_cores <= 2 || ram_gib(ram_bytes) <= 4 {
        1500
    } else if cpu_cores <= 4 || ram_gib(ram_bytes) <= 8 {
        1200
    } else if cpu_cores <= 8 {
        1000
    } else {
        750
    }
}

fn adaptive_monitor_interval_seconds(cpu_cores: usize, ram_bytes: Option<u64>) -> usize {
    if cpu_cores <= 2 || ram_gib(ram_bytes) <= 4 {
        5
    } else if cpu_cores <= 4 || ram_gib(ram_bytes) <= 8 {
        3
    } else {
        2
    }
}

fn adaptive_monitor_deep_scan_interval_seconds(cpu_cores: usize, ram_bytes: Option<u64>) -> usize {
    if cpu_cores <= 2 || ram_gib(ram_bytes) <= 4 {
        180
    } else if cpu_cores <= 4 || ram_gib(ram_bytes) <= 8 {
        120
    } else {
        60
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::storage_identity_for_metadata;
    use super::{
        adaptive_raw_writer_inflight_limit, adaptive_writer_buffer_size,
        estimate_upload_part_working_set_bytes,
    };

    #[test]
    fn single_disk_writer_buffer_size_is_not_artificially_tiny() {
        let bytes = adaptive_writer_buffer_size(8, Some(16 * 1024 * 1024 * 1024), 1, 1);
        assert!(bytes >= 1024 * 1024);
    }

    #[test]
    fn raw_writer_inflight_limit_stays_conservative_on_low_ram() {
        let limit = adaptive_raw_writer_inflight_limit(
            2,
            Some(4 * 1024 * 1024 * 1024),
            1,
            32,
            512 * 1024,
            1,
        );
        assert_eq!(limit, 2);
    }

    #[test]
    fn low_ram_single_disk_writer_buffer_stays_small() {
        let bytes = adaptive_writer_buffer_size(2, Some(4 * 1024 * 1024 * 1024), 1, 1);
        assert!(bytes <= 256 * 1024);
    }

    #[test]
    fn raw_writer_inflight_limit_scales_for_better_hardware() {
        let limit = adaptive_raw_writer_inflight_limit(
            16,
            Some(32 * 1024 * 1024 * 1024),
            1,
            64,
            1024 * 1024,
            1,
        );
        assert!(limit >= 8);
    }

    #[test]
    fn raw_writer_inflight_limit_goes_deeper_on_large_single_disk_nodes() {
        let limit = adaptive_raw_writer_inflight_limit(
            24,
            Some(128 * 1024 * 1024 * 1024),
            1,
            128,
            1024 * 1024,
            1,
        );
        assert!(limit >= 16);
    }

    #[test]
    fn upload_working_set_accounts_for_raw_writer_queueing() {
        let shallow = estimate_upload_part_working_set_bytes(1, 0, 1024 * 1024, 2, 1024 * 1024);
        let deep = estimate_upload_part_working_set_bytes(1, 0, 1024 * 1024, 8, 1024 * 1024);
        assert!(deep > shallow);
    }

    #[cfg(unix)]
    #[test]
    fn block_device_identity_prefers_rdev_over_devtmpfs_dev() {
        let identity = storage_identity_for_metadata(42, 1337, true);
        assert_eq!(identity, 1337);
    }

    #[cfg(unix)]
    #[test]
    fn mounted_path_identity_stays_on_filesystem_dev() {
        let identity = storage_identity_for_metadata(42, 1337, false);
        assert_eq!(identity, 42);
    }
}

fn parse_usize_or_auto(raw: Option<&str>, key: &str, auto_value: usize) -> usize {
    match raw.map(str::trim) {
        None => auto_value,
        Some(v) if v.is_empty() => auto_value,
        Some(v) if v.eq_ignore_ascii_case("auto") => auto_value,
        Some(v) => match v.parse::<usize>() {
            Ok(0) => auto_value,
            Ok(parsed) => parsed,
            Err(_) => panic!("{key} must be a positive usize or 'auto'"),
        },
    }
}

fn parse_bool_or_default(raw: Option<&str>, default_value: bool) -> bool {
    match raw.map(str::trim) {
        None => default_value,
        Some(v) if v.is_empty() => default_value,
        Some(v) if matches!(v, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON") => true,
        Some(v) if matches!(v, "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF") => false,
        Some(_) => default_value,
    }
}

fn is_auto_or_empty(raw: Option<&str>) -> bool {
    match raw.map(str::trim) {
        None => true,
        Some(v) if v.is_empty() => true,
        Some(v) if v.eq_ignore_ascii_case("auto") => true,
        Some(_) => false,
    }
}

pub fn init_tracing() -> Result<WorkerGuard, Box<dyn std::error::Error>> {
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "info,litiaina_n1=trace,server_cli=trace,axum=info,hyper=info".into()
        }))
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_target(false)
                .with_file(false)
                .with_line_number(true)
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_ansi(true)
                .with_timer(LocalTimer),
        )
        .try_init()?;

    Ok(guard)
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install 'ring' as the default TLS provider.");

    if let Err(e) = ensure_env_file() {
        tracing::error!("Failed to ensure .env file: {}", e);
        return;
    }

    dotenv::from_filename(ENV_FILE).ok();
    tracing::info!("Loaded environment variables from .env file.");

    if let Err(e) = ensure_config_file() {
        tracing::error!("Failed to ensure config file: {}", e);
        return;
    }

    let _guard = init_tracing().expect("Failed to initialize tracing subscriber");

    let args = Cli::parse();

    match &args.command {
        Some(Commands::Server) | None => {}
        Some(Commands::FormatBlobDevice {
            path,
            force,
            prepare,
            nonce,
            fingerprint,
        }) => {
            let result = if *prepare {
                prepare_raw_blob_device_format(Path::new(path), *force).map(|intent| {
                    if args.json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&intent)
                                .expect("serialize blob format intent")
                        );
                    } else {
                        println!("Prepared raw blob format intent:");
                        println!("  path: {}", intent.path);
                        println!("  canonical_path: {}", intent.canonical_path);
                        println!("  force: {}", intent.force);
                        println!("  nonce: {}", intent.nonce);
                        println!("  fingerprint: {}", intent.fingerprint);
                        println!("  expires_at_unix: {}", intent.expires_at);
                        println!("Run format command with:");
                        println!(
                            "  --path '{}' --force={} --nonce '{}' --fingerprint '{}'",
                            intent.path, intent.force, intent.nonce, intent.fingerprint
                        );
                    }
                })
            } else {
                let nonce = nonce.as_deref().ok_or_else(|| {
                    crate::enums::app_error_enum::AppError::BadRequest(
                        "missing --nonce; run format-blob-device --prepare first".to_string(),
                    )
                });
                let fingerprint = fingerprint.as_deref().ok_or_else(|| {
                    crate::enums::app_error_enum::AppError::BadRequest(
                        "missing --fingerprint; run format-blob-device --prepare first".to_string(),
                    )
                });
                match (nonce, fingerprint) {
                    (Ok(nonce), Ok(fingerprint)) => format_raw_blob_device_with_intent(
                        Path::new(path),
                        *force,
                        nonce,
                        fingerprint,
                    )
                    .map(|status| {
                        if args.json {
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&status)
                                    .expect("serialize blob device status")
                            );
                        } else {
                            println!("Formatted raw blob device:");
                            println!("  path: {}", status.path);
                            println!("  canonical_path: {}", status.canonical_path);
                            println!("  status: {}", status.status);
                            println!(
                                "  device_id: {}",
                                status.device_id.unwrap_or_else(|| "<none>".to_string())
                            );
                            println!(
                                "  block_size_bytes: {}",
                                status
                                    .block_size_bytes
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "<unknown>".to_string())
                            );
                            println!(
                                "  total_bytes: {}",
                                status
                                    .total_bytes
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "<unknown>".to_string())
                            );
                            println!(
                                "  available_bytes: {}",
                                status
                                    .available_bytes
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "<unknown>".to_string())
                            );
                            if let Some(warning) = status.path_warning {
                                println!("  warning: {}", warning);
                            }
                        }
                    }),
                    (Err(err), _) | (_, Err(err)) => Err(err),
                }
            };

            drop(_guard);

            match result {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Command failed: {}", e);
                    std::process::exit(1);
                }
            }

            return;
        }
        Some(Commands::FormatIndexDevice {
            path,
            force,
            prepare,
            nonce,
            fingerprint,
        }) => {
            let result = if *prepare {
                prepare_raw_index_device_format(Path::new(path), *force).map(|intent| {
                    if args.json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&intent)
                                .expect("serialize index format intent")
                        );
                    } else {
                        println!("Prepared raw index format intent:");
                        println!("  path: {}", intent.path);
                        println!("  canonical_path: {}", intent.canonical_path);
                        println!("  force: {}", intent.force);
                        println!("  nonce: {}", intent.nonce);
                        println!("  fingerprint: {}", intent.fingerprint);
                        println!("  expires_at_unix: {}", intent.expires_at);
                        println!("Run format command with:");
                        println!(
                            "  --path '{}' --force={} --nonce '{}' --fingerprint '{}'",
                            intent.path, intent.force, intent.nonce, intent.fingerprint
                        );
                    }
                })
            } else {
                let nonce = nonce.as_deref().ok_or_else(|| {
                    crate::enums::app_error_enum::AppError::BadRequest(
                        "missing --nonce; run format-index-device --prepare first".to_string(),
                    )
                });
                let fingerprint = fingerprint.as_deref().ok_or_else(|| {
                    crate::enums::app_error_enum::AppError::BadRequest(
                        "missing --fingerprint; run format-index-device --prepare first"
                            .to_string(),
                    )
                });
                match (nonce, fingerprint) {
                    (Ok(nonce), Ok(fingerprint)) => format_raw_index_device_with_intent(
                        Path::new(path),
                        *force,
                        nonce,
                        fingerprint,
                    )
                    .map(|status| {
                        if args.json {
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&status)
                                    .expect("serialize index device status")
                            );
                        } else {
                            println!("Formatted raw index device:");
                            println!("  path: {}", status.path);
                            println!("  canonical_path: {}", status.canonical_path);
                            println!("  status: {}", status.status);
                            println!(
                                "  device_id: {}",
                                status.device_id.unwrap_or_else(|| "<none>".to_string())
                            );
                            println!(
                                "  block_size_bytes: {}",
                                status
                                    .block_size_bytes
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "<unknown>".to_string())
                            );
                            println!(
                                "  total_bytes: {}",
                                status
                                    .total_bytes
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "<unknown>".to_string())
                            );
                            println!(
                                "  available_bytes: {}",
                                status
                                    .available_bytes
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "<unknown>".to_string())
                            );
                            if let Some(warning) = status.path_warning {
                                println!("  warning: {}", warning);
                            }
                        }
                    }),
                    (Err(err), _) | (_, Err(err)) => Err(err),
                }
            };

            drop(_guard);

            match result {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Command failed: {}", e);
                    std::process::exit(1);
                }
            }

            return;
        }
        Some(cmd) => {
            dotenv().ok();

            let target_url = args.url.or_else(|| {
                Ini::load_from_file(CONFIG_FILE).ok().and_then(|conf| {
                    conf.section(Some("server")).map(|section| {
                        let host = section.get("host").unwrap_or("127.0.0.1");
                        let port = section.get("https_port").unwrap_or("50001");
                        let effective_host = if host == "0.0.0.0" { "127.0.0.1" } else { host };
                        format!("https://{}:{}", effective_host, port)
                    })
                })
            });

            let result = execute_command(
                cmd,
                args.secret.clone(),
                args.auth_fragment.clone(),
                args.fragment.clone(),
                target_url,
                args.insecure,
                args.poll_ms,
                args.json,
            )
            .await;

            drop(_guard);

            if let Err(e) = result {
                eprintln!("Command failed: {}", e);
                std::process::exit(1);
            }

            return;
        }
    }

    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    tracing::info!("╔══════════════════════════════════════╗");
    tracing::info!("║         L I T I A I N A  N 1         ║");
    tracing::info!("╚══════════════════════════════════════╝");
    tracing::info!("│ Version        : {}", env!("CARGO_PKG_VERSION"));
    tracing::info!("│ Status         : ONLINE");
    tracing::info!("│ Start Time     : {}", current_time);
    tracing::info!("│ PID            : {}", std::process::id());
    tracing::info!(
        "│ Hostname       : {}",
        hostname::get().unwrap_or_default().to_string_lossy()
    );
    tracing::info!(
        "│ Working Dir    : {}",
        std::env::current_dir().unwrap_or_default().display()
    );
    tracing::info!("╞═════════ ACTIVE CONNECTIONS ═════════╡");

    run_server().await
}

pub async fn run_server() {
    let config =
        Ini::load_from_file(CONFIG_FILE).expect(format!("Failed to load {}", CONFIG_FILE).as_str());

    let server_section = config.section(Some("server")).unwrap_or_else(|| {
        panic!("Missing [server] section in {}", CONFIG_FILE);
    });

    let host = server_section.get("host").unwrap_or("127.0.0.1");

    let ip: IpAddr = IpAddr::from_str(host).unwrap_or_else(|_| {
        panic!("Invalid host IP address in config.ini: {}", host);
    });

    let https_port = server_section
        .get("https_port")
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(50001);

    let cert_path = server_section
        .get("cert_path")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "cert.pem".to_string());

    let key_path = server_section
        .get("key_path")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "key.pem".to_string());

    let disk_section = config.section(Some("disk_config"));

    let fs_storage_path = disk_section
        .and_then(|s| s.get("fs_storage_path"))
        .unwrap_or("fs-storage-path");

    let index_store_str = disk_section
        .and_then(|s| s.get("index_store_disks"))
        .expect("Missing index_store_disks in [disk_config]");

    let index_disks: Vec<String> = index_store_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if index_disks.is_empty() {
        panic!("At least one index store disk must be configured in index_store_disks!");
    }

    let blob_store_str = disk_section
        .and_then(|s| s.get("blob_store_disks"))
        .expect("Missing blob_store_disks in [disk_config]");

    let blob_disks: Vec<String> = blob_store_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if blob_disks.is_empty() {
        panic!("At least one blob store disk must be configured in blob_store_disks!");
    }
    initialize_storage_roots(
        index_disks.clone(),
        blob_disks.clone(),
        fs_storage_path.to_string(),
    )
    .expect("storage root initialization");
    let raw_blob_statuses =
        validate_configured_raw_blob_disks().expect("raw blob device validation");
    for status in &raw_blob_statuses {
        if let Some(warning) = status.path_warning.as_deref() {
            tracing::warn!("blob disk {}: {}", status.path, warning);
        }
        tracing::info!(
            "Blob disk {} kind={:?} status={} mounted={} total_bytes={} signature={}",
            status.path,
            status.kind,
            status.status,
            status.mounted,
            status.total_bytes.unwrap_or(0),
            status.detected_signature.as_deref().unwrap_or("none")
        );
    }
    let blob_disk_count = blob_disks.len();
    let blob_device_count = detect_unique_storage_devices(&blob_disks);
    let effective_blob_disks_for_auto = blob_device_count.min(blob_disk_count).max(1);
    if blob_device_count < blob_disk_count {
        tracing::warn!(
            "blob_store_disks has {} configured path(s) but only {} unique physical device(s); auto tuning will use {} effective disk(s)",
            blob_disk_count,
            blob_device_count,
            effective_blob_disks_for_auto
        );
    }

    let upload_section = config.section(Some("upload_config"));
    let env_index_backend_mode = std::env::var("N1_ARC_INDEX_BACKEND_MODE").ok();
    let index_backend_mode = parse_index_backend_mode(
        env_index_backend_mode
            .as_deref()
            .or_else(|| upload_section.and_then(|s| s.get("index_backend_mode"))),
    );
    set_index_backend_mode(index_backend_mode);
    tracing::info!(
        "Arc index backend mode initialized: {:?} (env_override={})",
        index_backend_mode,
        env_index_backend_mode.is_some()
    );
    let cpu_cores = detect_cpu_cores();
    let detected_ram_bytes = detect_total_ram_bytes();
    let detected_ram_gib = ram_gib(detected_ram_bytes);

    let data_shards_value = upload_section
        .and_then(|s| s.get("data_shards"))
        .unwrap_or("1")
        .parse::<usize>()
        .expect("data_shards does not fit into usize");

    let parity_shards_value = upload_section
        .and_then(|s| s.get("parity_shards"))
        .unwrap_or("0")
        .parse::<usize>()
        .expect("parity_shards does not fit into usize");
    let total_shards_value = data_shards_value.saturating_add(parity_shards_value).max(1);

    let stripe_block_size_value = upload_section
        .and_then(|s| s.get("stripe_block_size"))
        .unwrap_or("262144")
        .parse::<usize>()
        .expect("stripe_block_size does not fit into usize");

    let writer_buffer_raw = upload_section.and_then(|s| s.get("writer_buffer_size"));
    let mut writer_buffer_size = align_up_4k(parse_usize_or_auto(
        writer_buffer_raw,
        "writer_buffer_size",
        adaptive_writer_buffer_size(
            cpu_cores,
            detected_ram_bytes,
            effective_blob_disks_for_auto,
            total_shards_value,
        ),
    ));
    if is_auto_or_empty(writer_buffer_raw) {
        writer_buffer_size = writer_buffer_size.max(stripe_block_size_value.max(4096));
    }

    let configured_max_io = parse_usize_or_auto(
        upload_section.and_then(|s| s.get("max_concurrent_io")),
        "max_concurrent_io",
        adaptive_max_concurrent_io(cpu_cores, effective_blob_disks_for_auto, detected_ram_bytes),
    );

    let raw_writer_inflight_limit = adaptive_raw_writer_inflight_limit(
        cpu_cores,
        detected_ram_bytes,
        effective_blob_disks_for_auto,
        configured_max_io,
        writer_buffer_size,
        total_shards_value,
    );

    let upload_memory_budget_bytes = parse_usize_or_auto(
        upload_section.and_then(|s| s.get("upload_memory_budget_bytes")),
        "upload_memory_budget_bytes",
        adaptive_upload_memory_budget_bytes(
            cpu_cores,
            detected_ram_bytes,
            effective_blob_disks_for_auto,
            total_shards_value,
        ),
    )
    .max(8 * 1024 * 1024);

    let per_upload_bytes = estimate_upload_part_working_set_bytes(
        data_shards_value,
        parity_shards_value,
        writer_buffer_size,
        raw_writer_inflight_limit,
        stripe_block_size_value,
    );

    let effective_upload_budget_bytes = upload_memory_budget_bytes
        .saturating_mul(80)
        .saturating_div(100);
    let budget_limited_max_io = (effective_upload_budget_bytes / per_upload_bytes).max(1);
    let max_io_semaphore = configured_max_io.min(budget_limited_max_io);

    MAX_IO_SEMAPHORE
        .set(max_io_semaphore)
        .expect("Double initialization of MAX_IO_SEMAPHORE");
    ADAPTIVE_IO_TARGET_ATOMIC
        .set(AtomicUsize::new(max_io_semaphore))
        .expect("Double initialization of ADAPTIVE_IO_TARGET_ATOMIC");

    let max_part_size = upload_section
        .and_then(|s| s.get("max_part_size"))
        .unwrap_or("52428800");

    let max_part_size: AtomicU64 = AtomicU64::new(
        max_part_size
            .parse::<u64>()
            .expect("max_part_size does not fit into u64"),
    );

    MAX_PART_SIZE_ATOMIC
        .set(max_part_size)
        .expect("Double initialization of MAX_PART_SIZE!");

    DATA_SHARDS_ATOMIC
        .set(AtomicUsize::new(data_shards_value))
        .expect("Double initialization of DATA_SHARDS!");

    PARITY_SHARDS_ATOMIC
        .set(AtomicUsize::new(parity_shards_value))
        .expect("Double initialization of PARITY_SHARDS!");

    STRIPE_SIZE_ATOMIC
        .set(AtomicUsize::new(stripe_block_size_value))
        .expect("Double initialization of STRIPE_SIZE!");

    let upload_part_strict_durability_default = parse_bool_or_default(
        upload_section.and_then(|s| s.get("upload_part_strict_durability_default")),
        false,
    );
    let upload_io_gate_enabled = parse_bool_or_default(
        upload_section.and_then(|s| s.get("upload_io_gate_enabled")),
        true,
    );
    let raw_verify_on_read = parse_bool_or_default(
        upload_section.and_then(|s| s.get("raw_verify_on_read")),
        false,
    );

    tracing::info!(
        "Upload tuning: cpu_cores={}, ram_gib={}, blob_paths={}, blob_devices={}, auto_effective_disks={}, ec={}D+{}P, max_concurrent_io={} (configured={} budget_cap={} per_upload_est={}KiB budget={}MiB effective_budget={}MiB), writer_buffer_size={}, raw_writer_inflight_limit={}, part_durability_strict_default={}, io_gate_enabled={}, raw_verify_on_read={}",
        cpu_cores,
        detected_ram_gib,
        blob_disk_count,
        blob_device_count,
        effective_blob_disks_for_auto,
        data_shards_value,
        parity_shards_value,
        max_io_semaphore,
        configured_max_io,
        budget_limited_max_io,
        per_upload_bytes / 1024,
        upload_memory_budget_bytes / (1024 * 1024),
        effective_upload_budget_bytes / (1024 * 1024),
        writer_buffer_size,
        raw_writer_inflight_limit,
        upload_part_strict_durability_default,
        upload_io_gate_enabled,
        raw_verify_on_read
    );

    let writer_buffer_size: AtomicUsize = AtomicUsize::new(writer_buffer_size);

    WRITER_BUFFER_SIZE_ATOMIC
        .set(writer_buffer_size)
        .expect("Double initialization of WRITER_BUFFER_SIZE!");

    RAW_WRITER_INFLIGHT_LIMIT_ATOMIC
        .set(AtomicUsize::new(raw_writer_inflight_limit))
        .expect("Double initialization of RAW_WRITER_INFLIGHT_LIMIT_ATOMIC");

    UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC
        .set(AtomicBool::new(upload_part_strict_durability_default))
        .expect("Double initialization of UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC");

    UPLOAD_IO_GATE_ENABLED_ATOMIC
        .set(AtomicBool::new(upload_io_gate_enabled))
        .expect("Double initialization of UPLOAD_IO_GATE_ENABLED_ATOMIC");
    RAW_VERIFY_ON_READ_ATOMIC
        .set(AtomicBool::new(raw_verify_on_read))
        .expect("Double initialization of RAW_VERIFY_ON_READ_ATOMIC");

    let stream_chunk_size = align_up_4k(parse_usize_or_auto(
        upload_section.and_then(|s| s.get("stream_chunk_size")),
        "stream_chunk_size",
        adaptive_stream_chunk_size(cpu_cores, detected_ram_bytes),
    ))
    .clamp(4096, 4 * 1024 * 1024);

    let stream_readahead_bytes = align_up_4k(parse_usize_or_auto(
        upload_section.and_then(|s| s.get("stream_readahead_bytes")),
        "stream_readahead_bytes",
        adaptive_stream_readahead_bytes(stream_chunk_size, detected_ram_bytes),
    ))
    .clamp(64 * 1024, 8 * 1024 * 1024);

    let stream_max_concurrency = parse_usize_or_auto(
        upload_section.and_then(|s| s.get("stream_max_concurrency")),
        "stream_max_concurrency",
        adaptive_stream_max_concurrency(
            cpu_cores,
            effective_blob_disks_for_auto,
            detected_ram_bytes,
            stream_chunk_size,
            stream_readahead_bytes,
        ),
    );

    STREAM_MAX_CONCURRENCY
        .set(stream_max_concurrency)
        .expect("Double initialization of STREAM_MAX_CONCURRENCY");
    ADAPTIVE_STREAM_TARGET_ATOMIC
        .set(AtomicUsize::new(stream_max_concurrency))
        .expect("Double initialization of ADAPTIVE_STREAM_TARGET_ATOMIC");

    STREAM_CHUNK_SIZE_ATOMIC
        .set(AtomicUsize::new(stream_chunk_size))
        .expect("Double initialization of STREAM_CHUNK_SIZE_ATOMIC");
    STREAM_READAHEAD_BYTES_ATOMIC
        .set(AtomicUsize::new(stream_readahead_bytes))
        .expect("Double initialization of STREAM_READAHEAD_BYTES_ATOMIC");

    let stream_locality_bias_enabled = parse_bool_or_default(
        upload_section.and_then(|s| s.get("stream_locality_bias_enabled")),
        true,
    );
    STREAM_LOCALITY_BIAS_ENABLED_ATOMIC
        .set(AtomicBool::new(stream_locality_bias_enabled))
        .expect("Double initialization of STREAM_LOCALITY_BIAS_ENABLED_ATOMIC");

    let stream_per_request_kib = (stream_chunk_size
        .saturating_add(stream_readahead_bytes)
        .saturating_add(64 * 1024))
        / 1024;
    let stream_auto_budget_mib =
        adaptive_stream_memory_budget_bytes(detected_ram_bytes) / (1024 * 1024);
    tracing::info!(
        "Stream tuning: max_concurrency={}, chunk_size={}, readahead_bytes={}, per_request_est={}KiB, auto_budget={}MiB, locality_bias={}",
        stream_max_concurrency,
        stream_chunk_size,
        stream_readahead_bytes,
        stream_per_request_kib,
        stream_auto_budget_mib,
        stream_locality_bias_enabled
    );

    let adaptive_concurrency_enabled = parse_bool_or_default(
        upload_section.and_then(|s| s.get("adaptive_concurrency_enabled")),
        true,
    );
    ADAPTIVE_CONCURRENCY_ENABLED_ATOMIC
        .set(AtomicBool::new(adaptive_concurrency_enabled))
        .expect("Double initialization of ADAPTIVE_CONCURRENCY_ENABLED_ATOMIC");

    let adaptive_concurrency_interval_ms = parse_usize_or_auto(
        upload_section.and_then(|s| s.get("adaptive_concurrency_interval_ms")),
        "adaptive_concurrency_interval_ms",
        adaptive_concurrency_interval_ms(cpu_cores, detected_ram_bytes),
    )
    .clamp(100, 10_000);
    ADAPTIVE_CONCURRENCY_INTERVAL_MS_ATOMIC
        .set(AtomicU64::new(adaptive_concurrency_interval_ms as u64))
        .expect("Double initialization of ADAPTIVE_CONCURRENCY_INTERVAL_MS_ATOMIC");

    tracing::info!(
        "Adaptive concurrency: enabled={} interval_ms={}",
        adaptive_concurrency_enabled,
        adaptive_concurrency_interval_ms
    );

    let monitor_interval_seconds = parse_usize_or_auto(
        upload_section.and_then(|s| s.get("monitor_interval_seconds")),
        "monitor_interval_seconds",
        adaptive_monitor_interval_seconds(cpu_cores, detected_ram_bytes),
    )
    .clamp(1, 3600);
    MONITOR_INTERVAL_SECS_ATOMIC
        .set(AtomicU64::new(monitor_interval_seconds as u64))
        .expect("Double initialization of MONITOR_INTERVAL_SECS_ATOMIC");

    let monitor_deep_scan_interval_seconds = parse_usize_or_auto(
        upload_section.and_then(|s| s.get("monitor_deep_scan_interval_seconds")),
        "monitor_deep_scan_interval_seconds",
        adaptive_monitor_deep_scan_interval_seconds(cpu_cores, detected_ram_bytes),
    )
    .clamp(5, 24 * 3600);
    MONITOR_DEEP_SCAN_INTERVAL_SECS_ATOMIC
        .set(AtomicU64::new(monitor_deep_scan_interval_seconds as u64))
        .expect("Double initialization of MONITOR_DEEP_SCAN_INTERVAL_SECS_ATOMIC");

    tracing::info!(
        "Monitoring tuning: interval={}s deep_scan_interval={}s",
        monitor_interval_seconds,
        monitor_deep_scan_interval_seconds
    );

    let cert = RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("Failed to load TLS certificates");

    let storage_addr = SocketAddr::from((ip, https_port));
    let storage_router = serve_storage_routes().await;
    let serve_storage = storage_router.fallback(handler_404);

    let mut smb_config = crate::smb::config::SmbConfig::default();
    if let Some(smb_section) = config
        .section(Some("smb"))
        .or_else(|| config.section(Some("fsgwd")))
    {
        if let Some(v) = smb_section.get("enabled") {
            smb_config.enabled = parse_bool_or_default(Some(v), smb_config.enabled);
        }
        if let Some(v) = smb_section.get("bind_addr") {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                smb_config.bind_addr = trimmed.to_string();
            }
        }
        if let Some(v) = smb_section
            .get("smb_port")
            .and_then(|v| v.parse::<u16>().ok())
        {
            smb_config.smb_port = v;
        }
        if let Some(v) = smb_section
            .get("max_sessions")
            .and_then(|v| v.parse::<usize>().ok())
        {
            smb_config.max_sessions = v.clamp(64, 65_536);
        }
        if let Some(v) = smb_section
            .get("max_frame_bytes")
            .and_then(|v| v.parse::<usize>().ok())
        {
            smb_config.max_frame_bytes = v.clamp(4 * 1024, 64 * 1024 * 1024);
        }
        if let Some(v) = smb_section.get("async_close_commit") {
            smb_config.async_close_commit =
                parse_bool_or_default(Some(v), smb_config.async_close_commit);
        }
        if let Some(v) = smb_section
            .get("async_close_threshold_bytes")
            .and_then(|v| v.parse::<u64>().ok())
        {
            smb_config.async_close_threshold_bytes =
                v.clamp(1 * 1024 * 1024, 1024 * 1024 * 1024 * 1024);
        }
        if let Some(v) = smb_section.get("require_signing") {
            smb_config.require_signing = parse_bool_or_default(Some(v), smb_config.require_signing);
        }
        if let Some(v) = smb_section
            .get("query_directory_page_limit")
            .and_then(|v| v.parse::<usize>().ok())
        {
            smb_config.query_directory_page_limit =
                crate::smb::config::clamp_query_directory_page_limit(v);
        }
        if let Some(v) = smb_section
            .get("query_directory_max_buffered_entries")
            .and_then(|v| v.parse::<usize>().ok())
        {
            smb_config.query_directory_max_buffered_entries =
                crate::smb::config::clamp_query_directory_max_buffered_entries(v);
        }
        if let Some(v) = smb_section
            .get("query_directory_max_seeded_entries")
            .and_then(|v| v.parse::<usize>().ok())
        {
            smb_config.query_directory_max_seeded_entries =
                crate::smb::config::clamp_query_directory_max_seeded_entries(
                    v,
                    smb_config.query_directory_max_buffered_entries,
                );
        }
    }

    let mut _smb_join = None;
    match crate::smb::start(smb_config).await {
        Ok(Some(runtime)) => {
            tracing::info!("smb listener online at {}", runtime.addr);
            _smb_join = Some(runtime.join);
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!("failed to start smb listener: {}", e);
        }
    }

    serve(storage_addr, serve_storage, cert).await;
}

async fn serve(socket: SocketAddr, app: Router, cert: RustlsConfig) {
    tracing::info!("Secured port => {} type: https", socket);
    let handle = Arc::new(axum_server::Handle::new());

    tokio::spawn(shutdown_signal(handle.clone()));

    let mut server = axum_server::bind_rustls(socket, cert).handle((*handle).clone());
    server.http_builder().http2().enable_connect_protocol();

    server
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();
}

pub async fn shutdown_signal(handle: Arc<axum_server::Handle<SocketAddr>>) {
    #[cfg(unix)]
    let terminator = async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut interrupt =
            signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");

        tokio::select! {
            _ = terminate.recv() => {},
            _ = interrupt.recv() => {},
        }
    };

    #[cfg(not(unix))]
    let terminator = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c event");
    };

    terminator.await;

    tracing::info!("Shutdown signal received. Stopping server...");
    handle.graceful_shutdown(Some(Duration::from_secs(10)));
}
