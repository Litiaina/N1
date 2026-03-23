pub use n1::{
    enums, fs, fuse, handlers, middleware, core, smb, statics, structs, utils, websocket,
};

use crate::fs::cleanup_manager::interval_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::handlers::error_handlers::handler_404;
use crate::core::adaptive_concurrency::{
    ADAPTIVE_CONCURRENCY_ENABLED_ATOMIC, ADAPTIVE_CONCURRENCY_INTERVAL_MS_ATOMIC,
};
use crate::core::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
use crate::core::monitoring::{
    MONITOR_DEEP_SCAN_INTERVAL_SECS_ATOMIC, MONITOR_INTERVAL_SECS_ATOMIC,
};
use crate::core::object_parser::{
    ADAPTIVE_STREAM_TARGET_ATOMIC, STREAM_CHUNK_SIZE_ATOMIC, STREAM_LOCALITY_BIAS_ENABLED_ATOMIC,
    STREAM_MAX_CONCURRENCY, STREAM_READAHEAD_BYTES_ATOMIC,
};
use crate::core::upload::{
    ADAPTIVE_IO_TARGET_ATOMIC, DATA_SHARDS_ATOMIC, MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC,
    PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC, UPLOAD_IO_GATE_ENABLED_ATOMIC,
    UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC, WRITER_BUFFER_SIZE_ATOMIC,
};
use crate::distribution_core::config::{n2_runtime_config, resolve_effective_cross_params};
use crate::distribution_core::data_plane::schedule_n2_auto_maintenance;
use crate::distribution_core::governance::{governance_state_view, initialize_governance_runtime};
use crate::distribution_core::state::{get_cluster_view, get_membership};
use crate::routes::storage_routes::serve_storage_routes;
use crate::settings::constants::{CONFIG_FILE, ENV_FILE};
use crate::utils::date_time_formatter::LocalTimer;
use crate::validation::enforce_init_settings::{ensure_config_file, ensure_env_file};
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use chrono::Local;
use ini::Ini;
use n1::settings::constants::{
    CONFIG_FILE as STANDARD_CONFIG_FILE, ENV_FILE as STANDARD_ENV_FILE,
};
use std::collections::HashSet;
use std::fs as stdfs;
use std::fs::OpenOptions;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::time::Duration;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

mod distribution_core;
mod routes;
mod settings;
mod validation;
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
    let mut bytes = match ram_gib(ram_bytes) {
        0..=4 => 256 * 1024,
        5..=8 => 512 * 1024,
        9..=64 => 1024 * 1024,
        _ => 2 * 1024 * 1024,
    };

    if cpu_cores <= 2 {
        bytes = bytes.min(256 * 1024);
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
        bytes = bytes.min(256 * 1024);
    } else if blob_disks == 2 {
        bytes = bytes.min(512 * 1024);
    }

    align_up_4k(bytes.clamp(64 * 1024, 4 * 1024 * 1024))
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
        0..=4 => 48usize * 1024 * 1024,
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

    budget.clamp(48usize * 1024 * 1024, 384usize * 1024 * 1024)
}

fn estimate_upload_part_working_set_bytes(
    data_shards: usize,
    parity_shards: usize,
    writer_buffer_size: usize,
    stripe_block_size: usize,
) -> usize {
    let total_shards = data_shards.saturating_add(parity_shards).max(1);
    let writer_bytes = total_shards.saturating_mul(writer_buffer_size.max(4096));
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

fn normalize_fs_backed_disk_paths(role: &str, paths: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::with_capacity(paths.len());
    for raw in paths {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }

        let candidate = PathBuf::from(trimmed);
        if !candidate.exists() {
            std::fs::create_dir_all(&candidate).unwrap_or_else(|e| {
                panic!("Failed to create {} directory '{}': {}", role, trimmed, e)
            });
        }

        let meta = std::fs::symlink_metadata(&candidate)
            .unwrap_or_else(|e| panic!("Failed to inspect {} path '{}': {}", role, trimmed, e));
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            let ft = meta.file_type();
            if ft.is_block_device() || ft.is_char_device() {
                panic!(
                    "{} entry '{}' is a device. The standard variant is filesystem-backed and requires directory paths.",
                    role, trimmed
                );
            }
        }
        if !meta.is_dir() {
            panic!(
                "{} entry '{}' is not a directory. The standard variant is filesystem-backed and requires directory paths.",
                role, trimmed
            );
        }

        let resolved = std::fs::canonicalize(&candidate).unwrap_or(candidate);
        normalized.push(resolved.to_string_lossy().to_string());
    }

    if normalized.is_empty() {
        panic!(
            "At least one valid directory must be configured in {}",
            role
        );
    }
    normalized
}

#[cfg(unix)]
fn detect_unique_storage_devices(paths: &[String]) -> usize {
    use std::os::unix::fs::MetadataExt;

    let mut dev_ids: HashSet<u64> = HashSet::new();
    let mut fallback_paths: HashSet<PathBuf> = HashSet::new();

    for raw in paths {
        let p = PathBuf::from(raw);
        let resolved = std::fs::canonicalize(&p).unwrap_or(p);
        fallback_paths.insert(resolved.clone());

        if let Ok(meta) = std::fs::metadata(&resolved) {
            dev_ids.insert(meta.dev());
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
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,litiaina_n1=trace,axum=info,hyper=info".into()),
        )
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

fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

fn parse_https_url(raw: &str, label: &str) -> Result<reqwest::Url, String> {
    let parsed = reqwest::Url::parse(raw)
        .map_err(|e| format!("{label} '{}' is not a valid URL: {}", raw, e))?;
    if parsed.scheme() != "https" {
        return Err(format!("{label} '{}' must use https", raw));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| format!("{label} '{}' is missing a host", raw))?;
    if host.trim().is_empty() {
        return Err(format!("{label} '{}' is missing a usable host", raw));
    }
    if parsed.port_or_known_default().is_none() {
        return Err(format!("{label} '{}' is missing a port", raw));
    }
    Ok(parsed)
}

fn ensure_standard_compat_alias(source: &Path, alias: &Path, label: &str) -> Result<(), String> {
    if alias.exists() {
        return Ok(());
    }
    if !source.exists() {
        return Err(format!("{label} source '{}' is missing", source.display()));
    }
    if let Some(parent) = alias.parent() {
        stdfs::create_dir_all(parent).map_err(|err| {
            format!(
                "{label} parent '{}' could not be created: {}",
                parent.display(),
                err
            )
        })?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        match symlink(source, alias) {
            Ok(_) => return Ok(()),
            Err(err) => {
                tracing::warn!(
                    "N2 compatibility guard could not create symlink '{}' -> '{}': {}. Falling back to a copy.",
                    alias.display(),
                    source.display(),
                    err
                );
            }
        }
    }

    stdfs::copy(source, alias).map_err(|err| {
        format!(
            "{label} '{}' could not be created from '{}': {}",
            alias.display(),
            source.display(),
            err
        )
    })?;
    Ok(())
}

fn ensure_standard_compat_files() -> Result<(), String> {
    ensure_standard_compat_alias(
        Path::new(ENV_FILE),
        Path::new(STANDARD_ENV_FILE),
        "N2 env compatibility file",
    )?;
    ensure_standard_compat_alias(
        Path::new(CONFIG_FILE),
        Path::new(STANDARD_CONFIG_FILE),
        "N2 config compatibility file",
    )?;
    Ok(())
}

async fn validate_n2_startup_layout(config: &Ini, local_blob_roots: usize) {
    let cfg = n2_runtime_config();
    if !cfg.membership_file.exists() {
        panic!(
            "N2 startup guard requires a membership file at '{}'. Multi-node N2 will not auto-invent membership.",
            cfg.membership_file.display()
        );
    }
    ensure_readable_file(cfg.membership_file.as_path(), "N2 membership file").unwrap_or_else(|e| {
        panic!(
            "N2 startup guard rejected membership file '{}': {}",
            cfg.membership_file.display(),
            e
        )
    });
    if cfg.internal_token.trim().is_empty()
        || cfg.internal_token.trim() == "change-me-n2-internal-token"
        || cfg.internal_token.trim().len() < 24
    {
        panic!(
            "N2 startup guard requires a non-placeholder [n2].internal_token with at least 24 characters"
        );
    }
    let local_endpoint = parse_https_url(&cfg.node_endpoint, "N2 node_endpoint")
        .unwrap_or_else(|e| panic!("N2 startup guard rejected node_id='{}': {}", cfg.node_id, e));
    let cluster_view = get_cluster_view(None).await.unwrap_or_else(|e| {
        panic!(
            "N2 startup guard failed to load cluster membership from '{}': {}",
            cfg.membership_file.display(),
            e
        )
    });

    if !cluster_view.write_ready {
        let reason = if cluster_view.reasons.is_empty() {
            "cluster view is not write-ready".to_string()
        } else {
            cluster_view.reasons.join("; ")
        };
        panic!(
            "N2 startup guard rejected node_id='{}' endpoint='{}': {}",
            cfg.node_id, cfg.node_endpoint, reason
        );
    }

    let membership = get_membership().await.unwrap_or_else(|e| {
        panic!(
            "N2 startup guard failed to read membership from '{}': {}",
            cfg.membership_file.display(),
            e
        )
    });

    let server_section = config
        .section(Some("server"))
        .unwrap_or_else(|| panic!("Missing [server] section in {}", CONFIG_FILE));
    let bind_host = server_section.get("host").unwrap_or("127.0.0.1").trim();
    if cluster_view.active_nodes > 1 && is_loopback_host(bind_host) {
        panic!(
            "N2 startup guard rejected [server].host='{}' for a {}-node cluster. Multi-node N2 must not bind only to loopback.",
            bind_host, cluster_view.active_nodes
        );
    }

    let membership_local = membership
        .nodes
        .iter()
        .find(|node| node.node_id == cfg.node_id)
        .unwrap_or_else(|| {
            panic!(
                "N2 startup guard could not find local node_id='{}' in membership",
                cfg.node_id
            )
        });

    let membership_local_endpoint =
        parse_https_url(&membership_local.endpoint, "membership local endpoint").unwrap_or_else(
            |e| {
                panic!(
                    "N2 startup guard rejected membership entry for node_id='{}': {}",
                    cfg.node_id, e
                )
            },
        );

    if membership_local_endpoint.as_str().trim_end_matches('/')
        != local_endpoint.as_str().trim_end_matches('/')
    {
        panic!(
            "N2 startup guard rejected node_id='{}': [n2].node_endpoint '{}' does not match membership endpoint '{}'",
            cfg.node_id, cfg.node_endpoint, membership_local.endpoint
        );
    }

    if cluster_view.active_nodes > 1 && is_loopback_host(local_endpoint.host_str().unwrap_or("")) {
        panic!(
            "N2 startup guard rejected [n2].node_endpoint='{}' for a multi-node cluster. Use a routable LAN/WAN address, not loopback.",
            cfg.node_endpoint
        );
    }

    let mut normalized_endpoints = HashSet::new();
    for node in membership.nodes.iter().filter(|node| node.active) {
        let endpoint = parse_https_url(&node.endpoint, "active membership endpoint")
            .unwrap_or_else(|e| {
                panic!(
                    "N2 startup guard rejected active node '{}': {}",
                    node.node_id, e
                )
            });
        let normalized = endpoint.as_str().trim_end_matches('/').to_ascii_lowercase();
        if !normalized_endpoints.insert(normalized) {
            panic!(
                "N2 startup guard rejected membership: duplicate active endpoint '{}'",
                node.endpoint
            );
        }
        if cluster_view.active_nodes > 1 && is_loopback_host(endpoint.host_str().unwrap_or("")) {
            panic!(
                "N2 startup guard rejected active node '{}': endpoint '{}' is loopback-only in a multi-node cluster",
                node.node_id, node.endpoint
            );
        }
    }

    let (resolved, source) =
        resolve_effective_cross_params(cluster_view.active_nodes, local_blob_roots).unwrap_or_else(
            |e| {
                panic!(
                    "N2 startup guard rejected cluster layout for node_id='{}': active_nodes={} local_blob_roots={} error={}",
                    cfg.node_id, cluster_view.active_nodes, local_blob_roots, e
                )
            },
        );

    tracing::info!(
        "N2 layout: source={} membership_epoch={} active_nodes={} resolved kc={} mc={} kd={} md={} commit_mode={} auto_maintenance_mode={}",
        source,
        cluster_view.membership_epoch,
        cluster_view.active_nodes,
        resolved.kc,
        resolved.mc,
        resolved.kd,
        resolved.md,
        cfg.cluster_commit_mode.as_str(),
        cfg.auto_maintenance_mode.as_str()
    );

    if source == "configured" && cluster_view.active_nodes > 1 && resolved.mc == 0 {
        tracing::warn!(
            "N2 cluster is running with mc=0 across {} active node(s); one node loss can make clustered objects unavailable",
            cluster_view.active_nodes
        );
    }
    if cluster_view.active_nodes > 1
        && resolved.mc > 0
        && matches!(
            cfg.cluster_commit_mode,
            crate::distribution_core::model::ClusterCommitMode::Quorum
        )
    {
        tracing::warn!(
            "N2 cluster commit mode is quorum while mc={} across {} active node(s); successful writes may land with fewer immediately committed copies than kc+mc. Set [n2].cluster_commit_mode=strict_full_placement if you want hard placement guardrails.",
            resolved.mc,
            cluster_view.active_nodes
        );
    }
    if source == "configured" && local_blob_roots > 1 && resolved.md == 0 {
        tracing::warn!(
            "N2 local shard storage is running with md=0 across {} blob root(s); one local disk loss can make a cluster shard unavailable",
            local_blob_roots
        );
    }
}

fn warn_if_running_as_root(binary: &str) {
    #[cfg(unix)]
    unsafe {
        if libc::geteuid() == 0 {
            eprintln!(
                "warning: {binary} is running as root. This is allowed, but a non-root service user is preferred to avoid root-owned runtime state."
            );
        }
    }
}

fn probe_directory_writable(dir: &Path, label: &str) -> Result<(), String> {
    let probe_name = format!(
        ".n2-write-check-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let probe_path = dir.join(probe_name);
    stdfs::write(&probe_path, b"ok")
        .map_err(|err| format!("{label} '{}' is not writable: {}", dir.display(), err))?;
    let _ = stdfs::remove_file(&probe_path);
    Ok(())
}

fn ensure_readable_file(path: &Path, label: &str) -> Result<(), String> {
    let meta = stdfs::metadata(path)
        .map_err(|err| format!("{label} '{}' is not accessible: {}", path.display(), err))?;
    if !meta.is_file() {
        return Err(format!("{label} '{}' is not a file", path.display()));
    }
    OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|err| format!("{label} '{}' is not readable: {}", path.display(), err))?;
    Ok(())
}

fn ensure_existing_writable_dir(path: &Path, label: &str) -> Result<(), String> {
    let meta = stdfs::metadata(path)
        .map_err(|err| format!("{label} '{}' is not accessible: {}", path.display(), err))?;
    if !meta.is_dir() {
        return Err(format!("{label} '{}' is not a directory", path.display()));
    }
    probe_directory_writable(path, label)
}

fn ensure_generated_file_target(path: &Path, label: &str) -> Result<(), String> {
    if path.exists() {
        return ensure_readable_file(path, label);
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    stdfs::create_dir_all(parent).map_err(|err| {
        format!(
            "{label} parent '{}' could not be created: {}",
            parent.display(),
            err
        )
    })?;
    probe_directory_writable(parent, &format!("{label} parent"))
}

fn validate_bootstrap_targets() -> Result<(), String> {
    ensure_generated_file_target(Path::new(ENV_FILE), "N2 env file")?;
    ensure_generated_file_target(Path::new(CONFIG_FILE), "N2 config file")?;
    Ok(())
}

fn validate_runtime_paths(config: &Ini) -> Result<(), String> {
    let server_section = config
        .section(Some("server"))
        .ok_or_else(|| format!("Missing [server] section in {}", CONFIG_FILE))?;
    let cert_path = PathBuf::from(server_section.get("cert_path").unwrap_or("cert.pem"));
    let key_path = PathBuf::from(server_section.get("key_path").unwrap_or("key.pem"));
    ensure_readable_file(&cert_path, "N2 TLS certificate")?;
    ensure_readable_file(&key_path, "N2 TLS key")?;

    let disk_section = config.section(Some("disk_config"));
    let fs_storage_path = PathBuf::from(
        disk_section
            .and_then(|s| s.get("fs_storage_path"))
            .unwrap_or("fs-storage-path"),
    );
    ensure_existing_writable_dir(&fs_storage_path, "N2 fs_storage_path")?;

    let refresh_store_root = std::env::var("N1_REFRESH_STORE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| fs_storage_path.join(".auth").join("refresh_sessions"));
    stdfs::create_dir_all(&refresh_store_root).map_err(|err| {
        format!(
            "N2 refresh session store '{}' could not be created: {}",
            refresh_store_root.display(),
            err
        )
    })?;
    probe_directory_writable(&refresh_store_root, "N2 refresh session store")?;

    let index_store_str = disk_section
        .and_then(|s| s.get("index_store_disks"))
        .ok_or_else(|| "Missing index_store_disks in [disk_config]".to_string())?;
    for root in index_store_str
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        ensure_existing_writable_dir(Path::new(root), "N2 index_store_disks entry")?;
    }

    let blob_store_str = disk_section
        .and_then(|s| s.get("blob_store_disks"))
        .ok_or_else(|| "Missing blob_store_disks in [disk_config]".to_string())?;
    for root in blob_store_str
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        ensure_existing_writable_dir(Path::new(root), "N2 blob_store_disks entry")?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install 'ring' as the default TLS provider.");

    warn_if_running_as_root("litiaina-n2");

    if let Err(e) = validate_bootstrap_targets() {
        eprintln!("N2 startup failed: {}", e);
        return;
    }

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

    if let Err(e) = ensure_standard_compat_files() {
        eprintln!("N2 startup failed: {}", e);
        return;
    }

    let _guard = init_tracing().expect("Failed to initialize tracing subscriber");

    if std::env::args_os().nth(1).is_some() {
        eprintln!("litiaina-n2 runs the server only. Use `litiaina-n2-cli` for admin commands.");
        std::process::exit(2);
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

    if let Err(err) = validate_runtime_paths(&config) {
        panic!("N2 startup validation failed: {}", err);
    }

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

    FS_STORAGE_ROOT
        .set(fs_storage_path.to_string())
        .expect("Double initialization of STORAGE_ROOT!");

    let index_store_str = disk_section
        .and_then(|s| s.get("index_store_disks"))
        .expect("Missing index_store_disks in [disk_config]");

    let index_disks: Vec<String> = index_store_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let index_disks = normalize_fs_backed_disk_paths("index_store_disks", index_disks);

    INDEX_STORE_DISKS
        .set(index_disks)
        .expect("Double initialization of INDEX_STORE_DISKS!");

    let blob_store_str = disk_section
        .and_then(|s| s.get("blob_store_disks"))
        .expect("Missing blob_store_disks in [disk_config]");

    let blob_disks: Vec<String> = blob_store_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let blob_disks = normalize_fs_backed_disk_paths("blob_store_disks", blob_disks);
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

    BLOB_STORE_DISKS
        .set(blob_disks)
        .expect("Double initialization of BLOB_STORE_DISKS!");

    validate_n2_startup_layout(&config, blob_disk_count).await;
    let startup_membership = get_membership().await.unwrap_or_else(|e| {
        panic!(
            "N2 startup could not load membership after validation completed: {}",
            e
        )
    });
    initialize_governance_runtime()
        .await
        .unwrap_or_else(|e| panic!("N2 startup could not initialize governance runtime: {}", e));
    let governance_state = governance_state_view()
        .await
        .unwrap_or_else(|e| panic!("N2 startup could not read governance state: {}", e));
    schedule_n2_auto_maintenance("startup", startup_membership.epoch);

    tracing::info!(
        "N2 governance: term={} commit_index={} applied_membership_epoch={}",
        governance_state.current_term,
        governance_state.commit_index,
        governance_state.last_applied_membership_epoch
    );

    tracing::info!(
        "Storage mode: standard variant is filesystem-backed (directory-only index/blob roots)"
    );

    let upload_section = config.section(Some("upload_config"));
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

    tracing::info!(
        "Upload tuning: cpu_cores={}, ram_gib={}, blob_paths={}, blob_devices={}, auto_effective_disks={}, ec={}D+{}P, max_concurrent_io={} (configured={} budget_cap={} per_upload_est={}KiB budget={}MiB effective_budget={}MiB), writer_buffer_size={}, part_durability_strict_default={}, io_gate_enabled={}",
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
        upload_part_strict_durability_default,
        upload_io_gate_enabled
    );

    let writer_buffer_size: AtomicUsize = AtomicUsize::new(writer_buffer_size);

    WRITER_BUFFER_SIZE_ATOMIC
        .set(writer_buffer_size)
        .expect("Double initialization of WRITER_BUFFER_SIZE!");

    UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC
        .set(AtomicBool::new(upload_part_strict_durability_default))
        .expect("Double initialization of UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC");

    UPLOAD_IO_GATE_ENABLED_ATOMIC
        .set(AtomicBool::new(upload_io_gate_enabled))
        .expect("Double initialization of UPLOAD_IO_GATE_ENABLED_ATOMIC");

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

    let fs_root = FS_STORAGE_ROOT
        .get()
        .cloned()
        .unwrap_or_else(|| STORAGE_ROOT.to_string());
    tokio::spawn(async move {
        interval_cleanup(&fs_root).await;
    });

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
