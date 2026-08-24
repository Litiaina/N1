pub use n2::*;

use crate::core::adaptive_concurrency::{
    ADAPTIVE_CONCURRENCY_ENABLED_ATOMIC, ADAPTIVE_CONCURRENCY_INTERVAL_MS_ATOMIC,
};
use crate::core::disk_manager::{FS_STORAGE_ROOT, STORAGE_DISKS};
use crate::core::io_scheduler::{DiskIoSchedulerConfig, configure_disk_io_scheduler};
use crate::core::monitoring::{
    MONITOR_DEEP_SCAN_INTERVAL_SECS_ATOMIC, MONITOR_INTERVAL_SECS_ATOMIC,
};
use crate::core::object_parser::{
    ADAPTIVE_STREAM_TARGET_ATOMIC, STREAM_CHUNK_SIZE_ATOMIC, STREAM_LOCALITY_BIAS_ENABLED_ATOMIC,
    STREAM_MAX_CONCURRENCY, STREAM_READAHEAD_BYTES_ATOMIC,
};
use crate::core::root_identity::initialize_local_root_identities;
use crate::core::upload::{
    ADAPTIVE_IO_TARGET_ATOMIC, DATA_SHARDS_ATOMIC, MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC,
    PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC, UPLOAD_IO_GATE_ENABLED_ATOMIC,
    UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC, WRITER_BUFFER_SIZE_ATOMIC,
};
use crate::distribution::init_cluster_runtime;
use crate::distribution::membership::parse_storage_roots;
use crate::fs::cleanup_manager::interval_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::handlers::error_handlers::handler_404;
use crate::protocol::{bind_native_protocol, serve_native_protocol};
use crate::routes::storage_routes::{apply_runtime_http_layers, build_storage_routes_unbound};
use crate::settings::constants::{CONFIG_FILE, ENV_FILE};
use crate::utils::date_time_formatter::LocalTimer;
use crate::validation::enforce_init_settings::{ensure_config_file, ensure_env_file};
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use chrono::Local;
use ini::Ini;
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

#[path = "main/runtime_config.rs"]
mod runtime_config;

use runtime_config::*;
pub fn init_tracing() -> Result<WorkerGuard, Box<dyn std::error::Error>> {
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());

    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,litiaina_n2=trace,axum=info,hyper=info".into()),
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
            .and_then(|s| s.get("runtime_path"))
            .unwrap_or("n2-runtime"),
    );
    ensure_existing_writable_dir(&fs_storage_path, "N2 runtime_path")?;

    let refresh_store_root = std::env::var("N2_REFRESH_STORE_ROOT")
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

    let storage_roots = disk_section
        .and_then(|s| s.get("storage_roots"))
        .ok_or_else(|| "Missing storage_roots in [disk_config]".to_string())?;
    let storage_roots = parse_storage_roots(storage_roots).map_err(|error| error.to_string())?;
    for root in storage_roots {
        ensure_existing_writable_dir(Path::new(&root.path), "N2 storage_roots entry")?;
    }

    Ok(())
}

fn main() {
    let runtime_config = runtime_thread_config(detect_cpu_cores());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(runtime_config.worker_threads)
        .max_blocking_threads(runtime_config.max_blocking_threads)
        .thread_stack_size(runtime_config.thread_stack_bytes)
        .thread_name("n2-runtime")
        .build()
        .expect("Failed to build N2 runtime");

    runtime.block_on(async_main(runtime_config));
}

async fn async_main(runtime_config: RuntimeThreadConfig) {
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

    let _guard = init_tracing().expect("Failed to initialize tracing subscriber");
    raise_open_file_limit_for_storage();

    if std::env::args_os().nth(1).is_some() {
        eprintln!("litiaina-n2 runs the server only. Use `litiaina-n2-shell` for admin commands.");
        std::process::exit(2);
    }

    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    tracing::info!("╔════════════════════════════════════════════════╗");
    tracing::info!("║              L I T I A I N A  N 2              ║");
    tracing::info!("╚════════════════════════════════════════════════╝");
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
    tracing::info!(
        "│ Runtime        : worker_threads={} blocking_threads={} stack={}KiB",
        runtime_config.worker_threads,
        runtime_config.max_blocking_threads,
        runtime_config.thread_stack_bytes / 1024
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
    let native_port = server_section
        .get("native_port")
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(50002);
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
        .and_then(|s| s.get("runtime_path"))
        .unwrap_or("n2-runtime");

    FS_STORAGE_ROOT
        .set(fs_storage_path.to_string())
        .expect("Double initialization of STORAGE_ROOT!");

    let startup_safety_state = crate::core::operation_gate::load_safety_read_only_marker()
        .unwrap_or_else(|error| panic!("N2 could not load its safety read-only marker: {error}"));

    let storage_roots = disk_section
        .and_then(|s| s.get("storage_roots"))
        .expect("Missing storage_roots in [disk_config]");
    let storage_roots = parse_storage_roots(storage_roots)
        .unwrap_or_else(|err| panic!("Invalid storage_roots: {err}"));
    let storage_disks = storage_roots
        .iter()
        .map(|root| root.path.clone())
        .collect::<Vec<_>>();
    let storage_disks = normalize_fs_backed_disk_paths("storage_roots", storage_disks);
    let storage_disk_count = storage_disks.len();
    let storage_device_count = detect_unique_storage_devices(&storage_disks);
    let effective_blob_disks_for_auto = storage_device_count.min(storage_disk_count).max(1);
    if storage_device_count < storage_disk_count {
        tracing::warn!(
            "storage_roots has {} configured path(s) but only {} unique physical device(s); auto tuning will use {} effective disk(s)",
            storage_disk_count,
            storage_device_count,
            effective_blob_disks_for_auto
        );
    }

    STORAGE_DISKS
        .set(storage_disks)
        .expect("Double initialization of STORAGE_DISKS!");
    tracing::info!(
        "Storage mode: N2 distributed object storage with colocated immutable object metadata and shard payloads"
    );

    let https_endpoint = format!("https://{}:{}", host, https_port);
    let native_endpoint = format!("n2://{}:{}", host, native_port);
    let cluster_runtime = init_cluster_runtime(
        &config,
        fs_storage_path,
        &storage_roots,
        &https_endpoint,
        &native_endpoint,
    )
    .unwrap_or_else(|err| panic!("N2 cluster runtime initialization failed: {}", err));
    initialize_local_root_identities(&cluster_runtime)
        .unwrap_or_else(|err| panic!("N2 root identity initialization failed: {err}"));
    if let Some(state) = startup_safety_state {
        if state.cluster_id != cluster_runtime.membership.cluster_id
            || state.membership_epoch > cluster_runtime.membership.epoch
        {
            panic!(
                "N2 safety read-only marker belongs to cluster '{}' epoch {}, but configuration is cluster '{}' epoch {}",
                state.cluster_id,
                state.membership_epoch,
                cluster_runtime.membership.cluster_id,
                cluster_runtime.membership.epoch
            );
        }
        tracing::error!(
            incident_id = %state.incident_id,
            finding = %state.finding,
            reason = %state.reason,
            "N2 started in durable cluster safety read-only mode"
        );
    }
    let write_pool = cluster_runtime
        .membership
        .newest_writable_pool()
        .expect("validated N2 membership has a writable server pool");
    tracing::info!(
        "Cluster runtime: local_node={} pool={} membership_epoch={} unified_nodes={} ncnx_replicas={} object_ec={}+{} membership_file={} auto_rebalance={}",
        cluster_runtime.local_node_id,
        cluster_runtime.local_pool_id,
        cluster_runtime.membership.epoch,
        cluster_runtime.membership.nodes.len(),
        write_pool.policy.namespace_replicas(),
        write_pool.policy.object_data_shards,
        write_pool.policy.object_parity_shards,
        cluster_runtime.membership_path.display(),
        cluster_runtime.auto_rebalance
    );

    let cpu_cores = detect_cpu_cores();
    let detected_ram_bytes = detect_total_ram_bytes();
    let detected_ram_gib = ram_gib(detected_ram_bytes);

    let upload_section = config.section(Some("upload_config"));

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
    let disk_io_scheduler =
        DiskIoSchedulerConfig::from_runtime(max_io_semaphore, effective_blob_disks_for_auto);
    configure_disk_io_scheduler(disk_io_scheduler)
        .expect("Double initialization of N2 disk IO scheduler");

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
    let configured_upload_io_gate_enabled = parse_bool_or_default(
        upload_section.and_then(|s| s.get("upload_io_gate_enabled")),
        true,
    );
    let upload_io_gate_enabled = configured_upload_io_gate_enabled;

    tracing::info!(
        "Upload tuning: cpu_cores={}, ram_gib={}, storage_paths={}, storage_devices={}, auto_effective_disks={}, ec={}D+{}P, max_concurrent_io={} (configured={} budget_cap={} per_upload_est={}KiB budget={}MiB effective_budget={}MiB), disk_scheduler={{total:{}, foreground:{}, background:{}, fg_fragment:{}, bg_fragment:{}}}, writer_buffer_size={}, part_durability_strict_default={}, io_gate_enabled={}",
        cpu_cores,
        detected_ram_gib,
        storage_disk_count,
        storage_device_count,
        effective_blob_disks_for_auto,
        data_shards_value,
        parity_shards_value,
        max_io_semaphore,
        configured_max_io,
        budget_limited_max_io,
        per_upload_bytes / 1024,
        upload_memory_budget_bytes / (1024 * 1024),
        effective_upload_budget_bytes / (1024 * 1024),
        disk_io_scheduler.total_permits,
        disk_io_scheduler.foreground_permits,
        disk_io_scheduler.background_permits,
        disk_io_scheduler.foreground_per_fragment_permits,
        disk_io_scheduler.background_per_fragment_permits,
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
    let native_addr = SocketAddr::from((ip, native_port));
    let native_listener = bind_native_protocol(native_addr)
        .await
        .expect("Failed to bind required N2 native protocol listener");
    let (storage_router, app_state) = build_storage_routes_unbound();
    let native_state = app_state.clone();
    let serve_storage = apply_runtime_http_layers(storage_router)
        .with_state(app_state)
        .fallback(handler_404);

    let fs_root = FS_STORAGE_ROOT
        .get()
        .unwrap_or(&STORAGE_ROOT.to_string())
        .to_string();
    tokio::spawn(async move {
        interval_cleanup(&fs_root).await;
    });

    crate::distribution::peer::spawn_fragment_control_sync_task();
    crate::distribution::auto_healing::spawn_auto_healing_task();
    crate::distribution::reserve_set::spawn_reserve_set_task();

    tokio::spawn(async move {
        serve_native_protocol(native_listener, native_addr, native_state).await;
    });

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
