#[cfg(not(target_os = "linux"))]
compile_error!("N1 engine is Linux-only");
use chrono::Local;
use n1::utils::date_time_formatter::LocalTimer;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};
fn main() {
    // Keep offline recovery outside the server runtime. The supervisor starts
    // its own bounded runtimes and must also work while native/HTTP are down.
    let mut offline_args: Vec<_> = std::env::args_os().collect();
    if offline_args
        .get(1)
        .is_some_and(|arg| arg == "backup" || arg == "pool")
    {
        let operation = offline_args.remove(1);
        let result = if operation == "pool" {
            n1::core::storage_admin::run_from(offline_args)
        } else {
            n1::core::backup::cli::run_from(offline_args)
        };
        if let Err(error) = result {
            eprintln!("N1 offline maintenance failed: {error}");
            std::process::exit(1);
        }
        return;
    }
    let cli = n1::protocol::server::startup::Cli::parse_operator();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tracing_guard = init_tracing().expect("Failed to initialize tracing subscriber");
    let runtime_config = runtime_thread_config(detect_cpu_cores());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_config.worker_threads)
        .max_blocking_threads(runtime_config.max_blocking_threads)
        .thread_stack_size(runtime_config.thread_stack_bytes)
        .thread_name("n1-runtime")
        .enable_all()
        .build()
        .expect("N1 runtime");
    if cli.is_server() {
        print_startup_banner(runtime_config);
    }
    let result = runtime.block_on(n1::protocol::server::startup::run(cli));
    runtime.shutdown_timeout(std::time::Duration::from_secs(30));
    drop(tracing_guard);
    if let Err(error) = result {
        eprintln!("N1: {error}");
        std::process::exit(1);
    }
}

fn init_tracing() -> Result<WorkerGuard, Box<dyn std::error::Error>> {
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

fn print_startup_banner(runtime_config: RuntimeThreadConfig) {
    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    tracing::info!("╔════════════════════════════════════════════════╗");
    tracing::info!("║              L I T I A I N A  N 1              ║");
    tracing::info!("╚════════════════════════════════════════════════╝");
    tracing::info!("│ Version        : {}", env!("CARGO_PKG_VERSION"));
    tracing::info!("│ Component      : N1 object store");
    tracing::info!("│ Architecture   : LINK — Litiaina Immutable Native Key");
    tracing::info!("│ Status         : STARTING");
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
}
fn detect_cpu_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[derive(Clone, Copy)]
struct RuntimeThreadConfig {
    worker_threads: usize,
    max_blocking_threads: usize,
    thread_stack_bytes: usize,
}

fn parse_env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
}

fn runtime_thread_config(cpu_cores: usize) -> RuntimeThreadConfig {
    let worker_threads = parse_env_usize("N1_RUNTIME_WORKER_THREADS")
        .unwrap_or_else(|| cpu_cores.clamp(2, 16))
        .clamp(1, 64);
    let max_blocking_threads = parse_env_usize("N1_RUNTIME_MAX_BLOCKING_THREADS")
        .unwrap_or_else(|| cpu_cores.saturating_mul(4).clamp(16, 32))
        .clamp(8, 128);
    let thread_stack_bytes = parse_env_usize("N1_RUNTIME_THREAD_STACK_BYTES")
        .unwrap_or(1024 * 1024)
        .clamp(512 * 1024, 8 * 1024 * 1024);

    RuntimeThreadConfig {
        worker_threads,
        max_blocking_threads,
        thread_stack_bytes,
    }
}
