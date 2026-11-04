use clap::{Parser, Subcommand};
use n1_metadata_engine::{MetadataEngine, Options};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Litiaina N1 metadata storage administration")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve the original named NOA API over native N1 and HTTP(S).
    Serve {
        #[arg(long, default_value = "n1_config.ini")]
        config: PathBuf,
    },
    /// Create a new metadata database in an explicitly empty directory.
    Init {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Validate an existing database and report its allocated storage.
    Check {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Restore a verified metadata snapshot into a NEW database directory.
    Restore {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Explicit offline rebuild of a fragment's logical usage counters.
    Recount {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        fragment: String,
    },
}

fn main() {
    let mut args: Vec<_> = std::env::args_os().collect();
    if args.len() == 1 || args.get(1).is_some_and(|v| v == "--config") {
        args.insert(1, "serve".into());
    }
    let cli = Cli::parse_from(args);
    let result = match cli.command {
        Command::Serve { config } => {
            let _ = rustls::crypto::ring::default_provider().install_default();
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(
                    std::thread::available_parallelism()
                        .map_or(4, usize::from)
                        .clamp(2, 32),
                )
                .enable_all()
                .build()
                .expect("metadata runtime")
                .block_on(serve(config))
                .map_err(|e| n1_metadata_engine::Error::Invalid(e.to_string()))
        }
        command => run(Cli { command }),
    };
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> n1_metadata_engine::Result<()> {
    if let Command::Recount { data_dir, fragment } = cli.command {
        let db = MetadataEngine::open(data_dir, Options::default())?;
        let fragment = n1_metadata_engine::fragments::resolve(&db, &fragment)?;
        let usage = n1_metadata_engine::namespace::Namespace::new(db).recount_usage(fragment.id)?;
        println!(
            "{}",
            serde_json::to_string(&usage)
                .map_err(|e| n1_metadata_engine::Error::Invalid(e.to_string()))?
        );
        return Ok(());
    }
    if let Command::Restore { snapshot, data_dir } = cli.command {
        let records = MetadataEngine::restore_backup(&snapshot, &data_dir)?;
        println!(
            "{}",
            serde_json::json!({"restored_records":records,"data_dir":data_dir})
        );
        return Ok(());
    }
    let path = match cli.command {
        Command::Restore { .. } => unreachable!("restore handled separately"),
        Command::Recount { .. } => unreachable!("recount handled separately"),
        Command::Serve { .. } => unreachable!("serve runs inside its async runtime"),
        Command::Init { data_dir } => {
            if data_dir.exists() && std::fs::read_dir(&data_dir)?.next().is_some() {
                return Err(n1_metadata_engine::Error::Invalid(
                    "init requires an empty metadata directory".into(),
                ));
            }
            data_dir
        }
        Command::Check { data_dir } => {
            if !data_dir.is_dir() || !data_dir.join("version").is_file() {
                return Err(n1_metadata_engine::Error::Invalid(
                    "check requires an existing metadata database".into(),
                ));
            }
            data_dir
        }
    };
    let engine = MetadataEngine::open(&path, Options::default())?;
    println!(
        "{}",
        serde_json::json!({
            "component": "metadata_engine", "version": env!("CARGO_PKG_VERSION"),
            "storage": "procyon-engine", "data_dir": path, "disk_bytes": engine.disk_bytes()?,
        })
    );
    Ok(())
}

async fn serve(path: PathBuf) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use n1_metadata_engine::server::{App, ServerConfig, native, router};
    let config = ServerConfig::load(&path)?;
    let http = config.http;
    let address = config.native;
    let tls = match (&config.cert_path, &config.key_path) {
        (Some(cert), Some(key)) => {
            Some(axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?)
        }
        (None, None) => None,
        _ => return Err("TLS requires certificate and private key".into()),
    };
    let native_tls = tls.as_ref().map(|config| {
        let mut inner = (*config.get_inner()).clone();
        inner.alpn_protocols = vec![b"n1/0.5".to_vec()];
        tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(inner))
    });
    let listener = tokio::net::TcpListener::bind(address).await?;
    let app = App::open(config).await?;
    n1_metadata_engine::exports::initialize(&app).await?;
    eprintln!(
        "╔════════════════════════════════════════════════╗\n║              L I T I A I N A  N 1              ║\n╚════════════════════════════════════════════════╝\n│ Version        : {}\n│ Component      : metadata_engine\n│ Start Time     : {}\n│ PID            : {}\n│ Native         : n1://{}\n│ HTTP           : {}://{}",
        env!("CARGO_PKG_VERSION"),
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
        std::process::id(),
        address,
        if tls.is_some() { "https" } else { "http" },
        http
    );
    let stop = tokio_util::sync::CancellationToken::new();
    let handle = axum_server::Handle::new();
    let service = router(app.clone()).into_make_service();
    let mut native = tokio::spawn(native::serve(
        listener,
        app.clone(),
        native_tls,
        stop.clone(),
    ));
    let graceful = handle.clone();
    let mut http_task = tokio::spawn(async move {
        match tls {
            Some(tls) => {
                axum_server::bind_rustls(http, tls)
                    .handle(handle)
                    .serve(service)
                    .await
            }
            None => axum_server::bind(http).handle(handle).serve(service).await,
        }
    });
    tokio::select! { result=&mut native=>{result??;},result=&mut http_task=>{result??;}, _=shutdown_signal()=>() }
    stop.cancel();
    graceful.graceful_shutdown(Some(std::time::Duration::from_secs(30)));
    if !native.is_finished() {
        native.await??;
    }
    if !http_task.is_finished() {
        http_task.await??;
    }
    n1_metadata_engine::exports::shutdown().await;
    app.shutdown().await;
    Ok(())
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! {_=tokio::signal::ctrl_c()=>(),_=terminate.recv()=>()}
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
