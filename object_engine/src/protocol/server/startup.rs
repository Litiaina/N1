//! Explicit enrollment and startup of the LINK engine. There is one object
//! engine and both transports share it. Startup never enrolls absent roots.
use crate::{
    core::{Error, Result, storage_pool::Pool},
    protocol::ops::Service,
    settings::config::{CONFIG_FORMAT, Config, ConfiguredPool, Fragment, Tls},
};
use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use clap::{Parser, Subcommand};
use n1_link::{PoolId, PoolLayout, noa::Permissions};
use rand::{RngCore, rngs::OsRng};
use serde::Serialize;
use std::{
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Parser)]
#[command(name="litiaina-n1",version=concat!(env!("CARGO_PKG_VERSION"),"\nstorage_format LINK-v1\nnoa v2\nnative_protocol n1/1"),about="Litiaina N1 LINK object engine")]
pub struct Cli {
    #[command(subcommand)]
    command: Action,
}
impl Cli {
    pub fn parse_operator() -> Self {
        let mut args = std::env::args_os().collect::<Vec<_>>();
        if args.len() == 1 || args.get(1).is_some_and(|arg| arg == "--config") {
            args.insert(1, "serve".into());
        }
        Self::parse_from(args)
    }
    pub fn is_server(&self) -> bool {
        matches!(self.command, Action::Serve { .. })
    }
}
#[derive(Subcommand)]
enum Action {
    /// Enroll an empty immutable pool and create private service configuration.
    Init {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        member: Vec<PathBuf>,
        #[arg(long)]
        data_shards: Option<u16>,
        #[arg(long, default_value_t = 1)]
        parity_shards: u16,
        #[arg(long, default_value_t = 262144)]
        block_bytes: u32,
        #[arg(long, default_value = "127.0.0.1:19001")]
        native_address: SocketAddr,
        #[arg(long)]
        http_address: Option<SocketAddr>,
        #[arg(long, default_value = "root")]
        fragment: String,
        #[arg(long, requires = "tls_key")]
        tls_certificate: Option<PathBuf>,
        #[arg(long, requires = "tls_certificate")]
        tls_key: Option<PathBuf>,
    },
    /// Open enrolled pools and serve native NOA, plus an optional HTTP adapter.
    Serve {
        #[arg(long, default_value = "n1_config.ini")]
        config: PathBuf,
    },
    /// Validate private configuration and member bindings without listening.
    Check {
        #[arg(long, default_value = "n1_config.ini")]
        config: PathBuf,
    },
}

pub async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Action::Init {
            config,
            member,
            data_shards,
            parity_shards,
            block_bytes,
            native_address,
            http_address,
            fragment,
            tls_certificate,
            tls_key,
        } => {
            if crate::settings::ini::is_ini(&config) {
                if !member.is_empty()
                    || data_shards.is_some()
                    || tls_certificate.is_some()
                    || tls_key.is_some()
                {
                    return Err(Error::Invalid(
                        "INI enrollment uses storage_pool and server settings from the file".into(),
                    ));
                }
                let service = load(config, true).await?;
                println!(
                    "{}",
                    serde_json::to_string(&service.ping())
                        .map_err(|error| Error::Invalid(error.to_string()))?
                );
                return Ok(());
            }
            let data_shards = data_shards.ok_or_else(|| {
                Error::Invalid("JSON enrollment requires --data-shards and --member".into())
            })?;
            let tls = tls_certificate
                .zip(tls_key)
                .map(|(certificate, private_key)| Tls {
                    certificate,
                    private_key,
                });
            tokio::task::spawn_blocking(move || {
                init(Enrollment {
                    config,
                    roots: member,
                    k: data_shards,
                    p: parity_shards,
                    block_bytes,
                    native_address,
                    http_address,
                    alias: fragment,
                    tls,
                })
            })
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?
        }
        Action::Check { config } => {
            let service = load(config, false).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&service.ping())
                    .map_err(|e| Error::Invalid(e.to_string()))?
            );
            Ok(())
        }
        Action::Serve { config } => {
            let service = load(config, true).await?;
            // Keep control/member leases until admitted workers and journals
            // have drained, including work owned beyond a request connection.
            let shutdown_service = service.clone();
            service.config.runtime.configure()?;
            service.config.concurrency.configure(
                service.config.max_operations,
                service.config.pools.iter().map(|p| p.roots.len()).sum(),
            )?;
            let backend = crate::core::linux_io::initialize_backend()?;
            tracing::info!(?backend, "object I/O backend selected");
            let listener = tokio::net::TcpListener::bind(service.config.native_address).await?;
            let http = match service.config.http_address {
                Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
                None => None,
            };
            // Validate certificates before accepting either protocol.
            let tls = match &service.config.tls {
                Some(tls) => Some(
                    axum_server::tls_rustls::RustlsConfig::from_pem_file(
                        &tls.certificate,
                        &tls.private_key,
                    )
                    .await?,
                ),
                None => None,
            };
            let stop = tokio_util::sync::CancellationToken::new();
            let monitor = tokio::spawn(crate::core::monitor::run(
                service.store.clone(),
                stop.clone(),
            ));
            let adaptive_stop = stop.clone();
            let adaptive = tokio::spawn(
                crate::core::adaptive_concurrency::run_adaptive_concurrency_until(adaptive_stop),
            );
            let jobs = service.jobs.clone();
            let recovery_jobs = service.recovery_jobs.clone();
            jobs.schedule(
                service.store.clone(),
                service.config.maintenance.clone(),
                stop.clone(),
            );
            let calendar = if let Some(calendar) = service.config.maintenance.calendar_scrub.clone()
            {
                let jobs = jobs.clone();
                let store = service.store.clone();
                let stop = stop.clone();
                let journal = service.config.state_root().join("scheduled-scrub.json");
                Some(tokio::spawn(async move {
                    if let Err(error) = crate::core::auto_healing::schedule::run(
                        jobs, store, calendar, journal, stop,
                    )
                    .await
                    {
                        tracing::error!(%error, "calendar scrub scheduler stopped");
                    }
                }))
            } else {
                None
            };
            let native_stop = stop.clone();
            let http_enabled = http.is_some();
            let http_handle = axum_server::Handle::new();
            let graceful = http_handle.clone();
            let engine = service.clone();
            let mut native = tokio::spawn(async move {
                crate::protocol::server::serve_until(listener, engine, native_stop).await
            });
            let mut http = tokio::spawn(async move {
                match http {
                    Some(listener) => {
                        let app = crate::routes::storage_routes::router(service);
                        let listener = listener.into_std()?;
                        if let Some(tls) = tls {
                            axum_server::from_tcp_rustls(listener, tls)?
                                .handle(http_handle)
                                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                                .await?;
                        } else {
                            axum_server::from_tcp(listener)?
                                .handle(http_handle)
                                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                                .await?;
                        }
                        Ok::<(), Error>(())
                    }
                    None => std::future::pending().await,
                }
            });
            let mut native_done = false;
            let mut http_done = false;
            let result = tokio::select! {
                result=&mut native=>{native_done=true;result.unwrap_or_else(|e|Err(Error::Unavailable(e.to_string())))},
                result=&mut http=>{http_done=true;result.unwrap_or_else(|e|Err(Error::Unavailable(e.to_string())))},
                _=shutdown()=>{tracing::info!("draining admitted object I/O");Ok(())},
            };
            stop.cancel();
            shutdown_service.store.repair_manager.close()?;
            shutdown_service.store.activity.close()?;
            let _ = adaptive.await;
            let _ = monitor.await;
            jobs.stop_current();
            recovery_jobs.stop_current();
            graceful.graceful_shutdown(Some(std::time::Duration::from_secs(30)));
            if !native_done
                && tokio::time::timeout(std::time::Duration::from_secs(30), &mut native)
                    .await
                    .is_err()
            {
                native.abort();
            }
            if !http_done
                && (!http_enabled
                    || tokio::time::timeout(std::time::Duration::from_secs(30), &mut http)
                        .await
                        .is_err())
            {
                http.abort();
            }
            jobs.wait_idle().await;
            recovery_jobs.wait_idle().await;
            shutdown_service.store.repair_manager.wait_idle().await?;
            if let Some(calendar) = calendar {
                let _ = calendar.await;
            }
            shutdown_service.store.activity.wait_idle().await?;
            result
        }
    }
}
async fn load(path: PathBuf, enroll: bool) -> Result<Arc<Service>> {
    tokio::task::spawn_blocking(move || {
        if crate::settings::ini::is_ini(&path) {
            crate::settings::ini::open(&path, enroll)
        } else {
            Service::open(Config::load(&path)?)
        }
    })
    .await
    .map_err(|e| Error::Unavailable(e.to_string()))?
}
async fn shutdown() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
}
struct Enrollment {
    config: PathBuf,
    roots: Vec<PathBuf>,
    k: u16,
    p: u16,
    block_bytes: u32,
    native_address: SocketAddr,
    http_address: Option<SocketAddr>,
    alias: String,
    tls: Option<Tls>,
}
fn init(enrollment: Enrollment) -> Result<()> {
    let Enrollment {
        config,
        roots,
        k,
        p,
        block_bytes,
        native_address,
        http_address,
        alias,
        tls,
    } = enrollment;
    if !config.is_absolute() {
        return Err(Error::Invalid("configuration path must be absolute".into()));
    }
    let signing_key_file = config.with_extension("signing-key");
    let credentials = config.with_extension("credentials.json");
    for path in [&config, &signing_key_file, &credentials] {
        if path.symlink_metadata().is_ok() {
            return Err(Error::Conflict(format!(
                "refusing to replace {}",
                path.display()
            )));
        }
        if !path.parent().is_some_and(|p| p.is_dir()) {
            return Err(Error::Invalid(
                "configuration parent must already exist".into(),
            ));
        }
    }
    let mut pool_id = [0; 16];
    OsRng.fill_bytes(&mut pool_id);
    let mut fragment_id = [0; 16];
    OsRng.fill_bytes(&mut fragment_id);
    let mut secret = [0; 32];
    OsRng.fill_bytes(&mut secret);
    let mut password = [0; 32];
    OsRng.fill_bytes(&mut password);
    let password = hex::encode(password);
    let salt = SaltString::generate(&mut OsRng);
    let secret_hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .to_string();
    let layout = PoolLayout {
        id: PoolId(pool_id),
        generation: 1,
        data_shards: k,
        parity_shards: p,
        block_bytes,
        placement_version: 1,
        shard_format: n1_link::ShardFormat::Crc32cSha256V2,
    };
    let members = (0..layout.width())
        .map(|_| {
            let mut id = [0; 16];
            OsRng.fill_bytes(&mut id);
            PoolId(id)
        })
        .collect::<Vec<_>>();
    let cfg = Config {
        format: CONFIG_FORMAT.into(),
        native_address,
        http_address,
        signing_key_file: signing_key_file.clone(),
        state_dir: None,
        max_connections: 64,
        max_operations: 16,
        operation_memory_bytes: 256 * 1024 * 1024,
        read_repair: true,
        concurrency: Default::default(),
        rate_limits: Default::default(),
        repositories: Default::default(),
        maintenance: crate::core::maintenance::jobs::MaintenanceConfig::default(),
        runtime: Default::default(),
        tls,
        pools: vec![ConfiguredPool {
            layout: layout.clone(),
            members: members.clone(),
            roots: roots.clone(),
            sealed: false,
        }],
        fragments: vec![Fragment {
            id: PoolId(fragment_id),
            alias: alias.clone(),
            secret_hash,
            permissions: Permissions::all(),
            operator: true,
        }],
    };
    cfg.validate()?;
    crate::core::ec::StripeCodec::new(&layout)?;
    let _pool = Pool::enroll_with_members(layout, &roots, members)?;
    write_private(&signing_key_file, &secret)?;
    write_json(&config, &cfg)?;
    write_json(
        &credentials,
        &serde_json::json!({"fragment":alias,"fragment_id":PoolId(fragment_id),"secret":password,
        "native_url":format!("{}://{}",if cfg.tls.is_some(){"n1s"}else{"n1"},native_address)}),
    )?;
    println!(
        "Enrolled LINK pool. Configuration: {}\nCredentials: {}",
        config.display(),
        credentials.display()
    );
    Ok(())
}
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| Error::Invalid(e.to_string()))?;
    write_private(path, &bytes)
}
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::File::open(path.parent().expect("validated parent"))?.sync_all()?;
    Ok(())
}
