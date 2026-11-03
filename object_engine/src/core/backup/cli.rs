use super::{Result, capture, repository::Repository};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
#[derive(Parser)]
#[command(name = "litiaina-n1-backup", version)]
struct Cli {
    #[command(subcommand)]
    command: Action,
}
#[derive(Subcommand)]
enum Action {
    RunJob {
        #[arg(long)]
        config: PathBuf,
    },
    RecoverJob {
        #[arg(long)]
        state_dir: PathBuf,
    },
    JobStatus {
        #[arg(long)]
        config: PathBuf,
    },
    Keygen {
        #[arg(long)]
        output: PathBuf,
    },
    Init {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long = "key-file", alias = "key")]
        key: PathBuf,
    },
    #[command(name = "create", alias = "capture")]
    Capture {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long = "key-file", alias = "key")]
        key: PathBuf,
        #[arg(long)]
        config: PathBuf,
    },
    List {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long = "key-file", alias = "key")]
        key: PathBuf,
        #[arg(long, default_value = "")]
        after: String,
        #[arg(long, default_value_t = 64)]
        limit: usize,
    },
    Verify {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long = "key-file", alias = "key")]
        key: PathBuf,
        #[arg(long)]
        snapshot: String,
    },
    Restore {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long = "key-file", alias = "key")]
        key: PathBuf,
        #[arg(long)]
        snapshot: String,
        #[arg(long, required = true)]
        target: Vec<PathBuf>,
        #[arg(long)]
        resume: bool,
        #[arg(long)]
        output_config: Option<PathBuf>,
    },
    Replicate {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long = "key-file", alias = "key")]
        key: PathBuf,
        #[arg(long)]
        snapshot: String,
        #[arg(long)]
        destination: PathBuf,
        #[arg(long)]
        destination_key: PathBuf,
    },
    ReplicateRemote {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long = "key-file", alias = "key")]
        key: PathBuf,
        #[arg(long)]
        snapshot: String,
        #[arg(long)]
        credentials: PathBuf,
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        destination: String,
        #[arg(long)]
        ca: Option<PathBuf>,
    },
    Prune {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long = "key-file", alias = "key")]
        key: PathBuf,
        #[arg(long)]
        keep_last: usize,
        #[arg(long)]
        protect: String,
    },
    Gc {
        #[arg(long)]
        repository: PathBuf,
        #[arg(long = "key-file", alias = "key")]
        key: PathBuf,
        #[arg(long)]
        discard_incomplete: bool,
    },
}
pub fn run() -> Result<()> {
    run_from(std::env::args_os().collect())
}

pub fn run_from(args: Vec<std::ffi::OsString>) -> Result<()> {
    let value = match Cli::parse_from(args).command {
        Action::RunJob { config } => {
            super::automation::run(&config)?;
            super::automation::status_value(&config)?
        }
        Action::RecoverJob { state_dir } => {
            super::automation::recover(&state_dir)?;
            serde_json::json!({"recovered":true})
        }
        Action::JobStatus { config } => {
            super::automation::status(&config)?;
            return Ok(());
        }
        Action::Keygen { output } => {
            super::create_key(&output)?;
            serde_json::json!({"created":output})
        }
        Action::Init { repository, key } => {
            Repository::initialize(&repository, &key)?;
            serde_json::json!({"repository":repository})
        }
        Action::Capture {
            repository,
            key,
            config,
        } => {
            let repo = Repository::open(&repository, &key, true)?;
            let service = crate::protocol::ops::Service::open(
                crate::settings::config::Config::load(&config)?,
            )?;
            let point = capture::offline(&repo, &service.store)?;
            serde_json::json!({"id":point.id,"capture_mode":point.capture_mode,"files":point.files,"source_bytes":point.source_bytes,"service_control":point.service_control.is_some()})
        }
        Action::List {
            repository,
            key,
            after,
            limit,
        } => serde_json::to_value(
            Repository::open(&repository, &key, false)?.list_page(&after, limit)?,
        )?,
        Action::Verify {
            repository,
            key,
            snapshot,
        } => {
            let repo = Repository::open(&repository, &key, false)?;
            repo.verify(&repo.snapshot(&snapshot)?)?;
            serde_json::json!({"verified":snapshot})
        }
        Action::Restore {
            repository,
            key,
            snapshot,
            target,
            resume,
            output_config,
        } => {
            let repo = Repository::open(&repository, &key, false)?;
            let point = repo.snapshot(&snapshot)?;
            if output_config.is_some() && point.service_control.is_none() {
                return Err(super::invalid("snapshot has no service recovery identity"));
            }
            if resume {
                super::restore::resume(&repo, &point, &target)?;
            } else {
                super::restore::restore(&repo, &point, &target)?;
            }
            if let Some(output) = &output_config {
                super::control::restore(&repo, &point, output, &target)?;
            }
            serde_json::json!({"restored":snapshot,"targets":target,"configuration":output_config,"service_control_available":point.service_control.is_some()})
        }
        Action::Replicate {
            repository,
            key,
            snapshot,
            destination,
            destination_key,
        } => {
            let source = Repository::open(&repository, &key, false)?;
            let destination = Repository::open(&destination, &destination_key, true)?;
            serde_json::to_value(super::transfer::replicate(
                &source,
                &destination,
                &snapshot,
            )?)?
        }
        Action::ReplicateRemote {
            repository,
            key,
            snapshot,
            credentials,
            endpoint,
            destination,
            ca,
        } => {
            let source = std::sync::Arc::new(Repository::open(&repository, &key, false)?);
            let credentials = cli_core::Credentials::load(&credentials)?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            let point = runtime.block_on(async move {
                if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
                    super::network::replicate_http(
                        &endpoint,
                        &destination,
                        credentials,
                        ca.as_deref(),
                        source,
                        snapshot,
                    )
                    .await
                } else {
                    let client = credentials.connect(Some(&endpoint), ca.as_deref()).await?;
                    let mut channel = client.into_replication(destination).await?;
                    super::network::send(&mut channel, source, snapshot).await
                }
            })?;
            serde_json::to_value(point)?
        }
        Action::Prune {
            repository,
            key,
            keep_last,
            protect,
        } => {
            let repo = Repository::open(&repository, &key, true)?;
            super::retention::prune(&repo, keep_last, &protect)?;
            serde_json::json!({"retained":keep_last,"protected":protect})
        }
        Action::Gc {
            repository,
            key,
            discard_incomplete,
        } => {
            let repo = Repository::open(&repository, &key, true)?;
            serde_json::json!({"removed_bytes":super::retention::gc(&repo,discard_incomplete)?})
        }
    };
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
