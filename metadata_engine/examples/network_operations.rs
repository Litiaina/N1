//! Operator-controlled split-node qualification through the public M1 API.
//! This client never opens N1 member roots or changes server configuration.
mod network_operations {
    pub mod api;
    pub mod benchmark;
    pub mod multipart;
    pub mod snapshot;
}
use clap::{Parser, ValueEnum};
use network_operations::api::{Api, Result, publish};
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

#[derive(Clone, Copy, ValueEnum)]
enum Phase {
    MultipartBegin,
    MultipartComplete,
    MultipartCancel,
    Snapshot,
    Observe,
    Benchmark,
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    endpoint: PathBuf,
    #[arg(long)]
    root: PathBuf,
    #[arg(long, value_enum)]
    phase: Phase,
    #[arg(long, default_value = "candidate")]
    label: String,
    #[arg(long)]
    metadata_pid: Option<u32>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("split-node operation failed: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    for (path, directory) in [(&args.root, true), (&args.endpoint, false)] {
        let metadata = fs::symlink_metadata(path)?;
        if !path.is_absolute()
            || metadata.file_type().is_symlink()
            || metadata.is_dir() != directory
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err("qualification paths must be absolute, real and owner-only".into());
        }
    }
    let lease = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(args.root.join(".operations.lock"))?;
    lease.try_lock()?;
    let api = Api::attach(&args.endpoint).await?;
    let (phase, result) = match args.phase {
        Phase::MultipartBegin => (
            "multipart-begin",
            network_operations::multipart::begin(&api, &args.root).await?,
        ),
        Phase::MultipartComplete => (
            "multipart-complete",
            network_operations::multipart::complete(&api, &args.root).await?,
        ),
        Phase::MultipartCancel => (
            "multipart-cancel",
            network_operations::multipart::cancel(&api, &args.root).await?,
        ),
        Phase::Snapshot => (
            "snapshot",
            network_operations::snapshot::capture(&api, &args.root).await?,
        ),
        Phase::Observe => ("observe", api.observe().await?),
        Phase::Benchmark => (
            "benchmark",
            network_operations::benchmark::run(
                &api,
                &args.label,
                args.metadata_pid
                    .ok_or("benchmark requires --metadata-pid")?,
            )
            .await?,
        ),
    };
    let output = args
        .root
        .join(format!("{phase}-{}.json", ulid::Ulid::new()));
    publish(&output, &json!({"phase":phase,"result":result}))?;
    println!("{}", json!({"passed":true,"phase":phase,"report":output}));
    Ok(())
}
