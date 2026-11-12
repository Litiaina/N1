//! Offline split-node census. Only identity bytes cross nodes; no N1 signing
//! secret or object access credential is copied to perform physical checks.
mod network_inventory {
    pub mod metadata;
    pub mod physical;
}
use clap::{Parser, Subcommand};
use serde_json::Value;
use std::{fs, io::Write, os::unix::fs::PermissionsExt, path::PathBuf};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
    #[arg(long)]
    output: PathBuf,
}
#[derive(Subcommand)]
enum Command {
    Metadata {
        #[arg(long)]
        endpoint: PathBuf,
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        oracle: PathBuf,
        #[arg(long)]
        count: u64,
        #[arg(long)]
        references: PathBuf,
    },
    Physical {
        #[arg(long)]
        registry: PathBuf,
        #[arg(long)]
        member: Vec<PathBuf>,
        #[arg(long)]
        references: PathBuf,
        #[arg(long)]
        references_sha256: String,
        #[arg(long)]
        count: u64,
    },
}

fn main() {
    if let Err(error) = run(Args::parse()) {
        eprintln!("split-node inventory failed: {error}");
        std::process::exit(1);
    }
}
fn run(args: Args) -> Result<()> {
    let parent = args.output.parent().ok_or("missing output parent")?;
    private_path(parent, true)?;
    let result = match args.command {
        Command::Metadata {
            endpoint,
            data_dir,
            oracle,
            count,
            references,
        } => network_inventory::metadata::run(&endpoint, &data_dir, &oracle, count, &references)?,
        Command::Physical {
            registry,
            member,
            references,
            references_sha256,
            count,
        } => network_inventory::physical::run(
            &registry,
            &member,
            &references,
            &references_sha256,
            count,
        )?,
    };
    let mut file = create(&args.output)?;
    serde_json::to_writer_pretty(&mut file, &result)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::File::open(parent)?.sync_all()?;
    println!(
        "{}",
        serde_json::json!({"passed":true,"report":args.output})
    );
    Ok(())
}
fn private_path(path: &std::path::Path, directory: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !path.is_absolute()
        || metadata.file_type().is_symlink()
        || metadata.is_dir() != directory
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err("inventory paths must be absolute, real and owner-only".into());
    }
    Ok(())
}
fn create(path: &std::path::Path) -> Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    private_path(path.parent().ok_or("missing file parent")?, true)?;
    Ok(fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?)
}
fn bounds(count: u64) -> Result<()> {
    if !(1..=10_000_000).contains(&count) {
        return Err("invalid inventory count".into());
    }
    Ok(())
}
