//! Read-only continuation of a preserved campaign on a different verifier host.
//! The original endpoint, manifest and ACK journals are never rewritten.
#[path = "network_readback/provenance.rs"]
mod provenance;
#[path = "noa_qualification/workload.rs"]
#[allow(dead_code)] // Share the proven payload/version oracle, without population.
mod workload;
mod setup {
    #[derive(serde::Deserialize)]
    pub struct Config {
        pub url: String,
        pub identity: String,
        pub master: String,
        pub fragment_secret: String,
    }
}

use clap::Parser;
use serde_json::json;
use std::{fs, io::Write, os::unix::fs::PermissionsExt, path::PathBuf};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    oracle: PathBuf,
    #[arg(long)]
    original_endpoint: PathBuf,
    #[arg(long)]
    endpoint: PathBuf,
    /// A fresh, already-created owner-only directory for this complete pass.
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    scenario: String,
    #[arg(long, default_value_t = 8)]
    readers: usize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = Args::parse();
    if let Err(error) = run(&args).await {
        let _ = publish(
            &args.output.join("FAILED.json"),
            &json!({"passed":false,"error":error.to_string()}),
        );
        eprintln!("read-only campaign verification failed: {error}");
        std::process::exit(1);
    }
}

async fn run(args: &Args) -> Result<()> {
    if !(1..=128).contains(&args.readers) || args.scenario.trim().is_empty() {
        return Err("invalid readback parameters".into());
    }
    for path in [&args.oracle, &args.output] {
        private(path, true)?;
    }
    for path in [&args.original_endpoint, &args.endpoint] {
        private(path, false)?;
    }
    if fs::read_dir(&args.output)?.next().is_some() {
        return Err("readback output must be fresh; an interrupted pass restarts at zero".into());
    }
    let original = fs::read(&args.original_endpoint)?;
    let endpoint = fs::read(&args.endpoint)?;
    let manifest = fs::read(args.oracle.join("network-campaign.json"))?;
    let count = provenance::validate(&manifest, &original, &endpoint)?;
    let before = provenance::journals(&args.oracle)?;
    let prepared = json!({
        "format":1,"scenario":args.scenario,"count":count,"readers":args.readers,
        "original_manifest_sha256":provenance::hash(&manifest),
        "original_endpoint_sha256":provenance::hash(&original),
        "endpoint_sha256":provenance::hash(&endpoint),
        "original_harness_sha256":serde_json::from_slice::<serde_json::Value>(&manifest)?["harness_sha256"],
        "verifier_sha256":provenance::hash(&fs::read(std::env::current_exe()?)?),
        "ack_journals":before,
        "policy":"original ACKs and versions; full pass from zero; no fragment provisioning or object writes"
    });
    publish(&args.output.join("prepared.json"), &prepared)?;
    let config: setup::Config = serde_json::from_slice(&endpoint)?;
    let api = workload::Api::attach(&config, "api").await?;
    let result = workload::verify(api, &args.oracle, count, args.readers, "api").await?;
    if before != provenance::journals(&args.oracle)?
        || manifest != fs::read(args.oracle.join("network-campaign.json"))?
        || original != fs::read(&args.original_endpoint)?
        || endpoint != fs::read(&args.endpoint)?
    {
        return Err("source campaign changed during read-only verification".into());
    }
    publish(
        &args.output.join("PASSED.json"),
        &json!({"passed":true,"provenance":prepared,"result":result}),
    )?;
    println!(
        "{}",
        json!({"passed":true,"scenario":args.scenario,"result":result})
    );
    Ok(())
}

fn private(path: &std::path::Path, directory: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !path.is_absolute()
        || metadata.file_type().is_symlink()
        || metadata.is_dir() != directory
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err("readback paths must be absolute, real and owner-only".into());
    }
    Ok(())
}

fn publish(path: &std::path::Path, value: &serde_json::Value) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    private(path.parent().ok_or("missing output parent")?, true)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::File::open(path.parent().ok_or("missing output parent")?)?.sync_all()?;
    Ok(())
}
