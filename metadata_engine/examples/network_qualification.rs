//! Public-API workload for independently operated N1 and M1 nodes. Reuse the
//! established durable ACK journal and full payload/version oracle unchanged.
#[path = "noa_qualification/workload.rs"]
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
use clap::{Parser, ValueEnum};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs, io::Write, os::unix::fs::PermissionsExt, path::PathBuf};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, ValueEnum)]
enum Phase {
    Populate,
    Verify,
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    endpoint: PathBuf,
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    count: u64,
    #[arg(long, value_enum)]
    phase: Phase,
    #[arg(long, default_value_t = 4)]
    writers: usize,
    #[arg(long, default_value_t = 8)]
    readers: usize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("network qualification failed: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    if !(1..=10_000_000).contains(&args.count)
        || !(1..=64).contains(&args.writers)
        || !(1..=128).contains(&args.readers)
        || !args.root.is_absolute()
    {
        return Err("invalid qualification bounds".into());
    }
    for (path, directory) in [(&args.root, true), (&args.endpoint, false)] {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink()
            || metadata.is_dir() != directory
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err("qualification paths must be real owner-only paths".into());
        }
    }
    use std::os::unix::fs::OpenOptionsExt;
    let lease = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(args.root.join(".network-qualification.lock"))?;
    lease.try_lock()?;
    let endpoint = fs::read(&args.endpoint)?;
    let config: setup::Config = serde_json::from_slice(&endpoint)?;
    let campaign = json!({
        "format":1,"count":args.count,"writers":args.writers,"readers":args.readers,
        "endpoint_sha256":hex::encode(Sha256::digest(&endpoint)),
        "harness_sha256":hex::encode(Sha256::digest(fs::read(std::env::current_exe()?)?)),
        "payload":"u32-xor-v1-eight-sizes","mode":"api"
    });
    let manifest = args.root.join("network-campaign.json");
    if manifest.exists() {
        if serde_json::from_slice::<serde_json::Value>(&fs::read(manifest)?)? != campaign {
            return Err("campaign or frozen harness changed; refusing to reuse ACKs".into());
        }
    } else {
        publish(&manifest, &campaign)?;
    }
    let api = match args.phase {
        Phase::Populate => workload::Api::open(&config, "api").await?,
        Phase::Verify => workload::Api::attach(&config, "api").await?,
    };
    // Exercise the same authentication result as the existing campaign. Never
    // put tokens or administrator credentials in the public result file.
    let _ = api.login().await?;
    let (phase, result) = match args.phase {
        Phase::Populate => (
            "populate",
            workload::populate(api, &args.root, args.count, args.writers).await?,
        ),
        Phase::Verify => (
            "verify",
            workload::verify(api, &args.root, args.count, args.readers, "api").await?,
        ),
    };
    let output = args
        .root
        .join(format!("{phase}-{}.json", ulid::Ulid::new()));
    publish(
        &output,
        &json!({
            "phase":phase,"result":result,"server_lifecycle":"external operator orchestration",
            "faults_injected_by_this_client":false
        }),
    )?;
    println!("{}", json!({"phase":phase,"report":output,"passed":true}));
    Ok(())
}

fn publish(path: &std::path::Path, value: &serde_json::Value) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::File::open(path.parent().ok_or("missing output parent")?)?.sync_all()?;
    Ok(())
}
