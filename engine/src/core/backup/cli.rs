use super::repository::Repository;
use super::{Result, invalid};
use clap::{Args, Parser, Subcommand};
use std::io::BufReader;
use std::path::PathBuf;
use std::process::{Command, Stdio};

#[derive(Parser)]
#[command(
    name = "litiaina-n1-backup",
    about = "Encrypted offline N1 snapshots and one-way snapshot replication"
)]
struct Cli {
    #[command(subcommand)]
    command: Operation,
}

#[derive(Args)]
struct RepoArgs {
    #[arg(long)]
    repository: PathBuf,
    #[arg(long)]
    key_file: PathBuf,
}
impl RepoArgs {
    fn open(&self, writable: bool) -> Result<Repository> {
        Repository::open(&self.repository, &self.key_file, writable)
    }
}

#[derive(Subcommand)]
enum Operation {
    /// Generate a random 256-bit encryption key. Keep a separate protected copy.
    Keygen {
        #[arg(long)]
        key_file: PathBuf,
    },
    /// Initialize an empty owner-only repository.
    Init {
        #[command(flatten)]
        repo: RepoArgs,
    },
    /// Capture configured roots. The cooperating N1 engine MUST be stopped.
    Create {
        #[command(flatten)]
        repo: RepoArgs,
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 4)]
        workers: usize,
    },
    List {
        #[command(flatten)]
        repo: RepoArgs,
    },
    Verify {
        #[command(flatten)]
        repo: RepoArgs,
        #[arg(long)]
        snapshot: String,
    },
    /// Restore to empty roots; use --resume only for an interrupted matching restore.
    Restore {
        #[command(flatten)]
        repo: RepoArgs,
        #[arg(long)]
        snapshot: String,
        #[arg(long)]
        target_config: PathBuf,
        #[arg(long)]
        resume: bool,
    },
    /// Replicate a snapshot to a local/mounted repository or an SSH receiver.
    Replicate {
        #[command(flatten)]
        repo: RepoArgs,
        #[arg(long)]
        snapshot: String,
        #[arg(long)]
        target_repository: PathBuf,
        #[arg(long)]
        target_key_file: PathBuf,
        /// SSH target user@host; uses standard SSH keys/config/host verification.
        #[arg(long)]
        ssh: Option<String>,
        /// Optional OpenSSH configuration, useful for dedicated backup identities.
        #[arg(long, requires = "ssh")]
        ssh_config: Option<PathBuf>,
        #[arg(long, default_value = "litiaina-n1-backup")]
        remote_binary: String,
    },
    /// Forget one completed snapshot; refuses to remove the last recovery point.
    Forget {
        #[command(flatten)]
        repo: RepoArgs,
        #[arg(long)]
        snapshot: String,
    },
    /// Reclaim unreferenced blocks. Interrupted job pins are retained unless requested.
    Gc {
        #[command(flatten)]
        repo: RepoArgs,
        #[arg(long)]
        discard_incomplete: bool,
    },
    #[command(hide = true)]
    Receive {
        #[command(flatten)]
        repo: RepoArgs,
    },
}

fn quoted(value: &str) -> Result<String> {
    if value.contains('\0') || value.contains('\n') || value.is_empty() {
        return Err(invalid("invalid SSH command argument"));
    }
    Ok(format!("'{}'", value.replace('\'', "'\\''")))
}

pub fn run() -> Result<()> {
    match Cli::parse().command {
        Operation::Keygen { key_file } => super::crypto::create_key(&key_file)?,
        Operation::Init { repo } => Repository::initialize(&repo.repository, &repo.key_file)?,
        Operation::Create {
            repo,
            config,
            workers,
        } => {
            let repo = repo.open(true)?;
            let roots = super::capture::roots_from_config(&config)?;
            eprintln!(
                "Capturing {} offline roots with {} workers",
                roots.len(),
                workers.clamp(1, 32)
            );
            let snapshot = super::capture::create(&repo, roots, workers)?;
            println!(
                "{}",
                serde_json::json!({"snapshot": snapshot.id, "records": snapshot.records, "source_bytes": snapshot.source_bytes})
            );
        }
        Operation::List { repo } => {
            for snapshot in repo.open(false)?.list()? {
                println!(
                    "{}",
                    serde_json::json!({"snapshot":snapshot.id,"created_at":snapshot.created_at,"roots":snapshot.roots.len(),"records":snapshot.records,"source_bytes":snapshot.source_bytes})
                );
            }
        }
        Operation::Verify { repo, snapshot } => {
            let repo = repo.open(false)?;
            super::restore::verify(&repo, &repo.snapshot(&snapshot)?)?;
            println!("Verified {snapshot}");
        }
        Operation::Restore {
            repo,
            snapshot,
            target_config,
            resume,
        } => {
            let repo = repo.open(false)?;
            let target = super::capture::roots_from_config(&target_config)?;
            super::restore::restore(&repo, &repo.snapshot(&snapshot)?, target, resume)?;
            println!("Restored {snapshot}; configure the destination engine before starting it");
        }
        Operation::Replicate {
            repo,
            snapshot,
            target_repository,
            target_key_file,
            ssh,
            ssh_config,
            remote_binary,
        } => {
            let repo = repo.open(false)?;
            let snapshot = repo.snapshot(&snapshot)?;
            if let Some(host) = ssh {
                if host.starts_with('-')
                    || host.contains(char::is_whitespace)
                    || host.contains('\0')
                {
                    return Err(invalid("invalid SSH target"));
                }
                let remote = format!(
                    "{} receive --repository {} --key-file {}",
                    quoted(&remote_binary)?,
                    quoted(
                        target_repository
                            .to_str()
                            .ok_or_else(|| invalid("invalid remote path"))?
                    )?,
                    quoted(
                        target_key_file
                            .to_str()
                            .ok_or_else(|| invalid("invalid remote key path"))?
                    )?
                );
                let mut command = Command::new("ssh");
                if let Some(config) = ssh_config {
                    command.arg("-F").arg(config);
                }
                let mut child = command
                    .args([
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "ConnectTimeout=15",
                        "-o",
                        "ServerAliveInterval=15",
                        "-o",
                        "ServerAliveCountMax=3",
                        "--",
                        &host,
                        &remote,
                    ])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()?;
                let result = super::transfer::send(
                    &repo,
                    &snapshot,
                    &mut BufReader::new(
                        child
                            .stdout
                            .take()
                            .ok_or_else(|| invalid("missing SSH output"))?,
                    ),
                    &mut child
                        .stdin
                        .take()
                        .ok_or_else(|| invalid("missing SSH input"))?,
                );
                if result.is_err() {
                    let _ = child.kill();
                }
                let status = child.wait()?;
                result?;
                if !status.success() {
                    return Err(invalid("SSH receiver failed"));
                }
            } else {
                let target = Repository::open(&target_repository, &target_key_file, true)?;
                super::transfer::local(&repo, &snapshot, &target)?;
            }
            println!("Replicated {}", snapshot.id);
        }
        Operation::Forget { repo, snapshot } => {
            super::retention::forget(&repo.open(true)?, &snapshot)?
        }
        Operation::Gc {
            repo,
            discard_incomplete,
        } => {
            println!(
                "{}",
                serde_json::json!({"reclaimed_bytes":super::retention::gc(&repo.open(true)?, discard_incomplete)?})
            );
        }
        Operation::Receive { repo } => super::transfer::receive(
            &repo.open(true)?,
            &mut std::io::stdin().lock(),
            &mut std::io::stdout().lock(),
        )?,
    }
    Ok(())
}
