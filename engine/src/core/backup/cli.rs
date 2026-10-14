use super::repository::Repository;
use super::{Result, invalid};
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "litiaina-n1-backup",
    about = "Encrypted offline N1 snapshots and N1-owned snapshot replication"
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
    /// Committed-state snapshot through the running engine; excludes unfinished uploads.
    CreateOnline {
        #[arg(long)]
        source_url: String,
        #[arg(long)]
        repository: String,
        #[arg(long)]
        token_file: PathBuf,
        #[arg(long)]
        ca_file: Option<PathBuf>,
        #[arg(long)]
        allow_plaintext: bool,
        #[arg(long, default_value_t = 3600)]
        timeout_seconds: u64,
    },
    /// N1-owned HTTP/native operator endpoint. No external server or SSH required.
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    /// Recover recorded restart intent after an interrupted operator job.
    RecoverOperator {
        #[arg(long)]
        config: PathBuf,
    },
    /// Generate a separate owner-only operator API token (never printed).
    OperatorKeygen {
        #[arg(long)]
        token_file: PathBuf,
    },
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
    Prune {
        #[command(flatten)]
        repo: RepoArgs,
        #[arg(long)]
        keep_last: usize,
        #[arg(long)]
        protect: String,
    },
    Keygen {
        #[arg(long)]
        key_file: PathBuf,
    },
    Init {
        #[command(flatten)]
        repo: RepoArgs,
    },
    /// Capture configured roots. N1 must remain stopped.
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
    /// Local repository or N1-owned HTTP(S)/N1(S) destination.
    Replicate {
        #[command(flatten)]
        repo: RepoArgs,
        #[arg(long)]
        snapshot: String,
        #[arg(long)]
        target_repository: PathBuf,
        #[arg(long)]
        target_key_file: Option<PathBuf>,
        #[arg(long)]
        target_url: Option<String>,
        #[arg(long, requires = "target_url")]
        token_file: Option<PathBuf>,
        #[arg(long, requires = "target_url")]
        ca_file: Option<PathBuf>,
        #[arg(long)]
        allow_plaintext: bool,
    },
    Forget {
        #[command(flatten)]
        repo: RepoArgs,
        #[arg(long)]
        snapshot: String,
    },
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
pub fn run() -> Result<()> {
    run_from(std::env::args_os().collect())
}
pub fn run_from(args: Vec<std::ffi::OsString>) -> Result<()> {
    match Cli::parse_from(args).command {
        Operation::CreateOnline {
            source_url,
            repository,
            token_file,
            ca_file,
            allow_plaintext,
            timeout_seconds,
        } => {
            let destination = super::network::Destination {
                url: source_url,
                repository: repository.clone(),
                token_file,
                ca_file,
                allow_plaintext,
            };
            let result = super::network::run_async(super::network::completed_operation(
                &destination,
                super::network::Request {
                    operation: "create-online".into(),
                    repository,
                    confirm: true,
                    ..Default::default()
                },
                timeout_seconds,
            ))?;
            super::online::validate_result(&result)?;
            println!("{result}");
        }
        Operation::Serve { config } => super::network::serve(&config)?,
        Operation::RecoverOperator { config } => super::network::recover_operator(&config)?,
        Operation::OperatorKeygen { token_file } => super::network::generate_token(&token_file)?,
        Operation::RunJob { config } => super::automation::run(&config)?,
        Operation::RecoverJob { state_dir } => super::automation::recover(&state_dir)?,
        Operation::JobStatus { config } => super::automation::status(&config)?,
        Operation::Prune {
            repo,
            keep_last,
            protect,
        } => super::retention::prune(&repo.open(true)?, keep_last, &protect)?,
        Operation::Keygen { key_file } => super::crypto::create_key(&key_file)?,
        Operation::Init { repo } => Repository::initialize(&repo.repository, &repo.key_file)?,
        Operation::Create {
            repo,
            config,
            workers,
        } => {
            let repo = repo.open(true)?;
            let s = super::capture::create(
                &repo,
                super::capture::roots_from_config(&config)?,
                workers,
            )?;
            println!(
                "{}",
                serde_json::json!({"snapshot":s.id,"records":s.records,"source_bytes":s.source_bytes})
            );
        }
        Operation::List { repo } => {
            for s in repo.open(false)?.list()? {
                println!(
                    "{}",
                    serde_json::json!({"snapshot":s.id,"created_at":s.created_at,"roots":s.roots.len(),"records":s.records,"source_bytes":s.source_bytes})
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
            super::restore::restore(
                &repo,
                &repo.snapshot(&snapshot)?,
                super::capture::roots_from_config(&target_config)?,
                resume,
            )?;
            println!("Restored {snapshot}; configure the destination engine before starting it");
        }
        Operation::Replicate {
            repo,
            snapshot,
            target_repository,
            target_key_file,
            target_url,
            token_file,
            ca_file,
            allow_plaintext,
        } => {
            if let Some(url) = target_url {
                let destination = super::network::Destination {
                    url,
                    repository: target_repository.to_string_lossy().into_owned(),
                    token_file: token_file
                        .ok_or_else(|| invalid("remote replication requires --token-file"))?,
                    ca_file,
                    allow_plaintext,
                };
                let store = super::network::Store {
                    path: repo.repository,
                    key_file: repo.key_file,
                };
                super::network::run_async(super::network::replicate(
                    store,
                    &snapshot,
                    &destination,
                ))?;
            } else {
                let repo = repo.open(false)?;
                let s = repo.snapshot(&snapshot)?;
                let target = Repository::open(
                    &target_repository,
                    &target_key_file
                        .ok_or_else(|| invalid("local replication requires --target-key-file"))?,
                    true,
                )?;
                super::transfer::local(&repo, &s, &target)?;
            }
            println!("Replicated {snapshot}");
        }
        Operation::Forget { repo, snapshot } => {
            super::retention::forget(&repo.open(true)?, &snapshot)?
        }
        Operation::Gc {
            repo,
            discard_incomplete,
        } => println!(
            "{}",
            serde_json::json!({"reclaimed_bytes":super::retention::gc(&repo.open(true)?,discard_incomplete)?})
        ),
        Operation::Receive { repo } => super::transfer::receive(
            &repo.open(true)?,
            &mut std::io::stdin().lock(),
            &mut std::io::stdout().lock(),
        )?,
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_transport_parses_without_ssh_and_rejects_ssh_flags() {
        assert!(
            Cli::try_parse_from([
                "backup",
                "replicate",
                "--repository",
                "/repo",
                "--key-file",
                "/key",
                "--snapshot",
                "id",
                "--target-repository",
                "replica",
                "--target-url",
                "https://backup:50902",
                "--token-file",
                "/token"
            ])
            .is_ok()
        );
        assert!(Cli::try_parse_from(["backup", "replicate", "--ssh", "backup@host"]).is_err());
    }
}
