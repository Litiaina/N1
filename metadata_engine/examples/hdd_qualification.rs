//! Headless physical-storage qualification of N1's metadata foundation.
//! All data belongs to a newly created run directory; no existing database is adopted.
#[path = "hdd_qualification/mod.rs"]
mod hdd_qualification;

use clap::{Parser, Subcommand, ValueEnum};
use m1::{MetadataEngine, Options};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Run {
        #[arg(long)]
        root: PathBuf,
        #[arg(long, default_value_t = 1_000_000)]
        entries: usize,
        #[arg(long, default_value_t = 8)]
        writers: usize,
        #[arg(long, default_value_t = 4)]
        readers: usize,
        #[arg(long, default_value_t = 64)]
        batch: usize,
        #[arg(long, default_value_t = 100_000)]
        churn: usize,
    },
    Worker {
        #[arg(long)]
        root: PathBuf,
        #[arg(long, value_enum)]
        phase: Phase,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
enum Phase {
    Populate,
    Churn,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Populate => "populate",
            Self::Churn => "churn",
        }
    }
    fn count(self, plan: &Plan) -> usize {
        match self {
            Self::Populate => plan.entries,
            Self::Churn => plan.churn,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Plan {
    entries: usize,
    writers: usize,
    readers: usize,
    batch: usize,
    churn: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event")]
enum Event {
    Verified {
        records: usize,
        seconds: f64,
    },
    Ack {
        batch: usize,
        records: usize,
        created: usize,
        micros: u64,
    },
    Done {
        reader_checks: u64,
        rss_hwm_kib: Option<u64>,
    },
}

fn open(root: &Path) -> Result<MetadataEngine> {
    Ok(MetadataEngine::open(
        root.join("database"),
        Options {
            cache_bytes: 32 * 1024 * 1024,
            memtable_bytes: 1024 * 1024,
            journal_bytes: 64 * 1024 * 1024,
            background_workers: 2,
        },
    )?)
}

fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    use std::io::Write;
    let temporary = path.with_extension("pending");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    fs::File::open(path.parent().ok_or("missing report parent")?)?.sync_all()?;
    Ok(())
}

fn emit(event: Event) -> Result<()> {
    use std::io::Write;
    let mut output = std::io::stdout().lock();
    let mut frame = serde_json::to_vec(&event)?;
    frame.push(b'\n');
    output.write_all(&frame)?;
    output.flush()?;
    Ok(())
}

fn main() {
    let result = match Cli::parse().command {
        Command::Run {
            root,
            entries,
            writers,
            readers,
            batch,
            churn,
        } => hdd_qualification::supervisor::run(
            &root,
            Plan {
                entries,
                writers,
                readers,
                batch,
                churn,
            },
        ),
        Command::Worker { root, phase } => hdd_qualification::workload::worker(&root, phase),
    };
    if let Err(error) = result {
        eprintln!("metadata qualification failed: {error}");
        std::process::exit(1);
    }
}
