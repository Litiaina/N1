//! External ACK recorder. It neither owns server processes nor switches power.
#[path = "power_loss_witness/api.rs"]
mod api;
#[path = "power_loss_witness/journal.rs"]
mod journal;
#[path = "power_loss_witness/workload.rs"]
mod workload;

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum Backend {
    N1,
    M1,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    backend: Backend,
    url: String,
    ca: PathBuf,
    secret_env: PathBuf,
    fragment: String,
    /// Explicit native-aligned geometry, bounded independently of server limits.
    part_bytes: u64,
    /// Longer reception windows for operator-controlled physical interruptions.
    #[serde(
        default = "default_payload_parts",
        skip_serializing_if = "is_default_payload_parts"
    )]
    payload_parts: u64,
}

fn default_payload_parts() -> u64 {
    3
}
fn is_default_payload_parts(value: &u64) -> bool {
    *value == default_payload_parts()
}

#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    Write,
    Verify,
    Recover,
    Audit,
}

#[derive(Parser)]
struct Args {
    #[arg(value_enum)]
    mode: Mode,
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    journal: PathBuf,
    /// A new report is required for each verification/recovery attempt.
    #[arg(long)]
    report: Option<PathBuf>,
    #[arg(long, default_value_t = 32)]
    objects: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("power-loss witness stopped: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    let config: Config = serde_json::from_slice(&fs::read(&args.config)?)?;
    if !(1..=4096).contains(&args.objects)
        || !(4096..=8 << 20).contains(&config.part_bytes)
        || !config.part_bytes.is_multiple_of(4096)
        || !(3..=127).contains(&config.payload_parts)
        || args.objects.saturating_mul(config.payload_parts + 8) > 32768
        || config.fragment.is_empty()
        || !args.journal.is_absolute()
        || (!matches!(args.mode, Mode::Write) && args.report.is_none())
        || args
            .report
            .as_ref()
            .is_some_and(|path| !path.is_absolute() || path.exists())
    {
        return Err("invalid recorder bounds, journal path or missing report".into());
    }
    let binding = format!("{:x}", Sha256::digest(serde_json::to_vec(&config)?));
    let mut journal = match args.mode {
        Mode::Write => journal::Journal::create(&args.journal, &binding)?,
        Mode::Verify | Mode::Recover | Mode::Audit => {
            journal::Journal::open(&args.journal, &binding)?
        }
    };
    let mut api = api::Api::connect(&config).await?;
    match args.mode {
        Mode::Write => {
            workload::write(
                &mut api,
                &mut journal,
                config.part_bytes,
                config.payload_parts,
                args.objects,
            )
            .await?;
        }
        Mode::Verify | Mode::Recover => {
            // Verification is always complete before any recovery mutation.
            let before = workload::verify(&mut api, &journal).await?;
            let recovery = if matches!(args.mode, Mode::Recover) {
                workload::recover(&mut api, &mut journal).await?;
                Some(workload::verify(&mut api, &journal).await?)
            } else {
                None
            };
            journal::save_new(
                args.report.as_ref().unwrap(),
                &json!({"passed":true,"before_recovery":before,"after_recovery":recovery,
                    "journal_records":journal.records.len(),"config_sha256":binding,
                    "physical_power_cut_proven":false}),
            )?;
        }
        Mode::Audit => {
            let audited = workload::audit(&mut api, &journal).await?;
            journal::save_new(
                args.report.as_ref().unwrap(),
                &json!({
                    "passed":true,"deep_audit":audited,"config_sha256":binding,
                    "journal_records":journal.records.len()
                }),
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_shape_preserves_existing_journal_configuration_bindings() {
        let original = r#"{"backend":"n1","url":"n1s://localhost:57612","ca":"/test/ca.pem","secret_env":"/test/n1.env","fragment":"root","part_bytes":6291456}"#;
        let config: Config = serde_json::from_str(original).unwrap();
        assert_eq!(config.payload_parts, 3);
        assert_eq!(serde_json::to_string(&config).unwrap(), original);
        let extended = Config {
            payload_parts: 63,
            ..config
        };
        assert_eq!(serde_json::to_value(extended).unwrap()["payload_parts"], 63);
    }
}
