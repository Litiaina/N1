//! Inspect a stopped database copy without printing LINK capabilities or secrets.
//! Opening an active database is rejected by the normal exclusive database lock.
use clap::Parser;
use m1::{MetadataEngine, Options, RecordKey, Table, fragments};
use serde_json::{Value, json};
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    snapshot: PathBuf,
    #[arg(long)]
    fragment: String,
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if !args.snapshot.join("version").is_file() {
        return Err("existing database copy required".into());
    }
    let db = MetadataEngine::open(args.snapshot, Options::default())?;
    let fragment = fragments::resolve(&db, &args.fragment)?;
    let snapshot = db.snapshot();
    let mut after = None;
    let mut rows = Vec::new();
    loop {
        let page = snapshot.scan(Table::Uploads, fragment.id, "", after.as_ref(), 1000)?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|(key, _)| key.clone());
        for (key, bytes) in page {
            let pending: Value = serde_json::from_slice(&bytes)?;
            let version = key.identifier();
            let plan = snapshot
                .get(
                    Table::Reclamation,
                    &RecordKey::new(fragment.id, format!("upload-plan/{version}"))?,
                )?
                .map(|bytes| serde_json::from_slice::<Value>(&bytes))
                .transpose()?;
            let has_intent = snapshot
                .get(
                    Table::Reclamation,
                    &RecordKey::new(fragment.id, format!("pending/{version}"))?,
                )?
                .is_some();
            rows.push(json!({
                "handle": pending["handle"],
                "completed": pending["completed"],
                "created_at_unix": pending["created_at_unix"],
                "expires_at": pending["upload"]["expires_at"],
                "has_publication_intent": has_intent,
                "has_multipart_plan": plan.is_some(),
                "cancelled": plan.as_ref().map(|p|p["cancelled"].clone()),
                "durable_mode": plan.as_ref().map(|p|p["durable"].clone())
            }));
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"fragment_name":fragment.name,"uploads":rows}))?
    );
    Ok(())
}
