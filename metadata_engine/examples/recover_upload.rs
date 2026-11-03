//! Explicit offline recovery of a cancelled metadata session whose LINK parts
//! survived failed cleanup. Never writes, recreates, finalizes or deletes shards.
//! Run without --apply first, against a stopped database copy.
use clap::Parser;
use cli_core::{Credentials, Result, result};
use n1_link::{ObjectKey, UploadTicket, noa::Command};
use n1_metadata_engine::{
    Error, MetadataEngine, Options, RecordKey, Table, fragments, object_store::ObjectUpload,
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    database: PathBuf,
    #[arg(long)]
    fragment: String,
    #[arg(long)]
    version: String,
    #[arg(long)]
    object_credentials: PathBuf,
    #[arg(long)]
    ca: PathBuf,
    #[arg(long)]
    apply: bool,
}
fn recoverable(pending: &Value, saved: &Value, now: u64) -> Result<ObjectUpload> {
    if pending["completed"] != false || saved["cancelled"] != true || saved["bridge"] != false {
        return Err(
            "requires an incomplete, cancelled, directly streamed multipart session".into(),
        );
    }
    let upload: ObjectUpload = serde_json::from_value(pending["upload"].clone())?;
    if upload.expires_at <= now || upload.parts == 0 || upload.parts > 4096 {
        return Err("ticket expired or exceeds this bounded recovery tool's part limit".into());
    }
    let ticket = UploadTicket::inspect(&upload.upload_key)?;
    if ticket.identity != ObjectKey::inspect(&upload.object_key)?
        || ticket.total_bytes != upload.total_bytes
        || ticket.part_bytes != upload.part_bytes
        || ticket.parts() != upload.parts
        || ticket.expires_at != upload.expires_at
        || pending["handle"]["version_id"] != saved["handle"]["version_id"]
        || pending["handle"]["session_id"] != saved["handle"]["session_id"]
        || saved["handle"]["total_size_bytes"].as_u64() != Some(upload.total_bytes)
        || saved["handle"]["part_size_bytes"].as_u64() != Some(upload.part_bytes)
        || saved["handle"]["expected_parts"].as_u64() != Some(upload.parts)
    {
        return Err("upload identity or reservation mismatch".into());
    }
    Ok(upload)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !args.database.join("version").is_file() {
        return Err("existing stopped database required".into());
    }
    let db = MetadataEngine::open(args.database, Options::default())?;
    let fragment = fragments::resolve(&db, &args.fragment)?;
    let pending_key = RecordKey::new(fragment.id, &args.version)?;
    let plan_key = RecordKey::new(fragment.id, format!("upload-plan/{}", args.version))?;
    let intent_key = RecordKey::new(fragment.id, format!("pending/{}", args.version))?;
    let snapshot = db.snapshot();
    let pending_bytes = snapshot
        .get(Table::Uploads, &pending_key)?
        .ok_or("missing reservation")?;
    let plan_bytes = snapshot
        .get(Table::Reclamation, &plan_key)?
        .ok_or("missing multipart plan")?;
    if snapshot.get(Table::Reclamation, &intent_key)?.is_some() {
        return Err("session already has an active publication intent".into());
    }
    let pending: Value = serde_json::from_slice(&pending_bytes)?;
    let mut saved: Value = serde_json::from_slice(&plan_bytes)?;
    if pending["handle"]["version_id"] != args.version {
        return Err("version differs from reservation key".into());
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let upload = recoverable(&pending, &saved, now)?;
    let mut native = Credentials::load(&args.object_credentials)?
        .connect(None, Some(&args.ca))
        .await?;
    let status = result(
        native
            .control(Command::UploadStatus {
                upload_key: upload.upload_key.clone(),
                start: 0,
                limit: 4096,
            })
            .await?,
    )?;
    if status["finalized"] != false
        || status["parts"].as_u64() != Some(upload.parts)
        || status["checked_start"] != 0
        || status["checked_end"].as_u64() != Some(upload.parts)
        || status["unavailable_members"]
            .as_array()
            .is_none_or(|members| !members.is_empty())
    {
        return Err("native status is incomplete, unavailable or already finalized".into());
    }
    let missing = status["missing_parts"]
        .as_array()
        .ok_or("missing native part status")?;
    let verified = upload
        .parts
        .checked_sub(missing.len() as u64)
        .ok_or("invalid missing count")?;
    if verified == 0 {
        return Err("no surviving verified parts; cannot claim resumability".into());
    }
    let before = saved.clone();
    saved["cancelled"] = json!(false);
    if args.apply {
        db.transact(1, |tx| {
            if tx.get(Table::Uploads, &pending_key)?.as_ref() != Some(&pending_bytes)
                || tx.get(Table::Reclamation, &plan_key)?.as_ref() != Some(&plan_bytes)
                || tx.get(Table::Reclamation, &intent_key)?.is_some()
            {
                return Err(Error::Conflict);
            }
            tx.put(
                Table::Reclamation,
                &plan_key,
                &serde_json::to_vec(&saved)
                    .map_err(|_| Error::Invalid("invalid recovery plan".into()))?,
            )?;
            tx.put(
                Table::Reclamation,
                &intent_key,
                &serde_json::to_vec(
                    &json!({"version":args.version,"session":pending["handle"]["session_id"]}),
                )
                .map_err(|_| Error::Invalid("invalid recovery intent".into()))?,
            )
        })?;
    }
    println!(
        "{}",
        json!({"checked":true,"applied":args.apply,"handle":before["handle"],
        "verified_surviving_parts":verified,"missing_parts":missing.len(),"shard_bytes_modified":false,
        "identity_preserved":true,"publication_intent_restored":args.apply})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_requires_the_original_unexpired_object_and_ticket_identity() {
        use n1_link::{Durability, Identity, KeySigner, PoolId, UploadPlan};
        let signer = KeySigner::new([29; 32]);
        let identity = Identity {
            pool: PoolId([11; 16]),
            generation: 1,
            token: [7; 32],
        };
        let plan = UploadPlan {
            identity,
            total_bytes: 8192,
            part_bytes: 4096,
            expires_at: 100,
            durability: Durability::Durable,
        };
        let ticket = signer.upload_ticket(&[0; 16], plan);
        let object = signer.object_key(&[0; 16], identity);
        let handle = json!({"version_id":"v","session_id":"s","total_size_bytes":8192,"part_size_bytes":4096,"expected_parts":2});
        let mut pending = json!({"completed":false,"handle":handle,"upload":{
            "upload_key":ticket.as_str(),"object_key":object.as_str(),"total_bytes":8192,"part_bytes":4096,
            "parts":2,"expires_at":100,"durable":true}});
        let saved = json!({"cancelled":true,"bridge":false,"handle":handle});
        assert!(recoverable(&pending, &saved, 99).is_ok());
        assert!(recoverable(&pending, &saved, 100).is_err());
        pending["upload"]["object_key"] = json!(
            signer
                .object_key(
                    &[0; 16],
                    Identity {
                        token: [8; 32],
                        ..identity
                    }
                )
                .as_str()
        );
        assert!(recoverable(&pending, &saved, 99).is_err());
    }
    #[test]
    fn recovery_refuses_completed_active_or_bridged_sessions() {
        for (completed, cancelled, bridge) in [
            (true, true, false),
            (false, false, false),
            (false, true, true),
        ] {
            assert!(
                recoverable(
                    &json!({"completed":completed}),
                    &json!({"cancelled":cancelled,"bridge":bridge}),
                    1
                )
                .is_err()
            );
        }
    }
}
