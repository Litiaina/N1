//! Coordinate an actual browser on an operator-controlled second host. The
//! marker only admits the next phase: census and every ACK/version/byte check
//! remain mandatory in the existing Rust supervisor.
use super::{Args, Result, workload::Api};
use serde_json::{Value, json};
use std::{
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
    time::Duration,
};

fn private_json(path: &Path) -> Result<Value> {
    let meta = std::fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no pointer arguments or ownership requirements.
    let uid = unsafe { libc::geteuid() };
    if !meta.is_file()
        || meta.uid() != uid
        || meta.permissions().mode() & 0o077 != 0
        || meta.len() > 4 * 1024 * 1024
    {
        return Err("external browser evidence must be a private regular file under 4 MiB".into());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}
pub(super) async fn run(args: &Args, api: &Api, url: &str) -> Result<Value> {
    let nonce = ulid::Ulid::new().to_string();
    let client = json!({
        "format": 1,
        "nonce": nonce,
        "count": args.count,
        "fragment": api.fragment,
        "api_url": url,
        "auth": api.login().await?
    });
    let target = args.root.join("external-console-client.json");
    let temporary = args
        .root
        .join(format!("external-console-client.{nonce}.tmp"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec_pretty(&client)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, &target)?;
    std::fs::File::open(&args.root)?.sync_all()?;
    println!(
        "{}",
        json!({"event":"external-console-ready","private_client_file":target,"count":args.count})
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20 * 3600);
    let marker = args.root.join("console-external-complete.json");
    loop {
        if marker.try_exists()? {
            let completion = private_json(&marker)?;
            if completion["nonce"] == nonce {
                if completion["passed"] != true
                    || completion["count"] != args.count
                    || completion["fragment"] != api.fragment
                {
                    return Err("external browser completion does not match this campaign".into());
                }
                let report = private_json(&args.root.join("console-browser-PASSED.json"))?;
                let prior = report["priorAcknowledged"]
                    .as_u64()
                    .ok_or("browser prior ACK count missing")?;
                if report["passed"] != true
                    || report["count"] != args.count
                    || report["acknowledged"] != args.count
                    || prior > args.count
                {
                    return Err(
                        "external browser did not acknowledge this entire population".into(),
                    );
                }
                return Ok(json!({
                    "prior_acknowledged": prior,
                    "acknowledged_this_invocation": args.count-prior,
                    "total_acknowledged": args.count,
                    "browser_host": "external; original console and proxy through operator SSH forwarding"
                }));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("external browser did not complete within twenty hours".into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
