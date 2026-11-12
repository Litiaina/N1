use super::api::{Api, Result, expect, publish, string};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::Path,
    time::{Duration, Instant},
};

pub async fn capture(api: &Api, root: &Path) -> Result<Value> {
    let base = format!("{}/maintenance/metadata/backups", api.base);
    if api
        .client
        .get(&base)
        .bearer_auth(&api.token)
        .send()
        .await?
        .status()
        != 403
    {
        return Err("ordinary fragment token could list operator backups".into());
    }
    let started = expect(
        api.client
            .post(&base)
            .bearer_auth(&api.operator)
            .send()
            .await?,
        202,
    )
    .await?;
    let id = string(&started, "id")?;
    ulid::Ulid::from_string(id)?;
    let deadline = Instant::now() + Duration::from_secs(180);
    let job = loop {
        let listing = expect(
            api.client
                .get(&base)
                .bearer_auth(&api.operator)
                .send()
                .await?,
            200,
        )
        .await?;
        let job = &listing["job"];
        if job["id"] != id {
            return Err("snapshot job identity changed".into());
        }
        match job["phase"].as_str() {
            Some("completed") => break job.clone(),
            Some("running") if Instant::now() < deadline => (),
            _ => return Err("snapshot did not complete within its deadline".into()),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut response = api
        .client
        .get(format!("{base}/{id}/download"))
        .bearer_auth(&api.operator)
        .send()
        .await?;
    if response.status() != 200 {
        return Err("snapshot download failed".into());
    }
    use std::os::unix::fs::OpenOptionsExt;
    let path = root.join(format!("{id}.n1meta"));
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)?;
    let mut bytes = 0u64;
    let mut hash = Sha256::new();
    while let Some(chunk) = response.chunk().await? {
        bytes += chunk.len() as u64;
        hash.update(&chunk);
        output.write_all(&chunk)?;
    }
    if job["bytes"].as_u64() != Some(bytes) {
        return Err("snapshot download length differs".into());
    }
    output.sync_all()?;
    fs::File::open(root)?.sync_all()?;
    let receipt = json!({"id":id,"job":job,"bytes":bytes,"sha256":hex::encode(hash.finalize()),
        "snapshot":path,"ordinary_fragment_refused":true});
    publish(&root.join(format!("{id}-snapshot.json")), &receipt)?;
    Ok(receipt)
}
