//! Adapt the retained Console host operations to the tested LINK CLI/native API.
//! No shard, EC, retirement, snapshot encryption or restore algorithm lives here.
use super::{
    Request,
    config::{Config, Store, identity},
};
use cli_core::Result;
use serde_json::{Value, json};
use std::{collections::BTreeSet, ffi::OsString};
use tokio::io::AsyncReadExt;

async fn output(mut input: impl tokio::io::AsyncRead + Unpin) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    (&mut input)
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 1024 * 1024 {
        return Err("operator child output exceeded 1 MiB".into());
    }
    Ok(bytes)
}
async fn cli(config: &Config, args: Vec<OsString>) -> Result<Value> {
    use std::process::Stdio;
    let mut child = tokio::process::Command::new(config.engine()?)
        .arg("backup")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().ok_or("missing child stdout")?;
    let stderr = child.stderr.take().ok_or("missing child stderr")?;
    let (out, err, status) = tokio::join!(output(stdout), output(stderr), child.wait());
    let status = status?;
    let err = err?;
    let out = out?;
    if !status.success() {
        return Err(format!(
            "N1 backup command failed ({status}): {}",
            String::from_utf8_lossy(&err)
                .chars()
                .take(4096)
                .collect::<String>()
        )
        .into());
    }
    Ok(serde_json::from_slice(&out)?)
}
fn repository(command: &str, store: &Store) -> Vec<OsString> {
    vec![
        command.into(),
        "--repository".into(),
        store.path.as_os_str().into(),
        "--key-file".into(),
        store.key_file.as_os_str().into(),
    ]
}
fn flag(args: &mut Vec<OsString>, name: &str, value: impl AsRef<std::ffi::OsStr>) {
    args.push(name.into());
    args.push(value.as_ref().into());
}
async fn online(config: &Config, request: &Request) -> Result<Value> {
    use n1_link::noa::Command;
    let source = config
        .online_source
        .as_ref()
        .ok_or("online source is not configured")?;
    if request.repository != source.repository {
        return Err("online capture requires its configured source repository alias".into());
    }
    // One authenticated connection owns capture and its bounded status polls.
    // A long backup must not create one password session per polling interval.
    let credentials = cli_core::Credentials::load(&source.credentials_file)?;
    let mut client = credentials.connect(None, source.ca_file.as_deref()).await?;
    let job = cli_core::result(
        client
            .control(Command::BackupCapture {
                repository: source.repository.clone(),
                boundary_seconds: config.capture_boundary_seconds,
            })
            .await?,
    )?;
    let id = job["id"]
        .as_str()
        .ok_or("source returned no capture job identity")?;
    loop {
        let status = cli_core::result(client.control(Command::BackupJob).await?)?;
        if status["id"] != id {
            return Err("source capture job changed; outcome is unconfirmed".into());
        }
        match status["phase"].as_str() {
            Some("completed") => {
                return status
                    .get("result")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .ok_or_else(|| "source completed without a recovery point".into());
            }
            Some("running" | "stopping") => {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await
            }
            _ => {
                return Err(format!("source capture did not complete: {}", status["error"]).into());
            }
        }
    }
}
async fn list(request: &Request, store: &Store) -> Result<Value> {
    // Preserve the original descending/keyset Console inventory, with O(limit)
    // retained names. The engine remains responsible for authenticated manifests.
    if !request.cursor.is_empty() {
        identity(&request.cursor)?;
    }
    let limit = if request.limit == 0 {
        20
    } else {
        request.limit.min(100)
    };
    let store = store.clone();
    let cursor = request.cursor.clone();
    tokio::task::spawn_blocking(move ||->Result<Value> {
        let repo=n1::core::backup::repository::Repository::open(&store.path,&store.key_file,false)?;
        let mut names=BTreeSet::new();
        for entry in std::fs::read_dir(repo.path.join("snapshots"))? {
            let entry=entry?;
            if !entry.file_type()?.is_dir() {return Err("invalid recovery-point directory".into());}
            let id=entry.file_name().into_string().map_err(|_|"non-UTF8 recovery-point identity")?;identity(&id)?;
            if cursor.is_empty()||id<cursor {names.insert(id);if names.len()>limit+1 {names.pop_first();}}
        }
        let has_more=names.len()>limit;if has_more {names.pop_first();}
        let cursor=names.first().cloned();let mut snapshots=Vec::new();
        for id in names.into_iter().rev() {
            let point=repo.snapshot(&id)?;
            snapshots.push(json!({"snapshot":point.id,"created_at":point.created_at,"members":point.roots.len(),"source_bytes":point.source_bytes,"records":point.records,"capture_mode":point.capture_mode}));
        }
        Ok(json!({"snapshots":snapshots,"has_more":has_more,"cursor":if has_more {cursor}else{None}}))
    }).await?
}
pub(super) async fn execute(config: &Config, request: &Request) -> Result<Value> {
    if matches!(request.operation.as_str(), "run-job" | "job-status") {
        let path = config
            .jobs
            .get(&request.job)
            .ok_or("unknown supervised job alias")?;
        let mut args = vec![request.operation.clone().into()];
        flag(&mut args, "--config", path);
        let result = cli(config, args).await?;
        if request.operation == "run-job" && result["enabled"] == false {
            return Err("configured backup job is disabled".into());
        }
        return Ok(result);
    }
    let store = config
        .repositories
        .get(&request.repository)
        .ok_or("unknown repository alias")?;
    if request.operation == "list" {
        return list(request, store).await;
    }
    if request.operation == "forget" {
        let store = store.clone();
        let id = request.snapshot.clone();
        return tokio::task::spawn_blocking(move || -> Result<Value> {
            let repo =
                n1::core::backup::repository::Repository::open(&store.path, &store.key_file, true)?;
            n1::core::backup::retention::forget(&repo, &id)?;
            Ok(json!({"forgotten":id}))
        })
        .await?;
    }
    if request.operation == "create-online" {
        return online(config, request).await;
    }
    let command = match request.operation.as_str() {
        "init" => "init",
        "create" => "create",
        "verify" => "verify",
        "replicate" => "replicate-remote",
        "restore" => "restore",
        "gc" => "gc",
        "prune" => "prune",
        _ => return Err("unsupported operator operation".into()),
    };
    let mut args = repository(command, store);
    if matches!(command, "verify" | "replicate-remote" | "restore") {
        flag(&mut args, "--snapshot", &request.snapshot);
    }
    match command {
        "create" => flag(
            &mut args,
            "--config",
            config
                .engine_config
                .as_ref()
                .ok_or("offline source engine_config is not configured")?,
        ),
        "replicate-remote" => {
            let dest = config
                .replicas
                .get(&request.target)
                .ok_or("unknown replica alias")?;
            let credentials = cli_core::Credentials::load(&dest.credentials_file)?;
            flag(
                &mut args,
                "--endpoint",
                credentials
                    .native_url
                    .as_deref()
                    .ok_or("native destination URL missing")?,
            );
            flag(&mut args, "--credentials", &dest.credentials_file);
            flag(&mut args, "--destination", &dest.repository);
            if let Some(ca) = &dest.ca_file {
                flag(&mut args, "--ca", ca);
            }
        }
        "restore" => {
            let target = config
                .restore_targets
                .get(&request.target)
                .ok_or("unknown restore target alias")?;
            let settings = cli_core::settings::N1Settings::load(target)?;
            for pool in settings.pools {
                for root in pool.roots {
                    flag(&mut args, "--target", root);
                }
            }
            if request.resume {
                args.push("--resume".into());
            }
        }
        "prune" => {
            flag(&mut args, "--keep-last", request.keep_last.to_string());
            flag(&mut args, "--protect", &request.snapshot);
        }
        _ => (),
    }
    cli(config, args).await
}

pub(super) fn validate(config: &Config, request: &Request) -> Result<()> {
    if !matches!(
        request.operation.as_str(),
        "capabilities"
            | "operations"
            | "list"
            | "job-status"
            | "init"
            | "create"
            | "create-online"
            | "verify"
            | "replicate"
            | "restore"
            | "forget"
            | "gc"
            | "prune"
            | "run-job"
    ) {
        return Err("unknown backup operation".into());
    }
    if matches!(request.operation.as_str(), "capabilities" | "operations") {
        return Ok(());
    }
    if matches!(request.operation.as_str(), "run-job" | "job-status") {
        if !config.jobs.contains_key(&request.job) {
            return Err("unknown supervised job alias".into());
        }
    } else if !config.repositories.contains_key(&request.repository) {
        return Err("unknown repository alias".into());
    }
    if matches!(
        request.operation.as_str(),
        "verify" | "replicate" | "restore" | "forget" | "prune"
    ) {
        identity(&request.snapshot)?;
    }
    if request.operation == "replicate" && !config.replicas.contains_key(&request.target) {
        return Err("unknown replica alias".into());
    }
    if request.operation == "restore" && !config.restore_targets.contains_key(&request.target) {
        return Err("unknown restore target alias".into());
    }
    if request.operation == "prune" && !(1..=10000).contains(&request.keep_last) {
        return Err("invalid retention count".into());
    }
    if !matches!(
        request.operation.as_str(),
        "capabilities" | "operations" | "list" | "job-status" | "verify" | "replicate"
    ) && !request.confirm
    {
        return Err("explicit confirmation required".into());
    }
    Ok(())
}
