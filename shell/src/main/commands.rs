use super::*;

pub(super) async fn run_fragment_command(
    client: &mut N2NativeClient,
    command: FragmentCommand,
    json_output: bool,
) -> NativeClientResult<()> {
    match command {
        FragmentCommand::List => print_op(client, "fragment.list", json!({}), json_output).await,
        FragmentCommand::Check { fragment } => {
            print_op(
                client,
                "fragment.check",
                json!({ "fragment": fragment }),
                json_output,
            )
            .await
        }
        FragmentCommand::Hash { fragment } => {
            print_op(
                client,
                "fragment.hash.public",
                json!({ "fragment": fragment }),
                json_output,
            )
            .await
        }
        FragmentCommand::Create {
            name,
            key,
            master_key,
        } => {
            print_op(
                client,
                "fragment.create",
                json!({
                    "name": name,
                    "key": key,
                    "secret": master_key,
                }),
                json_output,
            )
            .await
        }
    }
}

pub(super) async fn run_object_command(
    client: &mut N2NativeClient,
    command: ObjectCommand,
    json_output: bool,
) -> NativeClientResult<()> {
    match command {
        ObjectCommand::Meta {
            object_key,
            version_id,
        } => {
            print_op(
                client,
                "object.meta",
                json!({ "object_key": object_key, "version_id": version_id }),
                json_output,
            )
            .await
        }
        ObjectCommand::Versions { object_key } => {
            print_op(
                client,
                "object.versions",
                json!({ "object_key": object_key }),
                json_output,
            )
            .await
        }
        ObjectCommand::Status { object_key, mode } => {
            print_op(
                client,
                "object.status",
                json!({ "object_key": object_key, "mode": mode }),
                json_output,
            )
            .await
        }
        ObjectCommand::Repair { object_key, mode } => {
            print_op(
                client,
                "object.repair",
                json!({ "object_key": object_key, "mode": mode }),
                json_output,
            )
            .await
        }
        ObjectCommand::Redistribute {
            object_key,
            version_id,
            dry_run,
            force,
            mode,
        } => {
            print_op(
                client,
                "object.redistribute",
                json!({
                    "object_key": object_key,
                    "version_id": version_id,
                    "dry_run": dry_run,
                    "force": force,
                    "mode": mode,
                }),
                json_output,
            )
            .await
        }
    }
}

pub(super) async fn run_uploads_command(
    client: &mut N2NativeClient,
    command: UploadsCommand,
    json_output: bool,
) -> NativeClientResult<()> {
    match command {
        UploadsCommand::List { limit, durable } => {
            print_op(
                client,
                if durable {
                    "uploads.durable.list"
                } else {
                    "uploads.standard.list"
                },
                json!({ "limit": limit }),
                json_output,
            )
            .await
        }
        UploadsCommand::Status {
            version_id,
            durable,
        } => {
            print_op(
                client,
                if durable {
                    "uploads.durable.status"
                } else {
                    "uploads.standard.status"
                },
                json!({ "version_id": version_id }),
                json_output,
            )
            .await
        }
        UploadsCommand::Cancel {
            version_id,
            object_key,
            durable,
        } => {
            print_op(
                client,
                if durable {
                    "upload.multipart.durable.cancel"
                } else {
                    "upload.multipart.standard.cancel"
                },
                json!({ "version_id": version_id, "object_key": object_key }),
                json_output,
            )
            .await
        }
    }
}

pub(super) async fn run_gc_command(
    client: &mut N2NativeClient,
    args: JobArgs,
    json_output: bool,
) -> NativeClientResult<()> {
    match args.action {
        JobAction::Plan | JobAction::Status => {
            print_op(client, "maintenance.gc.status", json!({}), json_output).await
        }
        JobAction::Watch { interval_seconds } => {
            watch_job(
                client,
                "maintenance.gc",
                interval_seconds.max(1),
                json_output,
            )
            .await
        }
        JobAction::Stop => print_op(client, "maintenance.gc.stop", json!({}), json_output).await,
        JobAction::Start {
            target_fragment: _,
            prefix: _,
            limit: _,
            dry_run: _,
            mismatched_only: _,
            max_duration_seconds: _,
            max_objects: _,
            max_directories: _,
            max_bytes: _,
            continuation_token: _,
            priority: _,
        } => {
            print_op(
                client,
                "maintenance.gc.start",
                json!({ "mode": "scrub", "fast_mode": false }),
                json_output,
            )
            .await
        }
    }
}

pub(super) async fn run_job_command(
    client: &mut N2NativeClient,
    prefix: &str,
    args: JobArgs,
    auth_fragment: &Option<String>,
    json_output: bool,
) -> NativeClientResult<()> {
    match args.action {
        JobAction::Plan => {
            print_op(client, &format!("{prefix}.plan"), json!({}), json_output).await
        }
        JobAction::Status => {
            print_op(client, &format!("{prefix}.status"), json!({}), json_output).await
        }
        JobAction::Watch { interval_seconds } => {
            watch_job(client, prefix, interval_seconds.max(1), json_output).await
        }
        JobAction::Stop => {
            print_op(client, &format!("{prefix}.stop"), json!({}), json_output).await
        }
        JobAction::Start {
            target_fragment,
            prefix: object_prefix,
            limit,
            dry_run,
            mismatched_only,
            max_duration_seconds,
            max_objects,
            max_directories,
            max_bytes,
            continuation_token,
            priority,
        } => {
            let fragment = target_fragment
                .or_else(|| auth_fragment.clone())
                .ok_or_else(|| {
                    NativeClientError::protocol(
                        "job start requires --fragment or --target-fragment",
                    )
                })?;
            print_op(
                client,
                &format!("{prefix}.start"),
                json!({
                    "fragment": fragment,
                    "prefix": object_prefix,
                    "limit": limit,
                    "dry_run": dry_run,
                    "mismatched_only": mismatched_only,
                    "max_duration_seconds": max_duration_seconds,
                    "max_objects": max_objects,
                    "max_directories": max_directories,
                    "max_bytes": max_bytes,
                    "continuation_token": continuation_token,
                    "priority": priority,
                }),
                json_output,
            )
            .await
        }
    }
}

pub(super) async fn watch_job(
    client: &mut N2NativeClient,
    prefix: &str,
    interval_seconds: u64,
    json_output: bool,
) -> NativeClientResult<()> {
    loop {
        let value = client
            .operation_json(&format!("{prefix}.status"), &json!({}), None)
            .await?;
        if json_output {
            println!("{}", serde_json::to_string(&value)?);
        } else {
            print_job_status_line(prefix, &value);
        }
        if !value
            .get("running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            break;
        }
        sleep(Duration::from_secs(interval_seconds)).await;
    }
    Ok(())
}
