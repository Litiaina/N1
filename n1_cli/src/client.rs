use reqwest::{Client, Method};

use cli_core::client::{
    AuthSession, CreateFragmentRequest, DynError, api_url, authenticate, build_client, fetch_json,
    fetch_json_query, get_base_url, get_secret, print_output, print_response_message,
    resolve_auth_fragment, send_query_only, send_without_body, watch_rendered,
};
use cli_core::models::{
    GcJobStatus, HealthResponse, ListUploadsResponse, RedistributionJobStatus, RepairJobStatus,
    UploadStatusResponse,
};

use crate::args::{
    Cli, Commands, DurabilityArg, ExecutionModeArg, FragmentCommands, JobAction,
    RedistributeAction, RedistributeScopeArg, RepairAction, RepairModeArg, UploadCommands,
};
use crate::output::{
    format_gc_status, format_health, format_health_watch_line, format_redistribution_status,
    format_repair_status, format_upload_list, format_upload_status,
};

pub async fn run(cli: Cli) -> Result<(), DynError> {
    let base_url = get_base_url(cli.url.clone());
    let client = build_client(cli.insecure)?;

    match &cli.command {
        Commands::Fragment { command } => {
            let secret = get_secret(cli.secret.clone())?;
            handle_fragment_command(&client, &base_url, secret, command).await
        }
        _ => {
            let secret = get_secret(cli.secret.clone())?;
            let auth_fragment = resolve_auth_fragment(cli.auth_fragment.clone());
            let mut session = authenticate(&client, &base_url, &secret, &auth_fragment).await?;
            handle_authenticated_command(&client, &base_url, &mut session, &cli).await
        }
    }
}

async fn handle_fragment_command(
    client: &Client,
    base_url: &str,
    secret: String,
    command: &FragmentCommands,
) -> Result<(), DynError> {
    match command {
        FragmentCommands::Create(args) => {
            let response = client
                .post(api_url(base_url, "/v4/fragment/create"))
                .json(&CreateFragmentRequest {
                    name: args.name.clone(),
                    key: args.key.clone(),
                    secret,
                })
                .send()
                .await?;

            print_response_message(response).await
        }
    }
}

async fn handle_authenticated_command(
    client: &Client,
    base_url: &str,
    session: &mut AuthSession,
    cli: &Cli,
) -> Result<(), DynError> {
    match &cli.command {
        Commands::Health { watch, deep_scan } => {
            let url = format!(
                "{}?deep_scan={}",
                api_url(base_url, "/v4/maintenance/health"),
                if *deep_scan { "true" } else { "false" }
            );

            if *watch {
                watch_rendered::<HealthResponse, _, _>(
                    client,
                    base_url,
                    session,
                    &url,
                    cli.poll_ms,
                    cli.json,
                    format_health_watch_line,
                    |_| false,
                )
                .await
            } else {
                let health: HealthResponse = fetch_json(client, base_url, session, &url).await?;
                print_output(cli.json, &health, &format_health(&health))
            }
        }
        Commands::Uploads { command } => match command {
            UploadCommands::List { limit, offset } => {
                let query = vec![
                    ("limit".to_string(), limit.to_string()),
                    ("offset".to_string(), offset.to_string()),
                ];
                let uploads: ListUploadsResponse = fetch_json_query(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/upload/status/list"),
                    &query,
                )
                .await?;
                print_output(cli.json, &uploads, &format_upload_list(&uploads))
            }
            UploadCommands::Status { version_id } => {
                let status: UploadStatusResponse = fetch_json(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, &format!("/v4/upload/status/{version_id}")),
                )
                .await?;
                print_output(cli.json, &status, &format_upload_status(&status))
            }
        },
        Commands::Gc { action } => match action {
            JobAction::Start => {
                send_without_body(
                    client,
                    base_url,
                    session,
                    Method::POST,
                    &api_url(base_url, "/v4/maintenance/gc/start"),
                )
                .await?;
                watch_rendered::<GcJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/maintenance/gc/status"),
                    cli.poll_ms,
                    cli.json,
                    format_gc_status,
                    |status| !status.is_running,
                )
                .await
            }
            JobAction::Status => {
                let status: GcJobStatus = fetch_json(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/maintenance/gc/status"),
                )
                .await?;
                print_output(cli.json, &status, &format_gc_status(&status))
            }
            JobAction::Stop => {
                send_without_body(
                    client,
                    base_url,
                    session,
                    Method::POST,
                    &api_url(base_url, "/v4/maintenance/gc/stop"),
                )
                .await
            }
            JobAction::Watch => {
                watch_rendered::<GcJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/maintenance/gc/status"),
                    cli.poll_ms,
                    cli.json,
                    format_gc_status,
                    |_| false,
                )
                .await
            }
        },
        Commands::Repair { action } => match action {
            RepairAction::Start(args) => {
                let query = build_repair_start_query(
                    args.dry_run,
                    cli.fragment.as_deref(),
                    args.mode,
                    args.fast_mode,
                    args.read_only_fast,
                    args.meta_concurrency,
                    args.repair_concurrency,
                    args.batch,
                );
                send_query_only(
                    client,
                    base_url,
                    session,
                    Method::POST,
                    &api_url(base_url, "/v4/maintenance/repair/start"),
                    &query,
                )
                .await?;
                watch_rendered::<RepairJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/maintenance/repair/status"),
                    cli.poll_ms,
                    cli.json,
                    format_repair_status,
                    |status| !status.is_running,
                )
                .await
            }
            RepairAction::Status => {
                let status: RepairJobStatus = fetch_json(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/maintenance/repair/status"),
                )
                .await?;
                print_output(cli.json, &status, &format_repair_status(&status))
            }
            RepairAction::Stop => {
                send_without_body(
                    client,
                    base_url,
                    session,
                    Method::POST,
                    &api_url(base_url, "/v4/maintenance/repair/stop"),
                )
                .await
            }
            RepairAction::Watch => {
                watch_rendered::<RepairJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/maintenance/repair/status"),
                    cli.poll_ms,
                    cli.json,
                    format_repair_status,
                    |_| false,
                )
                .await
            }
        },
        Commands::Redistribute { action } => match action {
            RedistributeAction::Start(args) => {
                let query = build_redistribution_start_query(
                    args.dry_run,
                    cli.fragment.as_deref(),
                    args.mode,
                    args.fast_mode,
                    args.read_only_fast,
                    args.durability,
                    args.scope,
                    args.object_concurrency,
                    args.status_batch,
                );
                send_query_only(
                    client,
                    base_url,
                    session,
                    Method::POST,
                    &api_url(base_url, "/v4/maintenance/redistribution/start"),
                    &query,
                )
                .await?;
                watch_rendered::<RedistributionJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/maintenance/redistribution/monitor"),
                    cli.poll_ms,
                    cli.json,
                    format_redistribution_status,
                    |status| !status.is_running,
                )
                .await
            }
            RedistributeAction::Status => {
                let status: RedistributionJobStatus = fetch_json(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/maintenance/redistribution/monitor"),
                )
                .await?;
                print_output(cli.json, &status, &format_redistribution_status(&status))
            }
            RedistributeAction::Stop => {
                send_without_body(
                    client,
                    base_url,
                    session,
                    Method::PATCH,
                    &api_url(base_url, "/v4/maintenance/redistribution/stop"),
                )
                .await
            }
            RedistributeAction::Watch => {
                watch_rendered::<RedistributionJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/v4/maintenance/redistribution/monitor"),
                    cli.poll_ms,
                    cli.json,
                    format_redistribution_status,
                    |_| false,
                )
                .await
            }
        },
        Commands::Fragment { .. } => unreachable!("fragment commands are handled without auth"),
    }
}

fn push_query_flag(query: &mut Vec<(String, String)>, key: &str, enabled: bool) {
    if enabled {
        query.push((key.to_string(), "true".to_string()));
    }
}

fn push_query_value<T: ToString>(query: &mut Vec<(String, String)>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        query.push((key.to_string(), value.to_string()));
    }
}

fn build_repair_start_query(
    dry_run: bool,
    fragment: Option<&str>,
    mode: Option<RepairModeArg>,
    fast_mode: bool,
    read_only_fast: bool,
    meta_concurrency: Option<usize>,
    repair_concurrency: Option<usize>,
    batch: Option<usize>,
) -> Vec<(String, String)> {
    let mut query = Vec::new();
    push_query_flag(&mut query, "dry_run", dry_run);
    push_query_value(
        &mut query,
        "fragment",
        fragment.filter(|value| !value.trim().is_empty()),
    );
    push_query_value(
        &mut query,
        "mode",
        mode.map(|value| value.as_query_value().to_string()),
    );
    push_query_flag(&mut query, "fast_mode", fast_mode);
    push_query_flag(&mut query, "read_only_fast", read_only_fast);
    push_query_value(&mut query, "meta_concurrency", meta_concurrency);
    push_query_value(&mut query, "repair_concurrency", repair_concurrency);
    push_query_value(&mut query, "batch", batch);
    query
}

fn build_redistribution_start_query(
    dry_run: bool,
    fragment: Option<&str>,
    mode: Option<ExecutionModeArg>,
    fast_mode: bool,
    read_only_fast: bool,
    durability: Option<DurabilityArg>,
    scope: Option<RedistributeScopeArg>,
    object_concurrency: Option<usize>,
    status_batch: Option<u64>,
) -> Vec<(String, String)> {
    let mut query = Vec::new();
    push_query_flag(&mut query, "dry_run", dry_run);
    push_query_value(
        &mut query,
        "fragment",
        fragment.filter(|value| !value.trim().is_empty()),
    );
    push_query_value(
        &mut query,
        "mode",
        mode.map(|value| value.as_query_value().to_string()),
    );
    push_query_flag(&mut query, "fast_mode", fast_mode);
    push_query_flag(&mut query, "read_only_fast", read_only_fast);
    push_query_value(
        &mut query,
        "durability",
        durability.map(|value| value.as_query_value().to_string()),
    );
    push_query_value(
        &mut query,
        "scope",
        scope.map(|value| value.as_query_value().to_string()),
    );
    push_query_value(&mut query, "object_concurrency", object_concurrency);
    push_query_value(&mut query, "status_batch", status_batch);
    query
}
