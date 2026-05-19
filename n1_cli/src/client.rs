use std::io;

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use cli_core::client::{
    AuthSession, CreateFragmentRequest, DynError, N1_AUTH_ROUTES, api_url,
    authenticate_with_routes, build_client, fetch_json, fetch_json_query, get_base_url, get_secret,
    print_output, print_response_message, send_query_only, send_with_session, send_without_body,
    watch_rendered,
};
use cli_core::models::{
    GcJobStatus, HealthResponse, ListUploadsResponse, RedistributionJobStatus, RepairJobStatus,
    UploadStatusResponse,
};

use crate::args::{
    Cli, Commands, DurabilityArg, ExecutionModeArg, FragmentCommands, JobAction, NamespaceCommands,
    ObjectCommands, RedistributeAction, RedistributeScopeArg, RepairAction, RepairModeArg,
    UploadCommands,
};
use crate::output::{
    format_gc_status, format_health, format_health_watch_line, format_redistribution_status,
    format_repair_status, format_upload_list, format_upload_status,
};

#[derive(Deserialize)]
pub(crate) struct FragmentHashLookupResponse {
    pub(crate) fragment_hash: String,
}

#[derive(Serialize)]
pub(crate) struct FragmentHashLookupRequest {
    pub(crate) fragment: String,
}

#[derive(Serialize)]
pub(crate) struct ResizeFragmentQuotaRequest {
    pub(crate) quota_bytes: u64,
    pub(crate) secret: String,
}

pub(crate) fn looks_like_fragment_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn resolve_cli_auth_fragment_hint(cli: &Cli) -> Result<String, DynError> {
    cli.auth_fragment
        .clone()
        .or_else(|| cli.fragment.clone())
        .or_else(|| std::env::var("N1_AUTH_FRAGMENT").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            io::Error::other(
                "N1 auth is fragment-scoped. Pass --fragment <name-or-hash> or --auth-fragment <name-or-hash>.",
            )
            .into()
        })
}

async fn resolve_n1_auth_fragment(
    client: &Client,
    base_url: &str,
    fragment_hint: &str,
) -> Result<String, DynError> {
    if looks_like_fragment_hash(fragment_hint) {
        return Ok(fragment_hint.to_string());
    }

    let response = client
        .get(api_url(base_url, "/n1/fragments/hash"))
        .json(&FragmentHashLookupRequest {
            fragment: fragment_hint.to_string(),
        })
        .send()
        .await?;

    match response.status() {
        StatusCode::OK => {
            let parsed: FragmentHashLookupResponse = response.json().await?;
            Ok(parsed.fragment_hash)
        }
        StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST => {
            let body = response.text().await.unwrap_or_default();
            Err(io::Error::other(format!(
                "Could not resolve fragment '{}' for N1 auth. {}",
                fragment_hint,
                body.trim()
            ))
            .into())
        }
        _ => Err(io::Error::other(format!(
            "Failed to resolve fragment '{}' for N1 auth: {}",
            fragment_hint,
            response.status()
        ))
        .into()),
    }
}

pub async fn run(cli: Cli) -> Result<(), DynError> {
    if cli.native_url.is_some() || cli.url.is_none() {
        return crate::native::run_native(cli).await;
    }

    let base_url = get_base_url(cli.url.clone());
    let client = build_client(cli.insecure)?;

    match &cli.command {
        Commands::Fragment {
            command: command @ FragmentCommands::Create(_),
        } => {
            let secret = get_secret(cli.secret.clone())?;
            handle_fragment_command(&client, &base_url, secret, command).await
        }
        _ => {
            let secret = get_secret(cli.secret.clone())?;
            let auth_hint = resolve_cli_auth_fragment_hint(&cli)?;
            let auth_fragment = resolve_n1_auth_fragment(&client, &base_url, &auth_hint).await?;
            let mut session = authenticate_with_routes(
                &client,
                &base_url,
                &secret,
                &auth_fragment,
                N1_AUTH_ROUTES,
            )
            .await?;
            handle_authenticated_command(&client, &base_url, &mut session, &cli).await
        }
    }
}

async fn resolve_target_fragment(
    client: &Client,
    base_url: &str,
    cli: &Cli,
) -> Result<String, DynError> {
    let target = cli
        .fragment
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| io::Error::other("Pass --fragment <name-or-hash> for this command."))?;
    resolve_n1_auth_fragment(client, base_url, target).await
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
                .post(api_url(base_url, "/n1/fragments"))
                .json(&CreateFragmentRequest {
                    name: args.name.clone(),
                    key: args.key.clone(),
                    secret,
                    quota_bytes: Some(args.quota_bytes),
                })
                .send()
                .await?;

            print_response_message(response).await
        }
        FragmentCommands::ResizeQuota(_) => {
            unreachable!("fragment quota resize is handled after fragment authentication")
        }
        FragmentCommands::List | FragmentCommands::Hash { .. } | FragmentCommands::Quota => {
            unreachable!("fragment read commands are handled after fragment authentication")
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
        Commands::Console => Err(io::Error::other(
            "console requires native n1:// transport; omit --url or pass --native-url.",
        )
        .into()),
        Commands::Health { watch, deep_scan } => {
            let url = format!(
                "{}?deep_scan={}",
                api_url(base_url, "/n1/maintenance/health"),
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
        Commands::Namespace { command } => match command {
            NamespaceCommands::List { path, limit } => {
                let value: Value = fetch_json(
                    client,
                    base_url,
                    session,
                    &format!(
                        "{}?path={}&limit={}",
                        api_url(base_url, "/n1/posix/list"),
                        encode_query(path),
                        (*limit).clamp(1, 1000)
                    ),
                )
                .await?;
                print_output(cli.json, &value, &serde_json::to_string_pretty(&value)?)
            }
            NamespaceCommands::Stat { path } => {
                let value: Value = fetch_json(
                    client,
                    base_url,
                    session,
                    &format!(
                        "{}?path={}",
                        api_url(base_url, "/n1/posix/stat"),
                        encode_query(path)
                    ),
                )
                .await?;
                print_output(cli.json, &value, &serde_json::to_string_pretty(&value)?)
            }
        },
        Commands::Object { command } => match command {
            ObjectCommands::Status { object_key, mode } => {
                let value: Value = fetch_json(
                    client,
                    base_url,
                    session,
                    &format!(
                        "{}/{}?mode={}",
                        api_url(base_url, "/n1/objects/status"),
                        encode_path(object_key),
                        encode_query(mode)
                    ),
                )
                .await?;
                print_output(cli.json, &value, &serde_json::to_string_pretty(&value)?)
            }
            ObjectCommands::Repair { object_key, mode } => {
                let response = send_with_session(client, base_url, session, |token| {
                    client
                        .post(format!(
                            "{}/{}?mode={}",
                            api_url(base_url, "/n1/objects/repair"),
                            encode_path(object_key),
                            encode_query(mode)
                        ))
                        .bearer_auth(token)
                })
                .await?;
                print_response_message(response).await
            }
        },
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
                    &api_url(base_url, "/n1/upload/multipart"),
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
                    &api_url(base_url, &format!("/n1/upload/multipart/{version_id}")),
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
                    &api_url(base_url, "/n1/maintenance/gc/start"),
                )
                .await?;
                watch_rendered::<GcJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/n1/maintenance/gc/status"),
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
                    &api_url(base_url, "/n1/maintenance/gc/status"),
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
                    &api_url(base_url, "/n1/maintenance/gc/stop"),
                )
                .await
            }
            JobAction::Watch => {
                watch_rendered::<GcJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/n1/maintenance/gc/status"),
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
                    &api_url(base_url, "/n1/maintenance/repair/start"),
                    &query,
                )
                .await?;
                watch_rendered::<RepairJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/n1/maintenance/repair/status"),
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
                    &api_url(base_url, "/n1/maintenance/repair/status"),
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
                    &api_url(base_url, "/n1/maintenance/repair/stop"),
                )
                .await
            }
            RepairAction::Watch => {
                watch_rendered::<RepairJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/n1/maintenance/repair/status"),
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
                    &api_url(base_url, "/n1/maintenance/redistribution/start"),
                    &query,
                )
                .await?;
                watch_rendered::<RedistributionJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/n1/maintenance/redistribution/status"),
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
                    &api_url(base_url, "/n1/maintenance/redistribution/status"),
                )
                .await?;
                print_output(cli.json, &status, &format_redistribution_status(&status))
            }
            RedistributeAction::Stop => {
                send_without_body(
                    client,
                    base_url,
                    session,
                    Method::POST,
                    &api_url(base_url, "/n1/maintenance/redistribution/stop"),
                )
                .await
            }
            RedistributeAction::Watch => {
                watch_rendered::<RedistributionJobStatus, _, _>(
                    client,
                    base_url,
                    session,
                    &api_url(base_url, "/n1/maintenance/redistribution/status"),
                    cli.poll_ms,
                    cli.json,
                    format_redistribution_status,
                    |_| false,
                )
                .await
            }
        },
        Commands::Fragment { command } => {
            handle_authenticated_fragment_command(client, base_url, session, cli, command).await
        }
    }
}

async fn handle_authenticated_fragment_command(
    client: &Client,
    base_url: &str,
    session: &mut AuthSession,
    cli: &Cli,
    command: &FragmentCommands,
) -> Result<(), DynError> {
    match command {
        FragmentCommands::Create(_) => unreachable!("fragment create is handled without auth"),
        FragmentCommands::List => {
            let value: Value = fetch_json(
                client,
                base_url,
                session,
                &api_url(base_url, "/n1/fragments"),
            )
            .await?;
            print_output(cli.json, &value, &serde_json::to_string_pretty(&value)?)
        }
        FragmentCommands::Hash { fragment } => {
            let fragment = fragment
                .as_deref()
                .or(cli.fragment.as_deref())
                .ok_or_else(|| {
                    io::Error::other("pass --fragment or fragment hash --fragment <name>")
                })?;
            let response = send_with_session(client, base_url, session, |token| {
                client
                    .get(api_url(base_url, "/n1/fragments/hash"))
                    .bearer_auth(token)
                    .json(&FragmentHashLookupRequest {
                        fragment: fragment.to_string(),
                    })
            })
            .await?;
            print_response_message(response).await
        }
        FragmentCommands::Quota => {
            let value: Value = fetch_json(
                client,
                base_url,
                session,
                &api_url(base_url, "/n1/capacity/quota"),
            )
            .await?;
            print_output(cli.json, &value, &serde_json::to_string_pretty(&value)?)
        }
        FragmentCommands::ResizeQuota(args) => {
            let target_fragment = resolve_target_fragment(client, base_url, cli).await?;
            let url = api_url(
                base_url,
                &format!("/n1/fragments/{}/quota", target_fragment),
            );
            let response = send_with_session(client, base_url, session, |token| {
                client
                    .patch(&url)
                    .bearer_auth(token)
                    .json(&ResizeFragmentQuotaRequest {
                        quota_bytes: args.quota_bytes,
                        secret: args.master_secret.clone(),
                    })
            })
            .await?;
            print_response_message(response).await
        }
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

pub(crate) fn build_repair_start_query(
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

pub(crate) fn build_redistribution_start_query(
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

fn encode_query(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn encode_path(value: &str) -> String {
    encode_query(value).replace("%2F", "/")
}
