use crate::distribution_core::config::n2_runtime_config;
use crate::distribution_core::model::{
    ClusterMembership, GovernanceAbortRequest, GovernanceAbortResponse, GovernanceAppendRequest,
    GovernanceAppendResponse, GovernanceMembershipEntry, GovernanceVoteRequest,
    GovernanceVoteResponse, N2GovernanceStateResponse,
};
use crate::distribution_core::state::{
    apply_membership_snapshot, ensure_membership_artifacts, get_membership,
};
use crate::enums::app_error_enum::AppError;
use axum::http::HeaderMap;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const N2_INTERNAL_TOKEN_HEADER: &str = "x-n2-internal-token";
const GOVERNANCE_LEADER_LEASE_MS: u64 = 10_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GovernancePersistentState {
    current_term: u64,
    voted_for: Option<String>,
    commit_index: u64,
    last_applied_membership_epoch: u64,
}

#[derive(Clone, Debug)]
struct GovernanceRuntime {
    persistent: GovernancePersistentState,
    leader_id: Option<String>,
    leader_endpoint: Option<String>,
    leader_term: u64,
    leader_lease_deadline_ms: u64,
    pending: Option<GovernanceMembershipEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GovernanceCommittedJournalEntry {
    index: u64,
    term: u64,
    committed_at_ms: u64,
    membership_epoch: u64,
    membership: ClusterMembership,
}

static GOVERNANCE: OnceCell<Mutex<GovernanceRuntime>> = OnceCell::new();

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path
        .file_stem()
        .and_then(|v| v.to_str())
        .unwrap_or("n2_cluster_membership");
    parent.join(format!("{}.{}", stem, suffix))
}

fn governance_state_path() -> PathBuf {
    path_with_suffix(
        n2_runtime_config().membership_file.as_path(),
        "governance-state.json",
    )
}

fn governance_pending_path() -> PathBuf {
    path_with_suffix(
        n2_runtime_config().membership_file.as_path(),
        "governance-pending.json",
    )
}

fn governance_journal_path() -> PathBuf {
    path_with_suffix(
        n2_runtime_config().membership_file.as_path(),
        "governance-log.jsonl",
    )
}

async fn validate_internal_token(headers: &HeaderMap) -> Result<(), AppError> {
    let cfg = n2_runtime_config();
    let Some(token) = headers
        .get(N2_INTERNAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
    else {
        return Err(AppError::Forbidden("missing internal n2 token".to_string()));
    };
    if token != cfg.internal_token {
        return Err(AppError::Forbidden("invalid internal n2 token".to_string()));
    }
    Ok(())
}

async fn write_atomic_json<T: Serialize>(
    path: &Path,
    value: &T,
    label: &str,
) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::Io(e, format!("create {}", label)))?;
    }
    let payload = serde_json::to_vec_pretty(value)
        .map_err(|e| AppError::InternalServerError(format!("encode {} failed: {}", label, e)))?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, payload)
        .await
        .map_err(|e| AppError::Io(e, format!("write {}", label)))?;
    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("replace {}", label)))?;
    Ok(())
}

async fn read_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
    label: &str,
) -> Result<Option<T>, AppError> {
    let exists = fs::try_exists(path)
        .await
        .map_err(|e| AppError::Io(e, format!("check {}", label)))?;
    if !exists {
        return Ok(None);
    }
    let bytes = fs::read(path)
        .await
        .map_err(|e| AppError::Io(e, format!("read {}", label)))?;
    let parsed = serde_json::from_slice::<T>(&bytes)
        .map_err(|e| AppError::InternalServerError(format!("decode {} failed: {}", label, e)))?;
    Ok(Some(parsed))
}

async fn append_committed_journal(entry: &GovernanceMembershipEntry) -> Result<(), AppError> {
    let journal_path = governance_journal_path();
    if let Some(parent) = journal_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::Io(e, "create governance journal parent".to_string()))?;
    }
    let record = GovernanceCommittedJournalEntry {
        index: entry.index,
        term: entry.term,
        committed_at_ms: now_ms(),
        membership_epoch: entry.membership.epoch,
        membership: entry.membership.clone(),
    };
    let line = serde_json::to_vec(&record).map_err(|e| {
        AppError::InternalServerError(format!("encode governance journal failed: {}", e))
    })?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)
        .await
        .map_err(|e| AppError::Io(e, "open governance journal".to_string()))?;
    file.write_all(&line)
        .await
        .map_err(|e| AppError::Io(e, "append governance journal".to_string()))?;
    file.write_all(b"\n")
        .await
        .map_err(|e| AppError::Io(e, "append governance journal newline".to_string()))?;
    Ok(())
}

async fn persist_runtime_state(runtime: &GovernanceRuntime) -> Result<(), AppError> {
    write_atomic_json(
        governance_state_path().as_path(),
        &runtime.persistent,
        "governance state",
    )
    .await
}

async fn persist_pending_entry(
    pending: Option<&GovernanceMembershipEntry>,
) -> Result<(), AppError> {
    let pending_path = governance_pending_path();
    match pending {
        Some(entry) => {
            write_atomic_json(pending_path.as_path(), entry, "governance pending entry").await
        }
        None => {
            let exists = fs::try_exists(&pending_path)
                .await
                .map_err(|e| AppError::Io(e, "check governance pending entry".to_string()))?;
            if exists {
                fs::remove_file(&pending_path)
                    .await
                    .map_err(|e| AppError::Io(e, "remove governance pending entry".to_string()))?;
            }
            Ok(())
        }
    }
}

async fn ensure_bootstrap_journal(
    membership: &ClusterMembership,
    persistent: &GovernancePersistentState,
) -> Result<(), AppError> {
    let journal_path = governance_journal_path();
    let exists = fs::try_exists(&journal_path)
        .await
        .map_err(|e| AppError::Io(e, "check governance journal".to_string()))?;
    if exists {
        return Ok(());
    }
    let bootstrap = GovernanceMembershipEntry {
        index: persistent.commit_index.max(1),
        term: persistent.current_term.max(1),
        membership: membership.clone(),
    };
    append_committed_journal(&bootstrap).await
}

async fn initialize_runtime() -> Result<GovernanceRuntime, AppError> {
    let membership = get_membership().await?;
    ensure_membership_artifacts(&membership).await?;
    let default_state = GovernancePersistentState {
        current_term: membership.epoch.max(1),
        voted_for: None,
        commit_index: membership.epoch.max(1),
        last_applied_membership_epoch: membership.epoch,
    };
    let mut persistent = read_json::<GovernancePersistentState>(
        governance_state_path().as_path(),
        "governance state",
    )
    .await?
    .unwrap_or(default_state);
    if persistent.commit_index == 0 {
        persistent.commit_index = membership.epoch.max(1);
    }
    if persistent.current_term == 0 {
        persistent.current_term = membership.epoch.max(1);
    }
    if persistent.last_applied_membership_epoch < membership.epoch {
        persistent.last_applied_membership_epoch = membership.epoch;
        persistent.commit_index = persistent.commit_index.max(membership.epoch);
    }
    let pending = read_json::<GovernanceMembershipEntry>(
        governance_pending_path().as_path(),
        "governance pending entry",
    )
    .await?;
    ensure_bootstrap_journal(&membership, &persistent).await?;
    let runtime = GovernanceRuntime {
        persistent,
        leader_id: None,
        leader_endpoint: None,
        leader_term: 0,
        leader_lease_deadline_ms: 0,
        pending,
    };
    persist_runtime_state(&runtime).await?;
    if let Some(entry) = runtime.pending.as_ref() {
        persist_pending_entry(Some(entry)).await?;
    }
    Ok(runtime)
}

async fn runtime_lock() -> Result<&'static Mutex<GovernanceRuntime>, AppError> {
    if let Some(lock) = GOVERNANCE.get() {
        return Ok(lock);
    }
    let runtime = initialize_runtime().await?;
    let _ = GOVERNANCE.set(Mutex::new(runtime));
    Ok(GOVERNANCE.get().expect("governance runtime initialized"))
}

fn build_governance_view(runtime: &GovernanceRuntime) -> N2GovernanceStateResponse {
    let cfg = n2_runtime_config();
    let leader_lease_valid = runtime.leader_lease_deadline_ms > now_ms();
    N2GovernanceStateResponse {
        current_term: runtime.persistent.current_term,
        commit_index: runtime.persistent.commit_index,
        last_applied_membership_epoch: runtime.persistent.last_applied_membership_epoch,
        leader_id: runtime.leader_id.clone(),
        leader_endpoint: runtime.leader_endpoint.clone(),
        pending_index: runtime.pending.as_ref().map(|entry| entry.index),
        is_leader: leader_lease_valid
            && runtime.leader_id.as_deref() == Some(cfg.node_id.as_str())
            && runtime.leader_term == runtime.persistent.current_term,
        leader_lease_valid,
    }
}

fn majority(active_nodes: usize) -> usize {
    active_nodes / 2 + 1
}

fn active_peers(
    membership: &ClusterMembership,
) -> Vec<crate::distribution_core::model::NodeDescriptor> {
    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(cfg.node_endpoint.as_str());
    membership
        .nodes
        .iter()
        .filter(|node| {
            node.active
                && !(node.node_id == cfg.node_id
                    || normalize_endpoint(node.endpoint.as_str()) == local_endpoint)
        })
        .cloned()
        .collect()
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint.trim().trim_end_matches('/').to_ascii_lowercase()
}

fn commit_notification_endpoints(
    current: &ClusterMembership,
    next: &ClusterMembership,
) -> Vec<String> {
    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(cfg.node_endpoint.as_str());
    let mut endpoints = current
        .nodes
        .iter()
        .chain(next.nodes.iter())
        .filter(|node| node.active)
        .map(|node| node.endpoint.clone())
        .filter(|endpoint| normalize_endpoint(endpoint.as_str()) != local_endpoint)
        .collect::<Vec<_>>();
    endpoints.sort_by_key(|endpoint| normalize_endpoint(endpoint.as_str()));
    endpoints.dedup_by(|left, right| {
        normalize_endpoint(left.as_str()) == normalize_endpoint(right.as_str())
    });
    endpoints
}

async fn governance_http_client() -> Result<reqwest::Client, AppError> {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(
            n2_runtime_config().remote_request_timeout_ms.min(15_000),
        ))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build governance client failed: {}", e))
        })
}

async fn request_vote_from_peer(
    client: &reqwest::Client,
    endpoint: &str,
    request: &GovernanceVoteRequest,
) -> Result<GovernanceVoteResponse, AppError> {
    let url = format!(
        "{}/v4/n2/internal/governance/request-vote",
        endpoint.trim_end_matches('/')
    );
    let response = client
        .post(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .json(request)
        .send()
        .await
        .map_err(|e| {
            AppError::Io(
                std::io::Error::other(e),
                "request governance vote".to_string(),
            )
        })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::Conflict(format!(
            "governance vote request failed status={} body={}",
            status, body
        )));
    }
    response
        .json::<GovernanceVoteResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("decode governance vote response failed: {}", e))
        })
}

async fn append_to_peer(
    client: &reqwest::Client,
    endpoint: &str,
    request: &GovernanceAppendRequest,
) -> Result<GovernanceAppendResponse, AppError> {
    let url = format!(
        "{}/v4/n2/internal/governance/append",
        endpoint.trim_end_matches('/')
    );
    let response = client
        .post(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .json(request)
        .send()
        .await
        .map_err(|e| {
            AppError::Io(
                std::io::Error::other(e),
                "append governance entry".to_string(),
            )
        })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::Conflict(format!(
            "governance append request failed status={} body={}",
            status, body
        )));
    }
    response
        .json::<GovernanceAppendResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!(
                "decode governance append response failed: {}",
                e
            ))
        })
}

async fn abort_peer_pending(
    client: &reqwest::Client,
    endpoint: &str,
    request: &GovernanceAbortRequest,
) {
    let url = format!(
        "{}/v4/n2/internal/governance/abort",
        endpoint.trim_end_matches('/')
    );
    let _ = client
        .post(url)
        .header(
            N2_INTERNAL_TOKEN_HEADER,
            n2_runtime_config().internal_token.as_str(),
        )
        .json(request)
        .send()
        .await;
}

async fn clear_local_pending_due_to_newer_term(new_term: u64) -> Result<(), AppError> {
    let lock = runtime_lock().await?;
    let mut runtime = lock.lock().await;
    if new_term > runtime.persistent.current_term {
        runtime.persistent.current_term = new_term;
        runtime.persistent.voted_for = None;
    }
    runtime.pending = None;
    runtime.leader_id = None;
    runtime.leader_endpoint = None;
    runtime.leader_term = 0;
    runtime.leader_lease_deadline_ms = 0;
    persist_runtime_state(&runtime).await?;
    persist_pending_entry(None).await
}

async fn apply_committed_entry(
    entry: GovernanceMembershipEntry,
    leader_id: Option<String>,
    leader_endpoint: Option<String>,
) -> Result<ClusterMembership, AppError> {
    let applied = apply_membership_snapshot(entry.membership.clone()).await?;
    let lock = runtime_lock().await?;
    let mut runtime = lock.lock().await;
    runtime.pending = None;
    runtime.persistent.commit_index = entry.index;
    runtime.persistent.last_applied_membership_epoch = applied.epoch;
    runtime.leader_id = leader_id;
    runtime.leader_endpoint = leader_endpoint;
    runtime.leader_term = entry.term;
    runtime.leader_lease_deadline_ms = now_ms().saturating_add(GOVERNANCE_LEADER_LEASE_MS);
    persist_runtime_state(&runtime).await?;
    persist_pending_entry(None).await?;
    append_committed_journal(&entry).await?;
    Ok(applied)
}

pub async fn initialize_governance_runtime() -> Result<(), AppError> {
    let _ = runtime_lock().await?;
    Ok(())
}

pub async fn governance_state_view() -> Result<N2GovernanceStateResponse, AppError> {
    let lock = runtime_lock().await?;
    let runtime = lock.lock().await;
    Ok(build_governance_view(&runtime))
}

pub async fn ensure_local_governance_leader() -> Result<N2GovernanceStateResponse, AppError> {
    let membership = get_membership().await?;
    let active_count = membership.nodes.iter().filter(|node| node.active).count();
    let cfg = n2_runtime_config();
    let lock = runtime_lock().await?;

    {
        let mut runtime = lock.lock().await;
        let leader_valid = runtime.leader_lease_deadline_ms > now_ms()
            && runtime.leader_id.as_deref() == Some(cfg.node_id.as_str())
            && runtime.leader_term == runtime.persistent.current_term;
        if leader_valid {
            return Ok(build_governance_view(&runtime));
        }
        if majority(active_count) <= 1 {
            runtime.leader_id = Some(cfg.node_id.clone());
            runtime.leader_endpoint = Some(cfg.node_endpoint.clone());
            runtime.leader_term = runtime.persistent.current_term.max(1);
            runtime.persistent.current_term = runtime.persistent.current_term.max(1);
            runtime.leader_lease_deadline_ms = now_ms().saturating_add(GOVERNANCE_LEADER_LEASE_MS);
            persist_runtime_state(&runtime).await?;
            return Ok(build_governance_view(&runtime));
        }
    }

    let client = governance_http_client().await?;
    let peers = active_peers(&membership);
    let (term, last_commit_index, last_applied_epoch) = {
        let mut runtime = lock.lock().await;
        runtime.persistent.current_term = runtime.persistent.current_term.saturating_add(1).max(1);
        runtime.persistent.voted_for = Some(cfg.node_id.clone());
        runtime.leader_id = None;
        runtime.leader_endpoint = None;
        runtime.leader_term = 0;
        runtime.leader_lease_deadline_ms = 0;
        persist_runtime_state(&runtime).await?;
        (
            runtime.persistent.current_term,
            runtime.persistent.commit_index,
            runtime.persistent.last_applied_membership_epoch,
        )
    };

    let request = GovernanceVoteRequest {
        term,
        candidate_id: cfg.node_id.clone(),
        candidate_endpoint: cfg.node_endpoint.clone(),
        last_commit_index,
        last_applied_membership_epoch: last_applied_epoch,
    };

    let mut votes = 1usize;
    let mut newer_term = term;
    for peer in peers {
        match request_vote_from_peer(&client, peer.endpoint.as_str(), &request).await {
            Ok(response) => {
                if response.term > newer_term {
                    newer_term = response.term;
                }
                if response.vote_granted && response.term == term {
                    votes = votes.saturating_add(1);
                }
            }
            Err(err) => {
                tracing::warn!(
                    "n2 governance vote request failed endpoint={} err={}",
                    peer.endpoint,
                    err
                );
            }
        }
    }

    if newer_term > term {
        clear_local_pending_due_to_newer_term(newer_term).await?;
        return Err(AppError::Conflict(format!(
            "governance election lost to a newer term {}",
            newer_term
        )));
    }

    let needed = majority(active_count);
    if votes < needed {
        return Err(AppError::Conflict(format!(
            "governance election reached only {} votes, need majority {}",
            votes, needed
        )));
    }

    let mut runtime = lock.lock().await;
    if runtime.persistent.current_term != term {
        return Err(AppError::Conflict(format!(
            "governance term changed during election: local {}, elected {}",
            runtime.persistent.current_term, term
        )));
    }
    runtime.leader_id = Some(cfg.node_id.clone());
    runtime.leader_endpoint = Some(cfg.node_endpoint.clone());
    runtime.leader_term = term;
    runtime.leader_lease_deadline_ms = now_ms().saturating_add(GOVERNANCE_LEADER_LEASE_MS);
    persist_runtime_state(&runtime).await?;
    Ok(build_governance_view(&runtime))
}

pub async fn commit_membership_change(
    next: ClusterMembership,
) -> Result<ClusterMembership, AppError> {
    let membership = get_membership().await?;
    let active_count = membership.nodes.iter().filter(|node| node.active).count();
    let cfg = n2_runtime_config();
    let _ = ensure_local_governance_leader().await?;
    let client = governance_http_client().await?;
    let peers = active_peers(&membership);
    let commit_targets = commit_notification_endpoints(&membership, &next);
    let lock = runtime_lock().await?;

    let (term, prev_commit_index, entry) = {
        let mut runtime = lock.lock().await;
        let leader_valid = runtime.leader_lease_deadline_ms > now_ms()
            && runtime.leader_id.as_deref() == Some(cfg.node_id.as_str())
            && runtime.leader_term == runtime.persistent.current_term;
        if !leader_valid {
            return Err(AppError::Conflict(
                "local node is not the current governance leader".to_string(),
            ));
        }
        let entry = GovernanceMembershipEntry {
            index: runtime.persistent.commit_index.saturating_add(1),
            term: runtime.persistent.current_term,
            membership: next.clone(),
        };
        runtime.pending = Some(entry.clone());
        persist_pending_entry(Some(&entry)).await?;
        (
            runtime.persistent.current_term,
            runtime.persistent.commit_index,
            entry,
        )
    };

    let prepare = GovernanceAppendRequest {
        term,
        leader_id: cfg.node_id.clone(),
        leader_endpoint: cfg.node_endpoint.clone(),
        prev_commit_index,
        leader_commit_index: prev_commit_index,
        entry: Some(entry.clone()),
    };

    let mut acknowledgements = 1usize;
    let mut newer_term = term;
    let mut prepared_endpoints = Vec::new();
    for peer in &peers {
        match append_to_peer(&client, peer.endpoint.as_str(), &prepare).await {
            Ok(response) => {
                if response.term > newer_term {
                    newer_term = response.term;
                }
                if response.accepted && response.term == term {
                    acknowledgements = acknowledgements.saturating_add(1);
                    prepared_endpoints.push(peer.endpoint.clone());
                }
            }
            Err(err) => {
                tracing::warn!(
                    "n2 governance prepare failed endpoint={} err={}",
                    peer.endpoint,
                    err
                );
            }
        }
    }

    if newer_term > term {
        clear_local_pending_due_to_newer_term(newer_term).await?;
        return Err(AppError::Conflict(format!(
            "governance proposal superseded by newer term {}",
            newer_term
        )));
    }

    let needed = majority(active_count);
    if acknowledgements < needed {
        let abort = GovernanceAbortRequest {
            term,
            leader_id: cfg.node_id.clone(),
            pending_index: entry.index,
        };
        for endpoint in prepared_endpoints {
            abort_peer_pending(&client, endpoint.as_str(), &abort).await;
        }
        {
            let mut runtime = lock.lock().await;
            runtime.pending = None;
            persist_pending_entry(None).await?;
        }
        return Err(AppError::Conflict(format!(
            "governance proposal reached only {} acknowledgements, need majority {}",
            acknowledgements, needed
        )));
    }

    let applied = apply_committed_entry(
        entry.clone(),
        Some(cfg.node_id.clone()),
        Some(cfg.node_endpoint.clone()),
    )
    .await?;

    let commit = GovernanceAppendRequest {
        term,
        leader_id: cfg.node_id.clone(),
        leader_endpoint: cfg.node_endpoint.clone(),
        prev_commit_index,
        leader_commit_index: entry.index,
        entry: Some(entry),
    };

    for endpoint in commit_targets {
        if let Err(err) = append_to_peer(&client, endpoint.as_str(), &commit).await {
            tracing::warn!(
                "n2 governance commit notify failed endpoint={} err={}",
                endpoint,
                err
            );
        }
    }

    Ok(applied)
}

pub async fn handle_governance_vote(
    headers: &HeaderMap,
    request: GovernanceVoteRequest,
) -> Result<GovernanceVoteResponse, AppError> {
    validate_internal_token(headers).await?;
    let lock = runtime_lock().await?;
    let mut runtime = lock.lock().await;

    if request.term < runtime.persistent.current_term {
        return Ok(GovernanceVoteResponse {
            term: runtime.persistent.current_term,
            vote_granted: false,
            leader_id: runtime.leader_id.clone(),
            leader_endpoint: runtime.leader_endpoint.clone(),
            commit_index: runtime.persistent.commit_index,
        });
    }

    if request.term > runtime.persistent.current_term {
        runtime.persistent.current_term = request.term;
        runtime.persistent.voted_for = None;
        runtime.leader_id = None;
        runtime.leader_endpoint = None;
        runtime.leader_term = 0;
        runtime.leader_lease_deadline_ms = 0;
        runtime.pending = None;
        persist_runtime_state(&runtime).await?;
        persist_pending_entry(None).await?;
    }

    let up_to_date = request.last_commit_index >= runtime.persistent.commit_index
        && request.last_applied_membership_epoch
            >= runtime.persistent.last_applied_membership_epoch;
    let vote_granted = up_to_date
        && runtime
            .persistent
            .voted_for
            .as_deref()
            .map_or(true, |voted_for| voted_for == request.candidate_id.as_str());

    if vote_granted {
        runtime.persistent.voted_for = Some(request.candidate_id);
        runtime.leader_id = None;
        runtime.leader_endpoint = None;
        runtime.leader_term = 0;
        runtime.leader_lease_deadline_ms = 0;
        persist_runtime_state(&runtime).await?;
    }

    Ok(GovernanceVoteResponse {
        term: runtime.persistent.current_term,
        vote_granted,
        leader_id: runtime.leader_id.clone(),
        leader_endpoint: runtime.leader_endpoint.clone(),
        commit_index: runtime.persistent.commit_index,
    })
}

pub async fn handle_governance_append(
    headers: &HeaderMap,
    request: GovernanceAppendRequest,
) -> Result<GovernanceAppendResponse, AppError> {
    validate_internal_token(headers).await?;
    let lock = runtime_lock().await?;

    {
        let mut runtime = lock.lock().await;
        if request.term < runtime.persistent.current_term {
            return Ok(GovernanceAppendResponse {
                term: runtime.persistent.current_term,
                accepted: false,
                commit_index: runtime.persistent.commit_index,
            });
        }

        if request.term > runtime.persistent.current_term {
            runtime.persistent.current_term = request.term;
            runtime.persistent.voted_for = None;
            persist_runtime_state(&runtime).await?;
        }

        if request.prev_commit_index != runtime.persistent.commit_index {
            return Ok(GovernanceAppendResponse {
                term: runtime.persistent.current_term,
                accepted: false,
                commit_index: runtime.persistent.commit_index,
            });
        }

        runtime.leader_id = Some(request.leader_id.clone());
        runtime.leader_endpoint = Some(request.leader_endpoint.clone());
        runtime.leader_term = request.term;
        runtime.leader_lease_deadline_ms = now_ms().saturating_add(GOVERNANCE_LEADER_LEASE_MS);

        if let Some(entry) = request.entry.as_ref() {
            runtime.pending = Some(entry.clone());
            persist_pending_entry(Some(entry)).await?;
        }
        persist_runtime_state(&runtime).await?;
    }

    if let Some(entry) = request.entry.clone() {
        if request.leader_commit_index >= entry.index {
            let applied = apply_committed_entry(
                entry,
                Some(request.leader_id),
                Some(request.leader_endpoint),
            )
            .await?;
            let lock = runtime_lock().await?;
            let runtime = lock.lock().await;
            return Ok(GovernanceAppendResponse {
                term: request.term,
                accepted: true,
                commit_index: runtime.persistent.commit_index.max(applied.epoch.max(1)),
            });
        }
    }

    let runtime = lock.lock().await;
    Ok(GovernanceAppendResponse {
        term: runtime.persistent.current_term,
        accepted: true,
        commit_index: runtime.persistent.commit_index,
    })
}

pub async fn handle_governance_abort(
    headers: &HeaderMap,
    request: GovernanceAbortRequest,
) -> Result<GovernanceAbortResponse, AppError> {
    validate_internal_token(headers).await?;
    let lock = runtime_lock().await?;
    let mut runtime = lock.lock().await;

    if request.term > runtime.persistent.current_term {
        runtime.persistent.current_term = request.term;
        runtime.persistent.voted_for = None;
    }

    let cleared = runtime
        .pending
        .as_ref()
        .map(|entry| entry.index == request.pending_index)
        .unwrap_or(false);
    if cleared {
        runtime.pending = None;
        persist_pending_entry(None).await?;
    }
    persist_runtime_state(&runtime).await?;

    Ok(GovernanceAbortResponse {
        term: runtime.persistent.current_term,
        cleared,
    })
}

pub async fn governance_cluster_fields() -> Result<N2GovernanceStateResponse, AppError> {
    governance_state_view().await
}

#[cfg(test)]
pub async fn reset_governance_for_tests() -> Result<(), AppError> {
    let membership = get_membership().await?;
    for path in [
        governance_state_path(),
        governance_pending_path(),
        governance_journal_path(),
    ] {
        if fs::try_exists(&path)
            .await
            .map_err(|e| AppError::Io(e, "check governance test artifact".to_string()))?
        {
            fs::remove_file(&path)
                .await
                .map_err(|e| AppError::Io(e, "remove governance test artifact".to_string()))?;
        }
    }

    let runtime = GovernanceRuntime {
        persistent: GovernancePersistentState {
            current_term: membership.epoch.max(1),
            voted_for: None,
            commit_index: membership.epoch.max(1),
            last_applied_membership_epoch: membership.epoch,
        },
        leader_id: None,
        leader_endpoint: None,
        leader_term: 0,
        leader_lease_deadline_ms: 0,
        pending: None,
    };

    if let Some(lock) = GOVERNANCE.get() {
        let mut guard = lock.lock().await;
        *guard = runtime.clone();
    } else {
        let _ = GOVERNANCE.set(Mutex::new(runtime.clone()));
    }

    persist_runtime_state(&runtime).await?;
    persist_pending_entry(None).await?;
    ensure_bootstrap_journal(&membership, &runtime.persistent).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution_core::model::{
        GovernanceAppendRequest, GovernanceMembershipEntry, NodeDescriptor, UpsertMembershipRequest,
    };
    use crate::distribution_core::state::{
        get_membership, stage_rollback_membership, stage_upsert_membership, upsert_membership,
    };
    use axum::Json as AxumJson;
    use axum::Router;
    use axum::extract::State as AxumState;
    use axum::http::{HeaderMap, HeaderValue, StatusCode};
    use axum::routing::post;
    use std::sync::{Arc, LazyLock};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex as AsyncMutex;

    static GOVERNANCE_TEST_LOCK: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));

    #[derive(Clone, Default)]
    struct MockGovernancePeer {
        prepared: Option<GovernanceMembershipEntry>,
        committed: Option<GovernanceMembershipEntry>,
        aborted_index: Option<u64>,
        vote_requests: usize,
    }

    fn internal_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            N2_INTERNAL_TOKEN_HEADER,
            HeaderValue::from_str(n2_runtime_config().internal_token.as_str())
                .expect("internal token"),
        );
        headers
    }

    fn local_node() -> NodeDescriptor {
        let cfg = n2_runtime_config();
        NodeDescriptor {
            node_id: cfg.node_id.clone(),
            endpoint: cfg.node_endpoint.clone(),
            epoch: 0,
            active: true,
        }
    }

    async fn spawn_mock_governance_peer() -> (
        String,
        Arc<AsyncMutex<MockGovernancePeer>>,
        tokio::task::JoinHandle<()>,
    ) {
        let state = Arc::new(AsyncMutex::new(MockGovernancePeer::default()));
        let app = Router::new()
            .route(
                "/v4/n2/internal/governance/request-vote",
                post(
                    |headers: HeaderMap,
                     AxumState(state): AxumState<Arc<AsyncMutex<MockGovernancePeer>>>,
                     AxumJson(payload): AxumJson<GovernanceVoteRequest>| async move {
                        let token_ok = headers
                            .get(N2_INTERNAL_TOKEN_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v == n2_runtime_config().internal_token.as_str())
                            .unwrap_or(false);
                        if !token_ok {
                            return Err::<AxumJson<GovernanceVoteResponse>, StatusCode>(
                                StatusCode::FORBIDDEN,
                            );
                        }
                        let mut guard = state.lock().await;
                        guard.vote_requests = guard.vote_requests.saturating_add(1);
                        Ok::<AxumJson<GovernanceVoteResponse>, StatusCode>(AxumJson(
                            GovernanceVoteResponse {
                                term: payload.term,
                                vote_granted: true,
                                leader_id: Some(payload.candidate_id),
                                leader_endpoint: Some(payload.candidate_endpoint),
                                commit_index: payload.last_commit_index,
                            },
                        ))
                    },
                ),
            )
            .route(
                "/v4/n2/internal/governance/append",
                post(
                    |headers: HeaderMap,
                     AxumState(state): AxumState<Arc<AsyncMutex<MockGovernancePeer>>>,
                     AxumJson(payload): AxumJson<GovernanceAppendRequest>| async move {
                        let token_ok = headers
                            .get(N2_INTERNAL_TOKEN_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v == n2_runtime_config().internal_token.as_str())
                            .unwrap_or(false);
                        if !token_ok {
                            return Err::<AxumJson<GovernanceAppendResponse>, StatusCode>(
                                StatusCode::FORBIDDEN,
                            );
                        }
                        let mut guard = state.lock().await;
                        if let Some(entry) = payload.entry.clone() {
                            if payload.leader_commit_index >= entry.index {
                                guard.committed = Some(entry);
                            } else {
                                guard.prepared = Some(entry);
                            }
                        }
                        let commit_index = guard
                            .committed
                            .as_ref()
                            .map(|entry| entry.index)
                            .unwrap_or(payload.prev_commit_index);
                        Ok::<AxumJson<GovernanceAppendResponse>, StatusCode>(AxumJson(
                            GovernanceAppendResponse {
                                term: payload.term,
                                accepted: true,
                                commit_index,
                            },
                        ))
                    },
                ),
            )
            .route(
                "/v4/n2/internal/governance/abort",
                post(
                    |headers: HeaderMap,
                     AxumState(state): AxumState<Arc<AsyncMutex<MockGovernancePeer>>>,
                     AxumJson(payload): AxumJson<GovernanceAbortRequest>| async move {
                        let token_ok = headers
                            .get(N2_INTERNAL_TOKEN_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v == n2_runtime_config().internal_token.as_str())
                            .unwrap_or(false);
                        if !token_ok {
                            return Err::<AxumJson<GovernanceAbortResponse>, StatusCode>(
                                StatusCode::FORBIDDEN,
                            );
                        }
                        let mut guard = state.lock().await;
                        guard.aborted_index = Some(payload.pending_index);
                        guard.prepared = None;
                        Ok::<AxumJson<GovernanceAbortResponse>, StatusCode>(AxumJson(
                            GovernanceAbortResponse {
                                term: payload.term,
                                cleared: true,
                            },
                        ))
                    },
                ),
            )
            .with_state(Arc::clone(&state));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind governance peer");
        let addr = listener.local_addr().expect("governance peer addr");
        let join = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve governance peer");
        });
        (format!("http://{}", addr), state, join)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn governance_runtime_initializes_from_membership() {
        let _guard = GOVERNANCE_TEST_LOCK.lock().await;
        reset_governance_for_tests()
            .await
            .expect("reset governance");
        let _ = initialize_governance_runtime()
            .await
            .expect("init governance");
        let view = governance_state_view().await.expect("governance view");
        assert!(view.current_term >= 1);
        assert!(view.commit_index >= 1);
        assert!(view.last_applied_membership_epoch >= 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn governance_vote_rejects_stale_candidate_term() {
        let _guard = GOVERNANCE_TEST_LOCK.lock().await;
        let previous = get_membership().await.expect("previous membership");
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node()],
        })
        .await
        .expect("single-node membership");
        reset_governance_for_tests()
            .await
            .expect("reset governance");

        let response = handle_governance_vote(
            &internal_headers(),
            GovernanceVoteRequest {
                term: 0,
                candidate_id: "candidate-a".to_string(),
                candidate_endpoint: "https://127.0.0.1:52001".to_string(),
                last_commit_index: 0,
                last_applied_membership_epoch: 0,
            },
        )
        .await
        .expect("vote response");

        assert!(!response.vote_granted);
        assert!(response.term >= 1);

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous.nodes,
        })
        .await;
        reset_governance_for_tests().await.expect("reset restore");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn governance_append_prepare_then_commit_updates_follower_state() {
        let _guard = GOVERNANCE_TEST_LOCK.lock().await;
        let previous = get_membership().await.expect("previous membership");
        let current = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node()],
        })
        .await
        .expect("single-node membership");
        reset_governance_for_tests()
            .await
            .expect("reset governance");

        let (_, next) = stage_upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node()],
        })
        .await
        .expect("stage membership");
        let entry = GovernanceMembershipEntry {
            index: current.epoch.saturating_add(1),
            term: current.epoch.saturating_add(1),
            membership: next.clone(),
        };

        let prepared = handle_governance_append(
            &internal_headers(),
            GovernanceAppendRequest {
                term: entry.term,
                leader_id: "leader-a".to_string(),
                leader_endpoint: "https://10.0.0.1:50001".to_string(),
                prev_commit_index: current.epoch,
                leader_commit_index: current.epoch,
                entry: Some(entry.clone()),
            },
        )
        .await
        .expect("prepare append");
        assert!(prepared.accepted);
        let prepared_state = governance_state_view().await.expect("prepared state");
        assert_eq!(prepared_state.pending_index, Some(entry.index));
        assert_eq!(
            get_membership()
                .await
                .expect("membership before commit")
                .epoch,
            current.epoch
        );

        let committed = handle_governance_append(
            &internal_headers(),
            GovernanceAppendRequest {
                term: entry.term,
                leader_id: "leader-a".to_string(),
                leader_endpoint: "https://10.0.0.1:50001".to_string(),
                prev_commit_index: current.epoch,
                leader_commit_index: entry.index,
                entry: Some(entry.clone()),
            },
        )
        .await
        .expect("commit append");
        assert!(committed.accepted);
        let committed_state = governance_state_view().await.expect("committed state");
        assert_eq!(committed_state.pending_index, None);
        assert_eq!(committed_state.commit_index, entry.index);
        assert_eq!(
            get_membership()
                .await
                .expect("membership after commit")
                .epoch,
            next.epoch
        );

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous.nodes,
        })
        .await;
        reset_governance_for_tests().await.expect("reset restore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn governance_commit_notifies_new_node_after_cluster_expansion() {
        let _guard = GOVERNANCE_TEST_LOCK.lock().await;
        let previous = get_membership().await.expect("previous membership");
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node()],
        })
        .await
        .expect("single-node membership");
        reset_governance_for_tests()
            .await
            .expect("reset governance");

        let (remote_endpoint, remote_state, remote_server) = spawn_mock_governance_peer().await;
        let (_, next) = stage_upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node(),
                NodeDescriptor {
                    node_id: format!("remote-{}", ulid::Ulid::new()),
                    endpoint: remote_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("stage expanded membership");

        let applied = commit_membership_change(next.clone())
            .await
            .expect("commit membership change");
        assert_eq!(applied.epoch, next.epoch);

        let remote = remote_state.lock().await.clone();
        assert_eq!(remote.vote_requests, 0, "new node is not in current quorum");
        let committed = remote
            .committed
            .expect("new node should receive committed membership");
        assert_eq!(committed.membership.epoch, next.epoch);
        assert_eq!(committed.membership.nodes.len(), 2);

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous.nodes,
        })
        .await;
        reset_governance_for_tests().await.expect("reset restore");
        remote_server.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn governance_initialization_materializes_baseline_snapshot_for_rollback() {
        let _guard = GOVERNANCE_TEST_LOCK.lock().await;
        let previous = get_membership().await.expect("previous membership");
        let current = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node()],
        })
        .await
        .expect("single-node membership");
        reset_governance_for_tests()
            .await
            .expect("reset governance");
        initialize_governance_runtime()
            .await
            .expect("initialize governance");

        let (_, next) = stage_upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node(),
                NodeDescriptor {
                    node_id: format!("remote-{}", ulid::Ulid::new()),
                    endpoint: "https://127.0.0.1:52009".to_string(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("stage expanded membership");

        let applied = commit_membership_change(next)
            .await
            .expect("commit membership");
        assert!(applied.epoch > current.epoch);

        let (_, rolled_back) = stage_rollback_membership(current.epoch)
            .await
            .expect("rollback snapshot should exist");
        assert_eq!(rolled_back.epoch, current.epoch);

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous.nodes,
        })
        .await;
        reset_governance_for_tests().await.expect("reset restore");
    }
}
