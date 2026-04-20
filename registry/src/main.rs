mod auth;
mod config;
mod create_env;
mod db;
mod error;
mod models;
mod realtime;
mod storage_broker;
mod totp;

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use auth::{
    AuthSettings, claims_from_headers, claims_from_token, issue_access_token, require_admin_claims,
    require_master_key, verify_password,
};
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State, WebSocketUpgrade};
use axum::http::HeaderName;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use axum_server::tls_rustls::RustlsConfig;
use chrono::{Local, Utc};
use config::{RegistryConfig, normalize_backend_url};
use db::{PendingUploadReservation, RegistryDb};
use error::RegistryError;
use models::{
    AdminStatsResponse, AdminUserListResponse, AdminUserSummary, ArchiveObjectsRequest,
    BootstrapAdminRequest, BootstrapAdminResponse, BulkObjectKeysRequest,
    BulkObjectMutationResponse, BulkObjectSelectionResponse, CreateShareRequest,
    CreateShareResponse, CreateUserRequest, CreateUserResponse, DeleteObjectQuery,
    DeleteObjectVersionRequest, DeleteObjectVersionResponse, DeleteUserResponse,
    DisableTwoFactorRequest, EnableTwoFactorRequest, FinalizeObjectRequest, FragmentListResponse,
    FragmentStateRequest, LinkFragmentRequest, LinkFragmentResponse, ListObjectsQuery,
    ListObjectsResponse, ListSharesQuery, ListSharesResponse, LoginRequest, LoginResponse,
    ObjectDeleteResponse, ObjectMutationResponse, ObjectStateRequest, ObjectStatsQuery,
    ObjectStatsResponse, PrefixObjectMutationRequest, RefreshRequest, RefreshResponse,
    RegisterObjectRequest, RenameRegistryObjectRequest, RenameRegistryPrefixRequest,
    ResolveObjectQuery, RevokeShareResponse, SetupTwoFactorRequest, SignupRequest, SignupResponse,
    StorageSessionRequest, StorageSessionResponse, SuggestObjectsQuery, SuggestObjectsResponse,
    TwoFactorSetupResponse, UpdateCredentialsRequest, UpdateEmailRequest, UpdatePasswordRequest,
    UpdateQuotaRequest, UserMutationResponse, UserRecord,
};
use realtime::{
    DeleteObjectNotice, DeleteUserNotice, FragmentNotice, RegistryRealtimeHub,
    RegistryRealtimePayload, RenameObjectNotice, SessionNotice, StateInvalidatedNotice,
    TwoFactorNotice, WsAuthParams,
};
use std::time::Duration;
use storage_broker::{EngineUploadSession, StorageBroker};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use tracing::warn;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
struct AppState {
    db: RegistryDb,
    auth: Arc<AuthSettings>,
    broker: StorageBroker,
    realtime: RegistryRealtimeHub,
    write_gate: Arc<Mutex<()>>,
    pending_upload_grace_secs: u64,
}

const MAX_SHARE_EXPIRATION_SECS: u64 = 60 * 60 * 24 * 7;
const PENDING_UPLOAD_RECONCILE_INTERVAL_SECS: u64 = 60;
const OBJECT_PURGE_QUEUE_INTERVAL_SECS: u64 = 2;
const OBJECT_PURGE_QUEUE_RETRY_SECS: i64 = 15;

fn default_fragment_name(user: &UserRecord) -> String {
    user.email.clone()
}

fn publish_owner_event(state: &AppState, owner_uid: &str, payload: RegistryRealtimePayload) {
    state.realtime.publish_owner(owner_uid, payload);
}

fn publish_owner_invalidation(
    state: &AppState,
    owner_uid: &str,
    fragment: Option<String>,
    scope: &str,
) {
    publish_owner_event(
        state,
        owner_uid,
        RegistryRealtimePayload::StateInvalidated(StateInvalidatedNotice {
            fragment,
            scope: scope.to_string(),
        }),
    );
}

fn publish_admin_event(state: &AppState, payload: RegistryRealtimePayload) {
    state.realtime.publish_admin(payload);
}

#[derive(Debug)]
enum EngineListingEntry {
    Object(models::ObjectRecord),
    CommonPrefix(String),
}

fn registry_rfc3339_from_unix(timestamp: i64) -> String {
    chrono::DateTime::<Utc>::from_timestamp(timestamp, 0)
        .unwrap_or_else(Utc::now)
        .to_rfc3339()
}

fn normalized_optional_query(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

async fn list_objects_via_engine(
    state: &AppState,
    owner_uid: &str,
    binding: &models::FragmentBindingRecord,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    search: Option<&str>,
    limit: usize,
    offset: usize,
    include_trashed: bool,
) -> Result<db::RegistryListPage, RegistryError> {
    let fragment = binding.fragment.clone();
    let search = normalized_optional_query(search);
    let target_count = offset.saturating_add(limit).saturating_add(1);
    let page_size = target_count.clamp(1, 1000);
    let mut entries = Vec::new();
    let mut seen_prefixes = std::collections::BTreeSet::new();
    let mut continuation_token = None;
    let mut has_more = false;

    loop {
        let page = state
            .broker
            .list_logical_objects_page(
                binding,
                prefix,
                delimiter,
                page_size,
                continuation_token.as_deref(),
                include_trashed,
            )
            .await?;

        for common_prefix in page.common_prefixes {
            if let Some(search) = search.as_deref() {
                if !common_prefix.contains(search) {
                    continue;
                }
            }
            if seen_prefixes.insert(common_prefix.clone()) {
                entries.push(EngineListingEntry::CommonPrefix(common_prefix));
                if entries.len() >= target_count {
                    has_more = true;
                    break;
                }
            }
        }
        if has_more {
            break;
        }

        for object in page.objects {
            if let Some(search) = search.as_deref() {
                if !object.object_key.contains(search) {
                    continue;
                }
            }

            let stored = state
                .db
                .resolve_stored_object_record(
                    owner_uid.to_string(),
                    fragment.clone(),
                    object.object_key.clone(),
                )
                .await?;
            let created_at = stored
                .as_ref()
                .map(|record| record.created_at.clone())
                .unwrap_or_else(|| registry_rfc3339_from_unix(object.created_at_unix));
            let updated_at = registry_rfc3339_from_unix(object.created_at_unix);

            entries.push(EngineListingEntry::Object(models::ObjectRecord {
                owner_uid: owner_uid.to_string(),
                fragment: fragment.clone(),
                object_key: object.object_key.clone(),
                storage_object_id: stored
                    .as_ref()
                    .map(|record| record.storage_object_id.clone())
                    .unwrap_or_else(|| object.object_key.clone()),
                version_id: object.version_id,
                status: if include_trashed {
                    "trashed".to_string()
                } else {
                    "ready".to_string()
                },
                object_size_bytes: object.object_size_bytes.max(0),
                content_type: object.content_type,
                created_at,
                updated_at,
            }));

            if entries.len() >= target_count {
                has_more = true;
                break;
            }
        }
        if has_more {
            break;
        }

        continuation_token = page.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    let paged_entries = entries
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    for entry in paged_entries {
        match entry {
            EngineListingEntry::Object(object) => objects.push(object),
            EngineListingEntry::CommonPrefix(prefix) => common_prefixes.push(prefix),
        }
    }

    Ok(db::RegistryListPage {
        objects,
        common_prefixes,
        has_more: has_more || continuation_token.is_some(),
    })
}

async fn object_stats_via_engine(
    state: &AppState,
    binding: &models::FragmentBindingRecord,
    prefix: Option<&str>,
    search: Option<&str>,
    include_trashed: bool,
) -> Result<db::RegistryObjectStats, RegistryError> {
    let search = normalized_optional_query(search);
    let mut continuation_token = None;
    let mut total_objects = 0usize;
    let mut total_size_bytes = 0i64;
    let mut extension_stats = std::collections::BTreeMap::<String, (usize, i64)>::new();

    loop {
        let page = state
            .broker
            .list_logical_objects_page(
                binding,
                prefix,
                None,
                1000,
                continuation_token.as_deref(),
                include_trashed,
            )
            .await?;
        for object in page.objects {
            if let Some(search) = search.as_deref() {
                if !object.object_key.contains(search) {
                    continue;
                }
            }
            total_objects = total_objects.saturating_add(1);
            total_size_bytes = total_size_bytes.saturating_add(object.object_size_bytes.max(0));
            let extension = object
                .object_key
                .rsplit_once('.')
                .map(|(_, value)| value.trim().to_lowercase())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "_none".to_string());
            let entry = extension_stats.entry(extension).or_insert((0usize, 0i64));
            entry.0 += 1;
            entry.1 = entry.1.saturating_add(object.object_size_bytes.max(0));
        }
        continuation_token = page.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    let mut extension_stats = extension_stats
        .into_iter()
        .map(
            |(extension, (object_count, total_size_bytes))| models::ObjectExtensionStat {
                extension,
                object_count,
                total_size_bytes,
            },
        )
        .collect::<Vec<_>>();
    extension_stats.sort_by(|a, b| {
        b.total_size_bytes
            .cmp(&a.total_size_bytes)
            .then_with(|| a.extension.cmp(&b.extension))
    });

    Ok(db::RegistryObjectStats {
        total_objects,
        total_size_bytes,
        extension_stats,
    })
}

fn canonical_bulk_object_keys(keys: Vec<String>) -> Vec<String> {
    keys.into_iter()
        .map(|value| value.trim().trim_start_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn infer_latest_engine_head(
    versions: &[storage_broker::EngineObjectMetadata],
) -> Option<storage_broker::EngineObjectMetadata> {
    versions.iter().cloned().max_by(|left, right| {
        left.created_at_unix
            .cmp(&right.created_at_unix)
            .then_with(|| left.version_id.cmp(&right.version_id))
    })
}

async fn issue_registry_session(
    state: &AppState,
    user: &UserRecord,
) -> Result<(String, u64, String, u64), RegistryError> {
    let (access_token, expires_in) = issue_access_token(user, &state.auth)?;
    let (refresh_token, refresh_expires_in) = state
        .db
        .create_refresh_session(user.uid.clone(), state.auth.refresh_token_ttl_secs)
        .await?;
    Ok((access_token, expires_in, refresh_token, refresh_expires_in))
}

fn normalized_upload_tracking_value(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn pending_upload_age_secs(reservation: &PendingUploadReservation) -> Option<i64> {
    let parse = |value: &str| chrono::DateTime::parse_from_rfc3339(value).ok();
    let timestamp = parse(&reservation.updated_at).or_else(|| parse(&reservation.created_at))?;
    Some(
        Utc::now()
            .signed_duration_since(timestamp.with_timezone(&Utc))
            .num_seconds()
            .max(0),
    )
}

fn object_record_age_secs(object: &models::ObjectRecord) -> Option<i64> {
    let parse = |value: &str| chrono::DateTime::parse_from_rfc3339(value).ok();
    let timestamp = parse(&object.updated_at).or_else(|| parse(&object.created_at))?;
    Some(
        Utc::now()
            .signed_duration_since(timestamp.with_timezone(&Utc))
            .num_seconds()
            .max(0),
    )
}

fn reservation_matches_active_session(
    reservation: &PendingUploadReservation,
    active_sessions: &[EngineUploadSession],
) -> bool {
    if let Some(session_id) = normalized_upload_tracking_value(reservation.session_id.as_deref()) {
        if active_sessions
            .iter()
            .any(|session| session.session_id == session_id)
        {
            return true;
        }
    }
    if let Some(version_id) = normalized_upload_tracking_value(reservation.version_id.as_deref()) {
        if active_sessions
            .iter()
            .any(|session| session.version_id == version_id)
        {
            return true;
        }
    }
    active_sessions
        .iter()
        .any(|session| session.object_key.as_deref() == Some(reservation.object_key.as_str()))
}

async fn reconcile_pending_uploads(
    state: &AppState,
    owner_uid: Option<String>,
) -> Result<Vec<(String, String)>, RegistryError> {
    let reservations = state.db.list_pending_upload_reservations(owner_uid).await?;
    if reservations.is_empty() {
        return Ok(Vec::new());
    }

    let mut grouped =
        std::collections::BTreeMap::<(String, String), Vec<PendingUploadReservation>>::new();
    for reservation in reservations {
        grouped
            .entry((reservation.owner_uid.clone(), reservation.fragment.clone()))
            .or_default()
            .push(reservation);
    }

    let legacy_grace_secs = state.pending_upload_grace_secs.max(300) as i64;
    let mut stale = Vec::new();

    for ((owner_uid, fragment), group) in grouped {
        let binding = state
            .db
            .get_fragment_binding(owner_uid.clone(), fragment.clone())
            .await?;
        let Some(binding) = binding else {
            stale.extend(group);
            continue;
        };

        let active_sessions = match state.broker.list_active_upload_sessions(&binding).await {
            Ok(active_sessions) => active_sessions,
            Err(err) => {
                warn!(
                    "pending upload reconciliation skipped for {}/{}: {}",
                    owner_uid, fragment, err.message
                );
                continue;
            }
        };

        for reservation in group {
            let has_tracking = normalized_upload_tracking_value(reservation.version_id.as_deref())
                .is_some()
                || normalized_upload_tracking_value(reservation.session_id.as_deref()).is_some();
            let active = reservation_matches_active_session(&reservation, &active_sessions);
            let should_reap = if has_tracking {
                !active
            } else {
                !active
                    && pending_upload_age_secs(&reservation).unwrap_or_default()
                        >= legacy_grace_secs
            };
            if should_reap {
                stale.push(reservation);
            }
        }
    }

    if stale.is_empty() {
        return Ok(Vec::new());
    }

    state.db.remove_pending_upload_reservations(stale).await
}

fn publish_pending_upload_cleanup_invalidations(state: &AppState, scopes: Vec<(String, String)>) {
    for (owner_uid, fragment) in scopes {
        publish_owner_invalidation(state, &owner_uid, Some(fragment.clone()), "objects");
        publish_owner_invalidation(state, &owner_uid, Some(fragment), "account_profile");
    }
}

async fn run_pending_upload_reconciler(state: AppState) {
    let mut interval =
        tokio::time::interval(Duration::from_secs(PENDING_UPLOAD_RECONCILE_INTERVAL_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        match reconcile_pending_uploads(&state, None).await {
            Ok(scopes) => {
                if !scopes.is_empty() {
                    let removed = scopes.len();
                    publish_pending_upload_cleanup_invalidations(&state, scopes);
                    warn!(
                        "registry pending upload reconciliation removed {} stale reservation scope(s)",
                        removed
                    );
                }
            }
            Err(err) => {
                warn!(
                    "registry pending upload reconciliation failed: {}",
                    err.message
                );
            }
        }
    }
}

async fn process_next_object_purge(state: &AppState) -> Result<bool, RegistryError> {
    let _write_guard = state.write_gate.lock().await;
    let Some(entry) = state.db.peek_due_object_purge().await? else {
        return Ok(false);
    };

    let binding = state
        .db
        .get_fragment_binding(entry.owner_uid.clone(), entry.fragment.clone())
        .await?;

    match binding {
        Some(binding) => match state
            .broker
            .soft_delete_object(&binding, &entry.object_key)
            .await
        {
            Ok(()) => {
                state
                    .db
                    .complete_object_purge(
                        entry.owner_uid.clone(),
                        entry.fragment.clone(),
                        entry.object_key.clone(),
                    )
                    .await?;
            }
            Err(err) => {
                warn!(
                    "object purge queue failed for {}/{}/{} (attempt {}): {}",
                    entry.owner_uid,
                    entry.fragment,
                    entry.object_key,
                    entry.attempts.saturating_add(1),
                    err.message
                );
                state
                    .db
                    .fail_object_purge(
                        entry.owner_uid.clone(),
                        entry.fragment.clone(),
                        entry.object_key.clone(),
                        err.message,
                        OBJECT_PURGE_QUEUE_RETRY_SECS,
                    )
                    .await?;
            }
        },
        None => {
            let message = "fragment is not linked to a storage backend".to_string();
            warn!(
                "object purge queue deferred for {}/{}/{}: {}",
                entry.owner_uid, entry.fragment, entry.object_key, message
            );
            state
                .db
                .fail_object_purge(
                    entry.owner_uid.clone(),
                    entry.fragment.clone(),
                    entry.object_key.clone(),
                    message,
                    OBJECT_PURGE_QUEUE_RETRY_SECS,
                )
                .await?;
        }
    }

    Ok(true)
}

async fn run_object_purge_reconciler(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(OBJECT_PURGE_QUEUE_INTERVAL_SECS));
    loop {
        interval.tick().await;
        loop {
            match process_next_object_purge(&state).await {
                Ok(true) => continue,
                Ok(false) => break,
                Err(err) => {
                    warn!("object purge reconciler iteration failed: {}", err.message);
                    break;
                }
            }
        }
    }
}

async fn ensure_user_default_fragment(
    state: &AppState,
    user: &UserRecord,
) -> Result<String, RegistryError> {
    let existing = state.db.list_fragments(user.uid.clone()).await?;
    if let Some(fragment) = existing.first() {
        return Ok(fragment.clone());
    }

    let fragment_name = default_fragment_name(user);
    let (deployment, fragment_hash) = state.broker.ensure_default_fragment(&fragment_name).await?;
    state
        .db
        .upsert_fragment_binding(user.uid.clone(), fragment_hash.clone(), deployment, None)
        .await?;
    Ok(fragment_hash)
}

async fn ensure_user_default_fragment_if_needed(
    state: &AppState,
    user: &UserRecord,
) -> Result<Option<String>, RegistryError> {
    if user.is_admin {
        return Ok(None);
    }
    ensure_user_default_fragment(state, user).await.map(Some)
}

async fn create_user_with_initial_storage_if_needed(
    state: &AppState,
    email: String,
    password_hash: String,
    quota_limit_bytes: Option<i64>,
    is_admin: bool,
) -> Result<(UserRecord, Option<String>), RegistryError> {
    let user = state
        .db
        .create_user(email, password_hash, quota_limit_bytes, is_admin)
        .await?;
    match ensure_user_default_fragment_if_needed(state, &user).await {
        Ok(fragment) => Ok((user, fragment)),
        Err(err) => {
            if let Err(cleanup_err) = state.db.delete_user_cascade(user.uid.clone()).await {
                warn!(
                    "failed to roll back registry user {} after fragment provisioning error: {}",
                    user.uid, cleanup_err.message
                );
            }
            Err(err)
        }
    }
}

fn legacy_fragment_name(uid: &str) -> String {
    format!("registry-user-{}", uid)
}

fn hash_fragment_name(name: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hex::encode(hasher.finalize())
}

fn managed_fragment_candidates(user: &UserRecord) -> Vec<String> {
    vec![
        hash_fragment_name(&user.email),
        hash_fragment_name(&legacy_fragment_name(&user.uid)),
    ]
}

fn normalize_email(email: &str) -> Result<String, RegistryError> {
    let normalized = email.trim().to_lowercase();
    if normalized.is_empty() {
        return Err(RegistryError::bad_request("email is required"));
    }
    Ok(normalized)
}

fn validate_new_password(password: &str) -> Result<(), RegistryError> {
    if password.len() < 8 {
        return Err(RegistryError::bad_request(
            "password must be at least 8 characters",
        ));
    }
    Ok(())
}

fn verify_optional_totp(
    auth_user: &models::UserAuthRecord,
    otp: Option<&str>,
) -> Result<(), RegistryError> {
    if !auth_user.user.two_factor_enabled {
        return Ok(());
    }
    let code = otp
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RegistryError::unauthorized("OTP required"))?;
    let secret = auth_user
        .totp_secret
        .as_deref()
        .ok_or_else(|| RegistryError::unauthorized("2FA is not configured correctly"))?;
    if !totp::verify_totp(secret, code, 1) {
        return Err(RegistryError::unauthorized("invalid OTP"));
    }
    Ok(())
}

async fn rename_managed_fragment_for_email_change(
    state: &AppState,
    user: &UserRecord,
    next_email: &str,
) -> Result<(), RegistryError> {
    let bindings = state.db.list_fragment_bindings(user.uid.clone()).await?;
    let managed = managed_fragment_candidates(user);
    let Some(binding) = bindings
        .into_iter()
        .find(|binding| managed.iter().any(|value| value == &binding.fragment))
    else {
        return Ok(());
    };

    let next_fragment = state.broker.rename_fragment(&binding, next_email).await?;
    if next_fragment != binding.fragment {
        state
            .db
            .rename_fragment_references(user.uid.clone(), binding.fragment, next_fragment)
            .await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install 'ring' as the default TLS provider.");

    warn_if_running_as_root("registry-server");

    if let Err(err) = run().await {
        eprintln!("registry startup failed: {}", err);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = RegistryConfig::load()?;
    validate_runtime_paths(&config)?;
    init_tracing();

    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    tracing::info!("╔══════════════════════════════════════════════╗");
    tracing::info!("║         R E G I S T R Y  S E R V E R         ║");
    tracing::info!("╚══════════════════════════════════════════════╝");
    tracing::info!("│ Version        : {}", env!("CARGO_PKG_VERSION"));
    tracing::info!("│ Status         : ONLINE");
    tracing::info!("│ Start Time     : {}", current_time);
    tracing::info!("│ PID            : {}", std::process::id());
    tracing::info!(
        "│ Hostname       : {}",
        hostname::get().unwrap_or_default().to_string_lossy()
    );
    tracing::info!(
        "│ Working Dir    : {}",
        std::env::current_dir().unwrap_or_default().display()
    );
    tracing::info!("╞═════════ ACTIVE CONNECTIONS ═════════╡");

    let db = RegistryDb::new(config.sqlite_path.clone(), config.default_quota_bytes);
    db.init()
        .await
        .map_err(|err| format!("failed to initialize registry db: {}", err.message))?;

    let state = AppState {
        db,
        auth: Arc::new(AuthSettings {
            jwt_secret: config.jwt_secret.clone(),
            master_key: config.master_key.clone(),
            access_token_ttl_secs: config.access_token_ttl_secs,
            refresh_token_ttl_secs: config.refresh_token_ttl_secs,
        }),
        broker: StorageBroker::new(Arc::new(config.clone()))
            .map_err(|err| format!("failed to initialize storage broker: {}", err.message))?,
        realtime: RegistryRealtimeHub::new(),
        write_gate: Arc::new(Mutex::new(())),
        pending_upload_grace_secs: config.storage_session_ttl_secs,
    };

    tokio::spawn(run_pending_upload_reconciler(state.clone()));
    tokio::spawn(run_object_purge_reconciler(state.clone()));

    let app = build_router(state);
    let addr: SocketAddr = format!("{}:{}", config.host, config.https_port)
        .parse()
        .map_err(|err| format!("invalid registry bind address: {}", err))?;
    let tls = RustlsConfig::from_pem_file(config.cert_path, config.key_path)
        .await
        .map_err(|err| format!("failed to load registry TLS config: {}", err))?;

    serve(addr, app, tls)
        .await
        .map_err(|err| format!("registry server failed: {}", err))
}

async fn serve(socket: SocketAddr, app: Router, cert: RustlsConfig) -> Result<(), std::io::Error> {
    tracing::info!("Secured port => {} type: https", socket);
    let handle = Arc::new(axum_server::Handle::new());

    tokio::spawn(shutdown_signal(handle.clone()));

    let mut server = axum_server::bind_rustls(socket, cert).handle((*handle).clone());
    server.http_builder().http2().enable_connect_protocol();

    server
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
}

pub async fn shutdown_signal(handle: Arc<axum_server::Handle<SocketAddr>>) {
    #[cfg(unix)]
    let terminator = async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut interrupt =
            signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");

        tokio::select! {
            _ = terminate.recv() => {},
            _ = interrupt.recv() => {},
        }
    };

    #[cfg(not(unix))]
    let terminator = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c event");
    };

    terminator.await;

    tracing::info!("Shutdown signal received. Stopping server...");
    handle.graceful_shutdown(Some(Duration::from_secs(10)));
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/admin/bootstrap", post(bootstrap_admin))
        .route("/v1/admin/auth/login", post(admin_login))
        .route("/v1/admin/users", post(create_user))
        .route("/v1/admin/users", get(list_admin_users))
        .route("/v1/admin/stats", get(get_admin_stats))
        .route("/v1/admin/users/{uid}/quota", post(update_admin_user_quota))
        .route(
            "/v1/admin/users/{uid}/credentials",
            post(update_admin_user_credentials),
        )
        .route(
            "/v1/admin/users/{uid}/2fa/setup",
            post(admin_setup_user_2fa),
        )
        .route(
            "/v1/admin/users/{uid}/2fa/disable",
            post(admin_disable_user_2fa),
        )
        .route("/v1/admin/users/{uid}", delete(delete_admin_user))
        .route("/v1/auth/signup", post(signup))
        .route("/v1/auth/login", post(login))
        .route("/v1/auth/refresh", post(refresh))
        .route("/v1/auth/logout", post(logout))
        .route("/v1/ws", get(registry_ws))
        .route("/v1/me", get(me))
        .route("/v1/account/email", post(update_account_email))
        .route("/v1/account/password", post(update_account_password))
        .route("/v1/account/2fa/setup", post(setup_account_2fa))
        .route("/v1/account/2fa/enable", post(enable_account_2fa))
        .route("/v1/account/2fa/disable", post(disable_account_2fa))
        .route("/v1/fragments", get(list_fragments))
        .route("/v1/fragments/link", post(link_fragment))
        .route("/v1/storage/session", post(issue_storage_session))
        .route("/v1/shares", post(create_share))
        .route("/v1/shares", get(list_shares))
        .route("/v1/shares/{share_id}", delete(revoke_share))
        .route("/v1/public/shares/{token}", get(get_public_share))
        .route("/v1/objects/register", post(register_object))
        .route("/v1/objects/finalize", post(finalize_object))
        .route("/v1/objects/pending", delete(cancel_pending_object))
        .route("/v1/objects/suggest", get(suggest_objects))
        .route("/v1/objects/rename", post(rename_object))
        .route("/v1/objects/rename-prefix", post(rename_object_prefix))
        .route("/v1/objects/version/delete", post(delete_object_version))
        .route("/v1/objects/trash/bulk", post(trash_objects_bulk))
        .route("/v1/objects/trash-prefix", post(trash_objects_prefix))
        .route("/v1/objects/trash", post(trash_object))
        .route("/v1/objects/recover/bulk", post(recover_objects_bulk))
        .route("/v1/objects/recover-prefix", post(recover_objects_prefix))
        .route("/v1/objects/recover", post(recover_object))
        .route("/v1/objects/recover-all", post(recover_all_objects))
        .route("/v1/objects/permanent-prefix", post(delete_objects_prefix))
        .route("/v1/objects/trash/empty", post(empty_trash))
        .route("/v1/objects", delete(delete_object))
        .route("/v1/objects", get(list_objects))
        .route("/v1/objects/stats", get(object_stats))
        .route("/v1/objects/archive", post(archive_objects))
        .route("/v1/objects/resolve", get(resolve_object))
        .layer(build_cors_layer())
        .with_state(state)
}

fn build_cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_headers([
            HeaderName::from_static("authorization"),
            HeaderName::from_static("content-type"),
            HeaderName::from_static("x-registry-master-key"),
            HeaderName::from_static("x-forwarded-proto"),
            HeaderName::from_static("x-forwarded-host"),
        ])
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PUT,
            axum::http::Method::PATCH,
            axum::http::Method::DELETE,
            axum::http::Method::OPTIONS,
        ])
        .max_age(Duration::from_secs(86400))
}

fn public_base_url(headers: &HeaderMap) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("https");
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(header::HOST))
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("127.0.0.1:51001");
    format!("{}://{}", scheme, host)
}

fn build_public_share_url(headers: &HeaderMap, token: &str) -> String {
    format!("{}/v1/public/shares/{}", public_base_url(headers), token)
}

fn copy_upstream_header(
    builder: axum::http::response::Builder,
    upstream: &reqwest::Response,
    header_name: axum::http::header::HeaderName,
) -> axum::http::response::Builder {
    if let Some(value) = upstream.headers().get(header_name.as_str()) {
        if let Ok(value) = axum::http::HeaderValue::from_bytes(value.as_bytes()) {
            return builder.header(header_name, value);
        }
    }
    builder
}

async fn health(
    State(state): State<AppState>,
) -> Result<Json<models::HealthResponse>, RegistryError> {
    Ok(Json(state.db.health().await?))
}

async fn registry_ws(
    ws: WebSocketUpgrade,
    Query(params): Query<WsAuthParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let claims = match claims_from_token(&params.token, &state.auth) {
        Ok(claims) => claims,
        Err(err) => return err.into_response(),
    };
    let hub = state.realtime.clone();
    ws.on_upgrade(move |socket| realtime::handle_socket(socket, hub, claims))
        .into_response()
}

async fn bootstrap_admin(
    State(state): State<AppState>,
    Json(req): Json<BootstrapAdminRequest>,
) -> Result<(StatusCode, Json<BootstrapAdminResponse>), RegistryError> {
    let _write_guard = state.write_gate.lock().await;
    if state.db.count_admin_users().await? > 0 {
        return Err(RegistryError::conflict(
            "registry administrator is already configured",
        ));
    }
    if req.master_key.trim() != state.auth.master_key {
        return Err(RegistryError::forbidden("invalid registry master key"));
    }
    let email = normalize_email(&req.email)?;
    validate_new_password(&req.password)?;
    let password_hash = auth::hash_password(&req.password)?;
    let user = state
        .db
        .create_user(email, password_hash, None, true)
        .await?;
    let (access_token, expires_in, refresh_token, refresh_expires_in) =
        issue_registry_session(&state, &user).await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::UserCreated(user.clone()),
    );
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::AccountSessionUpdated(SessionNotice {
            uid: user.uid.clone(),
            email: user.email.clone(),
            active: true,
        }),
    );
    Ok((
        StatusCode::CREATED,
        Json(BootstrapAdminResponse {
            access_token,
            token_type: "Bearer",
            expires_in,
            refresh_token,
            refresh_expires_in,
            user,
        }),
    ))
}

async fn admin_login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, RegistryError> {
    let _write_guard = state.write_gate.lock().await;
    if state.db.count_admin_users().await? == 0 {
        return Err(RegistryError::conflict(
            "registry administrator is not configured yet",
        ));
    }
    let email = normalize_email(&req.email)?;
    let auth_user = state
        .db
        .find_user_auth_by_email(email)
        .await?
        .ok_or_else(|| RegistryError::unauthorized("invalid credentials"))?;
    if !auth_user.user.is_admin {
        return Err(RegistryError::forbidden("administrator access is required"));
    }
    verify_password(&req.password, &auth_user.password_hash)?;
    verify_optional_totp(&auth_user, req.otp.as_deref())?;
    let (access_token, expires_in, refresh_token, refresh_expires_in) =
        issue_registry_session(&state, &auth_user.user).await?;
    publish_owner_event(
        &state,
        &auth_user.user.uid,
        RegistryRealtimePayload::AccountSessionUpdated(SessionNotice {
            uid: auth_user.user.uid.clone(),
            email: auth_user.user.email.clone(),
            active: true,
        }),
    );
    Ok(Json(LoginResponse {
        access_token,
        token_type: "Bearer",
        expires_in,
        refresh_token,
        refresh_expires_in,
        user: auth_user.user,
    }))
}

async fn create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<CreateUserResponse>), RegistryError> {
    let _admin_claims = match require_admin_claims(&headers, &state.auth) {
        Ok(claims) => Some(claims),
        Err(_) => {
            require_master_key(&headers, &state.auth)?;
            None
        }
    };
    let _write_guard = state.write_gate.lock().await;
    let email = normalize_email(&req.email)?;
    validate_new_password(&req.password)?;
    let password_hash = auth::hash_password(&req.password)?;
    let (user, fragment) = create_user_with_initial_storage_if_needed(
        &state,
        email,
        password_hash,
        req.quota_limit_bytes,
        req.is_admin.unwrap_or(false),
    )
    .await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::UserCreated(user.clone()),
    );
    if let Some(fragment) = fragment {
        publish_owner_event(
            &state,
            &user.uid,
            RegistryRealtimePayload::FragmentsInvalidated(FragmentNotice {
                uid: user.uid.clone(),
                fragment: Some(fragment),
            }),
        );
    }
    Ok((StatusCode::CREATED, Json(CreateUserResponse { user })))
}

async fn signup(
    State(state): State<AppState>,
    Json(req): Json<SignupRequest>,
) -> Result<(StatusCode, Json<SignupResponse>), RegistryError> {
    let _write_guard = state.write_gate.lock().await;
    let email = normalize_email(&req.email)?;
    validate_new_password(&req.password)?;
    let password_hash = auth::hash_password(&req.password)?;
    let (user, fragment) =
        create_user_with_initial_storage_if_needed(&state, email, password_hash, None, false)
            .await?;
    let (access_token, expires_in, refresh_token, refresh_expires_in) =
        issue_registry_session(&state, &user).await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::AccountCreated(user.clone()),
    );
    if let Some(fragment) = fragment {
        publish_owner_event(
            &state,
            &user.uid,
            RegistryRealtimePayload::FragmentsInvalidated(FragmentNotice {
                uid: user.uid.clone(),
                fragment: Some(fragment),
            }),
        );
    }
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::AccountSessionUpdated(SessionNotice {
            uid: user.uid.clone(),
            email: user.email.clone(),
            active: true,
        }),
    );
    Ok((
        StatusCode::CREATED,
        Json(SignupResponse {
            access_token,
            token_type: "Bearer",
            expires_in,
            refresh_token,
            refresh_expires_in,
            user,
        }),
    ))
}

async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, RegistryError> {
    let _write_guard = state.write_gate.lock().await;
    let email = normalize_email(&req.email)?;
    let auth_user = state
        .db
        .find_user_auth_by_email(email)
        .await?
        .ok_or_else(|| RegistryError::unauthorized("invalid credentials"))?;
    verify_password(&req.password, &auth_user.password_hash)?;
    verify_optional_totp(&auth_user, req.otp.as_deref())?;
    let _ = ensure_user_default_fragment_if_needed(&state, &auth_user.user).await?;
    let (access_token, expires_in, refresh_token, refresh_expires_in) =
        issue_registry_session(&state, &auth_user.user).await?;
    publish_owner_event(
        &state,
        &auth_user.user.uid,
        RegistryRealtimePayload::AccountSessionUpdated(SessionNotice {
            uid: auth_user.user.uid.clone(),
            email: auth_user.user.email.clone(),
            active: true,
        }),
    );
    Ok(Json(LoginResponse {
        access_token,
        token_type: "Bearer",
        expires_in,
        refresh_token,
        refresh_expires_in,
        user: auth_user.user,
    }))
}

async fn refresh(
    State(state): State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> Result<Json<RefreshResponse>, RegistryError> {
    let _write_guard = state.write_gate.lock().await;
    if req.refresh_token.trim().is_empty() {
        return Err(RegistryError::bad_request("refresh token is required"));
    }
    let (user, refresh_token, refresh_expires_in) = state
        .db
        .rotate_refresh_session(req.refresh_token, state.auth.refresh_token_ttl_secs)
        .await?;
    let (access_token, expires_in) = issue_access_token(&user, &state.auth)?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::AccountSessionUpdated(SessionNotice {
            uid: user.uid.clone(),
            email: user.email.clone(),
            active: true,
        }),
    );
    Ok(Json(RefreshResponse {
        access_token,
        token_type: "Bearer",
        expires_in,
        refresh_token,
        refresh_expires_in,
        user,
    }))
}

async fn logout(
    State(state): State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> Result<StatusCode, RegistryError> {
    let _write_guard = state.write_gate.lock().await;
    if req.refresh_token.trim().is_empty() {
        return Err(RegistryError::bad_request("refresh token is required"));
    }
    state.db.revoke_refresh_session(req.refresh_token).await?;
    publish_admin_event(
        &state,
        RegistryRealtimePayload::StateInvalidated(StateInvalidatedNotice {
            fragment: None,
            scope: "admin_sessions".to_string(),
        }),
    );
    Ok(StatusCode::NO_CONTENT)
}

async fn me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<UserRecord>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let removed_scopes = reconcile_pending_uploads(&state, Some(claims.sub.clone())).await?;
    if !removed_scopes.is_empty() {
        publish_pending_upload_cleanup_invalidations(&state, removed_scopes);
    }
    let user = state.db.get_user(claims.sub).await?;
    Ok(Json(user))
}

async fn list_admin_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<AdminUserListResponse>, RegistryError> {
    let _claims = require_admin_claims(&headers, &state.auth)?;
    let users = state.db.list_admin_users().await?;
    let mut summaries = Vec::with_capacity(users.len());
    for user in users {
        let fragments = state.db.list_fragments(user.uid.clone()).await?;
        summaries.push(AdminUserSummary {
            uid: user.uid,
            email: user.email,
            is_admin: user.is_admin,
            two_factor_enabled: user.two_factor_enabled,
            quota_limit_bytes: user.quota_limit_bytes,
            quota_used_bytes: user.quota_used_bytes,
            fragment_count: fragments.len(),
            fragments,
            created_at: user.created_at,
            updated_at: user.updated_at,
        });
    }
    Ok(Json(AdminUserListResponse { users: summaries }))
}

async fn get_admin_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<AdminStatsResponse>, RegistryError> {
    let _claims = require_admin_claims(&headers, &state.auth)?;
    let stats = state.db.admin_stats().await?;
    Ok(Json(AdminStatsResponse {
        generated_at: Utc::now().to_rfc3339(),
        totals: stats.totals,
        file_types: stats.file_types,
        largest_objects: stats.largest_objects,
        active_users: stats.active_users,
        top_storage_users: stats.top_storage_users,
    }))
}

async fn update_admin_user_quota(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(uid): AxumPath<String>,
    Json(req): Json<UpdateQuotaRequest>,
) -> Result<Json<UserMutationResponse>, RegistryError> {
    let _claims = require_admin_claims(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    if req.quota_limit_bytes < 0 {
        return Err(RegistryError::bad_request(
            "quota_limit_bytes must be non-negative",
        ));
    }
    let user = state
        .db
        .update_user_quota(uid, req.quota_limit_bytes)
        .await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::UserUpdated(user.clone()),
    );
    Ok(Json(UserMutationResponse { user }))
}

async fn update_admin_user_credentials(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(uid): AxumPath<String>,
    Json(req): Json<UpdateCredentialsRequest>,
) -> Result<Json<UserMutationResponse>, RegistryError> {
    let _claims = require_admin_claims(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let target_before = state.db.get_user(uid.clone()).await?;

    if let Some(email) = req.email.as_deref() {
        let email = normalize_email(email)?;
        if email != target_before.email {
            rename_managed_fragment_for_email_change(&state, &target_before, &email).await?;
        }
    }

    let next_email = req.email.as_deref().map(normalize_email).transpose()?;
    let next_password_hash = req
        .password
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            validate_new_password(value)?;
            auth::hash_password(value)
        })
        .transpose()?;
    let user = state
        .db
        .update_user_credentials(uid, next_email, next_password_hash, req.is_admin)
        .await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::UserUpdated(user.clone()),
    );
    publish_owner_invalidation(&state, &user.uid, None, "account_profile");
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::FragmentsInvalidated(FragmentNotice {
            uid: user.uid.clone(),
            fragment: None,
        }),
    );
    Ok(Json(UserMutationResponse { user }))
}

async fn admin_setup_user_2fa(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(uid): AxumPath<String>,
    Json(req): Json<SetupTwoFactorRequest>,
) -> Result<Json<TwoFactorSetupResponse>, RegistryError> {
    let claims = require_admin_claims(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let admin_auth = state
        .db
        .get_user_auth_by_uid(claims.sub)
        .await?
        .ok_or_else(|| RegistryError::unauthorized("administrator account not found"))?;
    verify_password(&req.password, &admin_auth.password_hash)?;

    let target = state.db.get_user(uid.clone()).await?;
    let secret = totp::generate_secret();
    state.db.force_set_totp_secret(uid, secret.clone()).await?;
    let otpauth_uri = totp::otpauth_uri(&target.email, &secret);
    let qr_data_url = totp::qr_data_url(&otpauth_uri)
        .map_err(|err| RegistryError::internal(format!("failed to build 2FA QR: {}", err)))?;
    publish_owner_event(
        &state,
        &target.uid,
        RegistryRealtimePayload::AccountTwoFactorSetup(TwoFactorNotice {
            uid: target.uid.clone(),
            email: target.email.clone(),
            pending: false,
        }),
    );
    Ok(Json(TwoFactorSetupResponse {
        secret,
        otpauth_uri,
        qr_data_url,
        pending: false,
    }))
}

async fn admin_disable_user_2fa(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(uid): AxumPath<String>,
    Json(req): Json<DisableTwoFactorRequest>,
) -> Result<Json<UserMutationResponse>, RegistryError> {
    let claims = require_admin_claims(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let admin_auth = state
        .db
        .get_user_auth_by_uid(claims.sub)
        .await?
        .ok_or_else(|| RegistryError::unauthorized("administrator account not found"))?;
    verify_password(&req.password, &admin_auth.password_hash)?;
    verify_optional_totp(&admin_auth, req.otp.as_deref())?;
    let user = state.db.clear_totp(uid).await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::UserUpdated(user.clone()),
    );
    Ok(Json(UserMutationResponse { user }))
}

async fn delete_admin_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(uid): AxumPath<String>,
) -> Result<Json<DeleteUserResponse>, RegistryError> {
    let claims = require_admin_claims(&headers, &state.auth)?;
    if claims.sub == uid {
        return Err(RegistryError::conflict(
            "administrator account cannot delete itself",
        ));
    }
    let _write_guard = state.write_gate.lock().await;
    let bindings = state.db.list_fragment_bindings(uid.clone()).await?;
    let deleted_fragments = bindings
        .iter()
        .map(|binding| binding.fragment.clone())
        .collect::<Vec<_>>();
    for binding in &bindings {
        state.broker.delete_fragment(binding).await?;
    }
    let deleted_uid = uid.clone();
    state.db.delete_user_cascade(uid).await?;
    publish_owner_event(
        &state,
        &deleted_uid,
        RegistryRealtimePayload::UserDeleted(DeleteUserNotice {
            uid: deleted_uid.clone(),
            deleted_fragments: deleted_fragments.clone(),
        }),
    );
    Ok(Json(DeleteUserResponse {
        removed: true,
        deleted_fragments,
    }))
}

async fn update_account_email(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<UpdateEmailRequest>,
) -> Result<Json<UserMutationResponse>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let auth_user = state
        .db
        .get_user_auth_by_uid(claims.sub.clone())
        .await?
        .ok_or_else(|| RegistryError::unauthorized("account not found"))?;
    verify_password(&req.current_password, &auth_user.password_hash)?;
    verify_optional_totp(&auth_user, req.otp.as_deref())?;
    let next_email = normalize_email(&req.new_email)?;
    if next_email != auth_user.user.email {
        rename_managed_fragment_for_email_change(&state, &auth_user.user, &next_email).await?;
    }
    let user = state
        .db
        .update_user_credentials(claims.sub, Some(next_email), None, None)
        .await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::AccountUpdated(user.clone()),
    );
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::FragmentsInvalidated(FragmentNotice {
            uid: user.uid.clone(),
            fragment: None,
        }),
    );
    Ok(Json(UserMutationResponse { user }))
}

async fn update_account_password(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<UpdatePasswordRequest>,
) -> Result<Json<UserMutationResponse>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let auth_user = state
        .db
        .get_user_auth_by_uid(claims.sub.clone())
        .await?
        .ok_or_else(|| RegistryError::unauthorized("account not found"))?;
    verify_password(&req.current_password, &auth_user.password_hash)?;
    verify_optional_totp(&auth_user, req.otp.as_deref())?;
    validate_new_password(&req.new_password)?;
    let password_hash = auth::hash_password(&req.new_password)?;
    let user = state
        .db
        .update_user_credentials(claims.sub, None, Some(password_hash), None)
        .await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::AccountUpdated(user.clone()),
    );
    Ok(Json(UserMutationResponse { user }))
}

async fn setup_account_2fa(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SetupTwoFactorRequest>,
) -> Result<Json<TwoFactorSetupResponse>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let auth_user = state
        .db
        .get_user_auth_by_uid(claims.sub.clone())
        .await?
        .ok_or_else(|| RegistryError::unauthorized("account not found"))?;
    verify_password(&req.password, &auth_user.password_hash)?;
    if auth_user.user.two_factor_enabled {
        return Err(RegistryError::conflict(
            "2FA is already enabled for this account",
        ));
    }
    let secret = totp::generate_secret();
    state
        .db
        .prepare_pending_totp(claims.sub, secret.clone())
        .await?;
    let otpauth_uri = totp::otpauth_uri(&auth_user.user.email, &secret);
    let qr_data_url = totp::qr_data_url(&otpauth_uri)
        .map_err(|err| RegistryError::internal(format!("failed to build 2FA QR: {}", err)))?;
    publish_owner_event(
        &state,
        &auth_user.user.uid,
        RegistryRealtimePayload::AccountTwoFactorSetup(TwoFactorNotice {
            uid: auth_user.user.uid.clone(),
            email: auth_user.user.email.clone(),
            pending: true,
        }),
    );
    Ok(Json(TwoFactorSetupResponse {
        secret,
        otpauth_uri,
        qr_data_url,
        pending: true,
    }))
}

async fn enable_account_2fa(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<EnableTwoFactorRequest>,
) -> Result<Json<UserMutationResponse>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let auth_user = state
        .db
        .get_user_auth_by_uid(claims.sub.clone())
        .await?
        .ok_or_else(|| RegistryError::unauthorized("account not found"))?;
    verify_password(&req.password, &auth_user.password_hash)?;
    let pending = auth_user
        .totp_pending_secret
        .as_deref()
        .ok_or_else(|| RegistryError::conflict("2FA setup has not been started"))?;
    if !totp::verify_totp(pending, &req.otp, 1) {
        return Err(RegistryError::unauthorized("invalid OTP"));
    }
    let user = state.db.activate_totp_secret(claims.sub).await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::AccountUpdated(user.clone()),
    );
    Ok(Json(UserMutationResponse { user }))
}

async fn disable_account_2fa(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<DisableTwoFactorRequest>,
) -> Result<Json<UserMutationResponse>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let auth_user = state
        .db
        .get_user_auth_by_uid(claims.sub.clone())
        .await?
        .ok_or_else(|| RegistryError::unauthorized("account not found"))?;
    verify_password(&req.password, &auth_user.password_hash)?;
    verify_optional_totp(&auth_user, req.otp.as_deref())?;
    let user = state.db.clear_totp(claims.sub).await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::AccountUpdated(user.clone()),
    );
    Ok(Json(UserMutationResponse { user }))
}

async fn list_fragments(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<FragmentListResponse>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let fragments = state.db.list_fragments(claims.sub.clone()).await?;
    let bindings = state.db.list_fragment_bindings(claims.sub).await?;
    Ok(Json(FragmentListResponse {
        fragments,
        bindings,
    }))
}

async fn link_fragment(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<LinkFragmentRequest>,
) -> Result<(StatusCode, Json<LinkFragmentResponse>), RegistryError> {
    validate_fragment(&req.fragment)?;
    let _write_guard = state.write_gate.lock().await;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let binding = state
        .db
        .upsert_fragment_binding(
            claims.sub,
            req.fragment.trim().to_string(),
            req.deployment,
            req.engine_url_override
                .map(|value| normalize_backend_url(value.trim()))
                .transpose()
                .map_err(RegistryError::bad_request)?
                .filter(|value| !value.is_empty()),
        )
        .await?;
    publish_owner_event(
        &state,
        &binding.owner_uid,
        RegistryRealtimePayload::FragmentLinked(binding.clone()),
    );
    publish_owner_event(
        &state,
        &binding.owner_uid,
        RegistryRealtimePayload::FragmentsInvalidated(FragmentNotice {
            uid: binding.owner_uid.clone(),
            fragment: Some(binding.fragment.clone()),
        }),
    );
    Ok((StatusCode::CREATED, Json(LinkFragmentResponse { binding })))
}

async fn issue_storage_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<StorageSessionRequest>,
) -> Result<Json<StorageSessionResponse>, RegistryError> {
    validate_fragment(&req.fragment)?;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let binding = state
        .db
        .get_fragment_binding(claims.sub, req.fragment.trim().to_string())
        .await?
        .ok_or_else(|| {
            RegistryError::conflict(
                "fragment is not linked to a storage backend yet; link it first",
            )
        })?;
    let session = state
        .broker
        .issue_fragment_session(
            &binding,
            req.read || (!req.write && !req.modify && !req.delete),
            req.write,
            req.modify,
            req.delete,
        )
        .await?;
    Ok(Json(session))
}

async fn create_share(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateShareRequest>,
) -> Result<(StatusCode, Json<CreateShareResponse>), RegistryError> {
    validate_fragment_and_key(&req.fragment, &req.object_key)?;
    if req.expires_in == 0 || req.expires_in > MAX_SHARE_EXPIRATION_SECS {
        return Err(RegistryError::bad_request(format!(
            "expires_in must be between 1 and {} seconds",
            MAX_SHARE_EXPIRATION_SECS
        )));
    }

    let _write_guard = state.write_gate.lock().await;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let (share, token) = state
        .db
        .create_share(
            claims.sub.clone(),
            req.fragment,
            req.object_key,
            req.disposition,
            req.expires_in,
        )
        .await?;
    publish_owner_invalidation(&state, &claims.sub, Some(share.fragment.clone()), "shares");
    Ok((
        StatusCode::CREATED,
        Json(CreateShareResponse {
            url: build_public_share_url(&headers, &token),
            expires_at: share.expires_at.clone(),
            share,
        }),
    ))
}

async fn list_shares(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListSharesQuery>,
) -> Result<Json<ListSharesResponse>, RegistryError> {
    if let Some(fragment) = query.fragment.as_deref() {
        validate_fragment(fragment)?;
    }
    let claims = claims_from_headers(&headers, &state.auth)?;
    let shares = state
        .db
        .list_shares(
            claims.sub,
            query.fragment,
            query.include_expired.unwrap_or(false),
            query.include_revoked.unwrap_or(false),
        )
        .await?;
    Ok(Json(ListSharesResponse { shares }))
}

async fn revoke_share(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(share_id): AxumPath<String>,
) -> Result<Json<RevokeShareResponse>, RegistryError> {
    if share_id.trim().is_empty() {
        return Err(RegistryError::bad_request("share_id is required"));
    }
    let _write_guard = state.write_gate.lock().await;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let revoked = state.db.revoke_share(claims.sub, share_id).await?;
    Ok(Json(RevokeShareResponse { revoked }))
}

async fn get_public_share(
    State(state): State<AppState>,
    AxumPath(token): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, RegistryError> {
    if token.trim().is_empty() {
        return Err(RegistryError::not_found("share not found"));
    }
    let share = state
        .db
        .resolve_public_share(token)
        .await?
        .ok_or_else(|| RegistryError::not_found("share not found"))?;
    let live_object = state
        .db
        .resolve_live_object(
            share.owner_uid.clone(),
            share.fragment.clone(),
            share.object_key.clone(),
        )
        .await?;
    if live_object.is_none() {
        return Err(RegistryError::not_found("shared object not found"));
    }
    let binding = state
        .db
        .get_fragment_binding(share.owner_uid.clone(), share.fragment.clone())
        .await?
        .ok_or_else(|| RegistryError::not_found("share not found"))?;
    let upstream = state
        .broker
        .stream_shared_object(&binding, &share.object_key, share.disposition, &headers)
        .await?;
    let status = StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::OK);
    let mut builder = Response::builder().status(status);
    for header_name in [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::ETAG,
        header::ACCEPT_RANGES,
        header::CONTENT_RANGE,
        header::CACHE_CONTROL,
        header::LAST_MODIFIED,
    ] {
        builder = copy_upstream_header(builder, &upstream, header_name);
    }
    if share.disposition.is_download() {
        let logical_name = share
            .object_key
            .rsplit('/')
            .next()
            .filter(|value| !value.is_empty())
            .unwrap_or("download");
        if let Ok(value) =
            header::HeaderValue::from_str(&format!("attachment; filename=\"{}\"", logical_name))
        {
            builder = builder.header(header::CONTENT_DISPOSITION, value);
        }
    } else {
        builder = copy_upstream_header(builder, &upstream, header::CONTENT_DISPOSITION);
    }
    builder
        .body(Body::from_stream(upstream.bytes_stream()))
        .map_err(|err| RegistryError::internal(format!("failed to build share response: {}", err)))
}

async fn register_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RegisterObjectRequest>,
) -> Result<(StatusCode, Json<ObjectMutationResponse>), RegistryError> {
    validate_fragment_and_key(&req.fragment, &req.object_key)?;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let removed_scopes = reconcile_pending_uploads(&state, Some(claims.sub.clone())).await?;
    if !removed_scopes.is_empty() {
        publish_pending_upload_cleanup_invalidations(&state, removed_scopes);
    }
    let _write_guard = state.write_gate.lock().await;
    let object = state
        .db
        .register_pending_object(
            claims.sub.clone(),
            req.fragment,
            req.object_key,
            req.expected_size_bytes,
            req.content_type,
            req.version_id,
            req.session_id,
        )
        .await?;
    let user = state.db.get_user(claims.sub).await?;
    publish_owner_event(
        &state,
        &object.owner_uid,
        RegistryRealtimePayload::FileRegistered(object.clone()),
    );
    publish_owner_invalidation(
        &state,
        &object.owner_uid,
        Some(object.fragment.clone()),
        "objects",
    );
    Ok((
        StatusCode::CREATED,
        Json(ObjectMutationResponse {
            object,
            quota_limit_bytes: user.quota_limit_bytes,
            quota_used_bytes: user.quota_used_bytes,
        }),
    ))
}

async fn finalize_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<FinalizeObjectRequest>,
) -> Result<Json<ObjectMutationResponse>, RegistryError> {
    validate_fragment_and_key(&req.fragment, &req.object_key)?;
    if req.version_id.trim().is_empty() {
        return Err(RegistryError::bad_request("version_id is required"));
    }
    if req.object_size_bytes < 0 {
        return Err(RegistryError::bad_request(
            "object_size_bytes must be non-negative",
        ));
    }
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let fragment = req.fragment.clone();
    let object_key = req.object_key.clone();
    let owner_uid = claims.sub.clone();

    let binding = state
        .db
        .get_fragment_binding(owner_uid.clone(), fragment.clone())
        .await?;
    let pending_or_existing = state
        .db
        .resolve_any_object_record(owner_uid.clone(), fragment.clone(), object_key.clone())
        .await?;

    let (object, user) = if let (Some(binding), Some(stored)) = (binding, pending_or_existing) {
        let mut engine_state = None;
        let mut backoff_ms = 75;
        for attempt in 0..5 {
            let candidate = state
                .broker
                .read_current_object_state(&binding, &object_key, true)
                .await?;
            if candidate.current.is_some() {
                engine_state = Some(candidate);
                break;
            }
            if attempt == 4 {
                engine_state = Some(candidate);
                break;
            }
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms *= 2;
        }
        let engine_state = engine_state.expect("engine state should be set by finalize retry loop");
        let current = engine_state
            .current
            .or_else(|| infer_latest_engine_head(&engine_state.versions))
            .ok_or_else(|| {
                RegistryError::bad_gateway(
                    "storage finalize did not produce a readable committed object",
                )
            })?;
        let (object, user) = state
            .db
            .sync_object_from_engine(
                owner_uid,
                fragment.clone(),
                object_key.clone(),
                stored.storage_object_id,
                Some(current),
                engine_state.versions,
            )
            .await?;
        let object = object.ok_or_else(|| {
            RegistryError::bad_gateway("storage finalize did not produce a committed registry head")
        })?;
        (object, user)
    } else {
        state
            .db
            .finalize_object(
                claims.sub,
                req.fragment,
                req.object_key,
                req.version_id,
                req.object_size_bytes,
                req.content_type,
            )
            .await?
    };
    publish_owner_event(
        &state,
        &object.owner_uid,
        RegistryRealtimePayload::FileUploaded(object.clone()),
    );
    publish_owner_invalidation(
        &state,
        &object.owner_uid,
        Some(object.fragment.clone()),
        "objects",
    );
    Ok(Json(ObjectMutationResponse {
        object,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn list_objects(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Json<ListObjectsResponse>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let limit = query.limit.unwrap_or(200).clamp(1, 5000);
    let offset = query.offset.unwrap_or(0);
    let include_pending = query.include_pending.unwrap_or(false);
    let include_trashed = query.include_trashed.unwrap_or(false);
    let owner_uid = claims.sub.clone();
    let page = if let Some(fragment) = query.fragment.clone() {
        let binding = state
            .db
            .get_fragment_binding(owner_uid.clone(), fragment.clone())
            .await?;
        if let (Some(binding), false) = (binding, include_pending) {
            list_objects_via_engine(
                &state,
                &owner_uid,
                &binding,
                query.prefix.as_deref(),
                query.delimiter.as_deref(),
                query.search.as_deref(),
                limit,
                offset,
                include_trashed,
            )
            .await?
        } else {
            state
                .db
                .list_objects(
                    owner_uid,
                    Some(fragment),
                    query.prefix,
                    query.delimiter,
                    query.search,
                    limit,
                    offset,
                    include_pending,
                    include_trashed,
                )
                .await?
        }
    } else {
        state
            .db
            .list_objects(
                owner_uid,
                None,
                query.prefix,
                query.delimiter,
                query.search,
                limit,
                offset,
                include_pending,
                include_trashed,
            )
            .await?
    };
    Ok(Json(ListObjectsResponse {
        objects: page.objects,
        common_prefixes: page.common_prefixes,
        limit,
        offset,
        continuation_token: page
            .has_more
            .then(|| offset.saturating_add(limit).to_string()),
    }))
}

async fn suggest_objects(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SuggestObjectsQuery>,
) -> Result<Json<SuggestObjectsResponse>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let suggestions = state
        .db
        .suggest_object_keys(
            claims.sub,
            query.fragment,
            query.prefix,
            query.q,
            query.limit.unwrap_or(8).clamp(1, 100),
            query.include_trashed.unwrap_or(false),
        )
        .await?;
    Ok(Json(SuggestObjectsResponse { suggestions }))
}

async fn object_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ObjectStatsQuery>,
) -> Result<Json<ObjectStatsResponse>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let include_pending = query.include_pending.unwrap_or(false);
    let include_trashed = query.include_trashed.unwrap_or(false);
    let stats = if let Some(fragment) = query.fragment.clone() {
        let binding = state
            .db
            .get_fragment_binding(claims.sub.clone(), fragment.clone())
            .await?;
        if let (Some(binding), false) = (binding, include_pending) {
            object_stats_via_engine(
                &state,
                &binding,
                query.prefix.as_deref(),
                query.search.as_deref(),
                include_trashed,
            )
            .await?
        } else {
            state
                .db
                .object_stats(
                    claims.sub,
                    Some(fragment),
                    query.prefix,
                    query.search,
                    include_pending,
                    include_trashed,
                )
                .await?
        }
    } else {
        state
            .db
            .object_stats(
                claims.sub,
                None,
                query.prefix,
                query.search,
                include_pending,
                include_trashed,
            )
            .await?
    };
    Ok(Json(ObjectStatsResponse {
        total_objects: stats.total_objects,
        total_size_bytes: stats.total_size_bytes,
        extension_stats: stats.extension_stats,
    }))
}

async fn archive_objects(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ArchiveObjectsRequest>,
) -> Result<Response, RegistryError> {
    validate_fragment(&req.fragment)?;
    let requested_format = req.format.as_deref().unwrap_or("tar");
    if requested_format != "tar" {
        return Err(RegistryError::bad_request(
            "registry archive currently supports tar only",
        ));
    }

    let has_prefix = req
        .prefix
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    let has_search = req
        .search
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    if !req.keys.is_empty() && (has_prefix || has_search) {
        return Err(RegistryError::bad_request(
            "archive request must use either explicit keys or prefix/search selection, not both",
        ));
    }

    let claims = claims_from_headers(&headers, &state.auth)?;
    let binding = state
        .db
        .get_fragment_binding(claims.sub.clone(), req.fragment.trim().to_string())
        .await?
        .ok_or_else(|| {
            RegistryError::conflict(
                "fragment is not linked to a storage backend yet; link it first",
            )
        })?;

    let selection = state
        .db
        .select_archive_objects(
            claims.sub,
            req.fragment.trim().to_string(),
            req.keys,
            req.prefix,
            req.search,
            req.include_trashed.unwrap_or(false),
        )
        .await?;
    let fallback_filename = req.filename.clone().unwrap_or_else(|| "export".to_string());
    let archive_file = state
        .broker
        .stream_logical_archive(&binding, &selection.objects)
        .await?;
    let content_disposition = header::HeaderValue::from_str(&format!(
        "attachment; filename=\"{}.tar\"",
        fallback_filename
    ))
    .unwrap_or_else(|_| header::HeaderValue::from_static("attachment"));

    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/x-tar"),
        )
        .header(header::CONTENT_DISPOSITION, content_disposition)
        .body(Body::from_stream(tokio_util::io::ReaderStream::new(
            archive_file,
        )))
        .map_err(|err| {
            RegistryError::internal(format!("failed to build archive response: {}", err))
        })
}

async fn rename_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RenameRegistryObjectRequest>,
) -> Result<Json<ObjectMutationResponse>, RegistryError> {
    if req.fragment.trim().is_empty() {
        return Err(RegistryError::bad_request("fragment is required"));
    }
    if req.source_key.trim().is_empty() || req.destination_key.trim().is_empty() {
        return Err(RegistryError::bad_request(
            "source_key and destination_key are required",
        ));
    }
    let fragment = req.fragment.clone();
    let source_key = req.source_key.trim().to_string();
    let destination_key = req.destination_key.trim().to_string();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let removed_scopes = reconcile_pending_uploads(&state, Some(claims.sub.clone())).await?;
    if !removed_scopes.is_empty() {
        publish_pending_upload_cleanup_invalidations(&state, removed_scopes);
    }
    let _write_guard = state.write_gate.lock().await;
    let (object, user) = state
        .db
        .rename_object(
            claims.sub,
            fragment.clone(),
            source_key.clone(),
            destination_key.clone(),
        )
        .await?;
    publish_owner_event(
        &state,
        &object.owner_uid,
        RegistryRealtimePayload::FileRenamed(RenameObjectNotice {
            source_fragment: fragment.clone(),
            source_key,
            destination_fragment: fragment.clone(),
            destination_key,
        }),
    );
    publish_owner_invalidation(&state, &object.owner_uid, Some(fragment), "objects");
    Ok(Json(ObjectMutationResponse {
        object,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn rename_object_prefix(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RenameRegistryPrefixRequest>,
) -> Result<Json<BulkObjectMutationResponse>, RegistryError> {
    validate_fragment(&req.fragment)?;
    if req.source_prefix.trim().is_empty() || req.destination_prefix.trim().is_empty() {
        return Err(RegistryError::bad_request(
            "source_prefix and destination_prefix are required",
        ));
    }

    let fragment = req.fragment.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let removed_scopes = reconcile_pending_uploads(&state, Some(claims.sub.clone())).await?;
    if !removed_scopes.is_empty() {
        publish_pending_upload_cleanup_invalidations(&state, removed_scopes);
    }
    let _write_guard = state.write_gate.lock().await;
    let (affected, user) = state
        .db
        .rename_prefix(
            claims.sub,
            fragment.clone(),
            req.source_prefix,
            req.destination_prefix,
        )
        .await?;
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "objects");
    Ok(Json(BulkObjectMutationResponse {
        affected,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn trash_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ObjectStateRequest>,
) -> Result<Json<ObjectMutationResponse>, RegistryError> {
    validate_fragment_and_key(&req.fragment, &req.object_key)?;
    let fragment = req.fragment.clone();
    let object_key = req.object_key.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let (object, user) = state
        .db
        .trash_object_if_ready(claims.sub, fragment.clone(), object_key.clone())
        .await?;
    publish_owner_event(
        &state,
        &object.owner_uid,
        RegistryRealtimePayload::FileDeleted(DeleteObjectNotice {
            fragment: fragment.clone(),
            object_key,
            version_id: None,
        }),
    );
    publish_owner_invalidation(&state, &object.owner_uid, Some(fragment), "objects");
    Ok(Json(ObjectMutationResponse {
        object,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn trash_objects_prefix(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PrefixObjectMutationRequest>,
) -> Result<Json<BulkObjectMutationResponse>, RegistryError> {
    validate_fragment(&req.fragment)?;
    if req.prefix.trim().is_empty() {
        return Err(RegistryError::bad_request("prefix is required"));
    }

    let fragment = req.fragment.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let (affected, user) = state
        .db
        .trash_objects_by_prefix_from_ready(claims.sub, fragment.clone(), req.prefix)
        .await?;
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "objects");
    Ok(Json(BulkObjectMutationResponse {
        affected,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn trash_objects_bulk(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<BulkObjectKeysRequest>,
) -> Result<Json<BulkObjectSelectionResponse>, RegistryError> {
    validate_fragment(&req.fragment)?;
    let requested = req.object_keys.len();
    let object_keys = canonical_bulk_object_keys(req.object_keys);
    if object_keys.is_empty() {
        return Err(RegistryError::bad_request(
            "object_keys must include at least one object key",
        ));
    }

    let fragment = req.fragment.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let (affected, not_found, user) = state
        .db
        .trash_objects_bulk_from_ready(claims.sub, fragment.clone(), object_keys.clone())
        .await?;
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "objects");
    Ok(Json(BulkObjectSelectionResponse {
        requested,
        unique: object_keys.len(),
        affected,
        not_found,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn recover_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ObjectStateRequest>,
) -> Result<Json<ObjectMutationResponse>, RegistryError> {
    validate_fragment_and_key(&req.fragment, &req.object_key)?;
    let fragment = req.fragment.clone();
    let object_key = req.object_key.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let stored = state
        .db
        .resolve_stored_object_record(claims.sub.clone(), fragment.clone(), object_key.clone())
        .await?
        .ok_or_else(|| RegistryError::not_found("object not found"))?;

    let (object, user) = if stored.status == "purged" {
        let binding = state
            .db
            .get_fragment_binding(claims.sub.clone(), fragment.clone())
            .await?
            .ok_or_else(|| {
                RegistryError::conflict(
                    "fragment is not linked to a storage backend yet; link it first",
                )
            })?;

        let engine_state = match state
            .broker
            .read_current_object_state(&binding, &object_key, true)
            .await
        {
            Ok(engine_state) if engine_state.current.is_some() => engine_state,
            Ok(_) => {
                state
                    .broker
                    .recover_object_and_read_state(&binding, &object_key)
                    .await?
            }
            Err(err) => return Err(err),
        };

        state
            .db
            .cancel_object_purge(claims.sub.clone(), fragment.clone(), object_key.clone())
            .await?;
        let (object, user) = state
            .db
            .sync_object_from_engine(
                claims.sub,
                fragment.clone(),
                object_key.clone(),
                stored.storage_object_id.clone(),
                engine_state.current,
                engine_state.versions,
            )
            .await?;
        let object = object.ok_or_else(|| {
            RegistryError::not_found("object can no longer be recovered from storage")
        })?;
        (object, user)
    } else {
        state
            .db
            .set_object_status(claims.sub, fragment.clone(), object_key.clone(), "ready")
            .await?
    };
    publish_owner_event(
        &state,
        &object.owner_uid,
        RegistryRealtimePayload::FileUploaded(object.clone()),
    );
    publish_owner_invalidation(&state, &object.owner_uid, Some(fragment), "objects");
    Ok(Json(ObjectMutationResponse {
        object,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn recover_objects_prefix(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PrefixObjectMutationRequest>,
) -> Result<Json<BulkObjectMutationResponse>, RegistryError> {
    validate_fragment(&req.fragment)?;
    if req.prefix.trim().is_empty() {
        return Err(RegistryError::bad_request("prefix is required"));
    }

    let fragment = req.fragment.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let (affected, user) = state
        .db
        .recover_objects_by_prefix_from_trash(claims.sub, fragment.clone(), req.prefix)
        .await?;
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "objects");
    Ok(Json(BulkObjectMutationResponse {
        affected,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn recover_objects_bulk(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<BulkObjectKeysRequest>,
) -> Result<Json<BulkObjectSelectionResponse>, RegistryError> {
    validate_fragment(&req.fragment)?;
    let requested = req.object_keys.len();
    let object_keys = canonical_bulk_object_keys(req.object_keys);
    if object_keys.is_empty() {
        return Err(RegistryError::bad_request(
            "object_keys must include at least one object key",
        ));
    }

    let fragment = req.fragment.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let (affected, not_found, user) = state
        .db
        .recover_objects_bulk_from_trash(claims.sub, fragment.clone(), object_keys.clone())
        .await?;
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "objects");
    Ok(Json(BulkObjectSelectionResponse {
        requested,
        unique: object_keys.len(),
        affected,
        not_found,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn delete_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DeleteObjectQuery>,
) -> Result<Json<ObjectDeleteResponse>, RegistryError> {
    validate_fragment_and_key(&query.fragment, &query.object_key)?;
    let fragment = query.fragment.clone();
    let object_key = query.object_key.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let (removed, user) = state
        .db
        .purge_object(claims.sub, fragment.clone(), object_key.clone())
        .await?;
    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::FileDeleted(DeleteObjectNotice {
            fragment: fragment.clone(),
            object_key,
            version_id: None,
        }),
    );
    publish_owner_invalidation(&state, &user.uid, Some(fragment.clone()), "objects");
    publish_owner_invalidation(&state, &user.uid, Some(fragment.clone()), "trash");
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "account_profile");
    Ok(Json(ObjectDeleteResponse {
        removed,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn delete_objects_prefix(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PrefixObjectMutationRequest>,
) -> Result<Json<BulkObjectMutationResponse>, RegistryError> {
    validate_fragment(&req.fragment)?;
    if req.prefix.trim().is_empty() {
        return Err(RegistryError::bad_request("prefix is required"));
    }

    let fragment = req.fragment.clone();
    let prefix = req.prefix.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let (affected, user) = state
        .db
        .purge_objects_by_prefix(claims.sub, fragment.clone(), prefix)
        .await?;
    publish_owner_invalidation(&state, &user.uid, Some(fragment.clone()), "objects");
    publish_owner_invalidation(&state, &user.uid, Some(fragment.clone()), "trash");
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "account_profile");
    Ok(Json(BulkObjectMutationResponse {
        affected,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn delete_object_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<DeleteObjectVersionRequest>,
) -> Result<Json<DeleteObjectVersionResponse>, RegistryError> {
    validate_fragment_and_key(&req.fragment, &req.object_key)?;
    if req.version_id.trim().is_empty() {
        return Err(RegistryError::bad_request("version_id is required"));
    }

    let _write_guard = state.write_gate.lock().await;
    let fragment = req.fragment.clone();
    let object_key = req.object_key.clone();
    let version_id = req.version_id.trim().to_string();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let current_object = state
        .db
        .resolve_object(claims.sub.clone(), fragment.clone(), object_key.clone())
        .await?
        .ok_or_else(|| RegistryError::not_found("object not found"))?;
    let binding = state
        .db
        .get_fragment_binding(claims.sub.clone(), fragment.clone())
        .await?
        .ok_or_else(|| {
            RegistryError::conflict(
                "fragment is not linked to a storage backend yet; link it first",
            )
        })?;

    let engine_state = state
        .broker
        .delete_object_version_and_read_state(&binding, &object_key, &version_id)
        .await?;
    let (object, user) = state
        .db
        .sync_object_from_engine(
            claims.sub,
            fragment.clone(),
            object_key.clone(),
            current_object.storage_object_id,
            engine_state.current,
            engine_state.versions,
        )
        .await?;

    publish_owner_event(
        &state,
        &user.uid,
        RegistryRealtimePayload::FileDeleted(DeleteObjectNotice {
            fragment: fragment.clone(),
            object_key,
            version_id: Some(version_id),
        }),
    );
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "objects");

    Ok(Json(DeleteObjectVersionResponse {
        removed: true,
        object_removed: object.is_none(),
        object,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn cancel_pending_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DeleteObjectQuery>,
) -> Result<Json<ObjectDeleteResponse>, RegistryError> {
    validate_fragment_and_key(&query.fragment, &query.object_key)?;
    let fragment = query.fragment.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let removed_scopes = reconcile_pending_uploads(&state, Some(claims.sub.clone())).await?;
    if !removed_scopes.is_empty() {
        publish_pending_upload_cleanup_invalidations(&state, removed_scopes);
    }
    let _write_guard = state.write_gate.lock().await;
    let (removed, user) = state
        .db
        .cancel_pending_object(
            claims.sub,
            fragment.clone(),
            query.object_key,
            query.version_id,
            query.session_id,
        )
        .await?;
    if removed {
        publish_owner_invalidation(&state, &user.uid, Some(fragment), "objects");
    }
    Ok(Json(ObjectDeleteResponse {
        removed,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn recover_all_objects(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<FragmentStateRequest>,
) -> Result<Json<BulkObjectMutationResponse>, RegistryError> {
    validate_fragment(&req.fragment)?;
    let fragment = req.fragment.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let (affected, user) = state
        .db
        .recover_all_trashed(claims.sub, fragment.clone())
        .await?;
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "objects");
    Ok(Json(BulkObjectMutationResponse {
        affected,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn empty_trash(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<FragmentStateRequest>,
) -> Result<Json<BulkObjectMutationResponse>, RegistryError> {
    validate_fragment(&req.fragment)?;
    let fragment = req.fragment.clone();
    let claims = claims_from_headers(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    let (affected, user) = state
        .db
        .purge_trashed_objects(claims.sub, fragment.clone())
        .await?;
    publish_owner_invalidation(&state, &user.uid, Some(fragment.clone()), "trash");
    publish_owner_invalidation(&state, &user.uid, Some(fragment.clone()), "objects");
    publish_owner_invalidation(&state, &user.uid, Some(fragment), "account_profile");
    Ok(Json(BulkObjectMutationResponse {
        affected,
        quota_limit_bytes: user.quota_limit_bytes,
        quota_used_bytes: user.quota_used_bytes,
    }))
}

async fn resolve_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ResolveObjectQuery>,
) -> Result<Json<models::ObjectRecord>, RegistryError> {
    validate_fragment_and_key(&query.fragment, &query.object_key)?;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let object = state
        .db
        .resolve_object(
            claims.sub.clone(),
            query.fragment.clone(),
            query.object_key.clone(),
        )
        .await?
        .ok_or_else(|| RegistryError::not_found("object not found"))?;

    let binding = match state
        .db
        .get_fragment_binding(claims.sub.clone(), query.fragment.clone())
        .await?
    {
        Some(binding) => binding,
        None => return Ok(Json(object)),
    };

    match state
        .broker
        .read_current_object_state(&binding, &query.object_key, false)
        .await
    {
        Ok(engine_state) => {
            if engine_state.current.is_none() {
                let _write_guard = state.write_gate.lock().await;
                let full_state = state
                    .broker
                    .read_current_object_state(&binding, &query.object_key, true)
                    .await?;
                let inferred_current = full_state
                    .current
                    .clone()
                    .or_else(|| infer_latest_engine_head(&full_state.versions));
                if inferred_current.is_none() {
                    let age_secs = object_record_age_secs(&object).unwrap_or(i64::MAX);
                    warn!(
                        fragment = %query.fragment,
                        object_key = %query.object_key,
                        storage_object_id = %object.storage_object_id,
                        age_secs,
                        "registry resolve saw no readable engine head; preserving existing registry row"
                    );
                    return Ok(Json(object));
                }
                let (synced_object, user) = state
                    .db
                    .sync_object_from_engine(
                        claims.sub,
                        query.fragment.clone(),
                        query.object_key.clone(),
                        object.storage_object_id.clone(),
                        inferred_current,
                        full_state.versions,
                    )
                    .await?;
                publish_owner_invalidation(
                    &state,
                    &user.uid,
                    Some(query.fragment.clone()),
                    "objects",
                );
                return synced_object
                    .map(Json)
                    .ok_or_else(|| RegistryError::not_found("object not found"));
            }

            if let Some(current) = engine_state.current.as_ref() {
                let same_head = current.version_id == object.version_id
                    && current.object_size_bytes == object.object_size_bytes
                    && current.content_type == object.content_type;
                if !same_head {
                    let _write_guard = state.write_gate.lock().await;
                    let full_state = state
                        .broker
                        .read_current_object_state(&binding, &query.object_key, true)
                        .await?;
                    let (synced_object, user) = state
                        .db
                        .sync_object_from_engine(
                            claims.sub,
                            query.fragment.clone(),
                            query.object_key.clone(),
                            object.storage_object_id.clone(),
                            full_state.current,
                            full_state.versions,
                        )
                        .await?;
                    publish_owner_invalidation(
                        &state,
                        &user.uid,
                        Some(query.fragment.clone()),
                        "objects",
                    );
                    return synced_object
                        .map(Json)
                        .ok_or_else(|| RegistryError::not_found("object not found"));
                }
            }
        }
        Err(err) => {
            warn!(
                fragment = %query.fragment,
                object_key = %query.object_key,
                "registry resolve self-check skipped: {}",
                err
            );
        }
    }

    Ok(Json(object))
}

fn validate_fragment_and_key(fragment: &str, object_key: &str) -> Result<(), RegistryError> {
    validate_fragment(fragment)?;
    if object_key.trim().is_empty() {
        return Err(RegistryError::bad_request("object_key is required"));
    }
    Ok(())
}

fn validate_fragment(fragment: &str) -> Result<(), RegistryError> {
    if fragment.trim().is_empty() {
        return Err(RegistryError::bad_request("fragment is required"));
    }
    Ok(())
}

fn warn_if_running_as_root(binary: &str) {
    #[cfg(unix)]
    unsafe {
        if libc::geteuid() == 0 {
            eprintln!(
                "warning: {binary} is running as root. This is allowed, but a non-root service user is preferred to avoid root-owned runtime state."
            );
        }
    }
}

fn probe_directory_writable(dir: &Path, label: &str) -> Result<(), String> {
    let probe_name = format!(
        ".registry-write-check-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let probe_path = dir.join(probe_name);
    fs::write(&probe_path, b"ok")
        .map_err(|err| format!("{label} '{}' is not writable: {}", dir.display(), err))?;
    let _ = fs::remove_file(&probe_path);
    Ok(())
}

fn ensure_readable_file(path: &Path, label: &str) -> Result<(), String> {
    let meta = fs::metadata(path)
        .map_err(|err| format!("{label} '{}' is not accessible: {}", path.display(), err))?;
    if !meta.is_file() {
        return Err(format!("{label} '{}' is not a file", path.display()));
    }
    OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|err| format!("{label} '{}' is not readable: {}", path.display(), err))?;
    Ok(())
}

fn ensure_file_writable(path: &Path, label: &str) -> Result<(), String> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|err| format!("{label} '{}' is not writable: {}", path.display(), err))?;
    Ok(())
}

fn ensure_writable_file_target(path: &Path, label: &str) -> Result<(), String> {
    if path.exists() {
        return ensure_file_writable(path, label);
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|err| {
        format!(
            "{label} parent '{}' could not be created: {}",
            parent.display(),
            err
        )
    })?;
    probe_directory_writable(parent, &format!("{label} parent"))
}

fn validate_runtime_paths(config: &RegistryConfig) -> Result<(), String> {
    ensure_readable_file(&config.cert_path, "Registry TLS certificate")?;
    ensure_readable_file(&config.key_path, "Registry TLS key")?;
    ensure_writable_file_target(&config.sqlite_path, "Registry database")?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::StorageDeployment;
    use axum::body::Body;
    use axum::extract::Path as StubPath;
    use axum::http::header::{
        ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
        ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, CONTENT_DISPOSITION,
        CONTENT_TYPE, ORIGIN,
    };
    use axum::http::{Request, StatusCode};
    use axum::routing::{delete, get, post};
    use futures_util::StreamExt;
    use reqwest::Client;
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use tokio::sync::Mutex as TokioMutex;
    use tokio::time::{Duration, timeout};
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
    use tower::ServiceExt;

    type TestWsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

    #[derive(Debug, Clone)]
    struct StubVersion {
        version_id: String,
        object_size_bytes: i64,
        content_type: String,
        created_at: i64,
    }

    #[derive(Debug, Clone)]
    struct StubObjectState {
        current_visible: bool,
        versions: Vec<StubVersion>,
    }

    async fn spawn_storage_stub(token: &'static str) -> String {
        spawn_storage_stub_with_auth_status(token, StatusCode::OK).await
    }

    async fn spawn_storage_stub_with_auth_status(
        token: &'static str,
        auth_status: StatusCode,
    ) -> String {
        let app = Router::new()
            .route(
                "/n1/auth/fragment",
                post(move |Json(_payload): Json<Value>| async move {
                    if auth_status.is_success() {
                        (
                            auth_status,
                            Json(json!({
                                "access_token": token,
                                "token_type": "Bearer",
                                "expires_in": 600
                            })),
                        )
                    } else {
                        (
                            auth_status,
                            Json(json!({
                                "message": "fragment not ready"
                            })),
                        )
                    }
                }),
            )
            .route(
                "/n1/fragments",
                post(|Json(payload): Json<Value>| async move {
                    let name = payload["name"].as_str().unwrap_or_default();
                    let mut hasher = sha2::Sha256::new();
                    use sha2::Digest;
                    hasher.update(name.as_bytes());
                    Json(json!({
                        "fragment_hash": hex::encode(hasher.finalize()),
                        "name": name
                    }))
                }),
            )
            .route(
                "/n1/objects/archive/manifest",
                post(
                    |Query(query): Query<std::collections::HashMap<String, String>>,
                     body: String| async move {
                        Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/x-tar")
                            .header(
                                CONTENT_DISPOSITION,
                                query
                                    .get("filename")
                                    .map(|value| format!("attachment; filename=\"{}.tar\"", value))
                                    .unwrap_or_else(|| {
                                        "attachment; filename=\"export.tar\"".to_string()
                                    }),
                            )
                            .body(Body::from(body))
                            .expect("stub archive response")
                    },
                ),
            )
            .route(
                "/n1/objects/get/{*object_key}",
                get(move |headers: HeaderMap, StubPath(object_key): StubPath<String>| async move {
                    if headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        != Some(&format!("Bearer {}", token))
                    {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "application/octet-stream")
                        .body(Body::from(format!("view:{object_key}")))
                        .expect("stub get response")
                        .into_response()
                }),
            )
            .route(
                "/n1/objects/download",
                get(
                    move |headers: HeaderMap, Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        if headers
                            .get(header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            != Some(&format!("Bearer {}", token))
                        {
                            return StatusCode::UNAUTHORIZED.into_response();
                        }
                        let object_key = query
                            .get("object_key")
                            .cloned()
                            .unwrap_or_else(|| "missing".to_string());
                        Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/octet-stream")
                            .header(
                                CONTENT_DISPOSITION,
                                format!("attachment; filename=\"{}\"", object_key.rsplit('/').next().unwrap_or("download")),
                            )
                            .body(Body::from(format!("download:{object_key}")))
                            .expect("stub download response")
                            .into_response()
                    },
                ),
            )
            .route(
                "/n1/objects/{*object_key}",
                delete(|StubPath(_object_key): StubPath<String>| async move {
                    StatusCode::OK
                }),
            )
            .route(
                "/n1/objects/bulk-delete",
                post(|Json(_payload): Json<Value>| async move { StatusCode::OK }),
            )
            .route(
                "/n1/objects/permanent/{*object_key}",
                delete(|StubPath(_object_key): StubPath<String>| async move {
                    StatusCode::NO_CONTENT
                }),
            )
            .route(
                "/n1/objects/recover/version/{version_id}/{*object_key}",
                post(
                    |StubPath((_version_id, _object_key)): StubPath<(String, String)>| async move {
                        StatusCode::OK
                    },
                ),
            )
            .route(
                "/n1/objects/recover/{*object_key}",
                post(|StubPath(_object_key): StubPath<String>| async move {
                    StatusCode::OK
                }),
            )
            .route(
                "/n1/trash/recover-all",
                post(|| async move {
                    (
                        StatusCode::ACCEPTED,
                        Json(json!({
                            "is_running": false,
                            "phase": "Completed",
                            "last_error": null
                        })),
                    )
                }),
            )
            .route(
                "/n1/trash/empty",
                delete(|| async move {
                    (
                        StatusCode::ACCEPTED,
                        Json(json!({
                            "is_running": false,
                            "phase": "Completed",
                            "last_error": null
                        })),
                    )
                }),
            )
            .route(
                "/n1/maintenance/trash/recover-all/status",
                get(|| async move {
                    Json(json!({
                        "is_running": false,
                        "phase": "Completed",
                        "last_error": null
                    }))
                }),
            )
            .route(
                "/n1/maintenance/trash/empty/status",
                get(|| async move {
                    Json(json!({
                        "is_running": false,
                        "phase": "Completed",
                        "last_error": null
                    }))
                }),
            )
            .route(
                "/n1/upload/multipart",
                get(|| async move {
                    Json(json!({
                        "sessions": [],
                        "total": 0
                    }))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind storage stub");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve stub");
        });
        format!("http://{}", addr)
    }

    async fn spawn_storage_listing_stub(
        token: &'static str,
        live_payload: Value,
        trash_payload: Option<Value>,
    ) -> String {
        let trash_payload = trash_payload.unwrap_or_else(|| {
            json!({
                "objects": [],
                "common_prefixes": [],
                "continuation_token": null
            })
        });
        let app = Router::new()
            .route(
                "/n1/auth/fragment",
                post(move |Json(_payload): Json<Value>| async move {
                    (
                        StatusCode::OK,
                        Json(json!({
                            "access_token": token,
                            "token_type": "Bearer",
                            "expires_in": 600
                        })),
                    )
                }),
            )
            .route(
                "/n1/objects",
                get({
                    let live_payload = live_payload.clone();
                    move |headers: HeaderMap| {
                        let live_payload = live_payload.clone();
                        async move {
                            if headers
                                .get(header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                != Some(&format!("Bearer {}", token))
                            {
                                return StatusCode::UNAUTHORIZED.into_response();
                            }
                            Json(live_payload).into_response()
                        }
                    }
                }),
            )
            .route(
                "/n1/trash",
                get({
                    let trash_payload = trash_payload.clone();
                    move |headers: HeaderMap| {
                        let trash_payload = trash_payload.clone();
                        async move {
                            if headers
                                .get(header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                != Some(&format!("Bearer {}", token))
                            {
                                return StatusCode::UNAUTHORIZED.into_response();
                            }
                            Json(trash_payload).into_response()
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind storage listing stub");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve storage listing stub");
        });
        format!("http://{}", addr)
    }

    async fn spawn_storage_recording_stub(
        token: &'static str,
        calls: Arc<TokioMutex<Vec<String>>>,
    ) -> String {
        let delete_calls = calls.clone();
        let bulk_calls = calls.clone();
        let permanent_calls = calls.clone();
        let recover_calls = calls.clone();
        let recover_object_calls = calls.clone();
        let recover_all_calls = calls.clone();
        let empty_calls = calls.clone();
        let recover_status_calls = calls.clone();
        let empty_status_calls = calls.clone();

        let app = Router::new()
            .route(
                "/n1/auth/fragment",
                post(move |Json(_payload): Json<Value>| async move {
                    (
                        StatusCode::OK,
                        Json(json!({
                            "access_token": token,
                            "token_type": "Bearer",
                            "expires_in": 600
                        })),
                    )
                }),
            )
            .route(
                "/n1/objects/{*object_key}",
                delete(move |StubPath(object_key): StubPath<String>| {
                    let delete_calls = delete_calls.clone();
                    async move {
                        delete_calls
                            .lock()
                            .await
                            .push(format!("DELETE /n1/objects/{object_key}"));
                        StatusCode::OK
                    }
                }),
            )
            .route(
                "/n1/objects/bulk-delete",
                post(move |Json(payload): Json<Value>| {
                    let bulk_calls = bulk_calls.clone();
                    async move {
                        let body = serde_json::to_string(&payload).expect("serialize bulk payload");
                        bulk_calls
                            .lock()
                            .await
                            .push(format!("POST /n1/objects/bulk-delete {body}"));
                        StatusCode::OK
                    }
                }),
            )
            .route(
                "/n1/objects/permanent/{*object_key}",
                delete(move |StubPath(object_key): StubPath<String>| {
                    let permanent_calls = permanent_calls.clone();
                    async move {
                        permanent_calls
                            .lock()
                            .await
                            .push(format!("DELETE /n1/objects/permanent/{object_key}"));
                        StatusCode::NO_CONTENT
                    }
                }),
            )
            .route(
                "/n1/objects/recover/version/{version_id}/{*object_key}",
                post(
                    move |StubPath((version_id, object_key)): StubPath<(String, String)>| {
                        let recover_calls = recover_calls.clone();
                        async move {
                            recover_calls.lock().await.push(format!(
                                "POST /n1/objects/recover/version/{version_id}/{object_key}"
                            ));
                            StatusCode::OK
                        }
                    },
                ),
            )
            .route(
                "/n1/objects/recover/{*object_key}",
                post(move |StubPath(object_key): StubPath<String>| {
                    let recover_object_calls = recover_object_calls.clone();
                    async move {
                        recover_object_calls
                            .lock()
                            .await
                            .push(format!("POST /n1/objects/recover/{object_key}"));
                        StatusCode::OK
                    }
                }),
            )
            .route(
                "/n1/trash/recover-all",
                post(move || {
                    let recover_all_calls = recover_all_calls.clone();
                    async move {
                        recover_all_calls
                            .lock()
                            .await
                            .push("POST /n1/trash/recover-all".to_string());
                        (
                            StatusCode::ACCEPTED,
                            Json(json!({
                                "is_running": false,
                                "phase": "Completed",
                                "last_error": null
                            })),
                        )
                    }
                }),
            )
            .route(
                "/n1/trash/empty",
                delete(move || {
                    let empty_calls = empty_calls.clone();
                    async move {
                        empty_calls
                            .lock()
                            .await
                            .push("DELETE /n1/trash/empty".to_string());
                        (
                            StatusCode::ACCEPTED,
                            Json(json!({
                                "is_running": false,
                                "phase": "Completed",
                                "last_error": null
                            })),
                        )
                    }
                }),
            )
            .route(
                "/n1/maintenance/trash/recover-all/status",
                get(move || {
                    let recover_status_calls = recover_status_calls.clone();
                    async move {
                        recover_status_calls
                            .lock()
                            .await
                            .push("GET /n1/maintenance/trash/recover-all/status".to_string());
                        Json(json!({
                            "is_running": false,
                            "phase": "Completed",
                            "last_error": null
                        }))
                    }
                }),
            )
            .route(
                "/n1/maintenance/trash/empty/status",
                get(move || {
                    let empty_status_calls = empty_status_calls.clone();
                    async move {
                        empty_status_calls
                            .lock()
                            .await
                            .push("GET /n1/maintenance/trash/empty/status".to_string());
                        Json(json!({
                            "is_running": false,
                            "phase": "Completed",
                            "last_error": null
                        }))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind storage recording stub");
        let addr = listener.local_addr().expect("recording stub addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve recording storage stub");
        });
        format!("http://{}", addr)
    }

    async fn spawn_storage_version_stub(initial: HashMap<String, StubObjectState>) -> String {
        let state = Arc::new(TokioMutex::new(initial));
        let list_state = state.clone();
        let meta_state = state.clone();
        let versions_state = state.clone();
        let delete_state = state.clone();
        let soft_delete_state = state.clone();
        let recover_state = state.clone();

        let app = Router::new()
            .route(
                "/n1/auth/fragment",
                post(|Json(_payload): Json<Value>| async move {
                    (
                        StatusCode::OK,
                        Json(json!({
                            "access_token": "version-stub-token",
                            "token_type": "Bearer",
                            "expires_in": 600
                        })),
                    )
                }),
            )
            .route(
                "/n1/objects/meta/{*object_key}",
                get(move |StubPath(object_key): StubPath<String>| {
                    let meta_state = meta_state.clone();
                    async move {
                        let state = meta_state.lock().await;
                        let response = state
                            .get(&object_key)
                            .or_else(|| {
                                if state.len() == 1 {
                                    state.values().next()
                                } else {
                                    None
                                }
                            })
                            .and_then(|entry| {
                                if !entry.current_visible {
                                    return None;
                                }
                                entry.versions.first().map(|version| {
                                    json!({
                                        "fragment": "fragment-a",
                                        "object_key": object_key,
                                        "version_id": version.version_id,
                                        "content_type": version.content_type,
                                        "total_size_bytes": version.object_size_bytes,
                                        "created_at": version.created_at
                                    })
                                })
                            });
                        response
                            .map(|payload| (StatusCode::OK, Json(payload)).into_response())
                            .unwrap_or_else(|| StatusCode::NOT_FOUND.into_response())
                    }
                }),
            )
            .route(
                "/n1/objects/versions/{*object_key}",
                get(move |StubPath(object_key): StubPath<String>| {
                    let versions_state = versions_state.clone();
                    async move {
                        let state = versions_state.lock().await;
                        let payload = state
                            .get(&object_key)
                            .or_else(|| {
                                if state.len() == 1 {
                                    state.values().next()
                                } else {
                                    None
                                }
                            })
                            .map(|entry| {
                                entry
                                    .versions
                                    .iter()
                                    .map(|version| {
                                        json!({
                                            "fragment": "fragment-a",
                                            "object_key": object_key,
                                            "version_id": version.version_id,
                                            "content_type": version.content_type,
                                            "total_size_bytes": version.object_size_bytes,
                                            "created_at": version.created_at
                                        })
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        Json(payload)
                    }
                }),
            )
            .route(
                "/n1/upload/multipart",
                get(|| async move {
                    Json(json!({
                        "sessions": [],
                        "total": 0
                    }))
                }),
            )
            .route(
                "/n1/objects",
                get(
                    move |Query(params): Query<std::collections::HashMap<String, String>>| {
                        let list_state = list_state.clone();
                        async move {
                            let state = list_state.lock().await;
                            let prefix = params
                                .get("prefix")
                                .map(|value| value.trim().trim_start_matches('/').to_string())
                                .filter(|value| !value.is_empty());
                            let delimiter = params
                                .get("delimiter")
                                .map(|value| value.trim().to_string())
                                .filter(|value| !value.is_empty());
                            let limit = params
                                .get("limit")
                                .and_then(|value| value.parse::<usize>().ok())
                                .unwrap_or(200)
                                .clamp(1, 1000);

                            let mut objects = Vec::new();
                            let mut common_prefixes = std::collections::BTreeSet::new();

                            for (object_key, entry) in state.iter() {
                                let Some(version) = entry.versions.first() else {
                                    continue;
                                };
                                if let Some(prefix) = prefix.as_deref() {
                                    if !object_key.starts_with(prefix) {
                                        continue;
                                    }
                                }

                                if delimiter.as_deref() == Some("/") {
                                    let suffix = prefix
                                        .as_deref()
                                        .and_then(|value| object_key.strip_prefix(value))
                                        .unwrap_or(object_key.as_str());
                                    if let Some(index) = suffix.find('/') {
                                        let base = prefix.clone().unwrap_or_default();
                                        common_prefixes.insert(format!(
                                            "{}{}/",
                                            base,
                                            &suffix[..index]
                                        ));
                                        continue;
                                    }
                                }

                                objects.push(json!({
                                    "fragment": "fragment-a",
                                    "object_key": object_key,
                                    "version_id": version.version_id,
                                    "content_type": version.content_type,
                                    "total_size_bytes": version.object_size_bytes,
                                    "created_at": version.created_at
                                }));
                            }

                            objects.sort_by(|a, b| {
                                a["object_key"].as_str().cmp(&b["object_key"].as_str())
                            });
                            let mut common_prefixes =
                                common_prefixes.into_iter().collect::<Vec<_>>();
                            common_prefixes.sort();

                            let total_items = objects.len().saturating_add(common_prefixes.len());
                            if total_items > limit {
                                let mut remaining = limit;
                                let mut limited_prefixes = Vec::new();
                                for prefix in common_prefixes.into_iter() {
                                    if remaining == 0 {
                                        break;
                                    }
                                    limited_prefixes.push(prefix);
                                    remaining -= 1;
                                }

                                let mut limited_objects = Vec::new();
                                for object in objects.into_iter() {
                                    if remaining == 0 {
                                        break;
                                    }
                                    limited_objects.push(object);
                                    remaining -= 1;
                                }

                                return Json(json!({
                                    "objects": limited_objects,
                                    "common_prefixes": limited_prefixes,
                                    "continuation_token": "stub-more"
                                }));
                            }

                            Json(json!({
                                "objects": objects,
                                "common_prefixes": common_prefixes,
                                "continuation_token": Value::Null
                            }))
                        }
                    },
                ),
            )
            .route(
                "/n1/objects/version/{version_id}/{*object_key}",
                delete(
                    move |StubPath((version_id, object_key)): StubPath<(String, String)>| {
                        let delete_state = delete_state.clone();
                        async move {
                            let mut state = delete_state.lock().await;
                            let entry_key = if state.contains_key(&object_key) {
                                Some(object_key.clone())
                            } else if state.len() == 1 {
                                state.keys().next().cloned()
                            } else {
                                None
                            };
                            let Some(entry_key) = entry_key else {
                                return StatusCode::NOT_FOUND;
                            };
                            let Some(entry) = state.get_mut(&entry_key) else {
                                return StatusCode::NOT_FOUND;
                            };
                            let removed_index = entry
                                .versions
                                .iter()
                                .position(|version| version.version_id == version_id);
                            let Some(index) = removed_index else {
                                return StatusCode::NOT_FOUND;
                            };
                            entry.versions.remove(index);
                            if index == 0 || entry.versions.is_empty() {
                                entry.current_visible = false;
                            }
                            StatusCode::NO_CONTENT
                        }
                    },
                ),
            );
        let app = app
            .route(
                "/n1/objects/{*object_key}",
                delete(move |StubPath(object_key): StubPath<String>| {
                    let soft_delete_state = soft_delete_state.clone();
                    async move {
                        let mut state = soft_delete_state.lock().await;
                        let entry_key = if state.contains_key(&object_key) {
                            Some(object_key.clone())
                        } else if state.len() == 1 {
                            state.keys().next().cloned()
                        } else {
                            None
                        };
                        let Some(entry_key) = entry_key else {
                            return StatusCode::NOT_FOUND;
                        };
                        let Some(entry) = state.get_mut(&entry_key) else {
                            return StatusCode::NOT_FOUND;
                        };
                        if !entry.current_visible {
                            return StatusCode::NOT_FOUND;
                        }
                        entry.current_visible = false;
                        StatusCode::OK
                    }
                }),
            )
            .route(
                "/n1/objects/recover/{*object_key}",
                post(move |StubPath(object_key): StubPath<String>| {
                    let recover_state = recover_state.clone();
                    async move {
                        let mut state = recover_state.lock().await;
                        let entry_key = if state.contains_key(&object_key) {
                            Some(object_key.clone())
                        } else if state.len() == 1 {
                            state.keys().next().cloned()
                        } else {
                            None
                        };
                        let Some(entry_key) = entry_key else {
                            return StatusCode::NOT_FOUND;
                        };
                        let Some(entry) = state.get_mut(&entry_key) else {
                            return StatusCode::NOT_FOUND;
                        };
                        if entry.versions.is_empty() {
                            return StatusCode::NOT_FOUND;
                        }
                        entry.current_visible = true;
                        StatusCode::OK
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind version storage stub");
        let addr = listener.local_addr().expect("version storage local addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve version storage stub");
        });
        format!("http://{}", addr)
    }

    async fn test_state(single_url: String, multi_url: String) -> (AppState, String) {
        let db_path =
            std::env::temp_dir().join(format!("registry-router-test-{}.db", uuid::Uuid::new_v4()));
        let db = RegistryDb::new(db_path, 1024 * 1024);
        db.init().await.expect("init registry db");
        let password_hash = auth::hash_password("password123").expect("hash password");
        let user = db
            .create_user(
                "drive@example.com".to_string(),
                password_hash,
                Some(50 * 1024 * 1024),
                false,
            )
            .await
            .expect("create user");
        let config = Arc::new(RegistryConfig {
            host: "127.0.0.1".to_string(),
            https_port: 51001,
            cert_path: "unused-cert.pem".into(),
            key_path: "unused-key.pem".into(),
            sqlite_path: "unused.db".into(),
            jwt_secret: "registry-secret".to_string(),
            master_key: "registry-master".to_string(),
            access_token_ttl_secs: 900,
            refresh_token_ttl_secs: 3600,
            default_quota_bytes: 25 * 1024 * 1024 * 1024,
            storage_session_ttl_secs: 900,
            single_node_url: Some(single_url),
            single_node_master_key: Some("single-master".to_string()),
            single_node_insecure_tls: false,
            multi_node_url: Some(multi_url),
            multi_node_master_key: Some("multi-master".to_string()),
            multi_node_insecure_tls: false,
        });
        let auth = Arc::new(AuthSettings {
            jwt_secret: config.jwt_secret.clone(),
            master_key: config.master_key.clone(),
            access_token_ttl_secs: config.access_token_ttl_secs,
            refresh_token_ttl_secs: config.refresh_token_ttl_secs,
        });
        let state = AppState {
            db,
            auth: auth.clone(),
            broker: StorageBroker::new(config).expect("build broker"),
            realtime: RegistryRealtimeHub::new(),
            write_gate: Arc::new(Mutex::new(())),
            pending_upload_grace_secs: 900,
        };
        let (token, _) = issue_access_token(&user, &auth).expect("issue access token");
        (state, token)
    }

    async fn spawn_registry_server(state: AppState) -> String {
        let app = build_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind registry test server");
        let addr = listener.local_addr().expect("registry test server addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve registry test server");
        });
        format!("http://{}", addr)
    }

    #[tokio::test]
    async fn list_objects_prefers_engine_namespace_for_bound_fragment() {
        let single_url = spawn_storage_listing_stub(
            "single-stub-token",
            json!({
                "objects": [
                    {
                        "object_key": "live.txt",
                        "version_id": "01ENGINELIST0000000000000000001",
                        "content_type": "text/plain",
                        "total_size_bytes": 1234,
                        "created_at": 1710000000
                    }
                ],
                "common_prefixes": ["docs/"],
                "continuation_token": null
            }),
            None,
        )
        .await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let owner = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user")
            .user;
        state
            .db
            .upsert_fragment_binding(
                owner.uid.clone(),
                "fragment-a".to_string(),
                StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                owner.uid.clone(),
                "fragment-a".to_string(),
                "stale.txt".to_string(),
                "01STALELIST000000000000000000001".to_string(),
                64,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize stale projection");

        let base_url = spawn_registry_server(state).await;
        let client = Client::new();
        let response = client
            .get(format!(
                "{}/v1/objects?fragment=fragment-a&limit=200",
                base_url
            ))
            .bearer_auth(&access_token)
            .send()
            .await
            .expect("list objects request");
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = response.json().await.expect("parse list objects");
        assert_eq!(payload["common_prefixes"], json!(["docs/"]));
        let objects = payload["objects"].as_array().expect("objects array");
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0]["object_key"].as_str(), Some("live.txt"));
        assert_eq!(objects[0]["object_size_bytes"].as_i64(), Some(1234));
        assert_eq!(objects[0]["status"].as_str(), Some("ready"));
    }

    #[tokio::test]
    async fn object_stats_prefers_engine_namespace_for_bound_fragment() {
        let single_url = spawn_storage_listing_stub(
            "single-stub-token",
            json!({
                "objects": [
                    {
                        "object_key": "live.txt",
                        "version_id": "01ENGSTATS000000000000000000001",
                        "content_type": "text/plain",
                        "total_size_bytes": 1234,
                        "created_at": 1710000000
                    },
                    {
                        "object_key": "docs/report.pdf",
                        "version_id": "01ENGSTATS000000000000000000002",
                        "content_type": "application/pdf",
                        "total_size_bytes": 2048,
                        "created_at": 1710000010
                    }
                ],
                "common_prefixes": [],
                "continuation_token": null
            }),
            None,
        )
        .await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let owner = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user")
            .user;
        state
            .db
            .upsert_fragment_binding(
                owner.uid.clone(),
                "fragment-a".to_string(),
                StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                owner.uid.clone(),
                "fragment-a".to_string(),
                "stale.bin".to_string(),
                "01STALESTAT00000000000000000001".to_string(),
                9999,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect("finalize stale stats projection");

        let base_url = spawn_registry_server(state).await;
        let client = Client::new();
        let response = client
            .get(format!("{}/v1/objects/stats?fragment=fragment-a", base_url))
            .bearer_auth(&access_token)
            .send()
            .await
            .expect("object stats request");
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = response.json().await.expect("parse object stats");
        assert_eq!(payload["total_objects"].as_u64(), Some(2));
        assert_eq!(payload["total_size_bytes"].as_i64(), Some(3282));
        let extensions = payload["extension_stats"]
            .as_array()
            .expect("extension stats array");
        assert!(extensions.iter().any(|entry| {
            entry["extension"].as_str() == Some("txt")
                && entry["object_count"].as_u64() == Some(1)
                && entry["total_size_bytes"].as_i64() == Some(1234)
        }));
        assert!(extensions.iter().any(|entry| {
            entry["extension"].as_str() == Some("pdf")
                && entry["object_count"].as_u64() == Some(1)
                && entry["total_size_bytes"].as_i64() == Some(2048)
        }));
        assert!(
            !extensions
                .iter()
                .any(|entry| entry["extension"].as_str() == Some("bin")),
            "stats should come from engine namespace, not stale registry projection"
        );
    }

    async fn connect_registry_ws(base_url: &str, token: &str) -> TestWsStream {
        let ws_url = format!(
            "{}/v1/ws?token={}",
            base_url.replacen("http://", "ws://", 1),
            token
        );
        let (socket, _response) = connect_async(ws_url).await.expect("connect registry ws");
        socket
    }

    async fn consume_ws_hello(socket: &mut TestWsStream) {
        loop {
            let message = timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("hello timeout")
                .expect("hello frame")
                .expect("hello message");
            if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                let json: Value = serde_json::from_str(&text).expect("parse hello message");
                if json["op"] == "HELLO" {
                    return;
                }
            }
        }
    }

    async fn wait_for_event_type(socket: &mut TestWsStream, wanted_type: &str) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let message = timeout(remaining, socket.next())
                .await
                .expect("event timeout")
                .expect("event frame")
                .expect("event message");
            if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                let json: Value = serde_json::from_str(&text).expect("parse event json");
                if json["op"] == "PONG" || json["op"] == "HELLO" {
                    continue;
                }
                if json["op"] == "EVENT" && json["d"]["type"] == wanted_type {
                    return json["d"].clone();
                }
            }
        }
    }

    async fn wait_for_state_scope(socket: &mut TestWsStream, wanted_scope: &str) -> Value {
        loop {
            let event = wait_for_event_type(socket, "STATE_INVALIDATED").await;
            if event["data"]["scope"].as_str() == Some(wanted_scope) {
                return event;
            }
        }
    }

    async fn expect_no_registry_event(socket: &mut TestWsStream, wait_for: Duration) {
        let start = tokio::time::Instant::now();
        loop {
            let remaining = wait_for.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return;
            }
            let next = timeout(remaining, socket.next()).await;
            let Ok(Some(Ok(message))) = next else {
                return;
            };
            if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                let json: Value = serde_json::from_str(&text).expect("parse no-event json");
                if json["op"] == "HELLO" || json["op"] == "PONG" {
                    continue;
                }
                panic!("unexpected websocket event: {}", json);
            }
        }
    }

    async fn drain_object_purge_queue(state: &AppState) {
        loop {
            let processed = process_next_object_purge(state)
                .await
                .expect("process purge queue");
            if !processed {
                break;
            }
        }
    }

    #[tokio::test]
    async fn fragment_link_and_storage_session_follow_deployment_binding() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url.clone(), multi_url.clone()).await;
        let app = build_router(state);

        let single_link = Request::builder()
            .method("POST")
            .uri("/v1/fragments/link")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "frag-single",
                    "deployment": "single_node"
                }))
                .expect("serialize request"),
            ))
            .expect("single link request");
        let single_link_resp = app.clone().oneshot(single_link).await.expect("single link");
        assert_eq!(single_link_resp.status(), StatusCode::CREATED);

        let multi_link = Request::builder()
            .method("POST")
            .uri("/v1/fragments/link")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "frag-multi",
                    "deployment": "multi_node"
                }))
                .expect("serialize request"),
            ))
            .expect("multi link request");
        let multi_link_resp = app.clone().oneshot(multi_link).await.expect("multi link");
        assert_eq!(multi_link_resp.status(), StatusCode::CREATED);

        let fragments_req = Request::builder()
            .method("GET")
            .uri("/v1/fragments")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("fragments request");
        let fragments_resp = app
            .clone()
            .oneshot(fragments_req)
            .await
            .expect("fragments response");
        assert_eq!(fragments_resp.status(), StatusCode::OK);
        let fragments_body = axum::body::to_bytes(fragments_resp.into_body(), usize::MAX)
            .await
            .expect("read fragments body");
        let fragments_json: Value =
            serde_json::from_slice(&fragments_body).expect("parse fragments response");
        let bindings = fragments_json["bindings"]
            .as_array()
            .expect("bindings array");
        assert_eq!(bindings.len(), 2);

        let single_session_req = Request::builder()
            .method("POST")
            .uri("/v1/storage/session")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "frag-single",
                    "read": true,
                    "write": true
                }))
                .expect("serialize single session request"),
            ))
            .expect("single session request");
        let single_session_resp = app
            .clone()
            .oneshot(single_session_req)
            .await
            .expect("single session response");
        assert_eq!(single_session_resp.status(), StatusCode::OK);
        let single_body = axum::body::to_bytes(single_session_resp.into_body(), usize::MAX)
            .await
            .expect("read single session body");
        let single_json: Value =
            serde_json::from_slice(&single_body).expect("parse single session");
        assert_eq!(single_json["deployment"], "single_node");
        assert_eq!(single_json["engine_url"], single_url);
        assert_eq!(single_json["access_token"], "single-stub-token");

        let multi_session_req = Request::builder()
            .method("POST")
            .uri("/v1/storage/session")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "frag-multi",
                    "read": true,
                    "modify": true,
                    "delete": true
                }))
                .expect("serialize multi session request"),
            ))
            .expect("multi session request");
        let multi_session_resp = app
            .oneshot(multi_session_req)
            .await
            .expect("multi session response");
        assert_eq!(multi_session_resp.status(), StatusCode::OK);
        let multi_body = axum::body::to_bytes(multi_session_resp.into_body(), usize::MAX)
            .await
            .expect("read multi session body");
        let multi_json: Value = serde_json::from_slice(&multi_body).expect("parse multi session");
        assert_eq!(multi_json["deployment"], "multi_node");
        assert_eq!(multi_json["engine_url"], multi_url);
        assert_eq!(multi_json["access_token"], "multi-stub-token");
    }

    #[tokio::test]
    async fn signup_route_allows_browser_preflight() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, _) = test_state(single_url, multi_url).await;
        let app = build_router(state);

        let request = Request::builder()
            .method("OPTIONS")
            .uri("/v1/auth/signup")
            .header(ORIGIN, "https://cloud.example.test")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
            .body(Body::empty())
            .expect("preflight request");
        let response = app.oneshot(request).await.expect("preflight response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&"*".parse().expect("allow origin header"))
        );
        let allow_methods = response
            .headers()
            .get(ACCESS_CONTROL_ALLOW_METHODS)
            .expect("allow methods header")
            .to_str()
            .expect("allow methods string");
        assert!(allow_methods.contains("POST"));

        let allow_headers = response
            .headers()
            .get(ACCESS_CONTROL_ALLOW_HEADERS)
            .expect("allow headers header")
            .to_str()
            .expect("allow headers string")
            .to_ascii_lowercase();
        assert!(allow_headers.contains("content-type"));
    }

    #[tokio::test]
    async fn signup_auto_provisions_and_links_default_fragment() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, _) = test_state(single_url.clone(), multi_url).await;
        let app = build_router(state);

        let signup_req = Request::builder()
            .method("POST")
            .uri("/v1/auth/signup")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "email": "autofragment@example.com",
                    "password": "password123"
                }))
                .expect("serialize signup request"),
            ))
            .expect("signup request");
        let signup_resp = app
            .clone()
            .oneshot(signup_req)
            .await
            .expect("signup response");
        assert_eq!(signup_resp.status(), StatusCode::CREATED);
        let signup_body = axum::body::to_bytes(signup_resp.into_body(), usize::MAX)
            .await
            .expect("read signup body");
        let signup_json: Value = serde_json::from_slice(&signup_body).expect("parse signup body");
        let access_token = signup_json["access_token"]
            .as_str()
            .expect("signup access token");

        let fragments_req = Request::builder()
            .method("GET")
            .uri("/v1/fragments")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("fragments request");
        let fragments_resp = app
            .clone()
            .oneshot(fragments_req)
            .await
            .expect("fragments response");
        assert_eq!(fragments_resp.status(), StatusCode::OK);
        let fragments_body = axum::body::to_bytes(fragments_resp.into_body(), usize::MAX)
            .await
            .expect("read fragments body");
        let fragments_json: Value =
            serde_json::from_slice(&fragments_body).expect("parse fragments response");
        let fragments = fragments_json["fragments"]
            .as_array()
            .expect("fragments array");
        assert_eq!(fragments.len(), 1);

        let bindings = fragments_json["bindings"]
            .as_array()
            .expect("bindings array");
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0]["deployment"], "single_node");
        assert_eq!(bindings[0]["fragment"], fragments[0]);

        let session_req = Request::builder()
            .method("POST")
            .uri("/v1/storage/session")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": fragments[0].as_str().expect("fragment string"),
                    "read": true,
                    "write": true
                }))
                .expect("serialize storage session request"),
            ))
            .expect("storage session request");
        let session_resp = app
            .oneshot(session_req)
            .await
            .expect("storage session response");
        assert_eq!(session_resp.status(), StatusCode::OK);
        let session_body = axum::body::to_bytes(session_resp.into_body(), usize::MAX)
            .await
            .expect("read session body");
        let session_json: Value =
            serde_json::from_slice(&session_body).expect("parse session body");
        assert_eq!(session_json["engine_url"], single_url);
        assert_eq!(session_json["access_token"], "single-stub-token");
    }

    #[tokio::test]
    async fn signup_rolls_back_user_when_storage_fragment_is_not_ready() {
        let single_url =
            spawn_storage_stub_with_auth_status("single-stub-token", StatusCode::NOT_FOUND).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, _) = test_state(single_url, multi_url).await;
        let app = build_router(state.clone());

        let signup_req = Request::builder()
            .method("POST")
            .uri("/v1/auth/signup")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "email": "broken-fragment@example.com",
                    "password": "password123"
                }))
                .expect("serialize signup request"),
            ))
            .expect("signup request");
        let signup_resp = app.oneshot(signup_req).await.expect("signup response");
        assert_eq!(signup_resp.status(), StatusCode::BAD_GATEWAY);

        assert!(
            state
                .db
                .find_user_auth_by_email("broken-fragment@example.com".to_string())
                .await
                .expect("lookup failed signup user")
                .is_none(),
            "registry user should be rolled back when storage provisioning fails"
        );
    }

    #[tokio::test]
    async fn signup_ignores_client_supplied_quota_limit() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, _) = test_state(single_url, multi_url).await;
        let app = build_router(state);

        let signup_req = Request::builder()
            .method("POST")
            .uri("/v1/auth/signup")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "email": "quota-abuse@example.com",
                    "password": "password123",
                    "quota_limit_bytes": 999999999999i64
                }))
                .expect("serialize signup request"),
            ))
            .expect("signup request");
        let signup_resp = app.oneshot(signup_req).await.expect("signup response");
        assert_eq!(signup_resp.status(), StatusCode::CREATED);
        let signup_body = axum::body::to_bytes(signup_resp.into_body(), usize::MAX)
            .await
            .expect("read signup body");
        let signup_json: Value = serde_json::from_slice(&signup_body).expect("parse signup body");
        assert_eq!(
            signup_json["user"]["quota_limit_bytes"].as_i64(),
            Some(1024 * 1024)
        );
    }

    #[tokio::test]
    async fn registry_refresh_rotates_and_logout_revokes_browser_session() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, _) = test_state(single_url, multi_url).await;
        let app = build_router(state);

        let login_req = Request::builder()
            .method("POST")
            .uri("/v1/auth/login")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "email": "drive@example.com",
                    "password": "password123"
                }))
                .expect("serialize login request"),
            ))
            .expect("login request");
        let login_resp = app
            .clone()
            .oneshot(login_req)
            .await
            .expect("login response");
        assert_eq!(login_resp.status(), StatusCode::OK);
        let login_body = axum::body::to_bytes(login_resp.into_body(), usize::MAX)
            .await
            .expect("read login body");
        let login_json: Value = serde_json::from_slice(&login_body).expect("parse login body");
        let first_access = login_json["access_token"].as_str().expect("access token");
        let first_refresh = login_json["refresh_token"].as_str().expect("refresh token");

        let me_req = Request::builder()
            .method("GET")
            .uri("/v1/me")
            .header("authorization", format!("Bearer {}", first_access))
            .body(Body::empty())
            .expect("me request");
        let me_resp = app.clone().oneshot(me_req).await.expect("me response");
        assert_eq!(me_resp.status(), StatusCode::OK);

        let refresh_req = Request::builder()
            .method("POST")
            .uri("/v1/auth/refresh")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "refresh_token": first_refresh
                }))
                .expect("serialize refresh request"),
            ))
            .expect("refresh request");
        let refresh_resp = app
            .clone()
            .oneshot(refresh_req)
            .await
            .expect("refresh response");
        assert_eq!(refresh_resp.status(), StatusCode::OK);
        let refresh_body = axum::body::to_bytes(refresh_resp.into_body(), usize::MAX)
            .await
            .expect("read refresh body");
        let refresh_json: Value =
            serde_json::from_slice(&refresh_body).expect("parse refresh body");
        let rotated_access = refresh_json["access_token"]
            .as_str()
            .expect("rotated access token");
        let rotated_refresh = refresh_json["refresh_token"]
            .as_str()
            .expect("rotated refresh token");
        assert_ne!(first_refresh, rotated_refresh);

        let rotated_me_req = Request::builder()
            .method("GET")
            .uri("/v1/me")
            .header("authorization", format!("Bearer {}", rotated_access))
            .body(Body::empty())
            .expect("rotated me request");
        let rotated_me_resp = app
            .clone()
            .oneshot(rotated_me_req)
            .await
            .expect("rotated me response");
        assert_eq!(rotated_me_resp.status(), StatusCode::OK);

        let logout_req = Request::builder()
            .method("POST")
            .uri("/v1/auth/logout")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "refresh_token": rotated_refresh
                }))
                .expect("serialize logout request"),
            ))
            .expect("logout request");
        let logout_resp = app
            .clone()
            .oneshot(logout_req)
            .await
            .expect("logout response");
        assert_eq!(logout_resp.status(), StatusCode::NO_CONTENT);

        let refresh_again_req = Request::builder()
            .method("POST")
            .uri("/v1/auth/refresh")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "refresh_token": rotated_refresh
                }))
                .expect("serialize second refresh request"),
            ))
            .expect("second refresh request");
        let refresh_again_resp = app
            .oneshot(refresh_again_req)
            .await
            .expect("second refresh response");
        assert_eq!(refresh_again_resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_bootstrap_and_user_listing_work() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, _) = test_state(single_url, multi_url).await;
        let app = build_router(state);

        let bootstrap_req = Request::builder()
            .method("POST")
            .uri("/v1/admin/bootstrap")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "email": "admin@example.com",
                    "password": "password123",
                    "master_key": "registry-master"
                }))
                .expect("serialize bootstrap request"),
            ))
            .expect("bootstrap request");
        let bootstrap_resp = app
            .clone()
            .oneshot(bootstrap_req)
            .await
            .expect("bootstrap response");
        assert_eq!(bootstrap_resp.status(), StatusCode::CREATED);
        let bootstrap_body = axum::body::to_bytes(bootstrap_resp.into_body(), usize::MAX)
            .await
            .expect("read bootstrap body");
        let bootstrap_json: Value =
            serde_json::from_slice(&bootstrap_body).expect("parse bootstrap body");
        let access_token = bootstrap_json["access_token"]
            .as_str()
            .expect("bootstrap access token");
        assert_eq!(bootstrap_json["user"]["is_admin"], true);

        let list_req = Request::builder()
            .method("GET")
            .uri("/v1/admin/users")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("list admin users request");
        let list_resp = app.clone().oneshot(list_req).await.expect("list response");
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
            .await
            .expect("read list body");
        let list_json: Value = serde_json::from_slice(&list_body).expect("parse list body");
        let users = list_json["users"].as_array().expect("users array");
        assert_eq!(users.len(), 2);
        assert!(
            users
                .iter()
                .any(|user| user["email"] == "admin@example.com" && user["is_admin"] == true)
        );
    }

    #[tokio::test]
    async fn admin_bootstrap_and_login_do_not_require_storage_provisioning() {
        let single_url =
            spawn_storage_stub_with_auth_status("single-stub-token", StatusCode::NOT_FOUND).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, _) = test_state(single_url, multi_url).await;
        let app = build_router(state.clone());

        let bootstrap_req = Request::builder()
            .method("POST")
            .uri("/v1/admin/bootstrap")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "email": "ops-admin@example.com",
                    "password": "password123",
                    "master_key": "registry-master"
                }))
                .expect("serialize bootstrap request"),
            ))
            .expect("bootstrap request");
        let bootstrap_resp = app
            .clone()
            .oneshot(bootstrap_req)
            .await
            .expect("bootstrap response");
        assert_eq!(bootstrap_resp.status(), StatusCode::CREATED);
        let bootstrap_body = axum::body::to_bytes(bootstrap_resp.into_body(), usize::MAX)
            .await
            .expect("read bootstrap body");
        let bootstrap_json: Value =
            serde_json::from_slice(&bootstrap_body).expect("parse bootstrap body");
        let admin_uid = bootstrap_json["user"]["uid"]
            .as_str()
            .expect("bootstrap uid")
            .to_string();
        let access_token = bootstrap_json["access_token"]
            .as_str()
            .expect("bootstrap access token");

        assert!(
            state
                .db
                .list_fragments(admin_uid)
                .await
                .expect("list admin fragments")
                .is_empty(),
            "bootstrap admin should not auto-provision a storage fragment"
        );

        let fragments_req = Request::builder()
            .method("GET")
            .uri("/v1/fragments")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("fragments request");
        let fragments_resp = app
            .clone()
            .oneshot(fragments_req)
            .await
            .expect("fragments response");
        assert_eq!(fragments_resp.status(), StatusCode::OK);
        let fragments_body = axum::body::to_bytes(fragments_resp.into_body(), usize::MAX)
            .await
            .expect("read fragments body");
        let fragments_json: Value =
            serde_json::from_slice(&fragments_body).expect("parse fragments body");
        assert_eq!(
            fragments_json["fragments"]
                .as_array()
                .expect("fragments array")
                .len(),
            0
        );

        let login_req = Request::builder()
            .method("POST")
            .uri("/v1/admin/auth/login")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "email": "ops-admin@example.com",
                    "password": "password123"
                }))
                .expect("serialize admin login request"),
            ))
            .expect("admin login request");
        let login_resp = app
            .clone()
            .oneshot(login_req)
            .await
            .expect("admin login response");
        assert_eq!(login_resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_stats_expose_usage_and_activity() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, _) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user")
            .user;
        let admin_hash = auth::hash_password("password123").expect("hash admin password");
        let admin_user = state
            .db
            .create_user(
                "admin@example.com".to_string(),
                admin_hash,
                Some(100 * 1024 * 1024),
                true,
            )
            .await
            .expect("create admin user");

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment a");
        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-b".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment b");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "images/scan1.dcm".to_string(),
                "01HREGSTATS0000000000000000001".to_string(),
                4096,
                Some("application/dicom".to_string()),
            )
            .await
            .expect("finalize scan");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "reports/report.pdf".to_string(),
                "01HREGSTATS0000000000000000002".to_string(),
                2048,
                Some("application/pdf".to_string()),
            )
            .await
            .expect("finalize report");
        state
            .db
            .register_pending_object(
                drive_user.uid.clone(),
                "fragment-b".to_string(),
                "uploads/pending.bin".to_string(),
                Some(8192),
                Some("application/octet-stream".to_string()),
                None,
                None,
            )
            .await
            .expect("register pending");
        state
            .db
            .create_refresh_session(drive_user.uid.clone(), 3600)
            .await
            .expect("create refresh session");

        let (admin_token, _) =
            issue_access_token(&admin_user, &state.auth).expect("issue admin access token");
        let app = build_router(state);
        let req = Request::builder()
            .method("GET")
            .uri("/v1/admin/stats")
            .header("authorization", format!("Bearer {}", admin_token))
            .body(Body::empty())
            .expect("admin stats request");
        let resp = app
            .clone()
            .oneshot(req)
            .await
            .expect("admin stats response");
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read admin stats body");
        let json: Value = serde_json::from_slice(&body).expect("parse admin stats body");

        assert_eq!(json["totals"]["total_users"], 2);
        assert_eq!(json["totals"]["admin_users"], 1);
        assert_eq!(json["totals"]["ready_objects"], 2);
        assert_eq!(json["totals"]["pending_uploads"], 1);
        assert_eq!(json["totals"]["active_sessions"], 1);
        assert_eq!(json["largest_objects"][0]["object_key"], "images/scan1.dcm");
        assert!(
            json["file_types"]
                .as_array()
                .expect("file types array")
                .iter()
                .any(|entry| entry["extension"] == "dcm")
        );
        assert!(
            json["active_users"]
                .as_array()
                .expect("active users array")
                .iter()
                .any(|entry| entry["email"] == "drive@example.com")
        );
    }

    #[tokio::test]
    async fn me_reconciles_stale_pending_upload_reservations_when_engine_session_is_gone() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment a");

        state
            .db
            .register_pending_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "uploads/orphaned.bin".to_string(),
                Some(8192),
                Some("application/octet-stream".to_string()),
                Some("01HPENDINGSESSION000000000000001".to_string()),
                Some("01HPENDINGSESSION000000000000ABC".to_string()),
            )
            .await
            .expect("register pending multipart");

        let user_before = state
            .db
            .get_user(drive_user.uid.clone())
            .await
            .expect("user before reconcile");
        assert_eq!(user_before.quota_used_bytes, 8192);

        let app = build_router(state.clone());
        let me_req = Request::builder()
            .method("GET")
            .uri("/v1/me")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("me request");
        let me_resp = app.clone().oneshot(me_req).await.expect("me response");
        assert_eq!(me_resp.status(), StatusCode::OK);
        let me_body = axum::body::to_bytes(me_resp.into_body(), usize::MAX)
            .await
            .expect("read me body");
        let me_json: Value = serde_json::from_slice(&me_body).expect("parse me body");
        assert_eq!(me_json["quota_used_bytes"].as_i64(), Some(0));

        let pending = state
            .db
            .list_pending_upload_reservations(Some(drive_user.uid))
            .await
            .expect("list pending uploads after reconcile");
        assert!(
            pending.is_empty(),
            "stale pending reservation should be removed"
        );
    }

    #[tokio::test]
    async fn rename_prefix_reconciles_stale_destination_pending_upload_reservations() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment a");

        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "photos/source.jpg".to_string(),
                "01HRENAMESOURCE000000000000001".to_string(),
                4096,
                Some("image/jpeg".to_string()),
            )
            .await
            .expect("finalize source object");

        state
            .db
            .register_pending_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "archive/source.jpg".to_string(),
                Some(4096),
                Some("image/jpeg".to_string()),
                Some("01HRENAMEPENDING000000000001".to_string()),
                Some("01HRENAMEPENDING000000000XYZ".to_string()),
            )
            .await
            .expect("register stale pending destination");

        let app = build_router(state.clone());
        let req = Request::builder()
            .method("POST")
            .uri("/v1/objects/rename-prefix")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "fragment": "fragment-a",
                    "source_prefix": "photos",
                    "destination_prefix": "archive"
                })
                .to_string(),
            ))
            .expect("rename-prefix request");
        let resp = app
            .clone()
            .oneshot(req)
            .await
            .expect("rename-prefix response");
        assert_eq!(resp.status(), StatusCode::OK);

        let renamed = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "archive/source.jpg".to_string(),
            )
            .await
            .expect("resolve renamed object")
            .expect("renamed object exists");
        assert_eq!(renamed.object_key, "archive/source.jpg");

        let source = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "photos/source.jpg".to_string(),
            )
            .await
            .expect("resolve source object after rename");
        assert!(source.is_none(), "source object should no longer exist");

        let pending = state
            .db
            .list_pending_upload_reservations(Some(drive_user.uid))
            .await
            .expect("list pending uploads after rename");
        assert!(
            pending.is_empty(),
            "stale pending destination reservation should be removed before rename"
        );
    }

    #[tokio::test]
    async fn rename_object_reconciles_stale_destination_pending_upload_reservations() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment a");

        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "IMG_8634.mov".to_string(),
                "01HRENAMEFILE000000000000001".to_string(),
                4096,
                Some("video/quicktime".to_string()),
            )
            .await
            .expect("finalize source object");

        state
            .db
            .register_pending_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "videos/IMG_8634.mov".to_string(),
                Some(4096),
                Some("video/quicktime".to_string()),
                Some("01HRENAMEFILEPENDING000001".to_string()),
                Some("01HRENAMEFILEPENDING000XYZ".to_string()),
            )
            .await
            .expect("register stale pending destination");

        let app = build_router(state.clone());
        let req = Request::builder()
            .method("POST")
            .uri("/v1/objects/rename")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "fragment": "fragment-a",
                    "source_key": "IMG_8634.mov",
                    "destination_key": "videos/IMG_8634.mov"
                })
                .to_string(),
            ))
            .expect("rename request");
        let resp = app.clone().oneshot(req).await.expect("rename response");
        assert_eq!(resp.status(), StatusCode::OK);

        let renamed = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "videos/IMG_8634.mov".to_string(),
            )
            .await
            .expect("resolve renamed object")
            .expect("renamed object exists");
        assert_eq!(renamed.object_key, "videos/IMG_8634.mov");

        let source = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "IMG_8634.mov".to_string(),
            )
            .await
            .expect("resolve source object after rename");
        assert!(source.is_none(), "source object should no longer exist");

        let pending = state
            .db
            .list_pending_upload_reservations(Some(drive_user.uid))
            .await
            .expect("list pending uploads after rename");
        assert!(
            pending.is_empty(),
            "stale pending destination reservation should be removed before rename"
        );
    }

    #[tokio::test]
    async fn registry_ws_scopes_owner_events_and_mirrors_to_admin() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let extra_hash = auth::hash_password("password123").expect("hash extra password");
        let other_user = state
            .db
            .create_user(
                "other@example.com".to_string(),
                extra_hash.clone(),
                Some(50 * 1024 * 1024),
                false,
            )
            .await
            .expect("create other user");
        let admin_user = state
            .db
            .create_user(
                "admin@example.com".to_string(),
                extra_hash,
                Some(50 * 1024 * 1024),
                true,
            )
            .await
            .expect("create admin user");
        let (other_token, _) =
            issue_access_token(&other_user, &state.auth).expect("issue other token");
        let (admin_token, _) =
            issue_access_token(&admin_user, &state.auth).expect("issue admin token");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let mut drive_ws = connect_registry_ws(&base_url, &drive_token).await;
        let mut other_ws = connect_registry_ws(&base_url, &other_token).await;
        let mut admin_ws = connect_registry_ws(&base_url, &admin_token).await;
        consume_ws_hello(&mut drive_ws).await;
        consume_ws_hello(&mut other_ws).await;
        consume_ws_hello(&mut admin_ws).await;

        let response = client
            .post(format!("{}/v1/objects/finalize", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_key": "images/scan-001.png",
                "version_id": "01HREGWS0000000000000000000001",
                "object_size_bytes": 4096,
                "content_type": "image/png"
            }))
            .send()
            .await
            .expect("finalize request");
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        let drive_event = wait_for_event_type(&mut drive_ws, "FILE_UPLOADED").await;
        assert_eq!(drive_event["data"]["fragment"], "fragment-a");
        assert_eq!(drive_event["data"]["object_key"], "images/scan-001.png");

        let admin_event = wait_for_event_type(&mut admin_ws, "FILE_UPLOADED").await;
        assert_eq!(admin_event["data"]["fragment"], "fragment-a");
        assert_eq!(admin_event["data"]["object_key"], "images/scan-001.png");

        expect_no_registry_event(&mut other_ws, Duration::from_millis(350)).await;
    }

    #[tokio::test]
    async fn finalize_route_syncs_engine_truth_instead_of_trusting_client_finalize_payload() {
        let mut objects = HashMap::new();
        objects.insert(
            "videos/IMG_8634.mov".to_string(),
            StubObjectState {
                current_visible: true,
                versions: vec![
                    StubVersion {
                        version_id: "01ENGINEFINALIZE0000000000000002".to_string(),
                        object_size_bytes: 157_230_177,
                        content_type: "video/quicktime".to_string(),
                        created_at: 2_000,
                    },
                    StubVersion {
                        version_id: "01ENGINEFINALIZE0000000000000001".to_string(),
                        object_size_bytes: 157_230_176,
                        content_type: "video/quicktime".to_string(),
                        created_at: 1_000,
                    },
                ],
            },
        );

        let single_url = spawn_storage_version_stub(objects).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");

        let pending = state
            .db
            .register_pending_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "videos/IMG_8634.mov".to_string(),
                Some(512),
                Some("application/octet-stream".to_string()),
                Some("01CLIENTWRONGFINALIZE000000000001".to_string()),
                Some("session-client-wrong".to_string()),
            )
            .await
            .expect("register pending object");

        let app = build_router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/objects/finalize")
            .header("authorization", format!("Bearer {}", drive_token))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "fragment-a",
                    "object_key": "videos/IMG_8634.mov",
                    "version_id": "01CLIENTWRONGFINALIZE000000000099",
                    "object_size_bytes": 1,
                    "content_type": "application/octet-stream"
                }))
                .expect("serialize finalize request"),
            ))
            .expect("build finalize request");

        let response = app.oneshot(request).await.expect("finalize response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read finalize body");
        let payload: Value = serde_json::from_slice(&body).expect("parse finalize body");

        assert_eq!(
            payload["object"]["storage_object_id"].as_str(),
            Some(pending.storage_object_id.as_str())
        );
        assert_eq!(
            payload["object"]["version_id"].as_str(),
            Some("01ENGINEFINALIZE0000000000000002")
        );
        assert_eq!(
            payload["object"]["object_size_bytes"].as_i64(),
            Some(157_230_177)
        );
        assert_eq!(
            payload["object"]["content_type"].as_str(),
            Some("video/quicktime")
        );
        assert_eq!(payload["quota_used_bytes"].as_i64(), Some(157_230_177));

        let resolved = state
            .db
            .resolve_object(
                drive_user.uid,
                "fragment-a".to_string(),
                "videos/IMG_8634.mov".to_string(),
            )
            .await
            .expect("resolve finalized object")
            .expect("finalized object exists");
        assert_eq!(resolved.storage_object_id, pending.storage_object_id);
        assert_eq!(resolved.version_id, "01ENGINEFINALIZE0000000000000002");
        assert_eq!(resolved.object_size_bytes, 157_230_177);
        assert_eq!(resolved.content_type.as_deref(), Some("video/quicktime"));
    }

    #[tokio::test]
    async fn finalize_route_can_commit_from_engine_versions_when_head_metadata_lags() {
        let mut objects = HashMap::new();
        objects.insert(
            "videos/IMG_8634.mov".to_string(),
            StubObjectState {
                current_visible: false,
                versions: vec![
                    StubVersion {
                        version_id: "01ENGINEVERSIONSLAG000000000002".to_string(),
                        object_size_bytes: 4_096,
                        content_type: "video/quicktime".to_string(),
                        created_at: 2_000,
                    },
                    StubVersion {
                        version_id: "01ENGINEVERSIONSLAG000000000001".to_string(),
                        object_size_bytes: 4_095,
                        content_type: "video/quicktime".to_string(),
                        created_at: 1_000,
                    },
                ],
            },
        );

        let single_url = spawn_storage_version_stub(objects).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");

        let pending = state
            .db
            .register_pending_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "videos/IMG_8634.mov".to_string(),
                Some(4_096),
                Some("video/quicktime".to_string()),
                Some("01CLIENTVERSIONSLAG000000000001".to_string()),
                Some("session-client-versions-lag".to_string()),
            )
            .await
            .expect("register pending object");

        let app = build_router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/objects/finalize")
            .header("authorization", format!("Bearer {}", drive_token))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "fragment-a",
                    "object_key": "videos/IMG_8634.mov",
                    "version_id": "01CLIENTVERSIONSLAG000000000099",
                    "object_size_bytes": 1,
                    "content_type": "application/octet-stream"
                }))
                .expect("serialize finalize request"),
            ))
            .expect("build finalize request");

        let response = app.oneshot(request).await.expect("finalize response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read finalize body");
        let payload: Value = serde_json::from_slice(&body).expect("parse finalize body");

        assert_eq!(
            payload["object"]["storage_object_id"].as_str(),
            Some(pending.storage_object_id.as_str())
        );
        assert_eq!(
            payload["object"]["version_id"].as_str(),
            Some("01ENGINEVERSIONSLAG000000000002")
        );
        assert_eq!(payload["object"]["object_size_bytes"].as_i64(), Some(4_096));
        assert_eq!(
            payload["object"]["content_type"].as_str(),
            Some("video/quicktime")
        );
    }

    #[tokio::test]
    async fn registry_ws_emits_mutation_categories_for_admin_consumers() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let admin_hash = auth::hash_password("password123").expect("hash admin password");
        let admin_user = state
            .db
            .create_user(
                "admin@example.com".to_string(),
                admin_hash,
                Some(100 * 1024 * 1024),
                true,
            )
            .await
            .expect("create admin user");
        let (admin_token, _) =
            issue_access_token(&admin_user, &state.auth).expect("issue admin token");

        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();
        let mut admin_ws = connect_registry_ws(&base_url, &admin_token).await;
        consume_ws_hello(&mut admin_ws).await;

        let create_resp = client
            .post(format!("{}/v1/admin/users", base_url))
            .bearer_auth(&admin_token)
            .json(&json!({
                "email": "created@example.com",
                "password": "password123",
                "quota_limit_bytes": 123456,
                "is_admin": false
            }))
            .send()
            .await
            .expect("create user request");
        assert_eq!(create_resp.status(), reqwest::StatusCode::CREATED);
        let created_event = wait_for_event_type(&mut admin_ws, "USER_CREATED").await;
        assert_eq!(created_event["data"]["email"], "created@example.com");

        let link_resp = client
            .post(format!("{}/v1/fragments/link", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "linked-fragment",
                "deployment": "single_node"
            }))
            .send()
            .await
            .expect("link fragment request");
        assert_eq!(link_resp.status(), reqwest::StatusCode::CREATED);
        let link_event = wait_for_event_type(&mut admin_ws, "FRAGMENT_LINKED").await;
        assert_eq!(link_event["data"]["fragment"], "linked-fragment");

        let finalize_resp = client
            .post(format!("{}/v1/objects/finalize", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "linked-fragment",
                "object_key": "folder/sample.dcm",
                "version_id": "01HREGWS0000000000000000000002",
                "object_size_bytes": 8192,
                "content_type": "application/dicom"
            }))
            .send()
            .await
            .expect("finalize object request");
        assert_eq!(finalize_resp.status(), reqwest::StatusCode::OK);
        let uploaded_event = wait_for_event_type(&mut admin_ws, "FILE_UPLOADED").await;
        assert_eq!(uploaded_event["data"]["object_key"], "folder/sample.dcm");

        let rename_resp = client
            .post(format!("{}/v1/objects/rename", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "linked-fragment",
                "source_key": "folder/sample.dcm",
                "destination_key": "folder/sample-renamed.dcm"
            }))
            .send()
            .await
            .expect("rename object request");
        assert_eq!(rename_resp.status(), reqwest::StatusCode::OK);
        let renamed_event = wait_for_event_type(&mut admin_ws, "FILE_RENAMED").await;
        assert_eq!(
            renamed_event["data"]["destination_key"],
            "folder/sample-renamed.dcm"
        );

        let trash_resp = client
            .post(format!("{}/v1/objects/trash", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "linked-fragment",
                "object_key": "folder/sample-renamed.dcm"
            }))
            .send()
            .await
            .expect("trash object request");
        assert_eq!(trash_resp.status(), reqwest::StatusCode::OK);
        let deleted_event = wait_for_event_type(&mut admin_ws, "FILE_DELETED").await;
        assert_eq!(
            deleted_event["data"]["object_key"],
            "folder/sample-renamed.dcm"
        );

        let recover_resp = client
            .post(format!("{}/v1/objects/recover", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "linked-fragment",
                "object_key": "folder/sample-renamed.dcm"
            }))
            .send()
            .await
            .expect("recover object request");
        assert_eq!(recover_resp.status(), reqwest::StatusCode::OK);
        let recovered_event = wait_for_event_type(&mut admin_ws, "FILE_UPLOADED").await;
        assert_eq!(
            recovered_event["data"]["object_key"],
            "folder/sample-renamed.dcm"
        );

        let quota_resp = client
            .post(format!(
                "{}/v1/admin/users/{}/quota",
                base_url, drive_user.uid
            ))
            .bearer_auth(&admin_token)
            .json(&json!({
                "quota_limit_bytes": 222222
            }))
            .send()
            .await
            .expect("update quota request");
        assert_eq!(quota_resp.status(), reqwest::StatusCode::OK);
        let user_update_event = wait_for_event_type(&mut admin_ws, "USER_UPDATED").await;
        assert_eq!(
            user_update_event["data"]["uid"].as_str(),
            Some(drive_user.uid.as_str())
        );

        let password_resp = client
            .post(format!("{}/v1/account/password", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "current_password": "password123",
                "new_password": "password456",
                "otp": null
            }))
            .send()
            .await
            .expect("account password request");
        assert_eq!(password_resp.status(), reqwest::StatusCode::OK);
        let account_update_event = wait_for_event_type(&mut admin_ws, "ACCOUNT_UPDATED").await;
        assert_eq!(
            account_update_event["data"]["uid"].as_str(),
            Some(drive_user.uid.as_str())
        );

        let second_trash_resp = client
            .post(format!("{}/v1/objects/trash", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "linked-fragment",
                "object_key": "folder/sample-renamed.dcm"
            }))
            .send()
            .await
            .expect("second trash object request");
        assert_eq!(second_trash_resp.status(), reqwest::StatusCode::OK);
        let _ = wait_for_event_type(&mut admin_ws, "FILE_DELETED").await;

        let recover_all_resp = client
            .post(format!("{}/v1/objects/recover-all", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "linked-fragment"
            }))
            .send()
            .await
            .expect("recover all request");
        assert_eq!(recover_all_resp.status(), reqwest::StatusCode::OK);
        let recover_all_event = wait_for_state_scope(&mut admin_ws, "objects").await;
        assert_eq!(recover_all_event["data"]["scope"], "objects");

        let third_trash_resp = client
            .post(format!("{}/v1/objects/trash", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "linked-fragment",
                "object_key": "folder/sample-renamed.dcm"
            }))
            .send()
            .await
            .expect("third trash object request");
        assert_eq!(third_trash_resp.status(), reqwest::StatusCode::OK);
        let _ = wait_for_event_type(&mut admin_ws, "FILE_DELETED").await;

        let empty_resp = client
            .post(format!("{}/v1/objects/trash/empty", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "linked-fragment"
            }))
            .send()
            .await
            .expect("empty trash request");
        assert_eq!(empty_resp.status(), reqwest::StatusCode::OK);
        let empty_event = wait_for_state_scope(&mut admin_ws, "trash").await;
        assert_eq!(empty_event["data"]["scope"], "trash");
    }

    #[tokio::test]
    async fn registry_ws_fans_out_to_many_concurrent_subscribers() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let base_url = spawn_registry_server(state).await;
        let client = Client::new();

        let mut sockets = Vec::new();
        for _ in 0..48 {
            let mut socket = connect_registry_ws(&base_url, &drive_token).await;
            consume_ws_hello(&mut socket).await;
            sockets.push(socket);
        }

        let response = client
            .post(format!("{}/v1/objects/finalize", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_key": "bulk/concurrency.bin",
                "version_id": "01HREGWS0000000000000000000099",
                "object_size_bytes": 16384,
                "content_type": "application/octet-stream"
            }))
            .send()
            .await
            .expect("fanout finalize request");
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        for socket in &mut sockets {
            let event = wait_for_event_type(socket, "FILE_UPLOADED").await;
            assert_eq!(event["data"]["object_key"], "bulk/concurrency.bin");
        }
    }

    #[tokio::test]
    async fn bulk_registry_object_mutations_emit_single_state_invalidation() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        for (index, key) in ["bulk/a.bin", "bulk/b.bin", "bulk/c.bin"]
            .iter()
            .enumerate()
        {
            state
                .db
                .finalize_object(
                    drive_user.uid.clone(),
                    "fragment-a".to_string(),
                    (*key).to_string(),
                    format!("01HREGBULK{:020}", index),
                    2048 + index as i64,
                    Some("application/octet-stream".to_string()),
                )
                .await
                .expect("finalize seed object");
        }

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();
        let mut drive_ws = connect_registry_ws(&base_url, &drive_token).await;
        consume_ws_hello(&mut drive_ws).await;

        let bulk_trash_resp = client
            .post(format!("{}/v1/objects/trash/bulk", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_keys": [
                    "bulk/a.bin",
                    "/bulk/a.bin",
                    "bulk/b.bin",
                    "bulk/missing.bin"
                ]
            }))
            .send()
            .await
            .expect("bulk trash request");
        assert_eq!(bulk_trash_resp.status(), reqwest::StatusCode::OK);
        let bulk_trash_json: Value = bulk_trash_resp.json().await.expect("parse bulk trash json");
        assert_eq!(bulk_trash_json["requested"].as_u64(), Some(4));
        assert_eq!(bulk_trash_json["unique"].as_u64(), Some(3));
        assert_eq!(bulk_trash_json["affected"].as_u64(), Some(2));
        assert_eq!(bulk_trash_json["not_found"].as_u64(), Some(1));

        let trash_event = wait_for_state_scope(&mut drive_ws, "objects").await;
        assert_eq!(trash_event["data"]["fragment"].as_str(), Some("fragment-a"));
        expect_no_registry_event(&mut drive_ws, Duration::from_millis(350)).await;

        let bulk_recover_resp = client
            .post(format!("{}/v1/objects/recover/bulk", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_keys": [
                    "bulk/a.bin",
                    "bulk/b.bin",
                    "bulk/missing.bin"
                ]
            }))
            .send()
            .await
            .expect("bulk recover request");
        assert_eq!(bulk_recover_resp.status(), reqwest::StatusCode::OK);
        let bulk_recover_json: Value = bulk_recover_resp
            .json()
            .await
            .expect("parse bulk recover json");
        assert_eq!(bulk_recover_json["requested"].as_u64(), Some(3));
        assert_eq!(bulk_recover_json["unique"].as_u64(), Some(3));
        assert_eq!(bulk_recover_json["affected"].as_u64(), Some(2));
        assert_eq!(bulk_recover_json["not_found"].as_u64(), Some(1));

        let recover_event = wait_for_state_scope(&mut drive_ws, "objects").await;
        assert_eq!(
            recover_event["data"]["fragment"].as_str(),
            Some("fragment-a")
        );
        expect_no_registry_event(&mut drive_ws, Duration::from_millis(350)).await;
    }

    #[tokio::test]
    async fn bound_registry_mutations_delegate_to_engine_using_logical_object_keys() {
        let calls = Arc::new(TokioMutex::new(Vec::<String>::new()));
        let single_url = spawn_storage_recording_stub("single-stub-token", calls.clone()).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");

        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/single.bin".to_string(),
                "01HREGMUTATE0000000000000000001".to_string(),
                2048,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect("finalize single");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/bulk.bin".to_string(),
                "01HREGMUTATE0000000000000000002".to_string(),
                3072,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect("finalize bulk");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/delete.bin".to_string(),
                "01HREGMUTATE0000000000000000003".to_string(),
                4096,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect("finalize delete");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/prefix/a.bin".to_string(),
                "01HREGMUTATE0000000000000000004".to_string(),
                1024,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect("finalize prefix a");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/prefix/b.bin".to_string(),
                "01HREGMUTATE0000000000000000005".to_string(),
                1024,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect("finalize prefix b");

        let single_object = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/single.bin".to_string(),
            )
            .await
            .expect("resolve single")
            .expect("single exists");
        let delete_object = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/delete.bin".to_string(),
            )
            .await
            .expect("resolve delete")
            .expect("delete exists");
        let prefix_a = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/prefix/a.bin".to_string(),
            )
            .await
            .expect("resolve prefix a")
            .expect("prefix a exists");
        let prefix_b = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/prefix/b.bin".to_string(),
            )
            .await
            .expect("resolve prefix b")
            .expect("prefix b exists");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let trash_resp = client
            .post(format!("{}/v1/objects/trash", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_key": "logical/single.bin"
            }))
            .send()
            .await
            .expect("trash request");
        assert_eq!(trash_resp.status(), reqwest::StatusCode::OK);

        let recover_resp = client
            .post(format!("{}/v1/objects/recover", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_key": "logical/single.bin"
            }))
            .send()
            .await
            .expect("recover request");
        assert_eq!(recover_resp.status(), reqwest::StatusCode::OK);

        let bulk_trash_resp = client
            .post(format!("{}/v1/objects/trash/bulk", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_keys": ["logical/single.bin", "logical/bulk.bin"]
            }))
            .send()
            .await
            .expect("bulk trash request");
        assert_eq!(bulk_trash_resp.status(), reqwest::StatusCode::OK);

        let recover_all_resp = client
            .post(format!("{}/v1/objects/recover-all", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a"
            }))
            .send()
            .await
            .expect("recover all request");
        assert_eq!(recover_all_resp.status(), reqwest::StatusCode::OK);

        let prefix_trash_resp = client
            .post(format!("{}/v1/objects/trash-prefix", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "prefix": "logical"
            }))
            .send()
            .await
            .expect("prefix trash request");
        assert_eq!(prefix_trash_resp.status(), reqwest::StatusCode::OK);

        let prefix_recover_resp = client
            .post(format!("{}/v1/objects/recover-prefix", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "prefix": "logical"
            }))
            .send()
            .await
            .expect("prefix recover request");
        assert_eq!(prefix_recover_resp.status(), reqwest::StatusCode::OK);

        let delete_resp = client
            .delete(format!(
                "{}/v1/objects?fragment=fragment-a&object_key=logical/delete.bin",
                base_url
            ))
            .bearer_auth(&drive_token)
            .send()
            .await
            .expect("delete request");
        assert_eq!(delete_resp.status(), reqwest::StatusCode::OK);

        let retrash_resp = client
            .post(format!("{}/v1/objects/trash", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_key": "logical/single.bin"
            }))
            .send()
            .await
            .expect("retrash request");
        assert_eq!(retrash_resp.status(), reqwest::StatusCode::OK);

        let empty_resp = client
            .post(format!("{}/v1/objects/trash/empty", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a"
            }))
            .send()
            .await
            .expect("empty trash request");
        assert_eq!(empty_resp.status(), reqwest::StatusCode::OK);

        let prefix_delete_resp = client
            .post(format!("{}/v1/objects/permanent-prefix", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "prefix": "logical/prefix"
            }))
            .send()
            .await
            .expect("prefix delete request");
        assert_eq!(prefix_delete_resp.status(), reqwest::StatusCode::OK);

        let recorded = calls.lock().await.clone();
        assert!(
            recorded.is_empty(),
            "request path should stay registry-local before the purge worker runs: {:?}",
            recorded
        );
        assert!(
            !recorded.iter().any(|entry| {
                entry.starts_with("DELETE /n1/objects/")
                    && !entry.starts_with("DELETE /n1/objects/permanent/")
            }),
            "logical trash should stay in registry: {:?}",
            recorded
        );
        assert!(
            !recorded
                .iter()
                .any(|entry| entry.contains("/recover/version")),
            "logical recover should stay in registry: {:?}",
            recorded
        );
        assert!(
            !recorded.iter().any(|entry| entry.contains("/bulk-delete")),
            "logical bulk trash should stay in registry: {:?}",
            recorded
        );

        let deleted_status = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/delete.bin".to_string(),
            )
            .await
            .expect("resolve delete after purge")
            .expect("delete tombstone exists");
        assert_eq!(deleted_status.status, "purged");
        let single_status = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/single.bin".to_string(),
            )
            .await
            .expect("resolve single after purge")
            .expect("single tombstone exists");
        assert_eq!(single_status.status, "purged");
        let prefix_a_status = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/prefix/a.bin".to_string(),
            )
            .await
            .expect("resolve prefix a after purge")
            .expect("prefix a tombstone exists");
        assert_eq!(prefix_a_status.status, "purged");
        let prefix_b_status = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/prefix/b.bin".to_string(),
            )
            .await
            .expect("resolve prefix b after purge")
            .expect("prefix b tombstone exists");
        assert_eq!(prefix_b_status.status, "purged");

        drain_object_purge_queue(&state).await;

        let recorded = calls.lock().await.clone();
        assert_eq!(recorded.len(), 4);
        assert!(recorded.contains(&format!("DELETE /n1/objects/{}", delete_object.object_key)));
        assert!(recorded.contains(&format!("DELETE /n1/objects/{}", single_object.object_key)));
        assert!(recorded.contains(&format!("DELETE /n1/objects/{}", prefix_a.object_key)));
        assert!(recorded.contains(&format!("DELETE /n1/objects/{}", prefix_b.object_key)));
        assert!(
            !recorded
                .iter()
                .any(|entry| entry.starts_with("DELETE /n1/objects/permanent/")),
            "purge worker should only soft-delete in N1: {:?}",
            recorded
        );
        assert!(
            state
                .db
                .peek_due_object_purge()
                .await
                .expect("peek purge queue after drain")
                .is_none(),
            "purge queue should be empty after draining"
        );
    }

    #[tokio::test]
    async fn purged_object_can_be_recovered_before_storage_lifecycle_removes_it() {
        let mut objects = HashMap::new();
        objects.insert(
            "logical/purged.bin".to_string(),
            StubObjectState {
                current_visible: true,
                versions: vec![StubVersion {
                    version_id: "01HPURGEDRECOVER000000000000001".to_string(),
                    object_size_bytes: 2048,
                    content_type: "application/octet-stream".to_string(),
                    created_at: 100,
                }],
            },
        );

        let single_url = spawn_storage_version_stub(objects).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/purged.bin".to_string(),
                "01HPURGEDRECOVER000000000000001".to_string(),
                2048,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect("finalize object");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let delete_resp = client
            .delete(format!(
                "{}/v1/objects?fragment=fragment-a&object_key=logical/purged.bin",
                base_url
            ))
            .bearer_auth(&drive_token)
            .send()
            .await
            .expect("delete request");
        assert_eq!(delete_resp.status(), reqwest::StatusCode::OK);

        assert!(
            state
                .db
                .resolve_any_object_record(
                    drive_user.uid.clone(),
                    "fragment-a".to_string(),
                    "logical/purged.bin".to_string(),
                )
                .await
                .expect("resolve purged object")
                .is_some_and(|object| object.status == "purged")
        );

        drain_object_purge_queue(&state).await;

        let recover_resp = client
            .post(format!("{}/v1/objects/recover", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_key": "logical/purged.bin"
            }))
            .send()
            .await
            .expect("recover request");
        assert_eq!(recover_resp.status(), reqwest::StatusCode::OK);
        let recover_json: Value = recover_resp.json().await.expect("parse recover response");
        assert_eq!(recover_json["object"]["object_key"], "logical/purged.bin");

        let restored = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "logical/purged.bin".to_string(),
            )
            .await
            .expect("resolve restored object")
            .expect("restored object exists");
        assert_eq!(restored.status, "ready");
        assert!(
            state
                .db
                .peek_due_object_purge()
                .await
                .expect("peek purge queue after recover")
                .is_none(),
            "recover should cancel the pending purge entry"
        );
    }

    #[tokio::test]
    async fn empty_trash_hides_object_immediately_and_leaves_a_purge_tombstone() {
        let single_url = spawn_storage_recording_stub(
            "single-stub-token",
            Arc::new(TokioMutex::new(Vec::new())),
        )
        .await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash/emptyme.txt".to_string(),
                "01HEMPTYTRASH000000000000000001".to_string(),
                1024,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize object");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let trash_resp = client
            .post(format!("{}/v1/objects/trash", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_key": "trash/emptyme.txt"
            }))
            .send()
            .await
            .expect("trash request");
        assert_eq!(trash_resp.status(), reqwest::StatusCode::OK);

        let empty_resp = client
            .post(format!("{}/v1/objects/trash/empty", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a"
            }))
            .send()
            .await
            .expect("empty trash request");
        assert_eq!(empty_resp.status(), reqwest::StatusCode::OK);

        let resolve_resp = client
            .get(format!(
                "{}/v1/objects/resolve?fragment=fragment-a&object_key=trash/emptyme.txt",
                base_url
            ))
            .bearer_auth(&drive_token)
            .send()
            .await
            .expect("resolve purged object");
        assert_eq!(resolve_resp.status(), reqwest::StatusCode::NOT_FOUND);

        let stored = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash/emptyme.txt".to_string(),
            )
            .await
            .expect("resolve tombstone")
            .expect("tombstone exists");
        assert_eq!(stored.status, "purged");
        assert!(
            state
                .db
                .peek_due_object_purge()
                .await
                .expect("peek purge queue")
                .is_some(),
            "empty trash should enqueue a background N1 soft delete"
        );
    }

    #[tokio::test]
    async fn recover_prefix_restores_only_trashed_objects_not_purged_tombstones() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "folder/trashed.txt".to_string(),
                "01HRECOVERPREFIX0000000000000001".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize trashed object");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "folder/purged.txt".to_string(),
                "01HRECOVERPREFIX0000000000000002".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize purged object");
        state
            .db
            .set_object_status(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "folder/trashed.txt".to_string(),
                "trashed",
            )
            .await
            .expect("trash object");
        state
            .db
            .purge_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "folder/purged.txt".to_string(),
            )
            .await
            .expect("purge object");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let recover_resp = client
            .post(format!("{}/v1/objects/recover-prefix", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "prefix": "folder"
            }))
            .send()
            .await
            .expect("recover prefix request");
        assert_eq!(recover_resp.status(), reqwest::StatusCode::OK);
        let recover_json: Value = recover_resp.json().await.expect("parse recover prefix");
        assert_eq!(recover_json["affected"], 1);

        let restored = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "folder/trashed.txt".to_string(),
            )
            .await
            .expect("resolve restored trashed object")
            .expect("restored object exists");
        assert_eq!(restored.status, "ready");

        let purged = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "folder/purged.txt".to_string(),
            )
            .await
            .expect("resolve purged tombstone")
            .expect("purged tombstone exists");
        assert_eq!(purged.status, "purged");
    }

    #[tokio::test]
    async fn recover_bulk_restores_only_trashed_objects_not_purged_tombstones() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "bulk/trashed.txt".to_string(),
                "01HRECOVERBULK00000000000000001".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize trashed object");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "bulk/purged.txt".to_string(),
                "01HRECOVERBULK00000000000000002".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize purged object");
        state
            .db
            .set_object_status(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "bulk/trashed.txt".to_string(),
                "trashed",
            )
            .await
            .expect("trash object");
        state
            .db
            .purge_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "bulk/purged.txt".to_string(),
            )
            .await
            .expect("purge object");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let recover_resp = client
            .post(format!("{}/v1/objects/recover/bulk", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_keys": ["bulk/trashed.txt", "bulk/purged.txt", "bulk/missing.txt"]
            }))
            .send()
            .await
            .expect("recover bulk request");
        assert_eq!(recover_resp.status(), reqwest::StatusCode::OK);
        let recover_json: Value = recover_resp.json().await.expect("parse recover bulk");
        assert_eq!(recover_json["affected"], 1);
        assert_eq!(recover_json["not_found"], 2);

        let restored = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "bulk/trashed.txt".to_string(),
            )
            .await
            .expect("resolve restored trashed object")
            .expect("restored object exists");
        assert_eq!(restored.status, "ready");

        let purged = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "bulk/purged.txt".to_string(),
            )
            .await
            .expect("resolve purged tombstone")
            .expect("purged tombstone exists");
        assert_eq!(purged.status, "purged");
    }

    #[tokio::test]
    async fn trash_single_does_not_reexpose_purged_tombstones() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "single/purged.txt".to_string(),
                "01HTRASHSINGLEPURGED00000000001".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize purged object");
        state
            .db
            .purge_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "single/purged.txt".to_string(),
            )
            .await
            .expect("purge object");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let trash_resp = client
            .post(format!("{}/v1/objects/trash", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_key": "single/purged.txt"
            }))
            .send()
            .await
            .expect("trash single request");
        assert_eq!(trash_resp.status(), reqwest::StatusCode::NOT_FOUND);

        let purged = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "single/purged.txt".to_string(),
            )
            .await
            .expect("resolve purged tombstone")
            .expect("purged tombstone exists");
        assert_eq!(purged.status, "purged");
    }

    #[tokio::test]
    async fn trash_prefix_does_not_reexpose_purged_tombstones() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-prefix/ready.txt".to_string(),
                "01HTRASHPREFIXREADY00000000001".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize ready object");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-prefix/purged.txt".to_string(),
                "01HTRASHPREFIXPURGED0000000001".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize purged object");
        state
            .db
            .purge_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-prefix/purged.txt".to_string(),
            )
            .await
            .expect("purge object");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let trash_resp = client
            .post(format!("{}/v1/objects/trash-prefix", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "prefix": "trash-prefix"
            }))
            .send()
            .await
            .expect("trash prefix request");
        assert_eq!(trash_resp.status(), reqwest::StatusCode::OK);
        let trash_json: Value = trash_resp.json().await.expect("parse trash prefix");
        assert_eq!(trash_json["affected"], 1);

        let ready = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-prefix/ready.txt".to_string(),
            )
            .await
            .expect("resolve trashed ready object")
            .expect("trashed ready object exists");
        assert_eq!(ready.status, "trashed");

        let purged = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-prefix/purged.txt".to_string(),
            )
            .await
            .expect("resolve purged tombstone")
            .expect("purged tombstone exists");
        assert_eq!(purged.status, "purged");
    }

    #[tokio::test]
    async fn trash_bulk_does_not_reexpose_purged_tombstones() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-bulk/ready.txt".to_string(),
                "01HTRASHBULKREADY000000000001".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize ready object");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-bulk/purged.txt".to_string(),
                "01HTRASHBULKPURGED00000000001".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize purged object");
        state
            .db
            .purge_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-bulk/purged.txt".to_string(),
            )
            .await
            .expect("purge object");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let trash_resp = client
            .post(format!("{}/v1/objects/trash/bulk", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "object_keys": [
                    "trash-bulk/ready.txt",
                    "trash-bulk/purged.txt",
                    "trash-bulk/missing.txt"
                ]
            }))
            .send()
            .await
            .expect("trash bulk request");
        assert_eq!(trash_resp.status(), reqwest::StatusCode::OK);
        let trash_json: Value = trash_resp.json().await.expect("parse trash bulk");
        assert_eq!(trash_json["affected"], 1);
        assert_eq!(trash_json["not_found"], 2);

        let ready = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-bulk/ready.txt".to_string(),
            )
            .await
            .expect("resolve trashed ready object")
            .expect("trashed ready object exists");
        assert_eq!(ready.status, "trashed");

        let purged = state
            .db
            .resolve_any_object_record(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "trash-bulk/purged.txt".to_string(),
            )
            .await
            .expect("resolve purged tombstone")
            .expect("purged tombstone exists");
        assert_eq!(purged.status, "purged");
    }

    #[tokio::test]
    async fn prefix_namespace_mutations_stay_registry_local_and_move_logical_paths_only() {
        let calls = Arc::new(TokioMutex::new(Vec::<String>::new()));
        let single_url = spawn_storage_recording_stub("single-stub-token", calls.clone()).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, drive_token) = test_state(single_url, multi_url).await;
        let drive_user = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");

        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "folder/a.txt".to_string(),
                "01HREGPREFIX0000000000000000001".to_string(),
                100,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize folder a");
        state
            .db
            .finalize_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "folder/nested/b.txt".to_string(),
                "01HREGPREFIX0000000000000000002".to_string(),
                200,
                Some("text/plain".to_string()),
            )
            .await
            .expect("finalize folder b");

        let a_before = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "folder/a.txt".to_string(),
            )
            .await
            .expect("resolve a before")
            .expect("a exists");

        let base_url = spawn_registry_server(state.clone()).await;
        let client = Client::new();

        let rename_resp = client
            .post(format!("{}/v1/objects/rename-prefix", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "source_prefix": "folder",
                "destination_prefix": "renamed"
            }))
            .send()
            .await
            .expect("prefix rename request");
        assert_eq!(rename_resp.status(), reqwest::StatusCode::OK);

        let renamed_a = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "renamed/a.txt".to_string(),
            )
            .await
            .expect("resolve renamed a")
            .expect("renamed a exists");
        let renamed_b = state
            .db
            .resolve_object(
                drive_user.uid.clone(),
                "fragment-a".to_string(),
                "renamed/nested/b.txt".to_string(),
            )
            .await
            .expect("resolve renamed b")
            .expect("renamed b exists");
        assert_eq!(renamed_a.storage_object_id, a_before.storage_object_id);
        assert_eq!(renamed_a.status, "ready");
        assert_eq!(renamed_b.status, "ready");

        let trash_resp = client
            .post(format!("{}/v1/objects/trash-prefix", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "prefix": "renamed"
            }))
            .send()
            .await
            .expect("prefix trash request");
        assert_eq!(trash_resp.status(), reqwest::StatusCode::OK);

        let trashed_listing = state
            .db
            .list_objects(
                drive_user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("renamed".to_string()),
                None,
                None,
                100,
                0,
                false,
                true,
            )
            .await
            .expect("list trashed renamed folder");
        assert_eq!(trashed_listing.objects.len(), 2);
        assert!(
            trashed_listing
                .objects
                .iter()
                .all(|object| object.status == "trashed")
        );

        let recover_resp = client
            .post(format!("{}/v1/objects/recover-prefix", base_url))
            .bearer_auth(&drive_token)
            .json(&json!({
                "fragment": "fragment-a",
                "prefix": "renamed"
            }))
            .send()
            .await
            .expect("prefix recover request");
        assert_eq!(recover_resp.status(), reqwest::StatusCode::OK);

        let ready_listing = state
            .db
            .list_objects(
                drive_user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("renamed".to_string()),
                None,
                None,
                100,
                0,
                false,
                false,
            )
            .await
            .expect("list recovered renamed folder");
        assert_eq!(ready_listing.objects.len(), 2);
        assert!(
            ready_listing
                .objects
                .iter()
                .all(|object| object.status == "ready")
        );

        let recorded = calls.lock().await.clone();
        assert!(
            recorded.is_empty(),
            "prefix namespace mutations should stay inside registry: {:?}",
            recorded
        );
    }

    #[tokio::test]
    async fn share_routes_create_proxy_list_and_revoke_public_access() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let owner = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                owner.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("link fragment");
        state
            .db
            .finalize_object(
                owner.uid.clone(),
                "fragment-a".to_string(),
                "docs/report.pdf".to_string(),
                "01HREGSHARE0000000000000000001".to_string(),
                4096,
                Some("application/pdf".to_string()),
            )
            .await
            .expect("finalize shared object");

        let app = build_router(state.clone());
        let create_request = Request::builder()
            .method("POST")
            .uri("/v1/shares")
            .header("authorization", format!("Bearer {}", access_token))
            .header("host", "cloud.example.test")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "fragment-a",
                    "object_key": "docs/report.pdf",
                    "expires_in": 3600,
                    "disposition": "download"
                }))
                .expect("serialize create share request"),
            ))
            .expect("create share request");
        let create_response = app
            .clone()
            .oneshot(create_request)
            .await
            .expect("create share response");
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let create_body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("read create share body");
        let create_json: Value =
            serde_json::from_slice(&create_body).expect("parse create share json");
        let share_id = create_json["share"]["share_id"]
            .as_str()
            .expect("share id")
            .to_string();
        let public_url = create_json["url"].as_str().expect("public share url");
        assert!(public_url.starts_with("https://cloud.example.test/v1/public/shares/"));
        let public_token = public_url
            .rsplit('/')
            .next()
            .expect("share token in url")
            .to_string();

        let list_request = Request::builder()
            .method("GET")
            .uri("/v1/shares?fragment=fragment-a")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("list shares request");
        let list_response = app
            .clone()
            .oneshot(list_request)
            .await
            .expect("list shares response");
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
            .await
            .expect("read list shares body");
        let list_json: Value = serde_json::from_slice(&list_body).expect("parse list shares json");
        let shares = list_json["shares"].as_array().expect("shares array");
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0]["share_id"], share_id);
        assert_eq!(shares[0]["object_key"], "docs/report.pdf");
        assert_eq!(shares[0]["disposition"], "download");

        let public_request = Request::builder()
            .method("GET")
            .uri(format!("/v1/public/shares/{}", public_token))
            .body(Body::empty())
            .expect("public share request");
        let public_response = app
            .clone()
            .oneshot(public_request)
            .await
            .expect("public share response");
        assert_eq!(public_response.status(), StatusCode::OK);
        assert_eq!(
            public_response.headers().get(CONTENT_DISPOSITION),
            Some(&header::HeaderValue::from_static(
                "attachment; filename=\"report.pdf\""
            ))
        );
        let public_body = axum::body::to_bytes(public_response.into_body(), usize::MAX)
            .await
            .expect("read public share body");
        assert!(
            public_body.as_ref().starts_with(b"download:"),
            "unexpected public share body: {:?}",
            public_body
        );

        let revoke_request = Request::builder()
            .method("DELETE")
            .uri(format!("/v1/shares/{}", share_id))
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("revoke share request");
        let revoke_response = app
            .clone()
            .oneshot(revoke_request)
            .await
            .expect("revoke share response");
        assert_eq!(revoke_response.status(), StatusCode::OK);
        let revoke_body = axum::body::to_bytes(revoke_response.into_body(), usize::MAX)
            .await
            .expect("read revoke share body");
        let revoke_json: Value =
            serde_json::from_slice(&revoke_body).expect("parse revoke share json");
        assert_eq!(revoke_json["revoked"].as_bool(), Some(true));

        let after_revoke_request = Request::builder()
            .method("GET")
            .uri(format!("/v1/public/shares/{}", public_token))
            .body(Body::empty())
            .expect("after revoke share request");
        let after_revoke_response = app
            .oneshot(after_revoke_request)
            .await
            .expect("after revoke share response");
        assert_eq!(after_revoke_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn share_public_token_survives_registry_rename_and_uses_new_logical_name() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let owner = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                owner.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("link fragment");
        state
            .db
            .finalize_object(
                owner.uid,
                "fragment-a".to_string(),
                "docs/report.pdf".to_string(),
                "01HSHARERENAMEVERSION0000000001".to_string(),
                1024,
                Some("application/pdf".to_string()),
            )
            .await
            .expect("finalize shared object");

        let app = build_router(state.clone());
        let create_request = Request::builder()
            .method("POST")
            .uri("/v1/shares")
            .header("authorization", format!("Bearer {}", access_token))
            .header("host", "cloud.example.test")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "fragment-a",
                    "object_key": "docs/report.pdf",
                    "expires_in": 3600,
                    "disposition": "download"
                }))
                .expect("serialize create share request"),
            ))
            .expect("create share request");
        let create_response = app
            .clone()
            .oneshot(create_request)
            .await
            .expect("create share response");
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let create_body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("read create share body");
        let create_json: Value =
            serde_json::from_slice(&create_body).expect("parse create share json");
        let public_url = create_json["url"].as_str().expect("public share url");
        let public_token = public_url
            .rsplit('/')
            .next()
            .expect("share token in url")
            .to_string();

        let rename_request = Request::builder()
            .method("POST")
            .uri("/v1/objects/rename")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "fragment-a",
                    "source_key": "docs/report.pdf",
                    "destination_key": "docs/report-renamed.pdf"
                }))
                .expect("serialize rename request"),
            ))
            .expect("rename request");
        let rename_response = app
            .clone()
            .oneshot(rename_request)
            .await
            .expect("rename response");
        assert_eq!(rename_response.status(), StatusCode::OK);

        let list_request = Request::builder()
            .method("GET")
            .uri("/v1/shares?fragment=fragment-a")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("list shares request");
        let list_response = app
            .clone()
            .oneshot(list_request)
            .await
            .expect("list shares response");
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
            .await
            .expect("read list shares body");
        let list_json: Value = serde_json::from_slice(&list_body).expect("parse list shares json");
        let shares = list_json["shares"].as_array().expect("shares array");
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0]["object_key"], "docs/report-renamed.pdf");

        let public_request = Request::builder()
            .method("GET")
            .uri(format!("/v1/public/shares/{}", public_token))
            .body(Body::empty())
            .expect("public share request");
        let public_response = app
            .clone()
            .oneshot(public_request)
            .await
            .expect("public share response");
        assert_eq!(public_response.status(), StatusCode::OK);
        assert_eq!(
            public_response.headers().get(CONTENT_DISPOSITION),
            Some(&header::HeaderValue::from_static(
                "attachment; filename=\"report-renamed.pdf\""
            ))
        );
        let public_body = axum::body::to_bytes(public_response.into_body(), usize::MAX)
            .await
            .expect("read public share body");
        assert!(
            public_body.as_ref().starts_with(b"download:"),
            "unexpected public share body after rename: {:?}",
            public_body
        );
    }

    #[tokio::test]
    async fn archive_route_selects_prefix_via_registry_and_streams_engine_tar() {
        let single_url = spawn_storage_stub("single-stub-token").await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let owner = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                owner.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("link fragment");
        state
            .db
            .finalize_object(
                owner.uid.clone(),
                "fragment-a".to_string(),
                "patients/a/one.dcm".to_string(),
                "01HARCHIVEVERSION000000000000001".to_string(),
                1024,
                Some("application/dicom".to_string()),
            )
            .await
            .expect("finalize one");
        state
            .db
            .finalize_object(
                owner.uid.clone(),
                "fragment-a".to_string(),
                "patients/a/two.dcm".to_string(),
                "01HARCHIVEVERSION000000000000002".to_string(),
                1024,
                Some("application/dicom".to_string()),
            )
            .await
            .expect("finalize two");
        state
            .db
            .finalize_object(
                owner.uid,
                "fragment-a".to_string(),
                "patients/b/three.dcm".to_string(),
                "01HARCHIVEVERSION000000000000003".to_string(),
                1024,
                Some("application/dicom".to_string()),
            )
            .await
            .expect("finalize three");

        let app = build_router(state);
        let request = Request::builder()
            .method("POST")
            .uri("/v1/objects/archive")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "fragment-a",
                    "prefix": "patients/a",
                    "filename": "selected"
                }))
                .expect("serialize archive request"),
            ))
            .expect("archive request");
        let response = app.oneshot(request).await.expect("archive response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE),
            Some(&"application/x-tar".parse().expect("content type"))
        );
        assert_eq!(
            response.headers().get(CONTENT_DISPOSITION),
            Some(
                &"attachment; filename=\"selected.tar\""
                    .parse()
                    .expect("content disposition")
            )
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read archive body");
        let reader = tokio_util::io::StreamReader::new(futures_util::stream::iter(vec![Ok::<
            bytes::Bytes,
            std::io::Error,
        >(
            bytes::Bytes::from(body.to_vec()),
        )]));
        let mut archive = tokio_tar::Archive::new(reader);
        let mut entries = archive.entries().expect("archive entries");
        let mut paths = Vec::new();
        let mut contents = Vec::new();
        use tokio::io::AsyncReadExt;
        while let Some(entry) = entries.next().await {
            let mut entry = entry.expect("archive entry");
            let path = entry
                .path()
                .expect("entry path")
                .to_string_lossy()
                .to_string();
            let mut data = Vec::new();
            entry
                .read_to_end(&mut data)
                .await
                .expect("read archive entry data");
            paths.push(path);
            contents.push(String::from_utf8(data).expect("entry utf8"));
        }
        assert_eq!(
            paths,
            vec![
                "patients/a/one.dcm".to_string(),
                "patients/a/two.dcm".to_string()
            ]
        );
        assert_eq!(contents.len(), 2);
        assert!(contents[0].starts_with("download:"));
        assert!(contents[1].starts_with("download:"));
        assert!(!contents[0].contains("patients/a/one.dcm"));
        assert!(!contents[1].contains("patients/a/two.dcm"));
    }

    #[tokio::test]
    async fn deleting_current_object_version_removes_registry_ghost_head() {
        let mut objects = HashMap::new();
        objects.insert(
            "mhw.mp4".to_string(),
            StubObjectState {
                current_visible: true,
                versions: vec![
                    StubVersion {
                        version_id: "01MHW0000000000000000000002".to_string(),
                        object_size_bytes: 2048,
                        content_type: "video/mp4".to_string(),
                        created_at: 200,
                    },
                    StubVersion {
                        version_id: "01MHW0000000000000000000001".to_string(),
                        object_size_bytes: 1024,
                        content_type: "video/mp4".to_string(),
                        created_at: 100,
                    },
                ],
            },
        );

        let single_url = spawn_storage_version_stub(objects).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let owner = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                owner.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                owner.uid.clone(),
                "fragment-a".to_string(),
                "mhw.mp4".to_string(),
                "01MHW0000000000000000000001".to_string(),
                1024,
                Some("video/mp4".to_string()),
            )
            .await
            .expect("finalize version one");
        state
            .db
            .finalize_object(
                owner.uid,
                "fragment-a".to_string(),
                "mhw.mp4".to_string(),
                "01MHW0000000000000000000002".to_string(),
                2048,
                Some("video/mp4".to_string()),
            )
            .await
            .expect("finalize version two");

        let app = build_router(state);
        let delete_req = Request::builder()
            .method("POST")
            .uri("/v1/objects/version/delete")
            .header("authorization", format!("Bearer {}", access_token))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "fragment": "fragment-a",
                    "object_key": "mhw.mp4",
                    "version_id": "01MHW0000000000000000000002"
                }))
                .expect("serialize version delete request"),
            ))
            .expect("version delete request");
        let delete_resp = app
            .clone()
            .oneshot(delete_req)
            .await
            .expect("version delete response");
        assert_eq!(delete_resp.status(), StatusCode::OK);
        let delete_body = axum::body::to_bytes(delete_resp.into_body(), usize::MAX)
            .await
            .expect("read version delete body");
        let delete_json: Value =
            serde_json::from_slice(&delete_body).expect("parse version delete body");
        assert_eq!(delete_json["removed"], true);
        assert_eq!(delete_json["object_removed"], true);
        assert!(delete_json["object"].is_null());

        let resolve_req = Request::builder()
            .method("GET")
            .uri("/v1/objects/resolve?fragment=fragment-a&object_key=mhw.mp4")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("resolve request");
        let resolve_resp = app
            .clone()
            .oneshot(resolve_req)
            .await
            .expect("resolve response");
        assert_eq!(resolve_resp.status(), StatusCode::NOT_FOUND);

        let list_req = Request::builder()
            .method("GET")
            .uri("/v1/objects?fragment=fragment-a")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("list request");
        let list_resp = app.oneshot(list_req).await.expect("list response");
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
            .await
            .expect("read list body");
        let list_json: Value = serde_json::from_slice(&list_body).expect("parse list body");
        assert_eq!(
            list_json["objects"]
                .as_array()
                .expect("objects array")
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn resolve_object_preserves_registry_row_when_engine_self_check_is_empty() {
        let single_url = spawn_storage_version_stub(HashMap::new()).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let owner = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                owner.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");
        state
            .db
            .finalize_object(
                owner.uid,
                "fragment-a".to_string(),
                "ghost.mp4".to_string(),
                "01GHOST0000000000000000000001".to_string(),
                4096,
                Some("video/mp4".to_string()),
            )
            .await
            .expect("seed ghost object");

        let app = build_router(state);
        let resolve_req = Request::builder()
            .method("GET")
            .uri("/v1/objects/resolve?fragment=fragment-a&object_key=ghost.mp4")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("resolve request");
        let resolve_resp = app
            .clone()
            .oneshot(resolve_req)
            .await
            .expect("resolve response");
        assert_eq!(resolve_resp.status(), StatusCode::OK);
        let resolve_body = axum::body::to_bytes(resolve_resp.into_body(), usize::MAX)
            .await
            .expect("read resolve body");
        let resolve_json: Value =
            serde_json::from_slice(&resolve_body).expect("parse resolve body");
        assert_eq!(resolve_json["object_key"].as_str(), Some("ghost.mp4"));
        assert!(resolve_json["storage_object_id"].as_str().is_some());

        let list_req = Request::builder()
            .method("GET")
            .uri("/v1/objects?fragment=fragment-a")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("list request");
        let list_resp = app.oneshot(list_req).await.expect("list response");
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
            .await
            .expect("read list body");
        let list_json: Value = serde_json::from_slice(&list_body).expect("parse list body");
        assert_eq!(
            list_json["objects"]
                .as_array()
                .expect("objects array")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn resolve_object_recovers_when_engine_head_metadata_lags_after_finalize() {
        let mut objects = HashMap::new();
        objects.insert(
            "mhw.mp4".to_string(),
            StubObjectState {
                current_visible: false,
                versions: vec![
                    StubVersion {
                        version_id: "01RESOLVEHEADLAG00000000000002".to_string(),
                        object_size_bytes: 15_912_736_534,
                        content_type: "video/mp4".to_string(),
                        created_at: 2_000,
                    },
                    StubVersion {
                        version_id: "01RESOLVEHEADLAG00000000000001".to_string(),
                        object_size_bytes: 15_912_736_000,
                        content_type: "video/mp4".to_string(),
                        created_at: 1_000,
                    },
                ],
            },
        );

        let single_url = spawn_storage_version_stub(objects).await;
        let multi_url = spawn_storage_stub("multi-stub-token").await;
        let (state, access_token) = test_state(single_url, multi_url).await;
        let owner = state
            .db
            .find_user_auth_by_email("drive@example.com".to_string())
            .await
            .expect("find drive user")
            .expect("drive user exists")
            .user;

        state
            .db
            .upsert_fragment_binding(
                owner.uid.clone(),
                "fragment-a".to_string(),
                crate::models::StorageDeployment::SingleNode,
                None,
            )
            .await
            .expect("bind fragment");

        let (seeded, _user) = state
            .db
            .sync_object_from_engine(
                owner.uid.clone(),
                "fragment-a".to_string(),
                "mhw.mp4".to_string(),
                "flat-engine-key".to_string(),
                Some(crate::storage_broker::EngineObjectMetadata {
                    version_id: "01STALEHEADCLIENTVERSION000000001".to_string(),
                    object_size_bytes: 1,
                    content_type: Some("application/octet-stream".to_string()),
                    created_at_unix: 500,
                }),
                vec![crate::storage_broker::EngineObjectMetadata {
                    version_id: "01STALEHEADCLIENTVERSION000000001".to_string(),
                    object_size_bytes: 1,
                    content_type: Some("application/octet-stream".to_string()),
                    created_at_unix: 500,
                }],
            )
            .await
            .expect("seed object from engine");
        assert!(seeded.is_some(), "seeded object should exist");

        let app = build_router(state.clone());
        let resolve_req = Request::builder()
            .method("GET")
            .uri("/v1/objects/resolve?fragment=fragment-a&object_key=mhw.mp4")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("resolve request");
        let resolve_resp = app
            .clone()
            .oneshot(resolve_req)
            .await
            .expect("resolve response");
        assert_eq!(resolve_resp.status(), StatusCode::OK);
        let resolve_body = axum::body::to_bytes(resolve_resp.into_body(), usize::MAX)
            .await
            .expect("read resolve body");
        let resolve_json: Value =
            serde_json::from_slice(&resolve_body).expect("parse resolve body");
        assert_eq!(
            resolve_json["storage_object_id"].as_str(),
            Some("flat-engine-key")
        );
        assert_eq!(
            resolve_json["version_id"].as_str(),
            Some("01RESOLVEHEADLAG00000000000002")
        );
        assert_eq!(
            resolve_json["object_size_bytes"].as_i64(),
            Some(15_912_736_534)
        );
        assert_eq!(resolve_json["content_type"].as_str(), Some("video/mp4"));

        let list_req = Request::builder()
            .method("GET")
            .uri("/v1/objects?fragment=fragment-a")
            .header("authorization", format!("Bearer {}", access_token))
            .body(Body::empty())
            .expect("list request");
        let list_resp = app.oneshot(list_req).await.expect("list response");
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
            .await
            .expect("read list body");
        let list_json: Value = serde_json::from_slice(&list_body).expect("parse list body");
        let objects = list_json["objects"].as_array().expect("objects array");
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0]["object_key"].as_str(), Some("mhw.mp4"));
        assert_eq!(
            objects[0]["version_id"].as_str(),
            Some("01RESOLVEHEADLAG00000000000002")
        );
    }
}
