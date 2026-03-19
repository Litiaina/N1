mod auth;
mod config;
mod create_env;
mod db;
mod error;
mod models;
mod storage_broker;

use std::fs::{self, OpenOptions};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use auth::{
    AuthSettings, claims_from_headers, issue_access_token, require_master_key, verify_password,
};
use axum::extract::{Query, State};
use axum::http::HeaderName;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use axum_server::tls_rustls::RustlsConfig;
use config::{RegistryConfig, normalize_backend_url};
use db::RegistryDb;
use error::RegistryError;
use models::{
    BulkObjectMutationResponse, CreateUserRequest, CreateUserResponse, DeleteObjectQuery,
    FinalizeObjectRequest, FragmentListResponse, FragmentStateRequest, LinkFragmentRequest,
    LinkFragmentResponse, ListObjectsQuery, ListObjectsResponse, LoginRequest, LoginResponse,
    ObjectDeleteResponse, ObjectMutationResponse, ObjectStateRequest, RegisterObjectRequest,
    RenameRegistryObjectRequest, ResolveObjectQuery, SignupRequest, SignupResponse,
    StorageSessionRequest, StorageSessionResponse, SuggestObjectsQuery, SuggestObjectsResponse,
    UserRecord,
};
use std::time::Duration;
use storage_broker::StorageBroker;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
struct AppState {
    db: RegistryDb,
    auth: Arc<AuthSettings>,
    broker: StorageBroker,
    write_gate: Arc<Mutex<()>>,
}

fn default_fragment_name(user: &UserRecord) -> String {
    format!("registry-user-{}", user.uid)
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
        }),
        broker: StorageBroker::new(Arc::new(config.clone()))
            .map_err(|err| format!("failed to initialize storage broker: {}", err.message))?,
        write_gate: Arc::new(Mutex::new(())),
    };

    let app = build_router(state);
    let addr: SocketAddr = format!("{}:{}", config.host, config.https_port)
        .parse()
        .map_err(|err| format!("invalid registry bind address: {}", err))?;
    let tls = RustlsConfig::from_pem_file(config.cert_path, config.key_path)
        .await
        .map_err(|err| format!("failed to load registry TLS config: {}", err))?;

    info!("registry runtime listening on {}", addr);
    axum_server::bind_rustls(addr, tls)
        .serve(app.into_make_service())
        .await
        .map_err(|err| format!("registry server failed: {}", err))
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/admin/users", post(create_user))
        .route("/v1/auth/signup", post(signup))
        .route("/v1/auth/login", post(login))
        .route("/v1/me", get(me))
        .route("/v1/fragments", get(list_fragments))
        .route("/v1/fragments/link", post(link_fragment))
        .route("/v1/storage/session", post(issue_storage_session))
        .route("/v1/objects/register", post(register_object))
        .route("/v1/objects/finalize", post(finalize_object))
        .route("/v1/objects/suggest", get(suggest_objects))
        .route("/v1/objects/rename", post(rename_object))
        .route("/v1/objects/trash", post(trash_object))
        .route("/v1/objects/recover", post(recover_object))
        .route("/v1/objects/recover-all", post(recover_all_objects))
        .route("/v1/objects/trash/empty", post(empty_trash))
        .route("/v1/objects", delete(delete_object))
        .route("/v1/objects", get(list_objects))
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

async fn health(
    State(state): State<AppState>,
) -> Result<Json<models::HealthResponse>, RegistryError> {
    Ok(Json(state.db.health().await?))
}

async fn create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<CreateUserResponse>), RegistryError> {
    require_master_key(&headers, &state.auth)?;
    let _write_guard = state.write_gate.lock().await;
    if req.email.trim().is_empty() {
        return Err(RegistryError::bad_request("email is required"));
    }
    if req.password.len() < 8 {
        return Err(RegistryError::bad_request(
            "password must be at least 8 characters",
        ));
    }
    let password_hash = auth::hash_password(&req.password)?;
    let user = state
        .db
        .create_user(
            req.email.trim().to_lowercase(),
            password_hash,
            req.quota_limit_bytes,
        )
        .await?;
    let _ = ensure_user_default_fragment(&state, &user).await?;
    Ok((StatusCode::CREATED, Json(CreateUserResponse { user })))
}

async fn signup(
    State(state): State<AppState>,
    Json(req): Json<SignupRequest>,
) -> Result<(StatusCode, Json<SignupResponse>), RegistryError> {
    let _write_guard = state.write_gate.lock().await;
    if req.email.trim().is_empty() {
        return Err(RegistryError::bad_request("email is required"));
    }
    if req.password.len() < 8 {
        return Err(RegistryError::bad_request(
            "password must be at least 8 characters",
        ));
    }
    let password_hash = auth::hash_password(&req.password)?;
    let user = state
        .db
        .create_user(
            req.email.trim().to_lowercase(),
            password_hash,
            req.quota_limit_bytes,
        )
        .await?;
    let _ = ensure_user_default_fragment(&state, &user).await?;
    let (access_token, expires_in) = issue_access_token(&user, &state.auth)?;
    Ok((
        StatusCode::CREATED,
        Json(SignupResponse {
            access_token,
            token_type: "Bearer",
            expires_in,
            user,
        }),
    ))
}

async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, RegistryError> {
    let _write_guard = state.write_gate.lock().await;
    let auth_user = state
        .db
        .find_user_auth_by_email(req.email.trim().to_lowercase())
        .await?
        .ok_or_else(|| RegistryError::unauthorized("invalid credentials"))?;
    verify_password(&req.password, &auth_user.password_hash)?;
    let _ = ensure_user_default_fragment(&state, &auth_user.user).await?;
    let (access_token, expires_in) = issue_access_token(&auth_user.user, &state.auth)?;
    Ok(Json(LoginResponse {
        access_token,
        token_type: "Bearer",
        expires_in,
        user: auth_user.user,
    }))
}

async fn me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<UserRecord>, RegistryError> {
    let claims = claims_from_headers(&headers, &state.auth)?;
    let user = state.db.get_user(claims.sub).await?;
    Ok(Json(user))
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

async fn register_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RegisterObjectRequest>,
) -> Result<(StatusCode, Json<ObjectMutationResponse>), RegistryError> {
    validate_fragment_and_key(&req.fragment, &req.object_key)?;
    let _write_guard = state.write_gate.lock().await;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let object = state
        .db
        .register_pending_object(
            claims.sub.clone(),
            req.fragment,
            req.object_key,
            req.expected_size_bytes,
            req.content_type,
        )
        .await?;
    let user = state.db.get_user(claims.sub).await?;
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
    let _write_guard = state.write_gate.lock().await;
    if req.version_id.trim().is_empty() {
        return Err(RegistryError::bad_request("version_id is required"));
    }
    if req.object_size_bytes < 0 {
        return Err(RegistryError::bad_request(
            "object_size_bytes must be non-negative",
        ));
    }
    let claims = claims_from_headers(&headers, &state.auth)?;
    let (object, user) = state
        .db
        .finalize_object(
            claims.sub,
            req.fragment,
            req.object_key,
            req.version_id,
            req.object_size_bytes,
            req.content_type,
        )
        .await?;
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
    let page = state
        .db
        .list_objects(
            claims.sub,
            query.fragment,
            query.prefix,
            query.delimiter,
            query.search,
            limit,
            offset,
            query.include_pending.unwrap_or(false),
            query.include_trashed.unwrap_or(false),
        )
        .await?;
    Ok(Json(ListObjectsResponse {
        objects: page.objects,
        common_prefixes: page.common_prefixes,
        limit,
        offset,
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

async fn rename_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RenameRegistryObjectRequest>,
) -> Result<Json<ObjectMutationResponse>, RegistryError> {
    if req.fragment.trim().is_empty() {
        return Err(RegistryError::bad_request("fragment is required"));
    }
    let _write_guard = state.write_gate.lock().await;
    if req.source_key.trim().is_empty() || req.destination_key.trim().is_empty() {
        return Err(RegistryError::bad_request(
            "source_key and destination_key are required",
        ));
    }
    let claims = claims_from_headers(&headers, &state.auth)?;
    let (object, user) = state
        .db
        .rename_object(
            claims.sub,
            req.fragment,
            req.source_key.trim().to_string(),
            req.destination_key.trim().to_string(),
        )
        .await?;
    Ok(Json(ObjectMutationResponse {
        object,
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
    let _write_guard = state.write_gate.lock().await;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let (object, user) = state
        .db
        .set_object_status(claims.sub, req.fragment, req.object_key, "trashed")
        .await?;
    Ok(Json(ObjectMutationResponse {
        object,
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
    let _write_guard = state.write_gate.lock().await;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let (object, user) = state
        .db
        .set_object_status(claims.sub, req.fragment, req.object_key, "ready")
        .await?;
    Ok(Json(ObjectMutationResponse {
        object,
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
    let _write_guard = state.write_gate.lock().await;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let user = state
        .db
        .delete_object(claims.sub, query.fragment, query.object_key)
        .await?;
    Ok(Json(ObjectDeleteResponse {
        removed: true,
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
    let _write_guard = state.write_gate.lock().await;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let (affected, user) = state
        .db
        .recover_all_trashed(claims.sub, req.fragment)
        .await?;
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
    let _write_guard = state.write_gate.lock().await;
    let claims = claims_from_headers(&headers, &state.auth)?;
    let (affected, user) = state.db.empty_trash(claims.sub, req.fragment).await?;
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
        .resolve_object(claims.sub, query.fragment, query.object_key)
        .await?
        .ok_or_else(|| RegistryError::not_found("object not found"))?;
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
    use axum::body::Body;
    use axum::http::header::{
        ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
        ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, ORIGIN,
    };
    use axum::http::{Request, StatusCode};
    use axum::routing::post;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    async fn spawn_storage_stub(token: &'static str) -> String {
        let app = Router::new()
            .route(
                "/authenticate/fragment",
                post(move |Json(_payload): Json<Value>| async move {
                    Json(json!({
                        "access_token": token,
                        "token_type": "Bearer",
                        "expires_in": 600
                    }))
                }),
            )
            .route(
                "/v4/fragment/create",
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
        });
        let state = AppState {
            db,
            auth: auth.clone(),
            broker: StorageBroker::new(config).expect("build broker"),
            write_gate: Arc::new(Mutex::new(())),
        };
        let (token, _) = issue_access_token(&user, &auth).expect("issue access token");
        (state, token)
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
}
