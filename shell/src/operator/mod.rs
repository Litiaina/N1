//! Console's original dedicated operator contract, kept outside both data
//! services. This host coordinates the tested LINK backup library/CLI/native
//! operations; it owns no namespace, shard format or placement algorithm.
mod backend;
pub(crate) mod config;
#[cfg(test)]
mod tests;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use cli_core::Result;
use config::Config;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Semaphore;

#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    operation: String,
    #[serde(default)]
    repository: String,
    #[serde(default)]
    snapshot: String,
    #[serde(default)]
    target: String,
    #[serde(default)]
    job: String,
    #[serde(default)]
    confirm: bool,
    #[serde(default)]
    resume: bool,
    #[serde(default)]
    keep_last: usize,
    #[serde(default)]
    cursor: String,
    #[serde(default)]
    limit: usize,
}
#[derive(Clone, Serialize)]
struct Job {
    id: String,
    operation: String,
    state: &'static str,
    result: Option<Value>,
    error: Option<String>,
}
struct Host {
    config: Config,
    secret: String,
    jobs: Mutex<VecDeque<Job>>,
    busy: Arc<Semaphore>,
    readers: Arc<Semaphore>,
    closing: AtomicBool,
}
impl Host {
    fn open(config: Config) -> Result<Arc<Self>> {
        let secret = config::token(&config.token_file)?;
        Ok(Arc::new(Self {
            config,
            secret,
            jobs: Mutex::default(),
            busy: Arc::new(Semaphore::new(1)),
            readers: Arc::new(Semaphore::new(4)),
            closing: AtomicBool::new(false),
        }))
    }
    fn authenticate(&self, headers: &HeaderMap) -> std::result::Result<(), StatusCode> {
        // Same exact-origin and fixed-length digest comparison as the retained
        // original host. Fragment login tokens never grant operator access.
        if let Some(origin) = headers.get("origin")
            && !origin.to_str().ok().is_some_and(|origin| {
                self.config
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed == origin)
            })
        {
            return Err(StatusCode::FORBIDDEN);
        }
        let supplied = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or("");
        let a = Sha256::digest(supplied.as_bytes());
        let b = Sha256::digest(self.secret.as_bytes());
        if a.iter().zip(b.iter()).fold(0u8, |v, (a, b)| v | (a ^ b)) == 0 {
            Ok(())
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }
    async fn dispatch(self: &Arc<Self>, request: Request) -> Result<Value> {
        backend::validate(&self.config, &request)?;
        match request.operation.as_str() {
            "capabilities" => {
                return Ok(
                    json!({"protocol":"LINK-backup-operator-v1","repositories":self.config.repositories.keys().collect::<Vec<_>>(),"replicas":self.config.replicas.keys().collect::<Vec<_>>(),"restore_targets":self.config.restore_targets.keys().collect::<Vec<_>>(),"jobs":self.config.jobs.keys().collect::<Vec<_>>(),"capture_requires_stopped_engine":true,"online_committed_capture":self.config.online_source.is_some(),"capture_boundary_seconds":self.config.capture_boundary_seconds}),
                );
            }
            "operations" => {
                return Ok(
                    json!({"operations":self.jobs.lock().map_err(|_|"operator job state poisoned")?.iter().cloned().collect::<Vec<_>>()}),
                );
            }
            _ => (),
        }
        if self.closing.load(Ordering::Acquire) {
            return Err("operator host is draining".into());
        }
        if matches!(request.operation.as_str(), "list" | "job-status") {
            let permit = self
                .readers
                .clone()
                .try_acquire_owned()
                .map_err(|_| "operator inventory admission is full")?;
            let host = self.clone();
            // A cancelled HTTP caller cannot drop admission while its child
            // or repository read is still alive. Shutdown also drains readers.
            return tokio::spawn(async move {
                let _permit = permit;
                backend::execute(&host.config, &request).await
            })
            .await?;
        }
        let permit = self
            .busy
            .clone()
            .try_acquire_owned()
            .map_err(|_| "another operator job is running")?;
        let id = ulid::Ulid::new().to_string();
        {
            let mut jobs = self
                .jobs
                .lock()
                .map_err(|_| "operator job state poisoned")?;
            if jobs.len() >= 32 {
                jobs.pop_front();
            }
            jobs.push_back(Job {
                id: id.clone(),
                operation: request.operation.clone(),
                state: "running",
                result: None,
                error: None,
            });
        }
        let host = self.clone();
        let job_id = id.clone();
        tokio::spawn(async move {
            let config = host.config.clone();
            let outcome =
                tokio::spawn(async move { backend::execute(&config, &request).await }).await;
            if let Ok(mut jobs) = host.jobs.lock()
                && let Some(job) = jobs.iter_mut().find(|job| job.id == job_id)
            {
                match outcome {
                    Ok(Ok(result)) => {
                        job.state = "complete";
                        job.result = Some(result);
                    }
                    Ok(Err(error)) => {
                        job.state = "failed";
                        job.error = Some(error.to_string().chars().take(4096).collect());
                    }
                    Err(error) => {
                        job.state = "incomplete";
                        job.error = Some(format!(
                            "operator worker ended without a confirmed outcome: {error}"
                        ));
                    }
                }
            }
            drop(permit);
        });
        Ok(json!({"operation_id":id,"state":"running"}))
    }
    async fn drain(&self) {
        self.closing.store(true, Ordering::Release);
        let _job = self.busy.acquire().await;
        let _reads = self.readers.acquire_many(4).await;
        self.busy.close();
        self.readers.close();
    }
}
async fn operation(
    State(host): State<Arc<Host>>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> (StatusCode, Json<Value>) {
    if let Err(status) = host.authenticate(&headers) {
        return (
            status,
            Json(json!({"error":"operator authentication/origin rejected"})),
        );
    }
    match host.dispatch(request).await {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":error.to_string()})),
        ),
    }
}
fn router(host: Arc<Host>) -> Result<Router> {
    let origins = host
        .config
        .allowed_origins
        .iter()
        .map(|origin| origin.parse())
        .collect::<std::result::Result<Vec<axum::http::HeaderValue>, _>>()?;
    Ok(Router::new()
        .route("/noa/v1/maintenance/backups/operation", post(operation))
        .layer(DefaultBodyLimit::max(65536))
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(origins)
                .allow_methods([axum::http::Method::POST])
                .allow_headers([
                    axum::http::header::AUTHORIZATION,
                    axum::http::header::CONTENT_TYPE,
                ]),
        )
        .with_state(host))
}
pub(crate) async fn serve(path: &Path) -> Result<()> {
    let config = Config::load(path)?;
    let bind = config.bind;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => {
            config::private_file(key, 1024 * 1024)?;
            Some(axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?)
        }
        _ => None,
    };
    let host = Host::open(config)?;
    let app = router(host.clone())?;
    let handle = axum_server::Handle::new();
    let graceful = handle.clone();
    let mut server = tokio::spawn(async move {
        match tls {
            Some(tls) => {
                axum_server::bind_rustls(bind, tls)
                    .handle(handle)
                    .serve(app.into_make_service())
                    .await
            }
            None => {
                axum_server::bind(bind)
                    .handle(handle)
                    .serve(app.into_make_service())
                    .await
            }
        }
    });
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let stopped = tokio::select! {result=&mut server=>Some(result),_=tokio::signal::ctrl_c()=>None,_=terminate.recv()=>None};
    host.closing.store(true, Ordering::Release);
    graceful.graceful_shutdown(Some(std::time::Duration::from_secs(30)));
    // A listener error must not bypass accepted work. Jobs retain their
    // admission and are drained before propagating either listener outcome.
    let result = match stopped {
        Some(result) => result,
        None => server.await,
    };
    host.drain().await;
    result??;
    Ok(())
}
