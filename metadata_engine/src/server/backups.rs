//! Operator-only logical database snapshots. One explicit, low-priority worker;
//! no backup or scan is started by a health request.
use super::{
    App,
    error::{ApiError, ApiResult},
};
use crate::middleware::auth;
use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use serde_json::{Value, json};
use std::{
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

pub(crate) struct Backups {
    pub(super) state: Mutex<Value>,
    admission: Arc<Semaphore>,
}
impl Default for Backups {
    fn default() -> Self {
        Self {
            state: Mutex::new(json!({"phase":"idle"})),
            admission: Arc::new(Semaphore::new(1)),
        }
    }
}
impl Backups {
    pub(super) async fn drain(&self) {
        let _permit = self.admission.acquire().await;
        self.admission.close();
    }
}
fn directory(app: &App) -> ApiResult<PathBuf> {
    app.config.backup_dir.clone().ok_or_else(|| {
        ApiError::invalid("configure [metadata_engine] backup_dir on the metadata host")
    })
}
fn identity(id: &str) -> ApiResult<()> {
    if ulid::Ulid::from_string(id).is_ok_and(|v| v.to_string() == id) {
        Ok(())
    } else {
        Err(ApiError::invalid("invalid metadata snapshot ID"))
    }
}
pub(crate) async fn list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    claims.require("operator")?;
    let dir = directory(&app)?;
    crate::uploads::private_directory(&dir).await?;
    let snapshots=tokio::task::spawn_blocking(move ||->std::io::Result<Vec<Value>> {
        let mut snapshots=Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry=entry?;
            if snapshots.len()>=1000 {break;}
            let name=entry.file_name();let Some(id)=name.to_str().and_then(|s|s.strip_suffix(".n1meta")) else{continue;};
            if identity(id).is_err(){continue;}
            let stat=entry.metadata()?;
            if stat.is_file()&&!entry.file_type()?.is_symlink() {snapshots.push(json!({"id":id,"bytes":stat.len(),"created_at":stat.modified().ok().and_then(|t|t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d|d.as_secs())}));}
        }
        snapshots.sort_by(|a,b|b["id"].as_str().cmp(&a["id"].as_str()));
        Ok(snapshots)
    }).await.map_err(|_|ApiError::invalid("metadata snapshot listing worker failed"))??;
    Ok(Json(
        json!({"component":"metadata_engine","snapshots":snapshots,"job":app.backups.state.lock().unwrap_or_else(|e|e.into_inner()).clone(),"payloads_included":false,"download_requires_operator":true}),
    ))
}
pub(crate) async fn create(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let claims = auth::authorize(&app, &headers, "maintenance").await?;
    claims.require("operator")?;
    let dir = directory(&app)?;
    crate::uploads::private_directory(&dir).await?;
    let permit = app
        .backups
        .admission
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError(
                StatusCode::CONFLICT,
                "a metadata backup is already running".into(),
            )
        })?;
    let id = ulid::Ulid::new().to_string();
    *app.backups.state.lock().unwrap_or_else(|e| e.into_inner()) =
        json!({"id":id,"phase":"running","started_at":auth::now()});
    let worker_id = id.clone();
    let db = app.db.observation_handle();
    let status = app.backups.clone();
    std::thread::Builder::new().name("metadata-backup".into()).spawn(move || {
        let _permit=permit;
        // This dedicated thread exits after capture; lowering its priority
        // cannot change a later foreground task in Tokio's blocking pool.
        let old=unsafe {libc::getpriority(libc::PRIO_PROCESS,0)};
        let _=unsafe {libc::setpriority(libc::PRIO_PROCESS,0,old.max(10))};
        let result=(||->crate::Result<Value> {
            let partial=dir.join(format!("{worker_id}.partial"));
            let complete=dir.join(format!("{worker_id}.n1meta"));
            let file=std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).custom_flags(libc::O_NOFOLLOW).open(&partial)?;
            let mut output=std::io::BufWriter::with_capacity(256*1024,file);
            let records=db.backup(&mut output)?;
            output.flush()?;output.get_ref().sync_all()?;
            let bytes=output.get_ref().metadata()?.len();drop(output);
            std::fs::rename(partial,complete)?;std::fs::File::open(&dir)?.sync_all()?;
            Ok(json!({"id":worker_id,"phase":"completed","records":records,"bytes":bytes,"finished_at":auth::now(),"capture_mode":"mvcc_snapshot","payloads_included":false}))
        })();
        *status.state.lock().unwrap_or_else(|e|e.into_inner())=match result {Ok(value)=>value,Err(_)=>json!({"id":worker_id,"phase":"incomplete","error":"metadata snapshot creation failed; partial evidence retained","finished_at":auth::now()})};
    }).map_err(|_| {
        *app.backups.state.lock().unwrap_or_else(|e|e.into_inner())=json!({"id":id,"phase":"incomplete","error":"cannot start backup worker"});
        ApiError::invalid("cannot start metadata backup worker")
    })?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"id":id,"phase":"running"})),
    ))
}
pub(crate) async fn download(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    claims.require("operator")?;
    identity(&id)?;
    open_download(&app, &id).await
}
#[derive(serde::Serialize, serde::Deserialize)]
struct DownloadGrant {
    snapshot: String,
    scope: String,
    exp: u64,
}
pub(crate) async fn signed_url(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let claims = auth::authorize(&app, &headers, "read").await?;
    claims.require("operator")?;
    identity(&id)?;
    let grant = DownloadGrant {
        snapshot: id,
        scope: "metadata_backup".into(),
        exp: auth::now() + 120,
    };
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &grant,
        &jsonwebtoken::EncodingKey::from_secret(app.config.jwt_secret.as_bytes()),
    )
    .map_err(|_| ApiError::invalid("cannot sign snapshot download"))?;
    Ok(Json(
        json!({"path":format!("/noa/v1/maintenance/metadata/signed-backup?token={token}"),"expires_at":grant.exp}),
    ))
}
pub(crate) async fn signed_download(
    State(app): State<Arc<App>>,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> ApiResult<Response> {
    let token = query.get("token").ok_or_else(ApiError::unauthorized)?;
    let grant = jsonwebtoken::decode::<DownloadGrant>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(app.config.jwt_secret.as_bytes()),
        &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256),
    )
    .map_err(|_| ApiError::unauthorized())?
    .claims;
    if grant.scope != "metadata_backup" || grant.exp <= auth::now() {
        return Err(ApiError::unauthorized());
    }
    identity(&grant.snapshot)?;
    open_download(&app, &grant.snapshot).await
}
async fn open_download(app: &App, id: &str) -> ApiResult<Response> {
    let path = directory(app)?.join(format!("{id}.n1meta"));
    let file = tokio::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .await
        .map_err(|_| ApiError(StatusCode::NOT_FOUND, "metadata snapshot not found".into()))?;
    let stat = file.metadata().await?;
    if !stat.is_file() {
        return Err(ApiError::invalid("invalid metadata snapshot file"));
    }
    Response::builder()
        .header("content-type", "application/octet-stream")
        .header("content-length", stat.len())
        .header("cache-control", "no-store")
        .header(
            "content-disposition",
            format!("attachment; filename=\"{id}.n1meta\""),
        )
        .body(Body::from_stream(tokio_util::io::ReaderStream::new(file)))
        .map_err(|_| ApiError::invalid("cannot build snapshot response"))
}
