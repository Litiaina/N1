//! Explicit, single-admission integrity work. Ordinary monitoring never calls
//! start. Each page uses a live MVCC view; this is not a backup snapshot.
use super::{
    App,
    error::{ApiError, ApiResult},
};
use crate::{RecordKey, Table, namespace::store::StoredVersion};
use n1_link::{ObjectKey, PoolId, noa::Command};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

struct Scan {
    fragment: PoolId,
    pool: Option<String>,
    state: BTreeMap<String, Value>,
}
pub(super) struct Diagnostics {
    scan: Mutex<Option<Scan>>,
    admission: Arc<Semaphore>,
}
impl Default for Diagnostics {
    fn default() -> Self {
        Self {
            scan: Mutex::new(None),
            admission: Arc::new(Semaphore::new(1)),
        }
    }
}
impl Diagnostics {
    pub(super) fn status(&self, fragment: PoolId, pool: &str) -> Option<Value> {
        let scan = self.scan.lock().unwrap_or_else(|e| e.into_inner());
        scan.as_ref()
            .filter(|s| s.fragment == fragment && s.pool.as_deref().is_none_or(|p| p == pool))
            .and_then(|s| s.state.get(pool))
            .cloned()
    }
    pub(super) fn start(
        self: &Arc<Self>,
        app: Arc<App>,
        fragment: PoolId,
        pool: Option<String>,
    ) -> ApiResult<()> {
        let permit = self.admission.clone().try_acquire_owned().map_err(|_| {
            ApiError(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "an integrity scan is already running".into(),
            )
        })?;
        let state=app.pools.iter().filter(|p|pool.as_deref().is_none_or(|id|p.id.to_string()==id)).map(|p|(p.id.to_string(),json!({"status":"running","scope":"live fragment references","versions_checked":0,"failed_versions":0,"verified_bytes":0,"elapsed_ms":0}))).collect();
        *self.scan.lock().unwrap_or_else(|e| e.into_inner()) = Some(Scan {
            fragment,
            pool,
            state,
        });
        let this = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let started = Instant::now();
            let result = this.run(&app, fragment, started).await;
            let mut scan = this.scan.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(scan) = scan.as_mut() {
                for state in scan.state.values_mut() {
                    state["elapsed_ms"] = json!(started.elapsed().as_millis() as u64);
                    state["status"] = json!(if result.is_err() {
                        "incomplete"
                    } else if state["failed_versions"].as_u64().unwrap_or(0) > 0 {
                        "failed"
                    } else {
                        "passed"
                    });
                    if let Err(error) = &result {
                        state["error"] = json!(error.to_string());
                    }
                }
            }
        });
        Ok(())
    }
    async fn run(&self, app: &Arc<App>, fragment: PoolId, started: Instant) -> ApiResult<()> {
        let mut after: Option<RecordKey> = None;
        loop {
            if started.elapsed() > Duration::from_secs(3600) {
                return Err(ApiError::invalid(
                    "integrity scan window expired; unchecked objects are not declared healthy",
                ));
            }
            let cursor = after.clone();
            let page = app
                .db
                .execute(move |db| {
                    db.snapshot()
                        .scan(Table::Versions, fragment, "", cursor.as_ref(), 128)
                })
                .await?;
            if page.is_empty() {
                return Ok(());
            }
            for (key, bytes) in page {
                after = Some(key.clone());
                if key.identifier().starts_with('@') {
                    continue;
                }
                let stored: StoredVersion = serde_json::from_slice(&bytes).map_err(|_| {
                    ApiError::invalid("invalid stored version during integrity scan")
                })?;
                let identity = ObjectKey::inspect(&stored.object_key)
                    .map_err(|_| ApiError::invalid("invalid stored LINK identity"))?;
                let pool = identity.pool.to_string();
                if self.status(fragment, &pool).is_none() {
                    continue;
                }
                // Keep admission until this object check actually ends. A
                // caller's wall-clock timeout must not admit a second scan
                // while the first object's I/O is still owned by the server.
                let health = app
                    .service
                    .objects()
                    .control(Command::ObjectHealth {
                        object_key: stored.object_key,
                        deep: true,
                    })
                    .await;
                {
                    let mut scan = self.scan.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(state) = scan.as_mut().and_then(|s| s.state.get_mut(&pool)) {
                        let checked = state["versions_checked"].as_u64().unwrap_or(0);
                        state["versions_checked"] = json!(checked + 1);
                        match health {
                            Ok(value) if value["status"] == "healthy" => {
                                state["verified_bytes"] = json!(
                                    state["verified_bytes"]
                                        .as_u64()
                                        .unwrap_or(0)
                                        .saturating_add(
                                            value["verified_bytes"].as_u64().unwrap_or(0)
                                        )
                                );
                            }
                            _ => {
                                state["failed_versions"] =
                                    json!(state["failed_versions"].as_u64().unwrap_or(0) + 1);
                                state["error"] = json!(
                                    "object did not pass the requested checksum verification"
                                );
                            }
                        }
                        state["elapsed_ms"] = json!(started.elapsed().as_millis() as u64);
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}
