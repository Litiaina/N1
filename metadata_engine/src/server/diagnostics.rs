//! Explicit, single-admission integrity work. Ordinary monitoring never calls
//! start. Each page uses a live MVCC view; this is not a backup snapshot.
use super::{
    App,
    error::{ApiError, ApiResult},
};
use crate::{RecordKey, Table, namespace::store::StoredVersion, object_store::ObjectStoreResult};
use n1_link::{ObjectKey, PoolId, noa::Command};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

enum Check {
    Verified(u64),
    Damaged,
    Deferred,
}
fn classify(health: ObjectStoreResult<Value>) -> Check {
    match health {
        Ok(value) if value["status"] == "healthy" => {
            Check::Verified(value["verified_bytes"].as_u64().unwrap_or(0))
        }
        Ok(value)
            if matches!(
                value["status"].as_str(),
                Some("degraded" | "critical" | "lost")
            ) =>
        {
            Check::Damaged
        }
        // 422 is the object API's explicit integrity failure. Admission,
        // transport and lifecycle races do not prove corruption. An unverified
        // reference stays visible in the incomplete count instead of passing.
        Err(error) if error.status() == 422 => Check::Damaged,
        _ => Check::Deferred,
    }
}

struct Scan {
    fragment: PoolId,
    pool: Option<String>,
    started_at: u64,
    state: BTreeMap<String, Value>,
}
pub(super) struct Diagnostics {
    scan: Mutex<Option<Scan>>,
    admission: Arc<Semaphore>,
    lifecycle: Mutex<Lifecycle>,
}
#[derive(Default)]
struct Lifecycle {
    stopping: bool,
    cancel_requested: bool,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Default for Diagnostics {
    fn default() -> Self {
        Self {
            scan: Mutex::new(None),
            admission: Arc::new(Semaphore::new(1)),
            lifecycle: Mutex::new(Lifecycle::default()),
        }
    }
}
impl Diagnostics {
    pub(super) fn summary(&self, fragment: PoolId) -> Value {
        let cancelled = self
            .lifecycle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cancel_requested;
        let scan = self.scan.lock().unwrap_or_else(|e| e.into_inner());
        let Some(scan) = scan.as_ref().filter(|scan| scan.fragment == fragment) else {
            return json!({"status": "not_scanned"});
        };
        let mut summary = json!({
            "status": "passed", "pool": scan.pool,
            "started_at": scan.started_at, "elapsed_ms": 0,
            "versions_checked": 0, "verified_versions": 0,
            "failed_versions": 0, "deferred_versions": 0, "verified_bytes": 0
        });
        for state in scan.state.values() {
            for key in [
                "versions_checked",
                "verified_versions",
                "failed_versions",
                "deferred_versions",
                "verified_bytes",
            ] {
                summary[key] = json!(
                    summary[key]
                        .as_u64()
                        .unwrap_or(0)
                        .saturating_add(state[key].as_u64().unwrap_or(0))
                );
            }
            summary["elapsed_ms"] = json!(
                summary["elapsed_ms"]
                    .as_u64()
                    .unwrap_or(0)
                    .max(state["elapsed_ms"].as_u64().unwrap_or(0))
            );
            if state["status"] == "running" {
                summary["status"] = json!("running");
            } else if summary["status"] != "running" {
                if state["status"] == "failed" {
                    summary["status"] = json!("failed");
                } else if summary["status"] != "failed" && state["status"] != "passed" {
                    summary["status"] = state["status"].clone();
                }
            }
        }
        summary["stop_requested"] = json!(cancelled && summary["status"] == "running");
        if summary["status"] == "running" {
            summary["elapsed_ms"] = json!(
                cli_core::monitoring::now()
                    .saturating_sub(scan.started_at)
                    .saturating_mul(1000)
            );
        }
        summary
    }

    /// A stop drains the currently admitted check; it never aborts owned N1 I/O.
    pub(super) fn cancel(&self, fragment: PoolId) -> ApiResult<Value> {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
        let scan = self.scan.lock().unwrap_or_else(|e| e.into_inner());
        if scan.as_ref().is_some_and(|scan| scan.fragment == fragment) {
            lifecycle.cancel_requested = true;
        }
        drop(scan);
        drop(lifecycle);
        Ok(self.summary(fragment))
    }

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
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
        if lifecycle.stopping {
            return Err(ApiError::invalid("integrity monitoring is shutting down"));
        }
        let permit = match self.admission.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let scan = self.scan.lock().unwrap_or_else(|e| e.into_inner());
                if scan.as_ref().is_some_and(|scan| {
                    scan.fragment == fragment
                        && scan
                            .pool
                            .as_deref()
                            .is_none_or(|active| pool.as_deref() == Some(active))
                }) {
                    return Ok(());
                }
                return Err(ApiError(
                    axum::http::StatusCode::CONFLICT,
                    "An integrity check is already running in another scope. Wait for it to finish before starting this scope.".into(),
                ));
            }
        };
        lifecycle.cancel_requested = false;
        let state = app
            .pools
            .iter()
            .filter(|p| pool.as_deref().is_none_or(|id| p.id.to_string() == id))
            .map(|p| {
                (
                    p.id.to_string(),
                    json!({
                        "status": "running",
                        "scope": "live fragment references",
                        "versions_checked": 0,
                        "verified_versions": 0,
                        "failed_versions": 0,
                        "deferred_versions": 0,
                        "verified_bytes": 0,
                        "elapsed_ms": 0
                    }),
                )
            })
            .collect();
        *self.scan.lock().unwrap_or_else(|e| e.into_inner()) = Some(Scan {
            fragment,
            pool,
            started_at: cli_core::monitoring::now(),
            state,
        });
        let this = self.clone();
        lifecycle.task = Some(tokio::spawn(async move {
            let _permit = permit;
            let started = Instant::now();
            let result = this.run(&app, fragment, started).await;
            let cancelled = this
                .lifecycle
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .cancel_requested;
            let mut scan = this.scan.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(scan) = scan.as_mut() {
                for state in scan.state.values_mut() {
                    state["elapsed_ms"] = json!(started.elapsed().as_millis() as u64);
                    state["status"] =
                        json!(if state["failed_versions"].as_u64().unwrap_or(0) > 0 {
                            "failed"
                        } else if cancelled {
                            "cancelled"
                        } else if result.is_err()
                            || state["deferred_versions"].as_u64().unwrap_or(0) > 0
                        {
                            "incomplete"
                        } else {
                            "passed"
                        });
                    if let Err(error) = &result {
                        state["error"] = json!(error.to_string());
                    }
                }
            }
        }));
        Ok(())
    }
    /// Stop between objects and drain the currently owned native check before
    /// maintenance connections and database workers are closed. Never abort
    /// a worker while the object server still owns its admitted I/O.
    pub(super) async fn shutdown(&self) {
        let task = {
            let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
            lifecycle.stopping = true;
            lifecycle.task.take()
        };
        if let Some(task) = task {
            let _ = task.await;
        }
    }
    fn ensure_running(&self) -> ApiResult<()> {
        let lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
        if lifecycle.stopping || lifecycle.cancel_requested {
            return Err(ApiError::invalid(
                "integrity check stopped; unchecked objects are not declared healthy",
            ));
        }
        Ok(())
    }
    async fn run(&self, app: &Arc<App>, fragment: PoolId, started: Instant) -> ApiResult<()> {
        let (db, objects) = app.maintenance.diagnostic_handles();
        let mut after: Option<RecordKey> = None;
        loop {
            self.ensure_running()?;
            if started.elapsed() > Duration::from_secs(3600) {
                return Err(ApiError::invalid(
                    "integrity scan window expired; unchecked objects are not declared healthy",
                ));
            }
            let cursor = after.clone();
            let page = db
                .execute(move |db| {
                    db.snapshot()
                        .scan(Table::Versions, fragment, "", cursor.as_ref(), 128)
                })
                .await?;
            if page.is_empty() {
                return Ok(());
            }
            for (key, bytes) in page {
                self.ensure_running()?;
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
                let health = objects
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
                        match classify(health) {
                            Check::Verified(bytes) => {
                                state["verified_versions"] =
                                    json!(state["verified_versions"].as_u64().unwrap_or(0) + 1);
                                state["verified_bytes"] = json!(
                                    state["verified_bytes"]
                                        .as_u64()
                                        .unwrap_or(0)
                                        .saturating_add(bytes)
                                );
                            }
                            Check::Damaged => {
                                state["failed_versions"] =
                                    json!(state["failed_versions"].as_u64().unwrap_or(0) + 1);
                                state["error"] = json!(
                                    "object did not pass the requested checksum verification"
                                );
                            }
                            Check::Deferred => {
                                state["deferred_versions"] =
                                    json!(state["deferred_versions"].as_u64().unwrap_or(0) + 1);
                                state["deferred_reason"] = json!(concat!(
                                    "Some references could not be verified because of admission, ",
                                    "transport, access or concurrent lifecycle changes. ",
                                    "Retry when available; this is not a corruption verdict."
                                ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::ObjectStoreError;
    use n1_native_client::NativeClientError;
    #[test]
    fn progress_is_scoped_and_cancellation_never_hides_confirmed_damage() {
        let diagnostics = Diagnostics::default();
        let fragment = PoolId([9; 16]);
        assert_eq!(diagnostics.summary(fragment)["status"], "not_scanned");
        *diagnostics.scan.lock().unwrap() = Some(Scan {
            fragment,
            pool: None,
            started_at: 123,
            state: BTreeMap::from([
                (
                    "A".into(),
                    json!({"status":"running","versions_checked":3,"verified_versions":2,"failed_versions":0,"deferred_versions":1,"verified_bytes":100,"elapsed_ms":50}),
                ),
                (
                    "B".into(),
                    json!({"status":"running","versions_checked":2,"verified_versions":1,"failed_versions":1,"deferred_versions":0,"verified_bytes":20,"elapsed_ms":60}),
                ),
            ]),
        });
        let progress = diagnostics.summary(fragment);
        assert_eq!(progress["versions_checked"], 5);
        assert_eq!(progress["failed_versions"], 1);
        assert_eq!(progress["verified_bytes"], 120);
        assert!(progress["elapsed_ms"].as_u64().unwrap() >= 60);
        assert_eq!(
            diagnostics.cancel(PoolId([8; 16])).unwrap()["status"],
            "not_scanned"
        );
        assert!(!diagnostics.lifecycle.lock().unwrap().cancel_requested);
        assert_eq!(
            diagnostics.cancel(fragment).unwrap()["stop_requested"],
            true
        );
        let mut scan = diagnostics.scan.lock().unwrap();
        scan.as_mut().unwrap().state.get_mut("A").unwrap()["status"] = json!("cancelled");
        scan.as_mut().unwrap().state.get_mut("B").unwrap()["status"] = json!("failed");
        drop(scan);
        assert_eq!(diagnostics.summary(fragment)["status"], "failed");
    }
    #[test]
    fn only_confirmed_integrity_failures_count_as_damage() {
        for status in [400, 401, 403, 404, 409, 410, 429, 500, 502, 503, 504] {
            let error = ObjectStoreError::Native(NativeClientError::Native {
                code: status.to_string(),
                message: "test".into(),
            });
            assert!(
                matches!(classify(Err(error)), Check::Deferred),
                "status {status}"
            );
        }
        let error = ObjectStoreError::Native(NativeClientError::Native {
            code: "422".into(),
            message: "corrupt".into(),
        });
        assert!(matches!(classify(Err(error)), Check::Damaged));
        assert!(matches!(
            classify(Ok(json!({"status":"unknown"}))),
            Check::Deferred
        ));
        assert!(matches!(
            classify(Ok(json!({"status":"degraded"}))),
            Check::Damaged
        ));
        assert!(matches!(
            classify(Ok(json!({"status":"healthy","verified_bytes":123}))),
            Check::Verified(123)
        ));
    }
}
