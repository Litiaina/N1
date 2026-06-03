use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadRoute {
    pub session_id: String,
    pub version_id: String,
    pub coordinator_node_id: String,
    pub fragment: Option<String>,
    pub object_key: Option<String>,
    pub created_at: i64,
    pub expires_at: i64,
    #[serde(default)]
    pub finalized_at: Option<i64>,
    #[serde(default)]
    pub cancelled_at: Option<i64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct UploadRouteFile {
    routes: Vec<UploadRoute>,
}

#[derive(Clone)]
pub struct GatewaySessionDb {
    path: PathBuf,
    routes: Arc<RwLock<HashMap<String, UploadRoute>>>,
}

impl GatewaySessionDb {
    pub async fn load(path: PathBuf) -> Result<Self, String> {
        let mut map = HashMap::new();
        if path.exists() {
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|err| format!("read {}: {err}", path.display()))?;
            if !bytes.is_empty() {
                let file: UploadRouteFile = serde_json::from_slice(&bytes)
                    .map_err(|err| format!("parse {}: {err}", path.display()))?;
                let now = now_unix();
                for route in file.routes {
                    if route.cancelled_at.is_none()
                        && route.finalized_at.is_none()
                        && route.expires_at > now
                    {
                        map.insert(route.session_id.clone(), route.clone());
                        map.insert(route.version_id.clone(), route);
                    }
                }
            }
        }
        Ok(Self {
            path,
            routes: Arc::new(RwLock::new(map)),
        })
    }

    pub async fn upsert(&self, route: UploadRoute) -> Result<(), String> {
        let mut routes = self.routes.write().await;
        routes.insert(route.session_id.clone(), route.clone());
        routes.insert(route.version_id.clone(), route);
        self.persist_locked(&routes).await
    }

    pub async fn get(&self, key: &str) -> Option<UploadRoute> {
        self.routes
            .read()
            .await
            .get(key)
            .filter(|route| route_is_active(route, now_unix()))
            .cloned()
    }

    pub async fn mark_finalized(&self, session_id: &str, version_id: &str) -> Result<(), String> {
        self.mark_terminal(session_id, version_id, true).await
    }

    pub async fn mark_cancelled(
        &self,
        session_id: Option<&str>,
        version_id: &str,
    ) -> Result<(), String> {
        self.mark_terminal(session_id.unwrap_or(version_id), version_id, false)
            .await
    }

    pub async fn list_active(&self) -> Vec<UploadRoute> {
        let now = now_unix();
        let mut unique = HashMap::<String, UploadRoute>::new();
        for route in self.routes.read().await.values() {
            if route.cancelled_at.is_none()
                && route.finalized_at.is_none()
                && route.expires_at > now
            {
                unique.insert(route.session_id.clone(), route.clone());
            }
        }
        let mut out = unique.into_values().collect::<Vec<_>>();
        out.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        out
    }

    async fn mark_terminal(
        &self,
        session_id: &str,
        version_id: &str,
        finalized: bool,
    ) -> Result<(), String> {
        let mut routes = self.routes.write().await;
        let now = now_unix();
        for key in [session_id, version_id] {
            if let Some(route) = routes.get_mut(key) {
                if finalized {
                    route.finalized_at = Some(now);
                } else {
                    route.cancelled_at = Some(now);
                }
            }
        }
        routes.remove(session_id);
        routes.remove(version_id);
        self.persist_locked(&routes).await
    }

    async fn persist_locked(&self, routes: &HashMap<String, UploadRoute>) -> Result<(), String> {
        let now = now_unix();
        let mut unique = HashMap::<String, UploadRoute>::new();
        for route in routes.values() {
            if route.cancelled_at.is_none()
                && route.finalized_at.is_none()
                && route.expires_at > now
            {
                unique.insert(route.session_id.clone(), route.clone());
            }
        }
        let mut active = unique.into_values().collect::<Vec<_>>();
        active.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        let file = UploadRouteFile { routes: active };
        write_json_atomic(&self.path, &file).await
    }
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn route_is_active(route: &UploadRoute, now: i64) -> bool {
    route.cancelled_at.is_none() && route.finalized_at.is_none() && route.expires_at > now
}

async fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| format!("create {}: {err}", parent.display()))?;
    }
    let tmp = path.with_extension("tmp");
    let bytes = serde_json::to_vec_pretty(value).map_err(|err| err.to_string())?;
    tokio::fs::write(&tmp, bytes)
        .await
        .map_err(|err| format!("write {}: {err}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .map_err(|err| format!("rename {} -> {}: {err}", tmp.display(), path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_state_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("n2-gateway-{name}-{}.json", std::process::id()))
    }

    fn route(session_id: &str, version_id: &str, expires_at: i64) -> UploadRoute {
        UploadRoute {
            session_id: session_id.to_string(),
            version_id: version_id.to_string(),
            coordinator_node_id: "node-a".to_string(),
            fragment: Some("frag".to_string()),
            object_key: Some("docs/report.pdf".to_string()),
            created_at: now_unix(),
            expires_at,
            finalized_at: None,
            cancelled_at: None,
        }
    }

    #[tokio::test]
    async fn route_is_available_by_session_and_version_after_reload() {
        let path = temp_state_path("reload");
        let _ = tokio::fs::remove_file(&path).await;
        let db = GatewaySessionDb::load(path.clone()).await.unwrap();
        db.upsert(route("sess-a", "ver-a", now_unix() + 60))
            .await
            .unwrap();

        let reloaded = GatewaySessionDb::load(path.clone()).await.unwrap();
        assert_eq!(
            reloaded.get("sess-a").await.unwrap().coordinator_node_id,
            "node-a"
        );
        assert_eq!(reloaded.get("ver-a").await.unwrap().session_id, "sess-a");
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn finalized_and_cancelled_routes_stop_routing() {
        let path = temp_state_path("terminal");
        let _ = tokio::fs::remove_file(&path).await;
        let db = GatewaySessionDb::load(path.clone()).await.unwrap();
        db.upsert(route("sess-a", "ver-a", now_unix() + 60))
            .await
            .unwrap();
        db.mark_finalized("sess-a", "ver-a").await.unwrap();
        assert!(db.get("sess-a").await.is_none());
        assert!(db.get("ver-a").await.is_none());

        db.upsert(route("sess-b", "ver-b", now_unix() + 60))
            .await
            .unwrap();
        db.mark_cancelled(Some("sess-b"), "ver-b").await.unwrap();
        assert!(db.get("sess-b").await.is_none());
        assert!(db.get("ver-b").await.is_none());
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn expired_routes_are_ignored_on_load() {
        let path = temp_state_path("expired");
        let _ = tokio::fs::remove_file(&path).await;
        let file = UploadRouteFile {
            routes: vec![route("sess-a", "ver-a", now_unix() - 1)],
        };
        write_json_atomic(&path, &file).await.unwrap();

        let db = GatewaySessionDb::load(path.clone()).await.unwrap();
        assert!(db.get("sess-a").await.is_none());
        assert!(db.list_active().await.is_empty());
        let _ = tokio::fs::remove_file(path).await;
    }
}
