//! NOA adapters for the optional named N1 service. Both transports call the
//! same domain service; object bytes cross the public native LINK boundary.
pub mod config;
pub mod error;
pub mod native;
mod recovery;
pub(crate) mod routes;

use crate::{
    MetadataEngine, MetadataExecutor, Options,
    object_store::{NativeObjectStore, ObjectStoreConfig},
    service::MetadataService,
};
pub use config::ServerConfig;
pub use routes::router;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{RwLock, Semaphore};

pub struct App {
    pub config: ServerConfig,
    pub db: MetadataExecutor,
    pub service: MetadataService,
    pub auth_slots: Arc<Semaphore>,
    pub upload_slots: Arc<Semaphore>,
    pub upload_memory: Arc<Semaphore>,
    pub events: tokio::sync::broadcast::Sender<serde_json::Value>,
    upload_locks: Mutex<HashMap<String, Weak<RwLock<()>>>>,
    pub(crate) pools: Vec<PoolProfile>,
    pub recovery_errors: Mutex<Vec<String>>,
}
#[derive(Clone, serde::Deserialize)]
pub(crate) struct PoolProfile {
    pub id: n1_link::PoolId,
    pub data_shards: u16,
    pub block_bytes: u32,
    pub sealed: bool,
}
impl App {
    pub async fn open(config: ServerConfig) -> error::ApiResult<Arc<Self>> {
        let engine = MetadataEngine::open(&config.data_dir, Options::default())?;
        let db = MetadataExecutor::new(engine, config.metadata_workers)?;
        let objects = NativeObjectStore::new(ObjectStoreConfig {
            native_url: config.object_native_url.clone(),
            fragment: config.object_fragment.clone(),
            secret: config.object_secret.clone(),
            max_connections: config.object_connections,
            tls: config
                .object_ca
                .as_deref()
                .map(cli_core::client_tls)
                .transpose()
                .map_err(|_| error::ApiError::invalid("cannot load object engine CA"))?,
        })?;
        let profiles = objects.control(n1_link::noa::Command::Pools).await?;
        let pools: Vec<PoolProfile> = serde_json::from_value(profiles["pools"].clone())
            .map_err(|_| error::ApiError::invalid("invalid object pool profiles"))?;
        if pools.is_empty()
            || pools
                .iter()
                .any(|p| p.data_shards == 0 || p.block_bytes == 0)
        {
            return Err(error::ApiError::invalid("object engine has no valid pools"));
        }
        crate::uploads::private_directory(&config.staging_dir).await?;
        let (events, _) = tokio::sync::broadcast::channel(256);
        let memory_units = config.upload_memory_budget.div_ceil(65536);
        let app = Arc::new(Self {
            service: MetadataService::new(db.clone(), objects),
            db,
            auth_slots: Arc::new(Semaphore::new(4)),
            upload_slots: Arc::new(Semaphore::new(config.upload_concurrency)),
            upload_memory: Arc::new(Semaphore::new(memory_units)),
            config,
            events,
            upload_locks: Mutex::new(HashMap::new()),
            pools,
            recovery_errors: Mutex::new(Vec::new()),
        });
        recovery::reconcile(&app).await?;
        Ok(app)
    }
    pub(crate) fn upload_lock(&self, fragment: n1_link::PoolId, version: &str) -> Arc<RwLock<()>> {
        let key = format!("{fragment}/{version}");
        let mut locks = self.upload_locks.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        if locks.len() >= 1024 {
            locks.retain(|_, v| v.strong_count() > 0);
        }
        let lock = Arc::new(RwLock::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        lock
    }
    pub(crate) fn emit(&self, fragment: n1_link::PoolId) {
        let _ = self.events.send(serde_json::json!({"event":"NAMESPACE_CHANGED", "fragment":ulid::Ulid::from_bytes(fragment.0).to_string()}));
    }
    pub async fn shutdown(&self) {
        self.service.shutdown().await;
    }
}
