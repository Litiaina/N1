//! NOA adapters for M1, the metadata store. Both transports call the
//! same domain service; object bytes cross the public native LINK boundary.
pub(crate) mod accel;
pub(crate) mod backups;
pub mod config;
mod diagnostics;
pub mod error;
mod monitoring;
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
use std::sync::{Arc, Mutex};
use tokio::sync::{RwLock, Semaphore};

pub struct App {
    pub config: ServerConfig,
    pub db: MetadataExecutor,
    pub service: MetadataService,
    pub auth_slots: Arc<Semaphore>,
    pub upload_slots: Arc<Semaphore>,
    pub upload_memory: Arc<Semaphore>,
    pub(crate) rate_limits: Arc<crate::middleware::rate_limiter::RateLimits>,
    pub(crate) accel: accel::Accel,
    pub events: tokio::sync::broadcast::Sender<serde_json::Value>,
    upload_locks: Arc<crate::uploads::UploadLocks>,
    pub(crate) pools: Vec<PoolProfile>,
    pub recovery_errors: Mutex<Vec<String>>,
    pub(crate) metadata_observations: Arc<cli_core::monitoring::ObservationCache>,
    pub(crate) object_observations: Arc<cli_core::monitoring::ObservationCache>,
    pub(crate) monitor_objects: NativeObjectStore,
    diagnostics: Arc<diagnostics::Diagnostics>,
    pub(crate) backups: Arc<backups::Backups>,
    pub(crate) maintenance: Arc<crate::maintenance::Manager>,
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
        let maintenance_db =
            MetadataExecutor::new(engine.clone(), config.maintenance.object_concurrency + 1)?;
        maintenance_db
            .execute(|db| crate::namespace::Namespace::new(db).ensure_maintenance_indexes())
            .await?;
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
        // A single separate control connection cannot occupy an upload's
        // native connection permit when a monitoring request is slow.
        let monitor_objects = NativeObjectStore::new(ObjectStoreConfig {
            native_url: config.object_native_url.clone(),
            fragment: config.object_fragment.clone(),
            secret: config.object_secret.clone(),
            max_connections: 1,
            tls: config
                .object_ca
                .as_deref()
                .map(cli_core::client_tls)
                .transpose()
                .map_err(|_| error::ApiError::invalid("cannot load object engine CA"))?,
        })?;
        let pools: Vec<PoolProfile> = serde_json::from_value(profiles["pools"].clone())
            .map_err(|_| error::ApiError::invalid("invalid object pool profiles"))?;
        if pools.is_empty()
            || pools
                .iter()
                .any(|p| p.data_shards == 0 || p.block_bytes == 0)
        {
            return Err(error::ApiError::invalid("object engine has no valid pools"));
        }
        let background_objects = NativeObjectStore::new(ObjectStoreConfig {
            native_url: config.object_native_url.clone(),
            fragment: config.object_fragment.clone(),
            secret: config.object_secret.clone(),
            max_connections: config.maintenance.object_concurrency * 2,
            tls: config
                .object_ca
                .as_deref()
                .map(cli_core::client_tls)
                .transpose()
                .map_err(|_| error::ApiError::invalid("cannot load object engine CA"))?,
        })?;
        let upload_locks = Arc::new(crate::uploads::UploadLocks::default());
        let maintenance = crate::maintenance::Manager::new(
            maintenance_db,
            background_objects,
            config.maintenance.clone(),
            pools.clone(),
            config.staging_dir.clone(),
            upload_locks.clone(),
        );
        crate::uploads::private_directory(&config.staging_dir).await?;
        crate::uploads::persist_staging_root(&config.staging_dir).await?;
        let (events, _) = tokio::sync::broadcast::channel(256);
        let memory_units = config.upload_memory_budget.div_ceil(65536);
        let app = Arc::new(Self {
            service: MetadataService::new(db.clone(), objects),
            db,
            auth_slots: Arc::new(Semaphore::new(4)),
            upload_slots: Arc::new(Semaphore::new(config.upload_concurrency)),
            upload_memory: Arc::new(Semaphore::new(memory_units)),
            rate_limits: Arc::new(crate::middleware::rate_limiter::RateLimits::new(
                &config.rate_limits,
            )),
            accel: accel::Accel::new(&config.accel)?,
            config,
            events,
            upload_locks,
            pools,
            recovery_errors: Mutex::new(Vec::new()),
            metadata_observations: Arc::default(),
            object_observations: Arc::default(),
            monitor_objects,
            diagnostics: Arc::default(),
            backups: Arc::default(),
            maintenance,
        });
        recovery::reconcile(&app).await?;
        Ok(app)
    }
    pub(crate) fn upload_lock(&self, fragment: n1_link::PoolId, version: &str) -> Arc<RwLock<()>> {
        self.upload_locks.get(fragment, version)
    }
    pub(crate) fn emit(&self, fragment: n1_link::PoolId) {
        let _ = self.events.send(serde_json::json!({"event":"NAMESPACE_CHANGED", "fragment":ulid::Ulid::from_bytes(fragment.0).to_string()}));
    }
    pub async fn shutdown(&self) {
        self.rate_limits.shutdown().await;
        self.diagnostics.shutdown().await;
        self.maintenance.shutdown().await;
        self.backups.drain().await;
        self.monitor_objects.shutdown().await;
        self.service.shutdown().await;
    }
    pub async fn start_maintenance(&self) -> error::ApiResult<()> {
        self.rate_limits.start();
        self.maintenance.resume_and_schedule().await
    }
}
