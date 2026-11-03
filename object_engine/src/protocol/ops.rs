//! Shared NOA semantics. Native and HTTP call this service directly.
use crate::{
    core::{Error, Result, Store},
    middleware::auth_middleware::{Auth, Claims},
    settings::config::Config,
};
use n1_link::{
    Durability,
    noa::{Command, NOA_VERSION, WIRE_VERSION},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::Arc;

pub struct Service {
    pub(crate) _control_lease: Option<Arc<crate::core::instance_lease::InstanceLease>>,
    pub config: Config,
    pub store: Arc<Store>,
    pub auth: Auth,
    pub limits: crate::middleware::rate_limiter_middleware::RateLimits,
    pub jobs: Arc<crate::core::maintenance::jobs::Jobs>,
    pub recovery_jobs: Arc<crate::core::backup::jobs::Jobs>,
    pub transfers: Arc<tokio::sync::Semaphore>,
}
impl Service {
    /// Called on the startup blocking executor; pool opens never block an async
    /// request reactor or enroll a missing member as a side effect.
    pub fn open(config: Config) -> Result<Arc<Self>> {
        config.validate()?;
        let control_root = config.ensure_state_root()?;
        let control_lease = Arc::new(crate::core::instance_lease::InstanceLease::engine(
            std::slice::from_ref(&control_root),
        )?);
        Self::open_leased(config, control_lease)
    }

    pub(crate) fn open_leased(
        config: Config,
        control_lease: Arc<crate::core::instance_lease::InstanceLease>,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        let control_root = config.state_root();
        let secret = config.signing_secret()?;
        let store = Store::with_limits(
            config.open_pools()?,
            secret,
            config.max_operations,
            config.operation_memory_bytes,
        )?;
        store.configure_read_repair(config.read_repair);
        store.configure_runtime(config.runtime.clone())?;
        store.activity.bind_lease(control_lease.clone())?;
        store.configure_recovery_control(config.clone(), secret)?;
        let auth = Auth::with_session_root(
            config.fragments.clone(),
            &secret,
            control_root.join("refresh_sessions"),
        )
        .with_token_policy(&config.runtime)
        .with_activity(store.activity.clone());
        let limits =
            crate::middleware::rate_limiter_middleware::RateLimits::new(config.rate_limits);
        Ok(Arc::new(Self {
            _control_lease: Some(control_lease),
            config,
            store,
            auth,
            limits,
            jobs: crate::core::maintenance::jobs::Jobs::new(),
            recovery_jobs: crate::core::backup::jobs::Jobs::new(),
            transfers: Arc::new(tokio::sync::Semaphore::new(2)),
        }))
    }
    pub fn ping(&self) -> Value {
        json!({"engine":"Litiaina N1","version":env!("CARGO_PKG_VERSION"),"placement":"LINK-v1","noa":NOA_VERSION,"native_protocol":WIRE_VERSION})
    }

    pub fn authorize(&self, claims: &Claims, command: &Command) -> Result<()> {
        claims.live()?;
        let allowed = match command {
            Command::Ping | Command::SessionInfo => true,
            Command::UploadInit { .. }
            | Command::UploadPart { .. }
            | Command::UploadPut { .. }
            | Command::UploadBatch { .. }
            | Command::UploadFinalize { .. }
            | Command::UploadCancel { .. } => claims.permissions.write,
            Command::UploadStatus { .. } | Command::Pools => {
                claims.permissions.read || claims.permissions.write
            }
            Command::ObjectStat { .. }
            | Command::ObjectGet { .. }
            | Command::SignedRead { .. }
            | Command::ObjectHealth { .. }
            | Command::Health => claims.permissions.read,
            Command::ObjectDelete { .. } | Command::BulkDelete { .. } => claims.permissions.delete,
            Command::ObjectRepair { .. } => claims.permissions.repair,
            Command::BackupReceive { .. }
            | Command::BackupList { .. }
            | Command::BackupCapture { .. }
            | Command::BackupVerify { .. }
            | Command::BackupReplicate { .. }
            | Command::BackupPrune { .. }
            | Command::BackupGc { .. }
            | Command::BackupJob
            | Command::BackupStop { .. } => claims.operator,
            Command::GarbageCollect
            | Command::Scrub { .. }
            | Command::MaintenanceJob
            | Command::MaintenanceStop { .. } => claims.operator,
            Command::Login { .. }
            | Command::Refresh { .. }
            | Command::Bearer { .. }
            | Command::Logout
            | Command::SignedGet { .. } => false,
        };
        if allowed {
            match command {
                Command::UploadInit { .. }
                | Command::UploadPut { .. }
                | Command::UploadPart { .. }
                | Command::UploadBatch { .. }
                | Command::UploadFinalize { .. }
                | Command::UploadCancel { .. }
                | Command::ObjectDelete { .. }
                | Command::BulkDelete { .. } => {
                    self.store.operation_gate.ensure_mutations_allowed()?
                }
                Command::ObjectGet { .. }
                | Command::ObjectStat { .. }
                | Command::ObjectHealth { .. }
                | Command::SignedRead { .. } => self.store.operation_gate.ensure_reads_allowed()?,
                _ => {}
            }
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }

    pub async fn control(self: &Arc<Self>, claims: &Claims, command: Command) -> Result<Value> {
        self.authorize(claims, &command)?;
        let scope = claims.sub.0;
        match command {
            Command::SignedRead {
                object_key,
                offset,
                length,
                ttl_seconds,
            } => {
                if !(1..=21600).contains(&ttl_seconds) {
                    return Err(Error::Invalid(
                        "signed read TTL must be 1..21600 seconds".into(),
                    ));
                }
                let info = self.store.stat(scope, object_key.clone()).await?;
                if offset > info.total_bytes
                    || length.is_some_and(|len| len > info.total_bytes - offset)
                {
                    return Err(Error::Invalid("signed read range exceeds object".into()));
                }
                let expires_at =
                    (chrono::Utc::now().timestamp() + ttl_seconds as i64).min(claims.exp as i64);
                value(
                    self.auth
                        .signed_reads
                        .issue(claims.sub, object_key, offset, length, expires_at),
                )
            }
            Command::Ping => Ok(self.ping()),
            Command::SessionInfo => value(claims),
            Command::UploadInit {
                pool,
                total_bytes,
                part_bytes,
                durable,
                ttl_seconds,
            } => {
                let selected = self
                    .config
                    .pools
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, p)| !p.sealed && pool.is_none_or(|id| p.layout.id == id))
                    .map(|(i, _)| i)
                    .ok_or_else(|| {
                        Error::Unavailable("no unsealed allocation pool matches request".into())
                    })?;
                let store = self.store.clone();
                let admission = store.admit_metadata(&store.pools[selected]).await?;
                value(
                    tokio::task::spawn_blocking(move || {
                        let _admission = admission;
                        store.init(
                            scope,
                            selected,
                            total_bytes,
                            part_bytes,
                            if durable {
                                Durability::Durable
                            } else {
                                Durability::Standard
                            },
                            ttl_seconds,
                        )
                    })
                    .await
                    .map_err(|_| Error::Unavailable("init worker failed".into()))??,
                )
            }
            Command::UploadStatus {
                upload_key,
                start,
                limit,
            } => value(
                self.store
                    .upload_status(scope, upload_key, start, limit)
                    .await?,
            ),
            Command::UploadFinalize { upload_key } => {
                value(self.store.finalize(scope, upload_key).await?)
            }
            Command::UploadCancel { upload_key } => {
                Ok(json!({"removed_shards":self.store.cancel_upload(scope,upload_key).await?}))
            }
            Command::ObjectStat { object_key } => value(self.store.stat(scope, object_key).await?),
            Command::ObjectHealth { object_key, deep } => {
                value(self.store.object_health(scope, object_key, deep).await?)
            }
            Command::ObjectRepair { object_key } => {
                value(self.store.repair(scope, object_key).await?)
            }
            Command::ObjectDelete { object_key } => {
                Ok(json!({"retired_shards":self.store.delete(scope,object_key).await?}))
            }
            Command::BulkDelete { object_keys } => {
                if object_keys.is_empty() || object_keys.len() > 256 {
                    return Err(Error::Invalid(
                        "bulk delete requires 1..=256 explicit keys".into(),
                    ));
                }
                let mut results = Vec::with_capacity(object_keys.len());
                for chunk in object_keys.chunks(16) {
                    let jobs = chunk
                        .iter()
                        .map(|key| self.store.delete(scope, key.clone()));
                    for (key, result) in
                        chunk.iter().zip(futures_util::future::join_all(jobs).await)
                    {
                        results.push(match result {
                            Ok(removed) => {
                                json!({"object_key":key,"status":200,"retired_shards":removed})
                            }
                            Err(e) => {
                                json!({"object_key":key,"status":e.status(),"error":e.to_string()})
                            }
                        });
                    }
                }
                Ok(json!({"results":results}))
            }
            Command::GarbageCollect => value(self.jobs.gc(self.store.clone())?),
            Command::Scrub { options } => value(self.jobs.scrub(self.store.clone(), options)?),
            Command::MaintenanceJob => value(self.jobs.status()?),
            Command::MaintenanceStop { job_id } => value(self.jobs.stop(&job_id)?),
            Command::BackupJob => value(self.recovery_jobs.status()?),
            Command::BackupStop { job_id } => value(self.recovery_jobs.stop(&job_id)?),
            Command::BackupList {
                repository,
                after,
                limit,
            } => {
                if limit == 0 || limit > 256 {
                    return Err(Error::Invalid("recovery page size must be 1..=256".into()));
                }
                if !after.is_empty()
                    && after
                        .parse::<ulid::Ulid>()
                        .map(|id| id.to_string() != after)
                        .unwrap_or(true)
                {
                    return Err(Error::Invalid(
                        "recovery cursor must be a canonical point ID".into(),
                    ));
                }
                let repo = self.open_repository(&repository, false).await?;
                value(
                    tokio::task::spawn_blocking(move || repo.list_page(&after, limit))
                        .await
                        .map_err(worker_error)?
                        .map_err(backup_error)?,
                )
            }
            Command::BackupCapture {
                repository,
                boundary_seconds,
            } => {
                if !(1..=300).contains(&boundary_seconds) {
                    return Err(Error::Invalid(
                        "online backup boundary must be 1..=300 seconds".into(),
                    ));
                }
                let configured = self
                    .config
                    .repositories
                    .get(&repository)
                    .cloned()
                    .ok_or(Error::NotFound)?;
                let store = self.store.clone();
                value(
                    self.recovery_jobs
                        .start("capture", move |cancel| async move {
                            value(
                                store
                                    .snapshot_online_controlled(
                                        configured.path,
                                        configured.key_file,
                                        boundary_seconds,
                                        cancel,
                                    )
                                    .await
                                    .map_err(backup_error)?,
                            )
                        })?,
                )
            }
            Command::BackupVerify {
                repository,
                snapshot,
            } => {
                let service = self.clone();
                self.validate_recovery_point(&repository, &snapshot)?;
                value(
                    self.recovery_jobs
                        .start("verify", move |cancel| async move {
                            let repo = service
                                .open_repository_controlled(&repository, false, cancel)
                                .await?;
                            tokio::task::spawn_blocking(move || {
                                repo.verify(&repo.snapshot(&snapshot)?)?;
                                Ok::<_, crate::core::backup::Error>(json!({"verified":snapshot}))
                            })
                            .await
                            .map_err(worker_error)?
                            .map_err(backup_error)
                        })?,
                )
            }
            Command::BackupReplicate {
                repository,
                destination,
                snapshot,
            } => {
                self.validate_recovery_point(&repository, &snapshot)?;
                if repository == destination || !self.config.repositories.contains_key(&destination)
                {
                    return Err(Error::Invalid(
                        "replication requires a different configured repository".into(),
                    ));
                }
                let service = self.clone();
                value(
                    self.recovery_jobs
                        .start("replicate", move |cancel| async move {
                            let source = service
                                .open_repository_controlled(&repository, false, cancel.clone())
                                .await?;
                            let target = service
                                .open_repository_controlled(&destination, true, cancel)
                                .await?;
                            value(
                                tokio::task::spawn_blocking(move || {
                                    crate::core::backup::transfer::replicate(
                                        &source, &target, &snapshot,
                                    )
                                })
                                .await
                                .map_err(worker_error)?
                                .map_err(backup_error)?,
                            )
                        })?,
                )
            }
            Command::BackupPrune {
                repository,
                keep_last,
                protect,
            } => {
                self.validate_recovery_point(&repository, &protect)?;
                if !(1..=10000).contains(&keep_last) {
                    return Err(Error::Invalid("keep-last must be 1..10000".into()));
                }
                let service = self.clone();
                value(
                    self.recovery_jobs
                        .start("prune", move |cancel| async move {
                            let repo = service
                                .open_repository_controlled(&repository, true, cancel)
                                .await?;
                            tokio::task::spawn_blocking(move || {
                                crate::core::backup::retention::prune(&repo, keep_last, &protect)?;
                                Ok::<_, crate::core::backup::Error>(
                                    json!({"keep_last":keep_last,"protected":protect}),
                                )
                            })
                            .await
                            .map_err(worker_error)?
                            .map_err(backup_error)
                        })?,
                )
            }
            Command::BackupGc {
                repository,
                discard_incomplete,
            } => {
                if !self.config.repositories.contains_key(&repository) {
                    return Err(Error::NotFound);
                }
                let service = self.clone();
                value(self.recovery_jobs.start("gc", move |cancel| async move {
                    let repo = service
                        .open_repository_controlled(&repository, true, cancel)
                        .await?;
                    tokio::task::spawn_blocking(move || {
                        let removed =
                            crate::core::backup::retention::gc(&repo, discard_incomplete)?;
                        Ok::<_, crate::core::backup::Error>(json!({"removed_bytes":removed}))
                    })
                    .await
                    .map_err(worker_error)?
                    .map_err(backup_error)
                })?)
            }
            Command::Health => {
                let system = crate::core::system::get_system_metrics().await;
                let capacity =
                    crate::core::capacity_manager::get_storage_usage(&self.store.pools).await?;
                Ok(
                    json!({"engine":self.ping(),"io":crate::io_control::io_control_snapshot(),"system":system,"capacity":capacity,
                    "operations":self.store.activity.status()?,"monitor":self.store.monitor.lock().unwrap_or_else(|error|error.into_inner()).clone(),
                    "maintenance":crate::core::auto_healing::status(&self.store.operation_gate),"job":self.jobs.status()?,
                    "configuration":{"native_address":self.config.native_address,"http_address":self.config.http_address,"max_connections":self.config.max_connections,
                    "max_operations":self.config.max_operations,"operation_memory_bytes":self.config.operation_memory_bytes,"concurrency":self.config.concurrency,
                    "runtime":self.config.runtime,"scheduled_maintenance":self.config.maintenance}}),
                )
            }
            Command::Pools => Ok(
                json!({"pools":self.config.pools.iter().enumerate().map(|(i,p)|json!({
                "id":p.layout.id,"generation":p.layout.generation,"data_shards":p.layout.data_shards,"parity_shards":p.layout.parity_shards,
                "block_bytes":p.layout.block_bytes,"sealed":p.sealed,"available_members":self.store.pools[i].members.iter().filter(|m|m.is_some()).count()
            })).collect::<Vec<_>>() }),
            ),
            _ => Err(Error::Invalid(
                "operation requires the streaming or authentication entry point".into(),
            )),
        }
    }

    pub fn upload_length(&self, claims: &Claims, ticket: &str, part: u64) -> Result<u64> {
        claims.live()?;
        if !claims.permissions.write {
            return Err(Error::Forbidden);
        }
        let length = self
            .store
            .inspect_upload(claims.sub.0, ticket)?
            .part_length(part)
            .map_err(Error::from)?;
        if length > self.config.runtime.max_part_size {
            return Err(Error::Invalid(
                "part exceeds configured max_part_size".into(),
            ));
        }
        Ok(length)
    }
    pub async fn open_repository(
        &self,
        alias: &str,
        write: bool,
    ) -> Result<Arc<crate::core::backup::repository::Repository>> {
        self.open_repository_controlled(alias, write, tokio_util::sync::CancellationToken::new())
            .await
    }
    fn validate_recovery_point(&self, alias: &str, id: &str) -> Result<()> {
        if !self.config.repositories.contains_key(alias) {
            return Err(Error::NotFound);
        }
        if id
            .parse::<ulid::Ulid>()
            .map(|parsed| parsed.to_string() != id)
            .unwrap_or(true)
        {
            return Err(Error::Invalid("invalid recovery point ID".into()));
        }
        Ok(())
    }
    async fn open_repository_controlled(
        &self,
        alias: &str,
        write: bool,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Arc<crate::core::backup::repository::Repository>> {
        let config = self
            .config
            .repositories
            .get(alias)
            .cloned()
            .ok_or(Error::NotFound)?;
        let roots = self
            .config
            .pools
            .iter()
            .flat_map(|p| p.roots.iter().cloned())
            .collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || {
            let repo = crate::core::backup::repository::Repository::open(
                &config.path,
                &config.key_file,
                write,
            )
            .map_err(backup_error)?
            .with_cancel(cancel);
            for root in roots {
                let root = root.canonicalize().unwrap_or(root);
                if repo.path.starts_with(&root) || root.starts_with(&repo.path) {
                    return Err(Error::Invalid(
                        "recovery repository must be outside active member roots".into(),
                    ));
                }
            }
            Ok(Arc::new(repo))
        })
        .await
        .map_err(|e| Error::Unavailable(e.to_string()))?
    }
    pub fn body_length(&self, claims: &Claims, command: &Command) -> Result<u64> {
        self.authorize(claims, command)?;
        match command {
            Command::UploadPart { upload_key, part } => {
                self.upload_length(claims, upload_key, *part)
            }
            Command::UploadPut { upload_key } => {
                let plan = self.store.inspect_upload(claims.sub.0, upload_key)?;
                if plan.parts() > 1 {
                    return Err(Error::Invalid(
                        "normal upload needs a single-part ticket".into(),
                    ));
                }
                Ok(plan.total_bytes)
            }
            Command::UploadBatch { upload_keys } => {
                self.store.batch_length(claims.sub.0, upload_keys)
            }
            _ => Err(Error::Invalid(
                "operation does not accept an object body".into(),
            )),
        }
    }
    pub async fn upload_body<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
        &self,
        claims: &Claims,
        command: Command,
        reader: R,
    ) -> Result<Value> {
        self.authorize(claims, &command)?;
        let scope = claims.sub.0;
        match command {
            Command::UploadPart { upload_key, part } => {
                self.store
                    .write_part(scope, upload_key, part, reader)
                    .await?;
                Ok(json!({"part":part,"complete":true}))
            }
            Command::UploadPut { upload_key } => {
                value(self.store.put(scope, upload_key, reader).await?)
            }
            Command::UploadBatch { upload_keys } => {
                Ok(json!({"objects":self.store.put_batch(scope,upload_keys,reader).await?}))
            }
            _ => Err(Error::Invalid(
                "operation does not accept an object body".into(),
            )),
        }
    }
}
fn worker_error(error: tokio::task::JoinError) -> Error {
    Error::Unavailable(format!("recovery worker failed: {error}"))
}
fn value(v: impl Serialize) -> Result<Value> {
    serde_json::to_value(v).map_err(|e| Error::Invalid(e.to_string()))
}

pub(crate) fn backup_error(error: crate::core::backup::Error) -> Error {
    match error.downcast::<std::io::Error>() {
        Ok(error) => match error.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound,
            std::io::ErrorKind::WouldBlock => Error::Unavailable(error.to_string()),
            std::io::ErrorKind::InvalidData => Error::Corrupt(error.to_string()),
            _ => Error::Io(*error),
        },
        Err(error) => Error::Corrupt(error.to_string()),
    }
}
