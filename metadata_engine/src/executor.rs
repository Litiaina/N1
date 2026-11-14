use crate::{Error, MetadataEngine, Result};
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};

/// Bounded bridge from async request handling to synchronous database work.
/// The worker retains admission and the database when its caller is cancelled.
#[derive(Clone)]
pub struct MetadataExecutor {
    engine: MetadataEngine,
    permits: Arc<Semaphore>,
    lifecycle: Arc<RwLock<()>>,
}

impl MetadataExecutor {
    /// Read-only sampling uses its own single admitted worker, rather than
    /// competing for the transaction executor's foreground permits.
    pub(crate) fn observation_handle(&self) -> MetadataEngine {
        self.engine.clone()
    }
    pub fn new(engine: MetadataEngine, capacity: usize) -> Result<Self> {
        if capacity == 0 || capacity > 1024 {
            return Err(Error::Invalid(
                "metadata worker capacity must be 1–1024".into(),
            ));
        }
        Ok(Self {
            engine,
            permits: Arc::new(Semaphore::new(capacity)),
            lifecycle: Arc::new(RwLock::new(())),
        })
    }

    pub async fn execute<T: Send + 'static>(
        &self,
        work: impl FnOnce(MetadataEngine) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        #[cfg(feature = "upload-profile")]
        let timing = (
            crate::upload_profile::Trace::current(),
            std::time::Instant::now(),
        );
        let lifecycle = self.lifecycle.clone().read_owned().await;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => Error::Closed,
                tokio::sync::TryAcquireError::NoPermits => Error::Busy,
            })?;
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || {
            let _lifecycle = lifecycle;
            let _permit = permit;
            #[cfg(feature = "upload-profile")]
            let started = std::time::Instant::now();
            #[cfg(feature = "upload-profile")]
            let _context = crate::upload_profile::WorkerContext::enter(timing.0.clone());
            let result = work(engine);
            #[cfg(feature = "upload-profile")]
            if let (Some(trace), queued) = timing {
                trace.record("metadata.worker_queue", queued, started);
                trace.record("metadata.worker_run", started, std::time::Instant::now());
            }
            result
        })
        .await
        .map_err(|error| Error::Worker(format!("metadata worker failed: {error}")))?
    }

    /// Stops admission across every clone, then waits for owned workers. All
    /// accepted mutation transactions retain their normal durability semantics.
    pub async fn shutdown(&self) {
        self.permits.close();
        let _drained = self.lifecycle.write().await;
    }
}
