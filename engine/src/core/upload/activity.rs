//! Lifetime ownership for admitted operations. Caller cancellation does not
//! abandon an owned worker or release its engine lease before I/O completes.
//! LINK GC independently revalidates fingerprints under the per-object gate;
//! the former mutable-namespace scan snapshots are no longer needed here.
use crate::core::{Error, Result, instance_lease::InstanceLease};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Notify;

#[derive(Default)]
struct State {
    active: usize,
    closing: bool,
}
#[derive(Default)]
pub(crate) struct Activity {
    state: Mutex<State>,
    changed: Notify,
    lease: OnceLock<Arc<InstanceLease>>,
}
struct Owner(Arc<Activity>);
#[derive(serde::Serialize)]
pub(crate) struct Status {
    pub active_operations: usize,
    pub draining: bool,
}
impl Drop for Owner {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active = state.active.saturating_sub(1);
        self.0.changed.notify_waiters();
    }
}
impl Activity {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn bind_lease(&self, lease: Arc<InstanceLease>) -> Result<()> {
        self.lease
            .set(lease)
            .map_err(|_| Error::Conflict("engine activity lease already bound".into()))
    }
    pub fn status(&self) -> Result<Status> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("engine activity lock poisoned".into()))?;
        Ok(Status {
            active_operations: state.active,
            draining: state.closing,
        })
    }
    pub fn spawn<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl std::future::Future<Output = Result<T>> + Send + 'static,
    ) -> Result<tokio::task::JoinHandle<Result<T>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("engine activity lock poisoned".into()))?;
        if state.closing {
            return Err(Error::Unavailable(
                "engine is draining admitted operations".into(),
            ));
        }
        state.active += 1;
        let owner = Owner(self.clone());
        // Registration and closing share the lock. Shutdown cannot observe
        // empty ownership between admission and the worker being spawned.
        let worker = tokio::spawn(async move {
            let _owner = owner;
            work.await
        });
        Ok(worker)
    }
    pub async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl std::future::Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T> {
        self.spawn(work)?
            .await
            .map_err(|e| Error::Unavailable(format!("owned operation failed: {e}")))?
    }
    pub fn close(&self) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| Error::Unavailable("engine activity lock poisoned".into()))?
            .closing = true;
        self.changed.notify_waiters();
        Ok(())
    }
    pub async fn wait_idle(&self) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            if self
                .state
                .lock()
                .map_err(|_| Error::Unavailable("engine activity lock poisoned".into()))?
                .active
                == 0
            {
                return Ok(());
            }
            changed.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancelled_caller_cannot_release_a_workers_control_lease_or_cancel_its_filesystem_write()
     {
        use std::{fs, io::Write};
        let fixture = crate::core::tests::Fixture::new();
        let root = fixture.base.join("control");
        fs::create_dir(&root).unwrap();
        let activity = Activity::new();
        let lease = Arc::new(InstanceLease::acquire(std::slice::from_ref(&root), false).unwrap());
        activity.bind_lease(lease.clone()).unwrap();
        drop(lease);
        let weak = Arc::downgrade(&activity);
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        let path = root.join("persisted");
        let output = path.clone();
        let caller = {
            let activity = activity.clone();
            tokio::spawn(async move {
                activity
                    .run(async move {
                        let _ = entered.send(());
                        resume
                            .await
                            .map_err(|_| Error::Unavailable("test release gone".into()))?;
                        tokio::task::spawn_blocking(move || {
                            let mut file = fs::File::create(output)?;
                            file.write_all(b"owned I/O finished")?;
                            file.sync_all()
                        })
                        .await
                        .map_err(|e| Error::Unavailable(e.to_string()))??;
                        Ok(())
                    })
                    .await
            })
        };
        entry.await.unwrap();
        activity.close().unwrap();
        assert!(activity.spawn(async { Ok(()) }).is_err());
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(activity);
        assert!(InstanceLease::acquire(std::slice::from_ref(&root), true).is_err());
        assert!(!path.exists());
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(fs::read(path).unwrap(), b"owned I/O finished");
        InstanceLease::acquire(&[root], true).unwrap();
    }
}
