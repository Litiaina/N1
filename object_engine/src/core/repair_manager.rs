mod object_repair;
pub use object_repair::RepairResult;

use super::{Error, Result};
use n1_link::Fingerprint;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct State {
    jobs: usize,
    // Only active/queued read repairs; the bool coalesces a further request.
    // Entries leave on completion. This is not an object catalogue.
    reads: HashMap<Fingerprint, bool>,
    closing: bool,
}
#[derive(Default)]
pub(crate) struct RepairManager {
    state: Mutex<State>,
    changed: Notify,
    pub stop: CancellationToken,
}
pub(crate) struct RepairJob {
    owner: Arc<RepairManager>,
    fingerprint: Option<Fingerprint>,
    active: bool,
}
impl RepairManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn register(self: &Arc<Self>) -> Result<RepairJob> {
        self.begin(None)?
            .ok_or_else(|| Error::Unavailable("recovery is stopping".into()))
    }
    pub fn read(self: &Arc<Self>, fp: Fingerprint) -> Result<Option<RepairJob>> {
        self.begin(Some(fp))
    }
    fn begin(self: &Arc<Self>, fingerprint: Option<Fingerprint>) -> Result<Option<RepairJob>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("repair manager poisoned".into()))?;
        if state.closing || self.stop.is_cancelled() {
            return Err(Error::Unavailable("recovery is stopping".into()));
        }
        if let Some(fp) = fingerprint {
            if let Some(rerun) = state.reads.get_mut(&fp) {
                *rerun = true;
                return Ok(None);
            }
            state.reads.insert(fp, false);
        }
        state.jobs += 1;
        Ok(Some(RepairJob {
            owner: self.clone(),
            fingerprint,
            active: true,
        }))
    }
    pub fn close(&self) -> Result<()> {
        self.stop.cancel();
        self.state
            .lock()
            .map_err(|_| Error::Unavailable("repair manager poisoned".into()))?
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
                .map_err(|_| Error::Unavailable("repair manager poisoned".into()))?
                .jobs
                == 0
            {
                return Ok(());
            }
            changed.await;
        }
    }
}

impl RepairJob {
    pub fn finish_or_again(&mut self) -> Result<bool> {
        let mut state = self
            .owner
            .state
            .lock()
            .map_err(|_| Error::Unavailable("repair manager poisoned".into()))?;
        if let Some(fp) = self.fingerprint {
            if !state.closing && state.reads.get(&fp) == Some(&true) {
                state.reads.insert(fp, false);
                return Ok(true);
            }
            state.reads.remove(&fp);
        }
        state.jobs -= 1;
        self.active = false;
        self.owner.changed.notify_waiters();
        Ok(false)
    }
}
impl Drop for RepairJob {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        // Recover a poisoned lock only to release ownership and wake shutdown.
        let mut state = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(fp) = self.fingerprint {
            state.reads.remove(&fp);
        }
        state.jobs = state.jobs.saturating_sub(1);
        self.owner.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn coalesced_requests_are_not_lost_at_completion_and_shutdown_drains_owners() {
        let manager = RepairManager::new();
        let fp = Fingerprint([7; 32]);
        let mut first = manager.read(fp).unwrap().unwrap();
        assert!(manager.read(fp).unwrap().is_none());
        assert!(first.finish_or_again().unwrap());
        assert!(!first.finish_or_again().unwrap());
        let second = manager.read(fp).unwrap().unwrap();
        drop(first); // An ended owner must not unregister its replacement.
        assert!(manager.read(fp).unwrap().is_none());
        let manual = manager.register().unwrap();
        manager.close().unwrap();
        assert!(manager.stop.is_cancelled());
        assert!(manager.read(fp).is_err());
        assert!(manager.register().is_err());
        let waiting = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.wait_idle().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(second);
        drop(manual);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let state = manager.state.lock().unwrap();
        assert_eq!(state.jobs, 0);
        assert!(state.reads.is_empty());
    }
}
