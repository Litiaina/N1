//! Shared, per-upload exclusion for foreground operations and reclamation.
use n1_link::PoolId;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::RwLock;

#[derive(Default)]
pub(crate) struct UploadLocks(Mutex<HashMap<String, Weak<RwLock<()>>>>);

impl UploadLocks {
    pub(crate) fn get(&self, fragment: PoolId, version: &str) -> Arc<RwLock<()>> {
        let key = format!("{fragment}/{version}");
        let mut locks = self.0.lock().unwrap_or_else(|e| e.into_inner());
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
}
