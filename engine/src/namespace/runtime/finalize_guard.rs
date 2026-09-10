//! The existing per-path publication gate belongs to the blocking publisher,
//! not its HTTP/native caller. Cancellation must not unlock an active worker.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::enums::app_error_enum::AppError;

struct Entry {
    lock: Arc<AsyncMutex<()>>,
    users: usize,
}

static LOCKS: LazyLock<Mutex<HashMap<String, Entry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(super) struct FinalizeGuard {
    key: String,
    lock: Arc<AsyncMutex<()>>,
    owned: Option<OwnedMutexGuard<()>>,
}

impl FinalizeGuard {
    pub(super) async fn acquire(key: String) -> Result<Self, AppError> {
        // Only bookkeeping is synchronous. No filesystem work, waiting for a
        // path lock, or other await occurs while the registry is locked.
        let lock = {
            let mut locks = LOCKS.lock().map_err(|_| {
                AppError::InternalServerError("namespace publication registry poisoned".into())
            })?;
            let entry = locks.entry(key.clone()).or_insert_with(|| Entry {
                lock: Arc::new(AsyncMutex::new(())),
                users: 0,
            });
            entry.users = entry.users.checked_add(1).ok_or_else(|| {
                AppError::InternalServerError("namespace publication admission overflow".into())
            })?;
            entry.lock.clone()
        };
        // Register ownership before waiting so cancellation of a queued waiter
        // unregisters it too. Explicit users avoid Arc drop/count races.
        let mut guard = Self {
            key,
            lock,
            owned: None,
        };
        guard.owned = Some(guard.lock.clone().lock_owned().await);
        Ok(guard)
    }
}

impl Drop for FinalizeGuard {
    fn drop(&mut self) {
        self.owned.take();
        if let Ok(mut locks) = LOCKS.lock() {
            if let Some(entry) = locks.get_mut(&self.key) {
                debug_assert!(Arc::ptr_eq(&entry.lock, &self.lock));
                entry.users -= 1;
                if entry.users == 0 {
                    locks.remove(&self.key);
                }
            }
        }
    }
}

#[cfg(test)]
pub(super) fn lock_for_test(key: &str) -> Option<Arc<AsyncMutex<()>>> {
    LOCKS
        .lock()
        .unwrap()
        .get(key)
        .map(|entry| entry.lock.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[tokio::test]
    async fn publication_worker_unwind_releases_gate_and_registry_entry() {
        let key = format!("finalize-unwind-{}", ulid::Ulid::new());
        let guard = FinalizeGuard::acquire(key.clone()).await.unwrap();
        let worker = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            panic!("injected publication worker failure");
        });
        assert!(worker.await.unwrap_err().is_panic());
        assert!(lock_for_test(&key).is_none());
        drop(FinalizeGuard::acquire(key.clone()).await.unwrap());
        assert!(lock_for_test(&key).is_none());
    }

    #[tokio::test]
    async fn cancelled_waiters_unregister_without_blocking_other_paths() {
        let key = format!("finalize-waiter-{}", ulid::Ulid::new());
        let owner = FinalizeGuard::acquire(key.clone()).await.unwrap();
        let mut waiters = Vec::new();
        for _ in 0..16 {
            let mut waiter = Box::pin(FinalizeGuard::acquire(key.clone()));
            assert!(waiter.as_mut().now_or_never().is_none());
            waiters.push(waiter);
        }
        assert_eq!(LOCKS.lock().unwrap()[&key].users, 17);
        let other_key = format!("{key}-independent");
        let other = FinalizeGuard::acquire(other_key.clone())
            .now_or_never()
            .unwrap()
            .unwrap();
        drop(waiters);
        assert_eq!(LOCKS.lock().unwrap()[&key].users, 1);
        assert!(lock_for_test(&key).unwrap().try_lock().is_err());
        drop(owner);
        drop(other);
        assert!(lock_for_test(&key).is_none());
        assert!(lock_for_test(&other_key).is_none());
    }
}
