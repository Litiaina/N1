//! Whole-fragment deletion is exclusive; uploads and repair share admission.
//! This is per fragment, never a single-writer lock for object mutations.

use crate::enums::app_error_enum::AppError;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll, Wake};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock as AsyncRwLock};

static LOCKS: LazyLock<Mutex<HashMap<String, Arc<AsyncRwLock<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) struct FragmentLifecycleGuard {
    fragment: String,
    lock: Option<Arc<AsyncRwLock<()>>>,
    read: Option<OwnedRwLockReadGuard<()>>,
    write: Option<OwnedRwLockWriteGuard<()>>,
}

impl FragmentLifecycleGuard {
    fn new(fragment: &str) -> Result<Self, AppError> {
        let mut locks = LOCKS.lock().map_err(|_| {
            AppError::InternalServerError("fragment lifecycle lock poisoned".into())
        })?;
        let lock = locks
            .entry(fragment.to_owned())
            .or_insert_with(|| Arc::new(AsyncRwLock::new(())))
            .clone();
        Ok(Self {
            fragment: fragment.to_owned(),
            lock: Some(lock),
            read: None,
            write: None,
        })
    }
}

impl Drop for FragmentLifecycleGuard {
    fn drop(&mut self) {
        let Ok(mut locks) = LOCKS.lock() else {
            return;
        };
        let same_lock = locks.get(&self.fragment).is_some_and(|lock| {
            self.lock
                .as_ref()
                .is_some_and(|owned| Arc::ptr_eq(lock, owned))
        });
        // Release both references under the registry mutex: a woken waiter
        // must not finish cleanup while this guard still holds its Arc.
        drop(self.read.take());
        drop(self.write.take());
        drop(self.lock.take());
        if same_lock
            && locks
                .get(&self.fragment)
                .is_some_and(|lock| Arc::strong_count(lock) == 1)
        {
            locks.remove(&self.fragment);
        }
    }
}

pub(crate) async fn acquire(fragment: &str) -> Result<FragmentLifecycleGuard, AppError> {
    let mut guard = FragmentLifecycleGuard::new(fragment)?;
    guard.write = Some(guard.lock.as_ref().unwrap().clone().write_owned().await);
    Ok(guard)
}

/// Only call from synchronous workers, never from a Tokio runtime thread.
#[cfg(test)]
pub(crate) fn acquire_blocking(fragment: &str) -> Result<FragmentLifecycleGuard, AppError> {
    let mut guard = FragmentLifecycleGuard::new(fragment)?;
    guard.write = Some(block_on_lock(
        guard.lock.as_ref().unwrap().clone().write_owned(),
    ));
    Ok(guard)
}

pub(crate) async fn acquire_shared(fragment: &str) -> Result<FragmentLifecycleGuard, AppError> {
    let mut guard = FragmentLifecycleGuard::new(fragment)?;
    guard.read = Some(guard.lock.as_ref().unwrap().clone().read_owned().await);
    Ok(guard)
}

pub(crate) fn acquire_shared_blocking(fragment: &str) -> Result<FragmentLifecycleGuard, AppError> {
    let mut guard = FragmentLifecycleGuard::new(fragment)?;
    guard.read = Some(block_on_lock(
        guard.lock.as_ref().unwrap().clone().read_owned(),
    ));
    Ok(guard)
}

// Tokio offers blocking borrowed RwLock guards, but not owned ones. This small
// adapter parks only the synchronous worker and preserves owned-guard cleanup.
fn block_on_lock<T>(future: impl Future<Output = T>) -> T {
    struct Worker(std::thread::Thread);
    impl Wake for Worker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Arc::new(Worker(std::thread::current())).into();
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}

/// Recheck the existing on-disk fragment identity *after* admission. A valid
/// JWT alone must not allow mkdir to resurrect a deleted fragment.
pub(crate) async fn acquire_live(fragment: &str) -> Result<FragmentLifecycleGuard, AppError> {
    let fragment = crate::core::index_store::stable_fragment_id_for_fragment(fragment);
    let roots = crate::core::disk_manager::ObjectPathManager::new().get_all_index_roots();
    acquire_live_on_roots(&fragment, &roots).await
}

async fn acquire_live_on_roots(
    fragment: &str,
    roots: &[std::path::PathBuf],
) -> Result<FragmentLifecycleGuard, AppError> {
    let guard = acquire_shared(&fragment).await?;
    require_live_on_roots(fragment, roots).await?;
    Ok(guard)
}

/// Read-triggered repair must not queue behind a pending deletion while its
/// caller may itself hold shared admission. Defer instead; a later read retries.
pub(crate) async fn try_acquire_live(
    fragment: &str,
) -> Result<Option<FragmentLifecycleGuard>, AppError> {
    let fragment = crate::core::index_store::stable_fragment_id_for_fragment(fragment);
    let mut guard = FragmentLifecycleGuard::new(&fragment)?;
    let Ok(read) = guard.lock.as_ref().unwrap().clone().try_read_owned() else {
        return Ok(None);
    };
    guard.read = Some(read);
    let roots = crate::core::disk_manager::ObjectPathManager::new().get_all_index_roots();
    require_live_on_roots(&fragment, &roots).await?;
    Ok(Some(guard))
}

async fn require_live_on_roots(
    fragment: &str,
    roots: &[std::path::PathBuf],
) -> Result<(), AppError> {
    for root in roots {
        let path = crate::core::fragment_identity::physical_fragment_root_on(&root, &fragment)
            .join(".fragment.ndx");
        match tokio::fs::metadata(&path).await {
            Ok(meta) if meta.is_file() => return Ok(()),
            Ok(_) => return Err(AppError::Conflict("invalid fragment identity file".into())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(AppError::Io(e, "check live fragment identity".into())),
        }
    }
    Err(AppError::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn shared_uploads_overlap_but_deletion_waits_and_cancelled_waiters_are_pruned() {
        let fragment = ulid::Ulid::new().to_string();
        let first = acquire_shared(&fragment).await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(100), acquire_shared(&fragment))
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), acquire(&fragment))
                .await
                .is_err()
        );
        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), acquire(&fragment))
                .await
                .is_err()
        );
        drop(second);
        let deletion = acquire(&fragment).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), acquire_shared(&fragment))
                .await
                .is_err()
        );
        drop(deletion);
        assert!(!LOCKS.lock().unwrap().contains_key(&fragment));
    }

    #[tokio::test]
    async fn queued_upload_rechecks_identity_after_deletion_without_recreating_it() {
        let fragment = ulid::Ulid::new().to_string();
        let root = std::env::temp_dir().join(format!("n1-live-check-{fragment}"));
        let dir = crate::core::fragment_identity::physical_fragment_root_on(&root, &fragment);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(".fragment.ndx"), b"identity fixture").unwrap();
        let roots = vec![root.clone()];
        drop(acquire_live_on_roots(&fragment, &roots).await.unwrap());
        let deletion = acquire(&fragment).await.unwrap();
        let mut upload = std::pin::pin!(acquire_live_on_roots(&fragment, &roots));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut upload)
                .await
                .is_err()
        );
        std::fs::remove_dir_all(&dir).unwrap();
        drop(deletion);
        assert!(matches!(upload.await, Err(AppError::NotFound)));
        assert!(!dir.exists());
        assert!(!LOCKS.lock().unwrap().contains_key(&fragment));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn lifecycle_is_per_fragment_and_releases_registry_entries() {
        let fragment = format!("lifecycle-{}", ulid::Ulid::new());
        let guard = acquire(&fragment).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), acquire(&fragment))
                .await
                .is_err()
        );
        let other = acquire(&format!("{fragment}-other")).await.unwrap();
        drop(other);
        drop(guard);
        assert!(!LOCKS.lock().unwrap().contains_key(&fragment));
        drop(acquire(&fragment).await.unwrap());
        assert!(!LOCKS.lock().unwrap().contains_key(&fragment));
    }

    #[tokio::test]
    async fn blocking_worker_keeps_guard_after_caller_is_cancelled() {
        let fragment = format!("lifecycle-cancel-{}", ulid::Ulid::new());
        let worker_fragment = fragment.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let caller = tokio::spawn(async move {
            let guard = acquire(&worker_fragment).await.unwrap();
            tokio::task::spawn_blocking(move || {
                let _guard = guard;
                started_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
            })
            .await
            .unwrap();
        });
        started_rx.await.unwrap();
        caller.abort();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), acquire(&fragment))
                .await
                .is_err()
        );
        finish_tx.send(()).unwrap();
        drop(acquire(&fragment).await.unwrap());
        assert!(!LOCKS.lock().unwrap().contains_key(&fragment));
    }
}
