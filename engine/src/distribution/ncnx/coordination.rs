use super::*;
use crate::core::fragment_identity::{PHYSICAL_FRAGMENT_PREFIX, physical_fragment_dir_name};
use std::sync::{Mutex, Weak};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

type Key = (String, String);

// Receiver-local serialization, not distributed mutation authority. A repair
// must not replace a valid generation between a local create/remove and its
// response. Weak entries exist only while an owner or waiter uses that key.
static KEY_LOCKS: once_cell::sync::Lazy<Mutex<BTreeMap<Key, Weak<KeyLock>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(BTreeMap::new()));

struct KeyLock {
    key: Key,
    mutex: Arc<AsyncMutex<()>>,
}

impl Drop for KeyLock {
    fn drop(&mut self) {
        if let Ok(mut locks) = KEY_LOCKS.lock() {
            if locks
                .get(&self.key)
                .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
            {
                locks.remove(&self.key);
            }
        }
    }
}

pub(super) struct KeyGuards {
    // Drop mutex guards before their registry owners. The final KeyLock owner
    // prunes its own weak entry, including all cancelled pending acquisitions.
    guards: Vec<OwnedMutexGuard<()>>,
    entries: Vec<Arc<KeyLock>>,
}

pub(super) fn canonical_fragment(fragment_id: &str) -> Result<String, AppError> {
    let stable_id = fragment_id
        .strip_prefix(PHYSICAL_FRAGMENT_PREFIX)
        .unwrap_or(fragment_id);
    let physical_fragment = physical_fragment_dir_name(fragment_id);
    if stable_id.trim().is_empty()
        || matches!(stable_id, "." | "..")
        || fragment_id.contains(['/', '\\', '\0'])
        || physical_fragment.len() > 255
    {
        return Err(AppError::BadRequest(
            "NCNX fragment must be a valid single directory identity".into(),
        ));
    }
    Ok(physical_fragment)
}

pub(super) async fn lock_keys<'a>(
    fragment_id: &str,
    logical_keys: impl IntoIterator<Item = &'a str>,
) -> Result<KeyGuards, AppError> {
    let physical_fragment = canonical_fragment(fragment_id)?;
    let keys = logical_keys
        .into_iter()
        .map(|key| validate_logical_key(key).map(|key| (physical_fragment.clone(), key)))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let entries = {
        let mut locks = KEY_LOCKS.lock().map_err(|_| {
            AppError::InternalServerError("NCNX key lock registry is poisoned".into())
        })?;
        keys.into_iter()
            .map(|key| {
                let lock = locks.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
                    let lock = Arc::new(KeyLock {
                        key: key.clone(),
                        mutex: Arc::new(AsyncMutex::new(())),
                    });
                    locks.insert(key.clone(), Arc::downgrade(&lock));
                    lock
                });
                lock
            })
            .collect::<Vec<_>>()
    };
    let mut held = KeyGuards {
        guards: Vec::with_capacity(entries.len()),
        entries,
    };
    // Sorted acquisition prevents peer batches with overlapping keys from
    // deadlocking. Waiting uses no blocking filesystem worker or I/O permit.
    for entry in &held.entries {
        held.guards.push(entry.mutex.clone().lock_owned().await);
    }
    Ok(held)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn same_key_waits_other_keys_progress_and_owned_entries_are_reclaimed() {
        let fragment = format!("locks-{}", ulid::Ulid::new());
        let held = lock_keys(&fragment, ["key-a"]).await.unwrap();
        let waiting_fragment = fragment.clone();
        let waiter = tokio::spawn(async move {
            lock_keys(&waiting_fragment, ["key-b", "key-a"])
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        let unrelated =
            tokio::time::timeout(Duration::from_secs(1), lock_keys(&fragment, ["key-c"]))
                .await
                .expect("unrelated key must not wait")
                .unwrap();
        drop(held);
        let acquired = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("same key progresses after release")
            .unwrap();
        drop(acquired);
        drop(unrelated);
        let locks = KEY_LOCKS.lock().unwrap();
        assert!(
            !locks
                .keys()
                .any(|(id, _)| id == &physical_fragment_dir_name(&fragment))
        );
    }

    #[tokio::test]
    async fn cancelled_batch_waiter_does_not_retain_key_entries() {
        let fragment = format!("cancel-locks-{}", ulid::Ulid::new());
        let held = lock_keys(&fragment, ["key-b"]).await.unwrap();
        let waiting_fragment = fragment.clone();
        let waiter =
            tokio::spawn(
                async move { lock_keys(&waiting_fragment, ["key-a", "key-b", "key-c"]).await },
            );
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        waiter.abort();
        assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
        drop(held);
        let locks = KEY_LOCKS.lock().unwrap();
        assert!(
            !locks
                .keys()
                .any(|(id, _)| id == &physical_fragment_dir_name(&fragment))
        );
    }

    #[tokio::test]
    async fn physical_fragment_alias_uses_the_same_key_guard_and_invalid_ids_register_nothing() {
        let fragment = format!("alias-locks-{}", ulid::Ulid::new());
        let physical = physical_fragment_dir_name(&fragment);
        let held = lock_keys(&fragment, ["key"]).await.unwrap();
        let alias = physical.clone();
        let waiter = tokio::spawn(async move { lock_keys(&alias, ["key"]).await.unwrap() });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if KEY_LOCKS
                    .lock()
                    .unwrap()
                    .get(&(physical.clone(), "key".into()))
                    .is_some_and(|entry| entry.strong_count() >= 2)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("alias waiter must reach the same registry entry");
        assert!(!waiter.is_finished());
        drop(held);
        drop(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .unwrap()
                .unwrap(),
        );
        let invalid_key = format!("invalid-fragment-check-{}", ulid::Ulid::new());
        for invalid in [
            "",
            ".",
            "..",
            "@f_",
            "@f_.",
            "@f_..",
            "a/b",
            "a\\b",
            "a\0b",
            "/absolute",
        ] {
            assert!(matches!(
                lock_keys(invalid, [invalid_key.as_str()]).await,
                Err(AppError::BadRequest(_))
            ));
        }
        let too_long = "x".repeat(253);
        assert!(matches!(
            lock_keys(&too_long, [invalid_key.as_str()]).await,
            Err(AppError::BadRequest(_))
        ));
        let locks = KEY_LOCKS.lock().unwrap();
        assert!(
            !locks
                .keys()
                .any(|(id, key)| id == &physical || key == &invalid_key)
        );
    }
}
