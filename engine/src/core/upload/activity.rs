//! Process-local protection against orphan GC racing an upload. No receipts,
//! payload buffering or locks held across I/O. A scan remembers uploads that
//! overlap it even after they finish, since its namespace snapshot is older.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use crate::enums::app_error_enum::AppError;

const MAX_SCAN_OBJECTS: usize = 65_536;

#[derive(Default)]
struct ObjectActivity {
    uploads: usize,
    reclaiming: bool,
}

#[derive(Default)]
struct ScanState {
    objects: HashSet<String>,
    saturated: bool,
}

impl ScanState {
    fn protect(&mut self, object: &str) {
        if self.saturated || self.objects.contains(object) {
            return;
        }
        if self.objects.len() == MAX_SCAN_OBJECTS {
            // Bound memory without slowing uploads or deleting from an unsafe
            // snapshot. This scan defers its remaining orphan reclamation.
            self.saturated = true;
            return;
        }
        self.objects.insert(object.to_owned());
    }
}

#[derive(Default)]
struct FragmentActivity {
    objects: HashMap<String, ObjectActivity>,
    scans: HashMap<u64, ScanState>,
    next_scan: u64,
}

type Registry = HashMap<String, Arc<Mutex<FragmentActivity>>>;
static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, AppError> {
    mutex
        .lock()
        .map_err(|_| AppError::InternalServerError("upload activity lock poisoned".into()))
}

fn prune_registry(fragment: &str, state: &Arc<Mutex<FragmentActivity>>) {
    // Admission registers activity before releasing REGISTRY. Thus an empty
    // state cannot have a pending admission, even during simultaneous drops.
    // Arc reference counts alone cannot establish that invariant.
    if let Ok(mut registry) = REGISTRY.lock() {
        if registry
            .get(fragment)
            .is_some_and(|current| Arc::ptr_eq(current, state))
        {
            if let Ok(current) = state.lock() {
                if current.objects.is_empty() && current.scans.is_empty() {
                    registry.remove(fragment);
                }
            }
        }
    }
}

pub(crate) struct UploadActivityGuard {
    fragment: String,
    object: String,
    state: Arc<Mutex<FragmentActivity>>,
}

impl UploadActivityGuard {
    pub(super) fn active(fragment: &str, object: &str) -> Result<bool, AppError> {
        let object = crate::core::index_store::normalize_object_id(object)?;
        let state = lock(&REGISTRY)?.get(fragment).cloned();
        let Some(state) = state else { return Ok(false) };
        let active = lock(&state)?
            .objects
            .get(&object)
            .is_some_and(|value| value.uploads != 0);
        prune_registry(fragment, &state);
        Ok(active)
    }

    pub(crate) fn begin(fragment: &str, object: &str) -> Result<Self, AppError> {
        let object = crate::core::index_store::normalize_object_id(object)?;
        let mut registry = lock(&REGISTRY)?;
        let state = registry.entry(fragment.to_owned()).or_default().clone();
        {
            let mut current = lock(&state)?;
            let activity = current.objects.entry(object.to_owned()).or_default();
            if activity.reclaiming {
                return Err(AppError::Conflict(
                    "Expired upload is being reclaimed; retry the upload".into(),
                ));
            }
            activity.uploads += 1;
            for scan in current.scans.values_mut() {
                scan.protect(&object);
            }
        }
        drop(registry);
        Ok(Self {
            fragment: fragment.to_owned(),
            object: object.to_owned(),
            state,
        })
    }
}

impl Drop for UploadActivityGuard {
    fn drop(&mut self) {
        if let Ok(mut current) = self.state.lock() {
            if let Some(activity) = current.objects.get_mut(&self.object) {
                activity.uploads -= 1;
                if activity.uploads == 0 && !activity.reclaiming {
                    current.objects.remove(&self.object);
                }
            }
        }
        prune_registry(&self.fragment, &self.state);
    }
}

pub(crate) struct UploadGcScan {
    fragment: String,
    id: u64,
    state: Arc<Mutex<FragmentActivity>>,
}

impl UploadGcScan {
    /// Must precede the durable-session and namespace snapshots.
    pub(crate) fn begin(fragment: &str) -> Result<Self, AppError> {
        let mut registry = lock(&REGISTRY)?;
        let state = registry.entry(fragment.to_owned()).or_default().clone();
        let id;
        {
            let mut current = lock(&state)?;
            id = current.next_scan;
            current.next_scan = id.checked_add(1).ok_or_else(|| {
                AppError::InternalServerError("upload GC scan id exhausted".into())
            })?;
            let mut scan = ScanState::default();
            for (object, activity) in &current.objects {
                if activity.uploads != 0 {
                    scan.protect(object);
                }
            }
            current.scans.insert(id, scan);
        }
        drop(registry);
        Ok(Self {
            fragment: fragment.to_owned(),
            id,
            state,
        })
    }

    /// Atomically excludes uploads while reclaiming this one orphan. Uploads
    /// of other objects never wait for its filesystem work.
    pub(crate) fn try_reclaim(&self, object: &str) -> Result<Option<ReclaimGuard>, AppError> {
        let object = crate::core::index_store::normalize_object_id(object)?;
        let mut current = lock(&self.state)?;
        let scan = current
            .scans
            .get(&self.id)
            .expect("live scan is registered");
        if scan.saturated || scan.objects.contains(&object) {
            return Ok(None);
        }
        let activity = current.objects.entry(object.to_owned()).or_default();
        if activity.uploads != 0 || activity.reclaiming {
            return Ok(None);
        }
        activity.reclaiming = true;
        Ok(Some(ReclaimGuard {
            fragment: self.fragment.clone(),
            object: object.to_owned(),
            state: self.state.clone(),
        }))
    }
}

impl Drop for UploadGcScan {
    fn drop(&mut self) {
        if let Ok(mut current) = self.state.lock() {
            if current
                .scans
                .get(&self.id)
                .is_some_and(|scan| scan.saturated)
            {
                tracing::info!(fragment = %self.fragment, "GC upload-protection budget reached; remaining orphan reclamation deferred");
            }
            current.scans.remove(&self.id);
        }
        prune_registry(&self.fragment, &self.state);
    }
}

pub(crate) struct ReclaimGuard {
    fragment: String,
    object: String,
    state: Arc<Mutex<FragmentActivity>>,
}

impl Drop for ReclaimGuard {
    fn drop(&mut self) {
        if let Ok(mut current) = self.state.lock() {
            current.objects.remove(&self.object);
        }
        prune_registry(&self.fragment, &self.state);
    }
}

#[cfg(test)]
mod tests;
