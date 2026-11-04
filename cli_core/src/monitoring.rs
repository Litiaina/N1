//! Demand-driven observations. Clients share one sample and one admitted
//! collector; slow probes retain admission even after an HTTP caller leaves.
use serde::Serialize;
use serde_json::Value;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, Semaphore};

#[derive(Clone, Serialize)]
pub struct Observation {
    pub sampled_at: Option<u64>,
    pub age_seconds: Option<u64>,
    pub refreshing: bool,
    pub sequence: u64,
    pub interval_seconds: u64,
    #[serde(skip_serializing)]
    pub value: Option<Value>,
}
struct State {
    attempted: Option<Instant>,
    completed: Option<Instant>,
    sampled_at: Option<u64>,
    sequence: u64,
    value: Option<Value>,
}
pub struct ObservationCache {
    state: Mutex<State>,
    admission: Arc<Semaphore>,
    changed: Notify,
}
impl Default for ObservationCache {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                attempted: None,
                completed: None,
                sampled_at: None,
                sequence: 0,
                value: None,
            }),
            admission: Arc::new(Semaphore::new(1)),
            changed: Notify::new(),
        }
    }
}
impl ObservationCache {
    pub async fn observe<F, Fut>(self: &Arc<Self>, interval: Duration, collect: F) -> Observation
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Value> + Send + 'static,
    {
        let changed = self.changed.notified();
        let pending = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.attempted.is_none_or(|at| at.elapsed() >= interval)
                && let Ok(permit) = self.admission.clone().try_acquire_owned()
            {
                state.attempted = Some(Instant::now());
                let cache = self.clone();
                tokio::spawn(async move {
                    // Admission belongs to the collector, never to the requester.
                    let value = collect().await;
                    {
                        let mut state = cache.state.lock().unwrap_or_else(|e| e.into_inner());
                        state.value = Some(value);
                        state.completed = Some(Instant::now());
                        state.sampled_at = Some(now());
                        state.sequence += 1;
                    }
                    drop(permit);
                    cache.changed.notify_waiters();
                });
            }
            state.value.is_none()
        };
        // Cold observations have a bounded wait; later calls return the last
        // sample immediately, including its age and refreshing flag.
        if pending {
            let _ = tokio::time::timeout(Duration::from_secs(2), changed).await;
        }
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Observation {
            sampled_at: state.sampled_at,
            age_seconds: state.completed.map(|at| at.elapsed().as_secs()),
            refreshing: self.admission.available_permits() == 0,
            sequence: state.sequence,
            interval_seconds: interval.as_secs(),
            value: state.value.clone(),
        }
    }
}
pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[tokio::test]
    async fn concurrent_clients_share_one_collector_and_cached_sample() {
        let cache = Arc::new(ObservationCache::default());
        let count = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let (cache, count) = (cache.clone(), count.clone());
            tasks.push(tokio::spawn(async move {
                cache
                    .observe(Duration::from_secs(60), move || async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        serde_json::json!({"ok":true})
                    })
                    .await
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap().value.unwrap()["ok"], true);
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn cancelled_request_does_not_release_collector_admission() {
        let cache = Arc::new(ObservationCache::default());
        let (release, wait) = tokio::sync::oneshot::channel();
        let worker = cache.clone();
        let request = tokio::spawn(async move {
            worker
                .observe(Duration::ZERO, move || async move {
                    wait.await.unwrap();
                    serde_json::json!("complete")
                })
                .await
        });
        while cache.admission.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
        request.abort();
        let pending = cache
            .observe(Duration::ZERO, || async {
                panic!("second collector admitted")
            })
            .await;
        assert!(pending.refreshing);
        assert!(pending.value.is_none());
        release.send(()).unwrap();
        while cache.admission.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
        let sampled = cache
            .observe(Duration::from_secs(60), || async {
                panic!("cached sample ignored")
            })
            .await;
        assert_eq!(sampled.sequence, 1);
        assert_eq!(sampled.value.unwrap(), "complete");
    }
}
