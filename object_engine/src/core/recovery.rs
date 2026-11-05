//! Retained N1 recovery execution policy. LINK changes the shard reader, not
//! the four-job recovery bound or the separate CPU-affine recovery runtime.
use super::{Error, Result, maintenance::WorkControl};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

const RECOVERY_CONCURRENCY: usize = 4;
static RECOVERY_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(RECOVERY_CONCURRENCY)));

// Original N1 runtime/core-affinity policy, independent of native/HTTP workers.
static RECOVERY_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let total_cores = core_ids.len();
    let recovery_worker_count = (total_cores / 4).max(1);
    let pinned_cores = core_ids
        .into_iter()
        .rev()
        .take(recovery_worker_count)
        .collect::<Vec<_>>();
    let pinned_cores_arc = Arc::new(pinned_cores);
    let next_core_idx = Arc::new(AtomicUsize::new(0));
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(recovery_worker_count)
        .thread_name("n1-recovery")
        .on_thread_start(move || {
            if !pinned_cores_arc.is_empty() {
                let idx = next_core_idx.fetch_add(1, Ordering::Relaxed);
                let core = pinned_cores_arc[idx % pinned_cores_arc.len()];
                let _ = core_affinity::set_for_current(core);
            }
        })
        .enable_all()
        .build()
        .expect("Failed to build dedicated recovery runtime")
});

pub(crate) async fn admit(
    stop: &CancellationToken,
    control: Option<&WorkControl>,
) -> Result<OwnedSemaphorePermit> {
    let acquire = RECOVERY_SEMAPHORE.clone().acquire_owned();
    let stopped = || Error::Conflict("repair stopped or its maintenance window ended".into());
    let permit = if let Some(control) = control {
        tokio::select! {
            _ = stop.cancelled() => return Err(stopped()),
            _ = control.until_stopped() => return Err(stopped()),
            permit = acquire => permit,
        }
    } else {
        tokio::select! {
            _ = stop.cancelled() => return Err(stopped()),
            permit = acquire => permit,
        }
    }
    .map_err(|_| Error::Unavailable("recovery admission closed".into()))?;
    if stop.is_cancelled() || control.is_some_and(WorkControl::stopped) {
        return Err(stopped());
    }
    Ok(permit)
}
pub(crate) fn spawn(work: impl std::future::Future<Output = ()> + Send + 'static) {
    RECOVERY_RUNTIME.spawn(work);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn retained_recovery_runtime_is_separate_from_request_workers() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        spawn(async move {
            tx.send(std::thread::current().name().unwrap_or("").to_owned())
                .unwrap();
        });
        assert_eq!(rx.await.unwrap(), "n1-recovery");
    }

    struct ObservedWriter {
        bytes: Vec<u8>,
        expected: usize,
        ready: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl tokio::io::AsyncWrite for ObservedWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.bytes.extend_from_slice(bytes);
            if self.bytes.len() == self.expected
                && let Some(ready) = self.ready.take()
            {
                let _ = ready.send(());
            }
            std::task::Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_recovery_budget_backpressures_and_does_not_silently_skip_verified_read_repairs()
    {
        use crate::core::tests::Fixture;
        use n1_link::Durability;
        let fixture = Fixture::new();
        let store = fixture.enroll();
        let scope = [11; 16];
        let mut objects = Vec::new();
        for index in 0..3u8 {
            let bytes = vec![index + 31; 8193 + usize::from(index)];
            let init = store
                .init(
                    scope,
                    0,
                    bytes.len() as u64,
                    12288,
                    Durability::Durable,
                    3600,
                )
                .await
                .unwrap();
            store
                .put(
                    scope,
                    init.object_key.clone(),
                    std::io::Cursor::new(bytes.clone()),
                )
                .await
                .unwrap();
            let fp = store
                .inspect_upload(scope, &init.object_key.clone())
                .await
                .unwrap()
                .identity
                .fingerprint();
            let target = store.pools[0]
                .holder(fp, 0)
                .unwrap()
                .path
                .join(fp.shard_path(0));
            std::fs::remove_file(target).unwrap();
            objects.push((init.object_key.clone(), bytes));
        }
        store.configure_read_repair(true);
        let held = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            RECOVERY_SEMAPHORE.clone().acquire_many_owned(4),
        )
        .await
        .unwrap()
        .unwrap();
        let mut downloads = Vec::new();
        let mut ready = Vec::new();
        for (key, bytes) in &objects {
            let (tx, rx) = tokio::sync::oneshot::channel();
            ready.push(rx);
            let writer = ObservedWriter {
                bytes: Vec::new(),
                expected: bytes.len(),
                ready: Some(tx),
            };
            let store = store.clone();
            let key = key.clone();
            downloads.push(tokio::spawn(async move {
                store.download(scope, key, 0, None, writer).await
            }));
        }
        for result in tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures_util::future::join_all(ready),
        )
        .await
        .unwrap()
        {
            result.unwrap();
        }
        assert!(downloads.iter().all(|download| !download.is_finished()));
        drop(held);
        for (download, (_, expected)) in downloads.into_iter().zip(&objects) {
            let (writer, _) = tokio::time::timeout(std::time::Duration::from_secs(10), download)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(&writer.bytes, expected);
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let mut complete = true;
                for (key, _) in &objects {
                    complete &= store
                        .object_health(scope, key.clone(), true)
                        .await
                        .unwrap()
                        .status
                        == "healthy";
                }
                if complete {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        store.repair_manager.close().unwrap();
        store.repair_manager.wait_idle().await.unwrap();
    }
}
