//! Snapshot boundary only. Shared participants are independent mutations, not
//! a single writer/cache. Exclusive admission only builds a pinned backup view.
use std::cell::Cell;
use std::future::Future;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll, Wake};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

static BOUNDARY: LazyLock<Arc<RwLock<()>>> = LazyLock::new(|| Arc::new(RwLock::new(())));
thread_local! { static DEPTH: Cell<usize> = const { Cell::new(0) }; }
pub(crate) struct Mutation {
    _guard: Option<OwnedRwLockReadGuard<()>>,
    _local: std::marker::PhantomData<*mut ()>,
}
impl Drop for Mutation {
    fn drop(&mut self) {
        DEPTH.with(|d| d.set(d.get() - 1));
    }
}
pub(crate) fn mutation() -> Mutation {
    let guard = DEPTH.with(|depth| {
        let guard = (depth.get() == 0).then(|| wait(BOUNDARY.clone().read_owned()));
        depth.set(depth.get() + 1);
        guard
    });
    Mutation {
        _guard: guard,
        _local: std::marker::PhantomData,
    }
}
pub(crate) async fn mutation_async() -> OwnedRwLockReadGuard<()> {
    BOUNDARY.clone().read_owned().await
}
pub(crate) fn freeze(seconds: u64) -> super::Result<OwnedRwLockWriteGuard<()>> {
    let end = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
    struct Worker(std::thread::Thread);
    impl Wake for Worker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Arc::new(Worker(std::thread::current())).into();
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(tokio::task::unconstrained(BOUNDARY.clone().write_owned()));
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(guard) => return Ok(guard),
            Poll::Pending => {
                let Some(remaining) = end.checked_duration_since(std::time::Instant::now()) else {
                    return Err(super::invalid(
                        "online capture boundary admission timed out",
                    ));
                };
                std::thread::park_timeout(remaining);
            }
        }
    }
}
fn wait<T>(future: impl Future<Output = T>) -> T {
    struct Worker(std::thread::Thread);
    impl Wake for Worker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Arc::new(Worker(std::thread::current())).into();
    let mut context = Context::from_waker(&waker);
    // This adapter parks a thread, not an async task. A cooperative yield
    // cannot replenish its task budget here; only real contention may wait.
    let mut future = std::pin::pin!(tokio::task::unconstrained(future));
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blocking_admission_completes_with_an_exhausted_async_task_budget() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::spawn(async {
                for _ in 0..1024 {
                    if !tokio::task::coop::has_budget_remaining() {
                        break;
                    }
                    tokio::task::consume_budget().await;
                }
                assert!(!tokio::task::coop::has_budget_remaining());
                let lock = Arc::new(RwLock::new(()));
                let mut admission = std::pin::pin!(lock.read_owned());
                let mut polls = 0;
                let acquired = wait(std::future::poll_fn(|cx| {
                    polls += 1;
                    // Bound the regression itself: the old adapter spins forever
                    // on cooperative Pending rather than actual lock contention.
                    if polls > 2 {
                        return Poll::Ready(None);
                    }
                    match admission.as_mut().poll(cx) {
                        Poll::Ready(guard) => Poll::Ready(Some(guard)),
                        Poll::Pending => {
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    }
                }));
                assert!(
                    acquired.is_some(),
                    "blocking admission spun on Tokio budget exhaustion"
                );
                assert_eq!(polls, 1, "an uncontended lock must be acquired immediately");
            })
            .await
            .unwrap()
        });
    }

    #[test]
    fn admission_timeout_drops_the_queued_writer_without_blocking_future_mutations() {
        let active = mutation();
        assert!(std::thread::spawn(|| freeze(1).is_err()).join().unwrap());
        drop(active);
        std::thread::spawn(|| {
            let _guard = mutation();
        })
        .join()
        .unwrap();
    }
    #[test]
    fn mutations_overlap_and_nested_transactions_do_not_deadlock() {
        let first = mutation();
        let nested = mutation();
        std::thread::spawn(|| {
            let _other = mutation();
        })
        .join()
        .unwrap();
        drop(nested);
        drop(first);
        let frozen = freeze(5).unwrap();
        assert!(BOUNDARY.try_read().is_err());
        drop(frozen);
        let _again = mutation();
    }
}
