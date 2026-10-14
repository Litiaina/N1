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
    let mut future = std::pin::pin!(BOUNDARY.clone().write_owned());
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
    let mut future = std::pin::pin!(future);
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
