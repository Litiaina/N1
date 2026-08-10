use once_cell::sync::Lazy;
use tokio::sync::Mutex;

static GLOBAL_TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

pub fn global_test_lock() -> &'static Mutex<()> {
    &GLOBAL_TEST_LOCK
}
