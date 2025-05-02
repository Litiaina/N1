use tokio::sync::Semaphore;

pub static MERGE_SEMAPHORE: tokio::sync::OnceCell<Semaphore> = tokio::sync::OnceCell::const_new();