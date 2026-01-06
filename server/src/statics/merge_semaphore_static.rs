use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::OnceLock};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

pub static MAX_CONCURRENT_UPLOADS: OnceLock<usize> = OnceLock::new();
pub static MERGE_SEMAPHORE: tokio::sync::OnceCell<Semaphore> = tokio::sync::OnceCell::const_new();
pub static CHUNK_UPLOAD_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(*MAX_CONCURRENT_UPLOADS.get().expect("Config not initialized!")));
pub static FILE_UPLOAD_LOCKS: Lazy<Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));