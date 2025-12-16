use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

use crate::object_storage::constants::MAX_CONCURRENT_UPLOADS;

pub static MERGE_SEMAPHORE: tokio::sync::OnceCell<Semaphore> = tokio::sync::OnceCell::const_new();
pub static CHUNK_UPLOAD_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(MAX_CONCURRENT_UPLOADS));
pub static FILE_UPLOAD_LOCKS: Lazy<Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));