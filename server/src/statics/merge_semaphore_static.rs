use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

pub static MERGE_SEMAPHORE: tokio::sync::OnceCell<Semaphore> = tokio::sync::OnceCell::const_new();
pub static FILE_UPLOAD_LOCKS: Lazy<Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));