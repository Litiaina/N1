use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type SharedState = Arc<Mutex<HashMap<String, Vec<Option<PathBuf>>>>>;