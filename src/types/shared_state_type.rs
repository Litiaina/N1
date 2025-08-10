use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type SharedState = Arc<Mutex<StateStore>>;
pub type StateStore = HashMap<String, Vec<bool>>;