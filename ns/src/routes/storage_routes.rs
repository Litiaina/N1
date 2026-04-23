use crate::enums::app_error_enum::AppError;
use crate::metadata::NasMetadataStore;
use crate::settings::config::NsConfig;
use once_cell::sync::OnceCell;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub metadata: NasMetadataStore,
    pub staging_root: PathBuf,
    pub config: Arc<NsConfig>,
}

impl AppState {
    pub fn new(metadata: NasMetadataStore, staging_root: PathBuf, config: Arc<NsConfig>) -> Self {
        Self {
            metadata,
            staging_root,
            config,
        }
    }
}

static GLOBAL_APP_STATE: OnceCell<Arc<AppState>> = OnceCell::new();

pub fn init_global_app_state(state: Arc<AppState>) -> Result<(), AppError> {
    GLOBAL_APP_STATE.set(state).map_err(|_| {
        AppError::InternalServerError("global NS state already initialized".to_string())
    })
}

pub fn global_app_state() -> &'static Arc<AppState> {
    GLOBAL_APP_STATE
        .get()
        .expect("global NS state must be initialized before use")
}
