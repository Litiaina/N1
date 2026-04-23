use crate::enums::app_error_enum::AppError;
use once_cell::sync::Lazy;

#[derive(Default)]
pub struct NamespaceRuntime;

static GLOBAL_NAMESPACE_RUNTIME: Lazy<NamespaceRuntime> = Lazy::new(NamespaceRuntime::default);

pub fn global_namespace_runtime() -> &'static NamespaceRuntime {
    &GLOBAL_NAMESPACE_RUNTIME
}

impl NamespaceRuntime {
    pub async fn select_object_id_for_write(
        &self,
        _fragment: &str,
        object_key: &str,
    ) -> Result<String, AppError> {
        Ok(object_key.to_string())
    }

    pub async fn bind_pending_upload(
        &self,
        _fragment: &str,
        _object_key: &str,
        _physical_object_id: &str,
        _version_id: &str,
        _session_id: &str,
    ) -> Result<(), AppError> {
        Ok(())
    }

    pub async fn clear_pending_upload(
        &self,
        _fragment: &str,
        _version_id: &str,
    ) -> Result<(), AppError> {
        Ok(())
    }

    pub async fn finalize_logical_upload(
        &self,
        _fragment: &str,
        _object_key: &str,
        _physical_object_id: &str,
        _metadata: &crate::core::disk_manager::ObjectMetadata,
    ) -> Result<(), AppError> {
        Ok(())
    }
}
