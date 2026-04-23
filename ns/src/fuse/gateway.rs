use crate::enums::app_error_enum::AppError;
use crate::fuse::backend::{DirListPage, FsdBackend};
use crate::fuse::model::{FsdPath, NodeMetadata, WriteHandle};
use std::sync::Arc;

#[derive(Clone)]
pub struct FsdGateway<B: FsdBackend> {
    backend: Arc<B>,
}

impl<B: FsdBackend> FsdGateway<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }

    pub fn resolve_path(&self, mount_relative_path: &str) -> Result<FsdPath, AppError> {
        FsdPath::parse(mount_relative_path)
    }

    pub async fn list_dir_page(
        &self,
        mount_relative_path: &str,
        continuation_token: Option<String>,
        page_limit: usize,
    ) -> Result<DirListPage, AppError> {
        let path = self.resolve_path(mount_relative_path)?;
        self.backend
            .list_dir_page(&path, continuation_token, page_limit)
            .await
    }

    pub async fn stat(&self, mount_relative_path: &str) -> Result<NodeMetadata, AppError> {
        let path = self.resolve_path(mount_relative_path)?;
        self.backend.stat(&path).await
    }

    pub async fn read_range(
        &self,
        mount_relative_path: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, AppError> {
        let path = self.resolve_path(mount_relative_path)?;
        self.backend.read_range(&path, offset, length).await
    }

    pub async fn begin_write(
        &self,
        mount_relative_path: &str,
        expected_size_bytes: Option<u64>,
    ) -> Result<WriteHandle, AppError> {
        let path = self.resolve_path(mount_relative_path)?;
        self.backend.begin_write(&path, expected_size_bytes).await
    }

    pub async fn write_chunk(
        &self,
        handle: &WriteHandle,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<(), AppError> {
        self.backend.write_chunk(handle, offset, bytes).await
    }

    pub async fn commit_write(&self, handle: &WriteHandle) -> Result<(), AppError> {
        self.backend.commit_write(handle).await
    }

    pub async fn abort_write(&self, handle: &WriteHandle) -> Result<(), AppError> {
        self.backend.abort_write(handle).await
    }

    pub async fn delete(&self, mount_relative_path: &str) -> Result<(), AppError> {
        let path = self.resolve_path(mount_relative_path)?;
        self.backend.delete(&path).await
    }

    pub async fn mkdir(&self, mount_relative_path: &str) -> Result<(), AppError> {
        let path = self.resolve_path(mount_relative_path)?;
        self.backend.mkdir(&path).await
    }

    pub async fn rmdir(&self, mount_relative_path: &str) -> Result<(), AppError> {
        let path = self.resolve_path(mount_relative_path)?;
        self.backend.rmdir(&path).await
    }

    pub async fn rename(&self, from_mount_path: &str, to_mount_path: &str) -> Result<(), AppError> {
        let from = self.resolve_path(from_mount_path)?;
        let to = self.resolve_path(to_mount_path)?;

        if from.fragment != to.fragment {
            return Err(AppError::BadRequest(
                "cross-fragment rename is not supported in fsd gateway".to_string(),
            ));
        }

        self.backend.rename(&from, &to).await
    }
}
