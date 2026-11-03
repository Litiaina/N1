use crate::server::error::{ApiError, ApiResult};
use std::path::Path;
pub(crate) async fn private_directory(path: &Path) -> ApiResult<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        #[cfg(unix)]
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        builder.mode(0o700);
        match builder.create(&path) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        }
        let metadata = std::fs::symlink_metadata(&path)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(ApiError::invalid(
                "staging root must be a real private directory",
            ));
        }
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ApiError::invalid("staging root must be owner-only"));
        }
        Ok(())
    })
    .await
    .map_err(|_| ApiError::invalid("staging directory worker failed"))?
}
