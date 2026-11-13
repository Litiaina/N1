use crate::server::error::{ApiError, ApiResult};
use std::path::Path;

/// Share the configured upload memory budget with batch uploads. Reject
/// pressure rather than waiting while holding an upload slot: a batch can
/// already hold memory while waiting for that same slot.
pub(crate) async fn io_buffer(
    app: &crate::server::App,
) -> ApiResult<(Vec<u8>, tokio::sync::OwnedSemaphorePermit)> {
    // Account for both our buffer and Tokio File's blocking-I/O buffer. A
    // one-unit configuration uses 32 KiB buffers rather than over-admitting.
    let units = app.config.upload_memory_budget.div_ceil(65536).clamp(1, 8);
    let permit = app
        .upload_memory
        .clone()
        .try_acquire_many_owned(units as u32)
        .map_err(|_| {
            ApiError(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Upload memory is busy; retry this part".into(),
            )
        })?;
    Ok((vec![0; units * 65536 / 2], permit))
}

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

/// Establish configured directories once at startup, including newly created
/// ancestors. Per-part requests never walk this chain or call syncfs.
pub(crate) async fn persist_staging_root(path: &Path) -> ApiResult<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        for directory in path.ancestors().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::File::open(directory)?.sync_all()?;
        }
        Ok(())
    })
    .await
    .map_err(|_| ApiError::invalid("staging root sync worker failed"))??;
    Ok(())
}

/// The part file was synchronized before this call. Its first strict ACK also
/// requires the version directory's link and fragment directory's link durable.
pub(crate) async fn persist_part_directory(
    directory: &Path,
    root: &Path,
    first_strict_payload: bool,
) -> ApiResult<()> {
    let directory = directory.to_owned();
    let root = root.to_owned();
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let fragment = directory
            .parent()
            .ok_or_else(|| std::io::Error::other("missing staging parent"))?;
        if fragment.parent() != Some(root.as_path()) {
            return Err(std::io::Error::other(
                "staging path is outside its configured root",
            ));
        }
        std::fs::File::open(&directory)?.sync_all()?;
        if first_strict_payload {
            std::fs::File::open(fragment)?.sync_all()?;
            std::fs::File::open(&root)?.sync_all()?;
        }
        Ok(())
    })
    .await
    .map_err(|_| ApiError::invalid("staging sync worker failed"))??;
    Ok(())
}
