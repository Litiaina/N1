use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::global_app_state;
use std::path::{Path, PathBuf};

pub async fn filesystem_capacity_for_share(share_name: &str) -> Result<(u64, u64), AppError> {
    let Some(share) = global_app_state().metadata.share(share_name)? else {
        return Err(AppError::NotFound);
    };
    let root = PathBuf::from(share.root_path);
    tokio::task::spawn_blocking(move || filesystem_capacity(root.as_path()))
        .await
        .map_err(|err| AppError::InternalServerError(format!("capacity join error: {}", err)))?
}

#[cfg(unix)]
fn filesystem_capacity(path: &Path) -> Result<(u64, u64), AppError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path_bytes = path.as_os_str().as_bytes();
    let c_path = CString::new(path_bytes)
        .map_err(|_| AppError::BadRequest(format!("path contains NUL byte: {}", path.display())))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return Err(AppError::Io(
            std::io::Error::last_os_error(),
            format!("statvfs {}", path.display()),
        ));
    }
    let stats = unsafe { stats.assume_init() };
    let block_size = if stats.f_frsize == 0 {
        stats.f_bsize as u64
    } else {
        stats.f_frsize as u64
    };
    let total = (stats.f_blocks as u64).saturating_mul(block_size);
    let available = (stats.f_bavail as u64).saturating_mul(block_size);
    Ok((total, available))
}

#[cfg(not(unix))]
fn filesystem_capacity(_path: &Path) -> Result<(u64, u64), AppError> {
    Err(AppError::InternalServerError(
        "filesystem capacity is unsupported on this platform".to_string(),
    ))
}
