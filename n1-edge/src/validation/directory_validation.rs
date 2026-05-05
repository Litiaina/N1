use std::io;
use std::path::{Component, Path, PathBuf};
use tokio::fs;
use walkdir::WalkDir;

#[cfg(unix)]
fn capacity_total_bytes_sync(path: &Path) -> io::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;

    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut vfs) } != 0 {
        return Err(io::Error::last_os_error());
    }

    Ok((vfs.f_frsize as u64).saturating_mul(vfs.f_blocks as u64))
}

#[cfg(windows)]
fn capacity_total_bytes_sync(path: &Path) -> io::Result<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    wide.push(0);

    let mut total: u64 = 0;
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            std::ptr::null_mut(),
            &mut total,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(total)
}

pub async fn get_allocated_storage(path: &str) -> Result<Option<u64>, io::Error> {
    let p = Path::new(path);
    if !p.exists() {
        return Ok(None);
    }

    let p = p.to_path_buf();
    let total = tokio::task::spawn_blocking(move || capacity_total_bytes_sync(&p))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("spawn_blocking: {e}")))??;

    Ok(Some(total))
}

pub fn sanitize_path(path: &str) -> String {
    let mut clean_path = PathBuf::new();

    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => {
                let part_str = part
                    .to_string_lossy()
                    .replace('\0', "")
                    .replace(['/', '\\'], "");
                clean_path.push(part_str);
            }
            _ => {
                continue;
            }
        }
    }

    clean_path.to_string_lossy().to_string()
}

pub fn sanitize_fragmented_path(path: &str) -> PathBuf {
    path.split('/')
        .filter_map(|s| {
            if s.is_empty() || s == "." || s == ".." {
                None
            } else {
                Some(s)
            }
        })
        .collect()
}

pub async fn get_dir_size(path: &PathBuf) -> u64 {
    let mut total_size = 0;

    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        let metadata = fs::metadata(entry.path()).await;

        if let Ok(metadata) = metadata {
            if metadata.is_file() {
                total_size += metadata.len();
            }
        }
    }

    total_size
}
