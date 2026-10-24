//! Cooperative process exclusion, not an upload/data-plane lock.
//! Held for the engine's lifetime; offline backup/restore takes the same lease.
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub const LEASE_FILE: &str = ".n1-instance.lock";
pub const RESTORE_PENDING: &str = ".n1-restore-pending";
const FORMAT: &[u8] = b"n1-instance-lease-v1\n";

pub struct InstanceLease(Vec<File>);

impl InstanceLease {
    pub fn acquire(roots: &[PathBuf], require_initialized: bool) -> io::Result<Self> {
        let roots = roots
            .iter()
            .map(fs::canonicalize)
            .collect::<io::Result<BTreeSet<_>>>()?;
        let mut files = Vec::new();
        for root in roots {
            let path = root.join(LEASE_FILE);
            let opened = OpenOptions::new()
                .read(true)
                .write(!require_initialized)
                .create(!require_initialized)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path);
            let mut file = match opened {
                Ok(file) => file,
                Err(error) if matches!(error.raw_os_error(), Some(libc::EROFS | libc::EACCES)) => {
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_NOFOLLOW)
                        .open(&path)
                        .map_err(|_| error)?
                }
                Err(error) => return Err(error),
            };
            if !file.metadata()?.is_file() {
                return Err(io::Error::other("N1 instance lease is not a regular file"));
            }
            // SAFETY: the descriptor remains owned by File for the lease lifetime.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "N1 storage root is in use; stop its engine before backup/restore: {}",
                        root.display()
                    ),
                ));
            }
            let mut bytes = Vec::new();
            (&mut file).take(128).read_to_end(&mut bytes)?;
            if bytes.is_empty() && !require_initialized {
                let started = std::time::Instant::now();
                tracing::info!(root = %root.display(), "Synchronizing new N1 instance lease");
                file.write_all(FORMAT)?;
                file.sync_all()?;
                File::open(&root)?.sync_all()?;
                tracing::info!(root = %root.display(), elapsed_ms = started.elapsed().as_millis(), "New N1 instance lease synchronized");
            } else if bytes != FORMAT {
                return Err(io::Error::other(
                    "unrecognized instance lease; refuse unsafe offline capture",
                ));
            }
            files.push(file);
        }
        Ok(Self(files))
    }

    pub fn engine(roots: &[PathBuf]) -> io::Result<Self> {
        let roots = roots
            .iter()
            .map(fs::canonicalize)
            .collect::<io::Result<BTreeSet<_>>>()?;
        let mut lease = Self(Vec::new());
        for root in &roots {
            match Self::acquire(std::slice::from_ref(root), false) {
                Ok(mut acquired) => lease.0.append(&mut acquired.0),
                // Do not turn an existing read-only degraded deployment into
                // an engine startup failure just to enable the new utility.
                // Offline capture still requires every root's initialized lease.
                Err(error) if error.raw_os_error() == Some(libc::EROFS) => {
                    tracing::warn!(
                        "Read-only root has no usable instance lease; offline backup is unavailable for this topology"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        for root in roots {
            match fs::symlink_metadata(root.join(RESTORE_PENDING)) {
                Ok(_) => {
                    return Err(io::Error::other(
                        "N1 restore is incomplete; complete or discard the isolated restore before startup",
                    ));
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(lease)
    }

    pub fn root_count(&self) -> usize {
        self.0.len()
    }
}

pub fn restore_marker(root: &Path) -> PathBuf {
    root.join(RESTORE_PENDING)
}
