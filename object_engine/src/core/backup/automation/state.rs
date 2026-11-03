use super::super::{Result, invalid};
use super::config::Service;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::{
    fd::AsRawFd,
    unix::fs::{OpenOptionsExt, PermissionsExt},
};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Status {
    pub phase: String,
    pub updated_at: i64,
    pub last_success_at: Option<i64>,
    pub last_success_snapshot: Option<String>,
    pub pending_snapshot: Option<String>,
    /// Configuration identity prevents retrying into an accidentally changed destination.
    pub pending_config: Option<String>,
    /// Written and fsynced BEFORE requesting a service stop.
    pub restart: Option<Service>,
    pub error: Option<String>,
}

pub struct State {
    pub path: PathBuf,
    pub status: Status,
    _lock: File,
}
pub fn lock(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("job lock is not a regular file"));
    }
    // SAFETY: the File owns the descriptor for the lock lifetime.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(invalid(
            "backup job/service is already in use; refusing an overlapping run",
        ));
    }
    Ok(file)
}
pub fn service_lock(service: &Service) -> Result<File> {
    service.validate()?;
    let base = if service.user {
        PathBuf::from(
            std::env::var_os("XDG_RUNTIME_DIR")
                .ok_or_else(|| invalid("user service needs XDG_RUNTIME_DIR"))?,
        )
    } else {
        PathBuf::from("/run/lock")
    };
    lock(&base.join(format!("litiaina-n1-backup-{}.lock", service.unit)))
}
impl State {
    pub fn open(path: &Path) -> Result<Self> {
        super::super::fs::private_dir(path)?;
        if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err(invalid("backup job state directory must be owner-only"));
        }
        let path = fs::canonicalize(path)?;
        let lock = lock(&path.join("job.lock"))?;
        let status = Self::read(&path)?;
        Ok(Self {
            path,
            status,
            _lock: lock,
        })
    }
    pub fn read(path: &Path) -> Result<Status> {
        match super::super::fs::read_regular(&path.join("status.json"), 65536) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                Ok(Status::default())
            }
            Err(error) => Err(error),
        }
    }
    pub fn save(&mut self) -> Result<()> {
        self.status.updated_at = chrono::Utc::now().timestamp();
        let bytes = serde_json::to_vec_pretty(&self.status)?;
        let temp = self.path.join("status.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temp, self.path.join("status.json"))?;
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }
    pub fn phase(&mut self, phase: &str) -> Result<()> {
        self.status.phase = phase.into();
        eprintln!("N1 automatic backup: {phase}");
        self.save()
    }
}
