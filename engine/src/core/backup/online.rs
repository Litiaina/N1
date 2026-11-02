//! Pin a committed physical view under a publication boundary, then release it
//! before reading/hashing payload. Upload bodies and GET do not take that gate.
use super::{Result, capture, format::Snapshot, invalid, repository::Repository};
use crate::core::{Store, storage_pool::Member};
use rustix::fs::{self as rfs, AtFlags, Mode, OFlags};
use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
const VIEWS: &str = ".n1-backup-views";
struct View {
    members: Vec<Arc<Member>>,
    jobs: Vec<PathBuf>,
    sources: Vec<PathBuf>,
}
impl Drop for View {
    fn drop(&mut self) {
        for path in &self.jobs {
            if let Err(e) = std::fs::remove_dir_all(path) {
                tracing::warn!("backup scratch cleanup incomplete: {e}");
            }
        }
    }
}
fn owner(member: &Member, id: &str) -> Vec<u8> {
    format!(
        "N1 LINK backup view v1\n{}\n{}\n{}\n{id}\n",
        member.marker.layout.id, member.marker.layout.generation, member.marker.slot
    )
    .into_bytes()
}
fn cleanup(member: &Arc<Member>) -> Result<()> {
    let base = member.directory(Path::new(VIEWS), true, true)?;
    drop(base);
    for entry in std::fs::read_dir(member.path.join(VIEWS))? {
        let entry = entry?;
        let id = entry.file_name();
        let id = id
            .to_str()
            .ok_or_else(|| invalid("invalid backup view name"))?;
        super::fs::safe_id(id)?;
        if !entry.file_type()?.is_dir() {
            return Err(invalid("unknown entry in backup scratch"));
        }
        let file = member.open_read(Path::new(&format!("{VIEWS}/{id}/owner")))?;
        if file.metadata()?.len() != owner(member, id).len() as u64 {
            return Err(invalid("unknown backup view owner"));
        }
        let mut bytes = vec![0; file.metadata()?.len() as usize];
        crate::core::linux_io::pread_exact_at(&*file, &mut bytes, 0)?;
        if bytes != owner(member, id) {
            return Err(invalid("backup view belongs to another member"));
        }
        std::fs::remove_dir_all(entry.path())?;
    }
    member.check_binding()?;
    Ok(())
}
fn prepare(store: &Store, id: &str) -> Result<View> {
    let mut view = View {
        members: Vec::new(),
        jobs: Vec::new(),
        sources: Vec::new(),
    };
    for pool in &store.pools {
        pool.writable()?;
        for member in pool.members.iter().flatten() {
            cleanup(member)?;
            let relative = format!("{VIEWS}/{id}");
            let directory = member.directory(Path::new(&relative), true, false)?;
            let mut file = File::from(rfs::openat(
                &*directory,
                "owner",
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?);
            file.write_all(&owner(member, id))?;
            member.directory(Path::new(&format!("{relative}/root")), true, false)?;
            view.members.push(member.clone());
            view.jobs.push(member.path.join(&relative));
            view.sources
                .push(member.path.join(format!("{relative}/root")));
        }
    }
    Ok(view)
}
fn pin(member: &Member, id: &str, relative: &Path) -> Result<()> {
    // Validate leaf type through anchored no-follow opens before linking. The
    // engine's publication gate prevents its own delete/repair from replacing it.
    let _file = member.open_read(relative)?;
    let parent = relative
        .parent()
        .ok_or_else(|| invalid("backup source has no parent"))?;
    let name = relative
        .file_name()
        .ok_or_else(|| invalid("backup source has no filename"))?;
    let source = member.directory(parent, false, false)?;
    let destination = member.directory(
        &Path::new(&format!("{VIEWS}/{id}/root")).join(parent),
        true,
        false,
    )?;
    rfs::linkat(&*source, name, &*destination, name, AtFlags::empty())?;
    Ok(())
}
fn freeze(
    view: &View,
    id: &str,
    deadline: Instant,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    for member in &view.members {
        if Instant::now() >= deadline {
            return Err(invalid("online backup pin window ended before completion"));
        }
        pin(member, id, Path::new(".n1-arc-pool.json"))?;
        let entries = walkdir::WalkDir::new(&member.path)
            .min_depth(3)
            .max_depth(3)
            .max_open(16)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| {
                entry.depth() == 0 || entry.depth() > 2 || {
                    let s = entry.file_name().to_string_lossy();
                    s.len() == 2
                        && s.bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                }
            });
        for entry in entries {
            if cancel.is_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "recovery job stopped",
                )
                .into());
            }
            if Instant::now() >= deadline {
                return Err(invalid("online backup pin window ended before completion"));
            }
            let entry = entry?;
            let path = entry.path().strip_prefix(&member.path)?;
            let name = path
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 backup path"))?;
            if super::restore::allowed_path(name, &member.marker, false)?.is_some() {
                if !entry.file_type().is_file() {
                    return Err(invalid("nonregular committed backup source"));
                }
                pin(member, id, path)?;
            }
        }
        member.check_binding()?;
    }
    Ok(())
}
impl Store {
    pub async fn snapshot_online(
        self: &Arc<Self>,
        repository: PathBuf,
        key: PathBuf,
        boundary_seconds: u64,
    ) -> Result<Snapshot> {
        self.snapshot_online_controlled(
            repository,
            key,
            boundary_seconds,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
    }
    pub(crate) async fn snapshot_online_controlled(
        self: &Arc<Self>,
        repository: PathBuf,
        key: PathBuf,
        boundary_seconds: u64,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Snapshot> {
        if !(1..=300).contains(&boundary_seconds) {
            return Err(invalid("online backup boundary must be 1..=300 seconds"));
        }
        let store = self.clone();
        tokio::spawn(async move {
            let _slot = store
                .backup_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| invalid("a backup view is already active"))?;
            let engine = store.clone();
            let (repo, mut snapshot, view, cancel) = tokio::task::spawn_blocking(move || {
                let repo = Repository::open(&repository, &key, true)?.with_cancel(cancel.clone());
                repo.check_cancel()?;
                for member in engine
                    .pools
                    .iter()
                    .flat_map(|pool| pool.members.iter().flatten())
                {
                    let root = member.path.canonicalize()?;
                    if repo.path.starts_with(&root) || root.starts_with(&repo.path) {
                        return Err(invalid("backup repository must be outside member roots"));
                    }
                }
                let snapshot = capture::descriptors(&engine.pools, "online_committed")?;
                let view = prepare(&engine, &snapshot.id)?;
                Ok::<_, super::Error>((repo, snapshot, view, cancel))
            })
            .await
            .map_err(|e| invalid(format!("backup preparation worker failed: {e}")))??;
            let gate = tokio::time::timeout(
                Duration::from_secs(boundary_seconds),
                store.snapshot_gate.clone().write_owned(),
            )
            .await
            .map_err(|_| invalid("online backup boundary admission timed out"))?;

            tokio::task::spawn_blocking(move || {
                let guard = gate;
                snapshot.created_at = chrono::Utc::now().timestamp();
                let started = Instant::now();
                repo.check_cancel()?;
                freeze(
                    &view,
                    &snapshot.id,
                    started + Duration::from_secs(boundary_seconds),
                    &cancel,
                )?;
                drop(guard); // Hashing/encryption/copying do not hold publication.
                let point =
                    capture::capture(&repo, snapshot, &view.sources, store.recovery_control.get())?;
                drop(view);
                Ok::<_, super::Error>(point)
            })
            .await
            .map_err(|e| invalid(format!("online backup worker failed: {e}")))?
        })
        .await
        .map_err(|e| invalid(format!("online backup owner failed: {e}")))?
    }
}
