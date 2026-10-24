//! Request-owned, bounded group persistence. No background commit queue and no
//! alternate on-disk object representation are introduced here.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const SYNC_WORKERS: usize = 4;

struct Member {
    path: PathBuf,
    directory: File,
    device: u64,
    inode: u64,
}

/// Open before admitting any batch writes. The caller has already verified
/// enrollment; these pinned descriptors prevent syncing a replacement mount
/// instead of the filesystem which actually received the batch.
pub(crate) struct FilesystemGroup {
    members: Vec<Member>,
    representatives: Vec<usize>,
}

impl FilesystemGroup {
    pub(crate) fn open(roots: &[PathBuf]) -> io::Result<Self> {
        super::selected_backend()?;
        if roots.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty batch filesystem group",
            ));
        }
        let mut members = Vec::with_capacity(roots.len());
        let mut representatives = Vec::new();
        let mut devices = BTreeSet::new();
        for path in roots {
            if !path.is_absolute() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "batch member must be absolute",
                ));
            }
            let directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)?;
            let metadata = directory.metadata()?;
            let member = Member {
                path: path.clone(),
                directory,
                device: metadata.dev(),
                inode: metadata.ino(),
            };
            member.validate()?;
            if devices.insert(member.device) {
                representatives.push(members.len());
            }
            members.push(member);
        }
        Ok(Self {
            members,
            representatives,
        })
    }

    /// Linux 6.8 has no io_uring SYNCFS opcode. This deliberately uses POSIX
    /// syncfs, with at most four joined workers, not a fallback/replayed write.
    /// Every member path is checked on both sides, even on a sync error.
    pub(crate) fn sync_all(&self) -> io::Result<()> {
        self.sync_all_with(|directory| {
            // SAFETY: directory owns an open descriptor throughout the call.
            if unsafe { libc::syncfs(directory.as_raw_fd()) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        })
    }

    fn sync_all_with(&self, sync: impl Fn(&File) -> io::Result<()> + Sync) -> io::Result<()> {
        self.validate()?;
        let directories = self
            .representatives
            .iter()
            .map(|&index| &self.members[index].directory)
            .collect::<Vec<_>>();
        let result = sync_join_all(&directories, &sync);
        let validation = self.validate();
        result.and(validation)
    }

    fn validate(&self) -> io::Result<()> {
        for member in &self.members {
            member.validate()?;
        }
        Ok(())
    }
}

impl Member {
    fn validate(&self) -> io::Result<()> {
        let current = fs::symlink_metadata(&self.path)?;
        if !current.is_dir() || current.dev() != self.device || current.ino() != self.inode {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("batch member changed: {}", self.path.display()),
            ));
        }
        Ok(())
    }
}

fn sync_join_all(
    directories: &[&File],
    sync: &(impl Fn(&File) -> io::Result<()> + Sync),
) -> io::Result<()> {
    let mut first_error = None;
    for chunk in directories.chunks(SYNC_WORKERS) {
        std::thread::scope(|scope| {
            let workers = chunk
                .iter()
                .map(|directory| scope.spawn(move || sync(directory)))
                .collect::<Vec<_>>();
            for worker in workers {
                let result = worker
                    .join()
                    .unwrap_or_else(|_| Err(io::Error::other("batch sync worker panicked")));
                if let Err(error) = result {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        });
    }
    first_error.map_or(Ok(()), Err)
}

/// A private ordinary file whose contents must pass a group persistence
/// barrier before `publish`. The temporary file belongs under an uncommitted
/// payload version, never in the namespace where a crash could leak garbage.
pub(crate) struct StagedFile {
    target: PathBuf,
    temporary: PathBuf,
    device: u64,
    inode: u64,
    published: bool,
}

impl StagedFile {
    pub(crate) fn create(target: &Path, temporary_parent: &Path, data: &[u8]) -> io::Result<Self> {
        let target_parent = target.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "batch target has no parent")
        })?;
        let target_metadata = fs::symlink_metadata(target_parent)?;
        let temporary_metadata = fs::symlink_metadata(temporary_parent)?;
        if !target_metadata.is_dir()
            || !temporary_metadata.is_dir()
            || target_metadata.dev() != temporary_metadata.dev()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch staging must use existing directories on the target filesystem",
            ));
        }
        let temporary = temporary_parent.join(format!(".n1-batch-{}.tmp", ulid::Ulid::new()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o644)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temporary)?;
        let metadata = file.metadata()?;
        let staged = Self {
            target: target.to_path_buf(),
            temporary,
            device: metadata.dev(),
            inode: metadata.ino(),
            published: false,
        };
        // write_all owns no deferred userspace buffer. Persistence is provided
        // by the caller's following filesystem-group barrier, not a local sync.
        file.write_all(data)?;
        drop(file);
        Ok(staged)
    }

    /// Exclusive publication: a racing or existing committed file is never
    /// replaced. Publishing cannot be rolled back by Drop, including when the
    /// following durability barrier fails.
    pub(crate) fn publish(&mut self) -> io::Result<()> {
        if self.published {
            return Ok(());
        }
        self.validate_temporary()?;
        // One atomic no-replace move, not link+unlink: a crash cannot leave a
        // second private alias inside an otherwise committed payload version.
        // Modern Linux is required; an unsupported operation fails closed.
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            &self.temporary,
            rustix::fs::CWD,
            &self.target,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(io::Error::from)?;
        self.published = true;
        Ok(())
    }

    #[cfg(test)]
    fn target(&self) -> &Path {
        &self.target
    }

    #[cfg(test)]
    fn temporary_path(&self) -> &Path {
        &self.temporary
    }

    fn validate_temporary(&self) -> io::Result<()> {
        let metadata = fs::symlink_metadata(&self.temporary)?;
        if !metadata.is_file() || metadata.dev() != self.device || metadata.ino() != self.inode {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "batch staged file changed",
            ));
        }
        Ok(())
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        // Never remove target: even a failed later barrier may have left a
        // committed version. Only the private file created by this value is ours.
        if !self.published && self.validate_temporary().is_ok() {
            let _ = fs::remove_file(&self.temporary);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("n1-group-{}", ulid::Ulid::new()));
            fs::create_dir_all(path.join("payload/version")).unwrap();
            Self(path)
        }
        fn private(&self) -> PathBuf {
            self.0.join("payload/version")
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn staging_is_private_and_drop_only_cleans_its_temporary_file() {
        let fixture = Fixture::new();
        let target = fixture.0.join("final.ndx");
        let temporary;
        {
            let file = StagedFile::create(&target, &fixture.private(), b"immutable").unwrap();
            assert_eq!(file.target(), target);
            temporary = file.temporary_path().to_path_buf();
            assert!(!target.exists());
            assert_eq!(fs::read(&temporary).unwrap(), b"immutable");
        }
        assert!(!target.exists());
        assert!(!temporary.exists());
    }

    #[test]
    fn exclusive_publication_never_overwrites_and_drop_preserves_final_file() {
        let fixture = Fixture::new();
        let target = fixture.0.join("final.ndx");
        let mut first = StagedFile::create(&target, &fixture.private(), b"first").unwrap();
        let original_inode = fs::metadata(first.temporary_path()).unwrap().ino();
        let private_path = first.temporary_path().to_path_buf();
        first.publish().unwrap();
        assert!(
            !private_path.exists(),
            "publication moves, rather than aliases, the private file"
        );
        assert_eq!(fs::metadata(&target).unwrap().ino(), original_inode);
        let mut second = StagedFile::create(&target, &fixture.private(), b"second").unwrap();
        assert_eq!(
            second.publish().unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        drop(first);
        drop(second);
        assert_eq!(fs::read(target).unwrap(), b"first");
    }

    #[test]
    fn error_after_publication_never_rolls_back_the_authoritative_file() {
        let fixture = Fixture::new();
        let group = FilesystemGroup::open(std::slice::from_ref(&fixture.0)).unwrap();
        let target = fixture.0.join("final.ndx");
        let mut staged = StagedFile::create(&target, &fixture.private(), b"preserved").unwrap();
        staged.publish().unwrap();
        assert!(
            group
                .sync_all_with(|_| Err(io::Error::from_raw_os_error(libc::EIO)))
                .is_err()
        );
        drop(staged);
        assert_eq!(fs::read(target).unwrap(), b"preserved");
    }

    #[test]
    fn replacement_private_file_is_not_published_or_deleted_by_drop() {
        let fixture = Fixture::new();
        let target = fixture.0.join("final.ndx");
        let mut staged = StagedFile::create(&target, &fixture.private(), b"original").unwrap();
        let private = staged.temporary_path().to_path_buf();
        fs::rename(&private, fixture.private().join("original")).unwrap();
        fs::write(&private, b"not-owned").unwrap();
        assert!(staged.publish().is_err());
        drop(staged);
        assert!(!target.exists());
        assert_eq!(fs::read(private).unwrap(), b"not-owned");
    }

    #[test]
    fn barrier_failure_does_not_publish_and_has_no_retry() {
        let fixture = Fixture::new();
        let group = FilesystemGroup::open(&[fixture.0.clone(), fixture.private()]).unwrap();
        let target = fixture.0.join("final.ndx");
        let staged = StagedFile::create(&target, &fixture.private(), b"private").unwrap();
        let calls = AtomicUsize::new(0);
        let error = group
            .sync_all_with(|_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::from_raw_os_error(libc::EIO))
            })
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "same filesystem is synchronized once"
        );
        drop(staged);
        assert!(!target.exists());
    }

    #[test]
    fn replaced_member_before_barrier_is_rejected_without_syncing_replacement() {
        let fixture = Fixture::new();
        let original = fixture.0.join("member");
        fs::create_dir(&original).unwrap();
        let group = FilesystemGroup::open(std::slice::from_ref(&original)).unwrap();
        fs::rename(&original, fixture.0.join("old-member")).unwrap();
        fs::create_dir(&original).unwrap();
        let calls = AtomicUsize::new(0);
        assert!(
            group
                .sync_all_with(|_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn replaced_member_during_barrier_is_rejected() {
        let fixture = Fixture::new();
        let original = fixture.0.join("member");
        fs::create_dir(&original).unwrap();
        let group = FilesystemGroup::open(std::slice::from_ref(&original)).unwrap();
        assert!(
            group
                .sync_all_with(|_| {
                    fs::rename(&original, fixture.0.join("old-member"))?;
                    fs::create_dir(&original)
                })
                .is_err()
        );
    }

    #[test]
    fn every_started_sync_worker_is_joined_after_failure() {
        let fixture = Fixture::new();
        let file = File::open(&fixture.0).unwrap();
        let calls = AtomicUsize::new(0);
        let done = AtomicUsize::new(0);
        assert!(
            sync_join_all(&[&file, &file, &file, &file], &|_| {
                let number = calls.fetch_add(1, Ordering::SeqCst);
                if number != 0 {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                done.fetch_add(1, Ordering::SeqCst);
                if number == 0 {
                    Err(io::Error::from_raw_os_error(libc::EIO))
                } else {
                    Ok(())
                }
            })
            .is_err()
        );
        assert_eq!(done.load(Ordering::SeqCst), 4);
    }
}
