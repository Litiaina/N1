//! Request-owned, bounded group persistence. Concurrent requests share only
//! synchronous filesystem flushes; there is no background commit queue or
//! alternate on-disk object representation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, LazyLock, LockResult, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Instant;

const SYNC_WORKERS: usize = 4;
const MAX_DEVICE_METRICS: usize = 64;

type FilesystemRegistry = Mutex<BTreeMap<u64, Weak<SharedFilesystem>>>;

static FILESYSTEMS: LazyLock<FilesystemRegistry> = LazyLock::new(|| Mutex::new(BTreeMap::new()));
static DEVICE_METRICS: LazyLock<Mutex<BTreeSet<u64>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

#[derive(Clone)]
struct SyncFailure {
    errno: Option<i32>,
    kind: io::ErrorKind,
    message: String,
}

impl SyncFailure {
    fn capture(error: io::Error) -> Self {
        Self {
            errno: error.raw_os_error(),
            kind: error.kind(),
            message: error.to_string(),
        }
    }

    fn error(&self) -> io::Error {
        self.errno.map_or_else(
            || io::Error::new(self.kind, self.message.clone()),
            io::Error::from_raw_os_error,
        )
    }
}

#[derive(Default)]
struct SyncRound {
    result: OnceLock<Result<(), SyncFailure>>,
}

#[derive(Default)]
struct SyncState {
    running: bool,
    pending: Option<Arc<SyncRound>>,
    failure: Option<SyncFailure>,
}

struct SharedFilesystem {
    /// Opened before any attached group writes. Do not select a newer caller's
    /// FD: syncfs writeback-error accounting is per open file description.
    directory: File,
    metric: String,
    state: Mutex<SyncState>,
    changed: Condvar,
}

impl SharedFilesystem {
    fn new(directory: File, device: u64) -> Self {
        // Device cardinality, not object/key cardinality. Even repeated root
        // replacements cannot create unbounded global latency-class history.
        let metric = match DEVICE_METRICS.lock() {
            Ok(mut devices) if devices.contains(&device) || devices.len() < MAX_DEVICE_METRICS => {
                devices.insert(device);
                format!("upload.batch.syncfs.dev.{device}")
            }
            _ => "upload.batch.syncfs.dev.other".into(),
        };
        Self {
            directory,
            metric,
            state: Mutex::new(SyncState::default()),
            changed: Condvar::new(),
        }
    }

    fn recover_state<'a>(
        &self,
        result: LockResult<MutexGuard<'a, SyncState>>,
    ) -> MutexGuard<'a, SyncState> {
        match result {
            Ok(state) => state,
            Err(poisoned) => {
                // Only recover the guard to fail closed and wake owners. Do
                // not leave running/pending waiters asleep after a panic.
                let mut state = poisoned.into_inner();
                let error = state
                    .failure
                    .get_or_insert_with(|| {
                        SyncFailure::capture(io::Error::other("group filesystem barrier poisoned"))
                    })
                    .clone();
                if let Some(pending) = state.pending.take() {
                    let _ = pending.result.set(Err(error));
                }
                self.changed.notify_all();
                state
            }
        }
    }

    fn healthy(&self) -> io::Result<()> {
        let state = self.recover_state(self.state.lock());
        state
            .failure
            .as_ref()
            .map_or(Ok(()), |error| Err(error.error()))
    }

    fn sync_with(&self, sync: &(impl Fn(&File) -> io::Result<()> + Sync)) -> io::Result<()> {
        let started = Instant::now();
        let mut state = self.recover_state(self.state.lock());
        if let Some(error) = &state.failure {
            return Err(error.error());
        }
        // Register only after this caller's writes are complete. A running
        // round has already been removed from `pending`, so late writers can
        // never inherit a syscall which started before their writes.
        let round = state
            .pending
            .get_or_insert_with(|| Arc::new(SyncRound::default()))
            .clone();
        self.changed.notify_all();
        loop {
            if let Some(result) = round.result.get() {
                let outcome = result.as_ref().map(|_| ()).map_err(SyncFailure::error);
                drop(state);
                crate::io_control::record_io_latency(
                    "upload.batch.syncfs.joined",
                    started.elapsed(),
                    0,
                    result.is_ok(),
                );
                return outcome;
            }
            if let Some(error) = &state.failure {
                return Err(error.error());
            }
            if !state.running {
                let pending = state.pending.take().ok_or_else(|| {
                    io::Error::other("group filesystem barrier lost pending round")
                })?;
                debug_assert!(Arc::ptr_eq(&pending, &round));
                state.running = true;
                drop(state);
                // No shared lock, timer, detached worker or payload buffer is
                // held here. One existing request worker performs the syscall.
                let syscall_started = Instant::now();
                let mut outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    sync(&self.directory)
                }))
                .unwrap_or_else(|_| Err(io::Error::other("group filesystem sync panicked")))
                .map_err(SyncFailure::capture);
                let syscall_elapsed = syscall_started.elapsed();
                state = self.recover_state(self.state.lock());
                if let Some(error) = &state.failure {
                    outcome = Err(error.clone());
                }
                if let Err(error) = &outcome {
                    // A failed shared syncfs may consume a writeback error
                    // belonging to writes in the NEXT round. Latch failure for
                    // every attached owner, including pending and new groups.
                    // Recovery requires all owners to drain/drop and a fresh
                    // descriptor, never an automatic successful retry.
                    state.failure = Some(error.clone());
                    if let Some(pending) = state.pending.take() {
                        let _ = pending.result.set(Err(error.clone()));
                    }
                }
                let result = outcome.as_ref().map(|_| ()).map_err(SyncFailure::error);
                let _ = round.result.set(outcome);
                state.running = false;
                self.changed.notify_all();
                drop(state);
                crate::io_control::record_io_latency(
                    &self.metric,
                    syscall_elapsed,
                    0,
                    result.is_ok(),
                );
                return result;
            }
            state = self.recover_state(self.changed.wait(state));
        }
    }
}

fn shared_filesystem(device: u64, directory: &File) -> io::Result<Arc<SharedFilesystem>> {
    shared_filesystem_in(&FILESYSTEMS, device, directory)
}

fn shared_filesystem_in(
    registry: &FilesystemRegistry,
    device: u64,
    directory: &File,
) -> io::Result<Arc<SharedFilesystem>> {
    let mut filesystems = registry
        .lock()
        .map_err(|_| io::Error::other("group filesystem registry poisoned"))?;
    filesystems.retain(|_, filesystem| filesystem.strong_count() != 0);
    let shared = match filesystems.get(&device).and_then(Weak::upgrade) {
        Some(shared) => shared,
        None => {
            let shared = Arc::new(SharedFilesystem::new(directory.try_clone()?, device));
            filesystems.insert(device, Arc::downgrade(&shared));
            shared
        }
    };
    shared.healthy()?;
    Ok(shared)
}

struct Member {
    path: PathBuf,
    directory: File,
    device: u64,
    inode: u64,
    filesystem: Arc<SharedFilesystem>,
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
        Self::open_with(roots, shared_filesystem)
    }

    fn open_with(
        roots: &[PathBuf],
        share: impl Fn(u64, &File) -> io::Result<Arc<SharedFilesystem>>,
    ) -> io::Result<Self> {
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
            let filesystem = share(metadata.dev(), &directory)?;
            let member = Member {
                path: path.clone(),
                directory,
                device: metadata.dev(),
                inode: metadata.ino(),
                filesystem,
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
    /// Pending callers on the same filesystem share a sealed syscall round;
    /// writes arriving during a syscall require a later round. Every member
    /// path is checked on both sides, even on a shared sync error.
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
        let filesystems = self
            .representatives
            .iter()
            .map(|&index| &self.members[index].filesystem)
            .collect::<Vec<_>>();
        let result = sync_join_all(&filesystems, &|filesystem| filesystem.sync_with(&sync));
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
        let pinned = self.directory.metadata()?;
        if !current.is_dir()
            || current.dev() != self.device
            || current.ino() != self.inode
            || pinned.dev() != self.device
            || pinned.ino() != self.inode
        {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("batch member changed: {}", self.path.display()),
            ));
        }
        Ok(())
    }
}

fn sync_join_all<T: Sync>(
    directories: &[T],
    sync: &(impl Fn(&T) -> io::Result<()> + Sync),
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
    use std::sync::mpsc;
    use std::time::Duration;

    fn group(roots: &[PathBuf]) -> FilesystemGroup {
        // Injected EIO must not poison other tests which happen to use the
        // same temporary filesystem concurrently with this test process.
        FilesystemGroup::open_with(roots, |device, directory| {
            Ok(Arc::new(SharedFilesystem::new(
                directory.try_clone()?,
                device,
            )))
        })
        .unwrap()
    }

    fn wait_pending(filesystem: &SharedFilesystem, participants: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = filesystem.state.lock().unwrap();
        loop {
            if state
                .pending
                .as_ref()
                .is_some_and(|round| Arc::strong_count(round) >= participants + 1)
            {
                return;
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("pending sync callers did not register");
            let (next, _) = filesystem.changed.wait_timeout(state, remaining).unwrap();
            state = next;
        }
    }

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
        let group = group(std::slice::from_ref(&fixture.0));
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
        let group = group(&[fixture.0.clone(), fixture.private()]);
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
        let group = group(std::slice::from_ref(&original));
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
        let group = group(std::slice::from_ref(&original));
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
    fn late_writers_require_next_flush_and_pending_callers_share_only_that_flush() {
        let fixture = Fixture::new();
        let directory = File::open(&fixture.0).unwrap();
        let filesystem = SharedFilesystem::new(directory, 0);
        let calls = AtomicUsize::new(0);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let (late_done_tx, late_done_rx) = mpsc::channel();
        let sync = |_: &File| {
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            started_tx.send(call).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(())
        };
        std::thread::scope(|scope| {
            let first = scope.spawn(|| filesystem.sync_with(&sync));
            assert_eq!(started_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
            let late = (0..2)
                .map(|_| {
                    let done = late_done_tx.clone();
                    let filesystem = &filesystem;
                    let sync = &sync;
                    scope.spawn(move || {
                        filesystem.sync_with(sync).unwrap();
                        done.send(()).unwrap();
                    })
                })
                .collect::<Vec<_>>();
            wait_pending(&filesystem, 2);
            assert!(matches!(
                late_done_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            release_tx.send(()).unwrap();
            first.join().unwrap().unwrap();
            assert_eq!(started_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
            assert!(matches!(
                late_done_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            release_tx.send(()).unwrap();
            for worker in late {
                worker.join().unwrap();
            }
        });
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "three callers, two sealed flushes"
        );
        assert_eq!(late_done_rx.try_iter().count(), 2);
        // A later phase cannot reuse either completed result.
        filesystem
            .sync_with(&|_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn writeback_error_fails_pending_and_new_owners_until_all_old_owners_drop() {
        let fixture = Fixture::new();
        let directory = File::open(&fixture.0).unwrap();
        let device = directory.metadata().unwrap().dev();
        let registry = FilesystemRegistry::default();
        let filesystem = shared_filesystem_in(&registry, device, &directory).unwrap();
        let original = Arc::downgrade(&filesystem);
        let calls = AtomicUsize::new(0);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let sync = |_: &File| {
            calls.fetch_add(1, Ordering::SeqCst);
            started_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Err(io::Error::from_raw_os_error(libc::EIO))
        };
        std::thread::scope(|scope| {
            let first = scope.spawn(|| filesystem.sync_with(&sync));
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let late = (0..2)
                .map(|_| scope.spawn(|| filesystem.sync_with(&sync)))
                .collect::<Vec<_>>();
            wait_pending(&filesystem, 2);
            release_tx.send(()).unwrap();
            assert_eq!(
                first.join().unwrap().unwrap_err().raw_os_error(),
                Some(libc::EIO)
            );
            for worker in late {
                assert_eq!(
                    worker.join().unwrap().unwrap_err().raw_os_error(),
                    Some(libc::EIO)
                );
            }
        });
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "never retry a consumed shared error"
        );
        assert_eq!(
            shared_filesystem_in(&registry, device, &directory)
                .err()
                .expect("new owner must not bypass a latched live descriptor")
                .raw_os_error(),
            Some(libc::EIO)
        );
        assert!(
            filesystem
                .sync_with(&|_| panic!("no retry allowed"))
                .is_err()
        );
        assert!(filesystem.state.lock().unwrap().pending.is_none());
        drop(filesystem);
        assert!(original.upgrade().is_none());
        drop(directory);
        let fresh_directory = File::open(&fixture.0).unwrap();
        let recovered = shared_filesystem_in(&registry, device, &fresh_directory).unwrap();
        recovered.sync_with(&|_| Ok(())).unwrap();
        assert_eq!(
            registry.lock().unwrap().len(),
            1,
            "no per-round history retained"
        );
    }

    #[test]
    fn paused_filesystem_does_not_block_an_independent_filesystem() {
        let fixture = Fixture::new();
        let first = SharedFilesystem::new(File::open(&fixture.0).unwrap(), 0);
        let other = SharedFilesystem::new(File::open(&fixture.0).unwrap(), 1);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        std::thread::scope(|scope| {
            let blocked = scope.spawn(|| {
                first.sync_with(&|_| {
                    started_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                    Ok(())
                })
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            other.sync_with(&|_| Ok(())).unwrap();
            release_tx.send(()).unwrap();
            blocked.join().unwrap().unwrap();
        });
    }

    #[test]
    fn panicking_sync_is_a_latched_error_not_a_stuck_running_round() {
        let fixture = Fixture::new();
        let filesystem = SharedFilesystem::new(File::open(&fixture.0).unwrap(), 0);
        assert!(
            filesystem
                .sync_with(&|_| panic!("injected sync panic"))
                .is_err()
        );
        let state = filesystem.state.lock().unwrap();
        assert!(!state.running);
        assert!(state.pending.is_none());
        assert!(state.failure.is_some());
        drop(state);
        assert!(filesystem.sync_with(&|_| Ok(())).is_err());
    }

    #[test]
    fn poisoned_state_fails_and_wakes_pending_callers_after_running_sync_returns() {
        let fixture = Fixture::new();
        let filesystem = SharedFilesystem::new(File::open(&fixture.0).unwrap(), 0);
        let calls = AtomicUsize::new(0);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let sync = |_: &File| {
            calls.fetch_add(1, Ordering::SeqCst);
            started_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(())
        };
        std::thread::scope(|scope| {
            let first = scope.spawn(|| filesystem.sync_with(&sync));
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let late = scope.spawn(|| filesystem.sync_with(&sync));
            wait_pending(&filesystem, 1);
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _state = filesystem.state.lock().unwrap();
                    panic!("injected state poison");
                }))
                .is_err()
            );
            release_tx.send(()).unwrap();
            assert!(first.join().unwrap().is_err());
            assert!(late.join().unwrap().is_err());
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let state = filesystem.recover_state(filesystem.state.lock());
        assert!(!state.running);
        assert!(state.pending.is_none());
        assert!(state.failure.is_some());
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
