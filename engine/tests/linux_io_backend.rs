//! Backend selection is process-local. Run each mode in a fresh process so
//! these tests neither mutate a shared environment nor reset a live backend.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use futures_util::poll;
use n1::core::ec::{ALIGNMENT, AlignedBuffer};
use n1::core::linux_io;

const WORKER_MODE: &str = "N1_TEST_IO_WORKER_MODE";
const WORKER_CASE: &str = "N1_TEST_IO_WORKER_CASE";

fn run_backend_process(mode: &str, case: &str) {
    let mut child = Command::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "backend_process_worker",
            "--ignored",
            "--nocapture",
        ])
        .env("N1_IO_BACKEND", mode)
        .env(WORKER_MODE, mode)
        .env(WORKER_CASE, case)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start isolated backend test");
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        if child
            .try_wait()
            .expect("poll backend test process")
            .is_some()
        {
            break;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill timed-out backend test process");
            let output = child
                .wait_with_output()
                .expect("collect timed-out test output");
            panic!(
                "{mode} backend test timed out; its fixture is preserved\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = child
        .wait_with_output()
        .expect("collect backend test output");
    assert!(
        output.status.success(),
        "{mode} backend test failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    print!("{}", String::from_utf8_lossy(&output.stdout));
}

#[test]
fn blocking_backend_preserves_positioned_io_and_publication() {
    run_backend_process("blocking", "io");
}

/// This is intentionally opt-in: restricted CI may deny io_uring_setup.
/// An explicit native run must succeed with io_uring, never a fallback.
#[test]
#[ignore = "requires a Linux host that permits io_uring; run this exact test with --ignored"]
fn io_uring_backend_preserves_positioned_io_and_publication() {
    run_backend_process("uring", "io");
}

#[test]
fn invalid_backend_selection_is_rejected() {
    run_backend_process("not-a-backend", "invalid");
}

#[test]
fn auto_backend_reports_startup_fallback_when_kernel_denies_io_uring() {
    run_backend_process("auto", "denied");
}

#[test]
fn required_io_uring_refuses_startup_when_kernel_denies_it() {
    run_backend_process("uring", "denied");
}

/// Restrict only this subprocess's current thread and its future children.
/// No host sysctl, privileged operation, or production test hook is involved.
fn deny_io_uring_setup_in_this_process() {
    let mut instructions = [
        libc::sock_filter {
            code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
            jt: 0,
            jf: 0,
            k: 0, // offsetof(struct seccomp_data, nr)
        },
        libc::sock_filter {
            code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
            jt: 0,
            jf: 1,
            k: libc::SYS_io_uring_setup as u32,
        },
        libc::sock_filter {
            code: (libc::BPF_RET | libc::BPF_K) as u16,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
        },
        libc::sock_filter {
            code: (libc::BPF_RET | libc::BPF_K) as u16,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ALLOW,
        },
    ];
    let program = libc::sock_fprog {
        len: instructions.len() as u16,
        filter: instructions.as_mut_ptr(),
    };
    // SAFETY: prctl receives the documented integer arguments; the BPF program
    // remains alive until the kernel copies it during PR_SET_SECCOMP.
    unsafe {
        assert_eq!(
            libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0),
            0,
            "enable unprivileged seccomp: {}",
            io::Error::last_os_error()
        );
        assert_eq!(
            libc::prctl(
                libc::PR_SET_SECCOMP,
                libc::SECCOMP_MODE_FILTER,
                &program as *const libc::sock_fprog,
                0,
                0,
            ),
            0,
            "install child-only syscall restriction: {}",
            io::Error::last_os_error()
        );
        // The filter rejects the syscall before the invalid parameter pointer
        // can be inspected. EINVAL/EFAULT would mean the restriction failed.
        assert_eq!(
            libc::syscall(
                libc::SYS_io_uring_setup,
                8_u32,
                std::ptr::null_mut::<libc::c_void>(),
            ),
            -1
        );
    }
    assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM));
}

#[test]
#[ignore = "internal subprocess helper; invoked by the backend selection tests"]
fn backend_process_worker() {
    let Ok(mode) = std::env::var(WORKER_MODE) else {
        return;
    };
    if std::env::var(WORKER_CASE).as_deref() == Ok("invalid") {
        let error = linux_io::initialize_backend().expect_err("invalid mode must not initialize");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let file = OpenOptions::new().write(true).open("/dev/null").unwrap();
        assert_eq!(
            linux_io::write_bytes_at_blocking(&file, b"not written", 0)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput,
            "POSIX publication must not bypass failed backend initialization"
        );
        assert_eq!(linux_io::backend_snapshot().publication_backend, "posix");
        return;
    }
    if std::env::var(WORKER_CASE).as_deref() == Ok("denied") {
        deny_io_uring_setup_in_this_process();
        if mode == "auto" {
            let snapshot = linux_io::initialize_backend()
                .expect("automatic backend selection must tolerate a denied ring setup");
            assert_eq!(snapshot.backend, "blocking");
            assert_eq!(snapshot.publication_backend, "posix");
            assert_eq!(snapshot.requested, "auto");
            assert!(
                snapshot
                    .fallback_reason
                    .is_some_and(|reason| !reason.is_empty())
            );
        } else {
            let error = linux_io::initialize_backend()
                .expect_err("explicit io_uring must never silently fall back");
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            let snapshot = linux_io::backend_snapshot();
            assert_eq!(snapshot.backend, "unavailable");
            assert_eq!(snapshot.requested, "uring");
            assert_eq!(snapshot.publication_backend, "posix");
            let file = OpenOptions::new().write(true).open("/dev/null").unwrap();
            assert_eq!(
                linux_io::write_bytes_at_blocking(&file, b"not written", 0)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::PermissionDenied,
                "required native initialization also gates POSIX publication"
            );
        }
        return;
    }

    let initialized = linux_io::initialize_backend().expect("initialize requested backend");
    let expected = if mode == "uring" {
        "io_uring"
    } else {
        "blocking"
    };
    assert_eq!(
        initialized.backend, expected,
        "never silently change an explicit backend"
    );
    assert_eq!(initialized.requested, mode);
    assert_eq!(initialized.publication_backend, "posix");

    let root = std::env::var_os("N1_TEST_IO_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("n1-io-backend-{mode}-{}", ulid::Ulid::new()));
    std::fs::create_dir(&root).expect("create backend fixture");
    println!("backend={expected}, fixture={}", root.display());

    // A failure intentionally leaves the fixture intact for diagnosis.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("backend test runtime");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(240), async {
            zero_length_flood(&root, expected).await;
            concurrent_publication(&root, expected).await;
            streaming_reads_survive_caller_cancellation(&root, expected).await;
            atomic_metadata_keeps_posix_barriers_and_async_sync_uses_selected_backend(
                &root, expected,
            )
            .await;
            cancellation_keeps_accepted_io_owned(&root, expected).await;
            errors_do_not_switch_backends(&root, expected).await;
        })
        .await
        .expect("backend operations completed before deadline");
    });
    let final_snapshot = linux_io::backend_snapshot();
    assert_eq!(final_snapshot.backend, expected);
    assert_eq!(final_snapshot.publication_backend, "posix");
    assert_eq!(final_snapshot.queued, 0);
    assert_eq!(final_snapshot.in_flight, 0);
    println!("completed backend checks: {final_snapshot:?}");
    std::fs::remove_dir_all(root).expect("remove only this test's successful fixture");
}

fn aligned_pattern(value: u8, len: usize) -> AlignedBuffer {
    let mut buffer = AlignedBuffer::new(len);
    assert_eq!(buffer.extend_from_slice(&vec![value; len]), len);
    buffer
}

fn assert_bounded_backend(expected: &str) {
    let snapshot = linux_io::backend_snapshot();
    assert_eq!(snapshot.backend, expected);
    assert_eq!(snapshot.publication_backend, "posix");
    if expected == "io_uring" {
        assert!(snapshot.queue_capacity > 0);
        assert!(snapshot.queued <= snapshot.queue_capacity, "{snapshot:?}");
        assert!(
            snapshot.in_flight <= snapshot.queue_capacity,
            "{snapshot:?}"
        );
    }
}

async fn wait_until_drained(file: Option<&Arc<File>>) {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let snapshot = linux_io::backend_snapshot();
            let ownership_released = file.is_none_or(|file| Arc::strong_count(file) == 1);
            if snapshot.queued == 0 && snapshot.in_flight == 0 && ownership_released {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("accepted requests drained and released their file ownership");
}

async fn zero_length_flood(root: &Path, expected: &'static str) {
    const REQUESTS: usize = 2048;
    let path = root.join("empty.bin");
    let file = Arc::new(
        linux_io::open_direct_write_blocking(&path).expect("open zero-length flood fixture"),
    );
    // Queue-capacity batches of empty operations need no kernel CQEs. The
    // driver must keep draining even when eventfd wakeups have coalesced.
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut operations = tokio::task::JoinSet::new();
        for request in 0..REQUESTS {
            let file = file.clone();
            let path = path.clone();
            operations.spawn(async move {
                if request % 2 == 0 {
                    let returned = linux_io::write_aligned_at_async(file, AlignedBuffer::new(0), 0)
                        .await
                        .expect("empty flood write");
                    assert!(returned.is_empty());
                } else {
                    let file = File::open(path).expect("open empty flood reader");
                    let (_, returned) = linux_io::pread_exact_at_async(file, 0, 0)
                        .await
                        .expect("empty flood read");
                    assert!(returned.is_empty());
                }
                assert_bounded_backend(expected);
            });
        }
        while let Some(result) = operations.join_next().await {
            result.expect("empty operation task");
        }
    })
    .await
    .expect("zero-length flood must not wait for a nonexistent kernel completion");
    wait_until_drained(Some(&file)).await;
    assert_eq!(file.metadata().expect("empty fixture metadata").len(), 0);
}

async fn concurrent_publication(root: &Path, expected: &'static str) {
    // More callers than the bounded ring and queue combined. Non-overlapping
    // offsets prove that completion reordering cannot use a shared file cursor.
    const BLOCKS: usize = 768;
    let temporary = root.join("payload.partial");
    let committed = root.join("payload.committed");
    let file =
        Arc::new(linux_io::open_direct_write_blocking(&temporary).expect("open O_DIRECT payload"));
    file.set_len((BLOCKS * ALIGNMENT) as u64)
        .expect("size payload");
    let mut writes = tokio::task::JoinSet::new();
    for block in 0..BLOCKS {
        let file = file.clone();
        writes.spawn(async move {
            let value = (block % 251 + 1) as u8;
            let buffer = linux_io::write_aligned_at_async(
                file,
                aligned_pattern(value, ALIGNMENT),
                (block * ALIGNMENT) as u64,
            )
            .await
            .expect("positioned direct write");
            assert_eq!(buffer.as_slice(), vec![value; ALIGNMENT]);
            assert_bounded_backend(expected);
        });
    }
    while let Some(result) = writes.join_next().await {
        result.expect("direct write task");
        assert!(
            !committed.exists(),
            "publication must not precede data completion and sync"
        );
    }
    let empty = linux_io::write_aligned_at_async(file.clone(), AlignedBuffer::new(0), 0)
        .await
        .expect("empty write");
    assert!(empty.is_empty());
    linux_io::fdatasync_async(file.clone())
        .await
        .expect("persist payload data");
    linux_io::rename(&temporary, &committed)
        .await
        .expect("publish completed payload");
    linux_io::sync_dir(root)
        .await
        .expect("persist publication directory entry");
    wait_until_drained(Some(&file)).await;
    drop(file);

    let mut file = File::open(&committed).expect("reopen committed payload");
    for block in (0..BLOCKS).rev() {
        let (returned_file, bytes) =
            linux_io::pread_exact_at_async(file, ALIGNMENT, (block * ALIGNMENT) as u64)
                .await
                .expect("read committed positioned block");
        assert_eq!(bytes, vec![(block % 251 + 1) as u8; ALIGNMENT]);
        file = returned_file;
    }
    let (_, empty) = linux_io::pread_exact_at_async(file, 0, 0)
        .await
        .expect("empty read");
    assert!(empty.is_empty());
    let directory = Arc::new(File::open(root).expect("open publication directory"));
    linux_io::fsync_async(directory.clone())
        .await
        .expect("native directory fsync");
    wait_until_drained(Some(&directory)).await;
    assert_bounded_backend(expected);
}

async fn streaming_reads_survive_caller_cancellation(root: &Path, expected: &str) {
    const CHUNK: usize = 32 * 1024;
    let bytes: Vec<u8> = (0..CHUNK * 2 + 19)
        .map(|index| (index % 251) as u8)
        .collect();
    let path = root.join("streaming-read.bin");
    std::fs::write(&path, &bytes).expect("create streaming-read fixture");
    let file = Arc::new(File::open(&path).expect("open streaming-read fixture"));
    // A dropped streaming reader must not free its Vec or close its descriptor
    // before an accepted read completes. No kernel cancellation is assumed.
    const READS: usize = 16;
    let before = linux_io::backend_snapshot();
    let mut abandoned = Vec::new();
    for _ in 0..READS {
        let mut operation = Box::pin(linux_io::read_at_async(file.clone(), vec![0; CHUNK], 0));
        match poll!(operation.as_mut()) {
            Poll::Ready(result) => assert_eq!(result.expect("completed read"), bytes[..CHUNK]),
            Poll::Pending => abandoned.push(operation),
        }
    }
    drop(abandoned);
    wait_until_drained(Some(&file)).await;
    let after = linux_io::backend_snapshot();
    assert_eq!(after.backend, expected);
    assert!(after.fallback_reason.is_none());
    if expected == "io_uring" {
        assert!(after.submitted >= before.submitted + READS as u64);
        assert!(after.completed >= before.completed + READS as u64);
    }
    assert_bounded_backend(expected);
}

async fn atomic_metadata_keeps_posix_barriers_and_async_sync_uses_selected_backend(
    root: &Path,
    expected: &str,
) {
    let path = root.join("metadata.ndx");
    let old = b"previous committed metadata";
    std::fs::write(&path, old).expect("create old metadata fixture");
    assert_eq!(std::fs::read(&path).expect("read old metadata"), old);
    let replacement = vec![0xB7; 65_537];
    wait_until_drained(None).await;
    let before = linux_io::backend_snapshot();
    let task_path = path.clone();
    let task_bytes = replacement.clone();
    tokio::task::spawn_blocking(move || {
        linux_io::atomic_write_blocking_with_durability(&task_path, &task_bytes, true)
    })
    .await
    .expect("metadata publication worker")
    .expect("write, synchronize, and rename strict metadata");
    assert_eq!(
        std::fs::read(&path).expect("read committed metadata"),
        replacement
    );
    wait_until_drained(None).await;
    let after = linux_io::backend_snapshot();
    assert_eq!(after.backend, expected);
    assert_eq!(after.publication_backend, "posix");
    assert!(after.fallback_reason.is_none());
    // Publication is already synchronous: direct write + file fsync + rename +
    // directory fsync avoids an extra queue/reply bridge, not a durability step.
    assert_eq!(after.submitted, before.submitted);
    assert_eq!(after.completed, before.completed);
    let file = Arc::new(OpenOptions::new().write(true).open(&path).unwrap());
    linux_io::fdatasync_async(Arc::clone(&file))
        .await
        .expect("async data sync retains its selected backend");
    linux_io::fsync_async(file)
        .await
        .expect("async file sync retains its selected backend");
    linux_io::sync_dir(root)
        .await
        .expect("async directory sync retains its selected backend");
    wait_until_drained(None).await;
    let after_async = linux_io::backend_snapshot();
    assert_eq!(after_async.backend, expected);
    assert_eq!(after_async.publication_backend, "posix");
    assert!(after_async.fallback_reason.is_none());
    if expected == "io_uring" {
        assert!(
            after_async.submitted >= after.submitted + 3,
            "{after:?} -> {after_async:?}"
        );
        assert!(
            after_async.completed >= after.completed + 3,
            "{after:?} -> {after_async:?}"
        );
    } else {
        assert_eq!(after_async.submitted, after.submitted);
        assert_eq!(after_async.completed, after.completed);
    }
    assert_bounded_backend(expected);
}

async fn cancellation_keeps_accepted_io_owned(root: &Path, expected: &str) {
    const REQUESTS: usize = 32;
    const REQUEST_BYTES: usize = ALIGNMENT * 16;
    let path = root.join("cancelled-awaiters.bin");
    let file =
        Arc::new(linux_io::open_direct_write_blocking(&path).expect("open cancellation fixture"));
    file.set_len((REQUESTS * REQUEST_BYTES) as u64)
        .expect("size cancellation fixture");
    let before = linux_io::backend_snapshot();
    let mut abandoned = Vec::new();
    for request in 0..REQUESTS {
        let mut operation = Box::pin(linux_io::write_aligned_at_async(
            file.clone(),
            aligned_pattern((request + 1) as u8, REQUEST_BYTES),
            (request * REQUEST_BYTES) as u64,
        ));
        match poll!(operation.as_mut()) {
            Poll::Ready(result) => {
                result.expect("synchronously completed cancellation write");
            }
            Poll::Pending => abandoned.push(operation),
        }
    }
    // One poll hands ownership to the bounded backend before waiting for its
    // reply. These callers disappear while their requests may still be active.
    drop(abandoned);
    wait_until_drained(Some(&file)).await;
    linux_io::fdatasync_async(file.clone())
        .await
        .expect("persist writes after caller cancellation");
    wait_until_drained(Some(&file)).await;
    let after = linux_io::backend_snapshot();
    assert_eq!(after.backend, expected);
    if expected == "io_uring" {
        assert!(
            after.submitted >= before.submitted + REQUESTS as u64,
            "all polled writes were accepted: {before:?} -> {after:?}"
        );
    }
    drop(file);
    let bytes = std::fs::read(path).expect("read cancellation fixture");
    for (request, bytes) in bytes.chunks_exact(REQUEST_BYTES).enumerate() {
        assert!(
            bytes.iter().all(|byte| *byte == (request + 1) as u8),
            "cancelled receiver corrupted write {request}"
        );
    }

    if expected != "io_uring" {
        return;
    }
    // The exact-read API consumes a File. Dropping its future must not close
    // the descriptor or free its Vec while the kernel still uses either one.
    let before = linux_io::backend_snapshot();
    let mut abandoned_reads = Vec::new();
    for request in 0..REQUESTS {
        let file = File::open(root.join("cancelled-awaiters.bin"))
            .expect("open cancellation read fixture");
        let mut operation = Box::pin(linux_io::pread_exact_at_async(
            file,
            REQUEST_BYTES,
            (request * REQUEST_BYTES) as u64,
        ));
        match poll!(operation.as_mut()) {
            Poll::Ready(result) => {
                let (_, bytes) = result.expect("synchronously completed cancellation read");
                assert!(bytes.iter().all(|byte| *byte == (request + 1) as u8));
            }
            Poll::Pending => abandoned_reads.push(operation),
        }
    }
    drop(abandoned_reads);
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let snapshot = linux_io::backend_snapshot();
            if snapshot.completed >= before.completed + REQUESTS as u64
                && snapshot.queued == 0
                && snapshot.in_flight == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("all accepted reads finish after their callers disappear");
}

async fn errors_do_not_switch_backends(root: &Path, expected: &str) {
    let path = root.join("errors.bin");
    std::fs::write(&path, b"short").expect("prepare read-only fixture");
    let file = Arc::new(File::open(&path).expect("open read-only fixture"));
    wait_until_drained(None).await;
    let before_posix_error = linux_io::backend_snapshot();
    assert_eq!(
        linux_io::write_bytes_at_blocking(&*file, b"must fail", 0)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EBADF),
        "POSIX publication propagates a read-only descriptor error"
    );
    let full = OpenOptions::new().write(true).open("/dev/full").unwrap();
    assert_eq!(
        linux_io::write_bytes_at_blocking(&full, b"must fail", 0)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::ENOSPC),
        "POSIX publication propagates capacity exhaustion"
    );
    let null = OpenOptions::new().write(true).open("/dev/null").unwrap();
    for error in [
        linux_io::fdatasync_blocking(&null).unwrap_err(),
        linux_io::fsync_blocking(&null).unwrap_err(),
    ] {
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
    }
    let after_posix_error = linux_io::backend_snapshot();
    assert_eq!(after_posix_error.backend, expected);
    assert_eq!(after_posix_error.publication_backend, "posix");
    assert!(after_posix_error.fallback_reason.is_none());
    assert_eq!(after_posix_error.submitted, before_posix_error.submitted);
    assert_eq!(after_posix_error.completed, before_posix_error.completed);
    let buffer = vec![0; 100];
    let allocation = buffer.as_ptr() as usize;
    let buffer = linux_io::read_at_async(file.clone(), buffer, 2)
        .await
        .expect("positioned partial checksum read");
    assert_eq!(buffer, b"ort");
    assert_eq!(
        buffer.as_ptr() as usize,
        allocation,
        "read reuses its owned allocation"
    );
    let mut buffer = buffer;
    buffer.resize(100, 0);
    let buffer = linux_io::read_at_async(file.clone(), buffer, 5)
        .await
        .expect("checksum stream reaches EOF successfully");
    assert!(buffer.is_empty());
    assert_eq!(buffer.as_ptr() as usize, allocation);
    let error = match linux_io::write_aligned_at_async(
        file.clone(),
        aligned_pattern(7, ALIGNMENT),
        0,
    )
    .await
    {
        Ok(_) => panic!("read-only write unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.raw_os_error(), Some(libc::EBADF));
    assert_eq!(
        std::fs::read(&path).expect("unchanged read-only fixture"),
        b"short"
    );
    wait_until_drained(Some(&file)).await;
    drop(file);

    let file = File::open(&path).expect("open EOF fixture");
    let error = linux_io::pread_exact_at_async(file, 100, 0)
        .await
        .expect_err("short exact read must fail");
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    // Linux's /dev/full supplies a deterministic capacity error without filling
    // a filesystem or changing the test disk's reserve space.
    let full = Arc::new(
        OpenOptions::new()
            .write(true)
            .open("/dev/full")
            .expect("open deterministic ENOSPC device"),
    );
    let error = match linux_io::write_aligned_at_async(
        full.clone(),
        aligned_pattern(9, ALIGNMENT),
        0,
    )
    .await
    {
        Ok(_) => panic!("capacity exhaustion unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
    wait_until_drained(Some(&full)).await;
    let file = Arc::new(
        OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open write-only fixture"),
    );
    linux_io::fdatasync_async(file.clone())
        .await
        .expect("backend remains usable after errors");
    wait_until_drained(Some(&file)).await;
    assert_bounded_backend(expected);
}
