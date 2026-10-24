//! Owned, bounded io_uring operations. Only this thread touches the ring.
//!
//! Cancellation drops a reply receiver, never an in-flight allocation or fd.
//! Publication barriers remain the caller's responsibility: await writes, then
//! sync, then publish, then sync the containing directory before acknowledging.

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use io_uring::{IoUring, Probe, opcode, squeue, types};
use tokio::sync::{Notify, oneshot};

pub(super) const CAPACITY: usize = 128;
const WAKE_ID: u64 = u64::MAX;

pub(super) enum Buffer {
    Aligned(crate::core::ec::AlignedBuffer),
    Bytes(Vec<u8>),
}

impl Buffer {
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Aligned(buffer) => buffer.as_slice(),
            Self::Bytes(buffer) => buffer,
        }
    }

    pub(super) fn as_mut_slice(&mut self) -> io::Result<&mut [u8]> {
        match self {
            Self::Aligned(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "aligned buffers support writes only",
            )),
            Self::Bytes(buffer) => Ok(buffer),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Snapshot {
    pub queue_depth: usize,
    pub inflight: usize,
    pub submitted: u64,
    pub completed: u64,
}

#[derive(Default)]
struct Counters {
    inflight: AtomicUsize,
    submitted: AtomicU64,
    completed: AtomicU64,
}

#[derive(Clone, Copy)]
enum Kind {
    Read,
    ReadSome,
    Write,
    Sync { data_only: bool },
}

struct Request {
    file: Arc<File>,
    buffer: Option<Buffer>,
    offset: u64,
    done: usize,
    started: bool,
    kind: Kind,
    reply: oneshot::Sender<io::Result<Option<Buffer>>>,
}

impl Request {
    fn chunk_length(&self) -> usize {
        let limit = if matches!(self.buffer, Some(Buffer::Aligned(_))) {
            (u32::MAX as usize / crate::core::ec::ALIGNMENT) * crate::core::ec::ALIGNMENT
        } else {
            u32::MAX as usize
        };
        self.buffer
            .as_ref()
            .map_or(0, |buffer| buffer.as_slice().len() - self.done)
            .min(limit)
    }

    fn entry(&mut self) -> squeue::Entry {
        let fd = types::Fd(self.file.as_raw_fd());
        let offset = self.offset + self.done as u64;
        let length = self.chunk_length() as u32;
        let entry = match self.kind {
            Kind::Read | Kind::ReadSome => {
                let tail = &mut self
                    .buffer
                    .as_mut()
                    .expect("read buffer")
                    .as_mut_slice()
                    .expect("validated mutable read buffer")[self.done..];
                opcode::Read::new(fd, tail.as_mut_ptr(), length)
                    .offset(offset)
                    .build()
            }
            Kind::Write => {
                let tail = &self.buffer.as_ref().expect("write buffer").as_slice()[self.done..];
                opcode::Write::new(fd, tail.as_ptr(), length)
                    .offset(offset)
                    .build()
            }
            Kind::Sync { data_only } => opcode::Fsync::new(fd)
                .flags(if data_only {
                    types::FsyncFlags::DATASYNC
                } else {
                    types::FsyncFlags::empty()
                })
                .build(),
        };
        entry
    }

    fn advance(&mut self, result: i32) -> io::Result<bool> {
        if result == -libc::EINTR {
            return Ok(false);
        }
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        if matches!(self.kind, Kind::Sync { .. }) {
            return Ok(true);
        }
        let length = self.buffer.as_ref().expect("I/O buffer").as_slice().len();
        if result as usize > self.chunk_length() {
            fatal("kernel returned an impossible I/O completion length");
        }
        if matches!(self.kind, Kind::ReadSome) {
            self.done = result as usize;
            match self.buffer.as_mut() {
                Some(Buffer::Bytes(buffer)) => buffer.truncate(self.done),
                _ => fatal("read-some completed without an owned mutable buffer"),
            }
            return Ok(true);
        }
        if result == 0 {
            let kind = if matches!(self.kind, Kind::Read) {
                io::ErrorKind::UnexpectedEof
            } else {
                io::ErrorKind::WriteZero
            };
            return Err(io::Error::new(kind, "io_uring made no I/O progress"));
        }
        self.done += result as usize;
        Ok(self.done == length)
    }
}

struct Handle {
    queue: Arc<Queue>,
    wake: Arc<File>,
    counters: Arc<Counters>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.queue.state.lock().unwrap().closed = true;
        self.queue.available.notify_waiters();
        notify(&self.wake);
    }
}

#[derive(Clone)]
pub(super) struct Backend(Arc<Handle>);

#[derive(Default)]
struct QueueState {
    requests: VecDeque<Request>,
    closed: bool,
}

#[derive(Default)]
struct Queue {
    state: Mutex<QueueState>,
    available: Notify,
}

impl Backend {
    pub(super) fn new() -> io::Result<Self> {
        let ring = IoUring::new((CAPACITY * 2) as u32)?;
        let mut probe = Probe::new();
        ring.submitter().register_probe(&mut probe)?;
        if ![
            opcode::Read::CODE,
            opcode::Write::CODE,
            opcode::Fsync::CODE,
            opcode::PollAdd::CODE,
        ]
        .into_iter()
        .all(|code| probe.is_supported(code))
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring requires read, write, fsync and poll support",
            ));
        }
        // CLOEXEC prevents inherited control fds; nonblocking avoids a producer
        // ever waiting on wake-up I/O (an already-full counter is already awake).
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let wake = Arc::new(unsafe { File::from_raw_fd(fd) });
        let counters = Arc::new(Counters::default());
        let queue = Arc::new(Queue::default());
        let driver_queue = Arc::clone(&queue);
        let driver_wake = Arc::clone(&wake);
        let driver_counters = Arc::clone(&counters);
        std::thread::Builder::new()
            .name("n1-io-uring".into())
            .spawn(move || drive(ring, driver_queue, driver_wake, driver_counters))?;
        Ok(Self(Arc::new(Handle {
            queue,
            wake,
            counters,
        })))
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot {
            queue_depth: self.0.queue.state.lock().unwrap().requests.len(),
            inflight: self.0.counters.inflight.load(Ordering::Relaxed),
            submitted: self.0.counters.submitted.load(Ordering::Relaxed),
            completed: self.0.counters.completed.load(Ordering::Relaxed),
        }
    }

    fn request(
        file: Arc<File>,
        mut buffer: Option<Buffer>,
        offset: u64,
        kind: Kind,
        reply: oneshot::Sender<io::Result<Option<Buffer>>>,
    ) -> io::Result<Request> {
        let length = buffer.as_ref().map_or(0, |buffer| buffer.as_slice().len());
        if offset
            .checked_add(length as u64)
            .is_none_or(|end| end > i64::MAX as u64)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O offset exceeds the file offset range",
            ));
        }
        if matches!(kind, Kind::Read | Kind::ReadSome) {
            buffer.as_mut().expect("read buffer").as_mut_slice()?;
        }
        Ok(Request {
            file,
            buffer,
            offset,
            done: 0,
            started: false,
            kind,
            reply,
        })
    }

    async fn execute(
        &self,
        file: Arc<File>,
        buffer: Option<Buffer>,
        offset: u64,
        kind: Kind,
    ) -> io::Result<Option<Buffer>> {
        let (reply, receiver) = oneshot::channel();
        let request = Self::request(file, buffer, offset, kind, reply)?;
        let wake = loop {
            // Register before checking capacity: a dequeue cannot be missed.
            let available = self.0.queue.available.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            {
                let mut state = self.0.queue.state.lock().unwrap();
                if state.closed {
                    return Err(closed());
                }
                if state.requests.len() < CAPACITY {
                    let wake = state.requests.is_empty();
                    state.requests.push_back(request);
                    break wake;
                }
            }
            available.await;
        };
        if wake {
            notify(&self.0.wake);
        }
        receiver.await.map_err(|_| closed())?
    }

    pub(super) async fn write_all(
        &self,
        file: Arc<File>,
        buffer: Buffer,
        offset: u64,
    ) -> io::Result<Buffer> {
        self.execute(file, Some(buffer), offset, Kind::Write)
            .await?
            .ok_or_else(closed)
    }

    pub(super) async fn read_exact(
        &self,
        file: Arc<File>,
        buffer: Buffer,
        offset: u64,
    ) -> io::Result<Buffer> {
        self.execute(file, Some(buffer), offset, Kind::Read)
            .await?
            .ok_or_else(closed)
    }

    pub(super) async fn read_some(
        &self,
        file: Arc<File>,
        buffer: Buffer,
        offset: u64,
    ) -> io::Result<Buffer> {
        self.execute(file, Some(buffer), offset, Kind::ReadSome)
            .await?
            .ok_or_else(closed)
    }

    pub(super) async fn sync(&self, file: Arc<File>, data_only: bool) -> io::Result<()> {
        self.execute(file, None, 0, Kind::Sync { data_only })
            .await
            .map(|_| ())
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "io_uring driver stopped")
}

fn fatal(message: &str) -> ! {
    eprintln!("N1 io_uring fatal: {message}; refusing to release potentially kernel-owned I/O");
    std::process::abort()
}

fn notify(wake: &File) {
    let value = 1_u64;
    loop {
        let result = unsafe { libc::write(wake.as_raw_fd(), (&value as *const u64).cast(), 8) };
        if result == 8 {
            return;
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            return;
        }
        if error.kind() != io::ErrorKind::Interrupted {
            fatal("eventfd wake failed after queueing work");
        }
    }
}

fn clear_wake(wake: &File) {
    let mut value = 0_u64;
    loop {
        let result = unsafe { libc::read(wake.as_raw_fd(), (&mut value as *mut u64).cast(), 8) };
        if result == 8 {
            continue;
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            return;
        }
        if error.kind() != io::ErrorKind::Interrupted {
            fatal("eventfd read failed");
        }
    }
}

struct AbortOnPanic;
impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            fatal("driver panicked with potentially outstanding I/O");
        }
    }
}

fn drive(mut ring: IoUring, queue: Arc<Queue>, wake: Arc<File>, counters: Arc<Counters>) {
    let mut slots: Vec<Option<Request>> = (0..CAPACITY).map(|_| None).collect();
    let mut free: Vec<usize> = (0..CAPACITY).rev().collect();
    let mut ready = VecDeque::with_capacity(CAPACITY);
    let mut completions = Vec::with_capacity(CAPACITY + 1);
    let mut arm_wake = true;
    let mut disconnected = false;
    // Declared last so unwinding aborts BEFORE slots or their allocations drop.
    let _abort_on_panic = AbortOnPanic;
    loop {
        completions.extend(
            ring.completion()
                .map(|entry| (entry.user_data(), entry.result())),
        );
        for (id, result) in completions.drain(..) {
            if id == WAKE_ID {
                if result < 0 {
                    fatal("eventfd poll failed");
                }
                clear_wake(&wake);
                arm_wake = true;
                continue;
            }
            let Some(slot) = slots.get_mut(id as usize) else {
                fatal("invalid completion id");
            };
            let Some(mut request) = slot.take() else {
                fatal("duplicate completion id");
            };
            match request.advance(result) {
                Ok(false) => {
                    *slot = Some(request);
                    ready.push_back(id as usize);
                }
                outcome => {
                    counters.inflight.fetch_sub(1, Ordering::Relaxed);
                    counters.completed.fetch_add(1, Ordering::Relaxed);
                    let _ = request.reply.send(outcome.map(|_| request.buffer));
                    free.push(id as usize);
                }
            }
        }
        // Limit even zero-byte requests per pass so a producer cannot starve
        // completion processing by continuously replacing them in the queue.
        for _ in 0..CAPACITY {
            let Some(index) = free.pop() else {
                break;
            };
            let request = {
                let mut state = queue.state.lock().unwrap();
                disconnected = state.closed;
                state.requests.pop_front()
            };
            match request {
                Some(request) => {
                    queue.available.notify_waiters();
                    if request
                        .buffer
                        .as_ref()
                        .is_some_and(|buffer| buffer.as_slice().is_empty())
                    {
                        counters.submitted.fetch_add(1, Ordering::Relaxed);
                        counters.completed.fetch_add(1, Ordering::Relaxed);
                        let _ = request.reply.send(Ok(request.buffer));
                        free.push(index);
                        continue;
                    }
                    slots[index] = Some(request);
                    counters.inflight.fetch_add(1, Ordering::Relaxed);
                    ready.push_back(index);
                }
                None => {
                    free.push(index);
                    break;
                }
            }
        }
        if disconnected && free.len() == CAPACITY {
            return;
        }
        if arm_wake {
            let entry = opcode::PollAdd::new(types::Fd(wake.as_raw_fd()), libc::POLLIN as u32)
                .build()
                .user_data(WAKE_ID);
            if unsafe { ring.submission().push(&entry) }.is_ok() {
                arm_wake = false;
            }
        }
        while let Some(index) = ready.pop_front() {
            let request = slots[index].as_mut().expect("queued request");
            let entry = request.entry().user_data(index as u64);
            if unsafe { ring.submission().push(&entry) }.is_err() {
                ready.push_front(index);
                break;
            }
            if !request.started {
                request.started = true;
                counters.submitted.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Zero-byte operations complete in userspace. If the admission budget
        // left queued work and free slots, no I/O completion may exist to wake
        // us: submit the armed poll without waiting and drain the next batch.
        let queued_work = !free.is_empty() && !queue.state.lock().unwrap().requests.is_empty();
        if let Err(error) = ring.submit_and_wait(usize::from(!queued_work)) {
            if error.kind() != io::ErrorKind::Interrupted {
                fatal(&format!("submit/wait failed: {error}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(kind: Kind, length: usize) -> Request {
        Backend::request(
            Arc::new(File::open("/dev/null").unwrap()),
            Some(Buffer::Bytes(vec![0; length])),
            0,
            kind,
            oneshot::channel().0,
        )
        .unwrap()
    }

    #[test]
    fn partial_completion_and_interrupt_preserve_exact_progress() {
        let mut operation = request(Kind::Write, 4096);
        assert!(!operation.advance(1024).unwrap());
        assert_eq!(operation.done, 1024);
        assert!(!operation.advance(-libc::EINTR).unwrap());
        assert_eq!(operation.done, 1024);
        assert!(operation.advance(3072).unwrap());
        assert_eq!(operation.done, 4096);
        let mut read = request(Kind::Read, 4096);
        assert!(!read.advance(17).unwrap());
        assert!(!read.advance(-libc::EINTR).unwrap());
        assert_eq!(read.done, 17);
        assert!(read.advance(4079).unwrap());
        assert_eq!(read.done, 4096);
    }

    #[test]
    fn zero_progress_and_errors_do_not_become_success() {
        assert_eq!(
            request(Kind::Read, 4096).advance(0).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(
            request(Kind::Write, 4096).advance(0).unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        assert_eq!(
            request(Kind::Write, 4096)
                .advance(-libc::ENOSPC)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOSPC)
        );
        assert_eq!(
            request(Kind::Sync { data_only: true }, 0)
                .advance(-libc::EIO)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
    }

    #[test]
    fn direct_short_write_keeps_buffer_owned_and_does_not_fallback_on_alignment_error() {
        let mut buffer = crate::core::ec::AlignedBuffer::new(8192);
        buffer.fill_zeros();
        let file = Arc::new(File::open("/dev/null").unwrap());
        let mut operation = Backend::request(
            file,
            Some(Buffer::Aligned(buffer)),
            4096,
            Kind::Write,
            oneshot::channel().0,
        )
        .unwrap();
        assert!(!operation.advance(4096).unwrap());
        assert_eq!(operation.offset + operation.done as u64, 8192);
        assert!(!operation.advance(512).unwrap());
        assert_eq!(operation.done, 4608);
        // An actual direct descriptor may reject an insufficiently aligned
        // remaining tail. That error is terminal, never a buffered retry.
        assert_eq!(
            operation.advance(-libc::EINVAL).unwrap_err().raw_os_error(),
            Some(libc::EINVAL)
        );
        assert_eq!(operation.buffer.as_ref().unwrap().as_slice().len(), 8192);
    }

    #[test]
    fn invalid_offset_and_unsupported_read_buffer_are_rejected_before_submission() {
        let file = Arc::new(File::open("/dev/null").unwrap());
        let offset_error = Backend::request(
            Arc::clone(&file),
            Some(Buffer::Bytes(vec![0; 1])),
            i64::MAX as u64,
            Kind::Write,
            oneshot::channel().0,
        );
        assert!(matches!(offset_error, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
        let mut buffer = crate::core::ec::AlignedBuffer::new(4096);
        buffer.fill_zeros();
        let read_error = Backend::request(
            file,
            Some(Buffer::Aligned(buffer)),
            0,
            Kind::Read,
            oneshot::channel().0,
        );
        assert!(matches!(read_error, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    }

    #[test]
    fn owned_read_buffer_keeps_its_allocation_until_completion() {
        let bytes = vec![17; 8192];
        let allocation = bytes.as_ptr();
        let capacity = bytes.capacity();
        let mut read = Backend::request(
            Arc::new(File::open("/dev/null").unwrap()),
            Some(Buffer::Bytes(bytes)),
            4096,
            Kind::ReadSome,
            oneshot::channel().0,
        )
        .unwrap();
        assert!(!read.advance(-libc::EINTR).unwrap());
        assert_eq!(
            read.buffer.as_ref().unwrap().as_slice().as_ptr(),
            allocation
        );
        assert!(read.advance(17).unwrap());
        match read.buffer.take().unwrap() {
            Buffer::Bytes(mut bytes) => {
                assert_eq!(bytes.as_ptr(), allocation);
                assert_eq!(bytes.len(), 17);
                assert_eq!(bytes.capacity(), capacity);
                bytes.resize(8192, 0);
                assert_eq!(bytes.as_ptr(), allocation);
            }
            Buffer::Aligned(_) => panic!("read changed its owned buffer type"),
        }
    }

    #[test]
    fn read_some_returns_short_reads_and_eof_without_changing_exact_read_semantics() {
        let mut short = request(Kind::ReadSome, 4096);
        assert!(!short.advance(-libc::EINTR).unwrap());
        assert_eq!(short.done, 0);
        assert_eq!(short.buffer.as_ref().unwrap().as_slice().len(), 4096);
        assert!(short.advance(17).unwrap());
        assert_eq!(short.done, 17);
        assert_eq!(short.buffer.as_ref().unwrap().as_slice().len(), 17);
        let mut eof = request(Kind::ReadSome, 4096);
        assert!(eof.advance(0).unwrap());
        assert!(eof.buffer.as_ref().unwrap().as_slice().is_empty());
        assert_eq!(
            request(Kind::Read, 4096).advance(0).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        let mut aligned = crate::core::ec::AlignedBuffer::new(4096);
        aligned.fill_zeros();
        let invalid = Backend::request(
            Arc::new(File::open("/dev/null").unwrap()),
            Some(Buffer::Aligned(aligned)),
            0,
            Kind::ReadSome,
            oneshot::channel().0,
        );
        assert!(matches!(invalid, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    }
}
