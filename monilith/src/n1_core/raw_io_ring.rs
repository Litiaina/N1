use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use block_manager::DirectExtentIo;
use futures_util::task::AtomicWaker;
use tokio::sync::Notify;

use crate::n1_core::ec::AlignedBuffer;

pub(crate) enum RawIoCompletion {
    WriteOk(AlignedBuffer),
    WriteErr(Option<AlignedBuffer>, io::Error),
}

pub(crate) enum RawIoSubmitError {
    Full(AlignedBuffer),
    Closed(AlignedBuffer),
}

pub(crate) enum RawReadCompletion {
    ReadOk {
        id: u64,
        buffer: Vec<u8>,
    },
    ReadErr {
        id: u64,
        buffer: Option<Vec<u8>>,
        err: io::Error,
    },
}

pub(crate) enum RawReadSubmitError {
    Full(Vec<u8>),
    Closed(Vec<u8>),
}

struct RawIoCommand {
    offset: u64,
    buffer: AlignedBuffer,
}

struct SubmissionState {
    queue: VecDeque<RawIoCommand>,
    max_depth: usize,
    closed: bool,
}

struct CompletionState {
    queue: Mutex<VecDeque<RawIoCompletion>>,
    waker: AtomicWaker,
}

struct RawReadCommand {
    id: u64,
    offset: u64,
    buffer: Vec<u8>,
}

struct RawReadSubmissionState {
    queue: VecDeque<RawReadCommand>,
    max_depth: usize,
    closed: bool,
}

struct ReadCompletionState {
    queue: Mutex<VecDeque<RawReadCompletion>>,
    parked: Mutex<HashMap<u64, RawReadCompletion>>,
    notify: Notify,
}

pub(crate) struct RawIoRing {
    submissions: Arc<(Mutex<SubmissionState>, Condvar)>,
    completions: Arc<CompletionState>,
    workers: Vec<JoinHandle<()>>,
}

pub(crate) struct RawReadRing {
    submissions: Arc<(Mutex<RawReadSubmissionState>, Condvar)>,
    completions: Arc<ReadCompletionState>,
    next_id: AtomicU64,
    workers: Vec<JoinHandle<()>>,
}

impl RawIoRing {
    pub(crate) fn new(io: DirectExtentIo, queue_depth: usize) -> Self {
        let queue_depth = queue_depth.clamp(2, 32);
        let submissions = Arc::new((
            Mutex::new(SubmissionState {
                queue: VecDeque::with_capacity(queue_depth),
                max_depth: queue_depth,
                closed: false,
            }),
            Condvar::new(),
        ));
        let completions = Arc::new(CompletionState {
            queue: Mutex::new(VecDeque::with_capacity(queue_depth)),
            waker: AtomicWaker::new(),
        });

        let mut workers = Vec::new();
        let worker_count = worker_count_for_depth(queue_depth);
        for idx in 0..worker_count {
            let worker_io = io.clone();
            let worker_submissions = Arc::clone(&submissions);
            let worker_completions = Arc::clone(&completions);
            let handle = thread::Builder::new()
                .name(format!("n1-raw-io-{}", idx))
                .spawn(move || worker_loop(worker_io, worker_submissions, worker_completions))
                .expect("failed to spawn raw io worker");
            workers.push(handle);
        }

        Self {
            submissions,
            completions,
            workers,
        }
    }

    pub(crate) fn submit_write(
        &self,
        offset: u64,
        buffer: AlignedBuffer,
    ) -> Result<(), RawIoSubmitError> {
        let (lock, cvar) = &*self.submissions;
        let mut state = lock.lock().expect("raw io ring submissions poisoned");
        if state.closed {
            return Err(RawIoSubmitError::Closed(buffer));
        }
        if state.queue.len() >= state.max_depth {
            return Err(RawIoSubmitError::Full(buffer));
        }
        state.queue.push_back(RawIoCommand { offset, buffer });
        cvar.notify_one();
        Ok(())
    }

    pub(crate) fn poll_completions(&self, cx: &std::task::Context<'_>) -> Vec<RawIoCompletion> {
        self.completions.waker.register(cx.waker());
        self.take_completions_now()
    }

    pub(crate) fn take_completions_now(&self) -> Vec<RawIoCompletion> {
        let mut queue = self
            .completions
            .queue
            .lock()
            .expect("raw io ring completions poisoned");
        queue.drain(..).collect()
    }

    fn close(&self) {
        let (lock, cvar) = &*self.submissions;
        let mut state = lock.lock().expect("raw io ring submissions poisoned");
        state.closed = true;
        cvar.notify_all();
    }
}

impl Drop for RawIoRing {
    fn drop(&mut self) {
        self.close();
        let workers: Vec<JoinHandle<()>> = self.workers.drain(..).collect();
        if workers.is_empty() {
            return;
        }
        // Never block async runtime threads on potentially slow direct-IO worker joins.
        let _ = thread::Builder::new()
            .name("n1-raw-io-reaper".to_string())
            .spawn(move || {
                for worker in workers {
                    let _ = worker.join();
                }
            });
    }
}

impl RawReadRing {
    pub(crate) fn new(io: DirectExtentIo, queue_depth: usize) -> Self {
        let queue_depth = queue_depth.clamp(1, 16);
        let submissions = Arc::new((
            Mutex::new(RawReadSubmissionState {
                queue: VecDeque::with_capacity(queue_depth),
                max_depth: queue_depth,
                closed: false,
            }),
            Condvar::new(),
        ));
        let completions = Arc::new(ReadCompletionState {
            queue: Mutex::new(VecDeque::with_capacity(queue_depth)),
            parked: Mutex::new(HashMap::with_capacity(queue_depth)),
            notify: Notify::new(),
        });

        let mut workers = Vec::new();
        let worker_count = read_worker_count_for_depth(queue_depth);
        for idx in 0..worker_count {
            let worker_io = io.clone();
            let worker_submissions = Arc::clone(&submissions);
            let worker_completions = Arc::clone(&completions);
            let handle = thread::Builder::new()
                .name(format!("n1-raw-read-{}", idx))
                .spawn(move || read_worker_loop(worker_io, worker_submissions, worker_completions))
                .expect("failed to spawn raw read worker");
            workers.push(handle);
        }

        Self {
            submissions,
            completions,
            next_id: AtomicU64::new(1),
            workers,
        }
    }

    pub(crate) fn submit_read(
        &self,
        offset: u64,
        buffer: Vec<u8>,
    ) -> Result<u64, RawReadSubmitError> {
        let (lock, cvar) = &*self.submissions;
        let mut state = lock.lock().expect("raw read ring submissions poisoned");
        if state.closed {
            return Err(RawReadSubmitError::Closed(buffer));
        }
        if state.queue.len() >= state.max_depth {
            return Err(RawReadSubmitError::Full(buffer));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        state.queue.push_back(RawReadCommand { id, offset, buffer });
        cvar.notify_one();
        Ok(id)
    }

    pub(crate) fn take_completion_for(&self, id: u64) -> Option<RawReadCompletion> {
        if let Some(completion) = self
            .completions
            .parked
            .lock()
            .expect("raw read ring parked completions poisoned")
            .remove(&id)
        {
            return Some(completion);
        }

        let mut queue = self
            .completions
            .queue
            .lock()
            .expect("raw read ring completions poisoned");
        if queue.is_empty() {
            return None;
        }

        let mut parked = self
            .completions
            .parked
            .lock()
            .expect("raw read ring parked completions poisoned");
        let mut matched = None;
        while let Some(completion) = queue.pop_front() {
            let completion_id = match &completion {
                RawReadCompletion::ReadOk { id, .. } => *id,
                RawReadCompletion::ReadErr { id, .. } => *id,
            };
            if completion_id == id && matched.is_none() {
                matched = Some(completion);
            } else {
                parked.insert(completion_id, completion);
            }
        }
        matched
    }

    pub(crate) async fn wait_for_completion(&self, id: u64) -> RawReadCompletion {
        loop {
            if let Some(completion) = self.take_completion_for(id) {
                return completion;
            }
            self.completions.notify.notified().await;
        }
    }

    fn close(&self) {
        let (lock, cvar) = &*self.submissions;
        let mut state = lock.lock().expect("raw read ring submissions poisoned");
        state.closed = true;
        cvar.notify_all();
    }
}

impl Drop for RawReadRing {
    fn drop(&mut self) {
        self.close();
        let workers: Vec<JoinHandle<()>> = self.workers.drain(..).collect();
        if workers.is_empty() {
            return;
        }
        // Never block async runtime threads on potentially slow direct-IO worker joins.
        let _ = thread::Builder::new()
            .name("n1-raw-read-reaper".to_string())
            .spawn(move || {
                for worker in workers {
                    let _ = worker.join();
                }
            });
    }
}

fn worker_loop(
    io: DirectExtentIo,
    submissions: Arc<(Mutex<SubmissionState>, Condvar)>,
    completions: Arc<CompletionState>,
) {
    loop {
        let command = {
            let (lock, cvar) = &*submissions;
            let mut state = lock.lock().expect("raw io ring submissions poisoned");
            while state.queue.is_empty() && !state.closed {
                state = cvar.wait(state).expect("raw io ring condvar poisoned");
            }
            if state.closed && state.queue.is_empty() {
                return;
            }
            state.queue.pop_front()
        };

        let Some(mut command) = command else {
            continue;
        };

        let result = io.write(command.offset, command.buffer.as_slice());
        match result {
            Ok(()) => {
                command.buffer.clear();
                push_completion(&completions, RawIoCompletion::WriteOk(command.buffer));
            }
            Err(e) => {
                let err = io::Error::other(format!("block manager: {}", e));
                fail_ring(&submissions, &completions, Some(command.buffer), err);
                return;
            }
        }
    }
}

fn read_worker_loop(
    io: DirectExtentIo,
    submissions: Arc<(Mutex<RawReadSubmissionState>, Condvar)>,
    completions: Arc<ReadCompletionState>,
) {
    loop {
        let command = {
            let (lock, cvar) = &*submissions;
            let mut state = lock.lock().expect("raw read ring submissions poisoned");
            while state.queue.is_empty() && !state.closed {
                state = cvar.wait(state).expect("raw read ring condvar poisoned");
            }
            if state.closed && state.queue.is_empty() {
                return;
            }
            state.queue.pop_front()
        };

        let Some(mut command) = command else {
            continue;
        };

        let result = io.read_into(command.offset, &mut command.buffer);
        match result {
            Ok(()) => push_read_completion(
                &completions,
                RawReadCompletion::ReadOk {
                    id: command.id,
                    buffer: command.buffer,
                },
            ),
            Err(e) => {
                let err = io::Error::other(format!("block manager: {}", e));
                fail_read_ring(
                    &submissions,
                    &completions,
                    command.id,
                    Some(command.buffer),
                    err,
                );
                return;
            }
        }
    }
}

fn push_completion(completions: &CompletionState, completion: RawIoCompletion) {
    let mut queue = completions
        .queue
        .lock()
        .expect("raw io ring completions poisoned");
    queue.push_back(completion);
    completions.waker.wake();
}

fn push_read_completion(completions: &ReadCompletionState, completion: RawReadCompletion) {
    let mut queue = completions
        .queue
        .lock()
        .expect("raw read ring completions poisoned");
    queue.push_back(completion);
    completions.notify.notify_waiters();
}

fn fail_ring(
    submissions: &Arc<(Mutex<SubmissionState>, Condvar)>,
    completions: &CompletionState,
    failed_buffer: Option<AlignedBuffer>,
    err: io::Error,
) {
    let mut pending = Vec::new();
    {
        let (lock, cvar) = &**submissions;
        let mut state = lock.lock().expect("raw io ring submissions poisoned");
        state.closed = true;
        pending.extend(state.queue.drain(..));
        cvar.notify_all();
    }

    let err_kind = err.kind();
    let err_message = err.to_string();
    {
        let mut queue = completions
            .queue
            .lock()
            .expect("raw io ring completions poisoned");
        queue.push_back(RawIoCompletion::WriteErr(failed_buffer, err));
        for pending_cmd in pending {
            queue.push_back(RawIoCompletion::WriteErr(
                Some(pending_cmd.buffer),
                io::Error::new(err_kind, err_message.clone()),
            ));
        }
    }
    completions.waker.wake();
}

fn fail_read_ring(
    submissions: &Arc<(Mutex<RawReadSubmissionState>, Condvar)>,
    completions: &ReadCompletionState,
    failed_id: u64,
    failed_buffer: Option<Vec<u8>>,
    err: io::Error,
) {
    let mut pending = Vec::new();
    {
        let (lock, cvar) = &**submissions;
        let mut state = lock.lock().expect("raw read ring submissions poisoned");
        state.closed = true;
        pending.extend(state.queue.drain(..));
        cvar.notify_all();
    }

    let err_kind = err.kind();
    let err_message = err.to_string();
    {
        let mut queue = completions
            .queue
            .lock()
            .expect("raw read ring completions poisoned");
        queue.push_back(RawReadCompletion::ReadErr {
            id: failed_id,
            buffer: failed_buffer,
            err,
        });
        for pending_cmd in pending {
            queue.push_back(RawReadCompletion::ReadErr {
                id: pending_cmd.id,
                buffer: Some(pending_cmd.buffer),
                err: io::Error::new(err_kind, err_message.clone()),
            });
        }
    }
    completions.notify.notify_waiters();
}

fn worker_count_for_depth(queue_depth: usize) -> usize {
    match queue_depth {
        0..=8 => 1,
        9..=16 => 2,
        _ => 3,
    }
}

fn read_worker_count_for_depth(queue_depth: usize) -> usize {
    match queue_depth {
        0..=4 => 1,
        5..=12 => 2,
        _ => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::{read_worker_count_for_depth, worker_count_for_depth};

    #[test]
    fn worker_count_scales_with_depth_but_stays_bounded() {
        assert_eq!(worker_count_for_depth(4), 1);
        assert_eq!(worker_count_for_depth(8), 1);
        assert_eq!(worker_count_for_depth(16), 2);
        assert_eq!(worker_count_for_depth(32), 3);
    }

    #[test]
    fn read_worker_count_scales_with_depth_but_stays_bounded() {
        assert_eq!(read_worker_count_for_depth(2), 1);
        assert_eq!(read_worker_count_for_depth(8), 2);
        assert_eq!(read_worker_count_for_depth(16), 3);
    }
}
