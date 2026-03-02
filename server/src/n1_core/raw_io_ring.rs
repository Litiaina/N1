use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use block_manager::DirectExtentIo;
use futures_util::task::AtomicWaker;

use crate::n1_core::ec::AlignedBuffer;

pub(crate) enum RawIoCompletion {
    WriteOk(AlignedBuffer),
    WriteErr(Option<AlignedBuffer>, io::Error),
}

pub(crate) enum RawIoSubmitError {
    Full(AlignedBuffer),
    Closed(AlignedBuffer),
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

pub(crate) struct RawIoRing {
    submissions: Arc<(Mutex<SubmissionState>, Condvar)>,
    completions: Arc<CompletionState>,
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

        Self { submissions, completions, workers }
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

    pub(crate) fn poll_completions(
        &self,
        cx: &std::task::Context<'_>,
    ) -> Vec<RawIoCompletion> {
        let completions = self.take_completions();
        if !completions.is_empty() {
            return completions;
        }

        self.completions.waker.register(cx.waker());
        self.take_completions()
    }
    fn take_completions(&self) -> Vec<RawIoCompletion> {
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
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
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

fn push_completion(completions: &CompletionState, completion: RawIoCompletion) {
    let mut queue = completions
        .queue
        .lock()
        .expect("raw io ring completions poisoned");
    queue.push_back(completion);
    completions.waker.wake();
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

fn worker_count_for_depth(queue_depth: usize) -> usize {
    match queue_depth {
        0..=4 => 1,
        5..=8 => 2,
        9..=16 => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::worker_count_for_depth;

    #[test]
    fn worker_count_scales_with_depth_but_stays_bounded() {
        assert_eq!(worker_count_for_depth(4), 1);
        assert_eq!(worker_count_for_depth(8), 2);
        assert_eq!(worker_count_for_depth(16), 3);
        assert_eq!(worker_count_for_depth(32), 4);
    }
}
