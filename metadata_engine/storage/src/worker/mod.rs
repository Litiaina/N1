//! A bounded durable commit queue. Transaction preparation and readers run
//! independently; only validation, root construction and publication coordinate.
mod group;
mod maintenance;
#[cfg(test)]
mod tests;

use crate::{Core, Error, Result, checkpoint, format::Appender, transaction, tree};
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering, mpsc},
    thread::JoinHandle,
    time::{Duration, Instant},
};

pub(crate) struct Request {
    pub data: transaction::TransactionData,
    pub reply: mpsc::SyncSender<Result<()>>,
}

pub(crate) enum Command {
    Commit(Request),
    Checkpoint(mpsc::SyncSender<Result<()>>),
    Shutdown,
}

struct CheckpointJob {
    receiver: mpsc::Receiver<Result<checkpoint::Prepared>>,
    worker: JoinHandle<()>,
}

pub(crate) fn run(core: Arc<Core>, receiver: mpsc::Receiver<Command>) {
    let mut deferred = None;
    let mut checkpoint = None;
    let mut waiters = Vec::new();
    let mut retry_after = Instant::now();
    loop {
        maintenance::poll(&core, &mut checkpoint, &mut waiters, &mut retry_after);
        let command = match deferred
            .take()
            .map(Ok)
            .unwrap_or_else(|| receiver.recv_timeout(Duration::from_millis(10)))
        {
            Ok(command) => command,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        match command {
            Command::Shutdown => break,
            Command::Checkpoint(reply) => {
                if let Err(error) = core.healthy() {
                    let _ = reply.send(Err(error));
                    continue;
                }
                waiters.push(reply);
                if checkpoint.is_none() {
                    maintenance::start(&core, &mut checkpoint, &mut waiters, None);
                }
            }
            Command::Commit(first) => {
                let mut bytes = first
                    .data
                    .writes
                    .iter()
                    .map(|(key, write)| key.len() + write.value.as_ref().map_or(0, |v| v.len()))
                    .sum::<usize>();
                let mut requests = vec![first];
                // One valid transaction can exceed the batching target. Keep
                // it intact, but never combine valid requests into a commit
                // record that exceeds the format's bound at large settings.
                let group_limit =
                    core.options
                        .write_buffer_bytes
                        .min(crate::MAX_TRANSACTION_BYTES as u64) as usize;
                while requests.len() < 64 && bytes < group_limit {
                    match receiver.try_recv() {
                        Ok(Command::Commit(request)) => {
                            let next_bytes = request
                                .data
                                .writes
                                .iter()
                                .map(|(key, write)| {
                                    key.len() + write.value.as_ref().map_or(0, |v| v.len())
                                })
                                .sum::<usize>();
                            if bytes + next_bytes > group_limit {
                                deferred = Some(Command::Commit(request));
                                break;
                            }
                            bytes += next_bytes;
                            requests.push(request);
                        }
                        Ok(other) => {
                            deferred = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                group::commit(&core, requests);
                let state = core.current();
                let threshold = core.options.journal_bytes.max(state.anchor_end / 2);
                if checkpoint.is_none()
                    && Instant::now() >= retry_after
                    && !core.poisoned.load(Ordering::Acquire)
                    && state.end.saturating_sub(state.anchor_end) >= threshold
                {
                    maintenance::start(&core, &mut checkpoint, &mut waiters, None);
                }
            }
        }
    }
    core.stopping.store(true, Ordering::Release);
    if let Some(job) = checkpoint {
        let _ = job.worker.join();
    }
    for reply in waiters {
        let _ = reply.send(Err(Error::Closed));
    }
    while let Ok(command) = receiver.try_recv() {
        match command {
            Command::Commit(request) => {
                let _ = request.reply.send(Err(Error::Closed));
            }
            Command::Checkpoint(reply) => {
                let _ = reply.send(Err(Error::Closed));
            }
            Command::Shutdown => (),
        }
    }
}

fn duplicate(error: &Error) -> Error {
    match error {
        Error::Io(error) => Error::Io(std::io::Error::new(error.kind(), error.to_string())),
        Error::Invalid(message) => Error::Invalid(message.clone()),
        Error::Incompatible(message) => Error::Incompatible(message.clone()),
        Error::Corrupt(message) => Error::Corrupt(message.clone()),
        Error::Conflict => Error::Conflict,
        Error::Closed => Error::Closed,
        Error::Locked => Error::Locked,
        Error::Poisoned => Error::Poisoned,
    }
}
