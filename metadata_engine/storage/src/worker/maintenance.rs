use super::*;

pub(super) fn start(
    core: &Arc<Core>,
    job: &mut Option<CheckpointJob>,
    waiters: &mut Vec<mpsc::SyncSender<Result<()>>>,
    prepared: Option<checkpoint::Prepared>,
) {
    let (sender, receiver) = mpsc::sync_channel(1);
    let core = core.clone();
    let spawned = std::thread::Builder::new()
        .name("m1-checkpoint".into())
        .spawn(move || {
            let result = match prepared {
                Some(mut prepared) => {
                    checkpoint::catch_up(&core, &core.current(), &mut prepared).map(|_| prepared)
                }
                None => checkpoint::build(&core, core.current()),
            };
            let _ = sender.send(result);
        });
    match spawned {
        Ok(worker) => *job = Some(CheckpointJob { receiver, worker }),
        Err(error) => {
            for reply in waiters.drain(..) {
                let _ = reply.send(Err(Error::Io(std::io::Error::new(
                    error.kind(),
                    error.to_string(),
                ))));
            }
        }
    }
}

pub(super) fn poll(
    core: &Arc<Core>,
    job: &mut Option<CheckpointJob>,
    waiters: &mut Vec<mpsc::SyncSender<Result<()>>>,
    retry_after: &mut Instant,
) {
    let result = match job.as_ref().map(|job| job.receiver.try_recv()) {
        Some(Ok(result)) => result,
        Some(Err(mpsc::TryRecvError::Disconnected)) => Err(Error::Closed),
        _ => return,
    };
    let finished = job.take().unwrap();
    let _ = finished.worker.join();
    let result = result.and_then(|prepared| {
        core.healthy()?;
        let current = core.current();
        if current.end.saturating_sub(prepared.source_end) > core.options.write_buffer_bytes {
            start(core, job, waiters, Some(prepared));
            return Ok(false);
        }
        // Publication is the only checkpoint boundary coordinated with commits.
        // Bulk copying and large catch-up tails run outside this coordinator.
        checkpoint::install(core, prepared).map(|_| true)
    });
    if matches!(result, Ok(false)) {
        return;
    }
    if result.is_err() {
        *retry_after = Instant::now() + Duration::from_secs(60);
    }
    for reply in waiters.drain(..) {
        let _ = reply.send(result.as_ref().map(|_| ()).map_err(duplicate));
    }
}
