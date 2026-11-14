use super::*;

pub(super) fn commit(core: &Core, requests: Vec<Request>) {
    let current = core.current();
    let mut pending = BTreeMap::new();
    let mut accepted = Vec::new();
    let mut sequence = current.sequence;
    for request in requests {
        let result = core
            .healthy()
            .and_then(|_| transaction::validate(&request.data, &current, &pending));
        match result {
            Ok(true) => {
                let Some(next) = sequence.checked_add(1) else {
                    let _ = request
                        .reply
                        .send(Err(Error::Invalid("M1 sequence counter exhausted".into())));
                    continue;
                };
                sequence = next;
                for (key, mut write) in request.data.writes {
                    write.sequence = sequence;
                    pending.insert(key, write);
                }
                accepted.push(request.reply);
            }
            Ok(false) => {
                let _ = request.reply.send(Err(Error::Conflict));
            }
            Err(error) => {
                let _ = request.reply.send(Err(error));
            }
        }
    }
    if accepted.is_empty() {
        return;
    }
    let result = (|| {
        core.healthy()?;
        let mut writer = Appender::new(
            current.generation.clone(),
            current.end,
            // Retain older pages when concurrent snapshots or a batch may
            // reuse them; single-writer mutations otherwise accumulate cold
            // COW versions. This changes cache admission, never file lifetime.
            accepted.len() == 1 && Arc::strong_count(&current) == 2,
        );
        let mut changes = Vec::with_capacity(pending.len());
        for (key, write) in pending {
            let entry = write
                .value
                .as_ref()
                .map(|value| writer.value(&key, write.sequence, value))
                .transpose()?;
            changes.push((key, entry));
        }
        #[cfg(test)]
        crate::fault::pause("values-written");
        let root = tree::apply(&mut writer, current.root, &changes)?;
        #[cfg(test)]
        crate::fault::pause("tree-written");
        let mut state = writer.commit(sequence, root, current.commit, &changes)?;
        #[cfg(test)]
        crate::fault::pause("commit-written");
        // Neither the new root nor successful replies precede this barrier.
        writer.generation.file.sync_all()?;
        #[cfg(test)]
        crate::fault::pause("commit-synced");
        state.anchor_end = current.anchor_end;
        core.publish(state);
        #[cfg(test)]
        crate::fault::pause("root-published");
        Ok(())
    })();
    if result.is_err() {
        core.poisoned.store(true, Ordering::Release);
    }
    for reply in accepted {
        let _ = reply.send(result.as_ref().map(|_| ()).map_err(duplicate));
    }
}
