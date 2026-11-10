use super::*;

pub(crate) fn catch_up(core: &Core, current: &State, prepared: &mut Prepared) -> Result<()> {
    if current.generation.id != prepared.source.generation.id {
        return Err(Error::Conflict);
    }
    let source = &prepared.source.generation;
    let mut offset = prepared.source_end;
    while offset < current.end {
        if core.stopping.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let record = source.record(offset)?;
        if record.end > current.end {
            return Err(Error::Corrupt(
                "checkpoint tail crosses its captured root".into(),
            ));
        }
        if record.kind == format::COMMIT {
            let commit = format::decode_commit(&record, offset)?;
            if commit.previous != prepared.source_commit
                || commit.sequence <= prepared.state.sequence
            {
                return Err(Error::Corrupt(
                    "checkpoint replay commit chain differs".into(),
                ));
            }
            let mut changes = Vec::with_capacity(commit.changes.len());
            for (key, entry) in commit.changes {
                let replacement = match entry {
                    Some(entry) => {
                        let value = source.value(&entry)?;
                        Some(prepared.writer.value(&key, entry.sequence, &value)?)
                    }
                    None => None,
                };
                changes.push((key, replacement));
            }
            let root = tree::apply(&mut prepared.writer, prepared.state.root, &changes)?;
            prepared.state =
                prepared
                    .writer
                    .commit(commit.sequence, root, prepared.state.commit, &changes)?;
            prepared.source_commit = offset;
        }
        offset = record.end;
    }
    if prepared.state.sequence != current.sequence || prepared.source_commit != current.commit {
        return Err(Error::Corrupt(
            "checkpoint tail did not reach the selected commit".into(),
        ));
    }
    prepared.source_end = offset;
    Ok(())
}
