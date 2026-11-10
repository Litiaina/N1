use super::*;

pub(crate) fn recover(generation: Arc<Generation>, manifest: &Manifest) -> Result<State> {
    let file_end = generation.file.metadata()?.len();
    let mut state = State {
        generation: generation.clone(),
        sequence: manifest.sequence,
        root: manifest.root,
        end: manifest.end,
        commit: manifest.commit,
        anchor_end: manifest.end,
    };
    if manifest.commit != 0 {
        let record = generation.record(manifest.commit)?;
        let commit = decode_commit(&record, manifest.commit)?;
        if commit.sequence != manifest.sequence
            || commit.root != manifest.root
            || record.end != manifest.end
        {
            return Err(Error::Corrupt("M1 checkpoint and commit disagree".into()));
        }
    } else if manifest.sequence != 0 || manifest.root != 0 || manifest.end != DATA_START {
        return Err(Error::Corrupt("invalid empty M1 checkpoint".into()));
    }
    let mut offset = manifest.end;
    while offset < file_end {
        let Some(record) = read_record(&generation.file, offset, file_end)? else {
            break;
        };
        if record.kind == COMMIT {
            let commit = decode_commit(&record, offset)?;
            if commit.previous != state.commit || commit.sequence <= state.sequence {
                return Err(Error::Corrupt("M1 commit chain differs".into()));
            }
            state.sequence = commit.sequence;
            state.root = commit.root;
            state.commit = offset;
            state.end = record.end;
        }
        offset = record.end;
    }
    if state.root != 0 {
        tree::load(&generation, state.root)?;
    }
    if file_end != state.end {
        generation.file.set_len(state.end)?;
        generation.file.sync_all()?;
    }
    Ok(state)
}
