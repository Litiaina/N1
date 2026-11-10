use super::*;

pub(crate) fn build(core: &Core, source: Arc<State>) -> Result<Prepared> {
    let mut id = source
        .generation
        .id
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("M1 generation counter overflow".into()))?;
    let file = loop {
        match format::create_generation(&core.path, id, &core.identity) {
            Ok(file) => break file,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                id = id
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("M1 generation counter exhausted".into()))?;
            }
            Err(error) => return Err(error),
        }
    };
    let generation = Arc::new(Generation::new(
        file,
        id,
        format::generation_path(&core.path, id),
        core.cache.clone(),
        core.poisoned.clone(),
        true,
    ));
    let mut writer = Appender {
        generation,
        end: format::DATA_START,
        // The target has no external readers before checkpoint publication.
        evict_replaced: true,
    };
    let mut cursor = tree::Cursor::new(
        source.generation.clone(),
        source.root,
        Bound::Unbounded,
        Bound::Unbounded,
    )?;
    let mut builder = tree::BulkBuilder::new();
    while let Some(entry) = cursor.next_entry()? {
        if core.stopping.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let value = source.generation.value(&entry)?;
        let entry = writer.value(&entry.key, entry.sequence, &value)?;
        builder.push(&mut writer, entry)?;
    }
    let root = builder.finish(&mut writer)?;
    let state = writer.commit(source.sequence, root, 0, &[])?;
    let mut prepared = Prepared {
        source_end: source.end,
        source_commit: source.commit,
        source,
        writer,
        state,
    };
    catch_up(core, &core.current(), &mut prepared)?;
    Ok(prepared)
}
