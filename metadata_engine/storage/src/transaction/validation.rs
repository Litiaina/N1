use super::*;

pub(crate) fn validate(
    data: &TransactionData,
    current: &State,
    pending: &BTreeMap<Vec<u8>, Mutation>,
) -> Result<bool> {
    if data.base.sequence == current.sequence && pending.is_empty() {
        return Ok(true);
    }
    let version = |state: &State, key: &[u8]| -> Result<Option<u64>> {
        Ok(tree::get(&state.generation, state.root, key)?.map(|e| e.sequence))
    };
    let latest = |key: &[u8]| -> Result<Option<u64>> {
        if let Some(write) = pending.get(key) {
            Ok(write.value.as_ref().map(|_| write.sequence))
        } else {
            version(current, key)
        }
    };
    for (key, expected) in &data.points {
        if latest(key)? != *expected {
            return Ok(false);
        }
    }
    for key in data.writes.keys() {
        if latest(key)? != version(&data.base, key)? {
            return Ok(false);
        }
    }
    for (start, end) in &data.ranges {
        let mut original = tree::Cursor::new(
            data.base.generation.clone(),
            data.base.root,
            start.clone(),
            end.clone(),
        )?;
        let mut latest = Merged::new(current, pending, start.clone(), end.clone())?;
        loop {
            match (original.next_entry()?, latest.next_entry()?) {
                (None, None) => break,
                (Some(a), Some(b)) if a.key == b.key && a.sequence == b.sequence => (),
                _ => return Ok(false),
            }
        }
    }
    Ok(true)
}
