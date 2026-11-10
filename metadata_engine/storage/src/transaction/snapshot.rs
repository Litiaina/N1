use super::*;

#[derive(Clone)]
pub struct Snapshot {
    // Release generation files before the last process-lease owner.
    pub(crate) state: Arc<State>,
    pub(crate) owner: Arc<Owner>,
}

impl Snapshot {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_key(key)?;
        tree::get(&self.state.generation, self.state.root, key)?
            .map(|entry| self.state.generation.value(&entry))
            .transpose()
    }

    pub fn range(&self, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Result<Scan> {
        validate_bounds(&start, &end)?;
        Ok(Scan {
            snapshot: self.clone(),
            cursor: tree::Cursor::new(self.state.generation.clone(), self.state.root, start, end)?,
            failed: false,
        })
    }

    pub fn sequence(&self) -> u64 {
        self.state.sequence
    }
}

/// Streaming ordered records. The snapshot/file lease survives the database
/// handle, and only a tree path plus the current value is held by this iterator.
pub struct Scan {
    cursor: tree::Cursor,
    failed: bool,
    snapshot: Snapshot,
}

impl Iterator for Scan {
    type Item = Result<Row>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let result = (|| {
            let Some(entry) = self.cursor.next_entry()? else {
                return Ok(None);
            };
            let value = self.snapshot.state.generation.value(&entry)?;
            Ok(Some((entry.key, value)))
        })();
        match result {
            Ok(row) => row.map(Ok),
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}
