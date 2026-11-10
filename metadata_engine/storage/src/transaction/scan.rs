use super::*;

pub(crate) struct Merged<'a> {
    cursor: tree::Cursor,
    base: Option<tree::Entry>,
    writes: Peekable<btree_map::Range<'a, Vec<u8>, Mutation>>,
}

impl<'a> Merged<'a> {
    pub fn new(
        state: &State,
        writes: &'a BTreeMap<Vec<u8>, Mutation>,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<Self> {
        let mut cursor = tree::Cursor::new(
            state.generation.clone(),
            state.root,
            start.clone(),
            end.clone(),
        )?;
        let base = cursor.next_entry()?;
        Ok(Self {
            cursor,
            base,
            writes: writes.range((start, end)).peekable(),
        })
    }

    pub fn next_entry(&mut self) -> Result<Option<tree::Entry>> {
        loop {
            let order = match (&self.base, self.writes.peek()) {
                (None, None) => return Ok(None),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(base), Some((key, _))) => base.key.cmp(key),
            };
            if order == std::cmp::Ordering::Less {
                let entry = self.base.take();
                self.base = self.cursor.next_entry()?;
                return Ok(entry);
            }
            if order == std::cmp::Ordering::Equal {
                self.base = self.cursor.next_entry()?;
            }
            let (key, write) = self.writes.next().unwrap();
            if let Some(entry) = write.entry(key) {
                return Ok(Some(entry));
            }
        }
    }
}
