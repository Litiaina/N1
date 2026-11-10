use super::*;

pub(crate) fn get(generation: &Generation, root: u64, key: &[u8]) -> Result<Option<Entry>> {
    let mut offset = root;
    for _ in 0..MAX_DEPTH {
        if offset == 0 {
            return Ok(None);
        }
        match &*load(generation, offset)? {
            Node::Leaf(entries) => {
                return Ok(entries
                    .binary_search_by(|e| e.key.as_slice().cmp(key))
                    .ok()
                    .map(|i| entries[i].clone()));
            }
            Node::Branch(children) => {
                let index = children
                    .partition_point(|c| c.key.as_slice() <= key)
                    .saturating_sub(1);
                offset = children[index].offset;
            }
        }
    }
    Err(Error::Corrupt("M1 tree exceeds its maximum depth".into()))
}

pub(crate) struct Cursor {
    generation: Arc<Generation>,
    stack: Vec<(Arc<Node>, usize)>,
    end: Bound<Vec<u8>>,
    done: bool,
}

impl Cursor {
    pub fn new(
        generation: Arc<Generation>,
        root: u64,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<Self> {
        let mut cursor = Self {
            generation,
            stack: Vec::new(),
            end,
            done: root == 0,
        };
        if root != 0 {
            cursor.descend(root, Some(&start))?;
        }
        Ok(cursor)
    }

    fn descend(&mut self, mut offset: u64, start: Option<&Bound<Vec<u8>>>) -> Result<()> {
        loop {
            if self.stack.len() >= MAX_DEPTH {
                return Err(Error::Corrupt("M1 tree exceeds its maximum depth".into()));
            }
            let node = load(&self.generation, offset)?;
            match &*node {
                Node::Leaf(entries) => {
                    let index = match start {
                        Some(Bound::Included(key)) => entries.partition_point(|e| e.key < *key),
                        Some(Bound::Excluded(key)) => entries.partition_point(|e| e.key <= *key),
                        _ => 0,
                    };
                    self.stack.push((node, index));
                    return Ok(());
                }
                Node::Branch(children) => {
                    let index = match start {
                        Some(Bound::Included(key) | Bound::Excluded(key)) => children
                            .partition_point(|c| c.key <= *key)
                            .saturating_sub(1),
                        _ => 0,
                    };
                    offset = children[index].offset;
                    self.stack.push((node, index + 1));
                }
            }
        }
    }

    pub fn next_entry(&mut self) -> Result<Option<Entry>> {
        while !self.done {
            let Some((node, index)) = self.stack.last_mut() else {
                self.done = true;
                break;
            };
            match &**node {
                Node::Leaf(entries) if *index < entries.len() => {
                    let entry = entries[*index].clone();
                    *index += 1;
                    if match &self.end {
                        Bound::Included(end) => entry.key > *end,
                        Bound::Excluded(end) => entry.key >= *end,
                        Bound::Unbounded => false,
                    } {
                        self.done = true;
                        break;
                    }
                    return Ok(Some(entry));
                }
                Node::Branch(children) if *index < children.len() => {
                    let next = children[*index].offset;
                    *index += 1;
                    self.descend(next, None)?;
                }
                _ => {
                    self.stack.pop();
                }
            }
        }
        Ok(None)
    }
}
