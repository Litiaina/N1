use super::*;

/// Streaming checkpoint builder: memory grows with tree height, not row count.
pub(crate) struct BulkBuilder {
    leaf: Vec<Entry>,
    leaf_bytes: usize,
    levels: Vec<Vec<Child>>,
}

impl BulkBuilder {
    pub fn new() -> Self {
        Self {
            leaf: Vec::new(),
            leaf_bytes: 5,
            levels: Vec::new(),
        }
    }

    pub fn push(&mut self, writer: &mut Appender, entry: Entry) -> Result<()> {
        let size = entry.key.len() + 22;
        if !self.leaf.is_empty()
            && (self.leaf_bytes + size > LEAF_TARGET_BYTES || self.leaf.len() == MAX_ITEMS)
        {
            self.flush_leaf(writer)?;
        }
        self.leaf_bytes += size;
        self.leaf.push(entry);
        Ok(())
    }

    fn flush_leaf(&mut self, writer: &mut Appender) -> Result<()> {
        if self.leaf.is_empty() {
            return Ok(());
        }
        let child = write_node(writer, Node::Leaf(std::mem::take(&mut self.leaf)))?;
        self.leaf_bytes = 5;
        self.push_child(writer, 0, child)
    }

    fn push_child(&mut self, writer: &mut Appender, level: usize, child: Child) -> Result<()> {
        if level >= MAX_DEPTH {
            return Err(Error::Invalid("M1 checkpoint exceeds maximum depth".into()));
        }
        if self.levels.len() <= level {
            self.levels.push(Vec::new());
        }
        let bytes: usize = self.levels[level].iter().map(|c| c.key.len() + 10).sum();
        if !self.levels[level].is_empty()
            && (bytes + child.key.len() + 10 > TARGET_BYTES
                || self.levels[level].len() == MAX_ITEMS)
        {
            let node = std::mem::take(&mut self.levels[level]);
            let parent = write_node(writer, Node::Branch(node))?;
            self.push_child(writer, level + 1, parent)?;
        }
        self.levels[level].push(child);
        Ok(())
    }

    pub fn finish(mut self, writer: &mut Appender) -> Result<u64> {
        self.flush_leaf(writer)?;
        loop {
            let Some(lowest) = self.levels.iter().position(|l| !l.is_empty()) else {
                return Ok(0);
            };
            let highest = self.levels.iter().rposition(|l| !l.is_empty()).unwrap();
            if lowest == highest && self.levels[lowest].len() == 1 {
                return Ok(self.levels[lowest][0].offset);
            }
            let node = std::mem::take(&mut self.levels[lowest]);
            let child = write_node(writer, Node::Branch(node))?;
            self.push_child(writer, lowest + 1, child)?;
        }
    }
}
