use super::*;

fn leaves(writer: &mut Appender, entries: Vec<Entry>) -> Result<Vec<Child>> {
    let mut result = Vec::new();
    let mut node = Vec::new();
    let mut bytes = 5;
    for entry in entries {
        let size = entry.key.len() + 22;
        if !node.is_empty() && (bytes + size > LEAF_TARGET_BYTES || node.len() == MAX_ITEMS) {
            result.push(write_node(writer, Node::Leaf(std::mem::take(&mut node)))?);
            bytes = 5;
        }
        bytes += size;
        node.push(entry);
    }
    if !node.is_empty() {
        result.push(write_node(writer, Node::Leaf(node))?);
    }
    Ok(result)
}

fn branches(writer: &mut Appender, children: Vec<Child>) -> Result<Vec<Child>> {
    let mut result = Vec::new();
    let mut node = Vec::new();
    let mut bytes = 5;
    for child in children {
        let size = child.key.len() + 10;
        if !node.is_empty() && (bytes + size > TARGET_BYTES || node.len() == MAX_ITEMS) {
            result.push(write_node(writer, Node::Branch(std::mem::take(&mut node)))?);
            bytes = 5;
        }
        bytes += size;
        node.push(child);
    }
    if !node.is_empty() {
        if node.len() == 1 {
            result.push(node.remove(0));
        } else {
            result.push(write_node(writer, Node::Branch(node))?);
        }
    }
    Ok(result)
}

pub(crate) type Change = (Vec<u8>, Option<Entry>);

fn rewrite(
    writer: &mut Appender,
    offset: u64,
    changes: &[Change],
    depth: usize,
) -> Result<Vec<Child>> {
    if depth >= MAX_DEPTH {
        return Err(Error::Corrupt("M1 tree exceeds maximum depth".into()));
    }
    let node = if offset == 0 {
        Arc::new(Node::Leaf(Vec::new()))
    } else {
        load(&writer.generation, offset)?
    };
    let replacement = match &*node {
        Node::Leaf(entries) => {
            let mut merged = Vec::with_capacity(entries.len() + changes.len());
            let mut old = entries.iter().peekable();
            for (key, replacement) in changes {
                while old.peek().is_some_and(|entry| entry.key < *key) {
                    merged.push(old.next().unwrap().clone());
                }
                if old.peek().is_some_and(|entry| entry.key == *key) {
                    old.next();
                }
                if let Some(entry) = replacement {
                    merged.push(entry.clone());
                }
            }
            merged.extend(old.cloned());
            leaves(writer, merged)
        }
        Node::Branch(children) => {
            let mut result = Vec::new();
            let mut begin = 0;
            for (index, child) in children.iter().enumerate() {
                let end = if let Some(next) = children.get(index + 1) {
                    begin + changes[begin..].partition_point(|(key, _)| *key < next.key)
                } else {
                    changes.len()
                };
                if begin == end {
                    result.push(child.clone());
                } else {
                    result.extend(rewrite(
                        writer,
                        child.offset,
                        &changes[begin..end],
                        depth + 1,
                    )?);
                }
                begin = end;
            }
            branches(writer, result)
        }
    }?;
    if offset != 0 && writer.evict_replaced {
        // This path has been replaced. Old snapshots still own valid on-disk
        // records and any in-flight Arc<Node>; retaining every replaced page
        // in the shared cache needlessly fills it with obsolete versions.
        writer.generation.cache.remove(writer.generation.id, offset);
    }
    Ok(replacement)
}

pub(crate) fn apply(writer: &mut Appender, root: u64, changes: &[Change]) -> Result<u64> {
    if changes.is_empty() {
        return Ok(root);
    }
    let mut children = rewrite(writer, root, changes, 0)?;
    while children.len() > 1 {
        children = branches(writer, children)?;
    }
    Ok(children.first().map_or(0, |c| c.offset))
}
