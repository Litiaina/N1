use super::*;

pub(crate) fn load(generation: &Generation, offset: u64) -> Result<Arc<Node>> {
    if let Some(node) = generation.cache.get(generation.id, offset) {
        return Ok(node);
    }
    let result = read_uncached(generation, offset).map(Arc::new);
    if let Ok(node) = &result {
        generation.cache.insert(generation.id, offset, node.clone());
    } else {
        generation.poisoned.store(true, Ordering::Release);
    }
    result
}

pub(super) fn read_uncached(generation: &Generation, offset: u64) -> Result<Node> {
    let result = (|| {
        let record = generation.record(offset)?;
        if record.kind != format::NODE {
            return Err(Error::Corrupt("M1 tree points to a non-node record".into()));
        }
        let mut input = Input::new(&record.bytes);
        let kind = input.u8()?;
        let count = input.u32()? as usize;
        if count == 0 || count > MAX_ITEMS {
            return Err(Error::Corrupt("invalid M1 node occupancy".into()));
        }
        let node = match kind {
            0 => {
                let mut entries: Vec<Entry> = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = input.key()?.to_vec();
                    let entry = Entry {
                        key,
                        sequence: input.u64()?,
                        offset: input.u64()?,
                        length: input.u32()?,
                    };
                    if entry.sequence == 0
                        || !(format::DATA_START..offset).contains(&entry.offset)
                        || entry.length as usize > MAX_VALUE_BYTES
                        || entries.last().is_some_and(|e| e.key >= entry.key)
                    {
                        return Err(Error::Corrupt(
                            "invalid M1 leaf order or value reference".into(),
                        ));
                    }
                    entries.push(entry);
                }
                Node::Leaf(entries)
            }
            1 => {
                let mut children: Vec<Child> = Vec::with_capacity(count);
                for _ in 0..count {
                    let child = Child {
                        key: input.key()?.to_vec(),
                        offset: input.u64()?,
                    };
                    if !(format::DATA_START..offset).contains(&child.offset)
                        || children.last().is_some_and(|c| c.key >= child.key)
                    {
                        return Err(Error::Corrupt(
                            "invalid M1 branch order or child reference".into(),
                        ));
                    }
                    children.push(child);
                }
                Node::Branch(children)
            }
            _ => return Err(Error::Corrupt("invalid M1 node type".into())),
        };
        input.finish()?;
        Ok(node)
    })();
    if result.is_err() {
        generation.poisoned.store(true, Ordering::Release);
    }
    result
}

pub(super) fn write_node(writer: &mut Appender, node: Node) -> Result<Child> {
    let mut bytes = Vec::new();
    let key = match &node {
        Node::Leaf(entries) => {
            bytes.push(0);
            bytes.extend_from_slice(&(entries.len() as u32).to_be_bytes());
            for entry in entries {
                format::encode_key(&mut bytes, &entry.key);
                bytes.extend_from_slice(&entry.sequence.to_be_bytes());
                bytes.extend_from_slice(&entry.offset.to_be_bytes());
                bytes.extend_from_slice(&entry.length.to_be_bytes());
            }
            entries[0].key.clone()
        }
        Node::Branch(children) => {
            bytes.push(1);
            bytes.extend_from_slice(&(children.len() as u32).to_be_bytes());
            for child in children {
                format::encode_key(&mut bytes, &child.key);
                bytes.extend_from_slice(&child.offset.to_be_bytes());
            }
            children[0].key.clone()
        }
    };
    let offset = writer.append(format::NODE, &bytes)?;
    writer
        .generation
        .cache
        .insert(writer.generation.id, offset, Arc::new(node));
    Ok(Child { key, offset })
}
