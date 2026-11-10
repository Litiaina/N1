//! Explicit full verification bypasses cached nodes and checks parent fences.
use super::*;

#[derive(Default, Debug)]
pub struct Integrity {
    pub records: u64,
    pub logical_bytes: u64,
    pub nodes: u64,
}

pub(crate) fn verify(generation: &Generation, root: u64, sequence: u64) -> Result<Integrity> {
    let mut counts = Integrity::default();
    if root != 0 {
        visit(generation, root, sequence, 0, &mut counts)?;
    }
    Ok(counts)
}

fn visit(
    generation: &Generation,
    offset: u64,
    sequence: u64,
    depth: usize,
    counts: &mut Integrity,
) -> Result<(Vec<u8>, Vec<u8>)> {
    if depth >= MAX_DEPTH {
        return Err(Error::Corrupt(
            "M1 integrity traversal exceeded tree depth".into(),
        ));
    }
    counts.nodes += 1;
    match node::read_uncached(generation, offset)? {
        Node::Leaf(entries) => {
            for entry in &entries {
                if entry.sequence > sequence {
                    return Err(Error::Corrupt(
                        "M1 leaf contains a future transaction".into(),
                    ));
                }
                let value = generation.value(entry)?;
                counts.records += 1;
                counts.logical_bytes += (entry.key.len() + value.len()) as u64;
            }
            Ok((
                entries.first().unwrap().key.clone(),
                entries.last().unwrap().key.clone(),
            ))
        }
        Node::Branch(children) => {
            let mut first = None;
            let mut last = None;
            for child in children {
                let (minimum, maximum) =
                    visit(generation, child.offset, sequence, depth + 1, counts)?;
                if minimum != child.key || last.as_ref().is_some_and(|last| *last >= minimum) {
                    return Err(Error::Corrupt(
                        "M1 child range differs from its parent fence".into(),
                    ));
                }
                first.get_or_insert(minimum);
                last = Some(maximum);
            }
            Ok((first.unwrap(), last.unwrap()))
        }
    }
}
