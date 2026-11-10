//! Snapshot reads and optimistic point/range validation.
use crate::{
    Error, MAX_KEY_BYTES, MAX_TRANSACTION_BYTES, MAX_TRANSACTION_WRITES, MAX_VALUE_BYTES, Owner,
    Result, State, tree, worker,
};
use std::{
    collections::{BTreeMap, btree_map},
    iter::Peekable,
    ops::Bound,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

pub type Row = (Vec<u8>, Vec<u8>);
pub(crate) type Bounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

#[derive(Clone)]
pub(crate) struct Mutation {
    pub sequence: u64,
    pub value: Option<Arc<[u8]>>,
}

impl Mutation {
    fn entry(&self, key: &[u8]) -> Option<tree::Entry> {
        self.value.as_ref().map(|value| tree::Entry {
            key: key.to_vec(),
            sequence: self.sequence,
            offset: 0,
            length: value.len() as u32,
        })
    }
}

#[derive(Default)]
struct Reads {
    points: BTreeMap<Vec<u8>, Option<u64>>,
    ranges: Vec<Bounds>,
    bytes: usize,
}

pub struct Transaction {
    snapshot: Snapshot,
    reads: Mutex<Reads>,
    writes: BTreeMap<Vec<u8>, Mutation>,
    write_count: usize,
    bytes: usize,
    failed: AtomicBool,
}

pub(crate) struct TransactionData {
    pub base: Arc<State>,
    pub points: BTreeMap<Vec<u8>, Option<u64>>,
    pub ranges: Vec<Bounds>,
    pub writes: BTreeMap<Vec<u8>, Mutation>,
}

impl Transaction {
    pub(crate) fn new(snapshot: Snapshot) -> Self {
        Self {
            snapshot,
            reads: Mutex::new(Reads::default()),
            writes: BTreeMap::new(),
            write_count: 0,
            bytes: 0,
            failed: AtomicBool::new(false),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_key(key)?;
        if let Some(write) = self.writes.get(key) {
            return Ok(write.value.as_deref().map(<[u8]>::to_vec));
        }
        let entry = tree::get(
            &self.snapshot.state.generation,
            self.snapshot.state.root,
            key,
        )?;
        {
            let mut reads = self.reads.lock().unwrap_or_else(|e| e.into_inner());
            if !reads.points.contains_key(key) {
                if reads.points.len() == 65536 || reads.bytes + key.len() > MAX_TRANSACTION_BYTES {
                    self.failed.store(true, Ordering::Release);
                    return Err(Error::Invalid(
                        "M1 transaction read set exceeds its bound".into(),
                    ));
                }
                reads.bytes += key.len();
                reads
                    .points
                    .insert(key.to_vec(), entry.as_ref().map(|e| e.sequence));
            }
        }
        entry
            .map(|entry| self.snapshot.state.generation.value(&entry))
            .transpose()
    }

    pub fn scan(
        &self,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
        byte_limit: usize,
    ) -> Result<Vec<Row>> {
        validate_bounds(&start, &end)?;
        if limit == 0 || limit > 1000 || byte_limit < MAX_VALUE_BYTES {
            return Err(Error::Invalid("invalid M1 transaction scan bound".into()));
        }
        let state = &self.snapshot.state;
        let mut cursor = Merged::new(state, &self.writes, start.clone(), end.clone())?;
        let mut rows = Vec::new();
        let mut bytes = 0;
        let mut last = None;
        let exhausted;
        loop {
            if rows.len() == limit {
                exhausted = false;
                break;
            }
            let Some(entry) = cursor.next_entry()? else {
                exhausted = true;
                break;
            };
            last = Some(entry.key.clone());
            let size = entry.key.len() + entry.length as usize;
            if bytes + size > byte_limit {
                exhausted = false;
                break;
            }
            let value = match self.writes.get(&entry.key) {
                Some(write) => write.value.as_ref().expect("merged live write").to_vec(),
                None => state.generation.value(&entry)?,
            };
            bytes += size;
            rows.push((entry.key, value));
        }
        let observed_end = if exhausted {
            end
        } else {
            last.map_or(end, Bound::Included)
        };
        let mut reads = self.reads.lock().unwrap_or_else(|e| e.into_inner());
        let bound_bytes = bound_size(&start) + bound_size(&observed_end);
        if reads.ranges.len() == MAX_TRANSACTION_WRITES
            || reads.bytes + bound_bytes > MAX_TRANSACTION_BYTES
        {
            self.failed.store(true, Ordering::Release);
            return Err(Error::Invalid(
                "M1 transaction range set exceeds its bound".into(),
            ));
        }
        reads.bytes += bound_bytes;
        reads.ranges.push((start, observed_end));
        Ok(rows)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.reserve(key, value.len())?;
        self.writes.insert(
            key.to_vec(),
            Mutation {
                sequence: 0,
                value: Some(Arc::from(value)),
            },
        );
        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.reserve(key, 0)?;
        self.writes.insert(
            key.to_vec(),
            Mutation {
                sequence: 0,
                value: None,
            },
        );
        Ok(())
    }

    fn reserve(&mut self, key: &[u8], length: usize) -> Result<()> {
        if validate_key(key).is_err()
            || length > MAX_VALUE_BYTES
            || self.write_count == MAX_TRANSACTION_WRITES
            || self.bytes + key.len() + length > MAX_TRANSACTION_BYTES
        {
            self.failed.store(true, Ordering::Release);
            return Err(Error::Invalid(
                "M1 transaction exceeds its key/value/write bound".into(),
            ));
        }
        self.bytes += key.len() + length;
        self.write_count += 1;
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::Invalid(
                "M1 transaction must be discarded after exceeding its bounds".into(),
            ));
        }
        if self.writes.is_empty() {
            return Ok(());
        }
        let owner = self.snapshot.owner;
        owner.core.healthy()?;
        let reads = self.reads.into_inner().unwrap_or_else(|e| e.into_inner());
        let request = TransactionData {
            base: self.snapshot.state,
            points: reads.points,
            ranges: reads.ranges,
            writes: self.writes,
        };
        let (sender, receiver) = mpsc::sync_channel(1);
        owner
            .sender
            .send(worker::Command::Commit(worker::Request {
                data: request,
                reply: sender,
            }))
            .map_err(|_| Error::Closed)?;
        receiver.recv().map_err(|_| Error::Closed)?
    }
}

fn bound_size(bound: &Bound<Vec<u8>>) -> usize {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => key.len(),
        Bound::Unbounded => 0,
    }
}

fn validate_key(key: &[u8]) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES || key[0] > 3 {
        Err(Error::Invalid("invalid M1 table/key encoding".into()))
    } else {
        Ok(())
    }
}

fn validate_bounds(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> Result<()> {
    match (start, end) {
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b))
            if a > b
                || (a == b
                    && (!matches!(start, Bound::Included(_))
                        || !matches!(end, Bound::Included(_)))) =>
        {
            Err(Error::Invalid("invalid M1 scan range".into()))
        }
        _ => Ok(()),
    }
}

mod scan;
mod snapshot;
mod validation;
use scan::Merged;
pub use snapshot::{Scan, Snapshot};
pub(crate) use validation::validate;
