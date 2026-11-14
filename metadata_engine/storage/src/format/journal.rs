use super::*;

pub(crate) struct Record {
    pub kind: u8,
    pub bytes: Vec<u8>,
    pub end: u64,
}

pub(super) fn read_record(file: &File, offset: u64, file_end: u64) -> Result<Option<Record>> {
    if offset < DATA_START {
        return Err(Error::Corrupt("M1 record points inside its header".into()));
    }
    if file_end.saturating_sub(offset) < RECORD_HEADER as u64 {
        return Ok(None);
    }
    let mut header = [0; RECORD_HEADER];
    file.read_exact_at(&mut header, offset)?;
    // The payload checksum alone cannot distinguish a damaged length from
    // an incomplete final write. Reject header corruption before trusting the
    // advertised length or permitting recovery to truncate an uncommitted tail.
    if checksum(&header[..24]) != u32::from_be_bytes(header[24..28].try_into().unwrap()) {
        return Err(Error::Corrupt("M1 record header checksum mismatch".into()));
    }
    let length = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
    if &header[..4] != b"M1R1"
        || !matches!(header[4], VALUE | NODE | COMMIT)
        || header[5..8] != [0; 3]
        || length > MAX_RECORD_BYTES
        || u64::from_be_bytes(header[16..24].try_into().unwrap()) != offset
    {
        return Err(Error::Corrupt("invalid M1 record header".into()));
    }
    let end = offset
        .checked_add(RECORD_HEADER as u64 + length as u64)
        .ok_or_else(|| Error::Corrupt("M1 record offset overflow".into()))?;
    if end > file_end {
        return Ok(None);
    }
    let mut bytes = vec![0; length];
    file.read_exact_at(&mut bytes, offset + RECORD_HEADER as u64)?;
    let crc = crc32c::crc32c_append(checksum(&header[..12]), &header[16..24]);
    let crc = crc32c::crc32c_append(crc, &bytes);
    if crc != u32::from_be_bytes(header[12..16].try_into().unwrap()) {
        return Err(Error::Corrupt(format!(
            "M1 record checksum mismatch at {offset}"
        )));
    }
    Ok(Some(Record {
        kind: header[4],
        bytes,
        end,
    }))
}

pub(crate) struct Appender {
    pub generation: Arc<Generation>,
    pub end: u64,
    pub evict_replaced: bool,
    buffer: Vec<u8>,
}

impl Appender {
    pub fn new(generation: Arc<Generation>, end: u64, evict_replaced: bool) -> Self {
        Self {
            generation,
            end,
            evict_replaced,
            buffer: Vec::with_capacity(64 * 1024),
        }
    }

    fn flush(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.generation
                .file
                .write_all_at(&self.buffer, self.end - self.buffer.len() as u64)?;
            self.buffer.clear();
        }
        Ok(())
    }

    pub fn append(&mut self, kind: u8, bytes: &[u8]) -> Result<u64> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(Error::Invalid("M1 record exceeds format bound".into()));
        }
        let offset = self.end;
        let mut header = [0u8; RECORD_HEADER];
        header[..4].copy_from_slice(b"M1R1");
        header[4] = kind;
        header[8..12].copy_from_slice(&(bytes.len() as u32).to_be_bytes());
        header[16..24].copy_from_slice(&offset.to_be_bytes());
        let crc = crc32c::crc32c_append(checksum(&header[..12]), &header[16..24]);
        let crc = crc32c::crc32c_append(crc, bytes);
        header[12..16].copy_from_slice(&crc.to_be_bytes());
        let header_crc = checksum(&header[..24]);
        header[24..28].copy_from_slice(&header_crc.to_be_bytes());
        let length = RECORD_HEADER + bytes.len();
        let end = offset
            .checked_add(RECORD_HEADER as u64 + bytes.len() as u64)
            .ok_or_else(|| Error::Invalid("M1 file offset overflow".into()))?;
        // Coalesce consecutive records within one commit, not across durable
        // groups. Tree edits only read the preceding committed root; commit()
        // drains this bounded buffer before any new root can be used or synced.
        // Oversized values retain the existing exact positioned-write path.
        if length > 64 * 1024 {
            self.flush()?;
            let mut record = Vec::with_capacity(length);
            record.extend_from_slice(&header);
            record.extend_from_slice(bytes);
            self.generation.file.write_all_at(&record, offset)?;
        } else {
            if self.buffer.len() + length > 64 * 1024 {
                self.flush()?;
            }
            self.buffer.extend_from_slice(&header);
            self.buffer.extend_from_slice(bytes);
        }
        self.end = end;
        Ok(offset)
    }

    pub fn value(&mut self, key: &[u8], sequence: u64, value: &[u8]) -> Result<tree::Entry> {
        if key.is_empty()
            || key.len() > MAX_KEY_BYTES
            || key[0] > 3
            || value.len() > MAX_VALUE_BYTES
        {
            return Err(Error::Invalid("invalid M1 value bounds".into()));
        }
        let mut bytes = Vec::with_capacity(10 + key.len() + value.len());
        bytes.extend_from_slice(&sequence.to_be_bytes());
        encode_key(&mut bytes, key);
        bytes.extend_from_slice(value);
        let offset = self.append(VALUE, &bytes)?;
        Ok(tree::Entry {
            key: key.to_vec(),
            sequence,
            offset,
            length: value.len() as u32,
        })
    }

    pub fn commit(
        &mut self,
        sequence: u64,
        root: u64,
        previous: u64,
        changes: &[(Vec<u8>, Option<tree::Entry>)],
    ) -> Result<State> {
        let mut bytes = Vec::new();
        for number in [sequence, root, previous, 0] {
            bytes.extend_from_slice(&number.to_be_bytes());
        }
        bytes.extend_from_slice(&(changes.len() as u32).to_be_bytes());
        for (key, value) in changes {
            encode_key(&mut bytes, key);
            bytes.push(u8::from(value.is_some()));
            if let Some(entry) = value {
                bytes.extend_from_slice(&entry.sequence.to_be_bytes());
                bytes.extend_from_slice(&entry.offset.to_be_bytes());
                bytes.extend_from_slice(&entry.length.to_be_bytes());
            }
        }
        let unpadded = self.end + RECORD_HEADER as u64 + bytes.len() as u64;
        let end = unpadded.next_multiple_of(DATA_START);
        bytes.resize(bytes.len() + (end - unpadded) as usize, 0);
        bytes[24..32].copy_from_slice(&end.to_be_bytes());
        let commit = self.append(COMMIT, &bytes)?;
        self.flush()?;
        Ok(State {
            generation: self.generation.clone(),
            sequence,
            root,
            end: self.end,
            commit,
            anchor_end: DATA_START,
        })
    }
}

pub(crate) struct Commit {
    pub sequence: u64,
    pub root: u64,
    pub previous: u64,
    pub changes: Vec<(Vec<u8>, Option<tree::Entry>)>,
}

pub(crate) fn decode_commit(record: &Record, offset: u64) -> Result<Commit> {
    if record.kind != COMMIT {
        return Err(Error::Corrupt("expected M1 commit record".into()));
    }
    let mut input = Input::new(&record.bytes);
    let sequence = input.u64()?;
    let root = input.u64()?;
    let previous = input.u64()?;
    let end = input.u64()?;
    let count = input.u32()? as usize;
    if end != record.end
        || (root != 0 && !(DATA_START..offset).contains(&root))
        || (previous != 0 && !(DATA_START..offset).contains(&previous))
        || count > 262144
    {
        return Err(Error::Corrupt("invalid M1 commit boundary".into()));
    }
    let mut changes = Vec::new();
    for _ in 0..count {
        let key = input.key()?.to_vec();
        let entry = match input.u8()? {
            0 => None,
            1 => {
                let entry = tree::Entry {
                    key: key.clone(),
                    sequence: input.u64()?,
                    offset: input.u64()?,
                    length: input.u32()?,
                };
                if entry.sequence == 0
                    || entry.sequence > sequence
                    || entry.offset < DATA_START
                    || entry.offset >= offset
                    || entry.length as usize > MAX_VALUE_BYTES
                {
                    return Err(Error::Corrupt(
                        "invalid M1 committed value reference".into(),
                    ));
                }
                Some(entry)
            }
            _ => return Err(Error::Corrupt("invalid M1 change tag".into())),
        };
        if changes
            .last()
            .is_some_and(|(previous, _): &(Vec<u8>, _)| previous >= &key)
        {
            return Err(Error::Corrupt("M1 changes are not strictly ordered".into()));
        }
        changes.push((key, entry));
    }
    if input.remaining().iter().any(|&byte| byte != 0) || !record.end.is_multiple_of(DATA_START) {
        return Err(Error::Corrupt(
            "invalid M1 commit alignment or padding".into(),
        ));
    }
    Ok(Commit {
        sequence,
        root,
        previous,
        changes,
    })
}

pub(crate) fn encode_key(bytes: &mut Vec<u8>, key: &[u8]) {
    bytes.extend_from_slice(&(key.len() as u16).to_be_bytes());
    bytes.extend_from_slice(key);
}

pub(crate) struct Input<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Input<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    pub fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(length)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| Error::Corrupt("truncated M1 record payload".into()))?;
        let value = &self.bytes[self.at..end];
        self.at = end;
        Ok(value)
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn key(&mut self) -> Result<&'a [u8]> {
        let length = u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize;
        if length == 0 || length > MAX_KEY_BYTES {
            return Err(Error::Corrupt("invalid M1 key length".into()));
        }
        let key = self.take(length)?;
        if key[0] > 3 {
            return Err(Error::Corrupt("invalid M1 table identifier".into()));
        }
        Ok(key)
    }
    pub fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.at..]
    }
    pub fn finish(&self) -> Result<()> {
        if self.at != self.bytes.len() {
            Err(Error::Corrupt("trailing M1 record bytes".into()))
        } else {
            Ok(())
        }
    }
}
