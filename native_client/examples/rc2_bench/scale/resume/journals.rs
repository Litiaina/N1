//! Bounded, strict prefix recovery. Only an unterminated final record can be
//! discarded; malformed committed lines, gaps and changed identities fail.
use super::*;
use std::io::Read;

pub(super) struct Records<R> {
    reader: R,
    pub torn_tail_bytes: usize,
    pub hash: Sha256,
}
impl<R: BufRead> Records<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            torn_tail_bytes: 0,
            hash: Sha256::new(),
        }
    }
    pub fn next(&mut self) -> Result<Option<Entry>> {
        let mut line = Vec::new();
        let read = self
            .reader
            .by_ref()
            .take(4097)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            return Ok(None);
        }
        if read > 4096 {
            return Err("oversized client journal record".into());
        }
        self.hash.update(&line);
        if !line.ends_with(b"\n") {
            self.torn_tail_bytes = read;
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&line)?))
    }
}
pub(super) fn index(worker: u64, ordinal: u64) -> u64 {
    ordinal / BATCH * (WORKERS * BATCH) + worker * BATCH + ordinal % BATCH
}
pub(super) fn validate(entry: &Entry, expected_index: u64, total: u64) -> Result<()> {
    if expected_index >= total || entry.index != expected_index {
        return Err("journal index is duplicated, out of order or outside the workload".into());
    }
    let source = payload(entry.index);
    if entry.bytes != source.len() as u64
        || entry.sha256 != format!("{:x}", Sha256::digest(&source))
    {
        return Err("journal length/SHA-256 differs from deterministic source".into());
    }
    if entry.key != entry.ticket {
        return Err("journal continuation changed LINK identity".into());
    }
    n1_link::ObjectKey::inspect(&entry.key)?;
    Ok(())
}

pub(super) fn unique(directory: &Path, total: u64) -> Result<Value> {
    let start = Instant::now();
    let mut identities = std::collections::HashSet::with_capacity(total as usize);
    for worker in 0..WORKERS {
        let mut rows = Records::new(BufReader::new(File::open(
            directory.join(format!("acknowledged-{worker}.jsonl")),
        )?));
        let mut ordinal = 0;
        while let Some(entry) = rows.next()? {
            if entry.index != index(worker, ordinal) || entry.index >= total {
                return Err("identity census found an unexpected journal index".into());
            }
            if !identities.insert(n1_link::ObjectKey::inspect(&entry.key)?.fingerprint().0) {
                return Err(format!(
                    "duplicate immutable identity at client index {}",
                    entry.index
                )
                .into());
            }
            ordinal += 1;
        }
        if rows.torn_tail_bytes != 0 {
            return Err("completed journal has a torn tail".into());
        }
    }
    if identities.len() as u64 != total {
        return Err("distinct object identity count mismatch".into());
    }
    Ok(json!({
        "distinct_identities": total,
        "seconds": start.elapsed().as_secs_f64(),
        "scope": "client-only fingerprint census; engine has no per-object index"
    }))
}

#[derive(Clone, Serialize)]
pub(super) struct Prefix {
    pub records: u64,
    pub source_bytes: u64,
    pub torn_tail_bytes: usize,
    pub original_sha256: String,
}
fn copy_prefix(input: &Path, output: &Path, worker: u64, total: u64) -> Result<Prefix> {
    let mut rows = Records::new(BufReader::new(File::open(input)?));
    let mut journal = Journal::new(output)?;
    let mut records = 0;
    let mut source_bytes = 0;
    while let Some(entry) = rows.next()? {
        validate(&entry, index(worker, records), total)?;
        serde_json::to_writer(&mut journal.0, &entry)?;
        journal.0.write_all(b"\n")?;
        records += 1;
        source_bytes += entry.bytes;
    }
    journal.0.sync_all()?;
    Ok(Prefix {
        records,
        source_bytes,
        torn_tail_bytes: rows.torn_tail_bytes,
        original_sha256: format!("{:x}", rows.hash.finalize()),
    })
}

#[derive(Clone, Serialize)]
pub(super) struct Snapshot {
    pub intents: Prefix,
    pub acknowledged: Prefix,
}
pub(super) fn snapshot(
    source: &Path,
    destination: &Path,
    worker: u64,
    total: u64,
) -> Result<Snapshot> {
    let intents_path = destination.join(format!("intents-{worker}.jsonl"));
    let ack_path = destination.join(format!("acknowledged-{worker}.jsonl"));
    let intents = copy_prefix(
        &source.join(format!("intents-{worker}.jsonl")),
        &intents_path,
        worker,
        total,
    )?;
    let acknowledged = copy_prefix(
        &source.join(format!("acknowledged-{worker}.jsonl")),
        &ack_path,
        worker,
        total,
    )?;
    let mut a = Records::new(BufReader::new(File::open(intents_path)?));
    let mut b = Records::new(BufReader::new(File::open(ack_path)?));
    while let Some(ack) = b.next()? {
        if a.next()?.as_ref() != Some(&ack) {
            return Err("acknowledgement differs from durable intent".into());
        }
    }
    Ok(Snapshot {
        intents,
        acknowledged,
    })
}

fn append_journal(path: &Path) -> Result<Journal> {
    let file = OpenOptions::new()
        .append(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err("journal is not a regular file".into());
    }
    Ok(Journal(file))
}

pub(super) async fn upload(
    login: Login,
    directory: PathBuf,
    worker: u64,
    total: u64,
    snapshot: Snapshot,
    progress: Arc<Counters>,
) -> Result<Vec<f64>> {
    let mut client = login.connect().await?;
    let intent_path = directory.join(format!("intents-{worker}.jsonl"));
    let ack_path = directory.join(format!("acknowledged-{worker}.jsonl"));
    let mut old_intents = Records::new(BufReader::new(File::open(&intent_path)?));
    let mut old_acks = Records::new(BufReader::new(File::open(&ack_path)?));
    let mut intents = append_journal(&intent_path)?;
    let mut acknowledged = append_journal(&ack_path)?;
    let mut ordinal = 0;
    let mut times = Vec::new();
    for first in (worker * BATCH..total).step_by((WORKERS * BATCH) as usize) {
        let mut pending = Vec::new();
        let mut fresh = Vec::new();
        let mut body = Vec::new();
        for i in first..total.min(first + BATCH) {
            let old = if ordinal < snapshot.intents.records {
                Some(old_intents.next()?.ok_or("short copied intent prefix")?)
            } else {
                None
            };
            if ordinal < snapshot.acknowledged.records {
                let ack = old_acks
                    .next()?
                    .ok_or("short copied acknowledgement prefix")?;
                if old.as_ref() != Some(&ack) {
                    return Err("copied acknowledged identity changed".into());
                }
                // Crucially, no PUT or finalize touches any previously acknowledged key.
                ordinal += 1;
                continue;
            }
            let source = payload(i);
            let entry = if let Some(entry) = old {
                progress.retried.fetch_add(1, Ordering::Relaxed);
                entry
            } else {
                let initialized = value(
                    client
                        .control(Command::UploadInit {
                            pool: None,
                            total_bytes: source.len() as u64,
                            part_bytes: 65536,
                            durable: true,
                            ttl_seconds: 604800,
                        })
                        .await?,
                )?;
                let entry = Entry {
                    index: i,
                    key: field(&initialized, "object_key")?,
                    ticket: field(&initialized, "object_key")?,
                    bytes: source.len() as u64,
                    sha256: format!("{:x}", Sha256::digest(&source)),
                };
                fresh.push(entry.clone());
                entry
            };
            validate(&entry, i, total)?;
            body.extend_from_slice(&source);
            pending.push(entry);
            ordinal += 1;
        }
        if pending.is_empty() {
            continue;
        }
        if !fresh.is_empty() {
            let (next, _) = intents.persist(fresh).await?;
            intents = next;
        }
        let timer = Instant::now();
        let tickets = pending.iter().map(|entry| entry.ticket.clone()).collect();
        let reply = value(
            client
                .put_batch(tickets, body.len() as u64, &mut std::io::Cursor::new(&body))
                .await?,
        )?;
        let objects = reply["objects"]
            .as_array()
            .ok_or("batch response has no objects")?;
        if objects.len() != pending.len()
            || objects.iter().zip(&pending).any(|(object, entry)| {
                object["object_key"] != entry.key || object["total_bytes"] != entry.bytes
            })
        {
            return Err("resumed PUT returned different immutable identities".into());
        }
        progress.already_finalized.fetch_add(
            objects
                .iter()
                .filter(|v| v["already_finalized"] == true)
                .count() as u64,
            Ordering::Relaxed,
        );
        times.push(timer.elapsed().as_secs_f64());
        let (next, pending) = acknowledged.persist(pending).await?;
        acknowledged = next;
        progress
            .bytes
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        progress
            .done
            .fetch_add(pending.len() as u64, Ordering::Relaxed);
    }
    Ok(times)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn entry(index: u64) -> Entry {
        let source = payload(index);
        Entry {
            index,
            key: "test".into(),
            ticket: "test".into(),
            bytes: source.len() as u64,
            sha256: format!("{:x}", Sha256::digest(source)),
        }
    }
    #[test]
    fn a_torn_final_record_never_discards_an_earlier_committed_record() {
        let mut bytes = serde_json::to_vec(&entry(0)).unwrap();
        bytes.push(b'\n');
        bytes.extend_from_slice(b"{\"index\":1");
        let mut rows = Records::new(std::io::Cursor::new(bytes));
        assert_eq!(rows.next().unwrap().unwrap().index, 0);
        assert!(rows.next().unwrap().is_none());
        assert!(rows.torn_tail_bytes > 0);
    }
    #[test]
    fn malformed_committed_or_oversized_records_fail_instead_of_being_skipped() {
        for bytes in [b"{broken}\n".to_vec(), vec![b'x'; 4097]] {
            assert!(Records::new(std::io::Cursor::new(bytes)).next().is_err());
        }
    }
    #[test]
    fn worker_ranges_cover_each_index_exactly_once_including_the_partial_tail() {
        let mut indices = Vec::new();
        for worker in 0..WORKERS {
            for ordinal in 0.. {
                let i = index(worker, ordinal);
                if i >= 4099 {
                    break;
                }
                indices.push(i);
            }
        }
        indices.sort_unstable();
        assert_eq!(indices, (0..4099).collect::<Vec<_>>());
    }
}
