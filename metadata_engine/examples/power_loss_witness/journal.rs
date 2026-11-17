//! One in-flight request. Both intent and successful reply are synchronized.
use super::*;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::Path,
};

#[derive(Clone, Deserialize, Serialize)]
pub struct Record {
    pub sequence: usize,
    pub unix_millis: u128,
    pub kind: String,
    pub object: u64,
    pub details: Value,
}

pub struct Journal {
    file: File,
    pub records: Vec<Record>,
}

fn lock(file: &File) -> Result<()> {
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

impl Journal {
    pub fn create(path: &Path, binding: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        lock(&file)?;
        File::open(path.parent().ok_or("journal parent missing")?)?.sync_all()?;
        let mut journal = Self {
            file,
            records: Vec::new(),
        };
        journal.record("header", 0, json!({"format":1,"config_sha256":binding}))?;
        Ok(journal)
    }

    pub fn open(path: &Path, binding: &str) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        lock(&file)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() > 64 << 20 {
            return Err("journal must be a bounded regular file".into());
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let records = parse(&bytes, binding)?;
        Ok(Self { file, records })
    }

    pub fn record(&mut self, kind: &str, object: u64, details: Value) -> Result<()> {
        let record = Record {
            sequence: self.records.len(),
            unix_millis: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
            kind: kind.into(),
            object,
            details,
        };
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');
        self.file.write_all(&line)?;
        self.file.sync_all()?;
        self.records.push(record);
        if kind.ends_with("_ack") {
            println!(
                "{}",
                json!({"journal_synced":true,"kind":kind,"object":object,
                "sequence":self.records.len()-1})
            );
            std::io::stdout().flush()?;
        }
        Ok(())
    }
}

fn parse(bytes: &[u8], binding: &str) -> Result<Vec<Record>> {
    if bytes.is_empty() || !bytes.ends_with(b"\n") {
        return Err("empty or incomplete journal; preserve it without truncation".into());
    }
    let mut records = Vec::new();
    for line in bytes[..bytes.len() - 1].split(|byte| *byte == b'\n') {
        let record: Record = serde_json::from_slice(line)?;
        if record.sequence != records.len() {
            return Err("journal sequence mismatch".into());
        }
        records.push(record);
    }
    let header = &records[0];
    if header.kind != "header" || header.details != json!({"format":1,"config_sha256":binding}) {
        return Err("journal format or configuration mismatch".into());
    }
    Ok(records)
}

pub fn save_new(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    File::open(path.parent().ok_or("report parent missing")?)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_partial_reordered_and_foreign_journals_without_repairing_them() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("acks.jsonl");
        let mut journal = Journal::create(&path, "a").unwrap();
        journal.record("begin_intent", 0, json!({})).unwrap();
        drop(journal);
        let bytes = fs::read(&path).unwrap();
        assert!(parse(&bytes, "a").is_ok());
        assert!(parse(&bytes, "b").is_err());
        assert!(parse(&bytes[..bytes.len() - 1], "a").is_err());
        let reordered = String::from_utf8(bytes.clone())
            .unwrap()
            .replace("\"sequence\":1", "\"sequence\":2");
        assert!(parse(reordered.as_bytes(), "a").is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
    #[test]
    fn refuses_a_second_recorder_and_preserves_every_complete_ack() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("acks.jsonl");
        let mut journal = Journal::create(&path, "a").unwrap();
        assert!(Journal::open(&path, "a").is_err());
        journal.record("part_ack", 1, json!({"part":2})).unwrap();
        drop(journal);
        let reopened = Journal::open(&path, "a").unwrap();
        assert_eq!(reopened.records[1].kind, "part_ack");
        assert!(Journal::create(&path, "a").is_err());
    }
}
