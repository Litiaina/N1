//! Streaming, cross-keyspace MVCC backups. Payloads live in the object engine
//! and are not included. Restores publish only a fully verified new database.
use super::*;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
};
const MAGIC: &[u8] = b"N1-METADATA-SNAPSHOT\0\x01";

impl MetadataEngine {
    pub fn backup(&self, writer: &mut impl Write) -> Result<u64> {
        let snapshot = self.snapshot();
        let mut digest = Sha256::new();
        let mut write = |bytes: &[u8]| -> Result<()> {
            writer.write_all(bytes)?;
            digest.update(bytes);
            Ok(())
        };
        write(MAGIC)?;
        let mut count = 0u64;
        for table in Table::ALL {
            let mut after = None;
            loop {
                let rows = snapshot.scan_all(table, after.as_ref(), MAX_PAGE_SIZE)?;
                if rows.is_empty() {
                    break;
                }
                for (key, value) in rows {
                    after = Some(key.clone());
                    let key = key.encode();
                    write(&[table.index() as u8])?;
                    write(&(key.len() as u32).to_be_bytes())?;
                    write(&(value.len() as u32).to_be_bytes())?;
                    write(&key)?;
                    write(&value)?;
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("snapshot record count overflow".into()))?;
                }
            }
        }
        write(&[255])?;
        write(&count.to_be_bytes())?;
        writer.write_all(&digest.finalize())?;
        Ok(count)
    }
    /// Fresh destination only. A partial restore remains in a private sibling
    /// directory; an existing database is never opened or changed.
    pub fn restore_backup(archive: &Path, destination: &Path) -> Result<u64> {
        if destination.exists() {
            return Err(Error::Invalid(
                "restore requires an absent destination".into(),
            ));
        }
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let stage = parent.join(format!(".m1-restore-{}", ulid::Ulid::new()));
        std::fs::DirBuilder::new().mode(0o700).create(&stage)?;
        // Keep failed restore evidence; it is never published as the target.
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(archive)?;
        if !file.metadata()?.is_file() {
            return Err(Error::Invalid("snapshot must be a regular file".into()));
        }
        let db = MetadataEngine::open(&stage, Options::default())?;
        let count = restore(&db, &mut file)?;
        drop(db);
        // Linux atomic no-replace publication closes the destination race.
        use std::{ffi::CString, os::unix::ffi::OsStrExt};
        let from = CString::new(stage.as_os_str().as_bytes())
            .map_err(|_| Error::Invalid("invalid restore path".into()))?;
        let to = CString::new(destination.as_os_str().as_bytes())
            .map_err(|_| Error::Invalid("invalid restore path".into()))?;
        if unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        File::open(parent)?.sync_all()?;
        Ok(count)
    }
}
use std::os::unix::fs::DirBuilderExt;
fn restore(db: &MetadataEngine, reader: &mut impl Read) -> Result<u64> {
    fn read(reader: &mut impl Read, digest: &mut Sha256, bytes: &mut [u8]) -> Result<()> {
        reader.read_exact(bytes)?;
        digest.update(bytes);
        Ok(())
    }
    let mut digest = Sha256::new();
    let mut magic = vec![0; MAGIC.len()];
    read(reader, &mut digest, &mut magic)?;
    if magic != MAGIC {
        return Err(Error::Incompatible("unsupported metadata snapshot".into()));
    }
    let mut tx = db.write()?;
    let (mut count, mut batch_bytes, mut batch_count) = (0u64, 0usize, 0usize);
    let mut previous: Option<(usize, Vec<u8>)> = None;
    loop {
        let mut table = [0];
        read(reader, &mut digest, &mut table)?;
        if table[0] == 255 {
            break;
        }
        let index = table[0] as usize;
        let table = *Table::ALL
            .get(index)
            .ok_or_else(|| Error::Incompatible("invalid snapshot table".into()))?;
        let mut size = [0; 4];
        read(reader, &mut digest, &mut size)?;
        let key_len = u32::from_be_bytes(size) as usize;
        read(reader, &mut digest, &mut size)?;
        let value_len = u32::from_be_bytes(size) as usize;
        if !(17..=4112).contains(&key_len) || value_len > MAX_VALUE_BYTES {
            return Err(Error::Incompatible("invalid snapshot record length".into()));
        }
        let mut key = vec![0; key_len];
        let mut value = vec![0; value_len];
        read(reader, &mut digest, &mut key)?;
        read(reader, &mut digest, &mut value)?;
        if previous.as_ref().is_some_and(|(prev_table, prev)| {
            *prev_table > index || (*prev_table == index && prev >= &key)
        }) {
            return Err(Error::Incompatible(
                "snapshot records are not ordered and unique".into(),
            ));
        }
        if batch_bytes + key_len + value_len > 8 * 1024 * 1024 || batch_count >= 512 {
            tx.commit()?;
            tx = db.write()?;
            batch_bytes = 0;
            batch_count = 0;
        }
        tx.put(table, &RecordKey::decode(&key)?, &value)?;
        batch_bytes += key_len + value_len;
        batch_count += 1;
        count += 1;
        previous = Some((index, key));
    }
    let mut expected = [0; 8];
    read(reader, &mut digest, &mut expected)?;
    let mut checksum = [0; 32];
    reader.read_exact(&mut checksum)?;
    if u64::from_be_bytes(expected) != count || digest.finalize()[..] != checksum {
        return Err(Error::Incompatible(
            "snapshot checksum or record count differs".into(),
        ));
    }
    let mut trailing = [0];
    if reader.read(&mut trailing)? != 0 {
        return Err(Error::Incompatible("trailing snapshot bytes".into()));
    }
    tx.commit()?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn snapshot_restores_cross_table_state_and_rejects_corruption_and_existing_targets() {
        let root = tempfile::tempdir().unwrap();
        let db = MetadataEngine::open(root.path().join("db"), Options::default()).unwrap();
        let key = RecordKey::new(PoolId([9; 16]), "test").unwrap();
        db.transact(8, |tx| {
            for table in Table::ALL {
                tx.put(table, &key, b"old")?;
            }
            Ok(())
        })
        .unwrap();
        struct MutatingWriter {
            bytes: Vec<u8>,
            db: MetadataEngine,
            key: RecordKey,
            mutated: bool,
        }
        impl Write for MutatingWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if !self.mutated {
                    self.db
                        .transact(8, |tx| {
                            for table in Table::ALL {
                                tx.put(table, &self.key, b"new")?;
                            }
                            Ok(())
                        })
                        .unwrap();
                    self.mutated = true;
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut writer = MutatingWriter {
            bytes: vec![],
            db: db.clone(),
            key: key.clone(),
            mutated: false,
        };
        assert_eq!(db.backup(&mut writer).unwrap(), 4);
        let archive = root.path().join("snapshot");
        std::fs::write(&archive, &writer.bytes).unwrap();
        let restored = root.path().join("restored");
        assert_eq!(
            MetadataEngine::restore_backup(&archive, &restored).unwrap(),
            4
        );
        let restore = MetadataEngine::open(&restored, Options::default()).unwrap();
        for table in Table::ALL {
            assert_eq!(
                restore.snapshot().get(table, &key).unwrap().unwrap(),
                b"old"
            );
            assert_eq!(db.snapshot().get(table, &key).unwrap().unwrap(), b"new");
        }
        assert!(MetadataEngine::restore_backup(&archive, &restored).is_err());
        let n = writer.bytes.len();
        writer.bytes[n - 1] ^= 1;
        std::fs::write(&archive, &writer.bytes).unwrap();
        let bad = root.path().join("bad");
        assert!(MetadataEngine::restore_backup(&archive, &bad).is_err());
        assert!(!bad.exists());
        std::fs::write(&archive, &writer.bytes[..12]).unwrap();
        assert!(MetadataEngine::restore_backup(&archive, &bad).is_err());
        assert!(!bad.exists());
    }
}
