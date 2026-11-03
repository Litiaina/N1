//! Maintenance-only external merge of retirement witnesses. No object catalogue
//! survives this check; RAM and open descriptors stay bounded by the run size
//! and merge fan-in even when the repository contains billions of objects.
use super::{
    Result, invalid,
    repository::Repository,
    restore::{FileKind, allowed_path},
};
use n1_link::retirement::Retirement;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

#[cfg(not(test))]
const RUN_RECORDS: usize = 8192;
#[cfg(test)]
const RUN_RECORDS: usize = 4;
const FAN_IN: u64 = 16;
#[derive(Clone, Copy)]
struct Witness {
    identity: [u8; 64],
    retained: bool,
}
struct Runs {
    directory: PathBuf,
    buffer: Vec<Witness>,
    count: u64,
}
impl Drop for Runs {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            tracing::warn!(
                "cannot remove retention scratch {}: {error}",
                self.directory.display()
            );
        }
    }
}
impl Runs {
    fn new(parent: &Path) -> Result<Self> {
        let directory = parent.join(ulid::Ulid::new().to_string());
        fs::DirBuilder::new().mode(0o700).create(&directory)?;
        Ok(Self {
            directory,
            buffer: Vec::with_capacity(RUN_RECORDS),
            count: 0,
        })
    }
    fn path(&self, level: u32, run: u64) -> PathBuf {
        self.directory.join(format!("{level}-{run}"))
    }
    fn writer(&self, level: u32, run: u64) -> Result<BufWriter<File>> {
        Ok(BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(self.path(level, run))?,
        ))
    }
    fn push(&mut self, witness: Witness) -> Result<()> {
        self.buffer.push(witness);
        if self.buffer.len() == RUN_RECORDS {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_unstable_by_key(|w| w.identity);
        let mut writer = self.writer(0, self.count)?;
        let mut pending: Option<Witness> = None;
        for witness in self.buffer.drain(..) {
            aggregate(&mut writer, &mut pending, witness)?;
        }
        if let Some(witness) = pending {
            write(&mut writer, witness)?;
        }
        writer.flush()?;
        self.count += 1;
        Ok(())
    }
    fn check(mut self) -> Result<()> {
        self.flush()?;
        if self.count == 0 {
            return Ok(());
        }
        let mut count = self.count;
        let mut level = 0;
        while count > 1 {
            let next_count = count.div_ceil(FAN_IN);
            for output in 0..next_count {
                let first = output * FAN_IN;
                let end = (first + FAN_IN).min(count);
                let mut readers = (first..end)
                    .map(|run| Ok(BufReader::new(File::open(self.path(level, run))?)))
                    .collect::<Result<Vec<_>>>()?;
                let mut heads = readers.iter_mut().map(read).collect::<Result<Vec<_>>>()?;
                let mut writer = self.writer(level + 1, output)?;
                let mut pending = None;
                while let Some((index, witness)) = heads
                    .iter()
                    .enumerate()
                    .filter_map(|(i, w)| w.map(|w| (i, w)))
                    .min_by_key(|(_, w)| w.identity)
                {
                    aggregate(&mut writer, &mut pending, witness)?;
                    heads[index] = read(&mut readers[index])?;
                }
                if let Some(witness) = pending {
                    write(&mut writer, witness)?;
                }
                writer.flush()?;
                drop(readers);
                for run in first..end {
                    fs::remove_file(self.path(level, run))?;
                }
            }
            count = next_count;
            level += 1;
        }
        let mut reader = BufReader::new(File::open(self.path(level, 0))?);
        while let Some(witness) = read(&mut reader)? {
            if !witness.retained {
                return Err(invalid(
                    "retention would discard the last saved retirement for a deleted key; retain a recovery point containing that retirement",
                ));
            }
        }
        Ok(())
    }
}
fn write(writer: &mut impl Write, witness: Witness) -> Result<()> {
    writer.write_all(&witness.identity)?;
    writer.write_all(&[u8::from(witness.retained)])?;
    Ok(())
}
fn read(reader: &mut impl Read) -> Result<Option<Witness>> {
    let mut identity = [0; 64];
    if reader.read(&mut identity[..1])? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut identity[1..])?;
    let mut flag = [0];
    reader.read_exact(&mut flag)?;
    if flag[0] > 1 {
        return Err(invalid("invalid retention scratch record"));
    }
    Ok(Some(Witness {
        identity,
        retained: flag[0] == 1,
    }))
}
fn aggregate(writer: &mut impl Write, pending: &mut Option<Witness>, next: Witness) -> Result<()> {
    if let Some(previous) = pending.as_mut() {
        if previous.identity == next.identity {
            previous.retained |= next.retained;
            return Ok(());
        }
        write(writer, *previous)?;
    }
    *pending = Some(next);
    Ok(())
}
pub(super) fn check(repo: &Repository, retained: impl Fn(&str) -> bool) -> Result<()> {
    let mut runs = Runs::new(&repo.path.join("pending"))?;
    repo.walk_snapshots(|point| {
        let descriptors = point
            .pools
            .iter()
            .map(|pool| -> Result<[u8; 32]> {
                let mut digest = Sha256::new();
                digest.update(b"Litiaina/LINK/retention-pool/v1\0");
                digest.update(serde_json::to_vec(pool)?);
                Ok(digest.finalize().into())
            })
            .collect::<Result<Vec<_>>>()?;
        // Retention already verified every complete point while holding its
        // exclusive repository lease. Scan authenticated manifest records here;
        // do not reread ordinary payload just to find small terminal records.
        repo.records(&point, |record| {
            if record.total_bytes == 96 {
                let marker = point.marker(record.root)?;
                if let Some(FileKind::Final(fp, shard)) =
                    allowed_path(&record.path, &marker, false)?
                {
                    if record.offset != 0 || record.bytes != 96 || !record.last {
                        return Err(invalid("invalid recovery retirement chunk"));
                    }
                    let bytes = repo.get(&record.hash)?;
                    Retirement::decode(&bytes, fp, shard)?;
                    let mut identity = [0; 64];
                    identity[..32].copy_from_slice(&descriptors[point.roots[record.root].pool]);
                    identity[32..].copy_from_slice(&fp.0);
                    runs.push(Witness {
                        identity,
                        retained: retained(&point.id),
                    })?;
                }
            }
            Ok(())
        })
    })?;
    runs.check()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn external_merge_preserves_union_across_multiple_levels_and_cleans_scratch() {
        let parent = std::env::temp_dir().join(format!("n1-retention-{}", ulid::Ulid::new()));
        fs::create_dir(&parent).unwrap();
        for missing in [None, Some(319)] {
            let mut runs = Runs::new(&parent).unwrap();
            for retained in [false, true] {
                for i in (0u64..640).rev() {
                    if retained && missing == Some(i) {
                        continue;
                    }
                    let mut identity = [0; 64];
                    identity[..8].copy_from_slice(&i.to_be_bytes());
                    runs.push(Witness { identity, retained }).unwrap();
                }
            }
            assert_eq!(runs.check().is_ok(), missing.is_none());
            assert_eq!(fs::read_dir(&parent).unwrap().count(), 0);
        }
        fs::remove_dir(parent).unwrap();
    }
}
