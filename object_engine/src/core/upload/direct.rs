//! Stream complete pages through O_DIRECT without changing immutable framing.
//! Only the two unaligned ends of a part use the buffered descriptor. Carry a
//! partial page between encoder groups; never read/modify/write a neighbor's
//! bytes. Distinct parts can share a buffered boundary page, but no direct page.
mod record;
pub(super) use record::Record;

use crate::core::{
    Error, Result,
    ec::ALIGNMENT,
    linux_io,
    store::{ShardFile, blocking, collect},
};
use n1_link::Fingerprint;
use std::{fs::File, ops::Range, os::fd::AsRawFd, sync::Arc};

pub(super) const MIN_DIRECT_BYTES: u64 = 1024 * 1024;

pub(super) struct Writer {
    direct: Arc<File>,
    buffered: Arc<File>,
    offset: u64,
    alignment: usize,
    capacity: usize,
    tail: Vec<u8>,
    pub(super) hole: Range<u64>,
}

impl Writer {
    pub(super) async fn open(
        files: &[ShardFile],
        fp: Fingerprint,
        stripe: u64,
        group_stripes: u64,
    ) -> Result<Vec<Option<Self>>> {
        // Buffered/direct ranges must also be disjoint cache pages on hosts
        // whose kernel page size exceeds ECS's 4096-byte buffer alignment.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let alignment = usize::try_from(page)
            .ok()
            .filter(|n| n.is_power_of_two())
            .ok_or_else(|| Error::Unavailable("cannot determine I/O page alignment".into()))?
            .max(ALIGNMENT);
        collect(
            futures_util::future::join_all(files.iter().map(|file| {
                let file = file.clone();
                blocking(move || {
                    if file.finalized {
                        return Ok(None);
                    }
                    let direct = file.member.open_stage_direct(fp, &file.file)?;
                    let hole = first_hole(&direct, file.header.frame_offset(stripe)?)?;
                    // One output group plus at most one carried partial page.
                    // Reused for the entire part; independent of object length.
                    let capacity = (group_stripes as usize * file.header.record_bytes() as usize
                        + alignment)
                        / alignment
                        * alignment;
                    Ok(Some(Self {
                        direct,
                        buffered: file.file,
                        offset: file.header.frame_offset(stripe)?,
                        alignment,
                        capacity,
                        tail: Vec::with_capacity(alignment),
                        hole,
                    }))
                })
            }))
            .await,
        )
    }

    pub(super) fn record(&mut self, reuse: Option<Record>, bytes: usize) -> Record {
        let prefix = ((self.alignment as u64 - self.offset % self.alignment as u64)
            % self.alignment as u64) as usize;
        let prefix = prefix.min(bytes);
        let aligned = (self.tail.len() + bytes - prefix) / self.alignment * self.alignment;
        let mut record = reuse.unwrap_or_else(|| {
            Record::new(self.direct.clone(), self.buffered.clone(), self.capacity)
        });
        record.reset(self.offset, prefix, aligned);
        // A previous turn's carry is always at an aligned offset. The prefix
        // belongs only to the first turn, before any carry can exist.
        assert!(prefix == 0 || self.tail.is_empty());
        record.append(&self.tail);
        self.tail.clear();
        self.offset += prefix as u64 + aligned as u64;
        record
    }

    pub(super) fn carry(&mut self, record: &mut Record) {
        record.complete();
        assert!(record.tail.len() < self.alignment);
        self.tail.extend_from_slice(&record.tail);
        record.tail.clear();
    }

    pub(super) async fn finish(&mut self) -> Result<()> {
        if !self.tail.is_empty() {
            linux_io::write_at_async(
                self.buffered.clone(),
                std::mem::take(&mut self.tail),
                self.offset,
            )
            .await?;
        }
        Ok(())
    }
}

/// Capture one sparse interval before this part starts writing. The caller
/// holds the part's exclusive lock; other parts cannot change its frame bytes.
/// Filesystems may report fewer holes than actually exist, which only loses
/// acceleration. Unsupported extent queries retain the ordinary frame reader.
fn first_hole(file: &File, offset: u64) -> Result<Range<u64>> {
    let seek = |offset, whence| {
        let position = unsafe { libc::lseek(file.as_raw_fd(), offset as i64, whence) };
        if position < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(position as u64)
        }
    };
    let start = match seek(offset, libc::SEEK_HOLE) {
        Ok(start) => start,
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::ENXIO | libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)
            ) =>
        {
            return Ok(0..0);
        }
        Err(error) => return Err(error.into()),
    };
    let end = match seek(start, libc::SEEK_DATA) {
        Ok(end) => end,
        Err(error) if error.raw_os_error() == Some(libc::ENXIO) => file.metadata()?.len(),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)
            ) =>
        {
            return Ok(0..0);
        }
        Err(error) => return Err(error.into()),
    };
    if start < offset || end < start {
        return Err(Error::Invalid("invalid sparse upload extent".into()));
    }
    Ok(start..end)
}
