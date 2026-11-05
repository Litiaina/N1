//! Stream complete pages through O_DIRECT without changing immutable framing.
//! Only the two unaligned ends of a part use the buffered descriptor. Carry a
//! partial page between encoder groups; never read/modify/write a neighbor's
//! bytes. Distinct parts can share a buffered boundary page, but no direct page.
use crate::core::{
    Error, Result,
    ec::{ALIGNMENT, AlignedBuffer},
    linux_io,
    store::{ShardFile, blocking, collect},
};
use n1_link::Fingerprint;
use std::{fs::File, sync::Arc};

pub(super) const MIN_DIRECT_BYTES: u64 = 1024 * 1024;

pub(super) struct Writer {
    direct: Arc<File>,
    buffered: Arc<File>,
    offset: u64,
    alignment: usize,
    buffer: Option<AlignedBuffer>,
    tail: Vec<u8>,
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
                        buffer: Some(AlignedBuffer::new(capacity)),
                        tail: Vec::with_capacity(alignment),
                    }))
                })
            }))
            .await,
        )
    }

    pub(super) async fn write(&mut self, mut bytes: &[u8]) -> Result<()> {
        let alignment = self.alignment;
        let prefix = (alignment as u64 - self.offset % alignment as u64) % alignment as u64;
        if prefix != 0 {
            let length = bytes.len().min(prefix as usize);
            linux_io::write_at_async(self.buffered.clone(), bytes[..length].to_vec(), self.offset)
                .await?;
            self.offset += length as u64;
            bytes = &bytes[length..];
        }
        let length = (self.tail.len() + bytes.len()) / alignment * alignment;
        if length == 0 {
            self.tail.extend_from_slice(bytes);
            return Ok(());
        }
        let mut buffer = self.buffer.take().expect("joined direct write owns buffer");
        buffer.clear();
        let take = length - self.tail.len();
        assert!(length <= buffer.capacity(), "bounded direct group capacity");
        buffer.extend_from_slice(&self.tail);
        buffer.extend_from_slice(&bytes[..take]);
        self.tail.clear();
        self.tail.extend_from_slice(&bytes[take..]);
        self.buffer =
            Some(linux_io::write_aligned_at_async(self.direct.clone(), buffer, self.offset).await?);
        self.offset += length as u64;
        Ok(())
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
