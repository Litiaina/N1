//! Owned aligned output of one encoder turn. Only the first unaligned prefix
//! uses buffered I/O; the writer carries the final partial page to its next turn.
use crate::core::{Result, ec::AlignedBuffer, linux_io};
use std::{fs::File, sync::Arc};

pub(in crate::core::upload) struct Record {
    direct: Arc<File>,
    buffered: Arc<File>,
    prefix_offset: u64,
    offset: u64,
    prefix_bytes: usize,
    aligned_bytes: usize,
    prefix: Vec<u8>,
    buffer: Option<AlignedBuffer>,
    pub(super) tail: Vec<u8>,
}

impl Record {
    pub(super) fn new(direct: Arc<File>, buffered: Arc<File>, capacity: usize) -> Self {
        Self {
            direct,
            buffered,
            prefix_offset: 0,
            offset: 0,
            prefix_bytes: 0,
            aligned_bytes: 0,
            prefix: Vec::new(),
            buffer: Some(AlignedBuffer::new(capacity)),
            tail: Vec::new(),
        }
    }

    pub(super) fn reset(&mut self, offset: u64, prefix: usize, aligned: usize) {
        self.prefix_offset = offset;
        self.offset = offset + prefix as u64;
        self.prefix_bytes = prefix;
        self.aligned_bytes = aligned;
        self.prefix.clear();
        self.buffer.as_mut().expect("joined aligned output").clear();
        self.tail.clear();
        assert!(aligned <= self.buffer.as_ref().unwrap().capacity());
    }

    pub(in crate::core::upload) fn append(&mut self, mut bytes: &[u8]) {
        let take = bytes.len().min(self.prefix_bytes - self.prefix.len());
        self.prefix.extend_from_slice(&bytes[..take]);
        bytes = &bytes[take..];
        let buffer = self.buffer.as_mut().expect("encoder owns aligned output");
        let take = bytes.len().min(self.aligned_bytes - buffer.len());
        assert_eq!(buffer.extend_from_slice(&bytes[..take]), take);
        self.tail.extend_from_slice(&bytes[take..]);
    }

    pub(super) fn complete(&self) {
        assert_eq!(self.prefix.len(), self.prefix_bytes);
        assert_eq!(self.buffer.as_ref().unwrap().len(), self.aligned_bytes);
    }

    pub(in crate::core::upload) async fn write(mut self) -> Result<Self> {
        if !self.prefix.is_empty() {
            self.prefix = linux_io::write_owned_at_async(
                self.buffered.clone(),
                std::mem::take(&mut self.prefix),
                self.prefix_offset,
            )
            .await?;
        }
        if self.aligned_bytes != 0 {
            self.buffer = Some(
                linux_io::write_aligned_at_async(
                    self.direct.clone(),
                    self.buffer.take().expect("owned aligned output"),
                    self.offset,
                )
                .await?,
            );
        }
        Ok(self)
    }
}
