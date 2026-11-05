//! Two bounded stages: prepare up to four adjacent stripes while the previous
//! group's owned records are in flight. Both stages are joined on every error.
use super::read_frames;
use crate::core::{
    Error, Result, corrupt,
    ec::StripeCodec,
    io_scheduler::IoProgress,
    linux_io,
    storage_pool::Pool,
    store::{ShardFile, blocking, collect},
};
use n1_link::{Fingerprint, UploadPlan, shard};
use sha2::{Digest, Sha256};
use std::ops::Range;
use tokio::io::{AsyncRead, AsyncReadExt};

const BUFFERED_STRIPES: u64 = 4;

struct Group {
    stripes: Range<u64>,
    source_bytes: u64,
    partial_tail: bool,
    records: Vec<Vec<u8>>,
}

struct Encoder<'a, R> {
    pool: &'a Pool,
    files: &'a [ShardFile],
    fp: Fingerprint,
    reader: &'a mut R,
    codec: StripeCodec,
    source: Vec<u8>,
    remaining: u64,
    root: Sha256,
}
impl<R: AsyncRead + Unpin> Encoder<'_, R> {
    async fn prepare(&mut self, stripes: Range<u64>, mut records: Vec<Vec<u8>>) -> Result<Group> {
        // Fresh files short-circuit without disk I/O. A retry reads each member's
        // adjacent frames once, then compares every complete immutable record.
        let old = collect(
            futures_util::future::join_all(
                self.files
                    .iter()
                    .map(|file| read_frames(file, self.fp, stripes.clone())),
            )
            .await,
        )?;
        for buffer in &mut records {
            buffer.clear();
        }
        let mut source_bytes = 0;
        let mut partial_tail = false;
        for (position, stripe) in stripes.clone().enumerate() {
            let take = self.remaining.min(self.pool.layout.stripe_bytes()) as usize;
            self.source.resize(take, 0);
            self.reader.read_exact(&mut self.source).await?;
            self.remaining -= take as u64;
            let hash = self.files[0]
                .header
                .source_hash(self.fp, stripe, &self.source)?;
            self.codec.encode_with(&self.source, |i, payload| {
                shard::append_frame(
                    &self.files[i].header,
                    self.fp,
                    stripe,
                    hash,
                    payload,
                    &mut records[i],
                )?;
                Ok(())
            })?;
            // Validate all members before this group can change any shard.
            for ((file, buffer), previous) in self.files.iter().zip(&records).zip(&old) {
                let record_bytes = file.header.record_bytes() as usize;
                let record = &buffer[position * record_bytes..(position + 1) * record_bytes];
                if let Some(previous) = &previous[position] {
                    if previous.bytes() != record {
                        return Err(Error::Conflict(
                            "part retry conflicts with existing immutable bytes".into(),
                        ));
                    }
                } else if file.finalized {
                    return Err(corrupt("finalized shard requires repair before retry"));
                }
            }
            self.root.update(hash);
            source_bytes += take as u64;
            partial_tail = take < self.pool.layout.stripe_bytes() as usize;
        }
        Ok(Group {
            stripes,
            source_bytes,
            partial_tail,
            records,
        })
    }
}

async fn write(
    files: &[ShardFile],
    group: Group,
    direct: &mut [Option<super::direct::Writer>],
) -> Result<Vec<Vec<u8>>> {
    collect(
        futures_util::future::join_all(files.iter().zip(group.records).zip(direct).map(
            |((file, mut record), direct)| async move {
                if file.finalized {
                    return Ok(record);
                }
                if let Some(writer) = direct {
                    writer.write(&record).await?;
                    return Ok(record);
                }
                let offset = file.header.frame_offset(group.stripes.start)?;
                let tail = file.header.frame_offset(group.stripes.end - 1)?;
                let length = record.len();
                // Only a region beyond the original EOF is known to contain zeros.
                // Retry/overlap regions are written in full to clear stale contents.
                let sparse = if group.partial_tail && file.readable_bytes <= tail {
                    record
                        .iter()
                        .rposition(|byte| *byte != 0)
                        .map_or(length, |i| i + 1)
                } else {
                    length
                };
                if length.saturating_sub(sparse) >= 4096 {
                    record.truncate(sparse);
                    // Positioned tail writes preserve other multipart ranges.
                    let (prefix, tail) = tokio::join!(
                        linux_io::write_owned_at_async(file.file.clone(), record, offset),
                        linux_io::write_at_async(
                            file.file.clone(),
                            vec![0],
                            offset + length as u64 - 1
                        )
                    );
                    let bytes = prefix?;
                    tail?;
                    Ok(bytes)
                } else {
                    linux_io::write_owned_at_async(file.file.clone(), record, offset)
                        .await
                        .map_err(Error::from)
                }
            },
        ))
        .await,
    )
}

pub(crate) async fn write_frames<R: AsyncRead + Unpin>(
    pool: &Pool,
    files: &[ShardFile],
    plan: UploadPlan,
    part: u64,
    reader: &mut R,
    progress: &IoProgress,
    writer_buffer_size: usize,
) -> Result<[u8; 32]> {
    // Preserve first-payload capacity admission without treating Begin's tiny
    // persisted headers as a reservation for the entire declared object.
    collect(
        futures_util::future::join_all(
            files
                .iter()
                .filter(|file| !file.finalized && file.readable_bytes == 128)
                .map(|file| {
                    let member = file.member.clone();
                    let required = file.header.file_bytes();
                    let file = file.file.clone();
                    blocking(move || {
                        let length = required?;
                        member.check_capacity(length)?;
                        // Sparse extension fixes the immutable shard's logical
                        // end before payload writes. This prevents speculative
                        // allocation beyond EOF without reserving disk blocks
                        // or flushing a large dirty file with a final truncate.
                        // Every writer uses the same length; this never shrinks
                        // another multipart range. Holes are still missing
                        // frames until their identity/checksum validates.
                        file.set_len(length)?;
                        Ok(())
                    })
                }),
        )
        .await,
    )?;
    let mut encoder = Encoder {
        pool,
        files,
        fp: plan.identity.fingerprint(),
        reader,
        codec: StripeCodec::new(&pool.layout)?,
        source: Vec::new(),
        remaining: plan.part_length(part)?,
        root: shard::part_hasher(pool.layout.shard_format, part),
    };
    let buffers = || (0..files.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    let mut spare = buffers();
    let stripes = files[0].header.part_stripes(part)?;
    let buffered = if writer_buffer_size == 0 {
        BUFFERED_STRIPES
    } else {
        (writer_buffer_size / pool.layout.block_bytes as usize).clamp(1, BUFFERED_STRIPES as usize)
            as u64
    };
    let mut direct = if plan.total_bytes >= super::direct::MIN_DIRECT_BYTES && !stripes.is_empty() {
        super::direct::Writer::open(files, plan.identity.fingerprint(), stripes.start, buffered)
            .await?
    } else {
        (0..files.len()).map(|_| None).collect()
    };
    let mut groups = (stripes.start..stripes.end)
        .step_by(buffered as usize)
        .map(|start| start..(start + buffered).min(stripes.end));
    if let Some(first) = groups.next() {
        let mut current = encoder.prepare(first, buffers()).await?;
        for group in groups {
            let completed_bytes = current.source_bytes;
            let (written, prepared) = tokio::join!(biased;
                async {
                    let written = write(files, current, &mut direct).await;
                    if written.is_ok() { progress.record(completed_bytes); }
                    written
                }, encoder.prepare(group, spare));
            // Both stages have completed before either error is propagated.
            spare = written?;
            current = prepared?;
        }
        let bytes = current.source_bytes;
        write(files, current, &mut direct).await?;
        progress.record(bytes);
    }
    collect(
        futures_util::future::join_all(direct.iter_mut().flatten().map(|writer| writer.finish()))
            .await,
    )?;
    if encoder.remaining != 0 {
        return Err(Error::Invalid("multipart body length mismatch".into()));
    }
    Ok(encoder.root.finalize().into())
}
