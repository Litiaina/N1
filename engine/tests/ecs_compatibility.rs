use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use ecs_core::{
    ChecksumPolicy, CompletionPolicy, EcLayout, EncodeOptions, ErasureStreamEncoder, ShardWriters,
};
use n1::core::ec::ErasureStreamWriter;
use tokio::io::AsyncWrite;

#[derive(Clone, Debug, Default)]
struct MemoryWriter(Arc<Mutex<Vec<u8>>>);

impl MemoryWriter {
    fn bytes(&self) -> Vec<u8> {
        self.0.lock().unwrap().clone()
    }
}

impl AsyncWrite for MemoryWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Copy, Debug)]
enum CapacityFailure {
    Write,
    Flush,
}

#[derive(Clone, Copy, Debug)]
struct CapacityFailureWriter(CapacityFailure);

impl AsyncWrite for CapacityFailureWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.0 {
            CapacityFailure::Write => Poll::Ready(Err(io::Error::from_raw_os_error(libc::ENOSPC))),
            CapacityFailure::Flush => Poll::Ready(Ok(bytes.len())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.0 {
            CapacityFailure::Write => Poll::Ready(Ok(())),
            CapacityFailure::Flush => Poll::Ready(Err(io::Error::from_raw_os_error(libc::EDQUOT))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn source_bytes(length: usize) -> Vec<u8> {
    (0..length)
        .map(|position| {
            (position as u8)
                .wrapping_mul(31)
                .wrapping_add((position >> 11) as u8)
        })
        .collect()
}

#[tokio::test]
async fn ecs_core_is_byte_identical_to_n1_streaming_ec() {
    const BLOCK_SIZE: usize = 4096;

    for (data_shards, parity_shards) in [(1, 0), (1, 1), (3, 1), (4, 2), (6, 2)] {
        let layout = EcLayout::new(data_shards, parity_shards, BLOCK_SIZE).unwrap();
        let lengths = [
            0,
            1,
            BLOCK_SIZE - 1,
            BLOCK_SIZE,
            layout.stripe_size() - 1,
            layout.stripe_size(),
            layout.stripe_size() + 1,
            layout.stripe_size() * 2 + 123,
        ];

        for length in lengths {
            let source = source_bytes(length);
            let old_handles = (0..layout.total_shards())
                .map(|_| MemoryWriter::default())
                .collect::<Vec<_>>();
            let mut old = ErasureStreamWriter::new(
                old_handles.iter().cloned().map(Some).collect(),
                data_shards,
                parity_shards,
                length as u64,
                true,
                BLOCK_SIZE,
            )
            .unwrap();
            for chunk in source.chunks(7919) {
                old.write(chunk).await.unwrap();
            }
            old.finalize().await.unwrap();
            let old_source_checksum = old.get_total_checksum().unwrap();
            let old_piece_checksums = old.get_shard_checksums();

            let new_handles = (0..layout.total_shards())
                .map(|_| MemoryWriter::default())
                .collect::<Vec<_>>();
            let writers = ShardWriters::new(
                new_handles[..data_shards]
                    .iter()
                    .cloned()
                    .map(Some)
                    .collect(),
                new_handles[data_shards..]
                    .iter()
                    .cloned()
                    .map(Some)
                    .collect(),
            );
            let mut new = ErasureStreamEncoder::new(
                layout,
                writers,
                length as u64,
                EncodeOptions {
                    completion: CompletionPolicy::RequireAll,
                    checksum: Some(ChecksumPolicy::crc32(true)),
                    expected_source_checksum: None,
                },
            )
            .unwrap();
            for chunk in source.chunks(7919) {
                new.write(chunk).await.unwrap();
            }
            let output = new.finish().await.unwrap();

            assert_eq!(
                output.report.source_checksum.unwrap().to_hex(),
                old_source_checksum,
                "source checksum differs for {data_shards}+{parity_shards}, length {length}"
            );
            assert_eq!(
                output
                    .report
                    .pieces
                    .iter()
                    .map(|piece| piece.checksum.as_ref().unwrap().to_hex())
                    .collect::<Vec<_>>(),
                old_piece_checksums,
                "piece checksums differ for {data_shards}+{parity_shards}, length {length}"
            );
            for index in 0..layout.total_shards() {
                assert_eq!(
                    new_handles[index].bytes(),
                    old_handles[index].bytes(),
                    "piece {index} differs for {data_shards}+{parity_shards}, length {length}"
                );
            }
        }
    }
}

#[tokio::test]
async fn ecs_adapter_preserves_kernel_capacity_failures() {
    for failure in [CapacityFailure::Write, CapacityFailure::Flush] {
        let mut writer = ErasureStreamWriter::new(
            vec![Some(CapacityFailureWriter(failure))],
            1,
            0,
            4096,
            true,
            4096,
        )
        .unwrap();
        let error = match writer.write(&vec![0x5a; 4096]).await {
            Err(error) => error,
            Ok(()) => writer
                .finalize()
                .await
                .expect_err("kernel capacity failure must reject finalization"),
        };
        assert!(
            error.is_capacity_error(),
            "{failure:?} did not preserve the capacity category: {error:?}"
        );
    }
}
