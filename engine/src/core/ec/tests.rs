use super::ErasureStreamWriter;
use crate::core::checksum::{format_sha256, sha256_digest};

#[tokio::test]
async fn stream_writer_records_checksums_for_every_immutable_shard_block() {
    let block_size = 4096;
    let input = (0..block_size * 2)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let writers = vec![
        Some(tokio::io::sink()),
        Some(tokio::io::sink()),
        Some(tokio::io::sink()),
    ];
    let mut writer = ErasureStreamWriter::new(writers, 2, 1, block_size, true, block_size)
        .expect("create writer");
    writer.write(&input).await.expect("write stripe");
    writer.finalize().await.expect("finalize");

    let checksums = writer.get_shard_block_checksums();
    let shard_checksums = writer.get_shard_checksums();
    assert_eq!(checksums.len(), 3);
    assert!(checksums.iter().all(|blocks| blocks.len() == 1));
    assert_eq!(checksums[0][0], sha256_digest(&input[..block_size]));
    assert_eq!(checksums[1][0], sha256_digest(&input[block_size..]));
    assert_eq!(
        shard_checksums[0],
        format_sha256(&sha256_digest(&input[..block_size]))
    );
    assert_eq!(
        shard_checksums[1],
        format_sha256(&sha256_digest(&input[block_size..]))
    );
    assert_eq!(
        writer.get_total_checksum(),
        Some(format_sha256(&sha256_digest(&input)))
    );
}

#[tokio::test]
async fn stream_writer_commits_at_k_and_describes_the_missing_exact_slot() {
    let block_size = 4096;
    let input = (0..block_size * 2)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let writers = vec![None, Some(tokio::io::sink()), Some(tokio::io::sink())];
    let mut writer = ErasureStreamWriter::new(writers, 2, 1, block_size, true, block_size)
        .expect("create degraded writer");

    writer.write(&input).await.expect("write at K");
    writer.finalize().await.expect("finalize at K");

    assert_eq!(writer.get_shard_checksums().len(), 3);
    assert_eq!(writer.get_shard_block_checksums().len(), 3);
    assert!(
        writer
            .get_shard_block_checksums()
            .iter()
            .all(|blocks| blocks.len() == 1)
    );
    assert_eq!(
        writer
            .into_writers()
            .iter()
            .filter(|writer| writer.is_some())
            .count(),
        2
    );
}

#[tokio::test]
async fn stream_writer_rejects_a_commit_below_k() {
    let block_size = 4096;
    let writers = vec![None, Some(tokio::io::sink()), None];
    let mut writer = ErasureStreamWriter::new(writers, 2, 1, block_size, true, block_size)
        .expect("create under-width writer");

    assert!(writer.write(&vec![7; block_size * 2]).await.is_err());
}
