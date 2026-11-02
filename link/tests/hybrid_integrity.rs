use n1_link::{Durability, Fingerprint, PoolId, PoolLayout, ShardFormat, shard::*};
use sha2::{Digest, Sha256};

fn layout(format: ShardFormat) -> PoolLayout {
    PoolLayout {
        id: PoolId([1; 16]),
        generation: 1,
        data_shards: 1,
        parity_shards: 1,
        block_bytes: 4096,
        placement_version: 1,
        shard_format: format,
    }
}

fn header(format: ShardFormat) -> ShardHeader {
    ShardHeader {
        format,
        shard: 0,
        data_shards: 1,
        parity_shards: 1,
        block_bytes: 4096,
        total_bytes: 8193,
        part_bytes: 8192,
        expires_at: 2_000_000_000,
        durability: Durability::Durable,
    }
}

// Independent bitwise Castagnoli reference: no production CRC implementation.
fn reference_crc32c(previous: u32, bytes: &[u8]) -> u32 {
    let mut state = !previous;
    for byte in bytes {
        state ^= u32::from(*byte);
        for _ in 0..8 {
            state = (state >> 1) ^ (0u32.wrapping_sub(state & 1) & 0x82f6_3b78);
        }
    }
    !state
}

#[test]
fn crc32c_known_answer_and_segmented_unaligned_inputs_match_independent_reference() {
    assert_eq!(crc32c::crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(crc32c::crc32c(b""), 0);
    let bytes = (0..65545).map(|i| (i * 197 + 11) as u8).collect::<Vec<_>>();
    for offset in 0..8 {
        for length in [0, 1, 7, 8, 31, 64, 255, 256, 4096, 65536] {
            let input = &bytes[offset..offset + length];
            let expected = reference_crc32c(0, input);
            assert_eq!(crc32c::crc32c(input), expected);
            let split = length / 3;
            let crc = crc32c::crc32c_append(crc32c::crc32c(&input[..split]), &input[split..]);
            assert_eq!(crc, expected);
        }
    }
}

#[test]
fn hybrid_frame_checksum_matches_reference_and_reserved_bytes_are_canonical() {
    let h = header(ShardFormat::Crc32cSha256V2);
    let fp = Fingerprint([13; 32]);
    let source = vec![7; 4096];
    let hash = h.source_hash(fp, 0, &source).unwrap();
    let frame = encode_frame(&h, fp, 0, hash, &source).unwrap();
    let mut crc = reference_crc32c(0, b"Litiaina/LINK/frame/v2\0");
    for part in [&fp.0[..], &frame[..56], &frame[60..]] {
        crc = reference_crc32c(crc, part);
    }
    assert_eq!(&frame[56..60], crc.to_be_bytes());
    assert!(frame[60..96].iter().all(|byte| *byte == 0));
    for index in 60..96 {
        let mut bad = frame.clone();
        bad[index] = 1;
        // Recalculate the CRC: noncanonical reserved bytes must still fail.
        let mut crc = reference_crc32c(0, b"Litiaina/LINK/frame/v2\0");
        for part in [&fp.0[..], &bad[..56], &bad[60..]] {
            crc = reference_crc32c(crc, part);
        }
        bad[56..60].copy_from_slice(&crc.to_be_bytes());
        assert!(decode_frame(&h, fp, 0, &bad).is_err());
    }
}

#[test]
fn source_sha256_binds_identity_position_length_and_exact_logical_bytes() {
    let h = header(ShardFormat::Crc32cSha256V2);
    let fp = Fingerprint([13; 32]);
    let bytes = vec![7; 4096];
    let digest = h.source_hash(fp, 0, &bytes).unwrap();
    let mut expected = Sha256::new();
    expected.update(b"Litiaina/LINK/source/v2\0");
    expected.update(fp.0);
    expected.update(0u64.to_be_bytes());
    expected.update(4096u32.to_be_bytes());
    expected.update(&bytes);
    assert_eq!(digest, <[u8; 32]>::from(expected.finalize()));
    assert_ne!(
        digest,
        h.source_hash(Fingerprint([14; 32]), 0, &bytes).unwrap()
    );
    assert_ne!(digest, h.source_hash(fp, 1, &bytes).unwrap());
    assert!(h.source_hash(fp, 2, &bytes).is_err());
    assert!(h.source_hash(fp, 0, &bytes[..4095]).is_err());
    let tail = h.source_hash(fp, 2, &[7]).unwrap();
    assert_ne!(tail, digest);
    let altered = vec![8; 4096];
    assert_ne!(digest, h.source_hash(fp, 0, &altered).unwrap());
}

#[test]
fn crc_valid_foreign_frame_cannot_supply_a_valid_source_digest() {
    let h = header(ShardFormat::Crc32cSha256V2);
    let original = Fingerprint([13; 32]);
    let destination = Fingerprint([14; 32]);
    let source = vec![7; 4096];
    let hash = h.source_hash(original, 0, &source).unwrap();
    // Even a recalculated destination CRC cannot turn a foreign source digest
    // into an identity-bound digest. The engine must enforce this before emit.
    let frame = encode_frame(&h, destination, 0, hash, &source).unwrap();
    let decoded = decode_frame(&h, destination, 0, &frame).unwrap();
    assert_ne!(
        decoded.source_hash,
        h.source_hash(destination, 0, decoded.payload).unwrap()
    );
    let moved = encode_frame(&h, original, 1, hash, &source).unwrap();
    let decoded = decode_frame(&h, original, 1, &moved).unwrap();
    assert_ne!(
        decoded.source_hash,
        h.source_hash(original, 1, decoded.payload).unwrap()
    );
}

#[test]
fn legacy_pool_defaults_and_hybrid_pool_format_cannot_be_reinterpreted() {
    let mut encoded = serde_json::to_value(layout(ShardFormat::Sha256V1)).unwrap();
    encoded.as_object_mut().unwrap().remove("shard_format");
    let old: PoolLayout = serde_json::from_value(encoded).unwrap();
    assert_eq!(old.shard_format, ShardFormat::Sha256V1);
    let new = layout(ShardFormat::Crc32cSha256V2);
    let fp = Fingerprint([13; 32]);
    let v1 = header(ShardFormat::Sha256V1).encode(fp);
    let v2 = header(ShardFormat::Crc32cSha256V2).encode(fp);
    assert!(ShardHeader::decode(&v1, &old, fp, 0).is_ok());
    assert!(ShardHeader::decode(&v2, &new, fp, 0).is_ok());
    assert!(ShardHeader::decode(&v1, &new, fp, 0).is_err());
    assert!(ShardHeader::decode(&v2, &old, fp, 0).is_err());
}

#[test]
fn byte_exact_prechange_v1_fixture_still_decodes_and_reencodes_identically() {
    // Captured before this change from the frozen 252b engine's LINK library.
    let rows = include_str!("fixtures/shard-v1.hex")
        .lines()
        .map(|line| hex::decode(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 3);
    let pool = layout(ShardFormat::Sha256V1);
    let fp = Fingerprint([0x42; 32]);
    let h = ShardHeader::decode(&rows[0], &pool, fp, 0).unwrap();
    assert_eq!(h.total_bytes, 4093);
    assert_eq!(h.encode(fp).as_slice(), rows[0]);
    let frame = decode_frame(&h, fp, 0, &rows[1]).unwrap();
    assert_eq!(
        h.source_hash(fp, 0, &frame.payload[..4093]).unwrap(),
        frame.source_hash
    );
    assert_eq!(
        encode_frame(&h, fp, 0, frame.source_hash, frame.payload).unwrap(),
        rows[1]
    );
    let mut part = part_hasher(h.format, 0);
    part.update(frame.source_hash);
    let mut object = object_hasher(h.format, 4093, 4096);
    object.update(part.finalize());
    let root = object.finalize().into();
    assert_eq!(decode_seal(&h, fp, &rows[2]).unwrap(), root);
    assert_eq!(encode_seal(&h, fp, root).as_slice(), rows[2]);
}
