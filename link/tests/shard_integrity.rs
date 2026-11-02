use n1_link::shard::*;
use n1_link::{Durability, Fingerprint, Identity, PoolId, PoolLayout, UploadPlan};
use sha2::{Digest, Sha256};

fn setup() -> (PoolLayout, UploadPlan, ShardHeader, Fingerprint) {
    let pool = PoolLayout {
        id: PoolId([7; 16]),
        generation: 2,
        data_shards: 3,
        parity_shards: 1,
        block_bytes: 4096,
        placement_version: 1,
        shard_format: n1_link::ShardFormat::Crc32cSha256V2,
    };
    let plan = UploadPlan {
        identity: Identity {
            pool: pool.id,
            generation: 2,
            token: [9; 32],
        },
        total_bytes: 24577,
        part_bytes: 12288,
        expires_at: 2_000_000_000,
        durability: Durability::Durable,
    };
    let header = ShardHeader::new(&pool, plan, 0).unwrap();
    (pool, plan, header, plan.identity.fingerprint())
}

#[test]
fn canonical_inline_header_roundtrip_and_all_byte_corruption() {
    let (pool, _, h, fp) = setup();
    let encoded = h.encode(fp);
    assert_eq!(ShardHeader::decode(&encoded, &pool, fp, 0).unwrap(), h);
    for i in 0..HEADER_BYTES {
        let mut bad = encoded;
        bad[i] ^= 1;
        assert!(
            ShardHeader::decode(&bad, &pool, fp, 0).is_err(),
            "header byte {i}"
        );
    }
    assert!(ShardHeader::decode(&encoded, &pool, Fingerprint([0; 32]), 0).is_err());
    assert!(ShardHeader::decode(&encoded, &pool, fp, 1).is_err());
    for size in 0..HEADER_BYTES {
        assert!(ShardHeader::decode(&encoded[..size], &pool, fp, 0).is_err());
    }
}

#[test]
fn sparse_holes_torn_writes_wrong_offsets_and_foreign_objects_fail() {
    let (_, _, h, fp) = setup();
    let payload = vec![123; 4096];
    let source: [u8; 32] = Sha256::digest(&payload).into();
    let record = encode_frame(&h, fp, 0, source, &payload).unwrap();
    let frame = decode_frame(&h, fp, 0, &record).unwrap();
    assert_eq!(frame.payload, payload);
    assert_eq!(frame.source_hash, source);
    assert!(decode_frame(&h, fp, 0, &vec![0; record.len()]).is_err());
    assert!(decode_frame(&h, fp, 1, &record).is_err());
    assert!(decode_frame(&h, Fingerprint([0; 32]), 0, &record).is_err());
    for i in 0..record.len() {
        let mut bad = record.clone();
        bad[i] ^= 1;
        assert!(decode_frame(&h, fp, 0, &bad).is_err(), "frame byte {i}");
    }
    for size in [0, 1, 95, 96, 200, record.len() - 1] {
        assert!(decode_frame(&h, fp, 0, &record[..size]).is_err());
    }
}

#[test]
fn final_seals_are_bound_to_full_header_and_object_identity() {
    let (_, _, h, fp) = setup();
    let root = [5; 32];
    let seal = encode_seal(&h, fp, root);
    assert_eq!(decode_seal(&h, fp, &seal).unwrap(), root);
    for i in 0..SEAL_BYTES {
        let mut bad = seal;
        bad[i] ^= 1;
        assert!(decode_seal(&h, fp, &bad).is_err(), "seal byte {i}");
    }
    let mut other = h.clone();
    other.expires_at += 1;
    assert!(decode_seal(&other, fp, &seal).is_err());
    assert!(decode_seal(&h, Fingerprint([0; 32]), &seal).is_err());
    assert!(decode_seal(&h, fp, &[0; SEAL_BYTES]).is_err());
}

#[test]
fn stripe_layout_and_partial_last_part_have_no_overlap() {
    let (_, _, h, _) = setup();
    assert_eq!(h.stripes(), 3);
    assert_eq!(h.parts(), 3);
    assert_eq!(h.part_stripes(0).unwrap(), 0..1);
    assert_eq!(h.part_stripes(2).unwrap(), 2..3);
    assert_eq!(h.source_bytes_in_stripe(2).unwrap(), 1);
    assert_eq!(h.frame_offset(1).unwrap(), 128 + 96 + 4096);
    assert_eq!(h.seal_offset(), 128 + 3 * (96 + 4096));
    assert_eq!(h.file_bytes().unwrap(), 128 + 3 * (96 + 4096) + 96);
    assert!(h.frame_offset(3).is_err());
    assert!(h.part_stripes(3).is_err());
}

#[test]
fn empty_objects_have_headers_and_seals_but_no_frames() {
    let (pool, plan, _, fp) = setup();
    let empty = UploadPlan {
        total_bytes: 0,
        ..plan
    };
    let h = ShardHeader::new(&pool, empty, 0).unwrap();
    assert_eq!(h.stripes(), 0);
    assert_eq!(h.parts(), 0);
    assert_eq!(h.file_bytes().unwrap(), 224);
    assert!(h.frame_offset(0).is_err());
    assert!(decode_seal(&h, fp, &encode_seal(&h, fp, [0; 32])).is_ok());
}
