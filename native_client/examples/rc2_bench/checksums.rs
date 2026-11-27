//! Isolated hot-buffer CPU work: actual LINK frame validation/source SHA-256
//! versus the former shard CRC32 pass. This is not a download benchmark.
use super::{Result, pattern, process, supervisor::private_json};
use n1_link::{
    Durability, Fingerprint,
    shard::{self, ShardHeader},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{hint::black_box, path::Path, time::Instant};

pub fn run(args: Vec<String>) -> Result<()> {
    if args.len() != 3 || cfg!(debug_assertions) {
        return Err("release rc2_bench --profile-checksums PROJECT-report.json".into());
    }
    let output = Path::new(&args[2]);
    if !output
        .parent()
        .ok_or("missing report parent")?
        .canonicalize()?
        .starts_with("/mnt/test-project")
    {
        return Err("CPU profile report must be on the PROJECT HDD".into());
    }
    const BLOCK: usize = 256 * 1024;
    const BYTES: usize = 1024 * 1024 * 1024;
    let block = pattern(0x20261102);
    let payload = &block[..BLOCK];
    let source_hash: [u8; 32] = Sha256::digest(payload).into();
    let header = ShardHeader {
        format: n1_link::ShardFormat::Sha256V1,
        shard: 0,
        data_shards: 1,
        parity_shards: 1,
        block_bytes: BLOCK as u32,
        total_bytes: BLOCK as u64,
        part_bytes: BLOCK as u64,
        expires_at: 0,
        durability: Durability::Durable,
    };
    let fp = Fingerprint([19; 32]);
    let frame = shard::encode_frame(&header, fp, 0, source_hash, payload)?;
    let hybrid = ShardHeader {
        format: n1_link::ShardFormat::Crc32cSha256V2,
        ..header.clone()
    };
    let hybrid_hash = hybrid.source_hash(fp, 0, payload)?;
    let hybrid_frame = shard::encode_frame(&hybrid, fp, 0, hybrid_hash, payload)?;
    let names = [
        "crc32_shard_pass",
        "sha256_source_per_stripe",
        "sha256_frame_per_stripe",
        "sha256_frame_and_source",
        "ring_sha256_source_per_stripe",
        "crc32c_payload_pass",
        "crc32c_frame_per_stripe",
        "bound_sha256_source_per_stripe",
        "crc32c_frame_and_bound_source",
    ];
    let mut rows = Vec::new();
    for round in 0..3 {
        for step in 0..names.len() {
            let case = (step + round) % names.len();
            let before = process(std::process::id())?;
            let start = Instant::now();
            if case == 0 || case == 5 {
                if case == 0 {
                    let mut hash = crc32fast::Hasher::new();
                    for _ in 0..BYTES / (64 * 1024) {
                        hash.update(black_box(&payload[..64 * 1024]));
                    }
                    black_box(hash.finalize());
                } else {
                    let mut crc = 0;
                    for _ in 0..BYTES / (64 * 1024) {
                        crc = crc32c::crc32c_append(crc, black_box(&payload[..64 * 1024]));
                    }
                    black_box(crc);
                }
            } else {
                for _ in 0..BYTES / BLOCK {
                    match case {
                        1 => {
                            black_box(Sha256::digest(black_box(payload)));
                        }
                        2 => {
                            black_box(
                                shard::decode_frame(&header, fp, 0, black_box(&frame))?.source_hash,
                            );
                        }
                        3 => {
                            let verified = shard::decode_frame(&header, fp, 0, black_box(&frame))?;
                            let hash: [u8; 32] = Sha256::digest(black_box(verified.payload)).into();
                            if black_box(hash) != verified.source_hash {
                                return Err("profile source hash mismatch".into());
                            }
                        }
                        4 => {
                            black_box(ring::digest::digest(
                                &ring::digest::SHA256,
                                black_box(payload),
                            ));
                        }
                        6 => {
                            black_box(
                                shard::decode_frame(&hybrid, fp, 0, black_box(&hybrid_frame))?
                                    .source_hash,
                            );
                        }
                        7 => {
                            black_box(hybrid.source_hash(fp, 0, black_box(payload))?);
                        }
                        8 => {
                            let verified =
                                shard::decode_frame(&hybrid, fp, 0, black_box(&hybrid_frame))?;
                            if black_box(hybrid.source_hash(fp, 0, verified.payload)?)
                                != verified.source_hash
                            {
                                return Err("hybrid profile source hash mismatch".into());
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
            let wall = start.elapsed().as_secs_f64();
            let after = process(std::process::id())?;
            rows.push(json!({
                "round": round+1,
                "case": names[case],
                "source_bytes": BYTES,
                "wall_seconds": wall,
                "cpu_seconds": after["cpu_seconds"].as_f64().unwrap_or(0.0)-before["cpu_seconds"].as_f64().unwrap_or(0.0),
                "source_mib_per_second": BYTES as f64 / 1048576.0 / wall
            }));
        }
    }
    private_json(
        output,
        &json!({
            "passed": true,
            "scope": "three rotating-order rounds; hot in-memory buffer; excludes storage, transport, metadata lookup and EC reconstruction; comparison of checksum work, not full-pipeline causal attribution",
            "block_bytes": BLOCK,
            "source_bytes_per_case": BYTES,
            "cases": rows
        }),
    )?;
    println!("Rust checksum CPU profile: {}", output.display());
    Ok(())
}
