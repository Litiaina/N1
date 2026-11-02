//! Post-shutdown two-HDD census: prove exactly one correctly placed shard per
//! member for every distinct client identity, with no unreferenced files or
//! leftover staging. This is not part of foreground performance measurements.
use n1_link::{Fingerprint, ObjectKey, PoolLayout};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Deserialize)]
struct Record {
    index: u64,
    key: String,
    bytes: u64,
}
struct Expected {
    bytes: u64,
    members: u8,
}
#[derive(Default)]
struct Counts {
    files: u64,
    logical: u64,
    allocated: u64,
    directories: u64,
}
const SIZES: [u64; 8] = [0, 1, 127, 1024, 4096, 8193, 32768, 65536];
fn hex2(name: &str) -> bool {
    name.len() == 2
        && name
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
}
fn decode(name: &str) -> Result<([u8; 32], u16)> {
    let (fp, shard) = if let Some(fp) = name.strip_suffix(".s0.shard") {
        (fp, 0)
    } else if let Some(fp) = name.strip_suffix(".s1.shard") {
        (fp, 1)
    } else {
        return Err("unexpected final filename".into());
    };
    if fp.len() != 64
        || !fp
            .bytes()
            .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value))
    {
        return Err("noncanonical fingerprint filename".into());
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&fp[index * 2..index * 2 + 2], 16)?;
    }
    Ok((bytes, shard))
}
fn census(
    root: &Path,
    slot: usize,
    layout: &PoolLayout,
    objects: &mut HashMap<[u8; 32], Expected>,
) -> Result<Counts> {
    let mut count = Counts::default();
    for first in fs::read_dir(root)? {
        let first = first?;
        let name = first
            .file_name()
            .into_string()
            .map_err(|_| "non-UTF8 root entry")?;
        let kind = first.file_type()?;
        if name == ".n1-arc-pool.json" {
            if !kind.is_file() {
                return Err("nonregular member marker".into());
            }
            continue;
        }
        if name == ".uploads" {
            if !kind.is_dir() || fs::read_dir(first.path())?.next().is_some() {
                return Err("completed campaign has leftover staging entries".into());
            }
            continue;
        }
        if !kind.is_dir() || !hex2(&name) {
            return Err("unexpected member-root entry".into());
        }
        count.directories += 1;
        for second in fs::read_dir(first.path())? {
            let second = second?;
            let leaf = second
                .file_name()
                .into_string()
                .map_err(|_| "non-UTF8 fanout entry")?;
            if !second.file_type()?.is_dir() || !hex2(&leaf) {
                return Err("unexpected fanout entry".into());
            }
            count.directories += 1;
            for entry in fs::read_dir(second.path())? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    return Err("nonregular shard entry".into());
                }
                let file = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| "non-UTF8 shard filename")?;
                let (fp, shard) = decode(&file)?;
                if file[..2] != name
                    || file[2..4] != leaf
                    || layout.member_slot(Fingerprint(fp), shard)? != slot
                {
                    return Err("shard does not match calculated fanout/member placement".into());
                }
                let expected = objects
                    .get_mut(&fp)
                    .ok_or("unreferenced object shard on disk")?;
                if expected.members & (1 << slot) != 0 {
                    return Err("duplicate member shard for one identity".into());
                }
                let metadata = entry.metadata()?;
                let length = if expected.bytes == 0 {
                    224
                } else {
                    128 + 96 + u64::from(layout.block_bytes) + 96
                };
                if metadata.len() != length || metadata.nlink() != 1 {
                    return Err("unexpected shard length or hard link".into());
                }
                expected.members |= 1 << slot;
                count.files += 1;
                count.logical += metadata.len();
                count.allocated += metadata.blocks() * 512;
            }
        }
    }
    Ok(count)
}
fn run() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(
            "scale_inventory original-run completed-resume-directory new-report.json".into(),
        );
    }
    let start = Instant::now();
    let root = PathBuf::from(&args[1]).canonicalize()?;
    let completed = PathBuf::from(&args[2]).canonicalize()?;
    if root.parent() != Some(Path::new("/mnt/PROJECT-DISK-1TB"))
        || completed.parent() != Some(root.as_path())
    {
        return Err("inventory paths are outside the isolated PROJECT HDD fixture".into());
    }
    let passed: Value = serde_json::from_reader(File::open(completed.join("PASSED.json"))?)?;
    if passed["passed"] != true {
        return Err("full GET verification has not passed".into());
    }
    let total = passed["verified_after_sigkill"]
        .as_u64()
        .ok_or("missing verified count")?;
    if total != 500000 {
        return Err("inventory requires the requested 500,000-object case".into());
    }
    let config: Value = serde_json::from_reader(File::open(root.join("service.json"))?)?;
    let layout: PoolLayout = serde_json::from_value(config["pools"][0]["layout"].clone())?;
    let roots: Vec<PathBuf> = serde_json::from_value(config["pools"][0]["roots"].clone())?;
    if layout.data_shards != 1
        || layout.parity_shards != 1
        || layout.block_bytes != 65536
        || roots
            != [
                root.join("member-0"),
                Path::new("/media/altear/TEST-1TB/mx-tests")
                    .join(root.file_name().ok_or("invalid fixture name")?)
                    .join("member-1"),
            ]
        || roots[0].metadata()?.dev() == roots[1].metadata()?.dev()
    {
        return Err("unexpected physical pool".into());
    }
    let mut objects = HashMap::with_capacity(total as usize);
    let mut source_bytes = 0;
    for worker in 0..4 {
        for (ordinal, line) in BufReader::new(File::open(
            completed.join(format!("acknowledged-{worker}.jsonl")),
        )?)
        .lines()
        .enumerate()
        {
            let row: Record = serde_json::from_str(&line?)?;
            let ordinal = ordinal as u64;
            let index = ordinal / 256 * 1024 + worker * 256 + ordinal % 256;
            if row.index != index || index >= total || row.bytes != SIZES[index as usize % 8] {
                return Err("noncanonical client manifest coverage".into());
            }
            let identity = ObjectKey::inspect(&row.key)?;
            if !layout.accepts(identity)
                || objects
                    .insert(
                        identity.fingerprint().0,
                        Expected {
                            bytes: row.bytes,
                            members: 0,
                        },
                    )
                    .is_some()
            {
                return Err("duplicate identity or foreign pool in client manifest".into());
            }
            source_bytes += row.bytes;
        }
    }
    if objects.len() as u64 != total {
        return Err("client manifest population differs from requested count".into());
    }
    let mut members = Vec::new();
    let mut logical = 0;
    let mut allocated = 0;
    for (slot, root) in roots.iter().enumerate() {
        let count = census(root, slot, &layout, &mut objects)?;
        if count.files != total {
            return Err("member shard count differs from object count".into());
        }
        logical += count.logical;
        allocated += count.allocated;
        members.push(json!({"slot":slot,"root":root,"device":root.metadata()?.dev(),"shards":count.files,
            "logical_shard_bytes":count.logical,"allocated_data_file_bytes":count.allocated,"fanout_directories":count.directories,"staging_entries":0}));
    }
    if objects.values().any(|value| value.members != 3) {
        return Err("some client identities lack one of their two shards".into());
    }
    let output = Path::new(&args[3]);
    if !output
        .parent()
        .ok_or("missing report parent")?
        .canonicalize()?
        .starts_with("/mnt/PROJECT-DISK-1TB")
    {
        return Err("report must be on PROJECT HDD".into());
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(output)?;
    serde_json::to_writer_pretty(
        &mut file,
        &json!({"passed":true,"distinct_client_identities":total,"physical_shards":2*total,
        "every_client_identity_has_both_correctly_placed_shards":true,"unreferenced_shards":0,"staging_entries":0,
        "source_bytes":source_bytes,"logical_shard_bytes":logical,"allocated_data_file_bytes":allocated,
        "logical_amplification":logical as f64/source_bytes as f64,"allocated_file_amplification":allocated as f64/source_bytes as f64,
        "allocation_scope":"data-file st_blocks only; excludes directory/inode/journal/filesystem overhead",
        "seconds":start.elapsed().as_secs_f64(),"members":members,"scope":"post-shutdown metadata census; full payload GET verification is recorded separately"}),
    )?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    println!(
        "physical inventory PASS: {total} distinct objects; {} correctly placed shards",
        2 * total
    );
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("physical inventory failed: {error}");
        std::process::exit(1);
    }
}
