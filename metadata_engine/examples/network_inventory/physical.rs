use super::super::{Result, Value, bounds, private_path};
use n1_link::{Fingerprint, Identity, PoolLayout};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs,
    io::BufRead,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::Instant,
};

pub fn run(
    registry: &Path,
    members: &[PathBuf],
    references: &Path,
    expected_hash: &str,
    count: u64,
) -> Result<Value> {
    bounds(count)?;
    private_path(registry, false)?;
    private_path(references, false)?;
    let started = Instant::now();
    let registry: Value = serde_json::from_slice(&fs::read(registry)?)?;
    let pool: PoolLayout =
        serde_json::from_value(registry["pools"]["A"]["config"]["layout"].clone())?;
    if members.len() != pool.width() {
        return Err("physical member count differs from immutable pool".into());
    }
    let mut devices = HashSet::new();
    for member in members {
        private_path(member, true)?;
        if !devices.insert(fs::metadata(member)?.dev()) {
            return Err("physical members share a filesystem".into());
        }
    }
    let mut known = HashSet::with_capacity(count as usize);
    let mut hash = Sha256::new();
    for line in std::io::BufReader::new(fs::File::open(references)?).lines() {
        let line = line?;
        if line.len() != n1_link::IDENTITY_BYTES * 2 {
            return Err("invalid reference length".into());
        }
        hash.update(line.as_bytes());
        hash.update(b"\n");
        let identity = Identity::decode(&hex::decode(line)?)?;
        if !pool.accepts(identity) || !known.insert(identity.fingerprint()) {
            return Err("foreign or duplicate physical reference".into());
        }
    }
    if known.len() as u64 != count || hex::encode(hash.finalize()) != expected_hash {
        return Err("reference count or digest differs".into());
    }
    let mut physical = 0u64;
    let mut staged = 0u64;
    for (slot, root) in members.iter().enumerate() {
        walk(root, root, slot, &pool, &known, &mut physical, &mut staged)?;
    }
    if physical != count * pool.width() as u64 || staged != 0 {
        return Err("wrong physical count or staged files remain".into());
    }
    Ok(
        json!({"known_objects":count,"physical_shards":physical,"staged_files":staged,
        "wrong_placements":0,"unreferenced_shards":0,"independent_filesystems":devices.len(),
        "payloads_verified_by_this_census":false,"seconds":started.elapsed().as_secs_f64()}),
    )
}

fn walk(
    root: &Path,
    path: &Path,
    slot: usize,
    pool: &PoolLayout,
    known: &HashSet<Fingerprint>,
    physical: &mut u64,
    staged: &mut u64,
) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = entry.path();
        if kind.is_symlink() {
            return Err("member contains a symlink".into());
        }
        if kind.is_dir() {
            walk(root, &path, slot, pool, known, physical, staged)?;
            continue;
        }
        if !kind.is_file() {
            return Err("member contains a nonregular file".into());
        }
        let relative = path.strip_prefix(root)?;
        if relative == Path::new(".n1-arc-pool.json") {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or("non-UTF8 member file")?;
        if name.ends_with(".shard") {
            let (fp, suffix) = name.split_once(".s").ok_or("invalid shard filename")?;
            let shard = suffix
                .strip_suffix(".shard")
                .ok_or("invalid shard suffix")?
                .parse::<u16>()?;
            let fp: Fingerprint = fp.parse()?;
            if !known.contains(&fp)
                || pool.member_slot(fp, shard)? != slot
                || relative != Path::new(&fp.shard_path(shard))
            {
                return Err("unreferenced shard or incorrect calculated placement".into());
            }
            *physical += 1;
        } else if relative.starts_with(".uploads") {
            *staged += 1;
        } else {
            return Err("unexpected non-shard member record".into());
        }
    }
    Ok(())
}
