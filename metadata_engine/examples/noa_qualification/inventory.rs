//! Offline qualification-only census. Production metadata never opens members.
use super::{Result, setup::Config};
use m1::{MetadataEngine, Options, RecordKey, Table};
use n1_link::{Fingerprint, KeySigner, PoolId, PoolLayout};
use serde_json::{Value, json};
use std::{collections::HashSet, path::Path};
pub(super) fn census(config: &Config, alias: &str, mode: &str, count: u64) -> Result<Value> {
    let start = std::time::Instant::now();
    let db = MetadataEngine::open(
        config.root.join("metadata/control/metadata"),
        Options::default(),
    )?;
    let view = db.snapshot();
    let global = PoolId([0; 16]);
    let id: PoolId = serde_json::from_slice(
        &view
            .get(
                Table::Reclamation,
                &RecordKey::new(global, format!("alias/{alias}"))?,
            )?
            .ok_or("metadata fragment alias disappeared")?,
    )?;
    let registry: Value = serde_json::from_slice(&std::fs::read(
        config.root.join("object/control/pool-enrollment.json"),
    )?)?;
    let pool: PoolLayout =
        serde_json::from_value(registry["pools"]["A"]["config"]["layout"].clone())?;
    let scope: PoolId = serde_json::from_value(registry["principal"].clone())?;
    let key: [u8; 32] = std::fs::read(config.root.join("object/control/link.signing-key"))?
        .try_into()
        .map_err(|_| "invalid private signing key length")?;
    let signer = KeySigner::new(key);
    let mut fingerprints = HashSet::with_capacity(count as usize);
    let mut versions = 0;
    let mut after = None;
    loop {
        let rows = view.scan(Table::Versions, id, "", after.as_ref(), 1000)?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|r| r.0.clone());
        for (record, bytes) in rows {
            if record.identifier().starts_with('@') {
                continue;
            }
            let version: Value = serde_json::from_slice(&bytes)?;
            let object_key = version["object_key"]
                .as_str()
                .ok_or("immutable version has no LINK reference")?;
            let identity = signer.verify_object(&scope.0, object_key)?;
            if !pool.accepts(identity) {
                return Err("metadata points outside its qualified immutable pool".into());
            }
            if !fingerprints.insert(identity.fingerprint()) {
                return Err("distinct version rows share one unexpected LINK identity".into());
            }
            versions += 1;
        }
    }
    let mut entries = 0;
    let prefix = format!("qual/{mode}/");
    let mut after = None;
    loop {
        let rows = view.scan(Table::Entries, id, &prefix, after.as_ref(), 1000)?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|r| r.0.clone());
        for (key, bytes) in rows {
            let entry: Value = serde_json::from_slice(&bytes)?;
            if entry["head"].is_null() {
                return Err("qualification left an unpublished logical entry".into());
            }
            let index = key
                .identifier()
                .strip_prefix(&prefix)
                .and_then(|s| s.strip_suffix(".bin"))
                .ok_or("foreign logical path in census")?
                .parse::<u64>()?;
            if index >= count {
                return Err("census logical index exceeds campaign".into());
            }
            entries += 1;
        }
    }
    if entries != count {
        return Err(format!("metadata census found {entries} entries instead of {count}").into());
    }
    if !view
        .scan(Table::Reclamation, id, "pending/", None, 1)?
        .is_empty()
    {
        return Err("completed campaign retains unfinished publication intents".into());
    }
    let roots = [config.first.clone(), config.second.join("member1")];
    let mut physical = 0u64;
    let mut staged = 0;
    fn walk(
        root: &Path,
        path: &Path,
        slot: usize,
        pool: &PoolLayout,
        known: &HashSet<Fingerprint>,
        physical: &mut u64,
        staged: &mut u64,
    ) -> Result<()> {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            let path = entry.path();
            if kind.is_symlink() {
                return Err("unexpected member symlink in qualification census".into());
            }
            if kind.is_dir() {
                walk(root, &path, slot, pool, known, physical, staged)?;
                continue;
            }
            if !kind.is_file() {
                return Err("unexpected nonregular member entry".into());
            }
            let relative = path.strip_prefix(root)?;
            if relative == Path::new(".n1-arc-pool.json") {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_str().ok_or("non-UTF8 qualified shard filename")?;
            if name.ends_with(".shard") {
                let (fp, suffix) = name.split_once(".s").ok_or("invalid shard filename")?;
                let shard = suffix
                    .strip_suffix(".shard")
                    .ok_or("invalid shard suffix")?
                    .parse::<u16>()?;
                let raw: [u8; 32] = hex::decode(fp)?
                    .try_into()
                    .map_err(|_| "invalid fingerprint length")?;
                let fp = Fingerprint(raw);
                if !known.contains(&fp) {
                    return Err("unreferenced physical shard found".into());
                }
                if pool.member_slot(fp, shard)? != slot
                    || relative != Path::new(&fp.shard_path(shard))
                {
                    return Err("shard does not occupy its calculated member/path".into());
                }
                *physical += 1;
            } else if relative.starts_with(".uploads") {
                *staged += 1;
            } else {
                return Err("unexpected non-shard member file".into());
            }
        }
        Ok(())
    }
    for (slot, root) in roots.iter().enumerate() {
        walk(
            root,
            root,
            slot,
            &pool,
            &fingerprints,
            &mut physical,
            &mut staged,
        )?;
    }
    if physical != versions * pool.width() as u64 || staged != 0 {
        return Err(format!("census expected {} shards and no staging, found {physical} shards / {staged} staging files",versions*pool.width()as u64).into());
    }
    Ok(json!({
        "named_objects": entries,
        "immutable_versions": versions,
        "unique_LINK_identities": fingerprints.len(),
        "physical_shards": physical,
        "unreferenced_shards": 0,
        "wrong_placements": 0,
        "staged_files": staged,
        "pending_publications": 0,
        "seconds": start.elapsed().as_secs_f64()
    }))
}
