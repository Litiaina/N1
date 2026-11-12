use super::super::{Result, Value, bounds, create, private_path};
use m1::{MetadataEngine, Options, RecordKey, Table};
use n1_link::{ObjectKey, PoolId};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{BufRead, Write},
    path::Path,
    time::Instant,
};

#[derive(Deserialize)]
struct Ack {
    index: u64,
    version: String,
}

pub fn run(
    endpoint: &Path,
    data_dir: &Path,
    oracle: &Path,
    count: u64,
    references: &Path,
) -> Result<Value> {
    bounds(count)?;
    for (path, directory) in [(endpoint, false), (data_dir, true), (oracle, true)] {
        private_path(path, directory)?;
    }
    let started = Instant::now();
    let config: Value = serde_json::from_slice(&fs::read(endpoint)?)?;
    let label = format!(
        "N1 qualification {} api",
        config["identity"].as_str().ok_or("missing identity")?
    );
    let alias = hex::encode(Sha256::digest(label.as_bytes()));
    let mut acks = vec![None; count as usize];
    for entry in fs::read_dir(oracle)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("api-acks-") || !name.ends_with(".jsonl") {
            continue;
        }
        private_path(&entry.path(), false)?;
        for line in std::io::BufReader::new(fs::File::open(entry.path())?).lines() {
            for ack in serde_json::from_str::<Vec<Ack>>(&line?)? {
                if ack.index >= count {
                    return Err("ACK outside campaign".into());
                }
                let version = ulid::Ulid::from_string(&ack.version)?.to_bytes();
                let slot = &mut acks[ack.index as usize];
                if slot.is_some_and(|old| old != version) {
                    return Err("conflicting client ACKs".into());
                }
                *slot = Some(version);
            }
        }
    }
    if acks.iter().any(Option::is_none) {
        return Err("inventory requires every client ACK".into());
    }
    let db = MetadataEngine::open(data_dir, Options::default())?;
    let view = db.snapshot();
    let id: PoolId = serde_json::from_slice(
        &view
            .get(
                Table::Reclamation,
                &RecordKey::new(PoolId([0; 16]), format!("alias/{alias}"))?,
            )?
            .ok_or("fragment alias lost")?,
    )?;
    let mut objects = HashMap::with_capacity(count as usize);
    let mut indices = HashSet::with_capacity(count as usize);
    let mut after = None;
    loop {
        let rows = view.scan(Table::Entries, id, "qual/api/", after.as_ref(), 1000)?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|row| row.0.clone());
        for (key, bytes) in rows {
            let index = key
                .identifier()
                .strip_prefix("qual/api/")
                .and_then(|s| s.strip_suffix(".bin"))
                .ok_or("foreign entry in qualification prefix")?
                .parse::<u64>()?;
            if index >= count || !indices.insert(index) {
                return Err("duplicate or foreign logical index".into());
            }
            let entry: Value = serde_json::from_slice(&bytes)?;
            let version = ulid::Ulid::from_string(
                entry["head"]["version_id"]
                    .as_str()
                    .ok_or("entry has no head")?,
            )?;
            if acks[index as usize] != Some(version.to_bytes()) {
                return Err("metadata head differs from client ACK".into());
            }
            let object = ulid::Ulid::from_string(
                entry["object_id"]
                    .as_str()
                    .ok_or("missing object identity")?,
            )?;
            if objects.insert(object.to_bytes(), index).is_some() {
                return Err("logical entries unexpectedly share an object".into());
            }
        }
    }
    if objects.len() as u64 != count {
        return Err("metadata entry cardinality differs".into());
    }
    let mut output = std::io::BufWriter::new(create(references)?);
    let mut hash = Sha256::new();
    let mut fingerprints = HashSet::with_capacity(count as usize);
    let mut versions = 0u64;
    let mut after = None;
    loop {
        let rows = view.scan(Table::Versions, id, "", after.as_ref(), 1000)?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|row| row.0.clone());
        for (key, bytes) in rows {
            if key.identifier().starts_with('@') {
                continue;
            }
            let stored: Value = serde_json::from_slice(&bytes)?;
            let object = ulid::Ulid::from_string(
                stored["metadata"]["object_id"]
                    .as_str()
                    .ok_or("version has no object identity")?,
            )?;
            let Some(index) = objects.get(&object.to_bytes()) else {
                continue;
            };
            let version = ulid::Ulid::from_string(
                stored["metadata"]["version_id"]
                    .as_str()
                    .ok_or("version has no identity")?,
            )?;
            if acks[*index as usize] != Some(version.to_bytes()) {
                return Err("extra or changed immutable version in campaign".into());
            }
            let identity = ObjectKey::inspect(
                stored["object_key"]
                    .as_str()
                    .ok_or("version has no LINK reference")?,
            )?;
            if !fingerprints.insert(identity.fingerprint()) {
                return Err("duplicate LINK reference".into());
            }
            let line = format!("{}\n", hex::encode(identity.encode()));
            hash.update(line.as_bytes());
            output.write_all(line.as_bytes())?;
            versions += 1;
        }
    }
    if versions != count {
        return Err(format!("expected {count} immutable versions, found {versions}").into());
    }
    let mut pending_rows = Vec::new();
    let mut after = None;
    loop {
        let rows = view.scan(Table::Reclamation, id, "pending/", after.as_ref(), 1000)?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|row| row.0.clone());
        for (key, _) in rows {
            let version = key.identifier().strip_prefix("pending/").unwrap();
            let pending: Value = serde_json::from_slice(
                &view
                    .get(Table::Uploads, &RecordKey::new(id, version)?)?
                    .ok_or("pending publication has no upload owner")?,
            )?;
            let object = ulid::Ulid::from_string(
                pending["object_id"]
                    .as_str()
                    .ok_or("pending object ID missing")?,
            )?;
            let index = objects.get(&object.to_bytes()).copied();
            pending_rows.push(json!({
                "version_id":version,"completed":pending["completed"],
                "expires_at":pending["upload"]["expires_at"],"logical_index":index,
                "ack_version":index.and_then(|i| acks[i as usize]).map(|v| ulid::Ulid::from_bytes(v).to_string())
            }));
        }
    }
    if !pending_rows.is_empty() {
        let path = references.with_extension("pending.json");
        let mut file = create(&path)?;
        serde_json::to_writer_pretty(&mut file, &json!({"pending":pending_rows}))?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        return Err(format!(
            "all {versions} expected immutable versions present, but {} pending publications remain; diagnostics: {}",
            pending_rows.len(), path.display()
        ).into());
    }
    output.flush()?;
    output.get_ref().sync_all()?;
    fs::File::open(references.parent().ok_or("missing reference parent")?)?.sync_all()?;
    Ok(
        json!({"named_objects":objects.len(),"immutable_versions":versions,"unique_LINK_identities":fingerprints.len(),
        "pending_publications":0,"references_sha256":hex::encode(hash.finalize()),"references":references,
        "identity_tags_verified_by_this_census":false,"requires_full_public_readback":true,
        "seconds":started.elapsed().as_secs_f64()}),
    )
}
