//! One-way, resumable snapshot replication. SSH supplies network authentication
//! and encryption; the repository independently authenticates every stored block.
use super::format::{BLOCK_BYTES, Kind, MAX_RECORD_BYTES, Snapshot};
use super::repository::Repository;
use super::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Read, Write};

#[derive(Serialize, Deserialize)]
#[serde(tag = "message", deny_unknown_fields)]
enum Message {
    Begin { snapshot: Snapshot, key_id: String },
    Offer { hash: String },
    Data { length: usize },
    Finish,
    Ready,
    Have,
    Send,
    Complete,
}

fn write_message(out: &mut impl Write, message: &Message) -> Result<()> {
    let bytes = serde_json::to_vec(message)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(invalid("replication control frame exceeds limit"));
    }
    out.write_all(&bytes)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}

fn read_message(input: &mut impl BufRead) -> Result<Message> {
    let mut bytes = Vec::new();
    input
        .take(MAX_RECORD_BYTES as u64 + 2)
        .read_until(b'\n', &mut bytes)?;
    if bytes.len() > MAX_RECORD_BYTES + 1 || bytes.last() != Some(&b'\n') {
        return Err(invalid("truncated/oversized replication frame"));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn send(
    repo: &Repository,
    snapshot: &Snapshot,
    input: &mut impl BufRead,
    out: &mut impl Write,
) -> Result<()> {
    write_message(
        out,
        &Message::Begin {
            snapshot: snapshot.clone(),
            key_id: repo.key.id.clone(),
        },
    )?;
    match read_message(input)? {
        Message::Complete => return Ok(()),
        Message::Ready => {}
        _ => return Err(invalid("replication receiver rejected start")),
    }
    let mut offer = |hash: &str| -> Result<()> {
        write_message(
            out,
            &Message::Offer {
                hash: hash.to_owned(),
            },
        )?;
        match read_message(input)? {
            Message::Have => {}
            Message::Send => {
                let bytes = repo.get(hash)?;
                write_message(
                    out,
                    &Message::Data {
                        length: bytes.len(),
                    },
                )?;
                out.write_all(&bytes)?;
                out.flush()?;
                if !matches!(read_message(input)?, Message::Have) {
                    return Err(invalid("replication block was not acknowledged"));
                }
            }
            _ => return Err(invalid("invalid replication block response")),
        }
        Ok(())
    };
    for hash in &snapshot.manifest {
        offer(hash)?;
    }
    repo.records(snapshot, |record| {
        if let Kind::File { blocks, .. } = record.kind {
            for hash in blocks {
                offer(&hash)?;
            }
        }
        Ok(())
    })?;
    write_message(out, &Message::Finish)?;
    if !matches!(read_message(input)?, Message::Complete) {
        return Err(invalid("replication snapshot was not committed"));
    }
    Ok(())
}

pub fn receive(repo: &Repository, input: &mut impl BufRead, out: &mut impl Write) -> Result<()> {
    repo.ensure_writable()?;
    let Message::Begin { snapshot, key_id } = read_message(input)? else {
        return Err(invalid("expected replication start"));
    };
    super::fs::safe_id(&snapshot.id)?;
    if key_id != repo.key.id
        || snapshot.format != super::format::FORMAT
        || snapshot.engine_format != super::format::ENGINE_FORMAT
    {
        return Err(invalid("replication format/key mismatch"));
    }
    super::capture::validate_roots(&snapshot.roots)?;
    if repo
        .path
        .join("snapshots")
        .join(&snapshot.id)
        .try_exists()?
    {
        let existing = repo.snapshot(&snapshot.id)?;
        if serde_json::to_vec(&existing)? != serde_json::to_vec(&snapshot)? {
            return Err(invalid("snapshot identity collision"));
        }
        super::restore::verify(repo, &existing)?;
        // A previous receiver may have died after rename but before syncing
        // its parents. Re-establish that barrier before a retry is acknowledged.
        repo.sync_publication(&existing.id)?;
        write_message(out, &Message::Complete)?;
        return Ok(());
    }
    // A failed attempt never occupies the published snapshot identity. Retrying
    // reuses authenticated CAS blocks, not a trusted transfer progress counter.
    let pending = repo.pending(&ulid::Ulid::new().to_string())?;
    write_message(out, &Message::Ready)?;
    loop {
        match read_message(input)? {
            Message::Offer { hash } => {
                let path = repo.block_path(&hash)?;
                if path.try_exists()? {
                    repo.get(&hash)?;
                    repo.pin(&hash, &pending)?;
                    write_message(out, &Message::Have)?;
                } else {
                    write_message(out, &Message::Send)?;
                    let Message::Data { length } = read_message(input)? else {
                        return Err(invalid("expected replication data"));
                    };
                    if length > BLOCK_BYTES {
                        return Err(invalid("replication block exceeds limit"));
                    }
                    let mut bytes = vec![0; length];
                    input.read_exact(&mut bytes)?;
                    if super::crypto::digest(&bytes) != hash {
                        return Err(invalid("replication checksum mismatch"));
                    }
                    repo.put(&bytes, &pending)?;
                    write_message(out, &Message::Have)?;
                }
            }
            Message::Finish => {
                super::restore::verify(repo, &snapshot)?;
                for hash in &snapshot.manifest {
                    repo.pin(hash, &pending)?;
                }
                repo.records(&snapshot, |record| {
                    if let Kind::File { blocks, .. } = record.kind {
                        for hash in blocks {
                            repo.pin(&hash, &pending)?;
                        }
                    }
                    Ok(())
                })?;
                repo.publish(&snapshot, &pending)?;
                write_message(out, &Message::Complete)?;
                return Ok(());
            }
            _ => return Err(invalid("unexpected replication frame")),
        }
    }
}

pub fn local(source: &Repository, snapshot: &Snapshot, target: &Repository) -> Result<()> {
    use std::os::unix::net::UnixStream;
    let (a, b) = UnixStream::pair()?;
    std::thread::scope(|scope| -> Result<()> {
        let receiver = scope.spawn(move || {
            let result = receive(target, &mut std::io::BufReader::new(&b), &mut &b);
            let _ = b.shutdown(std::net::Shutdown::Both);
            result
        });
        let result = send(source, snapshot, &mut std::io::BufReader::new(&a), &mut &a);
        let _ = a.shutdown(std::net::Shutdown::Both);
        let received = receiver
            .join()
            .map_err(|_| invalid("replication receiver panicked"))?;
        result.and(received)
    })
}
