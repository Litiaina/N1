use crate::{Event, Phase, Plan, Result, emit, open};
use n1_link::PoolId;
use n1_metadata_engine::{
    Error, MetadataEngine, MetadataSnapshot, MetadataTransaction, RecordKey, Table,
};
use std::{
    fs,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

const FRAGMENT: PoolId = PoolId([0x31; 16]);

trait ReadView {
    fn get(&self, table: Table, key: &RecordKey) -> n1_metadata_engine::Result<Option<Vec<u8>>>;
}

impl ReadView for MetadataTransaction {
    fn get(&self, table: Table, key: &RecordKey) -> n1_metadata_engine::Result<Option<Vec<u8>>> {
        MetadataTransaction::get(self, table, key)
    }
}

impl ReadView for MetadataSnapshot {
    fn get(&self, table: Table, key: &RecordKey) -> n1_metadata_engine::Result<Option<Vec<u8>>> {
        MetadataSnapshot::get(self, table, key)
    }
}

fn key(index: usize) -> RecordKey {
    RecordKey::new(FRAGMENT, format!("item/{index:010}")).expect("bounded fixture identifier")
}
fn history(index: usize, revision: u64) -> RecordKey {
    RecordKey::new(FRAGMENT, format!("history/{index:010}/{revision:04}"))
        .expect("bounded fixture identifier")
}

fn value(index: usize, revision: u64) -> Vec<u8> {
    let size = [128, 256, 512, 1024][index % 4];
    let mut value = vec![0; size];
    value[..8].copy_from_slice(&(index as u64).to_be_bytes());
    value[8..16].copy_from_slice(&revision.to_be_bytes());
    let mut state = (index as u64).wrapping_mul(0x9e3779b97f4a7c15) ^ revision ^ 0x514d_31a7;
    for byte in &mut value[16..] {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    value
}

fn check(
    actual: Option<Vec<u8>>,
    expected: Option<Vec<u8>>,
    index: usize,
    label: &str,
) -> n1_metadata_engine::Result<()> {
    if actual != expected {
        return Err(Error::Invalid(format!(
            "{label} differs for record {index}"
        )));
    }
    Ok(())
}

/// Returns true only for a completely applied batch. Mixed presence would mean
/// cross-keyspace or within-batch atomicity was lost; never overwrite that evidence.
fn applied(tx: &impl ReadView, phase: Phase, index: usize) -> n1_metadata_engine::Result<bool> {
    let current = tx.get(Table::Entries, &key(index))?;
    let original = tx.get(Table::Versions, &history(index, 0))?;
    if phase == Phase::Populate {
        return match (current, original) {
            (None, None) => Ok(false),
            (Some(current), Some(original))
                if current == value(index, 0) && current == original =>
            {
                Ok(true)
            }
            _ => Err(Error::Invalid(format!(
                "incomplete/corrupt population record {index}"
            ))),
        };
    }
    check(
        original,
        Some(value(index, 0)),
        index,
        "immutable original history",
    )?;
    let marker = tx.get(Table::Reclamation, &key(index))?;
    let updated = tx.get(Table::Versions, &history(index, 1))?;
    if current.as_deref() == Some(value(index, 0).as_slice())
        && marker.is_none()
        && updated.is_none()
    {
        return Ok(false);
    }
    if index.is_multiple_of(5) {
        check(current, None, index, "deleted entry")?;
        check(marker, Some(value(index, 1)), index, "deletion marker")?;
        check(updated, None, index, "unexpected update history")?;
    } else {
        check(current, Some(value(index, 1)), index, "updated entry")?;
        check(updated, Some(value(index, 1)), index, "updated history")?;
        check(marker, None, index, "unexpected deletion marker")?;
    }
    Ok(true)
}

fn apply_batch(
    engine: &MetadataEngine,
    plan: &Plan,
    phase: Phase,
    batch: usize,
) -> n1_metadata_engine::Result<usize> {
    let start = batch * plan.batch;
    let end = (start + plan.batch).min(phase.count(plan));
    engine.transact(64, |tx| {
        let complete = applied(tx, phase, start)?;
        for index in start + 1..end {
            if applied(tx, phase, index)? != complete {
                return Err(Error::Invalid(format!(
                    "partially applied atomic batch {batch}"
                )));
            }
        }
        if complete {
            return Ok(0);
        }
        for index in start..end {
            if phase == Phase::Populate {
                let bytes = value(index, 0);
                tx.put(Table::Entries, &key(index), &bytes)?;
                tx.put(Table::Versions, &history(index, 0), &bytes)?;
            } else if index.is_multiple_of(5) {
                tx.remove(Table::Entries, &key(index))?;
                tx.put(Table::Reclamation, &key(index), &value(index, 1))?;
            } else {
                let bytes = value(index, 1);
                tx.put(Table::Entries, &key(index), &bytes)?;
                tx.put(Table::Versions, &history(index, 1), &bytes)?;
            }
        }
        Ok(end - start)
    })
}

fn live_check(view: &MetadataSnapshot, phase: Phase, index: usize) -> Result<()> {
    let current = view.get(Table::Entries, &key(index))?;
    if let Some(current) = current {
        if current.len() < 16 {
            return Err("truncated concurrent entry".into());
        }
        let revision = u64::from_be_bytes(current[8..16].try_into()?);
        if revision > 1 || current != value(index, revision) {
            return Err("corrupt concurrent entry".into());
        }
        check(
            view.get(Table::Versions, &history(index, revision))?,
            Some(current),
            index,
            "snapshot history",
        )?;
    } else if phase == Phase::Churn {
        check(
            view.get(Table::Reclamation, &key(index))?,
            Some(value(index, 1)),
            index,
            "snapshot deletion marker",
        )?;
        check(
            view.get(Table::Versions, &history(index, 0))?,
            Some(value(index, 0)),
            index,
            "snapshot deleted history",
        )?;
    } else {
        check(
            view.get(Table::Versions, &history(index, 0))?,
            None,
            index,
            "snapshot unpublished history",
        )?;
    }
    Ok(())
}

pub fn rss_hwm_kib(pid: u32) -> Option<u64> {
    fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|value| value.split_whitespace().next()?.parse().ok())
        })
}

pub fn worker(root: &Path, phase: Phase) -> Result<()> {
    let plan: Plan = serde_json::from_slice(&fs::read(root.join("plan.json"))?)?;
    let ack_path = root.join(format!("{}-acks.json", phase.name()));
    let batches = phase.count(&plan).div_ceil(plan.batch);
    let acknowledged: Vec<bool> = if ack_path.exists() {
        serde_json::from_slice(&fs::read(ack_path)?)?
    } else {
        vec![false; batches]
    };
    if acknowledged.len() != batches {
        return Err("acknowledgement set has wrong length".into());
    }
    let engine = open(root)?;
    let started = Instant::now();
    let mut prior_records = 0;
    {
        let tx = engine.snapshot();
        for (batch, ack) in acknowledged.iter().enumerate() {
            if !ack {
                continue;
            }
            let start = batch * plan.batch;
            let end = (start + plan.batch).min(phase.count(&plan));
            for index in start..end {
                if !applied(&tx, phase, index)? {
                    return Err(format!("acknowledged record {index} vanished").into());
                }
            }
            prior_records += end - start;
        }
        // This is a verification-only transaction. Never commit or put an ACKed row.
    }
    emit(Event::Verified {
        records: prior_records,
        seconds: started.elapsed().as_secs_f64(),
    })?;
    let next = AtomicUsize::new(0);
    let stop = Arc::new(AtomicBool::new(false));
    let reader_checks = std::thread::scope(|scope| -> Result<u64> {
        let mut readers = Vec::new();
        for reader in 0..plan.readers {
            let engine = engine.clone();
            let stop = stop.clone();
            let entries = plan.entries;
            readers.push(scope.spawn(move || -> Result<u64> {
                let mut checks = 0;
                let mut state = 0x7748a21u64 + reader as u64;
                let result = (|| -> Result<u64> {
                    while !stop.load(Ordering::Acquire) {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        live_check(&engine.snapshot(), phase, state as usize % entries)?;
                        checks += 1;
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Ok(checks)
                })();
                if result.is_err() {
                    stop.store(true, Ordering::Release);
                }
                result
            }));
        }
        let mut writers = Vec::new();
        for _ in 0..plan.writers {
            let engine = engine.clone();
            let next = &next;
            let stop = &stop;
            let acknowledged = &acknowledged;
            let plan = &plan;
            writers.push(scope.spawn(move || -> Result<()> {
                while !stop.load(Ordering::Acquire) {
                    let batch = next.fetch_add(1, Ordering::Relaxed);
                    if batch >= batches {
                        break;
                    }
                    if acknowledged[batch] {
                        continue;
                    }
                    let started = Instant::now();
                    let created = apply_batch(&engine, plan, phase, batch)?;
                    let records = (phase.count(plan) - batch * plan.batch).min(plan.batch);
                    emit(Event::Ack {
                        batch,
                        records,
                        created,
                        micros: started.elapsed().as_micros().try_into()?,
                    })?;
                }
                Ok(())
            }));
        }
        let mut writer_error = None;
        for writer in writers {
            match writer.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    writer_error = Some(error);
                    stop.store(true, Ordering::Release);
                }
                Err(_) => {
                    writer_error = Some("metadata writer panicked".into());
                    stop.store(true, Ordering::Release);
                }
            }
        }
        stop.store(true, Ordering::Release);
        let mut total = 0;
        for reader in readers {
            total += reader.join().map_err(|_| "metadata reader panicked")??;
        }
        if let Some(error) = writer_error {
            return Err(error);
        }
        Ok(total)
    })?;
    emit(Event::Done {
        reader_checks,
        rss_hwm_kib: rss_hwm_kib(std::process::id()),
    })?;
    Ok(())
}

pub fn verify(root: &Path, plan: &Plan, churned: bool) -> Result<serde_json::Value> {
    let started = Instant::now();
    let engine = open(root)?;
    let view = engine.snapshot();
    for index in 0..plan.entries {
        let changed = churned && index < plan.churn;
        let deleted = changed && index.is_multiple_of(5);
        check(
            view.get(Table::Entries, &key(index))?,
            if deleted {
                None
            } else {
                Some(value(index, u64::from(changed)))
            },
            index,
            "full entry verification",
        )?;
        check(
            view.get(Table::Versions, &history(index, 0))?,
            Some(value(index, 0)),
            index,
            "full original history verification",
        )?;
        check(
            view.get(Table::Versions, &history(index, 1))?,
            if changed && !deleted {
                Some(value(index, 1))
            } else {
                None
            },
            index,
            "full update history verification",
        )?;
        check(
            view.get(Table::Reclamation, &key(index))?,
            if deleted { Some(value(index, 1)) } else { None },
            index,
            "full reclamation verification",
        )?;
        if index.is_multiple_of(100_000) {
            eprintln!("verified {index}/{} metadata entries", plan.entries);
        }
    }
    let deleted = if churned { plan.churn.div_ceil(5) } else { 0 };
    let updated = if churned { plan.churn - deleted } else { 0 };
    let mut counts = Vec::new();
    for (table, expected) in [
        (Table::Entries, plan.entries - deleted),
        (Table::Versions, plan.entries + updated),
        (Table::Reclamation, deleted),
        (Table::Uploads, 0),
    ] {
        let mut after = None;
        let mut count = 0;
        loop {
            let page = view.scan(table, FRAGMENT, "", after.as_ref(), 1000)?;
            if page.is_empty() {
                break;
            }
            count += page.len();
            after = Some(page.last().ok_or("empty page")?.0.clone());
        }
        if count != expected {
            return Err(format!("{table:?} count {count}, expected {expected}").into());
        }
        counts.push(serde_json::json!({"table":format!("{table:?}"),"records":count}));
    }
    Ok(
        serde_json::json!({"verified_entries":plan.entries,"churned":churned,"seconds":started.elapsed().as_secs_f64(),"counts":counts,"engine_disk_bytes":engine.disk_bytes()?}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_preserves_applied_batches_but_refuses_partial_cross_table_state() {
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path()).unwrap();
        let plan = Plan {
            entries: 64,
            batch: 32,
            writers: 2,
            readers: 1,
            churn: 32,
        };
        assert_eq!(apply_batch(&engine, &plan, Phase::Populate, 0).unwrap(), 32);
        assert_eq!(apply_batch(&engine, &plan, Phase::Populate, 0).unwrap(), 0);
        engine
            .transact(1, |tx| tx.remove(Table::Versions, &history(0, 0)))
            .unwrap();
        assert!(apply_batch(&engine, &plan, Phase::Populate, 0).is_err());
        assert!(
            engine
                .snapshot()
                .get(Table::Versions, &history(0, 0))
                .unwrap()
                .is_none()
        );
    }
}
