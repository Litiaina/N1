use super::workload::{rss_hwm_kib, verify};
use crate::{Event, Phase, Plan, Result, open, save};
use serde_json::json;
use std::{
    fs,
    io::{BufRead, Write},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn population(acks: &[bool], plan: &Plan, phase: Phase) -> usize {
    acks.iter()
        .enumerate()
        .filter(|(_, ack)| **ack)
        .map(|(batch, _)| (phase.count(plan) - batch * plan.batch).min(plan.batch))
        .sum()
}

fn run_child(root: &Path, plan: &Plan, phase: Phase, interrupt: bool) -> Result<serde_json::Value> {
    let name = format!(
        "{}-{}",
        phase.name(),
        if interrupt { "interrupted" } else { "resumed" }
    );
    let ack_path = root.join(format!("{}-acks.json", phase.name()));
    let mut acks: Vec<bool> = if ack_path.exists() {
        serde_json::from_slice(&fs::read(&ack_path)?)?
    } else {
        vec![false; phase.count(plan).div_ceil(plan.batch)]
    };
    let prior = population(&acks, plan, phase);
    let mut journal = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(format!("{name}.jsonl")))?;
    let stderr = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(format!("{name}.stderr")))?;
    let mut child = ChildGuard(
        Command::new(std::env::current_exe()?)
            .arg("worker")
            .arg("--root")
            .arg(root)
            .arg("--phase")
            .arg(phase.name())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .spawn()?,
    );
    let output = child.0.stdout.take().ok_or("child stdout missing")?;
    let (send, receive) = mpsc::sync_channel(64);
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(output).lines() {
            if send.send(line).is_err() {
                return;
            }
        }
    });
    let started = Instant::now();
    let mut killed = false;
    let mut done = false;
    let mut verified_prior = 0;
    let mut created = 0;
    let mut peak_kib = 0;
    let mut last_sample = Instant::now();
    let mut latencies = Vec::new();
    let mut checks = 0;
    let mut received_records = 0;
    loop {
        let line = match receive.recv_timeout(Duration::from_secs(120)) {
            Ok(line) => line?,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(format!("no progress in {name} for 120 seconds").into());
            }
        };
        let event: Event = serde_json::from_str(&line)?;
        journal.write_all(line.as_bytes())?;
        journal.write_all(b"\n")?;
        match event {
            Event::Verified { records, .. } => {
                if records != prior {
                    return Err("child did not verify every prior acknowledgement".into());
                }
                verified_prior = records;
            }
            Event::Ack {
                batch,
                records,
                created: batch_created,
                micros,
            } => {
                if batch >= acks.len() || acks[batch] {
                    return Err("duplicate/out-of-range acknowledgement".into());
                }
                let expected = (phase.count(plan) - batch * plan.batch).min(plan.batch);
                if records != expected || batch_created > records {
                    return Err("invalid acknowledged batch size".into());
                }
                acks[batch] = true;
                received_records += records;
                created += batch_created;
                if batch_created > 0 {
                    latencies.push(micros);
                }
                if last_sample.elapsed() >= Duration::from_millis(100) {
                    peak_kib = peak_kib.max(rss_hwm_kib(child.0.id()).unwrap_or_default());
                    last_sample = Instant::now();
                }
                if received_records.is_multiple_of(plan.batch * 128) {
                    eprintln!("{name}: {} acknowledged records", prior + received_records);
                }
                if interrupt && !killed && received_records >= phase.count(plan) / 2 {
                    peak_kib = peak_kib.max(rss_hwm_kib(child.0.id()).unwrap_or_default());
                    child.0.kill()?;
                    killed = true;
                }
            }
            Event::Done {
                reader_checks,
                rss_hwm_kib,
            } => {
                done = true;
                checks = reader_checks;
                peak_kib = peak_kib.max(rss_hwm_kib.unwrap_or_default());
            }
        }
    }
    reader
        .join()
        .map_err(|_| "acknowledgement reader panicked")?;
    let status = child.0.wait()?;
    if interrupt {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if !killed || status.signal() != Some(9) {
                return Err("child did not stop through the injected SIGKILL".into());
            }
        }
    } else if !status.success() || !done || acks.iter().any(|ack| !ack) {
        return Err(format!("{name} incomplete, child status {status}").into());
    }
    journal.sync_all()?;
    save(&ack_path, &acks)?;
    latencies.sort_unstable();
    let percentile = |percent: usize| {
        latencies
            .get((latencies.len().saturating_sub(1) * percent) / 100)
            .copied()
            .unwrap_or_default() as f64
            / 1000.0
    };
    let result = json!({
        "phase":name,"seconds":started.elapsed().as_secs_f64(),"sigkill_injected":killed,
        "acknowledged_records":population(&acks,plan,phase),"prior_acknowledged_records_verified_without_writing":verified_prior,
        "newly_acknowledged_records":received_records,"records_actually_mutated":created,
        "concurrent_reader_checks_for_completed_child":if done {Some(checks)} else {None},
        "child_rss_hwm_kib_observed":peak_kib,
        "durable_batch_latency_ms":{"samples":latencies.len(),"p50":percentile(50),"p95":percentile(95),"p99":percentile(99),"max":latencies.last().copied().unwrap_or_default() as f64 /1000.0},
        "latency_scope":"whole bounded metadata transaction, includes contention and fsync; not single-object upload latency"
    });
    save(&root.join(format!("{name}.result.json")), &result)?;
    Ok(result)
}

pub fn run(root: &Path, plan: Plan) -> Result<()> {
    if !root.is_absolute()
        || plan.entries < 64
        || plan.entries > 2_000_000
        || plan.writers == 0
        || plan.writers > 32
        || plan.readers > 32
        || plan.batch == 0
        || plan.batch > 256
        || plan.churn < 64
        || plan.churn > plan.entries
    {
        return Err("invalid qualification limits".into());
    }
    fs::create_dir(root)?;
    save(&root.join("plan.json"), &plan)?;
    let result = (|| -> Result<serde_json::Value> {
        drop(open(root)?);
        let interrupted = run_child(root, &plan, Phase::Populate, true)?;
        let resumed = run_child(root, &plan, Phase::Populate, false)?;
        let population = verify(root, &plan, false)?;
        save(&root.join("population-verified.json"), &population)?;
        let churn_interrupted = run_child(root, &plan, Phase::Churn, true)?;
        let churn_resumed = run_child(root, &plan, Phase::Churn, false)?;
        let final_state = verify(root, &plan, true)?;
        // Reopen once more after every verification handle has been released.
        drop(open(root)?);
        Ok(
            json!({"passed":true,"scope":"metadata storage foundation; no NOA namespace service or object uploads", "plan":plan,
            "population_interrupted":interrupted,"population_resumed":resumed,"population_verified":population,
            "churn_interrupted":churn_interrupted,"churn_resumed":churn_resumed,"final_verified":final_state}),
        )
    })();
    match result {
        Ok(result) => {
            save(&root.join("PASSED.json"), &result)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        Err(error) => {
            save(
                &root.join("FAILED.json"),
                &json!({"passed":false,"error":error.to_string()}),
            )?;
            Err(error)
        }
    }
}
