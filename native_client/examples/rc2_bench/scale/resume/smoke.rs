//! Kill an owned client/server process group, prove acknowledged data is never
//! rewritten by resume, then recover all identities on the two real HDDs.
use super::*;
use std::{
    os::unix::{fs::FileExt, process::CommandExt},
    process::Child,
};

struct Group(Child);
impl Group {
    fn interrupt(&mut self) -> Result<()> {
        if self.0.try_wait()?.is_some() {
            return Err("smoke client finished before interruption".into());
        }
        // This unreaped child is the group leader created by process_group(0).
        if unsafe { libc::kill(-(self.0.id() as i32), libc::SIGKILL) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        self.0.wait()?;
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            let mut active = false;
            for entry in fs::read_dir("/proc")? {
                let entry = entry?;
                if let Ok(stat) = fs::read_to_string(entry.path().join("stat"))
                    && let Some(end) = stat.rfind(')')
                {
                    let fields = stat[end + 1..].split_whitespace().collect::<Vec<_>>();
                    active |= fields.first().is_some_and(|state| *state != "Z")
                        && fields.get(2).and_then(|group| group.parse::<u32>().ok())
                            == Some(self.0.id());
                }
            }
            if !active {
                break;
            }
            if Instant::now() >= deadline {
                return Err("owned smoke process group did not drain".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }
}
impl Drop for Group {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_none()) {
            let _ = self.interrupt();
        }
    }
}
fn latest(root: &Path) -> Result<PathBuf> {
    let mut paths = fs::read_dir(root)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("resume-"))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    paths.sort();
    paths.pop().ok_or_else(|| "no resume output".into())
}
async fn command(executable: &Path, engine: &Path, root: &Path, log: &Path) -> Result<bool> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(log)?;
    let mut child = Owned(
        Process::new(executable)
            .arg("--scale-resume")
            .arg(engine)
            .arg(root)
            .stdin(Stdio::null())
            .stdout(file.try_clone()?)
            .stderr(file)
            .spawn()?,
    );
    Ok(wait(&mut child.0, 240).await?.success())
}

pub(super) async fn self_test(args: Vec<String>) -> Result<()> {
    if cfg!(debug_assertions) || args.len() != 5 {
        return Err("release --scale-test-resume engine PROJECT-parent TEST-parent".into());
    }
    let engine = PathBuf::from(&args[2]).canonicalize()?;
    let project = PathBuf::from(&args[3]).canonicalize()?;
    let test = PathBuf::from(&args[4]).canonicalize()?;
    if project != Path::new("/mnt/PROJECT-DISK-1TB")
        || test != Path::new("/media/altear/TEST-1TB/mx-tests")
        || project.metadata()?.dev() == test.metadata()?.dev()
    {
        return Err("resume self-test requires the two selected HDDs".into());
    }
    let report = project.join(format!(
        "n1-scale-resume-witness-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::DirBuilder::new().mode(0o700).create(&report)?;
    let executable = std::env::current_exe()?;
    let log_path = report.join("interrupted-client.log");
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&log_path)?;
    let mut client = Group(
        Process::new(&executable)
            .arg("--scale-smoke")
            .arg(&engine)
            .arg(&project)
            .arg(&test)
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(file.try_clone()?)
            .stderr(file)
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(120);
    let fixture = loop {
        if client.0.try_wait()?.is_some() {
            return Err("smoke exited before an acknowledged checkpoint".into());
        }
        let output = fs::read_to_string(&log_path)?;
        if let Some(path) = output
            .lines()
            .find_map(|line| line.strip_prefix("headless scale evidence: "))
        {
            let root = PathBuf::from(path);
            if root.parent() != Some(project.as_path())
                || !root.file_name().is_some_and(|v| {
                    v.to_string_lossy()
                        .starts_with(&format!("n1-scale-4099-{}-", client.0.id()))
                })
            {
                return Err("unexpected smoke fixture path".into());
            }
            if (0..WORKERS).any(|worker| {
                root.join(format!("acknowledged-{worker}.jsonl"))
                    .metadata()
                    .is_ok_and(|m| m.len() > 0)
            }) {
                break root;
            }
        }
        if Instant::now() > deadline {
            return Err("smoke acknowledgement deadline exceeded".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    client.interrupt()?;
    println!(
        "resume witness interrupted owned processes: {}",
        fixture.display()
    );
    let mut originals = Vec::new();
    let mut acknowledged = None;
    for worker in 0..WORKERS {
        let path = fixture.join(format!("acknowledged-{worker}.jsonl"));
        originals.push((
            path.clone(),
            format!("{:x}", Sha256::digest(fs::read(&path)?)),
        ));
        let mut rows = journals::Records::new(BufReader::new(File::open(path)?));
        if let Some(entry) = rows.next()? {
            acknowledged.get_or_insert(entry);
        }
    }
    let acknowledged = acknowledged.ok_or("no acknowledged object to protect")?;
    let configuration: Value = serde_json::from_reader(File::open(fixture.join("service.json"))?)?;
    let layout: n1_link::PoolLayout =
        serde_json::from_value(configuration["pools"][0]["layout"].clone())?;
    let roots: Vec<PathBuf> = serde_json::from_value(configuration["pools"][0]["roots"].clone())?;
    let fp = n1_link::ObjectKey::inspect(&acknowledged.key)?.fingerprint();
    let mut damaged = Vec::new();
    for shard in 0..layout.width() {
        let path = roots[layout.member_slot(fp, shard as u16)?].join(fp.shard_path(shard as u16));
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut byte = [0];
        file.read_exact_at(&mut byte, 0)?;
        file.write_all_at(&[byte[0] ^ 1], 0)?;
        file.sync_all()?;
        damaged.push((
            path.clone(),
            byte,
            file.metadata()?.ino(),
            format!("{:x}", Sha256::digest(fs::read(path)?)),
        ));
    }
    if command(
        &executable,
        &engine,
        &fixture,
        &report.join("expected-corruption-rejection.log"),
    )
    .await?
    {
        return Err("resume hid damage to an acknowledged object".into());
    }
    let rejected = latest(&fixture)?;
    if !rejected.join("upload-result.json").is_file() || !rejected.join("FAILED.json").is_file() {
        return Err("expected rejection did not occur in full GET verification".into());
    }
    for (path, byte, inode, hash) in damaged {
        if fs::metadata(&path)?.ino() != inode
            || format!("{:x}", Sha256::digest(fs::read(&path)?)) != hash
        {
            return Err("resume rewrote deliberately damaged acknowledged data".into());
        }
        let file = OpenOptions::new().write(true).open(path)?;
        file.write_all_at(&byte, 0)?;
        file.sync_all()?;
    }
    if !command(
        &executable,
        &engine,
        &fixture,
        &report.join("successful-resume.log"),
    )
    .await?
    {
        return Err("repeated resumed smoke failed".into());
    }
    let passed = latest(&fixture)?;
    let result: Value = serde_json::from_reader(File::open(passed.join("PASSED.json"))?)?;
    if result["durably_acknowledged"] != 4099 || result["verified_after_sigkill"] != 4099 {
        return Err("smoke final identity count differs".into());
    }
    for (path, hash) in originals {
        if format!("{:x}", Sha256::digest(fs::read(path)?)) != hash {
            return Err("original journal was modified".into());
        }
    }
    private_json(
        &report.join("PASSED.json"),
        &json!({
            "passed":true,"objects":4099,"client_and_server_process_group_sigkill":true,
            "acknowledged_corruption_was_rejected_without_reupload_or_inode_change":true,
            "injected_header_bytes_restored_exactly":true,"resume_of_interrupted_resume":true,
            "all_gets_verified_after_sigkill":true,"original_ack_journals_unchanged":true,
            "fixture":fixture,"rejected_verification":rejected,"successful_verification":passed
        }),
    )?;
    println!("physical resume witness PASS: {}", report.display());
    Ok(())
}
