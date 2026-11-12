//! Four distinct filesystems, real shards and the public native API. Faults
//! detach only private member directories; no shared mount or cable is touched.
use super::*;
use std::collections::HashSet;

struct Acknowledged {
    key: String,
    bytes: Vec<u8>,
    shard_sha256: Vec<String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit N1_WITNESS_FOUR_ROOTS on four different physical filesystems"]
async fn physical_four_member_loss_corruption_and_replacement_matrix() -> Result<()> {
    let paths =
        std::env::var_os("N1_WITNESS_FOUR_ROOTS").ok_or("N1_WITNESS_FOUR_ROOTS is required")?;
    let parents = std::env::split_paths(&paths)
        .map(fs::canonicalize)
        .collect::<std::io::Result<Vec<_>>>()?;
    if parents.len() != 4 {
        return Err("exactly four explicit filesystem roots are required".into());
    }
    let devices = parents
        .iter()
        .map(|path| path.metadata().map(|metadata| metadata.dev()))
        .collect::<std::io::Result<HashSet<_>>>()?;
    if devices.len() != 4 {
        return Err("four different filesystem devices are required".into());
    }
    for (data, parity) in [(3, 1), (2, 2)] {
        let id = format!("n1-four-member-{data}-{parity}-{}", ulid::Ulid::new());
        let directories = parents.iter().map(|p| p.join(&id)).collect::<Vec<_>>();
        for path in &directories {
            fs::DirBuilder::new().mode(0o700).create(path)?;
        }
        let result = profile(&directories, data, parity).await;
        if let Err(error) = &result {
            private_json(
                &directories[0].join("FAILED.json"),
                &json!({"error":error.to_string(),"data":data,"parity":parity}),
            )?;
        }
        result?;
        eprintln!("four-member evidence: {}", directories[0].display());
    }
    Ok(())
}

async fn verify_all(client: &mut N1NativeClient, objects: &[Acknowledged]) -> Result<()> {
    for object in objects {
        verify(client, object.key.clone(), &object.bytes).await?;
    }
    Ok(())
}

fn shard_paths(config: &Config, key: &str) -> Result<Vec<PathBuf>> {
    let fingerprint = ObjectKey::inspect(key)?.fingerprint();
    let pool = &config.pools[0];
    (0..4)
        .map(|shard| {
            Ok(pool.roots[pool.layout.member_slot(fingerprint, shard)?]
                .join(fingerprint.shard_path(shard)))
        })
        .collect()
}

fn write_damage(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes)?;
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

async fn profile(directories: &[PathBuf], data: u16, parity: u16) -> Result<()> {
    let evidence = &directories[0];
    let path = evidence.join("n1.json");
    let credentials = path.with_extension("credentials.json");
    let roots = directories
        .iter()
        .map(|p| p.join("member"))
        .collect::<Vec<_>>();
    let mut init_command = Process::new(&*ENGINE);
    init_command.args(["init", "--config"]).arg(&path);
    for root in &roots {
        fs::create_dir(root)?;
        init_command.arg("--member").arg(root);
    }
    let output = init_command
        .args([
            "--data-shards",
            &data.to_string(),
            "--parity-shards",
            &parity.to_string(),
            "--block-bytes",
            "65536",
            "--native-address",
        ])
        .arg(free_port()?)
        .output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    let mut config = Config::load(&path)?;
    config.rate_limits.enabled = false;
    config.read_repair = false;
    fs::write(&path, serde_json::to_vec_pretty(&config)?)?;
    let log = evidence.join("engine.log");
    let mut engine = Engine::spawn(&path, &log)?;
    let mut client = connect(&credentials, &mut engine).await?;
    let mut objects = Vec::new();
    for (index, length) in [0, 1, 65535, 65536, 65537, 196625, 2097169]
        .into_iter()
        .enumerate()
    {
        let bytes = (0..length)
            .map(|n| ((n * 19 + index * 31) % 251) as u8)
            .collect::<Vec<_>>();
        let stripe_bytes = u64::from(data) * 65536;
        let part_bytes = (4_u64 * 1024 * 1024).div_ceil(stripe_bytes) * stripe_bytes;
        let upload = init(&mut client, bytes.len() as u64, part_bytes).await?;
        let key = text(&upload, "object_key")?;
        client
            .put(
                key.clone(),
                bytes.len() as u64,
                &mut std::io::Cursor::new(&bytes),
            )
            .await?;
        let shard_sha256 = shard_paths(&config, &key)?
            .iter()
            .map(|path| Ok(hex::encode(Sha256::digest(fs::read(path)?))))
            .collect::<Result<Vec<_>>>()?;
        objects.push(Acknowledged {
            key,
            bytes,
            shard_sha256,
        });
    }
    let acknowledgements = objects
        .iter()
        .map(|object| {
            json!({
                "key": object.key,
                "bytes": object.bytes.len(),
                "sha256": hex::encode(Sha256::digest(&object.bytes)),
                "shard_sha256": object.shard_sha256
            })
        })
        .collect::<Vec<_>>();
    private_json(&evidence.join("ACKNOWLEDGED.json"), &acknowledgements)?;
    verify_all(&mut client, &objects).await?;

    let mut losses = Vec::new();
    // Every member combination through the first loss count beyond parity.
    // Beyond parity, a cold server must refuse enrollment rather than serve
    // from an unreadable pool. Live API refusal is checked with frame faults.
    for mask in 1u8..15 {
        let lost = mask.count_ones();
        if lost > u32::from(parity) + 1 {
            continue;
        }
        drop(client);
        engine.stop(libc::SIGKILL)?;
        for (index, root) in roots.iter().enumerate() {
            if mask & (1 << index) != 0 {
                fs::rename(root, root.with_extension(format!("detached-{mask}")))?;
            }
        }
        engine = Engine::spawn(&path, &log)?;
        if lost <= u32::from(parity) {
            client = connect(&credentials, &mut engine).await?;
            verify_all(&mut client, &objects).await?;
            drop(client);
            engine.stop(libc::SIGKILL)?;
        } else {
            if connect(&credentials, &mut engine).await.is_ok() {
                return Err(format!("unreadable pool accepted startup: mask={mask}").into());
            }
            let status = engine
                .child
                .try_wait()?
                .ok_or("unreadable pool did not exit")?;
            let output = fs::read_to_string(&log)?;
            if status.code() != Some(1)
                || output.lines().last() != Some("N1: too few enrolled members to read this pool")
            {
                return Err(format!("unexpected startup failure: {status}").into());
            }
        }
        for (index, root) in roots.iter().enumerate() {
            if mask & (1 << index) != 0 && root.exists() {
                return Err("read/startup silently recreated a missing member".into());
            }
        }
        for (index, root) in roots.iter().enumerate() {
            if mask & (1 << index) != 0 {
                fs::rename(root.with_extension(format!("detached-{mask}")), root)?;
            }
        }
        engine = Engine::spawn(&path, &log)?;
        client = connect(&credentials, &mut engine).await?;
        losses.push(json!({"mask":mask,"lost":lost,"recoverable":lost<=u32::from(parity),"startup_refused":lost>u32::from(parity)}));
    }

    // Corrupt payload frames, with intact headers. Reconstruct and explicitly
    // repair every shard, then refuse both GET and repair beyond parity.
    let object = objects.last().unwrap();
    let paths = shard_paths(&config, &object.key)?;
    let original = paths
        .iter()
        .map(fs::read)
        .collect::<std::io::Result<Vec<_>>>()?;
    let fingerprint = ObjectKey::inspect(&object.key)?.fingerprint();
    let damaged = original
        .iter()
        .enumerate()
        .map(|(shard, bytes)| -> Result<Vec<u8>> {
            let header = n1_link::shard::ShardHeader::decode(
                &bytes[..128],
                &config.pools[0].layout,
                fingerprint,
                shard as u16,
            )?;
            let mut damaged = bytes.clone();
            damaged[header.frame_offset(0)? as usize + 64] ^= 0x80;
            Ok(damaged)
        })
        .collect::<Result<Vec<_>>>()?;
    for shard in 0..4 {
        write_damage(&paths[shard], &damaged[shard])?;
        verify(&mut client, object.key.clone(), &object.bytes).await?;
        client
            .control(Command::ObjectRepair {
                object_key: object.key.clone(),
            })
            .await?;
        if fs::read(&paths[shard])? != original[shard] {
            return Err("explicit repair differs from the original immutable shard".into());
        }
    }
    for shard in 0..=usize::from(parity) {
        write_damage(&paths[shard], &damaged[shard])?;
    }
    let mut returned = Vec::new();
    if client
        .download(object.key.clone(), 0, None, &mut returned)
        .await
        .is_ok()
    {
        return Err("GET incorrectly succeeded beyond checksum redundancy".into());
    }
    client = connect(&credentials, &mut engine).await?;
    if client
        .control(Command::ObjectRepair {
            object_key: object.key.clone(),
        })
        .await
        .is_ok()
    {
        return Err("repair incorrectly succeeded beyond checksum redundancy".into());
    }
    for shard in 0..=usize::from(parity) {
        if fs::read(&paths[shard])? != damaged[shard] {
            return Err("failed repair rewrote corrupted source evidence".into());
        }
        write_damage(&paths[shard], &original[shard])?;
    }
    client = connect(&credentials, &mut engine).await?;
    verify_all(&mut client, &objects).await?;

    // Blank replacements retain logical member IDs, not filesystem inodes or
    // drive hardware identities. Replace up to the pool's full parity budget.
    drop(client);
    engine.stop(libc::SIGKILL)?;
    let mut markers = Vec::new();
    for root in roots.iter().take(usize::from(parity)) {
        markers.push(fs::read(root.join(".n1-arc-pool.json"))?);
        fs::rename(root, root.with_extension("replaced-original"))?;
    }
    for (slot, directory) in directories.iter().enumerate().take(usize::from(parity)) {
        // All losses and all blank replacement disks are present together.
        // Each command must enroll only its explicitly authorized member.
        let replacement = directory.join("replacement");
        fs::create_dir(&replacement)?;
        config.pools[0].roots[slot] = replacement;
    }
    fs::write(&path, serde_json::to_vec_pretty(&config)?)?;
    let configured = serde_json::to_value(&config)?;
    for (slot, marker) in markers.iter().enumerate() {
        offline(&[
            "pool".into(),
            "replace".into(),
            "--config".into(),
            path.display().to_string(),
            "--pool".into(),
            configured["pools"][0]["layout"]["id"]
                .as_str()
                .unwrap()
                .into(),
            "--member".into(),
            slot.to_string(),
            "--authorize".into(),
        ])?;
        if fs::read(config.pools[0].roots[slot].join(".n1-arc-pool.json"))? != *marker {
            return Err("authorized replacement changed immutable member identity".into());
        }
        for root in config.pools[0]
            .roots
            .iter()
            .take(usize::from(parity))
            .skip(slot + 1)
        {
            if fs::read_dir(root)?.next().is_some() {
                return Err("replacement wrote to another unauthorized blank member".into());
            }
        }
    }
    engine = Engine::spawn(&path, &log)?;
    client = connect(&credentials, &mut engine).await?;
    for object in &objects {
        client
            .control(Command::ObjectRepair {
                object_key: object.key.clone(),
            })
            .await?;
        for (shard, path) in shard_paths(&config, &object.key)?.iter().enumerate() {
            if hex::encode(Sha256::digest(fs::read(path)?)) != object.shard_sha256[shard] {
                return Err("replacement shard differs from the pre-fault client oracle".into());
            }
        }
    }
    verify_all(&mut client, &objects).await?;
    drop(client);
    engine.stop(libc::SIGKILL)?;
    engine = Engine::spawn(&path, &log)?;
    client = connect(&credentials, &mut engine).await?;
    verify_all(&mut client, &objects).await?;
    private_json(
        &evidence.join("PASSED.json"),
        &json!({
            "data_shards": data,
            "parity_shards": parity,
            "objects": objects.len(),
            "member_loss_cases": losses,
            "frame_corruption_and_exact_repair_cases": 4,
            "beyond_parity_corruption_refused": true,
            "simultaneous_blank_replacements": parity,
            "final_process_restart_full_readback": true,
            "fault_method": "private member directories detached; physical cables and host power unchanged",
            "member_paths": config.pools[0].roots
        }),
    )?;
    drop(client);
    engine.stop(libc::SIGTERM)?;
    Ok(())
}
