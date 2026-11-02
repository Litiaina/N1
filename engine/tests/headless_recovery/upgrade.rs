//! Execute the previously selected binary first, then open its unchanged pool
//! with the hybrid binary. This uses real on-disk bytes, not regenerated fixtures.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires two explicit HDD parents and N1_WITNESS_BASELINE_BINARY"]
async fn immutable_sha256_pool_survives_hybrid_binary_upgrade() -> Result<()> {
    let baseline = PathBuf::from(std::env::var("N1_WITNESS_BASELINE_BINARY")?).canonicalize()?;
    let baseline_sha = format!("{:x}", Sha256::digest(fs::read(&baseline)?));
    if baseline_sha != "252b92024f59f2d339fa4e5af14412fef25272594404ab3ec1e39dc60e4ffff2" {
        return Err("unexpected upgrade baseline binary".into());
    }
    let first = PathBuf::from(std::env::var("N1_WITNESS_PROJECT_ROOT")?).canonicalize()?;
    let second = PathBuf::from(std::env::var("N1_WITNESS_TEST_ROOT")?).canonicalize()?;
    if first.metadata()?.dev() == second.metadata()?.dev()
        || [first.metadata()?.dev(), second.metadata()?.dev()]
            .contains(&std::env::current_dir()?.metadata()?.dev())
    {
        return Err("upgrade requires two HDD filesystems outside the workspace".into());
    }
    let name = format!("n1-integrity-upgrade-{}", ulid::Ulid::new());
    let a = first.join(&name);
    let b = second.join(&name);
    for path in [&a, &b] {
        fs::DirBuilder::new().mode(0o700).create(path)?;
    }
    let result = execute(&baseline, &a, &b).await;
    if let Err(error) = &result {
        private_json(&a.join("failure.json"), &json!({"error":error.to_string()}))?;
    }
    result?;
    eprintln!("SHA-256 pool upgrade witness: {}", a.display());
    Ok(())
}

async fn execute(baseline: &Path, a: &Path, b: &Path) -> Result<()> {
    let config = a.join("service.json");
    let credentials = config.with_extension("credentials.json");
    let roots = [a.join("member-0"), b.join("member-1")];
    for root in &roots {
        fs::create_dir(root)?;
    }
    let enrollment = Process::new(baseline)
        .args(["init", "--config"])
        .arg(&config)
        .arg("--member")
        .arg(&roots[0])
        .arg("--member")
        .arg(&roots[1])
        .args([
            "--data-shards",
            "1",
            "--parity-shards",
            "1",
            "--block-bytes",
            "65536",
            "--native-address",
        ])
        .arg(free_port()?)
        .output()?;
    if !enrollment.status.success() {
        return Err(format!(
            "baseline enrollment failed: {}",
            String::from_utf8_lossy(&enrollment.stderr)
        )
        .into());
    }
    // Preserve the absent shard_format field: the baseline must see its own config.
    let mut configuration: Value = serde_json::from_reader(fs::File::open(&config)?)?;
    assert!(
        configuration["pools"][0]["layout"]
            .get("shard_format")
            .is_none()
    );
    configuration["rate_limits"]["enabled"] = json!(false);
    fs::write(&config, serde_json::to_vec_pretty(&configuration)?)?;
    let layout: n1_link::PoolLayout =
        serde_json::from_value(configuration["pools"][0]["layout"].clone())?;
    let mut server = Engine::spawn_binary(baseline, &config, &a.join("baseline.log"))?;
    let mut client = connect(&credentials, &mut server).await?;
    let data: Vec<u8> = (0..131073).map(|n| (n * 197 % 251) as u8).collect();
    let live = init(&mut client, 65549, 131072).await?;
    client
        .put(
            text(&live, "upload_key")?,
            65549,
            &mut std::io::Cursor::new(&data[..65549]),
        )
        .await?;
    let partial = init(&mut client, data.len() as u64, 65536).await?;
    let ticket = text(&partial, "upload_key")?;
    client
        .upload_part(
            ticket.clone(),
            0,
            65536,
            &mut std::io::Cursor::new(&data[..65536]),
        )
        .await?;
    let retired = init(&mut client, 1, 65536).await?;
    client
        .put(
            text(&retired, "upload_key")?,
            1,
            &mut std::io::Cursor::new([7]),
        )
        .await?;
    client
        .control(Command::ObjectDelete {
            object_key: text(&retired, "object_key")?,
        })
        .await?;
    let key = text(&live, "object_key")?;
    let fp = ObjectKey::inspect(&key)?.fingerprint();
    let mut original = Vec::new();
    for shard in 0..2u16 {
        let path = roots[layout.member_slot(fp, shard)?].join(fp.shard_path(shard));
        original.push((
            path.clone(),
            fs::metadata(&path)?.ino(),
            Sha256::digest(fs::read(path)?),
        ));
    }
    drop(client);
    server.stop(libc::SIGTERM)?;
    let mut server = Engine::spawn(&config, &a.join("upgraded.log"))?;
    let mut client = connect(&credentials, &mut server).await?;
    verify(&mut client, key.clone(), &data[..65549]).await?;
    let stat = value(
        client
            .control(Command::ObjectStat {
                object_key: key.clone(),
            })
            .await?,
    )?;
    assert_eq!(stat["integrity_algorithm"], "LINK-SHA256-tree-v1");
    for (path, inode, hash) in &original {
        assert_eq!(fs::metadata(path)?.ino(), *inode);
        assert_eq!(Sha256::digest(fs::read(path)?), *hash);
    }
    let status = value(
        client
            .control(Command::UploadStatus {
                upload_key: ticket.clone(),
                start: 0,
                limit: 4096,
            })
            .await?,
    )?;
    assert_eq!(status["missing_parts"], json!([1, 2]));
    for part in [2, 1] {
        let start = part * 65536;
        let end = data.len().min(start + 65536);
        client
            .upload_part(
                ticket.clone(),
                part as u64,
                (end - start) as u64,
                &mut std::io::Cursor::new(&data[start..end]),
            )
            .await?;
    }
    for _ in 0..2 {
        client
            .control(Command::UploadFinalize {
                upload_key: ticket.clone(),
            })
            .await?;
    }
    verify(&mut client, text(&partial, "object_key")?, &data).await?;
    drop(client);
    server.stop(libc::SIGKILL)?;
    let mut server = Engine::spawn(&config, &a.join("restarted.log"))?;
    let mut client = connect(&credentials, &mut server).await?;
    verify(&mut client, key, &data[..65549]).await?;
    verify(&mut client, text(&partial, "object_key")?, &data).await?;
    for command in [
        Command::ObjectStat {
            object_key: text(&retired, "object_key")?,
        },
        Command::UploadFinalize {
            upload_key: text(&retired, "upload_key")?,
        },
    ] {
        assert!(
            matches!(client.control(command).await,Err(n1_native_client::NativeClientError::Native{code,..}) if code=="410")
        );
    }
    drop(client);
    server.stop(libc::SIGTERM)?;
    private_json(
        &a.join("result.json"),
        &json!({
            "passed":true,"baseline_sha256":format!("{:x}",Sha256::digest(fs::read(baseline)?)),
            "candidate_sha256":format!("{:x}",Sha256::digest(fs::read(&*ENGINE)?)),
            "pool_format":"unchanged sha256-v1; absent configuration field",
            "unchanged_shard_hashes_and_inodes":true,"resumed_parts":[2,1],"finalize_twice":true,
            "acknowledged_objects_verified_after_sigkill":2,"permanent_retirement_verified_after_restart":true,
            "project_device":a.metadata()?.dev(),"test_device":b.metadata()?.dev()
        }),
    )?;
    Ok(())
}
