//! Sequential small-object stage latency; each sample is verified and purged.
use super::*;

pub(super) async fn run(
    api: &Api,
    payload: &Payload,
    pids: &Pids,
    output: &Path,
    count: usize,
) -> Result<()> {
    for durable in [false, true] {
        let mut client = api.connect().await?;
        let mut rows = Vec::with_capacity(count);
        let mut init = Vec::new();
        let mut part = Vec::new();
        let mut finalize = Vec::new();
        let mut again = Vec::new();
        let mut get = Vec::new();
        let mut purge = Vec::new();
        let data = payload.part.slice(..4096);
        let phase = Measure::start(pids)?;
        for index in 0..count {
            let started = Instant::now();
            let plan = cli_core::result(
                client
                    .control(Command::UploadInit {
                        pool: None,
                        total_bytes: data.len() as u64,
                        part_bytes: PART,
                        durable,
                        ttl_seconds: 3600,
                    })
                    .await?,
            )?;
            init.push(started.elapsed().as_secs_f64() * 1000.0);
            let key = plan["object_key"]
                .as_str()
                .ok_or("missing latency key")?
                .to_owned();
            let started = Instant::now();
            client
                .write_at(key.clone(), 0, data.len() as u64, &mut &data[..])
                .await?;
            part.push(started.elapsed().as_secs_f64() * 1000.0);
            let started = Instant::now();
            let published = cli_core::result(
                client
                    .control(Command::UploadFinalize {
                        object_key: key.clone(),
                    })
                    .await?,
            )?;
            finalize.push(started.elapsed().as_secs_f64() * 1000.0);
            if published["object_key"] != key || published["already_finalized"] != false {
                return Err("first latency finalize mismatch".into());
            }
            let started = Instant::now();
            let repeated = cli_core::result(
                client
                    .control(Command::UploadFinalize {
                        object_key: key.clone(),
                    })
                    .await?,
            )?;
            again.push(started.elapsed().as_secs_f64() * 1000.0);
            if repeated["object_key"] != key || repeated["already_finalized"] != true {
                return Err("repeated latency finalize mismatch".into());
            }
            let started = Instant::now();
            let mut downloaded = Vec::with_capacity(data.len());
            let (_, length) = client
                .download(key.clone(), 0, None, &mut downloaded)
                .await?;
            get.push(started.elapsed().as_secs_f64() * 1000.0);
            if length != data.len() as u64 || downloaded.as_slice() != data.as_ref() {
                return Err("latency GET mismatch".into());
            }
            let started = Instant::now();
            let removed = cli_core::result(
                client
                    .control(Command::ObjectDelete {
                        object_key: key.clone(),
                    })
                    .await?,
            )?;
            purge.push(started.elapsed().as_secs_f64() * 1000.0);
            if removed["removed_shards"] != payload.width {
                return Err("latency purge count mismatch".into());
            }
            match client
                .control(Command::ObjectStat {
                    object_key: key.clone(),
                })
                .await
            {
                Err(NativeClientError::Native { ref code, .. }) if code == "404" => (),
                _ => return Err("latency purge absence mismatch".into()),
            }
            rows.push(json!({"index":index,"object_key":key,"verified":true,"purged":true}));
            if (index + 1) % 32 == 0 {
                println!(
                    "{}",
                    json!({"phase":"latency","durable":durable,"verified":index+1,"total":count})
                );
            }
        }
        let resources = phase
            .finish(count as u64 * data.len() as u64, count)
            .await?;
        let upload = init
            .iter()
            .zip(&part)
            .zip(&finalize)
            .map(|((init, part), finalize)| init + part + finalize)
            .collect::<Vec<_>>();
        let upload_seconds = upload.iter().sum::<f64>() / 1000.0;
        write_json(
            &output.join(if durable {
                "latency-durable.json"
            } else {
                "latency-normal.json"
            }),
            &json!({
                "passed":true,"objects":count,"object_bytes":data.len(),"durable":durable,"resources":resources,
                "upload":distribution(upload),"upload_operations_per_second":count as f64 / upload_seconds,
                "init":distribution(init),"part":distribution(part),"finalize":distribution(finalize),
                "repeated_finalize":distribution(again),"verified_get":distribution(get),"purge":distribution(purge),"rows":rows
            }),
        )?;
    }
    write_json(
        &output.join("PASSED.json"),
        &json!({"passed":true,"verified_and_purged":count*2,"no_m1_requests":true}),
    )
}
