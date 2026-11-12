use super::api::{Api, Result, expect};
use serde_json::{Value, json};
use std::time::Instant;

fn resources(pid: u32) -> Result<Value> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    let kib = |key: &str| -> Result<u64> {
        Ok(status
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .ok_or("missing process memory")?
            .split_whitespace()
            .next()
            .ok_or("missing memory value")?
            .parse()?)
    };
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .ok_or("invalid process stat")?
        .1
        .split_whitespace()
        .collect();
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    if ticks <= 0.0 || fields.len() < 13 {
        return Err("invalid process CPU counter".into());
    }
    Ok(json!({"pid":pid,"start_ticks":fields.get(19),
        "user_seconds":fields[11].parse::<u64>()? as f64/ticks,
        "system_seconds":fields[12].parse::<u64>()? as f64/ticks,
        "rss_kib":kib("VmRSS:")?,"peak_rss_kib":kib("VmHWM:")?}))
}

fn distribution(mut samples: Vec<f64>) -> Value {
    let order = samples.clone();
    samples.sort_by(f64::total_cmp);
    let percentile = |p: f64| samples[(samples.len() as f64 * p).ceil() as usize - 1];
    json!({"samples":samples.len(),"sum_ms":samples.iter().sum::<f64>(),
        "p50_ms":percentile(0.5),"p95_ms":percentile(0.95),"p99_ms":percentile(0.99),
        "completion_order_ms":order})
}

pub async fn run(api: &Api, label: &str, pid: u32) -> Result<Value> {
    let before = resources(pid)?;
    let phase = ulid::Ulid::new();
    let bytes: Vec<u8> = (0..4096).map(|i| ((i * 17 + 71) % 251) as u8).collect();
    let (mut uploads, mut reads, mut renames, mut trash) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let started = Instant::now();
    for index in 0..100 {
        let key = format!("network-bench/{phase}/original-{index:03}.bin");
        let renamed = format!("network-bench/{phase}/renamed-{index:03}.bin");
        let now = Instant::now();
        let published = expect(
            api.client
                .post(format!(
                    "{}/upload/one-shot/{key}?durability=strict",
                    api.base
                ))
                .bearer_auth(&api.token)
                .body(bytes.clone())
                .send()
                .await?,
            201,
        )
        .await?;
        uploads.push(now.elapsed().as_secs_f64() * 1000.0);
        let now = Instant::now();
        let response = api
            .client
            .get(format!("{}/objects/get/{key}", api.base))
            .bearer_auth(&api.token)
            .send()
            .await?;
        if response.status() != 200 {
            return Err("benchmark read failed".into());
        }
        if response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim_matches('"'))
            != published["version_id"].as_str()
            || response.bytes().await?.as_ref() != bytes
        {
            return Err("benchmark read changed version or bytes".into());
        }
        reads.push(now.elapsed().as_secs_f64() * 1000.0);
        let now = Instant::now();
        expect(
            api.client
                .post(format!("{}/objects/rename", api.base))
                .bearer_auth(&api.token)
                .json(&json!({"source_key":key,"destination_key":renamed}))
                .send()
                .await?,
            200,
        )
        .await?;
        renames.push(now.elapsed().as_secs_f64() * 1000.0);
        let now = Instant::now();
        expect(
            api.client
                .delete(format!("{}/objects/{renamed}", api.base))
                .bearer_auth(&api.token)
                .send()
                .await?,
            200,
        )
        .await?;
        trash.push(now.elapsed().as_secs_f64() * 1000.0);
    }
    let after = resources(pid)?;
    if before["start_ticks"] != after["start_ticks"] {
        return Err("M1 restarted during benchmark".into());
    }
    Ok(
        json!({"label":label,"phase_id":phase.to_string(),"objects":100,"bytes_per_object":4096,
        "strict_durability":true,"seconds":started.elapsed().as_secs_f64(),
        "small_upload":distribution(uploads),"small_get":distribution(reads),
        "rename":distribution(renames),"trash":distribution(trash),
        "metadata_before":before,"metadata_after":after,"campaign":api.campaign}),
    )
}
