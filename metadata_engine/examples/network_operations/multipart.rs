use super::api::{Api, Result, expect, publish, string};
use serde_json::{Value, json};
use std::path::Path;

fn body(case: usize, index: u64, part_bytes: usize) -> Vec<u8> {
    let length = if index == 5 { 17 } else { part_bytes };
    (0..length)
        .map(|offset| ((offset as u64 * 17 + index * 31 + case as u64 * 73) % 251) as u8)
        .collect()
}

fn route(api: &Api, durable: bool) -> String {
    format!(
        "{}/upload/multipart{}",
        api.base,
        if durable { "/durable" } else { "" }
    )
}

async fn status(api: &Api, url: &str) -> Result<Value> {
    expect(
        api.client.get(url).bearer_auth(&api.token).send().await?,
        200,
    )
    .await
}

/// Explicit operator cleanup also accepts a prior harness's incomplete
/// fixtures. Payload verification still requires the identical frozen client.
pub async fn cancel(api: &Api, root: &Path) -> Result<Value> {
    let state: Value = serde_json::from_slice(&std::fs::read(root.join("multipart-state.json"))?)?;
    if state["campaign"]["endpoint_sha256"] != api.campaign["endpoint_sha256"] {
        return Err("refusing to cancel fixtures belonging to another endpoint".into());
    }
    let cases = state["cases"].as_array().ok_or("missing multipart cases")?;
    for saved in cases {
        let base = route(api, saved["durable"].as_bool().ok_or("missing durability")?);
        let version = string(&saved["handle"], "version_id")?;
        expect(
            api.client
                .delete(format!("{base}/{version}"))
                .bearer_auth(&api.token)
                .send()
                .await?,
            200,
        )
        .await?;
        if api
            .client
            .get(format!("{base}/{version}"))
            .bearer_auth(&api.token)
            .send()
            .await?
            .status()
            != 410
        {
            return Err("cancelled multipart session did not return gone".into());
        }
    }
    Ok(json!({"cancelled":cases.len(),"status_after_cancel":410}))
}

pub async fn begin(api: &Api, root: &Path) -> Result<Value> {
    let path = root.join("multipart-state.json");
    if path.exists() {
        return Err("multipart state already exists; refusing to replace ACKs".into());
    }
    let mut cases = Vec::new();
    let identity = ulid::Ulid::new();
    for (case, (durable, part_bytes)) in [(false, 65536usize), (true, 8192usize)]
        .into_iter()
        .enumerate()
    {
        let base = route(api, durable);
        let key = format!("split-resume/{identity}/{case}.bin");
        let shape = json!({"total_size_bytes":part_bytes * 5 + 17,
            "part_size_bytes":part_bytes,"expected_parts":6,"content_type":"application/octet-stream"});
        let handle = expect(
            api.client
                .post(format!("{base}/init/{key}?durability=strict"))
                .bearer_auth(&api.token)
                .json(&shape)
                .send()
                .await?,
            200,
        )
        .await?;
        let version = string(&handle, "version_id")?;
        for index in [0, 2, 5, 0] {
            expect(
                api.client
                    .put(format!("{base}/{version}/{index}"))
                    .bearer_auth(&api.token)
                    .body(body(case, index, part_bytes))
                    .send()
                    .await?,
                200,
            )
            .await?;
        }
        let conflict = api
            .client
            .put(format!("{base}/{version}/0"))
            .bearer_auth(&api.token)
            .body(vec![99; part_bytes])
            .send()
            .await?;
        if conflict.status() != 409 {
            return Err("conflicting part retry was not refused".into());
        }
        let before = api.submitted().await?;
        let progress = status(api, &format!("{base}/{version}")).await?;
        if progress["uploaded_parts"] != json!([0, 2, 5]) || api.submitted().await? != before {
            return Err("initial progress is incorrect or scanned object payloads".into());
        }
        cases.push(
            json!({"case":case,"durable":durable,"part_bytes":part_bytes,
            "key":key,"shape":shape,"handle":handle,"acknowledged_parts":[0,2,5]}),
        );
    }
    let state = json!({"campaign":api.campaign,"cases":cases});
    publish(&path, &state)?;
    Ok(json!({"cases":2,"acknowledged_parts_per_case":[0,2,5],
        "same_part_retry_passed":true,"conflicting_retry_refused":true,"state":path}))
}

pub async fn complete(api: &Api, root: &Path) -> Result<Value> {
    let state: Value = serde_json::from_slice(&std::fs::read(root.join("multipart-state.json"))?)?;
    if state["campaign"] != api.campaign {
        return Err("multipart endpoint or frozen harness changed".into());
    }
    let mut results = Vec::new();
    for saved in state["cases"].as_array().ok_or("missing multipart cases")? {
        let case = saved["case"].as_u64().ok_or("missing case")? as usize;
        let part_bytes = saved["part_bytes"].as_u64().ok_or("missing part size")? as usize;
        let durable = saved["durable"].as_bool().ok_or("missing durability")?;
        let base = route(api, durable);
        let version = string(&saved["handle"], "version_id")?;
        let session = string(&saved["handle"], "session_id")?;
        let key = string(saved, "key")?;
        let before = api.submitted().await?;
        let progress = status(api, &format!("{base}/{version}")).await?;
        if progress["uploaded_parts"] != saved["acknowledged_parts"]
            || api.submitted().await? != before
        {
            return Err("cold progress lost ACKs or scanned object payloads".into());
        }
        let reused = expect(
            api.client
                .post(format!("{base}/init/{key}?durability=strict"))
                .bearer_auth(&api.token)
                .json(&saved["shape"])
                .send()
                .await?,
            200,
        )
        .await?;
        if reused != saved["handle"] {
            return Err("matching init changed multipart identity".into());
        }
        for index in [1, 3, 4] {
            expect(
                api.client
                    .put(format!("{base}/{version}/{index}"))
                    .bearer_auth(&api.token)
                    .body(body(case, index, part_bytes))
                    .send()
                    .await?,
                200,
            )
            .await?;
        }
        let mut published = None;
        for _ in 0..2 {
            let response = expect(
                api.client
                    .post(format!("{base}/{version}/{session}/finalize"))
                    .bearer_auth(&api.token)
                    .json(&json!({}))
                    .send()
                    .await?,
                200,
            )
            .await?;
            if response["version_id"] != version {
                return Err("finalize changed version identity".into());
            }
            if let Some(previous) = &published
                && *previous != response
            {
                return Err("repeated finalize changed publication".into());
            }
            published = Some(response);
        }
        let response = api
            .client
            .get(format!("{}/objects/get/{key}", api.base))
            .bearer_auth(&api.token)
            .send()
            .await?;
        if response.status() != 200 {
            return Err("finalized object cannot be read".into());
        }
        if response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim_matches('"'))
            != Some(version)
        {
            return Err("finalized object version differs".into());
        }
        let expected: Vec<_> = (0..6)
            .flat_map(|index| body(case, index, part_bytes))
            .collect();
        if response.bytes().await?.as_ref() != expected {
            return Err("resumed object payload differs".into());
        }
        results.push(
            json!({"case":case,"durable":durable,"verified_bytes":expected.len(),
            "cold_progress_submitted_payload_io":false,"finalize_retry_identical":true}),
        );
    }
    Ok(json!({"cases":results}))
}
