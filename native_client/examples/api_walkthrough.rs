//! Executable NOA v2 walkthrough linked from docs/api/N1.md.
//! Creates fresh objects and purges only the identities it creates.
use n1_native_client::{Command, N1NativeClient, NativeClientError, Response};
use serde_json::{Value, json};
use std::{env, error::Error, io};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn result(response: Response) -> Result<Value> {
    response
        .result
        .ok_or_else(|| io::Error::other("missing NOA result").into())
}

fn key(value: &Value) -> Result<String> {
    value["object_key"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("missing LINK key").into())
}

async fn begin(client: &mut N1NativeClient, total: u64, part: u64) -> Result<String> {
    key(&result(
        client
            .control(Command::UploadInit {
                pool: None,
                total_bytes: total,
                part_bytes: part,
                durable: true,
                ttl_seconds: 3600,
            })
            .await?,
    )?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = env::var("N1_URL")?;
    let fragment = env::var("N1_FRAGMENT")?;
    let secret = env::var("N1_SECRET")?;
    let mut client = N1NativeClient::connect(&url).await?;
    client.auth_fragment(&fragment, &secret).await?;

    let pools = result(client.control(Command::Pools).await?)?;
    let pool = pools["pools"]
        .as_array()
        .and_then(|pools| pools.iter().rev().find(|pool| pool["sealed"] == false))
        .ok_or_else(|| io::Error::other("no unsealed pool"))?;
    let part = pool["data_shards"]
        .as_u64()
        .zip(pool["block_bytes"].as_u64())
        .and_then(|(data, block)| data.checked_mul(block))
        .filter(|bytes| (8..=16 * 1024 * 1024).contains(bytes))
        .ok_or_else(|| io::Error::other("walkthrough requires a stripe of 8 bytes..16 MiB"))?;

    // Two full parts and a short final part. Retain this key across reconnects.
    let payload: Vec<u8> = (0..2 * part + 7).map(|i| (i % 251) as u8).collect();
    let object_key = begin(&mut client, payload.len() as u64, part).await?;
    client
        .write_at(object_key.clone(), 0, part, &mut &payload[..part as usize])
        .await?;
    drop(client);

    let mut client = N1NativeClient::connect(&url).await?;
    client.auth_fragment(&fragment, &secret).await?;
    let progress = result(
        client
            .control(Command::UploadStatus {
                object_key: object_key.clone(),
                start: 0,
                limit: 4096,
            })
            .await?,
    )?;
    let missing = progress["missing_parts"]
        .as_array()
        .ok_or_else(|| io::Error::other("missing part page"))?;
    assert_eq!(missing, &[json!(1), json!(2)]);
    for index in missing {
        let offset = index
            .as_u64()
            .ok_or_else(|| io::Error::other("part index"))?
            * part;
        let end = (offset + part).min(payload.len() as u64);
        client
            .write_at(
                object_key.clone(),
                offset,
                end - offset,
                &mut &payload[offset as usize..end as usize],
            )
            .await?;
    }
    let finalized = result(
        client
            .control(Command::UploadFinalize {
                object_key: object_key.clone(),
            })
            .await?,
    )?;
    assert_eq!(key(&finalized)?, object_key);
    let repeated = result(
        client
            .control(Command::UploadFinalize {
                object_key: object_key.clone(),
            })
            .await?,
    )?;
    assert_eq!(repeated["already_finalized"], true);

    let mut downloaded = Vec::new();
    client
        .download(object_key.clone(), 0, None, &mut downloaded)
        .await?;
    assert_eq!(downloaded, payload);
    let grant = result(
        client
            .control(Command::SignedRead {
                object_key: object_key.clone(),
                offset: part - 2,
                length: Some(8),
                ttl_seconds: 60,
            })
            .await?,
    )?;
    let mut range = Vec::new();
    client
        .download_signed(serde_json::from_value(grant)?, &mut range)
        .await?;
    assert_eq!(range, payload[part as usize - 2..part as usize + 6]);
    let health = result(
        client
            .control(Command::ObjectHealth {
                object_key: object_key.clone(),
                deep: true,
            })
            .await?,
    )?;
    assert_eq!(health["status"], "healthy");

    let single = begin(&mut client, 5, part).await?;
    client.put(single.clone(), 5, &mut &b"hello"[..]).await?;
    let first = begin(&mut client, 3, part).await?;
    let second = begin(&mut client, 3, part).await?;
    client
        .put_batch(vec![first.clone(), second.clone()], 6, &mut &b"onetwo"[..])
        .await?;
    for (identity, expected) in [
        (&single, &b"hello"[..]),
        (&first, &b"one"[..]),
        (&second, &b"two"[..]),
    ] {
        let mut bytes = Vec::new();
        client
            .download(identity.clone(), 0, None, &mut bytes)
            .await?;
        assert_eq!(bytes, expected);
    }
    let deleted = result(
        client
            .control(Command::BulkDelete {
                object_keys: vec![object_key.clone(), single, first, second],
            })
            .await?,
    )?;
    assert!(
        deleted["results"]
            .as_array()
            .is_some_and(|rows| rows.len() == 4 && rows.iter().all(|row| row["status"] == 200))
    );
    let old_key = client
        .control(Command::UploadStatus {
            object_key,
            start: 0,
            limit: 1,
        })
        .await;
    assert!(matches!(old_key, Err(NativeClientError::Native { ref code, .. }) if code == "404"));
    println!(
        "{}",
        json!({"verified_objects":4,"resumed":true,"repeated_finalize":true,"signed_range":true,"purged":4,"old_key_status":404})
    );
    Ok(())
}
