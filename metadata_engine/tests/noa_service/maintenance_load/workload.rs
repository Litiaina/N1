//! Application traffic uses public NOA requests and an independent byte oracle.
use super::*;

const BLOCK: usize = 1 << 20;

fn pattern(seed: u64) -> Vec<u8> {
    (0..BLOCK)
        .map(|i| ((i as u64 * 31 + seed) % 251) as u8)
        .collect()
}

fn payload(seed: u64, length: usize) -> bytes::Bytes {
    let block = pattern(seed);
    let mut bytes = Vec::with_capacity(length);
    while bytes.len() < length {
        let take = (length - bytes.len()).min(block.len());
        bytes.extend_from_slice(&block[..take]);
    }
    bytes.into()
}

pub(super) async fn upload(context: &Context, path: String, length: usize, seed: u64) -> Expected {
    let bytes = payload(seed, length);
    let ack = json_response(
        context
            .client
            .post(format!(
                "{}/upload/one-shot/{path}?durability=strict",
                context.base
            ))
            .bearer_auth(&context.token)
            .body(bytes.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    Expected {
        path,
        version: ack["version_id"].as_str().unwrap().into(),
        bytes: length as u64,
        seed,
        sha256: hex::encode(Sha256::digest(&bytes)),
        trashed: false,
    }
}

pub(super) async fn verify(context: &Context, expected: &Expected, range: bool) {
    let mut request = context
        .client
        .get(format!("{}/objects/get/{}", context.base, expected.path))
        .bearer_auth(&context.token);
    let (offset, length) = if range {
        let offset = 123_457;
        request = request.header("range", format!("bytes={offset}-{}", offset + BLOCK - 1));
        (offset, BLOCK as u64)
    } else {
        (0, expected.bytes)
    };
    let mut response = request.send().await.unwrap();
    if expected.trashed {
        assert_eq!(response.status(), 404);
        return;
    }
    assert_eq!(response.status(), if range { 206 } else { 200 });
    assert_eq!(
        response.headers()["etag"]
            .to_str()
            .unwrap()
            .trim_matches('"'),
        expected.version
    );
    if range {
        assert_eq!(
            response.headers()["content-range"].to_str().unwrap(),
            format!(
                "bytes {offset}-{}/{total}",
                offset as u64 + length - 1,
                total = expected.bytes
            )
        );
    }
    let block = pattern(expected.seed);
    let mut position = 0u64;
    let mut hash = Sha256::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        let mut checked = 0;
        while checked < chunk.len() {
            let index = (offset as u64 + position + checked as u64) as usize % BLOCK;
            let take = (chunk.len() - checked).min(BLOCK - index);
            assert_eq!(&chunk[checked..checked + take], &block[index..index + take]);
            checked += take;
        }
        hash.update(&chunk);
        position += chunk.len() as u64;
    }
    assert_eq!(position, length);
    if !range {
        assert_eq!(hex::encode(hash.finalize()), expected.sha256);
    }
}

pub(super) async fn foreground(
    context: Context,
    meter: Arc<Meter>,
    stop: Instant,
    phase: usize,
    library: Vec<Expected>,
    tasks: &mut tokio::task::JoinSet<()>,
) {
    let (trash, mut receiver) = tokio::sync::mpsc::channel::<usize>(16);
    for worker in 0..2 {
        let (context, meter, trash) = (context.clone(), meter.clone(), trash.clone());
        tasks.spawn(async move {
            let mut index = 0;
            while Instant::now() < stop {
                let seed = (phase * 100_000 + worker * 10_000 + index) as u64;
                let length = [4096, 65536, 262144][index % 3];
                let path = format!("load/p{phase}-w{worker}-{index}.bin");
                let mut expected = meter
                    .call(
                        1,
                        "small_upload",
                        length as u64,
                        upload(&context, path, length, seed),
                    )
                    .await;
                if index % 4 == 0 {
                    let destination = format!("{}.renamed", expected.path);
                    meter
                        .call(1, "rename", 0, async {
                            json_response(
                                context
                                    .client
                                    .post(format!("{}/objects/rename", context.base))
                                    .bearer_auth(&context.token)
                                    .json(&json!({
                                        "source_key":expected.path,"destination_key":destination
                                    }))
                                    .send()
                                    .await
                                    .unwrap(),
                                200,
                            )
                            .await;
                        })
                        .await;
                    expected.path = destination;
                }
                meter
                    .call(
                        1,
                        "small_readback",
                        expected.bytes,
                        verify(&context, &expected, false),
                    )
                    .await;
                let slot = context.save(expected);
                if index % 2 == 0 {
                    trash.send(slot).await.unwrap();
                }
                index += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }
    drop(trash);
    let (deleting, deleted_meter) = (context.clone(), meter.clone());
    tasks.spawn(async move {
        while let Some(slot) = receiver.recv().await {
            let path = deleting.oracle.lock().unwrap()[slot].path.clone();
            deleted_meter
                .call(4, "trash", 0, async {
                    json_response(
                        deleting
                            .client
                            .delete(format!("{}/objects/{path}", deleting.base))
                            .bearer_auth(&deleting.token)
                            .send()
                            .await
                            .unwrap(),
                        200,
                    )
                    .await;
                })
                .await;
            deleting.oracle.lock().unwrap()[slot].trashed = true;
        }
    });
    for (role, range) in [(2, false), (3, true)] {
        let (context, meter, expected) = (
            context.clone(),
            meter.clone(),
            library[usize::from(range)].clone(),
        );
        tasks.spawn(async move {
            while Instant::now() < stop {
                meter
                    .call(
                        role,
                        if range {
                            "video_range"
                        } else {
                            "full_download"
                        },
                        if range { BLOCK as u64 } else { expected.bytes },
                        verify(&context, &expected, range),
                    )
                    .await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }
    tasks.spawn(async move {
        let mut index = 0;
        while Instant::now() < stop {
            let length = 128 << 20;
            let part_bytes = 8 << 20;
            let seed = (phase * 100_000 + 90_000 + index) as u64;
            let bytes = payload(seed, part_bytes);
            let mut digest = Sha256::new();
            for _ in 0..16 {
                digest.update(&bytes);
            }
            let path = format!("load/large-p{phase}-{index}.bin");
            let route = format!(
                "{}/upload/multipart{}",
                context.base,
                if index % 2 == 0 { "" } else { "/durable" }
            );
            let policy = if index % 2 == 0 { "batched" } else { "strict" };
            let plan = meter
                .call(0, "multipart_init", 0, async {
                    json_response(context.client.post(format!("{route}/init/{path}?durability={policy}"))
                    .bearer_auth(&context.token).json(&json!({
                        "total_size_bytes":length,"part_size_bytes":part_bytes,"expected_parts":16
                    })).send().await.unwrap(),200).await
                })
                .await;
            let version = plan["version_id"].as_str().unwrap();
            let mode = if index % 2 == 0 {
                "normal_part"
            } else {
                "durable_part"
            };
            for part in 0..16 {
                meter
                    .call(0, mode, part_bytes as u64, async {
                        json_response(
                            context
                                .client
                                .put(format!("{route}/{version}/{part}?durability={policy}"))
                                .bearer_auth(&context.token)
                                .body(bytes.clone())
                                .send()
                                .await
                                .unwrap(),
                            200,
                        )
                        .await;
                    })
                    .await;
            }
            let finalize = || async {
                json_response(
                    context
                        .client
                        .post(format!(
                            "{route}/{version}/{}/finalize",
                            plan["session_id"].as_str().unwrap()
                        ))
                        .bearer_auth(&context.token)
                        .json(&json!({"total_size_bytes":length,"expected_parts":16}))
                        .send()
                        .await
                        .unwrap(),
                    200,
                )
                .await;
            };
            meter.call(0, "finalize", 0, finalize()).await;
            meter.call(0, "repeated_finalize", 0, finalize()).await;
            context.save(Expected {
                path,
                version: version.into(),
                bytes: length,
                seed,
                sha256: hex::encode(digest.finalize()),
                trashed: false,
            });
            index += 1;
        }
    });
}
