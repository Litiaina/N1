use super::{
    Result,
    api::{self, Api, MIB, PART, Payload},
    observe::{Monitor, Recorder},
};
use n1_native_client::{Command, N1NativeClient};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const PENDING: u8 = 0;
const LIVE: u8 = 1;
const DELETING: u8 = 2;
const DELETED: u8 = 3;

pub struct Object {
    key: String,
    seed: u64,
    size: u64,
    state: AtomicU8,
}
pub struct Catalog {
    objects: Mutex<Vec<Arc<Object>>>,
    eligible: Mutex<VecDeque<Arc<Object>>>,
    videos: Vec<Arc<Object>>,
    documents: Vec<Arc<Object>>,
    next_seed: AtomicU64,
}
impl Catalog {
    fn allocate(&self, key: String, seed: u64, size: u64) -> Arc<Object> {
        let object = Arc::new(Object {
            key,
            seed,
            size,
            state: AtomicU8::new(PENDING),
        });
        self.objects.lock().unwrap().push(object.clone());
        object
    }
    fn published(&self, object: &Arc<Object>, eligible: bool) {
        object.state.store(LIVE, Ordering::Release);
        if eligible {
            self.eligible.lock().unwrap().push_back(object.clone());
        }
    }
    fn manifest(&self) -> Value {
        json!(self.objects.lock().unwrap().iter().map(|o|json!({
            "object_key":o.key,"seed":o.seed,"bytes":o.size,"state":o.state.load(Ordering::Acquire)
        })).collect::<Vec<_>>())
    }
}

pub struct Profile {
    pub label: String,
    pub seconds: u64,
    pub bulk: usize,
    pub video: usize,
    pub downloads: usize,
    pub interactive: usize,
    pub deletes: usize,
}
pub async fn seed(api: &Api, output: &Path) -> Result<Arc<Catalog>> {
    let mut catalog = Catalog {
        objects: Mutex::new(Vec::new()),
        eligible: Mutex::new(VecDeque::new()),
        videos: Vec::new(),
        documents: Vec::new(),
        next_seed: AtomicU64::new(1000),
    };
    let mut client = api.connect().await?;
    // Existing video/document library plus older files that users may delete.
    for index in 0..138 {
        let size = if index < 2 {
            64 * MIB
        } else if index < 10 {
            8 * MIB
        } else if index % 2 == 0 {
            4096
        } else {
            256 * 1024
        };
        let payload = Payload::new(index, size);
        let key = api::begin(&mut client, size, index % 2 == 0).await?;
        let object = catalog.allocate(key, index, size);
        if size > PART {
            for offset in (0..size).step_by(PART as usize) {
                let length = (size - offset).min(PART);
                client
                    .write_at(
                        object.key.clone(),
                        offset,
                        length,
                        &mut payload.reader(offset, length),
                    )
                    .await?;
            }
            client
                .control(Command::UploadFinalize {
                    object_key: object.key.clone(),
                })
                .await?;
        } else {
            client
                .put(object.key.clone(), size, &mut payload.reader(0, size))
                .await?;
        }
        api::get(&mut client, &object.key, &payload, 0, size).await?;
        catalog.published(&object, index >= 10);
        if index < 2 {
            catalog.videos.push(object);
        } else if index < 10 {
            catalog.documents.push(object);
        }
    }
    api::save(&output.join("seed.json"), &catalog.manifest())?;
    println!("{}", json!({"phase":"seed","objects":138,"verified":true}));
    Ok(Arc::new(catalog))
}

pub async fn phase(
    api: &Api,
    catalog: Arc<Catalog>,
    profile: Profile,
    pid: u32,
    output: &Path,
) -> Result<Value> {
    let monitor = Monitor::start(pid)?;
    let recorder = Recorder::new();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(profile.seconds);
    let clients =
        profile.bulk + profile.video + profile.downloads + profile.interactive + profile.deletes;
    let start = Arc::new(tokio::sync::Barrier::new(clients + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let diagnostic = profile.seconds < 10;
    let mut jobs = tokio::task::JoinSet::new();
    for (role, count) in [
        ("bulk", profile.bulk),
        ("video", profile.video),
        ("download", profile.downloads),
        ("interactive", profile.interactive),
        ("delete", profile.deletes),
    ] {
        for worker in 0..count {
            let api = api.clone();
            let catalog = catalog.clone();
            let recorder = recorder.clone();
            let start = start.clone();
            let stop = stop.clone();
            jobs.spawn(async move {
                let mut client = api.connect().await?;
                start.wait().await;
                let result = session(
                    &api,
                    &mut client,
                    &catalog,
                    &recorder,
                    role,
                    worker,
                    deadline,
                    &stop,
                    diagnostic,
                )
                .await;
                if result.is_err() {
                    stop.store(true, Ordering::Relaxed);
                }
                result
            });
        }
    }
    println!(
        "{}",
        json!({"phase":profile.label,"seconds":profile.seconds,"sessions":clients,
        "roles":{"large_uploaders":profile.bulk,"video_viewers":profile.video,"file_downloaders":profile.downloads,
            "interactive_uploaders":profile.interactive,"deletion_sessions":profile.deletes}})
    );
    tokio::time::timeout(Duration::from_secs(30), start.wait()).await?;
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    let mut errors = Vec::new();
    while !jobs.is_empty() {
        tokio::select! {
            result = jobs.join_next() => {
                match result {
                    Some(Ok(Ok(())))=>(),
                    Some(Ok(Err(e)))=>errors.push(e.to_string()),
                    Some(Err(e))=>{stop.store(true,Ordering::Relaxed);errors.push(e.to_string());},
                    None=>(),
                }
            }
            _ = interval.tick() => println!("{}",json!({"phase":profile.label,"elapsed_seconds":started.elapsed().as_secs_f64(),
                "progress":recorder.progress()})),
        }
        if started.elapsed() > Duration::from_secs(profile.seconds + 300) {
            return Err("mixed workload failed to drain within five minutes".into());
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    let resources = monitor.finish(output, &profile.label).await?;
    let measurements = recorder.summary(output, &profile.label, seconds, profile.seconds as f64)?;
    let row = json!({"phase":profile.label,"offered_seconds":profile.seconds,"wall_seconds":seconds,
        "sessions":clients,"measurements":measurements,"resources":resources,"worker_errors":errors,
        "drain_note":"new user operations stop at the window; already-started objects finish and may extend wall time"});
    api::save(
        &output.join(format!("{}-summary.json", profile.label)),
        &row,
    )?;
    api::save(
        &output.join(format!("{}-catalog.json", profile.label)),
        &catalog.manifest(),
    )?;
    if !errors.is_empty() || measurements["errors"] != 0 {
        return Err("mixed workload recorded request/worker errors".into());
    }
    if measurements["peak_distinct_roles_in_flight"]
        .as_u64()
        .unwrap_or(0)
        < 4
    {
        return Err("workload did not demonstrate overlapping operation classes".into());
    }
    println!(
        "{}",
        json!({"phase":profile.label,"completed":true,"wall_seconds":seconds,
        "samples":measurements["request_samples"],"n1_cpu_percent_one_core":resources["processes"]["n1"]["cpu_percent_one_core"]})
    );
    Ok(row)
}

#[allow(clippy::too_many_arguments)]
async fn session(
    api: &Api,
    client: &mut N1NativeClient,
    catalog: &Catalog,
    recorder: &Recorder,
    role: &'static str,
    worker: usize,
    deadline: Instant,
    stop: &AtomicBool,
    diagnostic: bool,
) -> Result<()> {
    let mut sequence = 0u64;
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        match role {
            "bulk" => bulk(api, client, catalog, recorder, sequence, diagnostic, worker).await?,
            "interactive" => {
                let size =
                    [4096, 4096, 64 * 1024, 64 * 1024, 256 * 1024, MIB][sequence as usize % 6];
                let seed = catalog.next_seed.fetch_add(1, Ordering::Relaxed);
                let payload = Payload::new(seed, size);
                let durable = sequence.is_multiple_of(3);
                let object = recorder
                    .call(
                        role,
                        if durable {
                            "small_upload_strict"
                        } else {
                            "small_upload_normal"
                        },
                        size,
                        async {
                            let key = api::begin(client, size, durable).await?;
                            let object = catalog.allocate(key, seed, size);
                            client
                                .put(object.key.clone(), size, &mut payload.reader(0, size))
                                .await?;
                            // Keep the identity out of the deletion queue until its immediate GET ends.
                            catalog.published(&object, false);
                            Ok(object)
                        },
                    )
                    .await?;
                recorder
                    .call(
                        role,
                        "small_read",
                        size,
                        api::get(client, &object.key, &payload, 0, size),
                    )
                    .await?;
                if sequence.is_multiple_of(2) {
                    catalog.eligible.lock().unwrap().push_back(object);
                }
                think(150 + (sequence * 71 + worker as u64 * 97) % 600).await;
            }
            "video" => {
                let object =
                    &catalog.videos[((sequence / 16) as usize + worker) % catalog.videos.len()];
                let payload = Payload::new(object.seed, object.size);
                let offset = (sequence * MIB + worker as u64 * 4093) % (object.size - MIB);
                recorder
                    .call(
                        role,
                        "video_range",
                        MIB,
                        api::get(client, &object.key, &payload, offset, MIB),
                    )
                    .await?;
                think(1000).await;
            }
            "download" => {
                let object =
                    &catalog.documents[(sequence as usize + worker) % catalog.documents.len()];
                let payload = Payload::new(object.seed, object.size);
                recorder
                    .call(
                        role,
                        "file_download",
                        object.size,
                        api::get(client, &object.key, &payload, 0, object.size),
                    )
                    .await?;
                think(500 + (sequence * 131 + worker as u64 * 173) % 1000).await;
            }
            "delete" => {
                let object = catalog.eligible.lock().unwrap().pop_front();
                if let Some(object) = object {
                    object
                        .state
                        .compare_exchange(LIVE, DELETING, Ordering::AcqRel, Ordering::Acquire)
                        .map_err(|_| "deletion queue contains a non-live object")?;
                    recorder
                        .call(role, "delete_and_confirm_absence", object.size, async {
                            client
                                .control(Command::ObjectDelete {
                                    object_key: object.key.clone(),
                                })
                                .await?;
                            object.state.store(DELETED, Ordering::Release);
                            api::absent(client, &object.key).await
                        })
                        .await?;
                    think(1200 + (sequence * 193 + worker as u64 * 211) % 1300).await;
                } else {
                    think(500).await;
                }
            }
            _ => return Err("invalid client role".into()),
        }
        sequence += 1;
    }
    Ok(())
}
async fn think(millis: u64) {
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

async fn bulk(
    api: &Api,
    client: &mut N1NativeClient,
    catalog: &Catalog,
    recorder: &Recorder,
    sequence: u64,
    smoke: bool,
    worker: usize,
) -> Result<()> {
    let size = if smoke {
        32 * MIB
    } else if sequence == 0 {
        1024 * MIB
    } else {
        512 * MIB
    };
    let seed = catalog.next_seed.fetch_add(1, Ordering::Relaxed);
    let payload = Payload::new(seed, size);
    let durable = (sequence + worker as u64) % 2 == 1;
    let key = recorder
        .call(
            "bulk",
            "multipart_init",
            0,
            api::begin(client, size, durable),
        )
        .await?;
    let object = catalog.allocate(key.clone(), seed, size);
    let transfer = Instant::now();
    for part in 0..size / PART {
        if part == 2 {
            recorder
                .call("bulk", "reconnect_and_resume_status", 0, async {
                    *client = api.connect().await?;
                    let status = api::value(
                        client
                            .control(Command::UploadStatus {
                                object_key: key.clone(),
                                start: 0,
                                limit: 4,
                            })
                            .await?,
                    )?;
                    let missing = status["missing_parts"]
                        .as_array()
                        .ok_or("missing resume progress")?;
                    if missing != &[json!(2), json!(3)] {
                        return Err("resume lost acknowledged parts".into());
                    }
                    Ok(())
                })
                .await?;
        }
        recorder
            .call(
                "bulk",
                if durable {
                    "multipart_part_strict"
                } else {
                    "multipart_part_normal"
                },
                PART,
                async {
                    client
                        .write_at(
                            key.clone(),
                            part * PART,
                            PART,
                            &mut payload.reader(part * PART, PART),
                        )
                        .await?;
                    Ok(())
                },
            )
            .await?;
    }
    recorder
        .call("bulk", "finalize", 0, async {
            let result = api::value(
                client
                    .control(Command::UploadFinalize {
                        object_key: key.clone(),
                    })
                    .await?,
            )?;
            if result["object_key"] != key {
                return Err("finalize changed LINK identity".into());
            }
            Ok(())
        })
        .await?;
    catalog.published(&object, false);
    recorder
        .call("bulk", "repeated_finalize", 0, async {
            let result = api::value(
                client
                    .control(Command::UploadFinalize {
                        object_key: key.clone(),
                    })
                    .await?,
            )?;
            if result["already_finalized"] != true {
                return Err("finalize retry is not idempotent".into());
            }
            Ok(())
        })
        .await?;
    println!(
        "{}",
        json!({"event":"large_object_completed","bytes":size,"strict_parts":durable,
        "seconds":transfer.elapsed().as_secs_f64(),"mib_per_second":size as f64/MIB as f64/transfer.elapsed().as_secs_f64()})
    );
    Ok(())
}

pub async fn verify(api: &Api, catalog: &Catalog, output: &Path, label: &str) -> Result<()> {
    let objects = catalog.objects.lock().unwrap().clone();
    let mut client = api.connect().await?;
    let mut live = 0;
    let mut deleted = 0;
    let mut bytes = 0;
    let mut rows = Vec::new();
    println!(
        "{}",
        json!({"verification":label,"objects":objects.len(),"started":true})
    );
    for object in objects {
        match object.state.load(Ordering::Acquire) {
            LIVE => {
                let payload = Payload::new(object.seed, object.size);
                let digest = api::get(&mut client, &object.key, &payload, 0, object.size).await?;
                rows.push(json!({"object_key":object.key,"state":"live","readback":digest}));
                live += 1;
                bytes += object.size;
            }
            DELETED => {
                api::absent(&mut client, &object.key).await?;
                deleted += 1;
            }
            _ => return Err("unfinished object lifecycle after mixed load".into()),
        }
    }
    api::save(
        &output.join(format!("{label}-verification.json")),
        &json!({"passed":true,"live":live,"deleted":deleted,"verified_bytes":bytes,"rows":rows}),
    )?;
    println!(
        "{}",
        json!({"verification":label,"passed":true,"live":live,"deleted":deleted,"verified_bytes":bytes})
    );
    Ok(())
}
pub async fn cleanup(api: &Api, catalog: &Catalog, output: &Path) -> Result<Value> {
    let objects = catalog.objects.lock().unwrap().clone();
    let mut client = api.connect().await?;
    let mut purged = 0;
    for object in objects {
        if object.state.load(Ordering::Acquire) == LIVE {
            client
                .control(Command::ObjectDelete {
                    object_key: object.key.clone(),
                })
                .await?;
            api::absent(&mut client, &object.key).await?;
            object.state.store(DELETED, Ordering::Release);
            purged += 1;
        }
    }
    let result = json!({"purged_remaining_objects":purged,"all_workload_identities_deleted":true});
    api::save(&output.join("cleanup.json"), &result)?;
    Ok(result)
}
