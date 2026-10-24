use super::*;
use crate::core::checksum::{format_sha256, sha256_digest};
use crate::core::disk_manager::{PartMetadata, ShardInfo};
use crate::distribution::download::{
    read_cluster_data_chunk_cached, read_verified_cluster_data_stripe,
};
use crate::distribution::membership::{
    ClusterMembership, ClusterNode, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState,
    PoolState, ServerPool, StorageRoot, init_cluster_runtime,
};
use crate::distribution::peer::encode_peer_shard_locator;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;
use ulid::Ulid;

#[test]
fn active_read_repair_retains_new_targets_and_old_worker_cannot_remove_successor() {
    let mut queue = HashMap::new();
    let owner = enqueue_read_repair_targets(&mut queue, "part", HashSet::from([1])).unwrap();
    assert_eq!(
        take_read_repair_targets(&mut queue, "part", &owner),
        Some(HashSet::from([1]))
    );

    // Targets discovered while the worker is blocked on a peer are retained,
    // rather than discarded merely because this part already has a worker.
    assert!(enqueue_read_repair_targets(&mut queue, "part", HashSet::from([1, 3])).is_none());
    assert_eq!(
        take_read_repair_targets(&mut queue, "part", &owner),
        Some(HashSet::from([1, 3]))
    );
    assert!(take_read_repair_targets(&mut queue, "part", &owner).is_none());

    let successor = enqueue_read_repair_targets(&mut queue, "part", HashSet::from([2])).unwrap();
    remove_owned_read_repair(&mut queue, "part", &owner);
    assert_eq!(
        take_read_repair_targets(&mut queue, "part", &successor),
        Some(HashSet::from([2]))
    );
    assert!(take_read_repair_targets(&mut queue, "part", &successor).is_none());
    assert!(queue.is_empty());
}

#[test]
fn read_repair_admission_is_bounded_but_retains_targets_for_active_parts() {
    let mut queue = HashMap::new();
    let mut owners = Vec::new();
    for index in 0..MAX_CLUSTER_READ_REPAIR_PARTS {
        let key = format!("part-{index}");
        owners.push(enqueue_read_repair_targets(&mut queue, &key, HashSet::from([0])).unwrap());
    }
    assert!(enqueue_read_repair_targets(&mut queue, "overflow", HashSet::from([0])).is_none());
    assert_eq!(queue.len(), MAX_CLUSTER_READ_REPAIR_PARTS);
    assert!(enqueue_read_repair_targets(&mut queue, "part-0", HashSet::from([2])).is_none());
    assert_eq!(
        take_read_repair_targets(&mut queue, "part-0", &owners[0]),
        Some(HashSet::from([0, 2]))
    );
}

// ClusterRuntime is process-global. Run the network/physical regression in its
// own test process rather than modifying the runtime of concurrently run tests.
#[test]
fn verified_get_repairs_physical_bitrot_missing_shards_and_new_inflight_targets() {
    const CHILD: &str = "N2_TARGETED_READ_REPAIR_CHILD";
    if let Some(completion) = std::env::var_os(CHILD) {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(physical_read_repair_regression());
        std::fs::write(completion, b"PASS").expect("record actual child execution");
        return;
    }
    let completion = std::env::temp_dir().join(format!("n2-read-repair-completed-{}", Ulid::new()));
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "distribution::repair::tests::verified_get_repairs_physical_bitrot_missing_shards_and_new_inflight_targets", "--nocapture"])
        .env(CHILD, &completion)
        .spawn()
        .expect("spawn isolated read-repair test");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().expect("poll isolated read-repair test") {
            assert!(
                status.success(),
                "isolated physical read-repair regression failed: {status}"
            );
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("isolated read-repair regression exceeded its 60-second limit");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(
        std::fs::read(&completion)
            .expect("child must execute the regression, not match zero tests"),
        b"PASS"
    );
    std::fs::remove_file(completion).unwrap();
}

struct PeerFixture {
    root: PathBuf,
    expected: Vec<ShardInfo>,
    unavailable: Mutex<HashSet<usize>>,
    writes: Mutex<Vec<usize>>,
    pause_first_write: AtomicBool,
    write_started: Semaphore,
    release_write: Semaphore,
}

async fn fixture_get(
    State(fixture): State<Arc<PeerFixture>>,
    Path((fragment, object, version, part, shard)): Path<(String, String, String, u64, usize)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if fragment != "fragment"
        || object != "object"
        || version != "version"
        || part != 0
        || shard >= 4
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    assert_eq!(query.get("slot_id"), Some(&format!("node-{shard}")));
    assert_eq!(query.get("root_id"), Some(&format!("root-{shard}")));
    assert_eq!(query.get("pool_id").map(String::as_str), Some("pool"));
    assert_eq!(query.get("pool_generation").map(String::as_str), Some("1"));
    if fixture.unavailable.lock().unwrap().contains(&shard) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let bytes = match tokio::fs::read(fixture.root.join(format!("shard-{shard}"))).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return StatusCode::NOT_FOUND.into_response();
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let offset = query["offset"].parse::<usize>().unwrap();
    let length = query["length"].parse::<usize>().unwrap();
    bytes
        .get(offset..offset.saturating_add(length))
        .map(|slice| slice.to_vec().into_response())
        .unwrap_or_else(|| StatusCode::RANGE_NOT_SATISFIABLE.into_response())
}

async fn fixture_put(
    State(fixture): State<Arc<PeerFixture>>,
    Path((fragment, object, version, part, shard)): Path<(String, String, String, u64, usize)>,
    Query(query): Query<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    assert_eq!(
        (fragment.as_str(), object.as_str(), version.as_str(), part),
        ("fragment", "object", "version", 0)
    );
    assert!(shard < 4);
    assert_eq!(query.get("slot_id"), Some(&format!("node-{shard}")));
    assert_eq!(query.get("root_id"), Some(&format!("root-{shard}")));
    assert_eq!(
        query.get("deferred_durability").map(String::as_str),
        Some("false")
    );
    let expected = &fixture.expected[shard];
    assert_eq!(bytes.len() as u64, expected.size_bytes);
    assert_eq!(format_sha256(&sha256_digest(&bytes)), expected.checksum);
    if fixture.pause_first_write.swap(false, Ordering::SeqCst) {
        fixture.write_started.add_permits(1);
        fixture.release_write.acquire().await.unwrap().forget();
    }
    tokio::fs::write(fixture.root.join(format!("shard-{shard}")), &bytes)
        .await
        .unwrap();
    fixture.writes.lock().unwrap().push(shard);
    Json(expected.clone()).into_response()
}

async fn await_repairs() {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if CLUSTER_READ_REPAIR_INFLIGHT.lock().unwrap().is_empty() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("read-repair workers drain without an unbounded self-enqueue loop");
}

async fn physical_read_repair_regression() {
    let root = std::env::temp_dir().join(format!("n2-physical-read-repair-{}", Ulid::new()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let block_size = 1024;
    let mut expected = vec![Vec::new(); 4];
    for block in 0..4 {
        let data = [
            (0..block_size)
                .map(|byte| ((byte + block * 13) % 251) as u8)
                .collect::<Vec<_>>(),
            (0..block_size)
                .map(|byte| ((byte * 7 + block * 19) % 253) as u8)
                .collect::<Vec<_>>(),
        ];
        let mut encoder = ecs_core::ReedSolomonEncoder::new(2, 2, block_size).unwrap();
        for bytes in &data {
            encoder.add_original_shard(bytes).unwrap();
        }
        let encoded = encoder.encode().unwrap();
        expected[0].extend_from_slice(&data[0]);
        expected[1].extend_from_slice(&data[1]);
        expected[2].extend_from_slice(encoded.recovery(0).unwrap());
        expected[3].extend_from_slice(encoded.recovery(1).unwrap());
    }
    let nodes = (0..4)
        .map(|index| ClusterNode {
            id: format!("node-{index}"),
            instance_id: format!("node-{index}"),
            binding_epoch: 1,
            endpoint: endpoint.clone(),
            native_endpoint: None,
            state: Some(NodeState::Online),
            weight: 1,
            storage_roots: vec![StorageRoot {
                id: format!("root-{index}"),
                physical_id: None,
                path: root
                    .join(format!("member-{index}"))
                    .to_string_lossy()
                    .to_string(),
            }],
            joined_epoch: 1,
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        })
        .collect::<Vec<_>>();
    let membership = ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "read-repair-test".into(),
        epoch: 1,
        binding_epoch: 1,
        pools: vec![ServerPool {
            id: "pool".into(),
            generation: 1,
            policy: ClusterPolicy {
                object_data_shards: 2,
                object_parity_shards: 2,
            },
            state: PoolState::Writable,
            node_ids: nodes.iter().map(|node| node.id.clone()).collect(),
        }],
        nodes,
        reserve_nodes: Vec::new(),
        retired_instances: Vec::new(),
    };
    tokio::fs::write(
        root.join("n2_membership.json"),
        serde_json::to_vec(&membership).unwrap(),
    )
    .await
    .unwrap();
    let mut config = ini::Ini::new();
    config
        .with_section(Some("cluster_config"))
        .set("node_id", "node-0")
        .set("pool_id", "pool")
        .set("object_data_shards", "2")
        .set("object_parity_shards", "2");
    init_cluster_runtime(
        &config,
        root.to_str().unwrap(),
        &membership.nodes[0].storage_roots,
        &endpoint,
        &endpoint,
    )
    .unwrap();

    let shards = expected
        .iter()
        .enumerate()
        .map(|(index, bytes)| ShardInfo {
            shard_index: index,
            file_name: format!("part_0_shard_{index}"),
            absolute_path: encode_peer_shard_locator(&PeerShardTarget {
                pool_id: "pool".into(),
                pool_generation: 1,
                node_id: format!("node-{index}"),
                instance_id: format!("node-{index}"),
                binding_epoch: 1,
                root_id: format!("root-{index}"),
                endpoint: endpoint.clone(),
                fragment: "fragment".into(),
                object_id: "object".into(),
                version_id: "version".into(),
                part_index: 0,
                shard_index: index,
                is_parity: index >= 2,
                expected_len: bytes.len() as u64,
                deferred_durability: false,
                repair: false,
                expected_checksum: None,
            }),
            size_bytes: bytes.len() as u64,
            checksum: format_sha256(&sha256_digest(bytes)),
            block_checksums: bytes.chunks(block_size).map(sha256_digest).collect(),
            checksum_block_size: block_size,
            is_parity: index >= 2,
        })
        .collect::<Vec<_>>();
    let part = PartMetadata {
        part_index: 0,
        size_bytes: (expected[0].len() * 2) as u64,
        created_at: 0,
        checksum: None,
        shards,
    };
    let metadata_before = serde_json::to_vec(&part).unwrap();
    for (index, bytes) in expected.iter().enumerate() {
        tokio::fs::write(root.join(format!("shard-{index}")), bytes)
            .await
            .unwrap();
    }
    let fixture = Arc::new(PeerFixture {
        root: root.clone(),
        expected: part.shards.clone(),
        unavailable: Mutex::new(HashSet::new()),
        writes: Mutex::new(Vec::new()),
        pause_first_write: AtomicBool::new(true),
        write_started: Semaphore::new(0),
        release_write: Semaphore::new(0),
    });
    let app = Router::new()
        .route(
            "/n2/peer/shards/{fragment}/{object}/{version}/{part}/{shard}",
            get(fixture_get).put(fixture_put),
        )
        .with_state(fixture.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Same length and same first byte: the old one-byte presence probe called
    // this healthy even though the GET had proved corruption in this block.
    let mut corrupt_data = expected[1].clone();
    corrupt_data[17] ^= 0x5a;
    tokio::fs::write(root.join("shard-1"), &corrupt_data)
        .await
        .unwrap();
    assert_eq!(
        shard_probe(&part.shards[1], false).await,
        ShardProbeResult::Healthy
    );
    let stripe = [&expected[0][..block_size], &expected[1][..block_size]].concat();
    assert_eq!(
        read_verified_cluster_data_stripe(&part, 0, block_size)
            .await
            .unwrap(),
        stripe
    );
    tokio::time::timeout(Duration::from_secs(10), fixture.write_started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    // Discover a different corrupt holder while the first repair is blocked.
    let mut corrupt_parity = expected[3].clone();
    corrupt_parity[17] ^= 0x3c;
    tokio::fs::write(root.join("shard-3"), &corrupt_parity)
        .await
        .unwrap();
    assert_eq!(
        read_verified_cluster_data_stripe(&part, 0, block_size)
            .await
            .unwrap(),
        stripe
    );
    fixture.release_write.add_permits(1);
    await_repairs().await;
    assert_eq!(*fixture.writes.lock().unwrap(), vec![1, 3]);
    for (index, bytes) in expected.iter().enumerate() {
        assert_eq!(
            &tokio::fs::read(root.join(format!("shard-{index}")))
                .await
                .unwrap(),
            bytes
        );
        assert_eq!(
            shard_probe(&part.shards[index], true).await,
            ShardProbeResult::Healthy
        );
    }

    // A transport-unavailable holder is not replaced solely because GET had
    // to reconstruct. Once reachable again its original bytes are intact.
    fixture.unavailable.lock().unwrap().insert(0);
    assert_eq!(
        read_verified_cluster_data_stripe(&part, 0, block_size)
            .await
            .unwrap(),
        stripe
    );
    await_repairs().await;
    assert_eq!(*fixture.writes.lock().unwrap(), vec![1, 3]);
    fixture.unavailable.lock().unwrap().remove(&0);
    assert_eq!(
        tokio::fs::read(root.join("shard-0")).await.unwrap(),
        expected[0]
    );

    tokio::fs::remove_file(root.join("shard-0")).await.unwrap();
    assert_eq!(
        read_verified_cluster_data_stripe(&part, 0, block_size)
            .await
            .unwrap(),
        stripe
    );
    await_repairs().await;
    assert_eq!(*fixture.writes.lock().unwrap(), vec![1, 3, 0]);
    assert_eq!(
        tokio::fs::read(root.join("shard-0")).await.unwrap(),
        expected[0]
    );

    // The cached/range GET path must also carry the exact failed holder into
    // detached repair, not merely return reconstructed response bytes.
    tokio::fs::write(root.join("shard-1"), &corrupt_data)
        .await
        .unwrap();
    let mut cache = VerifiedShardCache::default();
    assert_eq!(
        read_cluster_data_chunk_cached(&part, 1, 0, block_size, &mut cache)
            .await
            .unwrap(),
        expected[1][..block_size]
    );
    await_repairs().await;
    assert_eq!(*fixture.writes.lock().unwrap(), vec![1, 3, 0, 1]);
    assert_eq!(
        tokio::fs::read(root.join("shard-1")).await.unwrap(),
        expected[1]
    );
    for index in [0, 2, 3] {
        assert_eq!(
            tokio::fs::read(root.join(format!("shard-{index}")))
                .await
                .unwrap(),
            expected[index]
        );
    }

    // Do not publish reconstructed bytes that disagree with the immutable
    // recorded whole-shard SHA, even when its individual blocks decode.
    tokio::fs::write(root.join("shard-1"), &corrupt_data)
        .await
        .unwrap();
    let mut bad_metadata = part.clone();
    bad_metadata.shards[1].checksum = format_sha256(&sha256_digest(b"not the recorded shard"));
    assert_eq!(
        read_verified_cluster_data_stripe(&bad_metadata, 0, block_size)
            .await
            .unwrap(),
        stripe
    );
    await_repairs().await;
    assert_eq!(*fixture.writes.lock().unwrap(), vec![1, 3, 0, 1]);
    assert_eq!(
        tokio::fs::read(root.join("shard-1")).await.unwrap(),
        corrupt_data
    );
    assert_eq!(serde_json::to_vec(&part).unwrap(), metadata_before);
    server.abort();
    let _ = server.await;
    tokio::fs::remove_dir_all(root).await.unwrap();
}
