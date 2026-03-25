# N2 Multi-Node Deployment Runbook

This document is the operator runbook for `n2` (`litiaina-n2`).

Use this document when:
- you are deploying `N2` across multiple nodes
- you want cluster-specific guardrails, not the single-node `n1` path
- you want the tested 2-node reference layout and verification flow

Use other documents for:
- single-node deployment: [DEPLOYMENT.md](/home/administrator/Development/N1/DEPLOYMENT.md)
- profile split: [DEPLOYMENT_PROFILES.md](/home/administrator/Development/N1/DEPLOYMENT_PROFILES.md)
- HTTP/API reference: [DOCUMENTATION.md](/home/administrator/Development/N1/DOCUMENTATION.md)

## 1. Current Runtime Baseline

As of `2026-03-20`:
- `n2` (`litiaina-n2`) is released at `0.2.0`
- `n1` (`litiaina-n1`) is the stable single-node engine/runtime at `0.7.0`
- `registry-server` is released independently at `0.3.0`
- `n1-arc` remains experimental and is not part of this runbook

Multi-node N2 in this workspace is:
- filesystem-backed on each node
- bidirectional, with every node able to coordinate client-facing requests
- built on the shared N1 core, plus N2 placement, governance, peer traffic, and reconcile

## 2. Startup Guardrails

`litiaina-n2` now fails fast when a deployment is not operator-safe.

Current enforced guardrails:
- `cluster_membership_file` must already exist
- `[n2].internal_token` must not be the template placeholder and must be at least `24` characters
- membership endpoints must be valid `https://host:port` URLs
- duplicate normalized peer endpoints are rejected
- in a multi-node cluster, loopback-only endpoints are rejected:
  - `localhost`
  - `127.0.0.1`
  - `::1`
- in a multi-node cluster, `[server].host` must not be loopback-only
- the local `[n2].node_endpoint` must match the local node entry in membership

Operator quality-of-life guardrail:
- the runtime now auto-creates `n1.env` and `n1_config.ini` compatibility aliases if they are missing beside `n2.env` and `n2_config.ini`
- that keeps the shared core happy without making operators maintain duplicate files by hand

What is still a warning, not a hard failure:
- `mc=0` in a multi-node cluster
- `md=0` on a node with multiple blob roots

Those are valid operator choices, but they reduce redundancy.

## 3. Deployment Identity

N2 is not a “gateway node” model.

Every N2 node is both:
- a client-facing entry node
- and a peer node for other N2 nodes

Practical usage:
- point your CLI, Registry, or load balancer at any healthy N2 node
- that node becomes the coordinator for the request
- peer traffic then uses `/v4/n2/internal/*`
- governance only decides committed cluster state
- object bytes still move directly node-to-node

## 4. Files Per Node

Each node should have a working directory containing:
- `n2.env`
- `n2_config.ini`
- `n2_cluster_membership.json`

The runtime now also manages governance state beside the membership file:
- `n2_cluster_membership.governance-state.json`
- `n2_cluster_membership.governance-log.jsonl`
- `n2_cluster_membership.governance-pending.json` only while a proposal is in flight
- `n2_cluster_membership.epoch-<N>.json` rollback snapshots for committed membership epochs

The runtime now auto-prepares compatibility aliases for:
- `n1.env`
- `n1_config.ini`

So the operator-owned files remain:
- `n2.env`
- `n2_config.ini`
- `n2_cluster_membership.json`

Rollback note:
- governance bootstrap now materializes the current epoch snapshot before the first topology change
- `POST /v4/n2/membership/rollback/{epoch}` restores from those `n2_cluster_membership.epoch-<N>.json` files

## 5. Minimum Safe Config Pattern

Example `n2_config.ini`:

```ini
[server]
host=0.0.0.0
https_port=50001
cert_path=/opt/n2/cert.pem
key_path=/opt/n2/key.pem

[disk_config]
index_store_disks=/srv/n2/index0,/srv/n2/index1
blob_store_disks=/srv/n2/blob0,/srv/n2/blob1
fs_storage_path=/srv/n2/runtime

[n2]
node_id=n2-a
node_endpoint=https://192.168.0.10:50001
cluster_membership_file=n2_cluster_membership.json
internal_token=replace-this-with-a-long-random-secret
cluster_commit_mode=strict_full_placement
auto_maintenance_mode=off
auto_maintenance_max_fragments=64
auto_maintenance_max_objects_per_fragment=256
auto_maintenance_cleanup_orphans=true
auto_cross_params=true
cluster_stripe_block_size=262144
remote_request_timeout_ms=15000
kc=1
mc=1
kd=1
md=1
```

Notes:
- prefer `host=0.0.0.0` or a routable interface address for multi-node clusters
- on low-latency LANs, a lower `remote_request_timeout_ms` usually gives better degraded-mode behavior than the conservative default
- `auto_cross_params=true` is a good default unless you intentionally want fixed EC policy
- `cluster_commit_mode=strict_full_placement` is now the safe default for operator-first clusters
- set `cluster_commit_mode=quorum` only when you explicitly want degraded writes to succeed with less than full planned placement
- `auto_maintenance_mode=off` keeps N2 fully operator-driven
- `auto_maintenance_mode=observe` lets N2 detect redistribution/healing debt on topology changes without mutating data
- `auto_maintenance_mode=repair_and_redistribute` lets N2 automatically run bounded reconcile after membership epoch changes

## 6. Membership File Pattern

Example `n2_cluster_membership.json` for two nodes:

```json
{
  "epoch": 1,
  "nodes": [
    {
      "node_id": "n2-a",
      "endpoint": "https://192.168.0.10:50001",
      "epoch": 1,
      "active": true
    },
    {
      "node_id": "n2-b",
      "endpoint": "https://192.168.0.18:50001",
      "epoch": 1,
      "active": true
    }
  ]
}
```

Rules:
- keep `node_id` unique
- keep `endpoint` unique after normalization
- do not use loopback endpoints for active multi-node peers
- the local node entry must match local `[n2].node_endpoint`

## 7. Bring-Up Sequence

On every node:
1. Create the working directory.
2. Place `n2.env`, `n2_config.ini`, and `n2_cluster_membership.json`.
3. Ensure TLS files exist and match `[server]`.
4. Ensure all index/blob roots already exist and are writable by the service user.
5. Start `litiaina-n2`.

Basic checks after startup:
- `GET /ping`
- `GET /v4/n2/cluster/view`
- `GET /v4/n2/governance/state`
- `GET /v4/n2/placement/{fragment}/{object_key}`
- verify `n2_cluster_membership.epoch-1.json` exists beside the membership file on a fresh cluster

What you want to see:
- `write_ready=true`
- correct `membership_epoch`
- a stable `cluster_fingerprint` across all healthy nodes on the same epoch
- a stable governance `commit_index` across all healthy nodes
- correct `active_nodes`
- placement target count that matches `kc+mc`

## 8. Governance Truth

N2 governance is now consensus-scoped only to cluster state.

That means:
- membership apply and rollback go through majority governance commit
- any healthy node may receive the admin request and can lead that governance operation
- governance only commits cluster truth
- governance does not sit in the object upload, read, delete, rename, or reconcile byte path
- bidirectional object traffic remains direct
- rollback consumes the committed `n2_cluster_membership.epoch-<N>.json` snapshots written beside the active membership file

Relevant operator routes:
- `GET /v4/n2/governance/state`
- `POST /v4/n2/internal/governance/request-vote`
- `POST /v4/n2/internal/governance/append`
- `POST /v4/n2/internal/governance/abort`

## 9. Current Write Truth

Current N2 write semantics are:
- default `cluster_commit_mode` is `strict_full_placement`
- write success now means the planned `kc+mc` target set committed successfully
- `committed_shards` is the immediate placement truth
- `required_commit_shards` in the response shows the actual success threshold for that write

Operator rule:
- keep `cluster_commit_mode=strict_full_placement` for normal multi-node deployments
- switch to `cluster_commit_mode=quorum` only as an explicit operator choice when degraded-write availability matters more than immediate full placement
- still watch `committed_shards` and run `POST /v4/n2/reconcile/{fragment}` after outages, degraded windows, or peer rejoin
- keep `auto_maintenance_mode=off` until you trust the cluster and want bounded automatic healing/redistribution
- use `observe` first, then promote to `repair_and_redistribute` once you are satisfied with the debt reporting
- internal N2 peer traffic now carries both the expected membership epoch and a cluster fingerprint
- peers on stale or drifted cluster views reject internal object/map/session requests instead of silently participating
- under `strict_full_placement`, writes with too few active nodes are rejected before ingest with `400 insufficient active nodes: need <kc+mc>, have <active>`
- some naive streaming clients may surface that early reject as a transport EOF while they are still uploading the body; the authoritative server result is still the `400` response

## 10. Tested 2-Node Reference

The current real-hardware reference pass used:
- `2` nodes
- `2` index roots per node
- `2` blob roots per node
- `kc=1 mc=1 kd=1 md=1`

Reference results from the tested pass:
- `256 MiB` put: `24.86 MiB/s`
- local read: `164.96 MiB/s`
- remote read: `75.79 MiB/s`
- one-node-loss read: `147.45 MiB/s`
- degraded write (`64 MiB`): `28.95 MiB/s`
- rejoin reconcile: `14.34 s`

Current operator model:
- N2 is operational
- N2 is distributed
- full planned placement is the default write guardrail
- reconcile is still the repair path after degraded periods, peer outages, or operator-chosen quorum mode

Current governance validation pass in this workspace:
- isolated 2-node harness on one host with the production `release` binary
- baseline write/read passed through both nodes
- membership apply `epoch 1 -> 2` passed in about `186.56 ms`
- degraded write rejected under `strict_full_placement`
- rollback `epoch 2 -> 1` passed in about `97.72 ms`
- post-rollback write/read passed again

Saved artifact:
- [governance-summary.json](/home/administrator/DEPLOY_TEST/N2-GOV-LOCAL-20260320-210454/governance-summary.json)

Important scope note:
- this governance pass is a real runtime validation, but it is still same-host two-process validation
- cross-machine hardware certification is still recommended before calling the governance layer field-proven

## 11. Recommended Operator Checks

Before production traffic:
- verify all nodes report the same membership epoch
- verify all nodes report the same governance `commit_index`
- verify `last_applied_membership_epoch` matches `membership_epoch`
- verify `write_ready=true` on all active nodes
- create one fragment
- write through node A
- read through node B
- stop one node
- verify degraded read/write behavior
- bring the node back
- run reconcile
- verify placement recovers

This is the cleanest way to turn “configured” into “trusted”.
