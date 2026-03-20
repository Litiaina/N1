# N2 Multi-Node Deployment Runbook

This document is the operator runbook for `distribution-runtime` (`litiaina-n2`).

Use this document when:
- you are deploying `N2` across multiple nodes
- you want cluster-specific guardrails, not the single-node `standard` path
- you want the tested 2-node reference layout and verification flow

Use other documents for:
- single-node deployment: [DEPLOYMENT.md](/home/administrator/Development/N1/DEPLOYMENT.md)
- profile split: [DEPLOYMENT_PROFILES.md](/home/administrator/Development/N1/DEPLOYMENT_PROFILES.md)
- HTTP/API reference: [DOCUMENTATION.md](/home/administrator/Development/N1/DOCUMENTATION.md)

## 1. Current Runtime Baseline

As of `2026-03-20`:
- `distribution-runtime` (`litiaina-n2`) is released at `0.1.2`
- `standard` (`litiaina-n1-standard`) is the stable single-node engine/runtime at `0.6.2`
- `registry-server` is released independently at `0.2.1`
- `arc` remains experimental and is not part of this runbook

Multi-node N2 in this workspace is:
- filesystem-backed on each node
- bidirectional, with every node able to coordinate client-facing requests
- built on the shared N1 core, plus N2 placement, membership, peer traffic, and reconcile

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

## 4. Files Per Node

Each node should have a working directory containing:
- `n2.env`
- `n2_config.ini`
- `n2_cluster_membership.json`

The runtime now auto-prepares compatibility aliases for:
- `n1.env`
- `n1_config.ini`

So the operator-owned files remain:
- `n2.env`
- `n2_config.ini`
- `n2_cluster_membership.json`

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
- `GET /v4/n2/placement/{fragment}/{object_key}`

What you want to see:
- `write_ready=true`
- correct `membership_epoch`
- correct `active_nodes`
- placement target count that matches `kc+mc`

## 8. Current Write Truth

Current N2 write semantics are:
- default `cluster_commit_mode` is `strict_full_placement`
- write success now means the planned `kc+mc` target set committed successfully
- `committed_shards` is the immediate placement truth
- `required_commit_shards` in the response shows the actual success threshold for that write

Operator rule:
- keep `cluster_commit_mode=strict_full_placement` for normal multi-node deployments
- switch to `cluster_commit_mode=quorum` only as an explicit operator choice when degraded-write availability matters more than immediate full placement
- still watch `committed_shards` and run `POST /v4/n2/reconcile/{fragment}` after outages, degraded windows, or peer rejoin

## 9. Tested 2-Node Reference

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

## 10. Recommended Operator Checks

Before production traffic:
- verify all nodes report the same membership epoch
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
