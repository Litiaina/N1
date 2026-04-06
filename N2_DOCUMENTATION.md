# Litiaina N2 Documentation

Current `0.2.0` reference for the multi-node N2 cluster layer.

Status:
- `n2` is not the stable engine contract. `N1` owns the stable single-node engine surface.
- N2 adds cluster membership, placement, peer transport, governance, and reconcile behavior on top of shared N1 core code.
- Public and protected N2 routes still live under `/v4/n2/*`.

## 1. Scope

N2 owns:
- cluster membership
- governance state
- placement resolution
- multi-node object CRUD
- fragment reconcile
- peer-to-peer internal shard/session/map traffic

N2 does not replace Registry:
- no user accounts
- no share policy
- no business metadata ownership

## 2. Base URL And Auth

Base URL example:
- `https://127.0.0.1:50001`

Protected N2 routes require the same fragment bearer token model used by shared N1 auth middleware.

Typical requirement:
- read routes require `read=true`
- mutating cluster routes require `modify=true`
- admin-sensitive cluster changes expect the authenticated fragment to be the N1 admin fragment

Public route:
- `GET /v4/n2/cluster/view/public`

## 3. Public Routes

### `GET /v4/n2/cluster/view/public`

Returns the current public cluster view without requiring a bearer token.

Use this for:
- cluster liveness display
- peer discovery diagnostics
- cluster epoch visibility

## 4. Protected External Routes

### 4.1 Cluster And Governance

- `GET /v4/n2/cluster/view`
- `GET /v4/n2/governance/state`
- `GET /v4/n2/membership`
- `PUT /v4/n2/membership`
- `POST /v4/n2/membership/rollback/{epoch}`

Notes:
- membership apply and rollback are admin operations
- governance tracks committed cluster state, not all object traffic

### 4.2 Placement

- `GET /v4/n2/placement/{fragment}/{*object_key}`

Purpose:
- resolve target node and placement decisions for a specific logical object identity
- inspect current cross-node shard planning

Common query parameters:
- `uid`
- `version_id`
- `kc`
- `mc`
- `kd`
- `md`

### 4.3 N2 Object Read Surface

- `GET /v4/n2/object/list/{fragment}`
- `GET /v4/n2/object/versions/{fragment}/{*object_key}`
- `GET /v4/n2/object/{fragment}/{*object_key}`

These are the multi-node equivalents of direct object listing and retrieval.

### 4.4 N2 Object Mutation Surface

- `POST /v4/n2/object/{fragment}/{*object_key}`
- `DELETE /v4/n2/object/{fragment}/{*object_key}`
- `POST /v4/n2/object/rename`
- `POST /v4/n2/reconcile/{fragment}`

Purpose:
- coordinated cluster writes
- coordinated cluster deletes
- logical rename at cluster level
- reconcile a fragment after membership or placement drift

## 5. Internal Peer Routes

These are node-to-node routes only. They are not for public clients.

### 5.1 Internal Shard Sessions

- `POST /v4/n2/internal/session/begin/{fragment}/{version_id}/{cluster_shard_index}`
- `PUT /v4/n2/internal/session/chunk/{session_id}`
- `POST /v4/n2/internal/session/commit/{session_id}`
- `POST /v4/n2/internal/session/abort/{session_id}`

Used for:
- staged remote shard upload between peers
- bounded chunk transfer before final commit

### 5.2 Direct Internal Shard Operations

- `POST /v4/n2/internal/store/{fragment}/{version_id}/{cluster_shard_index}`
- `GET /v4/n2/internal/read/{fragment}/{version_id}/{cluster_shard_index}`
- `DELETE /v4/n2/internal/delete/{fragment}/{version_id}/{cluster_shard_index}`

Used for:
- direct shard persistence
- remote shard reads
- remote shard cleanup

### 5.3 Internal Object Map Operations

- `POST /v4/n2/internal/map/upsert`
- `GET /v4/n2/internal/map/{fragment}`
- `DELETE /v4/n2/internal/map/{fragment}`

Used for:
- shard-map persistence
- current map inspection
- fragment map cleanup

### 5.4 Internal Governance Routes

- `POST /v4/n2/internal/governance/request-vote`
- `POST /v4/n2/internal/governance/append`
- `POST /v4/n2/internal/governance/abort`

Used for:
- governance leader election and commit
- log append
- proposal abort

## 6. Internal Peer Authentication

Internal N2 peer routes require:
- `x-n2-internal-token`

Cluster-proof headers used by the runtime:
- `x-n2-expected-epoch`
- `x-n2-cluster-fingerprint`

Operational rule:
- treat all `/v4/n2/internal/*` routes as private peer transport only
- do not expose them as a user-facing contract behind Registry or UI

## 7. Common Responses

Representative response families in N2 include:
- cluster view payloads
- governance state payloads
- list/version/object summaries
- reconcile responses
- internal session acks such as:
  - `InternalShardSessionResponse`
  - `InternalShardChunkAck`
  - `InternalShardAbortResponse`
  - `InternalMapUpsertResponse`
  - `InternalMapDeleteResponse`
  - `InternalShardDeleteResponse`

## 8. Operational Notes

N2 current design expectations:
- every node can be client-facing
- governance commits cluster state, not every object byte
- strict placement and quorum behavior depend on cluster config
- internal transport must prove the current cluster view to avoid stale peers participating

## 9. Related Docs

- [DOCUMENTATION.md](/home/administrator/Development/N1/DOCUMENTATION.md)
- [N1_DOCUMENTATION.md](/home/administrator/Development/N1/N1_DOCUMENTATION.md)
- [REGISTRY_DOCUMENTATION.md](/home/administrator/Development/N1/REGISTRY_DOCUMENTATION.md)
- [N2_DEPLOYMENT.md](/home/administrator/Development/N1/N2_DEPLOYMENT.md)
- [REDISTRIBUTION.md](/home/administrator/Development/N1/REDISTRIBUTION.md)
