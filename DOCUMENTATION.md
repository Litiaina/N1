# API Documentation Index

This repository now keeps the platform docs split into:

- one platform overview in [README.md](README.md)
- one detailed API reference per runtime

## Read This First

- [README.md](README.md)
  Platform overview, architecture split, deployment orientation, and high-level operational model.

## Runtime API References

- [N1_DOCUMENTATION.md](N1_DOCUMENTATION.md)
  Detailed `N1 3.0.0` storage-kernel reference:
  breaking metadata rewrite, fragment provisioning, auth, upload, read, lifecycle, POSIX, maintenance, request parameters, and example payloads.

- [NS_DOCUMENTATION.md](NS_DOCUMENTATION.md)
  Detailed `NS 1.0.0` NAS Lite deployment reference:
  SMB listener setup, dynamic N1 fragment auth, share roots, staging, capacity behavior, and optional cold sync to `N1`.

- [N2_DOCUMENTATION.md](N2_DOCUMENTATION.md)
  Detailed `N2 0.2.0` cluster-layer reference:
  membership, governance, placement, object APIs, reconcile, and internal peer routes.

- [REGISTRY_DOCUMENTATION.md](REGISTRY_DOCUMENTATION.md)
  Detailed `Registry 0.9.1` user-facing reference:
  auth, admin, fragments, storage sessions, logical objects, shares, trash/recover flows, and example request/response bodies.

## Which Doc To Use

- Use `N1` docs when you are talking directly to the storage kernel.
- Use `NS` docs when you are deploying standalone NAS/SMB file serving.
- Use `N2` docs when you are operating or integrating the clustered storage layer.
- Use `Registry` docs when you are building the user-facing product/API experience.

## Contract Boundary

- `N1`
  Physical storage truth, per-fragment sharded `indexdb` namespace/metadata authority, upload, read, lifecycle, maintenance.
  Stable route namespace: `/n1/...`

- `NS`
  Standalone NAS Lite / SMB file serving, dynamic fragment shares, N1-backed auth, backing-filesystem capacity, and optional cold sync to `N1`.
  Network surface: configured SMB bind address and port.

- `N2`
  Cluster coordination, placement, reconcile, internal peer traffic.
  Current route namespace: `/v4/n2/...`

- `Registry`
  Users, quotas, logical namespace, shares, storage-session brokering, and product-facing metadata.
  Public route namespace: `/v1/...`
