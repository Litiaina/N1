# API Documentation Index

This repository now keeps the platform docs split into:

- one platform overview in [README.md](/home/administrator/Development/N1/README.md)
- one detailed API reference per runtime

## Read This First

- [README.md](/home/administrator/Development/N1/README.md)
  Platform overview, architecture split, deployment orientation, and high-level operational model.

## Runtime API References

- [N1_DOCUMENTATION.md](/home/administrator/Development/N1/N1_DOCUMENTATION.md)
  Detailed `N1 2.0.1` storage-kernel reference:
  fragment provisioning, auth, upload, read, lifecycle, POSIX, maintenance, request parameters, and example payloads.

- [N2_DOCUMENTATION.md](/home/administrator/Development/N1/N2_DOCUMENTATION.md)
  Detailed `N2 0.2.0` cluster-layer reference:
  membership, governance, placement, object APIs, reconcile, and internal peer routes.

- [REGISTRY_DOCUMENTATION.md](/home/administrator/Development/N1/REGISTRY_DOCUMENTATION.md)
  Detailed `Registry 0.8.0` user-facing reference:
  auth, admin, fragments, storage sessions, logical objects, shares, trash/recover flows, and example request/response bodies.

## Which Doc To Use

- Use `N1` docs when you are talking directly to the storage kernel.
- Use `N2` docs when you are operating or integrating the clustered storage layer.
- Use `Registry` docs when you are building the user-facing product/API experience.

## Contract Boundary

- `N1`
  Physical storage truth, standalone namespace, upload, read, lifecycle, maintenance.
  Stable route namespace: `/n1/...`

- `N2`
  Cluster coordination, placement, reconcile, internal peer traffic.
  Current route namespace: `/v4/n2/...`

- `Registry`
  Users, quotas, logical namespace, shares, storage-session brokering, and product-facing metadata.
  Public route namespace: `/v1/...`
