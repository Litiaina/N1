# API Documentation Index

This file is the workspace entry point for API documentation.

Use the specialized references instead of treating the whole workspace as one API:

- [N1_DOCUMENTATION.md](/home/administrator/Development/N1/N1_DOCUMENTATION.md)
  - Stable `1.1.1` documentation for the pure N1 fragment-scoped storage engine.
- [N2_DOCUMENTATION.md](/home/administrator/Development/N1/N2_DOCUMENTATION.md)
  - Current `0.2.0` documentation for the multi-node N2 cluster layer.
- [REGISTRY_DOCUMENTATION.md](/home/administrator/Development/N1/REGISTRY_DOCUMENTATION.md)
  - Current `0.7.0` documentation for the Registry user-owned metadata and POSIX facade.

Recommended reading order:

1. Read [N1_DOCUMENTATION.md](/home/administrator/Development/N1/N1_DOCUMENTATION.md) if you need the stable engine contract.
2. Read [N2_DOCUMENTATION.md](/home/administrator/Development/N1/N2_DOCUMENTATION.md) if you need cluster governance, placement, or peer APIs.
3. Read [REGISTRY_DOCUMENTATION.md](/home/administrator/Development/N1/REGISTRY_DOCUMENTATION.md) if you need users, logical objects, quotas, shares, or storage-session brokering.

Contract boundary:

- `N1` is the authenticated fragment-scoped storage engine.
- `N2` is the multi-node cluster and peer-distribution layer built on the shared N1 core.
- `Registry` is the user-owned metadata, lifecycle, and policy layer above storage.
