# N2 durable archive exports

N2 supports background ZIP64 and TAR exports through the same NOA job API used
by Console for N1. Use this API for new integrations; the older
`/objects/archive` streaming endpoints remain compatibility routes and do not
provide durable jobs or resumable artifacts.

## API

All job-management routes require a fragment token with read permission. Jobs
are scoped to that fragment, not to arbitrary paths on the coordinator.

| Method | Route | Purpose |
| --- | --- | --- |
| POST | `/noa/v1/exports` | Accept a background job (202) |
| GET | `/noa/v1/exports/{id}` | Phase, progress, error, artifact size and ETag |
| DELETE | `/noa/v1/exports/{id}` | Cancel/revoke the job |
| GET | `/noa/v1/exports/{id}/download` | Download a ready artifact; supports Range |
| POST | `/noa/v1/exports/{id}/signed-url` | Mint a bounded browser-download capability |
| GET | `/noa/v1/signed-exports/download` | Download using that signed capability |

Example creation body:

```json
{"keys":["reports/summary.csv"],"prefixes":["records"],"format":"zip","filename":"records.zip"}
```

Use `export_all:true` to capture the fragment's live namespace. `entries` can
instead map `object_key`, `archive_path`, optional `version_id`, `offset_bytes`,
and `length_bytes`; mapped entries cannot be mixed with ordinary selections.
There are at most 10,000 explicit keys/mapped entries and 1,024 prefixes per
request. Prefix selection is paginated and deduplicated. Internal state cannot
be exported. ZIP is not silently changed to TAR for folder/large selections;
an oversized ZIP directory can instead fail its memory admission check.

Phases: `queued`, `capturing_manifest`, `building`, `ready`, `failed`, `cancelled`.
Missing, corrupt, or unreadable selected source versions fail the entire job.
No partial artifact is downloadable. Ready downloads have an exact length,
SHA-256 ETag, byte ranges (206/416), and If-Range support. Signed URLs expire no
later than the artifact; keep them private like other bearer capabilities.

## Ownership, retention, and failure behavior

- Each ID binds a logical node, its physical instance, and a unique job ID.
  Gateway derives routing from this identity and never substitutes another
  node for status, cancellation, signed-URL minting, or artifact download.
  Direct clients must continue using the coordinator that accepted the job.
  Gateway node IDs must match the membership's logical slot IDs.
- State and immutable NDJSON manifests live beneath runtime_path in
  `.n2-exports`. Artifact chunks live under `.n2-export-artifacts` on enrolled
  local storage roots. These are temporary derived data, not authoritative
  objects and **not EC-replicated export state**.
- Restarting the original instance repeats interrupted capture, or rebuilds
  an interrupted artifact from the completed manifest. Ready artifacts remain
  downloadable until expiry. Loss of the coordinator or its runtime/artifact
  storage is not automatic failover: wait for it to return or create a new job.
  A replacement physical instance cannot impersonate the original owner.
- Before capture/resume, every current cluster member must durably acknowledge
  a source-retention pin. `N2_CLUSTER_SECRET` must be configured. An unavailable
  member prevents a new/resumed build; already-ready downloads need only their
  coordinator and artifact roots. This conservative first implementation
  protects the entire fragment against TTL reclamation while building.
- Pins in `.n2-export-pins` survive holder restarts and serialize with physical
  holder-side purge. Completion/cancellation releases them best-effort; expiry
  clears abandoned pins. Unreadable/corrupt pin records block reclamation.
- Capture/build must finish within the creation lease (`retention_seconds`).
  Ready artifacts then get a fresh retention interval. A membership or binding
  change during a build causes explicit failure, not automatic migration.
- Active downloads hold a cleanup lease. Cancel/expiry prevents new downloads;
  an already-started download may finish before chunks are removed.

This is an archive export facility, **not a cluster backup/restore system**.
It does not capture credentials, membership, historical trash, or application
databases. Administrative fragment removal is not an export-preservation API.

## Configuration

Add `[export]` to `n2_config.ini`; existing installations work with defaults.
No new secret file is required: existing `n2.env` supplies peer authentication
and `SIGNED_READ_URL_SECRET` (or `JWT_SECRET`) for download signing.

```ini
[export]
retention_seconds=86400
cleanup_interval_seconds=60
max_concurrent_jobs=2
max_queued_jobs=128
max_concurrent_downloads=32
artifact_chunk_size_bytes=16777216
artifact_write_concurrency=4
max_buffered_bytes_per_job=134217728
max_buffered_bytes_global=268435456
max_retained_bytes=0
```

`max_retained_bytes=0` means no retained-artifact quota; set a nonzero byte
budget for production capacity planning. Manifest validation and object-read
concurrency default to twice available parallelism, clamped to 2–32; override
with `manifest_validation_concurrency` and `object_read_concurrency`.
Archive staging buffers have per-job/global admission. Existing distributed EC
verification/reconstruction workspaces and source metadata are additional
memory; these limits are not a whole-process RSS cap.

Gateway keeps the normal total timeout for short requests. Export-download
bodies instead use `request_timeout_seconds` as a connect/read-idle limit, so
a progressing large download is not cut off after five minutes.

Upgrade engine, gateway, and Console together. Search and backup parity are
separate work; this change does not enable global N2 namespace search.

## Verification

```sh
cargo test -p n2 -p n2_gateway
python3 scripts/proof/n2_export_proof.py --engine /path/to/litiaina-n2 \
  --gateway /path/to/litiaina-n2-gateway --out /path/to/isolated-evidence
```

The proof starts two loopback engine processes (EC 1+1) and a gateway in fresh
directories, validates archives/ranges/signatures/failure/cancellation, kills
the owner during building, restarts a holder to test durable GC fencing, then
checks restart recovery, cross-fragment isolation, cancellation across owner
restart, and natural retention. It kills only its child PIDs
and retains evidence. One-host directories are functional evidence, not
multi-host/disk/power-loss qualification. No production service is involved.
Recorded results and qualification limits are in [TESTING.md](TESTING.md).
