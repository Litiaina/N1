# LINK operator guide

This guide covers the unreleased N1 1.0.0-RC.2 engine. Read
[ARCHITECTURE.md](ARCHITECTURE.md), [NOA.md](NOA.md) and the
[implementation ledger](docs/RC2-IMPLEMENTATION.md) for the contract and remaining
qualification work. The previous RC.1 guide is available in Git at `061a40a`.
No old storage-format migration is provided.

## Explicit setup

The member roots must already exist, be empty and belong to independent
filesystems. This example uses an illustrative 1D+1P pool; substitute actual
operator-selected paths. Enrollment persists immutable pool markers and private
service files. It must not run against an existing data root.

```sh
litiaina-n1 init --config /srv/n1/service.json \
  --member /data/disk-a/n1 --member /data/disk-b/n1 \
  --data-shards 1 --parity-shards 1 --native-address 127.0.0.1:19001
litiaina-n1 check --config /srv/n1/service.json
litiaina-n1 serve --config /srv/n1/service.json
```

Init writes a private signing key and client credential file beside the service
configuration. Preserve the signing key, stable fragment IDs, immutable pool
descriptors and repository keys separately. They are not recoverable from a
client key or a shard. Service-created encrypted recovery points also preserve
configuration, signing identity and configured TLS material. Keep configuration,
keys and credentials owner-only.
An interrupted init can leave enrolled markers; do not erase them or silently
retry with a different pool identity.

Native TLS requires `--tls-certificate` and `--tls-key` together at init.
A configured HTTP address is optional. Remote clients should use verified TLS;
private certificate authorities can be supplied with client `--ca`. Native
clients do not route through the HTTP adapter.

## Client operations

The native CLI loads a private credential file containing a native URL and
fragment credentials or a bearer token. `--endpoint` can explicitly override its
native URL. Keys below are values returned by N1, retained by the client.

```sh
litiaina-n1-shell --credentials /srv/n1/service.credentials.json put ./payload.bin
litiaina-n1-shell --credentials /srv/n1/service.credentials.json batch ./one.bin ./two.bin
litiaina-n1-shell --credentials /srv/n1/service.credentials.json put ./large.bin \
  --part-bytes 16777216 --durable-parts
litiaina-n1-shell --credentials /srv/n1/service.credentials.json status --upload-key N1U1.EXAMPLE
litiaina-n1-shell --credentials /srv/n1/service.credentials.json finalize --upload-key N1U1.EXAMPLE
litiaina-n1-shell --credentials /srv/n1/service.credentials.json get --key N1A1.EXAMPLE --output ./recovered.bin
litiaina-n1-shell --credentials /srv/n1/service.credentials.json health --key N1A1.EXAMPLE --deep
litiaina-n1-shell --credentials /srv/n1/service.credentials.json repair --key N1A1.EXAMPLE
litiaina-n1-shell --credentials /srv/n1/service.credentials.json delete --key N1A1.EXAMPLE
```

`N1U1.EXAMPLE` and `N1A1.EXAMPLE` are placeholders, not valid keys. Part size must
respect the pool's EC stripe geometry; `put --part-bytes` rounds its requested
size to that geometry. Standard parts may require retransmission after a crash;
durable parts acknowledge only after their persistence barriers. Finalization
is durable and idempotent for the same immutable identity.

Download output uses exclusive creation and never overwrites a local file.
A failed transfer can leave a partial output for inspection. Keep original
client-held keys in the separate metadata system or another client-owned record;
N1 cannot enumerate or reconstruct that record for you.

Delete permanently retires the key, including still-valid upload tickets. Partial
retirement requires retry and blocks reads or recreation. There is no rename,
object version history, trash or undelete API.

## Optional maintenance

Configuration `maintenance.gc` and `maintenance.scrub` are absent/disabled by
default. Each configured interval has `every_seconds` and `start_delay_seconds`;
scrub also has `options` with `repair`, `bytes_per_second` and `window_seconds`.
GC touches only confidently expired private uploads. It never expires final
payload or retirement state.

Operator CLI commands `gc`, `scrub`, `job` and `stop --job-id ...` start and inspect
maintenance. A stopped or incomplete scrub reports job status and leaves N1
available. Rate and time limits apply between bounded work units; an already
blocked device request cannot be interrupted safely. Foreground access can
preempt background work on the same object.

For an optional UTC maintenance window, configure `maintenance.calendar_scrub`
instead of the interval scrub. For example:

```json
"calendar_scrub": {
  "schedule_utc": "Sun 02:00",
  "initial_delay_seconds": 300,
  "options": {"repair": true, "bytes_per_second": 16777216, "window_seconds": 3600}
}
```

Only the configured minute starts a job; missed windows do not cause a boot-time
catch-up scrub. The private journal prevents repeat admission in the same slot.
An interrupted scrub is reported incomplete after restart. Read repair operates
independently of this schedule.

`concurrency` preserves N1's adaptive controller. Its defaults enable adjustment
every 1,000 ms, up to 128 streams, 256 KiB chunks and a 512 KiB stream buffer.
`max_operations` and `operation_memory_bytes` remain hard admission ceilings.
Runtime environment overrides remain `N1_RUNTIME_WORKER_THREADS`,
`N1_RUNTIME_MAX_BLOCKING_THREADS` and `N1_RUNTIME_THREAD_STACK_BYTES`.
Use `N1_IO_BACKEND=uring` when fallback is unacceptable; `auto` reports any
startup fallback and `blocking` explicitly selects the blocking backend.

The private configuration contains immutable pool member IDs. Never regenerate
them when a disk is missing. `state_dir` optionally selects an owner-only control
directory outside member roots; its default is beside the signing key. It holds
the instance lease, refresh sessions and the calendar journal.

## Recovery repositories

Create a separate owner-only encrypted repository and retain its key externally:

```sh
litiaina-n1-backup keygen --output /recovery/n1.key
litiaina-n1-backup init --repository /recovery/points --key /recovery/n1.key
```

Before service startup, configure an alias in `service.json`:

```json
"repositories": {
  "archive": {"path": "/recovery/points", "key_file": "/recovery/n1.key"}
}
```

Repository paths must be outside every active member root. Operator requests
accept only these configured aliases. Start online capture and poll its job:

```sh
litiaina-n1-shell --credentials /srv/n1/service.credentials.json backup capture --repository archive
litiaina-n1-shell --credentials /srv/n1/service.credentials.json backup job
litiaina-n1-shell --credentials /srv/n1/service.credentials.json backup list --repository archive --limit 64
litiaina-n1-shell --credentials /srv/n1/service.credentials.json backup verify --repository archive --snapshot POINT_ULID
```

A job start acknowledges admission. Check completed/stopped/incomplete status
before using its result. `backup stop --job-id ...` requests cooperative stop;
repository leases remain owned until accepted work finishes. Requested/scoped
tokens cannot operate recovery jobs even when they originate from an operator.

Online capture pins only committed shards and retirement records. Upload bodies
and reads can continue; publication briefly waits for the pin boundary. Offline
capture additionally preserves private multipart progress and requires the
service to be stopped so the helper can acquire all member leases:

```sh
litiaina-n1-backup capture --repository /recovery/points --key /recovery/n1.key \
  --config /srv/n1/service.json
litiaina-n1-backup verify --repository /recovery/points --key /recovery/n1.key --snapshot POINT_ULID
```

Restore is offline into explicit empty, independent targets, one per saved root
in descriptor order. `--target` repeats for each target. Failed recovery keeps
startup fences; `--resume` accepts only the same point, root assignment and key.
Older-point restore overlays retirement state known to any available repository
point. A deletion absent from all surviving backups cannot be recovered after
all original members are lost. Retention refuses to remove the last saved
retirement witness, even if its point is outside the requested keep count.

```sh
litiaina-n1-backup restore --repository /recovery/points --key /recovery/n1.key \
  --snapshot POINT_ULID --target /recovery/disk-a/n1 --target /recovery/disk-b/n1 \
  --output-config /srv/recovered/service.json
```

The output parent must already exist. Recovery writes private signing/TLS files
and configuration without overwriting operator edits. It rewrites physical roots
while preserving member identities and client-held keys. Fresh login sessions
are created when the recovered service starts.

Local `replicate` can use a different destination encryption key. Network
`replicate-remote` accepts a native endpoint or secondary HTTP endpoint and a
configured destination alias; network peers share a repository key. Transfer
retries authenticate and reuse existing blocks and never publish an incomplete
point. This transfers recovery points, not live failover state.

The retained optional backup supervisor uses a private JSON job configuration
and `litiaina-n1 backup run-job --config /etc/litiaina/n1/backup-job.json`.
The shipped systemd timer/recovery units are opt-in. Restart intent is persisted
before stopping N1; the engine restarts before long verification/replication.
Failed transfers retry the same pending point. `backup recover-job --state-dir`
uses that durable restart intent even if the job configuration changed.
The standalone helper exposes the same operations. An `online_source` can select
native/HTTP capture without stopping N1; its `token_file` is a private client
credentials JSON file.

Offline pool operations are `litiaina-n1 pool verify`, `prepare-copy`, `clone`
and `verify-copy`. Clone requires `--config`, `--pool`, `--member` (zero-based
logical slot) and an empty `--copy-root` on another filesystem. Stop the engine
first; source and destination leases prevent concurrent use. Failed copies stay
fenced; `--resume` accepts only the same owned operation and refuses changed bytes
or extra entries. After successful verification, change that slot's configured
physical root; keep its immutable member ID.

For one failed member, stop N1, mount an empty replacement on an independent
filesystem and change only that slot's configured physical root. Keep the
registered member IDs, pool ID, generation and EC geometry unchanged. Run
`litiaina-n1 pool replace --config <config> --pool <pool-id> --member <slot>
--authorize`. Replacement takes the control, source and destination leases;
normal startup never adopts the blank root. `--resume --authorize` accepts only
the same fenced operation. This path requires parity and every other member.
It verifies source files, reconstructs payload through the pinned codec, checks
the integrity root and verifies the complete expected inventory before activation.
Conflicts, corrupt sources or unexpected files prevent activation. Source shards
are not rewritten. Valid permanent retirements are preserved; an incomplete
retirement stays incomplete and requires deletion retry. Acknowledged durable
parts survive reconstruction, while holes remain missing. Sparse private uploads
use filesystem data extents when available, with conservative sequential
fallback. There is no automatic failed-disk enrollment or replacement.

The shell forwards `pool ...` and `backup offline ...` to the matching adjacent
engine, with optional `--engine-binary`. These commands require no network login.
Use `--help` on the engine or backup helper for exact flags. Installer/UI
adaptation and remaining administration/qualification work are in the ledger.
