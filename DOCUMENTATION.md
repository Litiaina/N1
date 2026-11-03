# N1 operator guide

This is the operator guide for N1 with LINK and the optional metadata engine.
RC.2 is in development. Server settings use INI; secrets come from a private
file or the service environment. No migration from earlier development configurations
or storage formats is required or promised.

For a named N1 deployment, the connection path is:

`Console / named client → metadata_engine → native LINK object_engine → shards`

A client that owns its metadata can use `object_engine` directly. The table below
uses object ports 19001/19002 and public metadata ports 50001/50002; these are
examples, all editable under each service's `[server]` section.

| Setting | Object engine | Metadata engine |
| --- | --- | --- |
| HTTP(S) listener | Internal 19001 | Public 50001; Console connects here |
| Native listener | Internal 19002, `n1/1` | Public 50002, named `n1/0.5` |
| Pool roots | Owns `[storage_pool.NAME]` | No member roots |
| Private state | Pool identities, signing key, control state | Database, named uploads and exports |

## Prepare and start the services

Build `n1` and `n1_metadata_engine` from the workspace. Give each service its
own configuration and control directories, with separate secrets. Starting the
object binary without a prepared configuration creates templates and stops;
complete them before retrying. Both services require `JWT_SECRET`,
`AUTH_KEY`, `MASTER_KEY` and `ADMIN_KEY` with real private values. Metadata's
object-service credential must match the selected object's `AUTH_KEY`; its
logical user-fragment credentials are managed separately.

The object INI example below uses internal ports 19001/19002. Standalone
LINK deployments can choose their own ports. Start it with:

`litiaina-n1 serve --config /etc/litiaina/object/n1_config.ini`

Configure metadata as shown in the next section and run:

`litiaina-n1-metadata serve --config /etc/litiaina/metadata/n1_config.ini`

Point the original Console at metadata's HTTPS address, not at LINK's internal
object listener. Use certificates valid for the actual client/server names.
`n1s://` verifies TLS for native connections as well.

## Object configuration and pool enrollment

The default server configuration is `n1_config.ini`, with private credentials in
an optional adjacent `n1.env`. `[server] env_file` selects a different secrets
file; the service environment can supply credentials without one. Running
`litiaina-n1` in its service directory, or passing
`--config /etc/litiaina/object/n1_config.ini`, uses that configuration. A first
run creates missing templates and stops until the operator supplies credentials
and complete storage roots. It never enrolls a nonempty unknown root.

```ini
[server]
host=127.0.0.1
https_port=19001
native_port=19002
native_max_connections=32
cert_path=cert.pem
key_path=key.pem

[disk_config]
fs_storage_path=/var/lib/litiaina/object-control

[storage_pool.A]
generation=1
roots=/mnt/storage-00/n1,/mnt/storage-01/n1
data_shards=1
parity_shards=1
sealed=false
```

Edit `[server]` to change the bind IP, ports, certificate or key; restart the
service to apply it. Certificate and key paths must be supplied together. Empty
paths explicitly select plaintext for local testing. Existing pool identities,
signing keys and member markers are retained across these edits.

The original `[upload_config]`, `[io_control]`, `[rate_limiter_config]`,
`[garbage_collector]`, `[auto_healing]`, `[export]` and token lifetime settings
retain their INI names. They control concurrency, adaptive admission, memory,
streaming, durability, I/O admission, rate quotas, cleanup and optional healing.
Automatic full scrubs require an explicitly enabled UTC calendar; ordinary
repair on read remains independent. A scheduled scrub has no whole-scan timeout.
The object health response reports effective operational controls.

To expand, create empty N1 directories on the new independent filesystems and
append a **complete** `[storage_pool.B]` section with its own roots and EC width.
Restart the object service. New uploads use the new eligible pool; existing
LINK keys retain their original pool, generation and EC topology. Do not edit an
enrolled pool's generation, width or identities. Seal a pool to stop allocation;
do not remove its section while its objects must remain accessible. Replacing a
member is an explicit offline recovery operation, not enrollment of an empty
directory at a previously used path.

Enrollment persists an operator journal under `fs_storage_path`. It contains
pool/member identities and service credentials, not an object catalogue. The
control lease fences enrollment against an active server. Interrupted enrollment
resumes its recorded identities; missing signing keys for established storage
are rejected rather than regenerated.

## Optional named metadata service

The object service works independently over native NOA v2. Named N1 clients,
including the existing Console, connect to `litiaina-n1-metadata`. Its original
NOA v1 HTTP endpoints and named native operations resolve names and versions,
then stream bytes through the object's native interface. The Console's product
layout and workflows are retained.

Each independently running service has its own INI, environment file, control
directory and listening ports. In the metadata INI, `[server]` controls the
public client address and TLS exactly as above. `[disk_config]` selects metadata
control storage. Metadata does not enroll disks or require pool-root sections;
it obtains immutable pool profiles from the object service.

For a named N1 deployment, put the public ports on metadata:
`https_port=50001` and `native_port=50002`. Assign the independent object process
different local ports in its own `[server]` section, for example HTTP 19001 and
native 19002. Console connections use that public address;
metadata's `object_native_url` points to the internal object listener. A direct
LINK-only deployment can choose its own object listener ports.

```ini
[server]
host=127.0.0.1
https_port=50001
native_port=50002
native_max_connections=32
cert_path=/etc/litiaina/metadata/cert.pem
key_path=/etc/litiaina/metadata/key.pem

[disk_config]
fs_storage_path=/var/lib/litiaina/metadata/control

[metadata_engine]
object_native_url=n1s://127.0.0.1:19002
object_fragment=root
object_secret_env=AUTH_KEY
object_ca_path=/etc/litiaina/object-ca.pem
data_dir=/var/lib/litiaina/metadata/database
staging_dir=/var/lib/litiaina/metadata/uploads
object_connections=32
workers=64
```

The referenced environment secret authenticates the metadata service to LINK.
Logical fragment secrets, permissions, refresh sessions, names, versions, upload
coordination and trash live in metadata. Its database and staging directories
must be real owner-only directories. LINK tickets remain private to this service.

```sh
litiaina-n1-metadata serve --config /etc/litiaina/metadata/n1_config.ini
```

An external metadata implementation can use the object interface directly.
The object engine requires neither this metadata process nor its database.

## Direct LINK client operations

The LINK native CLI loads a private client credential file containing a native
URL and object-service fragment credentials or a bearer token. For example:

```json
{"native_url":"n1s://storage.example:19002","fragment":"root","secret":"OBJECT_AUTH_KEY"}
```

Save this as an owner-only client file (for example `chmod 600 object-client.json`).
This client file is not a server configuration or a migration format. Use the
shell's `--ca` option for a private CA. `--endpoint` can override the native URL.
Keys below are values returned by LINK, retained by the client.

```sh
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json put ./payload.bin
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json batch ./one.bin ./two.bin
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json put ./large.bin \
  --part-bytes 16777216 --durable-parts
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json status --upload-key N1U1.EXAMPLE
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json finalize --upload-key N1U1.EXAMPLE
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json get --key N1A1.EXAMPLE --output ./recovered.bin
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json health --key N1A1.EXAMPLE --deep
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json repair --key N1A1.EXAMPLE
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json delete --key N1A1.EXAMPLE
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
object version history, trash or undelete API in the object engine. Metadata
provides the named version/trash operations used by Console.

## Optional maintenance

Change the existing INI controls and restart the relevant service:

| Control | INI section and keys |
| --- | --- |
| Bind IP, ports and TLS | `[server]`: `host`, `https_port`, `native_port`, `cert_path`, `key_path` |
| Native connection limit | `[server] native_max_connections` |
| Upload I/O and memory | `[upload_config]`: `max_concurrent_io`, `upload_memory_budget_bytes`, `writer_buffer_size` |
| Adaptive concurrency | `[upload_config]`: `adaptive_concurrency_enabled`, `adaptive_concurrency_interval_ms`, `stream_max_concurrency` |
| Stream buffers | `[upload_config]`: `stream_chunk_size`, `stream_readahead_bytes` |
| Upload policy | `[upload_config]`: `max_part_size`, `stripe_block_size`, `upload_part_strict_durability_default`, `upload_io_gate_enabled` |
| Foreground wait/stall diagnostics | `[io_control]`: `foreground_wait_timeout_ms`, `stall_threshold_ms`, `slow_io_threshold_ms` |
| Object staging cleanup | `[garbage_collector]`: `cleanup_interval_seconds`, `file_ttl_seconds` |
| Named trash retention | `[garbage_collector] trash_retention_seconds`; metadata reclamation remains a completion gate |
| Scheduled healing | `[auto_healing]`: `enabled`, `scrub_schedule_utc`, `initial_scan_delay_seconds`, `scheduled_scrub_pause_millis` |
| Repair worker bounds | `[auto_healing]`: `object_scan_concurrency`, `shard_repair_concurrency`, `metadata_batch_size` |
| Rate quotas | `[rate_limiter_config]`: `enable_rate_limiting` and the existing rate/burst keys |
| Optional file size ceiling | `[set_limits] max_file_size`: `unlimited` (default), or a positive byte count |
| Export bounds | `[export]`: retention, job, download and buffer controls on metadata |

Values accepting `auto` retain their host-derived defaults. Pool geometry and
`stripe_block_size` must match enrolled immutable layouts; they are not tuning
knobs for changing already stored objects. Operational tuning does not alter
existing LINK identities.

There is no configured total-object size cap by default. In **both** engines'
`n1_config.ini`, omit `max_file_size` or set:

```ini
[set_limits]
max_file_size=unlimited
```

A positive byte count enables an operator cap; `0` is invalid. Restart the
component after changing its INI. The metadata and object engines enforce their
caps independently, so either finite cap can reject a named upload.
`[upload_config] max_part_size` limits each multipart part, not the whole file.
Large objects use multiple parts with bounded buffers; removing the total-size
policy cap does not remove part-count, LINK representation, memory or I/O
admission checks. For example, a 1 TiB object using 32 MiB parts has 32,768 parts.

Each engine accepts an arbitrary INI filename through `--config`. For a shared
configuration directory, use distinct files such as `object.ini` and
`metadata.ini`, with `[server] env_file=object.env` and `env_file=metadata.env`
respectively. Relative secrets paths resolve beside the selected INI. An
explicit secrets file must exist, be regular, and have owner-only permissions;
symlinks are rejected. Do not put these files in source control.

When `env_file` is omitted, an owner-only `n1.env` beside the INI is read if
present. It is optional when the service supplies all required secrets through
the process environment. Exported values override matching file values. A bad
or insecure file still fails startup; it is not silently ignored.

For systemd-managed secrets, `LoadCredential=` or `LoadCredentialEncrypted=`
can supply a private dotenv-format credential file. Point `env_file` at that
service's absolute credential path (normally `/run/credentials/<unit>/<id>`);
N1 reads it directly without exporting the secrets to child processes. Keep
object and metadata credentials separate. Raw `Environment=` entries are not
a secret store; see [systemd's credential guidance](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html).
The object systemd installer accepts an arbitrary `--config-file`. Use
`--no-env-file` when N1 reads `[server] env_file` directly or the service obtains
credentials another way; the installer then omits its `EnvironmentFile=` line.

Staging GC runs at the configured cleanup interval; the generated INI uses
3,600 seconds. It conservatively reclaims expired unpublished uploads, never
committed objects or retirement records. Scheduled healing requires both an
enabled flag and an explicit UTC calendar, for example:

```ini
[auto_healing]
enabled=true
scrub_schedule_utc=Sun 02:00
initial_scan_delay_seconds=300
scheduled_scrub_pause_millis=25
object_scan_concurrency=1
shard_repair_concurrency=1
metadata_batch_size=128
```

Use `enabled=false` when no scheduled scrub is wanted. The calendar starts work
at the configured UTC minute; it does not impose a whole-scan deadline or run a
missed-slot catch-up scan at boot. A persisted slot journal prevents duplicate
starts. Manual job stop is cooperative, and stopped/incomplete healing does not
by itself mean foreground object storage has failed. Repair on read remains
independent of scheduled healing.

The LINK shell's `gc`, `scrub`, `job` and `stop --job-id ...` commands control
manual object maintenance. Foreground work can preempt background work at
bounded work boundaries. An already blocked kernel/device request cannot be
safely interrupted. Object health reports the effective runtime controls.

Runtime environment overrides are `N1_RUNTIME_WORKER_THREADS`,
`N1_RUNTIME_MAX_BLOCKING_THREADS` and `N1_RUNTIME_THREAD_STACK_BYTES`.
`N1_IO_BACKEND=auto` selects the backend with reported fallback;
`uring` requires io_uring, and `blocking` explicitly selects blocking I/O.

## Recovery repositories

Create a separate owner-only encrypted repository and retain its key externally:

```sh
litiaina-n1-backup keygen --output /recovery/n1.key
litiaina-n1-backup init --repository /recovery/points --key /recovery/n1.key
```

Before object-service startup, configure the alias in its INI:

```ini
[backup_repository.archive]
path=/recovery/points
key_file=/recovery/n1.key
```

Repository paths must be outside every active member root. Operator requests
accept only these configured aliases. Start online capture and poll its job:

```sh
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json backup capture --repository archive
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json backup job
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json backup list --repository archive --limit 64
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json backup verify --repository archive --snapshot POINT_ULID
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
  --config /etc/litiaina/object/n1_config.ini
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

The helper generates `service.json` as a private effective recovery snapshot
containing the restored identities and member paths. The output parent must
already exist. Recovery writes private signing/TLS files and this generated
configuration without overwriting operator edits. It rewrites physical roots
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
Use `--help` on the engine or backup helper for exact flags. These object recovery tools do not yet provide a coordinated metadata/database
recovery point. Complete named-service recovery and Console administration
qualification remain in [the completion checklist](docs/RC2-IMPLEMENTATION.md).
