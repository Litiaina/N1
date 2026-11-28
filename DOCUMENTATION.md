# N1 and M1 operator guide

Client request/response documentation is in the [M1 API guide](docs/api/M1.md)
and [N1 API guide](docs/api/N1.md). The shared contract is [NOA.md](NOA.md).

This is the operator guide for **N1**, the LINK object store, and **M1**, the
independent metadata store with its own specialized transactional database. Both systems live in this
repository. N1 operates on its own, and Console's named-object workflows use
both engines. Server settings use INI. Supply secrets in a private file or the
service environment. For a first run, see the [local quickstart](docs/QUICKSTART.md).

System `1.0.0-RC.3` includes N1 `1.0.0-RC.2` and
M1 `1.0.0-RC.2`. Each server's `--version`, banner and API version fields report
its own package version. Check [the system manifest](deploy/system-release.json)
and [versioning policy](docs/VERSIONING.md) when selecting a supported pair.

For a named N1 deployment, the connection path is:

`Console / named client → M1 → native N1 (LINK) → shards`

A client that owns its metadata can use `object_engine` directly. The table below
uses object ports 19001/19002 and public metadata ports 50001/50002; these are
examples, all editable under each service's `[server]` section.

| Setting | N1 (`object_engine/`) | M1 (`metadata_engine/`) |
| --- | --- | --- |
| HTTP(S) listener | Internal 19001 | Public 50001; Console connects here |
| Native listener | Internal 19002, `n1/1` | Public 50002, named `n1/0.5` |
| Pool roots | Owns `[storage_pool.NAME]` | No member roots |
| Private state | Pool identities, signing key, control state | Database, named uploads and exports |

## Prepare and start the services

Build `n1` and `m1` from the workspace. Give each service its
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

`litiaina-m1 serve --config /etc/litiaina/metadata/m1_config.ini`

A manual launch inherits its shell's file-descriptor limits. The packaged N1
systemd service sets `LimitNOFILE=1048576`; a shell qualification using the same
concurrent batch profile should use that limit too. Where its hard limit allows,
`ulimit -Sn 1048576` applies it to the current shell and its children without
changing system-wide settings.

Point Console at M1's HTTPS address. Provision certificates valid for the
client-facing names and keep private keys readable only by their service
accounts. Use `n1s://` for verified native TLS connections.

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

## M1 metadata service

The object service works independently over native NOA v2. Named N1 clients,
including the existing Console, connect to `litiaina-m1`. Its original
NOA v1 HTTP endpoints and named native operations resolve names and versions,
then stream bytes through the object's native interface. The Console's product
layout and workflows are retained.

Each independently running service has its own INI, environment file, control
directory and listening ports. In the metadata INI, `[server]` controls the
public client address and TLS exactly as above. `[disk_config]` selects metadata
control storage. Metadata does not enroll disks or require pool-root sections;
it obtains immutable pool profiles from the object service.

M1's default configuration is `m1_config.ini`, with an optional adjacent
`m1.env`. N1 retains `n1_config.ini` and `n1.env`. On either service,
`--config` and `[server] env_file` select explicit paths. Exported service
credentials override file values; neither service requires an environment file.

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
backup_dir=/var/backups/litiaina/metadata
object_connections=32
workers=64
```

The referenced environment secret authenticates the metadata service to LINK.
Logical fragment secrets, permissions, refresh sessions, names, versions, upload
coordination and trash live in metadata. Its database and staging directories
must be real owner-only directories. LINK keys remain private to this service.

```sh
litiaina-m1 serve --config /etc/litiaina/metadata/m1_config.ini
```

An external metadata implementation can use the object interface directly.
The object engine requires neither this metadata process nor its database.

## Console monitoring and backups

System Monitoring → Health reports **two independent service nodes**. Metadata
CPU, RAM, process RSS, database space and staging-filesystem capacity come from
the metadata host. Object CPU/RAM, member identities, pool EC topology and
filesystem capacity come from the object host over native NOA. The services may
run on different machines. Filesystem usage includes everything on that
filesystem; it is not an exact accounting of N1 shard bytes.

Ordinary refreshes share observations controlled by
`[upload_config] monitor_interval_seconds` on each service. More Console clients
do not start more collectors. Refresh cannot bypass that interval. Responses
include sample age and pending/stale state. Collection performs read-only root
binding and filesystem-counter probes; it never walks object directories or
reads payloads. Console polls only while its monitoring screen is visible.
The explicit integrity action starts one paced, asynchronous fragment-reference
check. Its result is a live observation, not a consistent backup snapshot.

Fragment live-byte counts are transactionally maintained in 256 counter shards;
capacity queries read those bounded counters, never enumerate all names. An
absent counter is reported as unknown. The explicit offline administration
command `litiaina-m1 recount --data-dir DATABASE --fragment FRAGMENT`
rebuilds those counters when an operator requests a full recount.

The **Metadata backups** panel requires master-key operator authentication and
an explicit `[metadata_engine] backup_dir` on the metadata host. Capture pins
one cross-keyspace MVCC snapshot and streams it to an owner-only file, with
SHA-256 and record-count verification. It does not copy the live database
directory. Capture is single-admission and runs only on request. Download URLs
expire after two minutes. Snapshots contain private LINK credentials; protect
them like the database. Restore only into a new directory:

```sh
litiaina-m1 restore --snapshot SNAPSHOT.n1meta --data-dir /new/metadata-db
```

The metadata snapshot includes named transactions and upload controls; staged
payload bytes and object shards are separate. Preserve metadata staging for
bridged unfinished uploads and preserve an object recovery point, pool identities,
service signing keys, INI, secrets and TLS material. For a coordinated N1 + M1
recovery point, pause writers, pool migration and deletion/GC affecting the captured objects,
capture metadata and object state, verify both, then resume. Independent live
captures alone do not guarantee a shared point in time.

If the metadata database is lost, stop the metadata service and retain the
remaining database/staging directories as evidence. Restore a verified metadata
snapshot into an absent destination using the command above, then select that
directory in metadata's INI and restart. Keep its original service credentials
and the object pool/member identities. Verify fragments, named versions, trash,
permissions and upload status before reopening writes. Bridged unfinished
uploads can require their separately retained staging bytes; a database archive
does not supply those bytes.

Object shards alone do not reconstruct logical names or permissions. Without a
metadata backup, an authorized client that independently retained LINK keys can
still read surviving objects directly, but cannot rebuild the named catalog
from those shards. If object state was also lost, restore its matching verified
recovery point before reopening the named service. A copy of a live database
directory is not a substitute for the snapshot workflow.

Object recovery retains the existing `backup create`, `verify`, `replicate`,
`replicate-remote` and `restore` workflows. Local repository replication can
reencrypt under a new backup key. Native remote replication requires matching
repository encryption keys, transferred through the operator's secure process.
It preserves logical pool/member identities and the exact captured shard state.
Hardware serial numbers are not membership identities: a blank replacement
uses the existing logical slot and reconstructs its shards through the explicit
`pool replace --authorize` workflow. The live descriptor/device binding fence
prevents accidental mount substitution during operation.

Byte-copy recovery preserves the original immutable pool geometry and keys.
Copying shards into an unrelated pool with another identity or EC geometry does
not change LINK placement; such a destination needs an explicit object transfer
with new returned keys. A metadata snapshot may itself be uploaded using the
normal object API, but its recovery LINK key and service credentials must be
kept outside the database being backed up.

Console's **Backups & snapshot replication** controls connect to the dedicated Shell operator
host on the object/repository machine. They do not send filesystem paths or
object-administration credentials through the metadata service. Start it with:

```sh
litiaina-n1-shell backup operator-keygen --token-file /etc/litiaina/n1/operator.token
litiaina-n1-shell backup serve --config /etc/litiaina/n1/backup-operator.json
```

Use [the operator configuration example](deploy/backup-operator.json.example),
save it with mode `0600`, and configure the Console's exact HTTPS origin in
`allowed_origins`. The listener requires TLS unless `allow_plaintext` is
explicitly enabled for a private test. Enter its origin and dedicated token in
Console; the token is cleared from the input and is not saved in browser storage.
Repository, replica, restore-target and job aliases are operator-configured.
The host admits one modifying job and four inventory readers, keeps the last
32 operation outcomes, and drains accepted work on orderly shutdown. Browser
disconnection does not cancel an accepted job. Its operation history is in
memory; after a host crash, inspect the repository and recovery/job state.

For online capture, configure `online_source` with `credentials_file` pointing
to a private native object-client file and `repository` matching a repository
alias registered on both the object service and this host. A replica uses the
same fields, with an optional `ca_file` for its native TLS endpoint. Existing
object NOA performs the transfer. Restore targets map aliases to target INI
files; their configured roots must be empty/fenced and the target engine must
be stopped. Preserve its control registry, signing key, authentication and TLS
material separately, or use the offline recovery tools to restore control state.
The operator adapter restores member contents, not a replacement deployment.

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
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json status --object-key N1A1.EXAMPLE
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json finalize --object-key N1A1.EXAMPLE
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json get --key N1A1.EXAMPLE --output ./recovered.bin
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json health --key N1A1.EXAMPLE --deep
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json repair --key N1A1.EXAMPLE
litiaina-n1-shell --credentials /etc/litiaina/object/object-client.json delete --key N1A1.EXAMPLE
```

`N1A1.EXAMPLE` is a placeholder, not a valid key. Part size must
respect the pool's EC stripe geometry; `put --part-bytes` rounds its requested
size to that geometry. Standard parts may require retransmission after a crash;
durable parts acknowledge only after their persistence barriers. Finalization
is durable and idempotent for the same immutable identity.

Begin acknowledges creation of incomplete shards. An unused allocation may
disappear on power loss before its first durable payload reply. In that case
the old key returns 404; start a fresh upload. The first durable part persists
its header, payload and staging paths together. Completed normal/batch uploads
and finalized objects remain durable.

Download output uses exclusive creation and never overwrites a local file.
A failed transfer can leave a partial output for inspection. Keep original
client-held keys in the separate metadata system or another client-owned record;
N1 cannot enumerate or reconstruct that record for you.

Delete removes all incomplete and final shard files and persists their removal.
It leaves no terminal record. Retrying after success returns zero removals; old
continuation/finalize requests return 404. A failed or interrupted delete must be
retried using the same LINK key. With sufficient surviving shards, an interrupted
purge can remain readable: missing files alone do not encode deletion intent.
Metadata GC retains its durable claim until native success; standalone clients
must retain their own pending deletion request. There is no rename, history,
trash or undelete API in the object engine. Metadata provides those named
operations for Console.

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
| Named trash retention | Metadata `[garbage_collector] trash_retention_seconds`; measured from deletion, reset by recovery followed by deletion |
| Metadata GC scheduling and bounds | Metadata `[garbage_collector]`: `enabled`, `cleanup_interval_seconds`, `batch_size`, `object_concurrency`, `pause_millis`, `bytes_per_second` |
| Scheduled healing | `[auto_healing]`: `enabled`, `scrub_schedule_utc`, `initial_scan_delay_seconds`, `scheduled_scrub_pause_millis` |
| Repair worker bounds | `[auto_healing]`: `object_scan_concurrency`, `shard_repair_concurrency`, `metadata_batch_size` |
| Rate quotas | `[rate_limiter_config]`: `enable_rate_limiting` and the existing rate/burst keys |
| Optional file size ceiling | `[set_limits] max_file_size`: `unlimited` (default), or a positive byte count |
| Export bounds | `[export]`: retention, job, download and buffer controls on metadata |

Values accepting `auto` retain their host-derived defaults. Pool geometry and
`stripe_block_size` must match enrolled immutable layouts; they are not tuning
knobs for changing already stored objects. Operational tuning does not alter
existing LINK identities.

M1 enforces the same `[rate_limiter_config]` rate/burst controls on its HTTP and
named native listeners. Global/auth budgets use the actual TCP peer; forwarding
headers do not override it. View/upload budgets use the canonical fragment,
shared across tokens and transports. HTTP rejection returns 429 with
`Retry-After`; native rejection returns 429 without accepting an upload body.
Set `enable_rate_limiting=false` to disable this policy. Configure a trusted
proxy's aggregate peer budget for its actual traffic rather than assuming
client-provided headers select independent quotas.

Optional Accel integration belongs to M1. Set these in M1's service environment
or private `m1.env`, then restart M1:

```dotenv
N1_ACCEL_READ_URL=http://127.0.0.1:9100
N1_ACCEL_READ_BACKEND_ID=n1-primary
N1_ACCEL_SEARCH_URL=http://127.0.0.1:9100
N1_ACCEL_SEARCH_BACKEND_ID=n1-primary
```

Configure the corresponding Accel backend to use M1's public named origin and
JWT verification secret. URLs accept HTTP(S), without embedded credentials,
query or fragment; backend IDs accept letters, digits, `_` and `-`. Omit either
URL to disable that integration; backend IDs default to `n1-primary`. M1 verifies
permissions and resolves the version before using Accel. Failed read headers or
invalid response geometry fall back to N1; a failed body ends the download.
Search works without the accelerator through bounded M1 metadata pages.
Fresh unavailable acceleration falls back to M1; an existing Accel cursor
retains its provider, so start a fresh search if that provider is unavailable.
Scoped searches use M1's exact namespace path case. See the
[search API](docs/api/M1.md#search-named-objects) for fields, limits and cursors.
HTTPS uses the host trust store. The qualified Linux M1 build also accepts a
private CA bundle through its **process/service environment**:

```sh
SSL_CERT_FILE=/etc/litiaina/accel-ca.pem ./litiaina-m1 serve --config /etc/litiaina/m1_config.ini
```

This setting belongs in the service environment; putting `SSL_CERT_FILE` only
inside `m1.env` does not export it to the TLS library. Keep certificate and
hostname verification enabled. The [Four-HDD reference system TLS
witness](docs/FOUR-HDD-OPTIMIZATION-20261113.md) checks trusted/untrusted CAs,
wrong hostnames, concurrent pinned reads and actual Accel restart without
changing the global trust store.

Direct `/noa/v1/objects/archive` accepts selected keys/entries in JSON. Prefixes,
`export_all` or more than 1,000 explicit entries choose TAR. JSON selection bodies
are bounded to 4 MiB. For larger inventories, POST newline-delimited keys to
`/noa/v1/objects/archive/manifest`; an optional
`VERSION<TAB>KEY` pins a historical version. Manifest lines are limited to 8 KiB;
TAR capture uses a disk spool and one metadata snapshot rather than retaining the
whole inventory in RAM. These requests share `[export]` worker/queue and buffer
admission. Retained export jobs use indexed expiry, transactional retained-byte
reservations, download leases and restart cleanup; the
[qualification checklist](docs/RC2-IMPLEMENTATION.md) tracks final acceptance.
Direct named-native ZIP64/TAR and TAR-manifest archives also stream beyond the
8 MiB control-frame bound: M1 counts framing bytes during immutable manifest
capture, then streams the retained writer's output with an exact length. The
last chunk waits for source verification and pin/manifest cleanup.

There is no configured total-object size cap by default. In N1's
`n1_config.ini` and M1's `m1_config.ini`, omit `max_file_size` or set:

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

When `env_file` is omitted, N1 reads an owner-only `n1.env` beside its INI and
M1 reads an owner-only `m1.env` beside its INI, if present.
The file is optional when the service supplies all required secrets through
the process environment. Exported values override matching file values. A bad
or insecure file still fails startup; it is not silently ignored.

For systemd-managed secrets, `LoadCredential=` or `LoadCredentialEncrypted=`
can supply a private dotenv-format credential file. Point `env_file` at that
service's absolute credential path (normally `/run/credentials/<unit>/<id>`);
N1 reads it directly without exporting the secrets to child processes. Keep
object and metadata credentials separate. Raw `Environment=` entries are not
a secret store; see [systemd's credential guidance](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html).
Preview each systemd unit on the machine that will run it.

```sh
scripts/install-n1-systemd.sh --install-dir /opt/litiaina/n1 --dry-run
scripts/install-m1-systemd.sh --install-dir /opt/litiaina/m1 --dry-run
```

N1 requires the configured control and pool mounts. M1 requires its local
control, database, staging and backup mounts and connects to N1 at the address
in its INI. The two units can run on different hosts. Both installers accept
`--config-file`, `--user` and `--group`. Use `--no-env-file` when the engine
reads its INI-selected secrets file or receives credentials from the service.
To install and start a unit, run the corresponding command with sudo and
replace `--dry-run` with `--enable --start`.

Staging GC runs at the configured cleanup interval; the generated INI uses
3,600 seconds. It conservatively reclaims expired unpublished uploads, never
committed objects. Scheduled healing requires both an
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

### Rate limits for Console folder uploads

M1's batch uploads issue N1 allocation and verification requests for each
file. A saturated N1 quota can appear as per-file 429s inside M1's HTTP 207
batch response. For concurrent folder uploads, this tested profile gives
those bursts more headroom. Set it in N1's INI and restart N1.

```ini
[rate_limiter_config]
enable_rate_limiting=true
view_limit_per_second=300
view_limit_burst_size=1024
upload_limit_per_second=600
upload_limit_burst_size=3000
global_limit_per_second=1200
global_limit_burst_size=4096
```

The profile keeps authentication and I/O admission active. To retry a partial
folder upload in Console, select **Skip existing** to keep files already
committed and submit the missing ones.

## Metadata lifecycle and maintenance

Metadata owns named trash and expiration policy. A named delete changes metadata
only; recovery remains available until GC claims the expired entry. There is no
empty-trash operation, forced early purge or per-version deletion. Configure
the **metadata INI** independently of object staging GC:

```ini
[garbage_collector]
enabled=true
cleanup_interval_seconds=3600
trash_retention_seconds=2592000
batch_size=128
object_concurrency=1
pause_millis=25
bytes_per_second=16777216
```

Restart metadata to apply changes. `enabled=false` disables automatic passes but
retains Run now. Both modes respect `deleted_at + trash_retention_seconds <= now`;
expiration makes trash eligible for the next pass. Recovery followed by another
delete starts a new TTL. The first automatic pass follows one interval after
startup, without replaying missed intervals. Bounds are batch 1–1000, concurrency
1–16, pause 0–60000 ms and a positive byte rate. The byte rate paces migration;
GC bounds deletion through concurrency and pause.

Routine GC seeks ordered database indexes, not namespace or shard-directory
walks. The first startup with unindexed existing metadata builds indexes from
the database before request admission. Claims fence recovery/publication races;
native LINK deletion precedes metadata purge. Failure retains keys and durable
retry state. Shared references and export/migration pins protect retained data.
Unfinished upload cleanup uses immutable staging expiry and never expires a
completed live version.

Console Maintenance shows effective GC policy and Run now / Stop / Status.
Mounted-fragment actions stay scoped to that fragment. An operator can explicitly
request `all_fragments=true` through NOA. Global stop and cross-fragment access
require master-key authentication. Stop is cooperative. Already claimed trash
stays nonrecoverable until reclamation finishes; failures are reported as
incomplete and retried on a later pass.

**Integrity check / healing** selects indexed version references and calls the
object health or repair API. Dry run checks without repair. The metadata object
worker is sequential; object repair retains its own shard admission. Ordinary
read repair and optional object scrub scheduling remain independent.

**Pool migration** selects different enrolled source and unsealed destination
pool IDs within the connected object service. Source bytes are reconstructed and
streamed into the destination EC geometry using bounded buffers and strict
durable parts. Full source/destination payload SHA-256 comparison precedes the
atomic metadata-reference switch. Part size adapts within metadata's configured
`max_part_size`. Names, version IDs and ETags stay unchanged; private LINK keys
change. Retained history versions are included. This does not mutate existing
keys or pool layouts; independent direct clients manage their own key changes.

Only after the switch does migration purge the unreferenced source, retaining
retry state until confirmation. Stopping an unswitched transfer purges only its
destination. Durable running/interrupted work resumes after process restart.
Retry incomplete migration with its original parameters, or explicitly stop it.
Conflicting export pins are reported for retry rather than bypassed. Cross-node
recovery-point replication remains a separate backup operation.

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

Online capture pins only committed shards. Upload bodies
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
An older-point restore is an explicit rollback: it can restore data purged after
that point. Coordinate metadata/object recovery points. No permanent deletion
witness or cross-point overlay is maintained. Retention preserves protected
points and refuses to remove the final point.

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
are not rewritten. Retry pending metadata reclamation after physical recovery.
Acknowledged durable
parts survive reconstruction, while holes remain missing. Sparse private uploads
use filesystem data extents when available, with conservative sequential
fallback. There is no automatic failed-disk enrollment or replacement.

The shell forwards `pool ...` and `backup offline ...` to the matching adjacent
engine, with optional `--engine-binary`. These commands require no network login.
Use `--help` on the engine or backup helper for exact flags. The object tools
capture object state. Use the paused-writer metadata/object capture procedure
above for a matching named-service recovery point. Independent live captures
are not an atomic whole-product snapshot. Outstanding qualification is tracked
in [the completion checklist](docs/RC2-IMPLEMENTATION.md).
