# N1 backup and snapshot replication

N1 provides `litiaina-n1-backup`, an operator utility for encrypted,
incremental **offline storage snapshots**, portable restore, and one-way
replication of completed snapshots. It does not change ECS Core, EC placement,
upload finalization, namespace publication, or the normal request path.

This is **snapshot replication, not continuous live engine replication**. The
destination is a backup repository, not a running writable N1 standby. Recovery
requires an explicit restore and destination configuration. There is no automatic
failover, active-active operation, distributed quorum, or zero-RPO promise.

## Capture boundary and downtime

Stop applications that write through N1, then stop the source N1 engine before
capture. Keep the engine stopped until `create` succeeds or fails. Capture reads
and hashes the entire configured storage tree; unchanged content is reused in
the repository, but this first implementation does **not** skip reading unchanged
files. A large installation can require a substantial maintenance window.

The updated engine holds an OS advisory lease for its lifetime on each accessible
configured root. Capture requires initialized leases on **every** configured
root and takes them exclusively. It refuses a running cooperating engine, an
incomplete restore, a missing root, or pending redistribution intent files.
These are process-level leases, not upload or namespace serialization locks.
An existing read-only root without an initialized lease does not by itself
disable engine startup, but backup cannot proceed until all roots support the
capture contract.

First run the updated engine normally, then stop it for capture. Older N1 binaries
and external programs do not honor these advisory leases. Do not run them against
the roots during capture, and do not bypass the lease check by creating markers
manually. Do not format, remount, replace, or edit roots while capture is running.

Concurrent writes across objects cannot be converted into an application-consistent
database backup just by copying N1. Quiesce Space/custom applications and back up
their databases separately using their supported backup mechanisms. Record which
application database recovery point corresponds to the N1 snapshot.

## What is captured

All regular files, directories, and N1 identity symlinks under the configured
`index_store_disks` and `blob_store_disks` are captured, including:

- Fragment identities, aliases, and fragment authentication metadata.
- Live namespace entries, empty directories, and trash entries.
- Immutable object/version metadata and their physical data/parity shards.
- Durable multipart manifests, part metadata, and receipts.
- Existing file/directory modification times, needed for lifecycle behavior.

The capture excludes root-instance leases, physical root identity markers, and
restore completion markers. Fresh physical identities are established when the
restored engine enrolls its destination roots. Standard in-memory multipart
sessions are intentionally not recoverable after stopping N1. Uncommitted
physical files may be captured; normal lifecycle handling remains responsible
for them after restore.

**Not included:** the N1 configuration/environment file, TLS private keys and
certificates, systemd units, external `fs_storage_path` control/workspace state,
refresh-token storage, export jobs/artifacts outside storage roots, Space or
other application databases, Accel's rebuildable cache, and OS configuration.
Keep protected copies of the required configuration and credentials separately.
Expect fresh authentication sessions, not restoration of old login tokens or
in-progress export/maintenance jobs.

Snapshot verification proves the recorded bytes and manifest are intact. It does
not prove the source was healthy: a preexisting corrupt shard can be faithfully
backed up. Complete source health checks/scrubs and perform restored-object
readback as part of the operational acceptance procedure.

## Repository format and security

Each repository contains encrypted content-addressed blocks, completed snapshot
directories, and unpublished work directories. Block names are SHA-256 hashes
of plaintext, so equality and block-size information is observable. File contents,
namespace paths, fragment secrets, source root paths, and snapshot manifests are
encrypted with AES-256-GCM. HKDF-SHA256 derives a separate encryption key per
content hash or snapshot identity; each encryption also uses a random nonce.

The repository and key must be owner-only. Generate a random 32-byte key with the
utility; do not use a fragment/master password as the key file. Keep at least one
protected copy of the key outside the repository's failure domain. Losing the
key makes all recovery points unusable. Replicas currently use the same key;
key rotation/re-encryption is not implemented.

Use a filesystem supporting ordinary files, hard links, `flock`, and durable
file/directory `fsync` (for example a locally mounted Linux data filesystem).
Hard-link pins retain blocks referenced by completed and incomplete jobs.
Repository mutation is exclusive; verification/restore hold a shared lease so
retention cannot remove their input. Do not edit the repository manually.

The utility streams file bytes in 4 MiB blocks with a bounded capture queue and
1–32 workers (default 4). Namespace records are streamed into encrypted manifest
blocks. A single record and the snapshot header are limited to 8 MiB; individual
physical files are limited to 100,000 block references. This is not an
unbounded-format claim. Internal metadata requiring path remapping is limited to
64 MiB per file; larger metadata is rejected during capture, before publication.
Restore uses a temporary disk-backed inventory; budget
space/inodes for that inventory, final data, and one temporary physical file.

Restore preserves ownership-independent contents, not source Unix ownership,
ACLs, xattrs, or permissions. New files are mode 0600 and directories mode 0700.
Run the utility as the destination engine's storage owner, or explicitly set
the correct ownership before starting N1.

## Build and first backup

Build the helper and engine from the same revision:

```sh
cargo build --release -p n1 --bin litiaina-n1 --bin litiaina-n1-backup
```

Install the helper beside the engine. Examples use generic operator-chosen paths;
the repository's parent directory must already exist. Never put a repository
inside an engine storage root, or use an engine root as a repository.

```sh
litiaina-n1-backup keygen --key-file /secure/n1-backup.key
litiaina-n1-backup init \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key

# Quiesce application writes, then stop the source service.
sudo systemctl stop litiaina-n1
litiaina-n1-backup create \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --config /etc/litiaina/n1/n1_config.ini --workers 4
# Restart the engine and applications after create has exited, including on error.
sudo systemctl start litiaina-n1

litiaina-n1-backup list \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key
litiaina-n1-backup verify \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID
```

Configuration storage roots must be absolute, existing paths. Capture canonicalizes
and merges colocated index/blob roles, then sorts unique roots lexicographically.
Nested/overlapping roots are rejected. A successful `create` prints its snapshot
ID, entry count, and captured physical byte count as JSON. Failures return nonzero;
unfinished work never appears as a completed recovery point.

For scheduling, use a service-account-owned job/systemd timer with explicit
application quiescing, engine stop/start, error handling, and alerting. Ensure its
failure handler restarts a source service that the job stopped. **Do not** rely on
`stop && create && start`: a failed capture would leave the engine stopped. The
utility deliberately does not stop arbitrary host services or install timers.

## One-way replication

Initialize a second repository with a protected copy of the same key. It should
be in an independent disk/host/site failure domain. Replication can run after
the source engine restarts because it reads the committed repository snapshot,
not changing engine files. The network transport uses standard SSH host-key
verification, authentication, connection timeout, and keepalives.

```sh
# Local/mounted destination:
litiaina-n1-backup replicate \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID \
  --target-repository /independent-backup/n1-replica \
  --target-key-file /secure/n1-backup.key

# Remote destination: initialize its repository first, with the same key.
# Install the same helper version there and configure SSH keys/known_hosts.
litiaina-n1-backup replicate \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID --ssh backup@backup-host \
  --remote-binary /opt/litiaina/n1/litiaina-n1-backup \
  --target-repository /backup/n1-replica \
  --target-key-file /secure/n1-backup.key
```

Use `--ssh-config /path/to/backup-ssh.conf` when a dedicated OpenSSH configuration
is needed; normal OpenSSH host-key verification still applies.

Existing destination blocks are authenticated before reuse. Interrupted transfers
can be retried with the same command; verified blocks are reused, not retransmitted.
The destination acknowledges completion only after validating the full snapshot,
syncing it, and atomically publishing it. Conflicting content under an existing
snapshot ID is rejected. Unexpected/corrupt existing blocks fail the transfer;
they are not silently overwritten.

Replication does **not** propagate repository retention or delete old snapshots.
Keep destination retention independently controlled. Deleting an object in N1
does not erase its earlier recovery points. Replication is therefore not a
substitute for choosing and testing an appropriate backup retention policy.

## Restore and recover

Create fresh empty destination storage roots. Keep the destination engine stopped.
Provision a destination configuration with the same EC layouts supported and
the same count and ordered roles of canonical roots. A source root maps to the
destination root at the same index in the lexicographically sorted unique-root
list. Review that mapping explicitly; use consistently ordered root names.
Index/blob roles that were colocated remain colocated. Distinct shards of a part
must map to distinct destination filesystem devices.

Restore is **not** EC recoding or migration between different root counts. It
preserves object/version identities and rewrites stored shard paths and durable
session placement roots. Source absolute paths must not be used as fallback.

```sh
litiaina-n1-backup restore \
  --repository /backup/n1-replica --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID --target-config /etc/litiaina/n1-restored.ini

# Only for an interrupted, matching restore into these same roots:
litiaina-n1-backup restore \
  --repository /backup/n1-replica --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID --target-config /etc/litiaina/n1-restored.ini --resume
```

All snapshot blocks are authenticated before writing restored data. Existing
nonempty roots are rejected. Restore writes persistent start fences, remaps
metadata, syncs files/directories, and removes those fences only after completion.
The updated engine refuses to start against any root with an incomplete restore.
Resume checks snapshot/key/target identity and existing file contents. Unexpected
destination files cause an error and are not deleted. Do not remove a restore
fence just to force the engine to start. An incomplete/corrupted control marker
requires inspection; automatic recovery is intentionally not guessed.

Use a **fresh destination control directory** (`fs_storage_path`) and configure
the engine environment, master/signing keys, TLS, service ownership, and limits.
Do not copy a source topology-identity registry over the new physical roots.
Then start N1, authenticate fragments, verify namespace/version history and trash,
download representative objects with full checksum comparison, check health,
and verify new uploads. Test durable-session continuation if it is required;
original timestamps/expiry policies still apply. Restore an application database
matching the selected storage recovery point before resuming application writes.

Before first startup of an older recovery point, review trash/session expiration
and automatic maintenance settings. Restoring does not reset their age: items
already beyond retention may become eligible for cleanup as soon as N1 starts.
Keep the completed backup repository intact while validating recovery, rather
than treating the restored writable instance as the only remaining copy.

## Retention and cleanup

```sh
litiaina-n1-backup forget \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot OLD_SNAPSHOT_ID
litiaina-n1-backup gc \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --discard-incomplete
```

`forget` refuses to remove the last completed recovery point. Garbage collection
verifies completed snapshots and protects their referenced blocks before reclaiming
unpinned files. Without `--discard-incomplete`, failed-job pins remain protected.
An exclusive repository lease prevents cleanup of an active capture/transfer.
This is not immutable/WORM storage: an OS administrator can still destroy it.

## Validation and remaining scope

The focused unit suite is `cargo test -p n1 --lib core::backup::tests`.
`n1-release-proof --backup-proof` exercises the helper next to the test engine,
including encrypted capture, snapshot replication, unavailable original paths,
portable EC restore, fragment credentials, hashes, namespace/history/trash, and
durable multipart continuation. It creates isolated test roots and must never
be pointed at production storage roots.

Recorded acceptance results and their limits are in
[VERIFICATION.md](VERIFICATION.md#september-1112-2026-offline-backup-and-snapshot-replication).

Remaining work beyond this maintenance-window version includes live snapshot
staging/change tracking, continuous replication lag reporting, automated standby
promotion, different-layout recoding, managed schedules/UI integration, key
rotation, application-coordinated recovery orchestration, and independent-site
power-loss/restore drills. Do not infer those capabilities from a successful
offline recovery proof.
