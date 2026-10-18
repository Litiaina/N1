<div align="center">

  <img
    src="./assets/litiaina_icon.png"
    alt="Litiaina"
    width="150"
    height="150"
  />

  <h1>Litiaina N1</h1>

  <p>
    <b>N1 is a high-performance single-node object storage system built for durable, redundant, namespace-managed, and efficient storage.</b>
  </p>

</div>

<p align="center">
  <strong>Current release: N1 1.0.0 RC.1</strong>
</p>

## Overview

N1 is a single-node object store for durable data on local storage. It provides
authenticated fragments, immutable object versions, streaming erasure coding,
trash/recovery, repair, exports and encrypted online/offline backups.

N1 favors few moving parts and bounded-memory processing. Accel is an optional,
disposable search/cache layer, not part of storage authority. N1 is not N2 and
does not provide distributed failover.

Optional `N1_ACCEL_READ_URL` enables private Accel caching for existing Console
and API reads after N1 authorization/version resolution. Accel failures before
client headers fall back to N1. See [Accel setup](DOCUMENTATION.md#accel-integration).

Current version: **1.0.0-RC.1**, the first formal release candidate, not stable
or LTS. Earlier 3.x/4.x numbers were private development labels; formal product
versioning starts here. Test results and known limits are in [Testing](TESTING.md).

## Release and compatibility versions

| Contract | Current version |
| --- | --- |
| N1, Shell, native client and bundled tools | `1.0.0-RC.1` |
| Storage-format specification | `1.0.0-RC.1` |
| Embedded storage encoding | `4` (unchanged bytes/markers) |
| Node Object API | NOA v1, `/noa/v1` |
| Native wire protocol | `n1/0.5` |
| Backup repository | `n1-backup-v1` |

The storage-format specification names the existing tested encoding. Resetting
its formal version does not rewrite files, renumber pools/object versions or
invalidate existing format-4 data/backups. Older unsupported development formats
are not migrated automatically. Product, storage and wire versions are separate
compatibility contracts; see [versioning](DOCUMENTATION.md#release-versioning-and-compatibility).

## Build and start

Use Linux, a current Rust toolchain, mounted storage and a trusted TLS certificate.

```bash
cargo build --release -p n1 --bins -p n1_shell
./target/release/litiaina-n1 --version
```

Put the binaries in a deployment directory such as /opt/litiaina/n1. Start
litiaina-n1 there once to generate n1_config.ini and n1.env; an unconfigured
deployment will not become a usable server. Fill in the mounted roots, TLS
paths and independently generated secrets before starting again. Protect n1.env
and the TLS private key; do not commit them.

N1 reads configuration from its working directory. Keep the generated defaults;
change the server and storage sections for your machine:

```ini
[server]
host=127.0.0.1
https_port=50001
native_port=50002
cert_path=cert.pem
key_path=key.pem

[disk_config]
fs_storage_path=/var/lib/litiaina/n1-control

[storage_pool.A]
generation=1
roots=/mnt/storage00,/mnt/storage01,/mnt/storage02,/mnt/storage03
data_shards=3
parity_shards=1
sealed=false
```

Each pool needs exactly K+P members on independent mounted devices. Enrolled
pool identity, geometry and logical shard slots cannot be edited. Root paths
are discovery locations, not member identities. Expand by adding a
new named pool; existing objects stay in their original pool. The control path
is not a global object index. See [Architecture](ARCHITECTURE.md).

For a systemd deployment, ensure the service account already exists and owns
the deployment/control directories and storage roots, then run:

```bash
sudo scripts/install-n1-systemd.sh \
  --install-dir /opt/litiaina/n1 --user n1 --group n1 --enable --start
sudo systemctl status litiaina-n1.service --no-pager
sudo journalctl -u litiaina-n1.service -f
```

The [operator setup guide](DOCUMENTATION.md#operator-setup) also covers the
guarded browser-terminal installer. Formatting is destructive; it is not
required for already provisioned storage.

## Basic use

Create a fragment using the master key, authenticate with its fragment secret,
then use the returned bearer token. Clients use HTTPS under /noa/v1 or the
canonical n1_native_client Rust crate on a trusted native connection.

```bash
BASE='https://storage.example:50001'
TOKEN='<fragment-access-token>'
curl --fail-with-body "$BASE/noa/v1/ping"
curl --fail-with-body "$BASE/noa/v1/upload/one-shot/docs/example.txt" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: text/plain' --data-binary @example.txt
curl --fail-with-body "$BASE/noa/v1/posix/list?path=docs&limit=200" \
  -H "Authorization: Bearer $TOKEN"
curl --fail-with-body "$BASE/noa/v1/objects/download?object_key=docs/example.txt" \
  -H "Authorization: Bearer $TOKEN" --output downloaded.txt
```

Trust the certificate normally, or use --cacert with your private CA. Replace
the hostname and token; the upload example assumes an authenticated fragment.
Full creation/authentication examples and API contracts are in
[Documentation](DOCUMENTATION.md#quick-start).

For acknowledged upload durability, set
`upload_part_strict_durability_default=true` in `[upload_config]`, or request
`durability=strict` on every part. The generated default remains relaxed;
restart-resumable multipart and power-loss durability are different guarantees.
Strict standard multipart carries its part durability through NDX and NCX
publication. Hardware qualification still depends on the filesystem/controller.

Keys are flat; docs/ is a virtual prefix, not a real folder. Uploads create
versions; rename, move, trash and recovery change only NCX references. Prefix
operations process their child keys and can report partial results. Durable
multipart is the restart-resumable upload option.

## Storage maintenance

Keep the engine stopped during member cloning or replacement. A healthy member
can be copied byte-for-byte, including a 1+0 pool, verified, and used at the
same or a new configured root. Self-identifying descriptors preserve its logical
slot regardless of mount path or discovery order. Upgrade older path-bound v4
members at their original locations before relocating them. A blank replacement
is never trusted automatically.
Use `litiaina-n1-shell pool help` for offline verification, clone and replacement
commands. Keep pool identity/geometry unchanged; edit only discovery locations
after verifying a complete offline copy. Backup/restore remains available;
do not edit enrolled geometry or delete identity markers to force startup.
See [pool maintenance](DOCUMENTATION.md#offline-pool-maintenance).

Pool health shows free bytes, available inodes and low/exhausted headroom warnings.
These are observations, not reservations. Keep free space in old pools for NCX
mutation even after new uploads move to an expansion pool. Server-wide GC/repair/
redistribution, restrictive modes and global stops require master-key operator
authentication; ordinary fragment tokens remain scoped to their fragment.

## Backups

Erasure coding, trash, exports and Accel are not backups. Keep recovery points
on independent storage and keep a protected offsite copy of the encryption key.

```bash
litiaina-n1-shell backup keygen --key-file /secure/n1-backup.key
litiaina-n1-shell backup init \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key
```

For online capture, configure N1's dedicated backup operator preset/token and
set `N1_BACKUP_OPERATOR_CONFIG` before startup. This includes committed objects,
versions and trash, not unfinished uploads. N1 remains running; mutations may
briefly wait while the view is pinned. See the [online setup](DOCUMENTATION.md#online-committed-state-capture).

```bash
litiaina-n1-shell backup create-online \
  --source-url https://storage-host:21001 --repository primary \
  --token-file /secure/operator.token --ca-file /secure/storage-ca.pem
```

For full offline recovery including unfinished durable-upload progress, quiesce
writers first. N1 must remain stopped throughout this capture:

```bash
sudo systemctl stop litiaina-n1.service
litiaina-n1-shell backup create \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --config /opt/litiaina/n1/n1_config.ini --workers 4
sudo systemctl start litiaina-n1.service
litiaina-n1-shell backup verify \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID
```

Restart the service even if capture fails; the manual create command does not
manage it. Install the matching engine beside the shell and use an account with
access to the source and repository. The standalone helper remains compatible
for existing backup jobs; it is not required for manual shell commands.
Back up configuration, secrets, TLS keys and application databases separately.
Test restoration before relying on a recovery point. Replication, portable
restore, retention and disabled-by-default scheduled jobs are documented in
[Backup and restore](DOCUMENTATION.md#backup-and-restore).

Remote replication uses N1-owned HTTP(S) or native N1(S), not SSH/SCP. Run
`litiaina-n1-shell backup serve --config backup-operator.json` on the destination;
configure its allowed repositories and a separate operator token. The same
optional Shell endpoint provides Console Maintenance's working backup, verification,
replication, restore and job-status controls, including online capture when a
running N1 source is configured. Only offline capture and target restore need
the relevant engine stopped.
See [operator API](DOCUMENTATION.md#shell-owned-operator-api-and-console).

## Documentation

- [README](README.md): overview, startup and basic backup.
- [Changelog](CHANGELOG.md): releases and development history.
- [Contribution policy](CONTRIBUTING.md): bug reports; no pull requests.
- [Testing](TESTING.md): procedures, measured results, failures and open gates.
- [NOA](NOA.md): canonical HTTPS/WebSocket route inventory.
- [Architecture](ARCHITECTURE.md): storage design and invariants.
- [Documentation](DOCUMENTATION.md): API reference and operator instructions.

## License and reports

N1 is licensed under [Apache 2.0](LICENSE). Bug reports are welcome; external
code contributions and pull requests are not accepted. See
[CONTRIBUTING.md](CONTRIBUTING.md).
