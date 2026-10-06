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
  <strong>Current release: N1 4.0.0</strong>
</p>

## Overview

N1 is a single-node object store for durable data on local storage. It provides
authenticated fragments, immutable object versions, streaming erasure coding,
trash/recovery, repair, exports and encrypted offline backups.

N1 favors few moving parts and bounded-memory processing. Accel is an optional,
disposable search/cache layer, not part of storage authority. N1 is not N2 and
does not provide distributed failover.

Current version: **4.0.0 development**, storage format **4**. This is not an LTS
release. Earlier development storage formats are not migrated automatically.

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
pool identity, geometry and member order cannot be edited. Expand by adding a
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

Keys are flat; docs/ is a virtual prefix, not a real folder. Uploads create
versions; rename, move, trash and recovery change only NCX references. Prefix
operations process their child keys and can report partial results. Durable
multipart is the restart-resumable upload option.

## Backups

Erasure coding, trash, exports and Accel are not backups. Keep recovery points
on independent storage and keep a protected offsite copy of the encryption key.

```bash
litiaina-n1-backup keygen --key-file /secure/n1-backup.key
litiaina-n1-backup init \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key
```

Quiesce writers first. N1 must remain stopped throughout capture:

```bash
sudo systemctl stop litiaina-n1.service
litiaina-n1-backup create \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --config /opt/litiaina/n1/n1_config.ini --workers 4
sudo systemctl start litiaina-n1.service
litiaina-n1-backup verify \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID
```

Restart the service even if capture fails; the manual create command does not
manage it. Run the helper as an account with access to the source and repository.
Back up configuration, secrets, TLS keys and application databases separately.
Test restoration before relying on a recovery point. Replication, portable
restore, retention and disabled-by-default scheduled jobs are documented in
[Backup and restore](DOCUMENTATION.md#backup-and-restore).

## Documentation

- [README](README.md): overview, startup and basic backup.
- [Changelog](CHANGELOG.md): development changes.
- [Contribution policy](CONTRIBUTING.md): bug reports; no pull requests.
- [Testing](TESTING.md): procedures, measured results, failures and open gates.
- [NOA](NOA.md): canonical HTTPS/WebSocket route inventory.
- [Architecture](ARCHITECTURE.md): storage design and invariants.
- [Documentation](DOCUMENTATION.md): API reference and operator instructions.

## License and reports

N1 is licensed under [Apache 2.0](LICENSE). Bug reports are welcome; external
code contributions and pull requests are not accepted. See
[CONTRIBUTING.md](CONTRIBUTING.md).
