# Litiaina NS Deployment Guide

Current version: `1.0.0`

`Litiaina NS` is the standalone NAS Lite / SMB service for the Litiaina stack. It is intentionally separate from `N1`: `NS` owns live SMB file serving, local share roots, NAS users, share permissions, and disk placement. `N1` sync is optional backup/protection, not the live write path.

Use `NS` when clients need Windows/macOS/Linux file-manager access. Use `N1` as the optional durable object-storage target for sync and restore.

## Architecture

`NS` has its own runtime state:

- `ns_config.ini` for SMB, admin, users, shares, disks, and optional N1 sync
- `ns.env` for process environment overrides
- an `indexdb` metadata database for users, disks, shares, permissions, and sync cursors
- configurable share directories that hold the live NAS files
- an optional sync worker that snapshots stable files and uploads them to `N1`
- an admin API for share, disk, user, status, and restore operations

Do not point `NS` share roots, metadata, or staging paths at `N1` index roots or blob roots. They are separate products and must not share internal storage directories.

For current `N1`, the N1 index roots contain per-fragment metadata at `<index_root>/<fragment_hash>/.fragment.ndx`. `NS` may read those roots for fragment authentication metadata, but it must not treat old Standard/indexdb, `namespace_v2`, or redb-era files as active N1 authority.

## Build And Run

From the workspace root:

```bash
cargo build -p ns --release
./target/release/litiaina-ns
```

For development:

```bash
cargo run -p ns --bin litiaina-ns
```

On first run, `NS` creates `ns.env` and `ns_config.ini` if they do not exist.

## Recommended Disk Layout

Use explicit directories that do not overlap with `N1`:

```text
/srv/litiaina/ns/metadata/metadata.ndxdb
/srv/litiaina/ns/staging
/srv/litiaina/ns/shares/fragments
```

`NS` creates one directory per authenticated N1 fragment under `share_root`. For example, a successful login to `my-fragment` creates:

```text
/srv/litiaina/ns/shares/fragments/<fragment-hash>
```

For multiple disks, configure `[disk.<id>]` sections and point shares at the disk they should live on. `NS` does not require ZFS/Btrfs/mdraid; those remain valid deployment choices, but NS can manage multiple independent share roots directly.

This keeps `NS` configurable without interfering with `N1` disk placement, shard stores, index stores, or GC behavior.

## Configuration

Default config shape:

```ini
[storage]
metadata_db_path=ns-data/metadata.ndxdb
staging_root=ns-data/staging
share_root=ns-shares/fragments
read_only=false

[n1]
base_url=https://127.0.0.1:50001
native_url=n1://127.0.0.1:50002
accept_invalid_certs=true
index_roots=/srv/litiaina/n1/index

[auth]
domain=LITI
allow_guest=false

[sync]
enabled=false
scan_interval_seconds=300
request_timeout_seconds=300
min_stable_age_seconds=120
cold_n1_prefix=

[admin]
enabled=true
bind_addr=127.0.0.1
port=50730
admin_key=

[smb]
enabled=true
bind_addr=0.0.0.0
smb_port=445
max_sessions=4096
max_frame_bytes=16777216
memory_budget_bytes=134217728
async_close_commit=false
async_close_threshold_bytes=67108864
require_signing=false
auth_domain=LITI
allow_guest=false
query_directory_page_limit=1000
query_directory_max_buffered_entries=4096
query_directory_max_seeded_entries=512

[disk.local]
root_path=ns-shares/fragments
enabled=true

[share.public]
disk_id=local
read_only=false
sync_enabled=false

[user.staff]
password=<smb-password>
enabled=true
shares=public
read=true
write=true
delete=true
admin=false
```

NS supports two SMB auth modes:

- Local NAS users from `[user.<name>]`, backed by the NS metadata database.
- N1 fragment-backed auth, where NS reads N1 fragment metadata from `index_roots`, verifies the SMB NTLMv2 login locally, and creates a dynamic local share.

For local NAS users:

```text
\\localhost\public
username: staff
password: <smb-password>
```

For N1 fragment-backed shares, no per-fragment config is required. Current N1 roots are expected to contain `<index_root>/<fragment_hash>/.fragment.ndx`.

Example:

```text
\\localhost\my-fragment
username: my-fragment
password: <N1 fragment secret>
```

The hash form is also accepted:

```text
\\localhost\<fragment-hash>
username: <fragment-hash>
password: <N1 fragment secret>
```

If the N1 server already has a fragment whose stored hash does not match the
plain share name you want users to type, configure an alias. This keeps the SMB
UX stable without changing N1:

```ini
[fragment_aliases]
main=0de266d684b84fe81dc1d792b2fb50577823b4164839ab622203bb61edd3341b
```

With that alias, users can connect as:

```text
\\localhost\main
username: main
password: <N1 fragment secret>
```

Alias targets may be either an existing 64-character fragment hash or a fragment
name that resolves to one. `fragment_policy.*` and `fragment_sync.*` sections may
also use the alias name.

## SMB Port

Port `445` normally requires root privileges or `CAP_NET_BIND_SERVICE` on Linux. For production, either grant the binary that capability or run behind a controlled service account:

```bash
sudo setcap 'cap_net_bind_service=+ep' ./target/release/litiaina-ns
```

For development, use a high port:

```ini
[smb]
smb_port=1445
```

## Auth And Shares

NS-local shares are explicit NAS shares.

- `[disk.<id>]` defines a local disk/root.
- `[share.<name>]` maps a share to a disk or explicit root path.
- `[user.<name>]` controls SMB credentials and share permissions.
- `read`, `write`, `delete`, and `admin` are separate permissions.
- Empty user share lists default to wildcard access using the user's permission flags.

Fragment-backed shares are still supported for N1 integration.

- SMB username must be the N1 fragment name or the 64-character fragment hash.
- SMB password is the N1 fragment secret.
- Tree connect must target the same fragment name or hash authenticated by the session.
- `read_only=true` under `[storage]` blocks write, rename, delete, and mkdir operations for all dynamic shares.
- `NS` does not enforce logical quotas. Capacity is the free space of the backing filesystem mounted at the share root.
- Folder deletion follows normal NAS behavior: clients may delete a populated folder tree, while deletion of the share root itself is rejected.
- SMB writes go directly to the local share path. NS does not stage normal SMB writes before commit. Staging is used only for cold-sync snapshots so N1 uploads never read from a moving live file.

## Cold Sync To N1

Cold sync is optional. It is a backup/archive path, not the live SMB path.

1. Create an `N1` fragment for the target backup data.
2. Set `[n1].base_url`.
   For native sync and restore, also set `[n1].native_url`.
3. Enable the global sync worker.
4. Set `min_stable_age_seconds` so NS only syncs quiescent files.
5. Add an optional `[fragment_sync.<fragment-name-or-hash>]` section for each fragment you want mirrored to `N1`.

Example:

```ini
[sync]
enabled=true
scan_interval_seconds=300
request_timeout_seconds=300
min_stable_age_seconds=120
cold_n1_prefix=nas

[n1]
base_url=https://127.0.0.1:50001
native_url=n1://127.0.0.1:50002

[fragment_sync.my-fragment]
enabled=true
secret=<N1 fragment secret>
n1_base_url=https://127.0.0.1:50001
cold_n1_prefix=nas/my-fragment
```

`fragment_sync` keys may use either the fragment name or the 64-character fragment hash. `NS` normalizes them to the fragment hash internally.

`fragment_sync.secret` is used only by the background cold-sync and restore worker. When `native_url` is configured, NS uses the low-latency `n1://` client. If only `base_url` is configured, NS falls back to the stable HTTP auth/upload surface:

```text
POST /n1/auth/fragment
```

That keeps `N1` unchanged. `NS` owns the SMB runtime and uses the stable `N1` auth surface only when it needs a token for background uploads.

Cold sync guardrails:

- `NS` only considers files whose last-modified time is older than `min_stable_age_seconds`.
- `NS` marks active SMB writes locally and skips those files during cold sync until they close cleanly.
- Interrupted or aborted SMB writes remain visible in the live NAS path for users to inspect or delete, but they are blocked from cold sync.
- `NS` creates a local snapshot copy under `staging_root` before uploading, so it never streams a moving live file directly to `N1`.
- If the live file changes while the snapshot is being prepared or while the upload is in flight, `NS` leaves the sync cursor behind so the next scan retries the newer version.
- If a live file is deleted while the scanner is working, `NS` treats it as a normal NAS race and skips it without failing the whole share scan.
- Large files are uploaded to `N1` through multipart sync when one-shot upload would exceed the N1 part-size limit.

Restore is admin-triggered and writes a downloaded N1 object back into the local share path:

```text
POST /ns/admin/restore
{
  "share": "public",
  "path": "folder/report.pdf"
}
```

Restore requires `[n1].native_url` and a matching `[fragment_sync.<fragment>]` secret.

## Admin API

The admin API is local by default:

```text
GET  /ns/admin/status
GET  /ns/admin/shares
POST /ns/admin/shares
GET  /ns/admin/disks
GET  /ns/admin/disks/status
POST /ns/admin/disks
GET  /ns/admin/users
POST /ns/admin/users
POST /ns/admin/restore
```

Set `[admin].admin_key` to require `x-ns-admin-key` on admin requests.

Optional per-fragment policy overrides are also supported:

```ini
[fragment_policy.my-fragment]
root=/srv/litiaina/ns/shares/marketing
read_only=false
```

If no policy override exists, `NS` creates the share automatically under `share_root/<fragment-hash>` and applies the global storage defaults.

## Systemd Example

```ini
[Unit]
Description=Litiaina NS
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/litiaina/ns
ExecStart=/opt/litiaina/bin/litiaina-ns
Restart=on-failure
RestartSec=5
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

If binding to port `445`, grant `CAP_NET_BIND_SERVICE` to the binary or run with an appropriate service policy.

## Firewall

Open only the SMB port you configured:

```bash
sudo ufw allow 445/tcp
```

For development high ports, open that port instead.

## Operations

Useful checks:

```bash
cargo check -p ns
cargo test -p ns
```

Operational rules:

- keep `NS` share roots separate from `N1` internals
- size `memory_budget_bytes` according to expected concurrent SMB transfers
- use `[disk.<id>]` and `[share.<name>]` for direct multi-disk share placement
- keep each sync fragment secret scoped to its target `N1` fragment
- keep `[n1].index_roots` readable by `NS` only when fragment-backed SMB auth is used
- keep `N1` reachable from `NS` only when cold sync is enabled
- use `/ns/admin/status` and `/ns/admin/disks/status` for basic operator checks

## Troubleshooting

- If clients cannot connect on port `445`, check Linux capabilities, firewall, and whether another SMB server is already bound.
- If local authentication fails, confirm `[user.<name>]` has a password or NT hash and includes the target share.
- If fragment authentication fails, confirm `[n1].index_roots` points at the N1 index roots, the SMB username is the fragment name or hash, and the password is the N1 fragment secret.
- If writes fail with insufficient storage, check free space under the share disk/root.
- If cold sync does not upload, confirm `[sync].enabled=true`, `[n1].native_url` or `[n1].base_url` is set, and the matching `[fragment_sync.<name-or-hash>]` section has `enabled=true` plus the correct fragment secret.
