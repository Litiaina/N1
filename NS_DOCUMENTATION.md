# Litiaina NS Deployment Guide

Current version: `1.0.0`

`Litiaina NS` is the standalone NAS Lite / SMB service for the Litiaina stack. It is intentionally separate from `N1`: `NS` owns live SMB file serving and local share roots, while authentication remains fragment-backed and compatible with `N1` fragment secrets.

Use `NS` when clients need Windows/macOS/Linux file-manager access. Use `N1` as the storage kernel and optional cold-storage target.

## Architecture

`NS` has its own runtime state:

- `ns_config.ini` for SMB, N1 auth delegation, storage, and sync settings
- `ns.env` for process environment overrides
- an `indexdb` metadata database for fragment share records and sync cursors
- configurable dynamic share directories that hold the live NAS files
- a staging directory for safe SMB writes before commit
- an optional sync worker that uploads changed files to `N1`

Do not point `NS` share roots, metadata, or staging paths at `N1` index roots or blob roots. They are separate products and must not share internal storage directories.

For `N1 3.0`, the N1 index roots contain per-fragment metadata databases at `<index_root>/<fragment_hash>/indexdb/index.ndxdb`. `NS` may read those roots for fragment authentication metadata, but it must not treat old `namespace_v2` or redb-era files as active N1 authority.

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

For multiple disks, run separate `NS` instances with different `share_root` values, or mount the desired filesystem at the configured `share_root`.

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
```

No per-fragment config is required. `NS` reads N1 fragment metadata from the configured `index_roots`, verifies the SMB NTLMv2 login locally, and then creates the local NAS share automatically. For `N1 3.0`, those roots are expected to contain the per-fragment `indexdb/index.ndxdb` layout.

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

Fragment-backed shares are dynamic.

- SMB username must be the N1 fragment name or the 64-character fragment hash.
- SMB password is the N1 fragment secret.
- Tree connect must target the same fragment name or hash authenticated by the session.
- `read_only=true` under `[storage]` blocks write, rename, delete, and mkdir operations for all dynamic shares.
- `NS` does not enforce logical quotas. Capacity is the free space of the backing filesystem mounted at the share root.
- Folder deletion follows normal NAS behavior: clients may delete a populated folder tree, while deletion of the share root itself is rejected.

## Cold Sync To N1

Cold sync is optional. It is a backup/archive path, not the live SMB path.

1. Create an `N1` fragment for the target backup data.
2. Set `[n1].base_url`.
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

[fragment_sync.my-fragment]
enabled=true
secret=<N1 fragment secret>
n1_base_url=https://127.0.0.1:50001
cold_n1_prefix=nas/my-fragment
```

`fragment_sync` keys may use either the fragment name or the 64-character fragment hash. `NS` normalizes them to the fragment hash internally.

`fragment_sync.secret` is used only by the background cold-sync worker. `NS` mints upload tokens through the existing stable `N1` fragment-auth route:

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
- put staging on a disk with enough temporary write capacity
- keep each fragment secret scoped to its target `N1` fragment
- keep `[n1].index_roots` readable by `NS` for local dynamic SMB authentication
- keep `N1` reachable from `NS` only when cold sync is enabled
- use logs as the primary monitoring surface until a dedicated NS HTTP metrics API is added

## Troubleshooting

- If clients cannot connect on port `445`, check Linux capabilities, firewall, and whether another SMB server is already bound.
- If authentication fails, confirm `[n1].index_roots` points at the N1 index roots, the SMB username is the fragment name or hash, and the password is the N1 fragment secret.
- If writes fail with insufficient storage, check free space under both the share root and staging root.
- If cold sync does not upload, confirm `[sync].enabled=true`, `[n1].base_url` is set, and the matching `[fragment_sync.<name-or-hash>]` section has `enabled=true` plus the correct fragment secret.
