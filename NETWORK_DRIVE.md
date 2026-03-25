# N2 Network Drive Guide (SMB-lite)

> Deployment profile status (2026-03-09):
> - `N2` is the product identity of this workspace.
> - `N1` is the shared storage engine underneath the deployment shapes.
> - `n1` (`litiaina-n1`) is the stable/default single-node deployment path.
> - `n2` (`litiaina-n2`) is the multi-node deployment path.
> - `n1-arc` (`litiaina-n1-arc`) is experimental.
> - See `DEPLOYMENT_PROFILES.md`.

This guide covers the SMB-lite path for the single-node N2 deployment, which is powered by the N1 engine:

- Native SMB-lite gateway (`smb`) built into `litiaina-n1` (recommended).
- Legacy `fsd_mount + Samba` bridge note for older deployments; new workspace builds do not ship that bridge.

## Current Status

`smb` is SMB-lite, not full enterprise SMB.

Implemented in `n1/src/smb/server.rs` (stable profile), with mirrored implementation under `n1-arc/src/smb/server.rs`:
- auth + signing
- `list/read/open`
- query-directory / query-info metadata paths
- non-mutating `SET_INFO` compatibility paths
- conservative share-mode conflict handling
- conservative SMB2 lock ownership handling

Not implemented:
- write/create/rename/delete/mkdir/rmdir/truncate/delete-on-close mutation flows
- oplocks / leases
- durable or persistent handles
- SMB multichannel / advanced enterprise semantics

## Mode A: Native SMB-lite (`smb`) Recommended

### 1) Configure `config.ini`

Add `[smb]`:

```ini
[smb]
enabled=true
bind_addr=0.0.0.0
smb_port=445
max_sessions=4096
max_frame_bytes=16777216
async_close_commit=false
async_close_threshold_bytes=67108864
auth_domain=N1
require_signing=true
query_directory_page_limit=1000
query_directory_max_buffered_entries=4096
query_directory_max_seeded_entries=512
```

Config keys:
- `enabled`: enable SMB-lite listener.
- `bind_addr`: interface bind address.
- `smb_port`: SMB TCP port.
- `max_sessions`: connection/session cap.
- `max_frame_bytes`: SMB frame size cap.
- `async_close_commit`: retained for compatibility, ignored in SMB-lite transport mode.
- `async_close_threshold_bytes`: retained for compatibility, ignored in SMB-lite transport mode.
- `auth_username`: optional static SMB user.
- `auth_password`: optional static SMB password.
- `auth_domain`: SMB auth domain string (optional).
- `require_signing`: enforce SMB signing.
- `query_directory_page_limit`: backend page size used per SMB `QUERY_DIRECTORY` fetch (`16..1000`).
- `query_directory_max_buffered_entries`: max filtered directory entries buffered per open handle (`32..8192`).
- `query_directory_max_seeded_entries`: max initial seeded entries (`.`, `..`, pending-commit entries) per scan (`8..2048`, capped by `query_directory_max_buffered_entries`).

Preferred auth mode is fragment-scoped:
- SMB `username` is fragment identifier (plain fragment name or 64-hex fragment hash).
- SMB `password` is that fragment key.
- `\\<server>\N1` (or `smb://<server>/N1`) binds the SMB session to the authenticated fragment.

Optional static mode:
- if `auth_username` + `auth_password` are configured, that static account is also accepted.

Progress API (JWT protected):
- `GET /v4/smb/progress?include_completed=true&limit=100`
- In the current read-only SMB mode this normally stays empty because SMB uploads are disabled.

### 2) Optional env overrides

`smb` also reads:
- `N1_SMB_ENABLED`
- `N1_SMB_BIND_ADDR`
- `N1_SMB_PORT`
- `N1_SMB_MAX_SESSIONS`
- `N1_SMB_MAX_FRAME_BYTES`
- `N1_SMB_ASYNC_CLOSE_COMMIT`
- `N1_SMB_ASYNC_CLOSE_THRESHOLD_BYTES`
- `N1_SMB_AUTH_USERNAME`
- `N1_SMB_AUTH_PASSWORD`
- `N1_SMB_AUTH_DOMAIN`
- `N1_SMB_REQUIRE_SIGNING`
- `N1_SMB_QUERY_DIRECTORY_PAGE_LIMIT`
- `N1_SMB_QUERY_DIRECTORY_MAX_BUFFERED_ENTRIES`
- `N1_SMB_QUERY_DIRECTORY_MAX_SEEDED_ENTRIES`

Template:
- `deploy/network-drive/env/smb.env.example`

### 3) Start N1 server

```bash
cargo run -p n1 --bin litiaina-n1
```

Or run your installed systemd service for `litiaina-n1`.

### 4) Verify listener

```bash
ss -ltnp | rg ':445'
```

You should also see startup logs similar to:
- `smb listener online at ...`
- `smb loop started ...`

### 5) Mount from clients

Windows:
- File Explorer -> Map Network Drive -> `\\<server_ip>\N1`
- Username: fragment name or fragment hash.
- Password: fragment key.
- Domain: `N1`.

Linux:

```bash
sudo mount -t cifs //<server_ip>/N1 /mnt/n1 \
  -o username=<fragment_name>,password='<fragment_key>',domain=N1,sec=ntlmssp,vers=2.1,ro,noperm
```

macOS:
- Finder -> Connect to Server -> `smb://<server_ip>/N1`
- Username: fragment name or fragment hash.
- Password: fragment key.

## Mode B: Fallback Bridge (`fsd_mount` + Samba)

This legacy bridge path is retired in the current workspace. The supported network-drive path is the native SMB server in `n1`.

### Host setup

```bash
sudo apt update
sudo apt install -y samba fuse3
```

### Mount env template

Use:
- `deploy/network-drive/env/fragment.env.example`

### systemd template

Use:
- `deploy/network-drive/systemd/n1-fsd-mount@.service`

### Samba share template

Use:
- `deploy/network-drive/samba/n1.conf.example`

## Behavior Notes

- N1 remains object storage core; SMB is an adapter surface.
- Fragment is still the isolation boundary.
- `N1` share alias maps to the fragment authenticated in the SMB session.
- Native SMB is intentionally read-only to avoid fighting object-storage semantics with desktop filesystem mutation behavior.
- Keep signing enabled for untrusted networks.

## Troubleshooting

### Permission denied binding port 445

Use root/systemd service or bind to a higher port for testing.

### Mount works but I/O errors occur

Check:
- N1 server logs
- fragment auth config
- share path mapping and fragment visibility

### Unexpected share conflicts

This is expected with conservative share-mode handling. Retry after releasing conflicting client handles.

### Client expects advanced SMB features

`smb` is intentionally SMB-lite. Features like oplocks/leases and durable handles are out of scope.

## Verification Commands

```bash
cargo check -p n1
cargo test -p n1 smb::server::tests -- --nocapture
cargo test -p n1 -- --nocapture
```

Experimental arc track verification:

```bash
cargo check -p n1-arc
```
