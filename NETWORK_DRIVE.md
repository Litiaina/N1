# N1 Network Drive Guide (SMB-lite)

This guide covers both ways to expose N1 as a network drive:

- Native SMB-lite gateway (`fsgwd`) built into `litiaina-n1` (recommended).
- Fallback bridge (`fsd_mount` + Samba) for environments where you still need it.

## Current Status

`fsgwd` is SMB-lite, not full enterprise SMB.

Implemented in `server/src/fsgwd/server.rs`:
- auth + signing
- `list/read/write/create/rename/delete/mkdir/rmdir`
- query-directory / query-info / set-info metadata paths
- delete-on-close behavior
- conservative share-mode conflict handling
- conservative SMB2 lock ownership handling

Not implemented:
- oplocks / leases
- durable or persistent handles
- SMB multichannel / advanced enterprise semantics

## Mode A: Native SMB-lite (`fsgwd`) Recommended

### 1) Configure `config.ini`

Add `[fsgwd]`:

```ini
[fsgwd]
enabled=true
bind_addr=0.0.0.0
smb_port=445
max_sessions=4096
max_frame_bytes=16777216
auth_username=n1smb
auth_password=change_me
auth_domain=N1
require_signing=true
```

Config keys:
- `enabled`: enable SMB-lite listener.
- `bind_addr`: interface bind address.
- `smb_port`: SMB TCP port.
- `max_sessions`: connection/session cap.
- `max_frame_bytes`: SMB frame size cap.
- `auth_username`: SMB auth user (optional, but recommended).
- `auth_password`: SMB auth password (optional, but recommended).
- `auth_domain`: SMB auth domain string (optional).
- `require_signing`: enforce SMB signing.

If `auth_username` + `auth_password` are not set, authentication becomes permissive SMB-lite behavior. Use explicit credentials in production.

### 2) Optional env overrides

`fsgwd` also reads:
- `N1_FSGWD_ENABLED`
- `N1_FSGWD_BIND_ADDR`
- `N1_FSGWD_SMB_PORT`
- `N1_FSGWD_MAX_SESSIONS`
- `N1_FSGWD_MAX_FRAME_BYTES`
- `N1_FSGWD_AUTH_USERNAME`
- `N1_FSGWD_AUTH_PASSWORD`
- `N1_FSGWD_AUTH_DOMAIN`
- `N1_FSGWD_REQUIRE_SIGNING`

Template:
- `deploy/network-drive/env/fsgwd.env.example`

### 3) Start N1 server

```bash
cargo run -p server --bin litiaina-n1
```

Or run your installed systemd service for `litiaina-n1`.

### 4) Verify listener

```bash
ss -ltnp | rg ':445'
```

You should also see startup logs similar to:
- `fsgwd listener online at ...`
- `fsgwd smb loop started ...`

### 5) Mount from clients

Windows:
- File Explorer -> Map Network Drive -> `\\<server_ip>\<fragment_or_share_name>`

Linux:

```bash
sudo mount -t cifs //<server_ip>/<share> /mnt/n1 \
  -o username=n1smb,vers=3.1.1
```

macOS:
- Finder -> Connect to Server -> `smb://<server_ip>/<share>`

## Mode B: Fallback Bridge (`fsd_mount` + Samba)

Use this only if you need the previous bridge model.

### Build

```bash
cargo build --release -p server
cargo build --release -p server_cli --features fsd-mount --bin fsd_mount
```

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
- Prefer one share scope per fragment boundary.
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

`fsgwd` is intentionally SMB-lite. Features like oplocks/leases and durable handles are out of scope.

## Verification Commands

```bash
cargo check -p server
cargo test -p server fsgwd::server::tests -- --nocapture
cargo test -p server -- --nocapture
```
