<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N1</h1>
  <p>
    Single-node object storage system for Litiaina: stable object identity,
    namespace operations, multipart uploads, erasure-coded payloads, repair,
    lifecycle garbage collection, and a native <code>n1://</code> protocol for
    trusted internal tools.
  </p>
</div>

---

N1 is Litiaina's single-node object storage system. It is not a generic file
server and it is not the distributed N2 product. N1 owns local object storage
truth on one machine: fragments, namespace entries, immutable object versions,
payload placement, health checks, repair, and garbage collection.

This repository is the N1 product boundary. N2, Registry, NS, and other
Litiaina services should treat N1 as a stable storage system and integrate
through its supported APIs or through the native client crate.

## What Is Included

- `n1/` - N1 server and storage engine.
- `n1_cli/` - operator CLI for N1 inspection and maintenance.
- `n1_native_client/` - canonical Rust client for the `n1://` protocol.
- `simd-erasure-core/` - internal erasure-coding core used by N1.
- `cli_core/` - small shared CLI support crate used by `n1_cli`.
- `tools/n1-release-proof/` - N1 release-proof and hardware validation tooling.
- `N1_DOCUMENTATION.md` - user-facing N1 documentation.
- `N1_ARCHITECTURE.md` - storage architecture and invariants.
- `N1_TESTING.md` / `N1_VERIFICATION.md` - validation and release evidence.
- `INSTRUCTIONS.md` - operator flow without committed runtime files.

## Storage Identity

N1 separates mutable namespace identity from stable storage identity.

```text
<root>/
  .fragment/<fragment_hash> -> ../@f_<stable_fragment_id>
  @f_<stable_fragment_id>/
    .fragment.ndx
    .objects/
    .blobs/
    .uploads/
    .trash/
```

Fragment names and aliases may change. Stable fragment roots do not. Object
records are immutable, payload versions are immutable, and namespace movement
does not rewrite object truth.

## API Faces

N1 exposes two supported faces:

- `/n1/...` over HTTPS for general clients, browsers, and integrations.
- `n1://` for Litiaina-owned low-level tools and services.

Both faces are expected to preserve the same object-storage semantics. The
native protocol is not a separate storage engine.

## Build

```bash
cargo build -p n1 --release
cargo build -p n1_cli --release
```

Check the N1 server binary:

```bash
./target/release/litiaina-n1 --version
```

## Run Locally

N1 creates local runtime configuration when started in a fresh deployment
directory. Those files are machine-local and intentionally ignored by this repo.

```bash
./target/release/litiaina-n1
```

See `INSTRUCTIONS.md` for the deployment flow.

## Validation

Run the core test suite:

```bash
cargo test -p n1
```

Run CLI tests:

```bash
cargo test -p n1_cli
```

Run the release proof harness when validating a real deployment:

```bash
cargo run -p n1-release-proof --release -- --help
```

Long hardware proofs should be run against mounted production-like disks, not
against temporary development directories.

## Documentation

Start here:

- [N1_DOCUMENTATION.md](N1_DOCUMENTATION.md)
- [N1_ARCHITECTURE.md](N1_ARCHITECTURE.md)
- [INSTRUCTIONS.md](INSTRUCTIONS.md)
- [N1_VERIFICATION.md](N1_VERIFICATION.md)
