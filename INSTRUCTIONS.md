# N1 Instructions

N1 does not keep runtime files in the repository.

When N1 starts inside a fresh deployment directory, it creates the local files
it needs for that machine. Those generated files are intentionally ignored and
must stay out of git.

## Build

```bash
cargo build -p n1 --release
cargo build -p n1_cli --release
```

## First Run

Run N1 from the deployment directory:

```bash
./target/release/litiaina-n1
```

If local runtime files are missing, N1 creates them. Stop the process, fill the
real machine values, then start it again.

## Runtime Files

`n1.env` is local secret material.

`n1_config.ini` is local machine configuration: ports, disk roots, fragment
quota backend, erasure layout, and related deployment settings.

Production disks should already be mounted before N1 starts. If XFS quota is
enabled, mount the disks with project quota support.

## Repo Rule

The repo stores source, documentation, and test tooling only. Runtime config,
local profiles, generated certificates, and machine-specific files do not live
in git.
