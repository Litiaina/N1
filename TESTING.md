# Testing N1 and M1

Run the workspace checks from the repository root on Linux. Tests use temporary
storage. Set `TMPDIR` and `CARGO_TARGET_DIR` to writable paths with sufficient
space if the defaults are unsuitable.

```sh
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
cargo test -p m1 -p m1_storage --doc --locked
cargo clippy -p n1 -p m1 -p cli_core -p n1_shell --all-targets --no-deps --locked -- -D warnings
```

The [local quickstart](docs/QUICKSTART.md) and the
[native API walkthrough](docs/api/N1.md#native-rust-walkthrough) exercise the
installed N1 service. The [M1 API guide](docs/api/M1.md) includes examples for
the full stack. Tests marked ignored require the hardware, executables or
fault supervisor described in the [detailed test commands](docs/QUALIFICATION-HISTORY.md#detailed-test-commands).

## Qualification and benchmarks

| Report | Scope |
| --- | --- |
| [RC.2 qualification](docs/RC2-IMPLEMENTATION.md) | Release decision, artifact scope and known limits |
| [NOA and operator tests](docs/RC2-API-QUALIFICATION-20261124.md) | Standalone N1, named M1 and operator workflows |
| [Four-drive qualification](docs/FOUR-DRIVE-QUALIFICATION-20261117.md) | Member loss, repair and targeted power interruptions |
| [Million-object campaign](docs/MILLION-OBJECTS-20261108.md) · [Split-node checks](docs/SPLIT-NODE-20261111.md) | Scale, readback and retained-corpus recovery |
| [Standalone mixed workload](docs/N1-MIXED-WORKLOAD-20261116.md) | Concurrent uploads, reads, ranges and deletes |
| [Qualification history](docs/QUALIFICATION-HISTORY.md) | Earlier campaigns, harnesses and reproduction commands |

Reports identify the tested builds, hardware and workload. Compare measured
throughput and latency using the conditions in each report. The
[test profiles](docs/TEST-PROFILES.md) describe the reference configurations.
