# Metadata maintenance qualification — November 5, 2026

This is the historical maintenance checkpoint before the key-only upload and
purge change. Its retirement/410 observations describe that tested executable.
The current lifecycle is specified in [LINK-FORMAT.md](LINK-FORMAT.md): one LINK
key, existing-file continuation, zero-residue purge and metadata-owned retry
intent. Earlier passes do not qualify a changed executable.

**Passed for the scope below. RC.2 remains in development on `link`.**
This campaign adds metadata-owned lifecycle and maintenance while preserving
the LINK object engine. It does not change pool geometry, placement, payload
format, object-engine admission or object repair internals.

## Implemented behavior

Named deletion changes metadata only. GC selects eligible records through ordered
database indexes, durably claims them, calls native object deletion and purges
metadata only after confirmation. Recovery loses the claim race cleanly. A
failed deletion retains ownership and retry state. TTL applies to automatic and
manual passes; neither offers early purge or empty trash. Completed live versions
are excluded from unfinished-upload expiration.

The metadata INI controls automatic scheduling, interval, trash TTL, batch size,
object concurrency, cooperative pause and migration byte rate. Maintenance uses
its own bounded executor and native connections. Routine passes do not traverse
the namespace or shard directories. Index initialization reads database records
before request admission and records completion transactionally.

Explicit integrity/healing resolves indexed version references and calls the
existing object API. Pool migration reconstructs source bytes, uploads to a new
key in the selected destination pool, compares full source/destination SHA-256,
atomically switches the private metadata reference, then retires the unreferenced
source. Names, version IDs and ETags remain stable. Partial transfer intents,
strict parts, restart, cancellation and post-switch cleanup are durable. Export
pins prevent source retirement during a pinned export.

The original Console maintenance cards expose these operations and effective GC
policy. Mounted-fragment actions remain scoped. Incompatible fast/durability
options are not offered for strict migration. N2 behavior and the existing
Console product layout remain intact.

## Completed checks

| Check | Result | Evidence under the campaign root |
| --- | --- | --- |
| Workspace, all targets | 456 passed; 0 failed; 33 opt-in/helper cases ignored | `reports/workspace-final.log` |
| Metadata + imported storage documentation | 81 passed; 0 failed | `reports/docs-final02.log` |
| Scoped Clippy, warnings denied | Passed for object, metadata, CLI core and Shell | `reports/clippy-final.log` |
| Complete selected two-HDD service run | 18 passed; 0 failed; 681.80 seconds | `reports/physical06.log` |
| Six maintenance cases repeated against the packaged metadata executable | 6 passed; 0 failed; executable hashes unchanged throughout | `reports/physical07-packaged.log`, `physical07-binaries-after.log` |
| Metadata library | 32 passed, including nine new lifecycle/read-race cases | `reports/lib-final.log` |
| Original Console desktop regression | 151 passed; 6 opt-in cases skipped | `reports/console-desktop02.log` |
| Four service/maintenance browser cases across five viewports | 20 passed; overlaps desktop coverage | `reports/console-browser-final.log` |
| Console Node checks | 38 passed; 0 failed | `reports/console-node-final.log` |
| Console build/static checks | Build, Svelte/TypeScript and syntax checks passed | `reports/console-build03.log`, `console-check01.log` |
| Object-component preservation | No source changes in object engine, LINK, native client or CLI core | `reports/object-preservation.log` |

The default workspace includes one case checking 410 EC corruption/loss
combinations; these are not 410 additional physical tests. The 18 explicit
service cases are additional to default passes and include six new maintenance
cases. Browser viewport repetitions are not new distinct test functions.

## Physical and browser fault coverage

Both actual HDDs hold private new test roots. PROJECT uses XFS on rotational
ST1000LM049, serial `hdd-0-device-id`; TEST uses ext4 on ST1000LM049, serial `hdd-1-device-id`.
Control/database and one member share PROJECT; the other member uses TEST.
Device letters may change across reboot. The source NVMe holds no campaign
population, reports or build target. Workloads and payload oracles are Rust;
Node/Chromium drive the original Console. No Python is used.

The new cases verify:

- Trash before/after TTL, recovery, repeated deletion, all historical versions,
  fragment isolation, dry run, request bounds, backend outage, durable retry
  after both processes restart, and an enabled automatic schedule.
- An actually expired partial native upload, failed deletion during backend
  outage, metadata restart and subsequent cleanup without losing the intent.
- Check-only reporting of lost shards without implicit repair; explicit scoped
  healing reconstructs byte-identical shards while another fragment stays
  unchanged.
- Migration from **1+1 to 2+0 EC**, including empty payloads, retained history
  and a payload larger than one part. Actual SIGKILL occurs after destination
  staging bytes exist. Restart completes, payload/version checks pass and old
  keys are retired. Reverse migration cancellation preserves named reads.
- Unreadable-source migration fails without switching metadata. After metadata
  restart, Stop retires only the unswitched destination. Restoring the source
  fixture permits exact original-version reads.
- Headless production Console buttons start real migration and TTL GC through
  metadata and the native object service; Rust independently verifies their
  results. Screenshots and terminal JSON remain beside each fixture.

The broader repeated cases cover in-flight metadata/object/both SIGKILL, both
member-loss directions, corruption, failure beyond parity, full metadata/staging
loss and snapshot restore, encrypted replica recovery, blank replacement,
standard/durable resume and finite/unlimited admission. The resume-scale case
reserves 15 GiB but transfers **1 GiB**: 128 strict 8 MiB parts. Indexed status
after restarting both processes submits **zero native payload I/O**. This is
not a complete 15 GiB upload benchmark.

Two additional transport-boundary cases force a resolved old key to return 410
after a placement switch. Only the same unchanged immutable version is retried
at its new key, with the original range. Retirement without a switch is returned
as failure without replay. Existing partial-stream tests remain in the suite.

## Failures found and corrected

The first broad run found that an incomplete migration loaded from the database
could not be explicitly stopped after restart: Stop consulted only active memory
slots. Stop now loads persisted state and launches cancellation cleanup when
needed. `physical05.log` retains its **17 passed / 1 failed** result;
`physical06.log` repeats all 18 successfully.

The original Shell truncated-download regression also exposed a partial-file
write race: process exit could precede Tokio's queued file write. The Shell now
flushes and synchronizes the output before returning the source error. The full
workspace passes with the original assertion unchanged.

Earlier test setup used overlapping control/member parents and collided on its
private directory. A recovery assertion also incorrectly expected whole-request
409 rather than NOA's per-path conflict response. Both test defects were fixed
without weakening TTL, data, authorization or recovery checks. Their raw logs
remain under `reports/physical01.log` and `physical02.log`.

After the workstation rebooted during the interruption, a documentation check
initially failed because its RAM temporary directory no longer existed. The
directory was recreated and all 81 documentation checks passed. This is recorded
in `docs-final.log` and `docs-final02.log`; it is not an engine failure or a
controlled power-loss qualification.

## Evidence and limits

Campaign root: `/mnt/PROJECT-DISK-1TB/n1-maintenance-20261104-01`.
The object binary is the unchanged previously qualified executable, SHA-256
`95c8dac28b4adca91a59eacfb952f292bef37ec0f52f0239a931998941f784c2`.
Packaged metadata/Shell hashes, source checkpoints, browser assets and terminal
fixture results are retained under `reports/`, `source/` and `artifacts/`.
The invocation for reproducing the selected physical run is in
[TESTING.md](../TESTING.md).

One debug regression measured 1,000 idle index queries against 10,000 future
trash rows in **46.643 ms** with no namespace entries. This checks the bounded
seek path; it is not an old/new GC speed, cold-cache, RSS or saturated throughput
comparison. Background work still consumes device bandwidth and fsync time;
separate admission does not eliminate physical I/O contention.

This campaign does not rerun 500K object population, forcibly unmount the HDD
used by the independent user deployment, simulate firmware/power failure or
qualify larger physical EC topologies. Prior evidence retains its own scope.
At this campaign's checkpoint, RC.2 was unreleased and `main` was held. Current
development is on `main`; RC.2 remains unreleased. Other completion gates remain in
[RC2-IMPLEMENTATION.md](RC2-IMPLEMENTATION.md).
