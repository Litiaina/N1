# Procyon storage provenance and maintenance

Imported from Litiaina's ProcyonDB repository, directory `engine/`, commit
`7ee9a0685540b1d5a1d182379122d74b436907b2` (the source project remains unchanged).
Repository: <https://github.com/Litiaina/procyon-db>.
Package/library names remain `procyon-engine` / `procyon_engine`.
All original source modules, regression tests and binary recovery fixtures were
retained. The original full Procyon project is archived with the integration
evidence; its generic document service, Procyon wire protocols, HTTP listeners,
shell, benchmark executable and development certificates are not N1 services.

Copyright and the existing `LICENSE-APACHE` and `LICENSE-MIT` notices are retained.
The Fjall-derived source is not represented as newly authored N1 database code.

Selected fixes are backported from upstream
<https://github.com/fjall-rs/fjall> through the `3.1.13` release. This is not a
replacement of the Procyon source tree or a claim that every upstream change
has been imported. Patch context and crate references were adapted to Procyon.

| Upstream commits | Applied responsibility |
| --- | --- |
| `2672632`, `bcf4830` | Read/write process-lock descriptors and regression tests |
| `0cadc55` | Propagate poisoned journal mutex errors instead of panicking during cleanup |
| `e4377b4` | Close each optimistic transaction's snapshot exactly once; preserve concurrent snapshot ownership |
| `f4f7a56`, `864c185` | Enqueue compaction for recovered L0 data and retained regression |
| `c589756`, `2505216`, `2f7c596` | Release worker counters on failure/panic; account only actual thread spawns |
| `0020fd2`, `2e39418` | Restore missing active journal using the correct `.jnl` extension and regression |
| `8b2bae0`, `434377c` | Sync the journal before flushing memtables even without journal rotation |
| `1cc9fe8` | Prevent deleted keyspace identifiers from being reused across recovery |
| `2fa374c`, `0de9a9b`, `008550a` | Skip already-flushed journal items per keyspace; test partial cross-keyspace flushes and reopen memory accounting |
| `0a4693a` | Use the actual weak-removal operation in `remove_weak` |
| `f18624f` | Avoid blocking shutdown on a full worker message queue |

Local changes also propagate journal batch write errors, poison the database
before any failed batch can publish, propagate an ingestion journal-lock error
(matching upstream `dc4d14e`), and propagate a sealed-recovery clear error.
Linux `/dev/full` regressions separately cover write and sync failure without
memtable publication, later write refusal and acknowledged-data recovery.

The original four old-format fixture tests evaluated `matches!` without
asserting its result. They now assert rejection and copy their fixtures to
private temporary directories. V1's five-byte header is rejected as an unknown
format; V2 is rejected before its journal is interpreted. No legacy disk support
or fixture rewrite is introduced.

The external `lsm-tree` dependency is explicitly pinned to `3.1.10`, rather than
Procyon's yanked `3.1.4` lock entry. Upstream release fixes through this version
include blob relocation compression and clear/flush/compaction synchronization:
<https://github.com/fjall-rs/lsm-tree/releases/tag/3.1.10>.
N1's pre-existing dependency versions are retained; new dependencies and their
checksums are recorded in the workspace `Cargo.lock`.
