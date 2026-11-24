# RC.2 API and operator qualification — November 24–25, 2026

This record maps the release candidate's public N1 and M1 operations and
Console workflows to existing and new checks. The references are the
[NOA specification](../NOA.md), [N1 API](api/N1.md) and [M1 API](api/M1.md).

## Candidate and fixtures

| Component | Qualified build |
| --- | --- |
| N1 `1.0.0-RC.2` | SHA-256 `f1a54f1653c1c90ae51416fa1f53474855cc20e1ea7ef52e0561af2c8c166aab` |
| M1 `1.0.0-RC.1` | SHA-256 `f32dd61d283b549b52587c9905ad5c793de70e772a8d5067b1128b0de157b039` |
| N1 backup helper `1.0.0-RC.2` | SHA-256 `b87a5fa7b7c1634f2a3263dcf15922dd338286d00331cf50e649d4eceadc5f64` |
| Console | Source `d3d98e52b57ea109c2623cff0e8bc601a24345ba`. All 5,741 packaged static files verified. |
| Optional N1 Shell operator `1.0.0-RC.2` | SHA-256 `013874da66867f7c7896429b199f5daefd2fc1ed67ccb3261496ab8b7e8d2394` |

The live checks use the binaries and Console assets from the extracted Linux
x86-64 bundle. The four-drive reference host has four independent XFS HDDs for
N1 and an NVMe filesystem for M1. The fixtures use private 3+1, 2+2 and
two-member 1+1 pools. The final browser walkthrough ran on a separate two-HDD
XFS 1+1 fixture using the same binaries and Console assets.
The optional Shell operator proof uses a private 1+0 pool on the NVMe host
filesystem and independent repository directories. The simultaneously running
deployment and the power-loss corpus use separate paths. The workload uses
loopback endpoints. The earlier [TLS and four-drive results](FOUR-DRIVE-QUALIFICATION-20261117.md#system-candidate-verification--november-24)
belong to the same selected server executables and supply the existing
integration, restart and maintenance evidence.

## Contract evidence

| Area | Observed request, response, authorization or retry behavior | Evidence |
| --- | --- | --- |
| M1 connection and fragment lifecycle | HTTP/native version and frame checks, create and identity lookup, per-fragment listing, scoped denial, master-key rename, stable ID, key rotation and old credential revocation, exact confirmation and deletion | `public_noa_metadata_object_restart_and_native_contract`, `named_namespace_permissions_and_response_fields_match_noa_v1`, `fragment_alias_changes_and_key_rotation_preserve_identity_and_enforce_revocation`, real fragment creation in Console |
| Namespace and events | Virtual prefixes, mkdir refusal, POSIX file fields, bounded pages, cross-path and cross-fragment cursor rejection, native stat, per-item bulk results, soft trash and recovery, retired purge refusal, NDJSON completion and fragment-scoped WebSocket invalidation | `named_namespace_permissions_and_response_fields_match_noa_v1`, Console create and event witness, existing [search and archive cases](FOUR-DRIVE-QUALIFICATION-20261117.md#system-candidate-verification--november-24) |
| M1 named reads and publication | Exact downloaded bytes, versions, signed read and range, invalid range, 503 before streaming when admission is busy, terminal failure on late corruption, one-shot and batch, exact resume handle, immutable retry and no duplicate publication | `public_noa_metadata_object_restart_and_native_contract`, `deep_scan_distinguishes_saturated_upload_admission_from_confirmed_corruption`, `named_reads_fail_on_late_damage_without_publishing_corrupt_bytes`, `negotiated_init_streams_without_staging_and_reuses_identity_after_restart`, `native_finalize_failure_reconciles_lost_staging_and_cancel_removes_acknowledgements` |
| M1 maintenance, recovery and exports | Cached health versus integrity check, scan start/stop, permission-gated snapshots, downloaded snapshot restored with version and trash state, actual repair, GC outage and retry, cross-EC migration restart and cancellation, ZIP/TAR streaming, scope and archive retention/cleanup | `console_operator_backup_restores_versions_and_runs_original_upload_workflows`, `console_object_status_matches_link_health_and_verifies_repair`, `real_console_runs_pool_migration_and_metadata_trash_gc`, `pool_migration_changes_ec_preserves_versions_and_resumes_after_process_kill`, [existing four-drive cases](FOUR-DRIVE-QUALIFICATION-20261117.md#system-candidate-verification--november-24) |
| Direct LINK NOA v2 | HTTP envelope, scoped native/HTTP sessions, conflicting part refusal, public signed grant, conditional HEAD/GET and ranges, repeated finalize, permanent purge and absence of old upload identity; native batch, TLS, quotas, incomplete bodies, maintenance and backup operation controls | `link_http_and_native_share_permissions_ranges_and_immutable_retries` on the selected N1 on both four-drive layouts, plus the 21 cases in [N1 socket suite](../object_engine/src/protocol/server/tests.rs) |
| N1 object backup and helper | Authorized Console connection, online capture, offline refusal while source is live, verification, replica, restored bytes and purged identity absence; the packaged N1 backup helper independently lists and verifies the replicated point | `console_operator_captures_replicates_restores_and_preserves_retirement`, packaged helper `list` and `verify` |
| Installation and settings | Default startup leaves fragment creation to the operator. Released engine versions, private INI/env paths and verified TLS work in both launcher profiles. Rendered systemd units validate. Mixed HTTP/native quota enforcement, GC policy/retry, integrity and healing controls retain their tests. | [Packaging and service results](FOUR-DRIVE-QUALIFICATION-20261117.md#package-and-physical-test-preparation), [admission and overload](FOUR-DRIVE-QUALIFICATION-20261117.md#sustained-http-and-native-admission), existing [operator and fault report](M1-CONSOLE-20261117.md) |

The [three live contract cases](../metadata_engine/tests/noa_service/contract.rs)
ran on both 3+1 and 2+2. The 21 N1 socket cases exercise Rust service code in
process. The separate live cases use the actual packaged executable. The test
fixtures preserve documented boundaries for
virtual directories, retired manual purge, M1's transport-specific operations,
and the optional Shell operator and Accel services.

## Console walkthrough

The packaged Console starts with fresh browser storage. A wrong master key
returns 401 and displays the failed-create error. The correct key creates and
mounts the fragment. The browser receives the new fragment's namespace event
after a write and verifies its scoped inventory. Another fragment's write
produces no event in this session.

The mounted user is asked for the master key to list M1 snapshots. An invalid
key leaves the user session intact. The successful session lists, creates and
downloads a snapshot. The test restores 51 records into a separate M1 database,
compares both immutable versions byte for byte and confirms that the trashed
object stays absent from normal reads.
Four live browser tests exercise rename/TAR, re-added multipart resume,
small-object batch, and reload/resume without repeating acknowledged parts.
Further live browser checks cover migration, GC retry, search and the repair
of a missing shard. The optional Shell operator has a separate HTTPS
capture/replicate/restore workflow.

The original 3+1 attempts at the object-status browser case, negotiated
multipart geometry and staged-loss case used 1+1 fixture assumptions. The
tests passed with the two-member geometry they expected. A browser fixture
also timed out once at fragment submit. The final run waits for the populated
fragment hash preview before submitting and checks the full create-error
message. The first local attempt also reused a fixture parent for member data
and control state. The corrected run uses separate directories. The original
logs are retained.

The [release checklist](RC2-IMPLEMENTATION.md) tracks latency, physical
interruption coverage, and publication separately.
