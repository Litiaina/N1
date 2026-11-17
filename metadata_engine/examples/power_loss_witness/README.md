# Independent power-loss ACK witness

This Rust example connects to an already running, isolated N1 or M1. It does
not start engines, switch power, format disks or create fragments. Run it on a
separately powered client with its journal on persistent storage. Keep that
client and the network powered throughout a server interruption.

Use only the task's private pool/database and credentials. Choose a native-aligned
part size for the actual immutable pool. The example caps parts at 8 MiB and
uses three full parts plus a 17-byte tail by default; these are witness bounds, not server
limits. One object is processed at a time, so every complete successful reply
can be synchronized immediately before any next request. This is a recovery
witness, not a throughput benchmark.

An optional `payload_parts` configuration field selects 3–127 full parts plus
the same 17-byte tail. For example, 63 with 6 MiB parts creates a 378 MiB object
with 64 transfer parts, providing a longer active-write window for a manual
power cut. Memory remains bounded by one part. The original default configuration
encoding and retained journal bindings stay identical; existing ACKs can still
be verified without replacing their population. Total journal operations are
bounded independently of the server's object-size limits.

```sh
cargo build --release --locked --offline -p m1 --example power_loss_witness
```

Private configuration, with paths replaced for the actual recorder:

```json
{
  "backend": "n1",
  "url": "n1s://storage.example.test:57612",
  "ca": "/test/recorder/ca.pem",
  "secret_env": "/test/recorder/n1.env",
  "fragment": "root",
  "part_bytes": 6291456
}
```

N1 uses `AUTH_KEY` from the private environment file. For M1, set `backend` to
`m1`, use its HTTPS URL and existing private fragment hash, and supply a private
file containing `MASTER_KEY`. Authentication tokens and credentials are never
written to the ACK journal. TLS verification remains enabled.
The native witness explicitly requests a one-hour access token for its bounded
manual window; the server's configured access-token maximum is still enforced.

```sh
# New journal only. Save stdout/stderr separately; do not interrupt the client
# to trigger the server fault. After the server loses power, allow the pending
# response/connection failure to drain and the recorder to exit normally.
power_loss_witness write --config /test/recorder/n1.json \
  --journal /test/recorder/new-acks.jsonl --objects 32

# After the same engines/data return, verify before any repair or re-upload.
# Each attempt requires a new report; a failed journal is never truncated.
power_loss_witness verify --config /test/recorder/n1.json \
  --journal /test/recorder/new-acks.jsonl \
  --report /test/recorder/verify-01.json

# Optional explicit recovery performs the complete read-only pass first.
# It fills missing unacknowledged parts, finalizes and checks complete SHA-256.
power_loss_witness recover --config /test/recorder/n1.json \
  --journal /test/recorder/new-acks.jsonl \
  --report /test/recorder/recover-01.json

# Separate read-only redundancy audit after recovery, outside throughput timing.
power_loss_witness audit --config /test/recorder/n1.json \
  --journal /test/recorder/new-acks.jsonl \
  --report /test/recorder/deep-audit-01.json
```

The first object is a fully published normal-mode control. Following objects
use strict part acknowledgements. Every fourth cycle retains a sparse strict
upload with parts 0 and 2 acknowledged; another completes then deletes an object.
All source bytes derive from the recorded object index and offset. The journal
retains source hashes, original plans, intended requests, complete successful
replies and uncertain failures. File creation synchronizes the directory; every
intent and ACK synchronizes the journal. An exclusive file lock prevents two
recorders from using the same journal. Reopening validates its sequence,
configuration and complete lines without discarding a partial record.

Verification checks every retained published object's length/SHA-256, M1 version,
strict part progress and acknowledged absence. An upload with no finalize attempt
must still be unreadable. A finalize without a successful reply remains uncertain
and may already have completed. Normal intermediate ACKs and an unused Begin
carry no power-loss persistence promise. An unacknowledged deletion
is recorded as uncertain and can be retried explicitly. Recovery refuses to
resend an acknowledged strict part or recreate a wholly absent identity.
M1 deletion means trash; N1 deletion means physical purge. This witness does not
equate the two or silently run M1 GC.

The separate audit requires every live native object's deep checksum result to
have all configured shards present, no corruption/unavailable members, and the
full expected verified length. For M1 it starts the existing fragment diagnostic
and requires every referenced version to verify, including retained trash/history
references. That M1 audit covers the fragment rather than only the journal's
paths. Both audits are read-only; neither requests repair.

Process interruption is the recorder's dry run. A hardware result additionally
needs an operator record of the actual power feed, cut/recovery times, affected
services and drives, boot/filesystem evidence and the retained ACK set. This
client alone cannot classify a disconnect as a physical power failure. Its
first strict-upload window does not by itself qualify power loss during database
checkpoint, repair, GC or every publication instruction; keep those observations
separate in the [hardware checklist](../../../docs/RC2-FOUR-HDD-CHECKLIST-20261116.md).
