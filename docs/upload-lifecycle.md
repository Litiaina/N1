# Upload ownership and reclamation

This protocol adds small operational records; it does not change NCNX or NCDX
encoding, static pool placement, or object/version identity.

## Write and commit

1. Register the immutable upload identity on reachable exact pool holders before
   sending shards. At least K registrations must succeed.
2. Each holder accepts normal shard writes only while that version is Open.
   Concurrent parts share a version read guard through physical publication.
   Successful publication checkpoints the holder's last activity.
3. Local NCDX publication takes the exclusive version guard, makes immutable
   NCDX durable, then records Committed. An existing NCDX also prevents an abort
   after a crash between these two steps. No NCNX is required for protection.
4. A successful object commit still requires K metadata acknowledgements.

Metadata/shard repair is a separate path: exact committed metadata and checksum
authority are required. Repair cannot silently turn an aborted version Open.
Replacement/legacy metadata bootstrap requires K matching, binding-checked
NCDX observations, not a client's assertion.

## Abort and cleanup

An abort voter takes the exclusive version guard, drains previously admitted
shard writers, refuses committed NCDX, and durably records Aborted. GC additionally
checks its idle cutoff under that guard and defers an actively writing version.
Explicit cancellation does not use an idle cutoff.

Deletion requires M+1 distinct current holders' abort votes. Each cleanup
receiver independently rechecks these authenticated votes before taking its
local cleanup guard. For N = K+M, K commit holders and M+1 abort voters must
intersect; a holder cannot ordinarily vote both ways.

Cleanup removes only the identity's exact version payload directory on the
recorded pool holders. It does not traverse other objects or infer ownership
from age, missing NCNX, or directory names. A failure is deferred, not reported
as a completed reclamation or quota refund.

## Persistent records

- Active work: `.uploads/<version>/ownership.n2`.
- Terminal fence: `.uploads/.fences/<hash-byte>/<hash-byte>/<version>.n2`.

Records contain a versioned, checksummed upload identity and state. Committed
records are compacted out of the active inventory. Aborted work remains active
until exact cleanup succeeds; its terminal fence remains afterward. Session
cleanup must preserve these fences. GC inventories active ownership records,
not `.objects` or terminal-fence history. Page candidate memory is bounded;
directory enumeration is not an O(page-size) operation.
Each page currently re-enumerates the active work directory; cleanup throughput
at very large numbers of simultaneous abandoned uploads is not qualified.

## Boundaries

- Upgrade all holders and stop old writers before relying on these guarantees.
  An old peer that ignores lifecycle state is not a compatible rolling writer.
- Older ambiguous payloads without ownership records are retained. This protocol
  cannot retroactively prove that they were abandoned.
- Pre-upgrade unfinished uploads are not automatically adopted or blindly
  resumed under new ownership. Preserve their manifests/receipts and payload;
  an explicit validated adoption procedure remains separate work.
- Minority partial-commit states and conflicting terminal evidence are retained
  conservatively; no age-based terminal-state override is permitted.
- Permanent abort fences cannot be TTL-deleted while old requests may arrive.
- Expired committed trash reclamation needs its own object-reference exclusion
  against concurrent recovery. Upload abort ownership alone does not supply it.

Qualification results and failures belong in [TESTING.md](../TESTING.md).
