# NCNX key-scoped coordination

Status: implemented; qualification results are tracked in TESTING.md. It does
not declare a release, change NCNX/NCDX encoding, add tombstones, or replace
destination-first copy/delete semantics.

## The precise problem

Receiver-local key guards now prevent a repair from overwriting a valid
recreated pointer. They do not stop a missing pointer from being recreated
after removal. For example, a healer reads an old surviving source while a
rename has removed another replica; that delayed repair can restore the old
name after the remaining removals finish.

A receiver observation token followed by another ordinary read is insufficient:
the token can be issued after that receiver's removal but before another
receiver's removal. The second read then observes the still-present old source.

The missing condition is exclusive ownership of one replicated NCNX stage,
held across its observations and all physical changes.

## Minimal contract

1. Identify the exact `(cluster, pool, generation, physical fragment, key)`
   stage. Normalize the fragment and logical key identically at acquisition,
   read, publication, removal and release. Live/trash operations for that key
   share ownership; a trash locator is not an independent escape hatch.
2. Acquire exclusive ownership on **every exact NCNX holder** in that layout
   before any stage write. A health probe or pool majority does not substitute
   for ownership. No mutation starts after a partial acquisition.
3. Each receiver grants an unpredictable, newly generated token bound to its
   process incarnation, current physical-instance binding and exact key. The
   request owner identifies the attempt; it cannot choose the granted token.
4. All peer create/remove/repair requests, including individual batch items,
   require the appropriate token. Under the receiver's local key guard,
   validate ownership, target binding and expiry, then retain that guard until
   the admitted filesystem work and durability barriers have completed.
5. A healer acquires the complete layout first, then freshly reads the exact
   source generation under that ownership. Pre-acquisition `LocatedNcnx`
   bytes are planning hints, never sufficient publication authority. Complete
   absence means there is nothing to recreate; divergent valid bytes conflict.
6. If fresh lookup resolves a different pool, generation or holder layout,
   release ownership and replan. Never repair a new layout with old tokens.
7. Join every admitted peer operation before success and before explicit
   release. A lost response is an uncertain outcome, not permission to retry
   old writes without current ownership and fresh validation.

Rename may retain its existing two separate stages: all destination replicas
are durable, then all exact source replicas are removed. A healer between
these stages is harmless because the later owned removal deletes all source
replicas. A healer cannot run inside partial source removal, and after that
stage finishes its fresh read finds no source. Trash and recovery are opposing
operations on one key: they retain one owner across BOTH stages so they cannot
each delete the other's newly copied pointer. This does **not** make a compound
rename or full prefix atomic.

## Receiver lifetime and fencing

- Expiry uses the receiver's monotonic clock. Coordinator wall-clock time,
  clock synchronization and a claimed client deadline confer no authority.
- Expiry, release and competing grants use the same local key guard as writes.
  An admitted write must finish before a replacement owner can publish. An
  expired timer cannot let a second owner run alongside blocked disk work.
- Tokens change on every new grant. Reusing an attempt ID must not restore
  expired authority. A delayed acquisition can at worst temporarily acquire a
  fresh, bounded lease whose unknown token authorizes no old write.
- An explicit release is conditional on the exact token. A delayed release or
  renewal cannot affect a successor. Renewal after expiry is rejected.
- Restart discards all grants and changes process incarnation. Delayed old
  create/remove/repair/renew/release messages are rejected after restart.
- A binding transition invalidates old-instance authority. Its implementation
  must drain admitted writes or fence them before new-binding writes proceed;
  checking a stale captured `ClusterRuntime` is not sufficient evidence.
- Cancelled acquisition releases acquired grants best-effort; expiry bounds
  unreachable grants. Once publication starts, a detached bounded owner joins
  admitted work and retains ownership until completion or expiry. Transport
  cancellation must not release ownership around an active blocking write.

## Bounded work and availability

Use bounded, fail-fast acquisition and bounded retry/backoff. Never retain an
unbounded distributed waiter queue. Sort keys and targets consistently; on a
conflict, release partial grants rather than waiting in a cycle. Batch stage
ownership covers at most the existing 128 items; a rename can reference up to
256 distinct source/destination keys if stronger compound ownership is chosen.

Limit active attempts, keys per attempt, owner/token sizes, lease duration and
RPC concurrency. A receiver keeps only active grants, with a bounded expiry
index and per-entry cleanup; no namespace scan or ever-growing dead-key map.
Admission occurs before blocking I/O workers. Expiration maintenance must not
create a per-key unbounded task population.

The minimal stage contract requires all exact holders in one recorded pool.
An unavailable holder therefore defers namespace repair as well as mutations
requiring that layout; reads still use available verified replicas. Recovery
after reserve promotion uses the new binding and reacquires ownership. This
availability cost must be documented rather than hidden behind retries.

Stronger **logical-key exclusivity across pools** is a separate decision:
`find_live` searches historical and current pools, so owning only the newest
pool cannot serialize an old-pool mutation against new-pool publication.
Acquiring exact-key ownership across all configured pools is a straightforward
but more restrictive option: an unavailable historical holder can block a new
key operation, and metadata fanout grows with pool count. A shared logical-key
authority would require its own membership/availability proof. Neither is
silently implied by the minimal same-pool repair fix.

## Code integration points

- `distribution/ncnx/mutation.rs`: `apply_authorized` / `apply_at_least` and
  publication fallback must carry one owned stage across every receiver call;
  `publish_new_batch` needs per-item grants, not an unowned fallback path.
- `distribution/ncnx/batch.rs`: wrap each destination-create/source-remove
  stage in ownership. Preserve joined results and exact per-item identities.
- `distribution/ncnx/healing.rs`: acquire before the authoritative fresh
  read, target inspection and repair publication; do not reuse its first
  unlocked canonical lookup as the repair decision.
- `distribution/ncnx/{local,transport}.rs`: authenticated grant/renew/release
  endpoints and token-enforced single/batch dispatch. Reads used to establish
  repair authority must be checked against the same ownership.
- `distribution/namespace/{mutation,bulk,commit}.rs`: audit every fallback,
  recovery and upload pointer path. Planning results must be revalidated when
  required by the owning stage; no legacy peer mutation may bypass tokens.
- `core/garbage_collector.rs`: `remove_trash` participates. The broader
  recover-versus-physical-reclamation boundary is separate: merely owning the
  final pointer removal does not protect an earlier unguarded object purge.

New receivers require ownership tokens for mutations. New coordinators fail
closed when an old holder cannot issue a grant; there is no tokenless fallback.
This does not constrain an old binary's old write path. Stop old writers and
upgrade all holders before enabling this guarantee; mixed rolling writers are
not qualified.

## Required adversarial tests

- Pause removal after one holder changed. Healer cannot obtain ownership or
  restore the removed name; after removal it observes complete absence.
- Pause repair after fresh read. Concurrent removal cannot publish until the
  repair stage finishes or is fenced; after removal no source replica remains.
- Exercise rename, trash, recover, expired-trash removal, upload publication,
  grouped operations, partial group failures and every per-item retry path.
- Recreate the same key with both the same and a different object identity.
  No delayed old write may replace or delete the new generation.
- Delay and reorder acquire, renew, create, remove, repair and release across
  expiry, replacement grants, peer restart and binding-epoch changes.
- Block fsync past lease expiry. A new grant cannot admit overlapping writes;
  a stale token is rejected after the old write drains.
- Drop the coordinator at every stage, including during partial acquisition,
  after all grants and after a subset of acknowledgements. Verify bounded grant
  reclamation, all acknowledged data and explicitly uncertain outcomes.
- Test invalid fragment/key aliases, forged tokens, token reuse on another
  key/pool/instance, oversized grants and unauthenticated calls.
- Opposing multi-key batches must neither deadlock nor starve unrelated keys.
  Bound queue size, memory, task count, retries and blocking-worker occupancy.
- Preserve legitimate missing/corrupt NCNX repair and prove unavailable peers
  defer writes without creating alternate placement or false success.
- Separate the minimal same-pool proof from cross-pool and compound-operation
  semantics; test any stronger contract independently before claiming it.

Qualification needs real isolated node processes and controlled message
barriers. A local mutex test or a happy-path network test alone is insufficient.

## Executable proposal model

Run `python3 -B scripts/proof/ncnx_coordination_model.py`. This standard-library,
in-memory model currently passes eight tests. Its two-holder/two-attempt stage
exploration visits all **51 reachable states, 69 transitions and 34 finite
terminal schedules** under ordered, fail-fast acquisition. No repair restores a
pointer after acknowledged removal in that model. A negative control reusing
pre-acquisition source bytes explores 55 states/74 transitions/36 schedules and
produces a concrete stale-resurrection trace. A separate regression exhibits
the old target-check/fresh-source race.

Receiver-state regressions exercise expired/released tokens, process restart,
new grant nonces after delayed acquisition, writes admitted before expiry and
independent-key progress. They model completed namespace writes atomically and
assume genuine unique token identities, exact-key validation and no tokenless
writer. They do not test Rust, real peer transport, mixed versions, unbounded
retries/fairness, multiple pools, membership transitions, disk durability or
compound-operation atomicity. Passing this model is design evidence only;
the model alone does not qualify the runtime protocol. Runtime results are
recorded separately in TESTING.md, including failures and remaining scope.
