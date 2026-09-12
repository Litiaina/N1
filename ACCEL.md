# N1 + Console + Accel without Space

Space is not required for file search. Console authenticates directly with N1;
N1 forwards namespace-search requests to a fixed, private Accel backend. Accel
builds a disposable search projection from N1's authoritative namespace.

```text
Console -> N1 authenticated namespace-search route -> private Accel
                                                        |
                                              N1 namespace snapshot
```

N1 remains responsible for authentication, storage, EC, namespace and lifecycle.
Accel is optional. Its failure can make accelerated search unavailable, but
ordinary direct N1 reads, writes and listing do not require it.

## Configuration

Use the Accel repository's `accel.toml.example` and deployment instructions.
For an N1-only installation, remove the unused N2 backend entry rather than
inventing N2 credentials. Select the actual N1 origin, trusted TLS and a backend
identity such as `n1-primary`. On the same host, Accel can bind to loopback:

```ini
# Environment of the N1 engine service, not browser configuration:
N1_ACCEL_SEARCH_URL=http://127.0.0.1:53001
N1_ACCEL_SEARCH_BACKEND_ID=n1-primary
```

The backend identity must match Accel's `[[backends]].id`. Accel's backend origin
points to N1 itself, not back to Accel or Space. Protect its environment: the
configured `jwt_secret_env` contains the same token-signing secret used by N1,
**not** a fragment key or administrator login password. Keep Accel's service and
admin keys private; none belong in Console JavaScript.

Use a trusted certificate and matching hostname for the origin. Do not copy a
test fixture's `allow_invalid_tls=true` into production. When Accel is on another
host, protect the transport/network rather than sending bearer tokens over an
untrusted plain-HTTP link. Bind/expose only the intended private service.

Set independent byte-cache and search capacities for the host. The example's
8 GiB byte-cache and 512 MiB search ceilings are examples, not requirements or
total-process-RSS limits. Search admission also has a maximum fragment entry
count; do not assume arbitrarily large fragments will fit a fixed cache budget.

## Browser and API behavior

Console uses N1's existing fragment-authenticated endpoint:

```text
GET /noa/v1/namespace/search?q=video.mp4&field=filename&match=exact
Authorization: Bearer <fragment-read-token>
```

It supports filename/path/extension search with the modes provided by Accel.
Follow the returned continuation token with the same query for paginated results.
Authorization stays fragment-scoped; a search is not a cross-account inventory.

The search projection is rebuildable and refresh-based. Accel's example uses a
30-second refresh interval; clients can request `refresh=true`. A recent upload
or rename need not appear immediately in an existing projection. N1's ordinary
listing/lookup remains authoritative. Cold search and restart may require a
namespace scan, so a warm HIT does not establish cold-start speed at large scale.

Console's normal downloads currently go directly to N1. Accel separately supports
immutable, version-pinned download/range caching, but those reads must explicitly
use its byte-cache endpoint through an appropriate client/proxy integration.
Merely enabling namespace search does not route all downloads through that cache.

## Acceptance checks

1. Authenticate with N1 and search for a known nested filename from the fragment
   root. Check `x-accel-search` for MISS/HIT on repeated requests.
2. Verify pagination, unauthorized rejection and isolation between fragments.
3. Upload/rename a synthetic file; check visibility after refresh and confirm the
   old name disappears after the projection refreshes.
4. In an isolated fixture, stop Accel: search must report unavailable while a
   direct N1 download still matches its hash. Restart and verify index rebuild.
5. Test Console separately: API success alone does not prove the browser UI.

The dated checks and limits are recorded in [VERIFICATION.md](VERIFICATION.md).
Back up N1's authoritative data with [BACKUP.md](BACKUP.md); Accel cache contents
are not a recovery point and do not need to be copied into a storage snapshot.
