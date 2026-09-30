# Peer bootstrap for the whole LIST index

Status: **on by default wherever an index exists**. Every build includes peer
bootstrap, and the Helm chart wires it whenever `upstream.buckets` is set: a
joining pod copies a ready peer's verified index instead of listing the origin.
A process started without the `S3CACHE_FLEET_*` peer book (a bare local run, or
a deployment with no configured buckets) keeps the guarded origin recovery path.
The existing Groupnet volatile recovery driver remains the only recovery
scheduler. Peer bootstrap uses Groupnet TTL entries and bounded bulk streams;
neither path writes control or metadata objects into the origin bucket.

## First measurable cost slice

The first production target is one explicitly configured bucket universe and
two connected nodes starting cold against the same origin. Each
node starts the existing lease and feed tasks. Groupnet's one recovery worker
selects one provisional origin builder; it performs the **only** initial
guarded full LIST scan in the healthy schedule. The other node remains
origin-routed, receives a bounded complete image and donor mutation suffix,
attaches its native feed, and opens local index serving only after an atomic
generation-fenced install and its independent frontier/lease affirmation.
An established healthy donor keeps its finite capture and claim service alive
for a later joining follower. An unavailable, expired, incompatible, or
over-budget donor causes the follower to perform its own guarded origin scan
within the original episode budget. A partition may create one builder per
component; duplicate scans are safe. No transferred image licenses serving
by itself, and the origin remains available throughout bootstrap.

Two paired workloads run fleet **off and on** with the same seeded objects,
configured buckets, node count, request schedule, and S3 origin: a cold
all-node start, and a warm-body rolling join with an established Ready donor.
In the healthy peer schedule, pause the follower's own origin LIST response;
it must still reach local serving from the donor. Measure actual
origin LIST, GET, HEAD, and PUT requests separately (including LIST discovery
and read validation), elapsed time from join to a Ready local index, and
positive GET latency during startup. The current tests log latency samples
and bound responsiveness; a single run does not establish a statistical
latency nonregression. Healthy connected fleet mode must reach local serving
on both nodes, use fewer origin LIST requests than the off baseline, and
introduce no origin coordination PUTs. Peer index
rows remain skeletal, so unchanged HEAD or body-fetch traffic is reported as
measured rather than assumed away. A warm-body workload separately prices the
shorter origin-routed window; it must show any claimed GET reduction with
actual origin GET counters. Partition and donor-failure cases must reach
either a verified peer baseline or bounded origin fallback, never merely
remain safely closed. No fleet-wide savings claim precedes these tests.

The bootstrap worker waits up to three seconds in its existing claim settle
phase before the first donor observation, within the original finite recovery
episode. This setting is provisional until the paired workload measures its
LIST savings and index-startup delay. Requests continue through origin during
discovery, so prompt origin request responses and time to local index serving
are separate measurements.
The selected donor wait uses at most half of the capped 60-second bootstrap
budget (30 seconds with the ordinary recovery configuration); an unreachable
donor can delay local index readiness until bounded origin fallback while
origin GET requests remain available. The outage test uses a 20-second total
budget and a 10-second donor wait to verify that fallback still completes.

A join before native membership, presence, and the donor's renewed Ready
claim converge may still choose an independent origin scan; tests retain that
immediate-join case and report both LIST cost and first-local-serving time.

The current admission policy accepts at most 64 native members, 256 configured
buckets, and 100,000 index rows. A capture is capped at 16 MiB encoded and
64 MiB decoded, with separate finite journal and in-flight limits. A larger
roster, bucket universe, index, or image declines peer bootstrap and uses the
guarded origin path within the recovery budget. These caps are operational
bounds, not evidence that every admitted workload will transfer successfully.
The MinIO test counters record origin **request attempts**; provider billing
for failed or cancelled requests may differ.

The initial compatibility policy is strict: all currently eligible members
in the scope must have live, matching bootstrap participation identities in
the bounded same-cut observation. A legacy or missing participant declines
peer bootstrap and retains origin recovery. Transient builder claims alone
must not be used to invent the identity of a healthy non-builder; Groupnet
must provide a renewable scoped participation identity and carry that exact
roster in the donor offer/barrier before this cost slice can claim later-join
liveness. Its implementation may add an opt-in bulk body kind, but must leave
old wire bodies and the default recovery path unchanged. S3 only supplies its
process-fresh boot identity, complete bucket scope, origin image/index effects,
and native feed application; Groupnet owns claim/renewal/takeover and transfer
timeouts in the existing worker.

## Production data plane

Peer bootstrap separately binds Groupnet's `TcpBulkTransport`; UDP gossip remains
the ordinary control plane. `S3CACHE_FLEET_BIND` listens on a TCP
port, `S3CACHE_FLEET_ADVERTISE` names the address peers dial, and
`S3CACHE_FLEET_PEERS` supplies a complete bounded `NodeId=host:port` book.
`S3CACHE_FLEET_ORIGIN_ID` (Helm `fleet.originId`) is a nonsecret,
operator-supplied stable account and
endpoint identity shared by exactly the nodes allowed to exchange an index;
it is required with the other three settings. The collision-free Groupnet
scope also includes the configured upstream endpoint, SDK region, index schema,
and exact sorted bucket inventory; reusing the origin ID against another
endpoint cannot accept that peer's image. No credentials enter claim metadata.
When enabled, Helm renders every StatefulSet ordinal against the headless
service on `fleet.port`, not just the two gossip seeds. DNS entries are re-resolved on peer
connect and registered under their configured **exact** node IDs. Before a
peer-ready claim, the book's self entry must exactly match the configured
advertised endpoint; an incomplete or duplicate-ID book disables peer bootstrap. Address
discovery affects reachability only: the bootstrap claim, transport peer,
scope, follower incarnation, and transfer operation must still agree. An
unresolved or mismatched address declines peer transfer and leaves guarded
origin recovery available.

The TCP listener and inbox belong to the existing recovery worker. It binds
before the node can advertise a transferable `Ready` capture, serves only the
current exact claim/capture identity, and remains alive for finite donor
service after local reads become Ready. On normal cancellation the worker
withdraws its exact claim and fences reservations; if it exits abruptly,
native TTL bounds stale advertisement while listener admission closes. A
failed accept loop immediately retires its current donor capture and wakes
the worker to withdraw the exact Ready claim; it does not revoke a healthy
local read gate. No TCP socket, listener task,
peer registry, or extra S3 metadata write exists without a peer book.

## Scope and image

One `WriteSync` owns one recovery handle and one `KeyIndex`. Its claim covers
the **whole** index, not one bucket. The scope domain binds an explicit origin
account/endpoint namespace and the index schema. The partition is the exact
length-prefixed, sorted configured bucket inventory. Capture requires
`KeyIndex` to contain exactly that inventory; an extra indexed bucket refuses
or invalidates peer bootstrap. No truncated name or short hash substitutes
for the set. A new bucket, changed namespace, schema, or membership set during
capture invalidates it. An oversized universe routes recovery to the guarded
origin scan.

The first fleet deployment requires an explicit nonempty, bounded bucket
inventory in `S3CACHE_BUCKETS` on every node. s3cache does not call origin
`ListBuckets` during bootstrap. An empty configuration declines peer
bootstrap and uses origin recovery; although the codec can encode an empty
known universe, it never authorizes absence or LIST answers for an unlisted
origin bucket. A bucket discovered during capture changes the
scope, withdraws that candidate, and starts a new bounded episode or origin
fallback. Tests exercise both an existing unknown origin bucket at cold start
and a new bucket discovered mid-transfer.

The bounded image contains one record per bucket known complete, sorted key
rows, and delete tombstones. The empty bucket universe is a valid complete
image. Donor generation numbers are deliberately omitted; the guarded local
install assigns the current recovery generation to fence older async
LIST/HEAD callbacks. A row carries exact key, size when
known, origin `Last-Modified`, `ETag`, and storage class. Peer rows deliberately
become **skeletal**: `Content-Type` and user HEAD metadata are not fabricated;
HEAD falls through to origin. The donor cannot offer an image while any
unresolved per-key uncertainty exists. A follower also rejects an image if
its own unresolved uncertainty would be hidden by the swap; it keeps serving
such reads from origin until exact HEAD reconciliation or origin rebuild.
Absence and LIST can be served locally only after a complete image, continuous
native handoff, independent frontier check, and the existing lease/domain
gate. Direct external S3 mutations still lack a fleet event and remain outside
this volatile proof.

The codec is versioned, canonical, length-delimited, and bounded before each
allocation. It rejects duplicate/unsorted buckets or keys, invalid times and
sizes, inconsistent tombstones, incomplete buckets, trailing bytes, and an
unknown schema. Encoded bytes, decoded rows, native overlap, donor suffix,
and each physical reply clone use distinct shared `ByteAdmission` permits.
The decoder precharges conservative hash-table and tree-node capacity before
allocation. This bounds private image ownership, not allocator RSS exactly.
The exact `DonorJournal::storage_bound` must fit the configured Suffix and
global caps before cloning the image. Tests price the configured bound and
measured live usage; a cap refusal falls back to origin without publishing a
partial index.

## Atomic donor capture and live effects

`KeyIndex`'s write lock is the short publication coordinator. It takes the
lock after a current `PublicationPermit` when a recovery page or install is
involved; ordinary feed and read repairs take the index lock directly and
never consult recovery control while holding it. No await occurs under it.
The donor completes every bucket's origin scan and returns the usable local
baseline before the original builder deadline. Once recovery reaches Ready,
the same worker reserves image/journal capacity and attempts one finite,
generation-guarded recapture of that current index without another LIST.
Under the publication lock it samples C, starts `DonorJournal`, and attaches
`JournalIngress` to the index; large encoding then runs off-lock. Only then
may its `Ready` claim advertise an available donor image. The capture is
withdrawn when its finite lifetime, source continuity, bucket universe, or
budget changes. Donor-service expiry does not itself close a healthy local
read gate.

Every transferable *accepted final key effect* enters the journal under that
same lock: proxied PUT/DELETE/COPY/multipart results, peer feed mutations,
local origin GET/HEAD repairs, and per-key uncertainty resolutions. A
whole-bucket LIST/rebuild invalidates the candidate; a new complete capture
is required before donor service resumes.
Forgetting an expired delete tombstone is not a key effect and is never
journaled: while a capture is attached the index forgets none, so the image
plus suffix stays exactly the donor's rows, and a follower replaying the
suffix forgets none either. The one-hour tombstone TTL is a floor, not a
deadline, and a capture's lifetime is bounded, so deferring costs only the
tombstones deleted meanwhile. Uncaptured, a bucket holding more than 65,536
tombstones sweeps them incrementally: each delete visits at most 16, from a
cursor that wraps over the key order, and none while the oldest held
tombstone is provably unexpired.
Rejected duplicate native events still append a bounded native `Noop` so
writer coverage advances contiguously. The record includes an exact native
writer incarnation/sequence when one exists; other effects get a fresh
capture-local identity. A rejected stale async callback does not change the
index and does not append. A real mutation in a new recovery generation
invalidates an old capture before publication. Overflow or a feed gap closes
donor availability synchronously under the index lock, then wakes the one
recovery worker for async claim withdrawal. Journal request handling takes
the journal lock alone; it never reaches back into the index lock.

## Follower handoff

The follower remains origin-routed while its existing lease granter and feed
applier run. Native delivery continues into its live `KeyIndex` throughout
transfer. A peer install requires its live native writer cuts to align with
B exactly (Groupnet's `align_cuts`). A cut behind or ahead of B in the same
writer incarnation answers `NativePending`: the follower keeps its private
stage and Groupnet samples a later barrier, replaying the further suffix,
until the cuts align or the original transfer deadline expires. A changed
writer incarnation or an incomparable version aborts the peer candidate and
keeps origin recovery available. The donor image and suffix through exact B
are staged privately; B's cuts and membership are sampled in the same journal
decision, so a delayed B response cannot borrow later cuts. Native events
already covered by those exact writer cuts are represented by the donor image
and suffix. Timestamp ties never silently choose arrival order.

Every node, donor or follower, registers its own feed as a native writer at
its current position when its apply loop starts. A proxied PUT or DELETE
assigns its feed position inside the same `KeyIndex` write lock that indexes
it, so the node's own writes reach the index, an open donor journal, and the
feed in one contiguous order. A donor that keeps writing during a join
therefore covers its own writes in B like any other writer's, and a follower
that already applied those feed events aligns with it.

Coverage also compares every local key and tombstone against the staged donor
image, up to 100,000 local rows. A local origin-validated GET/HEAD repair that
predates B is accepted only when the donor row has the same ETag,
Last-Modified, size, and storage class. A donor delete, missing row, or
incomparable version declines transfer. The guarded install repeats the whole
check under the write lock that performs the swap, so an effect arriving
after coverage can only refuse the swap, never be overwritten: a native effect
answers `NativePending` and keeps the stage, a local repair declines. This
read-lock walk is priced under concurrent hot reads and mutations; if it
repeatedly causes fallback or latency regression, the next slice must use
bounded scoped overlap or reconciliation rather than weaken the check.

Under one current recovery publication permit and the `KeyIndex` write lock,
the final callback validates schema, exact scope/universe, source membership,
staged B/cuts, native attachment, overlap charge, and the current recovery
generation. It swaps the private index and switches the normal feed applier
to that index in the same critical section. An event arriving before the
swap is in the overlap; one arriving after it applies to the replacement.
The typed handoff receipt binds that publication to the recovery operation.
The ordinary frontier and lease affirmation then decide when local serving
may open. A gap, cancellation, timeout, or old callback drops the candidate
and leaves the origin path usable. Body fills use their existing fence and
recheck recovery generation outside the index lock before a local response.

## Verification and rollout

Default mode must retain zero Groupnet bootstrap writes and all existing
origin fallback behavior. The current MinIO cases price connected cold and
warm rolling joins, an immediate unconverged join, a third join, live writes,
and an unreachable donor using native gossip and loopback TCP. They count
actual origin request attempts and require bounded positive progress or
fallback. The index unit tests cover exact tombstone/repair conflict and
guarded publication refusal; Groupnet runtime tests cover capture retirement
and its claim withdrawal. Partition, builder-death takeover, delayed chunk,
and restart fault schedules remain to be verified before broad fleet claims.
No single-builder or latency claim is made until the relevant integration
tests pass.
