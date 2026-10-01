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
LIST/HEAD callbacks. A row carries exact key, size when known,
`Last-Modified` as the donor holds it (see Follower handoff), `ETag`, and
storage class. Peer rows deliberately become **skeletal**: `Content-Type` and
user HEAD metadata are not fabricated; HEAD falls through to origin. The
donor cannot offer an image while any unresolved per-key uncertainty exists.
A follower carries its own unresolved uncertainty across the swap with its
reconciliation token (see Follower handoff); such reads keep going to the
origin until exact HEAD reconciliation or origin rebuild.
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
B exactly (Groupnet's `align_cuts`). Positions order epoch-major, so a cut
behind or ahead of B, in the same writer life or another one, answers
`NativePending`: the follower keeps its private stage and Groupnet samples a
later barrier, replaying the further suffix, until the cuts align or the
original transfer deadline expires. The refusal names the writer and both
positions at debug. Unsorted cut lists, or a pair of versions coverage cannot
order, abort the peer candidate and keep origin recovery available. The donor
image and suffix through exact B are staged privately; B's cuts and membership are
sampled in the same journal decision, so a delayed B response cannot borrow
later cuts. Native events already covered by those exact writer cuts are
represented by the donor image and suffix. Timestamp ties never silently
choose arrival order.

Every node, donor or follower, registers its own feed as a native writer at
its current position when its apply loop starts. A proxied PUT or DELETE
assigns its feed position inside the same `KeyIndex` write lock that indexes
it, so the node's own writes reach the index, an open donor journal, and the
feed in one contiguous order. A donor that keeps writing during a join
therefore covers its own writes in B like any other writer's, and a follower
that already applied those feed events aligns with it.

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

### Coverage of this node's own effects

Aligned cuts prove that the stage and the live index applied the same native
effects in the same writer order only while the live index has taken no feed
gap. A delivered gap moves the writer's cut past effects this node never
applied (`skip_native_gap`), so the cuts align without the effects being the
same: the missed span may have deleted a key whose tombstone the donor's own
origin rebuild then forgot. The gap therefore also discards every bucket to
origin-serving state under the same lock (`KeyIndex::discard_for_gap`): no
rows or tombstones, a new sync generation, and only the per-key uncertainty
fences kept, as an origin resync keeps them. Every row the follower then holds
at install time was either applied from the feed after the gap, and lines up
with the donor's cut by position, or came from this node alone and is ordered
by no writer cut: rows from
origin-validated GET/HEAD observations, the row or tombstone an exact-token
HEAD writes when it reconciles an uncertain write, a delete of a key the
donor never held, and every still-open reconciliation. A follower that
proxies client traffic while it bootstraps holds such effects from its first
origin HEAD. Coverage walks every live row and tombstone, up to 100,000,
against the staged donor state of the same key.

Every origin time enters the index rounded down to whole seconds, through one
helper (`index::origin_time`): a LIST row's milliseconds, a GET or HEAD's
HTTP-date, and the reconciling HEAD's `Last-Modified` alike. Whole seconds
are the one unit every origin path can express, so a version one node saw in
LIST and another in a HEAD holds the same time on both, and an index-served
LIST reports the origin's mtime in whole seconds. Rows written through the
fleet are different. An own PUT, COPY, or multipart write, and a peer's feed
event, carry the writer's clock, stamped at the feed's microseconds after the
origin answered, because a PutObject response carries no `Last-Modified`.
That stamp becomes the row's `Last-Modified` on every node that applies the
write; it is never rounded, and LIST reports it at the XML's millisecond
precision. Write stamps and tombstones keep their microseconds because they
order same-second writes and deletes: rounded, two writes in one second would
tie, and a recreate in the same second as its delete would lose to its own
tombstone. Rows decoded from a donor image or suffix are taken exactly as the
donor holds them, its origin rows already rounded at its own entry.

So one version can reach the two sides under different clocks. A key written
through the donor after its scan holds the writer's stamp in the donor's row,
while the follower, which never applied that write's feed event, learns it
from its own origin HEAD with the origin's mtime. The two times differ by the
write's latency plus the offset between the writer's and the origin's clocks,
and even in whole seconds they disagree whenever that interval crosses a
second boundary. Time therefore cannot decide whether two rows are one
version. The `ETag`, size, and storage class are the origin's version
identity and compare exactly: a live row with the candidate's identity is the
same version whatever clock stamped either time, and the candidate's row is
installed. That also covers a byte-identical rewrite the donor has not seen;
readers get the same bytes, size, and class, and only `Last-Modified` reports
the donor's earlier write, as the donor itself does. A row without an `ETag`
has no identity beyond its exact time.

Time only orders two different versions of a key, or a row and a delete, and
only where it can. An origin time never runs ahead of its version's true time
and runs behind it by less than a second, so two times are ordered wherever
their whole seconds differ, and a row whose exact time is after a tombstone's
was written after that delete. Each live effect is then exactly one of:

- **Covered**: the candidate holds the same version, a row or delete of the
  key in a later whole second, or, for a live tombstone, a tombstone at least
  as late or a row written after it. The swap installs the candidate's state.
- **Carried**: the live effect is provably later than everything the
  candidate holds for the key. The install applies it to the candidate before
  the swap by the index's own per-key rules, as if the same origin answer
  arrived just after it: a carried row replaces the older row and keeps the
  tombstone history; a carried delete raises the tombstone and removes the
  older row.
- **Refused**: a live row and the candidate's are different versions within
  one whole second (`RowMismatch`, naming the first differing field, identity
  before time), or a row and a delete of the key share a second without the
  row being exactly after the delete (`TombstoneMismatch`). Neither side is
  provably later, and timestamp ties never silently choose arrival order.

Open reconciliations cross the swap with their exact tokens and the bucket's
token epoch. The fenced key stays origin-read, and its bucket's LIST stays
origin-served, until that key's own HEAD resolves it in the installed index or
a later definitive mutation supersedes it, as on a node that never
bootstrapped. No later fence can reuse a carried token.

**No live effect is lost.** A native effect the barrier does not cover
misaligns the cuts and answers `NativePending`; the stage waits for a later
barrier. A local effect is covered by an equal-or-later candidate state,
carried, or refuses, and an uncertain write keeps its fence. So neither a
write the donor has not covered nor the origin's evidence of one disappears in
the swap: it is pending, carried, or refused.

**No stale read results.** For every key, the installed state is the same
version the live index held, or at least as recent in the index's own order
as both the live index's and the candidate's: covered keys install a
candidate state no older than the live one, carried keys install the live
state later than the candidate's, and pairs that cannot be ordered refuse. A
key whose last mutation outcome is unknown keeps its fence and is never
served locally before its origin HEAD. The ordinary frontier and lease
affirmation still decide when local serving opens. The carried result is an
index a node that never bootstrapped could reach by receiving the same
effects in another legal order, so it rests on the same origin-versus-write
clock assumption as every other last-writer-wins decision in the index.

**A safe install stays safe.** The guarded install repeats the whole
classification under the write lock that performs the swap and carries
exactly what the live index holds at that instant. An effect arriving after
the coverage check is covered, carried, or refuses there; a native effect
answers `NativePending` and keeps the stage. Nothing lands between the check
and the swap.

A live bucket outside the universe or under an origin rebuild, an incomplete
or uncertain candidate bucket, and more live rows than the walk bound also
refuse. Every refusal logs at info with its clause, bucket, and key, such as
`RowMismatch(LastModified)` and the observed row's key. Before this rule, a
follower that answered origin HEADs while it joined declined the image: an
observed row's whole-second time differed from the donor's millisecond LIST
row of the same version, or from the writer's stamp on a donor row written
through the fleet, and an open reconciliation declined as live uncertainty.
The walk runs under the index read lock at coverage and the write lock at
install; if it causes fallback or latency regression under hot traffic, the
next slice must bound it rather than weaken the classification.

## Rejoin after a restart

A restarted node is a new feed life of the same writer, and its peers cannot
know from the feed alone what the old life wrote at the origin after its last
published write. What each kind of stop costs follows from that.

**Announcement.** A starting node advertises its new feed epoch before its
recovery opens (`WriteSync::announce`), so every peer settles its restart at
once rather than at the new life's first write.

**Planned stop: seal.** On `SIGTERM` the binary retracts its serve-lease,
drains HTTP connections for up to 10 s, and only if the drain completed seals
its write feed (`CachingProxy::seal_writes`, then `WriteSync::seal`). The seal
first waits for every mutation tail, the spawned origin-and-publish task each
PUT, DELETE, DeleteObjects, copy, and multipart completion runs
(`CachingProxy::mutation_tail`) so a client hang-up cannot strand an applied
write, and then promises the peers that this life publishes nothing more. It
waits up to 5 s for every peer a write waits on to acknowledge the seal. A
drain that timed out, a mutation tail still running, or a crash leaves the
feed unsealed: the restart stays an ordinary gap,
which is always safe. The Helm chart's
`availability.terminationGracePeriodSeconds` (default 30) must cover the
drain plus the seal wait.

**Survivor.** A survivor that delivered the seal crosses into the rejoiner's
new life with `PeerWrite::Renewed` instead of a gap (`feed_renewals`): it
keeps its index and bodies, its frontier and ledger move to the new epoch at
sequence zero, and an open donor capture journals the crossing, so its
capture stays a candidate. The index advances the writer's cut to the seal
and then to the new life (`note_native_seal`, `renew_native_writer`); a
renewal that does not continue the cut from its delivered seal withdraws the
capture as a gap would. In `strong` the stopped node's frozen grants lapse
the survivor's lease briefly, and the lease-lapse recovery runs while the
stopped node is reaped, relearned with no state by the seed resolver once its
replacement's address appears, and only later gossips its new life. The
recovery reports the seal the survivor delivered as `Peer::sealed` and each
sealed crossing as `Peer::renewal`, so Groupnet's barrier lets the stopped
node's head disappear and the node leave the roster, and follows it into its
new life, instead of falling back: the survivor re-affirms without an origin
scan. It answers from the origin only for that moment. A rolling update
therefore stops the second pod as soon as the first has installed its image
(see [Rollout readiness](#rollout-readiness)) without either restart costing
a scan.

**Rejoiner.** The survivor's donor journal renews the rejoiner's old-life cut
to the new epoch, so a barrier sampled after the crossing aligns with the
rejoiner's own writer registered at its new life's start, and the rejoiner
installs the survivor's image without an origin LIST. A barrier sampled
before the crossing pends rather than conflicts.

**Donor timing.** The survivor offers the rejoiner a fresh Ready capture on its
first maintenance turn after the join's lease lapse, about 2.5 s after the
rejoiner's gossip, and the transfer starts about 1 s later. Groupnet's
recapture backoff counts only the failures taken under the participants a
failed attempt named, so the retirements the restart itself caused (the leave
lapse, the reap, the join lapse) cannot delay the capture for the rejoiner's
new life. Before groupnet 8f8f1e6 they did: an origin-built survivor offered
its image 7.7 s after the gossip. The rejoiner is a donor too:
once its recovery is Ready on the installed image, groupnet adopts that image,
advertises it under a Building claim at once, and captures it on the next
verified cut. The pod a rolling update stops next therefore leaves behind a
node whose image is already Ready or advertised, whichever node first built
the index, and every later update's donor is such an adopted image.

A transfer operation that fails without a verdict on the image does not cost
a scan either. Each bulk request is one TCP connection, about 1,700 for a
108 MB image, and on 2026-10-01 a Windows test host refused one of them
because it reused a local port still in `TIME_WAIT` toward the same donor
(Tcpip event 4227). Before groupnet f945950 the follower then excluded that
donor attempt, waited out the 30 s donor wait while the healthy donor kept
renewing it, and scanned the origin: 791 LISTs, index-ready 277 s after the
stop. Now it samples again one observation interval later and transfers from
the same live Ready attempt; refusals and verification failures still exclude
it. `tests/it/fleet_production/rolling.rs` prices one and two updates at
production pace, with the second stop at `minReadySeconds`, while the first
rejoiner's recapture is pending, and while it runs: each restart lists nothing.

**Crash.** The dead life's tail is unknown, so the survivor takes the gap,
serves from the origin, and rebuilds. Neither node knows more than the other,
so the pair makes exactly one origin scan: whichever node builds, the other
follows it and installs its image.

## Rollout readiness

A StatefulSet rollout stops the next pod once the replacement is ready for
`minReadySeconds`. If readiness meant only "the S3 listener is bound", the
controller could stop the last index-holding pod while its replacement had no
index yet, and the pair would rebuild from the origin: on 2026-10-01 the old
`s3cache-0` was stopped 8 s after the new `s3cache-1`'s gossip bound, while
`s3cache-1` was following a pending recapture, and `s3cache-1` then scanned the
origin for 8 minutes.

`GET /ready` (`CachingProxy::probe_ready`) therefore answers: this node is
index-ready (`GET /index-ready`), or no peer that may hold an index is live.
Each node declares `s3cache:indexed` in its gossip capability set once its
initial index and coherence warm-up complete; a peer counts while this node
sees it `Alive` or `Suspect` and it declares that capability or nothing yet,
and a configured seed counts until it first appears in the roster (or its
address fails to resolve), because a starting pod's roster is empty until then.

- A cold fleet has no index anywhere: every pod is ready and forwards to the
  origin while one builds.
- A replacement beside an index holder stays not ready until it installs the
  holder's image or builds its own index, so the rollout waits for it.
- `Suspect` counts because the kubelet marks a pod ready on one success: a
  momentary suspicion of a slow index holder must not release the rollout. A
  peer that is alive but has no index does not count.
- A `Dead` or reaped peer never counts, whatever it last declared. A crash or a
  single remaining pod is ready once the membership detection window passes,
  and a replacement whose donor died is ready from then on and scans: no
  deadlock.
- During an origin outage a replacement that must scan stays not ready, so the
  rollout stalls and the index holder keeps serving.

Gossip capabilities are the mechanism because they already carry each
node's participation and are rewritten whole by every new life, so a restarted
node's previous `s3cache:indexed` is buried at its first declaration. A dead
peer's declaration can outlive its death in a roster, which is why the
membership status, not the declaration, decides whether it counts. The
bootstrap claim is not used: a Ready capture is retired on any membership
change, exactly when a rollout needs the answer. A serve-lease is not used
either: it lapses on a gap or a peer's stop while the index is still held.
`tests/it/readiness_probe.rs` covers a cold pair, a
replacement beside an index holder, a node left alone, and a live peer without
an index on the in-memory transport.

## Verification and rollout

Default mode must retain zero Groupnet bootstrap writes and all existing
origin fallback behavior. The current MinIO cases price connected cold and
warm rolling joins, an immediate unconverged join, a third join, live writes,
a follower serving client HEAD, GET, PUT, and DELETE traffic while it joins
(its HEADs include objects the donor listed and objects written through the
donor after its scan), one whose uncertain PUT's reconciliation is still open
at its install, and an unreachable donor using native gossip and loopback
TCP. They count actual origin request attempts and require bounded positive
progress or fallback.
`tests/it/fleet_production.rs` runs the production scenarios at production size
and pace: two nodes gossiping on the in-memory transport, each on a
one-worker runtime with the binary's recovery, claim and lease
configuration, over a synthetic listing of 790,000 rows at 250 ms a page,
merged with every object written through the proxy. A cold start makes one
scan and the follower lists nothing; a planned rejoin lists nothing and the
survivor takes no gap and no scan; a crash rejoin makes exactly one scan the
other node follows; and a follower serving HEAD, GET, PUT and DELETE while it
bootstraps installs with no LIST.
The index unit tests cover each coverage clause, the covered and carried
cases, and guarded publication refusal; Groupnet runtime tests cover capture
retirement and its claim withdrawal. Partition, builder-death takeover,
delayed chunk, and restart fault schedules remain to be verified before broad
fleet claims.
No single-builder or latency claim is made until the relevant integration
tests pass.
