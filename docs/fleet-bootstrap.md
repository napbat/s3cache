# Optional peer bootstrap for the whole LIST index

Status: **S3 integration contract; implementation in progress**. The existing
Groupnet volatile recovery driver remains the only recovery scheduler. Fleet
mode is explicit opt-in; the default makes no coordination or metadata writes
to S3. Fleet mode uses Groupnet TTL entries and bounded bulk streams. Neither
mode writes control objects into the origin bucket.

## Scope and image

One `WriteSync` owns one recovery handle and one `KeyIndex`. Its claim covers
the **whole** index, not one bucket. The scope domain binds an explicit origin
account/endpoint namespace and the index schema. The partition is the exact
length-prefixed, sorted union of configured buckets and bucket names already
present in `KeyIndex`. No truncated name or short hash substitutes for that
set. A new bucket, changed namespace, schema, or membership set during a
capture invalidates that capture. An oversized universe routes recovery to
the guarded origin scan.

The bounded image contains one record per bucket with its complete synced
flag, sorted key rows, delete tombstones, and the index generation needed to
fence older async LIST/HEAD callbacks. A row carries exact key, size when
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
The exact `DonorJournal::storage_bound` must fit the configured Suffix and
global caps before cloning the image. Tests price the configured bound and
measured live usage; a cap refusal falls back to origin without publishing a
partial index.

## Atomic donor capture and live effects

`KeyIndex`'s write lock is the short publication coordinator. It takes the
lock after a current `PublicationPermit` when a recovery page or install is
involved; ordinary feed and read repairs take the index lock directly and
never consult recovery control while holding it. No await occurs under it.
The donor first reserves all image/journal capacity, finishes every bucket's
origin scan, then under this lock captures the complete bounded image at C,
starts `DonorJournal`, and attaches `JournalIngress` to the index. Only then
may its `Ready` claim advertise an available donor image. The capture is
withdrawn when its finite lifetime, source continuity, bucket universe, or
budget changes. Donor-service expiry does not itself close a healthy local
read gate.

Every *accepted final index effect* enters the journal under that same lock:
proxied PUT/DELETE/COPY/multipart results, peer feed mutations, local origin
GET/HEAD repairs, per-key uncertainty resolutions, and origin LIST pages.
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
applier run. It attaches native delivery before accepting donor B. Feed events
and local authoritative repairs observed during transfer have bounded
generation-tagged overlap buffers. The donor image and suffix through exact
B are staged privately; B's cuts and membership are sampled in the same
journal decision. A delayed B response cannot borrow later cuts. Native
events covered by those exact writer cuts are skipped once. Uncovered native
events apply to the private stage in writer order after B. Incomparable
same-key native writers, or an unorderable local origin repair versus donor
effect, force a fresh guarded origin reconciliation or abort the peer
candidate. Timestamp ties never silently choose arrival order.

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
origin fallback behavior. Fleet tests use a real MinIO origin plus Groupnet
in-memory transport: connected cold cluster has one guarded origin builder
and a peer follower; builder death causes takeover; partition duplicates are
safe; a missing/expired donor falls back to origin. Fault schedules delay a
LIST page, native DELETE, origin repair, chunk, B, ack, and final install
across cancellation/restart. They verify no deleted row is resurrected and
no incomplete/uncertain index answers LIST or absence. Tests also count
actual origin LIST requests and admitted transfer bytes. No single-builder
or latency claim is made until those integration tests pass.
