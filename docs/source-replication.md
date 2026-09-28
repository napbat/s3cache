# S3 control source: first storage slice

The S3 origin is authoritative for object contents but supplies no committed
event stream for proxy writes. A proxy can die after an origin mutation commits
and before its Groupnet feed event exists. This document defines a separate,
durable **opt-in** control source for closing that window. The default remains
zero coordination or metadata writes to S3, and the origin bucket is never
used for control records. Without explicitly configured durable coordination
or an equivalent authoritative reconciliation source, existing origin fallback
remains and no complete peer index or event-complete subscription is claimed.
Event-complete delivery would additionally require retained committed history;
a snapshot cannot substitute for each event. The initial implementation in
`sync::control` provides its storage and codec only; it does not change write
paths, serving gates, startup, or current consistency claims.

## Identity and order

A deployment supplies a stable source ID and journal generation. The source
stores a manifest in a separate control bucket and refuses to open if its
identity, generation, format, or admission configuration differs. Generation
changes need an explicit migration protocol; restarting must reuse the same
identity. The first slice conservatively persists scan-page and append-probe
limits in that manifest too, so changing even local tuning requires an
explicit migration; a later revision may separate those limits. Control
objects never share a user bucket or key namespace.

Each application bucket has consecutive immutable slots `0, 1, ...`. An append
uses conditional create (`If-None-Match: *`) on the exact next slot. A loser
reads the winner and tries the following slot. If a conditional response is
lost, read back that *same* slot and compare the complete encoded record: an
exact match proves the append, a different record proves a collision, and an
absent/unreadable slot leaves an explicit unknown result. No writer skips a
slot. Reading stops at the first absent slot; it proves only a contiguous
committed prefix, never that no later record or origin mutation exists. The
native cursor is source ID, generation, bucket, and next slot.
An occupied record with the same logical identity but different bytes is
corruption, not a collision to probe past. Probe-budget exhaustion returns a
verified cursor after the occupied prefix for a later retry. An ambiguous PUT
retains the original retry cursor so the same candidate slot is checked again.

Records are versioned, length bounded, and checksummed. `Intent` names one key
and a stable operation ID. `Outcome` names that operation and key, binds its
immutable intent slot, and is either definite success, definite no-mutation
failure, or abort proven before
origin dispatch. An unknown origin result has no closing outcome and remains
pending. `ReaderAdmission` binds reader ID, unique incarnation, maximum serve
duration, and admission configuration version. Projection keeps *all* pending
operation IDs per key and rejects ID reuse, replay, and out-of-order or
cross-scope application; overlapping writes cannot clear each other's fence.
Closing an intent in this projection does not prove an existing LIST index
applied the result. Local-read authority still needs authoritative origin
reconciliation or later source-backed materialization, neither supplied here.
Concurrent origin mutations may commit in a different order from their
`Outcome` slots. A later integration must retain each fence until its exact
operation resolves, then use a quiescent, newer-intent-fenced origin observation
to establish index state; it cannot replay outcomes as origin commit order.
Malformed records, unsupported admission configuration, and wrong scope or
generation fail closed. First-slice scans and append collision probes have
explicit event, byte, record-size, slot-count, and retry limits. The maximum
retained bytes are bounded by `max_slots * max_record_bytes`; no slot is ever
retired in this slice. If capacity fills after an intent, its outcome cannot
append: the intent stays pending and affected local reads stay origin-routed.
The caller must stop creating new admissions/intents and report terminal
capacity exhaustion; it must never bypass an intent or silently authorize a
checkpoint. Retention/checkpoint compaction requires a later covered-cut
protocol because deleting a slot would invalidate the cursor.
An append from a nonzero cursor checks its immediate predecessor before any
conditional PUT; immutable no-retirement slots make a forged future cursor
unable to create a hole.

## Source-ordered admission requirement

Future strong-mode integration must start a reader's monotonic admission
deadline **before** appending its admission. The reader may serve only after
confirming that slot, replaying the journal through it, and checking both this
deadline and its existing Groupnet serve lease on every local read. Ordinary
lease renewal cannot extend source admission. A new admission/renewal is a new
slot and must replay preceding unresolved intents before it becomes usable.

An intent at slot `k` divides readers by source order. The writer must obtain
an invalidation acknowledgement, bound to that intent and each earlier active
admission incarnation, after the reader revokes local serving; or wait the
configured maximum admission duration from the writer's own confirmation of
`k`, adjusted for the documented clock-rate bound. Earlier readers started
their deadlines before their `<k` append, so they have expired by that wait.
Later `>k` admissions replay the pending intent before serving. This closes
the join between a lease wait's last sample and forwarding the origin request;
Groupnet's current lease wait alone does not close it. Unknown duration,
configuration, or clock behavior must fail closed. A restarted writer repeats
the full conservative wait rather than trusting a persisted wall timestamp.

Only after this fence may the writer dispatch to the origin. Client
cancellation cannot abandon a dispatched operation. A definite outcome closes
its intent after durable append; an ambiguous or crashed operation stays
pending and its affected local answers route to the origin. A HEAD or LIST
while an old request might still commit cannot close the intent. Joins and
total-fleet restart reconstruct pending operations from the control source
before any authoritative local read. Peer snapshots must carry a proven source
cut and pending set, capture all later slots, and replay through a source
barrier. A snapshot or matching gossip heads alone cannot establish freshness.

## Backend assumptions and cost

This adapter requires atomic conditional object creation, unambiguous exact-key
readback, durable objects, and sufficiently consistent GET visibility to read
a contiguous slot chain. [R2 documents conditional S3 headers](https://developers.cloudflare.com/r2/api/s3/api/)
and [strong global read-after-write consistency](https://developers.cloudflare.com/r2/reference/consistency/);
[AWS documents first-writer conditional wins, 412 conflicts, and possible 409s](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
The implementation must be tested on MinIO and the target R2 deployment; no
R2 integration has been validated here. External overwrite, deletion, version
delete markers, or lifecycle expiry of retained journal slots are prohibited:
any of them can break the no-skip prefix even with conditional PUTs.
A single chain serializes every append for one bucket. A successful mutation
will eventually cost at least an intent and outcome control write, plus a
pre-mutation invalidation round in the healthy strong path. Contention causes
conditional-create conflicts and readbacks; stale or unavailable control
storage must refuse authoritative local serving. Admission renewals add
control writes. Bound retries and measure p99 write latency, CAS collisions,
control GET/PUT traffic, and catch-up lag before choosing a production budget.
Sharding a hot bucket into lanes would require a vector cursor and a new
multi-key cut proof. This optional s3cache journal is not a second mandatory
log for consumers, such as shardstore, that already have a committed CAS log.
