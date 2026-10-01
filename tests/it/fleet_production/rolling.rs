//! (e) `StatefulSet` rolling updates. The higher ordinal stops first and its
//! replacement installs its peer's image; the other pod stops once that
//! replacement is ready and its replacement installs the first rejoiner's
//! image. After one update every pod holds an image it installed from a peer,
//! so every donor of a later update is such an adopted image. Every life seals
//! its feed, so no restart costs an origin scan, a LIST page or a recovery
//! fallback. The second stop's other landings in the rejoiner's recovery,
//! down to its millisecond peer proof stages, are groupnet's
//! `volatile_recovery_rolling` simulation.
//!
//! Each scenario prints its milestones on the wall clock of the decision log,
//! so a restart's steps, from the replacement's gossip to its donor's Ready
//! capture and the transfer, can be read off one timeline.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use groupnet::core::NodeId;

use super::common::fleet::serves_locally;
use super::{
    Fleet, Life, Node, ROWS, WRITTEN, cold_start, lists, planned_stop, wait_until, write_through,
    written,
};

/// A replacement pod's start, from its predecessor's stop to its gossip: on
/// 2026-10-01 each replacement's gossip bound 32–34 s after its predecessor's
/// `SIGTERM`, 25–31 s of it opening its warm tier, and its peer ran alone
/// meanwhile.
const POD_START: Duration = Duration::from_secs(30);

/// The `StatefulSet`'s `minReadySeconds`: it stops the next pod once the
/// replacement has reported ready for this long.
const MIN_READY: Duration = Duration::from_secs(5);

/// Print a scenario milestone on the wall clock the decision log stamps.
fn milestone(what: &str, pod: &str) {
    let at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis();
    println!("fleet_production milestone at_ms={at} pod={pod} {what}");
}

/// When the update stops the next pod, after the first one's replacement.
#[derive(Clone, Copy, Debug)]
enum NextStop {
    /// The `StatefulSet`: once the replacement's readiness probe has held for
    /// `minReadySeconds`.
    StatefulSet,
    /// The moment the replacement serves locally: its installed image's
    /// Ready recapture is still pending.
    RecapturePending,
    /// The moment the replacement starts its installed image's Ready
    /// recapture.
    RecaptureRunning,
}

/// A running pod, and what its life's recovery cost before the update.
struct Member {
    node: Node,
    before: Life,
}

/// One pod's restart: when its replacement served locally, counted from the
/// stop, and what the stopped life's recovery cost during the update.
#[derive(Debug)]
struct Restart {
    pod: &'static str,
    ready_ms: u128,
    stopped: Life,
}

/// The stopped pod's replacement, and when it served locally.
struct Replacement {
    node: Node,
    stopped: Life,
    ready: tokio::task::JoinHandle<Duration>,
}

/// Bring up the replacement of the stopped pod `index` the way a `StatefulSet`
/// does. The survivor reaps the old member; its seed resolver relearns the
/// name for the replacement's new address before the replacement gossips (on
/// 2026-10-01 at 4.9 s after the `SIGTERM`, once the old member was reaped);
/// and the new life starts [`POD_START`] after the stop.
async fn replace(fleet: &Fleet, index: usize, survivor: &Node, stopped: Instant) -> Node {
    let name = NodeId::new(fleet.names[index]);
    let group = survivor.sync.group();
    wait_until("the survivor reaps the stopped member", || {
        group.status_held_for(&name).is_none()
    })
    .await;
    group.add_peer(name);
    tokio::time::sleep(POD_START.saturating_sub(stopped.elapsed())).await;
    let node = fleet.node(index).await;
    milestone("gossip bound", fleet.names[index]);
    node.start(&fleet.bucket).await;
    node
}

/// Stop `member`, the pod `index`, by the binary's `SIGTERM` path, and bring
/// up its replacement beside `survivor`.
async fn restart(fleet: &Fleet, index: usize, member: Member, survivor: &Node) -> Replacement {
    let pod = fleet.names[index];
    let stopped = Instant::now();
    milestone("stop", pod);
    let life = planned_stop(member.node).await.since(member.before);
    let node = replace(fleet, index, survivor, stopped).await;
    let (proxy, bucket) = (node.proxy.clone(), fleet.bucket.clone());
    let ready = tokio::spawn(async move {
        while !serves_locally(&proxy, &bucket) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        milestone("index-ready", pod);
        stopped.elapsed()
    });
    Replacement {
        node,
        stopped: life,
        ready,
    }
}

/// Wait until the update stops the next pod: `next`, relative to the first
/// replacement.
async fn next_stop(replacement: &Node, bucket: &str, next: NextStop) {
    match next {
        NextStop::StatefulSet => {
            // `minReadySeconds` counts from the pod's last turn to ready.
            let mut ready_since = None;
            wait_until("the first replacement is ready for minReadySeconds", || {
                if !replacement.proxy.probe_ready() {
                    ready_since = None;
                    return false;
                }
                ready_since.get_or_insert_with(Instant::now).elapsed() >= MIN_READY
            })
            .await;
        }
        NextStop::RecapturePending => {
            wait_until("the first replacement serves locally", || {
                replacement.serves(bucket)
            })
            .await;
        }
        NextStop::RecaptureRunning => {
            wait_until("the first replacement starts its Ready recapture", || {
                replacement.count("recovery_ready_recaptures") > 0
            })
            .await;
        }
    }
}

/// One rolling update of `pair`: the higher ordinal stops and its replacement
/// joins the other pod, which stops at `next`; its replacement joins the first
/// one's. Returns the new pair and both restarts.
async fn roll(fleet: &Fleet, pair: [Member; 2], next: NextStop) -> ([Member; 2], [Restart; 2]) {
    let [a, b] = pair;
    let first = restart(fleet, 1, b, &a.node).await;
    next_stop(&first.node, &fleet.bucket, next).await;
    let second = restart(fleet, 0, a, &first.node).await;
    wait_until("both replacements serve locally", || {
        first.node.serves(&fleet.bucket) && second.node.serves(&fleet.bucket)
    })
    .await;
    let restarts = [
        Restart {
            pod: fleet.names[1],
            ready_ms: first.ready.await.expect("ready watch").as_millis(),
            stopped: first.stopped,
        },
        Restart {
            pod: fleet.names[0],
            ready_ms: second.ready.await.expect("ready watch").as_millis(),
            stopped: second.stopped,
        },
    ];
    let fresh = |node: Node| Member {
        before: node.life(),
        node,
    };
    ([fresh(second.node), fresh(first.node)], restarts)
}

/// Cold-start the pair, then roll it `updates` times. Before each update
/// both pods write through to each other, so every life has writes the
/// update must keep, and each update stops its second pod at `next`.
async fn rolling_update(label: &str, names: [&'static str; 2], updates: usize, next: NextStop) {
    let fleet = Fleet::new(label, names).await;
    let cold = cold_start(&fleet).await;
    let bucket = fleet.bucket.clone();
    let [a, b] = cold.nodes;
    let mut pair = [a, b].map(|node| Member {
        before: node.life(),
        node,
    });
    let mut restarts = Vec::new();
    let mut keys = 0;
    for update in 1..=updates {
        let [a, b] = &pair;
        write_through(&a.node, &b.node, &bucket, keys).await;
        write_through(&b.node, &a.node, &bucket, keys + WRITTEN).await;
        keys += 2 * WRITTEN;
        let before = fleet.counts();
        let (rolled, restarted) = roll(&fleet, pair, next).await;
        let cost = fleet.counts().since(before);
        for restart in &restarted {
            println!(
                "fleet_production {label} update={update} next={next:?} pod={} \
                 stop_to_ready_ms={} stopped_life={:?}",
                restart.pod, restart.ready_ms, restart.stopped
            );
        }
        println!("fleet_production {label} update={update} {cost:?}");
        assert_eq!(
            cost.list, 0,
            "rolling update {update} listed the origin: {cost:?}"
        );
        cost.assert_no_writes();
        restarts.extend(restarted);
        pair = rolled;
    }
    let lives: Vec<Life> = restarts
        .iter()
        .map(|restart| restart.stopped)
        .chain(pair.iter().map(|member| member.node.life()))
        .collect();
    println!("fleet_production {label} lives={lives:?}");
    for life in &lives {
        assert_eq!(life.scans, 0, "a restart scanned the origin: {lives:?}");
        assert_eq!(life.fallbacks, 0, "a recovery fell back: {lives:?}");
    }
    let before_check = fleet.counts();
    for member in &pair {
        for n in 0..keys {
            assert!(
                lists(&member.node.proxy, &bucket, &written(n)).await,
                "every life's write {n} survives the update"
            );
        }
        assert_eq!(member.node.count("index_objects"), (ROWS + keys) as u64);
    }
    assert_eq!(
        fleet.counts().since(before_check).list,
        0,
        "both nodes answer from their own index"
    );
}

/// The first update: one donor built the index, the other installed it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local MinIO and minutes of production-paced origin scans; run with --ignored --test-threads=1"]
async fn rolling_update_lists_nothing() {
    rolling_update(
        "fleet-prod-rolling",
        ["prod-roll-a", "prod-roll-b"],
        1,
        NextStop::StatefulSet,
    )
    .await;
}

/// A second update, whose donors both hold images installed from a peer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local MinIO and minutes of production-paced origin scans; run with --ignored --test-threads=1"]
async fn second_rolling_update_from_adopted_donors_lists_nothing() {
    rolling_update(
        "fleet-prod-rolling-adopted",
        ["prod-adopt-a", "prod-adopt-b"],
        2,
        NextStop::StatefulSet,
    )
    .await;
}

/// Each update's second stop lands as the first replacement serves locally,
/// while its installed image's Ready recapture is pending.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local MinIO and minutes of production-paced origin scans; run with --ignored --test-threads=1"]
async fn rolling_update_stopped_while_a_recapture_is_pending_lists_nothing() {
    rolling_update(
        "fleet-prod-rolling-pending",
        ["prod-pending-a", "prod-pending-b"],
        2,
        NextStop::RecapturePending,
    )
    .await;
}

/// Each update's second stop lands while the first replacement's Ready
/// recapture of its installed image runs.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local MinIO and minutes of production-paced origin scans; run with --ignored --test-threads=1"]
async fn rolling_update_stopped_while_a_recapture_runs_lists_nothing() {
    rolling_update(
        "fleet-prod-rolling-running",
        ["prod-running-a", "prod-running-b"],
        2,
        NextStop::RecaptureRunning,
    )
    .await;
}
