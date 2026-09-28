use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::journal::{Journal, JournalConfig, JournalError, Limits};
use super::{
    FinalOutcome, Mutation, PendingSet, PutVerdict, Record, SlotStore, SourceId, StoreError,
};

#[derive(Clone, Debug, Default)]
struct MemoryStore {
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    objects: BTreeMap<String, Vec<u8>>,
    lose_created_reply: bool,
    lose_before_create: bool,
}

#[async_trait]
impl SlotStore for MemoryStore {
    async fn put_if_absent(&self, key: &str, bytes: Vec<u8>) -> Result<PutVerdict, StoreError> {
        let mut state = self.state.lock().unwrap();
        if state.lose_before_create {
            state.lose_before_create = false;
            return Ok(PutVerdict::Unknown);
        }
        let occupied = state.objects.contains_key(key);
        if !occupied {
            state.objects.insert(key.to_owned(), bytes);
        }
        if state.lose_created_reply && !occupied {
            state.lose_created_reply = false;
            Ok(PutVerdict::Unknown)
        } else if occupied {
            Ok(PutVerdict::Occupied)
        } else {
            Ok(PutVerdict::Created)
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.state.lock().unwrap().objects.get(key).cloned())
    }
}

fn config() -> JournalConfig {
    JournalConfig {
        source: SourceId {
            name: "deployment-a".into(),
            generation: 7,
        },
        admission_version: 1,
        max_admission_ms: 2_000,
        limits: Limits {
            max_record_bytes: 1024,
            max_slots: 64,
            max_scan_events: 4,
            max_scan_bytes: 4096,
            max_append_probes: 4,
        },
    }
}

fn intent(id: &str, key: &str) -> Record {
    Record::Intent {
        op_id: id.into(),
        key: key.into(),
        mutation: Mutation::Put,
    }
}

fn outcome(id: &str, key: &str, intent_slot: u64, result: FinalOutcome) -> Record {
    Record::Outcome {
        op_id: id.into(),
        intent_slot,
        key: key.into(),
        result,
    }
}

fn at(slot: u64) -> super::Cursor {
    super::Cursor {
        source: config().source,
        bucket: "bucket".into(),
        next_slot: slot,
    }
}

#[tokio::test]
async fn lost_successful_reply_is_proved_by_exact_slot_readback() {
    let store = MemoryStore::default();
    let journal = Journal::open(store.clone(), config()).await.unwrap();
    store.state.lock().unwrap().lose_created_reply = true;
    let start = journal.start("bucket");
    let appended = journal
        .append(&start, &intent("op-1", "key"))
        .await
        .unwrap();
    assert!(matches!(appended, super::AppendResult::Recovered(_)));
    let page = journal.scan(&start).await.unwrap();
    assert_eq!(page.records, vec![intent("op-1", "key")]);
    assert!(page.at_first_absent);
    assert_eq!(page.cursor.next_slot, 1);
}

#[tokio::test]
async fn absent_ambiguous_slot_does_not_advance_or_skip() {
    let store = MemoryStore::default();
    let journal = Journal::open(store.clone(), config()).await.unwrap();
    store.state.lock().unwrap().lose_before_create = true;
    let start = journal.start("bucket");
    assert_eq!(
        journal.append(&start, &intent("op", "key")).await,
        Err(JournalError::UnknownWrite {
            slot: 0,
            retry_from: start.clone(),
        })
    );
    assert!(journal.scan(&start).await.unwrap().at_first_absent);
    assert!(matches!(
        journal.append(&start, &intent("op", "key")).await,
        Ok(super::AppendResult::Created(_))
    ));
}

#[tokio::test]
async fn concurrent_append_winner_and_exact_retry_share_one_slot() {
    let store = MemoryStore::default();
    let journal = Journal::open(store, config()).await.unwrap();
    let start = journal.start("bucket");
    let first = intent("a", "x");
    let second = intent("b", "y");
    let (a, b) = tokio::join!(
        journal.append(&start, &first),
        journal.append(&start, &second)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_ne!(a, b);
    let page = journal.scan(&start).await.unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.cursor.next_slot, 2);
    let retry = journal.append(&start, &page.records[0]).await.unwrap();
    assert!(matches!(retry, super::AppendResult::Recovered(_)));
    assert_eq!(journal.scan(&start).await.unwrap().records.len(), 2);
}

#[tokio::test]
async fn no_notification_tail_and_pending_reconstruct_from_slots() {
    let store = MemoryStore::default();
    let journal = Journal::open(store.clone(), config()).await.unwrap();
    let mut at = journal.start("bucket");
    for record in [
        intent("a", "x"),
        intent("b", "x"),
        intent("c", "y"),
        outcome("a", "x", 0, FinalOutcome::Committed),
        outcome("c", "y", 2, FinalOutcome::RejectedWithoutMutation),
    ] {
        at = match journal.append(&at, &record).await.unwrap() {
            super::AppendResult::Created(cursor) | super::AppendResult::Recovered(cursor) => cursor,
        };
    }
    let restarted = Journal::open(store, config()).await.unwrap();
    let mut cursor = restarted.start("bucket");
    let mut pending = PendingSet::default();
    let mut projected = cursor.clone();
    loop {
        let page = restarted.scan(&cursor).await.unwrap();
        for record in &page.records {
            pending.apply_at(&projected, record).unwrap();
            projected.next_slot += 1;
        }
        cursor = page.cursor;
        if page.at_first_absent {
            break;
        }
    }
    assert_eq!(cursor.next_slot, 5);
    assert_eq!(pending.operations_for("x").unwrap().len(), 1);
    assert!(pending.operations_for("x").unwrap().contains("b"));
    assert!(pending.operations_for("y").is_none());
}

#[tokio::test]
async fn corruption_and_admission_mismatch_fail_closed() {
    let store = MemoryStore::default();
    let journal = Journal::open(store.clone(), config()).await.unwrap();
    let start = journal.start("bucket");
    journal.append(&start, &intent("a", "x")).await.unwrap();
    let key = store
        .state
        .lock()
        .unwrap()
        .objects
        .keys()
        .find(|key| key.contains("/slot/"))
        .unwrap()
        .clone();
    store.state.lock().unwrap().objects.get_mut(&key).unwrap()[12] ^= 1;
    assert!(matches!(
        journal.scan(&start).await,
        Err(JournalError::Corrupt(_))
    ));
    assert_eq!(
        journal
            .append(
                &start,
                &Record::ReaderAdmission {
                    reader_id: "reader".into(),
                    incarnation: 1,
                    max_serve_ms: 2_001,
                    config_version: 1,
                }
            )
            .await,
        Err(JournalError::ConfigMismatch)
    );
    let mut changed = config();
    changed.source.generation = 8;
    assert!(matches!(
        Journal::open(store, changed).await,
        Err(JournalError::ConfigMismatch)
    ));
}

#[tokio::test]
async fn finite_capacity_keeps_an_unresolved_intent_pending() {
    let store = MemoryStore::default();
    let mut config = config();
    config.limits.max_slots = 1;
    let journal = Journal::open(store, config).await.unwrap();
    let start = journal.start("bucket");
    let after = match journal.append(&start, &intent("a", "x")).await.unwrap() {
        super::AppendResult::Created(cursor) | super::AppendResult::Recovered(cursor) => cursor,
    };
    assert_eq!(
        journal
            .append(&after, &outcome("a", "x", 0, FinalOutcome::Committed))
            .await,
        Err(JournalError::Capacity)
    );
    let mut pending = PendingSet::default();
    pending
        .apply_at(&start, &journal.scan(&start).await.unwrap().records[0])
        .unwrap();
    assert!(pending.operations_for("x").is_some());
}

#[test]
fn partial_outcomes_never_clear_an_overlapping_intent() {
    let mut pending = PendingSet::default();
    pending.apply_at(&at(0), &intent("first", "same")).unwrap();
    pending.apply_at(&at(1), &intent("second", "same")).unwrap();
    pending
        .apply_at(
            &at(2),
            &outcome("first", "same", 0, FinalOutcome::AbortedBeforeDispatch),
        )
        .unwrap();
    assert!(pending.operations_for("same").unwrap().contains("second"));
    let before = pending.clone();
    assert_eq!(
        pending.apply_at(
            &at(3),
            &outcome("second", "other", 1, FinalOutcome::Committed)
        ),
        Err(JournalError::Corrupt("outcome has no matching intent"))
    );
    assert_eq!(pending, before);
}

#[test]
fn replay_reorder_cross_scope_and_reused_id_leave_projection_unchanged() {
    let mut pending = PendingSet::default();
    pending.apply_at(&at(0), &intent("a", "x")).unwrap();
    let before = pending.clone();
    assert_eq!(
        pending.apply_at(&at(0), &intent("a", "x")),
        Err(JournalError::ConfigMismatch)
    );
    assert_eq!(
        pending.apply_at(&at(2), &intent("b", "x")),
        Err(JournalError::ConfigMismatch)
    );
    let mut other = at(1);
    other.bucket = "other".into();
    assert_eq!(
        pending.apply_at(&other, &intent("b", "x")),
        Err(JournalError::ConfigMismatch)
    );
    assert_eq!(pending, before);
    pending
        .apply_at(&at(1), &outcome("a", "x", 0, FinalOutcome::Committed))
        .unwrap();
    let closed = pending.clone();
    assert_eq!(
        pending.apply_at(&at(2), &intent("a", "x")),
        Err(JournalError::Corrupt("reused operation ID"))
    );
    assert_eq!(
        pending.apply_at(&at(2), &outcome("a", "x", 0, FinalOutcome::Committed)),
        Err(JournalError::Corrupt("outcome has no matching intent"))
    );
    assert_eq!(pending, closed);
}

#[tokio::test]
async fn forged_future_cursor_cannot_create_a_hole() {
    let store = MemoryStore::default();
    let journal = Journal::open(store.clone(), config()).await.unwrap();
    let mut future = journal.start("bucket");
    future.next_slot = 5;
    assert_eq!(
        journal.append(&future, &intent("a", "x")).await,
        Err(JournalError::Corrupt("cursor predecessor absent"))
    );
    assert!(
        !store
            .state
            .lock()
            .unwrap()
            .objects
            .keys()
            .any(|key| key.contains("/slot/"))
    );
}

#[tokio::test]
async fn occupied_probe_budget_returns_a_verified_resume_cursor() {
    let store = MemoryStore::default();
    let mut config = config();
    config.limits.max_append_probes = 2;
    let journal = Journal::open(store, config).await.unwrap();
    let start = journal.start("bucket");
    let mut next = start.clone();
    for id in ["a", "b", "c"] {
        next = match journal.append(&next, &intent(id, id)).await.unwrap() {
            super::AppendResult::Created(cursor) | super::AppendResult::Recovered(cursor) => cursor,
        };
    }
    let resume = match journal.append(&start, &intent("d", "d")).await {
        Err(JournalError::AppendBackpressured { resume }) => resume,
        other => panic!("expected append backpressure, got {other:?}"),
    };
    assert_eq!(resume.next_slot, 2);
    let appended = journal.append(&resume, &intent("d", "d")).await.unwrap();
    assert_eq!(appended, super::AppendResult::Created(at(4)));
    assert_eq!(journal.scan(&start).await.unwrap().records.len(), 4);
}

#[tokio::test]
async fn conflicting_logical_identity_never_creates_a_second_record() {
    let store = MemoryStore::default();
    let journal = Journal::open(store, config()).await.unwrap();
    let start = journal.start("bucket");
    let after_intent = match journal.append(&start, &intent("a", "x")).await.unwrap() {
        super::AppendResult::Created(cursor) | super::AppendResult::Recovered(cursor) => cursor,
    };
    assert_eq!(
        journal.append(&start, &intent("a", "other")).await,
        Err(JournalError::Corrupt("logical record identity conflict"))
    );
    let after_outcome = match journal
        .append(
            &after_intent,
            &outcome("a", "x", 0, FinalOutcome::Committed),
        )
        .await
        .unwrap()
    {
        super::AppendResult::Created(cursor) | super::AppendResult::Recovered(cursor) => cursor,
    };
    assert_eq!(
        journal
            .append(
                &after_intent,
                &outcome("a", "x", 0, FinalOutcome::RejectedWithoutMutation)
            )
            .await,
        Err(JournalError::Corrupt("logical record identity conflict"))
    );
    let admission = |duration| Record::ReaderAdmission {
        reader_id: "reader".into(),
        incarnation: 1,
        max_serve_ms: duration,
        config_version: 1,
    };
    journal
        .append(&after_outcome, &admission(1_000))
        .await
        .unwrap();
    assert_eq!(
        journal.append(&after_outcome, &admission(1_001)).await,
        Err(JournalError::Corrupt("logical record identity conflict"))
    );
    assert_eq!(journal.scan(&start).await.unwrap().records.len(), 3);
}

#[tokio::test]
async fn outcome_can_probe_past_its_intent_from_stale_cursor() {
    let store = MemoryStore::default();
    let journal = Journal::open(store, config()).await.unwrap();
    let start = journal.start("bucket");
    journal.append(&start, &intent("a", "x")).await.unwrap();
    assert_eq!(
        journal
            .append(&start, &outcome("a", "x", 0, FinalOutcome::Committed))
            .await,
        Ok(super::AppendResult::Created(at(2)))
    );
}
