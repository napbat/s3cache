//! Conditional control-slot behavior against the real S3 adapter and `MinIO`.

mod common;

use s3cache::sync::control::{
    AppendResult, Journal, JournalConfig, Limits, Mutation, Record, S3SlotStore, SourceId,
};

use common::Origin;

#[tokio::test]
async fn conditional_slots_are_contiguous_and_read_back_exactly() {
    let origin = Origin::start("control-data").await;
    let client = origin.counted_client();
    client
        .create_bucket()
        .bucket("control-journal")
        .send()
        .await
        .expect("separate control bucket");
    let limits = Limits {
        max_record_bytes: 1024,
        max_slots: 16,
        max_scan_events: 16,
        max_scan_bytes: 16 * 1024,
        max_append_probes: 8,
    };
    let config = JournalConfig {
        source: SourceId {
            name: "control-test".into(),
            generation: 1,
        },
        admission_version: 1,
        max_admission_ms: 2_000,
        limits,
    };
    let store = S3SlotStore::new(client, "control-journal", limits.max_record_bytes)
        .expect("store configuration");
    let journal = Journal::open(store.clone(), config.clone())
        .await
        .expect("manifest");
    let start = journal.start("objects");
    let first = Record::Intent {
        op_id: "first".into(),
        key: "a".into(),
        mutation: Mutation::Put,
    };
    let second = Record::Intent {
        op_id: "second".into(),
        key: "b".into(),
        mutation: Mutation::Delete,
    };
    let (a, b) = tokio::join!(
        journal.append(&start, &first),
        journal.append(&start, &second)
    );
    assert!(matches!(a.expect("first append"), AppendResult::Created(_)));
    assert!(matches!(
        b.expect("second append"),
        AppendResult::Created(_)
    ));
    let reopened = Journal::open(store, config)
        .await
        .expect("restart manifest");
    let page = reopened.scan(&start).await.expect("contiguous readback");
    assert_eq!(page.records.len(), 2);
    assert!(page.records.contains(&first));
    assert!(page.records.contains(&second));
    assert_eq!(page.cursor.next_slot, 2);
    assert!(page.at_first_absent);
    assert!(matches!(
        reopened.append(&start, &page.records[0]).await,
        Ok(AppendResult::Recovered(_))
    ));
}
