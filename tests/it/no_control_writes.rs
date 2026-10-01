//! The default proxy never stores its coordination state in the user's S3 origin.

use crate::common;

use bytes::Bytes;
use common::{
    Origin, WarmDir, body_blob, delete, get, gossip_pair, proxy_over, put, request, wait_for_index,
    warm_proxy_over,
};
use http::Method;
use s3cache::cache::proxy::CachingProxy;
use s3cache::metrics::Metrics;
use s3s::S3;
use s3s::dto::{
    CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart, CopyObjectInput,
    CopySource, CreateMultipartUploadInput, UploadPartInput,
};
use std::sync::Arc;

#[tokio::test]
async fn default_startup_recovery_reads_and_user_writes_create_no_control_objects() {
    let origin = Origin::start("no-control-writes").await;
    let bucket = origin.bucket();
    origin.seed("source", b"source body").await;
    let warm = WarmDir::new("no-control-writes");
    let metrics = Arc::new(Metrics::default());

    // The production constructor has no control-store argument. Boot and a warm-disk
    // restart both rebuild the index from origin LIST, without mutating the origin.
    {
        let first = warm_proxy_over(&origin.counted_client(), 1024 * 1024, None, &warm, &metrics);
        first.spawn_background_sync(vec![bucket.to_owned()]);
        wait_for_index(&first, &origin, bucket).await;
        assert_eq!(get(&first, bucket, "source").await, "source body");
    }
    let proxy = warm_proxy_over(&origin.counted_client(), 1024 * 1024, None, &warm, &metrics);
    proxy.spawn_background_sync(vec![bucket.to_owned()]);
    wait_for_index(&proxy, &origin, bucket).await;
    assert_eq!(get(&proxy, bucket, "source").await, "source body");
    assert!(
        origin.ops.writes().is_empty(),
        "boot, restart recovery, and reads must not write any origin key"
    );

    put(&proxy, bucket, "remove", b"user body").await;
    delete(&proxy, bucket, "remove").await;

    let mut copy = CopyObjectInput::builder();
    copy.set_bucket(bucket.to_owned());
    copy.set_key("copied".to_owned());
    copy.set_copy_source(CopySource::Bucket {
        bucket: bucket.to_owned().into(),
        key: "source".into(),
        version_id: None,
    });
    proxy
        .copy_object(request(copy.build().expect("complete copy request")))
        .await
        .expect("copy succeeds");

    complete_one_part(&proxy, bucket).await;

    assert_eq!(
        origin.stored("copied").await.as_deref(),
        Some(&b"source body"[..])
    );
    assert_eq!(
        origin.stored("assembled").await.as_deref(),
        Some(&b"one part"[..])
    );

    let writes = origin.ops.writes();
    let targets = writes
        .iter()
        .map(|write| (write.method.clone(), write.path.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        targets,
        [
            (Method::PUT, "/no-control-writes/remove"),
            (Method::DELETE, "/no-control-writes/remove"),
            (Method::PUT, "/no-control-writes/copied"),
            (Method::POST, "/no-control-writes/assembled"),
            (Method::PUT, "/no-control-writes/assembled"),
            (Method::POST, "/no-control-writes/assembled"),
        ],
        "only the named user objects may receive mutating requests"
    );
    assert!(
        writes[3]
            .query
            .as_deref()
            .is_some_and(|query| query.contains("uploads")),
        "multipart initiation is on the user key"
    );
    assert!(
        writes[4]
            .query
            .as_deref()
            .is_some_and(|query| query.contains("uploadId=")),
        "multipart part is on the user key"
    );
    assert!(
        writes[5]
            .query
            .as_deref()
            .is_some_and(|query| query.contains("uploadId=")),
        "multipart completion is on the user key"
    );
}

async fn complete_one_part(proxy: &CachingProxy, bucket: &str) {
    let upload = proxy
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: bucket.to_owned(),
            key: "assembled".to_owned(),
            ..Default::default()
        }))
        .await
        .expect("multipart upload starts")
        .output
        .upload_id
        .expect("upload id");
    let body = Bytes::from_static(b"one part");
    let part = proxy
        .upload_part(request(UploadPartInput {
            bucket: bucket.to_owned(),
            key: "assembled".to_owned(),
            upload_id: upload.clone(),
            part_number: 1,
            content_length: Some(i64::try_from(body.len()).expect("small part")),
            body: Some(body_blob(body)),
            ..Default::default()
        }))
        .await
        .expect("part uploads")
        .output;
    proxy
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: bucket.to_owned(),
            key: "assembled".to_owned(),
            upload_id: upload,
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    e_tag: part.e_tag,
                    part_number: Some(1),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        }))
        .await
        .expect("multipart upload completes");
}

#[tokio::test]
async fn default_strong_gossip_startup_and_write_still_touch_only_the_user_key() {
    let origin = Origin::start("no-control-gossip").await;
    let bucket = origin.bucket();
    origin.seed("seed", b"origin data").await;
    let (sync_a, sync_b) = gossip_pair("no-control-a", "no-control-b").await;
    let client = origin.counted_client();
    let node_a = proxy_over(&client, 1024 * 1024, Some(sync_a));
    let node_b = proxy_over(&client, 1024 * 1024, Some(sync_b));
    for node in [&node_a, &node_b] {
        node.start_coherence(&[bucket.to_owned()]);
        node.spawn_background_sync(vec![bucket.to_owned()]);
        wait_for_index(node, &origin, bucket).await;
    }
    assert_eq!(get(&node_b, bucket, "seed").await, "origin data");
    assert!(
        origin.ops.writes().is_empty(),
        "strong gossip boot, index recovery, and reads need no origin metadata"
    );

    put(&node_a, bucket, "new", b"user write").await;
    let writes = origin.ops.writes();
    assert_eq!(writes.len(), 1, "coherence must not add a control write");
    assert_eq!(writes[0].method, Method::PUT);
    assert_eq!(writes[0].path, "/no-control-gossip/new");
    assert!(
        writes[0]
            .query
            .as_deref()
            .is_some_and(|query| query == "x-id=PutObject"),
        "the only query is the SDK's ordinary user PUT operation ID"
    );
}
