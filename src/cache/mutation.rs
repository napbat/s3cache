//! The write-through paths: forward a mutation, then make every node's view of the key
//! agree with what the origin did before the client hears about it.
//!
//! Each mutation's origin call and coherence tail run on a task of their own
//! ([`CachingProxy::mutation_tail`]), so a client that hangs up cannot strand an applied
//! write outside the index. A success indexes and advertises what was written and holds
//! the response until the cluster has applied it. A failure that does not prove the
//! origin refused the write leaves the key's state unknown: it is fenced on every node
//! and reconciled from the origin ([`CachingProxy::publish_unknown`]) before the error
//! is answered, so no node answers that key from local state in the meantime.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::SystemTime;

use s3s::dto::{
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput,
    DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, PutObjectInput,
    PutObjectOutput,
};
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

use crate::cache::copy;
use crate::cache::proxy::{
    CachingProxy, IndexedWrite, observed_entry, write_storage_class, written_object,
};
use crate::index::{ObjEntry, ObjMeta};
use crate::tier::CachedObject;

/// Why a failed mutation cannot be assumed not to have reached durable origin state. A
/// 412 is normally a conclusive client-side race; it becomes contradictory only when
/// this proxy had just vouched that the exact precondition held in its
/// locally-serveable index.
fn uncertain_mutation_error(error: &S3Error, locally_vouched: bool) -> Option<&'static str> {
    match error.status_code() {
        Some(http::StatusCode::PRECONDITION_FAILED) if locally_vouched => {
            Some("origin rejected a locally-vouched precondition")
        }
        Some(status) if status.is_server_error() || status == http::StatusCode::REQUEST_TIMEOUT => {
            Some("origin returned an ambiguous server failure")
        }
        None => Some("origin response was unavailable"),
        _ => None,
    }
}

/// [`uncertain_mutation_error`] for a multipart completion, which has one more: a
/// completion the origin applied and then failed to answer is retried by the SDK, and
/// the retry finds the upload already gone.
fn uncertain_completion_error(error: &S3Error) -> Option<&'static str> {
    uncertain_mutation_error(error, false).or_else(|| {
        (error.code() == &S3ErrorCode::NoSuchUpload)
            .then_some("the upload is gone; an earlier attempt may have completed it")
    })
}

fn copy_conflict_needs_reconcile(error: &S3Error, create_only: bool) -> bool {
    create_only && error.status_code() == Some(http::StatusCode::PRECONDITION_FAILED)
}

impl CachingProxy {
    /// Forward a `PutObject`, then update the index from the result — and, when the write
    /// knows exactly what a read of it will report, keep the body it just wrote rather
    /// than dropping it (see [`buffered_put_body`](CachingProxy::buffered_put_body)).
    pub(super) async fn put(
        &self,
        mut req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        // Everything a HEAD of this object will report, straight off the request that
        // created it — no HEAD needed to learn what we were just told. Except the
        // Content-Type: with none set the origin invents one, and an entry claiming to
        // know it would answer HEADs the origin answers differently, so such an entry
        // stays skeletal until a forwarded HEAD completes it.
        let faithful = req.input.content_type.is_some();
        let meta = ObjMeta {
            cache_control: req.input.cache_control.clone(),
            content_disposition: req.input.content_disposition.clone(),
            content_encoding: req.input.content_encoding.clone(),
            content_language: req.input.content_language.clone(),
            // `x-amz-meta-*` names are HTTP header names, so the origin reports them
            // lowercased whatever case they were sent in; capturing them verbatim would
            // make a HEAD off this entry differ from the origin's in the key casing.
            metadata: req.input.metadata.as_ref().map(|m| {
                m.iter()
                    .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
                    .collect()
            }),
        };
        let content_type = req.input.content_type.clone();
        let mut entry = ObjEntry {
            // A PUT with no Content-Length leaves the size unknown rather than zero: a
            // fabricated `0` is served as an authoritative Content-Length forever.
            size: req.input.content_length,
            last_modified: SystemTime::UNIX_EPOCH, // stamped by `record_put`
            etag: None,
            storage_class: write_storage_class(req.input.storage_class.as_ref()),
            content_type: content_type.clone(),
            meta: faithful.then(|| Box::new(meta.clone())),
        };
        // Read before the write round-trip, not after it: a remediation that distrusts
        // the cache while this one is in flight must leave the copy it lands suspect —
        // this node's own bytes are not proof that a peer's concurrent write did not
        // land behind them at the origin.
        let generation = self.obj_cache.suspect_gen();
        // The bytes a client writes are the bytes its next read wants, and they are
        // already in hand: buffer them (bounded by the same per-object cap the read path
        // uses) so a freshly written object's first read is not a guaranteed origin GET.
        let written = if self.sync.as_ref().is_none_or(|sync| sync.may_serve_local()) {
            self.buffered_put_body(&mut req.input).await
        } else {
            None
        };
        let ckey = (bucket.clone(), key.clone());
        let locally_vouched = self.locally_vouches_for_put(&req.input);
        // Invalidate before the origin call. Besides closing the ordinary in-flight read
        // window, this is the only ordering that survives a process crash after the
        // origin applies a write but before it can answer: no warm or hot copy of the old
        // body is left behind to be trusted after restart.
        self.obj_cache.invalidate(&ckey).await;
        let worker = self.clone();
        let (tail_bucket, tail_key) = (bucket.clone(), key.clone());
        self.mutation_tail(&tail_bucket, &[tail_key], async move {
            let mut resp = match worker.inner.put_object(req).await {
                Ok(resp) => resp,
                Err(error) => {
                    if let Some(reason) = uncertain_mutation_error(&error, locally_vouched) {
                        worker.publish_unknown(&bucket, &key, reason).await;
                    }
                    return Err(error);
                }
            };
            // The origin's ETag rides back on the response, so the index learns it here
            // rather than paying a HEAD for what a later HEAD will want to report.
            entry.etag = resp.output.e_tag.clone();
            // An origin GET that started during this PUT may have filled the old
            // body after the pre-write invalidation. Fence it again at commit.
            worker.obj_cache.invalidate(&ckey).await;
            // The body just written takes the dropped copy's place. An ETag-less write
            // response cannot faithfully describe the object and therefore fills nothing.
            if let (Some(body), Some(e_tag)) = (written, entry.etag.clone()) {
                let out = written_object(content_type, e_tag, &meta, body.len());
                // Stamped as of before the write: a remediation that moved the generation
                // while it was in flight leaves this copy suspect, as intended.
                let filled = CachedObject::from_get(&out, body);
                filled.mark_trusted(generation);
                worker.obj_cache.insert(ckey, Arc::new(filled)).await;
                worker.metrics.write_fill();
            }
            let token = worker
                .record_put(IndexedWrite::Put, &bucket, &key, entry)
                .await;
            Self::attach_token(&mut resp.headers, token);
            Ok(resp)
        })
        .await
    }

    /// Forward a `DeleteObject`, then tombstone the key everywhere.
    pub(super) async fn delete(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let versioned = req.input.version_id.is_some();
        let worker = self.clone();
        let (tail_bucket, tail_key) = (bucket.clone(), key.clone());
        self.mutation_tail(&tail_bucket, &[tail_key], async move {
            let mut resp = match worker.inner.delete_object(req).await {
                Ok(resp) => resp,
                Err(error) => {
                    if let Some(reason) = uncertain_mutation_error(&error, false) {
                        worker.publish_unknown(&bucket, &key, reason).await;
                    }
                    return Err(error);
                }
            };
            worker
                .obj_cache
                .invalidate(&(bucket.clone(), key.clone()))
                .await;
            // A version-scoped delete removes one version, not the key: the current
            // object may be untouched, or may now be a different version entirely. What
            // the key resolves to is the origin's to report, so every node answers it
            // from the origin until an origin HEAD says.
            let token = if versioned {
                worker
                    .publish_unknown(&bucket, &key, "a version-scoped delete")
                    .await
            } else {
                let receipt = worker.record_del(&bucket, &key).await;
                worker.await_cluster(receipt, &bucket, &key).await
            };
            Self::attach_token(&mut resp.headers, token);
            Ok(resp)
        })
        .await
    }

    /// Forward a `DeleteObjects`, then tombstone every key the origin deleted.
    pub(super) async fn delete_batch(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = req.input.bucket.clone();
        let quiet = req.input.delete.quiet.unwrap_or(false);
        let requested: Vec<(String, bool)> = req
            .input
            .delete
            .objects
            .iter()
            .map(|o| (o.key.clone(), o.version_id.is_some()))
            .collect();
        let keys: Vec<String> = requested.iter().map(|(key, _)| key.clone()).collect();
        let worker = self.clone();
        let tail_bucket = bucket.clone();
        self.mutation_tail(&tail_bucket, &keys, async move {
            let mut resp = match worker.inner.delete_objects(req).await {
                Ok(resp) => resp,
                Err(error) => {
                    if let Some(reason) = uncertain_mutation_error(&error, false) {
                        let mut receipt = None;
                        for (key, _) in &requested {
                            receipt = worker
                                .announce_unknown(&bucket, key, reason)
                                .await
                                .or(receipt);
                        }
                        worker
                            .await_cluster(receipt, &bucket, "<batch delete>")
                            .await;
                    }
                    return Err(error);
                }
            };
            // `DeleteObjects` is partial-failure by contract: the call succeeds while
            // individual keys are refused (a legal hold, a retention lock, a permission).
            // Unindexing every *requested* key makes a key the origin still holds vanish
            // cluster-wide — LIST loses it and HEAD 404s — until the next resync, so the
            // applied set is read off the response. In quiet mode the origin omits the
            // Deleted half, and what was asked for minus what was refused is the same set.
            let refused: BTreeSet<&str> = resp
                .output
                .errors
                .iter()
                .flatten()
                .filter_map(|e| e.key.as_deref())
                .collect();
            let deleted: BTreeSet<&str> = resp
                .output
                .deleted
                .iter()
                .flatten()
                .filter_map(|d| d.key.as_deref())
                .collect();
            let mut receipt = None;
            for (key, versioned) in &requested {
                let applied = if quiet {
                    !refused.contains(key.as_str())
                } else {
                    deleted.contains(key.as_str())
                };
                if !applied {
                    continue;
                }
                worker
                    .obj_cache
                    .invalidate(&(bucket.clone(), key.clone()))
                    .await;
                // Keep the newest receipt: its token covers the whole batch (one writer,
                // ordered feed), so the cluster round is paid once rather than per key —
                // a 1000-key batch of 2s waits is half an hour of held response. One
                // version is not the key (see `delete`): its key's state is unknown.
                let published = if *versioned {
                    worker
                        .announce_unknown(&bucket, key, "a version-scoped delete")
                        .await
                } else {
                    worker.record_del(&bucket, key).await
                };
                receipt = published.or(receipt);
            }
            let token = worker
                .await_cluster(receipt, &bucket, "<batch delete>")
                .await;
            Self::attach_token(&mut resp.headers, token);
            Ok(resp)
        })
        .await
    }

    /// Forward a `CompleteMultipartUpload`, then index the assembled object.
    pub(super) async fn complete_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let worker = self.clone();
        let (tail_bucket, tail_key) = (bucket.clone(), key.clone());
        self.mutation_tail(&tail_bucket, &[tail_key], async move {
            let mut resp = match worker.inner.complete_multipart_upload(req).await {
                Ok(resp) => resp,
                Err(error) => {
                    if let Some(reason) = uncertain_completion_error(&error) {
                        worker.publish_unknown(&bucket, &key, reason).await;
                    }
                    return Err(error);
                }
            };
            worker
                .obj_cache
                .invalidate(&(bucket.clone(), key.clone()))
                .await;
            // Multipart is how the big objects arrive, and indexing them at a placeholder
            // size poisoned the range-promotion decision (a "0-byte" entry promoted a
            // multi-GB fetch). One HEAD learns the real size — and, since it is being paid
            // for anyway, everything else a HEAD of the assembled object reports, provided
            // it describes this completion and not a later overwrite.
            let completed = resp.output.e_tag.clone();
            let observed = worker
                .upstream_meta(&bucket, &key)
                .await
                .filter(|observed| completed.is_none() || observed.etag == completed);
            let mut entry = observed_entry(observed.as_ref());
            entry.etag = completed.or(entry.etag);
            let token = worker
                .record_put(IndexedWrite::MultipartComplete, &bucket, &key, entry)
                .await;
            Self::attach_token(&mut resp.headers, token);
            Ok(resp)
        })
        .await
    }

    /// Forward a `CopyObject`, then index the destination.
    pub(super) async fn copy(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let source = req.input.copy_source.clone();
        let storage_class = write_storage_class(req.input.storage_class.as_ref());
        let create_only = copy::destination_must_be_absent(&req);
        // Invalidate before forwarding, for the same crash boundary as PUT: a
        // successful overwrite must never leave an old body trusted locally.
        self.obj_cache
            .invalidate(&(bucket.clone(), key.clone()))
            .await;
        let worker = self.clone();
        let (tail_bucket, tail_key) = (bucket.clone(), key.clone());
        self.mutation_tail(&tail_bucket, &[tail_key], async move {
            let mut resp = match copy::forward(&worker.copy_inner, req).await {
                Ok(resp) => resp,
                Err(error) => {
                    if copy_conflict_needs_reconcile(&error, create_only) {
                        // The origin's 412 proves an immutable create-only
                        // destination already exists. That can race ahead of this
                        // proxy's LIST index (for example after an interrupted
                        // response), where an immediate HEAD would otherwise be a
                        // false local 404. One authoritative HEAD folds the proven
                        // object into this node before the 412 reaches the caller;
                        // the caller can then confirm it without replaying COPYs.
                        let fence = worker
                            .obj_cache
                            .observation_fence(&(bucket.clone(), key.clone()));
                        if let Some(observed) = worker.upstream_meta(&bucket, &key).await {
                            fence
                                .commit(|| worker.observe(&bucket, &key, &observed))
                                .await;
                            worker.metrics.copy_conflict_reconciled();
                        } else {
                            worker.metrics.copy_conflict_reconcile_miss();
                        }
                    } else if let Some(reason) = uncertain_mutation_error(&error, false) {
                        worker.publish_unknown(&bucket, &key, reason).await;
                    }
                    return Err(error);
                }
            };
            // A GET could have cached the old destination after the pre-copy
            // invalidation. Fence that fill after the origin commits the copy.
            worker
                .obj_cache
                .invalidate(&(bucket.clone(), key.clone()))
                .await;
            let copied_etag = resp
                .output
                .copy_object_result
                .as_ref()
                .and_then(|result| result.e_tag.clone());
            let mut entry = if let Some(size) = copied_etag
                .as_ref()
                .and_then(|etag| copy::indexed_source_size(&worker, &source, etag))
            {
                // The matching ETag proves the copied bytes have the indexed source's
                // length. Keep the row skeletal because metadata can change without the
                // ETag changing; a later HEAD/GET completes it if anyone needs those fields.
                worker.metrics.copy_head_avoided();
                ObjEntry {
                    size: Some(size),
                    last_modified: SystemTime::UNIX_EPOCH,
                    etag: copied_etag.clone(),
                    storage_class,
                    content_type: None,
                    meta: None,
                }
            } else {
                worker.metrics.copy_head_fallback();
                // Only a HEAD that describes this copy, not a later overwrite.
                let observed = worker
                    .upstream_meta(&bucket, &key)
                    .await
                    .filter(|observed| copied_etag.is_none() || observed.etag == copied_etag);
                observed_entry(observed.as_ref())
            };
            entry.etag = copied_etag.or(entry.etag);
            let token = worker
                .record_put(IndexedWrite::Copy, &bucket, &key, entry)
                .await;
            Self::attach_token(&mut resp.headers, token);
            Ok(resp)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        copy_conflict_needs_reconcile, uncertain_completion_error, uncertain_mutation_error,
    };
    use s3s::{S3Error, S3ErrorCode};

    #[test]
    fn only_a_locally_vouched_412_is_an_uncertain_mutation() {
        let rejected = S3Error::new(S3ErrorCode::PreconditionFailed);
        assert_eq!(uncertain_mutation_error(&rejected, false), None);
        assert_eq!(
            uncertain_mutation_error(&rejected, true),
            Some("origin rejected a locally-vouched precondition")
        );

        let failed = S3Error::new(S3ErrorCode::InternalError);
        assert_eq!(
            uncertain_mutation_error(&failed, false),
            Some("origin returned an ambiguous server failure")
        );
    }

    /// A retried completion whose first attempt the origin applied finds the upload
    /// gone; any other refusal is the origin's answer.
    #[test]
    fn a_vanished_upload_leaves_a_completion_uncertain() {
        assert!(uncertain_completion_error(&S3Error::new(S3ErrorCode::NoSuchUpload)).is_some());
        assert!(uncertain_completion_error(&S3Error::new(S3ErrorCode::InvalidPart)).is_none());
    }

    #[test]
    fn only_a_create_only_412_requests_copy_conflict_reconciliation() {
        let conflict = S3Error::new(S3ErrorCode::PreconditionFailed);
        assert!(copy_conflict_needs_reconcile(&conflict, true));
        assert!(!copy_conflict_needs_reconcile(&conflict, false));

        let failed = S3Error::new(S3ErrorCode::InternalError);
        assert!(!copy_conflict_needs_reconcile(&failed, true));
    }
}
