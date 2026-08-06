use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use s3s::dto::{
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput,
    DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, GetObjectInput,
    GetObjectOutput, HeadObjectInput, HeadObjectOutput, ListObjectsV2Input, ListObjectsV2Output,
    PutObjectInput, PutObjectOutput,
};
use s3s::{S3Request, S3Response, S3Result};

use crate::cache::proxy::{
    CachingProxy, IndexedWrite, ReadRoute, ResponseOverrides, observed_entry, write_storage_class,
    written_object,
};
use crate::index::{ObjEntry, ObjMeta};
use crate::tier::CachedObject;

/// Moves the `response-*` fields out of an input into a [`ResponseOverrides`]; one macro
/// so GET and HEAD (which spell them identically) cannot drift apart.
macro_rules! take_overrides {
    ($input:expr) => {{
        let input = $input;
        ResponseOverrides {
            content_type: input.response_content_type.take(),
            content_disposition: input.response_content_disposition.take(),
            content_encoding: input.response_content_encoding.take(),
            content_language: input.response_content_language.take(),
            cache_control: input.response_cache_control.take(),
            expires: input.response_expires.take(),
        }
    }};
}

/// Applies the overrides onto a response, leaving untouched anything the request did not
/// ask to override. `GetObjectOutput` and `HeadObjectOutput` spell these identically, so
/// one macro drives both.
macro_rules! apply_overrides {
    ($overrides:expr, $out:expr) => {{
        let (overrides, out) = ($overrides, $out);
        for (field, value) in [
            (&mut out.content_type, &overrides.content_type),
            (&mut out.content_disposition, &overrides.content_disposition),
            (&mut out.content_encoding, &overrides.content_encoding),
            (&mut out.content_language, &overrides.content_language),
            (&mut out.cache_control, &overrides.cache_control),
        ] {
            if value.is_some() {
                field.clone_from(value);
            }
        }
        if overrides.expires.is_some() {
            out.expires.clone_from(&overrides.expires);
        }
    }};
}

#[async_trait]
impl s3s::S3 for CachingProxy {
    // LIST served from the index when the bucket is synced; else passthrough.
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        // Three things the index cannot answer without guessing at the origin's own
        // wire format or authorisation, so they are forwarded verbatim:
        //   * `encoding-type` — percent-encoding is per-origin (MinIO escapes a space
        //     as `+` and leaves `/*-_.` alone; AWS and R2 differ in the margins), and a
        //     client that decodes what it asked for corrupts every key we guessed at.
        //   * `fetch-owner` — the Owner element is the origin's, and no write path
        //     carries it.
        //   * `x-amz-expected-bucket-owner` — a guard only the origin can evaluate; an
        //     answer served locally would skip a check the origin would have failed.
        let origin_only = req.input.encoding_type.is_some()
            || req.input.fetch_owner.unwrap_or(false)
            || req.input.expected_bucket_owner.is_some();
        if !origin_only
            && self.is_synced(req.input.bucket.as_str())
            && self.read_barrier(&req.headers).await == ReadRoute::Local
            && let Some(out) = self.list_from_index(&req.input)
        {
            self.metrics.list_from_index();
            return Ok(S3Response::new(out));
        }
        self.metrics.list_passthrough();
        self.inner.list_objects_v2(req).await
    }

    // Writes: forward (write-through), then update the index from the result — and, when
    // the write knows exactly what a read of it will report, keep the body it just wrote
    // rather than dropping it (see [`buffered_put_body`](CachingProxy::buffered_put_body)).
    async fn put_object(
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
        let written = self.buffered_put_body(&mut req.input).await;
        let mut resp = self.inner.put_object(req).await?;
        let ckey = (bucket.clone(), key.clone());
        // Drop this node's own copy the instant the origin has the new bytes — before
        // the cluster-ack round the index update pays for. Otherwise the writing node
        // is the one node in the fleet still serving the old body.
        self.obj_cache.invalidate(&ckey).await;
        // The origin's ETag rides back on the response, so the index learns it here
        // rather than paying a HEAD for what a later HEAD will want to report.
        entry.etag = resp.output.e_tag.clone();
        // ...and the body just written takes the dropped copy's place. Invalidate *then*
        // insert, rather than letting the insert overwrite on its own: a tier write is
        // best-effort each, so a warm-tier put that is refused (the encoded object over
        // the cap) or fails would otherwise leave the *older* body on disk, to be served
        // the moment hot evicts the new one. Between the two there is only a local miss
        // — an origin read, never a stale answer — where an insert-only ordering risks a
        // stale one. An ETag-less write response is not enough to describe the object
        // with, so it fills nothing either.
        if let (Some(body), Some(e_tag)) = (written, entry.etag.clone()) {
            let out = written_object(content_type, e_tag, &meta, body.len());
            // The writer's own bytes, stamped as of before the write: this node just made
            // them the truth, so its next read of them needs no proof — unless the
            // generation moved while the write was in flight, and then it does.
            let filled = CachedObject::from_get(&out, body);
            filled.mark_trusted(generation);
            self.obj_cache.insert(ckey, Arc::new(filled)).await;
            self.metrics.write_fill();
        }
        let token = self
            .record_put(IndexedWrite::Put, &bucket, &key, entry)
            .await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let versioned = req.input.version_id.is_some();
        let mut resp = self.inner.delete_object(req).await?;
        self.obj_cache
            .invalidate(&(bucket.clone(), key.clone()))
            .await;
        // A version-scoped delete removes one version, not the key: the current object
        // may be untouched, or may now be a different version entirely. Only the local
        // body copy is provably stale — what the key resolves to stays the origin's to
        // report, so no tombstone is recorded and the entry is left for a HEAD or the
        // next sync to correct.
        let token = if versioned {
            None
        } else {
            let receipt = self.record_del(&bucket, &key).await;
            self.await_cluster(receipt, &bucket, &key).await
        };
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn delete_objects(
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
        let mut resp = self.inner.delete_objects(req).await?;
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
            self.obj_cache
                .invalidate(&(bucket.clone(), key.clone()))
                .await;
            if *versioned {
                continue; // one version, not the key — see `delete_object`
            }
            // Keep the newest receipt: its token covers the whole batch (one writer,
            // ordered feed), so the cluster round is paid once rather than per key —
            // a 1000-key batch of 2s waits is half an hour of held response.
            receipt = self.record_del(&bucket, key).await.or(receipt);
        }
        let token = self.await_cluster(receipt, &bucket, "<batch delete>").await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let mut resp = self.inner.complete_multipart_upload(req).await?;
        self.obj_cache
            .invalidate(&(bucket.clone(), key.clone()))
            .await;
        // Multipart is how the big objects arrive, and indexing them at a placeholder
        // size poisoned the range-promotion decision (a "0-byte" entry promoted a
        // multi-GB fetch). One HEAD learns the real size — and, since it is being paid
        // for anyway, everything else a HEAD of the assembled object reports.
        let observed = self.upstream_meta(&bucket, &key).await;
        let mut entry = observed_entry(observed.as_ref());
        entry.etag = resp.output.e_tag.clone().or(entry.etag);
        let token = self
            .record_put(IndexedWrite::MultipartComplete, &bucket, &key, entry)
            .await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let mut resp = self.inner.copy_object(req).await?;
        self.obj_cache
            .invalidate(&(bucket.clone(), key.clone()))
            .await;
        // A copy's metadata is the source's (or the request's, per the directive) and
        // its size is whatever the origin assembled, neither of which the response
        // carries: one HEAD is what tells us what we just created.
        let observed = self.upstream_meta(&bucket, &key).await;
        let mut entry = observed_entry(observed.as_ref());
        entry.etag = resp
            .output
            .copy_object_result
            .as_ref()
            .and_then(|result| result.e_tag.clone())
            .or(entry.etag);
        let token = self
            .record_put(IndexedWrite::Copy, &bucket, &key, entry)
            .await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    // GET: cacheable (no part/conditional) small objects are served from the tiered
    // cache; a miss buffers the body and caches it. Ranged reads of cacheable-size
    // objects are served by slicing the cached whole object, promoted on first touch.
    // Oversized/unknown-size ranges and conditional requests stream straight through.
    // The per-request `response-*` overrides are lifted off the request first and put
    // back on the answer last, so they neither reach the cached copy nor go missing on
    // a hit (see [`ResponseOverrides`]).
    async fn get_object(
        &self,
        mut req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let overrides = take_overrides!(&mut req.input);
        let mut resp = self.serve_get(req).await?;
        apply_overrides!(&overrides, &mut resp.output);
        Ok(resp)
    }

    // HEAD served from the object cache when the body is already cached, and from the
    // LIST index when it is not: on a synced bucket the index is authoritative for
    // existence (the property LIST-from-index already rests on) and a *faithful* entry
    // carries everything a HEAD reports — so a HEAD of an uncached object costs nothing,
    // and a HEAD of an absent key is a local 404. A skeletal entry (a bootstrap LIST
    // row, a peer's feed event) is forwarded once and completed from the answer, so the
    // next HEAD of that key is local and identical. Requests that need the origin pass
    // through: a range or part, a specific version, checksums, SSE-C, a bucket-owner
    // guard, and — as on the GET path — anything conditional, since the origin is the
    // authority on whether a precondition holds. So does any bucket whose index has not
    // finished warming.
    async fn head_object(
        &self,
        mut req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let overrides = take_overrides!(&mut req.input);
        let mut resp = self.serve_head(req).await?;
        apply_overrides!(&overrides, &mut resp.output);
        Ok(resp)
    }

    async fn delete_bucket(
        &self,
        req: S3Request<s3s::dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketOutput>> {
        let bucket = req.input.bucket.clone();
        let resp = self.inner.delete_bucket(req).await?;
        // The bucket is gone; its index is not, and would go on answering LIST from a
        // key set the origin no longer has — and HEADs of those keys as authoritative
        // 404s of the wrong kind. Dropping the state returns the name to passthrough.
        self.state.write().unwrap().remove(&bucket);
        Ok(resp)
    }

    // Full S3 passthrough: every other op forwards to the upstream so any S3
    // client works, not just the cached read/write paths. Generated from the s3s S3 trait.
    async fn abort_multipart_upload(
        &self,
        req: S3Request<s3s::dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::AbortMultipartUploadOutput>> {
        self.inner.abort_multipart_upload(req).await
    }
    async fn create_bucket(
        &self,
        req: S3Request<s3s::dto::CreateBucketInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateBucketOutput>> {
        self.inner.create_bucket(req).await
    }
    async fn create_bucket_metadata_table_configuration(
        &self,
        req: S3Request<s3s::dto::CreateBucketMetadataTableConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateBucketMetadataTableConfigurationOutput>> {
        self.inner
            .create_bucket_metadata_table_configuration(req)
            .await
    }
    async fn create_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateMultipartUploadOutput>> {
        self.inner.create_multipart_upload(req).await
    }
    async fn create_session(
        &self,
        req: S3Request<s3s::dto::CreateSessionInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateSessionOutput>> {
        self.inner.create_session(req).await
    }
    async fn delete_bucket_analytics_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketAnalyticsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketAnalyticsConfigurationOutput>> {
        self.inner.delete_bucket_analytics_configuration(req).await
    }
    async fn delete_bucket_cors(
        &self,
        req: S3Request<s3s::dto::DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketCorsOutput>> {
        self.inner.delete_bucket_cors(req).await
    }
    async fn delete_bucket_encryption(
        &self,
        req: S3Request<s3s::dto::DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketEncryptionOutput>> {
        self.inner.delete_bucket_encryption(req).await
    }
    async fn delete_bucket_intelligent_tiering_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketIntelligentTieringConfigurationOutput>> {
        self.inner
            .delete_bucket_intelligent_tiering_configuration(req)
            .await
    }
    async fn delete_bucket_inventory_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketInventoryConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketInventoryConfigurationOutput>> {
        self.inner.delete_bucket_inventory_configuration(req).await
    }
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<s3s::dto::DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketLifecycleOutput>> {
        self.inner.delete_bucket_lifecycle(req).await
    }
    async fn delete_bucket_metadata_table_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketMetadataTableConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketMetadataTableConfigurationOutput>> {
        self.inner
            .delete_bucket_metadata_table_configuration(req)
            .await
    }
    async fn delete_bucket_metrics_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketMetricsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketMetricsConfigurationOutput>> {
        self.inner.delete_bucket_metrics_configuration(req).await
    }
    async fn delete_bucket_ownership_controls(
        &self,
        req: S3Request<s3s::dto::DeleteBucketOwnershipControlsInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketOwnershipControlsOutput>> {
        self.inner.delete_bucket_ownership_controls(req).await
    }
    async fn delete_bucket_policy(
        &self,
        req: S3Request<s3s::dto::DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketPolicyOutput>> {
        self.inner.delete_bucket_policy(req).await
    }
    async fn delete_bucket_replication(
        &self,
        req: S3Request<s3s::dto::DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketReplicationOutput>> {
        self.inner.delete_bucket_replication(req).await
    }
    async fn delete_bucket_tagging(
        &self,
        req: S3Request<s3s::dto::DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketTaggingOutput>> {
        self.inner.delete_bucket_tagging(req).await
    }
    async fn delete_bucket_website(
        &self,
        req: S3Request<s3s::dto::DeleteBucketWebsiteInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketWebsiteOutput>> {
        self.inner.delete_bucket_website(req).await
    }
    async fn delete_object_tagging(
        &self,
        req: S3Request<s3s::dto::DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteObjectTaggingOutput>> {
        self.inner.delete_object_tagging(req).await
    }
    async fn delete_public_access_block(
        &self,
        req: S3Request<s3s::dto::DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<s3s::dto::DeletePublicAccessBlockOutput>> {
        self.inner.delete_public_access_block(req).await
    }
    async fn get_bucket_accelerate_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketAccelerateConfigurationOutput>> {
        self.inner.get_bucket_accelerate_configuration(req).await
    }
    async fn get_bucket_acl(
        &self,
        req: S3Request<s3s::dto::GetBucketAclInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketAclOutput>> {
        self.inner.get_bucket_acl(req).await
    }
    async fn get_bucket_analytics_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketAnalyticsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketAnalyticsConfigurationOutput>> {
        self.inner.get_bucket_analytics_configuration(req).await
    }
    async fn get_bucket_cors(
        &self,
        req: S3Request<s3s::dto::GetBucketCorsInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketCorsOutput>> {
        self.inner.get_bucket_cors(req).await
    }
    async fn get_bucket_encryption(
        &self,
        req: S3Request<s3s::dto::GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketEncryptionOutput>> {
        self.inner.get_bucket_encryption(req).await
    }
    async fn get_bucket_intelligent_tiering_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketIntelligentTieringConfigurationOutput>> {
        self.inner
            .get_bucket_intelligent_tiering_configuration(req)
            .await
    }
    async fn get_bucket_inventory_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketInventoryConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketInventoryConfigurationOutput>> {
        self.inner.get_bucket_inventory_configuration(req).await
    }
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketLifecycleConfigurationOutput>> {
        self.inner.get_bucket_lifecycle_configuration(req).await
    }
    async fn get_bucket_location(
        &self,
        req: S3Request<s3s::dto::GetBucketLocationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketLocationOutput>> {
        self.inner.get_bucket_location(req).await
    }
    async fn get_bucket_logging(
        &self,
        req: S3Request<s3s::dto::GetBucketLoggingInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketLoggingOutput>> {
        self.inner.get_bucket_logging(req).await
    }
    async fn get_bucket_metadata_table_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketMetadataTableConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketMetadataTableConfigurationOutput>> {
        self.inner
            .get_bucket_metadata_table_configuration(req)
            .await
    }
    async fn get_bucket_metrics_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketMetricsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketMetricsConfigurationOutput>> {
        self.inner.get_bucket_metrics_configuration(req).await
    }
    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketNotificationConfigurationOutput>> {
        self.inner.get_bucket_notification_configuration(req).await
    }
    async fn get_bucket_ownership_controls(
        &self,
        req: S3Request<s3s::dto::GetBucketOwnershipControlsInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketOwnershipControlsOutput>> {
        self.inner.get_bucket_ownership_controls(req).await
    }
    async fn get_bucket_policy(
        &self,
        req: S3Request<s3s::dto::GetBucketPolicyInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketPolicyOutput>> {
        self.inner.get_bucket_policy(req).await
    }
    async fn get_bucket_policy_status(
        &self,
        req: S3Request<s3s::dto::GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketPolicyStatusOutput>> {
        self.inner.get_bucket_policy_status(req).await
    }
    async fn get_bucket_replication(
        &self,
        req: S3Request<s3s::dto::GetBucketReplicationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketReplicationOutput>> {
        self.inner.get_bucket_replication(req).await
    }
    async fn get_bucket_request_payment(
        &self,
        req: S3Request<s3s::dto::GetBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketRequestPaymentOutput>> {
        self.inner.get_bucket_request_payment(req).await
    }
    async fn get_bucket_tagging(
        &self,
        req: S3Request<s3s::dto::GetBucketTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketTaggingOutput>> {
        self.inner.get_bucket_tagging(req).await
    }
    async fn get_bucket_versioning(
        &self,
        req: S3Request<s3s::dto::GetBucketVersioningInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketVersioningOutput>> {
        self.inner.get_bucket_versioning(req).await
    }
    async fn get_bucket_website(
        &self,
        req: S3Request<s3s::dto::GetBucketWebsiteInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketWebsiteOutput>> {
        self.inner.get_bucket_website(req).await
    }
    async fn get_object_acl(
        &self,
        req: S3Request<s3s::dto::GetObjectAclInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectAclOutput>> {
        self.inner.get_object_acl(req).await
    }
    async fn get_object_attributes(
        &self,
        req: S3Request<s3s::dto::GetObjectAttributesInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectAttributesOutput>> {
        self.inner.get_object_attributes(req).await
    }
    async fn get_object_legal_hold(
        &self,
        req: S3Request<s3s::dto::GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectLegalHoldOutput>> {
        self.inner.get_object_legal_hold(req).await
    }
    async fn get_object_lock_configuration(
        &self,
        req: S3Request<s3s::dto::GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectLockConfigurationOutput>> {
        self.inner.get_object_lock_configuration(req).await
    }
    async fn get_object_retention(
        &self,
        req: S3Request<s3s::dto::GetObjectRetentionInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectRetentionOutput>> {
        self.inner.get_object_retention(req).await
    }
    async fn get_object_tagging(
        &self,
        req: S3Request<s3s::dto::GetObjectTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectTaggingOutput>> {
        self.inner.get_object_tagging(req).await
    }
    async fn get_object_torrent(
        &self,
        req: S3Request<s3s::dto::GetObjectTorrentInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectTorrentOutput>> {
        self.inner.get_object_torrent(req).await
    }
    async fn get_public_access_block(
        &self,
        req: S3Request<s3s::dto::GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<s3s::dto::GetPublicAccessBlockOutput>> {
        self.inner.get_public_access_block(req).await
    }
    async fn head_bucket(
        &self,
        req: S3Request<s3s::dto::HeadBucketInput>,
    ) -> S3Result<S3Response<s3s::dto::HeadBucketOutput>> {
        self.inner.head_bucket(req).await
    }
    async fn list_bucket_analytics_configurations(
        &self,
        req: S3Request<s3s::dto::ListBucketAnalyticsConfigurationsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketAnalyticsConfigurationsOutput>> {
        self.inner.list_bucket_analytics_configurations(req).await
    }
    async fn list_bucket_intelligent_tiering_configurations(
        &self,
        req: S3Request<s3s::dto::ListBucketIntelligentTieringConfigurationsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketIntelligentTieringConfigurationsOutput>> {
        self.inner
            .list_bucket_intelligent_tiering_configurations(req)
            .await
    }
    async fn list_bucket_inventory_configurations(
        &self,
        req: S3Request<s3s::dto::ListBucketInventoryConfigurationsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketInventoryConfigurationsOutput>> {
        self.inner.list_bucket_inventory_configurations(req).await
    }
    async fn list_bucket_metrics_configurations(
        &self,
        req: S3Request<s3s::dto::ListBucketMetricsConfigurationsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketMetricsConfigurationsOutput>> {
        self.inner.list_bucket_metrics_configurations(req).await
    }
    async fn list_buckets(
        &self,
        req: S3Request<s3s::dto::ListBucketsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketsOutput>> {
        self.inner.list_buckets(req).await
    }
    async fn list_directory_buckets(
        &self,
        req: S3Request<s3s::dto::ListDirectoryBucketsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListDirectoryBucketsOutput>> {
        self.inner.list_directory_buckets(req).await
    }
    async fn list_multipart_uploads(
        &self,
        req: S3Request<s3s::dto::ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListMultipartUploadsOutput>> {
        self.inner.list_multipart_uploads(req).await
    }
    async fn list_object_versions(
        &self,
        req: S3Request<s3s::dto::ListObjectVersionsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListObjectVersionsOutput>> {
        self.inner.list_object_versions(req).await
    }
    async fn list_objects(
        &self,
        req: S3Request<s3s::dto::ListObjectsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListObjectsOutput>> {
        self.inner.list_objects(req).await
    }
    async fn list_parts(
        &self,
        req: S3Request<s3s::dto::ListPartsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListPartsOutput>> {
        self.inner.list_parts(req).await
    }
    async fn put_bucket_accelerate_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketAccelerateConfigurationOutput>> {
        self.inner.put_bucket_accelerate_configuration(req).await
    }
    async fn put_bucket_acl(
        &self,
        req: S3Request<s3s::dto::PutBucketAclInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketAclOutput>> {
        self.inner.put_bucket_acl(req).await
    }
    async fn put_bucket_analytics_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketAnalyticsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketAnalyticsConfigurationOutput>> {
        self.inner.put_bucket_analytics_configuration(req).await
    }
    async fn put_bucket_cors(
        &self,
        req: S3Request<s3s::dto::PutBucketCorsInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketCorsOutput>> {
        self.inner.put_bucket_cors(req).await
    }
    async fn put_bucket_encryption(
        &self,
        req: S3Request<s3s::dto::PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketEncryptionOutput>> {
        self.inner.put_bucket_encryption(req).await
    }
    async fn put_bucket_intelligent_tiering_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketIntelligentTieringConfigurationOutput>> {
        self.inner
            .put_bucket_intelligent_tiering_configuration(req)
            .await
    }
    async fn put_bucket_inventory_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketInventoryConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketInventoryConfigurationOutput>> {
        self.inner.put_bucket_inventory_configuration(req).await
    }
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketLifecycleConfigurationOutput>> {
        self.inner.put_bucket_lifecycle_configuration(req).await
    }
    async fn put_bucket_logging(
        &self,
        req: S3Request<s3s::dto::PutBucketLoggingInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketLoggingOutput>> {
        self.inner.put_bucket_logging(req).await
    }
    async fn put_bucket_metrics_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketMetricsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketMetricsConfigurationOutput>> {
        self.inner.put_bucket_metrics_configuration(req).await
    }
    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketNotificationConfigurationOutput>> {
        self.inner.put_bucket_notification_configuration(req).await
    }
    async fn put_bucket_ownership_controls(
        &self,
        req: S3Request<s3s::dto::PutBucketOwnershipControlsInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketOwnershipControlsOutput>> {
        self.inner.put_bucket_ownership_controls(req).await
    }
    async fn put_bucket_policy(
        &self,
        req: S3Request<s3s::dto::PutBucketPolicyInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketPolicyOutput>> {
        self.inner.put_bucket_policy(req).await
    }
    async fn put_bucket_replication(
        &self,
        req: S3Request<s3s::dto::PutBucketReplicationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketReplicationOutput>> {
        self.inner.put_bucket_replication(req).await
    }
    async fn put_bucket_request_payment(
        &self,
        req: S3Request<s3s::dto::PutBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketRequestPaymentOutput>> {
        self.inner.put_bucket_request_payment(req).await
    }
    async fn put_bucket_tagging(
        &self,
        req: S3Request<s3s::dto::PutBucketTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketTaggingOutput>> {
        self.inner.put_bucket_tagging(req).await
    }
    async fn put_bucket_versioning(
        &self,
        req: S3Request<s3s::dto::PutBucketVersioningInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketVersioningOutput>> {
        self.inner.put_bucket_versioning(req).await
    }
    async fn put_bucket_website(
        &self,
        req: S3Request<s3s::dto::PutBucketWebsiteInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketWebsiteOutput>> {
        self.inner.put_bucket_website(req).await
    }
    async fn put_object_acl(
        &self,
        req: S3Request<s3s::dto::PutObjectAclInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectAclOutput>> {
        self.inner.put_object_acl(req).await
    }
    async fn put_object_legal_hold(
        &self,
        req: S3Request<s3s::dto::PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectLegalHoldOutput>> {
        self.inner.put_object_legal_hold(req).await
    }
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<s3s::dto::PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectLockConfigurationOutput>> {
        self.inner.put_object_lock_configuration(req).await
    }
    async fn put_object_retention(
        &self,
        req: S3Request<s3s::dto::PutObjectRetentionInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectRetentionOutput>> {
        self.inner.put_object_retention(req).await
    }
    async fn put_object_tagging(
        &self,
        req: S3Request<s3s::dto::PutObjectTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectTaggingOutput>> {
        self.inner.put_object_tagging(req).await
    }
    async fn put_public_access_block(
        &self,
        req: S3Request<s3s::dto::PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<s3s::dto::PutPublicAccessBlockOutput>> {
        self.inner.put_public_access_block(req).await
    }
    async fn restore_object(
        &self,
        req: S3Request<s3s::dto::RestoreObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::RestoreObjectOutput>> {
        self.inner.restore_object(req).await
    }
    async fn select_object_content(
        &self,
        req: S3Request<s3s::dto::SelectObjectContentInput>,
    ) -> S3Result<S3Response<s3s::dto::SelectObjectContentOutput>> {
        self.inner.select_object_content(req).await
    }
    async fn upload_part(
        &self,
        req: S3Request<s3s::dto::UploadPartInput>,
    ) -> S3Result<S3Response<s3s::dto::UploadPartOutput>> {
        self.inner.upload_part(req).await
    }
    async fn upload_part_copy(
        &self,
        req: S3Request<s3s::dto::UploadPartCopyInput>,
    ) -> S3Result<S3Response<s3s::dto::UploadPartCopyOutput>> {
        self.inner.upload_part_copy(req).await
    }
    async fn write_get_object_response(
        &self,
        req: S3Request<s3s::dto::WriteGetObjectResponseInput>,
    ) -> S3Result<S3Response<s3s::dto::WriteGetObjectResponseOutput>> {
        self.inner.write_get_object_response(req).await
    }
}
