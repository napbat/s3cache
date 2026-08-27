//! Destination-conditional `CopyObject` forwarding and copy metadata shortcuts.

use std::sync::Arc;

use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use http::header::{IF_MATCH, IF_NONE_MATCH};
use http::{HeaderName, HeaderValue};
use s3s::dto::{CopyObjectInput, CopyObjectOutput, CopySource, ETag};
use s3s::{S3, S3Request, S3Response, S3Result};

use crate::cache::proxy::CachingProxy;

const R2_IF_MATCH: HeaderName = HeaderName::from_static("cf-copy-destination-if-match");
const R2_IF_NONE_MATCH: HeaderName = HeaderName::from_static("cf-copy-destination-if-none-match");

#[derive(Clone)]
struct DestinationConditions {
    if_match: Option<HeaderValue>,
    if_none_match: Option<HeaderValue>,
}

impl DestinationConditions {
    fn from_request(req: &S3Request<CopyObjectInput>) -> Option<Self> {
        let conditions = Self {
            if_match: req.headers.get(IF_MATCH).cloned(),
            if_none_match: req.headers.get(IF_NONE_MATCH).cloned(),
        };
        (conditions.if_match.is_some() || conditions.if_none_match.is_some()).then_some(conditions)
    }
}

/// Whether this copy is the create-only form used for immutable destinations.
/// A 412 from the origin then proves the destination exists, even if this
/// proxy's LIST index has not observed it yet.
pub(super) fn destination_must_be_absent(req: &S3Request<CopyObjectInput>) -> bool {
    req.headers
        .get(IF_NONE_MATCH)
        .is_some_and(|value| value == HeaderValue::from_static("*"))
}

tokio::task_local! {
    static DESTINATION_CONDITIONS: DestinationConditions;
}

/// `s3s` 0.14 predates destination conditions on its `CopyObjectInput`, so its
/// AWS adapter cannot carry those fields. This interceptor restores the original
/// headers after serialization and before `SigV4` signing. R2 names destination
/// copy conditions with `cf-copy-destination-*`; the standard headers stay too
/// so AWS S3 and `MinIO` retain the same semantics.
#[derive(Debug)]
struct CopyConditionInterceptor;

impl Intercept for CopyConditionInterceptor {
    fn name(&self) -> &'static str {
        "CopyConditionInterceptor"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let Ok(conditions) = DESTINATION_CONDITIONS.try_with(Clone::clone) else {
            return Ok(());
        };
        let headers = context.request_mut().headers_mut();
        if let Some(value) = conditions.if_match {
            headers.insert(IF_MATCH, value.clone());
            headers.insert(R2_IF_MATCH, value);
        }
        if let Some(value) = conditions.if_none_match {
            headers.insert(IF_NONE_MATCH, value.clone());
            headers.insert(R2_IF_NONE_MATCH, value);
        }
        Ok(())
    }
}

pub(super) fn conditioned_client(client: &aws_sdk_s3::Client) -> aws_sdk_s3::Client {
    let config = client
        .config()
        .to_builder()
        .interceptor(CopyConditionInterceptor)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

pub(super) async fn forward(
    inner: &Arc<s3s_aws::Proxy>,
    req: S3Request<CopyObjectInput>,
) -> S3Result<S3Response<CopyObjectOutput>> {
    let Some(conditions) = DestinationConditions::from_request(&req) else {
        return inner.copy_object(req).await;
    };
    DESTINATION_CONDITIONS
        .scope(conditions, inner.copy_object(req))
        .await
}

/// Reuse a source size only when the copy result names the exact `ETag` held by
/// the index. Metadata is deliberately not reused: rewriting identical bytes can
/// keep an `ETag` while changing headers, but it cannot change the byte length.
pub(super) fn indexed_source_size(
    proxy: &CachingProxy,
    source: &CopySource,
    copied_etag: &ETag,
) -> Option<i64> {
    let CopySource::Bucket {
        bucket,
        key,
        version_id: None,
    } = source
    else {
        return None;
    };
    let state = proxy.state.read().unwrap();
    let bucket = state.get(bucket.as_ref())?;
    if !bucket.synced || bucket.uncertain_keys.contains_key(key.as_ref()) {
        return None;
    }
    let entry = bucket.keys.get(key.as_ref())?;
    (entry.etag.as_ref() == Some(copied_etag))
        .then_some(entry.size)
        .flatten()
}
