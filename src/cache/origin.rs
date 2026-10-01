//! Origin request accounting: every HTTP attempt the upstream client sends is
//! counted by billing class and by the outcomes that cost money without moving data.
//!
//! Counting happens in the AWS SDK's interceptor chain, below every forwarding path
//! (passthrough, fills, metadata probes, index scans, recovery), so no call site can
//! reach the origin uncounted. Each attempt that received an HTTP response counts once —
//! retries included, because the origin bills each of them — whatever its status.

use std::sync::Arc;

use aws_sdk_s3::config::interceptors::FinalizerInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use aws_smithy_runtime_api::client::orchestrator::Metadata;
use http::StatusCode;

use crate::metrics::Metrics;

/// R2/S3 request-pricing class of one S3 operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OriginClass {
    /// Mutating and listing operations: `PutObject`, `CopyObject`, the multipart
    /// create/upload/complete family, every `List*`, and bucket `Put*`/`Create*`.
    A,
    /// Reads: `GetObject`, `HeadObject`, `HeadBucket` and every other `Get*`/`Head*`.
    B,
    /// Unbilled: `DeleteObject`, `DeleteObjects`, `DeleteBucket` (and every other
    /// `Delete*`) and `AbortMultipartUpload`.
    Free,
}

impl OriginClass {
    /// Classify an S3 operation by its SDK operation name.
    pub(crate) fn of(operation: &str) -> Self {
        if operation.starts_with("Get") || operation.starts_with("Head") {
            Self::B
        } else if operation.starts_with("Delete") || operation == "AbortMultipartUpload" {
            Self::Free
        } else {
            Self::A
        }
    }
}

/// Whether a refused attempt of `operation` was a write the origin declined.
fn is_object_write(operation: &str) -> bool {
    matches!(
        operation,
        "PutObject" | "CopyObject" | "CompleteMultipartUpload" | "UploadPart" | "UploadPartCopy"
    )
}

/// Counts each origin attempt into [`Metrics`]; installed by [`counted_client`].
struct OriginCounter(Arc<Metrics>);

impl std::fmt::Debug for OriginCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginCounter")
    }
}

impl Intercept for OriginCounter {
    fn name(&self) -> &'static str {
        "OriginCounter"
    }

    fn read_after_attempt(
        &self,
        context: &FinalizerInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // No response: the attempt never reached the origin (or its answer was lost in
        // transit), so there is nothing the origin could have billed us to observe.
        let Some(response) = context.response() else {
            return Ok(());
        };
        let operation = cfg.load::<Metadata>().map_or("", Metadata::name);
        let status = response.status().as_u16();
        self.0.record_origin(operation, status);
        Ok(())
    }
}

impl Metrics {
    /// Account one origin attempt of `operation` answered with HTTP `status`.
    pub(crate) fn record_origin(&self, operation: &str, status: u16) {
        match OriginClass::of(operation) {
            OriginClass::A => self.origin_class_a_request(),
            OriginClass::B => self.origin_class_b_request(),
            OriginClass::Free => self.origin_free_request(),
        }
        if status < 400 {
            return;
        }
        match operation {
            "GetObject" if status == StatusCode::NOT_FOUND.as_u16() => {
                self.origin_get_not_found();
            }
            "HeadObject" if status == StatusCode::NOT_FOUND.as_u16() => {
                self.origin_head_not_found();
            }
            op if is_object_write(op) => {
                if status == StatusCode::PRECONDITION_FAILED.as_u16() {
                    self.origin_write_precondition_failed();
                } else {
                    self.origin_write_refused();
                }
            }
            _ => {}
        }
    }
}

/// `client` with origin request accounting into `metrics` installed.
pub(super) fn counted_client(
    client: &aws_sdk_s3::Client,
    metrics: Arc<Metrics>,
) -> aws_sdk_s3::Client {
    let config = client
        .config()
        .to_builder()
        .interceptor(OriginCounter(metrics))
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

#[cfg(test)]
mod tests {
    use super::OriginClass;
    use crate::metrics::Metrics;

    #[test]
    fn operations_fall_in_their_billing_class() {
        for op in [
            "PutObject",
            "CopyObject",
            "CreateMultipartUpload",
            "UploadPart",
            "CompleteMultipartUpload",
            "ListObjectsV2",
            "ListBuckets",
        ] {
            assert_eq!(OriginClass::of(op), OriginClass::A, "{op}");
        }
        for op in ["GetObject", "HeadObject", "HeadBucket", "GetBucketLocation"] {
            assert_eq!(OriginClass::of(op), OriginClass::B, "{op}");
        }
        for op in ["DeleteObject", "DeleteObjects", "AbortMultipartUpload"] {
            assert_eq!(OriginClass::of(op), OriginClass::Free, "{op}");
        }
    }

    #[test]
    fn only_object_reads_and_writes_count_their_refusals() {
        let metrics = Metrics::default();
        metrics.record_origin("ListObjectsV2", 404);
        metrics.record_origin("CopyObject", 412);
        metrics.record_origin("PutObject", 503);
        metrics.record_origin("GetObject", 304);
        let text = metrics.prometheus_text();
        for (name, value) in [
            ("origin_class_a_requests", 3),
            ("origin_class_b_requests", 1),
            ("origin_get_not_found", 0),
            ("origin_head_not_found", 0),
            ("origin_write_precondition_failed", 1),
            ("origin_write_refused", 1),
        ] {
            assert!(
                text.contains(&format!("s3cache_{name} {value}\n")),
                "{name}: {text}"
            );
        }
    }
}
