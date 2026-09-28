use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;

use super::{PutVerdict, SlotStore, StoreError};

/// Conditional exact-key control storage in a bucket separate from user data.
#[derive(Clone, Debug)]
pub struct S3SlotStore {
    client: Client,
    bucket: String,
    max_object_bytes: usize,
}

impl S3SlotStore {
    /// Construct a store with a hard limit on each object read or write.
    ///
    /// # Errors
    /// Returns an error for an empty control bucket or zero byte limit.
    pub fn new(
        client: Client,
        control_bucket: impl Into<String>,
        max_object_bytes: usize,
    ) -> Result<Self, StoreError> {
        let bucket = control_bucket.into();
        if bucket.is_empty() || max_object_bytes == 0 {
            return Err(StoreError("invalid control bucket or object limit".into()));
        }
        Ok(Self {
            client,
            bucket,
            max_object_bytes,
        })
    }
}

#[async_trait]
impl SlotStore for S3SlotStore {
    async fn put_if_absent(&self, key: &str, bytes: Vec<u8>) -> Result<PutVerdict, StoreError> {
        if bytes.len() > self.max_object_bytes {
            return Err(StoreError("control object exceeds configured limit".into()));
        }
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(bytes))
            .send()
            .await;
        match result {
            Ok(_) => Ok(PutVerdict::Created),
            Err(error) => {
                let code = error
                    .as_service_error()
                    .and_then(|service| service.meta().code());
                if code == Some("PreconditionFailed") {
                    Ok(PutVerdict::Occupied)
                } else {
                    // Transport failures, 409s, and 5xx responses cannot prove
                    // non-commit. Even other service errors are conservatively
                    // read back at this exact slot before any progress.
                    Ok(PutVerdict::Unknown)
                }
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        let mut body = match response {
            Ok(response) => {
                if response.content_length().is_some_and(|length| {
                    usize::try_from(length).map_or(true, |size| size > self.max_object_bytes)
                }) {
                    return Err(StoreError("control Content-Length outside limit".into()));
                }
                response.body
            }
            Err(error) => {
                let code = error
                    .as_service_error()
                    .and_then(|service| service.meta().code());
                if matches!(code, Some("NoSuchKey" | "NotFound")) {
                    return Ok(None);
                }
                return Err(StoreError(format!("control GET failed: {error}")));
            }
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| StoreError(format!("control GET body: {error}")))?;
            if chunk.len() > self.max_object_bytes.saturating_sub(bytes.len()) {
                return Err(StoreError("control object exceeds configured limit".into()));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Some(bytes))
    }
}
