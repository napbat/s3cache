//! Portable continuation tokens for LIST pages served from the node-local index.
//!
//! Origin continuation tokens are opaque to this proxy and always stay on the origin
//! route. Tokens minted here use a reserved, versioned namespace and carry the exact
//! request shape plus the last underlying key consumed by the index. That cursor can be
//! resumed by any replica, or translated to S3's exclusive `start-after` when local
//! service is unavailable.

use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bincode::Options as _;
use s3s::dto::{ListObjectsV2Input, ListObjectsV2Output};
use serde::{Deserialize, Serialize};

const OWNED_NAMESPACE: &str = "s3cache:list-token:";
const V1_PREFIX: &str = "s3cache:list-token:v1:";
const CHECKSUM_BYTES: usize = 16;
const MAX_PAYLOAD_BYTES: u64 = 16 * 1024;

#[derive(Serialize, Deserialize)]
struct Payload {
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    cursor: String,
}

/// Whose continuation token a request carries.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Continuation {
    Absent,
    /// An opaque origin token, which must bypass the local index unchanged.
    Origin,
    /// A validated token minted by any s3cache replica.
    Local {
        token: String,
        cursor: String,
    },
}

/// Why a token in s3cache's reserved namespace cannot be used.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TokenError {
    Malformed,
    RequestMismatch,
}

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => f.write_str("malformed s3cache continuation token"),
            Self::RequestMismatch => {
                f.write_str("s3cache continuation token does not match this LIST request")
            }
        }
    }
}

/// Classify and, for an owned token, validate the token against the request shape.
pub(crate) fn classify(input: &ListObjectsV2Input) -> Result<Continuation, TokenError> {
    let Some(token) = input.continuation_token.as_deref() else {
        return Ok(Continuation::Absent);
    };
    if !token.starts_with(OWNED_NAMESPACE) {
        return Ok(Continuation::Origin);
    }
    let encoded = token.strip_prefix(V1_PREFIX).ok_or(TokenError::Malformed)?;
    let framed = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| TokenError::Malformed)?;
    if framed.len() <= CHECKSUM_BYTES
        || u64::try_from(framed.len()).unwrap_or(u64::MAX)
            > MAX_PAYLOAD_BYTES + CHECKSUM_BYTES as u64
    {
        return Err(TokenError::Malformed);
    }
    let payload_len = framed.len() - CHECKSUM_BYTES;
    let (payload, checksum) = framed.split_at(payload_len);
    let digest = blake3::hash(payload);
    if checksum != &digest.as_bytes()[..CHECKSUM_BYTES] {
        return Err(TokenError::Malformed);
    }
    let decoded: Payload = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_PAYLOAD_BYTES)
        .reject_trailing_bytes()
        .deserialize(payload)
        .map_err(|_| TokenError::Malformed)?;
    if decoded.bucket != input.bucket
        || decoded.prefix != input.prefix
        || decoded.delimiter != input.delimiter
    {
        return Err(TokenError::RequestMismatch);
    }
    Ok(Continuation::Local {
        token: token.to_owned(),
        cursor: decoded.cursor,
    })
}

/// Mint a deterministic, replica-portable token whose cursor is exclusive.
pub(crate) fn encode(input: &ListObjectsV2Input, cursor: &str) -> String {
    let payload = Payload {
        bucket: input.bucket.clone(),
        prefix: input.prefix.clone(),
        delimiter: input.delimiter.clone(),
        cursor: cursor.to_owned(),
    };
    let bytes = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize(&payload)
        .expect("serializing a LIST cursor made only of strings cannot fail");
    let digest = blake3::hash(&bytes);
    let mut framed = Vec::with_capacity(bytes.len() + CHECKSUM_BYTES);
    framed.extend_from_slice(&bytes);
    framed.extend_from_slice(&digest.as_bytes()[..CHECKSUM_BYTES]);
    format!("{V1_PREFIX}{}", URL_SAFE_NO_PAD.encode(framed))
}

/// The client-visible fields hidden while an owned cursor is translated to an origin
/// `start-after` request.
pub(crate) struct OriginEnvelope {
    continuation_token: String,
    start_after: Option<String>,
}

impl OriginEnvelope {
    /// Restore the original request envelope while leaving the origin's next token
    /// untouched, so subsequent pages stay origin-routed.
    pub(crate) fn restore(self, output: &mut ListObjectsV2Output) {
        output.continuation_token = Some(self.continuation_token);
        output.start_after = self.start_after;
    }
}

/// Rewrite an owned exclusive cursor into the equivalent origin request.
pub(crate) fn rewrite_for_origin(
    input: &mut ListObjectsV2Input,
    token: String,
    cursor: String,
) -> OriginEnvelope {
    let start_after = input.start_after.replace(cursor);
    input.continuation_token = None;
    OriginEnvelope {
        continuation_token: token,
        start_after,
    }
}

#[cfg(test)]
mod tests {
    use s3s::dto::{ListObjectsV2Input, ListObjectsV2Output};

    use super::{Continuation, TokenError, classify, encode, rewrite_for_origin};

    fn input(bucket: &str, prefix: Option<&str>, delimiter: Option<&str>) -> ListObjectsV2Input {
        ListObjectsV2Input {
            bucket: bucket.to_owned(),
            prefix: prefix.map(str::to_owned),
            delimiter: delimiter.map(str::to_owned),
            ..Default::default()
        }
    }

    #[test]
    fn owned_token_round_trips_utf8_and_is_deterministic() {
        let shape = input("bucket-水", Some("路径/"), Some("‣"));
        let cursor = "路径/🦀/文件";
        let first = encode(&shape, cursor);
        let second = encode(&shape, cursor);
        assert_eq!(first, second);

        let mut resumed = shape;
        resumed.continuation_token = Some(first.clone());
        assert_eq!(
            classify(&resumed),
            Ok(Continuation::Local {
                token: first,
                cursor: cursor.to_owned(),
            })
        );
    }

    #[test]
    fn origin_token_is_never_mistaken_for_an_owned_token() {
        let mut request = input("b", None, None);
        request.continuation_token = Some("opaque-origin-value+/=".to_owned());
        assert_eq!(classify(&request), Ok(Continuation::Origin));
    }

    #[test]
    fn malformed_owned_token_is_rejected() {
        let mut request = input("b", None, None);
        request.continuation_token = Some("s3cache:list-token:v1:not_base64!".to_owned());
        assert_eq!(classify(&request), Err(TokenError::Malformed));

        request.continuation_token = Some("s3cache:list-token:v2:anything".to_owned());
        assert_eq!(classify(&request), Err(TokenError::Malformed));
    }

    #[test]
    fn owned_token_is_bound_to_bucket_prefix_and_delimiter() {
        let shape = input("b", Some("p/"), Some("/"));
        let token = encode(&shape, "p/last");
        for mut changed in [
            input("other", Some("p/"), Some("/")),
            input("b", Some("q/"), Some("/")),
            input("b", Some("p/"), Some("-")),
        ] {
            changed.continuation_token = Some(token.clone());
            assert_eq!(classify(&changed), Err(TokenError::RequestMismatch));
        }
    }

    #[test]
    fn origin_rewrite_is_exclusive_and_restores_the_client_envelope() {
        let mut request = input("b", Some("p/"), None);
        request.continuation_token = Some("owned".to_owned());
        request.start_after = Some("client-value".to_owned());
        let envelope = rewrite_for_origin(&mut request, "owned".to_owned(), "p/b".to_owned());
        assert!(request.continuation_token.is_none());
        assert_eq!(request.start_after.as_deref(), Some("p/b"));

        let mut output = ListObjectsV2Output {
            continuation_token: None,
            next_continuation_token: Some("origin-next".to_owned()),
            start_after: Some("p/b".to_owned()),
            ..Default::default()
        };
        envelope.restore(&mut output);
        assert_eq!(output.continuation_token.as_deref(), Some("owned"));
        assert_eq!(output.start_after.as_deref(), Some("client-value"));
        assert_eq!(
            output.next_continuation_token.as_deref(),
            Some("origin-next")
        );
    }
}
