//! The serde binary codec behind s3cache's own formats: warm-tier record headers,
//! control-journal records, LIST continuation tokens and the peer write feed.
//!
//! New bytes are [postcard](https://docs.rs/postcard) (a stable, specified wire
//! format). Bytes written by earlier releases were bincode 1 with fixed-width integers;
//! [`legacy`] reads those, so each durable format keeps accepting them.

use serde::{Deserialize, Serialize};

pub(crate) mod legacy;

/// Encodes one value in the current format.
pub(crate) fn to_vec<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_stdvec(value)
}

/// Decodes exactly one value in the current format; trailing bytes are an error, so
/// one input never has two readings.
pub(crate) fn from_slice<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, postcard::Error> {
    let (value, rest) = postcard::take_from_bytes(bytes)?;
    if rest.is_empty() {
        Ok(value)
    } else {
        Err(postcard::Error::DeserializeBadEncoding)
    }
}

#[cfg(test)]
mod tests {
    use super::{from_slice, to_vec};

    #[test]
    fn current_format_round_trips_and_rejects_trailing_bytes() {
        let value = (String::from("bucket"), Some(7_u64), vec![1_u8, 2, 3]);
        let mut bytes = to_vec(&value).unwrap();
        assert_eq!(
            from_slice::<(String, Option<u64>, Vec<u8>)>(&bytes).unwrap(),
            value
        );
        bytes.push(0);
        assert!(from_slice::<(String, Option<u64>, Vec<u8>)>(&bytes).is_err());
    }
}
