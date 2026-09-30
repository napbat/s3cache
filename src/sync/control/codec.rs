use serde::Serialize;
use serde::de::DeserializeOwned;

use super::journal::JournalError;
use crate::codec;

const MAGIC: &[u8; 4] = b"S3CJ";
/// Current record format: a postcard payload.
const VERSION: u8 = 2;
/// The first format: a bincode 1 payload, still read so existing journals reopen.
const LEGACY_VERSION: u8 = 1;
const HEADER: usize = 9;
const DIGEST: usize = 32;

pub(super) fn encode<T: Serialize>(value: &T, max_bytes: usize) -> Result<Vec<u8>, JournalError> {
    let payload =
        codec::to_vec(value).map_err(|_| JournalError::Corrupt("record cannot encode"))?;
    let total = HEADER
        .checked_add(payload.len())
        .and_then(|size| size.checked_add(DIGEST))
        .ok_or(JournalError::Capacity)?;
    if total > max_bytes {
        return Err(JournalError::Capacity);
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| JournalError::Capacity)?;
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(MAGIC);
    bytes.push(VERSION);
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload);
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    Ok(bytes)
}

/// Whether `bytes` carry the format [`encode`] writes today. Immutable records in
/// the first format stay valid, but can never be byte-identical to a fresh encoding.
pub(super) fn is_current(bytes: &[u8]) -> bool {
    bytes.get(..MAGIC.len()) == Some(MAGIC) && bytes.get(MAGIC.len()) == Some(&VERSION)
}

pub(super) fn decode<T: DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<T, JournalError> {
    if bytes.len() > max_bytes || bytes.len() < HEADER + DIGEST {
        return Err(JournalError::Corrupt("record length outside limit"));
    }
    let version = bytes[4];
    if &bytes[..4] != MAGIC || (version != VERSION && version != LEGACY_VERSION) {
        return Err(JournalError::Corrupt("record format mismatch"));
    }
    let len = u32::from_le_bytes(bytes[5..9].try_into().expect("fixed header")) as usize;
    if len.checked_add(HEADER + DIGEST) != Some(bytes.len()) {
        return Err(JournalError::Corrupt("record length mismatch"));
    }
    let (contents, checksum) = bytes.split_at(HEADER + len);
    if blake3::hash(contents).as_bytes() != checksum {
        return Err(JournalError::Corrupt("record checksum mismatch"));
    }
    let payload = &contents[HEADER..];
    if version == VERSION {
        codec::from_slice(payload).map_err(|_| JournalError::Corrupt("record payload invalid"))
    } else {
        codec::legacy::from_slice(payload)
            .map_err(|_| JournalError::Corrupt("record payload invalid"))
    }
}
