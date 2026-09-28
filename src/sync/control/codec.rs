use bincode::Options;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::journal::JournalError;

const MAGIC: &[u8; 4] = b"S3CJ";
const VERSION: u8 = 1;
const HEADER: usize = 9;
const DIGEST: usize = 32;

pub(super) fn encode<T: Serialize>(value: &T, max_bytes: usize) -> Result<Vec<u8>, JournalError> {
    let payload_len = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialized_size(value)
        .map_err(|_| JournalError::Corrupt("record size cannot encode"))?;
    if payload_len > max_bytes.saturating_sub(HEADER + DIGEST) as u64
        || payload_len > u64::from(u32::MAX)
    {
        return Err(JournalError::Capacity);
    }
    let payload = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize(value)
        .map_err(|_| JournalError::Corrupt("record cannot encode"))?;
    let total = HEADER
        .checked_add(payload.len())
        .and_then(|size| size.checked_add(DIGEST))
        .ok_or(JournalError::Capacity)?;
    if total > max_bytes || payload.len() > u32::MAX as usize {
        return Err(JournalError::Capacity);
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(MAGIC);
    bytes.push(VERSION);
    bytes.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("checked length")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&payload);
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    Ok(bytes)
}

pub(super) fn decode<T: DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<T, JournalError> {
    if bytes.len() > max_bytes || bytes.len() < HEADER + DIGEST {
        return Err(JournalError::Corrupt("record length outside limit"));
    }
    if &bytes[..4] != MAGIC || bytes[4] != VERSION {
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
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(u64::try_from(len).expect("bounded record length"))
        .reject_trailing_bytes()
        .deserialize(&bytes[HEADER..HEADER + len])
        .map_err(|_| JournalError::Corrupt("record payload invalid"))
}
