//! Process-fresh, nonzero worker identity from the operating-system CSPRNG.

use groupnet::core::volatile_bootstrap::BootId;

use super::config::FleetConfigError;

/// Draw a distinct 128-bit token for each fleet worker construction. The
/// collision risk is the CSPRNG's 128-bit birthday bound, not a timestamp or
/// process-local counter that repeats after restart.
pub(crate) fn fresh_boot() -> Result<BootId, FleetConfigError> {
    for _ in 0..2 {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|_| FleetConfigError::Entropy)?;
        if let Some(identity) = from_bytes(bytes) {
            return Ok(identity);
        }
    }
    Err(FleetConfigError::Entropy)
}

fn from_bytes(bytes: [u8; 16]) -> Option<BootId> {
    let token = u128::from_le_bytes(bytes);
    (token != 0).then_some(BootId(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_never_an_identity_and_full_width_is_retained() {
        assert_eq!(from_bytes([0; 16]), None);
        let bytes = [0x7f; 16];
        assert_eq!(from_bytes(bytes), Some(BootId(u128::from_le_bytes(bytes))));
    }
}
