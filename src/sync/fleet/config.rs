//! Explicit finite fleet address book; no gossip address is a serving proof.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::net::SocketAddr;

const MAX_PEERS: usize = 256;
const MAX_ID_BYTES: usize = 256;
const MAX_ADDRESS_BYTES: usize = 512;
const MAX_BUCKETS: usize = 4_096;
const MAX_SCOPE_BYTES: usize = 8_192;

/// TCP listener and complete peer book for peer index bootstrap. A process
/// without one runs ordinary UDP coherence on guarded origin recovery.
#[derive(Clone, Debug)]
pub struct FleetConfig {
    pub(crate) bind: SocketAddr,
    pub(crate) origin_id: String,
    pub(crate) origin_endpoint: String,
    pub(crate) origin_region: String,
    pub(crate) buckets: Vec<String>,
    pub(crate) peers: Vec<(String, String)>,
}

/// Why a supplied peer book declines peer transfer. The caller keeps
/// ordinary guarded origin recovery available.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetConfigError {
    Partial,
    InvalidBind,
    InvalidAddress,
    IncompleteBook,
    Capacity,
    BucketInventory,
    Entropy,
}

impl FleetConfig {
    /// Validate the deployment's peer book. Missing all four values leaves
    /// peer bootstrap unconfigured; a partial configuration fails visibly.
    ///
    /// # Errors
    /// Rejects a missing, duplicate, oversized, or incompatible peer book or
    /// bucket inventory. The caller may retain guarded origin recovery.
    #[expect(
        clippy::too_many_arguments,
        reason = "explicit origin identity and complete peer inventory are independently validated inputs"
    )]
    pub fn parse(
        bind: Option<String>,
        advertise: Option<String>,
        peers: Option<String>,
        origin_id: Option<String>,
        origin_endpoint: &str,
        origin_region: &str,
        node_id: &str,
        buckets: &[String],
    ) -> Result<Option<Self>, FleetConfigError> {
        let any = bind.is_some() || advertise.is_some() || peers.is_some() || origin_id.is_some();
        if !any {
            return Ok(None);
        }
        let (Some(bind), Some(advertise), Some(peers), Some(origin_id)) =
            (bind, advertise, peers, origin_id)
        else {
            return Err(FleetConfigError::Partial);
        };
        if origin_id.is_empty()
            || origin_id.len() > MAX_ADDRESS_BYTES
            || !safe_origin_endpoint(origin_endpoint)
            || origin_endpoint.len() > MAX_ADDRESS_BYTES
            || origin_region.is_empty()
            || origin_region.len() > MAX_ID_BYTES
        {
            return Err(FleetConfigError::InvalidAddress);
        }
        let bind = bind
            .parse::<SocketAddr>()
            .map_err(|_| FleetConfigError::InvalidBind)?;
        if bind.port() == 0 || !valid_address(&advertise) {
            return Err(FleetConfigError::InvalidAddress);
        }
        if buckets.is_empty() || buckets.len() > MAX_BUCKETS {
            return Err(FleetConfigError::BucketInventory);
        }
        let unique_buckets: BTreeSet<&str> = buckets.iter().map(String::as_str).collect();
        if unique_buckets.len() != buckets.len()
            || unique_buckets
                .iter()
                .any(|name| name.is_empty() || name.len() > MAX_ID_BYTES)
        {
            return Err(FleetConfigError::BucketInventory);
        }
        let mut parsed = Vec::new();
        let mut names = BTreeSet::new();
        for raw in peers.split(',') {
            if parsed.len() >= MAX_PEERS {
                return Err(FleetConfigError::Capacity);
            }
            let Some((name, address)) = raw.split_once('=') else {
                return Err(FleetConfigError::InvalidAddress);
            };
            if name.is_empty()
                || name.len() > MAX_ID_BYTES
                || name.contains(char::is_whitespace)
                || !valid_address(address)
                || !names.insert(name)
            {
                return Err(FleetConfigError::InvalidAddress);
            }
            parsed.push((name.to_owned(), address.to_owned()));
        }
        if parsed.is_empty()
            || !parsed
                .iter()
                .any(|(name, address)| name == node_id && address == &advertise)
        {
            return Err(FleetConfigError::IncompleteBook);
        }
        Ok(Some(Self {
            bind,
            origin_id,
            origin_endpoint: origin_endpoint.to_owned(),
            origin_region: origin_region.to_owned(),
            buckets: unique_buckets.into_iter().map(str::to_owned).collect(),
            peers: parsed,
        }))
    }

    /// Build an unambiguous whole-index scope from exact backend coordinates
    /// and a complete sorted configured bucket inventory.
    ///
    /// # Errors
    /// Rejects a changed or oversized bucket universe before allocating a
    /// claim key; the caller continues guarded origin recovery.
    pub(crate) fn scope(
        &self,
        buckets: &[String],
    ) -> Result<groupnet::core::volatile_bootstrap::BootstrapScope, FleetConfigError> {
        let mut sorted = buckets.to_vec();
        sorted.sort();
        if sorted != self.buckets
            || sorted.is_empty()
            || sorted.len() > MAX_BUCKETS
            || sorted.windows(2).any(|pair| pair[0] == pair[1])
            || sorted
                .iter()
                .any(|bucket| bucket.is_empty() || bucket.len() > MAX_ID_BYTES)
        {
            return Err(FleetConfigError::BucketInventory);
        }
        let mut domain = String::new();
        let mut partition = String::new();
        for component in [
            self.origin_id.as_str(),
            self.origin_endpoint.as_str(),
            self.origin_region.as_str(),
            "s3cache-index-v1",
        ] {
            append_component(&mut domain, component)?;
        }
        for bucket in &sorted {
            append_component(&mut partition, bucket)?;
        }
        if domain.len().saturating_add(partition.len()) > MAX_SCOPE_BYTES {
            return Err(FleetConfigError::Capacity);
        }
        Ok(groupnet::core::volatile_bootstrap::BootstrapScope { domain, partition })
    }
}

fn append_component(out: &mut String, value: &str) -> Result<(), FleetConfigError> {
    let next = out
        .len()
        .checked_add(value.len())
        .and_then(|len| len.checked_add(32))
        .ok_or(FleetConfigError::Capacity)?;
    if next > MAX_SCOPE_BYTES {
        return Err(FleetConfigError::Capacity);
    }
    write!(out, "{}#{value}", value.len()).map_err(|_| FleetConfigError::Capacity)
}

fn valid_address(value: &str) -> bool {
    let Some((host, port)) = value.rsplit_once(':') else {
        return false;
    };
    !host.is_empty()
        && !host.contains(char::is_whitespace)
        && value.len() <= MAX_ADDRESS_BYTES
        && port.parse::<u16>().is_ok_and(|port| port != 0)
}

fn safe_origin_endpoint(value: &str) -> bool {
    let Some((scheme, address)) = value.split_once("://") else {
        return false;
    };
    if !matches!(scheme, "http" | "https") || value.contains('?') || value.contains('#') {
        return false;
    }
    let authority = address.split('/').next().unwrap_or_default();
    !authority.is_empty() && !authority.contains('@') && !authority.contains(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled(buckets: &[String]) -> Result<Option<FleetConfig>, FleetConfigError> {
        FleetConfig::parse(
            Some("0.0.0.0:7101".to_owned()),
            Some("node-0.svc:7101".to_owned()),
            Some("node-0=node-0.svc:7101,node-1=node-1.svc:7101".to_owned()),
            Some("origin-account-a".to_owned()),
            "https://origin.example/a",
            "us-east-1",
            "node-0",
            buckets,
        )
    }

    #[test]
    fn default_off_and_explicit_inventory_required() {
        assert!(
            FleetConfig::parse(None, None, None, None, "https://origin", "r", "node-0", &[])
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            enabled(&[]),
            Err(FleetConfigError::BucketInventory)
        ));
        let configured = enabled(&["bucket".to_owned()]).unwrap().unwrap();
        assert_eq!(configured.peers.len(), 2);
        assert_eq!(configured.bind.port(), 7101);
    }

    #[test]
    fn incomplete_or_duplicate_book_declines_peer_bootstrap() {
        let buckets = ["bucket".to_owned()];
        assert!(matches!(
            FleetConfig::parse(
                Some("0.0.0.0:7101".to_owned()),
                None,
                None,
                None,
                "https://origin.example/a",
                "us-east-1",
                "node-0",
                &buckets
            ),
            Err(FleetConfigError::Partial)
        ));
        assert!(matches!(
            FleetConfig::parse(
                Some("0.0.0.0:7101".to_owned()),
                Some("node-0.svc:7101".to_owned()),
                Some("node-1=node-1.svc:7101".to_owned()),
                Some("origin-account-a".to_owned()),
                "https://origin.example/a",
                "us-east-1",
                "node-0",
                &buckets
            ),
            Err(FleetConfigError::IncompleteBook)
        ));
        assert!(matches!(
            FleetConfig::parse(
                Some("0.0.0.0:7101".to_owned()),
                Some("node-0.svc:7101".to_owned()),
                Some("node-0=node-0.svc:7101,node-0=node-1.svc:7101".to_owned()),
                Some("origin-account-a".to_owned()),
                "https://origin.example/a",
                "us-east-1",
                "node-0",
                &buckets
            ),
            Err(FleetConfigError::InvalidAddress)
        ));
    }

    #[test]
    fn scope_binds_exact_backend_and_bucket_universe() {
        let buckets = ["bucket".to_owned()];
        let first = enabled(&buckets).unwrap().unwrap();
        let other_endpoint = FleetConfig::parse(
            Some("0.0.0.0:7101".to_owned()),
            Some("node-0.svc:7101".to_owned()),
            Some("node-0=node-0.svc:7101,node-1=node-1.svc:7101".to_owned()),
            Some("origin-account-a".to_owned()),
            "https://different-origin.example/a",
            "us-east-1",
            "node-0",
            &buckets,
        )
        .unwrap()
        .unwrap();
        assert_ne!(
            first.scope(&buckets).unwrap(),
            other_endpoint.scope(&buckets).unwrap()
        );
        assert!(matches!(
            first.scope(&["bucket".to_owned(), "new-bucket".to_owned()]),
            Err(FleetConfigError::BucketInventory)
        ));
    }

    #[test]
    fn origin_scope_never_publishes_endpoint_credentials() {
        let buckets = ["bucket".to_owned()];
        for endpoint in [
            "https://user:secret@origin.example/a",
            "https://origin.example/a?token=secret",
            "https://origin.example/a#secret",
        ] {
            assert!(matches!(
                FleetConfig::parse(
                    Some("0.0.0.0:7101".to_owned()),
                    Some("node-0.svc:7101".to_owned()),
                    Some("node-0=node-0.svc:7101,node-1=node-1.svc:7101".to_owned()),
                    Some("origin-account-a".to_owned()),
                    endpoint,
                    "us-east-1",
                    "node-0",
                    &buckets,
                ),
                Err(FleetConfigError::InvalidAddress)
            ));
        }
    }
}
