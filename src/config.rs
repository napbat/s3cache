//! Runtime configuration: the `S3CACHE_*` environment, read once at startup.
//!
//! The variable names and defaults here are the contract with the Helm chart
//! (`deploy/helm/s3cache`) — change them in both places or not at all. Malformed numbers
//! fall back to their defaults rather than refusing to start: a typo must not take the
//! proxy down, and the value it lands on is the documented one. The gossip knobs are
//! read separately, where the transport that needs them is built (see
//! [`crate::sync::from_env`]).

use std::path::PathBuf;
use std::str::FromStr;

use crate::cache::CacheConfig;

/// Bind address for the S3 API when `S3CACHE_LISTEN` is unset.
const DEFAULT_LISTEN: &str = "0.0.0.0:8014";
/// Hot (in-memory) tier capacity when `S3CACHE_CACHE_BYTES` is unset: 256 MiB.
const DEFAULT_CACHE_BYTES: u64 = 268_435_456;
/// Per-object cache cap when `S3CACHE_MAX_OBJECT_BYTES` is unset: 8 MiB.
const DEFAULT_MAX_OBJECT_BYTES: usize = 8_388_608;
/// Warm (disk) tier budget when `S3CACHE_DISK_CACHE_BYTES` is unset: 10 GiB.
const DEFAULT_DISK_CACHE_BYTES: u64 = 10_737_418_240;
/// Stats log interval when `S3CACHE_STATS_SECS` is unset.
const DEFAULT_STATS_SECS: u64 = 60;
/// Node name when `HOSTNAME` is unset (single-node: nobody gossips with it).
const DEFAULT_NODE_NAME: &str = "s3cache";

/// Everything the binary reads from the environment.
pub struct Config {
    /// Bind address for the S3 API (`S3CACHE_LISTEN`).
    pub listen: String,
    /// Upstream S3/R2 endpoint URL (`S3CACHE_UPSTREAM_ENDPOINT`; required).
    pub endpoint: String,
    /// Buckets to index eagerly (`S3CACHE_BUCKETS`, comma-separated).
    pub buckets: Vec<String>,
    /// Hot-tier capacity and the per-object cap (`S3CACHE_CACHE_BYTES`,
    /// `S3CACHE_MAX_OBJECT_BYTES`).
    pub cache: CacheConfig,
    /// Directory for the warm (disk) tier, `None` when disabled (`S3CACHE_DISK_CACHE`).
    pub disk_path: Option<PathBuf>,
    /// Warm tier byte budget (`S3CACHE_DISK_CACHE_BYTES`).
    pub disk_bytes: u64,
    /// This node's name in the gossip cluster (`HOSTNAME`).
    pub node_name: String,
    /// Stats log interval in seconds (`S3CACHE_STATS_SECS`).
    pub stats_secs: u64,
    /// Bind address for the Prometheus text endpoint, `None` when disabled
    /// (`S3CACHE_METRICS_LISTEN`).
    pub metrics_listen: Option<String>,
}

impl Config {
    /// Read the whole `S3CACHE_*` environment.
    ///
    /// # Panics
    ///
    /// If `S3CACHE_UPSTREAM_ENDPOINT` is unset: there is no sane default for the
    /// upstream, and a proxy with nothing to proxy to is worse than a refusal to start.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            listen: env_or(var("S3CACHE_LISTEN"), DEFAULT_LISTEN),
            endpoint: std::env::var("S3CACHE_UPSTREAM_ENDPOINT")
                .expect("S3CACHE_UPSTREAM_ENDPOINT is required (the upstream S3/R2 endpoint URL)"),
            buckets: parse_list(&var("S3CACHE_BUCKETS").unwrap_or_default()),
            cache: CacheConfig {
                cache_bytes: parse_or(var("S3CACHE_CACHE_BYTES"), DEFAULT_CACHE_BYTES),
                max_obj_bytes: parse_or(var("S3CACHE_MAX_OBJECT_BYTES"), DEFAULT_MAX_OBJECT_BYTES),
            },
            disk_path: var("S3CACHE_DISK_CACHE").map(PathBuf::from),
            disk_bytes: parse_or(var("S3CACHE_DISK_CACHE_BYTES"), DEFAULT_DISK_CACHE_BYTES),
            node_name: env_or(var("HOSTNAME"), DEFAULT_NODE_NAME),
            stats_secs: parse_or(var("S3CACHE_STATS_SECS"), DEFAULT_STATS_SECS),
            metrics_listen: var("S3CACHE_METRICS_LISTEN"),
        }
    }
}

/// An environment variable, treating "set but empty" as unset — a Helm value that
/// renders to `""` (an unset optional knob) must read as absent, not as an empty path.
fn var(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

/// `raw`, or `default` when it is absent.
fn env_or(raw: Option<String>, default: &str) -> String {
    raw.unwrap_or_else(|| default.to_owned())
}

/// `raw` parsed, or `default` when it is absent or malformed.
fn parse_or<T: FromStr>(raw: Option<String>, default: T) -> T {
    raw.and_then(|value| value.parse().ok()).unwrap_or(default)
}

/// Split a comma-separated list, dropping blanks — so `"a,,b, "` is `["a", "b"]` and an
/// empty setting is no entries at all (not one empty one).
fn parse_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{env_or, parse_list, parse_or};

    #[test]
    fn lists_drop_blanks_and_whitespace() {
        assert_eq!(parse_list("a,b"), ["a", "b"]);
        assert_eq!(parse_list(" a , b ,"), ["a", "b"]);
        assert!(parse_list("").is_empty(), "an unset list is no entries");
        assert!(parse_list(",  ,").is_empty());
    }

    #[test]
    fn numbers_fall_back_on_absent_or_malformed_values() {
        assert_eq!(parse_or(Some("42".to_owned()), 7_u64), 42);
        assert_eq!(parse_or(None, 7_u64), 7, "unset falls back");
        assert_eq!(
            parse_or(Some("8 MiB".to_owned()), 7_u64),
            7,
            "a typo falls back"
        );
        assert_eq!(
            parse_or(Some("-1".to_owned()), 7_usize),
            7,
            "so does a bad sign"
        );
    }

    #[test]
    fn strings_fall_back_only_when_absent() {
        assert_eq!(env_or(Some("host:1".to_owned()), "0.0.0.0:8014"), "host:1");
        assert_eq!(env_or(None, "0.0.0.0:8014"), "0.0.0.0:8014");
    }
}
