//! Transparent, S3-compatible caching proxy. Binds an S3 API, forwards to an upstream
//! S3 (e.g. R2), and layers LIST-from-index + a hot/warm/cold body cache on top. This is
//! the library: the proxy lives in [`cache`], with the LIST index in [`index`], the
//! object-body tiers in [`tier`], cross-node coherence (the gossip write feed) in
//! [`sync`], and counters in [`metrics`]. The `S3CACHE_*` environment they are built from
//! is parsed in [`config`], leaving the `s3cache` binary as the entry point that reads it
//! and runs the server.

pub mod cache;
pub mod config;
pub mod index;
pub(crate) mod list_token;
pub mod metrics;
pub mod sync;
pub mod tier;
