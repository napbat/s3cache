//! Caching layer over the upstream `s3s_aws::Proxy`.
//!
//! Every client request funnels through this proxy, so it sees every write. That lets
//! it answer **LIST** and **HEAD** from an in-memory key index (LISTs are R2's expensive
//! Class-A tier, and a HEAD-per-key existence probe its most voluminous Class-B one) and
//! serve small GET/HEAD bodies from an LRU, while writes forward through to the upstream
//! — which stays the authority for conditional (OCC) writes.
//!
//! Correctness rests on one property: this proxy is the *only* path to the bucket. The
//! index warms lazily — LISTs pass through until a bucket's background full-LIST sync
//! completes, then observed writes keep it current. The body cache is separately lazy:
//! populate on miss, invalidate on write. Cross-node coherence rides the gossip write
//! feed (see [`crate::sync`]): peers' writes fold into the index and invalidate local
//! copies; strict reads barrier on feed heads.

/// Cache configuration, proxy state, and core tier/index behavior.
pub mod proxy;
mod service;

#[cfg(test)]
mod tests;
