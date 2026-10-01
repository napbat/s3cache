//! The integration suite, linked as one test binary so a library change relinks
//! once rather than once per suite. Each module is a former `tests/*.rs` target;
//! select one with a name filter, e.g. `cargo test --test it -- tier_cache::`.

#[macro_use]
mod common;

mod coherence;
mod control_source;
mod differential;
mod e2e;
mod fleet_bootstrap;
mod fleet_production;
mod idle_origin;
mod metrics_endpoint;
mod no_control_writes;
mod origin_requests;
mod readiness_probe;
mod startup;
mod startup_readiness;
mod strong_rw;
mod tier_cache;
