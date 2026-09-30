//! Optional peer bootstrap application facts and production TCP wiring.

mod boot;
/// Explicit, bounded fleet listener and peer-address configuration.
pub mod config;

pub(crate) use boot::fresh_boot;
