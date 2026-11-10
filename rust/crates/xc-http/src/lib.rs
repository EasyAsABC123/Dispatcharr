//! Unwired XC read-output draft. No native route or parity acceptance yet.
//! Original Dispatcharr authors retain full application/algorithm credit.
//! See ATTRIBUTION.md and the repository AGPL-3.0 license.
pub mod counts;
pub mod epg;
pub mod epg_hash;
mod gateway;
pub mod numbers;
mod project;
mod reader;
pub use gateway::{LiveDispatcher, XcConfig, XcGateway};
