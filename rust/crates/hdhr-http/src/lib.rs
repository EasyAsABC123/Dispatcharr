//! HDHomeRun compatibility adapter for original Dispatcharr HTTP endpoints.
//! Original application credit belongs to Dispatcharr's authors and contributors.
//! See ATTRIBUTION.md and the repository AGPL-3.0 license. UDP discovery is separate.
mod devices;
mod gateway;
mod read;
pub use gateway::{HdhrConfig, HdhrGateway};
