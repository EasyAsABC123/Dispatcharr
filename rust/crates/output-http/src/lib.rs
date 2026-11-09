//! Native output contracts ported from Dispatcharr apps/output/views.py.
//! All original application authors retain full credit; see ATTRIBUTION.md.
//! Prototype only: no application route or output parity is accepted yet.
mod csrf;
mod gateway;
pub mod reader;
pub mod render;
mod string_cache;
pub use gateway::{OutputConfig, OutputGateway};

#[cfg(test)]
mod test_proxy;
