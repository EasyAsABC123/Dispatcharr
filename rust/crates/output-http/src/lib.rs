//! Native output contracts ported from Dispatcharr apps/output/views.py.
//! All original application authors retain full credit; see ATTRIBUTION.md.
//! Prototype only: no application route or output parity is accepted yet.
mod csrf;
mod gateway;
pub mod reader;
pub mod render;
mod string_cache;
pub use gateway::{OutputConfig, OutputGateway};

/// Reuse the original output endpoint's trusted-forwarding URL rules.
/// Account/server-info scheme handling is a different Source contract.
pub fn source_output_origin(
    auth: &dispatcharr_store::AuthService,
    request: &dispatcharr_contracts::PlaybackRequest,
) -> String {
    gateway::origin(auth, request, &axum::http::HeaderMap::new())
}

/// Original output CSRF failure page, if the shared check rejects a request.
/// This does not perform query authentication or enable an additional route.
pub fn source_csrf_failure(
    method: &axum::http::Method,
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> Option<&'static str> {
    csrf::check(method, headers, body).err().map(|error| error.body())
}

#[cfg(test)]
mod test_proxy;
