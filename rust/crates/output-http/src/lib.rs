//! Native output contracts ported from Dispatcharr apps/output/views.py.
//! All original application authors retain full credit; see ATTRIBUTION.md.
//! Prototype only: no application route or output parity is accepted yet.
mod csrf;
mod direct_profiles;
pub mod epg_custom;
pub mod epg_custom_scope;
pub mod epg_dummy;
pub mod epg_grid;
mod epg_grid_http;
pub mod epg_names;
pub use epg_grid_http::{GridGateway, GridReadLimits};
pub mod epg_window;
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
    dispatcharr_contracts::source_output_origin(request, auth.is_trusted_proxy(request.peer_ip))
}

/// Decode a public M3U or XMLTV profile suffix, preserving query delimiters.
pub fn source_export_profile(raw: &str) -> Option<String> {
    let mut profile = percent_path(raw.trim_end_matches('/'));
    // Django's once-decoded route allows a single optional trailing slash
    // after a profile. Keep the existing scope for other encoded separators.
    if !raw.ends_with('/') {
        if let Some(name) = profile.strip_suffix('/') {
            if !name.is_empty() && !name.contains('/') {
                profile.truncate(name.len());
            }
        }
    }
    (!profile.is_empty()).then_some(profile)
}

fn percent_path(value: &str) -> String {
    // Decode URI path bytes once; query-form separators and '+' are literal.
    let input = value.as_bytes();
    let mut output = Vec::with_capacity(input.len());
    let mut offset = 0;
    while offset < input.len() {
        if input[offset] == b'%' && offset + 2 < input.len() {
            let hex = |byte: u8| (byte as char).to_digit(16);
            if let Some((high, low)) = hex(input[offset + 1]).zip(hex(input[offset + 2])) {
                output.push((high * 16 + low) as u8);
                offset += 3;
                continue;
            }
        }
        output.push(input[offset]);
        offset += 1;
    }
    String::from_utf8_lossy(&output).into_owned()
}

#[cfg(test)]
mod profile_tests {
    use super::source_export_profile;

    #[test]
    fn named_export_profiles_accept_a_single_decoded_trailing_delimiter() {
        for path in [
            "Caf%C3%A9%20%26%20News%2B",
            "Caf%C3%A9%20%26%20News%2B/",
            "Caf%C3%A9%20%26%20News%2B%2F",
            "Caf%C3%A9%20%26%20News%2B%2f",
        ] {
            assert_eq!(source_export_profile(path).as_deref(), Some("Café & News+"));
        }
        assert_eq!(
            source_export_profile("Literal%252FName").as_deref(),
            Some("Literal%2FName")
        );
        assert_eq!(
            source_export_profile("Name%252F").as_deref(),
            Some("Name%2F")
        );
        assert_eq!(
            source_export_profile("Name%2FThere%2F").as_deref(),
            Some("Name/There/")
        );
        assert_eq!(source_export_profile("Name%2F/").as_deref(), Some("Name/"));
        assert_eq!(source_export_profile("%2F").as_deref(), Some("/"));
        assert!(source_export_profile("/").is_none());
    }
}

/// Original output CSRF failure page, if the shared check rejects a request.
/// This does not perform query authentication or enable an additional route.
pub fn source_csrf_failure(
    method: &axum::http::Method,
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> Option<&'static str> {
    csrf::check(method, headers, body)
        .err()
        .map(|error| error.body())
}

#[cfg(test)]
mod test_proxy;
