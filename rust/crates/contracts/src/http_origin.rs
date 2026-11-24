//! Original Dispatcharr output origin rules from core.utils.
//! Upstream application authors retain full credit.
use crate::PlaybackRequest;

/// Build an output URL origin using forwarding only when caller has already
/// admitted the request's socket peer through the configured proxy policy.
pub fn source_output_origin(request: &PlaybackRequest, trusted_forwarded: bool) -> String {
    let forwarded = |name| {
        trusted_forwarded
            .then(|| request.header(name))
            .flatten()
            .filter(|value| !value.is_empty())
    };
    let proto = forwarded("x-forwarded-proto");
    let scheme = proto
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("http");
    let standard = if scheme == "https" { "443" } else { "80" };
    let combine = |host: &str, port: &str| {
        if port.is_empty() || port == standard {
            format!("{scheme}://{host}")
        } else {
            format!("{scheme}://{host}:{port}")
        }
    };
    let forwarded_port = forwarded("x-forwarded-port");
    if let Some(host) = forwarded("x-forwarded-host") {
        if let Some((host, port)) = host.split_once(':') {
            return combine(host, port);
        }
        if let Some(port) = forwarded_port {
            return combine(host, port);
        }
        if proto.is_some() {
            return combine(host, "");
        }
    }
    let host = request.header("host").unwrap_or("localhost");
    if let Some((host, port)) = host.split_once(':') {
        return combine(host, port);
    }
    combine(host, forwarded_port.unwrap_or(""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_forwarded_fields_fall_back_without_producing_invalid_origin() {
        for (host, forwarded_host, forwarded_port, proto, expected) in [
            ("own.test", "", "8443", "https", "https://own.test:8443"),
            ("own.test", "", "", "https", "https://own.test"),
            (
                "own.test:8080",
                "public.test",
                "",
                "",
                "http://own.test:8080",
            ),
            (
                "own.test:8080",
                "public.test",
                "",
                "https",
                "https://public.test",
            ),
            (
                "own.test:8080",
                "public.test:",
                "8443",
                "https",
                "https://public.test",
            ),
            ("own.test", "", "", "", "http://own.test"),
        ] {
            let request = request(host, forwarded_host, forwarded_port, proto);
            assert_eq!(source_output_origin(&request, true), expected);
        }
    }

    #[test]
    fn ordinary_ports_and_configured_proxy_trust_stay_on_existing_rules() {
        for (host, forwarded_host, forwarded_port, proto, expected) in [
            (
                "own.test",
                "public.test:443",
                "8443",
                "https",
                "https://public.test",
            ),
            (
                "own.test:8080",
                "public.test:9443",
                "8443",
                "https",
                "https://public.test:9443",
            ),
            (
                "own.test:8080",
                "public.test",
                "8443",
                "https, http",
                "https://public.test:8443",
            ),
            (
                "own.test:8080",
                "public.test",
                "80",
                "http",
                "http://public.test",
            ),
        ] {
            let request = request(host, forwarded_host, forwarded_port, proto);
            assert_eq!(source_output_origin(&request, true), expected);
            assert_eq!(
                source_output_origin(&request, false),
                format!("http://{host}")
            );
        }
    }

    fn request(
        host: &str,
        forwarded_host: &str,
        forwarded_port: &str,
        proto: &str,
    ) -> PlaybackRequest {
        PlaybackRequest {
            channel_id: String::new(),
            query: vec![],
            headers: [
                ("host", host),
                ("x-forwarded-host", forwarded_host),
                ("x-forwarded-port", forwarded_port),
                ("x-forwarded-proto", proto),
            ]
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect(),
            peer_ip: "127.0.0.1".parse().unwrap(),
        }
    }
}
