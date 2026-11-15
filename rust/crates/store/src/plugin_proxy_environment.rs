//! Requests2.34.2 / CPython3.13 environment selection for a SINGLE current hop.
//! Original Requests/CPython authors retain all algorithm credit/licenses.
//! A real selected proxy remains unsupported; this grants only Source direct IO.
use super::Failure;
use std::{collections::BTreeMap, net::Ipv4Addr};
#[derive(Default)]
struct Environment {
    pairs: Vec<(String, String)>,
}
impl Environment {
    fn current() -> Result<Self, Failure> {
        let mut pairs = Vec::new();
        let mut bytes = 0usize;
        for (key, value) in std::env::vars_os() {
            let Some(key) = key.to_str() else { continue };
            if key != "REQUEST_METHOD" && !key.to_ascii_lowercase().ends_with("_proxy") {
                continue;
            }
            let value = value.to_str().ok_or(Failure::Unqualified(
                "Non-Unicode proxy environment remains unqualified",
            ))?;
            bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
            if pairs.len() >= 512 || bytes > 262144 {
                return Err(Failure::Unqualified(
                    "Proxy environment capacity unavailable",
                ));
            }
            pairs.push((key.to_owned(), value.to_owned()));
        }
        Ok(Self { pairs })
    }
    fn exact(&self, key: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
    fn proxies(&self) -> BTreeMap<String, String> {
        let mut proxies = BTreeMap::new();
        for (name, value) in &self.pairs {
            if name.len() > 5 && name.to_ascii_lowercase().ends_with("_proxy") && !value.is_empty()
            {
                proxies.insert(name[..name.len() - 6].to_lowercase(), value.clone());
            }
        }
        if self.exact("REQUEST_METHOD").is_some() {
            proxies.remove("http");
        }
        for (name, value) in &self.pairs {
            if name.ends_with("_proxy") {
                let key = name[..name.len() - 6].to_lowercase();
                if value.is_empty() {
                    proxies.remove(&key);
                } else {
                    proxies.insert(key, value.clone());
                }
            }
        }
        proxies
    }
    fn bypass(
        &self,
        url: &url::Url,
        port: Option<u16>,
        proxies: &BTreeMap<String, String>,
    ) -> bool {
        let Some(raw_host) = url.host_str() else {
            return true;
        };
        let host = raw_host.trim_matches(['[', ']']);
        let no_proxy = self
            .exact("no_proxy")
            .filter(|v| !v.is_empty())
            .or_else(|| self.exact("NO_PROXY"));
        if let Some(value) = no_proxy.filter(|v| !v.is_empty()) {
            let stripped = value.replace(' ', "");
            let values = stripped.split(',').filter(|v| !v.is_empty());
            if let Some(ip) = inet_aton(host) {
                for token in values {
                    if let Some((network, prefix)) = cidr(token) {
                        let mask = u32::MAX << (32 - prefix);
                        if u32::from(ip) & mask == u32::from(network) & mask {
                            return true;
                        }
                    } else if token == host {
                        return true;
                    }
                }
            } else {
                // Requests urlparse preserves an explicit default port. url::Url erases
                // it; caller supplies this separately through Hop when required.
                let with_port = match port.or(url.port()) {
                    Some(port) if port != 0 => format!("{host}:{port}"),
                    _ => host.to_owned(),
                };
                for token in values {
                    let token = token.trim_start_matches('.');
                    if host == token || with_port == token {
                        return true;
                    }
                    let suffix = format!(".{token}");
                    if host.ends_with(&suffix) || with_port.ends_with(&suffix) {
                        return true;
                    }
                }
            }
        }
        // Requests' fallback calls Linux urllib.proxy_bypass(hostname), not URL+port.
        let Some(no_proxy) = proxies.get("no") else {
            return false;
        };
        if no_proxy == "*" {
            return true;
        }
        let host = host.to_lowercase();
        let hostonly = splitport(&host);
        for token in no_proxy.split(',') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            let token = token.trim_start_matches('.').to_lowercase();
            if hostonly == token || host == token {
                return true;
            }
            let suffix = format!(".{token}");
            if hostonly.ends_with(&suffix) || host.ends_with(&suffix) {
                return true;
            }
        }
        false
    }
    fn selected(&self, url: &url::Url, port: Option<u16>) -> Option<String> {
        let proxies = self.proxies();
        if self.bypass(url, port, &proxies) {
            return None;
        }
        if let Some(host) = url.host_str() {
            let host = host.trim_matches(['[', ']']);
            for key in [
                format!("{}://{host}", url.scheme()),
                url.scheme().to_owned(),
                format!("all://{host}"),
                "all".into(),
            ] {
                if let Some(value) = proxies.get(&key) {
                    return Some(value.clone());
                }
            }
            None
        } else {
            proxies
                .get(url.scheme())
                .or_else(|| proxies.get("all"))
                .cloned()
        }
    }
}
pub(super) fn redirect_port(previous: Option<u16>, location: &str) -> Result<Option<u16>, Failure> {
    if location.starts_with("//") || location.contains("://") {
        let authority = location
            .split_once("://")
            .map(|(_, tail)| tail)
            .or_else(|| location.strip_prefix("//"))
            .unwrap_or("");
        if authority
            .split(['/', '?', '#'])
            .next()
            .is_none_or(str::is_empty)
        {
            return Err(Failure::Unqualified(
                "Empty-authority redirect identity remains unqualified",
            ));
        }
        Ok(explicit_port(location))
    } else {
        Ok(previous)
    }
}
pub(super) fn explicit_port(raw: &str) -> Option<u16> {
    let authority = if let Some((_, tail)) = raw.split_once("://") {
        tail
    } else {
        raw.strip_prefix("//")?
    };
    let authority = authority
        .split(['/', '?', '#'])
        .next()?
        .rsplit('@')
        .next()?;
    let port = if authority.starts_with('[') {
        authority.split_once(']')?.1.strip_prefix(':')?
    } else {
        authority.rsplit_once(':')?.1
    };
    if port.is_empty() || !port.bytes().all(|v| v.is_ascii_digit()) {
        return None;
    }
    port.parse().ok()
}
fn splitport(host: &str) -> &str {
    match host.rsplit_once(':') {
        Some((name, port)) if !port.is_empty() && port.bytes().all(|v| v.is_ascii_digit()) => name,
        _ => host,
    }
}
fn cidr(raw: &str) -> Option<(Ipv4Addr, u32)> {
    let (network, prefix) = raw.split_once('/')?;
    if prefix.contains('/') {
        return None;
    }
    let prefix = dispatcharr_ingest_core::source_integer_literal(prefix)?
        .parse::<u32>()
        .ok()?;
    if !(1..=32).contains(&prefix) {
        return None;
    }
    Some((inet_aton(network)?, prefix))
}
/// Linux inet_aton numeric forms used by pinned Requests' IPv4/CIDR checks.
/// No DNS or global environment mutation occurs here.
fn inet_aton(raw: &str) -> Option<Ipv4Addr> {
    let text = raw.split(|c: char| c.is_ascii_whitespace()).next()?;
    if text.is_empty() {
        return None;
    }
    let pieces = text.split('.').collect::<Vec<_>>();
    if pieces.is_empty() || pieces.len() > 4 {
        return None;
    }
    let mut values = Vec::new();
    for piece in pieces {
        if piece.is_empty() {
            return None;
        }
        let (base, digits) = if piece.starts_with("0x") || piece.starts_with("0X") {
            (16, &piece[2..])
        } else if piece.len() > 1 && piece.starts_with('0') {
            (8, &piece[1..])
        } else {
            (10, piece)
        };
        values.push(u32::from_str_radix(digits, base).ok()?);
    }
    let last = *values.last()?;
    let rest = values.len() - 1;
    if values[..rest].iter().any(|v| *v > 255) {
        return None;
    }
    let bits = 8 * (4 - rest);
    if bits < 32 && last >= (1u32 << bits) {
        return None;
    }
    let mut value = last;
    for (index, part) in values[..rest].iter().enumerate() {
        value |= part << (8 * (3 - index));
    }
    Some(Ipv4Addr::from(value))
}
pub(super) fn enforce(url: &url::Url, port: Option<u16>) -> Result<(), Failure> {
    if Environment::current()?.selected(url, port).is_some() {
        Err(Failure::Unqualified(
            "Proxy-selected manifest transport remains unqualified",
        ))
    } else {
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    #[test]
    fn pinned_requests_and_urllib_proxy_vectors_select_same_direct_or_proxy_hop() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-plugin-proxy-vectors.json"))
                .unwrap();
        for row in source["cases"].as_array().unwrap() {
            let pairs: Vec<(String, String)> =
                serde_json::from_value(row["environment"].clone()).unwrap();
            let environment = Environment { pairs };
            let parsed = url::Url::parse(row["url"].as_str().unwrap()).unwrap();
            let actual = environment.selected(&parsed, explicit_port(row["url"].as_str().unwrap()));
            assert_eq!(
                serde_json::json!(actual),
                row["selected"],
                "{}",
                row["name"]
            );
        }
    }
    #[test]
    fn explicit_default_ports_and_redirect_identity_are_not_invented_or_erased() {
        let environment = Environment {
            pairs: vec![
                ("ALL_PROXY".into(), "http://proxy.invalid:9".into()),
                ("NO_PROXY".into(), "example.com:80,example.com:443".into()),
            ],
        };
        for (raw, bypass) in [
            ("http://example.com/a", false),
            ("http://example.com:80/a", true),
            ("https://example.com/a", false),
            ("https://example.com:443/a", true),
        ] {
            assert_eq!(
                environment
                    .selected(&url::Url::parse(raw).unwrap(), explicit_port(raw))
                    .is_none(),
                bypass,
                "{raw}"
            );
        }
        assert_eq!(redirect_port(Some(80), "../next").unwrap(), Some(80));
        assert_eq!(redirect_port(Some(80), "/next").unwrap(), Some(80));
        assert_eq!(redirect_port(Some(80), "?query").unwrap(), Some(80));
        assert_eq!(redirect_port(Some(80), "//example.com/next").unwrap(), None);
        assert_eq!(
            redirect_port(None, "https://example.com:443/next").unwrap(),
            Some(443)
        );
        assert_eq!(
            redirect_port(Some(80), "http://example.com/next").unwrap(),
            None
        );
        assert!(redirect_port(Some(80), "http:///next").is_err());
    }
    #[test]
    fn each_redirect_target_is_reselected_instead_of_reusing_first_bypass() {
        let environment = Environment {
            pairs: vec![
                ("HTTP_PROXY".into(), "http://proxy.invalid:9".into()),
                ("NO_PROXY".into(), "11.254.252.3".into()),
            ],
        };
        assert!(environment
            .selected(
                &url::Url::parse("http://11.254.252.3:8080/start").unwrap(),
                None
            )
            .is_none());
        assert!(environment
            .selected(
                &url::Url::parse("http://11.254.252.4:8080/next").unwrap(),
                None
            )
            .is_some());
        assert!(environment
            .selected(
                &url::Url::parse("http://11.254.252.3:8080/end").unwrap(),
                None
            )
            .is_none());
    }
}
