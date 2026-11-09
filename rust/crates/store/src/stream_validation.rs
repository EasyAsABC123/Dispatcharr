//! Stream field validation ported from original Dispatcharr and Django contracts.
//! Original Dispatcharr authors and contributors retain full application credit.
use std::net::Ipv4Addr;
use url::{Host, Url};
fn space(value: char) -> bool {
    value.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&value)
}
fn fallback(value: &str) -> bool {
    let Some((scheme, rest)) = value.split_once("://") else {
        return false;
    };
    if !["http", "https", "rtp", "rtsp", "udp"].contains(&scheme) {
        return false;
    }
    let rest = match rest.rsplit_once('@') {
        Some((credentials, tail))
            if credentials.split_once(':').is_some_and(|(user, password)| {
                !user.is_empty()
                    && user
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
                    && !password.is_empty()
                    && !password.chars().any(|value| space(value) || value == '@')
            }) =>
        {
            tail
        }
        _ => rest,
    };
    let authority = rest
        .split_once('/')
        .map_or(rest, |(authority, _)| authority);
    let host = if let Some((host, port)) = authority.rsplit_once(':') {
        if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) || host.contains(':')
        {
            return false;
        }
        host
    } else {
        authority
    };
    if host.is_empty() {
        return true;
    }
    if host
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return true;
    }
    let bytes = host.as_bytes();
    bytes.len() <= 63
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(byte))
}
fn standard(value: &str) -> bool {
    if value.chars().count() > 2048 {
        return false;
    }
    let Ok(url) = Url::parse(value) else {
        return false;
    };
    if !["http", "https", "ftp", "ftps"].contains(&url.scheme()) {
        return false;
    }
    let Some((_, rest)) = value.split_once("://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = if let Some((user, host)) = authority.rsplit_once('@') {
        let (username, password) = user
            .split_once(':')
            .map_or((user, None), |(username, password)| {
                (username, Some(password))
            });
        if username.is_empty()
            || username
                .chars()
                .any(|value| space(value) || ":@/".contains(value))
            || password.is_some_and(|password| {
                password
                    .chars()
                    .any(|value| space(value) || ":@/".contains(value))
            })
        {
            return false;
        }
        host
    } else {
        authority
    };
    if host.ends_with(':') {
        return false;
    }
    match url.host() {
        Some(Host::Ipv6(_)) => true,
        Some(Host::Ipv4(_)) => {
            let raw = host.split(':').next().unwrap_or("");
            raw.split('.').count() == 4
                && !raw
                    .split('.')
                    .any(|part| part.len() > 1 && part.starts_with('0'))
                && raw.parse::<Ipv4Addr>().is_ok()
        }
        Some(Host::Domain(domain)) => {
            if domain == "localhost" {
                return true;
            }
            let domain = domain.strip_suffix('.').unwrap_or(domain);
            if domain.len() > 253 {
                return false;
            }
            let labels = domain.split('.').collect::<Vec<_>>();
            if labels.len() < 2
                || labels.iter().any(|label| {
                    label.is_empty()
                        || label.len() > 63
                        || label.starts_with('-')
                        || label.ends_with('-')
                        || !label
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                })
            {
                return false;
            }
            let tld = labels.last().expect("domain labels");
            (tld.len() >= 2
                && tld
                    .bytes()
                    .all(|byte| byte.is_ascii_alphabetic() || byte == b'-'))
                || tld.strip_prefix("xn--").is_some_and(|value| {
                    !value.is_empty()
                        && value.len() <= 59
                        && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
                })
        }
        None => false,
    }
}
/// Match original flexible HTTP/RTSP/RTP/UDP fallback, including its permitted
/// empty host and unbounded numeric port forms. Validation never fetches a URL.
pub(crate) fn flexible_url(value: &str) -> bool {
    value.is_empty() || (!value.chars().any(space) && (standard(value) || fallback(value)))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_pinned_validator_vectors_include_nonstandard_accepted_forms() {
        for (value, expected) in [
            ("http://host/", true),
            ("http://host?x", false),
            ("HTTP://host/", false),
            ("HTTP://valid.example/path", true),
            ("http://", true),
            ("http:///path", true),
            ("http://.", true),
            ("udp://:1234", true),
            ("http://host:999999/path", true),
            ("rtsp://u:p@host/path", true),
            ("rtp://192.168.1.1", true),
            ("rtmp://host/path", false),
            ("ftp://host/path", false),
            ("ftp://valid.example/path", true),
            ("ftp://u:@valid.example/", true),
            ("http://[::1]/", true),
            ("rtsp://[::1]/", false),
            ("https://user@local/path", false),
            ("http://localhost?x", true),
            ("http://-bad/", false),
        ] {
            assert_eq!(flexible_url(value), expected, "{value}");
        }
    }
}
