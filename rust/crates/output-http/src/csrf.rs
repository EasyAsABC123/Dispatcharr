//! CSRF compatibility adapted from Django6.0.9 middleware, BSD-3-Clause.
//! Original Django authors/Foundation retain credit; see data/Django-LICENSE.
//! Full renderer/middleware parity remains gated by additional Source cases.
use axum::http::{HeaderMap, Method};
use subtle::ConstantTimeEq;
const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
pub(crate) enum Failure {
    NoCookie,
    Invalid,
}
impl Failure {
    pub(crate) fn body(&self) -> &'static str {
        match self {
            Self::NoCookie => include_str!("../data/django-csrf-no-cookie.html"),
            Self::Invalid => include_str!("../data/django-csrf-invalid-token.html"),
        }
    }
}
fn header<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key).and_then(|value| value.to_str().ok())
}
fn secret(value: &str) -> Option<Vec<u8>> {
    if !matches!(value.len(), 32 | 64) || !value.bytes().all(|byte| CHARS.contains(&byte)) {
        return None;
    }
    if value.len() == 32 {
        return Some(value.as_bytes().to_vec());
    }
    Some(
        value.as_bytes()[32..]
            .iter()
            .zip(value.as_bytes()[..32].iter())
            .map(|(cipher, mask)| {
                let cipher = CHARS.iter().position(|byte| byte == cipher).unwrap();
                let mask = CHARS.iter().position(|byte| byte == mask).unwrap();
                CHARS[(cipher + 62 - mask) % 62]
            })
            .collect(),
    )
}
pub(crate) fn check(method: &Method, headers: &HeaderMap, body: &[u8]) -> Result<(), Failure> {
    if matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    ) {
        return Ok(());
    }
    // Django's configured SECURE_PROXY_SSL_HEADER applies before the endpoint's
    // separately trusted-forwarding origin builder. Preserve that distinction.
    let secure = header(headers, "x-forwarded-proto")
        .and_then(|value| value.split(',').next())
        .is_some_and(|value| value.trim() == "https");
    let host = header(headers, "host").unwrap_or("localhost");
    if let Some(origin) = header(headers, "origin") {
        if origin != format!("{}://{host}", if secure { "https" } else { "http" }) {
            return Err(Failure::Invalid);
        }
    } else if secure {
        let referer = header(headers, "referer").ok_or(Failure::Invalid)?;
        let parsed = url::Url::parse(referer).map_err(|_| Failure::Invalid)?;
        let authority = referer
            .strip_prefix("https://")
            .and_then(|value| value.split(['/', '?', '#']).next())
            .ok_or(Failure::Invalid)?;
        if parsed.scheme() != "https" || parsed.host_str().is_none() || authority != host {
            return Err(Failure::Invalid);
        }
    }
    let cookie = header(headers, "cookie")
        .and_then(|value| {
            value
                .split(';')
                .filter_map(|part| part.trim().split_once('='))
                .find(|(key, _)| *key == "csrftoken")
                .map(|(_, value)| value.trim().trim_matches('"'))
        })
        .ok_or(Failure::NoCookie)?;
    let cookie = secret(cookie).ok_or(Failure::Invalid)?;
    let form = if *method == Method::POST
        && header(headers, "content-type")
            .is_some_and(|value| value.starts_with("application/x-www-form-urlencoded"))
    {
        url::form_urlencoded::parse(body)
            .filter(|(key, _)| key == "csrfmiddlewaretoken")
            .map(|(_, value)| value.into_owned())
            .last()
    } else {
        None
    };
    let token = form
        .as_deref()
        .filter(|value| !value.is_empty())
        .or_else(|| header(headers, "x-csrftoken"))
        .ok_or(Failure::Invalid)?;
    let token = secret(token).ok_or(Failure::Invalid)?;
    if bool::from(cookie.ct_eq(&token)) {
        Ok(())
    } else {
        Err(Failure::Invalid)
    }
}
