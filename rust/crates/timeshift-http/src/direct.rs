//! Direct catch-up redirects ported from original Dispatcharr Timeshift views.
//! All original application credit belongs to upstream authors/contributors.
use axum::{body::Body, response::Response};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::StoreError;

#[cfg(test)]
pub(crate) fn mint_redirect(
    channel: uuid::Uuid,
    request: &PlaybackRequest,
) -> Result<Response, StoreError> {
    mint_redirect_path(&format!("/proxy/catchup/{channel}"), request)
}

pub(crate) fn mint_redirect_path(
    path: &str,
    request: &PlaybackRequest,
) -> Result<Response, StoreError> {
    // Django QueryDict groups repeated keys at the first occurrence. Replacing
    // an existing empty/foreign SID keeps that key's original position.
    let sid = URL_SAFE_NO_PAD.encode(uuid::Uuid::new_v4().as_bytes());
    let mut grouped: Vec<(&str, Vec<&str>)> = Vec::new();
    for (key, value) in &request.query {
        if let Some((_, values)) = grouped.iter_mut().find(|(name, _)| *name == key) {
            values.push(value);
        } else {
            grouped.push((key, vec![value]));
        }
    }
    if let Some((_, values)) = grouped.iter_mut().find(|(key, _)| *key == "session_id") {
        *values = vec![sid.as_str()];
    } else {
        grouped.push(("session_id", vec![sid.as_str()]));
    }
    let mut encoded = url::form_urlencoded::Serializer::new(String::new());
    for (key, values) in grouped {
        for value in values {
            encoded.append_pair(key, value);
        }
    }
    Response::builder()
        .status(301)
        .header("location", format!("{path}?{}", encoded.finish()))
        .header("cache-control", "private, max-age=21600")
        .header("content-type", "text/html; charset=utf-8")
        .header("content-length", "0")
        .header("allow", "GET, OPTIONS")
        .header("x-content-type-options", "nosniff")
        .body(Body::empty())
        .map_err(|_| StoreError::Unsupported("invalid catch-up redirect header"))
}

pub(crate) fn provider_redirect(location: &str) -> Result<Response, StoreError> {
    // Django's HttpResponseRedirect applies iri_to_uri to Location.
    let mut uri = String::new();
    for byte in location.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~/#%[]=:;$&()+,!?*@'".contains(&byte) {
            uri.push(byte as char);
        } else {
            use std::fmt::Write;
            let _ = write!(uri, "%{byte:02X}");
        }
    }
    if url::Url::parse(&uri)
        .ok()
        .is_some_and(|url| !matches!(url.scheme(), "http" | "https" | "ftp"))
    {
        return Err(StoreError::Unsupported(
            "provider redirect scheme is not supported",
        ));
    }
    Response::builder()
        .status(302)
        .header("location", uri)
        .header("content-type", "text/html; charset=utf-8")
        .header("content-length", "0")
        .header("allow", "GET, OPTIONS")
        .header("x-content-type-options", "nosniff")
        .body(Body::empty())
        .map_err(|_| StoreError::Unsupported("invalid provider redirect header"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mint_preserves_django_repeated_key_grouping_and_replaces_sid_in_place() {
        let channel = uuid::Uuid::nil();
        let request = PlaybackRequest {
            channel_id: channel.to_string(),
            peer_ip: "127.0.0.1".parse().unwrap(),
            headers: vec![],
            query: vec![
                ("start".into(), "2026-11-09T11:00:00Z".into()),
                ("foo".into(), "one".into()),
                ("session_id".into(), "old".into()),
                ("blank".into(), "".into()),
                ("foo".into(), "two".into()),
            ],
        };
        let response = mint_redirect(channel, &request).unwrap();
        assert_eq!(response.status(), 301);
        assert_eq!(
            response.headers()["cache-control"],
            "private, max-age=21600"
        );
        let location = response.headers()["location"].to_str().unwrap();
        let pairs: Vec<_> =
            url::form_urlencoded::parse(location.split_once('?').unwrap().1.as_bytes())
                .into_owned()
                .collect();
        assert_eq!(
            pairs
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            ["start", "foo", "foo", "session_id", "blank"]
        );
        assert_eq!(pairs[3].1.len(), 22);
        assert_ne!(pairs[3].1, "old");
    }
    #[test]
    fn redirect_retains_original_timestamp_and_escapes_iri_without_double_encoding() {
        let response =
            provider_redirect("http://origin/archive/2026-11-09T11:00:00Z?x=%20&y=café name")
                .unwrap();
        assert_eq!(response.status(), 302);
        assert_eq!(
            response.headers()["location"],
            "http://origin/archive/2026-11-09T11:00:00Z?x=%20&y=caf%C3%A9%20name"
        );
        assert!(!response.headers().contains_key("cache-control"));
        assert!(provider_redirect("javascript:invalid").is_err());
    }
}
