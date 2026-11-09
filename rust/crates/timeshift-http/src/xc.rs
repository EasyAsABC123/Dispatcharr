//! XC catch-up adapters ported from original Dispatcharr Timeshift views.
//! All original application credit belongs to upstream authors/contributors.
use crate::CatchupMedia;
use axum::{
    body::Body,
    extract::{ConnectInfo, OriginalUri, Path, State},
    http::Request,
    response::{IntoResponse, Response},
};
use dispatcharr_contracts::PlaybackRequest;
use serde_json::json;
use std::net::SocketAddr;

/// Intentionally no Debug/Serialize: both credential strings are secrets.
pub struct XcIntent {
    pub username: String,
    pub password: String,
    pub channel: String,
    pub timestamp: String,
    pub duration: Option<String>,
}
pub(crate) const NOT_FOUND:&str="\n<!doctype html>\n<html lang=\"en\">\n<head>\n  <title>Not Found</title>\n</head>\n<body>\n  <h1>Not Found</h1><p>The requested resource was not found on this server.</p>\n</body>\n</html>\n";

pub(crate) fn channel_pk(value: &str) -> Option<i64> {
    let value = value.strip_suffix(".ts").unwrap_or(value).trim();
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    if digits.is_empty()
        || digits.starts_with('_')
        || digits.ends_with('_')
        || digits.contains("__")
        || !digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'_')
    {
        return None;
    }
    value.replace('_', "").parse().ok()
}
fn identity(request: &Request<Body>) -> Option<PlaybackRequest> {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()?
        .0
        .ip();
    Some(PlaybackRequest {
        channel_id: String::new(),
        peer_ip: peer,
        query: url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
            .into_owned()
            .collect(),
        headers: dispatcharr_contracts::source_headers(
            request
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        ),
    })
}
/// Django's request.path is percent-decoded without treating '+' as a space.
fn decoded_path(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut offset = 0;
    while offset < bytes.len() {
        if bytes[offset] == b'%' && offset + 2 < bytes.len() {
            let hex = |byte: u8| (byte as char).to_digit(16);
            if let Some((hi, lo)) = hex(bytes[offset + 1]).zip(hex(bytes[offset + 2])) {
                result.push((hi * 16 + lo) as u8);
                offset += 3;
                continue;
            }
        }
        result.push(bytes[offset]);
        offset += 1;
    }
    String::from_utf8_lossy(&result).into_owned()
}
fn missing_peer() -> Response {
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(json!({"detail":"HTTP socket peer metadata is required"})),
    )
        .into_response()
}
pub(crate) async fn path(
    State(engine): State<CatchupMedia>,
    Path((username, password, duration, timestamp, channel)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    OriginalUri(uri): OriginalUri,
    request: Request<Body>,
) -> Response {
    let Some(identity) = identity(&request) else {
        return missing_peer();
    };
    engine
        .respond_xc(
            identity,
            XcIntent {
                username,
                password,
                duration: Some(duration),
                timestamp,
                channel,
            },
            decoded_path(uri.path()),
            false,
        )
        .await
}
pub(crate) async fn query(
    State(engine): State<CatchupMedia>,
    OriginalUri(uri): OriginalUri,
    request: Request<Body>,
) -> Response {
    let Some(identity) = identity(&request) else {
        return missing_peer();
    };
    let username = identity.parameter("username").unwrap_or("").to_owned();
    let password = identity.parameter("password").unwrap_or("").to_owned();
    let timestamp = identity
        .parameter("start")
        .filter(|v| !v.is_empty())
        .or_else(|| identity.parameter("utc"))
        .unwrap_or("")
        .to_owned();
    let channel = identity.parameter("stream").unwrap_or("").to_owned();
    if username.is_empty() || password.is_empty() || timestamp.is_empty() || channel.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            [
                ("content-type", "text/html; charset=utf-8"),
                ("x-content-type-options", "nosniff"),
            ],
            "Missing required parameters",
        )
            .into_response();
    }
    let duration = identity.parameter("duration").map(str::to_owned);
    engine
        .respond_xc(
            identity,
            XcIntent {
                username,
                password,
                duration,
                timestamp,
                channel,
            },
            decoded_path(uri.path()),
            true,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_pk_and_request_path_rules_keep_signs_underscores_colons_and_literal_plus() {
        assert_eq!(channel_pk(" +1_1.ts"), Some(11));
        for bad in ["1__1.ts", "1_.ts", "bad", "9223372036854775808"] {
            assert!(channel_pk(bad).is_none());
        }
        assert_eq!(
            decoded_path("/timeshift/name+tag/pass/30/2026-11-09%3A11-00/11.ts"),
            "/timeshift/name+tag/pass/30/2026-11-09:11-00/11.ts"
        );
        assert_eq!(NOT_FOUND.len(), 179);
    }
}
