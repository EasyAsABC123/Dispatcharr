//! Native Schedules Direct Requests transport via the existing owned factory.
//! Original Dispatcharr/Requests/serde authors retain protocol/algorithm credit.
use dispatcharr_upstream_http::{Attempt, ResourceLimitExceeded, WireLimitExceeded};
use futures_util::StreamExt;
use serde_json::Value;
use std::{io, sync::Arc, time::Duration};
pub(super) const BASE: &str = "https://json.schedulesdirect.org/20141201";
const USER_AGENT: &str = "Dispatcharr/0.33.0"; // pinned Original version.py
const BODY: usize = 16 * 1024 * 1024;
const REQUEST: usize = 8 * 1024 * 1024;
pub(super) type Owner = Arc<dyn Send + Sync>;
#[derive(Debug)]
pub(super) enum Error {
    Capacity,
    Unsupported,
    Transport,
    Factory,
}
pub(super) struct Reply {
    pub status: u16,
    pub actual_url: String,
    pub value: Option<Value>,
    pub payload_code: Option<i64>,
}
impl Reply {
    pub fn request_error(&self) -> Option<String> {
        let status = reqwest::StatusCode::from_u16(self.status).ok()?;
        if !status.is_client_error() && !status.is_server_error() {
            return None;
        }
        let kind = if status.is_client_error() {
            "Client"
        } else {
            "Server"
        };
        Some(format!(
            "{} {kind} Error: {} for url: {}",
            self.status,
            status.canonical_reason().unwrap_or(""),
            self.actual_url
        ))
    }
}
fn capacity(error: &io::Error) -> bool {
    let mut cause = std::error::Error::source(error);
    if error
        .get_ref()
        .is_some_and(|v| v.is::<WireLimitExceeded>() || v.is::<ResourceLimitExceeded>())
    {
        return true;
    }
    while let Some(value) = cause {
        if value.is::<WireLimitExceeded>() || value.is::<ResourceLimitExceeded>() {
            return true;
        }
        cause = value.source();
    }
    false
}
fn request_json(value: &Value) -> Result<Vec<u8>, Error> {
    crate::json_compat::default_json_bytes(value, REQUEST).map_err(|error| match error {
        crate::json_compat::DefaultJsonError::Invalid => Error::Unsupported,
        crate::json_compat::DefaultJsonError::Capacity => Error::Capacity,
    })
}
pub(super) fn integer_code(value: &Value) -> Result<Option<i64>, Error> {
    match value {
        Value::Bool(value) => Ok(Some(i64::from(*value))),
        Value::Number(value) if !value.is_f64() => {
            value.as_i64().map(Some).ok_or(Error::Unsupported)
        }
        _ => Ok(None),
    }
}
fn decoded_json(bytes: &[u8]) -> Result<Option<Value>, Error> {
    if bytes.starts_with(&[239, 187, 191]) || std::str::from_utf8(bytes).is_err() {
        return Err(Error::Unsupported);
    }
    let (mut quoted, mut escaped) = (false, false);
    for (i, b) in bytes.iter().enumerate() {
        if quoted {
            if escaped {
                escaped = false
            } else if *b == b'\\' {
                escaped = true
            } else if *b == b'"' {
                quoted = false
            }
        } else if *b == b'"' {
            quoted = true
        } else if bytes[i..].starts_with(b"NaN")
            || bytes[i..].starts_with(b"Infinity")
            || bytes[i..].starts_with(b"-Infinity")
        {
            return Err(Error::Unsupported);
        }
    }
    match crate::provider_json_admission::admit(
        bytes,
        crate::provider_json_admission::Limits {
            work_bytes: 64 * 1024 * 1024,
            depth: 64,
            string_bytes: BODY,
            string_copies: 2,
        },
    ) {
        Ok(()) => {
            let mut value: Value = serde_json::from_slice(bytes).map_err(|_| Error::Unsupported)?;
            crate::json_compat::normalize_input_numbers(&mut value)
                .map_err(|_| Error::Unsupported)?;
            Ok(Some(value))
        }
        Err(crate::provider_json_admission::Error::Syntax(_)) => Ok(None),
        Err(crate::provider_json_admission::Error::Capacity) => Err(Error::Capacity),
    }
}
pub(super) async fn request(
    method: reqwest::Method,
    path: &str,
    token: Option<&str>,
    debug: bool,
    payload: Option<&Value>,
    timeout: Duration,
    owner: Owner,
) -> Result<Reply, Error> {
    request_inner(
        method,
        path,
        token,
        debug,
        payload,
        RequestPolicy {
            timeout,
            owner,
            content_type: true,
        },
    )
    .await
}
pub(super) async fn countries(owner: Owner) -> Result<Reply, Error> {
    request_inner(
        reqwest::Method::GET,
        "/available/countries",
        None,
        false,
        None,
        RequestPolicy {
            timeout: Duration::from_secs(15),
            owner,
            content_type: false,
        },
    )
    .await
}
struct RequestPolicy {
    timeout: Duration,
    owner: Owner,
    content_type: bool,
}
async fn request_inner(
    method: reqwest::Method,
    path: &str,
    token: Option<&str>,
    debug: bool,
    payload: Option<&Value>,
    policy: RequestPolicy,
) -> Result<Reply, Error> {
    let RequestPolicy {
        timeout,
        owner,
        content_type,
    } = policy;
    let _owner = owner; // actual admitted task lives through final body/EOF/error
    if !path.starts_with('/') || path.len() > 65536 {
        return Err(Error::Unsupported);
    }
    let url = format!("{BASE}{path}");
    let attempt = Attempt::new_requests(timeout, timeout).map_err(|_| Error::Factory)?;
    let mut headers = Vec::new();
    if content_type {
        headers.push(("Content-Type".into(), "application/json".into()));
    }
    if let Some(token) = token {
        if token.len() > 65536 {
            return Err(Error::Capacity);
        }
        headers.push(("token".into(), token.to_owned()));
    }
    if debug {
        headers.push(("RouteTo".into(), "debug".into()))
    }
    let mut request = attempt
        .request_method(method, &url, USER_AGENT, &headers)
        .map_err(|_| Error::Unsupported)?;
    if let Some(payload) = payload {
        request = request.body(request_json(payload)?);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(_) => {
            return Err(if attempt.cookie_budget_exceeded() {
                Error::Capacity
            } else {
                Error::Transport
            })
        }
    };
    if attempt.cookie_budget_exceeded() {
        return Err(Error::Capacity);
    }
    let status = response.status().as_u16();
    let actual_url = response.url().as_str().to_owned();
    let json_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("json"));
    if response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.to_ascii_lowercase().split(';').skip(1).any(|p| {
                p.trim()
                    .strip_prefix("charset=")
                    .is_some_and(|c| !matches!(c.trim_matches('"'), "utf-8" | "utf8"))
            })
        })
    {
        return Err(Error::Unsupported);
    }
    if response
        .headers()
        .get_all(reqwest::header::CONTENT_ENCODING)
        .iter()
        .any(|header| {
            header.to_str().ok().is_none_or(|text| {
                text.split(',').any(|coding| {
                    !matches!(
                        coding.trim().to_ascii_lowercase().as_str(),
                        "" | "identity" | "gzip" | "x-gzip" | "deflate"
                    )
                })
            })
        })
    {
        return Err(Error::Unsupported);
    }
    let mut stream = dispatcharr_upstream_http::body_limited(response, BODY).map_err(|e| {
        if capacity(&e) {
            Error::Capacity
        } else {
            Error::Transport
        }
    })?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            if capacity(&e) {
                Error::Capacity
            } else {
                Error::Transport
            }
        })?;
        if chunk.len() > BODY - bytes.len() {
            return Err(Error::Capacity);
        }
        bytes.extend_from_slice(&chunk);
    }
    let looks_json = json_type
        || bytes
            .iter()
            .find(|b| !b.is_ascii_whitespace())
            .is_some_and(|b| matches!(b, b'{' | b'['));
    let value = decoded_json(&bytes)?;
    let payload_code = if looks_json {
        value
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|v| v.get("code"))
            .map(integer_code)
            .transpose()?
            .flatten()
    } else {
        None
    };
    Ok(Reply {
        status,
        actual_url,
        value,
        payload_code,
    })
}
#[cfg(test)]
mod controls {
    use super::*;
    use serde_json::json;
    #[test]
    fn requests_json_has_python_spacing_ascii_and_surrogate_pairs() {
        assert_eq!(
            request_json(&json!({"username":"Straße💡","date":["2026-11-15"],"force":false}))
                .unwrap(),
            br#"{"username": "Stra\u00dfe\ud83d\udca1", "date": ["2026-11-15"], "force": false}"#
        );
        assert_eq!(integer_code(&json!(false)).unwrap(), Some(0));
        assert_eq!(integer_code(&json!(1.0)).unwrap(), None);
    }
    #[test]
    fn malformed_transport_json_is_distinct_from_nonfinite_encoding_and_budget() {
        assert!(decoded_json(b"{actual malformed}").unwrap().is_none());
        assert!(matches!(decoded_json(b"[NaN]"), Err(Error::Unsupported)));
        assert!(matches!(
            decoded_json(b"[Infinity]"),
            Err(Error::Unsupported)
        ));
        assert!(matches!(decoded_json(&[0xff]), Err(Error::Unsupported)));
        let dense = format!("[{}]", "0,".repeat(1_500_000) + "0");
        assert!(matches!(
            decoded_json(dense.as_bytes()),
            Err(Error::Capacity)
        ));
        assert_eq!(
            decoded_json(br#"{"code":3001,"message":"owned"}"#)
                .unwrap()
                .unwrap()["code"],
            3001
        );
    }
}
