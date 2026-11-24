//! XC live/category session ported from Original Dispatcharr core/xtream_codes.py.
//! Original authors retain all application and algorithm credit.
use crate::{
    xc_account_provider::{credentials, Credentials},
    StoreError,
};
use dispatcharr_upstream_http::Attempt;
use futures_util::StreamExt;
use serde_json::Value;
use std::time::Duration;
use url::Url;
// Existing category import admits 16MiB and batch sink 8MiB; whole provider
// documents are additionally bounded here. Larger providers remain explicit.
const BODY: usize = 16 * 1024 * 1024;
pub(crate) struct Client {
    credentials: Credentials,
    attempt: Attempt,
    agent: String,
    authenticated: bool,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("XC live provider capacity or response shape")
}
impl Client {
    pub(crate) fn new(
        base: &str,
        user: &str,
        password: &str,
        agent: String,
    ) -> Result<Option<Self>, StoreError> {
        let Some(credentials) = credentials(base, user, password, "", "")? else {
            return Ok(None);
        };
        Ok(Some(Self {
            credentials,
            attempt: Attempt::new_requests(Duration::from_secs(60), Duration::from_secs(60))
                .map_err(|_| unsupported())?,
            agent,
            authenticated: false,
        }))
    }
    pub(crate) fn live_prefix(&self) -> String {
        format!(
            "{}/live/{}/{}/",
            self.credentials.url, self.credentials.username, self.credentials.password
        )
    }
    async fn request(&self, action: Option<&str>) -> Result<Result<Value, String>, StoreError> {
        let endpoint = format!("{}/player_api.php", self.credentials.url);
        let mut url = Url::parse(&endpoint).map_err(|_| unsupported())?;
        url.query_pairs_mut()
            .append_pair("username", &self.credentials.username)
            .append_pair("password", &self.credentials.password);
        if let Some(action) = action {
            url.query_pairs_mut().append_pair("action", action);
        }
        let mut response = None;
        for _ in 0..4 {
            match self
                .attempt
                .request(url.as_str(), &self.agent, &[])
                .map_err(|_| unsupported())?
                .send()
                .await
            {
                Ok(v) => {
                    response = Some(v);
                    break;
                }
                Err(_) => continue,
            }
        }
        let Some(response) = response else {
            return Err(unsupported());
        };
        if self.attempt.cookie_budget_exceeded() {
            return Err(unsupported());
        }
        let status = response.status();
        let actual_url = response.url().as_str().to_owned();
        let mut body =
            dispatcharr_upstream_http::body_limited(response, BODY).map_err(|_| unsupported())?;
        let mut bytes = Vec::new();
        while let Some(part) = body.next().await {
            let part = part.map_err(|_| unsupported())?;
            if part.len() > BODY - bytes.len() {
                return Err(unsupported());
            }
            bytes.extend_from_slice(&part)
        }
        if status.is_client_error() || status.is_server_error() {
            return Ok(Err(format!(
                "{} {} Error: {} for url: {actual_url}",
                status.as_u16(),
                if status.is_client_error() {
                    "Client"
                } else {
                    "Server"
                },
                status.canonical_reason().unwrap_or("")
            )));
        }
        if bytes.is_empty() {
            return Ok(Err(format!(
                "XC API returned empty response from {endpoint}"
            )));
        }
        // Python json accepts nonfinite literals; no accidental provider-none
        // substitution for an unimplemented parser representation.
        let text = std::str::from_utf8(&bytes).map_err(|_| unsupported())?;
        let trimmed = text.trim();
        if ["blocked", "forbidden", "access denied", "unauthorized"]
            .contains(&trimmed.to_lowercase().as_str())
        {
            return Ok(Err(format!(
                "XC API request blocked by server from {endpoint}. Response: {trimmed}"
            )));
        }
        match crate::provider_json_admission::admit(
            &bytes,
            crate::provider_json_admission::Limits {
                work_bytes: 32 * 1024 * 1024,
                depth: 64,
                string_bytes: 65536,
                string_copies: 8,
            },
        ) {
            Ok(()) => {}
            Err(crate::provider_json_admission::Error::Capacity) => return Err(unsupported()),
            Err(crate::provider_json_admission::Error::Syntax(error)) => {
                if text.contains("NaN")
                    || text.contains("Infinity")
                    || error.to_string().starts_with("recursion limit exceeded")
                {
                    return Err(unsupported());
                }
                return Ok(Err(format!(
                    "XC API returned invalid JSON from {endpoint}. Response: {}",
                    text.chars().take(1000).collect::<String>()
                )));
            }
        }
        // This is the first tree allocation. The non-building pass quoted every
        // node/key/string before account/profile/stream projections can clone it.
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| unsupported())?;
        let value = normalize_numbers(value)?;
        if value.is_object()
            && value.get("user_info").is_none_or(Value::is_null)
            && value.get("error").is_some()
        {
            let Some(error) = value["error"].as_str() else {
                return Err(unsupported());
            };
            return Ok(Err(format!("XC API Error: {error}")));
        }
        Ok(Ok(value))
    }
    pub(crate) async fn action(
        &mut self,
        action: &str,
    ) -> Result<Result<Value, String>, StoreError> {
        if !matches!(action, "get_live_categories" | "get_live_streams") {
            return Err(unsupported());
        }
        self.authenticated_action(action).await
    }
    /// Natural VOD list ingestion reuses this SAME authenticated Requests
    /// session/limits; it does not grant arbitrary provider action execution.
    pub(crate) async fn vod_action(
        &mut self,
        action: &str,
    ) -> Result<Result<Value, String>, StoreError> {
        let missing_list = match action {
            "get_vod_categories" => "Invalid VOD categories response: None",
            "get_series_categories" => "Invalid series categories response: None",
            "get_vod_streams" => "Invalid VOD streams response for category None: None",
            "get_series" => "Invalid series response for category None: None",
            _ => return Err(unsupported()),
        };
        match self.authenticated_action(action).await? {
            // The Original provider methods reject a delivered JSON null before
            // their caller mutates the catalogue. The task catches that error.
            Ok(Value::Null) => Ok(Err(missing_list.to_owned())),
            outcome => Ok(outcome),
        }
    }
    async fn authenticated_action(
        &mut self,
        action: &str,
    ) -> Result<Result<Value, String>, StoreError> {
        if !self.authenticated {
            match self.request(None).await? {
                Err(e) => return Ok(Err(e)),
                Ok(v) => {
                    if v.get("user_info").is_none_or(|v| !truthy(v)) {
                        return Ok(Err(
                            "Authentication failed: Invalid response from server".into()
                        ));
                    }
                    if !v["user_info"].is_object() {
                        return Err(unsupported());
                    }
                    self.authenticated = true;
                }
            }
        }
        self.request(Some(action)).await
    }
}
pub(crate) fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
/// Python scalar string spelling; compound repr semantics remain explicit.
pub(crate) fn scalar(value: &Value) -> Result<String, StoreError> {
    Ok(match value {
        Value::Null => "None".into(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::String(v) => v.clone(),
        Value::Number(v) => {
            let raw = v.to_string();
            if raw.contains(['.', 'e', 'E']) {
                let n = v
                    .as_f64()
                    .filter(|v| v.is_finite())
                    .ok_or_else(unsupported)?;
                crate::json_compat::float_string(n)
            } else {
                raw
            }
        }
        _ => return Err(unsupported()),
    })
}

// Match Source's ordinary Python JSON number normalization before selecting
// provider scalars. Unimplemented nonfinite/4300-digit-overflow domains remain
// explicit, never fabricated provider absence or differently persisted values.
fn normalize_numbers(mut value: Value) -> Result<Value, StoreError> {
    crate::json_compat::normalize_input_numbers(&mut value).map_err(|_| unsupported())?;
    Ok(value)
}
#[cfg(test)]
mod number_controls {
    use super::*;
    #[test]
    fn provider_numeric_domains_are_guarded_before_business_projection() {
        let value: Value =
            serde_json::from_slice(br#"{"category_id":3e2,"stream_id":-0}"#).unwrap();
        let value = normalize_numbers(value).unwrap();
        assert_eq!(scalar(&value["category_id"]).unwrap(), "300.0");
        assert_eq!(scalar(&value["stream_id"]).unwrap(), "0");
        let exponent: Value = serde_json::from_slice(b"1e20").unwrap();
        assert_eq!(
            scalar(&normalize_numbers(exponent).unwrap()).unwrap(),
            "1e+20"
        );
        let value: Value = serde_json::from_slice(br#"{"value":1e999}"#).unwrap();
        assert!(normalize_numbers(value).is_err());
        let text = format!("{{\"value\":{}}}", "1".repeat(4301));
        let value: Value = serde_json::from_str(&text).unwrap();
        assert!(normalize_numbers(value).is_err());
    }
}

#[cfg(test)]
mod vod_response_controls {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
    };

    async fn delivered(list_action: &str, body: &'static str) -> Result<Value, String> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let required = format!("action={list_action}");
        let server = std::thread::spawn(move || {
            for (index, reply) in [r#"{"user_info":{"auth":1}}"#, body]
                .into_iter()
                .enumerate()
            {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    assert!(request.len() < 8192);
                    let mut byte = [0u8];
                    socket.read_exact(&mut byte).unwrap();
                    request.push(byte[0]);
                }
                let request = std::str::from_utf8(&request).unwrap();
                let first = request.lines().next().unwrap();
                assert!(first.starts_with("GET /player_api.php?"));
                assert_eq!(first.contains(&required), index == 1);
                assert_eq!(first.contains("action="), index == 1);
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                )
                .unwrap();
                socket.flush().unwrap();
            }
        });
        let result = Client::new(&base, "owned-user", "owned-value", "Owned fixture".into())
            .unwrap()
            .unwrap()
            .vod_action(list_action)
            .await
            .unwrap();
        server.join().unwrap();
        result
    }

    #[tokio::test]
    async fn authoritative_vod_json_null_is_caught_with_the_exact_list_action_message() {
        for (action, expected) in [
            (
                "get_vod_categories",
                "Invalid VOD categories response: None",
            ),
            (
                "get_series_categories",
                "Invalid series categories response: None",
            ),
            (
                "get_vod_streams",
                "Invalid VOD streams response for category None: None",
            ),
            (
                "get_series",
                "Invalid series response for category None: None",
            ),
        ] {
            assert_eq!(delivered(action, "null").await, Err(expected.into()));
        }
        assert_eq!(
            delivered("get_vod_categories", "[]").await,
            Ok(Value::Array(Vec::new()))
        );
        assert_eq!(
            delivered("get_series", r#"{"error":"Owned provider response"}"#).await,
            Err("XC API Error: Owned provider response".into())
        );
    }
}
