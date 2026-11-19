//! Provider-side XC account normalization, ported from Original Dispatcharr.
//! Original Dispatcharr/Requests authors retain all application/algorithm credit.
use crate::StoreError;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, SecondsFormat, Utc};
use futures_util::StreamExt;
use serde_json::{json, Map, Value};
use std::time::Duration;
use url::Url;
const BODY: usize = 1024 * 1024;
pub(crate) fn unsupported() -> StoreError {
    StoreError::Unsupported("XC provider value or transform domain is unsupported")
}
/// These values are secret-bearing. Deliberately no Debug or Display.
pub(crate) struct Credentials {
    pub url: String,
    pub username: String,
    pub password: String,
}
pub(crate) fn credentials(
    base: &str,
    user: &str,
    password: &str,
    search: &str,
    replace: &str,
) -> Result<Option<Credentials>, StoreError> {
    if base.is_empty() || user.is_empty() || password.is_empty() {
        return Ok(None);
    }
    let mut normalized = Url::parse(base.trim()).map_err(|_| unsupported())?;
    if !matches!(normalized.scheme(), "http" | "https") || normalized.host_str().is_none() {
        return Err(unsupported());
    }
    let path = normalized.path().trim_end_matches('/');
    let path = if path
        .rsplit('/')
        .next()
        .is_some_and(|last| last.ends_with(".php"))
    {
        path.rsplit_once('/').map(|v| v.0).unwrap_or("")
    } else {
        path
    };
    let path = path.to_owned();
    normalized.set_path(&path);
    normalized.set_query(None);
    normalized.set_fragment(None);
    let original = normalized.as_str().trim_end_matches('/').to_owned();
    if search.is_empty() || replace.is_empty() || (search == "^(.*)$" && replace == "$1") {
        return Ok(Some(Credentials {
            url: original,
            username: user.into(),
            password: password.into(),
        }));
    }
    // Rust's linear-time engine supports the ordinary capture replacement
    // subset. Python lookaround/backreferences/regex flags remain explicit.
    if replace.contains('\\')
        || ["(?=", "(?!", "(?<=", "(?<!", "(?("]
            .iter()
            .any(|v| search.contains(v))
        || (1..=9).any(|n| search.contains(&format!("\\{n}")))
    {
        return Err(unsupported());
    }
    let regex = match regex::Regex::new(search) {
        Ok(v) => v,
        Err(_) if search.contains("(?") => return Err(unsupported()),
        Err(_) => return Ok(None),
    };
    let input = format!("{original}/live/{user}/{password}/1234.ts");
    if !regex.is_match(&input) {
        return Ok(None);
    }
    let mut replacement = String::new();
    let mut chars = replace.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            replacement.push(c);
            continue;
        }
        if chars.peek() == Some(&'<') {
            chars.next();
            replacement.push_str("${");
            for c in chars.by_ref() {
                if c == '>' {
                    break;
                }
                replacement.push(c)
            }
            replacement.push('}');
        } else if chars.peek().is_some_and(char::is_ascii_digit) {
            replacement.push_str("${");
            while chars.peek().is_some_and(char::is_ascii_digit) {
                replacement.push(chars.next().unwrap_or_default())
            }
            replacement.push('}');
        } else {
            replacement.push_str("$$")
        }
    }
    // Stream expansion into its actual declared budget, including captures;
    // checking len after Regex::replace_all would allocate an oversized result.
    let mut transformed = String::new();
    let mut previous = 0;
    for captures in regex.captures_iter(&input) {
        let whole = captures.get(0).ok_or_else(unsupported)?;
        if whole.start() == whole.end() {
            return Err(unsupported());
        }
        append(&mut transformed, &input[previous..whole.start()])?;
        let mut cursor = 0;
        while cursor < replacement.len() {
            let tail = &replacement[cursor..];
            if tail.starts_with("$$") {
                append(&mut transformed, "$")?;
                cursor += 2;
                continue;
            }
            if let Some(rest) = tail.strip_prefix("${") {
                let end = rest.find('}').ok_or_else(unsupported)?;
                let name = &rest[..end];
                let matched = if let Ok(index) = name.parse::<usize>() {
                    if index == 0 || index >= 100 {
                        return Err(unsupported());
                    }
                    if index >= regex.captures_len() {
                        return Ok(None);
                    }
                    captures.get(index)
                } else {
                    if !regex.capture_names().flatten().any(|v| v == name) {
                        return Ok(None);
                    }
                    captures.name(name)
                };
                if let Some(value) = matched {
                    append(&mut transformed, value.as_str())?
                }
                cursor += 2 + end + 1;
                continue;
            }
            let value = tail.chars().next().ok_or_else(unsupported)?;
            append(&mut transformed, &tail[..value.len_utf8()])?;
            cursor += value.len_utf8();
        }
        previous = whole.end();
    }
    append(&mut transformed, &input[previous..])?;
    let mut url = Url::parse(&transformed).map_err(|_| unsupported())?;
    let parts = url
        .path()
        .split('/')
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();
    if parts.len() < 4 || parts.last() != Some(&"1234.ts") {
        return Ok(None);
    }
    let username = parts[parts.len() - 3].to_owned();
    let password = parts[parts.len() - 2].to_owned();
    let base = if parts.len() == 4 {
        String::new()
    } else {
        format!("/{}", parts[..parts.len() - 4].join("/"))
    };
    url.set_path(&base);
    url.set_query(None);
    url.set_fragment(None);
    Ok(Some(Credentials {
        url: url.as_str().trim_end_matches('/').into(),
        username,
        password,
    }))
}
fn append(output: &mut String, value: &str) -> Result<(), StoreError> {
    if value.len() > 65536 - output.len() {
        return Err(unsupported());
    }
    output.push_str(value);
    Ok(())
}
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
pub(crate) enum FetchFailure {
    /// The awaited provider requests all ended without a response.
    ConnectionAttemptsExhausted,
    /// The bounded response settled, but the account-info fields cannot be read.
    InvalidAccountInfo,
    Operational(StoreError),
}

impl From<StoreError> for FetchFailure {
    fn from(error: StoreError) -> Self {
        Self::Operational(error)
    }
}

impl From<FetchFailure> for StoreError {
    fn from(error: FetchFailure) -> Self {
        match error {
            FetchFailure::ConnectionAttemptsExhausted | FetchFailure::InvalidAccountInfo => {
                unsupported()
            }
            FetchFailure::Operational(error) => error,
        }
    }
}

/// Secret values are persisted only in the actual profile CP, as Source does;
/// Source-compatible exception values may contain the actual provider URL/body.
/// They are application data only: never log them or publish them as evidence.
pub(crate) async fn fetch(
    c: Credentials,
    agent: &str,
) -> Result<Result<Value, String>, FetchFailure> {
    let attempt = dispatcharr_upstream_http::Attempt::new_requests(
        Duration::from_secs(60),
        Duration::from_secs(60),
    )
    .map_err(|_| unsupported())?;
    let mut url = Url::parse(&format!("{}/player_api.php", c.url)).map_err(|_| unsupported())?;
    url.query_pairs_mut()
        .append_pair("username", &c.username)
        .append_pair("password", &c.password);
    let mut response = None;
    for _ in 0..4 {
        let request = attempt
            .request(url.as_str(), agent, &[])
            .map_err(|_| unsupported())?;
        match request.send().await {
            Ok(value) => {
                response = Some(value);
                break;
            }
            Err(_) => continue,
        }
    }
    let Some(response) = response else {
        return Err(FetchFailure::ConnectionAttemptsExhausted);
    };
    if attempt.cookie_budget_exceeded() {
        return Err(unsupported().into());
    }
    let status = response.status();
    let actual_url = response.url().as_str().to_owned();
    let endpoint = format!("{}/player_api.php", c.url);
    let mut stream = dispatcharr_upstream_http::body(response).map_err(|_| unsupported())?;
    let mut raw = Vec::new();
    while let Some(part) = stream.next().await {
        let part = match part {
            Ok(part) => part,
            Err(_) => return Err(unsupported().into()),
        };
        if part.len() > BODY - raw.len() {
            return Err(unsupported().into());
        }
        raw.extend_from_slice(&part)
    }
    if status.is_client_error() || status.is_server_error() {
        let class = if status.is_client_error() {
            "Client"
        } else {
            "Server"
        };
        return Ok(Err(format!(
            "{} {class} Error: {} for url: {actual_url}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        )));
    }
    if raw.is_empty() {
        return Ok(Err(format!(
            "XC API returned empty response from {endpoint}"
        )));
    }
    let text = String::from_utf8_lossy(&raw);
    let trimmed = text.trim();
    if ["blocked", "forbidden", "access denied", "unauthorized"]
        .contains(&trimmed.to_lowercase().as_str())
    {
        return Ok(Err(format!(
            "XC API request blocked by server from {endpoint}. Response: {trimmed}"
        )));
    }
    let value: Value = match serde_json::from_slice(&raw) {
        Ok(v) => v,
        Err(error) => {
            if error.to_string().starts_with("recursion limit exceeded") {
                return Err(unsupported().into());
            }
            return Ok(Err(format!(
                "XC API returned invalid JSON from {endpoint}. Response: {}",
                text.chars().take(1000).collect::<String>()
            )));
        }
    };
    account_info(value, Utc::now())
}

fn account_info(value: Value, now: DateTime<Utc>) -> Result<Result<Value, String>, FetchFailure> {
    if value.is_object()
        && value.get("user_info").is_none_or(Value::is_null)
        && value.get("error").is_some()
    {
        let error = value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("Unknown error");
        return Ok(Err(format!("XC API Error: {error}")));
    }
    // Source's field access raises after a complete response. Batch refresh
    // counts this profile as failed. The single-profile route still needs
    // the Original exception descriptor before exposing it as application data.
    if value
        .get("user_info")
        .is_some_and(|v| truthy(v) && !v.is_object())
        || value.get("server_info").is_some_and(|v| !v.is_object())
    {
        return Err(FetchFailure::InvalidAccountInfo);
    }
    Ok(normalize(value, now))
}
fn normalize(value: Value, now: DateTime<Utc>) -> Result<Value, String> {
    if value.get("user_info").is_none_or(|v| !truthy(v)) {
        return Err("Authentication failed: Invalid response from server".into());
    }
    let Some(user) = value.get("user_info").and_then(Value::as_object) else {
        return Err("XC provider user_info is not an object".into());
    };
    let server = value.get("server_info").unwrap_or(&Value::Null);
    if value.get("server_info").is_some_and(|v| !v.is_object()) {
        return Err("XC provider server_info is not an object".into());
    }
    let mut users = Map::new();
    for key in [
        "username",
        "password",
        "message",
        "auth",
        "status",
        "exp_date",
        "is_trial",
        "active_cons",
        "created_at",
        "max_connections",
    ] {
        users.insert(key.into(), user.get(key).cloned().unwrap_or(Value::Null));
    }
    users.insert(
        "allowed_output_formats".into(),
        user.get("allowed_output_formats")
            .cloned()
            .unwrap_or_else(|| json!([])),
    );
    let mut servers = Map::new();
    for key in [
        "url",
        "port",
        "https_port",
        "server_protocol",
        "rtmp_port",
        "timezone",
        "timestamp_now",
        "time_now",
    ] {
        servers.insert(key.into(), server.get(key).cloned().unwrap_or(Value::Null));
    }
    Ok(
        json!({"last_refresh":now.to_rfc3339_opts(SecondsFormat::Micros,true),"auth_timestamp":now.timestamp_micros()as f64/1000000.0,"user_info":users,"server_info":servers}),
    )
}
pub(crate) fn expiration(value: &Value) -> Option<DateTime<Utc>> {
    let numeric = match value {
        Value::Number(v) => v.as_f64(),
        Value::Bool(v) => Some(f64::from(u8::from(*v))),
        Value::String(v) => v.trim().parse::<f64>().ok(),
        _ => None,
    };
    if let Some(n) = numeric {
        if !n.is_finite() {
            return None;
        }
        let seconds = n.trunc();
        if !(-62135596800.0..=253402300799.0).contains(&seconds) {
            return None;
        }
        return DateTime::from_timestamp(seconds as i64, 0)?
            .checked_add_signed(chrono::Duration::microseconds(
                ((n - seconds) * 1e6).round_ties_even() as i64,
            ))
            .filter(|v| (1..=9999).contains(&v.year()));
    }
    let s = value.as_str()?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|v| v.to_utc())
        .or_else(|| {
            ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
                .iter()
                .find_map(|format| {
                    NaiveDateTime::parse_from_str(s, format)
                        .ok()
                        .map(|v| v.and_utc())
                })
        })
        .or_else(|| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .ok()?
                .and_hms_opt(0, 0, 0)
                .map(|v| v.and_utc())
        })
        .filter(|v| (1..=9999).contains(&v.year()))
}

/// Resolve VOD profiles through the SAME bounded Source credential transform
/// and shared live/VOD credential fingerprint. Returned strings are secret-bearing
/// and must not be included in logs, public evidence or Debug diagnostics.
impl crate::AuthService {
    #[allow(clippy::type_complexity)]
    pub fn resolve_vod_profile_credentials(
        base: &str,
        username: &str,
        password: &str,
        search: &str,
        replacement: &str,
        group: Option<i64>,
        maximum: u32,
    ) -> Result<
        Option<(
            String,
            String,
            String,
            Option<dispatcharr_contracts::PlaybackCredentialPool>,
        )>,
        StoreError,
    > {
        let Some(current) = credentials(base, username, password, search, replacement)? else {
            return Ok(None);
        };
        let pool = match group {
            Some(group) if maximum > 0 => Some(crate::playback::xc_credential_pool(
                group,
                maximum,
                &current.username,
                &current.password,
            )?),
            _ => None,
        };
        Ok(Some((
            current.url,
            current.username,
            current.password,
            pool,
        )))
    }
}

#[cfg(test)]
mod source_controls {
    use super::*;

    #[tokio::test]
    async fn exhausted_provider_connections_are_distinct_from_operational_failures() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move {
            for _ in 0..4 {
                let (connection, _) = listener.accept().await.unwrap();
                // Close before sending headers to exercise actual send errors.
                drop(connection);
            }
        });
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            fetch(
                Credentials {
                    url: base,
                    username: "fixture-user".into(),
                    password: "fixture-password".into(),
                },
                "fixture",
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            Err(FetchFailure::ConnectionAttemptsExhausted)
        ));
        tokio::time::timeout(Duration::from_secs(1), upstream)
            .await
            .unwrap()
            .unwrap();
    }
    #[test]
    fn completed_account_info_shape_error_is_distinct_from_an_operational_error() {
        let now = DateTime::from_timestamp(1, 0).unwrap();
        for response in [
            json!({"user_info":["unexpected"],"server_info":{}}),
            json!({"user_info":{"auth":1},"server_info":null}),
        ] {
            let outcome = account_info(response, now);
            assert!(matches!(outcome, Err(FetchFailure::InvalidAccountInfo)));
            assert!(matches!(
                StoreError::from(outcome.err().unwrap()),
                StoreError::Unsupported(_)
            ));
        }
        let recovered = account_info(
            json!({"user_info":{"auth":1},"server_info":{"timezone":"UTC"}}),
            now,
        );
        assert!(matches!(recovered, Ok(Ok(info)) if info["server_info"]["timezone"]=="UTC"));
    }
    #[test]
    fn vod_profile_transform_and_pool_use_the_same_changed_credentials() {
        let (base, user, password, pool) = crate::AuthService::resolve_vod_profile_credentials(
            "http://provider.invalid/base/player_api.php?discard=1",
            "user",
            "pass",
            r"/live/([^/]+)/([^/]+)/",
            "/live/new-$1/new-$2/",
            Some(7),
            2,
        )
        .unwrap()
        .unwrap();
        assert_eq!(base, "http://provider.invalid/base");
        assert_eq!(user, "new-user");
        assert_eq!(password, "new-pass");
        let pool = pool.unwrap();
        assert_eq!(
            pool.counter_key,
            "server_group_connections:7:e2f437f8e5327e15"
        );
        assert_eq!(pool.max_streams, 2);
    }
    #[test]
    fn vod_nonmatching_profile_is_not_a_base_credential_fallback() {
        assert!(crate::AuthService::resolve_vod_profile_credentials(
            "http://provider.invalid",
            "user",
            "pass",
            "never-matches",
            "replacement",
            Some(7),
            2,
        )
        .unwrap()
        .is_none());
    }
    #[test]
    fn vod_unlimited_profile_skips_group_fingerprinting_like_source() {
        let (_, _, _, pool) = crate::AuthService::resolve_vod_profile_credentials(
            "http://provider.invalid",
            "unicode-é",
            "pass",
            "^(.*)$",
            "$1",
            Some(7),
            0,
        )
        .unwrap()
        .unwrap();
        assert!(pool.is_none());
        assert!(crate::AuthService::resolve_vod_profile_credentials(
            "http://provider.invalid",
            "unicode-é",
            "pass",
            "^(.*)$",
            "$1",
            Some(7),
            1,
        )
        .is_err());
    }
    #[test]
    fn provider_auth_zero_expired_is_still_source_success_when_user_info_is_truthy() {
        let v=normalize(json!({"user_info":{"auth":0,"status":"Expired","exp_date":"0","unexpected":"discarded"},"server_info":{"timezone":"UTC"}}),DateTime::from_timestamp(1,0).unwrap()).unwrap();
        assert_eq!(v["user_info"]["auth"], 0);
        assert_eq!(v["user_info"]["status"], "Expired");
        assert!(v["user_info"].get("unexpected").is_none());
        assert_eq!(v["user_info"]["allowed_output_formats"], json!([]));
        assert!(normalize(json!({"user_info":{}}), Utc::now()).is_err());
        assert_eq!(
            expiration(&v["user_info"]["exp_date"]).unwrap().timestamp(),
            0
        );
    }
    #[test]
    fn credential_transform_preserves_subpath_and_separates_numeric_capture_suffix() {
        let c = credentials(
            "http://provider.invalid/base/player_api.php?ignored=1",
            "fixture-user",
            "fixture-value",
            r"/live/([^/]+)/",
            "/live/$1suffix/",
        )
        .unwrap()
        .unwrap();
        assert_eq!(c.url, "http://provider.invalid/base");
        assert_eq!(c.username, "fixture-usersuffix");
        assert_eq!(c.password, "fixture-value");
        assert!(
            credentials("http://provider.invalid/base", "a", "b", "nomatch", "$1")
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn expiration_none_and_invalid_preserve_column_while_numeric_iso_zero_are_real_dates() {
        for value in [Value::Null, json!("not-a-date"), json!("NaN")] {
            assert!(expiration(&value).is_none())
        }
        for value in [json!(0), json!("0"), json!("1970-01-01T00:00:00Z")] {
            assert_eq!(expiration(&value).unwrap().timestamp(), 0)
        }
        assert_eq!(expiration(&json!(-0.25)).unwrap().timestamp_millis(), -250);
    }
}
