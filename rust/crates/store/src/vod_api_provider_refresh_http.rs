//! Isolated/unwired XC refresh HTTP transport draft, not production acceptance.
//! Same per-refresh Requests session for auth+info, no global cookie jar.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::vod_api_owned_read::{ReadError, ReadWork};
use dispatcharr_upstream_http::Attempt;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
#[derive(Debug)]
pub(super) enum Fault {
    Control(ReadError),
    Remote,
    BodyDecode,
    Capacity,
    UncapturedNumeric,
    UncapturedText,
}
/// Normalize exactly the captured XC shape: remove query/fragment and trailing
/// PHP endpoint, retain an owned base path. Exotic URL grammar remains a gate.
pub(super) fn endpoint(raw: &str) -> Result<url::Url, Fault> {
    let text = raw.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'));
    let mut url = url::Url::parse(text).map_err(|_| Fault::Remote)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Fault::Remote);
    }
    let path = url.path().trim_end_matches('/');
    let path = if path
        .rsplit('/')
        .next()
        .is_some_and(|part| part.ends_with(".php"))
    {
        path.rsplit_once('/').map_or("", |(parent, _)| parent)
    } else {
        path
    };
    let path = format!("{path}/player_api.php");
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}
// Two-pass JSON admission: the first walk discards values and charges nodes,
// key/value bytes and depth BEFORE building the retained serde_json tree.
// Escaped-string scratch remains bounded independently by the input byte cap.
struct Admission<'a> {
    remaining: &'a mut usize,
    depth: usize,
    exhausted: &'a mut bool,
}
impl Admission<'_> {
    fn charge<E: serde::de::Error>(&mut self, bytes: usize) -> Result<(), E> {
        if let Some(next) = self.remaining.checked_sub(bytes) {
            *self.remaining = next;
            Ok(())
        } else {
            *self.exhausted = true;
            Err(E::custom("Provider JSON memory budget"))
        }
    }
    fn child(&mut self) -> Admission<'_> {
        Admission {
            remaining: self.remaining,
            depth: self.depth + 1,
            exhausted: self.exhausted,
        }
    }
}
impl<'de> serde::de::DeserializeSeed<'de> for Admission<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(mut self, deserializer: D) -> Result<(), D::Error> {
        if self.depth > 64 {
            *self.exhausted = true;
            return Err(serde::de::Error::custom("Provider JSON depth budget"));
        }
        self.charge::<D::Error>(256)?;
        deserializer.deserialize_any(self)
    }
}
impl<'de> serde::de::Visitor<'de> for Admission<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("budgeted provider JSON")
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(mut self, value: &str) -> Result<(), E> {
        self.charge(value.len().saturating_mul(2))
    }
    fn visit_string<E: serde::de::Error>(mut self, value: String) -> Result<(), E> {
        self.charge(value.capacity().saturating_mul(2))
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element_seed(self.child())?.is_some() {}
        Ok(())
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        while map.next_key_seed(self.child())?.is_some() {
            map.next_value_seed(self.child())?;
        }
        Ok(())
    }
}
// Number markers are chosen after decoding every original string token, so an
// escaped user key cannot collide with an inserted marker. Never use a fixed
// magic object that an external provider could inject as ordinary JSON data.
fn json_nonfinite(bytes: &[u8], maximum: usize) -> Result<(Vec<u8>, String), Fault> {
    const PREFIX: &str = "__dispatcharr_provider_nonfinite_";
    fn string_end(bytes: &[u8], start: usize) -> Result<usize, Fault> {
        let mut at = start + 1;
        while at < bytes.len() {
            match bytes[at] {
                b'\\' => at += 2,
                b'"' => return Ok(at + 1),
                _ => at += 1,
            }
        }
        Err(Fault::Remote)
    }
    let maximum_candidate = bytes.len() / PREFIX.len();
    let mut seen = std::collections::HashSet::new();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'"' {
            let end = string_end(bytes, at)?;
            let value: String =
                serde_json::from_slice(&bytes[at..end]).map_err(|_| Fault::Remote)?;
            if let Some(index) = value
                .strip_prefix(PREFIX)
                .and_then(|v| v.strip_suffix("__"))
                .and_then(|v| v.parse::<usize>().ok())
            {
                if index <= maximum_candidate {
                    seen.try_reserve(1).map_err(|_| Fault::Capacity)?;
                    seen.insert(index);
                }
            }
            at = end;
        } else {
            at += 1;
        }
    }
    // There are fewer matching string tokens than candidates in this range;
    // ignore enormous suffixes instead of overflowing on valid provider text.
    let suffix = (0..=maximum_candidate)
        .find(|index| !seen.contains(index))
        .ok_or(Fault::Capacity)?;
    drop(seen);
    let marker = format!("{PREFIX}{suffix}__");
    let mut out = Vec::new();
    at = 0;
    while at < bytes.len() {
        let (end, insert) = if bytes[at] == b'"' {
            (string_end(bytes, at)?, None)
        } else if bytes[at..].starts_with(b"NaN") {
            (at + 3, Some("NaN"))
        } else if bytes[at..].starts_with(b"-Infinity") {
            (at + 9, Some("-Infinity"))
        } else if bytes[at..].starts_with(b"Infinity") {
            (at + 8, Some("Infinity"))
        } else if bytes[at].is_ascii_digit() || bytes[at] == b'-' {
            let mut end = at + 1;
            while end < bytes.len()
                && (bytes[end].is_ascii_digit()
                    || matches!(bytes[end], b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                end += 1;
            }
            let token = &bytes[at..end];
            let overflow = token.iter().any(|byte| matches!(byte, b'.' | b'e' | b'E'))
                && serde_json::from_slice::<serde_json::Number>(token).is_ok()
                && std::str::from_utf8(token)
                    .ok()
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some_and(f64::is_infinite);
            (
                end,
                overflow.then_some(if token.first() == Some(&b'-') {
                    "-Infinity"
                } else {
                    "Infinity"
                }),
            )
        } else {
            (at + 1, None)
        };
        let replacement = insert.map(|name| format!("{{\"{marker}\":\"{name}\"}}"));
        let part = replacement
            .as_deref()
            .map(str::as_bytes)
            .unwrap_or(&bytes[at..end]);
        let length = out
            .len()
            .checked_add(part.len())
            .filter(|n| *n <= maximum)
            .ok_or(Fault::Capacity)?;
        if length > out.capacity() {
            out.try_reserve_exact(
                out.capacity().saturating_mul(2).max(length).min(maximum) - out.len(),
            )
            .map_err(|_| Fault::Capacity)?;
        }
        out.extend_from_slice(part);
        at = end;
    }
    Ok((out, marker))
}
/// Admission for canonical PostgreSQL JSON, avoiding a second copied payload
/// and rejecting node/string amplification before allocating retained Values.
pub(super) fn admitted_value(bytes: &[u8], maximum: usize) -> Result<Value, Fault> {
    use serde::de::DeserializeSeed;
    let mut remaining = maximum;
    let mut exhausted = false;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    if (Admission {
        remaining: &mut remaining,
        depth: 0,
        exhausted: &mut exhausted,
    })
    .deserialize(&mut decoder)
    .and_then(|()| decoder.end())
    .is_err()
    {
        return Err(if exhausted {
            Fault::Capacity
        } else {
            Fault::Remote
        });
    }
    serde_json::from_slice(bytes).map_err(|_| Fault::Remote)
}
fn bounded_json(bytes: &[u8], maximum: usize) -> Result<Value, Fault> {
    let (prepared, marker) = json_nonfinite(bytes, maximum)?;
    let mut value = admitted_value(&prepared, maximum)?;
    // Never turn an accepted Python nonfinite value into a fabricated cached200.
    // Its DB/partial-write behavior remains an explicit future Source gate.
    let mut pending = vec![&value];
    while let Some(value) = pending.pop() {
        match value {
            Value::Number(number) if number.to_string().contains(['.', 'e', 'E']) => {
                if number.as_f64().is_none_or(|n| !n.is_finite()) {
                    return Err(Fault::UncapturedNumeric);
                }
            }
            Value::Array(values) => pending.extend(values),
            Value::Object(values) => pending.extend(values.values()),
            _ => {}
        }
    }
    crate::json_compat::normalize_input_numbers(&mut value).map_err(|_| Fault::Remote)?;
    let mut pending = vec![&mut value];
    while let Some(value) = pending.pop() {
        if let Some(token) = value
            .as_object()
            .filter(|object| object.len() == 1)
            .and_then(|object| object.get(&marker))
            .and_then(Value::as_str)
        {
            *value = Value::Number(serde_json::Number::from_string_unchecked(token.to_owned()));
        } else {
            match value {
                Value::Object(map) => pending.extend(map.values_mut()),
                Value::Array(array) => pending.extend(array),
                _ => {}
            }
        }
    }
    Ok(value)
}
/// Requests selects an explicit HTTP charset first. For an unlabelled non-text
/// response, Response.json probes JSON Unicode BOM/NUL patterns before text.
fn response_text(bytes: &[u8], content_type: &str, maximum: usize) -> Result<Vec<u8>, Fault> {
    let lower = content_type.to_ascii_lowercase();
    let charset = lower.split(';').skip(1).find_map(|p| {
        p.trim()
            .strip_prefix("charset=")
            .map(|s| s.trim_matches(['\"', '\'']).trim())
    });
    let json = lower
        .split(';')
        .next()
        .is_some_and(|s| s.trim() == "application/json");
    let mut input = bytes;
    let encoding = if let Some(charset) = charset {
        charset
    } else if lower.starts_with("text/") {
        "latin1"
    } else if json {
        "utf8"
    } else if bytes.starts_with(&[0xef, 0xbb, 0xbf]) {
        input = &bytes[3..];
        "utf8"
    } else if bytes.starts_with(&[0xff, 0xfe, 0, 0]) {
        input = &bytes[4..];
        "utf32le"
    } else if bytes.starts_with(&[0, 0, 0xfe, 0xff]) {
        input = &bytes[4..];
        "utf32be"
    } else if bytes.starts_with(&[0xff, 0xfe]) {
        input = &bytes[2..];
        "utf16le"
    } else if bytes.starts_with(&[0xfe, 0xff]) {
        input = &bytes[2..];
        "utf16be"
    } else if bytes.len() >= 4 && bytes[0] == 0 && bytes[1] == 0 && bytes[2] == 0 {
        "utf32be"
    } else if bytes.len() >= 4 && bytes[1] == 0 && bytes[2] == 0 && bytes[3] == 0 {
        "utf32le"
    } else if bytes.len() >= 4 && bytes[0] == 0 && bytes[2] == 0 {
        "utf16be"
    } else if bytes.len() >= 4 && bytes[1] == 0 && bytes[3] == 0 {
        "utf16le"
    } else {
        "utf8"
    };
    let mut output = String::new();
    let mut append = |ch: char| -> Result<(), Fault> {
        let needed = output
            .len()
            .checked_add(ch.len_utf8())
            .filter(|n| *n <= maximum)
            .ok_or(Fault::Capacity)?;
        if needed > output.capacity() {
            output
                .try_reserve_exact(
                    output.capacity().saturating_mul(2).max(needed).min(maximum) - output.len(),
                )
                .map_err(|_| Fault::Capacity)?;
        }
        output.push(ch);
        Ok(())
    };
    match encoding {
        "utf-8" | "utf8" | "utf_8" => {
            let mut remaining = input;
            while !remaining.is_empty() {
                match std::str::from_utf8(remaining) {
                    Ok(text) => {
                        for ch in text.chars() {
                            append(ch)?;
                        }
                        break;
                    }
                    Err(error) => {
                        for ch in std::str::from_utf8(&remaining[..error.valid_up_to()])
                            .unwrap()
                            .chars()
                        {
                            append(ch)?;
                        }
                        append('\u{fffd}')?;
                        let consumed = error.valid_up_to()
                            + error
                                .error_len()
                                .unwrap_or(remaining.len() - error.valid_up_to());
                        remaining = &remaining[consumed..];
                    }
                }
            }
        }
        "latin1" | "latin-1" | "iso-8859-1" | "iso8859-1" => {
            for byte in input {
                append(char::from(*byte))?;
            }
        }
        "utf16le" | "utf-16le" | "utf16be" | "utf-16be" => {
            let little = encoding.ends_with("le");
            for ch in char::decode_utf16(input.chunks_exact(2).map(|b| {
                if little {
                    u16::from_le_bytes([b[0], b[1]])
                } else {
                    u16::from_be_bytes([b[0], b[1]])
                }
            })) {
                append(ch.unwrap_or('\u{fffd}'))?;
            }
            if !input.len().is_multiple_of(2) {
                append('\u{fffd}')?;
            }
        }
        "utf32le" | "utf32be" => {
            let little = encoding.ends_with("le");
            for bytes in input.chunks_exact(4) {
                let bytes: [u8; 4] = bytes.try_into().unwrap();
                let code = if little {
                    u32::from_le_bytes(bytes)
                } else {
                    u32::from_be_bytes(bytes)
                };
                append(char::from_u32(code).unwrap_or('\u{fffd}'))?;
            }
            if !input.len().is_multiple_of(4) {
                append('\u{fffd}')?;
            }
        }
        _ => return Err(Fault::UncapturedText),
    }
    Ok(output.into_bytes())
}
/// Internal invariant fault only. Keep the actual Response and physical token
/// owned without polling/decoding before grants. No success or drain is claimed;
/// a future typed cleanup capability requires separate review and fault gates.
async fn retain_unsettled_response<T>(
    response: reqwest::Response,
    guard: Arc<dyn Send + Sync>,
    error: ReadError,
) -> Result<T, Fault> {
    tracing::error!(
        ?error,
        "refresh continuation invariant failed; unsettled response retained"
    );
    std::future::pending::<()>().await;
    drop(response);
    drop(guard);
    Err(Fault::Control(error))
}
fn strip_url_credentials(url: &mut url::Url) -> Result<(), Fault> {
    url.set_password(None).map_err(|_| Fault::Remote)?;
    url.set_username("").map_err(|_| Fault::Remote)
}
fn source_cookie_context(url: &url::Url) -> Result<url::Url, Fault> {
    let mut context = url.clone();
    if !url.username().is_empty() || url.password().is_some() {
        // Python cookiejar.request_host uses netloc.split(':',1)[0], which
        // includes the username when the URL contains Basic userinfo.
        // Keep this cookie-only context separate from the actual socket URL.
        let authority = url
            .as_str()
            .split_once("://")
            .ok_or(Fault::Remote)?
            .1
            .split('/')
            .next()
            .ok_or(Fault::Remote)?;
        let host = authority
            .split(':')
            .next()
            .ok_or(Fault::Remote)?
            .to_ascii_lowercase();
        context
            .set_host(Some(&host))
            .map_err(|_| Fault::UncapturedText)?;
        context.set_port(None).map_err(|_| Fault::Remote)?;
    }
    strip_url_credentials(&mut context)?;
    Ok(context)
}
fn basic_authorization(url: &url::Url) -> Result<Vec<(String, String)>, Fault> {
    if url.username().is_empty() && url.password().is_none_or(str::is_empty) {
        return Ok(Vec::new());
    }
    fn latin1_unquoted(value: &str) -> Result<Vec<u8>, Fault> {
        let raw = value.as_bytes();
        let mut bytes = Vec::with_capacity(raw.len());
        let mut at = 0;
        while at < raw.len() {
            if raw[at] == b'%' && at + 2 < raw.len() {
                let hex = |byte: u8| char::from(byte).to_digit(16);
                if let (Some(a), Some(b)) = (hex(raw[at + 1]), hex(raw[at + 2])) {
                    bytes.push((a * 16 + b) as u8);
                    at += 3;
                    continue;
                }
            }
            bytes.push(raw[at]);
            at += 1;
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| Fault::Remote)?;
        text.chars()
            .map(|ch| u8::try_from(u32::from(ch)).map_err(|_| Fault::Remote))
            .collect()
    }
    let mut secret = latin1_unquoted(url.username())?;
    secret.push(b':');
    secret.extend(latin1_unquoted(url.password().unwrap_or_default())?);
    use base64::Engine;
    Ok(vec![(
        "Authorization".into(),
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(secret)
        ),
    )])
}
fn strip_redirect_auth(old: &url::Url, new: &url::Url) -> bool {
    if old.host_str() != new.host_str() {
        return true;
    }
    if old.scheme() == "http"
        && new.scheme() == "https"
        && old.port_or_known_default() == Some(80)
        && new.port_or_known_default() == Some(443)
    {
        return false;
    }
    old.scheme() != new.scheme() || old.port_or_known_default() != new.port_or_known_default()
}
struct Session<'a> {
    work: &'a ReadWork,
    attempt: Arc<Attempt>,
    agent: String,
    maximum: usize,
    _physical: Arc<dyn Send + Sync>,
}
impl Session<'_> {
    async fn json(&self, url: &url::Url) -> Result<Value, Fault> {
        self.json_observed(url, None).await
    }
    async fn json_observed(
        &self,
        url: &url::Url,
        observer: Option<Arc<dyn super::vod_api_provider_refresh_body::Observer>>,
    ) -> Result<Value, Fault> {
        let mut current = url.clone();
        let mut headers = basic_authorization(&current)?;
        for followed in 0..=30 {
            let response = self.headers(&current, &headers).await?;
            let status = response.status().as_u16();
            let location = if matches!(status, 301 | 302 | 303 | 307 | 308) {
                response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .filter(|value| !value.as_bytes().is_empty())
                    .map(|value| value.as_bytes().to_vec())
            } else {
                None
            };
            if let Some(location) = location {
                // Source Requests resolves the next hop only after consuming
                // the intermediate body. Decoder/capacity failures retain the
                // raw pump until terminal completion before returning.
                let deadline = self.work.control_deadline();
                let discard = self.discard_reason(&response);
                let consumed = super::vod_api_provider_refresh_body::consume_redirect(
                    response,
                    Arc::new((self._physical.clone(), self.attempt.clone())),
                    self.maximum,
                    deadline,
                    discard,
                )
                .await;
                match consumed {
                    Ok(()) | Err(Fault::BodyDecode) => {}
                    Err(error) => return Err(error),
                }

                if self.attempt.cookie_budget_exceeded() {
                    return Err(Fault::Capacity);
                }
                if followed == 30 {
                    return Err(Fault::Remote);
                }
                let target = std::str::from_utf8(&location).map_err(|_| Fault::Remote)?;
                let next = current.join(target).map_err(|_| Fault::Remote)?;
                if !matches!(next.scheme(), "http" | "https") {
                    return Err(Fault::Remote);
                }
                if strip_redirect_auth(&current, &next) {
                    headers.clear();
                }
                current = next;
                continue;
            }
            return self.payload(response, observer).await;
        }
        Err(Fault::Remote)
    }
    async fn headers(
        &self,
        url: &url::Url,
        headers: &[(String, String)],
    ) -> Result<reqwest::Response, Fault> {
        let cookie_url = source_cookie_context(url)?;
        let mut request_url = url.clone();
        strip_url_credentials(&mut request_url)?;
        let mut headers = headers.to_vec();
        if let Some(cookie) = self.attempt.session_cookie_header(&cookie_url) {
            headers.push(("Cookie".into(), cookie));
        }
        // Construct all bounded GET builders before lending active credits.
        // Cookie injection still occurs at send time from this same session.
        let requests = (0..4)
            .map(|_| {
                self.attempt
                    .request(request_url.as_str(), &self.agent, &headers)
                    .map_err(|_| Fault::Remote)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.work.before_headers().await.map_err(Fault::Control)?;
        let deadline = self.work.control_deadline();
        let mut response = Err(Fault::Remote);
        for (index, request) in requests.into_iter().enumerate() {
            let Some(remaining) = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .filter(|duration| !duration.is_zero())
            else {
                response = Err(Fault::Control(ReadError::Deadline));
                break;
            };
            if let Err(error) = self.work.start_auth_attempt() {
                response = Err(Fault::Control(error));
                break;
            }
            match request.timeout(remaining).send().await {
                Ok(value) => {
                    response = Ok(value);
                    break;
                }
                Err(_error) if tokio::time::Instant::now() >= deadline => {
                    response = Err(Fault::Control(ReadError::Deadline));
                    break;
                }
                Err(error) if index < 3 && !error.is_builder() && !error.is_redirect() => {}
                Err(_) => {
                    response = Err(Fault::Remote);
                    break;
                }
            }
        }
        // No Response exists for a control failure before/at send termination;
        // leave error cleanup to the owner without acquiring a pointless frame.
        if matches!(&response, Err(Fault::Control(_))) {
            return Err(response.err().unwrap());
        }
        if let Err(error) = self.work.resume_active().await {
            return match response {
                Ok(response) => {
                    retain_unsettled_response(response, self._physical.clone(), error).await
                }
                Err(_) => Err(Fault::Control(error)),
            };
        }
        if let Ok(response) = &response {
            self.attempt
                .store_session_cookies(response.headers(), &cookie_url);
        }
        response
    }
    fn discard_reason(&self, response: &reqwest::Response) -> Option<Fault> {
        if tokio::time::Instant::now() >= self.work.control_deadline() {
            Some(Fault::Control(ReadError::Deadline))
        } else if response
            .content_length()
            .is_some_and(|length| length > self.maximum as u64)
        {
            Some(Fault::Capacity)
        } else {
            None
        }
    }
    async fn payload(
        &self,
        response: reqwest::Response,
        observer: Option<Arc<dyn super::vod_api_provider_refresh_body::Observer>>,
    ) -> Result<Value, Fault> {
        let deadline = self.work.control_deadline();
        let status_error = (400..600).contains(&response.status().as_u16());
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let discard = self.discard_reason(&response);
        let physical: Arc<dyn Send + Sync> =
            Arc::new((self._physical.clone(), self.attempt.clone()));
        let bytes = if observer.is_some() {
            super::vod_api_provider_refresh_body::collect_inner(
                response,
                physical,
                self.maximum,
                deadline,
                discard,
                observer,
                true,
            )
            .await
        } else {
            super::vod_api_provider_refresh_body::collect(
                response,
                physical,
                self.maximum,
                deadline,
                discard,
            )
            .await
        }?;
        if self.attempt.cookie_budget_exceeded() {
            return Err(Fault::Capacity);
        }
        // Requests.get(stream=False) consumes/decodes even HTTP-error bodies
        // before XCClient calls raise_for_status. Keep the same phase order.
        if status_error {
            return Err(Fault::Remote);
        }
        let text = response_text(&bytes, &content_type, self.maximum)?;
        drop(bytes);
        let value = bounded_json(&text, self.maximum * 2)?;
        if value.as_object().is_some_and(|object| {
            object.get("user_info").is_none_or(Value::is_null) && object.contains_key("error")
        }) {
            return Err(Fault::Remote);
        }
        Ok(value)
    }
}
/// The existing owned VOD worker supplies all memory/physical admission; no
/// second pool/semaphore or caller-detached unleased task is introduced here.
/// Once auth starts, preserve the admitted Source operation through info reply.
pub(super) async fn fetch(
    work: &ReadWork,
    account: &Value,
    agent: &str,
    identifier: &str,
    series: bool,
) -> Result<Value, Fault> {
    work.check().map_err(Fault::Control)?;
    fetch_admitted(work, account, agent, identifier, series).await
}
/// Only the already Source-qualified mutation actor enters here. Its physical
/// refresh continues after caller loss; no new global/background owner exists.
pub(super) async fn fetch_admitted(
    work: &ReadWork,
    account: &Value,
    agent: &str,
    identifier: &str,
    series: bool,
) -> Result<Value, Fault> {
    let mut url = endpoint(account["server_url"].as_str().ok_or(Fault::Remote)?)?;
    if identifier.len() > work.row_bytes() || agent.len() > work.row_bytes() {
        return Err(Fault::Capacity);
    }
    let session = Session {
        work,
        attempt: Arc::new(
            Attempt::new_manual_session(Duration::from_secs(60), Duration::from_secs(60))
                .map_err(|_| Fault::Remote)?,
        ),
        agent: agent.to_owned(),
        maximum: work.work_bytes() / 8,
        _physical: work.transport_guard().map_err(Fault::Control)?.erased(),
    };
    {
        let mut query = url.query_pairs_mut();
        for field in ["username", "password"] {
            if !account[field].is_null() {
                let value = account[field].as_str().ok_or(Fault::Remote)?;
                query.append_pair(field, value);
            }
        }
    }
    // Requests omits an empty params mapping entirely. query_pairs_mut creates
    // a bare '?' even when both optional credentials are None; remove that one
    // empty query without changing explicit empty-string credential values.
    if url.query() == Some("") {
        url.set_query(None);
    }
    let auth = session.json(&url).await?;
    if !auth
        .as_object()
        .and_then(|v| v.get("user_info"))
        .is_some_and(super::vod_api_projection::truthy)
    {
        return Err(Fault::Remote);
    }
    url.query_pairs_mut()
        .append_pair(
            "action",
            if series {
                "get_series_info"
            } else {
                "get_vod_info"
            },
        )
        .append_pair(if series { "series_id" } else { "vod_id" }, identifier);
    drop(auth);
    let data = session.json(&url).await?;
    if !data.is_object() {
        return Err(Fault::Remote);
    }
    if super::vod_api_actions_account::heap(&data).map_err(|_| Fault::Capacity)?
        > work.work_bytes() / 3
    {
        return Err(Fault::Capacity);
    }
    Ok(data)
}
impl From<Fault> for ReadError {
    fn from(fault: Fault) -> Self {
        match fault {Fault::Control(error)=>error,Fault::UncapturedText=>ReadError::Unsupported("Uncaptured provider response character encoding"),Fault::UncapturedNumeric=>ReadError::Unsupported("Non-finite provider JSON mutation phases unported"),Fault::Capacity=>ReadError::Capacity,Fault::Remote|Fault::BodyDecode=>ReadError::Unsupported("Refresh HTTP failed; Source task fallback must be applied by the not-yet-ported mutation actor")}
    }
}
#[cfg(test)]
pub(super) async fn observed_test_json(
    work: &ReadWork,
    address: &str,
    observer: Arc<dyn super::vod_api_provider_refresh_body::Observer>,
) -> Result<Value, Fault> {
    let session = Session {
        work,
        attempt: Arc::new(
            Attempt::new_manual_session(Duration::from_secs(5), Duration::from_secs(5))
                .map_err(|_| Fault::Remote)?,
        ),
        agent: "fixture-agent".into(),
        maximum: 4096,
        _physical: work.transport_guard().map_err(Fault::Control)?.erased(),
    };
    session
        .json_observed(
            &url::Url::parse(address).map_err(|_| Fault::Remote)?,
            Some(observer),
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "Owned Original local-provider protocol fixture only"]
    async fn native_refresh_http_peer() {
        use std::io::Read;
        let mut input = String::new();
        std::io::stdin().read_to_string(&mut input).unwrap();
        assert!(input.len() < 256 * 1024);
        let config: Value = serde_json::from_str(&input).unwrap();
        assert_eq!(config["fixture_guard"], "playback-fixture-only");
        let endpoint = endpoint(config["account"]["server_url"].as_str().unwrap()).unwrap();
        assert_eq!(endpoint.host_str(), Some("127.0.0.1"));
        let runtime = super::super::vod_api_owned_read::ReadRuntime::new(
            super::super::vod_api_owned_read::ReadLimits::default(),
        )
        .unwrap();
        let frame = runtime
            .run(1, move |work| async move {
                let result = fetch(
                    &work,
                    &config["account"],
                    config["agent"].as_str().unwrap(),
                    config["identifier"].as_str().unwrap(),
                    config["series"].as_bool().unwrap(),
                )
                .await;
                let report = match result {
                    Ok(value) => serde_json::json!({"outcome":"success","value":value}),
                    Err(Fault::Control(error)) => {
                        serde_json::json!({"outcome":"control","error":format!("{error:?}")})
                    }
                    Err(Fault::Remote | Fault::BodyDecode) => {
                        serde_json::json!({"outcome":"remote"})
                    }
                    Err(Fault::Capacity) => serde_json::json!({"outcome":"capacity"}),
                    Err(Fault::UncapturedText) => serde_json::json!({"outcome":"unsupported-text"}),
                    Err(Fault::UncapturedNumeric) => {
                        serde_json::json!({"outcome":"uncaptured-numeric"})
                    }
                };
                Ok(serde_json::to_vec(&report).unwrap())
            })
            .await
            .unwrap();
        let report: Value = serde_json::from_slice(&frame).unwrap();
        drop(frame);
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.physical_workers(), 0);
        assert_eq!(runtime.retained_memory_bytes(), 0);
        println!("REFRESH_TRANSPORT_RESULT={report}");
    }
    #[test]
    fn source_observed_charset_replacement_and_nonfinite_partial_sql_values() {
        assert_eq!(
            response_text(
                b"{\"plot\":\"caf\xe9\"}",
                "application/json; charset=iso-8859-1",
                1024
            )
            .unwrap(),
            "{\"plot\":\"café\"}".as_bytes()
        );
        assert_eq!(
            response_text(b"{\"plot\":\"caf\xe9\"}", "application/json", 1024).unwrap(),
            "{\"plot\":\"caf�\"}".as_bytes()
        );
        let text = "{\"plot\":\"café\"}";
        let mut utf16 = vec![0xff, 0xfe];
        for unit in text.encode_utf16() {
            utf16.extend(unit.to_le_bytes());
        }
        assert_eq!(
            response_text(&utf16, "application/octet-stream", 1024).unwrap(),
            text.as_bytes()
        );
        let value=bounded_json(br#"{"__dispatcharr_provider_nonfinite_\u0030__":"ordinary","number":NaN,"literal":"NaN"}"#,4096).unwrap();
        assert_eq!(value["number"].as_number().unwrap().to_string(), "NaN");
        assert_eq!(value["literal"], "NaN");
        assert_eq!(value["__dispatcharr_provider_nonfinite_0__"], "ordinary");
        let edge = bounded_json(
            br#"{"__dispatcharr_provider_nonfinite_18446744073709551615__":"kept","number":NaN}"#,
            4096,
        )
        .unwrap();
        assert_eq!(
            edge["__dispatcharr_provider_nonfinite_18446744073709551615__"],
            "kept"
        );
        assert_eq!(edge["number"].as_number().unwrap().to_string(), "NaN");
        assert!(matches!(
            response_text(b"\xff\xff", "application/json", 2),
            Err(Fault::Capacity)
        ));
    }
    #[test]
    fn node_admission_precedes_retained_tree() {
        assert!(matches!(
            bounded_json(br#"[null,null,null,null]"#, 256),
            Err(Fault::Capacity)
        ));
        assert_eq!(
            bounded_json(br#"{"x":1.0}"#, 4096).unwrap(),
            serde_json::json!({"x":1.0})
        );
        // Actual Source9 movie/series overflow-float cases save rating="inf"
        // before later Source SQL phases. Retain the Python nonfinite value.
        let overflow = bounded_json(br#"1e309"#, 4096).unwrap();
        assert_eq!(
            super::super::vod_api_provider_refresh_value::text(&overflow).unwrap(),
            "inf"
        );
        assert!(matches!(
            bounded_json(br#"{bad}"#, 4096),
            Err(Fault::Remote)
        ));
    }
    #[test]
    fn source_xc_endpoint_shapes() {
        for (raw, expected) in [
            ("http://example.test/", "http://example.test/player_api.php"),
            (
                "http://example.test/owned/base/pasted.php?discard=one#fragment",
                "http://example.test/owned/base/player_api.php",
            ),
            (
                "http://example.test/owned/base///",
                "http://example.test/owned/base/player_api.php",
            ),
        ] {
            assert_eq!(endpoint(raw).unwrap().as_str(), expected);
        }
    }
}
