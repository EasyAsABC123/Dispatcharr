//! Source Connect WebhookHandler over the existing bounded Requests factory.
//! Original Dispatcharr/Requests authors retain all application algorithm credit.
use super::{Error, Payload};
use dispatcharr_upstream_http::Attempt;
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::time::Duration;
const BODY: usize = 1024 * 1024;
fn unsupported(message: &'static str) -> Error {
    Error::Unsupported(message)
}
fn quote(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(byte as char)
        } else if byte == b' ' {
            out.push('+')
        } else {
            use std::fmt::Write;
            write!(&mut out, "%{byte:02X}").unwrap();
        }
    }
    out
}
fn scalar(value: &Value) -> Result<String, Error> {
    match value {
        Value::String(v) => Ok(v.clone()),
        Value::Bool(v) => Ok(if *v { "True" } else { "False" }.into()),
        Value::Number(v) if v.is_i64() || v.is_u64() => Ok(v.to_string()),
        Value::Number(v) if v.is_f64() => v
            .as_f64()
            .filter(|value| value.is_finite())
            .map(crate::json_compat::float_string)
            .ok_or_else(|| unsupported("Connect form non-finite number unsupported")),
        _ => Err(unsupported(
            "Connect form scalar/iteration semantics unsupported",
        )),
    }
}
fn form(value: &Value) -> Result<String, Error> {
    let map = value
        .as_object()
        .ok_or_else(|| unsupported("Connect form payload must be an object"))?;
    let mut result = Vec::new();
    for (key, value) in map {
        if value.is_null() {
            continue;
        }
        // Requests expands iterable form values. Native EPG fields are scalar;
        // template-created iterable values remain explicit until characterized.
        result.push(format!("{}={}", quote(key), quote(&scalar(value)?)));
    }
    Ok(result.join("&"))
}
fn text(content_type: Option<&str>, bytes: &[u8]) -> Result<String, Error> {
    let mut tokens = content_type.unwrap_or("").split(';');
    let media = tokens.next().unwrap_or("").trim();
    let mut charset = None;
    for param in tokens {
        if let Some((key, value)) = param.trim().split_once('=') {
            if key
                .trim_matches(['\'', '"', ' '])
                .eq_ignore_ascii_case("charset")
            {
                charset = Some(value.trim_matches(['\'', '"', ' ']).to_ascii_lowercase());
            }
        }
    }
    let charset = charset.unwrap_or_else(|| {
        if media.contains("text") {
            "iso-8859-1".into()
        } else if media.contains("application/json") {
            "utf-8".into()
        } else {
            "".into()
        }
    });
    match charset.as_str() {
        "utf-8" | "utf8" | "utf_8" => Ok(String::from_utf8_lossy(bytes).into_owned()),
        "iso-8859-1" | "iso8859-1" | "latin-1" | "latin1" | "latin_1" => {
            Ok(bytes.iter().map(|v| char::from(*v)).collect())
        }
        "ascii" | "us-ascii" => Ok(bytes
            .iter()
            .map(|v| if *v < 128 { char::from(*v) } else { '\u{fffd}' })
            .collect()),
        // ASCII is invariant across Source's apparent-encoding candidates.
        "" if bytes.is_ascii() => Ok(String::from_utf8_lossy(bytes).into_owned()),
        _ => Err(unsupported(
            "Connect response charset/apparent-encoding domain unsupported",
        )),
    }
}
pub(super) async fn send(config: &Value, payload: &Payload) -> Result<Value, Error> {
    let config = config
        .as_object()
        .ok_or_else(|| unsupported("Connect webhook config is not an object"))?;
    let url = config
        .get("url")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty() && v.len() <= 65536)
        .ok_or_else(|| unsupported("Connect webhook URL domain unsupported"))?;
    let mut headers = Vec::<(String, String)>::new();
    match config.get("headers") {
        None | Some(Value::Null) => {}
        Some(Value::Object(values)) => {
            for (key, value) in values {
                let value = value.as_str().ok_or_else(|| {
                    unsupported("Connect nullable/non-string header domain unsupported")
                })?;
                headers.push((key.clone(), value.to_owned()));
            }
        }
        _ => return Err(unsupported("Connect webhook header container unsupported")),
    }
    let (body, content_type) = match payload {
        Payload::Original(value) => {
            let body = form(value)?;
            let ct = (!body.is_empty()).then_some("application/x-www-form-urlencoded");
            (body, ct)
        }
        Payload::Rendered {
            body,
            source_json_loads_succeeded,
        } => (
            body.clone(),
            source_json_loads_succeeded.then_some("application/json"),
        ),
    };
    if body.len() > 256 * 1024 {
        return Err(unsupported("Connect request body capacity"));
    }
    if let Some(content_type) = content_type {
        // A successful Source json.loads assignment replaces the literal
        // Content-Type key; Requests subsequently treats header names case-insensitively.
        let source_json = matches!(
            payload,
            Payload::Rendered {
                source_json_loads_succeeded: true,
                ..
            }
        );
        let existing = headers.iter_mut().find(|(key, _)| {
            if source_json {
                key == "Content-Type"
            } else {
                key.eq_ignore_ascii_case("content-type")
            }
        });
        if source_json {
            if let Some((_, value)) = existing {
                *value = content_type.into()
            } else {
                headers.push(("Content-Type".into(), content_type.into()));
            }
        } else if existing.is_none() {
            headers.push(("Content-Type".into(), content_type.into()));
        }
    }
    if headers.iter().any(|(key, _)| {
        key.eq_ignore_ascii_case("content-length") || key.eq_ignore_ascii_case("transfer-encoding")
    }) {
        return Err(unsupported(
            "Connect explicit request framing headers unsupported",
        ));
    }
    let attempt = Attempt::new_requests(Duration::from_secs(10), Duration::from_secs(10))
        .map_err(|_| unsupported("Connect Requests factory unavailable"))?;
    let builder = attempt
        .request_method(reqwest::Method::POST, url, "", &headers)
        .map_err(|_| unsupported("Connect Requests request shape unsupported"))?
        .body(body);
    let (client, request) = builder.build_split();
    let mut request = request.map_err(|error| Error::Http(Box::new(error)))?;
    // Requests merges configured headers case-insensitively over Session
    // defaults. Preserve factory validation/Latin1 conversion, then retain the
    // last actual value per name instead of sending duplicate default headers.
    let names = request.headers().keys().cloned().collect::<Vec<_>>();
    for name in names {
        let value = request
            .headers()
            .get_all(&name)
            .iter()
            .next_back()
            .cloned()
            .ok_or_else(|| unsupported("Connect request header disappeared"))?;
        request.headers_mut().insert(name, value);
    }
    let response = client
        .execute(request)
        .await
        .map_err(|error| Error::Http(Box::new(error)))?;
    if attempt.cookie_budget_exceeded() {
        return Err(unsupported("Connect redirect cookie capacity"));
    }
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .map(|v| v.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| unsupported("Connect response content-type bytes unsupported"))?;
    if response
        .headers()
        .get_all(reqwest::header::CONTENT_ENCODING)
        .iter()
        .any(|v| {
            v.to_str().ok().is_none_or(|v| {
                v.split(',').any(|part| {
                    !matches!(
                        part.trim().to_ascii_lowercase().as_str(),
                        "" | "identity" | "gzip" | "x-gzip" | "deflate"
                    )
                })
            })
        })
    {
        return Err(unsupported("Connect response content coding unsupported"));
    }
    let mut stream = dispatcharr_upstream_http::body_limited(response, BODY)
        .map_err(|error| Error::Body(Box::new(error)))?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| Error::Body(Box::new(error)))?;
        if bytes
            .len()
            .checked_add(chunk.len())
            .is_none_or(|v| v > BODY)
        {
            return Err(unsupported("Connect response body capacity"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let body = text(content_type.as_deref(), &bytes)?;
    Ok(json!({"status_code":status,"body":body,"success":!(400..600).contains(&status)}))
}
#[cfg(test)]
mod source_wire_controls {
    use super::*;
    #[test]
    fn requests_form_quoting_and_text_decode_are_not_json_or_browser_form_defaults() {
        assert_eq!(
            form(&json!({"source_name":"Guide *~+/& 東京","programs":0,"enabled":false})).unwrap(),
            "source_name=Guide+%2A~%2B%2F%26+%E6%9D%B1%E4%BA%AC&programs=0&enabled=False"
        );
        assert_eq!(
            text(Some("text/plain"), &[0xff, 0x80]).unwrap(),
            "ÿ\u{0080}"
        );
        assert_eq!(
            text(Some("application/json; charset=UTF-8"), b"\xe6\x9d\xb1").unwrap(),
            "東"
        );
        assert!(text(None, &[0xff]).is_err());
    }
}

#[cfg(test)]
mod actual_requests_peer {
    use super::*;
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        time::Instant,
    };
    fn peer(
        status: u16,
        content_type: &'static str,
        body: Vec<u8>,
    ) -> (String, std::thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let handle = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(8);
            let mut stream: TcpStream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "actual Connect request not received"
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("actual peer accept: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let count = stream.read(&mut buffer).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
                assert!(request.len() <= 16384);
                if let Some(end) = request.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            write!(stream,"HTTP/1.1 {status} Owned\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).unwrap();
            stream.write_all(&body).unwrap();
            stream.flush().unwrap();
            drop(stream);
            request
        });
        (format!("http://{address}/owned-connect?literal=1"), handle)
    }
    #[tokio::test]
    async fn production_factory_posts_form_with_one_configured_header_and_preserves_http_failure_result(
    ) {
        let (url, peer) = peer(
            503,
            "text/plain; charset=utf-8",
            b"owned server rejection".to_vec(),
        );
        let result=send(&json!({"url":url,"headers":{"User-Agent":"Owned Connect/1","Content-Type":"owned/form"}}),&Payload::Original(json!({"source_name":"Guide *~+/&","programs":0}))).await.unwrap();
        let request = String::from_utf8(peer.join().unwrap()).unwrap();
        let (header, body) = request.split_once("\r\n\r\n").unwrap();
        assert!(header.starts_with("POST /owned-connect?literal=1 HTTP/1.1\r\n"));
        assert_eq!(
            header
                .lines()
                .filter(|v| v.to_ascii_lowercase().starts_with("user-agent:"))
                .count(),
            1
        );
        assert!(header
            .lines()
            .any(|v| v.eq_ignore_ascii_case("user-agent: Owned Connect/1")));
        assert!(header
            .lines()
            .any(|v| v.eq_ignore_ascii_case("content-type: owned/form")));
        assert_eq!(body, "source_name=Guide+%2A~%2B%2F%26&programs=0");
        assert_eq!(
            result,
            json!({"status_code":503,"body":"owned server rejection","success":false})
        );
    }
    #[tokio::test]
    async fn sends_typed_disconnect_duration_as_python_form_text() {
        let (url, peer) = peer(200, "text/plain; charset=utf-8", b"accepted".to_vec());
        let event = json!({"client_id": "owned viewer", "duration": 1.25, "bytes_sent": 75_200});
        let result = send(&json!({"url": url}), &Payload::Original(event.clone()))
            .await
            .unwrap();
        let request = String::from_utf8(peer.join().unwrap()).unwrap();
        let (header, body) = request.split_once("\r\n\r\n").unwrap();
        assert!(header.lines().any(
            |line| line.eq_ignore_ascii_case("content-type: application/x-www-form-urlencoded")
        ));
        assert_eq!(
            body,
            "client_id=owned+viewer&duration=1.25&bytes_sent=75200"
        );
        assert_eq!(
            result,
            json!({"status_code": 200, "body": "accepted", "success": true})
        );
        assert!(event["duration"].is_f64());
        assert_eq!(form(&json!({"duration": 1.0})).unwrap(), "duration=1.0");
    }

    #[tokio::test]
    async fn rendered_source_json_flag_changes_header_without_reserializing_body_and_latin1_is_not_windows1252(
    ) {
        let (url, peer) = peer(200, "text/plain", vec![0xff, 0x80]);
        let result = send(
            &json!({"url":url,"headers":{"Content-Type":"old"}}),
            &Payload::Rendered {
                body: "{\"value\":NaN}".into(),
                source_json_loads_succeeded: true,
            },
        )
        .await
        .unwrap();
        let request = String::from_utf8(peer.join().unwrap()).unwrap();
        let (header, body) = request.split_once("\r\n\r\n").unwrap();
        assert!(header
            .lines()
            .any(|v| v.eq_ignore_ascii_case("content-type: application/json")));
        assert_eq!(body, "{\"value\":NaN}");
        assert_eq!(
            result,
            json!({"status_code":200,"body":"ÿ\u{0080}","success":true})
        );
    }
}
