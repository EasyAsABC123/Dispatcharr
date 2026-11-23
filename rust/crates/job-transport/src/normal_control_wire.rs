//! Pinned Celery/Kombu JSON-only no-reply control envelopes.
//! Original project and library authors retain their credit and licenses.
use super::Error;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
pub(super) const MAX_BODY: usize = 64 * 1024;
pub(super) const MAX_FRAME: usize = 128 * 1024;
pub(super) const MAX_KEYS: usize = 128;

pub(super) fn valid_keys(keys: &[String]) -> bool {
    keys.len() <= MAX_KEYS
        && keys.iter().all(|key| {
            !key.is_empty() && key.len() <= 255 && key.bytes().all(|byte| byte.is_ascii_graphic())
        })
}
pub(super) fn topic(database: i64) -> Result<String, Error> {
    if !(0..=65535).contains(&database) {
        return Err(Error::Configuration);
    }
    Ok(format!("/{database}.celery.pidbox"))
}
pub(super) fn encode(
    keys: &[String],
    single: bool,
    clock: u64,
    now: f64,
) -> Result<Vec<u8>, Error> {
    if !valid_keys(keys) || (single && keys.len() != 1) || !now.is_finite() {
        return Err(Error::Capacity);
    }
    let task_id = if single { json!(keys[0]) } else { json!(keys) };
    let body = serde_json::to_vec(&json!({
        "method":"revoke", "arguments":{"task_id":task_id,"terminate":false,"signal":"SIGTERM"},
        "destination":null,"pattern":null,"matcher":null
    }))
    .map_err(|_| Error::Protocol)?;
    if body.len() > MAX_BODY {
        return Err(Error::Capacity);
    }
    let frame = serde_json::to_vec(&json!({
        "body":STANDARD.encode(body), "content-encoding":"utf-8","content-type":"application/json",
        "headers":{"clock":clock,"expires":now+1.0},
        "properties":{"delivery_mode":2,"delivery_info":{"exchange":"celery.pidbox","routing_key":""},
            "priority":0,"body_encoding":"base64","delivery_tag":uuid::Uuid::new_v4().to_string()}
    })).map_err(|_| Error::Protocol)?;
    if frame.len() > MAX_FRAME {
        return Err(Error::Capacity);
    }
    Ok(frame)
}

pub(super) struct Decoded {
    pub clock: u64,
    pub keys: Option<Vec<String>>,
    pub pause_default: Option<bool>,
}
#[cfg(test)]
pub(super) fn decode(raw: &[u8], now: f64) -> Result<Decoded, Error> {
    decode_for(raw, now, None)
}

pub(super) fn decode_for(
    raw: &[u8],
    now: f64,
    queue_control: Option<(&str, f64)>,
) -> Result<Decoded, Error> {
    if raw.len() > MAX_FRAME {
        return Err(Error::Capacity);
    }
    let outer: Value = serde_json::from_slice(raw).map_err(|_| Error::Protocol)?;
    if outer["content-type"] != "application/json"
        || outer["content-encoding"] != "utf-8"
        || outer["properties"]["body_encoding"] != "base64"
    {
        return Err(Error::Unsupported);
    }
    let clock = outer["headers"]["clock"].as_u64().unwrap_or(0);
    let expires = outer["headers"]["expires"].as_f64().unwrap_or(0.0);
    if expires != 0.0 && expires < now {
        return Ok(Decoded {
            clock,
            keys: None,
            pause_default: None,
        });
    }
    let encoded = outer["body"].as_str().ok_or(Error::Protocol)?;
    if encoded.len() > MAX_BODY.div_ceil(3) * 4 {
        return Err(Error::Capacity);
    }
    let bytes = STANDARD.decode(encoded).map_err(|_| Error::Protocol)?;
    if bytes.len() > MAX_BODY {
        return Err(Error::Capacity);
    }
    let body: Value = serde_json::from_slice(&bytes).map_err(|_| Error::Protocol)?;
    if let Some((target, enabled_at)) = queue_control {
        // Celery's default no-reply timeout is exactly one second. Reject
        // messages published before activation even if a socket backlog only
        // delivers them now, as well as messages using a longer expiry.
        const SOURCE_TIMEOUT: f64 = 1.0;
        if expires.is_finite()
            && expires >= now
            && expires <= now + SOURCE_TIMEOUT
            && expires >= enabled_at + SOURCE_TIMEOUT
            && clock > 0
            && body.as_object().is_some_and(|map| {
                map.len() == 5
                    && ["method", "arguments", "destination", "pattern", "matcher"]
                        .iter()
                        .all(|key| map.contains_key(*key))
            })
            && body["destination"] == json!([target])
            && body["pattern"].is_null()
            && body["matcher"].is_null()
        {
            let pause_default = match body["method"].as_str() {
                Some("cancel_consumer") if body["arguments"] == json!({"queue": "celery"}) => {
                    Some(true)
                }
                Some("add_consumer")
                    if body["arguments"]
                        == json!({
                            "queue": "celery", "exchange": null, "exchange_type": "direct", "routing_key": null
                        }) =>
                {
                    Some(false)
                }
                _ => None,
            };
            if pause_default.is_some() {
                return Ok(Decoded {
                    clock,
                    keys: None,
                    pause_default,
                });
            }
        }
    }
    // A no-reply broadcast to every worker is the actual recovery fallback.
    // Other commands/destinations/patterns and RPC are not silently accepted.
    if body["method"] != "revoke"
        || !body["destination"].is_null()
        || !body["pattern"].is_null()
        || !body["matcher"].is_null()
        || !body["reply_to"].is_null()
    {
        return Ok(Decoded {
            clock,
            keys: None,
            pause_default: None,
        });
    }
    if body["arguments"]["terminate"].as_bool() != Some(false) {
        return Err(Error::Unsupported);
    }
    let keys = match &body["arguments"]["task_id"] {
        Value::String(key) => vec![key.clone()],
        Value::Array(keys) if keys.len() <= MAX_KEYS => keys
            .iter()
            .map(|v| v.as_str().map(str::to_owned).ok_or(Error::Protocol))
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(Error::Protocol),
    };
    if !valid_keys(&keys) {
        return Err(Error::Capacity);
    }
    Ok(Decoded {
        clock,
        keys: Some(keys),
        pause_default: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn source_frame(method: &str, arguments: Value, destination: Value, expires: f64) -> Vec<u8> {
        let body = json!({"method": method, "arguments": arguments, "destination": destination,
                         "pattern": null, "matcher": null});
        source_body_frame(body, expires)
    }

    fn source_body_frame(body: Value, expires: f64) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "content-type":"application/json", "content-encoding":"utf-8",
            "properties":{"body_encoding":"base64"},
            "headers":{"clock":7,"expires":expires},
            "body":STANDARD.encode(serde_json::to_vec(&body).unwrap()),
        }))
        .unwrap()
    }

    #[test]
    fn exact_no_reply_original_caller_shapes_are_scoped_to_one_receiver() {
        let cancel = source_frame(
            "cancel_consumer",
            json!({"queue":"celery"}),
            json!(["default@owned"]),
            101.0,
        );
        let add = source_frame(
            "add_consumer",
            json!({"queue":"celery","exchange":null,
                        "exchange_type":"direct","routing_key":null}),
            json!(["default@owned"]),
            101.0,
        );
        for (frame, expected) in [(&cancel, true), (&add, false)] {
            let decoded = decode_for(frame, 100.5, Some(("default@owned", 100.0))).unwrap();
            assert_eq!(decoded.pause_default, Some(expected));
            assert!(decoded.keys.is_none());
            assert!(decode_for(frame, 100.5, None)
                .unwrap()
                .pause_default
                .is_none());
            assert!(decode_for(frame, 100.5, Some(("default@elsewhere", 100.0)))
                .unwrap()
                .pause_default
                .is_none());
            assert!(decode_for(frame, 102.0, Some(("default@owned", 100.0)))
                .unwrap()
                .pause_default
                .is_none());
        }
        for (method, arguments, destination, expires) in [
            (
                "cancel_consumer",
                json!({"queue":"dvr"}),
                json!(["default@owned"]),
                101.0,
            ),
            (
                "cancel_consumer",
                json!({"queue":"celery"}),
                json!(null),
                101.0,
            ),
            (
                "cancel_consumer",
                json!({"queue":"celery"}),
                json!(["default@owned", "default@elsewhere"]),
                101.0,
            ),
            (
                "cancel_consumer",
                json!({"queue":"celery"}),
                json!(["default@owned"]),
                0.0,
            ),
            (
                "add_consumer",
                json!({"queue":"celery","exchange":"other","exchange_type":"direct","routing_key":null}),
                json!(["default@owned"]),
                101.0,
            ),
        ] {
            let raw = source_frame(method, arguments, destination, expires);
            let decoded = decode_for(&raw, 100.5, Some(("default@owned", 100.0))).unwrap();
            assert!(decoded.pause_default.is_none());
            assert!(decoded.keys.is_none());
        }
    }

    #[test]
    fn default_control_rejects_pre_activation_backlog_and_extended_expiry() {
        let target = Some(("default@owned", 100.25));
        for (expires, expected) in [(101.0, None), (101.5, Some(true)), (102.5, None)] {
            let frame = source_frame(
                "cancel_consumer",
                json!({"queue":"celery"}),
                json!(["default@owned"]),
                expires,
            );
            assert_eq!(
                decode_for(&frame, 100.5, target).unwrap().pause_default,
                expected
            );
        }
    }

    #[test]
    fn default_control_rejects_reply_fields_and_extra_arguments() {
        let cancel = json!({
            "method": "cancel_consumer", "arguments": {"queue": "celery"},
            "destination": ["default@owned"], "pattern": null, "matcher": null
        });
        for (key, value) in [
            ("ticket", json!("a-reply-ticket")),
            (
                "reply_to",
                json!({"exchange":"pidbox.reply", "routing_key":"owned"}),
            ),
            ("pattern", json!("default@*")),
            ("arguments", json!({"queue":"celery", "exchange":"extra"})),
        ] {
            let mut frame = cancel.clone();
            frame[key] = value;
            let raw = source_body_frame(frame, 101.0);
            let decoded = decode_for(&raw, 100.5, Some(("default@owned", 100.0))).unwrap();
            assert!(decoded.pause_default.is_none());
            assert!(decoded.keys.is_none());
        }
    }

    #[test]
    fn exact_string_identity_single_and_batch_expiration_are_preserved() {
        let keys = vec![
            "dvr-recover-42".into(),
            "00000000-0000-4000-8000-000000000001".into(),
        ];
        let frame = encode(&keys, false, 3, 100.0).unwrap();
        let decoded = decode(&frame, 100.5).unwrap();
        assert_eq!(decoded.clock, 3);
        assert_eq!(decoded.keys.unwrap(), keys);
        let opted = decode_for(&frame, 100.5, Some(("default@owned", 100.0))).unwrap();
        assert!(opted.pause_default.is_none());
        assert_eq!(opted.keys.unwrap(), keys);
        assert!(decode(&frame, 102.0).unwrap().keys.is_none());
        let single = encode(&keys[..1], true, 4, 100.0).unwrap();
        let outer: Value = serde_json::from_slice(&single).unwrap();
        let body: Value =
            serde_json::from_slice(&STANDARD.decode(outer["body"].as_str().unwrap()).unwrap())
                .unwrap();
        assert_eq!(body["arguments"]["task_id"], "dvr-recover-42");
        assert_eq!(body["arguments"]["terminate"], false);
    }
    #[test]
    fn control_does_not_accept_termination_or_oversized_identity_as_revoke() {
        let keys = vec!["dvr-now-7-0123456789abcdef0123456789abcdef".into()];
        let frame = encode(&keys, true, 1, 100.0).unwrap();
        let mut outer: Value = serde_json::from_slice(&frame).unwrap();
        let mut body: Value =
            serde_json::from_slice(&STANDARD.decode(outer["body"].as_str().unwrap()).unwrap())
                .unwrap();
        body["arguments"]["terminate"] = json!(true);
        outer["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
        assert_eq!(
            decode(&serde_json::to_vec(&outer).unwrap(), 100.5).err(),
            Some(Error::Unsupported)
        );
        assert!(encode(&["x".repeat(256)], true, 1, 100.0).is_err());
        assert!(decode(&vec![b' '; MAX_FRAME + 1], 100.0).is_err());
    }
}
