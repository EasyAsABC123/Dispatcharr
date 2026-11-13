//! Bounded data-only parser for the actually captured Source2 Beat→dvr frame.
//! Dispatcharr, Celery and Kombu authors retain application/protocol credit.
//! Source2 failed before DVR execution: no media/result/lease/due authority.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{json, Value};
use std::collections::HashSet;
use uuid::Uuid;

pub const MAX_FRAME_BYTES: usize = 16_384;
pub const MAX_BODY_BYTES: usize = 2_048;
const TASK: &str = "apps.channels.tasks.run_recording";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    Capacity,
    Malformed,
    DuplicateKey,
    UnsupportedEnvelope,
    Identity,
    Argument,
    Date,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnqualifiedDueExecution;

/// This is observed producer data, not a runnable task or a scheduler lease.
/// In particular, no `now` predicate or FutureRecording constructor is reused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedProducerEnvelope {
    raw: Vec<u8>,
    task_id: Uuid,
    recording_id: i64,
    channel_id: i64,
    periodic_name: String,
    start_text: String,
    end_text: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}
impl ObservedProducerEnvelope {
    pub fn raw_frame(&self) -> &[u8] {
        &self.raw
    }
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }
    pub fn recording_id(&self) -> i64 {
        self.recording_id
    }
    pub fn channel_id(&self) -> i64 {
        self.channel_id
    }
    pub fn periodic_name(&self) -> &str {
        &self.periodic_name
    }
    pub fn start_text(&self) -> &str {
        &self.start_text
    }
    pub fn end_text(&self) -> &str {
        &self.end_text
    }
    pub fn start(&self) -> DateTime<Utc> {
        self.start
    }
    pub fn end(&self) -> DateTime<Utc> {
        self.end
    }
    /// An envelope alone never qualifies due execution, even with valid dates.
    pub fn require_due_execution(&self) -> Result<(), UnqualifiedDueExecution> {
        Err(UnqualifiedDueExecution)
    }
}
fn fields(value: &Value, names: &[&str]) -> bool {
    value.as_object().is_some_and(|map| {
        map.len() == names.len() && names.iter().all(|name| map.contains_key(*name))
    })
}
fn canonical(value: &Value, version: usize) -> Result<Uuid, Refusal> {
    let text = value.as_str().ok_or(Refusal::Identity)?;
    let id = Uuid::parse_str(text).map_err(|_| Refusal::Identity)?;
    if id.to_string() != text
        || id.get_version_num() != version
        || id.get_variant() != uuid::Variant::RFC4122
    {
        return Err(Refusal::Identity);
    }
    Ok(id)
}
fn positive_id(value: &Value) -> Result<i64, Refusal> {
    let id = value.as_i64().ok_or(Refusal::Argument)?;
    if !(1..=i64::from(i32::MAX)).contains(&id) {
        return Err(Refusal::Argument);
    }
    Ok(id)
}

/// Captured Python str(datetime): whole seconds, SPACE and explicit UTC offset.
/// Fractional/T/Z/other offsets/date-only/naive/leap-second inputs are open.
fn source_date(text: &str) -> Result<DateTime<Utc>, Refusal> {
    let b = text.as_bytes();
    if b.len() != 25
        || &b[19..] != b"+00:00"
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b' '
        || b[13] != b':'
        || b[16] != b':'
        || b.iter()
            .enumerate()
            .any(|(i, c)| ![4, 7, 10, 13, 16, 19, 22].contains(&i) && !c.is_ascii_digit())
    {
        return Err(Refusal::Date);
    }
    let number = |from: usize, to: usize| text[from..to].parse::<u32>().map_err(|_| Refusal::Date);
    let year = number(0, 4)?;
    if !(1..=9999).contains(&year) {
        return Err(Refusal::Date);
    }
    let day =
        NaiveDate::from_ymd_opt(year as i32, number(5, 7)?, number(8, 10)?).ok_or(Refusal::Date)?;
    Ok(day
        .and_hms_opt(number(11, 13)?, number(14, 16)?, number(17, 19)?)
        .ok_or(Refusal::Date)?
        .and_utc())
}

// Preallocation is bounded by frame/body byte caps. Scan before Value allocation
// to refuse excessive nesting and duplicate decoded object keys, including
// escaped spellings. This is not a second JSON parser: serde validates grammar.
enum Container {
    Object(HashSet<String>),
    Array,
}
fn json_value(bytes: &[u8]) -> Result<Value, Refusal> {
    let mut stack = Vec::new();
    let mut at = 0usize;
    let mut nodes = 0usize;
    while at < bytes.len() {
        match bytes[at] {
            b'{' | b'[' => {
                nodes += 1;
                if stack.len() >= 8 || nodes > 512 {
                    return Err(Refusal::Capacity);
                }
                stack.push(if bytes[at] == b'{' {
                    Container::Object(HashSet::new())
                } else {
                    Container::Array
                });
            }
            b'}' => {
                if !matches!(stack.pop(), Some(Container::Object(_))) {
                    return Err(Refusal::Malformed);
                }
            }
            b']' => {
                if !matches!(stack.pop(), Some(Container::Array)) {
                    return Err(Refusal::Malformed);
                }
            }
            b'"' => {
                nodes += 1;
                if nodes > 512 {
                    return Err(Refusal::Capacity);
                }
                let start = at;
                at += 1;
                loop {
                    let c = *bytes.get(at).ok_or(Refusal::Malformed)?;
                    if c == b'"' {
                        break;
                    }
                    if c == b'\\' {
                        at += 1;
                        if at >= bytes.len() {
                            return Err(Refusal::Malformed);
                        }
                    }
                    at += 1;
                }
                let mut after = at + 1;
                while bytes.get(after).is_some_and(u8::is_ascii_whitespace) {
                    after += 1;
                }
                if bytes.get(after) == Some(&b':') {
                    let key: String = serde_json::from_slice(&bytes[start..=at])
                        .map_err(|_| Refusal::Malformed)?;
                    if key.len() > 64 {
                        return Err(Refusal::Capacity);
                    }
                    match stack.last_mut() {
                        Some(Container::Object(keys)) => {
                            if !keys.insert(key) {
                                return Err(Refusal::DuplicateKey);
                            }
                        }
                        _ => return Err(Refusal::Malformed),
                    }
                }
            }
            _ => {}
        }
        at += 1;
    }
    if !stack.is_empty() {
        return Err(Refusal::Malformed);
    }
    serde_json::from_slice(bytes).map_err(|_| Refusal::Malformed)
}

/// Refusal is data-only: raw input is never rewritten, ACKed, removed or sent.
/// Default/legacy parsers and every task registry remain completely unchanged.
pub fn parse_head(raw: &[u8]) -> Result<ObservedProducerEnvelope, Refusal> {
    if raw.is_empty() || raw.len() > MAX_FRAME_BYTES {
        return Err(Refusal::Capacity);
    }
    let frame = json_value(raw)?;
    let h = &frame["headers"];
    let p = &frame["properties"];
    if !fields(
        &frame,
        &[
            "body",
            "content-encoding",
            "content-type",
            "headers",
            "properties",
        ],
    ) || !fields(
        h,
        &[
            "lang",
            "task",
            "id",
            "shadow",
            "eta",
            "expires",
            "group",
            "group_index",
            "retries",
            "timelimit",
            "root_id",
            "parent_id",
            "argsrepr",
            "kwargsrepr",
            "origin",
            "ignore_result",
            "replaced_task_nesting",
            "stamped_headers",
            "stamps",
            "periodic_task_name",
        ],
    ) || !fields(
        p,
        &[
            "correlation_id",
            "reply_to",
            "delivery_mode",
            "delivery_info",
            "priority",
            "body_encoding",
            "delivery_tag",
        ],
    ) {
        return Err(Refusal::UnsupportedEnvelope);
    }
    let task_id = canonical(&h["id"], 4)?;
    canonical(&p["reply_to"], 3)?;
    canonical(&p["delivery_tag"], 4)?;
    if frame["content-type"] != "application/json"
        || frame["content-encoding"] != "utf-8"
        || h["task"] != TASK
        || h["lang"] != "py"
        || h["root_id"] != task_id.to_string()
        || !h["parent_id"].is_null()
        || !h["eta"].is_null()
        || !h["expires"].is_null()
        || !h["shadow"].is_null()
        || !h["group"].is_null()
        || !h["group_index"].is_null()
        || h["retries"].as_i64() != Some(0)
        || h["ignore_result"].as_bool() != Some(false)
        || h["timelimit"] != json!([null, null])
        || h["replaced_task_nesting"].as_i64() != Some(0)
        || !h["stamped_headers"].is_null()
        || h["stamps"] != json!({})
        || h["kwargsrepr"] != "{}"
        || h["origin"]
            .as_str()
            .is_none_or(|s| s.is_empty() || s.len() > 128 || s.chars().any(char::is_control))
        || p["correlation_id"] != task_id.to_string()
        || p["body_encoding"] != "base64"
        || p["delivery_mode"].as_i64() != Some(2)
        || p["priority"].as_i64() != Some(0)
        || p["delivery_info"] != json!({"exchange":"","routing_key":"dvr"})
    {
        return Err(Refusal::UnsupportedEnvelope);
    }
    let encoded = frame["body"].as_str().ok_or(Refusal::Malformed)?;
    if encoded.len() > MAX_BODY_BYTES.div_ceil(3) * 4 {
        return Err(Refusal::Capacity);
    }
    let bytes = STANDARD.decode(encoded).map_err(|_| Refusal::Malformed)?;
    if bytes.len() > MAX_BODY_BYTES {
        return Err(Refusal::Capacity);
    }
    let body = json_value(&bytes)?;
    let args = body
        .get(0)
        .and_then(Value::as_array)
        .ok_or(Refusal::Argument)?;
    if args.len() != 4
        || !fields(body.get(1).ok_or(Refusal::Argument)?, &[])
        || body.as_array().is_none_or(|a| a.len() != 3)
        || body[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null})
    {
        return Err(Refusal::Argument);
    }
    let recording_id = positive_id(&args[0])?;
    let channel_id = positive_id(&args[1])?;
    let start_text = args[2].as_str().ok_or(Refusal::Date)?;
    let end_text = args[3].as_str().ok_or(Refusal::Date)?;
    let start = source_date(start_text)?;
    let end = source_date(end_text)?;
    let periodic_name = format!("dvr-recording-{recording_id}");
    if end <= start
        || h["periodic_task_name"] != periodic_name
        || h["argsrepr"] != format!("[{recording_id}, {channel_id}, '{start_text}', '{end_text}']")
    {
        return Err(Refusal::Argument);
    }
    Ok(ObservedProducerEnvelope {
        raw: raw.to_vec(),
        task_id,
        recording_id,
        channel_id,
        periodic_name,
        start_text: start_text.to_owned(),
        end_text: end_text.to_owned(),
        start,
        end,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn raw_source() -> Vec<u8> {
        let Ok(value) = serde_json::from_str::<Value>(include_str!(
            "../datasets/source-clocked-recording-producer.json"
        )) else {
            panic!("fixed actual Source2 producer dataset JSON");
        };
        assert_eq!(value["source_execution_verified"], false);
        let Some(encoded) = value["raw_frame_base64"].as_str() else {
            panic!("fixed actual raw frame field");
        };
        let Ok(raw) = STANDARD.decode(encoded) else {
            panic!("fixed Source base64");
        };
        raw
    }
    fn frame() -> Value {
        let Ok(frame) = serde_json::from_slice(&raw_source()) else {
            panic!("actual raw Source frame JSON");
        };
        frame
    }
    fn encode(value: &Value) -> Vec<u8> {
        let Ok(raw) = serde_json::to_vec(value) else {
            panic!("finite fixture JSON");
        };
        raw
    }
    fn args(frame: &Value) -> Value {
        let Some(encoded) = frame["body"].as_str() else {
            panic!("actual body");
        };
        let Ok(bytes) = STANDARD.decode(encoded) else {
            panic!("actual body base64");
        };
        let Ok(value) = serde_json::from_slice(&bytes) else {
            panic!("actual body JSON");
        };
        value
    }
    fn set_body(frame: &mut Value, body: &Value) {
        frame["body"] = json!(STANDARD.encode(encode(body)));
    }
    fn refused(frame: &Value) {
        assert!(parse_head(&encode(frame)).is_err());
    }

    #[test]
    fn actual_expired_source_wire_remains_exact_data_and_never_due_authority() {
        let raw = raw_source();
        let Ok(message) = parse_head(&raw) else {
            panic!("actual Source2 wire");
        };
        assert_eq!(message.raw_frame(), raw);
        assert_eq!(message.recording_id(), 1);
        assert_eq!(message.channel_id(), 2);
        assert_eq!(message.periodic_name(), "dvr-recording-1");
        assert_ne!(message.periodic_name(), message.task_id().to_string());
        assert_eq!(message.start_text(), "2026-11-13 14:53:28+00:00");
        assert_eq!(message.end_text(), "2026-11-13 14:53:58+00:00");
        assert_eq!((message.end() - message.start()).num_seconds(), 30);
        assert_eq!(
            message.require_due_execution(),
            Err(UnqualifiedDueExecution)
        );
    }
    #[test]
    fn unknown_task_queue_options_fields_and_named_identity_refuse() {
        let original = frame();
        for (key, value) in [
            ("task", json!("core.tasks.scan_and_process_files")),
            ("root_id", Value::Null),
            ("parent_id", original["headers"]["id"].clone()),
            ("periodic_task_name", original["headers"]["id"].clone()),
            ("periodic_task_name", json!("dvr-recording-2")),
            ("eta", json!("2026-11-13T14:53:28+00:00")),
            ("expires", json!("2026-11-14T00:00:00+00:00")),
            ("ignore_result", json!(true)),
            ("retries", json!(1)),
            ("retries", json!(false)),
            ("kwargsrepr", json!("{'force': True}")),
            ("argsrepr", json!("(1, 2)")),
        ] {
            let mut changed = original.clone();
            changed["headers"][key] = value;
            refused(&changed);
        }
        for queue in ["celery", "unknown"] {
            let mut changed = original.clone();
            changed["properties"]["delivery_info"]["routing_key"] = json!(queue);
            refused(&changed);
        }
        let mut changed = original.clone();
        changed["unknown"] = Value::Null;
        refused(&changed);
        let mut changed = original.clone();
        let Some(headers) = changed["headers"].as_object_mut() else {
            panic!("headers");
        };
        headers.remove("parent_id");
        refused(&changed);
    }
    #[test]
    fn canonical_ids_versions_and_relations_cannot_be_repaired() {
        let original = frame();
        for key in ["id", "root_id"] {
            let mut changed = original.clone();
            let Some(id) = original["headers"][key].as_str() else {
                panic!("ID");
            };
            changed["headers"][key] = json!(id.to_uppercase());
            refused(&changed);
        }
        for value in [
            json!("dvr-now-1-efa4ff914d8d4b65b80a6b3977c6beeb"),
            json!("efa4ff91-4d8d-1b65-b80a-6b3977c6beeb"),
            json!("efa4ff91-4d8d-4b65-080a-6b3977c6beeb"),
            json!("dvr-recording-1"),
        ] {
            let mut changed = original.clone();
            changed["headers"]["id"] = value;
            refused(&changed);
        }
        let mut changed = original.clone();
        changed["properties"]["correlation_id"] = json!("other");
        refused(&changed);
    }
    #[test]
    fn ids_bool_string_float_bounds_and_wrong_argument_count_refuse() {
        let original = frame();
        for index in [0, 1] {
            for value in [
                json!(true),
                json!("1"),
                json!(1.0),
                json!(0),
                json!(-1),
                json!(2_147_483_648_i64),
            ] {
                let mut changed = original.clone();
                let mut body = args(&changed);
                body[0][index] = value;
                set_body(&mut changed, &body);
                refused(&changed);
            }
        }
        for body in [
            json!([[1], {}, {}]),
            json!([[1, 2], {}, {}]),
            json!([[1,2,"x","y"],{"force":true},{}]),
        ] {
            let mut changed = original.clone();
            set_body(&mut changed, &body);
            refused(&changed);
        }
    }
    #[test]
    fn date_shape_calendar_and_order_are_strict_without_a_now_gate() {
        for value in [
            "2026-11-13T14:53:28+00:00",
            "2026-11-13 14:53:28Z",
            "2026-11-13 14:53:28",
            "2026-11-13",
            "2026-11-13 14:53:28.000000+00:00",
            "2026-11-13 14:53:28+01:00",
            "2026-02-29 14:53:28+00:00",
            "2026-11-13 14:53:60+00:00",
            "0000-11-13 14:53:28+00:00",
            "２０２６-11-13 14:53:28+00:00",
        ] {
            assert!(source_date(value).is_err(), "{value}");
        }
        let original = frame();
        for end in ["2026-11-13 14:53:28+00:00", "2026-11-13 14:53:27+00:00"] {
            let mut changed = original.clone();
            let mut body = args(&changed);
            body[0][3] = json!(end);
            set_body(&mut changed, &body);
            refused(&changed);
        }
    }
    #[test]
    fn capacity_depth_duplicates_and_decoded_alias_keys_refuse() {
        assert_eq!(
            parse_head(&vec![b' '; MAX_FRAME_BYTES + 1]),
            Err(Refusal::Capacity)
        );
        assert!(json_value(b"[[[[[[[[[0]]]]]]]]]").is_err());
        assert_eq!(
            json_value(br#"{"id":1,"id":2}"#),
            Err(Refusal::DuplicateKey)
        );
        assert_eq!(
            json_value(br#"{"id":1,"\u0069d":2}"#),
            Err(Refusal::DuplicateKey)
        );
        let mut changed = frame();
        changed["body"] = json!("A".repeat(MAX_BODY_BYTES * 2));
        refused(&changed);
    }
    #[test]
    fn every_refusal_leaves_caller_raw_bytes_unchanged() {
        let raw = b"{\"unknown\":true}".to_vec();
        let before = raw.clone();
        assert!(parse_head(&raw).is_err());
        assert_eq!(raw, before);
    }
}
