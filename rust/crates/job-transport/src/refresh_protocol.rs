//! Observed Original refresh protocol, bounded and component-only.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
use crate::{
    protocol::{self, ProtocolError, Task},
    worker,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Datelike, Timelike, Utc};
use dispatcharr_coordination::MissingRefreshSource;
use serde_json::{json, Map, Value};
use uuid::Uuid;
pub(crate) const NAME: &str = "apps.epg.tasks.refresh_epg_data";
pub(crate) const HARD_TIME_LIMIT_SECONDS: u32 = 14_400;
fn invalid() -> ProtocolError {
    ProtocolError::InvalidArgument
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshCall {
    pub args: Vec<Value>,
    pub kwargs: Map<String, Value>,
    pub list_args: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshMessage {
    pub task_id: Uuid,
    pub source_id: i64,
    pub force: bool,
    pub retry: u32,
    pub root: Uuid,
    pub parent: Option<Uuid>,
    pub eta: Option<DateTime<Utc>>,
    pub call: RefreshCall,
}
pub(crate) fn missing_source_argument(value: &Value) -> Option<MissingRefreshSource> {
    if value.is_null() {
        return Some(MissingRefreshSource::Null);
    }
    match value.as_i64()? {
        0 => Some(MissingRefreshSource::Zero),
        -1 => Some(MissingRefreshSource::MinusOne),
        _ => None,
    }
}

impl RefreshMessage {
    pub(crate) fn missing_api_source(&self) -> Option<MissingRefreshSource> {
        self.call.missing_api_source()
    }

    pub(crate) fn source_spelling(&self) -> String {
        self.missing_api_source().map_or_else(
            || self.source_id.to_string(),
            |source| source.source_spelling().to_owned(),
        )
    }

    /// Exact immediate import API arguments, after protocol parsing has
    /// established the actual published message fields.
    pub(crate) fn has_explicit_api_arguments(&self) -> bool {
        let [argument] = self.call.args.as_slice() else {
            return false;
        };
        let source_matches = argument.as_i64() == Some(self.source_id)
            || (!self.force
                && self.retry == 0
                && self.eta.is_none()
                && self.parent.is_none()
                && self.root == self.task_id
                && argument.as_str().and_then(canonical_decimal_source_id) == Some(self.source_id));
        self.source_id > 0
            && !self.call.list_args
            && source_matches
            && self.call.kwargs.len() == 1
            && self.call.kwargs.get("force").and_then(Value::as_bool) == Some(self.force)
    }
}
/// Canonical spelling is also the exact Source lock and provider-group suffix.
pub(crate) fn canonical_decimal_source_id(value: &str) -> Option<i64> {
    if value.len() > 10
        || !value
            .as_bytes()
            .first()
            .is_some_and(|&b| matches!(b, b'1'..=b'9'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse::<i32>().ok().map(i64::from)
}

impl RefreshCall {
    pub fn initial(source_id: i64) -> Self {
        Self {
            args: vec![json!(source_id)],
            kwargs: Map::new(),
            list_args: false,
        }
    }
    pub fn deferred(source_id: i64, force: bool, retry: u32) -> Self {
        Self {
            args: vec![json!(source_id)],
            kwargs: json!({"force":force,"_file_defer_retry":retry})
                .as_object()
                .unwrap()
                .clone(),
            list_args: true,
        }
    }
    fn has_decimal_source(&self) -> bool {
        self.args.first().is_some_and(Value::is_string)
    }

    fn missing_api_source(&self) -> Option<MissingRefreshSource> {
        let [value] = self.args.as_slice() else {
            return None;
        };
        if self.list_args
            || self.kwargs.len() != 1
            || self.kwargs.get("force") != Some(&Value::Bool(false))
        {
            return None;
        }
        missing_source_argument(value)
    }

    fn immediate_only(&self) -> bool {
        self.has_decimal_source() || self.missing_api_source().is_some()
    }

    pub fn resolve(&self) -> Result<(i64, bool, u32), ProtocolError> {
        if let Some(source) = self.missing_api_source() {
            // The legacy numeric field is only used to verify the wire message
            // here. The caller resolves the typed argument before any SQL lookup.
            return Ok((source.lookup().unwrap_or(0), false, 0));
        }
        if self.has_decimal_source() {
            if self.list_args
                || self.args.len() != 1
                || self.kwargs.len() != 1
                || self.kwargs.get("force") != Some(&Value::Bool(false))
            {
                return Err(invalid());
            }
            let source = self.args[0]
                .as_str()
                .and_then(canonical_decimal_source_id)
                .ok_or_else(invalid)?;
            return Ok((source, false, 0));
        }
        if self.args.len() > 3 || self.kwargs.len() > 3 {
            return Err(invalid());
        }
        let names = ["source_id", "force", "_file_defer_retry"];
        if self.kwargs.keys().any(|k| !names.contains(&k.as_str())) {
            return Err(invalid());
        }
        for k in names.iter().take(self.args.len()) {
            if self.kwargs.contains_key(*k) {
                return Err(invalid());
            }
        }
        let get = |i: usize| self.args.get(i).or_else(|| self.kwargs.get(names[i]));
        let source = get(0)
            .and_then(Value::as_i64)
            .filter(|v| *v > 0)
            .ok_or_else(invalid)?;
        let force = get(1)
            .map(|v| v.as_bool().ok_or_else(invalid))
            .transpose()?
            .unwrap_or(false);
        let retry = get(2)
            .map(|v| v.as_u64().filter(|v| *v <= 480).ok_or_else(invalid))
            .transpose()?
            .unwrap_or(0);
        Ok((source, force, retry as u32))
    }
    fn repr(&self) -> Result<(String, String), ProtocolError> {
        self.resolve()?;
        let literal = |v: &Value| {
            if v.is_null() {
                "None".to_owned()
            } else if let Some(b) = v.as_bool() {
                if b {
                    "True".to_owned()
                } else {
                    "False".to_owned()
                }
            } else if let Some(decimal) = v.as_str() {
                format!("'{decimal}'")
            } else {
                v.to_string()
            }
        };
        let joined = self.args.iter().map(literal).collect::<Vec<_>>().join(", ");
        let args = if self.list_args {
            format!("[{joined}]")
        } else if self.args.len() == 1 {
            format!("({joined},)")
        } else {
            format!("({joined})")
        };
        let kwargs = format!(
            "{{{}}}",
            self.kwargs
                .iter()
                .map(|(k, v)| format!("'{k}': {}", literal(v)))
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok((args, kwargs))
    }
}
pub fn encode(
    message: &RefreshMessage,
    reply: Uuid,
    delivery: Uuid,
    origin: &str,
) -> Result<Vec<u8>, ProtocolError> {
    let resolved = message.call.resolve()?;
    if resolved != (message.source_id, message.force, message.retry)
        || message.parent.is_none() && message.root != message.task_id
        || message.call.immediate_only() && (message.parent.is_some() || message.eta.is_some())
    {
        return Err(invalid());
    }
    if message
        .eta
        .is_some_and(|v| v.nanosecond() % 1000 != 0 || !(1..=9999).contains(&v.year()))
    {
        return Err(invalid());
    }
    let mut e: Value = serde_json::from_slice(&protocol::encode(
        &Task::CreateBackup,
        message.task_id,
        reply,
        delivery,
        origin,
    )?)
    .map_err(|_| invalid())?;
    let (args, kwargs) = message.call.repr()?;
    e["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([message.call.args,message.call.kwargs,{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|invalid())?));
    let h = &mut e["headers"];
    h["task"] = json!(NAME);
    h["root_id"] = json!(message.root.to_string());
    h["parent_id"] = message.parent.map_or(Value::Null, |v| json!(v.to_string()));
    h["eta"] = message.eta.map_or(Value::Null, |v| {
        json!(if v.nanosecond() == 0 {
            v.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
        } else {
            v.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
        })
    });
    h["timelimit"] = json!([HARD_TIME_LIMIT_SECONDS, null]);
    h["argsrepr"] = json!(args);
    h["kwargsrepr"] = json!(kwargs);
    let bytes = serde_json::to_vec(&e).map_err(|_| invalid())?;
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    Ok(bytes)
}
pub fn parse(bytes: &[u8]) -> Result<RefreshMessage, ProtocolError> {
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    let mut e: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    // Actual Source periodic worker34 preserves this bounded marker only in
    // task.request.headers; ordinary result metadata/body semantics are unchanged.
    let periodic = crate::periodic_headers::take(&mut e)?;
    let h = &e["headers"];
    if h["task"] != NAME || h["timelimit"] != json!([HARD_TIME_LIMIT_SECONDS, null]) {
        return Err(invalid());
    }
    let uuid = |v: &Value| Uuid::parse_str(v.as_str().ok_or_else(invalid)?).map_err(|_| invalid());
    let task_id = uuid(&h["id"])?;
    let root = uuid(&h["root_id"])?;
    let parent = if h["parent_id"].is_null() {
        None
    } else {
        Some(uuid(&h["parent_id"])?)
    };
    if parent.is_none() && root != task_id {
        return Err(invalid());
    }
    let eta = if h["eta"].is_null() {
        None
    } else {
        let d = DateTime::parse_from_rfc3339(h["eta"].as_str().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        if d.offset().local_minus_utc() != 0
            || d.nanosecond() % 1000 != 0
            || !(1..=9999).contains(&d.year())
        {
            return Err(invalid());
        }
        Some(d.with_timezone(&Utc))
    };
    let raw = STANDARD
        .decode(e["body"].as_str().ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    if raw.len() > 32768 {
        return Err(ProtocolError::Capacity);
    }
    let body: Value = serde_json::from_slice(&raw).map_err(|_| invalid())?;
    let parts = body
        .as_array()
        .filter(|v| v.len() == 3)
        .ok_or_else(invalid)?;
    if parts[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null}) {
        return Err(invalid());
    }
    let call = RefreshCall {
        args: parts[0].as_array().ok_or_else(invalid)?.clone(),
        kwargs: parts[1].as_object().ok_or_else(invalid)?.clone(),
        list_args: h["argsrepr"].as_str().is_some_and(|v| v.starts_with('[')),
    };
    let (source_id, force, retry) = call.resolve()?;
    if call.immediate_only() && (periodic.is_some() || parent.is_some() || eta.is_some()) {
        return Err(invalid());
    }
    let (args, kwargs) = call.repr()?;
    if h["argsrepr"] != args || h["kwargsrepr"] != kwargs {
        return Err(invalid());
    }
    // Reuse captured common options without widening task, timing, lineage or body.
    let h = &mut e["headers"];
    h["task"] = json!("apps.backups.tasks.create_backup_task");
    h["root_id"] = json!(task_id.to_string());
    h["parent_id"] = Value::Null;
    h["eta"] = Value::Null;
    h["timelimit"] = json!([null, null]);
    h["argsrepr"] = json!("()");
    h["kwargsrepr"] = json!("{}");
    e["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| invalid())?
    ));
    worker::create_identity(&serde_json::to_vec(&e).map_err(|_| invalid())?)
        .map_err(|_| invalid())?;
    Ok(RefreshMessage {
        task_id,
        source_id,
        force,
        retry,
        root,
        parent,
        eta,
        call,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    fn corpus() -> Value {
        serde_json::from_str(include_str!("../datasets/source-epg-refresh-import-2.json")).unwrap()
    }
    #[test]
    fn actual_source_default_positional_keyword_and_deferred_forms_roundtrip() {
        let mut count = 0;
        for row in corpus()["messages"].as_array().unwrap() {
            let e = &row["envelope"];
            if e["headers"]["task"] != NAME {
                continue;
            }
            let m = parse(&serde_json::to_vec(e).unwrap()).unwrap();
            let p = &e["properties"];
            let native = encode(
                &m,
                Uuid::parse_str(p["reply_to"].as_str().unwrap()).unwrap(),
                Uuid::parse_str(p["delivery_tag"].as_str().unwrap()).unwrap(),
                e["headers"]["origin"].as_str().unwrap(),
            )
            .unwrap();
            let mut n: Value = serde_json::from_slice(&native).unwrap();
            let mut original = e.clone();
            // JSON body whitespace is not wire-byte equality: compare decoded
            // structure, while every envelope/header/property field stays exact.
            for frame in [&mut n, &mut original] {
                frame["body"] = serde_json::from_slice::<Value>(
                    &STANDARD.decode(frame["body"].as_str().unwrap()).unwrap(),
                )
                .unwrap();
            }
            assert_eq!(n, original);
            assert_eq!(parse(&native).unwrap(), m);
            count += 1
        }
        assert_eq!(count, 11)
    }
    #[test]
    fn canonical_decimal_api_root_keeps_source_wire_and_integer_lookup() {
        let task = Uuid::parse_str("fd998eb9-5f75-4abe-987e-a10db69fa42b").unwrap();
        let mut message = RefreshMessage {
            task_id: task,
            source_id: 3,
            force: false,
            retry: 0,
            root: task,
            parent: None,
            eta: None,
            call: RefreshCall {
                args: vec![json!("3")],
                kwargs: json!({"force": false}).as_object().unwrap().clone(),
                list_args: false,
            },
        };
        let reply = Uuid::parse_str("2b75ad35-87aa-4caa-aeba-072f156a76b2").unwrap();
        let delivery = Uuid::parse_str("09564d08-b39e-469b-90ec-985855f450a9").unwrap();
        let wire = encode(&message, reply, delivery, "gen1@localhost").unwrap();
        let envelope: Value = serde_json::from_slice(&wire).unwrap();
        let body: Value =
            serde_json::from_slice(&STANDARD.decode(envelope["body"].as_str().unwrap()).unwrap())
                .unwrap();
        assert_eq!(body[0], json!(["3"]));
        assert_eq!(envelope["headers"]["argsrepr"], "('3',)");
        assert_eq!(envelope["headers"]["kwargsrepr"], "{'force': False}");
        let parsed = parse(&wire).unwrap();
        assert_eq!(parsed, message);
        assert!(parsed.has_explicit_api_arguments());
        message.call.args[0] = json!("4");
        assert!(!message.has_explicit_api_arguments());
        message.call.args[0] = json!("3");
        assert_eq!(
            message.source_id.to_string(),
            message.call.args[0].as_str().unwrap()
        );
        for value in [
            json!("03"),
            json!("+3"),
            json!(" 3"),
            json!("0"),
            json!("2147483648"),
            json!(true),
            json!(3.0),
            Value::Null,
        ] {
            message.call.args[0] = value;
            assert!(encode(&message, reply, delivery, "gen1@localhost").is_err());
        }
        message.call.args[0] = json!("3");
        message.call.kwargs.insert("force".into(), json!(true));
        assert!(encode(&message, reply, delivery, "gen1@localhost").is_err());
        message.call.kwargs.insert("force".into(), json!(false));
        message.parent = Some(reply);
        assert!(encode(&message, reply, delivery, "gen1@localhost").is_err());
    }
    #[test]
    fn qualified_missing_api_roots_roundtrip_the_exact_source_argument() {
        for (wire, source, argsrepr) in [
            (Value::Null, MissingRefreshSource::Null, "(None,)"),
            (json!(0), MissingRefreshSource::Zero, "(0,)"),
            (json!(-1), MissingRefreshSource::MinusOne, "(-1,)"),
        ] {
            let task = Uuid::new_v4();
            let mut message = RefreshMessage {
                task_id: task,
                root: task,
                parent: None,
                eta: None,
                source_id: source.lookup().unwrap_or(0),
                force: false,
                retry: 0,
                call: RefreshCall {
                    args: vec![wire.clone()],
                    kwargs: json!({"force":false}).as_object().unwrap().clone(),
                    list_args: false,
                },
            };
            let packed = |value: &RefreshMessage| {
                encode(value, Uuid::new_v4(), Uuid::new_v4(), "owned-test")
            };
            let raw = packed(&message).unwrap();
            assert_eq!(parse(&raw).unwrap(), message);
            assert_eq!(message.missing_api_source(), Some(source));
            assert_eq!(message.source_spelling(), source.source_spelling());
            assert!(!message.has_explicit_api_arguments());
            let mut frame: Value = serde_json::from_slice(&raw).unwrap();
            assert_eq!(frame["headers"]["argsrepr"], argsrepr);
            let decoded = STANDARD.decode(frame["body"].as_str().unwrap()).unwrap();
            let arguments: Value = serde_json::from_slice(&decoded).unwrap();
            assert_eq!(arguments[0], json!([wire]));
            assert_eq!(arguments[1], json!({"force":false}));

            frame["headers"]["periodic_task_name"] = json!("owned period");
            assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_err());
            message.parent = Some(Uuid::new_v4());
            assert!(packed(&message).is_err());
            message.parent = None;
            message.eta = Some(
                DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
            );
            assert!(packed(&message).is_err());
        }
    }

    #[test]
    fn missing_api_roots_do_not_admit_unmeasured_force_or_id_shapes() {
        let exact_force = json!({"force":false}).as_object().unwrap().clone();
        for value in [
            json!(-2),
            json!(true),
            json!(0.0),
            json!("0"),
            json!("-1"),
            json!([]),
        ] {
            let call = RefreshCall {
                args: vec![value],
                kwargs: exact_force.clone(),
                list_args: false,
            };
            assert!(call.resolve().is_err());
        }
        for kwargs in [
            json!({}),
            json!({"force": true}),
            json!({"force":null}),
            json!({"force":false,"_file_defer_retry":1}),
        ] {
            let call = RefreshCall {
                args: vec![Value::Null],
                kwargs: kwargs.as_object().unwrap().clone(),
                list_args: false,
            };
            assert!(call.resolve().is_err());
        }
        let list = RefreshCall {
            args: vec![json!(-1)],
            kwargs: exact_force,
            list_args: true,
        };
        assert!(list.resolve().is_err());
    }

    #[test]
    fn argument_types_duplicates_unknowns_and_bounds_are_rejected() {
        for (args, kwargs) in [
            (json!([1, true, 481]), json!({})),
            (json!([1]), json!({"source_id":1})),
            (json!([1, 1]), json!({})),
            (json!([0]), json!({})),
            (json!([1]), json!({"x":0})),
            (json!([]), json!({"source_id":1.5})),
            (json!([1, false, 1, 0]), json!({})),
        ] {
            let c = RefreshCall {
                args: args.as_array().unwrap().clone(),
                kwargs: kwargs.as_object().unwrap().clone(),
                list_args: false,
            };
            assert!(c.resolve().is_err())
        }
        assert!(parse(&vec![b' '; protocol::MAX_ENVELOPE + 1]).is_err());
        assert_eq!(
            RefreshCall::initial(i64::MAX).resolve().unwrap(),
            (i64::MAX, false, 0)
        );
        assert_eq!(
            RefreshCall::deferred(1, true, 480).resolve().unwrap(),
            (1, true, 480)
        );
    }
    #[test]
    fn unsupported_options_wrong_family_and_repr_are_not_normalized_away() {
        let c = corpus();
        let e = &c["messages"][0]["envelope"];
        for (field, value) in [
            ("task", json!("other.family")),
            ("argsrepr", json!("[999]")),
            ("timelimit", json!([null, null])),
            ("retries", json!(1)),
            ("expires", json!("2026-11-11")),
        ] {
            let mut n = e.clone();
            n["headers"][field] = value;
            assert!(parse(&serde_json::to_vec(&n).unwrap()).is_err(), "{field}")
        }
    }
    #[test]
    fn source_microsecond_precision_and_zero_fraction_are_explicit() {
        let rows = corpus();
        let e = rows["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| !v["envelope"]["headers"]["eta"].is_null())
            .unwrap();
        let mut m = parse(&serde_json::to_vec(&e["envelope"]).unwrap()).unwrap();
        m.eta = Some(DateTime::from_timestamp(1_800_000_000, 0).unwrap());
        let encoded = encode(&m, Uuid::new_v4(), Uuid::new_v4(), "fixture").unwrap();
        let value: Value = serde_json::from_slice(&encoded).unwrap();
        assert!(!value["headers"]["eta"].as_str().unwrap().contains('.'));
        assert_eq!(parse(&encoded).unwrap(), m);
        m.eta = Some(DateTime::from_timestamp(1_800_000_000, 1).unwrap());
        assert!(encode(&m, Uuid::new_v4(), Uuid::new_v4(), "fixture").is_err());
    }
}
