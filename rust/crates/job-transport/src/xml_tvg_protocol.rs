//! Exact normal XML TVG Source task signature; original Celery authors credit.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Xml,
}
#[derive(Clone)]
pub(crate) struct Message {
    pub common: crate::normal_xc_live::Message,
    pub kind: Kind,
    pub target: i64,
    pub force: bool,
    pub defer: i64,
    pub eta: Option<DateTime<Utc>>,
}
impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XmlTvgMessage")
            .field("id", &self.common.id)
            .field("kind", &self.kind)
            .field("target", &self.target)
            .field("force", &self.force)
            .field("defer", &self.defer)
            .field("eta", &self.eta)
            .finish()
    }
}
pub(crate) fn parse_eta(raw: &[u8]) -> Result<Message, ()> {
    let message = parse(raw)?;
    if message.eta.is_none() {
        return Err(());
    }
    Ok(message)
}
fn repr(v: &Value) -> Result<String, ()> {
    match v {
        Value::Bool(v) => Ok(if *v { "True" } else { "False" }.into()),
        Value::Number(n) if n.as_i64().is_some() => Ok(n.to_string()),
        _ => Err(()),
    }
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    // Preserve the existing normal TVG canonical lineage contract before any
    // family-projection parser can normalize UUID spelling.
    let canonical = |value: &Value| -> Result<uuid::Uuid, ()> {
        let text = value.as_str().ok_or(())?;
        let id = uuid::Uuid::parse_str(text).map_err(|_| ())?;
        if id.is_nil() || id.to_string() != text || id.get_variant() != uuid::Variant::RFC4122 {
            return Err(());
        }
        Ok(id)
    };
    let id = canonical(&frame["headers"]["id"])?;
    if id.get_version_num() != 4 {
        return Err(());
    }
    let root = canonical(&frame["headers"]["root_id"])?;
    match &frame["headers"]["parent_id"] {
        Value::Null if root == id => {}
        Value::Null => return Err(()),
        parent => {
            canonical(parent)?;
        }
    }
    // Kombu restore adds this exact pair. Validate only the known real pair in
    // the parser clone; queued/restored bytes remain unchanged for actual CAS.
    match (
        frame["headers"].get("redelivered"),
        frame["properties"]["delivery_info"].get("redelivered"),
    ) {
        (None, None) => {}
        (Some(Value::Bool(true)), Some(Value::Bool(true))) => {
            frame["headers"]
                .as_object_mut()
                .ok_or(())?
                .remove("redelivered");
            frame["properties"]["delivery_info"]
                .as_object_mut()
                .ok_or(())?
                .remove("redelivered");
        }
        _ => return Err(()),
    }

    let kind = match frame["headers"]["task"].as_str() {
        Some("apps.epg.tasks.parse_programs_for_tvg_id") => Kind::Xml,
        _ => return Err(()),
    };
    if frame["headers"]["timelimit"] != json!([3600, 3500]) {
        return Err(());
    }
    let eta = match &frame["headers"]["eta"] {
        Value::Null => None,
        Value::String(s) if s.len() <= 64 => Some(
            DateTime::parse_from_rfc3339(s)
                .map_err(|_| ())?
                .with_timezone(&Utc),
        ),
        _ => return Err(()),
    };
    let raw = STANDARD
        .decode(frame["body"].as_str().ok_or(())?)
        .map_err(|_| ())?;
    let mut body: Value = serde_json::from_slice(&raw).map_err(|_| ())?;
    let args = body[0].as_array().ok_or(())?;
    let kwargs = body[1].as_object().ok_or(())?;
    let target_name = "epg_id";
    if args.len() > 3
        || kwargs
            .keys()
            .any(|k| ![target_name, "force", "_defer_retry"].contains(&k.as_str()))
    {
        return Err(());
    }
    let arg = |index, key| match (args.get(index), kwargs.get(key)) {
        (Some(_), Some(_)) => Err(()),
        (Some(v), None) | (None, Some(v)) => Ok(Some(v)),
        _ => Ok(None),
    };
    let target = arg(0, target_name)?
        .and_then(Value::as_i64)
        .filter(|n| (1..=i64::from(i32::MAX)).contains(n))
        .ok_or(())?;
    let force = match arg(1, "force")? {
        None => false,
        Some(Value::Bool(v)) => *v,
        _ => return Err(()),
    };
    let defer = match arg(2, "_defer_retry")? {
        None => 0,
        Some(v) => v.as_i64().ok_or(())?,
    };
    let values = args.iter().map(repr).collect::<Result<Vec<_>, _>>()?;
    let tuple = match values.len() {
        0 => "()".into(),
        1 => format!("({},)", values[0]),
        _ => format!("({})", values.join(", ")),
    };
    let list = format!("[{}]", values.join(", "));
    if frame["headers"]["argsrepr"] != tuple && frame["headers"]["argsrepr"] != list {
        return Err(());
    }
    let keys = kwargs
        .iter()
        .map(|(k, v)| Ok(format!("'{k}': {}", repr(v)?)))
        .collect::<Result<Vec<_>, ()>>()?;
    if frame["headers"]["kwargsrepr"] != format!("{{{}}}", keys.join(", ")) {
        return Err(());
    }
    frame["headers"]["task"] = json!("apps.m3u.tasks.refresh_single_m3u_account");
    frame["headers"]["eta"] = Value::Null;
    frame["headers"]["argsrepr"] = json!(format!("({target},)"));
    frame["headers"]["kwargsrepr"] = json!("{}");
    body[0] = json!([target]);
    body[1] = json!({});
    frame["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).map_err(|_| ())?));
    let common = crate::normal_xc_live::parse(&serde_json::to_vec(&frame).map_err(|_| ())?)?;
    Ok(Message {
        common,
        kind,
        target,
        force,
        defer,
        eta,
    })
}
pub(crate) fn retry_frame(
    message: &Message,
    id: uuid::Uuid,
    reply: uuid::Uuid,
    origin: &str,
    now: DateTime<Utc>,
) -> Result<Vec<u8>, ()> {
    let raw = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        id,
        reply,
        uuid::Uuid::new_v4(),
        origin,
    )
    .map_err(|_| ())?;
    let mut frame: Value = serde_json::from_slice(&raw).map_err(|_| ())?;
    frame["headers"]["task"] = json!("apps.epg.tasks.parse_programs_for_tvg_id");
    frame["headers"]["timelimit"] = json!([3600, 3500]);
    frame["headers"]["argsrepr"] = json!(format!("[{}]", message.target));
    frame["headers"]["kwargsrepr"] = json!(format!(
        "{{'force': {}, '_defer_retry': {}}}",
        if message.force { "True" } else { "False" },
        message.defer.checked_add(1).ok_or(())?
    ));
    frame["headers"]["root_id"] = json!(message.common.root);
    frame["headers"]["parent_id"] = json!(message.common.id);
    frame["headers"]["eta"] =
        json!((now + chrono::Duration::seconds(15))
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, false));
    frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[message.target],{"force":message.force,"_defer_retry":message.defer+1},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|())?));
    let raw = serde_json::to_vec(&frame).map_err(|_| ())?;
    parse(&raw)?;
    Ok(raw)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Deferral {
    Publish,
    Exhausted,
}
pub(crate) fn deferral(count: i64) -> Deferral {
    if count >= 480 {
        Deferral::Exhausted
    } else {
        Deferral::Publish
    }
}
#[cfg(test)]
mod source_controls {
    use super::*;
    fn initial() -> Vec<u8> {
        let captured: Value =
            serde_json::from_str(include_str!("../datasets/tvg-channel-signal-source.json"))
                .unwrap();
        STANDARD
            .decode(captured["raw_frame_base64"].as_str().unwrap())
            .unwrap()
    }
    #[test]
    fn actual_source_signal_and_production_fifteen_second_retry_preserve_force_counter_and_lineage()
    {
        let initial = initial();
        let original = parse(&initial).unwrap();
        assert_eq!(original.defer, 0);
        assert!(!original.force);
        let mut forced = original.clone();
        forced.force = true;
        forced.defer = 479;
        let id = uuid::Uuid::new_v4();
        let now =
            chrono::DateTime::<chrono::Utc>::from_timestamp_micros(1_800_000_000_000_000).unwrap();
        let raw = retry_frame(&forced, id, uuid::Uuid::new_v4(), "gen123@owned", now).unwrap();
        let retry = parse_eta(&raw).unwrap();
        assert_eq!(retry.common.id, id);
        assert_eq!(retry.common.parent, Some(original.common.id));
        assert_eq!(retry.common.root, original.common.root);
        assert_eq!(retry.defer, 480);
        assert!(retry.force);
        assert_eq!(retry.eta, Some(now + chrono::Duration::seconds(15)));
        assert_eq!(deferral(479), Deferral::Publish);
        assert_eq!(deferral(retry.defer), Deferral::Exhausted);
        assert!(crate::tvg_programme_protocol::parse_initial(&raw).is_err());
        let receipt = crate::xml_tvg_eta::Receipt::prepare(&raw, false).unwrap();
        assert!(crate::xml_tvg_eta::Receipt::prepare(&raw, true).is_err());
        let decoded =
            crate::xml_tvg_eta::Receipt::from_stored(&receipt.tuple_bytes, receipt.delivery_tag)
                .unwrap();
        assert_eq!(decoded.queued_bytes, raw);
        assert_eq!(decoded.message.defer, 480);
        assert_eq!(parse_eta(&receipt.restored_bytes).unwrap().common.id, id);
        let mut invalid: Value = serde_json::from_slice(&receipt.restored_bytes).unwrap();
        invalid["headers"]["redelivered"] = json!(false);
        assert!(parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    #[test]
    fn immediate_source_cancel_tuple_is_recoverable_without_eta() {
        use crate::xml_tvg_eta::Receipt;
        let raw = initial();
        let original = parse(&raw).unwrap();
        assert!(Receipt::prepare(&raw, false).is_err());
        let receipt = Receipt::prepare(&raw, true).unwrap();
        let restart = Receipt::from_stored(&receipt.tuple_bytes, receipt.delivery_tag).unwrap();
        assert_eq!(restart.tuple_bytes, receipt.tuple_bytes);
        assert!(restart.message.eta.is_none());
        assert_eq!(restart.message.common.id, original.common.id);
        assert_eq!(
            parse(&restart.restored_bytes).unwrap().common.id,
            original.common.id
        );
        assert!(Receipt::from_stored(&receipt.tuple_bytes, uuid::Uuid::new_v4()).is_err());
    }

    #[test]
    fn normal_force_kwargs_are_validated_without_rewriting_actual_claim_bytes() {
        let original = parse(&initial()).unwrap();
        let raw = retry_frame(
            &original,
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "gen1@owned",
            chrono::Utc::now(),
        )
        .unwrap();
        let mut frame: Value = serde_json::from_slice(&raw).unwrap();
        frame["headers"]["eta"] = Value::Null;
        let raw = serde_json::to_vec(&frame).unwrap();
        let (normal, _) = crate::tvg_programme_protocol::parse_normal(&raw).unwrap();
        assert_eq!(normal.defer_retry, 1);
        frame["headers"]["kwargsrepr"] = json!("{}");
        assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_err());
    }
}
