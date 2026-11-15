//! Exact normal SD mapped/single task signatures; original Celery authors credit.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Mapped,
    Single,
}
#[derive(Clone)]
pub(crate) struct Message {
    pub common: crate::normal_xc_live::Message,
    pub kind: Kind,
    pub target: i64,
    pub force: bool,
    pub defer: i64,
    pub eta: Option<DateTime<Utc>>,
    pub delegated: bool,
}
impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SdMessage")
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

    let (kind, delegated) = match frame["headers"]["task"].as_str() {
        Some("apps.epg.tasks.fetch_sd_mapped_guide_batch") => (Kind::Mapped, false),
        Some("apps.epg.tasks.fetch_sd_guide_for_epg") => (Kind::Single, false),
        Some("apps.epg.tasks.parse_programs_for_tvg_id") => (Kind::Single, true),
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
    let target_name = if kind == Kind::Mapped {
        "source_id"
    } else {
        "epg_id"
    };
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
        defer: if delegated { 0 } else { defer },
        eta,
        delegated,
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
    frame["headers"]["task"] = json!(match message.kind {
        Kind::Mapped => "apps.epg.tasks.fetch_sd_mapped_guide_batch",
        Kind::Single => "apps.epg.tasks.fetch_sd_guide_for_epg",
    });
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
        json!((now + chrono::Duration::seconds(90))
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, false));
    frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[message.target],{"force":message.force,"_defer_retry":message.defer+1},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|())?));
    let raw = serde_json::to_vec(&frame).map_err(|_| ())?;
    parse(&raw)?;
    Ok(raw)
}

#[cfg(test)]
mod controls {
    use super::*;
    fn initial() -> Vec<u8> {
        let id = uuid::Uuid::new_v4();
        let raw = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            id,
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "gen1@owned",
        )
        .unwrap();
        let mut frame: Value = serde_json::from_slice(&raw).unwrap();
        frame["headers"]["task"] = json!("apps.epg.tasks.fetch_sd_mapped_guide_batch");
        frame["headers"]["timelimit"] = json!([3600, 3500]);
        frame["headers"]["argsrepr"] = json!("(21,)");
        frame["body"] = json!(STANDARD.encode(
            serde_json::to_vec(
                &json!([[21],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
            )
            .unwrap()
        ));
        serde_json::to_vec(&frame).unwrap()
    }
    #[test]
    fn production_retry_encoder_preserves_actual_parent_root_and_ninety_second_eta() {
        let raw = initial();
        let original = parse(&raw).unwrap();
        let now = DateTime::parse_from_rfc3339("2026-11-15T12:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let id = uuid::Uuid::new_v4();
        let retry = retry_frame(&original, id, uuid::Uuid::new_v4(), "gen2@owned", now).unwrap();
        let parsed = parse_eta(&retry).unwrap();
        assert_eq!(parsed.common.id, id);
        assert_eq!(parsed.common.parent, Some(original.common.id));
        assert_eq!(parsed.common.root, original.common.root);
        assert_eq!(parsed.eta, Some(now + chrono::Duration::seconds(90)));
        assert_eq!(parsed.defer, 1);
        assert!(!parsed.delegated);
        assert_eq!(
            crate::milestone_dispatch::classify(&retry),
            Some(crate::milestone_dispatch::Family::SdGuide)
        );
        // Real typed durable receipt codec, not a fabricated reservation or ACK.
        let receipt = crate::sd_guide_eta::Receipt::prepare(&retry).unwrap();
        assert_eq!(receipt.queued_bytes, retry);
        let restored =
            crate::sd_guide_eta::Receipt::from_stored(&receipt.tuple_bytes, receipt.delivery_tag)
                .unwrap();
        assert_eq!(restored.message.common.id, id);
        let redelivered = crate::sd_guide_eta::Receipt::prepare(&receipt.restored_bytes).unwrap();
        assert_eq!(redelivered.delivery_tag, receipt.delivery_tag);
    }
    #[test]
    fn altered_repr_or_wrong_family_cannot_become_a_durable_sd_receipt() {
        let initial = initial();
        assert!(parse_eta(&initial).is_err());
        let mut bad: Value = serde_json::from_slice(&initial).unwrap();
        bad["headers"]["argsrepr"] = json!("(22,)");
        assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
        bad["headers"]["argsrepr"] = json!("(21,)");
        bad["headers"]["task"] = json!("apps.plugins.tasks.refresh_plugin_repos");
        assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
    }
    #[test]
    fn tvg_sd_delegation_resets_only_source_ignored_defer_count() {
        let mut raw: Value = serde_json::from_slice(&initial()).unwrap();
        raw["headers"]["task"] = json!("apps.epg.tasks.parse_programs_for_tvg_id");
        raw["headers"]["kwargsrepr"] = json!("{'force': True, '_defer_retry': 2}");
        raw["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[21],{"force":true,"_defer_retry":2},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).unwrap()));
        let message = parse(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert!(message.delegated && message.force);
        assert_eq!(message.defer, 0);
    }
}
