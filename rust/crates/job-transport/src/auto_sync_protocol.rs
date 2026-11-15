//! Actual normal sync_auto_channels envelope, preserving surviving Source heads.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
pub(crate) struct Message {
    pub common: crate::normal_xc_live::Message,
    pub start: Option<DateTime<Utc>>,
}
fn timestamp(value: &Value) -> Result<Option<DateTime<Utc>>, ()> {
    if value.is_null() {
        return Ok(None);
    }
    let text = value
        .as_str()
        .filter(|text| {
            text.len() <= 64
                && text.is_ascii()
                && !text.contains(['\'', '"', '\\', '\n', '\r', '\t'])
        })
        .ok_or(())?;
    DateTime::parse_from_rfc3339(text)
        .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%:z"))
        .map(|time| Some(time.with_timezone(&Utc)))
        .map_err(|_| ())
}
fn spelling(value: &Value) -> Result<String, ()> {
    match value {
        Value::Null => Ok("None".into()),
        Value::String(value) => {
            timestamp(&Value::String(value.clone()))?;
            Ok(format!("'{value}'"))
        }
        Value::Number(value) if value.as_i64().is_some() => Ok(value.to_string()),
        _ => Err(()),
    }
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.len() > 65536 {
        return Err(());
    }
    let mut frame: Value = serde_json::from_slice(raw).map_err(|_| ())?;
    if frame["headers"]["task"] != "apps.m3u.tasks.sync_auto_channels"
        || frame["headers"]["timelimit"] != json!([null, null])
        || !frame["headers"]["eta"].is_null()
    {
        return Err(());
    }
    let decoded = STANDARD
        .decode(frame["body"].as_str().ok_or(())?)
        .map_err(|_| ())?;
    let mut body: Value = serde_json::from_slice(&decoded).map_err(|_| ())?;
    let args = body[0].as_array().ok_or(())?;
    let kwargs = body[1].as_object().ok_or(())?;
    if args.len() > 2
        || kwargs
            .keys()
            .any(|key| !matches!(key.as_str(), "account_id" | "scan_start_time"))
    {
        return Err(());
    }
    let account = match (args.first(), kwargs.get("account_id")) {
        (Some(id), None) | (None, Some(id)) => id
            .as_i64()
            .filter(|id| (1..=i64::from(i32::MAX)).contains(id))
            .ok_or(())?,
        _ => return Err(()),
    };
    let start = match (args.get(1), kwargs.get("scan_start_time")) {
        (Some(value), None) | (None, Some(value)) => timestamp(value)?,
        (None, None) => None,
        _ => return Err(()),
    };
    let arguments = args.iter().map(spelling).collect::<Result<Vec<_>, _>>()?;
    let tuple = match arguments.len() {
        0 => "()".to_owned(),
        1 => format!("({},)", arguments[0]),
        _ => format!("({})", arguments.join(", ")),
    };
    let list = format!("[{}]", arguments.join(", "));
    if frame["headers"]["argsrepr"] != tuple && frame["headers"]["argsrepr"] != list {
        return Err(());
    }
    let keywords = kwargs
        .iter()
        .map(|(key, value)| Ok(format!("'{key}': {}", spelling(value)?)))
        .collect::<Result<Vec<_>, ()>>()?;
    if frame["headers"]["kwargsrepr"] != format!("{{{}}}", keywords.join(", ")) {
        return Err(());
    }
    // Normalize only the validated signature clone. The transport retains and
    // CAS-claims the EXACT original raw Source bytes, not this projection.
    frame["headers"]["task"] = json!("apps.m3u.tasks.refresh_single_m3u_account");
    frame["headers"]["timelimit"] = json!([3600, 3500]);
    frame["headers"]["argsrepr"] = json!(format!("({account},)"));
    frame["headers"]["kwargsrepr"] = json!("{}");
    body[0] = json!([account]);
    body[1] = json!({});
    frame["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).map_err(|_| ())?));
    let common = crate::normal_xc_live::parse(&serde_json::to_vec(&frame).map_err(|_| ())?)?;
    Ok(Message { common, start })
}
