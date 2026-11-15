//! Actual loaded-plugin task envelope, retaining opaque pinned serializer body.
//! Capability comes from a live registered callable, never a task-name prefix.
//! Original Dispatcharr/Celery/Kombu authors retain all protocol credit.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use dispatcharr_store::PluginTaskCapability;
use serde_json::Value;
use uuid::Uuid;
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub id: Uuid,
    pub eta: Option<DateTime<Utc>>,
    pub expires: Option<DateTime<Utc>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Error {
    Unsupported,
    Corrupt,
    Capacity,
    Stale,
}
fn date(value: Option<&Value>) -> Result<Option<DateTime<Utc>>, Error> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.len() <= 64 => DateTime::parse_from_rfc3339(value)
            .map(|v| Some(v.with_timezone(&Utc)))
            .map_err(|_| Error::Unsupported),
        _ => Err(Error::Unsupported),
    }
}
pub(crate) fn parse(bytes: &[u8], capability: &PluginTaskCapability) -> Result<Message, Error> {
    if !capability.live() {
        return Err(Error::Stale);
    }
    if bytes.is_empty() || bytes.len() > 65536 {
        return Err(Error::Capacity);
    }
    let frame: Value = serde_json::from_slice(bytes).map_err(|_| Error::Corrupt)?;
    let headers = frame["headers"].as_object().ok_or(Error::Corrupt)?;
    if headers
        .get("stamped_headers")
        .is_some_and(|v| !v.is_null() && v.as_array().is_none_or(|a| !a.is_empty()))
        || headers.get("timelimit").is_some_and(|v| {
            !v.is_null()
                && v.as_array()
                    .is_none_or(|a| a.len() != 2 || a.iter().any(|x| !x.is_null()))
        })
    {
        // Header-stamp revocation and per-task soft/hard time limits need actual
        // owned worker signal ports; never accept and silently omit them.
        return Err(Error::Unsupported);
    }
    let name = headers
        .get("task")
        .and_then(Value::as_str)
        .ok_or(Error::Corrupt)?;
    if name != capability.task_name() {
        return Err(Error::Unsupported);
    }
    let id = headers
        .get("id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 512 && !s.contains('\0'))
        .ok_or(Error::Unsupported)?
        .to_owned();
    let parsed_id = Uuid::parse_str(&id).map_err(|_| Error::Unsupported)?;
    if parsed_id.to_string() != id {
        return Err(Error::Unsupported);
    }
    let properties = frame["properties"].as_object().ok_or(Error::Corrupt)?;
    if properties.get("correlation_id").and_then(Value::as_str) != Some(id.as_str())
        || frame["content-type"] != "application/json"
        || frame["content-encoding"] != "utf-8"
        || properties.get("body_encoding").and_then(Value::as_str) != Some("base64")
    {
        return Err(Error::Unsupported);
    }
    let delivery = &properties["delivery_info"];
    if delivery["routing_key"] != "celery"
        || !matches!(delivery["exchange"].as_str(), Some("" | "celery"))
        || properties
            .get("priority")
            .is_some_and(|v| !v.is_null() && v.as_i64() != Some(0))
    {
        return Err(Error::Unsupported);
    }
    let _delivery_tag: Uuid = properties
        .get("delivery_tag")
        .and_then(Value::as_str)
        .ok_or(Error::Corrupt)?
        .parse()
        .map_err(|_| Error::Unsupported)?;
    let body = frame["body"].as_str().ok_or(Error::Corrupt)?;
    let decoded = STANDARD.decode(body).map_err(|_| Error::Corrupt)?;
    if decoded.len() > 65536 {
        return Err(Error::Capacity);
    }
    // No invented JSON reserialization or kwargs validation: actual pinned Celery
    // trace_task_ret owns decoding, request Context, hooks, chains and exceptions.
    // Only producer-valid JSON envelopes enter the implemented ordinary slice.
    let payload: Value = serde_json::from_slice(&decoded).map_err(|_| Error::Corrupt)?;
    let parts = payload
        .as_array()
        .filter(|v| v.len() == 3)
        .ok_or(Error::Unsupported)?;
    if !parts[0].is_array() || !parts[1].is_object() || !parts[2].is_object() {
        return Err(Error::Unsupported);
    }
    Ok(Message {
        id: parsed_id,
        eta: date(headers.get("eta"))?,
        expires: date(headers.get("expires"))?,
    })
}
