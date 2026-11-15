//! Source PluginField/PluginAction serializer normalization; original authors retain credit.
use serde_json::{Map, Value};
fn text(value: &Value, blank: bool) -> Option<Value> {
    crate::admin_writes::character("value", value, None, blank)
        .ok()
        .map(Value::String)
}
fn required(input: &Map<String, Value>, out: &mut Map<String, Value>, key: &str) -> Option<()> {
    out.insert(key.into(), text(input.get(key)?, false)?);
    Some(())
}
fn optional(input: &Map<String, Value>, out: &mut Map<String, Value>, key: &str) -> Option<()> {
    if let Some(v) = input.get(key) {
        out.insert(key.into(), text(v, true)?);
    }
    Some(())
}
fn field(value: &Value) -> Option<Value> {
    let input = value.as_object()?;
    let mut out = Map::new();
    required(input, &mut out, "id")?;
    let kind = input.get("type")?.as_str()?;
    if !["string", "number", "boolean", "select", "text", "info"].contains(&kind) {
        return None;
    }
    out.insert("type".into(), Value::String(kind.into()));
    for key in [
        "label",
        "help_text",
        "description",
        "placeholder",
        "input_type",
        "value",
    ] {
        optional(input, &mut out, key)?;
    }
    if let Some(v) = input.get("default") {
        out.insert("default".into(), v.clone());
    }
    for key in ["min", "max", "step"] {
        if let Some(v) = input.get(key) {
            let number = match v {
                Value::Number(v) => v.as_f64()?,
                Value::String(v) => dispatcharr_ingest_core::source_float_literal(v)?,
                Value::Bool(v) => {
                    if *v {
                        1.0
                    } else {
                        0.0
                    }
                }
                _ => return None,
            };
            if !number.is_finite() {
                return None;
            }
            out.insert(key.into(), serde_json::json!(number));
        }
    }
    if let Some(v) = input.get("options") {
        let mut options = Vec::new();
        for item in v.as_array()? {
            let item = item.as_object()?;
            let mut option = Map::new();
            required(item, &mut option, "value")?;
            required(item, &mut option, "label")?;
            options.push(Value::Object(option));
        }
        out.insert("options".into(), Value::Array(options));
    }
    Some(Value::Object(out))
}
fn action(value: &Value) -> Option<Value> {
    let input = value.as_object()?;
    let mut out = Map::new();
    for key in ["id", "label"] {
        required(input, &mut out, key)?;
    }
    for key in [
        "description",
        "button_label",
        "button_variant",
        "button_color",
    ] {
        optional(input, &mut out, key)?;
    }
    if let Some(v) = input.get("confirm") {
        out.insert("confirm".into(), v.clone());
    }
    if let Some(v) = input.get("events") {
        out.insert(
            "events".into(),
            Value::Array(
                v.as_array()?
                    .iter()
                    .map(|v| text(v, false))
                    .collect::<Option<Vec<_>>>()?,
            ),
        );
    }
    Some(Value::Object(out))
}
pub(super) fn fields(value: &Value) -> Vec<Value> {
    value
        .as_array()
        .map(|v| v.iter().filter_map(field).collect())
        .unwrap_or_default()
}
pub(super) fn actions(value: &Value) -> Vec<Value> {
    value
        .as_array()
        .map(|v| v.iter().filter_map(action).collect())
        .unwrap_or_default()
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn source_individual_invalid_rows_are_dropped_without_losing_valid_entries() {
        assert_eq!(
            fields(
                &json!([{"id":" size ","type":"number","min":"2","ignored":3},{"id":"bad","type":"unknown"},{"id":"b","type":"select","options":[{"value":7,"label":"Seven"}]}])
            ),
            vec![
                json!({"id":"size","type":"number","min":2.0}),
                json!({"id":"b","type":"select","options":[{"value":"7","label":"Seven"}]})
            ]
        );
        assert_eq!(
            actions(
                &json!([{"id":"go","label":"Run","confirm":false,"events":[1,"connect"]},{"id":"bad"}])
            ),
            vec![json!({"id":"go","label":"Run","confirm":false,"events":["1","connect"]})]
        );
    }
}
