//! Standard dummy programmes from original apps/output/dummy_epg.py.
//! Original Dispatcharr authors retain full credit. Custom regex guides are separate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DummyError {
    Capacity,
    Unsupported,
}
use chrono::{DateTime, Duration, Timelike, Utc};
use serde_json::Value;
use std::sync::OnceLock;
use DummyError as RenderError;
#[derive(Debug)]
pub struct DummyProgramme {
    pub channel_id: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub title: String,
    pub description: String,
}
pub struct StandardDummy {
    channel: String,
    name: String,
    start: DateTime<Utc>,
    total: u32,
    next: u32,
    lookback: Option<DateTime<Utc>>,
    cutoff: Option<DateTime<Utc>>,
}
pub fn standard(
    channel: &str,
    name: &str,
    start: DateTime<Utc>,
    days: u32,
    lookback: Option<DateTime<Utc>>,
    cutoff: Option<DateTime<Utc>>,
) -> Result<StandardDummy, RenderError> {
    if days > 365 || channel.len() > 65536 || name.len() > 65536 {
        return Err(RenderError::Capacity);
    }
    Ok(StandardDummy {
        channel: channel.into(),
        name: name.into(),
        start,
        total: days * 6,
        next: 0,
        lookback,
        cutoff,
    })
}
fn description(name: &str, hour: u32, day: u32) -> String {
    static TEMPLATES: OnceLock<Vec<(u32, u32, Vec<String>)>> = OnceLock::new();
    let templates = TEMPLATES.get_or_init(|| {
        let value: Value =
            serde_json::from_str(include_str!("../datasets/source-dummy-templates.json"))
                .expect("Source standard dummy text");
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row["start"].as_u64().unwrap() as u32,
                    row["end"].as_u64().unwrap() as u32,
                    row["templates"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| v.as_str().unwrap().to_owned())
                        .collect(),
                )
            })
            .collect()
    });
    for (start, end, values) in templates {
        if *start <= hour && hour < *end {
            return values[((hour + day) as usize) % values.len()].replace("{channel}", name);
        }
    }
    format!("Placeholder program for {name} - EPG data went on vacation")
}
impl Iterator for StandardDummy {
    type Item = DummyProgramme;
    fn next(&mut self) -> Option<Self::Item> {
        while self.next < self.total {
            let offset = self.next;
            self.next += 1;
            let day = offset / 6;
            let start = self.start
                + Duration::days(i64::from(day))
                + Duration::hours(i64::from((offset % 6) * 4));
            let end = start + Duration::hours(4);
            if self.lookback.is_some_and(|lookback| end <= lookback)
                || self.cutoff.is_some_and(|cutoff| start >= cutoff)
            {
                continue;
            }
            return Some(DummyProgramme {
                channel_id: self.channel.clone(),
                start_time: start,
                end_time: end,
                title: self.name.clone(),
                description: description(&self.name, start.hour(), day),
            });
        }
        None
    }
}

/// Source's genuine no-title-pattern return precedes timezone/regex work, but
/// category splitting already happened and fallback fields must support strip.
/// Truthy patterns stay gated; stream selection is resolved by the real SQL port.
#[derive(Clone, Copy)]
pub struct SimpleConfig<'a> {
    fallback: Option<(&'a str, &'a str)>,
}
fn source_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
fn source_strip(value: &str) -> &str {
    value.trim_matches(|c:char|matches!(c,'\u{9}'..='\u{d}'|'\u{1c}'..='\u{20}'|'\u{85}'|'\u{a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'))
}
pub fn simple_config(properties: Option<&Value>) -> Result<SimpleConfig<'_>, DummyError> {
    let Some(properties) = properties.filter(|value| source_truthy(value)) else {
        return Ok(SimpleConfig { fallback: None });
    };
    let object = properties.as_object().ok_or(DummyError::Unsupported)?;
    if object.get("title_pattern").is_some_and(source_truthy) {
        return Err(DummyError::Unsupported);
    }
    if object
        .get("category")
        .filter(|value| source_truthy(value))
        .is_some_and(|value| !value.is_string())
    {
        return Err(DummyError::Unsupported);
    }
    let title = fallback_field(object, "fallback_title_template")?;
    let description = fallback_field(object, "fallback_description_template")?;
    Ok(SimpleConfig {
        fallback: (!title.is_empty() || !description.is_empty()).then_some((title, description)),
    })
}
fn fallback_field<'a>(
    object: &'a serde_json::Map<String, Value>,
    name: &str,
) -> Result<&'a str, DummyError> {
    let text = match object.get(name) {
        None => "",
        Some(Value::String(text)) => text,
        Some(_) => return Err(DummyError::Unsupported),
    };
    if text.len() > 65536 {
        return Err(DummyError::Capacity);
    }
    Ok(source_strip(text))
}
pub struct ConfiguredDummy {
    standard: StandardDummy,
    fallback: Option<(String, String)>,
}
pub fn configured(
    channel: &str,
    name: &str,
    start: DateTime<Utc>,
    days: u32,
    lookback: Option<DateTime<Utc>>,
    cutoff: Option<DateTime<Utc>>,
    properties: Option<&Value>,
) -> Result<ConfiguredDummy, DummyError> {
    let config = simple_config(properties)?;
    let fallback = config.fallback.map(|(title, description)| {
        (
            if title.is_empty() {
                name.to_owned()
            } else {
                title.to_owned()
            },
            if description.is_empty() {
                format!("EPG information is currently unavailable for {name}")
            } else {
                description.to_owned()
            },
        )
    });
    Ok(ConfiguredDummy {
        standard: standard(channel, name, start, days, lookback, cutoff)?,
        fallback,
    })
}
impl Iterator for ConfiguredDummy {
    type Item = DummyProgramme;
    fn next(&mut self) -> Option<Self::Item> {
        let mut programme = self.standard.next()?;
        if let Some((title, description)) = &self.fallback {
            programme.title = title.clone();
            programme.description = description.clone();
        }
        Some(programme)
    }
}
#[cfg(test)]
mod configured_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn source_no_title_returns_before_timezone_regex_and_uses_literal_fallback() {
        let props = json!({"title_pattern":"","time_pattern":"(?invalid","timezone":"Not/AZone","program_duration":0,"fallback_title_template":"\u{1c} Literal {channel} <&> \u{1f}","fallback_description_template":"\u{85}body {title}\u{3000}"});
        let start: DateTime<Utc> = "2026-01-02T01:00:00Z".parse().unwrap();
        let rows = configured(
            "id",
            "Name",
            start,
            1,
            Some(start + Duration::hours(4)),
            Some(start + Duration::hours(20)),
            Some(&props),
        )
        .unwrap()
        .collect::<Vec<_>>();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].start_time, start + Duration::hours(4));
        assert_eq!(rows[0].end_time, start + Duration::hours(8));
        assert!(rows
            .iter()
            .all(|r| r.title == "Literal {channel} <&>" && r.description == "body {title}"));
    }
    #[test]
    fn source_fallback_missing_fields_and_malformed_prior_category_remain_distinct() {
        let start: DateTime<Utc> = "2026-01-02T00:00:00Z".parse().unwrap();
        let props = json!({"fallback_description_template":" description "});
        let row = configured("id", "Name", start, 1, None, None, Some(&props))
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(row.title, "Name");
        assert_eq!(row.description, "description");
        let props = json!({"fallback_title_template":" title "});
        let row = configured("id", "Name", start, 1, None, None, Some(&props))
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(
            row.description,
            "EPG information is currently unavailable for Name"
        );
        for props in [
            json!({"title_pattern":"x"}),
            json!({"category":["x"]}),
            json!({"fallback_title_template":null}),
        ] {
            assert!(matches!(
                simple_config(Some(&props)),
                Err(DummyError::Unsupported)
            ));
        }
    }
}
