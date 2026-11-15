//! Shared standard dummy iterator; original Dispatcharr authors retain credit.
use crate::render::RenderError;
use chrono::{DateTime, Utc};
pub use dispatcharr_output_http::epg_dummy::{DummyProgramme, StandardDummy};
#[cfg(test)]
use serde_json::Value;
pub fn standard(
    channel: &str,
    name: &str,
    start: DateTime<Utc>,
    days: u32,
    lookback: Option<DateTime<Utc>>,
    cutoff: Option<DateTime<Utc>>,
) -> Result<StandardDummy, RenderError> {
    dispatcharr_output_http::epg_dummy::standard(channel, name, start, days, lookback, cutoff)
        .map_err(|_| RenderError::Capacity)
}

pub fn configured(
    channel: &str,
    name: &str,
    start: DateTime<Utc>,
    days: u32,
    lookback: Option<DateTime<Utc>>,
    cutoff: Option<DateTime<Utc>>,
    properties: Option<&serde_json::Value>,
) -> Result<dispatcharr_output_http::epg_dummy::ConfiguredDummy, RenderError> {
    dispatcharr_output_http::epg_dummy::configured(
        channel, name, start, days, lookback, cutoff, properties,
    )
    .map_err(|_| RenderError::InvalidMetadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configured_fallback_preserves_all_actual_source_standard_windows() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-render.json")).unwrap();
        let props = serde_json::json!({"title_pattern":"","fallback_title_template":" Literal <&> ","fallback_description_template":" description "});
        let cases = source["standard_dummy_cases"].as_array().unwrap();
        assert_eq!(cases.len(), 144);
        for case in cases {
            let start: DateTime<Utc> = format!(
                "2026-01-02T{:02}:00:00+00:00",
                case["hour"].as_u64().unwrap()
            )
            .parse()
            .unwrap();
            let time = |name: &str| {
                case[name]
                    .as_str()
                    .map(|v| v.parse::<DateTime<Utc>>().unwrap())
            };
            let actual=configured("fixture","Channel <&> 東京",start,case["days"].as_u64().unwrap()as u32,time("lookback"),time("cutoff"),Some(&props)).unwrap().map(|p|serde_json::json!({"channel_id":p.channel_id,"start_time":p.start_time.to_rfc3339(),"end_time":p.end_time.to_rfc3339(),"title":p.title,"description":p.description})).collect::<Vec<_>>();
            let mut expected = case["programmes"].as_array().unwrap().clone();
            for p in &mut expected {
                p["title"] = serde_json::json!("Literal <&>");
                p["description"] = serde_json::json!("description");
            }
            assert_eq!(actual, expected);
        }
    }
    #[test]
    fn actual_source_standard_dummy_times_descriptions_and_boundaries() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-render.json")).unwrap();
        let cases = source["standard_dummy_cases"].as_array().unwrap();
        assert_eq!(cases.len(), 144);
        for case in cases {
            let start: DateTime<Utc> = format!(
                "2026-01-02T{:02}:00:00+00:00",
                case["hour"].as_u64().unwrap()
            )
            .parse()
            .unwrap();
            let time = |name: &str| {
                case[name]
                    .as_str()
                    .map(|v| v.parse::<DateTime<Utc>>().unwrap())
            };
            let actual=standard("fixture","Channel <&> 東京",start,case["days"].as_u64().unwrap()as u32,time("lookback"),time("cutoff")).unwrap().map(|p|serde_json::json!({"channel_id":p.channel_id,"start_time":p.start_time.to_rfc3339(),"end_time":p.end_time.to_rfc3339(),"title":p.title,"description":p.description})).collect::<Vec<_>>();
            assert_eq!(Value::Array(actual), case["programmes"]);
        }
    }
}
