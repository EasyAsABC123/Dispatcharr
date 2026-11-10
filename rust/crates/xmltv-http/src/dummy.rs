//! Standard dummy programmes from original apps/output/dummy_epg.py.
//! Original Dispatcharr authors retain full credit. Custom regex guides are separate.
use crate::render::RenderError;
use chrono::{DateTime, Duration, Timelike, Utc};
use serde_json::Value;
use std::sync::OnceLock;
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
#[cfg(test)]
mod tests {
    use super::*;
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
