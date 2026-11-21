//! Native custom guide timelines with scoped regex dependency operations.
//! Original Dispatcharr authors retain all application/formatting credit.
use crate::{
    epg_custom_scope::{capacity, unsupported, Scope},
    epg_dummy,
};
use chrono::{DateTime, Datelike, Duration, Utc};
use chrono_tz::Tz;
use dispatcharr_store::{RegexNamedGroupRow, StoreError};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};

mod timed;
#[derive(Clone, Copy)]
pub struct Window {
    pub start: DateTime<Utc>,
    pub clock: DateTime<Utc>,
    pub days: u32,
    pub lookback: Option<DateTime<Utc>>,
    pub cutoff: Option<DateTime<Utc>>,
}
impl Window {
    fn source_precision(self) -> Result<Self, StoreError> {
        let exact = |t: DateTime<Utc>| {
            DateTime::from_timestamp_micros(t.timestamp_micros()).ok_or_else(capacity)
        };
        Ok(Self {
            start: exact(self.start)?,
            clock: exact(self.clock)?,
            days: self.days,
            lookback: self.lookback.map(exact).transpose()?,
            cutoff: self.cutoff.map(exact).transpose()?,
        })
    }
}
/// Conservative DOM charge, including strings, map keys and spare arrays;
/// this also bounds retained grid batches before pushing another programme.
pub fn metadata_bytes(value: &Value) -> Result<usize, StoreError> {
    let mut pending = vec![value];
    let mut bytes = 0usize;
    let mut count = 0usize;
    while let Some(value) = pending.pop() {
        count += 1;
        if count > 100000 {
            return Err(capacity());
        }
        bytes = bytes.checked_add(128).ok_or_else(capacity)?;
        match value {
            Value::String(text) => {
                bytes = bytes.checked_add(text.capacity()).ok_or_else(capacity)?
            }
            Value::Array(values) => {
                bytes = bytes
                    .checked_add(
                        values
                            .capacity()
                            .checked_mul(std::mem::size_of::<Value>())
                            .ok_or_else(capacity)?,
                    )
                    .ok_or_else(capacity)?;
                pending.extend(values)
            }
            Value::Object(values) => {
                for (key, value) in values {
                    bytes = bytes
                        .checked_add(key.capacity() + 128)
                        .ok_or_else(capacity)?;
                    pending.push(value)
                }
            }
            _ => {}
        }
    }
    Ok(bytes)
}
pub struct Programme {
    pub channel_id: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub title: String,
    pub description: String,
    pub sub_title: Option<String>,
    pub properties: Value,
}
pub enum Programmes {
    Legacy(epg_dummy::ConfiguredDummy),
    Custom(NoTime),
    Timed(Box<timed::Timed>),
}
pub struct Plan {
    pub logo: Option<String>,
    pub programmes: Programmes,
    pub bytes: usize,
}
fn dummy_error(error: epg_dummy::DummyError) -> StoreError {
    match error {
        epg_dummy::DummyError::Unsupported => unsupported(),
        epg_dummy::DummyError::Capacity => capacity(),
    }
}
fn truthy(value: Option<&Value>) -> bool {
    value.is_some_and(epg_dummy::source_truthy)
}
fn string<'a>(properties: &'a Value, key: &str) -> Result<&'a str, StoreError> {
    match properties.get(key) {
        None => Ok(""),
        Some(Value::String(v)) => Ok(v),
        Some(value) if !epg_dummy::source_truthy(value) => Ok(""),
        _ => Err(unsupported()),
    }
}
fn duration(properties: &Value) -> Result<i64, StoreError> {
    let Some(value) = properties.get("program_duration") else {
        return Ok(180);
    };
    let value = match value {
        Value::Bool(v) => i64::from(*v),
        Value::Number(v) if !v.to_string().contains(['.', 'e', 'E']) => {
            v.as_i64().ok_or_else(unsupported)?
        }
        _ => return Err(unsupported()),
    };
    if value <= 0 || value.checked_mul(60_000_000).is_none() {
        return Err(unsupported());
    }
    Ok(value)
}
fn source_timezone(properties: &Value) -> Result<Tz, StoreError> {
    let Some(value) = properties.get("timezone").filter(|value| !value.is_null()) else {
        return Ok(chrono_tz::UTC);
    };
    let requested = value.as_str().ok_or_else(unsupported)?;
    Ok(timezone(requested).unwrap_or(chrono_tz::UTC))
}

fn timezone(name: &str) -> Option<Tz> {
    // pytz resolves IANA names regardless of ASCII case.
    name.parse().ok().or_else(|| {
        chrono_tz::TZ_VARIANTS
            .iter()
            .copied()
            .find(|zone| zone.name().eq_ignore_ascii_case(name))
    })
}

/// A timed event uses the date capture or the request day in the source zone.
pub fn supported(properties: &Value) -> bool {
    validate(properties).is_ok()
}
fn validate(properties: &Value) -> Result<Tz, StoreError> {
    if !properties.is_object() || string(properties, "title_pattern")?.is_empty() {
        return Err(unsupported());
    }
    string(properties, "time_pattern")?;
    string(properties, "date_pattern")?;
    let timezone = source_timezone(properties)?;
    string(properties, "output_timezone")?;
    duration(properties)?;
    for key in [
        "title_template",
        "subtitle_template",
        "description_template",
        "channel_logo_url",
        "program_poster_url",
        "category",
    ] {
        string(properties, key)?;
    }
    Ok(timezone)
}
fn bounded_replace(
    input: &str,
    pattern: &str,
    replacement: &str,
    limit: usize,
) -> Result<String, StoreError> {
    let count = input.matches(pattern).count();
    let bytes = input
        .len()
        .checked_sub(count.checked_mul(pattern.len()).ok_or_else(capacity)?)
        .and_then(|v| v.checked_add(count.checked_mul(replacement.len())?))
        .filter(|v| *v <= limit)
        .ok_or_else(capacity)?;
    let mut result = String::new();
    result.try_reserve_exact(bytes).map_err(|_| capacity())?;
    let mut position = 0;
    for (offset, _) in input.match_indices(pattern) {
        result.push_str(&input[position..offset]);
        result.push_str(replacement);
        position = offset + pattern.len();
    }
    result.push_str(&input[position..]);
    Ok(result)
}
fn quote(value: &str, limit: usize) -> Result<String, StoreError> {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut result = String::new();
    for byte in value.bytes() {
        let plain = byte.is_ascii_alphanumeric() || b"-._~".contains(&byte);
        let extra = if plain { 1 } else { 3 };
        if result.len().checked_add(extra).is_none_or(|n| n > limit) {
            return Err(capacity());
        }
        if plain {
            result.push(byte as char)
        } else {
            result.push('%');
            result.push(HEX[(byte >> 4) as usize] as char);
            result.push(HEX[(byte & 15) as usize] as char)
        }
    }
    Ok(result)
}
fn template(
    value: &str,
    groups: &[(String, Option<String>)],
    url: bool,
    limit: usize,
) -> Result<String, StoreError> {
    if value.len() > limit {
        return Err(capacity());
    }
    let mut result = value.to_owned();
    for (key, value) in groups {
        let value = value.as_deref().unwrap_or("");
        let encoded = if url && !value.is_empty() {
            quote(value, limit)?
        } else {
            value.to_owned()
        };
        result = bounded_replace(&result, &format!("{{{key}}}"), &encoded, limit)?;
    }
    Ok(result)
}
async fn aliases(
    scope: &Scope,
    groups: &mut Vec<(String, Option<String>)>,
) -> Result<(), StoreError> {
    // Source snapshots items BEFORE alias insertion; replacements keep their
    // original key position, and later snapshot values are not retroactively read.
    let original = groups.clone();
    let mut positions = groups
        .iter()
        .enumerate()
        .map(|(i, (k, _))| (k.clone(), i))
        .collect::<HashMap<_, _>>();
    let selected = original
        .into_iter()
        .filter_map(|(key, value)| value.filter(|v| !v.is_empty()).map(|v| (key, v)))
        .collect::<Vec<_>>();
    for chunk in selected.chunks(100) {
        let values = scope
            .normalize(chunk.iter().map(|(_, value)| value.clone()).collect())
            .await?;
        for ((key, _), value) in chunk.iter().zip(values) {
            let key = format!("{key}_normalize");
            if let Some(position) = positions.get(&key) {
                groups[*position].1 = Some(value);
            } else {
                positions.insert(key.clone(), groups.len());
                groups.push((key, Some(value)));
            }
        }
    }
    Ok(())
}
fn group<'a>(groups: &'a [(String, Option<String>)], name: &str) -> Option<&'a str> {
    groups
        .iter()
        .find(|(key, _)| key == name)
        .and_then(|(_, v)| v.as_deref())
        .filter(|v| !v.is_empty())
}
type Groups = Vec<(String, Option<String>)>;

async fn captures(scope: &Scope, pattern: &str, name: &str) -> Result<Option<Groups>, StoreError> {
    let result = scope
        .groups(pattern.to_owned(), vec![name.to_owned()])
        .await?;
    if result.compile_error.is_some() {
        return Ok(None);
    }
    match result.rows.into_iter().next().ok_or_else(unsupported)? {
        RegexNamedGroupRow::Matched(groups) => Ok(Some(groups)),
        RegexNamedGroupRow::NotMatched => Ok(None),
        RegexNamedGroupRow::SearchError(_) => Err(StoreError::Unsupported(
            "Actual custom-guide regex search failed",
        )),
    }
}

fn merge_groups(title: &mut Groups, date: Groups) {
    let positions: HashMap<_, _> = title
        .iter()
        .enumerate()
        .map(|(index, (key, _))| (key.clone(), index))
        .collect();
    for (key, value) in date {
        if let Some(index) = positions.get(&key) {
            title[*index].1 = value;
        } else {
            title.push((key, value));
        }
    }
}

fn default_title(name: &str, groups: &[(String, Option<String>)]) -> String {
    let mut pieces = Vec::new();
    if let Some(league) = group(groups, "league") {
        pieces.push(league.to_owned());
    }
    if let (Some(a), Some(b)) = (group(groups, "team1"), group(groups, "team2")) {
        pieces.push(format!("{a} vs {b}"));
    } else if let Some(title) = group(groups, "title") {
        pieces.push(title.to_owned());
    }
    if pieces.is_empty() {
        name.to_owned()
    } else {
        pieces.join(" - ")
    }
}
pub async fn prepare(
    scope: Option<&Arc<Scope>>,
    channel: &str,
    name: &str,
    properties: Option<&Value>,
    window: Window,
    limit: usize,
) -> Result<Plan, StoreError> {
    let window = window.source_precision()?;
    if properties.is_none_or(|p| epg_dummy::simple_config(Some(p)).is_ok()) {
        return Ok(Plan {
            logo: None,
            programmes: Programmes::Legacy(
                epg_dummy::configured(
                    channel,
                    name,
                    window.start,
                    window.days,
                    window.lookback,
                    window.cutoff,
                    properties,
                )
                .map_err(dummy_error)?,
            ),
            bytes: channel.len() + name.len() + properties.map_or(0, |p| p.to_string().len()) + 512,
        });
    }
    let properties = properties.ok_or_else(unsupported)?;
    let source_timezone = validate(properties)?;
    let scope = scope.ok_or_else(unsupported)?;
    let Some(mut groups) = captures(scope, string(properties, "title_pattern")?, name).await?
    else {
        return Ok(Plan {
            logo: None,
            programmes: Programmes::Legacy(
                epg_dummy::fallback_after_no_match(
                    channel,
                    name,
                    window.start,
                    window.days,
                    window.lookback,
                    window.cutoff,
                    properties,
                )
                .map_err(dummy_error)?,
            ),
            bytes: channel.len() + name.len() + properties.to_string().len() + 512,
        });
    };
    // Output XML's channel element uses only title captures. Programme
    // templates use time/date captures, then computed aliases and clock fields.
    let time_pattern = string(properties, "time_pattern")?;
    let date_pattern = string(properties, "date_pattern")?;
    let time_groups = if time_pattern.is_empty() {
        None
    } else {
        captures(scope, time_pattern, name).await?
    };
    let date_groups = if date_pattern.is_empty() {
        None
    } else {
        captures(scope, date_pattern, name).await?
    };
    let kickoff = match time_groups.as_ref() {
        Some(groups) => timed::time_info(groups)?,
        None => None,
    };
    let dated = match (kickoff, date_groups.as_ref()) {
        (Some(_), Some(groups)) => timed::captured_date(groups, window.start.date_naive())?,
        _ => None,
    };
    let extra_matches = time_groups.is_some() || date_groups.is_some();
    let logo_template = string(properties, "channel_logo_url")?;
    let title_logo = if extra_matches && !logo_template.is_empty() && !groups.is_empty() {
        let mut logo_groups = groups.clone();
        aliases(scope, &mut logo_groups).await?;
        Some(template(logo_template, &logo_groups, true, limit)?)
    } else {
        None
    };

    for extra in time_groups.into_iter().chain(date_groups) {
        merge_groups(&mut groups, extra);
    }
    aliases(scope, &mut groups).await?;

    let logo = if extra_matches {
        title_logo
    } else {
        (!logo_template.is_empty() && !groups.is_empty())
            .then(|| template(logo_template, &groups, true, limit))
            .transpose()?
    }
    .filter(|value| !value.is_empty());
    // Source resolves the poster before adding derived time fields.
    let poster_template = string(properties, "program_poster_url")?;
    let poster = (!poster_template.is_empty())
        .then(|| template(poster_template, &groups, true, limit))
        .transpose()?
        .filter(|v| !v.is_empty());
    let duration = duration(properties)?;
    let runtime = Duration::try_minutes(duration).ok_or_else(capacity)?;
    if let Some(kickoff) = kickoff {
        let date = dated.unwrap_or(window.clock.date_naive());
        let output = string(properties, "output_timezone")?;
        let display_timezone = (!output.is_empty()).then(|| timezone(output)).flatten();
        let (date, kickoff) = match display_timezone {
            Some(zone) => {
                let displayed =
                    timed::localize(source_timezone, date.and_time(kickoff))?.with_timezone(&zone);
                (displayed.date_naive(), displayed.time())
            }
            None => (date, kickoff),
        };
        timed::clock_fields(&mut groups, date, kickoff, runtime);
    }
    let title_text = string(properties, "title_template")?;
    let title = if title_text.is_empty() {
        default_title(name, &groups)
    } else {
        template(title_text, &groups, false, limit)?
    };
    let text = string(properties, "description_template")?;
    let description = if text.is_empty() {
        title.clone()
    } else {
        template(text, &groups, false, limit)?
    };
    let text = string(properties, "subtitle_template")?;
    let subtitle = (!text.is_empty())
        .then(|| template(text, &groups, false, limit))
        .transpose()?;
    let fillers = kickoff
        .map(|_| {
            let upcoming = timed::Labels::for_filler(
                properties,
                "upcoming",
                "Upcoming: ",
                &title,
                &description,
                &groups,
                limit,
            )?;
            let ended = timed::Labels::for_filler(
                properties,
                "ended",
                "Ended: ",
                &title,
                &description,
                &groups,
                limit,
            )?;
            Ok::<_, StoreError>((upcoming, ended))
        })
        .transpose()?;
    let category = string(properties, "category")?;
    let charge = category
        .split(',')
        .map(epg_dummy::source_strip)
        .filter(|v| !v.is_empty())
        .try_fold(0usize, |bytes, value| {
            bytes
                .checked_add(192 + value.len())
                .filter(|v| *v <= limit)
                .ok_or_else(capacity)
        })?;
    let _ = charge;
    let categories = category
        .split(',')
        .map(epg_dummy::source_strip)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut props = serde_json::Map::new();
    if !categories.is_empty() {
        props.insert("categories".into(), json!(categories));
    }
    if truthy(properties.get("include_live")) {
        props.insert("live".into(), json!(true));
    }
    if truthy(properties.get("include_new")) {
        props.insert("new".into(), json!(true));
    }
    let include_date = properties
        .get("include_date")
        .is_none_or(epg_dummy::source_truthy);
    let props_bytes = props.iter().try_fold(256usize, |bytes, (key, value)| {
        bytes
            .checked_add(key.capacity() + 128)
            .and_then(|bytes| bytes.checked_add(metadata_bytes(value).ok()?))
            .ok_or_else(capacity)
    })?;
    let bytes = title
        .capacity()
        .checked_add(description.capacity())
        .and_then(|v| v.checked_add(subtitle.as_ref().map_or(0, String::capacity)))
        .and_then(|v| v.checked_add(logo.as_ref().map_or(0, String::capacity)))
        .and_then(|v| v.checked_add(poster.as_ref().map_or(0, String::capacity)))
        .and_then(|v| v.checked_add(props_bytes))
        .and_then(|v| v.checked_add(channel.len() + 512))
        .and_then(|v| match &fillers {
            Some((upcoming, ended)) => v.checked_add(upcoming.bytes())?.checked_add(ended.bytes()),
            None => Some(v),
        })
        .ok_or_else(capacity)?;
    if bytes > limit {
        return Err(capacity());
    }
    let shared = NoTime {
        channel: channel.to_owned(),
        title,
        description,
        subtitle,
        props,
        poster,
        include_date,
        source_timezone,
        window,
        duration,
        per_day: ((24.0 / (duration as f64 / 60.0)).trunc() as u32).max(1),
        day: 0,
        index: 0,
    };
    let programmes = match (kickoff, fillers) {
        (Some(start), Some((upcoming, ended))) => Programmes::Timed(Box::new(timed::Timed::new(
            shared, start, dated, runtime, upcoming, ended,
        )?)),
        _ => Programmes::Custom(shared),
    };
    Ok(Plan {
        logo,
        bytes,
        programmes,
    })
}

pub struct NoTime {
    channel: String,
    title: String,
    description: String,
    subtitle: Option<String>,
    props: serde_json::Map<String, Value>,
    poster: Option<String>,
    include_date: bool,
    source_timezone: Tz,
    window: Window,
    duration: i64,
    per_day: u32,
    day: u32,
    index: u32,
}
impl NoTime {
    fn programme(
        &self,
        start: DateTime<Utc>,
        stop: DateTime<Utc>,
        filler: Option<&timed::Labels>,
    ) -> Result<Programme, StoreError> {
        if !(1..=9999).contains(&start.year()) || !(1..=9999).contains(&stop.year()) {
            return Err(capacity());
        }
        let (title, description, sub_title, mut props) = match filler {
            Some(filler) => (
                filler.title.clone(),
                filler.description.clone(),
                None,
                serde_json::Map::new(),
            ),
            None => (
                self.title.clone(),
                self.description.clone(),
                self.subtitle.clone(),
                self.props.clone(),
            ),
        };
        if self.include_date {
            let local = start.with_timezone(&self.source_timezone);
            props.insert("date".into(), json!(local.format("%Y-%m-%d").to_string()));
        }
        if let Some(poster) = &self.poster {
            props.insert("icon".into(), json!(poster));
        }
        Ok(Programme {
            channel_id: self.channel.clone(),
            start_time: start,
            end_time: stop,
            title,
            description,
            sub_title,
            properties: Value::Object(props),
        })
    }
}
impl Iterator for NoTime {
    type Item = Result<Programme, StoreError>;
    fn next(&mut self) -> Option<Self::Item> {
        while self.day < self.window.days {
            if self.index >= self.per_day {
                self.day += 1;
                self.index = 0;
                continue;
            }
            let day = self.day;
            let index = self.index;
            self.index += 1;
            let result = (|| {
                let original = self
                    .window
                    .start
                    .checked_add_signed(Duration::days(i64::from(day)))
                    .ok_or_else(capacity)?;
                let end = original
                    .checked_add_signed(Duration::days(1))
                    .ok_or_else(capacity)?;
                let start = self
                    .window
                    .lookback
                    .map_or(original, |lookback| lookback.max(original));
                let end = self.window.cutoff.map_or(end, |cutoff| cutoff.min(end));
                if start >= end {
                    return Ok(None);
                }
                let offset = self
                    .duration
                    .checked_mul(i64::from(index))
                    .ok_or_else(capacity)?;
                let start = start
                    .checked_add_signed(Duration::microseconds(
                        offset.checked_mul(60_000_000).ok_or_else(capacity)?,
                    ))
                    .ok_or_else(capacity)?;
                let stop = start
                    .checked_add_signed(Duration::microseconds(self.duration * 60_000_000))
                    .ok_or_else(capacity)?;
                if start >= end {
                    return Ok(None);
                }
                self.programme(start, stop, None).map(Some)
            })();
            match result {
                Ok(None) => continue,
                Ok(Some(programme)) => return Some(Ok(programme)),
                Err(error) => {
                    self.day = self.window.days;
                    return Some(Err(error));
                }
            }
        }
        None
    }
}
impl Iterator for Programmes {
    type Item = Result<Programme, StoreError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Legacy(source) => source.next().map(|p| {
                Ok(Programme {
                    channel_id: p.channel_id,
                    start_time: p.start_time,
                    end_time: p.end_time,
                    title: p.title,
                    description: p.description,
                    sub_title: None,
                    properties: Value::Null,
                })
            }),
            Self::Custom(source) => source.next(),
            Self::Timed(source) => source.next(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_no_time_clamps_start_but_keeps_nondivisor_gap_and_unclipped_stop() {
        let start: DateTime<Utc> = "2026-01-02T00:00:00Z".parse().unwrap();
        let lookback: DateTime<Utc> = "2026-01-02T00:17:19.123456Z".parse().unwrap();
        let source = NoTime {
            channel: "owned".into(),
            title: "Owned".into(),
            description: "Source no-time".into(),
            subtitle: Some("".into()),
            props: serde_json::Map::new(),
            poster: None,
            include_date: true,
            source_timezone: chrono_tz::UTC,
            window: Window {
                start,
                clock: start,
                days: 1,
                lookback: Some(lookback),
                cutoff: Some(lookback + Duration::hours(8)),
            },
            duration: 181,
            per_day: 7,
            day: 0,
            index: 0,
        };
        let rows = source.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].start_time, lookback);
        assert_eq!(rows[1].start_time, lookback + Duration::minutes(181));
        assert_eq!(rows[2].end_time, lookback + Duration::minutes(543));
        assert_eq!(rows[0].sub_title.as_deref(), Some(""));
        assert_eq!(rows[0].properties["date"], json!("2026-01-02"));
    }
    #[test]
    fn source_no_time_dates_use_source_timezone_without_shifting_grid() {
        for (timezone, start, expected) in [
            (
                json!("america/new_york"),
                "2026-01-02T04:30:00Z",
                "2026-01-01",
            ),
            (json!("US/Eastern"), "2026-07-02T04:30:00Z", "2026-07-02"),
            (
                json!("Asia/Kathmandu"),
                "2026-01-02T18:30:00Z",
                "2026-01-03",
            ),
            (json!("unknown/owned"), "2026-01-02T04:30:00Z", "2026-01-02"),
            (Value::Null, "2026-01-02T04:30:00Z", "2026-01-02"),
        ] {
            let props = json!({"title_pattern":"(?P<title>Owned)","timezone":timezone,
                "output_timezone":"Pacific/Auckland"});
            assert!(supported(&props));
            let start: DateTime<Utc> = start.parse().unwrap();
            let row = NoTime {
                channel: "owned".into(),
                title: "Owned".into(),
                description: "Source local calendar date".into(),
                subtitle: None,
                props: serde_json::Map::new(),
                poster: None,
                include_date: true,
                source_timezone: source_timezone(&props).unwrap(),
                window: Window {
                    start,
                    clock: start,
                    days: 1,
                    lookback: None,
                    cutoff: None,
                },
                duration: 180,
                per_day: 8,
                day: 0,
                index: 0,
            }
            .next()
            .unwrap()
            .unwrap();
            assert_eq!(row.properties["date"], expected);
            assert_eq!(row.start_time, start);
            assert_eq!(row.end_time, start + Duration::hours(3));
        }
    }
    #[test]
    fn source_date_only_merges_raw_groups_in_python_dict_order() {
        assert!(supported(&json!({
            "title_pattern": "(?<title>Owned)",
            "date_pattern": "(?<month>[A-Za-z]+) (?<day>[0-9]+)"
        })));
        assert!(supported(&json!({
            "title_pattern": "(?<title>Owned)",
            "date_pattern": "(?<day>[0-9]+)",
            "time_pattern": "(?<hour>[0-9]+):[0-9]+"
        })));
        let mut title = vec![
            ("month".into(), Some("Title month".into())),
            ("title".into(), Some("Owned title".into())),
            ("year".into(), Some("2026".into())),
        ];
        merge_groups(
            &mut title,
            vec![
                ("day".into(), Some("15".into())),
                ("month".into(), Some("October".into())),
                ("year".into(), None),
            ],
        );
        assert_eq!(
            title,
            vec![
                ("month".into(), Some("October".into())),
                ("title".into(), Some("Owned title".into())),
                ("year".into(), None),
                ("day".into(), Some("15".into())),
            ]
        );
    }
    #[test]
    fn source_template_order_and_url_quote_are_literal_sequential_replacements() {
        let groups = vec![
            ("z".into(), Some("{a}".into())),
            ("a".into(), Some("A B/~*".into())),
        ];
        assert_eq!(template("{z}", &groups, false, 1024).unwrap(), "A B/~*");
        assert_eq!(
            template("{a}", &groups, true, 1024).unwrap(),
            "A%20B%2F~%2A"
        );
        assert!(bounded_replace("{x}{x}", "{x}", &"v".repeat(1024), 1024).is_err());
    }
}
