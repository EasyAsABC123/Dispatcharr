//! Bounded configured recording decisions, derived from pinned Dispatcharr
//! tasks.py and signals.py. Original authors retain implementation credit.
//! PRIVATE UNCOMPILED DRAFT: private module registration only, no worker/application authority.
//! Callers must load a bounded immutable SQL snapshot under the retained owner.
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Utc};
use dispatcharr_store::recording_schedule::SeriesTitle;
use serde_json::{json, Value};
use std::collections::HashSet;

const CAP: usize = 32;
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    Capacity,
    Shape,
    Date,
    Identity,
}
#[derive(Clone, Debug)]
pub struct Programme {
    pub id: i64,
    pub source_id: i64,
    pub channel_id: i64,
    pub tvg_id: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub original_air_date: String,
    pub description: Option<String>,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub season: Option<u32>,
    pub episode: Option<u32>,
}
#[derive(Clone, Debug)]
pub struct Existing {
    pub subtitle: Option<String>,
    pub tvg_id: String,
    pub title: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub season: Option<u32>,
    pub episode: Option<u32>,
}
/// A committed episode has the same identity across different airing slots.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SeriesEpisode(String, String, String);
#[derive(Clone, Debug)]
pub struct Intent {
    pub channel_id: i64,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub properties: Value,
    pub episode: Option<SeriesEpisode>,
    /// Source signal publishes once; Series explicitly publishes once more.
    pub artwork_publications: u8,
}
fn text(v: &str, max: usize) -> bool {
    !v.is_empty() && v.len() <= max && v.is_ascii() && !v.bytes().any(|b| b.is_ascii_control())
}
pub fn episode_key(
    season: Option<u32>,
    episode: Option<u32>,
    subtitle: Option<&str>,
) -> Option<String> {
    if let (Some(season), Some(episode)) = (season, episode) {
        return (season > 0 && episode > 0).then(|| format!("s{season}e{episode}"));
    }
    subtitle
        .filter(|subtitle| text(subtitle, 255))
        .map(|subtitle| subtitle.trim().to_ascii_lowercase())
        .filter(|identity| !identity.is_empty())
}
fn iso(v: DateTime<Utc>) -> String {
    // Python aware datetime.isoformat() uses +00:00, preserves nonzero micros.
    let base = v.format("%Y-%m-%dT%H:%M:%S").to_string();
    if v.timestamp_subsec_micros() == 0 {
        format!("{base}+00:00")
    } else {
        format!("{base}.{:06}+00:00", v.timestamp_subsec_micros())
    }
}
fn date(v: DateTime<Utc>) -> bool {
    v.timestamp_subsec_nanos().is_multiple_of(1_000)
}

/// This bounded planner consumes one exact ASCII title/TVG and one pinned
/// channel/source, with stable episode identity and zero offsets.
/// SQL loader must refuse all other programme/property shapes BEFORE ACK.
/// Input ordering MUST be the captured Source queryset order; never sort after
/// comparison. Earliest airing replaces in-place, preserving insertion order.
pub fn series(
    now: DateTime<Utc>,
    (title, title_match): (&str, SeriesTitle),
    tvg: &str,
    channel_id: i64,
    source_id: i64,
    programmes: &[Programme],
    existing: &[Existing],
) -> Result<Vec<Intent>, Refusal> {
    if programmes.len() > CAP || existing.len() > CAP {
        return Err(Refusal::Capacity);
    }
    if !text(title, 255) || !text(tvg, 255) || channel_id <= 0 || source_id <= 0 {
        return Err(Refusal::Shape);
    }
    let mut unique: Vec<((String, String), &Programme)> = Vec::new();
    for p in programmes {
        if p.id <= 0
            || p.source_id != source_id
            || p.channel_id != channel_id
            || (title_match == SeriesTitle::Exact && !p.title.eq_ignore_ascii_case(title))
            || p.tvg_id != tvg
            || !text(&p.title, 255)
            || p.subtitle
                .as_deref()
                .is_some_and(|subtitle| !text(subtitle, 255))
        {
            return Err(Refusal::Shape);
        }
        if !date(p.start)
            || !date(p.end)
            || p.end <= p.start
            || p.start <= now
            || p.start > now + Duration::days(7)
        {
            return Err(Refusal::Date);
        }
        let identity =
            episode_key(p.season, p.episode, p.subtitle.as_deref()).ok_or(Refusal::Shape)?;
        let key = (p.title.trim().to_ascii_lowercase(), identity);
        if let Some((_, slot)) = unique.iter_mut().find(|(owned, _)| *owned == key) {
            if p.start < slot.start {
                *slot = p;
            }
        } else {
            unique.push((key, p));
        }
    }
    let mut exact = HashSet::new();
    let mut episodes = HashSet::new();
    for p in existing {
        if !text(&p.tvg_id, 255)
            || !text(&p.title, 255)
            || !date(p.start)
            || !date(p.end)
            || p.end <= p.start
        {
            return Err(Refusal::Identity);
        }
        // Source preload excludes recordings with end_time < now. Caller has
        // validated this property against actual Recording.end_time as well.
        if p.end < now {
            return Err(Refusal::Date);
        }
        exact.insert((p.tvg_id.clone(), p.start, p.end));
        episodes.insert(SeriesEpisode(
            p.tvg_id.clone(),
            p.title.trim().to_ascii_lowercase(),
            episode_key(p.season, p.episode, p.subtitle.as_deref()).ok_or(Refusal::Identity)?,
        ));
    }
    let mut output = Vec::new();
    for ((title, identity), p) in unique {
        let episode = SeriesEpisode(p.tvg_id.clone(), title, identity);
        if exact.contains(&(p.tvg_id.clone(), p.start, p.end)) || episodes.contains(&episode) {
            continue;
        }
        exact.insert((p.tvg_id.clone(), p.start, p.end));
        episodes.insert(episode.clone());
        output.push(Intent { channel_id, start:p.start, end:p.end, artwork_publications:2,
            episode: Some(episode),
            properties:json!({"program":{"id":p.id,"tvg_id":p.tvg_id,"title":p.title,"sub_title":p.subtitle,"description":p.description,"original_air_date":p.original_air_date,"start_time":iso(p.start),"end_time":iso(p.end),"season":p.season,"episode":p.episode,"onscreen_episode":null,"epg_source_id":source_id}}) });
    }
    Ok(output)
}

pub struct Recurring {
    pub id: i64,
    pub channel_id: i64,
    pub name: String,
    pub enabled: bool,
    pub day: NaiveDate,
    pub weekday: u32,
    pub start: NaiveTime,
    pub end: NaiveTime,
}
/// One explicit UTC date, nonempty ASCII name, captured overnight +1day only;
/// equal-time, DST and offset modes remain unqualified.
/// Disabled is Source integer0 without deleting existing schedules/recordings.
/// `existing` is the exact DB predicate (channel,start,properties.rule.id), not
/// a caller-provided shortcut inferred from a programme ID.
pub fn recurring(
    now: DateTime<Utc>,
    r: &Recurring,
    existing: bool,
) -> Result<Option<Intent>, Refusal> {
    if !r.enabled {
        return Ok(None);
    }
    if r.id <= 0 || r.channel_id <= 0 || !text(&r.name, 255) || r.weekday > 6 {
        return Err(Refusal::Shape);
    }
    // The Source maintenance task intersects the saved date range with
    // [today, today + 14 days], then filters by weekday. An empty
    // intersection is a successful no-op, including expired enabled rules.
    if r.day < now.date_naive()
        || r.day > (now + Duration::days(14)).date_naive()
        || r.day.weekday().num_days_from_monday() != r.weekday
    {
        return Ok(None);
    }
    if r.end == r.start {
        return Err(Refusal::Date);
    }
    let start = r.day.and_time(r.start).and_utc();
    let end = r.day.and_time(r.end).and_utc();
    // Actual Source11 UTC recurring-positive crosses midnight. Source combines
    // the date/time then advances end by one day; equal-time stays unqualified.
    let end = if end < start {
        end + Duration::days(1)
    } else {
        end
    };
    if !date(start) || !date(end) {
        return Err(Refusal::Date);
    }
    if start <= now || existing {
        return Ok(None);
    }
    Ok(Some(Intent {
        channel_id: r.channel_id,
        start,
        end,
        artwork_publications: 1,
        episode: None,
        properties: json!({"rule":{"type":"recurring","id":r.id,"days_of_week":[r.weekday],"name":r.name},"status":"scheduled","description":r.name,"program":{"title":r.name,"description":r.name,"start_time":iso(start),"end_time":iso(end)}}),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn now() -> DateTime<Utc> {
        "2026-11-12T23:34:44+00:00".parse().unwrap()
    }
    fn p(id: i64, episode: u32, hours: i64) -> Programme {
        Programme {
            id,
            source_id: 1,
            channel_id: 2,
            tvg_id: "rust.owned.configured".into(),
            title: "Owned Exact Series".into(),
            subtitle: Some(format!("Owned Episode {episode}")),
            original_air_date: String::new(),
            description: None,
            start: now() + Duration::hours(hours),
            end: now() + Duration::hours(hours + 1),
            season: Some(1),
            episode: Some(episode),
        }
    }
    fn plan(programmes: &[Programme], existing: &[Existing]) -> Result<Vec<Intent>, Refusal> {
        series(
            now(),
            ("Owned Exact Series", SeriesTitle::Exact),
            "rust.owned.configured",
            2,
            1,
            programmes,
            existing,
        )
    }
    #[test]
    fn same_episode_keeps_earliest_and_preserves_other_episode_order() {
        let late = p(2, 1, 26);
        let early = p(1, 1, 24);
        let second = p(3, 2, 28);
        let intents = plan(&[late, early, second], &[]).unwrap();
        assert_eq!(intents.len(), 2);
        assert_eq!(intents[0].properties["program"]["id"], 1);
        assert_eq!(intents[1].properties["program"]["id"], 3);
        assert!(intents.iter().all(|i| i.artwork_publications == 2));
    }
    #[test]
    fn contains_keeps_same_episode_number_for_each_matching_title() {
        let mut morning = p(1, 1, 24);
        morning.title = "Morning News".into();
        let mut evening = p(2, 1, 25);
        evening.title = "Evening News".into();
        let mut replay = p(3, 1, 26);
        replay.title = "EVENING NEWS".into();
        let inputs = [morning, evening, replay];
        let choose = |scope, existing: &[Existing]| {
            series(
                now(),
                ("News", scope),
                "rust.owned.configured",
                2,
                1,
                &inputs,
                existing,
            )
        };
        let selected = choose(SeriesTitle::Contains, &[]).unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|r| r.properties["program"]["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let saved = Existing {
            tvg_id: inputs[1].tvg_id.clone(),
            title: "  EVENING NEWS  ".into(),
            start: inputs[1].start,
            end: inputs[1].end,
            season: Some(1),
            episode: Some(1),
            subtitle: None,
        };
        let remaining = choose(SeriesTitle::Contains, &[saved]).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].properties["program"]["id"], 1);
        assert_eq!(choose(SeriesTitle::Exact, &[]).unwrap_err(), Refusal::Shape);
    }

    #[test]
    fn refresh_changes_programme_id_and_times_but_episode_still_deduplicates() {
        let original = p(1, 1, 24);
        let shifted = p(99, 1, 25);
        let existing = Existing {
            tvg_id: original.tvg_id,
            title: original.title,
            start: original.start,
            end: original.end,
            season: Some(1),
            episode: Some(1),
            subtitle: None,
        };
        assert!(plan(&[shifted], &[existing]).unwrap().is_empty());
    }
    #[test]
    fn missing_identity_refuses_instead_of_suppressing_unrelated_airings() {
        let mut programme = p(1, 1, 24);
        programme.season = Some(0);
        assert_eq!(plan(&[programme], &[]).unwrap_err(), Refusal::Shape);
    }
    #[test]
    fn microseconds_use_python_six_digit_serialization() {
        assert_eq!(
            iso(now() + Duration::milliseconds(125)),
            "2026-11-12T23:34:44.125000+00:00"
        );
        assert_eq!(iso(now()), "2026-11-12T23:34:44+00:00");
    }
    #[test]
    fn recurring_enabled_empty_calendar_window_is_a_normal_noop() {
        let now = now();
        let mut rule = Recurring {
            id: 1,
            channel_id: 2,
            name: "Owned Recurring".into(),
            enabled: true,
            day: now.date_naive() + Duration::days(14),
            weekday: now.date_naive().weekday().num_days_from_monday(),
            start: NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
            end: NaiveTime::from_hms_opt(23, 0, 0).unwrap(),
        };
        assert!(recurring(now, &rule, false).unwrap().is_some());
        rule.day += Duration::days(1);
        rule.weekday = rule.day.weekday().num_days_from_monday();
        assert!(recurring(now, &rule, false).unwrap().is_none());
        rule.day = now.date_naive() - Duration::days(1);
        rule.weekday = rule.day.weekday().num_days_from_monday();
        assert!(recurring(now, &rule, false).unwrap().is_none());
        rule.day = now.date_naive() + Duration::days(2);
        assert!(recurring(now, &rule, false).unwrap().is_none());
        rule.day = now.date_naive();
        rule.weekday = rule.day.weekday().num_days_from_monday();
        assert!(recurring(now, &rule, false).unwrap().is_none());
        rule.day += Duration::days(7);
        rule.end = rule.start;
        assert!(matches!(recurring(now, &rule, false), Err(Refusal::Date)));
    }

    #[test]
    fn recurring_disabled_is_noop_and_repeat_creates_no_second_intent() {
        let mut rule = Recurring {
            id: 1,
            channel_id: 2,
            name: "Owned Recurring".into(),
            enabled: false,
            day: NaiveDate::from_ymd_opt(2026, 11, 13).unwrap(),
            weekday: 4,
            start: NaiveTime::from_hms_opt(23, 34, 44).unwrap(),
            end: NaiveTime::from_hms_opt(23, 59, 44).unwrap(),
        };
        assert!(recurring(now(), &rule, false).unwrap().is_none());
        rule.enabled = true;
        let intent = recurring(now(), &rule, false).unwrap().unwrap();
        assert_eq!(intent.artwork_publications, 1);
        assert_eq!(intent.properties["rule"]["id"], 1);
        assert!(recurring(now(), &rule, true).unwrap().is_none());
    }
}

#[cfg(test)]
mod source_normal_cases {
    use super::*;
    fn time(v: &Value) -> DateTime<Utc> {
        v.as_str().unwrap().parse().unwrap()
    }
    #[test]
    fn captured_source_positive_repeat_next_episode_and_recurring_decisions() {
        let data: Value = serde_json::from_str(include_str!(
            "../datasets/source-configured-recordings-normal.json"
        ))
        .unwrap();
        assert_eq!(
            data["raw_source_sha256"],
            "58e1bd132e8e1dbd876f8426245fc267ee42732e55471eab5470490f6316f5c3"
        );
        for c in data["cases"].as_array().unwrap() {
            let now = time(&c["source_command_began_utc"]);
            let expected = c["source_recordings_created"].as_array().unwrap();
            let actual = if c["name"].as_str().unwrap().starts_with("series-") {
                let rules = &c["source_dvr_settings_before"][0]["value"]["series_rules"][0];
                let programmes: Vec<Programme> = c["source_programmes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| Programme {
                        id: p["id"].as_i64().unwrap(),
                        source_id: rules["epg_source_id"].as_i64().unwrap(),
                        channel_id: rules["channel_id"].as_i64().unwrap(),
                        tvg_id: p["tvg_id"].as_str().unwrap().into(),
                        title: p["title"].as_str().unwrap().into(),
                        subtitle: p["sub_title"].as_str().map(str::to_owned),
                        original_air_date: String::new(),
                        description: None,
                        start: time(&p["start_time"]),
                        end: time(&p["end_time"]),
                        season: p["custom_properties"]["season"].as_u64().map(|n| n as u32),
                        episode: p["custom_properties"]["episode"].as_u64().map(|n| n as u32),
                    })
                    .collect();
                let existing: Vec<Existing> = c["source_recordings_before"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|r| r["custom_properties"]["program"]["tvg_id"].is_string())
                    .map(|r| {
                        let p = &r["custom_properties"]["program"];
                        Existing {
                            tvg_id: p["tvg_id"].as_str().unwrap().into(),
                            title: p["title"].as_str().unwrap().into(),
                            start: time(&p["start_time"]),
                            end: time(&p["end_time"]),
                            subtitle: p["sub_title"].as_str().map(str::to_owned),
                            season: p["season"].as_u64().map(|n| n as u32),
                            episode: p["episode"].as_u64().map(|n| n as u32),
                        }
                    })
                    .collect();
                series(
                    now,
                    (rules["title"].as_str().unwrap(), SeriesTitle::Exact),
                    rules["tvg_id"].as_str().unwrap(),
                    rules["channel_id"].as_i64().unwrap(),
                    rules["epg_source_id"].as_i64().unwrap(),
                    &programmes,
                    &existing,
                )
                .unwrap()
            } else {
                let rules: Vec<&Value> = c["source_rules_before"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|r| r["enabled"] == true)
                    .collect();
                if rules.is_empty() {
                    Vec::new()
                } else {
                    assert_eq!(rules.len(), 1);
                    let r = rules[0];
                    let rule = Recurring {
                        id: r["id"].as_i64().unwrap(),
                        channel_id: r["channel_id"].as_i64().unwrap(),
                        name: r["name"].as_str().unwrap().into(),
                        enabled: true,
                        day: r["start_date"].as_str().unwrap().parse().unwrap(),
                        weekday: r["days_of_week"][0].as_u64().unwrap() as u32,
                        start: r["start_time"].as_str().unwrap().parse().unwrap(),
                        end: r["end_time"].as_str().unwrap().parse().unwrap(),
                    };
                    let start = rule.day.and_time(rule.start).and_utc();
                    let exists = c["source_recordings_before"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|row| {
                            row["channel_id"].as_i64() == Some(rule.channel_id)
                                && time(&row["start_time"]) == start
                                && row["custom_properties"]["rule"]["id"].as_i64() == Some(rule.id)
                        });
                    recurring(now, &rule, exists).unwrap().into_iter().collect()
                }
            };
            assert_eq!(actual.len(), expected.len(), "{}", c["name"]);
            for (intent, row) in actual.iter().zip(expected) {
                assert_eq!(intent.channel_id, row["channel_id"].as_i64().unwrap());
                assert_eq!(intent.start, time(&row["start_time"]));
                assert_eq!(intent.end, time(&row["end_time"]));
                assert_eq!(intent.properties, row["custom_properties"], "{}", c["name"]);
                assert_eq!(
                    intent.artwork_publications,
                    if c["name"].as_str().unwrap().starts_with("series-") {
                        2
                    } else {
                        1
                    }
                );
            }
        }
    }
}
