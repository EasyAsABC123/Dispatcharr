//! Private inert draft of Source3's normal manual-TV recording decisions.
//! Original Dispatcharr authors retain algorithm/application credit.
//! No worker constructor, due capability, ACK or result-writer attachment.
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use std::path::PathBuf;

pub(crate) const TV_TEMPLATE: &str = "TV_Shows/{show}/S{season:02d}E{episode:02d}.mkv";
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    Settings,
    History,
    Properties,
    Title,
    Episode,
    Recovery,
    Poster,
    Identity,
}

/// Values obtained after bounded cache-authoritative Source-group resolution.
/// This data alone grants no execution authority. Cache resolution is not yet
/// implemented in this draft; SQL snapshots must never substitute for it.
pub(crate) struct EffectiveGroups {
    pub dvr: Value,
    pub system: Value,
    pub stream: Value,
}

#[derive(Clone, Debug)]
pub(crate) struct NormalPaths {
    pub final_path: PathBuf,
    pub hls_dir: PathBuf,
    pub filename: String,
    pub playlist: PathBuf,
    pub segments: PathBuf,
}

pub(crate) fn paths(
    recording: i64,
    properties: &Value,
    groups: &EffectiveGroups,
    history_rows: i64,
) -> Result<NormalPaths, Refusal> {
    if !(1..=i64::from(i32::MAX)).contains(&recording) {
        return Err(Refusal::Identity);
    }
    let settings = groups.dvr.as_object().ok_or(Refusal::Settings)?;
    let system = groups.system.as_object().ok_or(Refusal::Settings)?;
    let stream = groups.stream.as_object().ok_or(Refusal::Settings)?;
    // Intentionally only the captured effective normal configuration. Missing
    // fields below use the exact Source getter defaults, not a SQL-error fallback.
    if settings
        .get("tv_template")
        .is_some_and(|value| value.as_str() != Some(TV_TEMPLATE))
        || settings
            .get("comskip_enabled")
            .is_some_and(|v| v != &json!(false))
        || settings
            .get("output_profile_id")
            .is_some_and(|v| !v.is_null())
        || settings
            .get("pre_offset_minutes")
            .is_some_and(|v| v != &json!(0))
        || settings
            .get("post_offset_minutes")
            .is_some_and(|v| v != &json!(0))
        || system.get("time_zone") != Some(&json!("UTC"))
        || stream
            .get("default_stream_profile")
            .is_some_and(|value| !value.is_null())
    {
        return Err(Refusal::Settings);
    }
    let max_events = match system.get("max_system_events") {
        None | Some(Value::Null) => 100,
        Some(value) => value
            .as_u64()
            .filter(|v| (2..=100).contains(v))
            .ok_or(Refusal::Settings)?,
    };
    // Source int(value or100): integer zero means100. This first scope refuses
    // zero/bools/strings rather than pretending all Python normalization is ported.
    if history_rows < 0
        || u64::try_from(history_rows).map_err(|_| Refusal::History)? + 2 > max_events
    {
        return Err(Refusal::History);
    }
    let props = properties.as_object().ok_or(Refusal::Properties)?;
    if props.get("status") != Some(&json!("scheduled")) {
        return Err(Refusal::Properties);
    }
    if [
        "_hls_dir",
        "file_path",
        "file_name",
        "started_at",
        "ended_at",
        "bytes_written",
        "remux_success",
    ]
    .iter()
    .any(|key| props.contains_key(*key))
    {
        return Err(Refusal::Recovery);
    }
    let program = props
        .get("program")
        .and_then(Value::as_object)
        .ok_or(Refusal::Properties)?;
    // This avoids making guesses about _parse_epg_tv_movie_info/provider paths.
    if program.keys().any(|key| key != "title") {
        return Err(Refusal::Properties);
    }
    let title = program
        .get("title")
        .and_then(Value::as_str)
        .ok_or(Refusal::Title)?;
    if title.is_empty()
        || title.len() > 128
        || title.trim() != title
        || !title
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || v == b' ' || v == b'-' || v == b'_')
    {
        return Err(Refusal::Title);
    }
    let season = props
        .get("season")
        .and_then(Value::as_u64)
        .filter(|v| (1..=99).contains(v))
        .ok_or(Refusal::Episode)?;
    let episode = props
        .get("episode")
        .and_then(Value::as_u64)
        .filter(|v| (1..=99).contains(v))
        .ok_or(Refusal::Episode)?;
    if props
        .get("poster_logo_id")
        .and_then(Value::as_i64)
        .filter(|v| (1..=i64::from(i32::MAX)).contains(v))
        .is_none()
        || props.contains_key("poster_url")
    {
        return Err(Refusal::Poster);
    }
    let filename = format!("S{season:02}E{episode:02}.mkv");
    let parent = PathBuf::from("/data/recordings/TV_Shows").join(title);
    let hls_dir = parent.join(format!(".dvr_{recording}_hls"));
    Ok(NormalPaths {
        final_path: parent.join(&filename),
        filename,
        playlist: hls_dir.join("index.m3u8"),
        segments: hls_dir.join("seg_%05d.ts"),
        hls_dir,
    })
}

fn python_naive_utc(now: DateTime<Utc>) -> String {
    // Python datetime.now(): omit the fractional part iff microsecond is zero,
    // otherwise exactly six digits. Actual operation time, no reference clock.
    if now.timestamp_subsec_micros() == 0 {
        now.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        format!(
            "{}.{:06}",
            now.format("%Y-%m-%d %H:%M:%S"),
            now.timestamp_subsec_micros()
        )
    }
}

fn bounded_object(value: &Value) -> Result<Map<String, Value>, Refusal> {
    if serde_json::to_vec(value)
        .map_err(|_| Refusal::Properties)?
        .len()
        > 8192
    {
        return Err(Refusal::Properties);
    }
    value.as_object().cloned().ok_or(Refusal::Properties)
}

/// Source whitelist PATCH only. The one authoritative Store merge engine applies
/// it to the actual fresh bounded row; this cannot overwrite unrelated fields.
pub(crate) fn prime_patch(
    recording: i64,
    paths: &NormalPaths,
    actual_started: DateTime<Utc>,
) -> Result<Value, Refusal> {
    let mut props = Map::new();
    props.insert("status".into(), json!("recording"));
    props.insert("started_at".into(), json!(python_naive_utc(actual_started)));
    props.insert("file_name".into(), json!(paths.filename));
    props.insert(
        "file_path".into(),
        json!(paths.final_path.to_str().ok_or(Refusal::Title)?),
    );
    props.insert(
        "_hls_dir".into(),
        json!(paths.hls_dir.to_str().ok_or(Refusal::Title)?),
    );
    let url = format!("/api/channels/recordings/{recording}/hls/index.m3u8");
    props.insert("file_url".into(), json!(url));
    props.insert("output_file_url".into(), json!(url));
    let value = Value::Object(props);
    bounded_object(&value)?;
    Ok(value)
}

pub(crate) fn source_iso(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::Secs, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatcharr_store::recording_capture::{CaptureStore, PropertyWrite};
    fn props() -> Value {
        json!({"status":"scheduled","program":{"title":"Owned Parented Artwork"},"season":1,"episode":1,"poster_logo_id":1,"owned_fixture_marker":"preserve-me"})
    }
    fn groups() -> EffectiveGroups {
        EffectiveGroups {
            dvr: json!({}),
            system: json!({"time_zone":"UTC","max_system_events":100}),
            stream: json!({"default_stream_profile":null}),
        }
    }
    #[test]
    fn source3_paths_and_separate_hls_counter_survive_fresh_merge() {
        let base = props();
        let paths = paths(1, &base, &groups(), 0).expect("captured normal path");
        assert_eq!(
            paths.final_path.to_str(),
            Some("/data/recordings/TV_Shows/Owned Parented Artwork/S01E01.mkv")
        );
        let start = "2026-11-13T16:06:44.769585Z".parse().expect("Source start");
        let patch = prime_patch(1, &paths, start).expect("Source patch");
        let mut primed =
            CaptureStore::merge_properties(1, &base, PropertyWrite::Prime(patch)).expect("prime");
        assert_eq!(primed["started_at"], "2026-11-13 16:06:44.769585");
        primed["late_artwork_marker"] = json!("keep fresh row");
        let end = "2026-11-13T16:07:13.273917Z".parse().expect("Source end");
        let cleaned = CaptureStore::merge_properties(1, &primed, PropertyWrite::RemovedOwnedHls)
            .expect("physical cleanup receipt required separately");
        let done = CaptureStore::merge_properties(
            1,
            &cleaned,
            PropertyWrite::CompletedNormal {
                actual_ended: end,
                bytes_written: 1_062_576,
            },
        )
        .expect("normal finish");
        assert_eq!(done["bytes_written"], 1_062_576);
        assert_ne!(done["bytes_written"], 947_301);
        assert_eq!(done["late_artwork_marker"], "keep fresh row");
        assert_eq!(done["owned_fixture_marker"], "preserve-me");
        assert!(done.get("_hls_dir").is_none());
        assert_eq!(done["file_url"], "/api/channels/recordings/1/file/");
    }
    #[test]
    fn unsafe_or_unqualified_path_domains_are_refused_without_mutation() {
        for title in [
            "../escape",
            "/absolute",
            "a/b",
            "a\\b",
            "",
            " trailing ",
            "\u{202e}name",
        ] {
            let mut value = props();
            value["program"]["title"] = json!(title);
            let before = value.clone();
            assert!(paths(1, &value, &groups(), 0).is_err());
            assert_eq!(value, before);
        }
        for key in ["_hls_dir", "file_path", "started_at"] {
            let mut value = props();
            value[key] = json!("old");
            assert!(matches!(
                paths(1, &value, &groups(), 0),
                Err(Refusal::Recovery)
            ));
        }
    }
    #[test]
    fn effective_cache_settings_not_database_defaults_control_admission() {
        let mut actual = groups();
        actual.dvr["comskip_enabled"] = json!(true);
        assert!(matches!(
            paths(1, &props(), &actual, 0),
            Err(Refusal::Settings)
        ));
        actual = groups();
        actual.system["max_system_events"] = json!(2);
        assert!(paths(1, &props(), &actual, 0).is_ok());
        assert!(matches!(
            paths(1, &props(), &actual, 1),
            Err(Refusal::History)
        ));
        for value in [json!(true), json!("100"), json!(-1), json!(0)] {
            actual = groups();
            actual.system["max_system_events"] = value;
            assert!(matches!(
                paths(1, &props(), &actual, 0),
                Err(Refusal::Settings)
            ));
        }
    }
    #[test]
    fn concurrent_stopped_row_is_never_overwritten_as_normal() {
        let mut fresh = props();
        fresh["status"] = json!("stopped");
        let before = fresh.clone();
        let now = "2026-11-13T16:07:13Z".parse().expect("date");
        assert!(CaptureStore::merge_properties(
            1,
            &fresh,
            PropertyWrite::CompletedNormal {
                actual_ended: now,
                bytes_written: 1
            }
        )
        .is_err());
        assert_eq!(fresh, before);
    }
}
