//! Additive normal DVR metadata/path algorithm from Original channels/tasks.py.
//! Original Dispatcharr authors retain full algorithm/application credit.
use crate::recording_normal::{EffectiveGroups, NormalPaths};
use chrono::{DateTime, Datelike, Utc};
use dispatcharr_store::recording_capture::NormalCaptureSnapshot;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Component, PathBuf},
};

#[derive(Debug)]
pub(crate) enum Error {
    Unsupported(&'static str),
    PythonFormat,
}
fn truth(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
fn text(value: &Value) -> Result<String, Error> {
    Ok(match value {
        Value::Null => "None".into(),
        Value::Bool(v) => if *v { "True" } else { "False" }.into(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        _ => return Err(Error::Unsupported("Python structured str semantics")),
    })
}
fn safe_name(value: Option<&Value>) -> Result<String, Error> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    if !truth(value) {
        return Ok(String::new());
    }
    let value = value
        .as_str()
        .ok_or(Error::Unsupported("normal filename scalar type"))?;
    Ok(value
        .chars()
        .filter(|c| !"\\/:*?\"<>|".contains(*c))
        .collect::<String>()
        .trim()
        .to_string())
}
fn filled(properties: &Value, season: &mut Option<Value>, episode: &mut Option<Value>) {
    if !properties.is_object() {
        return;
    }
    if season.as_ref().is_none_or(Value::is_null) {
        *season = properties.get("season").cloned();
    }
    if episode.as_ref().is_none_or(Value::is_null) {
        *episode = properties.get("episode").cloned();
    }
    if season.as_ref().is_some_and(|v| !v.is_null())
        && episode.as_ref().is_some_and(|v| !v.is_null())
    {
        return;
    }
    let Some(onscreen) = properties.get("onscreen_episode").and_then(Value::as_str) else {
        return;
    };
    for (i, c) in onscreen.char_indices() {
        if !matches!(c, 's' | 'S') {
            continue;
        }
        let tail = &onscreen[i + 1..];
        let a = tail.bytes().take_while(u8::is_ascii_digit).count();
        if a == 0 || !matches!(tail.as_bytes().get(a), Some(b'e' | b'E')) {
            continue;
        }
        let rest = &tail[a + 1..];
        let b = rest.bytes().take_while(u8::is_ascii_digit).count();
        if b == 0 {
            continue;
        }
        if season.as_ref().is_none_or(Value::is_null) {
            *season = Some(Value::String(tail[..a].into()));
        }
        if episode.as_ref().is_none_or(Value::is_null) {
            *episode = Some(Value::String(rest[..b].into()));
        }
        break;
    }
}
fn integer(value: Option<&Value>) -> Result<String, Error> {
    let Some(v) = value.filter(|v| !v.is_null()) else {
        return Ok("0".into());
    };
    let raw = match v {
        Value::Bool(b) => if *b { "1" } else { "0" }.into(),
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                u.to_string()
            } else {
                return Err(Error::Unsupported("nonintegral Python int normalization"));
            }
        }
        _ => return Err(Error::Unsupported("Python int operand")),
    };
    let (negative, digits) = if let Some(v) = raw.strip_prefix('-') {
        (true, v)
    } else {
        (false, raw.strip_prefix('+').unwrap_or(&raw))
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::Unsupported("Python integer text"));
    }
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    Ok(if negative && digits != "0" {
        format!("-{digits}")
    } else {
        digits.into()
    })
}
fn render(
    template: &str,
    values: &BTreeMap<&str, String>,
    numeric: &[&str],
) -> Result<String, Error> {
    if template.len() > 4096 {
        return Err(Error::Unsupported("DVR template capacity"));
    }
    let mut out = String::new();
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' {
            if chars.peek() == Some(&'{') {
                chars.next();
                out.push('{');
                continue;
            }
            let mut field = String::new();
            let mut closed = false;
            for v in chars.by_ref() {
                if v == '}' {
                    closed = true;
                    break;
                }
                if v == '{' {
                    return Err(Error::PythonFormat);
                }
                field.push(v);
            }
            if !closed {
                return Err(Error::PythonFormat);
            }
            let (key, spec) = field.split_once(':').unwrap_or((&field, ""));
            if key.contains(['.', '[', ']', '!']) {
                return Err(Error::Unsupported("Python field access/conversion"));
            }
            let value = values.get(key).ok_or(Error::PythonFormat)?;
            if spec.is_empty() {
                out.push_str(value);
            } else if numeric.contains(&key) {
                let width = spec.strip_suffix('d').unwrap_or(spec);
                if width.is_empty() {
                    out.push_str(value);
                } else if width.bytes().all(|b| b.is_ascii_digit()) {
                    let n = width
                        .parse::<usize>()
                        .map_err(|_| Error::Unsupported("numeric format width"))?;
                    if n > 4096 {
                        return Err(Error::Unsupported("numeric format width capacity"));
                    }
                    let pad = if width.starts_with('0') { '0' } else { ' ' };
                    let amount = n.saturating_sub(value.len());
                    if pad == '0' && value.starts_with('-') {
                        out.push('-');
                        out.extend(std::iter::repeat_n('0', amount));
                        out.push_str(&value[1..]);
                    } else {
                        out.extend(std::iter::repeat_n(pad, amount));
                        out.push_str(value);
                    }
                } else {
                    return Err(Error::Unsupported("Python numeric format semantics"));
                }
            } else {
                return Err(Error::Unsupported("Python text format semantics"));
            }
        } else if c == '}' {
            if chars.peek() == Some(&'}') {
                chars.next();
                out.push('}');
            } else {
                return Err(Error::PythonFormat);
            }
        } else {
            out.push(c);
        }
        if out.len() > 4096 {
            return Err(Error::Unsupported("rendered path capacity"));
        }
    }
    Ok(out)
}
fn template(settings: &Value, key: &str, default: &str) -> Result<String, Error> {
    match settings.get(key) {
        None => Ok(default.into()),
        Some(Value::String(v)) => Ok(v.clone()),
        _ => Err(Error::PythonFormat),
    }
}
fn attempt(
    settings: &Value,
    key: &str,
    default: &str,
    values: &BTreeMap<&str, String>,
) -> Result<Option<String>, Error> {
    match template(settings, key, default).and_then(|t| {
        render(
            &t,
            values,
            &[
                "season",
                "episode",
                "start_year",
                "start_month",
                "start_day",
            ],
        )
    }) {
        Ok(value) => Ok(Some(value)),
        Err(Error::PythonFormat) => Ok(None),
        Err(e) => Err(e),
    }
}
fn original_air(value: Option<&Value>) -> Result<String, Error> {
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(String::new());
    };
    let raw = text(value)?.trim().to_string();
    let bytes = raw.as_bytes();
    let iso = bytes.len() == 10
        || (bytes.len() > 11 && bytes.get(10) == Some(&b'T'))
        || (bytes.len() >= 19
            && bytes.get(10).is_some_and(u8::is_ascii_whitespace)
            && raw.split_whitespace().nth(1).is_some_and(|part| {
                part.len() >= 8
                    && part.as_bytes()[2] == b':'
                    && part.as_bytes()[5] == b':'
                    && part.as_bytes()[..8]
                        .iter()
                        .enumerate()
                        .all(|(i, b)| i == 2 || i == 5 || b.is_ascii_digit())
            }));
    let compact8 = bytes.len() == 8 && bytes.iter().all(u8::is_ascii_digit);
    let mut words = raw.split_whitespace();
    let first = words.next().unwrap_or("");
    let second = words.next();
    let compact14 = first.len() == 14
        && first.bytes().all(|b| b.is_ascii_digit())
        && words.next().is_none()
        && second.is_none_or(|zone| {
            zone.len() == 5
                && matches!(zone.as_bytes()[0], b'+' | b'-')
                && zone.as_bytes()[1..].iter().all(u8::is_ascii_digit)
        });
    let date = if iso && raw.len() >= 10 && raw.is_char_boundary(10) {
        chrono::NaiveDate::parse_from_str(&raw[..10], "%Y-%m-%d").ok()
    } else if (compact8 || compact14) && raw.len() >= 8 && raw.is_char_boundary(8) {
        chrono::NaiveDate::parse_from_str(&raw[..8], "%Y%m%d").ok()
    } else {
        None
    };
    if let Some(date) = date {
        return Ok(date.format("%Y-%m-%d").to_string());
    }
    let v = Value::String(raw);
    Ok(safe_name(Some(&v))?
        .replace("..", "")
        .trim_matches([' ', '.'])
        .to_string())
}
pub(crate) fn resume_paths(snapshot: &NormalCaptureSnapshot) -> Result<Option<NormalPaths>, Error> {
    resume_properties(&snapshot.properties)
}
pub(crate) fn resume_properties(properties: &Value) -> Result<Option<NormalPaths>, Error> {
    let (Some(hls), Some(file)) = (
        properties.get("_hls_dir").and_then(Value::as_str),
        properties.get("file_path").and_then(Value::as_str),
    ) else {
        return Ok(None);
    };
    if !std::fs::metadata(hls).is_ok_and(|metadata| metadata.is_dir()) {
        return Ok(None);
    }
    let final_path = PathBuf::from(file);
    let hls_dir = PathBuf::from(hls);
    if !final_path.starts_with("/data/recordings") || !hls_dir.starts_with("/data/recordings") {
        return Err(Error::Unsupported("resume paths outside owned root"));
    }
    let filename = final_path
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or(Error::Unsupported("resume filename"))?
        .into();
    Ok(Some(NormalPaths {
        final_path,
        filename,
        playlist: hls_dir.join("index.m3u8"),
        segments: hls_dir.join("seg_%05d.ts"),
        hls_dir,
    }))
}
#[cfg(test)]
pub(crate) fn paths(
    snapshot: &NormalCaptureSnapshot,
    groups: &EffectiveGroups,
) -> Result<NormalPaths, Error> {
    paths_for_execution(
        snapshot,
        groups,
        snapshot.identity.start,
        snapshot.identity.end,
    )
}
pub(crate) fn paths_for_execution(
    snapshot: &NormalCaptureSnapshot,
    groups: &EffectiveGroups,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<NormalPaths, Error> {
    // Normalize the actual cached getter now, before accepted media/row effects.
    // Invalid Source ValueError/TypeError operands are None, not a refusal;
    // nonfinite/unsupported numeric domains remain explicit.
    crate::recording_output_profile::capture_id(groups.dvr.get("output_profile_id"))
        .map_err(Error::Unsupported)?;
    if !super::recording_normal::supported_comskip_settings(&groups.dvr) {
        return Err(Error::Unsupported("normal comskip adapter required"));
    }
    if snapshot.channel_epg_data_id.is_some()
        && snapshot
            .properties
            .get("program")
            .is_none_or(|p| p.get("id").is_none() && p.get("title").is_none())
    {
        return Err(Error::Unsupported(
            "normal EPG timeslot enrichment adapter required",
        ));
    }
    let props = &snapshot.properties;
    let program = props
        .get("program")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let metadata = snapshot.programme_custom_properties.as_ref();
    for value in metadata.into_iter().chain([&program, props]) {
        if value
            .get("onscreen_episode")
            .and_then(Value::as_str)
            .is_some_and(|s| s.chars().any(|c| !c.is_ascii() && c.is_numeric()))
        {
            return Err(Error::Unsupported("Unicode Python episode digit semantics"));
        }
    }
    if program
        .get("start_time")
        .is_some_and(|v| truth(v) && !v.is_string())
    {
        return Err(Error::Unsupported(
            "Python programme datetime scalar semantics",
        ));
    }
    let movie = metadata
        .and_then(|m| m.get("categories"))
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .any(|v| matches!(v.to_lowercase().as_str(), "movie" | "film"))
        });
    let mut season = None;
    let mut episode = None;
    if let Some(meta) = metadata {
        filled(meta, &mut season, &mut episode);
    }
    filled(&program, &mut season, &mut episode);
    filled(props, &mut season, &mut episode);
    let season = integer(season.as_ref())?;
    let episode = integer(episode.as_ref())?;
    let title = if program.is_object() {
        safe_name(program.get("title"))?
    } else {
        safe_name(Some(&Value::String(snapshot.channel_name.clone())))?
    };
    let subtitle = safe_name(program.get("sub_title"))?;
    let mut original = original_air(program.get("original_air_date"))?;
    if let Some(meta) = metadata {
        if let Some(v) = meta
            .get("previously_shown_details")
            .and_then(|v| v.get("start"))
        {
            let air = original_air(Some(v))?;
            if !air.is_empty() {
                original = air;
            }
        }
    }
    let broadcast = program
        .get("start_time")
        .and_then(Value::as_str)
        .and_then(|s| DateTime::parse_from_rfc3339(&s.replacen(' ', "T", 1)).ok())
        .map(|v| v.with_timezone(&Utc))
        .unwrap_or(start);
    let zone = groups
        .system
        .get("time_zone")
        .and_then(Value::as_str)
        .unwrap_or("UTC")
        .parse::<chrono_tz::Tz>()
        .unwrap_or(chrono_tz::UTC);
    let local = broadcast.with_timezone(&zone);
    let year = metadata
        .and_then(|v| v.get("date"))
        .filter(|v| truth(v))
        .map(text)
        .transpose()?
        .filter(|v| !v.is_empty())
        .map(|v| v.chars().take(4).collect::<String>())
        .unwrap_or_else(|| start.year().to_string());
    let mut values = BTreeMap::from([
        ("show", title.clone()),
        ("title", title),
        ("sub_title", subtitle),
        ("season", season.clone()),
        ("episode", episode.clone()),
        ("year", year),
        (
            "channel",
            safe_name(Some(&Value::String(snapshot.channel_name.clone())))?,
        ),
        ("start", start.format("%Y%m%d_%H%M%S").to_string()),
        ("end", end.format("%Y%m%d_%H%M%S").to_string()),
        ("start_date", local.format("%Y-%m-%d").to_string()),
        ("start_year", local.year().to_string()),
        ("start_month", local.month().to_string()),
        ("start_day", local.day().to_string()),
    ]);
    let mut relative = None;
    if !movie && (season == "0" || episode == "0") {
        values.insert("original_air_date", original);
        relative = attempt(
            &groups.dvr,
            "tv_fallback_template",
            "TV_Shows/{show}/{start}.mkv",
            &values,
        )?;
        values.remove("original_air_date");
        if relative.is_none() {
            let root = groups
                .dvr
                .get("tv_fallback_dir")
                .map(text)
                .transpose()?
                .unwrap_or_else(|| "TV_Shows".into());
            relative = Some(format!("{root}/{}/{}.mkv", values["show"], values["start"]));
        }
    }
    if relative.as_ref().is_none_or(|v| v.is_empty()) {
        relative = attempt(
            &groups.dvr,
            if movie {
                "movie_template"
            } else {
                "tv_template"
            },
            if movie {
                "Movies/{title} ({year}).mkv"
            } else {
                "TV_Shows/{show}/S{season:02d}E{episode:02d}.mkv"
            },
            &values,
        )?;
    }
    if movie && relative.as_ref().is_none_or(|v| v.is_empty()) {
        relative = attempt(
            &groups.dvr,
            "movie_fallback_template",
            "Movies/{start}.mkv",
            &values,
        )?
        .or_else(|| Some(format!("Movies/{}.mkv", values["start"])));
    }
    if !movie && relative.as_ref().is_none_or(|v| v.is_empty()) {
        relative = Some(render(
            "TV_Shows/{show}/S{season:02d}E{episode:02d}.mkv",
            &values,
            &["season", "episode"],
        )?);
    }
    let mut relative = relative.ok_or(Error::PythonFormat)?;
    if !relative.to_lowercase().ends_with(".mkv") {
        relative.push_str(".mkv");
    }
    if relative.starts_with("./") {
        relative = relative[2..].into();
    }
    let path = PathBuf::from(&relative);
    let mut final_path = if path.is_absolute() {
        PathBuf::new()
    } else {
        PathBuf::from("/data/recordings")
    };
    for component in path.components() {
        match component {
            Component::ParentDir => {
                final_path.pop();
            }
            Component::CurDir => {}
            part => final_path.push(part.as_os_str()),
        }
    }
    if !final_path.starts_with("/data/recordings") {
        return Err(Error::Unsupported(
            "normal output outside owned recording root",
        ));
    }
    let parent = final_path
        .parent()
        .ok_or(Error::Unsupported("normal output parent"))?;
    let filename = final_path
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or(Error::Unsupported("normal output filename"))?
        .to_string();
    let hls_dir = parent.join(format!(".dvr_{}_hls", snapshot.identity.recording));
    Ok(NormalPaths {
        final_path,
        filename,
        playlist: hls_dir.join("index.m3u8"),
        segments: hls_dir.join("seg_%05d.ts"),
        hls_dir,
    })
}

/// Pinned int(setting or100) policy for ordinary bounded scalar settings. Values
/// beyond a PostgreSQL offset are explicit unsupported, never silently clamped.
pub(crate) fn max_events(groups: &EffectiveGroups) -> Result<i64, Error> {
    let Some(v) = groups.system.get("max_system_events") else {
        return Ok(100);
    };
    if v.is_null()
        || v == &serde_json::json!(false)
        || v == &serde_json::json!(0)
        || v == &serde_json::json!("")
    {
        return Ok(100);
    }
    match v {
        Value::Bool(true) => Ok(1),
        Value::Number(n) => n
            .as_i64()
            .or_else(|| {
                n.as_f64()
                    .filter(|n| n.is_finite() && *n >= i64::MIN as f64 && *n < i64::MAX as f64)
                    .map(|v| v.trunc() as i64)
            })
            .ok_or(Error::Unsupported("event history offset capacity")),
        Value::String(s) => {
            let raw = s.trim();
            let bytes = raw.as_bytes();
            if bytes.iter().enumerate().any(|(i, b)| {
                *b == b'_'
                    && (i == 0
                        || i + 1 == bytes.len()
                        || !bytes[i - 1].is_ascii_digit()
                        || !bytes[i + 1].is_ascii_digit())
            }) {
                return Ok(100);
            }
            match raw.replace('_', "").parse() {
                Ok(v) => Ok(v),
                Err(_) => Ok(100),
            }
        }
        _ => Ok(100),
    }
}

#[cfg(test)]
mod normal_policy_controls {
    use super::*;
    fn snapshot() -> NormalCaptureSnapshot {
        let start = DateTime::parse_from_rfc3339("2026-11-14T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        NormalCaptureSnapshot {
            identity: dispatcharr_store::recording_capture::RecordingIdentity {
                recording: 7,
                channel: 9,
                start,
                end: start + chrono::Duration::seconds(95),
            },
            channel_uuid: uuid::Uuid::nil(),
            channel_name: "News / Ω".into(),
            channel_epg_data_id: None,
            channel_stream_profile_id: Some(27),
            channel_has_override: false,
            periodic_name: None,
            properties: serde_json::json!({"status":"scheduled","program":{"title":"Média / Épisode"}}),
            programme_custom_properties: None,
            actual_read_utc: start,
        }
    }
    fn groups() -> EffectiveGroups {
        EffectiveGroups {
            dvr: serde_json::json!({}),
            system: serde_json::json!({"time_zone":"UTC"}),
            stream: serde_json::json!({}),
        }
    }
    #[test]
    fn normal_paths_follow_metadata_templates_not_captured_duration_or_poster() {
        let mut snapshot = snapshot();
        let groups = groups();
        let fallback = paths(&snapshot, &groups).unwrap();
        assert_eq!(
            fallback.final_path,
            PathBuf::from("/data/recordings/TV_Shows/Média  Épisode/20261114_120000.mkv")
        );
        snapshot.identity.end = snapshot.identity.start + chrono::Duration::days(1);
        assert_eq!(
            paths(&snapshot, &groups).unwrap().final_path,
            fallback.final_path
        );
        snapshot.properties["program"]["id"] = serde_json::json!(42);
        snapshot.programme_custom_properties =
            Some(serde_json::json!({"categories":["Film"],"date":"2024-01-01"}));
        assert_eq!(
            paths(&snapshot, &groups).unwrap().final_path,
            PathBuf::from("/data/recordings/Movies/Média  Épisode (2024).mkv")
        );
    }
    #[test]
    fn unsupported_valid_python_format_cannot_silently_take_source_error_fallback() {
        let values = BTreeMap::from([("season", "15".to_string())]);
        assert!(matches!(
            render("{season:02x}", &values, &["season"]),
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            render("{missing}", &values, &["season"]),
            Err(Error::PythonFormat)
        ));
        assert_eq!(
            original_air(Some(&Value::String("2020-01-01suffix".into()))).unwrap(),
            "2020-01-01suffix"
        );
    }
    #[test]
    fn normal_profile_selection_does_not_bypass_comskip_guard() {
        let row = snapshot();
        let mut actual = groups();
        actual.dvr["output_profile_id"] = serde_json::json!(" +0007 ");
        assert!(paths_for_execution(&row, &actual, row.identity.start, row.identity.end).is_ok());
        actual.dvr["output_profile_id"] = serde_json::json!({"invalid":true});
        assert!(paths_for_execution(&row, &actual, row.identity.start, row.identity.end).is_ok());
        actual.dvr["comskip_enabled"] = serde_json::json!(true);
        assert!(matches!(
            paths_for_execution(&row, &actual, row.identity.start, row.identity.end),
            Err(Error::Unsupported("normal comskip adapter required"))
        ));
    }
    #[test]
    fn logger_limit_distinguishes_numeric_zero_from_truthy_zero_text() {
        let mut group = groups();
        group.system["max_system_events"] = serde_json::json!(0);
        assert_eq!(max_events(&group).unwrap(), 100);
        group.system["max_system_events"] = serde_json::json!("0");
        assert_eq!(max_events(&group).unwrap(), 0);
        group.system["max_system_events"] = serde_json::json!("1__0");
        assert_eq!(max_events(&group).unwrap(), 100);
        group.system["max_system_events"] = serde_json::json!(-1);
        assert_eq!(max_events(&group).unwrap(), -1);
    }
}
