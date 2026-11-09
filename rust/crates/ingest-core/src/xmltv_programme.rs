//! Original Dispatcharr streaming programme-row projection, without SQL effects.
use crate::{
    extract_xmltv_properties, parse_xmltv_time, projection::DECIMAL_ZEROS, trim,
    xmltv_metadata::integer, XmltvElement, XmltvMetadataError, XmltvTimeError,
};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde_json::Value;
use std::sync::OnceLock;

#[derive(Clone, Debug, PartialEq)]
pub struct XmltvProgramme {
    pub epg_id: i64,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub title: String,
    pub sub_title: Option<String>,
    pub description: Option<String>,
    pub tvg_id: String,
    pub custom_properties: Option<Value>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XmltvProgrammeError {
    MissingAttribute,
    Time(XmltvTimeError),
    Metadata(XmltvMetadataError),
    Integer,
}
fn description_patterns() -> &'static [Regex; 3] {
    static PATTERNS: OnceLock<[Regex; 3]> = OnceLock::new();
    PATTERNS.get_or_init(||{
        let mut digits=String::from("[");
        for zero in DECIMAL_ZEROS {digits.push_str(&format!("\\u{{{zero:x}}}-\\u{{{:x}}}",zero+9));}
        digits.push(']');
        // Python IGNORECASE includes long-s and dotted/dotless-I. Pin the
        // original decimal ranges instead of inheriting a newer Unicode table.
        let prefix=r"^[\s\x1c-\x1f\-:]*";
        let suffix=r"[\s\x1c-\x1f\-:.]*";
        let space=r"[\s\x1c-\x1f]*";
        [
            Regex::new(&format!("{prefix}[sSſ]({digits}+){space}[eE]({digits}+){suffix}")).unwrap(),
            Regex::new(&format!("{prefix}[sSſ][eE][aA][sSſ][oO][nN]{space}({digits}+){space}[eE][pP][iIİı][sSſ][oO][dD][eE]{space}({digits}+){suffix}")).unwrap(),
            Regex::new(&format!("{prefix}({digits}+)x({digits}{{2,}}){suffix}")).unwrap(),
        ]
    })
}
fn description_episode(text: &str) -> Result<Option<(Value, Value, String)>, XmltvProgrammeError> {
    for pattern in description_patterns() {
        if let Some(found) = pattern.captures(text) {
            let season = integer(&found[1], false).ok_or(XmltvProgrammeError::Integer)?;
            let episode = integer(&found[2], false).ok_or(XmltvProgrammeError::Integer)?;
            return Ok(Some((
                season,
                episode,
                trim(&text[found.get(0).unwrap().end()..]).into(),
            )));
        }
    }
    Ok(None)
}
/// Caller resolves membership in the mapped-channel snapshot first. Original
/// identifiers are matched exactly: programme@channel is not stripped here.
/// Data errors skip a row in the original loop; capacity errors must separately
/// fail a job instead of allowing an incomplete guide to replace stored data.
pub fn project_xmltv_programme(
    node: &XmltvElement,
    epg_id: i64,
    max_title_characters: usize,
) -> Result<XmltvProgramme, XmltvProgrammeError> {
    let attribute = |name: &str| {
        node.attrs
            .get(name)
            .ok_or(XmltvProgrammeError::MissingAttribute)
    };
    let tvg_id = attribute("channel")?.clone();
    let start_time = parse_xmltv_time(attribute("start")?).map_err(XmltvProgrammeError::Time)?;
    let end_time = parse_xmltv_time(attribute("stop")?).map_err(XmltvProgrammeError::Time)?;
    let mut custom = extract_xmltv_properties(node).map_err(XmltvProgrammeError::Metadata)?;
    let mut title = None;
    let mut description = None;
    let mut sub_title = None;
    for child in &node.children {
        match child.tag.as_str() {
            "title" => {
                title = Some(
                    child
                        .text
                        .as_deref()
                        .filter(|s| !s.is_empty())
                        .unwrap_or("No Title")
                        .to_string(),
                )
            }
            "desc" => description = Some(child.text.clone().unwrap_or_default()),
            "sub-title" => sub_title = Some(child.text.clone().unwrap_or_default()),
            _ => {}
        }
    }
    let title = title
        .unwrap_or_else(|| "No Title".into())
        .chars()
        .take(max_title_characters)
        .collect();
    if let Some(text) = description.as_deref().filter(|text| !text.is_empty()) {
        let has_season = custom.get("season").is_some_and(|value| !value.is_null());
        let has_episode = custom.get("episode").is_some_and(|value| !value.is_null());
        if !has_season || !has_episode {
            if let Some((season, episode, cleaned)) = description_episode(text)? {
                if !has_season {
                    custom.insert("season".into(), season);
                }
                if !has_episode {
                    custom.insert("episode".into(), episode);
                }
                custom.insert(
                    "season_episode_source".into(),
                    Value::String("description".into()),
                );
                description = Some(cleaned);
            }
        }
    }
    Ok(XmltvProgramme {
        epg_id,
        start_time,
        end_time,
        title,
        sub_title,
        description,
        tvg_id,
        custom_properties: (!custom.is_empty()).then_some(Value::Object(custom)),
    })
}
