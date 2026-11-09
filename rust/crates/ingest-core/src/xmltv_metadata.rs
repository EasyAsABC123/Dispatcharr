//! XMLTV metadata projection ported from original Dispatcharr authors' tasks.
//! Parser recovery and XML decoding are deliberately separate from this tree API.
use crate::{
    projection::{integer_literal, DECIMAL_ZEROS},
    trim,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::{collections::BTreeMap, str::FromStr, sync::OnceLock};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct XmltvElement {
    pub tag: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub attrs: BTreeMap<String, String>,
    #[serde(default)]
    pub children: Vec<XmltvElement>,
}
impl XmltvElement {
    fn all<'a>(&'a self, tag: &'a str) -> impl Iterator<Item = &'a Self> {
        self.children.iter().filter(move |node| node.tag == tag)
    }
    fn first(&self, tag: &str) -> Option<&Self> {
        self.children.iter().find(|node| node.tag == tag)
    }
    fn attribute(&self, name: &str) -> Option<&str> {
        self.attrs
            .get(name)
            .map(String::as_str)
            .filter(|s| !s.is_empty())
    }
    fn text(&self) -> Option<&str> {
        self.text.as_deref().filter(|s| !s.is_empty())
    }
    fn nonblank(&self) -> Option<&str> {
        self.text().map(trim).filter(|s| !s.is_empty())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XmltvMetadataError {
    Capacity,
    Integer,
}
fn bounded(node: &XmltvElement, depth: usize, count: &mut usize, bytes: &mut usize) -> bool {
    *count += 1;
    *bytes += node.tag.len()
        + node.text.as_ref().map_or(0, String::len)
        + node
            .attrs
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>();
    depth <= 32
        && *count <= 4096
        && *bytes <= 4 * 1024 * 1024
        && node
            .children
            .iter()
            .all(|child| bounded(child, depth + 1, count, bytes))
}
fn string(value: &str) -> Value {
    Value::String(value.into())
}
fn put(map: &mut Map<String, Value>, name: &str, value: &str) {
    map.insert(name.into(), string(value));
}
pub(crate) fn integer(value: &str, increment: bool) -> Option<Value> {
    let mut text = integer_literal(value)?;
    if increment {
        let negative = text.starts_with('-');
        let mut digits = text.trim_start_matches('-').as_bytes().to_vec();
        if negative {
            for digit in digits.iter_mut().rev() {
                if *digit > b'0' {
                    *digit -= 1;
                    break;
                }
                *digit = b'9';
            }
            text = String::from_utf8(digits)
                .ok()?
                .trim_start_matches('0')
                .to_string();
            if text.is_empty() {
                text = "0".into();
            } else {
                text.insert(0, '-');
            }
        } else {
            let mut carry = true;
            for digit in digits.iter_mut().rev() {
                if *digit < b'9' {
                    *digit += 1;
                    carry = false;
                    break;
                }
                *digit = b'0';
            }
            if carry {
                digits.insert(0, b'1');
            }
            text = String::from_utf8(digits).ok()?;
        }
    }
    Number::from_str(&text).ok().map(Value::Number)
}
fn onscreen() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        let mut digits = String::from("[");
        for zero in DECIMAL_ZEROS {
            digits.push_str(&format!("\\u{{{zero:x}}}-\\u{{{:x}}}", zero + 9));
        }
        digits.push(']');
        Regex::new(&format!(r"[sSſ]({digits}+)[\s\x1c-\x1f]*[eE]({digits}+)")).unwrap()
    })
}
fn attributes(node: &XmltvElement, names: &[&str], data: &mut Map<String, Value>) {
    for name in names {
        if let Some(value) = node.attribute(name) {
            put(data, name, value);
        }
    }
}
fn text_fields(node: &XmltvElement, names: &[&str]) -> Map<String, Value> {
    let mut result = Map::new();
    for name in names {
        if let Some(text) = node.first(name).and_then(XmltvElement::text) {
            put(&mut result, name, trim(text));
        }
    }
    result
}

/// Project a single bounded programme tree, preserving source first-child and
/// Python string/integer semantics. Namespace tags are matched literally.
pub fn extract_xmltv_properties(
    prog: &XmltvElement,
) -> Result<Map<String, Value>, XmltvMetadataError> {
    if !bounded(prog, 0, &mut 0, &mut 0) {
        return Err(XmltvMetadataError::Capacity);
    }
    let mut out = Map::new();
    for (tag, key) in [("category", "categories"), ("keyword", "keywords")] {
        let values: Vec<_> = prog
            .all(tag)
            .filter_map(XmltvElement::nonblank)
            .map(string)
            .collect();
        if !values.is_empty() {
            out.insert(key.into(), Value::Array(values));
        }
    }
    let mut original_air = None;
    for node in prog.all("episode-num") {
        let Some(text) = node.text() else {
            continue;
        };
        match node.attribute("system").unwrap_or("onscreen") {
            "xmltv_ns" => {
                let mut parts = text.split('.');
                let first = parts.next().unwrap_or("");
                if let Some(second) = parts.next() {
                    for (key, value) in [("season", first), ("episode", second)] {
                        if !trim(value).is_empty() {
                            if let Some(number) = integer(value, true) {
                                out.insert(key.into(), number);
                            }
                        }
                    }
                }
            }
            "onscreen" => {
                put(&mut out, "onscreen_episode", trim(text));
                if !out.contains_key("season") || !out.contains_key("episode") {
                    if let Some(found) = onscreen().captures(trim(text)) {
                        for (key, index) in [("season", 1), ("episode", 2)] {
                            if !out.contains_key(key) {
                                out.insert(
                                    key.into(),
                                    integer(&found[index], false)
                                        .ok_or(XmltvMetadataError::Integer)?,
                                );
                            }
                        }
                    }
                }
            }
            "dd_progid" => put(&mut out, "dd_progid", trim(text)),
            "original-air-date" => original_air = Some(trim(text)),
            system @ ("thetvdb.com" | "themoviedb.org" | "imdb.com") => {
                put(&mut out, &format!("{system}_id"), trim(text))
            }
            _ => {}
        }
    }
    if let Some(node) = prog.first("rating") {
        if let Some(value) = node.first("value").and_then(XmltvElement::text) {
            put(&mut out, "rating", trim(value));
            if let Some(system) = node.attribute("system") {
                put(&mut out, "rating_system", system);
            }
        }
    }
    let mut stars = Vec::new();
    for node in prog.all("star-rating") {
        if let Some(value) = node.first("value").and_then(XmltvElement::text) {
            let mut data = Map::new();
            put(&mut data, "value", trim(value));
            attributes(node, &["system"], &mut data);
            stars.push(Value::Object(data));
        }
    }
    if !stars.is_empty() {
        out.insert("star_ratings".into(), Value::Array(stars));
    }
    if let Some(credits) = prog.first("credits") {
        let mut data = Map::new();
        for key in [
            "director",
            "actor",
            "writer",
            "adapter",
            "producer",
            "composer",
            "editor",
            "presenter",
            "commentator",
            "guest",
        ] {
            let values: Vec<_> = credits
                .all(key)
                .filter_map(|node| {
                    let name = node.nonblank()?;
                    if key != "actor" {
                        return Some(string(name));
                    }
                    let mut actor = Map::new();
                    put(&mut actor, "name", name);
                    attributes(node, &["role"], &mut actor);
                    if node.attribute("guest") == Some("yes") {
                        actor.insert("guest".into(), Value::Bool(true));
                    }
                    Some(Value::Object(actor))
                })
                .collect();
            if !values.is_empty() {
                data.insert(key.into(), Value::Array(values));
            }
        }
        if !data.is_empty() {
            out.insert("credits".into(), Value::Object(data));
        }
    }
    for (tag, key) in [
        ("date", "date"),
        ("country", "country"),
        ("language", "language"),
        ("orig-language", "original_language"),
    ] {
        if let Some(value) = prog.first(tag).and_then(XmltvElement::text) {
            put(&mut out, key, trim(value));
        }
    }
    if let Some(node) = prog.first("length") {
        if let Some(value) = node.text().and_then(|v| integer(trim(v), false)) {
            let mut data = Map::new();
            data.insert("value".into(), value);
            put(
                &mut data,
                "units",
                node.attrs.get("units").map_or("minutes", String::as_str),
            );
            out.insert("length".into(), Value::Object(data));
        }
    }
    for (tag, names) in [
        ("video", &["present", "colour", "aspect", "quality"][..]),
        ("audio", &["present", "stereo"][..]),
    ] {
        if let Some(node) = prog.first(tag) {
            let data = text_fields(node, names);
            if !data.is_empty() {
                out.insert(tag.into(), Value::Object(data));
            }
        }
    }
    let subtitles: Vec<_> = prog
        .all("subtitles")
        .filter_map(|node| {
            let mut data = text_fields(node, &["language"]);
            attributes(node, &["type"], &mut data);
            (!data.is_empty()).then_some(Value::Object(data))
        })
        .collect();
    if !subtitles.is_empty() {
        out.insert("subtitles".into(), Value::Array(subtitles));
    }
    for (tag, key, textkey, names) in [
        (
            "review",
            "reviews",
            "content",
            &["type", "source", "reviewer"][..],
        ),
        (
            "image",
            "images",
            "url",
            &["type", "size", "orient", "system"][..],
        ),
    ] {
        let values: Vec<_> = prog
            .all(tag)
            .filter_map(|node| {
                let mut data = Map::new();
                put(&mut data, textkey, node.nonblank()?);
                attributes(node, names, &mut data);
                Some(Value::Object(data))
            })
            .collect();
        if !values.is_empty() {
            out.insert(key.into(), Value::Array(values));
        }
    }
    if let Some(value) = prog.first("icon").and_then(|node| node.attribute("src")) {
        put(&mut out, "icon", value);
    }
    for tag in ["previously-shown", "premiere", "new", "live", "last-chance"] {
        if let Some(node) = prog.first(tag) {
            let key = tag.replace('-', "_");
            out.insert(key.clone(), Value::Bool(true));
            if matches!(tag, "premiere" | "last-chance") {
                if let Some(text) = node.nonblank() {
                    put(&mut out, &format!("{key}_text"), text);
                }
            }
        }
    }
    let mut previous = Map::new();
    if let Some(node) = prog.first("previously-shown") {
        attributes(node, &["start", "channel"], &mut previous);
    }
    if !previous.contains_key("start") {
        if let Some(value) = original_air.filter(|s| !s.is_empty()) {
            put(&mut previous, "start", value);
        }
    }
    if !previous.is_empty() {
        out.insert("previously_shown_details".into(), Value::Object(previous));
    }
    Ok(out)
}
