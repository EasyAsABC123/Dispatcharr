//! Draft bounded XML programme formatting; original Dispatcharr authors retain credit.
use dispatcharr_ingest_core::XmltvProgramme;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenderError {
    Capacity,
    InvalidMetadata,
}
pub fn channel(id: &str, name: &str, logo: &str, limit: usize) -> Result<String, RenderError> {
    let mut out = Xml::new(limit);
    out.raw("  <channel id=\"")?;
    out.escape(id)?;
    out.raw("\">\n    <display-name>")?;
    out.escape(name)?;
    out.raw("</display-name>\n    <icon src=\"")?;
    out.escape(logo)?;
    out.raw("\" />\n  </channel>\n")?;
    Ok(out.value)
}
pub fn standard_dummy(
    input: &crate::dummy::DummyProgramme,
    limit: usize,
) -> Result<String, RenderError> {
    let mut out = Xml::new(limit);
    out.raw("  <programme start=\"")?;
    out.raw(&input.start_time.format("%Y%m%d%H%M%S +0000").to_string())?;
    out.raw("\" stop=\"")?;
    out.raw(&input.end_time.format("%Y%m%d%H%M%S +0000").to_string())?;
    out.raw("\" channel=\"")?;
    out.escape(&input.channel_id)?;
    out.raw("\">\n    <title>")?;
    out.escape(&input.title)?;
    out.raw("</title>\n    <desc>")?;
    out.escape(&input.description)?;
    out.raw("</desc>\n  </programme>\n")?;
    Ok(out.value)
}
pub struct Programme<'a> {
    pub id: i64,
    pub channel: &'a str,
    pub origin: &'a str,
    pub row: &'a XmltvProgramme,
}
use num_bigint::BigInt;
use serde_json::{Map, Value};
use std::{str::FromStr, sync::OnceLock};
fn bounded(input: &Programme<'_>, limit: usize) -> Result<(), RenderError> {
    use chrono::Datelike;
    if !(1..=9999).contains(&input.row.start_time.year())
        || !(1..=9999).contains(&input.row.end_time.year())
    {
        return Err(RenderError::InvalidMetadata);
    }
    let mut bytes = input
        .channel
        .len()
        .checked_add(input.origin.len())
        .and_then(|n| n.checked_add(input.row.title.len()))
        .and_then(|n| n.checked_add(input.row.sub_title.as_ref().map_or(0, String::len)))
        .and_then(|n| n.checked_add(input.row.description.as_ref().map_or(0, String::len)))
        .ok_or(RenderError::Capacity)?;
    let mut pending = Vec::new();
    if let Some(value) = &input.row.custom_properties {
        pending.push((value, 0usize));
    }
    let mut visited = 0usize;
    while let Some((value, depth)) = pending.pop() {
        visited += 1;
        if visited > 10000 || depth > 32 {
            return Err(RenderError::Capacity);
        }
        bytes = bytes
            .checked_add(std::mem::size_of::<Value>())
            .ok_or(RenderError::Capacity)?;
        match value {
            Value::String(value) => {
                bytes = bytes
                    .checked_add(value.len())
                    .ok_or(RenderError::Capacity)?
            }
            Value::Number(value) => {
                bytes = bytes
                    .checked_add(value.to_string().len())
                    .ok_or(RenderError::Capacity)?
            }
            Value::Array(values) => {
                if values.len() > 10000usize.saturating_sub(visited + pending.len()) {
                    return Err(RenderError::Capacity);
                }
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::Object(values) => {
                if values.len() > 10000usize.saturating_sub(visited + pending.len()) {
                    return Err(RenderError::Capacity);
                }
                for (key, value) in values {
                    bytes = bytes.checked_add(key.len()).ok_or(RenderError::Capacity)?;
                    pending.push((value, depth + 1));
                }
            }
            _ => {}
        }
        if bytes > limit {
            return Err(RenderError::Capacity);
        }
    }
    if bytes > limit {
        return Err(RenderError::Capacity);
    }
    Ok(())
}
struct Xml {
    value: String,
    limit: usize,
}
impl Xml {
    fn new(limit: usize) -> Self {
        Self {
            value: String::new(),
            limit,
        }
    }
    fn raw(&mut self, value: &str) -> Result<(), RenderError> {
        let size = self
            .value
            .len()
            .checked_add(value.len())
            .ok_or(RenderError::Capacity)?;
        if size > self.limit {
            return Err(RenderError::Capacity);
        }
        if size > self.value.capacity() {
            let capacity = size
                .checked_next_power_of_two()
                .unwrap_or(self.limit)
                .min(self.limit);
            self.value
                .try_reserve_exact(capacity - self.value.len())
                .map_err(|_| RenderError::Capacity)?;
            if self.value.capacity() > self.limit {
                return Err(RenderError::Capacity);
            }
        }
        self.value.push_str(value);
        Ok(())
    }
    fn escape(&mut self, value: &str) -> Result<(), RenderError> {
        for c in value.chars() {
            match c {
                '&' => self.raw("&amp;")?,
                '<' => self.raw("&lt;")?,
                '>' => self.raw("&gt;")?,
                '"' => self.raw("&quot;")?,
                '\'' => self.raw("&#x27;")?,
                c => {
                    let mut bytes = [0; 4];
                    self.raw(c.encode_utf8(&mut bytes))?
                }
            }
        }
        Ok(())
    }
    fn tag(&mut self, indent: &str, tag: &str, value: &str) -> Result<(), RenderError> {
        self.raw(indent)?;
        self.raw("<")?;
        self.raw(tag)?;
        self.raw(">")?;
        self.escape(value)?;
        self.raw("</")?;
        self.raw(tag)?;
        self.raw(">\n")
    }
    fn attribute(&mut self, name: &str, value: &str) -> Result<(), RenderError> {
        self.raw(" ")?;
        self.raw(name)?;
        self.raw("=\"")?;
        self.escape(value)?;
        self.raw("\"")
    }
}
fn text(value: &Value) -> Result<&str, RenderError> {
    value.as_str().ok_or(RenderError::InvalidMetadata)
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
fn float(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .into();
    }
    if !value.is_finite() {
        return if value.is_nan() {
            "nan"
        } else if value.is_sign_negative() {
            "-inf"
        } else {
            "inf"
        }
        .into();
    }
    if value.abs() < 1e-4 || value.abs() >= 1e16 {
        let raw = format!("{value:e}");
        let (mantissa, exponent) = raw.split_once('e').unwrap();
        let exponent: i32 = exponent.parse().unwrap();
        return format!("{mantissa}e{exponent:+03}");
    }
    let mut raw = value.to_string();
    if !raw.contains('.') {
        raw.push_str(".0")
    }
    raw
}
fn quoted(value: &str) -> String {
    let quote = if value.contains('\'') && !value.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut result = String::new();
    result.push(quote);
    for c in value.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c == quote => {
                result.push('\\');
                result.push(c)
            }
            c if c.is_control() => {
                let code = c as u32;
                result.push_str(&if code <= 255 {
                    format!("\\x{code:02x}")
                } else {
                    format!("\\u{code:04x}")
                })
            }
            c => result.push(c),
        }
    }
    result.push(quote);
    result
}
fn py(value: &Value, nested: bool) -> Result<String, RenderError> {
    Ok(match value {
        Value::Null => "None".into(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::String(v) => {
            if nested {
                quoted(v)
            } else {
                v.clone()
            }
        }
        Value::Number(v) => {
            let raw = v.to_string();
            if raw.contains(['.', 'e', 'E']) {
                float(v.as_f64().ok_or(RenderError::InvalidMetadata)?)
            } else {
                raw
            }
        }
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(|v| py(v, true))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        Value::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(k, v)| Ok(format!("{}: {}", quoted(k), py(v, true)?)))
                .collect::<Result<Vec<_>, RenderError>>()?
                .join(", ")
        ),
    })
}
fn zero_based(value: &Value) -> Result<String, RenderError> {
    let value = py(value, false)?;
    static DIGITS: OnceLock<Vec<u32>> = OnceLock::new();
    let digits = DIGITS.get_or_init(|| {
        let source: Value = serde_json::from_str(include_str!("../datasets/source-digits.json"))
            .expect("Source vectors");
        source
            .as_array()
            .expect("Source digit table")
            .iter()
            .map(|v| v.as_u64().unwrap() as u32)
            .collect()
    });
    if value.is_empty()
        || !value
            .chars()
            .all(|c| digits.binary_search(&(c as u32)).is_ok())
    {
        return Ok("0".into());
    }
    let number = dispatcharr_ingest_core::source_integer_literal(&value)
        .and_then(|v| BigInt::from_str(&v).ok())
        .ok_or(RenderError::InvalidMetadata)?;
    Ok((number - BigInt::from(1)).to_string())
}
type TextSequence<'a> =
    Box<dyn Iterator<Item = Result<std::borrow::Cow<'a, str>, RenderError>> + 'a>;
fn sequence(value: &Value) -> Result<TextSequence<'_>, RenderError> {
    match value {
        Value::Array(values) => Ok(Box::new(
            values
                .iter()
                .map(|v| text(v).map(std::borrow::Cow::Borrowed)),
        )),
        Value::String(value) => Ok(Box::new(
            value
                .chars()
                .map(|c| Ok(std::borrow::Cow::Owned(c.to_string()))),
        )),
        _ => Err(RenderError::InvalidMetadata),
    }
}
fn get_like<'a>(value: &'a Value, key: &str) -> Result<Option<&'a Value>, RenderError> {
    match value {
        Value::Object(v) => Ok(v.get(key)),
        Value::Array(v) => {
            if v.contains(&Value::String(key.into())) {
                Err(RenderError::InvalidMetadata)
            } else {
                Ok(None)
            }
        }
        Value::String(v) => {
            if v.contains(key) {
                Err(RenderError::InvalidMetadata)
            } else {
                Ok(None)
            }
        }
        _ => Err(RenderError::InvalidMetadata),
    }
}
fn properties(
    out: &mut Xml,
    map: &Map<String, Value>,
    id: i64,
    origin: &str,
) -> Result<(), RenderError> {
    for name in ["categories", "keywords"] {
        if let Some(value) = map.get(name).filter(|v| truthy(v)) {
            for value in sequence(value)? {
                out.tag(
                    "    ",
                    if name == "categories" {
                        "category"
                    } else {
                        "keyword"
                    },
                    &value?,
                )?;
            }
        }
    }
    if let Some(value) = map.get("onscreen_episode") {
        out.raw("    <episode-num system=\"onscreen\">")?;
        out.escape(text(value)?)?;
        out.raw("</episode-num>\n")?;
    } else if let Some(value) = map.get("episode") {
        out.raw("    <episode-num system=\"onscreen\">E")?;
        out.raw(&py(value, false)?)?;
        out.raw("</episode-num>\n")?;
    }
    for (name, system) in [
        ("dd_progid", "dd_progid"),
        ("thetvdb.com_id", "thetvdb.com"),
        ("themoviedb.org_id", "themoviedb.org"),
        ("imdb.com_id", "imdb.com"),
    ] {
        if let Some(value) = map.get(name) {
            out.raw("    <episode-num")?;
            out.attribute("system", system)?;
            out.raw(">")?;
            out.escape(text(value)?)?;
            out.raw("</episode-num>\n")?;
        }
    }
    if let (Some(season), Some(episode)) = (map.get("season"), map.get("episode")) {
        out.raw("    <episode-num system=\"xmltv_ns\">")?;
        out.raw(&zero_based(season)?)?;
        out.raw(".")?;
        out.raw(&zero_based(episode)?)?;
        out.raw(".</episode-num>\n")?;
    }
    for (name, tag) in [
        ("language", "language"),
        ("original_language", "orig-language"),
    ] {
        if let Some(value) = map.get(name) {
            out.tag("    ", tag, text(value)?)?;
        }
    }
    if let Some(Value::Object(value)) = map.get("length") {
        out.raw("    <length")?;
        out.attribute(
            "units",
            value
                .get("units")
                .map(text)
                .transpose()?
                .unwrap_or("minutes"),
        )?;
        out.raw(">")?;
        out.escape(&py(
            value.get("value").unwrap_or(&Value::String(String::new())),
            false,
        )?)?;
        out.raw("</length>\n")?;
    }
    for (group, attrs) in [
        ("video", &["present", "colour", "aspect", "quality"][..]),
        ("audio", &["present", "stereo"][..]),
    ] {
        if let Some(Value::Object(value)) = map.get(group) {
            out.raw("    <")?;
            out.raw(group)?;
            out.raw(">\n")?;
            for attr in attrs {
                if let Some(value) = value.get(*attr) {
                    out.tag("      ", attr, text(value)?)?;
                }
            }
            out.raw("    </")?;
            out.raw(group)?;
            out.raw(">\n")?;
        }
    }
    if let Some(Value::Array(values)) = map.get("subtitles") {
        for value in values {
            if let Value::Object(value) = value {
                out.raw("    <subtitles")?;
                if let Some(kind) = value.get("type").filter(|v| truthy(v)) {
                    out.attribute("type", text(kind)?)?;
                }
                out.raw(">\n")?;
                if let Some(language) = value.get("language") {
                    out.tag("      ", "language", text(language)?)?;
                }
                out.raw("    </subtitles>\n")?;
            }
        }
    }
    if let Some(value) = map.get("rating") {
        out.raw("    <rating")?;
        out.attribute(
            "system",
            map.get("rating_system")
                .map(text)
                .transpose()?
                .unwrap_or("TV Parental Guidelines"),
        )?;
        out.raw(">\n")?;
        out.tag("      ", "value", text(value)?)?;
        out.raw("    </rating>\n")?;
    }
    if let Some(Value::Array(values)) = map.get("star_ratings") {
        for value in values {
            if let Value::Object(value) = value {
                if let Some(rating) = value.get("value") {
                    out.raw("    <star-rating")?;
                    if let Some(system) = value.get("system") {
                        out.attribute("system", text(system)?)?;
                    }
                    out.raw(">\n")?;
                    out.tag("      ", "value", text(rating)?)?;
                    out.raw("    </star-rating>\n")?;
                }
            }
        }
    }
    if let Some(Value::Array(values)) = map.get("reviews") {
        for value in values {
            if let Value::Object(value) = value {
                if let Some(content) = value.get("content") {
                    out.raw("    <review")?;
                    out.attribute(
                        "type",
                        value.get("type").map(text).transpose()?.unwrap_or("text"),
                    )?;
                    for attr in ["source", "reviewer"] {
                        if let Some(value) = value.get(attr) {
                            out.attribute(attr, text(value)?)?;
                        }
                    }
                    out.raw(">")?;
                    out.escape(text(content)?)?;
                    out.raw("</review>\n")?;
                }
            }
        }
    }
    if let Some(Value::Array(values)) = map.get("images") {
        for value in values {
            if let Value::Object(value) = value {
                if let Some(url) = value.get("url") {
                    out.raw("    <image")?;
                    for attr in ["type", "size", "orient", "system"] {
                        if let Some(value) = value.get(attr) {
                            out.attribute(attr, text(value)?)?;
                        }
                    }
                    out.raw(">")?;
                    out.escape(text(url)?)?;
                    out.raw("</image>\n")?;
                }
            }
        }
    }
    if let Some(credits) = map.get("credits") {
        out.raw("    <credits>\n")?;
        for role in [
            "director",
            "writer",
            "adapter",
            "producer",
            "composer",
            "editor",
            "presenter",
            "commentator",
            "guest",
        ] {
            if let Some(value) = get_like(credits, role)? {
                match value {
                    Value::Array(people) => {
                        for person in people {
                            out.tag("      ", role, text(person)?)?
                        }
                    }
                    value => out.tag("      ", role, text(value)?)?,
                }
            }
        }
        if let Some(actors) = get_like(credits, "actor")? {
            if let Value::Array(actors) = actors {
                for actor in actors {
                    out.raw("      <actor")?;
                    let name = if let Value::Object(actor) = actor {
                        if let Some(role) = actor.get("role") {
                            out.attribute("role", text(role)?)?;
                        }
                        if actor.get("guest").is_some_and(truthy) {
                            out.attribute("guest", "yes")?;
                        }
                        actor.get("name").map(text).transpose()?.unwrap_or("")
                    } else {
                        text(actor)?
                    };
                    out.raw(">")?;
                    out.escape(name)?;
                    out.raw("</actor>\n")?;
                }
            } else {
                out.tag("      ", "actor", text(actors)?)?;
            }
        }
        out.raw("    </credits>\n")?;
    }
    for name in ["date", "country"] {
        if let Some(value) = map.get(name) {
            out.tag("    ", name, text(value)?)?;
        }
    }
    if let Some(value) = map.get("icon") {
        out.raw("    <icon")?;
        out.attribute("src", text(value)?)?;
        out.raw(" />\n")?;
    } else if let Some(value) = map.get("sd_icon") {
        use sha2::{Digest, Sha256};
        let value = if truthy(value) { text(value)? } else { "" };
        let bust = if value.is_empty() {
            String::new()
        } else {
            format!(
                "?v={}",
                &format!("{:x}", Sha256::digest(value.as_bytes()))[..12]
            )
        };
        out.raw("    <icon")?;
        out.attribute(
            "src",
            &format!("{origin}/api/epg/programs/{id}/poster/{bust}"),
        )?;
        out.raw(" />\n")?;
    }
    if map.get("previously_shown").is_some_and(truthy) {
        out.raw("    <previously-shown")?;
        if let Some(details) = map.get("previously_shown_details") {
            for attr in ["start", "channel"] {
                if let Some(value) = get_like(details, attr)? {
                    out.attribute(attr, text(value)?)?;
                }
            }
        }
        out.raw(" />\n")?;
    }
    for (flag, field, tag) in [
        ("premiere", "premiere_text", "premiere"),
        ("last_chance", "last_chance_text", "last-chance"),
    ] {
        if map.get(flag).is_some_and(truthy) {
            if let Some(value) = map.get(field).filter(|v| truthy(v)) {
                out.tag("    ", tag, text(value)?)?;
            } else {
                out.raw("    <")?;
                out.raw(tag)?;
                out.raw(" />\n")?;
            }
        }
    }
    for name in ["new", "live"] {
        if map.get(name).is_some_and(truthy) {
            out.raw("    <")?;
            out.raw(name)?;
            out.raw(" />\n")?;
        }
    }
    Ok(())
}
pub fn programme(input: Programme<'_>, limit: usize) -> Result<String, RenderError> {
    bounded(&input, limit)?;
    let mut out = Xml::new(limit);
    out.raw("  <programme")?;
    out.attribute(
        "start",
        &input
            .row
            .start_time
            .format("%Y%m%d%H%M%S +0000")
            .to_string(),
    )?;
    out.attribute(
        "stop",
        &input.row.end_time.format("%Y%m%d%H%M%S +0000").to_string(),
    )?;
    out.attribute("channel", input.channel)?;
    out.raw(">\n")?;
    out.tag("    ", "title", &input.row.title)?;
    if let Some(value) = input.row.sub_title.as_ref().filter(|v| !v.is_empty()) {
        out.tag("    ", "sub-title", value)?;
    }
    if let Some(value) = input.row.description.as_ref().filter(|v| !v.is_empty()) {
        out.tag("    ", "desc", value)?;
    }
    if let Some(value) = input.row.custom_properties.as_ref().filter(|v| truthy(v)) {
        properties(
            &mut out,
            value.as_object().ok_or(RenderError::InvalidMetadata)?,
            input.id,
            input.origin,
        )?;
    }
    out.raw("  </programme>")?;
    Ok(out.value)
}
#[cfg(test)]
#[path = "render_tests.rs"]
mod tests;
