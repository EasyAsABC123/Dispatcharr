//! Original Stream.generate_hash_key identity, including Python JSON encoding.
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt::Write};

/// Tokenize saved hash CSV with the same whitespace rules as Python.
pub fn stream_hash_field_tokens(text: &str) -> impl Iterator<Item = &str> {
    text.split(',')
        .map(|part| {
            part.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
        })
        .filter(|part| !part.is_empty())
}

/// Return the recognized fields in first-occurrence order.
pub fn stream_hash_fields(text: &str) -> Vec<&'static str> {
    const ALLOWED: [&str; 5] = ["name", "url", "tvg_id", "m3u_id", "group"];
    let mut fields = Vec::new();
    for token in stream_hash_field_tokens(text) {
        if let Some(field) = ALLOWED.into_iter().find(|field| *field == token) {
            if !fields.contains(&field) {
                fields.push(field);
            }
        }
    }
    fields
}

pub struct StreamIdentity<'a> {
    pub name: &'a str,
    pub url: &'a str,
    pub tvg_id: &'a str,
    pub m3u_id: Option<i64>,
    pub group: Option<&'a str>,
    pub account_type: &'a str,
    pub stream_id: Option<i64>,
}
enum Part<'a> {
    String(&'a str),
    Integer(i64),
    Null,
}
fn quote(value: &str, output: &mut String) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{8}' => output.push_str("\\b"),
            '\u{c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{20}'..='\u{7e}' => output.push(character),
            _ => {
                let mut storage = [0; 2];
                for unit in character.encode_utf16(&mut storage) {
                    write!(output, "\\u{unit:04x}").unwrap();
                }
            }
        }
    }
    output.push('"');
}
/// Hash selected fields using the original sorted, spaced, ASCII JSON spelling.
/// Unknown keys are ignored, duplicates collapse, and XC nonzero stream IDs
/// replace URL while forcing the account ID into the hash to prevent collisions.
pub struct NullableStreamIdentity<'a> {
    pub name: Option<&'a str>,
    pub url: Option<&'a str>,
    pub tvg_id: Option<&'a str>,
    pub m3u_id: Option<i64>,
    pub group: Option<&'a str>,
    pub account_type: &'a str,
    pub stream_id: Option<i64>,
}
pub fn stream_hash(identity: &StreamIdentity<'_>, keys: &[&str]) -> String {
    stream_hash_nullable(
        &NullableStreamIdentity {
            name: Some(identity.name),
            url: Some(identity.url),
            tvg_id: Some(identity.tvg_id),
            m3u_id: identity.m3u_id,
            group: identity.group,
            account_type: identity.account_type,
            stream_id: identity.stream_id,
        },
        keys,
    )
}
/// Source model fields preserve SQL NULL separately from an empty string.
pub fn stream_hash_nullable(identity: &NullableStreamIdentity<'_>, keys: &[&str]) -> String {
    let use_id = identity.account_type == "XC"
        && identity.stream_id.is_some_and(|id| id != 0)
        && keys.contains(&"url");
    let mut parts = BTreeMap::new();
    for key in keys {
        let part = match *key {
            "name" => identity.name.map_or(Part::Null, Part::String),
            "url" if use_id => Part::Integer(identity.stream_id.unwrap()),
            "url" => identity.url.map_or(Part::Null, Part::String),
            "tvg_id" => identity.tvg_id.map_or(Part::Null, Part::String),
            "m3u_id" => identity.m3u_id.map_or(Part::Null, Part::Integer),
            "group" => identity.group.map_or(Part::Null, Part::String),
            _ => continue,
        };
        parts.insert(*key, part);
    }
    if use_id {
        parts.insert("m3u_id", identity.m3u_id.map_or(Part::Null, Part::Integer));
    }
    let mut serialized = String::from("{");
    for (index, (key, part)) in parts.iter().enumerate() {
        if index > 0 {
            serialized.push_str(", ");
        }
        quote(key, &mut serialized);
        serialized.push_str(": ");
        match part {
            Part::Null => serialized.push_str("null"),
            Part::Integer(value) => write!(&mut serialized, "{value}").unwrap(),
            Part::String(value) => quote(value, &mut serialized),
        }
    }
    serialized.push('}');
    format!("{:x}", Sha256::digest(serialized.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_json_spelling_handles_controls_non_ascii_and_supplementary_planes() {
        let mut text = String::new();
        quote("é😀\x7f\0\n\t\"\\", &mut text);
        assert_eq!(text, r#""\u00e9\ud83d\ude00\u007f\u0000\n\t\"\\""#);
    }
    #[test]
    fn xc_identity_survives_credential_changes_but_never_collides_across_accounts() {
        let mut identity = StreamIdentity {
            name: "Name",
            url: "https://one/credentials",
            tvg_id: "EPG",
            m3u_id: Some(1),
            group: None,
            account_type: "XC",
            stream_id: Some(20),
        };
        let first = stream_hash(&identity, &["url"]);
        identity.url = "https://two/other-credentials";
        assert_eq!(first, stream_hash(&identity, &["url"]));
        identity.m3u_id = Some(2);
        assert_ne!(first, stream_hash(&identity, &["url"]));
        identity.stream_id = Some(0);
        let zero = stream_hash(&identity, &["url"]);
        identity.url = "https://three/new";
        assert_ne!(zero, stream_hash(&identity, &["url"]));
    }
    #[test]
    fn sql_null_hash_parts_are_not_empty_strings() {
        let identity = NullableStreamIdentity {
            name: Some("Name"),
            url: None,
            tvg_id: None,
            m3u_id: None,
            group: None,
            account_type: "",
            stream_id: None,
        };
        assert_eq!(
            stream_hash_nullable(&identity, &["url", "tvg_id"]),
            "d489df5c9806aa38c732f1490553295ac5722874232e17c39638e769d917d816"
        );
        let old = StreamIdentity {
            name: "Name",
            url: "",
            tvg_id: "",
            m3u_id: None,
            group: None,
            account_type: "",
            stream_id: None,
        };
        assert_ne!(
            stream_hash_nullable(&identity, &["url", "tvg_id"]),
            stream_hash(&old, &["url", "tvg_id"])
        );
    }
}
