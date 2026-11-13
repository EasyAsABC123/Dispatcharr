//! Borrowed info-property access and Source relation artwork precedence.
//! No User/Value tree, row allocation or upstream request is created here.
//! Callers retain charged SQL text and its scratch quote through projection.
//! Original Dispatcharr authors retain image_proxy/application credit.
use crate::{
    vod_json::{self, Error},
    vod_span::{self, RawValue},
};
use std::borrow::Cow;
pub(crate) fn parse(text: &str) -> Result<&RawValue, Error> {
    let raw = vod_span::parse(text)?;
    vod_json::validate_source_integer_limits(raw)?;
    Ok(raw)
}
fn mapping(raw: &RawValue) -> bool {
    raw.trim_start().starts_with('{')
}
/// Strict Python mapping get. Scan borrowed members and keep the last matching
/// field, without allocating a Value tree or interpreting private serde keys.
pub(crate) fn get<'a>(raw: &'a RawValue, name: &str) -> Result<Option<&'a RawValue>, Error> {
    if !mapping(raw) {
        return Err(Error::SourceObject);
    }
    let mut found = None;
    for member in vod_span::Object::new(raw)? {
        let (key, value) = member?;
        if key == name {
            found = Some(value);
        }
    }
    Ok(found)
}
fn backdrop(raw: &RawValue) -> Result<bool, Error> {
    if !vod_json::truthy(raw)? {
        return Ok(false);
    }
    let text = raw.trim_start();
    Ok(text.starts_with('"') || text.starts_with('['))
}
fn stripped(value: Cow<'_, str>) -> Cow<'_, str> {
    match value {
        Cow::Borrowed(value) => Cow::Borrowed(value.trim_matches(crate::vod_lists::space)),
        Cow::Owned(value) => Cow::Owned(value.trim_matches(crate::vod_lists::space).to_owned()),
    }
}
pub(crate) struct Artwork<'a> {
    pub image: Cow<'a, str>,
    /// A Source string becomes one backdrop item; an array preserves original
    /// element types/indices. No Vec<String/Value> amplification occurs here.
    pub backdrop: Option<&'a RawValue>,
}
fn relation<'a>(raw: Option<&'a RawValue>) -> Result<Artwork<'a>, Error> {
    let mut result = Artwork {
        image: Cow::Borrowed(""),
        backdrop: None,
    };
    let Some(root) = raw.filter(|raw| mapping(raw)) else {
        return Ok(result);
    };
    let info = get(root, "info")?.filter(|raw| mapping(raw));
    let nested = info
        .map(|raw| get(raw, "info"))
        .transpose()?
        .flatten()
        .filter(|raw| mapping(raw));
    let detailed = get(root, "detailed_info")?.filter(|raw| mapping(raw));
    let basic = get(root, "basic_data")?.filter(|raw| mapping(raw));
    // Source's actual order is info BEFORE info.info, then detailed/basic/root.
    for candidate in [info, nested, detailed, basic, Some(root)]
        .into_iter()
        .flatten()
    {
        if result.image.is_empty() {
            let mut selected = None;
            for key in ["movie_image", "cover_big", "stream_icon", "cover"] {
                if let Some(value) = get(candidate, key)? {
                    if vod_json::truthy(value)? {
                        selected = Some(value);
                        break;
                    }
                }
            }
            if let Some(value) = selected {
                if let Some(value) = vod_json::string(value)? {
                    result.image = stripped(value);
                }
            }
        }
        if result.backdrop.is_none() {
            if let Some(value) = get(candidate, "backdrop_path")? {
                if backdrop(value)? {
                    result.backdrop = Some(value);
                }
            }
        }
    }
    Ok(result)
}
pub(crate) fn prefer<'a>(
    relation_props: Option<&'a RawValue>,
    object_props: Option<&'a RawValue>,
) -> Result<Artwork<'a>, Error> {
    let mut result = relation(relation_props)?;
    let Some(object) = object_props.filter(|raw| mapping(raw)) else {
        return Ok(result);
    };
    if result.image.is_empty() {
        if let Some(raw) = get(object, "movie_image")? {
            if let Some(value) = vod_json::string(raw)? {
                result.image = stripped(value);
            }
        }
    }
    if result.backdrop.is_none() {
        if let Some(raw) = get(object, "backdrop_path")? {
            if backdrop(raw)? {
                result.backdrop = Some(raw);
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nested_candidate_order_and_ortype_precedence_match_definition() {
        let props = parse(r#"{"info":{"cover":"outer","info":{"movie_image":"nested"}},"detailed_info":{"movie_image":"detail"}}"#).unwrap();
        assert_eq!(prefer(Some(props), None).unwrap().image, "outer");
        let props = parse(r#"{"info":{"movie_image":1,"cover":"blocked"},"detailed_info":{"movie_image":"detail"}}"#).unwrap();
        assert_eq!(prefer(Some(props), None).unwrap().image, "detail");
    }
    #[test]
    fn source_strip_and_backdrop_indices_are_retained() {
        let props=parse(r#"{"basic_data":{"movie_image":" \u00a0http://origin/p \u3000","backdrop_path":[null,"relative",false]}}"#).unwrap();
        let art = prefer(Some(props), None).unwrap();
        assert_eq!(art.image, "http://origin/p");
        assert_eq!(art.backdrop.unwrap(), r#"[null,"relative",false]"#);
    }
    #[test]
    fn falsey_candidates_continue_and_object_fallback_is_narrow() {
        let props =
            parse(r#"{"detailed_info":{"backdrop_path":[]},"basic_data":{"backdrop_path":"one"}}"#)
                .unwrap();
        let obj=parse(r#"{"movie_image":" object ","cover_big":"not-an-object-fallback","backdrop_path":["later"]}"#).unwrap();
        let art = prefer(Some(props), Some(obj)).unwrap();
        assert_eq!(art.image, "object");
        assert_eq!(art.backdrop.unwrap(), r#""one""#);
        assert!(
            prefer(None, Some(parse(r#"{"cover_big":"ignored"}"#).unwrap()))
                .unwrap()
                .image
                .is_empty()
        );
    }
    #[test]
    fn strict_get_and_tolerant_artwork_shapes_are_separate() {
        for text in ["null", "false", "0", "[]", r#""truthy""#] {
            let raw = parse(text).unwrap();
            assert!(matches!(get(raw, "x"), Err(Error::SourceObject)));
            assert!(prefer(Some(raw), Some(raw)).unwrap().image.is_empty());
        }
    }
    #[test]
    fn unknown_private_serde_keys_remain_literal_borrowed_json() {
        let raw =
            parse(r#"{"$serde_json::private::RawValue":"[1,2]","selected":{"x":1}}"#).unwrap();
        assert_eq!(get(raw, "selected").unwrap().unwrap(), r#"{"x":1}"#);
        assert_eq!(
            get(raw, "$serde_json::private::RawValue").unwrap().unwrap(),
            r#""[1,2]""#
        );
    }
}
