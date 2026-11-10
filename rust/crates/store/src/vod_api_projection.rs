//! UNREGISTERED, uncompiled draft of original VOD REST pure policies.
//! No SQL, router, provider fetch, or native acceptance follows from this file.
//! All application credit belongs to original Dispatcharr contributors.
//! Source: apps/vod/utils.py _is_vod_access_enabled/parse_category_filter_value.
use serde_json::Value;

/// Original `props = custom_properties or {}; props.get(key) is not False`.
/// False is a literal bool test; numeric zero is not a disabled access flag.
/// Truthy nonmapping properties raise in Python rather than silently becoming
/// an object. Preserve that distinction for an eventual adapter error policy.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NonMappingProperties;
pub(crate) fn access_enabled(
    properties: &Value,
    key: &str,
) -> Result<bool, NonMappingProperties> {
    if let Some(properties) = properties.as_object() {
        return Ok(properties.get(key) != Some(&Value::Bool(false)));
    }
    let falsy = match properties {
        Value::Null => true,
        Value::Bool(value) => !value,
        Value::Number(value) => value.as_f64() == Some(0.0),
        Value::String(value) => value.is_empty(),
        Value::Array(value) => value.is_empty(),
        Value::Object(_) => unreachable!(),
    };
    if falsy { Ok(true) } else { Err(NonMappingProperties) }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CategoryFilter<'a> {
    pub name: &'a str,
    pub category_type: Option<&'a str>,
}
/// Only the final, case-sensitive `movie`/`series` token has type semantics.
/// Embedded pipes and unknown suffixes remain part of the literal name.
pub(crate) fn category_filter(value: &str) -> CategoryFilter<'_> {
    if let Some((name, kind)) = value.rsplit_once('|') {
        if matches!(kind, "movie" | "series") {
            return CategoryFilter { name, category_type: Some(kind) };
        }
    }
    CategoryFilter { name: value, category_type: None }
}
