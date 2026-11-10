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
    if !truthy(properties) { Ok(true) } else { Err(NonMappingProperties) }
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


/// Python JSON truth rules used by hide_adult_content and lazy category
/// account flags. This is distinct from literal-False content access.
pub(crate) fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64() != Some(0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Visibility { pub movies: bool, pub series: bool, pub hide_adult_movies: bool }
pub(crate) fn visibility(properties: &Value, user_level: i32) -> Result<Visibility, NonMappingProperties> {
    let movies = access_enabled(properties, "vod_movies_enabled")?;
    let series = access_enabled(properties, "vod_series_enabled")?;
    let hide_adult_movies = user_level < 10 && properties.as_object()
        .and_then(|v|v.get("hide_adult_content")).is_some_and(truthy);
    Ok(Visibility { movies, series, hide_adult_movies })
}
impl Visibility {
    pub fn movie(&self, any_active_provider: bool, adult: bool) -> bool {
        self.movies && any_active_provider && !(self.hide_adult_movies && adult)
    }
    pub fn series(&self, any_active_provider: bool) -> bool { self.series && any_active_provider }
    pub fn episode(&self, any_active_episode_provider: bool) -> bool {
        // Source does not additionally require an active parent-series relation.
        self.series && any_active_episode_provider
    }
    pub fn category(&self, kind: &str) -> bool {
        if self.movies && self.series { true } else {
            (self.movies && kind == "movie") || (self.series && kind == "series")
        }
    }
    pub fn unified_movie(&self, any_active_provider: bool) -> bool {
        // Unlike the ordinary movie view, Source raw UNION omits hide_adult.
        self.movies && any_active_provider
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Permission { AllowAny, Authenticated, Standard, Admin }
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Action { List, Retrieve, Providers, ProviderInfo, SeriesEpisodes, Image, Other }
pub(crate) fn permission(action: Action) -> Permission {
    match action {
        Action::List | Action::Retrieve => Permission::Standard,
        Action::Providers | Action::ProviderInfo | Action::SeriesEpisodes => Permission::Authenticated,
        Action::Image => Permission::AllowAny,
        Action::Other => Permission::Admin,
    }
}
// Authentication (including invalid presented credentials), network UI ACL and
// active-user checks must precede this policy in the future HTTP adapter.
// HEAD is mapped to its GET action by DRF; OPTIONS/unmapped methods use Other.

/// Defaults for missing account/category links only. Do not overwrite existing
/// relation.enabled/custom_properties when handling the lazy category GET.
/// Keep an explicit raw value: Django BooleanField coercion for malformed
/// nonboolean defaults is a separate corpus gate, not Rust truthiness.
pub(crate) enum AutoEnable<'a> { DefaultTrue, Raw(&'a Value) }
pub(crate) fn category_seed_policy(properties: &Value) -> Result<Option<AutoEnable<'_>>, NonMappingProperties> {
    if !truthy(properties) { return Ok(None); }
    let object = properties.as_object().ok_or(NonMappingProperties)?;
    if !object.get("enable_vod").is_some_and(truthy) { return Ok(None); }
    Ok(Some(match object.get("auto_enable_new_groups_vod") {
        Some(value) => AutoEnable::Raw(value), None => AutoEnable::DefaultTrue,
    }))
}
