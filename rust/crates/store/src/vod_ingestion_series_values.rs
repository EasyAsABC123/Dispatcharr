//! Private uncompiled plain-Series list values from Original apps/vod/tasks.py.
//! Original Dispatcharr authors retain all application and algorithm credit.
use crate::StoreError;
use serde_json::{json, Value};
use std::collections::HashSet;
fn unsupported() -> StoreError {
    StoreError::Unsupported("VOD basic Series list value domain unavailable")
}
pub(super) struct Series {
    pub name: String,
    pub external: String,
    pub provider: Option<String>,
    pub properties: Option<Value>,
    pub basic: Value,
}
fn scalar(value: &Value) -> Result<String, StoreError> {
    let result = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) if number.as_i64().is_some() => number.to_string(),
        _ => return Err(unsupported()),
    };
    if result.len() > 255 {
        return Err(unsupported());
    }
    Ok(result)
}
/// No release dates/external IDs/language/logo/descriptive metadata are admitted.
/// Source year comes from releaseDate/release_date, never a year in the name.
pub(super) fn series(
    value: Value,
    is_draining: &dyn Fn() -> bool,
) -> Result<Vec<Series>, StoreError> {
    let rows = value
        .as_array()
        .filter(|rows| rows.len() <= 64)
        .ok_or_else(unsupported)?;
    let mut ids = HashSet::new();
    rows.iter()
        .map(|value| {
            if is_draining() {
                return Err(unsupported());
            }
            let row = value.as_object().ok_or_else(unsupported)?;
            if serde_json::to_vec(value).map_err(|_| unsupported())?.len() > 16384 {
                return Err(unsupported());
            }
            for key in [
                "plot",
                "rating",
                "genre",
                "cover",
                "tmdb",
                "tmdb_id",
                "imdb",
                "imdb_id",
                "backdrop_path",
                "poster_path",
                "original_name",
                "first_air_date",
                "last_air_date",
                "episode_run_time",
                "status",
                "type",
                "cast",
                "director",
                "country",
                "language",
                "releaseDate",
                "release_date",
                "youtube_trailer",
                "age",
                "seasons",
            ] {
                if row.get(key).is_some_and(crate::xc_live_provider::truthy) {
                    return Err(unsupported());
                }
            }
            let name = row
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(unsupported)?;
            if !name.is_ascii() {
                return Err(unsupported());
            }
            let name = name
                .trim_matches(|c: char| c.is_ascii_whitespace() || matches!(c, '\x1c'..='\x1f'))
                .to_owned();
            if name.is_empty() || name.len() > 255 {
                return Err(unsupported());
            }
            let external = scalar(row.get("series_id").ok_or_else(unsupported)?)?;
            if !ids.insert(external.clone()) {
                return Err(unsupported());
            }
            let raw_category = row
                .get("category_id")
                .filter(|v| crate::xc_live_provider::truthy(v));
            let provider = raw_category.map(scalar).transpose()?;
            // Unlike Movie, Source list-sync retains category_id in Series CP.
            let properties = raw_category.map(|value| json!({"category_id":value}));
            Ok(Series {
                name,
                external,
                provider,
                properties,
                basic: value.clone(),
            })
        })
        .collect()
}
pub(super) fn basic(series: &Series, category: i64) -> Value {
    let mut value = series.basic.clone();
    value["_provider_category_id"] = json!(series.provider);
    value["_category_id"] = json!(category);
    value
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_series_keeps_category_metadata_and_separate_provider_occurrences() {
        let rows = series(
            json!([
                {"series_id":9201,"name":"Owned Series (2020)","category_id":"11"},
                {"series_id":9202,"name":"Owned Series (2020)","category_id":"11"}
            ]),
            &|| false,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "Owned Series (2020)");
        assert_eq!(rows[0].properties, Some(json!({"category_id":"11"})));
        assert_eq!(basic(&rows[0], 7)["_category_id"], 7);
        assert_ne!(rows[0].external, rows[1].external);
    }
    #[test]
    fn unqualified_release_dates_and_shutdown_do_not_become_sparse_success() {
        let plain = json!([{"series_id":1,"name":"Owned"}]);
        assert!(series(plain.clone(), &|| true).is_err());
        for key in ["releaseDate", "tmdb_id", "cover", "cast"] {
            let mut row = plain.clone();
            row[0][key] = json!("unqualified");
            assert!(series(row, &|| false).is_err(), "{key}");
        }
        assert!(series(
            json!([{"series_id":1,"name":"Owned"},{"series_id":1,"name":"Other"}]),
            &|| false
        )
        .is_err());
    }
}
