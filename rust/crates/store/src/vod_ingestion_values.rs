//! Initial list ingestion values ported from Original apps/vod/tasks.py.
//! Original Dispatcharr authors retain application/algorithm credit.
use crate::StoreError;
use serde_json::{json, Value};
use std::collections::HashSet;
fn unsupported() -> StoreError {
    StoreError::Unsupported("VOD initial catalog value domain")
}
pub(super) struct Category {
    pub provider: String,
    pub name: String,
}
pub(super) struct Movie {
    pub name: String,
    pub year: Option<i32>,
    pub adult: bool,
    pub stream: String,
    pub provider: Option<String>,
    pub extension: String,
    pub basic: Value,
}
pub(super) fn categories(value: Value) -> Result<Vec<Category>, StoreError> {
    let rows = value
        .as_array()
        .filter(|rows| rows.len() <= 64)
        .ok_or_else(unsupported)?;
    let (mut ids, mut names) = (HashSet::new(), HashSet::new());
    rows.iter()
        .map(|row| {
            let row = row.as_object().ok_or_else(unsupported)?;
            let name = match row.get("category_name") {
                None => "Unknown",
                Some(Value::String(name)) => name,
                _ => return Err(unsupported()),
            };
            if name.chars().count() > 255 || !names.insert(name.to_owned()) {
                return Err(unsupported());
            }
            let provider = scalar(row.get("category_id").ok_or_else(unsupported)?)?;
            if provider.is_empty() || provider == "0" || !ids.insert(provider.clone()) {
                return Err(unsupported());
            }
            Ok(Category {
                provider,
                name: name.to_owned(),
            })
        })
        .collect()
}
fn scalar(value: &Value) -> Result<String, StoreError> {
    let result = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) if number.as_i64().is_some() => number.to_string(),
        _ => return Err(unsupported()),
    };
    if result.chars().count() > 255 {
        return Err(unsupported());
    }
    Ok(result)
}
fn year(row: &serde_json::Map<String, Value>, name: &str) -> Result<Option<i32>, StoreError> {
    if let Some(value) = row.get("year") {
        if crate::xc_live_provider::truthy(value) {
            let value = match value {
                Value::String(text) => text.trim().parse::<i32>().ok(),
                Value::Number(number) if number.as_i64().is_some() => {
                    number.as_i64().and_then(|v| i32::try_from(v).ok())
                }
                _ => return Err(unsupported()),
            };
            if let Some(value) = value.filter(|v| (1900..=2030).contains(v)) {
                return Ok(Some(value));
            }
        }
    }
    for pattern in [
        r"\((\d{4})\)",
        r"[\t\n\r\x0B\x0C\x1c-\x1f ]-[\t\n\r\x0B\x0C\x1c-\x1f ](\d{4})",
        r"[\t\n\r\x0B\x0C\x1c-\x1f ](\d{4})(?:\n)?$",
    ] {
        let expression = regex::Regex::new(pattern).map_err(|_| unsupported())?;
        if let Some(found) = expression.captures(name) {
            if let Ok(value) = found[1].parse::<i32>() {
                if (1900..=2030).contains(&value) {
                    return Ok(Some(value));
                }
            }
        }
    }
    Ok(None)
}
/// Fresh basic-list slice: no metadata/logo/identity conflict or language split
/// is silently converted to a sparse row. Those values require later slices.
pub(super) fn movies(
    value: Value,
    is_draining: &dyn Fn() -> bool,
) -> Result<Vec<Movie>, StoreError> {
    let rows = value
        .as_array()
        .filter(|rows| !rows.is_empty() && rows.len() <= 64)
        .ok_or_else(unsupported)?;
    let mut streams = HashSet::new();
    let mut identities = HashSet::new();
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
                "description",
                "plot",
                "rating",
                "vote_average",
                "genre",
                "category_name",
                "duration",
                "duration_secs",
                "episode_run_time",
                "trailer",
                "youtube_trailer",
                "stream_icon",
                "director",
                "actors",
                "cast",
                "release_date",
                "releasedate",
                "releaseDate",
                "tmdb_id",
                "tmdb",
                "imdb_id",
                "imdb",
            ] {
                if row.get(key).is_some_and(crate::xc_live_provider::truthy) {
                    return Err(unsupported());
                }
            }
            let raw_name = row
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(unsupported)?;
            let name = raw_name;
            if !name.is_ascii() {
                return Err(unsupported());
            }
            let name = name
                .trim_matches(|c: char| c.is_ascii_whitespace() || matches!(c, '\x1c'..='\x1f'))
                .to_owned();
            if name.is_empty() || name.len() > 255 {
                return Err(unsupported());
            }
            let stream = scalar(row.get("stream_id").ok_or_else(unsupported)?)?;
            if !streams.insert(stream.clone()) {
                return Err(unsupported());
            }
            let provider = match row
                .get("category_id")
                .filter(|v| crate::xc_live_provider::truthy(v))
            {
                Some(value) => Some(scalar(value)?),
                None => None,
            };
            let extension = match row.get("container_extension") {
                None => "mp4".to_owned(),
                Some(Value::String(value)) if value.len() <= 10 => value.clone(),
                _ => return Err(unsupported()),
            };
            let year = year(row, raw_name)?;
            if !identities.insert((name.clone(), year)) {
                return Err(unsupported());
            }
            Ok(Movie {
                year,
                adult: adult(row.get("is_adult")),
                name,
                stream,
                provider,
                extension,
                basic: value.clone(),
            })
        })
        .collect()
}
fn adult(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value
            .as_f64()
            .is_some_and(|value| value.is_finite() && value.trunc() == 1.0),
        Some(Value::String(value)) => dispatcharr_ingest_core::source_integer_literal(value)
            .is_some_and(|value| value.parse::<i128>() == Ok(1)),
        _ => false,
    }
}
pub(super) fn basic(movie: &Movie, category: i64) -> Value {
    let mut value = movie.basic.clone();
    value["_provider_category_id"] = json!(movie.provider);
    value["_category_id"] = json!(category);
    value
}

#[cfg(test)]
mod source_basic_list_controls {
    use super::*;
    #[test]
    fn plain_provider_rows_keep_source_basic_payload_and_title_year() {
        let rows = json!([{"num":1,"stream_id":9101,"name":"Owned Movie (2020)","category_id":"4","container_extension":"mp4","is_adult":"0"},{"num":2,"stream_id":9102,"name":"Owned Second Movie (2021)","category_id":"4","container_extension":"mkv","is_adult":"0"}]);
        let imported = movies(rows.clone(), &|| false).unwrap();
        assert_eq!(imported.len(), 2);
        assert_eq!(imported[0].year, Some(2020));
        assert_eq!(imported[1].year, Some(2021));
        assert!(!imported[0].adult);
        let mut expected = rows[0].clone();
        expected["_provider_category_id"] = json!("4");
        expected["_category_id"] = json!(42);
        assert_eq!(basic(&imported[0], 42), expected);
    }
    #[test]
    fn unsupported_metadata_identity_and_shutdown_do_not_become_sparse_imports() {
        let plain = json!([{"stream_id":1,"name":"Owned - 2020","is_adult":true}]);
        assert!(movies(plain.clone(), &|| true).is_err());
        for field in ["tmdb_id", "stream_icon", "description", "releaseDate"] {
            let mut metadata = plain.clone();
            metadata[0][field] = json!("unsupported");
            assert!(movies(metadata, &|| false).is_err(), "{field}");
        }
        let imported = movies(plain, &|| false).unwrap();
        assert!(imported[0].adult);
        assert_eq!(imported[0].year, Some(2020));
        assert!(movies(
            json!([{"stream_id":1,"name":"Owned"},{"stream_id":1,"name":"Other"}]),
            &|| false
        )
        .is_err());
    }
}
