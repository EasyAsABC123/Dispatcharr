//! Initial list ingestion values ported from Original apps/vod/tasks.py.
//! Original Dispatcharr authors retain application/algorithm credit.
use crate::StoreError;
use serde_json::{json, Value};
use std::collections::HashSet;
#[path = "vod_ingestion_external_identity.rs"]
pub(super) mod external_identity;
#[path = "vod_ingestion_list_props.rs"]
pub(super) mod list_props;
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
    pub tmdb_id: Option<String>,
    pub imdb_id: Option<String>,
    pub adult: Option<bool>,
    pub stream: String,
    pub provider: Option<String>,
    pub extension: String,
    pub basic: Value,
    pub description: String,
    pub rating: Option<String>,
    pub genre: String,
    pub duration: Option<i64>,
    pub properties: Option<Value>,
    pub logo: Option<String>,
}
// Charge all retained vectors/maps/strings rather than impose a64-row ceiling.
// The provider document retains its existing16MiB HTTP/32MiB parser bounds.
// Category work is an additional256KiB owned domain, independent of row count.
pub(super) const CATEGORY_WORK_BYTES: usize = 256 * 1024;
pub(super) fn category_charge(work: &mut usize, bytes: usize) -> Result<(), StoreError> {
    *work = work
        .checked_add(bytes.checked_add(128).ok_or_else(unsupported)?)
        .filter(|value| *value <= CATEGORY_WORK_BYTES)
        .ok_or_else(unsupported)?;
    Ok(())
}
pub(super) fn categories(value: Value) -> Result<Vec<Category>, StoreError> {
    let rows = value.as_array().ok_or_else(unsupported)?;
    let mut work = 0usize;
    let (mut ids, mut names) = (HashSet::new(), HashSet::new());
    rows.iter()
        .map(|row| {
            let row = row.as_object().ok_or_else(unsupported)?;
            let name = match row.get("category_name") {
                None => "Unknown",
                Some(Value::String(name)) => name,
                _ => return Err(unsupported()),
            };
            if name.chars().count() > 255 {
                return Err(unsupported());
            }
            // Eight copies cover validation sets, returned rows and SQL mapping
            // arguments; reject before allocating those extra retained names.
            category_charge(
                &mut work,
                name.len().checked_mul(8).ok_or_else(unsupported)?,
            )?;
            if !names.insert(name.to_owned()) {
                return Err(unsupported());
            }
            let provider = scalar(row.get("category_id").ok_or_else(unsupported)?)?;
            category_charge(
                &mut work,
                provider.len().checked_mul(4).ok_or_else(unsupported)?,
            )?;
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
fn parsed_integer(value: &str) -> Option<i32> {
    dispatcharr_ingest_core::source_integer_literal(value)?
        .parse()
        .ok()
}
fn year(row: &serde_json::Map<String, Value>, name: &str) -> Result<Option<i32>, StoreError> {
    if let Some(value) = row
        .get("year")
        .filter(|v| crate::xc_live_provider::truthy(v))
    {
        let parsed = match value {
            Value::String(text) if text.len() <= 255 => parsed_integer(text),
            Value::Number(number) if number.as_i64().is_some() => {
                number.as_i64().and_then(|v| i32::try_from(v).ok())
            }
            _ => return Err(unsupported()),
        };
        if let Some(year) = parsed.filter(|v| (1900..=2030).contains(v)) {
            return Ok(Some(year));
        }
    }
    // Source Movie year considers both date spellings before title fallbacks;
    // this is independent of the narrower release_date custom property field.
    for key in ["releaseDate", "release_date"] {
        if let Some(value) = row.get(key).filter(|v| crate::xc_live_provider::truthy(v)) {
            let text = value
                .as_str()
                .filter(|v| v.len() <= 1024)
                .ok_or_else(unsupported)?;
            let head = list_props::python_trim(text.split('-').next().unwrap_or_default());
            if let Some(year) = parsed_integer(head).filter(|v| (1900..=2030).contains(v)) {
                return Ok(Some(year));
            }
        }
    }
    for pattern in [
        r"\((\d{4})\)",
        r"[\s\x1c-\x1f]-[\s\x1c-\x1f](\d{4})",
        r"[\s\x1c-\x1f](\d{4})(?:\n)?$",
    ] {
        let expression = regex::Regex::new(pattern).map_err(|_| unsupported())?;
        for found in expression.captures_iter(name) {
            // The existing integer decoder pins Source's Unicode15.1 digits;
            // newer regex-library Nd characters cannot hide a later Source match.
            if let Some(literal) = dispatcharr_ingest_core::source_integer_literal(&found[1]) {
                if let Ok(year) = literal.parse::<i32>() {
                    if (1900..=2030).contains(&year) {
                        return Ok(Some(year));
                    }
                }
                break; // Python re.search considers only its first matching group.
            }
        }
    }
    Ok(None)
}
/// Bounded Source list fields, including qualified external identities; language stays empty.
/// Metadata decoding alone does not widen the separately guarded SQL domain.
pub(super) fn movies(
    value: Value,
    is_draining: &dyn Fn() -> bool,
) -> Result<Vec<Movie>, StoreError> {
    let rows = value
        .as_array()
        .filter(|rows| rows.len() <= 1000)
        .ok_or_else(unsupported)?;
    rows.iter()
        .map(|value| {
            if is_draining() {
                return Err(unsupported());
            }
            let row = value.as_object().ok_or_else(unsupported)?;
            if serde_json::to_vec(value).map_err(|_| unsupported())?.len() > 16384 {
                return Err(unsupported());
            }
            for key in ["episode_run_time"] {
                if row.get(key).is_some_and(crate::xc_live_provider::truthy) {
                    return Err(unsupported());
                }
            }
            let raw_name = row
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(unsupported)?;
            let name = list_props::python_trim(raw_name).to_owned();
            if name.is_empty() || name.contains('\0') || name.chars().count() > 255 {
                return Err(unsupported());
            }
            let stream = scalar(row.get("stream_id").ok_or_else(unsupported)?)?;
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
            let ids = external_identity::movie(row)?;
            let props = list_props::movie(row)?;
            Ok(Movie {
                year,
                tmdb_id: ids.tmdb_id,
                imdb_id: ids.imdb_id,
                adult: row.get("is_adult").map(|v| adult(Some(v))),
                name,
                stream,
                provider,
                extension,
                basic: value.clone(),
                description: props.description,
                rating: props.rating,
                genre: props.genre,
                duration: props.duration,
                properties: props.properties,
                logo: props.logo,
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
        assert_eq!(imported[0].adult, Some(false));
        let mut expected = rows[0].clone();
        expected["_provider_category_id"] = json!("4");
        expected["_category_id"] = json!(42);
        assert_eq!(basic(&imported[0], 42), expected);
    }
    #[test]
    fn unsupported_metadata_identity_and_shutdown_do_not_become_sparse_imports() {
        let plain = json!([{"stream_id":1,"name":"Owned - 2020","is_adult":true}]);
        assert!(movies(plain.clone(), &|| true).is_err());
        let mut metadata = plain.clone();
        metadata[0]["episode_run_time"] = json!("unsupported");
        assert!(movies(metadata, &|| false).is_err());
        let imported = movies(plain, &|| false).unwrap();
        assert_eq!(imported[0].adult, Some(true));
        assert_eq!(imported[0].year, Some(2020));
        let cross_key = movies(
            json!([{"stream_id":1,"name":"Owned"},{"stream_id":1,"name":"Other"}]),
            &|| false,
        )
        .unwrap();
        assert_eq!(cross_key.len(), 2);
        assert_eq!(
            (cross_key[0].name.as_str(), cross_key[1].name.as_str()),
            ("Owned", "Other")
        );
    }
    #[test]
    fn multiple_provider_streams_share_identity_and_missing_adult_is_distinct() {
        let rows=movies(json!([{"stream_id":1,"name":"Shared (2020)"},{"stream_id":2,"name":"Shared (2020)","is_adult":"1"}]),&||false).unwrap();
        assert_eq!(
            (rows[0].name.as_str(), rows[0].year),
            (rows[1].name.as_str(), rows[1].year)
        );
        assert_eq!(rows[0].adult, None);
        assert_eq!(rows[1].adult, Some(true));
        assert_ne!(rows[0].stream, rows[1].stream);
        assert!(movies(json!([]), &|| false).unwrap().is_empty());
    }
    #[test]
    fn source_same_identity_provider_repeats_remain_ordered_with_exact_basic_payloads() {
        let input = json!([
            {"stream_id":1,"name":"First","tmdb_id":101,"imdb_id":"tt-first"},
            {"stream_id":1,"name":"Second","tmdb_id":101,"imdb_id":"tt-second"}
        ]);
        let rows = movies(input.clone(), &|| false).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "First");
        assert_eq!(rows[1].name, "Second");
        assert_eq!(rows[0].basic, input[0]);
        assert_eq!(rows[1].basic, input[1]);
        assert_eq!(rows[0].tmdb_id.as_deref(), Some("101"));
        assert_eq!(rows[1].imdb_id.as_deref(), Some("tt-second"));
    }
    #[test]
    fn international_names_dates_and_title_digits_preserve_source_identity() {
        let rows=movies(json!([
            {"stream_id":1,"name":"\u{2003}Été 東京\u{a0}","releaseDate":"\u{2003}2011-09-19\u{a0}"},
            {"stream_id":2,"name":"Film (٢٠١٢)"},
            {"stream_id":3,"name":"Cinéma\u{2003}1990"},
            {"stream_id":4,"name":"界".repeat(255)},
            {"stream_id":5,"name":"Café"},{"stream_id":6,"name":"Cafe\u{301}"}
        ]),&||false).unwrap();
        assert_eq!(rows[0].name, "Été 東京");
        assert_eq!(
            rows.iter().take(4).map(|r| r.year).collect::<Vec<_>>(),
            vec![Some(2011), Some(2012), Some(1990), None]
        );
        assert_eq!(rows[3].name.chars().count(), 255);
        assert_ne!(rows[4].name, rows[5].name);
        assert!(movies(json!([{"stream_id":1,"name":"界".repeat(256)}]), &|| false).is_err());
        assert_eq!(
            year(
                json!({"year":"\u{1c}2011","name":"Movie (2012)"})
                    .as_object()
                    .unwrap(),
                "Movie (2012)"
            )
            .unwrap(),
            Some(2012)
        );
    }
}

#[cfg(test)]
mod provider_category_work_controls {
    use super::*;
    #[test]
    fn sixty_five_provider_categories_are_retained_without_a_count_ceiling() {
        let raw=Value::Array((1..=65).map(|id|json!({"category_id":id.to_string(),"category_name":format!("Owned Lifecycle Movie {id}")})).collect());
        let rows = categories(raw).unwrap();
        assert_eq!(rows.len(), 65);
        assert_eq!(rows[0].provider, "1");
        assert_eq!(rows[64].provider, "65");
    }
    #[test]
    fn category_retention_budget_and_duplicate_identity_remain_fail_closed() {
        let raw=Value::Array((1..=1000).map(|id|json!({"category_id":id.to_string(),"category_name":format!("{}-{id}","x".repeat(200))})).collect());
        assert!(categories(raw).is_err());
        assert!(categories(
            json!([{"category_id":"1","category_name":"A"},{"category_id":"2","category_name":"A"}])
        )
        .is_err());
    }
}
