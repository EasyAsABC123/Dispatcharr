//! Bounded Series list values from Original apps/vod/tasks.py.
//! Original Dispatcharr authors retain all application and algorithm credit.
use crate::StoreError;
use serde_json::{json, Value};
fn unsupported() -> StoreError {
    StoreError::Unsupported("VOD basic Series list value domain unavailable")
}
pub(super) struct Series {
    pub name: String,
    pub year: Option<i32>,
    pub tmdb_id: Option<String>,
    pub imdb_id: Option<String>,
    pub external: String,
    pub provider: Option<String>,
    pub properties: Option<Value>,
    pub basic: Value,
    pub description: String,
    pub rating: Option<String>,
    pub genre: String,
    pub logo: Option<String>,
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
/// Source int(date.split('-')[0]) differs from str.strip and never reads title.
/// Unicode numeric syntax reuses the existing Source15.1 integer decoder.
fn date_year(value: Option<&Value>) -> Result<Option<i32>, StoreError> {
    let Some(value) = value.filter(|v| crate::xc_live_provider::truthy(v)) else {
        return Ok(None);
    };
    let text = value
        .as_str()
        .filter(|v| v.len() <= 255)
        .ok_or_else(unsupported)?;
    match dispatcharr_ingest_core::source_integer_literal(
        text.split('-').next().unwrap_or_default(),
    ) {
        None => Ok(None),
        Some(integer) => integer.parse::<i32>().map(Some).map_err(|_| unsupported()),
    }
}
fn list_year(row: &serde_json::Map<String, Value>) -> Result<Option<i32>, StoreError> {
    let mut year = date_year(row.get("releaseDate"))?;
    if year.is_none_or(|v| v == 0)
        && row
            .get("release_date")
            .is_some_and(crate::xc_live_provider::truthy)
    {
        year = date_year(row.get("release_date"))?;
    }
    Ok(year)
}
/// Provisional sparse untagged Series list slice. Existing catalogue admission
/// remains unchanged and rejects advanced stored rows until Source acceptance.
/// Qualified external IDs and bounded descriptive fields share Source converters; language stays empty.
pub(super) fn series(
    value: Value,
    is_draining: &dyn Fn() -> bool,
) -> Result<Vec<Series>, StoreError> {
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
            for key in [
                "backdrop_path",
                "poster_path",
                "original_name",
                "first_air_date",
                "last_air_date",
                "episode_run_time",
                "status",
                "type",
                "country",
                "language",
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
            let name = super::values::list_props::python_trim(name).to_owned();
            if name.is_empty() || name.contains('\0') || name.chars().count() > 255 {
                return Err(unsupported());
            }
            let external = scalar(row.get("series_id").ok_or_else(unsupported)?)?;
            let raw_category = row
                .get("category_id")
                .filter(|v| crate::xc_live_provider::truthy(v));
            let provider = raw_category.map(scalar).transpose()?;
            let props = super::values::list_props::series(row)?;
            let year = list_year(row)?;
            let ids = super::values::external_identity::series(row)?;
            Ok(Series {
                name,
                year,
                tmdb_id: ids.tmdb_id,
                imdb_id: ids.imdb_id,
                external,
                provider,
                properties: props.properties,
                basic: value.clone(),
                description: props.description,
                rating: props.rating,
                genre: props.genre,
                logo: props.logo,
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
    fn sparse_dates_determine_identity_without_guessing_from_title() {
        let rows = series(
            json!([
                {"series_id":1,"name":"Owned (2020)","category_id":"11","releaseDate":"2010-01-01"},
                {"series_id":2,"name":"Owned (2020)","release_date":"2011-02-03"},
                {"series_id":3,"name":"Owned (2020)"},
                {"series_id":4,"name":"Owned","releaseDate":"bad","release_date":"2012"},
                {"series_id":5,"name":"Owned","releaseDate":"0","release_date":"2013"}
            ]),
            &|| false,
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|s| s.year).collect::<Vec<_>>(),
            vec![Some(2010), Some(2011), None, Some(2012), Some(2013)]
        );
        assert_eq!(
            rows[0].properties,
            Some(json!({"category_id":"11","releaseDate":"2010-01-01"}))
        );
        assert_eq!(
            rows[1].properties, None,
            "release_date affects identity, not Source list CP"
        );
        assert_eq!(
            rows[3].properties,
            Some(json!({"releaseDate":"bad"})),
            "Source retains truthy raw releaseDate independently of parsed year"
        );
    }
    #[test]
    fn unqualified_date_shapes_are_refused_without_silent_identity_coercion() {
        for value in [
            json!(2010),
            json!(["2010"]),
            json!({"year":2010}),
            json!("2147483648"),
        ] {
            assert!(series(
                json!([{"series_id":1,"name":"Owned","releaseDate":value}]),
                &|| false
            )
            .is_err());
        }
    }

    #[test]
    fn sparse_plot_and_genre_refuse_source_coercions_outside_this_domain() {
        for key in ["plot", "genre"] {
            for value in [Value::Null, json!(false), json!(0), json!([]), json!({})] {
                let mut row = json!({"series_id":1,"name":"Owned"});
                row[key] = value;
                assert!(series(json!([row]), &|| false).is_err(), "{key}");
            }
            let mut row = json!({"series_id":1,"name":"Owned"});
            row[key] = json!("");
            assert!(series(json!([row]), &|| false).is_ok(), "{key}");
        }
    }
    #[test]
    fn date_integer_whitespace_differs_from_python_string_strip() {
        for byte in [b' ', b'\t', b'\n', 0x0b, 0x0c, b'\r'] {
            let space = char::from(byte);
            assert_eq!(
                date_year(Some(&json!(format!("{space}2010{space}-01-01")))).unwrap(),
                Some(2010)
            );
        }
        for byte in 0x1c..=0x1f {
            let separator = char::from(byte);
            for value in [format!("{separator}2010"), format!("2010{separator}")] {
                assert_eq!(date_year(Some(&json!(value))).unwrap(), None);
            }
        }
    }
    #[test]
    fn source_name_strip_includes_vertical_tab_without_changing_date_rules() {
        let rows = series(
            json!([{"series_id":1,"name":"\u{000b}Owned\u{000b}","releaseDate":"\u{001c}2010"}]),
            &|| false,
        )
        .unwrap();
        assert_eq!(rows[0].name, "Owned");
        assert_eq!(
            rows[0].year, None,
            "Python int does not strip the extra str.strip separators"
        );
    }
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
    fn unqualified_metadata_and_shutdown_do_not_become_sparse_success() {
        let plain = json!([{"series_id":1,"name":"Owned"}]);
        assert!(series(plain.clone(), &|| true).is_err());
        for key in ["language", "backdrop_path"] {
            let mut row = plain.clone();
            row[0][key] = json!("unqualified");
            assert!(series(row, &|| false).is_err(), "{key}");
        }
        let cross_key = series(
            json!([{"series_id":1,"name":"Owned"},{"series_id":1,"name":"Other"}]),
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
    fn source_same_identity_series_provider_repeats_keep_order_and_short_alias_priority() {
        let input = json!([
            {"series_id":1,"name":"First","tmdb":101,"tmdb_id":202},
            {"series_id":1,"name":"Second","tmdb":101,"tmdb_id":303}
        ]);
        let rows = series(input.clone(), &|| false).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "First");
        assert_eq!(rows[1].name, "Second");
        assert_eq!(rows[0].basic, input[0]);
        assert_eq!(rows[1].basic, input[1]);
        assert_eq!(rows[0].tmdb_id.as_deref(), Some("101"));
        assert_eq!(rows[1].tmdb_id.as_deref(), Some("101"));
    }
    #[test]
    fn international_series_uses_date_only_and_preserves_source_character_limits() {
        let rows=series(json!([
            {"series_id":1,"name":"\u{2003}Été Série 東京\u{a0}","releaseDate":"\u{2003}2011-09-19\u{a0}"},
            {"series_id":2,"name":"Série (٢٠١٢)"},
            {"series_id":3,"name":"語".repeat(255),"releaseDate":"２０１３-01-01"}
        ]),&||false).unwrap();
        assert_eq!(rows[0].name, "Été Série 東京");
        assert_eq!(
            rows.iter().map(|r| r.year).collect::<Vec<_>>(),
            vec![Some(2011), None, Some(2013)]
        );
        assert_eq!(date_year(Some(&json!("2_010"))).unwrap(), Some(2010));
        assert!(series(json!([{"series_id":1,"name":"語".repeat(256)}]), &|| false).is_err());
    }
}
