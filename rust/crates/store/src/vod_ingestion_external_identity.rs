//! Bounded Source list external-ID selection; SQL admission is separately guarded.
//! Source apps/vod/tasks.py Movie:554, Series:916. Original authors retain credit.
use crate::{vod_api_draft::vod_api_provider_refresh_value as source, StoreError};
use serde_json::{Map, Value};
#[derive(Debug, PartialEq)]
pub(crate) struct ExternalIds {
    pub tmdb_id: Option<String>,
    pub imdb_id: Option<String>,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("VOD bounded list external-ID domain unavailable")
}
fn bounded(value: &Value) -> Result<(), StoreError> {
    let valid = match value {
        Value::Null => true,
        Value::String(text) => {
            text.bytes().all(|byte| (b' '..=b'~').contains(&byte)) && text.len() <= 50
        }
        Value::Number(number) => {
            let text = number.to_string();
            !text.is_empty() && text.len() <= 50 && text.bytes().all(|b| b.is_ascii_digit())
        }
        _ => false,
    };
    valid.then_some(()).ok_or_else(unsupported)
}
fn selected(
    row: &Map<String, Value>,
    first: &str,
    fallback: &str,
) -> Result<Option<String>, StoreError> {
    // Validate both declared alias inputs: ignored exotic shapes remain outside
    // this bounded slice, rather than being silently admitted as sparse.
    for key in [first, fallback] {
        if let Some(value) = row.get(key) {
            bounded(value)?;
        }
    }
    // Python `a or b` chooses BEFORE cleaning. Numeric0 falls through; string
    // "0" is truthy, blocks fallback, and is cleaned only after selection.
    let value = row
        .get(first)
        .filter(|v| crate::xc_live_provider::truthy(v))
        .or_else(|| row.get(fallback));
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    // Uses existing exact Python text conversion, never array/string extraction,
    // whitespace trimming, integer parsing or case folding.
    let text = source::text(value).map_err(|_| unsupported())?;
    Ok((!text.is_empty() && text != "0").then_some(text))
}
pub(crate) fn movie(row: &Map<String, Value>) -> Result<ExternalIds, StoreError> {
    Ok(ExternalIds {
        tmdb_id: selected(row, "tmdb_id", "tmdb")?,
        imdb_id: selected(row, "imdb_id", "imdb")?,
    })
}
pub(crate) fn series(row: &Map<String, Value>) -> Result<ExternalIds, StoreError> {
    Ok(ExternalIds {
        tmdb_id: selected(row, "tmdb", "tmdb_id")?,
        imdb_id: selected(row, "imdb", "imdb_id")?,
    })
}
/// Source primary key precedence, used only to guard unqualified cross-key
/// provider-ID duplicates. Input rows remain in original order for SQL grouping.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum Key {
    Tmdb(String),
    Imdb(String),
    NameYear(String, Option<i32>),
}
pub(crate) fn key(ids: &ExternalIds, name: &str, year: Option<i32>) -> Key {
    if let Some(id) = &ids.tmdb_id {
        Key::Tmdb(id.clone())
    } else if let Some(id) = &ids.imdb_id {
        Key::Imdb(id.clone())
    } else {
        Key::NameYear(name.to_owned(), year)
    }
}

#[cfg(test)]
mod source_alias_controls {
    use super::*;
    use serde_json::json;
    #[test]
    fn aliases_select_before_cleaning_and_keep_movie_series_order_distinct() {
        let row = json!({"tmdb_id":101,"tmdb":202,"imdb_id":"tt-first","imdb":"tt-second"});
        let row = row.as_object().unwrap();
        assert_eq!(
            movie(row).unwrap(),
            ExternalIds {
                tmdb_id: Some("101".into()),
                imdb_id: Some("tt-first".into())
            }
        );
        assert_eq!(
            series(row).unwrap(),
            ExternalIds {
                tmdb_id: Some("202".into()),
                imdb_id: Some("tt-second".into())
            }
        );
        for first in [json!(0), json!(""), Value::Null] {
            assert_eq!(
                movie(json!({"tmdb_id":first,"tmdb":303}).as_object().unwrap())
                    .unwrap()
                    .tmdb_id,
                Some("303".into())
            );
        }
        assert_eq!(
            movie(json!({"tmdb_id":"0","tmdb":303}).as_object().unwrap())
                .unwrap()
                .tmdb_id,
            None
        );
        assert_eq!(
            series(json!({"tmdb":"0","tmdb_id":303}).as_object().unwrap())
                .unwrap()
                .tmdb_id,
            None
        );
    }
    #[test]
    fn opaque_text_is_not_trimmed_or_numeric_normalized_and_schema_bound_is_fifty() {
        for text in ["00", "0 ", " 0", "tt-ignored"] {
            assert_eq!(
                movie(json!({"imdb_id":text}).as_object().unwrap())
                    .unwrap()
                    .imdb_id,
                Some(text.into())
            );
        }
        assert!(movie(json!({"tmdb_id":"x".repeat(50)}).as_object().unwrap()).is_ok());
        let integer: Value = serde_json::from_str(&"9".repeat(50)).unwrap();
        assert_eq!(
            movie(json!({"tmdb_id":integer}).as_object().unwrap())
                .unwrap()
                .tmdb_id,
            Some("9".repeat(50))
        );
        let too_large: Value = serde_json::from_str(&"9".repeat(51)).unwrap();
        assert!(movie(json!({"tmdb_id":too_large}).as_object().unwrap()).is_err());
        // An ignored alias remains subject to the declared bounded input domain.
        assert!(movie(json!({"tmdb_id":101,"tmdb":true}).as_object().unwrap()).is_err());
        for value in [
            json!("x".repeat(51)),
            json!("é"),
            json!("a\0b"),
            json!("\tID\n"),
            json!("\u{007f}"),
            json!(true),
            json!(false),
            json!(1.0),
            json!(-1),
            json!([]),
            json!({}),
        ] {
            assert!(movie(json!({"tmdb_id":value}).as_object().unwrap()).is_err());
        }
    }
}
