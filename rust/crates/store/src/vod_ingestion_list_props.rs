//! Bounded list metadata from Original apps/vod/tasks.py:3210/3263.
//! All application/algorithm credit remains with the original Dispatcharr authors.
use crate::{vod_api_draft::vod_api_provider_refresh_value as source, StoreError};
use serde_json::{Map, Value};
fn unsupported() -> StoreError {
    StoreError::Unsupported("VOD bounded list metadata domain unavailable")
}
fn truthy(value: &Value) -> bool {
    crate::xc_live_provider::truthy(value)
}
fn chosen<'a>(row: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter()
        .find_map(|key| row.get(*key).filter(|v| truthy(v)))
}
fn ascii(value: &str, limit: usize) -> Result<&str, StoreError> {
    if !value.is_ascii() || value.contains('\0') || value.len() > limit {
        return Err(unsupported());
    }
    Ok(value)
}
fn direct_text(row: &Map<String, Value>, key: &str, limit: usize) -> Result<String, StoreError> {
    match row.get(key) {
        None => Ok(String::new()),
        Some(Value::String(value)) => Ok(ascii(value, limit)?.to_owned()),
        _ => Err(unsupported()),
    }
}
fn fallback_text(
    row: &Map<String, Value>,
    keys: &[&str],
    limit: usize,
) -> Result<String, StoreError> {
    chosen(row, keys)
        .map(|v| {
            v.as_str()
                .ok_or_else(unsupported)
                .and_then(|v| ascii(v, limit))
        })
        .transpose()
        .map(|v| v.unwrap_or_default().to_owned())
}
fn string_shape(value: &Value) -> Result<(), StoreError> {
    match value {
        Value::String(value) => {
            ascii(value, 1024)?;
        }
        Value::Array(values) if values.len() <= 16 => {
            for value in values {
                ascii(value.as_str().ok_or_else(unsupported)?, 256)?;
            }
        }
        _ => return Err(unsupported()),
    }
    Ok(())
}
fn first(value: Option<&Value>) -> Result<Option<String>, StoreError> {
    let Some(value) = value.filter(|v| truthy(v)) else {
        return Ok(None);
    };
    string_shape(value)?;
    source::string(value).map_err(|_| unsupported())
}
fn actors(value: Option<&Value>) -> Result<Option<String>, StoreError> {
    let Some(value) = value.filter(|v| truthy(v)) else {
        return Ok(None);
    };
    string_shape(value)?;
    let result = if let Value::Array(values) = value {
        let values = values
            .iter()
            .map(|v| v.as_str().expect("validated strings"))
            .map(|v| {
                v.trim_matches(|c: char| {
                    c.is_ascii_whitespace() || matches!(c, '\x0b' | '\x1c'..='\x1f')
                })
            })
            .filter(|v| !v.is_empty())
            .collect::<Vec<_>>();
        let joined = values.join(", ");
        if joined.len() > 4096 {
            return Err(unsupported());
        }
        (!joined.is_empty()).then_some(joined)
    } else {
        source::string(value).map_err(|_| unsupported())?
    };
    Ok(result)
}
fn rating(value: Option<&Value>) -> Result<Option<String>, StoreError> {
    let Some(value) = value.filter(|v| truthy(v)) else {
        return Ok(None);
    };
    let raw = match value {
        Value::String(value) => ascii(value, 32)?.to_owned(),
        Value::Number(value) => value.to_string(),
        _ => return Err(unsupported()),
    };
    let raw = raw.trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r'));
    if !raw.is_empty() {
        let unsigned = raw
            .strip_prefix('+')
            .or_else(|| raw.strip_prefix('-'))
            .unwrap_or(raw);
        if unsigned.is_empty()
            || unsigned
                .bytes()
                .any(|b| !b.is_ascii_digit() && !matches!(b, b'.' | b','))
            || unsigned
                .bytes()
                .filter(|b| matches!(b, b'.' | b','))
                .count()
                > 1
        {
            return Err(unsupported());
        }
        if !raw
            .replace(',', ".")
            .parse::<f64>()
            .is_ok_and(f64::is_finite)
        {
            return Err(unsupported());
        }
    }
    let normalized = source::rating(value).map_err(|_| unsupported())?;
    if normalized.as_ref().is_some_and(|v| v.len() > 10) {
        return Err(unsupported());
    }
    Ok(normalized)
}
fn duration(row: &Map<String, Value>) -> Result<Option<i64>, StoreError> {
    if let Some(value) = row.get("duration_secs").filter(|v| truthy(v)) {
        let raw = match value {
            Value::String(v) => ascii(v, 32)?.to_owned(),
            Value::Number(v) if v.as_i64().is_some() => v.to_string(),
            _ => return Err(unsupported()),
        };
        if raw.bytes().any(|b| !b.is_ascii_digit())
            || !raw.parse::<i64>().is_ok_and(|v| (0..=172800).contains(&v))
        {
            return Err(unsupported());
        }
    }
    if let Some(value) = row.get("duration").filter(|v| truthy(v)) {
        match value {
            Value::String(v) => {
                ascii(v, 64)?;
                if v.bytes().any(|b| !b.is_ascii_digit() && b != b':') {
                    return Err(unsupported());
                }
            }
            Value::Number(v) if v.as_u64().is_some() => {}
            _ => return Err(unsupported()),
        }
    }
    source::duration(&Value::Object(row.clone())).map_err(|_| unsupported())
}
fn insert(properties: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        properties.insert(key.into(), value.into());
    }
}
fn nonempty(properties: Map<String, Value>) -> Option<Value> {
    (!properties.is_empty()).then_some(Value::Object(properties))
}
pub(crate) struct Props {
    pub description: String,
    pub rating: Option<String>,
    pub genre: String,
    pub duration: Option<i64>,
    pub properties: Option<Value>,
    // Keep the raw nonempty string, including >500 bytes: Source must clear
    // such logos and must not choose a later duplicate's URL instead.
    pub logo: Option<String>,
}
pub(crate) fn movie(row: &Map<String, Value>) -> Result<Props, StoreError> {
    let mut properties = Map::new();
    insert(
        &mut properties,
        "youtube_trailer",
        first(chosen(row, &["trailer", "youtube_trailer"]))?,
    );
    insert(
        &mut properties,
        "director",
        first(chosen(row, &["director"]))?,
    );
    insert(
        &mut properties,
        "actors",
        actors(chosen(row, &["actors", "cast"]))?,
    );
    let release = fallback_text(row, &["release_date", "releasedate"], 1024)?;
    if !release.is_empty() {
        properties.insert("release_date".into(), release.into());
    }
    Ok(Props {
        description: fallback_text(row, &["description", "plot"], 4096)?,
        rating: rating(chosen(row, &["rating", "vote_average"]))?,
        genre: fallback_text(row, &["genre", "category_name"], 255)?,
        duration: duration(row)?,
        properties: nonempty(properties),
        logo: chosen(row, &["stream_icon"])
            .map(|v| {
                v.as_str()
                    .ok_or_else(unsupported)
                    .and_then(|v| ascii(v, 1024))
                    .map(str::to_owned)
            })
            .transpose()?,
    })
}
pub(crate) fn series(row: &Map<String, Value>) -> Result<Props, StoreError> {
    let mut properties = Map::new();
    insert(&mut properties, "cast", actors(chosen(row, &["cast"]))?);
    insert(
        &mut properties,
        "director",
        first(chosen(row, &["director"]))?,
    );
    insert(
        &mut properties,
        "youtube_trailer",
        first(chosen(row, &["youtube_trailer"]))?,
    );
    for key in ["category_id", "releaseDate"] {
        if let Some(value) = row.get(key).filter(|v| truthy(v)) {
            properties.insert(key.into(), value.clone());
        }
    }
    Ok(Props {
        description: direct_text(row, "plot", 4096)?,
        rating: rating(row.get("rating"))?,
        genre: direct_text(row, "genre", 255)?,
        duration: None,
        properties: nonempty(properties),
        logo: chosen(row, &["cover"])
            .map(|v| {
                v.as_str()
                    .ok_or_else(unsupported)
                    .and_then(|v| ascii(v, 1024))
                    .map(str::to_owned)
            })
            .transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn source_movie_fallbacks_and_array_rules() {
        let row = json!({"description":"","plot":"fallback","rating":0,"vote_average":"8,5","genre":"","category_name":"Drama","duration_secs":0,"duration":"01:02:03","director":[" "," First ","Second"],"actors":[" Actor One ",""," Actor Two "],"trailer":[""," trailer "],"release_date":"2010-01-01"});
        let props = movie(row.as_object().unwrap()).unwrap();
        assert_eq!(props.description, "fallback");
        assert_eq!(props.rating.as_deref(), Some("8.5"));
        assert_eq!(props.genre, "Drama");
        assert_eq!(props.duration, Some(3723));
        assert_eq!(
            props.properties,
            Some(
                json!({"youtube_trailer":"trailer","director":"First","actors":"Actor One, Actor Two","release_date":"2010-01-01"})
            )
        );
    }
    #[test]
    fn zero_rating_and_sparse_series_stay_distinct() {
        for (value, expected) in [(json!(0), None), (json!("0"), Some("0.0"))] {
            assert_eq!(rating(Some(&value)).unwrap().as_deref(), expected);
        }
        let props=series(json!({"plot":"","genre":"","category_id":"11","releaseDate":"2010","cast":[" A "," B "]}).as_object().unwrap()).unwrap();
        assert_eq!(
            props.properties,
            Some(json!({"category_id":"11","releaseDate":"2010","cast":"A, B"}))
        );
        for value in [Value::Null, json!(false), json!(0), json!([]), json!({})] {
            assert!(series(json!({"plot":value}).as_object().unwrap()).is_err());
        }
    }
    #[test]
    fn bounded_metadata_refuses_coercions_and_preserves_invalid_logo_for_source_clear() {
        for row in [
            json!({"director":[1]}),
            json!({"rating":"NaN"}),
            json!({"rating":"1e2"}),
            json!({"genre":"x".repeat(256)}),
            json!({"duration":1.5}),
            json!({"description":"é"}),
        ] {
            assert!(movie(row.as_object().unwrap()).is_err());
        }
        let raw = "x".repeat(501);
        let props = movie(json!({"stream_icon":raw}).as_object().unwrap()).unwrap();
        assert_eq!(props.logo.as_ref().unwrap().len(), 501);
    }
}
