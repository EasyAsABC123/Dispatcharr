//! Isolated Source Movie refresh phase conversion. No SQL/API registration.
//! Identity merge is an interleaved SQL phase, not an all-or-nothing plan.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{vod_api_projection::truthy, vod_api_provider_refresh_value as source};
use serde_json::{Map, Value};
use source::Fault;
pub(super) fn details(value: &Value) -> Value {
    if value.is_object() {
        value.clone()
    } else {
        value
            .as_array()
            .and_then(|v| v.first())
            .filter(|v| v.is_object())
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new()))
    }
}
/// Move the accepted provider object into its phase instead of duplicating an
/// entire adversarial info tree while the same work lease also owns SQL data.
pub(super) fn owned_details(value: Value) -> Value {
    match value {
        Value::Object(_) => value,
        Value::Array(values) => values
            .into_iter()
            .next()
            .filter(Value::is_object)
            .unwrap_or_else(|| Value::Object(Map::new())),
        _ => Value::Object(Map::new()),
    }
}
pub(super) struct Details {
    pub info: Value,
    pub data: Value,
}
impl Details {
    pub fn from_owned_response(mut value: Value) -> Option<Self> {
        let object = value.as_object_mut()?;
        let info = object.remove("info")?;
        let data = object.remove("movie_data").unwrap_or(Value::Null);
        Some(Self {
            info: owned_details(info),
            data: owned_details(data),
        })
    }
    pub fn from_response(value: &Value) -> Option<Self> {
        let map = value.as_object()?;
        map.contains_key("info").then(|| Self {
            info: details(&value["info"]),
            data: details(&value["movie_data"]),
        })
    }
}
pub(super) struct ParentPhase {
    /// Full task-read Movie snapshot: Django save writes unchanged fields too.
    pub movie: Value,
    pub updated: bool,
    pub tmdb: Option<Value>,
    pub imdb: Option<Value>,
    props_before_identity: Value,
    props_aliases_identity: bool,
}
fn field_text(value: &Value) -> Result<Value, Fault> {
    source::text(value).map(Into::into)
}
pub(super) fn before_identity(movie: &Value, details: &Details) -> Result<ParentPhase, Fault> {
    let mut phase = ParentPhase {
        movie: movie.clone(),
        updated: false,
        tmdb: None,
        imdb: None,
        props_before_identity: if truthy(&movie["custom_properties"])
            && !movie["custom_properties"].is_object()
        {
            movie["custom_properties"].clone()
        } else {
            Value::Object(Map::new())
        },
        props_aliases_identity: truthy(&movie["custom_properties"])
            && movie["custom_properties"].is_object(),
    };
    let info = &details.info;
    for (source_key, key) in [("plot", "description"), ("genre", "genre")] {
        if truthy(&info[source_key]) && info[source_key] != phase.movie[key] {
            phase.movie[key] = field_text(&info[source_key])?;
            phase.updated = true;
        }
    }
    if let Some(rating) = source::rating(&info["rating"])? {
        if phase.movie["rating"] != rating {
            phase.movie["rating"] = rating.into();
            phase.updated = true;
        }
    }
    if let Some(duration) = source::duration(info)? {
        if phase.movie["duration_secs"] != duration {
            phase.movie["duration_secs"] = duration.into();
            phase.updated = true;
        }
    }
    let release = if truthy(&info["releasedate"]) {
        &info["releasedate"]
    } else {
        &info["release_date"]
    };
    if truthy(release) {
        let text = source::text(release)?;
        if let Some(year) = source::integer(text.split('-').next().unwrap_or("")) {
            let value: Value =
                serde_json::from_str(&year.to_string()).map_err(|_| Fault::Capacity)?;
            if phase.movie["year"] != value {
                phase.movie["year"] = value;
                phase.updated = true;
            }
        }
    }
    // Source IDs can be arbitrary provider JSON, but a retained duplicate must
    // fit the already existing text-conversion budget before it is cloned.
    for key in ["tmdb_id","imdb_id"]{if super::vod_api_actions_account::heap(&info[key]).map_err(|_|Fault::Capacity)?>256*1024{return Err(Fault::Capacity);}}
    phase.tmdb = (truthy(&info["tmdb_id"]) && info["tmdb_id"] != phase.movie["tmdb_id"])
        .then(|| info["tmdb_id"].clone());
    phase.imdb = (truthy(&info["imdb_id"]) && info["imdb_id"] != phase.movie["imdb_id"])
        .then(|| info["imdb_id"].clone());
    Ok(phase)
}
/// Call only AFTER the actual Source identity lookup/clear/merge/save/transfer/
/// delete sequence. Its committed prefix must survive later conversion errors.
pub(super) fn after_identity(phase: &mut ParentPhase, details: &Details) -> Result<(), Fault> {
    for (key, value) in [("tmdb_id", &phase.tmdb), ("imdb_id", &phase.imdb)] {
        if let Some(value) = value {
            phase.movie[key] = field_text(value)?;
            phase.updated = true;
        }
    }
    let props = if phase.props_aliases_identity {
        &phase.movie["custom_properties"]
    } else {
        &phase.props_before_identity
    };
    let mut props = props.as_object().ok_or(Fault::Type)?.clone();
    let info = &details.info;
    for (key, incoming) in [
        ("youtube_trailer", "trailer"),
        ("youtube_trailer", "youtube_trailer"),
        ("backdrop_path", "backdrop_path"),
    ] {
        let old = props.get(key).unwrap_or(&Value::Null);
        if source::should_update(old, &info[incoming])? {
            let value = source::string(&info[incoming])?;
            props.insert(
                key.into(),
                if key == "backdrop_path" {
                    value
                        .map(|s| Value::Array(vec![s.into()]))
                        .unwrap_or(Value::Null)
                } else {
                    value.into()
                },
            );
            phase.updated = true;
        }
    }
    for key in ["actors", "cast"] {
        if source::should_update(props.get("actors").unwrap_or(&Value::Null), &info[key])? {
            let value = if let Some(values) = info[key].as_array() {
                let mut parts = Vec::new();
                for value in values {
                    if truthy(value) && !source::text(value)?.trim().is_empty() {
                        parts.push(value.as_str().ok_or(Fault::Type)?.trim().to_owned());
                    }
                }
                (!parts.is_empty()).then(|| parts.join(", "))
            } else {
                source::string(&info[key])?
            };
            props.insert("actors".into(), value.into());
            phase.updated = true;
            break;
        }
    }
    if source::should_update(
        props.get("director").unwrap_or(&Value::Null),
        &info["director"],
    )? {
        props.insert("director".into(), source::string(&info["director"])?.into());
        phase.updated = true;
    }
    if phase.updated {
        phase.movie["custom_properties"] = source::clean(&props.into())?;
    }
    Ok(())
}
/// Apply only AFTER the parent save (and its Source retry) physically finished.
/// Keep updated_at untouched: Django update_fields names only these two fields.
pub(super) fn relation(
    relation: &Value,
    details: &Details,
    attempt_time: &str,
) -> Result<Value, Fault> {
    let empty = Value::Object(Map::new());
    let props = if truthy(&relation["custom_properties"]) {
        &relation["custom_properties"]
    } else {
        &empty
    };
    let mut props = props.as_object().ok_or(Fault::Type)?.clone();
    for (key, value) in [
        ("detailed_info", &details.info),
        ("movie_data", &details.data),
    ] {
        let clean = source::clean(value)?;
        if truthy(&clean) {
            props.insert(key.into(), clean);
        }
    }
    props.insert("detailed_fetched".into(), true.into());
    let mut result = relation.clone();
    result["custom_properties"] = props.into();
    result["last_advanced_refresh"] = attempt_time.into();
    Ok(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_movie_phase_conversions() {
        let data: Value = serde_json::from_str(include_str!(
            "../datasets/source-provider-refresh-movie-phases-1.json"
        ))
        .unwrap();
        let rows = data["records"].as_array().unwrap();
        assert!(rows.len() > 15);
        for row in rows {
            let (mut movie, relation) =
                if let Some(details) = Details::from_response(&row["payload"]) {
                    let mut phase = before_identity(&row["movie"], &details).unwrap();
                    assert!(
                        phase.tmdb.is_none() && phase.imdb.is_none(),
                        "Identity SQL needs its own fixture"
                    );
                    after_identity(&mut phase, &details).unwrap();
                    let relation = relation(
                        &row["relation"],
                        &details,
                        row["expected_relation"]["last_advanced_refresh"]
                            .as_str()
                            .unwrap(),
                    )
                    .unwrap();
                    (phase.movie, relation)
                } else {
                    (row["movie"].clone(), row["relation"].clone())
                };
            // This tests pure metadata conversion, never invents a runtime clock
            // or claims the independently timed TCP rows are byte-identical.
            movie["updated_at"] = row["expected_movie"]["updated_at"].clone();
            assert_eq!(movie, row["expected_movie"], "Movie {}", row["label"]);
            assert_eq!(
                relation, row["expected_relation"],
                "Relation {}",
                row["label"]
            );
        }
    }
}
