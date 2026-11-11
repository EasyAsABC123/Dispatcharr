//! Source-derived cached provider-info extension and host controls.
//! Actual Source90 TCP and owned-resource gates passed; canonical app,
//! provider refresh, image serving and full React modal gates are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{vod_api_projection::truthy, vod_api_provider_artwork as artwork};
use num_bigint::BigInt;
use serde_json::{json, Map, Value};
#[derive(Debug)]
pub(super) enum Fault {
    Framework,
    Capacity,
    Series(String),
}
fn pystr(value: &Value) -> Result<String, Fault> {
    match value {
        Value::String(text) => Ok(text.clone()),
        _ => crate::python_action::collection_text(value, 256 * 1024).ok_or(Fault::Capacity),
    }
}
fn charfield(value: &Value) -> Result<Value, Fault> {
    if value.is_null() {
        Ok(Value::Null)
    } else {
        pystr(value).map(Value::String)
    }
}
fn integer(value: &Value) -> Result<Value, Fault> {
    let number = match value {
        Value::Bool(flag) => BigInt::from(u8::from(*flag)),
        Value::String(text) => dispatcharr_ingest_core::source_integer_literal(text)
            .and_then(|value| BigInt::parse_bytes(value.as_bytes(), 10))
            .ok_or(Fault::Framework)?,
        Value::Number(number) => {
            let text = number.to_string();
            if !text.contains(['.', 'e', 'E']) {
                BigInt::parse_bytes(text.as_bytes(), 10).ok_or(Fault::Framework)?
            } else {
                let value = number
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .ok_or(Fault::Framework)?;
                BigInt::parse_bytes(format!("{:.0}", value.trunc()).as_bytes(), 10)
                    .ok_or(Fault::Framework)?
            }
        }
        _ => return Err(Fault::Framework),
    };
    serde_json::from_str(&number.to_string()).map_err(|_| Fault::Framework)
}
fn object(value: &Value) -> Result<&Map<String, Value>, Fault> {
    value.as_object().ok_or(Fault::Framework)
}
fn get<'a>(map: &'a Map<String, Value>, key: &str) -> &'a Value {
    map.get(key).unwrap_or(&Value::Null)
}
fn or<'a>(values: impl IntoIterator<Item = &'a Value>) -> Option<&'a Value> {
    values.into_iter().find(|value| truthy(value))
}
fn fallback<'a>(map: &'a Map<String, Value>, key: &str, default: &'a Value) -> &'a Value {
    map.get(key).unwrap_or(default)
}
fn nullable_integer(value: &Value, original: &Value) -> Value {
    if value.is_null() || value == "" {
        Value::Null
    } else {
        integer(value).unwrap_or_else(|_| original.clone())
    }
}
fn dt(value: &Value) -> Result<Value, Fault> {
    let date = chrono::DateTime::parse_from_rfc3339(value.as_str().ok_or(Fault::Framework)?)
        .map_err(|_| Fault::Framework)?
        .with_timezone(&chrono::Utc);
    Ok(if date.timestamp_subsec_micros() == 0 {
        date.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    } else {
        date.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
    }
    .into())
}
fn boolfield(value: &Value) -> Value {
    if value.is_null() {
        return Value::Null;
    }
    if let Some(text) = value.as_str() {
        let text = text.to_ascii_lowercase();
        if ["t", "y", "yes", "true", "on", "1"].contains(&text.as_str()) {
            return true.into();
        }
        if ["f", "n", "no", "false", "off", "0"].contains(&text.as_str()) {
            return false.into();
        }
    }
    truthy(value).into()
}
fn account(raw: &Value) -> Value {
    json!({"id":raw["id"],"name":raw["name"],"account_type":raw["account_type"]})
}
/// `movie` is the original get_object snapshot, not a later replacement read.
/// `logo` is its select_related snapshot. URL shaping does not fetch images.
pub(super) fn movie(
    movie: &Value,
    relation: &Value,
    account_row: &Value,
    logo: Option<&Value>,
    origin: &str,
) -> Result<Value, Fault> {
    let empty = json!({});
    let blank = json!("");
    let zero = json!(0);
    let props = if truthy(&relation["custom_properties"]) {
        &relation["custom_properties"]
    } else {
        &empty
    };
    let props = object(props)?;
    let info = fallback(props, "detailed_info", &empty);
    let info = object(info)?;
    let data = fallback(props, "movie_data", &empty);
    let data = object(data)?;
    let movie_props = if truthy(&movie["custom_properties"]) {
        &movie["custom_properties"]
    } else {
        &empty
    };
    let movie_props = object(movie_props)?;
    let art = artwork::preferred(&relation["custom_properties"], &movie["custom_properties"]);
    let id = movie["id"].as_i64().ok_or(Fault::Framework)?;
    let account_id = account_row["id"].as_i64().ok_or(Fault::Framework)?;
    let cover = if artwork::proxyable(&art.image) {
        artwork::single(origin, "movie", id, "movie_image", account_id, &art.image)
    } else {
        logo.map(|logo| artwork::logo_url(origin, logo))
            .unwrap_or_default()
    };
    let raw_rating = if truthy(&movie["rating"]) {
        &movie["rating"]
    } else {
        get(info, "rating")
    };
    let rating = if raw_rating.is_null() || raw_rating == "" {
        Value::Null
    } else {
        pystr(raw_rating)?.into()
    };
    let year = if !movie["year"].is_null() {
        &movie["year"]
    } else {
        get(info, "year")
    };
    let year = nullable_integer(year, &movie["year"]);
    let duration = if !movie["duration_secs"].is_null() {
        &movie["duration_secs"]
    } else {
        get(info, "duration_secs")
    };
    let duration = nullable_integer(duration, &movie["duration_secs"]);
    let bitrate = integer(or([get(info, "bitrate"), &zero]).unwrap_or(&zero)).unwrap_or(zero);
    let value =
        |values: Vec<&Value>| -> Result<Value, Fault> { charfield(or(values).unwrap_or(&blank)) };
    Ok(
        json!({"id":movie["id"],"uuid":movie["uuid"],"stream_id":charfield(&relation["stream_id"])?,"name":charfield(fallback(info,"name",&movie["name"]))?,"o_name":value(vec![get(info,"o_name")])?,
 "description":charfield(fallback(info,"description",fallback(info,"plot",&movie["description"])))?,"plot":charfield(fallback(info,"plot",fallback(info,"description",&movie["description"])))?,"year":year,
 "release_date":value(vec![get(movie_props,"release_date"),get(info,"release_date"),get(info,"releasedate")])?,"genre":value(vec![&movie["genre"],get(info,"genre")])?,"director":value(vec![get(movie_props,"director"),get(info,"director")])?,"actors":value(vec![get(movie_props,"actors"),get(info,"actors")])?,"country":value(vec![get(movie_props,"country"),get(info,"country")])?,"rating":rating,
 "tmdb_id":charfield(or([&movie["tmdb_id"],get(info,"tmdb_id")]).unwrap_or(&Value::Null))?,"imdb_id":charfield(or([&movie["imdb_id"],get(info,"imdb_id")]).unwrap_or(&Value::Null))?,"youtube_trailer":value(vec![get(movie_props,"youtube_trailer"),get(info,"youtube_trailer"),get(info,"trailer")])?,"duration_secs":duration,"age":value(vec![get(info,"age")])?,
 "backdrop_path":artwork::rewritten_backdrops(origin,"movie",id,account_id,&art.backdrops),"cover":cover,"cover_big":cover,"movie_image":cover,"bitrate":bitrate,"video":or([get(info,"video"),&empty]).unwrap_or(&empty),"audio":or([get(info,"audio"),&empty]).unwrap_or(&empty),
 "container_extension":value(vec![get(data,"container_extension"),&json!("mp4")])?,"direct_source":value(vec![get(data,"direct_source")])?,"category_id":pystr(or([get(data,"category_id"),&blank]).unwrap_or(&blank))?,"added":value(vec![get(data,"added")])?,"m3u_account":account(account_row)}),
    )
}
/// Field conversion after Source view calculations. None skips the declared
/// serializer coercion even for nonnullable schema fields, matching DRF output.
pub(super) fn series_header(
    series: &Value,
    relation: &Value,
    account_row: &Value,
    category: Option<&Value>,
    logo: Option<&Value>,
    origin: &str,
) -> Result<Value, Fault> {
    let empty = json!({});
    let props = if truthy(&relation["custom_properties"]) {
        &relation["custom_properties"]
    } else {
        &empty
    };
    let props = object(props)?;
    let art = artwork::preferred(&relation["custom_properties"], &series["custom_properties"]);
    let id = series["id"].as_i64().ok_or(Fault::Framework)?;
    let account_id = account_row["id"].as_i64().ok_or(Fault::Framework)?;
    Ok(
        json!({"id":series["id"],"series_id":charfield(&relation["external_series_id"])?,"name":charfield(&series["name"])?,"description":charfield(&series["description"])?,"year":series["year"],"genre":charfield(&series["genre"])?,"rating":charfield(&series["rating"])?,"tmdb_id":charfield(&series["tmdb_id"])?,"imdb_id":charfield(&series["imdb_id"])?,"category_id":category.map(|row|row["id"].clone()),"category_name":category.map(|row|row["name"].clone()),"cover":artwork::series_cover(origin,id,series["name"].as_str().ok_or(Fault::Framework)?,account_id,&art,logo),"backdrop_path":artwork::rewritten_backdrops(origin,"series",id,account_id,&art.backdrops),"last_refreshed":dt(&series["updated_at"])?,"custom_properties":series["custom_properties"],"m3u_account":account(account_row),"episodes_fetched":boolfield(props.get("episodes_fetched").unwrap_or(&Value::Bool(false))),"detailed_fetched":boolfield(props.get("detailed_fetched").unwrap_or(&Value::Bool(false)))}),
    )
}

/// Rows are already provider-filtered and season/episode ordered by SQL. The
/// caller retains only one decoded episode at a time under shared work credits.
pub(super) fn episode(
    episode: &Value,
    relation: &Value,
    series: &Value,
    account_id: i64,
    origin: &str,
) -> Result<Value, Fault> {
    let id = episode["id"].as_i64().ok_or(Fault::Framework)?;
    let art = artwork::preferred(
        &relation["custom_properties"],
        &episode["custom_properties"],
    );
    Ok(
        json!({"id":episode["id"],"uuid":episode["uuid"],"name":charfield(&episode["name"])? ,"title":charfield(&episode["name"])? ,"episode_number":episode["episode_number"],"season_number":episode["season_number"],"description":charfield(&episode["description"])? ,"air_date":episode["air_date"],"plot":charfield(&episode["description"])? ,"duration_secs":episode["duration_secs"],"rating":charfield(&episode["rating"])? ,"tmdb_id":charfield(&episode["tmdb_id"])? ,"imdb_id":charfield(&episode["imdb_id"])? ,"movie_image":artwork::single(origin,"episode",id,"movie_image",account_id,&art.image),"container_extension":charfield(&relation["container_extension"])? ,"type":"episode","series":{"id":series["id"],"name":charfield(&series["name"])?}}),
    )
}
