//! Isolated/unwired Source episode preparation; SQL execution is a separate
//! ordered phase. No truncation, second runtime, or hidden fixed row clamp.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_actions_account as account,
    vod_api_owned_read::{ReadError, ReadWork},
    vod_api_projection::truthy,
    vod_api_provider_refresh_actor::Clock,
    vod_api_provider_refresh_series_value as series, vod_api_provider_refresh_value as source,
};
use num_bigint::BigInt;
use serde_json::{json, Value};
use std::collections::HashMap;
pub(super) struct FlatEntry<'a> {
    body: &'a Value,
    season: Value,
}
pub(super) struct Flat<'a> {
    pub entries: Vec<FlatEntry<'a>>,
    pub ids: Vec<String>,
}
fn source_error() -> ReadError {
    ReadError::SourceUnhandledFault("Source episode preparation failed")
}
fn error(fault: source::Fault) -> ReadError {
    match fault {
        source::Fault::Type => source_error(),
        source::Fault::Capacity => ReadError::Capacity,
    }
}
fn number(raw: &str) -> Result<Value, ReadError> {
    serde_json::from_str(raw).map_err(|_| ReadError::Capacity)
}
fn index(value: &Value) -> Result<Value, source::Fault> {
    match value {
        Value::Bool(flag) => Ok(u8::from(*flag).into()),
        Value::String(text) => Ok(source::integer(text)
            .and_then(|n| serde_json::from_str(&n.to_string()).ok())
            .unwrap_or_else(|| 0.into())),
        Value::Number(number) => {
            let text = number.to_string();
            // Source catches int(NaN/Infinity) and defaults episode_num to zero.
            if matches!(text.as_str(), "NaN" | "Infinity" | "-Infinity") { return Ok(0.into()); }
            if text.contains(['.', 'e', 'E']) {
                let n = number
                    .as_f64()
                    .filter(|n| n.is_finite())
                    .ok_or(source::Fault::Type)?;
                BigInt::parse_bytes(format!("{:.0}", n.trunc()).as_bytes(), 10)
                    .and_then(|n| serde_json::from_str(&n.to_string()).ok())
                    .ok_or(source::Fault::Capacity)
            } else {
                Ok(value.clone())
            }
        }
        _ => Ok(0.into()),
    }
}
pub(super) fn flatten<'a>(work: &ReadWork, raw: &'a Value) -> Result<Flat<'a>, ReadError> {
    let mut flat = Flat {
        entries: Vec::new(),
        ids: Vec::new(),
    };
    let mut retained = 0usize;
    let seasons: Vec<(String, &Value)> = match raw {
        Value::Object(values) => values.iter().map(|(k, v)| (k.clone(), v)).collect(),
        Value::Array(values) => values
            .iter()
            .enumerate()
            .map(|(i, v)| (i.to_string(), v))
            .collect(),
        _ => return Ok(flat),
    };
    for (season, bodies) in seasons {
        let Some(bodies) = bodies.as_array() else {
            continue;
        };
        for body in bodies.iter().filter(|v| v.is_object()) {
            let season = source::integer(&season).ok_or_else(source_error)?;
            let season = number(&season.to_string())?;
            if body
                .get("episode_num")
                .is_some_and(|v| v.is_array() || v.is_object())
            {
                return Err(source_error());
            }
            let id = source::text(body.get("id").unwrap_or(&Value::Null)).map_err(error)?;
            retained = retained
                .checked_add(id.capacity())
                .and_then(|n| n.checked_add(128))
                .ok_or(ReadError::Capacity)?;
            let next = (flat.entries.len() + 1).next_power_of_two();
            if retained
                .checked_add(
                    next * (std::mem::size_of::<FlatEntry<'_>>() + std::mem::size_of::<String>()),
                )
                .is_none_or(|n| n > work.work_bytes() / 32)
            {
                return Err(ReadError::Capacity);
            }
            if flat.entries.len() == flat.entries.capacity() {
                flat.entries
                    .try_reserve_exact(next - flat.entries.len())
                    .map_err(|_| ReadError::Capacity)?;
                flat.ids
                    .try_reserve_exact(next - flat.ids.len())
                    .map_err(|_| ReadError::Capacity)?;
            }
            flat.entries.push(FlatEntry { body, season });
            flat.ids.push(id);
        }
    }
    Ok(flat)
}
pub(super) struct Episode {
    pub model: Value,
    pub existing: bool,
    pub date_fault: bool,
    pub charge: usize,
}
pub(super) struct Relation {
    pub model: Value,
    pub existing: bool,
    pub episode: usize,
    pub charge: usize,
}
pub(super) struct Batch {
    pub count: usize,
    pub ids: Vec<String>,
    pub episodes: Vec<Episode>,
    pub relations: Vec<Relation>,
    pub episode_updates: Vec<usize>,
    pub relation_updates: Vec<usize>,
    pub relation_creates: Vec<usize>,
}
fn key(season: &Value, episode: &Value) -> Option<(String, String)> {
    Some((season.as_i64()?.to_string(), episode.as_i64()?.to_string()))
}
fn charged(value: &Value) -> Result<usize, ReadError> {
    account::heap(value)?
        .checked_add(512)
        .ok_or(ReadError::Capacity)
}
type EpisodeMap = HashMap<(String, String), Value>;
type RelationMap = HashMap<String, Value>;
fn raw_map(
    rows: Vec<String>,
    work: &ReadWork,
    episodes: bool,
) -> Result<(EpisodeMap, RelationMap), ReadError> {
    let mut episode_map = HashMap::new();
    let mut relation_map = HashMap::new();
    let mut bytes = 0usize;
    for raw in rows {
        let value = super::vod_api_provider_refresh_http::admitted_value(
            raw.as_bytes(),
            (work.work_bytes() / 16).saturating_sub(bytes),
        )
        .map_err(|error| match error {
            super::vod_api_provider_refresh_http::Fault::Capacity => ReadError::Capacity,
            _ => source_error(),
        })?;
        bytes = bytes
            .checked_add(charged(&value)?)
            .ok_or(ReadError::Capacity)?;
        if bytes > work.work_bytes() / 16 {
            return Err(ReadError::Capacity);
        }
        if episodes {
            if let Some(key) = key(&value["season_number"], &value["episode_number"]) {
                episode_map.insert(key, value);
            }
        } else {
            relation_map.insert(
                value["stream_id"]
                    .as_str()
                    .ok_or_else(source_error)?
                    .to_owned(),
                value,
            );
        }
    }
    Ok((episode_map, relation_map))
}
fn charge_change(
    total: &mut usize,
    old: usize,
    new: usize,
    maximum: usize,
) -> Result<(), ReadError> {
    *total = total
        .checked_sub(old)
        .and_then(|n| n.checked_add(new))
        .filter(|n| *n <= maximum)
        .ok_or(ReadError::Capacity)?;
    Ok(())
}
/// A DateField overflow is deferred to its actual create/update SQL group,
/// matching Django's later field-preparation failure and sequence consumption.
fn metadata(body: &Value) -> Result<(Value, Option<i64>, bool), source::Fault> {
    let blank = json!({});
    let info = body.get("info").unwrap_or(&blank);
    let info = if truthy(info) {
        info.as_object().ok_or(source::Fault::Type)?
    } else {
        blank.as_object().unwrap()
    };
    let description = if info.get("plot").is_some_and(truthy) {
        info["plot"].clone()
    } else {
        info.get("overview").cloned().unwrap_or_else(|| "".into())
    };
    let data: Value = info.clone().into();
    let date = series::date_field(&data);
    let date_fault = date.is_err();
    let mut props = serde_json::Map::new();
    if let Some(crew) = info.get("crew").filter(|v| truthy(v)) {
        props.insert("crew".into(), crew.clone());
    }
    if let Some(image) = source::string(info.get("movie_image").unwrap_or(&Value::Null))? {
        props.insert("movie_image".into(), image.into());
    }
    if let Some(image) = source::string(info.get("backdrop_path").unwrap_or(&Value::Null))? {
        props.insert("backdrop_path".into(), Value::Array(vec![image.into()]));
    }
    Ok((
        Value::Object(serde_json::Map::from_iter([
            ("name".into(), body.get("title").cloned().unwrap_or_else(||"Unknown Episode".into())),
            ("description".into(), description),
            ("rating".into(), source::rating(info.get("rating").unwrap_or(&Value::Null))?.into()),
            ("air_date".into(), date.unwrap_or(Value::Null)),
            ("tmdb_id".into(), info.get("tmdb_id").cloned().unwrap_or(Value::Null)),
            ("imdb_id".into(), info.get("imdb_id").cloned().unwrap_or(Value::Null)),
            ("custom_properties".into(), if props.is_empty(){Value::Null}else{props.into()}),
        ])),
        source::duration_seconds(info.get("duration_secs").unwrap_or(&Value::Null)),
        date_fault,
    ))
}
pub(super) struct Context {
    pub series_id: i64,
    pub account_id: i64,
    pub series_relation: Option<i64>,
    pub clock: Clock,
}
pub(super) fn prepare(
    work: &ReadWork,
    flat: Flat<'_>,
    context: Context,
    old_episodes: Vec<String>,
    old_relations: Vec<String>,
) -> Result<Batch, ReadError> {
    let Context {
        series_id,
        account_id,
        series_relation,
        clock,
    } = context;
    let (mut existing, _) = raw_map(old_episodes, work, true)?;
    let (_, mut existing_relations) = raw_map(old_relations, work, false)?;
    let mut states: HashMap<(String, String), usize> = HashMap::new();
    let mut relation_states: HashMap<String, usize> = HashMap::new();
    let mut retained = 0usize;
    let mut batch = Batch {
        count: flat.entries.len(),
        ids: flat.ids,
        episodes: Vec::new(),
        relations: Vec::new(),
        episode_updates: Vec::new(),
        relation_updates: Vec::new(),
        relation_creates: Vec::new(),
    };
    for entry in flat.entries {
        // Bound one row before metadata/date conversions can copy its strings.
        // This is an actual byte/node bound, never a truncated episode count.
        if account::heap(entry.body)?>work.work_bytes()/32{return Err(ReadError::Capacity);}
        let prepared = (|| {
            let number = index(entry.body.get("episode_num").unwrap_or(&json!(0)))?;
            let metadata = metadata(entry.body)?;
            Ok::<_, source::Fault>((number, metadata))
        })();
        let (number, (fields, duration, date_fault)) = match prepared {
            Ok(v) => v,
            Err(source::Fault::Type) => continue,
            Err(other) => return Err(error(other)),
        };
        let pair = (entry.season.to_string(), number.to_string());
        let state = if let Some(index) = states.get(&pair) {
            *index
        } else {
            let old = existing.remove(&pair);
            let is_existing = old.is_some();
            let model=old.unwrap_or_else(||json!({"id":null,"uuid":uuid::Uuid::new_v4().to_string(),"name":null,"description":null,"air_date":null,"rating":null,"duration_secs":null,"series_id":series_id,"season_number":entry.season,"episode_number":number,"tmdb_id":null,"imdb_id":null,"custom_properties":null,"created_at":null,"updated_at":null}));
            let index = batch.episodes.len();
            let charge = charged(&model)?;
            charge_change(&mut retained, 0, charge, work.work_bytes() / 6)?;
            batch.episodes.push(Episode {
                model,
                existing: is_existing,
                date_fault: false,
                charge,
            });
            states.insert(pair, index);
            index
        };
        let episode = &mut batch.episodes[state];
        let mut changed = false;
        for (name, value) in fields.as_object().unwrap() {
            // Existing DateField values are dates; extract_date returns a
            // datetime, so even an equal calendar date counts as changed.
            let date_changed = name == "air_date"
                && (!value.is_null() || date_fault || !episode.model["air_date"].is_null());
            if date_changed || !source::equal(&episode.model[name], value) {
                episode.model[name] = value.clone();
                changed = true;
            }
        }
        episode.date_fault = date_fault;
        if let Some(duration) = duration {
            if !source::equal(&episode.model["duration_secs"], &duration.into()) {
                episode.model["duration_secs"] = duration.into();
                changed = true;
            }
        }
        if changed && episode.existing && !batch.episode_updates.contains(&state) {
            batch.episode_updates.push(state);
        }
        let charge = charged(&episode.model)?;
        charge_change(&mut retained, episode.charge, charge, work.work_bytes() / 6)?;
        episode.charge = charge;
        let stream = source::text(entry.body.get("id").unwrap_or(&Value::Null)).map_err(error)?;
        let existing_index = relation_states.get(&stream).copied();
        let relation = if let Some(index) = existing_index {
            index
        } else {
            let old = existing_relations.remove(&stream);
            let is_existing = old.is_some();
            let model=old.unwrap_or_else(||json!({"id":null,"m3u_account_id":account_id,"episode_id":null,"series_relation_id":series_relation,"stream_id":stream,"container_extension":null,"custom_properties":null,"created_at":null,"updated_at":null,"last_seen":null}));
            let index = batch.relations.len();
            let charge = charged(&model)?;
            charge_change(&mut retained, 0, charge, work.work_bytes() / 6)?;
            batch.relations.push(Relation {
                model,
                existing: is_existing,
                episode: state,
                charge,
            });
            if is_existing {
                relation_states.insert(stream, index);
                batch.relation_updates.push(index);
            } else {
                batch.relation_creates.push(index);
            }
            index
        };
        let relation = &mut batch.relations[relation];
        relation.episode = state;
        relation.model["series_relation_id"] = series_relation.into();
        relation.model["container_extension"] = entry
            .body
            .get("container_extension")
            .cloned()
            .unwrap_or_else(|| "mp4".into());
        let mut info = entry.body.clone();
        info["_season_number"] = entry.season.clone();
        relation.model["custom_properties"] = Value::Object(serde_json::Map::from_iter([("info".into(),info),("season_number".into(),entry.season)]));
        relation.model["last_seen"] = clock.stamp().into();
        let charge = charged(&relation.model)?;
        charge_change(
            &mut retained,
            relation.charge,
            charge,
            work.work_bytes() / 6,
        )?;
        relation.charge = charge;
    }
    Ok(batch)
}
