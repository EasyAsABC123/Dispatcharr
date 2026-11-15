//! SD airing/metadata -> Source Programme fields; no network/SQL side effects.
//! Original Dispatcharr authors retain all projection and selection credit.
use chrono::{DateTime, Datelike, NaiveDateTime, Utc};
use serde_json::{json, Map, Value};
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Error {
    Shape,
    Capacity,
}
pub(super) struct Programme {
    pub epg: i64,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub title: String,
    pub subtitle: Option<String>,
    pub description: Option<String>,
    pub station: String,
    pub program: String,
    pub properties: Option<Value>,
}
impl Programme {
    pub fn value(&self) -> Value {
        json!({"epg_id":self.epg,"start_time":self.start,"end_time":self.end,"title":self.title,"sub_title":self.subtitle,"description":self.description,"tvg_id":self.station,"program_id":self.program,"custom_properties":self.properties})
    }
}
fn truth(v: &Value) -> bool {
    crate::xc_live_provider::truthy(v)
}
fn text(v: Option<&Value>) -> Result<&str, Error> {
    match v {
        None | Some(Value::Null) => Ok(""),
        Some(Value::String(v)) => Ok(v),
        _ => Err(Error::Shape),
    }
}
fn array(v: Option<&Value>) -> Result<&[Value], Error> {
    match v {
        None => Ok(&[]),
        Some(Value::Array(v)) => Ok(v),
        _ => Err(Error::Shape),
    }
}
fn truthy_array(value: Option<&Value>) -> Result<&[Value], Error> {
    array(value.filter(|v| truth(v)))
}
pub(super) fn integer_string(value: &Value) -> Result<String, Error> {
    let raw = match value {
        Value::Bool(value) => i64::from(*value).to_string(),
        Value::Number(value) => {
            let raw = value.to_string();
            if raw.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
                let value = value
                    .as_f64()
                    .filter(|v| v.is_finite())
                    .ok_or(Error::Shape)?;
                format!("{:.0}", value.trunc())
            } else {
                raw
            }
        }
        Value::String(value) => {
            dispatcharr_ingest_core::source_integer_literal(value).ok_or(Error::Shape)?
        }
        _ => return Err(Error::Shape),
    };
    let integer = raw
        .parse::<num_bigint::BigInt>()
        .map_err(|_| Error::Shape)?;
    Ok(integer.to_string())
}
fn integer(value: &Value) -> Result<i64, Error> {
    integer_string(value)?.parse().map_err(|_| Error::Capacity)
}
fn integer_json(value: &Value) -> Result<Value, Error> {
    Ok(Value::Number(
        integer_string(value)?.parse().map_err(|_| Error::Shape)?,
    ))
}
fn spelling(v: &Value) -> Result<String, Error> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => {
            if n.is_f64() {
                Ok(crate::json_compat::float_string(
                    n.as_f64().filter(|v| v.is_finite()).ok_or(Error::Shape)?,
                ))
            } else {
                Ok(n.to_string())
            }
        }
        Value::Bool(v) => Ok(if *v { "True" } else { "False" }.into()),
        Value::Null => Ok("None".into()),
        _ => Err(Error::Shape),
    }
}
fn field<'a>(value: &'a Value, key: &str) -> Result<Option<&'a Value>, Error> {
    match value {
        Value::Object(o) => Ok(o.get(key)),
        _ => Err(Error::Shape),
    }
}
fn actor(person: &Value, guest: bool) -> Result<Value, Error> {
    let mut actor = Map::new();
    actor.insert("name".into(), json!(text(field(person, "name")?)?));
    if let Some(value) = field(person, "characterName")?.filter(|v| truth(v)) {
        actor.insert("role".into(), value.clone());
    }
    if guest {
        actor.insert("guest".into(), json!(true));
    }
    Ok(Value::Object(actor))
}
pub(super) fn project(
    epg: i64,
    station: &str,
    airing: &Value,
    metadata: Option<&Value>,
    cached: Option<&Value>,
    failed: bool,
    country: Option<&str>,
) -> Result<Option<Programme>, Error> {
    let Some(program) = field(airing, "programID")?.filter(|v| truth(v)) else {
        return Ok(None);
    };
    let program = text(Some(program))?;
    let Some(air) = field(airing, "airDateTime")?.filter(|v| truth(v)) else {
        return Ok(None);
    };
    let Some(duration) = field(airing, "duration")?.filter(|v| truth(v)) else {
        return Ok(None);
    };
    // Only parsing the airing window is Source's per-airing caught exception.
    // Metadata conversion failures propagate to the enclosing real guide task.
    let (start, end) = match (|| {
        let start = NaiveDateTime::parse_from_str(text(Some(air))?, "%Y-%m-%dT%H:%M:%SZ")
            .map_err(|_| Error::Shape)?
            .and_utc();
        let end = start
            .checked_add_signed(chrono::Duration::seconds(integer(duration)?))
            .ok_or(Error::Capacity)?;
        if !(1..=9999).contains(&start.year()) || !(1..=9999).contains(&end.year()) {
            return Err(Error::Shape);
        }
        Ok::<_, Error>((start, end))
    })() {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let empty = json!({});
    let meta = metadata.filter(|v| truth(v)).unwrap_or(&empty);
    let cached = if truth(meta) {
        None
    } else {
        cached.filter(|v| truth(v))
    };
    if !truth(meta) && cached.is_none() && failed {
        return Ok(None);
    }
    let (title, description, subtitle, cp) = if let Some(cache) = cached {
        (
            match text(field(cache, "title")?)? {
                "" => "No Title",
                v => v,
            }
            .to_owned(),
            text(field(cache, "description")?)?.to_owned(),
            text(field(cache, "sub_title")?)?.to_owned(),
            field(cache, "custom_properties")?
                .filter(|v| truth(v))
                .cloned()
                .unwrap_or_else(|| json!({})),
        )
    } else {
        let first_title = truthy_array(field(meta, "titles")?)?.first();
        let mut title = match first_title {
            Some(v) => text(field(v, "title120")?)?,
            None => "",
        };
        if title.is_empty() {
            title = text(field(meta, "episodeTitle150")?)?;
        }
        if title.is_empty() {
            title = "No Title";
        }
        let mut description = "";
        if let Some(descriptions) = field(meta, "descriptions")? {
            for key in ["description1000", "description255", "description100"] {
                if let Some(candidate) = truthy_array(field(descriptions, key)?)?.first() {
                    description = text(field(candidate, "description")?)?;
                    if !description.is_empty() {
                        break;
                    }
                }
            }
        }
        let subtitle = text(field(meta, "episodeTitle150")?)?;
        let mut cp = Map::new();
        let blocks = array(field(meta, "metadata")?)?;
        let mut episode = None;
        for provider in ["Gracenote", "TVmaze"] {
            for block in blocks {
                if let Some(v) = field(block, provider)? {
                    episode = Some(v);
                    break;
                }
            }
            if episode.is_some_and(truth) {
                break;
            }
        }
        if let Some(episode) = episode.filter(|v| truth(v)) {
            let season = field(episode, "season")?.filter(|v| truth(v));
            let number = field(episode, "episode")?.filter(|v| truth(v));
            if let Some(v) = season {
                cp.insert("season".into(), integer_json(v)?);
            }
            if let Some(v) = number {
                cp.insert("episode".into(), integer_json(v)?);
            }
            if let (Some(s), Some(e)) = (season, number) {
                cp.insert(
                    "onscreen_episode".into(),
                    json!(format!("S{} E{}", integer_string(s)?, integer_string(e)?)),
                );
            }
        }
        let ratings = truthy_array(field(meta, "contentRating")?)?;
        if !ratings.is_empty() {
            if ratings.iter().any(|rating| !rating.is_object()) {
                return Err(Error::Shape);
            }
            cp.insert("content_ratings".into(), Value::Array(ratings.to_vec()));
            let selected = country
                .and_then(|country| {
                    ratings
                        .iter()
                        .find(|r| r.get("country").and_then(Value::as_str) == Some(country))
                })
                .or_else(|| {
                    ratings
                        .iter()
                        .find(|r| r.get("country").and_then(Value::as_str) == Some("USA"))
                })
                .unwrap_or(&ratings[0]);
            cp.insert(
                "rating".into(),
                field(selected, "code")?.cloned().unwrap_or(json!("")),
            );
            cp.insert(
                "rating_system".into(),
                field(selected, "body")?.cloned().unwrap_or(json!("")),
            );
        }
        if let Some(v) = field(meta, "contentAdvisory")?.filter(|v| truth(v)) {
            cp.insert("content_advisory".into(), v.clone());
        }
        let entity = field(meta, "entityType")?.filter(|v| truth(v));
        let show = field(meta, "showType")?.filter(|v| truth(v));
        let mut categories = Vec::new();
        if let Some(v) = entity {
            categories.push(v.clone())
        }
        if let Some(v) = show.filter(|v| Some(*v) != entity) {
            categories.push(v.clone())
        }
        categories.extend_from_slice(array(field(meta, "genres")?)?);
        if !categories.is_empty() {
            cp.insert("categories".into(), Value::Array(categories));
        }
        let mut sorted = Vec::new();
        for member in truthy_array(field(meta, "cast")?)? {
            if field(member, "name")?.is_some_and(truth) {
                let order = match field(member, "billingOrder")? {
                    None => num_bigint::BigInt::from(999),
                    Some(v) => integer_string(v)?
                        .parse::<num_bigint::BigInt>()
                        .map_err(|_| Error::Shape)?,
                };
                sorted.push((order, member));
            }
        }
        sorted.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut main = Vec::new();
        let mut guests = Vec::new();
        for (_, member) in &sorted {
            if text(field(member, "role")?)?.to_lowercase() == "guest star" {
                guests.push(*member)
            } else {
                main.push(*member)
            }
        }
        let primary = if main.is_empty() {
            sorted.iter().map(|(_, v)| *v).collect::<Vec<_>>()
        } else {
            main
        };
        let mut actors = primary
            .into_iter()
            .take(6)
            .map(|v| actor(v, false))
            .collect::<Result<Vec<_>, _>>()?;
        for guest in guests.into_iter().take(3) {
            actors.push(actor(guest, true)?)
        }
        let mut credits = Map::new();
        if !actors.is_empty() {
            credits.insert("actor".into(), Value::Array(actors));
        }
        for member in truthy_array(field(meta, "crew")?)? {
            let name = text(field(member, "name")?)?;
            if name.is_empty() {
                continue;
            }
            let role = text(field(member, "role")?)?.to_lowercase();
            let key = if role.contains("director") {
                Some("director")
            } else if role.contains("writer") {
                Some("writer")
            } else if role.contains("producer") {
                Some("producer")
            } else {
                None
            };
            if let Some(key) = key {
                credits
                    .entry(key.to_owned())
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .ok_or(Error::Shape)?
                    .push(json!(name));
            }
        }
        if !credits.is_empty() {
            cp.insert("credits".into(), Value::Object(credits));
        }
        if field(airing, "liveTapeDelay")? == Some(&json!("Live")) {
            cp.insert("live".into(), json!(true));
        }
        if field(airing, "new")?.is_some_and(truth) {
            cp.insert("new".into(), json!(true));
        } else {
            cp.insert("previously_shown".into(), json!(true));
        }
        if field(airing, "premiere")?.is_some_and(truth) {
            cp.insert("premiere".into(), json!(true));
        }
        let movie = field(meta, "movie")?.unwrap_or(&empty);
        if let Some(date) = field(meta, "originalAirDate")?.filter(|v| truth(v)) {
            cp.insert("date".into(), date.clone());
            let original = spelling(date)?;
            let original = original.trim();
            if !original.is_empty() {
                cp.insert("previously_shown_details".into(), json!({"start":original}));
            }
        } else if let Some(year) = field(movie, "year")?.filter(|v| truth(v)) {
            cp.insert("date".into(), json!(spelling(year)?));
        }
        let countries = truthy_array(field(meta, "country")?)?;
        if !countries.is_empty() {
            let value = if countries.len() == 1 {
                countries[0].clone()
            } else {
                json!(countries
                    .iter()
                    .map(|v| text(Some(v)))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", "))
            };
            cp.insert("country".into(), value);
        }
        if let Some(duration) = field(meta, "duration")?
            .filter(|v| truth(v))
            .or(field(movie, "duration")?.filter(|v| truth(v)))
        {
            let seconds = integer_string(duration)?
                .parse::<num_bigint::BigInt>()
                .map_err(|_| Error::Shape)?;
            let divisor = num_bigint::BigInt::from(60);
            let mut minutes = &seconds / &divisor;
            if seconds.sign() == num_bigint::Sign::Minus
                && (&seconds % &divisor) != num_bigint::BigInt::from(0)
            {
                minutes -= 1;
            }
            cp.insert(
                "length".into(),
                json!({"value":minutes.to_string(),"units":"minutes"}),
            );
        }
        let mut stars = Vec::new();
        for rating in truthy_array(field(movie, "qualityRating")?)? {
            if let (Some(a), Some(b)) = (
                field(rating, "rating")?.filter(|v| truth(v)),
                field(rating, "maxRating")?.filter(|v| truth(v)),
            ) {
                stars.push(json!({"value":format!("{}/{}",spelling(a)?,spelling(b)?),"system":field(rating,"ratingsBody")?.cloned().unwrap_or(json!(""))}));
            }
        }
        if !stars.is_empty() {
            cp.insert("star_ratings".into(), Value::Array(stars));
        }
        if let Some(event) = field(meta, "eventDetails")?.filter(|v| truth(v)) {
            cp.insert("event_details".into(), event.clone());
        }
        (
            title.to_owned(),
            description.to_owned(),
            subtitle.to_owned(),
            Value::Object(cp),
        )
    };
    if program.chars().count() > 64 || station.chars().count() > 255 {
        return Err(Error::Capacity);
    }
    Ok(Some(Programme {
        epg,
        start,
        end,
        title: title.chars().take(255).collect(),
        subtitle: (!subtitle.is_empty()).then_some(subtitle),
        description: (!description.is_empty()).then_some(description),
        station: station.into(),
        program: program.into(),
        properties: truth(&cp).then_some(cp),
    }))
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn source_guest_duplicate_country_rating_and_negative_runtime_are_preserved() {
        let airing = json!({"programID":"EP1","airDateTime":"2026-11-15T10:00:00Z","duration":60,"new":false});
        let meta = json!({"titles":[{"title120":"Owned"}],"metadata":[{"Other":{}},{"Gracenote":{"season":"2","episode":3}}],"contentRating":[{"country":"USA","code":"US","body":"u"},{"country":"GBR","code":"GB","body":"g"}],"cast":[{"name":"Guest","role":"Guest Star","characterName":"Role","billingOrder":"1"}],"duration":-61,"originalAirDate":" 2020-01-02 ","country":["USA","GBR"]});
        let row = project(1, "2", &airing, Some(&meta), None, false, Some("GBR"))
            .unwrap()
            .unwrap();
        let p = row.properties.unwrap();
        assert_eq!(p["rating"], "GB");
        assert_eq!(p["onscreen_episode"], "S2 E3");
        assert_eq!(p["credits"]["actor"].as_array().unwrap().len(), 2);
        assert_eq!(p["credits"]["actor"][1]["guest"], true);
        assert_eq!(p["length"]["value"], "-2");
        assert_eq!(p["previously_shown_details"]["start"], "2020-01-02");
        assert_eq!(p["date"], " 2020-01-02 ");
    }
    #[test]
    fn failed_uncached_metadata_skips_and_bad_airtime_is_source_caught() {
        let a = json!({"programID":"EP1","airDateTime":"2026-11-15T10:00:00Z","duration":60});
        assert!(project(1, "2", &a, None, None, true, None)
            .unwrap()
            .is_none());
        let a = json!({"programID":"EP1","airDateTime":"bad","duration":60});
        assert!(project(1, "2", &a, None, None, false, None)
            .unwrap()
            .is_none());
    }
}
