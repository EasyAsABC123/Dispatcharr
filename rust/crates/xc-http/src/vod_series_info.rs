//! Cached Series info projection from charged raw SQL rows. Source refresh and
//! route activation are separate; no provider traffic is hidden here. Original
//! Dispatcharr authors retain DTO/application/algorithm credit.
use crate::vod_span::{self, RawValue};
use crate::{
    project::JsonBytes,
    vod_episode_rows::{Episode, Relation, Rows},
    vod_info_row::InfoRow,
    vod_json,
    vod_lists::{self, Error},
    vod_object, vod_rating, vod_scalar, vod_seasons,
};
use chrono::Datelike;
use std::borrow::Cow;

#[derive(Clone, Copy)]
enum Atom<'a> {
    Text(&'a str),
    Raw(&'a RawValue),
}
impl<'a> Atom<'a> {
    fn write(self, out: &mut JsonBytes) -> Result<(), Error> {
        match self {
            Self::Text(s) => out.string(s)?,
            Self::Raw(raw) => vod_json::write(out, raw)?,
        }
        Ok(())
    }
    fn text(self) -> Result<Cow<'a, str>, Error> {
        match self {
            Self::Text(s) => Ok(Cow::Borrowed(s)),
            Self::Raw(raw) => vod_scalar::string(raw),
        }
    }
    // xc_language_suffix searches the name as text when language is set; it
    // does not apply Python str() first. Nonstring names are outside this
    // finite borrowed projection, including the empty-language raw-return
    // branch; do not invent a coerced successful string.
    fn name_text(self) -> Result<Cow<'a, str>, Error> {
        match self {
            Self::Text(s) => Ok(Cow::Borrowed(s)),
            Self::Raw(raw) => vod_json::string(raw)?.ok_or(Error::Unsupported),
        }
    }
    fn truthy(self) -> Result<bool, Error> {
        match self {
            Self::Text(s) => Ok(!s.is_empty()),
            Self::Raw(raw) => Ok(vod_json::truthy(raw)?),
        }
    }
    fn float(self) -> Result<f64, Error> {
        match self {
            Self::Text(s) => {
                let n = dispatcharr_ingest_core::source_float_literal(s).ok_or(Error::Source)?;
                if n.is_finite() {
                    Ok(n)
                } else {
                    Err(Error::Unsupported)
                }
            }
            Self::Raw(raw) => vod_scalar::float(raw),
        }
    }
}
fn literal(s: &'static str) -> &'static RawValue {
    vod_span::parse(s).expect("static JSON")
}
fn field<'a>(
    raw: &'a RawValue,
    key: &str,
    default: &'static RawValue,
) -> Result<&'a RawValue, Error> {
    Ok(vod_object::get(raw, key)?.unwrap_or(default))
}
fn named(out: &mut JsonBytes, name: &str, value: Atom<'_>, first: bool) -> Result<(), Error> {
    if !first {
        out.raw(b", ")?;
    }
    out.string(name)?;
    out.raw(b": ")?;
    value.write(out)
}
const NAME: usize = 0;
const DESCRIPTION: usize = 1;
const GENRE: usize = 2;
const RATING: usize = 3;
const CAST: usize = 4;
const DIRECTOR: usize = 5;
const TRAILER: usize = 6;
const RUN_TIME: usize = 7;
fn metadata<'a>(
    row: &'a InfoRow,
    object: Option<&'a RawValue>,
    relation: Option<&'a RawValue>,
) -> Result<[Atom<'a>; 8], Error> {
    let o = &row.object;
    let empty = literal(r#""""#);
    let mut data = [
        Atom::Text(&o.name),
        Atom::Text(o.description.as_deref().unwrap_or("")),
        Atom::Text(o.genre.as_deref().unwrap_or("")),
        Atom::Text(o.rating.as_deref().filter(|s| !s.is_empty()).unwrap_or("0")),
        Atom::Text(""),
        Atom::Text(""),
        Atom::Text(""),
        Atom::Text(""),
    ];
    let projected = (|| -> Result<(), Error> {
        if let Some(raw) = object.filter(|raw| vod_json::truthy(raw).unwrap_or(true)) {
            let cast = field(raw, "cast", empty)?;
            let director = field(raw, "director", empty)?;
            let trailer = field(raw, "youtube_trailer", empty)?;
            let runtime = field(raw, "episode_run_time", empty)?;
            let _backdrop = field(raw, "backdrop_path", literal("[]"))?;
            for (index, value) in [
                (CAST, cast),
                (DIRECTOR, director),
                (TRAILER, trailer),
                (RUN_TIME, runtime),
            ] {
                data[index] = Atom::Raw(value);
            }
        }
        if let Some(raw) = relation.filter(|raw| vod_json::truthy(raw).unwrap_or(true)) {
            if let Some(details) = vod_object::get(raw, "detailed_info")? {
                for (index, key) in [
                    (Some(NAME), "name"),
                    (Some(DESCRIPTION), "description"),
                    (None, "year"),
                    (Some(GENRE), "genre"),
                    (Some(RATING), "rating"),
                ] {
                    if let Some(value) = vod_object::get(details, key)? {
                        if let Some(index) = index {
                            if vod_json::truthy(value)? {
                                data[index] = Atom::Raw(value);
                            }
                        }
                    }
                }
                if let Some(value) = vod_object::get(details, "plot")?
                    .filter(|r| vod_json::truthy(r).unwrap_or(true))
                {
                    data[DESCRIPTION] = Atom::Raw(value);
                } else if let Some(value) = vod_object::get(details, "description")?
                    .filter(|r| vod_json::truthy(r).unwrap_or(true))
                {
                    data[DESCRIPTION] = Atom::Raw(value);
                }
                // Each default preserves the current typed value, including
                // explicit null; this is dict.get, not Python `or` fallback.
                let mut next = [data[CAST], data[DIRECTOR], data[TRAILER], data[RUN_TIME]];
                for (i, key) in ["cast", "director", "youtube_trailer", "episode_run_time"]
                    .iter()
                    .enumerate()
                {
                    if let Some(value) = vod_object::get(details, key)? {
                        next[i] = Atom::Raw(value);
                    }
                }
                let _backdrop = vod_object::get(details, "backdrop_path")?;
                for (index, value) in [CAST, DIRECTOR, TRAILER, RUN_TIME].into_iter().zip(next) {
                    data[index] = value;
                }
            }
        }
        Ok(())
    })();
    match projected {
        Ok(()) | Err(Error::Source) => {}
        Err(error) => return Err(error),
    }
    Ok(data)
}

struct EpisodeFields<'a> {
    video: &'a RawValue,
    audio: &'a RawValue,
    bitrate: &'a RawValue,
    art: vod_object::Artwork<'a>,
    crew: Cow<'a, str>,
    director: &'a RawValue,
    rating: String,
}
fn ep_fields<'a>(
    episode: &'a Episode,
    relation: Option<&'a Relation>,
) -> Result<EpisodeFields<'a>, Error> {
    let object = episode
        .properties
        .as_deref()
        .map(vod_object::parse)
        .transpose()?;
    let props = relation
        .and_then(|r| r.properties.as_deref())
        .map(vod_object::parse)
        .transpose()?;
    let mut media = [None, None, None];
    if let Some(raw) = props.filter(|r| vod_json::truthy(r).unwrap_or(true)) {
        if let Some(info) = vod_object::get(raw, "info")?
            .filter(|r| vod_json::truthy(r).unwrap_or(true) && r.trim_start().starts_with('{'))
        {
            if let Some(info) = vod_object::get(info, "info")?
                .filter(|r| vod_json::truthy(r).unwrap_or(true) && r.trim_start().starts_with('{'))
            {
                for (i, key) in ["video", "audio", "bitrate"].iter().enumerate() {
                    media[i] = Some(field(
                        info,
                        key,
                        if i == 2 { literal("0") } else { literal("{}") },
                    )?);
                }
            }
        }
    }
    for (i, key) in ["video", "audio", "bitrate"].iter().enumerate() {
        if media[i].is_none_or(|raw| raw.trim() == "null") {
            let default = if i == 2 { literal("0") } else { literal("{}") };
            media[i] = Some(
                match object.filter(|r| vod_json::truthy(r).unwrap_or(true)) {
                    Some(raw) => field(raw, key, default)?,
                    None => default,
                },
            );
        }
    }
    let art = vod_object::prefer(props, object)?;
    let (crew, director) = if let Some(raw) = object.filter(|r| vod_json::truthy(r).unwrap_or(true))
    {
        (
            vod_scalar::string(field(raw, "crew", literal(r#""""#))?)?,
            field(raw, "director", literal(r#""""#))?,
        )
    } else {
        (Cow::Borrowed(""), literal(r#""""#))
    };
    let value = dispatcharr_ingest_core::source_float_literal(
        episode
            .rating
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("0"),
    )
    .ok_or(Error::Source)?;
    let rating = dispatcharr_store::source_finite_float_string(value).ok_or(Error::Unsupported)?;
    Ok(EpisodeFields {
        video: media[0].expect("filled media"),
        audio: media[1].expect("filled media"),
        bitrate: media[2].expect("filled media"),
        art,
        crew,
        director,
        rating,
    })
}
fn backdrop_item(
    out: &mut JsonBytes,
    raw: &RawValue,
    origin: &str,
    resource: &str,
    id: i64,
    index: usize,
) -> Result<(), Error> {
    if let Some(value) = vod_json::string(raw)? {
        if vod_lists::proxyable(&value) {
            out.string(&vod_lists::image_url(
                origin,
                resource,
                id,
                "backdrop",
                Some(index),
                &value,
            ))?;
            return Ok(());
        }
    }
    vod_json::write(out, raw)?;
    Ok(())
}
fn backdrops(
    out: &mut JsonBytes,
    raw: Option<&RawValue>,
    origin: &str,
    resource: &str,
    id: i64,
) -> Result<(), Error> {
    match raw {
        None => out.raw(b"[]")?,
        Some(raw) if raw.trim_start().starts_with('"') => {
            out.raw(b"[")?;
            backdrop_item(out, raw, origin, resource, id, 0)?;
            out.raw(b"]")?;
        }
        Some(raw) => {
            out.raw(b"[")?;
            for (index, value) in vod_span::Array::new(raw)?.enumerate() {
                if index > 0 {
                    out.raw(b", ")?;
                }
                backdrop_item(out, value?, origin, resource, id, index)?;
            }
            out.raw(b"]")?;
        }
    }
    Ok(())
}
fn cover(origin: &str, resource: &str, id: i64, logo: Option<i64>, image: &str) -> Option<String> {
    if vod_lists::proxyable(image) {
        Some(vod_lists::image_url(
            origin,
            resource,
            id,
            "movie_image",
            None,
            image,
        ))
    } else {
        logo.map(|id| format!("{origin}/api/vod/vodlogos/{id}/cache/"))
    }
}
fn episode(
    out: &mut JsonBytes,
    row: &Episode,
    relation: Option<&Relation>,
    origin: &str,
) -> Result<(), Error> {
    let fields = ep_fields(row, relation)?;
    let season = row.season.unwrap_or(1);
    let number = row.number.unwrap_or(0);
    out.raw(
        format!(
            "{{\"id\": {}, \"season\": {season}, \"episode_num\": {number}",
            row.id
        )
        .as_bytes(),
    )?;
    named(out, "title", Atom::Text(&row.name), false)?;
    named(
        out,
        "container_extension",
        Atom::Text(
            relation
                .and_then(|r| r.extension.as_deref())
                .filter(|s| !s.is_empty())
                .unwrap_or("mp4"),
        ),
        false,
    )?;
    let added = vod_lists::epoch(relation.map_or(row.created, |r| r.created))?;
    named(out, "added", Atom::Text(&added), false)?;
    named(out, "custom_sid", Atom::Raw(literal("null")), false)?;
    named(out, "direct_source", Atom::Text(""), false)?;
    out.raw(b", \"info\": {")?;
    out.raw(format!("\"id\": {}", row.id).as_bytes())?;
    named(out, "name", Atom::Text(&row.name), false)?;
    named(
        out,
        "overview",
        Atom::Text(row.description.as_deref().unwrap_or("")),
        false,
    )?;
    named(out, "crew", Atom::Text(&fields.crew), false)?;
    named(out, "directed_by", Atom::Raw(fields.director), false)?;
    named(
        out,
        "imdb_id",
        Atom::Text(row.imdb.as_deref().unwrap_or("")),
        false,
    )?;
    if row
        .air_date
        .is_some_and(|d| !(1..=9999).contains(&d.year()))
    {
        return Err(Error::Unsupported);
    }
    let date = row.air_date.map_or_else(String::new, |d| d.to_string());
    named(out, "air_date", Atom::Text(&date), false)?;
    out.raw(b", \"backdrop_path\": ")?;
    backdrops(out, fields.art.backdrop, origin, "episodes", row.id)?;
    let image = if vod_lists::proxyable(&fields.art.image) {
        Some(vod_lists::image_url(
            origin,
            "episodes",
            row.id,
            "movie_image",
            None,
            &fields.art.image,
        ))
    } else {
        None
    };
    // Source rewrite_single_image_url preserves nonproxy raw image strings;
    // prefer_relation_artwork has already normalized nonstring values to ''.
    named(
        out,
        "movie_image",
        Atom::Text(image.as_deref().unwrap_or(&fields.art.image)),
        false,
    )?;
    out.raw(b", \"rating\": ")?;
    out.raw(fields.rating.as_bytes())?;
    named(out, "release_date", Atom::Text(&date), false)?;
    out.raw(format!(", \"duration_secs\": {}", row.duration.unwrap_or(0)).as_bytes())?;
    let duration = vod_scalar::duration(row.duration);
    named(out, "duration", Atom::Text(&duration), false)?;
    named(out, "video", Atom::Raw(fields.video), false)?;
    named(out, "audio", Atom::Raw(fields.audio), false)?;
    named(out, "bitrate", Atom::Raw(fields.bitrate), false)?;
    out.raw(b"}}")?;
    Ok(())
}
pub(crate) fn write(
    row: &InfoRow,
    rows: &Rows,
    origin: &str,
    available: usize,
    out: &mut JsonBytes,
) -> Result<(), Error> {
    let series_raw = row
        .object
        .heap_bytes()?
        .checked_add(row.relation_properties.as_ref().map_or(0, String::capacity))
        .ok_or_else(crate::reader::capacity)?;
    let mut episode_raw = 0usize;
    for ep in &rows.episodes {
        let n = ep
            .heap_bytes()?
            .checked_add(
                rows.relation(ep.id)
                    .map(Relation::heap_bytes)
                    .transpose()?
                    .unwrap_or(0),
            )
            .ok_or_else(crate::reader::capacity)?;
        episode_raw = episode_raw.max(n);
    }
    let reserved = series_raw
        .checked_add(episode_raw)
        .and_then(|n| n.checked_mul(3))
        .and_then(|n| n.checked_add(row.retained_bytes))
        .and_then(|n| n.checked_add(rows.retained_bytes))
        .and_then(|n| n.checked_add(out.maximum().checked_mul(2)?))
        .and_then(|n| n.checked_add(origin.len().checked_mul(6)?))
        .and_then(|n| n.checked_add(8192))
        .ok_or_else(crate::reader::capacity)?;
    let group_work = available
        .checked_sub(reserved)
        .ok_or_else(crate::reader::capacity)?;
    let groups = vod_seasons::build(&rows.episodes, group_work)?;
    if groups.retained_bytes > group_work {
        return Err(crate::reader::capacity().into());
    }
    // Source materializes every episode's values before Series metadata. Keep
    // that error order without retaining a Vec of decoded DTO/Value trees.
    for ep in &rows.episodes {
        drop(ep_fields(ep, rows.relation(ep.id))?);
    }
    let object = row
        .object
        .properties
        .as_deref()
        .map(vod_object::parse)
        .transpose()?;
    let relation = row
        .relation_properties
        .as_deref()
        .map(vod_object::parse)
        .transpose()?;
    let data = metadata(row, object, relation)?;
    let artwork = vod_object::prefer(relation, object)?;
    let image = cover(
        origin,
        "series",
        row.object.content_id,
        row.object.logo,
        &artwork.image,
    );
    let name = data[NAME].name_text()?;
    let name = vod_lists::display_name(&name, &row.object.language)?;
    let year = row
        .object
        .year
        .filter(|n| *n != 0)
        .map_or_else(String::new, |n| n.to_string());
    let release = match object.filter(|r| vod_json::truthy(r).unwrap_or(true)) {
        Some(raw) => vod_object::get(raw, "release_date")?.map_or(Atom::Text(&year), Atom::Raw),
        None => Atom::Text(&year),
    };
    let rating = data[RATING].text()?;
    let half = if data[RATING].truthy()? {
        vod_rating::half_number(data[RATING].float()?)?
    } else {
        "0".into()
    };
    let runtime = data[RUN_TIME].text()?;
    out.raw(b"{\"seasons\": [")?;
    for (n, &index) in groups.sorted.iter().enumerate() {
        if n > 0 {
            out.raw(b", ")?;
        }
        let season = groups.entries[index].season;
        out.raw(
            format!("{{\"season_number\": {season}, \"name\": \"Season {season}\"}}").as_bytes(),
        )?;
    }
    out.raw(b"], \"info\": {")?;
    named(out, "name", Atom::Text(&name), true)?;
    named(
        out,
        "cover",
        image
            .as_deref()
            .map_or(Atom::Raw(literal("null")), Atom::Text),
        false,
    )?;
    for (key, index) in [
        ("plot", DESCRIPTION),
        ("cast", CAST),
        ("director", DIRECTOR),
        ("genre", GENRE),
    ] {
        named(out, key, data[index], false)?;
    }
    named(out, "release_date", release, false)?;
    named(out, "releaseDate", release, false)?;
    let added = vod_lists::epoch(row.relation_created)?;
    let modified = vod_lists::epoch(row.relation_modified)?;
    named(out, "added", Atom::Text(&added), false)?;
    named(out, "last_modified", Atom::Text(&modified), false)?;
    named(out, "rating", Atom::Text(&rating), false)?;
    named(out, "rating_5based", Atom::Text(&half), false)?;
    out.raw(b", \"backdrop_path\": ")?;
    backdrops(
        out,
        artwork.backdrop,
        origin,
        "series",
        row.object.content_id,
    )?;
    named(out, "youtube_trailer", data[TRAILER], false)?;
    named(
        out,
        "imdb",
        Atom::Text(row.object.imdb.as_deref().unwrap_or("")),
        false,
    )?;
    named(
        out,
        "tmdb",
        Atom::Text(row.object.tmdb.as_deref().unwrap_or("")),
        false,
    )?;
    named(out, "episode_run_time", Atom::Text(&runtime), false)?;
    let category = row.object.category.unwrap_or(0).to_string();
    named(out, "category_id", Atom::Text(&category), false)?;
    out.raw(b", \"category_ids\": [")?;
    if let Some(id) = row.object.category {
        out.raw(id.to_string().as_bytes())?;
    }
    out.raw(b"]}, \"episodes\": {")?;
    for (n, group) in groups.entries.iter().enumerate() {
        if n > 0 {
            out.raw(b", ")?;
        }
        out.string(&group.season.to_string())?;
        out.raw(b": [")?;
        let mut first = true;
        for index in group
            .first
            .clone()
            .chain(group.trailing_nulls.clone().unwrap_or(0..0))
        {
            if !first {
                out.raw(b", ")?;
            }
            first = false;
            let ep = &rows.episodes[index];
            episode(out, ep, rows.relation(ep.id), origin)?;
        }
        out.raw(b"]")?;
    }
    out.raw(b"}}")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vod_priority::ListRow;
    use chrono::{DateTime, Utc};
    fn at(raw: &serde_json::Value) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(&raw.as_str().unwrap().replace(' ', "T"))
            .unwrap()
            .with_timezone(&Utc)
    }
    fn text(row: &serde_json::Value, key: &str) -> Option<String> {
        row[key].as_str().map(str::to_owned)
    }
    fn fixture() -> (InfoRow, Rows, serde_json::Value) {
        let data: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/Source-series-info-golden.json"))
                .unwrap();
        let o = &data["series"];
        let rel = &data["relation"];
        let object = ListRow {
            content_id: o["id"].as_i64().unwrap(),
            category: rel["category_id"].as_i64(),
            extension: None,
            name: text(o, "name").unwrap(),
            language: text(o, "language").unwrap(),
            rating: text(o, "rating"),
            description: text(o, "description"),
            genre: text(o, "genre"),
            year: o["year"].as_i64().map(|n| n as i32),
            tmdb: text(o, "tmdb_id"),
            imdb: text(o, "imdb_id"),
            adult: false,
            created: at(&o["created_at"]),
            modified: at(&rel["updated_at"]),
            logo: o["logo_id"].as_i64(),
            properties: Some(o["custom_properties"].to_string()),
            image: String::new(),
            backdrop: None,
        };
        let relation_properties = Some(rel["custom_properties"].to_string());
        let retained = object.heap_bytes().unwrap()
            + relation_properties.as_ref().map_or(0, String::capacity)
            + std::mem::size_of::<InfoRow>();
        let info = InfoRow {
            relation_id: rel["id"].as_i64().unwrap(),
            account_id: rel["m3u_account_id"].as_i64().unwrap(),
            object,
            relation_created: at(&rel["created_at"]),
            relation_modified: at(&rel["updated_at"]),
            relation_properties,
            last_refresh: Some(at(&rel["last_episode_refresh"])),
            retained_bytes: retained,
        };
        let active = |id: i64| {
            data["accounts"]
                .as_array()
                .unwrap()
                .iter()
                .find(|a| a["id"].as_i64() == Some(id))
                .unwrap()["is_active"]
                .as_bool()
                .unwrap()
        };
        let priority = |id: i64| {
            data["accounts"]
                .as_array()
                .unwrap()
                .iter()
                .find(|a| a["id"].as_i64() == Some(id))
                .unwrap()["priority"]
                .as_i64()
                .unwrap()
        };
        let mut episodes = Vec::new();
        for row in data["episodes"].as_array().unwrap() {
            if !data["episode_relations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| {
                    r["episode_id"] == row["id"] && active(r["m3u_account_id"].as_i64().unwrap())
                })
            {
                continue;
            }
            episodes.push(Episode {
                id: row["id"].as_i64().unwrap(),
                name: text(row, "name").unwrap(),
                description: text(row, "description"),
                rating: text(row, "rating"),
                imdb: text(row, "imdb_id"),
                air_date: row["air_date"].as_str().map(|s| s.parse().unwrap()),
                duration: row["duration_secs"].as_i64().map(|n| n as i32),
                season: row["season_number"].as_i64().map(|n| n as i32),
                number: row["episode_number"].as_i64().map(|n| n as i32),
                properties: Some(row["custom_properties"].to_string()),
                created: at(&row["created_at"]),
            });
        }
        episodes.sort_by_key(|e| (e.season.unwrap_or(i32::MAX), e.number.unwrap_or(i32::MAX)));
        let mut raw = data["episode_relations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| active(r["m3u_account_id"].as_i64().unwrap()))
            .collect::<Vec<_>>();
        raw.sort_by_key(|r| {
            (
                r["episode_id"].as_i64().unwrap(),
                -priority(r["m3u_account_id"].as_i64().unwrap()),
                r["id"].as_i64().unwrap(),
            )
        });
        let mut relations = Vec::<Relation>::new();
        for row in raw {
            let id = row["episode_id"].as_i64().unwrap();
            if relations.last().is_some_and(|r| r.episode == id) {
                continue;
            }
            relations.push(Relation {
                episode: id,
                extension: text(row, "container_extension"),
                properties: Some(row["custom_properties"].to_string()),
                created: at(&row["created_at"]),
            });
        }
        let retained_bytes = episodes
            .iter()
            .map(|e| e.heap_bytes().unwrap())
            .sum::<usize>()
            + episodes.capacity() * std::mem::size_of::<Episode>()
            + relations
                .iter()
                .map(|r| r.heap_bytes().unwrap())
                .sum::<usize>()
            + relations.capacity() * std::mem::size_of::<Relation>();
        (
            info,
            Rows {
                episodes,
                relations,
                retained_bytes,
            },
            data,
        )
    }
    #[test]
    fn retained_source_series_and_two_episode_dto_bytes_match() {
        let (row, rows, source) = fixture();
        assert_eq!(rows.episodes.len(), 2);
        assert_eq!(rows.episodes[0].season, Some(0));
        let mut out = JsonBytes::new(1024 * 1024).unwrap();
        write(
            &row,
            &rows,
            "http://127.0.0.1:5657",
            8 * 1024 * 1024,
            &mut out,
        )
        .unwrap();
        let expected = source["expected_body_hex"].as_str().unwrap();
        let expected = (0..expected.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&expected[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            out.finish(),
            expected,
            "retained Source3 DTO bytes; no Source call or SQL execution"
        );
    }
    #[test]
    fn null_media_falls_back_but_empty_object_does_not() {
        let (_, mut rows, _) = fixture();
        rows.relations[0].properties =
            Some(r#"{"info":{"info":{"video":null,"audio":{},"bitrate":null}}}"#.into());
        let fields = ep_fields(&rows.episodes[0], rows.relation(rows.episodes[0].id)).unwrap();
        assert_eq!(fields.video, r#"{"codec_name":"fallback"}"#);
        assert_eq!(fields.audio, "{}");
        assert_eq!(fields.bitrate, "0");
    }
    #[test]
    fn projection_checks_existing_quote_before_allocating_groups() {
        let (row, rows, _) = fixture();
        let mut out = JsonBytes::new(1024 * 1024).unwrap();
        assert!(matches!(
            write(&row, &rows, "http://fixture.invalid", 128, &mut out),
            Err(Error::Store(dispatcharr_store::StoreError::StatusBody(
                503,
                _
            )))
        ));
        assert!(out.finish().is_empty());
    }
    #[test]
    fn detailed_name_is_not_silently_stringified() {
        assert!(matches!(
            Atom::Raw(literal("42")).name_text(),
            Err(Error::Unsupported)
        ));
        assert!(matches!(
            Atom::Raw(literal("true")).name_text(),
            Err(Error::Unsupported)
        ));
        assert_eq!(
            Atom::Raw(literal(r#""Title""#)).name_text().unwrap(),
            "Title"
        );
    }
    #[test]
    fn zero_year_uses_source_falsey_fallback_without_changing_explicit_release_null() {
        let (mut row, rows, _) = fixture();
        row.object.year = Some(0);
        row.object.properties = Some("{}".into());
        let mut out = JsonBytes::new(1024 * 1024).unwrap();
        write(
            &row,
            &rows,
            "http://fixture.invalid",
            8 * 1024 * 1024,
            &mut out,
        )
        .unwrap();
        let body = String::from_utf8(out.finish()).unwrap();
        assert!(body.contains(r#""release_date": "", "releaseDate": """#));
        row.object.properties = Some(r#"{"release_date":null}"#.into());
        let mut out = JsonBytes::new(1024 * 1024).unwrap();
        write(
            &row,
            &rows,
            "http://fixture.invalid",
            8 * 1024 * 1024,
            &mut out,
        )
        .unwrap();
        assert!(String::from_utf8(out.finish())
            .unwrap()
            .contains(r#""release_date": null, "releaseDate": null"#));
    }
}
