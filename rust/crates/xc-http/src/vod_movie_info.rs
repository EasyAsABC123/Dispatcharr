//! Cached Movie info projection. No refresh, SQL or route activation here.
//! Original Dispatcharr authors retain DTO/application/algorithm credit.
use crate::vod_span::{self, RawValue};
use crate::{
    project::JsonBytes,
    vod_info_row::InfoRow,
    vod_json,
    vod_lists::{self, Error},
    vod_object,
};
use std::borrow::Cow;

#[derive(Clone, Copy)]
enum Atom<'a> {
    Text(&'a str),
    Integer(i64),
    Raw(&'a RawValue),
}
impl Atom<'_> {
    fn write(self, out: &mut JsonBytes) -> Result<(), Error> {
        match self {
            Self::Text(s) => out.string(s)?,
            Self::Integer(n) => out.raw(n.to_string().as_bytes())?,
            Self::Raw(raw) => vod_json::write(out, raw)?,
        }
        Ok(())
    }
    fn string(self) -> Result<Cow<'static, str>, Error> {
        // A temporary owned name is explicitly covered by the input scratch
        // quote. Avoid self-referential DTO state when RawValue has escapes.
        match self {
            Self::Text(value) => Ok(Cow::Owned(value.to_owned())),
            Self::Raw(value) => vod_json::string(value)?
                .map(|v| Cow::Owned(v.into_owned()))
                .ok_or(Error::Source),
            Self::Integer(_) => Err(Error::Source),
        }
    }
}
fn literal(text: &'static str) -> &'static RawValue {
    vod_span::parse(text).expect("static JSON")
}
fn field<'a>(
    map: Option<&'a RawValue>,
    key: &str,
    default: &'static RawValue,
) -> Result<&'a RawValue, Error> {
    match map {
        Some(map) => Ok(vod_object::get(map, key)?.unwrap_or(default)),
        None => Ok(default),
    }
}
fn or<'a>(
    left: &'a RawValue,
    right: impl FnOnce() -> Result<&'a RawValue, Error>,
) -> Result<&'a RawValue, Error> {
    if vod_json::truthy(left)? {
        Ok(left)
    } else {
        right()
    }
}
const NAME: usize = 0;
const DESCRIPTION: usize = 1;
const YEAR: usize = 2;
const GENRE: usize = 3;
const RATING: usize = 4;
const TMDB: usize = 5;
const IMDB: usize = 6;
const DIRECTOR: usize = 7;
const ACTORS: usize = 8;
const COUNTRY: usize = 9;
const RELEASE: usize = 10;
const TRAILER: usize = 11;
const BITRATE: usize = 12;
const VIDEO: usize = 13;
const AUDIO: usize = 14;

fn metadata<'a>(
    row: &'a InfoRow,
    object: Option<&'a RawValue>,
    relation: Option<&'a RawValue>,
) -> Result<[Atom<'a>; 15], Error> {
    let o = &row.object;
    let mut data = [
        Atom::Text(&o.name),
        Atom::Text(o.description.as_deref().unwrap_or("")),
        o.year
            .map_or(Atom::Raw(literal("null")), |n| Atom::Integer(i64::from(n))),
        Atom::Text(o.genre.as_deref().unwrap_or("")),
        o.rating
            .as_deref()
            .filter(|s| !s.is_empty())
            .map_or(Atom::Integer(0), Atom::Text),
        Atom::Text(o.tmdb.as_deref().unwrap_or("")),
        Atom::Text(o.imdb.as_deref().unwrap_or("")),
        Atom::Text(""),
        Atom::Text(""),
        Atom::Text(""),
        Atom::Text(""),
        Atom::Text(""),
        Atom::Integer(0),
        Atom::Raw(literal("{}")),
        Atom::Raw(literal("{}")),
    ];
    let detailed = (|| -> Result<(), Error> {
        let Some(object) = object.filter(|raw| vod_json::truthy(raw).unwrap_or(true)) else {
            return Ok(());
        };
        // Source only enters its detail-projection block when OBJECT custom
        // properties are truthy. It does not use relation data alone here.
        let rel = relation.ok_or(Error::Source)?;
        let detailed_map = vod_object::get(rel, "detailed_info")?.unwrap_or(literal("{}"));
        let custom = Some(object);
        let details = Some(detailed_map);
        let empty = literal(r#""""#);
        let director = or(field(custom, "director", empty)?, || {
            field(details, "director", empty)
        })?;
        let actors = or(field(custom, "actors", empty)?, || {
            field(details, "actors", empty)
        })?;
        let country = or(field(custom, "country", empty)?, || {
            field(details, "country", empty)
        })?;
        let release = or(field(custom, "release_date", empty)?, || {
            or(field(details, "release_date", literal("null"))?, || {
                field(details, "releasedate", empty)
            })
        })?;
        let trailer = or(field(custom, "youtube_trailer", empty)?, || {
            or(field(details, "youtube_trailer", literal("null"))?, || {
                field(details, "trailer", empty)
            })
        })?;
        // These unused movie_data entries still evaluate before the bulk update
        // in Source, so preserve mapping/error precedence without retaining them.
        let _backdrop = or(field(custom, "backdrop_path", literal("null"))?, || {
            field(details, "backdrop_path", literal("[]"))
        })?;
        let _cover = field(details, "cover_big", empty)?;
        let bitrate = field(details, "bitrate", literal("0"))?;
        let video = field(details, "video", literal("{}"))?;
        let audio = field(details, "audio", literal("{}"))?;
        // Dict-literal values finish before Source movie_data.update executes.
        for (index, value) in [
            (DIRECTOR, director),
            (ACTORS, actors),
            (COUNTRY, country),
            (RELEASE, release),
            (TRAILER, trailer),
            (BITRATE, bitrate),
            (VIDEO, video),
            (AUDIO, audio),
        ] {
            data[index] = Atom::Raw(value);
        }
        for (index, key) in [
            (NAME, "name"),
            (DESCRIPTION, "description"),
            (YEAR, "year"),
            (GENRE, "genre"),
            (RATING, "rating"),
            (TMDB, "tmdb_id"),
            (IMDB, "imdb_id"),
        ] {
            if let Some(raw) = vod_object::get(detailed_map, key)? {
                if vod_json::truthy(raw)? {
                    data[index] = Atom::Raw(raw);
                }
            }
        }
        if let Some(raw) = vod_object::get(detailed_map, "plot")?
            .filter(|raw| vod_json::truthy(raw).unwrap_or(true))
        {
            data[DESCRIPTION] = Atom::Raw(raw);
        } else if let Some(raw) = vod_object::get(detailed_map, "description")?
            .filter(|raw| vod_json::truthy(raw).unwrap_or(true))
        {
            data[DESCRIPTION] = Atom::Raw(raw);
        }
        Ok(())
    })();
    match detailed {
        // Source catches property-shape exceptions around this whole block.
        Err(Error::Source) => {}
        Err(error) => return Err(error),
        Ok(()) => {}
    }
    Ok(data)
}
fn named(out: &mut JsonBytes, name: &str, value: Atom<'_>, first: bool) -> Result<(), Error> {
    if !first {
        out.raw(b", ")?;
    }
    out.string(name)?;
    out.raw(b": ")?;
    value.write(out)
}
fn backdrop_item(
    out: &mut JsonBytes,
    raw: &RawValue,
    origin: &str,
    id: i64,
    index: usize,
) -> Result<(), Error> {
    if let Some(value) = vod_json::string(raw)? {
        if vod_lists::proxyable(&value) {
            out.string(&vod_lists::image_url(
                origin,
                "movies",
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
pub(crate) fn write(
    row: &InfoRow,
    origin: &str,
    available_work: usize,
    out: &mut JsonBytes,
) -> Result<(), Error> {
    let raw = row
        .object
        .heap_bytes()?
        .checked_add(row.relation_properties.as_ref().map_or(0, String::capacity))
        .ok_or_else(crate::reader::capacity)?;
    let peak = raw
        .checked_mul(3)
        .and_then(|n| n.checked_add(row.retained_bytes))
        .and_then(|n| n.checked_add(out.maximum().checked_mul(2)?))
        .and_then(|n| n.checked_add(origin.len().checked_mul(6)?))
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(crate::reader::capacity)?;
    if peak > available_work {
        return Err(crate::reader::capacity().into());
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
    let cover = if vod_lists::proxyable(&artwork.image) {
        Some(vod_lists::image_url(
            origin,
            "movies",
            row.object.content_id,
            "movie_image",
            None,
            &artwork.image,
        ))
    } else {
        row.object
            .logo
            .map(|id| format!("{origin}/api/vod/vodlogos/{id}/cache/"))
    };
    let name = data[NAME].string()?;
    let name = vod_lists::display_name(&name, &row.object.language)?;
    out.raw(b"{\"info\": {")?;
    named(out, "name", Atom::Text(&name), true)?;
    named(out, "o_name", Atom::Text(&name), false)?;
    let cover_atom = cover
        .as_deref()
        .map_or(Atom::Raw(literal("null")), Atom::Text);
    named(out, "cover_big", cover_atom, false)?;
    named(out, "movie_image", cover_atom, false)?;
    for (key, index) in [
        ("description", DESCRIPTION),
        ("plot", DESCRIPTION),
        ("year", YEAR),
        ("release_date", RELEASE),
        ("genre", GENRE),
        ("director", DIRECTOR),
        ("actors", ACTORS),
        ("cast", ACTORS),
        ("country", COUNTRY),
        ("rating", RATING),
        ("imdb_id", IMDB),
        ("tmdb_id", TMDB),
        ("youtube_trailer", TRAILER),
    ] {
        named(out, key, data[index], false)?;
    }
    out.raw(b", \"backdrop_path\": ")?;
    match artwork.backdrop {
        None => out.raw(b"[]")?,
        Some(raw) if raw.trim_start().starts_with('"') => {
            out.raw(b"[")?;
            backdrop_item(out, raw, origin, row.object.content_id, 0)?;
            out.raw(b"]")?;
        }
        Some(raw) => {
            out.raw(b"[")?;
            for (index, value) in vod_span::Array::new(raw)?.enumerate() {
                if index > 0 {
                    out.raw(b", ")?;
                }
                backdrop_item(out, value?, origin, row.object.content_id, index)?;
            }
            out.raw(b"]")?;
        }
    }
    named(out, "cover", cover_atom, false)?;
    for (key, index) in [("bitrate", BITRATE), ("video", VIDEO), ("audio", AUDIO)] {
        named(out, key, data[index], false)?;
    }
    out.raw(b"}, \"movie_data\": {")?;
    named(out, "stream_id", Atom::Integer(row.object.content_id), true)?;
    let original_name = vod_lists::display_name(&row.object.name, &row.object.language)?;
    named(out, "name", Atom::Text(&original_name), false)?;
    let added = vod_lists::epoch(row.relation_created)?;
    named(out, "added", Atom::Text(&added), false)?;
    let category = row.object.category.unwrap_or(0).to_string();
    named(out, "category_id", Atom::Text(&category), false)?;
    out.raw(b", \"category_ids\": [")?;
    if let Some(id) = row.object.category {
        out.raw(id.to_string().as_bytes())?;
    }
    out.raw(b"]")?;
    named(
        out,
        "container_extension",
        Atom::Text(
            row.object
                .extension
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("mp4"),
        ),
        false,
    )?;
    named(out, "custom_sid", Atom::Raw(literal("null")), false)?;
    named(out, "direct_source", Atom::Text(""), false)?;
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
    fn text(row: &serde_json::Value, name: &str) -> Option<String> {
        row[name].as_str().map(str::to_owned)
    }
    fn fixture() -> (InfoRow, serde_json::Value) {
        let data: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/Source-movie-info-golden.json"))
                .unwrap();
        let o = &data["movie"];
        let r = &data["relation"];
        let object = ListRow {
            content_id: o["id"].as_i64().unwrap(),
            category: r["category_id"].as_i64(),
            extension: text(r, "container_extension"),
            name: text(o, "name").unwrap(),
            language: text(o, "language").unwrap(),
            rating: text(o, "rating"),
            description: text(o, "description"),
            genre: text(o, "genre"),
            year: o["year"].as_i64().map(|n| n as i32),
            tmdb: text(o, "tmdb_id"),
            imdb: text(o, "imdb_id"),
            adult: o["is_adult"].as_bool().unwrap(),
            created: at(&o["created_at"]),
            modified: at(&r["updated_at"]),
            logo: o["logo_id"].as_i64(),
            properties: Some(o["custom_properties"].to_string()),
            image: String::new(),
            backdrop: None,
        };
        let properties = Some(r["custom_properties"].to_string());
        let retained = object.heap_bytes().unwrap()
            + properties.as_ref().map_or(0, String::capacity)
            + std::mem::size_of::<InfoRow>();
        let row = InfoRow {
            relation_id: r["id"].as_i64().unwrap(),
            account_id: r["m3u_account_id"].as_i64().unwrap(),
            object,
            relation_created: at(&r["created_at"]),
            relation_modified: at(&r["updated_at"]),
            relation_properties: properties,
            last_refresh: Some(at(&r["last_advanced_refresh"])),
            retained_bytes: retained,
        };
        (row, data)
    }
    fn output(row: &InfoRow) -> Vec<u8> {
        let mut out = JsonBytes::new(1024 * 1024).unwrap();
        write(row, "http://127.0.0.1:5657", 4 * 1024 * 1024, &mut out).unwrap();
        out.finish()
    }
    #[test]
    fn retained_source_movie_info_dto_bytes_match_exactly() {
        let (row, source) = fixture();
        let bytes = output(&row);
        let expected = source["expected_body_hex"].as_str().unwrap();
        let expected = (0..expected.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&expected[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            bytes, expected,
            "same retained SQL fields and Source3 TCP body, no clock/nonce normalization"
        );
        assert_eq!(row.relation_modified, row.object.modified);
    }
    #[test]
    fn bulk_update_shape_error_does_not_leak_partial_custom_fields() {
        let (mut row, _) = fixture();
        row.relation_properties = Some(r#"{"detailed_fetched":true,"detailed_info":null}"#.into());
        let bytes = output(&row);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["info"]["director"], "");
        assert_eq!(value["info"]["actors"], "");
        assert_eq!(value["info"]["video"], serde_json::json!({}));
    }
    #[test]
    fn object_falsey_properties_skip_relation_metadata_but_keep_artwork() {
        let (mut row, _) = fixture();
        row.object.properties = Some("{}".into());
        let value: serde_json::Value = serde_json::from_slice(&output(&row)).unwrap();
        assert_eq!(value["info"]["video"], serde_json::json!({}));
        assert!(value["info"]["movie_image"]
            .as_str()
            .unwrap()
            .contains("/image/?kind=movie_image&v="));
    }
    #[test]
    fn full_retained_and_growth_scratch_refuses_before_success() {
        let (row, _) = fixture();
        let mut out = JsonBytes::new(1024 * 1024).unwrap();
        assert!(matches!(
            write(&row, "http://fixture.invalid", 128, &mut out),
            Err(Error::Store(dispatcharr_store::StoreError::StatusBody(
                503,
                _
            )))
        ));
        assert!(out.finish().is_empty());
    }
}
