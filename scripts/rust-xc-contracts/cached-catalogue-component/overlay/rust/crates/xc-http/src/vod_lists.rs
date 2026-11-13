//! Unwired XC VOD list DTO extension, derived from pinned Source functions.
//! Physical7 qualification belongs only to compile3; this code needs its own
//! SQL/type/order/byte/resource gates. Original Dispatcharr authors retain credit.
use crate::vod_span::{self, RawValue};
use crate::{
    project::JsonBytes,
    reader::ReadLimit,
    vod_json,
    vod_priority::{self, Family, ListRow, ListRows},
    vod_properties::{Field, Properties},
    vod_rating::{self, Rating},
};
use chrono::Datelike;
use dispatcharr_store::{Store, StoreError, User};
use std::borrow::Cow;

#[derive(Debug)]
pub(crate) enum Error {
    Source,
    Unsupported,
    Store(StoreError),
}
impl From<StoreError> for Error {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}
impl From<vod_json::Error> for Error {
    fn from(value: vod_json::Error) -> Self {
        match value {
            vod_json::Error::Capacity(e) => Self::Store(e),
            vod_json::Error::Nonfinite => Self::Unsupported,
            _ => Self::Source,
        }
    }
}
impl From<vod_rating::Error> for Error {
    fn from(value: vod_rating::Error) -> Self {
        match value {
            vod_rating::Error::SourceValue => Self::Source,
            vod_rating::Error::Nonfinite => Self::Unsupported,
            vod_rating::Error::Capacity => Self::Store(crate::reader::capacity()),
        }
    }
}

pub(crate) struct Context<'a> {
    pub store: &'a Store,
    pub user: &'a User,
    pub family: Family,
    pub origin: &'a str,
    pub category: Option<&'a str>,
    pub limits: &'a ReadLimit,
    /// Portion of the SAME existing grant remaining after actual request/auth
    /// retained storage. This function acquires no new memory or worker pool.
    pub available_work_bytes: usize,
}
enum Category {
    All,
    Id(i64),
    Empty,
}
fn category(value: Option<&str>) -> Result<Category, Error> {
    let Some(value) = value.filter(|v| !v.is_empty()) else {
        return Ok(Category::All);
    };
    let parsed = dispatcharr_ingest_core::source_integer_literal(value).ok_or(Error::Source)?;
    Ok(match parsed.parse::<i64>() {
        Ok(id) => Category::Id(id),
        Err(_) => Category::Empty,
    })
}
pub(crate) async fn write(ctx: &Context<'_>, out: &mut JsonBytes) -> Result<(), Error> {
    if !crate::vod::enabled(
        &ctx.user.custom_properties,
        matches!(ctx.family, Family::Series),
    )? {
        out.raw(b"[]")?;
        return Ok(());
    }
    let selected = match category(ctx.category)? {
        Category::All => None,
        Category::Id(id) => Some(id),
        Category::Empty => {
            out.raw(b"[]")?;
            return Ok(());
        }
    };
    let rows = vod_priority::read(
        ctx.store,
        ctx.user,
        ctx.family,
        selected,
        ctx.limits,
        ctx.available_work_bytes,
    )
    .await?;
    project(rows, ctx.family, ctx.origin, ctx.available_work_bytes, out)
}
fn project(
    rows: ListRows,
    family: Family,
    origin: &str,
    work: usize,
    out: &mut JsonBytes,
) -> Result<(), Error> {
    let retained = rows.retained_bytes;
    out.raw(b"[")?;
    for (index, row) in rows.rows.into_iter().enumerate() {
        // SQL is settled/shrunk before this point. RawValue and all temporary
        // decoding/key buffers borrow a still-charged String. Conservatively
        // charge up to three input bytes of scratch per JSON byte, plus the
        // both old/new response capacities during growth, and aggregate raw metadata.
        // Include name/language/artwork temporary strings as well as JSON
        // decoder scratch. All current-row raw heaps remain in the same quote.
        let scratch = row
            .heap_bytes()?
            .checked_mul(3)
            .ok_or_else(crate::reader::capacity)?;
        let peak = scratch
            .checked_add(retained)
            .and_then(|n| n.checked_add(out.maximum().checked_mul(2)?))
            .and_then(|n| n.checked_add(origin.len().checked_mul(6)?))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(crate::reader::capacity)?;
        if peak > work {
            return Err(crate::reader::capacity().into());
        }
        if index > 0 {
            out.raw(b", ")?;
        }
        let properties = Properties::parse(row.properties.as_deref())?;
        match family {
            Family::Movie => movie(out, &row, &properties, index + 1, origin)?,
            Family::Series => series(out, &row, &properties, index + 1, origin)?,
        }
    }
    out.raw(b"]")?;
    Ok(())
}
fn key(out: &mut JsonBytes, name: &str, first: bool) -> Result<(), Error> {
    if !first {
        out.raw(b", ")?;
    }
    out.string(name)?;
    out.raw(b": ")?;
    Ok(())
}
fn string(out: &mut JsonBytes, name: &str, value: &str, first: bool) -> Result<(), Error> {
    key(out, name, first)?;
    out.string(value)?;
    Ok(())
}
fn number(out: &mut JsonBytes, name: &str, value: i64, first: bool) -> Result<(), Error> {
    key(out, name, first)?;
    out.raw(value.to_string().as_bytes())?;
    Ok(())
}
fn raw(out: &mut JsonBytes, name: &str, value: &RawValue) -> Result<(), Error> {
    key(out, name, false)?;
    vod_json::write(out, value)?;
    Ok(())
}
pub(crate) fn space(c: char) -> bool {
    matches!(c,'\u{9}'..='\u{d}'|'\u{1c}'..='\u{20}'|'\u{85}'|'\u{a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}')
}
pub(crate) fn display_name<'a>(name: &'a str, language: &str) -> Result<Cow<'a, str>, Error> {
    if name.is_empty() || language.is_empty() {
        return Ok(Cow::Borrowed(name));
    }
    // Source's normal category-language grammar is ASCII ISO639-1. Unicode
    // uppercase-version behavior beyond that grammar stays explicitly open.
    if !language.is_ascii() {
        return Err(Error::Unsupported);
    }
    let tag = language.to_ascii_uppercase();
    let trimmed = name.trim_end_matches(space);
    let matching = trimmed
        .as_bytes()
        .get(trimmed.len().saturating_sub(4)..)
        .is_some_and(|s| {
            s.len() == 4
                && s[0] == b'['
                && s[3] == b']'
                && s[1..3].iter().all(u8::is_ascii_alphabetic)
                && s[1..3].eq_ignore_ascii_case(tag.as_bytes())
        });
    Ok(if matching {
        Cow::Borrowed(name)
    } else {
        Cow::Owned(format!("{name} [{tag}]"))
    })
}
fn image<'a>(row: &'a ListRow, props: &Properties<'a>) -> Result<Cow<'a, str>, Error> {
    let selected = row.image.trim_matches(space);
    if !selected.is_empty() {
        return Ok(Cow::Borrowed(selected));
    }
    let Some(raw) = props.artwork(Field::MovieImage) else {
        return Ok(Cow::Borrowed(""));
    };
    let Some(value) = vod_json::string(raw)? else {
        return Ok(Cow::Borrowed(""));
    };
    Ok(match value {
        Cow::Borrowed(value) => Cow::Borrowed(value.trim_matches(space)),
        Cow::Owned(value) => Cow::Owned(value.trim_matches(space).to_owned()),
    })
}
pub(crate) fn proxyable(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://") || value.starts_with("/data")
}
pub(crate) fn image_url(
    origin: &str,
    resource: &str,
    id: i64,
    kind: &str,
    index: Option<usize>,
    source: &str,
) -> String {
    let mut result = format!("{origin}/api/vod/{resource}/{id}/image/?kind={kind}");
    if let Some(index) = index {
        result.push_str(&format!("&index={index}"));
    }
    if !source.is_empty() {
        result.push_str("&v=");
        result.push_str(&dispatcharr_store::source_vod_image_version(source));
    }
    result
}
fn cover(
    out: &mut JsonBytes,
    name: &str,
    row: &ListRow,
    props: &Properties<'_>,
    origin: &str,
    resource: &str,
) -> Result<(), Error> {
    key(out, name, false)?;
    let source = image(row, props)?;
    if proxyable(&source) {
        out.string(&image_url(
            origin,
            resource,
            row.content_id,
            "movie_image",
            None,
            &source,
        ))?;
    } else if let Some(id) = row.logo.filter(|id| *id != 0) {
        out.string(&format!("{origin}/api/vod/vodlogos/{id}/cache/"))?;
    } else {
        out.raw(b"null")?;
    }
    Ok(())
}
pub(crate) fn epoch(value: chrono::DateTime<chrono::Utc>) -> Result<String, Error> {
    if !(1..=9999).contains(&value.year()) {
        return Err(Error::Unsupported);
    }
    // Reconstruct the exact microsecond rational before one correctly-rounded
    // decimal->binary conversion, then Python int() truncation (not floor).
    let micros = i128::from(value.timestamp_micros());
    let magnitude = micros.abs();
    let text = format!(
        "{}{}.{:06}",
        if micros < 0 { "-" } else { "" },
        magnitude / 1_000_000,
        magnitude % 1_000_000
    );
    let seconds = dispatcharr_ingest_core::source_float_literal(&text).ok_or(Error::Source)?;
    Ok((seconds.trunc() as i64).to_string())
}
fn categories(out: &mut JsonBytes, id: Option<i64>) -> Result<(), Error> {
    let id = id.filter(|id| *id != 0);
    string(out, "category_id", &id.unwrap_or(0).to_string(), false)?;
    key(out, "category_ids", false)?;
    out.raw(b"[")?;
    if let Some(id) = id {
        out.raw(id.to_string().as_bytes())?;
    }
    out.raw(b"]")?;
    Ok(())
}
fn movie(
    out: &mut JsonBytes,
    row: &ListRow,
    props: &Properties<'_>,
    num: usize,
    origin: &str,
) -> Result<(), Error> {
    // prefer_relation_artwork precedes rating and the later properties
    // calls in Source. Compute its image before emitting the dictionary.
    drop(image(row, props)?);
    out.raw(b"{")?;
    number(
        out,
        "num",
        i64::try_from(num).map_err(|_| crate::reader::capacity())?,
        true,
    )?;
    string(out, "name", &display_name(&row.name, &row.language)?, false)?;
    string(out, "stream_type", "movie", false)?;
    number(out, "stream_id", row.content_id, false)?;
    cover(out, "stream_icon", row, props, origin, "movies")?;
    string(
        out,
        "rating",
        row.rating
            .as_deref()
            .filter(|v| !v.is_empty())
            .unwrap_or("0"),
        false,
    )?;
    key(out, "rating_5based", false)?;
    match vod_rating::half(row.rating.as_deref())? {
        Rating::Empty => out.raw(b"0")?,
        Rating::Finite(value) => out.raw(value.as_bytes())?,
    }
    string(out, "added", &epoch(row.created)?, false)?;
    number(out, "is_adult", i64::from(row.adult), false)?;
    string(out, "tmdb_id", row.tmdb.as_deref().unwrap_or(""), false)?;
    string(out, "imdb_id", row.imdb.as_deref().unwrap_or(""), false)?;
    let trailer = props.get(Field::Trailer)?;
    key(out, "trailer", false)?;
    if vod_json::truthy(trailer)? {
        vod_json::write(out, trailer)?;
    } else {
        out.string("")?;
    }
    string(out, "plot", row.description.as_deref().unwrap_or(""), false)?;
    string(out, "genre", row.genre.as_deref().unwrap_or(""), false)?;
    if let Some(year) = row.year.filter(|v| *v != 0) {
        number(out, "year", i64::from(year), false)?;
    } else {
        string(out, "year", "", false)?;
    }
    raw(out, "director", props.get(Field::Director)?)?;
    raw(out, "cast", props.get(Field::Actors)?)?;
    raw(out, "release_date", props.get(Field::ReleaseDate)?)?;
    categories(out, row.category)?;
    string(
        out,
        "container_extension",
        row.extension
            .as_deref()
            .filter(|v| !v.is_empty())
            .unwrap_or("mp4"),
        false,
    )?;
    key(out, "custom_sid", false)?;
    out.raw(b"null")?;
    string(out, "direct_source", "", false)?;
    out.raw(b"}")?;
    Ok(())
}
fn series(
    out: &mut JsonBytes,
    row: &ListRow,
    props: &Properties<'_>,
    num: usize,
    origin: &str,
) -> Result<(), Error> {
    let release = props.release_date_present()?;
    let year = row
        .year
        .filter(|v| *v != 0)
        .map(|v| v.to_string())
        .unwrap_or_default();
    drop(image(row, props)?);
    out.raw(b"{")?;
    number(
        out,
        "num",
        i64::try_from(num).map_err(|_| crate::reader::capacity())?,
        true,
    )?;
    string(out, "name", &display_name(&row.name, &row.language)?, false)?;
    number(out, "series_id", row.content_id, false)?;
    cover(out, "cover", row, props, origin, "series")?;
    string(out, "plot", row.description.as_deref().unwrap_or(""), false)?;
    raw(out, "cast", props.get(Field::Cast)?)?;
    raw(out, "director", props.get(Field::Director)?)?;
    string(out, "genre", row.genre.as_deref().unwrap_or(""), false)?;
    for name in ["release_date", "releaseDate"] {
        key(out, name, false)?;
        if let Some(raw) = release {
            vod_json::write(out, raw)?;
        } else {
            out.string(&year)?;
        }
    }
    string(out, "last_modified", &epoch(row.modified)?, false)?;
    string(
        out,
        "rating",
        row.rating
            .as_deref()
            .filter(|v| !v.is_empty())
            .unwrap_or("0"),
        false,
    )?;
    let half = match vod_rating::half(row.rating.as_deref())? {
        Rating::Empty => "0".to_owned(),
        Rating::Finite(value) => value,
    };
    string(out, "rating_5based", &half, false)?;
    key(out, "backdrop_path", false)?;
    backdrops(out, row, props, origin)?;
    raw(out, "youtube_trailer", props.get(Field::Trailer)?)?;
    raw(out, "episode_run_time", props.get(Field::EpisodeRunTime)?)?;
    categories(out, row.category)?;
    string(out, "tmdb_id", row.tmdb.as_deref().unwrap_or(""), false)?;
    string(out, "imdb_id", row.imdb.as_deref().unwrap_or(""), false)?;
    out.raw(b"}")?;
    Ok(())
}
fn backdrop_kind(raw: &RawValue) -> bool {
    let text = raw.trim_matches([' ', '\n', '\r', '\t']);
    if text.starts_with('"') {
        return text != "\"\"";
    }
    text.starts_with('[')
        && !text[1..text.len() - 1]
            .trim_matches([' ', '\n', '\r', '\t'])
            .is_empty()
}
fn backdrops(
    out: &mut JsonBytes,
    row: &ListRow,
    props: &Properties<'_>,
    origin: &str,
) -> Result<(), Error> {
    let relation = row
        .backdrop
        .as_deref()
        .map(vod_span::parse)
        .transpose()
        .map_err(|_| Error::Source)?;
    let relation = relation.filter(|raw| backdrop_kind(raw));
    let fallback = props
        .artwork(Field::Backdrop)
        .filter(|raw| backdrop_kind(raw));
    let Some(selected) = relation.or(fallback) else {
        out.raw(b"[]")?;
        return Ok(());
    };
    if selected.trim_start().starts_with('"') {
        out.raw(b"[")?;
        backdrop_item(out, selected, origin, row.content_id, 0)?;
        out.raw(b"]")?;
        return Ok(());
    }
    out.raw(b"[")?;
    for (index, value) in vod_span::Array::new(selected)?.enumerate() {
        if index > 0 {
            out.raw(b", ")?;
        }
        backdrop_item(out, value?, origin, row.content_id, index)?;
    }
    out.raw(b"]")?;
    Ok(())
}

fn backdrop_item(
    out: &mut JsonBytes,
    value: &RawValue,
    origin: &str,
    id: i64,
    index: usize,
) -> Result<(), Error> {
    if let Some(source) = vod_json::string(value)? {
        if proxyable(&source) {
            out.string(&image_url(
                origin,
                "series",
                id,
                "backdrop",
                Some(index),
                &source,
            ))?;
            return Ok(());
        }
    }
    vod_json::write(out, value)?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn row(properties: Option<&str>) -> ListRow {
        let at = chrono::DateTime::parse_from_rfc3339("2026-01-02T03:04:05.123456Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        ListRow {
            content_id: 7,
            category: Some(3),
            extension: Some("mkv".into()),
            name: "Owned title".into(),
            language: String::new(),
            rating: Some("7.50".into()),
            description: Some("plot".into()),
            genre: None,
            year: Some(2020),
            tmdb: None,
            imdb: Some("tt7".into()),
            adult: false,
            created: at,
            modified: at,
            logo: None,
            properties: properties.map(str::to_owned),
            image: "http://origin:8080/poster.png".into(),
            backdrop: None,
        }
    }
    fn projected(row: ListRow, family: Family) -> String {
        let retained = size_of_row() + row.heap_bytes().unwrap();
        let mut out = JsonBytes::new(8192).unwrap();
        project(
            ListRows {
                rows: vec![row],
                retained_bytes: retained,
            },
            family,
            "http://fixture.invalid:8088",
            32768,
            &mut out,
        )
        .unwrap();
        String::from_utf8(out.finish()).unwrap()
    }
    fn size_of_row() -> usize {
        std::mem::size_of::<ListRow>()
    }
    #[test]
    fn movie_fields_use_source_types_order_and_captured_image_rating() {
        let text = projected(
            row(Some(
                r#"{"actors":["a"],"director":null,"youtube_trailer":false}"#,
            )),
            Family::Movie,
        );
        assert!(text.starts_with("[{\"num\": 1, \"name\": \"Owned title\", \"stream_type\": \"movie\", \"stream_id\": 7,"));
        assert!(text.contains("kind=movie_image&v=6169e7b5"));
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        let value = &value[0];
        assert_eq!(value["rating"], "7.50");
        assert_eq!(value["rating_5based"], 3.75);
        assert_eq!(value["cast"], serde_json::json!(["a"]));
        assert!(value["director"].is_null());
        assert_eq!(value["trailer"], "");
        assert_eq!(value["year"], 2020);
        assert_eq!(value["category_id"], "3");
        assert_eq!(value["category_ids"], serde_json::json!([3]));
    }
    #[test]
    fn series_missing_release_date_uses_year_but_explicit_null_does_not() {
        let mut missing = row(Some("{}"));
        missing.rating = Some("8.25".into());
        missing.image = "http://origin:8080/series.png".into();
        let text = projected(missing, Family::Series);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value[0]["rating_5based"], "4.12");
        assert_eq!(value[0]["release_date"], "2020");
        assert_eq!(value[0]["releaseDate"], "2020");
        assert!(text.contains("&v=97595d52"));
        let text = projected(row(Some(r#"{"release_date":null}"#)), Family::Series);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert!(value[0]["release_date"].is_null());
        assert!(value[0]["releaseDate"].is_null());
    }
    #[test]
    fn backdrop_source_indices_and_nonproxy_values_survive() {
        let mut value = row(None);
        value.backdrop = Some(r#"["http://origin:8080/a.png",null,"relative",false]"#.into());
        let text = projected(value, Family::Series);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        let paths = value[0]["backdrop_path"].as_array().unwrap();
        assert!(paths[0]
            .as_str()
            .unwrap()
            .contains("kind=backdrop&index=0&v="));
        assert!(paths[1].is_null());
        assert_eq!(paths[2], "relative");
        assert_eq!(paths[3], false);
    }
    #[test]
    fn language_and_timestamp_definition_boundaries() {
        assert_eq!(display_name("Film [eN]  ", "en").unwrap(), "Film [eN]  ");
        assert_eq!(display_name("Film [FR]", "en").unwrap(), "Film [FR] [EN]");
        assert!(matches!(display_name("Film", "é"), Err(Error::Unsupported)));
        let at = chrono::DateTime::parse_from_rfc3339("1969-12-31T23:59:59.999999Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(epoch(at).unwrap(), "0");
    }
    #[test]
    fn aggregate_scratch_refuses_before_amplifying_or_returning_partial_success() {
        let value = row(Some(r#"{"director":"abc"}"#));
        let retained = size_of_row() + value.heap_bytes().unwrap();
        let mut out = JsonBytes::new(8192).unwrap();
        let error = project(
            ListRows {
                rows: vec![value],
                retained_bytes: retained,
            },
            Family::Movie,
            "http://fixture.invalid",
            100,
            &mut out,
        )
        .unwrap_err();
        match error {
            Error::Store(StoreError::StatusBody(503, _)) => {}
            other => panic!("expected explicit capacity, got {other:?}"),
        }
    }
    #[test]
    fn response_growth_overlap_is_in_the_same_work_quote() {
        let value = row(None);
        let retained = size_of_row() + value.heap_bytes().unwrap();
        let mut out = JsonBytes::new(8192).unwrap();
        // One maximum would fit; an old+new response allocation would not.
        let work = retained + 8192 + 4096 + 100;
        assert!(matches!(
            project(
                ListRows {
                    rows: vec![value],
                    retained_bytes: retained
                },
                Family::Movie,
                "http://fixture.invalid",
                work,
                &mut out
            ),
            Err(Error::Store(StoreError::StatusBody(503, _)))
        ));
    }
    #[test]
    fn deferred_nonmapping_error_order_and_source_integer_category_decoder() {
        let text = r#""truthy""#;
        let props = Properties::parse(Some(text)).unwrap();
        let mut value = row(Some(text));
        value.rating = Some("notfloat".into());
        let mut out = JsonBytes::new(8192).unwrap();
        assert!(matches!(
            movie(&mut out, &value, &props, 1, "http://fixture.invalid"),
            Err(Error::Source)
        ));
        assert!(matches!(category(None).unwrap(), Category::All));
        assert!(matches!(category(Some("")).unwrap(), Category::All));
        assert!(matches!(category(Some("1_2")).unwrap(), Category::Id(12)));
        assert!(matches!(
            category(Some("99999999999999999999")).unwrap(),
            Category::Empty
        ));
        // A future is not polled here. This keeps the concrete SQL integration
        // typechecked without pretending to execute a PostgreSQL contract.
        let _ = write;
    }
}
