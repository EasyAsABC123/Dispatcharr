//! Source SD poster precedence and real owned metadata endpoint/cache updates.
//! Original Dispatcharr/Gracenote authors retain all image selection credit.
use super::{
    sd_guide_auth::{self as auth, Source},
    sd_guide_http,
    sd_guide_state::State,
};
use crate::StoreError;
use serde_json::{json, Value};
use std::time::Duration;
const CATEGORIES: [&str; 8] = [
    "Iconic",
    "Banner-L1",
    "Banner-L2",
    "Banner-L3",
    "Banner",
    "Staple",
    "Poster Art",
    "Box Art",
];
#[derive(Debug)]
pub(super) enum Error {
    Auth(auth::Error),
    Store(StoreError),
    Unsupported,
    Capacity,
}
impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
fn primary(v: &Value) -> bool {
    match v.get("primary") {
        Some(Value::Bool(true)) => true,
        Some(Value::String(s)) => matches!(s.to_lowercase().as_str(), "true" | "1" | "yes"),
        _ => false,
    }
}
fn width(v: &Value) -> i64 {
    match v.get("width") {
        Some(Value::Bool(v)) => i64::from(*v),
        Some(Value::Number(n)) => n
            .as_i64()
            .or_else(|| {
                n.as_f64()
                    .filter(|n| n.is_finite() && *n > i64::MIN as f64 && *n < i64::MAX as f64)
                    .map(|n| n.trunc() as i64)
            })
            .unwrap_or(0),
        Some(Value::String(s)) => dispatcharr_ingest_core::source_integer_literal(s)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        _ => 0,
    }
}
fn best<'a>(
    images: &'a [Value],
    categories: Option<&[&str]>,
    aspects: Option<&[&str]>,
    minimum: i64,
    only_primary: bool,
) -> Option<&'a str> {
    let mut winner = None;
    let mut rank = None;
    for image in images {
        if !image.is_object() || only_primary && !primary(image) {
            continue;
        }
        if categories.is_some_and(|cs| {
            !image
                .get("category")
                .and_then(Value::as_str)
                .is_some_and(|v| cs.contains(&v))
        }) {
            continue;
        }
        if aspects.is_some_and(|cs| {
            !image
                .get("aspect")
                .and_then(Value::as_str)
                .is_some_and(|v| cs.contains(&v))
        }) {
            continue;
        }
        let w = width(image);
        if w < minimum {
            continue;
        }
        let Some(uri) = image
            .get("uri")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        let next = (primary(image), w);
        if rank.is_none_or(|old| next > old) {
            winner = Some(uri);
            rank = Some(next)
        }
    }
    winner
}
fn configured<'a>(images: &'a [Value], style: &str) -> Option<&'a str> {
    let (categories, aspects): (&[&str], &[&str]) = match style {
        "portrait_iconic" => (&["Iconic"], &["2x3", "3x4"]),
        "portrait_banner" => (
            &["Banner-L1", "Banner-L2", "Banner-L3", "Banner"],
            &["2x3", "3x4"],
        ),
        "landscape_iconic" => (&["Iconic"], &["16x9", "4x3"]),
        "landscape_banner" => (
            &["Banner-L1", "Banner-L2", "Banner-L3", "Banner"],
            &["16x9", "4x3"],
        ),
        "square_iconic" => (&["Iconic"], &["1x1"]),
        _ => return recommended(images),
    };
    for minimum in [240, 135, 120, 0] {
        for category in categories {
            if let Some(url) = best(images, Some(&[*category]), Some(aspects), minimum, false) {
                return Some(url);
            }
        }
    }
    for minimum in [240, 135, 120, 0] {
        if let Some(url) = best(images, Some(&CATEGORIES), Some(aspects), minimum, false) {
            return Some(url);
        }
    }
    if let Some(url) = best(images, None, Some(aspects), 0, false) {
        return Some(url);
    }
    for minimum in [240, 135, 120, 0] {
        if let Some(url) = best(images, Some(&CATEGORIES), None, minimum, true) {
            return Some(url);
        }
    }
    if style != "portrait_iconic" {
        return configured(images, "portrait_iconic");
    }
    None
}
fn recommended(images: &[Value]) -> Option<&str> {
    for minimum in [240, 135, 120, 0] {
        if let Some(url) = best(images, Some(&CATEGORIES), None, minimum, true) {
            return Some(url);
        }
    }
    for minimum in [240, 135, 120, 0] {
        if let Some(url) = best(images, None, None, minimum, true) {
            return Some(url);
        }
    }
    configured(images, "portrait_iconic")
}
pub(super) async fn run(
    source: &mut Source,
    state: &State,
    token: &mut String,
    progress: &super::sd_guide_progress::Progress,
) -> Result<u64, Error> {
    if !source
        .properties
        .get("fetch_posters")
        .is_some_and(crate::xc_live_provider::truthy)
    {
        return Ok(0);
    }
    let style = match source.properties.get("poster_style") {
        None => "sd_recommended",
        Some(Value::String(v)) => v,
        _ => return Err(Error::Unsupported),
    }
    .to_owned();
    let table = state.table.clone();
    let work = table.clone();
    let style_query = style.clone();
    source.session.run(move|c|Box::pin(async move{
  sqlx::query(&format!("CREATE TEMP TABLE {work}_posters(program_id text PRIMARY KEY,art_key text,url text) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
  sqlx::query(&format!("INSERT INTO {work}_posters(program_id,art_key) SELECT DISTINCT p.program_id,CASE WHEN left(p.program_id,2)='EP' THEN 'SH'||substring(p.program_id FROM 3 FOR 8)||'0000' ELSE p.program_id END FROM epg_programdata p WHERE p.epg_id IN(SELECT id FROM {work}_mapped) AND p.program_id IS NOT NULL AND(p.custom_properties IS NULL OR NOT(p.custom_properties?'sd_icon') OR NOT(COALESCE(p.custom_properties->>'sd_poster_style','')=$1)) AND NOT(COALESCE(p.custom_properties->'sd_icon_missing','false'::jsonb)='true'::jsonb AND COALESCE(p.custom_properties->>'sd_poster_style','')=$1)")).bind(style_query).execute(c).await?;Ok::<_,StoreError>(())
 })).await??;
    let mut after = String::new();
    let mut announced = false;
    loop {
        let work = table.clone();
        let from = after.clone();
        let keys:Vec<String>=source.session.run(move|c|Box::pin(async move{sqlx::query_scalar(&format!("SELECT DISTINCT art_key FROM {work}_posters WHERE art_key>$1 ORDER BY art_key LIMIT 500")).bind(from).fetch_all(c).await.map_err(StoreError::from)})).await??;
        if keys.is_empty() {
            break;
        }
        if !announced {
            progress
                .parsing(98, Some("Fetching program artwork...".into()))
                .await;
            announced = true;
        }
        after = keys.last().ok_or(Error::Unsupported)?.clone();
        let reply = match source
            .authorized(
                reqwest::Method::POST,
                "/metadata/programs/",
                token,
                Some(&json!(keys)),
                Duration::from_secs(120),
            )
            .await
        {
            Ok(v) => v,
            Err(auth::Error::Http(sd_guide_http::Error::Transport)) => continue,
            Err(e) => return Err(Error::Auth(e)),
        };
        if reply.status >= 400 {
            continue;
        }
        let Some(value) = reply.value else { continue };
        let rows = value.as_array().ok_or(Error::Unsupported)?;
        let mut updates = Vec::new();
        for entry in rows {
            if !entry.is_object() {
                continue;
            }
            let Some(pid) = entry
                .get("programID")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            let Some(images) = entry.get("data").and_then(Value::as_array) else {
                continue;
            };
            let Some(url) = (if style == "sd_recommended" {
                recommended(images)
            } else {
                configured(images, &style)
            }) else {
                continue;
            };
            let url = if url.starts_with("http") {
                url.to_owned()
            } else {
                format!("{}/image/{url}", sd_guide_http::BASE)
            };
            if url.len() > 65536 {
                return Err(Error::Capacity);
            }
            updates.push(json!({"program_id":pid,"url":url}));
        }
        let work = table.clone();
        source.session.run(move|c|Box::pin(async move{sqlx::query(&format!("UPDATE {work}_posters p SET url=x.url FROM jsonb_to_recordset($1) x(program_id text,url text) WHERE p.art_key=x.program_id")).bind(Value::Array(updates)).execute(c).await.map(|_|()).map_err(StoreError::from)})).await??;
    }
    let work = table.clone();
    Ok(source.session.run(move|c|Box::pin(async move{
  sqlx::query(&format!("UPDATE epg_programdata p SET custom_properties=(COALESCE(NULLIF(p.custom_properties,'null'::jsonb),'{{}}'::jsonb)||jsonb_build_object('sd_icon',a.url,'sd_poster_style',$1::text))-'sd_icon_missing' FROM {work}_posters a WHERE p.program_id=a.program_id AND p.epg_id IN(SELECT id FROM {work}_mapped) AND a.url IS NOT NULL")).bind(style).execute(c).await.map(|r|r.rows_affected()).map_err(StoreError::from)
 })).await??)
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn source_primary_category_then_width_and_first_tie_win() {
        let images = json!([{"uri":"nonprimary","category":"Iconic","aspect":"2x3","width":1000},{"uri":"primaryother","category":"Other","width":900,"primary":true},{"uri":"first","category":"Iconic","width":"240","primary":"yes"},{"uri":"second","category":"Iconic","width":240,"primary":true}]);
        assert_eq!(recommended(images.as_array().unwrap()), Some("first"));
        assert_eq!(
            configured(images.as_array().unwrap(), "portrait_iconic"),
            Some("nonprimary")
        );
        assert_eq!(
            configured(images.as_array().unwrap(), "unknown"),
            Some("first")
        );
    }
}
