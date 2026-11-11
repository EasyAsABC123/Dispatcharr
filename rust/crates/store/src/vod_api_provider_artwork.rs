//! Source-derived cached provider-info extension and host controls.
//! Actual Source90 TCP and owned-resource gates passed; canonical app,
//! provider refresh, image serving and full React modal gates are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::vod_api_projection::truthy;
use md5::{Digest, Md5};
use serde_json::{json, Value};
fn space(c: char) -> bool {
    matches!(c,'\u{9}'..='\u{d}'|'\u{1c}'..='\u{20}'|'\u{85}'|'\u{a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}')
}
fn backdrops(value: &Value) -> Vec<Value> {
    match value {
        Value::String(value) if !value.is_empty() => vec![value.clone().into()],
        Value::Array(values) => values.clone(),
        _ => Vec::new(),
    }
}
fn first<'a>(values: impl IntoIterator<Item = &'a Value>) -> Option<&'a Value> {
    values.into_iter().find(|value| truthy(value))
}
pub(super) struct Artwork {
    pub image: String,
    pub backdrops: Vec<Value>,
}
pub(super) fn preferred(relation: &Value, object: &Value) -> Artwork {
    let mut candidates = Vec::new();
    if relation.is_object() {
        let info = &relation["info"];
        if info.is_object() {
            candidates.push(info);
            if info["info"].is_object() {
                candidates.push(&info["info"]);
            }
        }
        for key in ["detailed_info", "basic_data"] {
            if relation[key].is_object() {
                candidates.push(&relation[key]);
            }
        }
        candidates.push(relation);
    }
    let mut image = String::new();
    let mut backdrop = Vec::new();
    for candidate in candidates {
        if image.is_empty() {
            if let Some(value) = first([
                &candidate["movie_image"],
                &candidate["cover_big"],
                &candidate["stream_icon"],
                &candidate["cover"],
            ])
            .and_then(Value::as_str)
            {
                let value = value.trim_matches(space);
                if !value.is_empty() {
                    image = value.to_owned();
                }
            }
        }
        if backdrop.is_empty() {
            backdrop = backdrops(&candidate["backdrop_path"]);
        }
    }
    if image.is_empty() {
        image = object
            .get("movie_image")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim_matches(space)
            .to_owned();
    }
    if backdrop.is_empty() {
        backdrop = backdrops(&object["backdrop_path"]);
    }
    Artwork {
        image,
        backdrops: backdrop,
    }
}
pub(super) fn proxyable(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://") || value.starts_with("/data")
}
pub(super) fn image_url(
    origin: &str,
    resource: &str,
    id: i64,
    kind: &str,
    index: Option<usize>,
    account: i64,
    source: &str,
) -> String {
    let plural = match resource {
        "movie" => "movies",
        "series" => "series",
        "episode" => "episodes",
        _ => unreachable!("closed metadata resource"),
    };
    let mut url = format!("{origin}/api/vod/{plural}/{id}/image/?kind={kind}");
    if let Some(index) = index {
        url.push_str(&format!("&index={index}"));
    }
    url.push_str(&format!("&m3u_account_id={account}"));
    if !source.is_empty() {
        url.push_str(&format!("&v={:x}", Md5::digest(source.as_bytes()))[..11]);
    }
    url
}
pub(super) fn single(
    origin: &str,
    resource: &str,
    id: i64,
    kind: &str,
    account: i64,
    value: &str,
) -> String {
    if proxyable(value) {
        image_url(origin, resource, id, kind, None, account, value)
    } else {
        value.to_owned()
    }
}
pub(super) fn rewritten_backdrops(
    origin: &str,
    resource: &str,
    id: i64,
    account: i64,
    values: &[Value],
) -> Value {
    Value::Array(
        values
            .iter()
            .enumerate()
            .map(|(index, value)| match value.as_str() {
                Some(text) if proxyable(text) => {
                    image_url(origin, resource, id, "backdrop", Some(index), account, text).into()
                }
                _ => value.clone(),
            })
            .collect(),
    )
}
pub(super) fn logo_url(origin: &str, logo: &Value) -> String {
    format!(
        "{origin}/api/vod/vodlogos/{}/cache/?v={}",
        logo["id"].as_i64().expect("typed logo id"),
        format!(
            "{:x}",
            Md5::digest(logo["url"].as_str().unwrap_or("").as_bytes())
        )[..8]
            .to_owned()
    )
}
pub(super) fn series_cover(
    origin: &str,
    id: i64,
    name: &str,
    account: i64,
    artwork: &Artwork,
    logo: Option<&Value>,
) -> Value {
    if proxyable(&artwork.image) {
        json!({"id":null,"url":artwork.image,"cache_url":single(origin,"series",id,"movie_image",account,&artwork.image),"name":name})
    } else if let Some(logo) = logo {
        json!({"id":logo["id"],"url":logo["url"],"cache_url":logo_url(origin,logo),"name":logo["name"]})
    } else {
        Value::Null
    }
}
