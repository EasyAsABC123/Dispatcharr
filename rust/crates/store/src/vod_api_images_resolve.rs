//! Source-derived VOD image/cache component and ownership controls.
//! Actual Original/native240 and physical/mixed host gates passed;
//! canonical app, unchanged React and broader image contracts are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{vod_api_projection::truthy, vod_api_provider_artwork as artwork};
use num_bigint::BigInt;
use serde_json::{json, Value};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Account {
    Any,
    Id(i64),
    NoMatch,
}
pub(super) fn account(raw: Option<&str>) -> Account {
    match raw.and_then(dispatcharr_ingest_core::source_integer_literal) {
        None => Account::Any,
        Some(text) => text.parse().map(Account::Id).unwrap_or(Account::NoMatch),
    }
}
fn property<'a>(props: &'a Value, key: &str) -> Result<&'a Value, ()> {
    if !truthy(props) {
        return Ok(&Value::Null);
    }
    props.as_object().ok_or(())?;
    Ok(&props[key])
}
fn path(props: &Value, kind: &str, index: &str) -> Result<Option<String>, ()> {
    let raw = property(
        props,
        if kind == "backdrop" {
            "backdrop_path"
        } else {
            kind
        },
    )?;
    let raw = if kind == "backdrop" {
        let Some(index) = dispatcharr_ingest_core::source_integer_literal(index)
            .and_then(|value| value.parse::<BigInt>().ok())
            .and_then(|value| value.to_string().parse::<usize>().ok())
        else {
            return Ok(None);
        };
        match raw {
            Value::String(text) if !text.is_empty() && index == 0 => Some(raw),
            Value::Array(values) => values.get(index),
            _ => None,
        }
    } else {
        Some(raw)
    };
    Ok(raw
        .and_then(Value::as_str)
        .filter(|text| artwork::proxyable(text))
        .map(str::to_owned))
}
pub(super) fn resolve(
    props: &Value,
    relation: Option<&Value>,
    kind: &str,
    index: &str,
) -> Result<Option<String>, ()> {
    if !["movie_image", "backdrop", "poster_path"].contains(&kind) {
        return Ok(None);
    }
    if let Some(relation) = relation {
        let art = artwork::preferred(relation, props);
        let prepared = json!({"movie_image":art.image,"backdrop_path":art.backdrops,"poster_path":property(props,"poster_path")?});
        if let Some(url) = path(&prepared, kind, index)? {
            return Ok(Some(url));
        }
    }
    path(props, kind, index)
}
