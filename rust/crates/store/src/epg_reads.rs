//! Draft stored-guide API row projection, ported from original Dispatcharr.
//! Original authors retain full credit. HTTP-unwired; row projection has separate source gates.
use crate::{channels::truthy, username::lower_source, Store, StoreError};
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::{Stream, TryStreamExt};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgRow, Row};
use std::{borrow::Cow, pin::Pin};

#[derive(Clone, Copy, Debug)]
pub enum EpgReadResource {
    Sources,
    EpgData,
    Programs,
}
impl EpgReadResource {
    fn query(self) -> &'static str {
        match self {
            Self::Sources => crate::provider_reads::epg_read_query(true),
            Self::EpgData => crate::provider_reads::epg_read_query(false),
            Self::Programs => "SELECT id::bigint AS id,start_time,end_time,title,sub_title,description,tvg_id,custom_properties FROM epg_programdata WHERE ($1::bigint IS NULL OR id=$1) AND $2::bigint IS NULL",
        }
    }
    pub fn missing(self) -> StoreError {
        let model = match self {
            Self::Sources => "EPGSource",
            Self::EpgData => "EPGData",
            Self::Programs => "ProgramData",
        };
        StoreError::StatusBody(
            404,
            json!({"detail":format!("No {model} matches the given query.")}),
        )
    }
    pub(crate) fn project(self, row: PgRow, detail: bool, origin: &str) -> Result<Value, StoreError> {
        match self {
            Self::Sources => return crate::provider_reads::epg_read_projection(row, true),
            Self::EpgData => return crate::provider_reads::epg_read_projection(row, false),
            Self::Programs => {}
        }
        let id = row.try_get::<i64, _>("id")?;
        let mut value = json!({"id":id});
        for name in ["start_time", "end_time"] {
            value[name] = timestamp(row.try_get(name)?).into();
        }
        value["title"] = row.try_get::<String, _>("title")?.into();
        for name in ["sub_title", "description", "tvg_id"] {
            value[name] = row.try_get::<Option<String>, _>(name)?.into();
        }
        programme_metadata(
            &mut value,
            row.try_get::<Option<Value>, _>("custom_properties")?
                .as_ref(),
            detail,
            origin,
        )?;
        Ok(value)
    }
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(
        if value.timestamp_subsec_micros() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        true,
    )
}
fn object(value: Option<&Value>) -> Result<Cow<'_, Map<String, Value>>, StoreError> {
    if !truthy(value) {
        return Ok(Cow::Owned(Map::new()));
    }
    value
        .and_then(Value::as_object)
        .map(Cow::Borrowed)
        .ok_or_else(|| {
            StoreError::StatusBody(
                500,
                json!({"error":"Original EPG metadata is not a mapping"}),
            )
        })
}
fn get(map: &Map<String, Value>, name: &str) -> Value {
    map.get(name).cloned().unwrap_or(Value::Null)
}
fn or_empty(map: &Map<String, Value>, name: &str) -> Value {
    if truthy(map.get(name)) {
        get(map, name)
    } else {
        json!([])
    }
}
fn programme_metadata(
    value: &mut Value,
    properties: Option<&Value>,
    detail: bool,
    origin: &str,
) -> Result<(), StoreError> {
    let cp = object(properties)?;
    for name in ["season", "episode"] {
        value[name] = get(&cp, name);
    }
    for (name, source) in [
        ("is_new", "new"),
        ("is_live", "live"),
        ("is_premiere", "premiere"),
    ] {
        value[name] = truthy(cp.get(source)).into();
    }
    value["is_finale"] = if truthy(cp.get("premiere_text")) {
        let text = cp
            .get("premiere_text")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                StoreError::StatusBody(
                    500,
                    json!({"error":"Original premiere text is not a string"}),
                )
            })?;
        lower_source(text).contains("finale")
    } else {
        false
    }
    .into();
    if !detail {
        return Ok(());
    }
    for name in [
        "categories",
        "star_ratings",
        "content_advisory",
        "content_ratings",
        "images",
    ] {
        value[name] = or_empty(&cp, name);
    }
    for name in [
        "rating",
        "rating_system",
        "country",
        "language",
        "event_details",
        "icon",
    ] {
        value[name] = get(&cp, name);
    }
    let credits = object(cp.get("credits"))?;
    value["credits"] = json!({"actors":or_empty(&credits,"actor"),"directors":or_empty(&credits,"director"),"writers":or_empty(&credits,"writer"),"producers":or_empty(&credits,"producer"),"presenters":or_empty(&credits,"presenter")});
    let video = object(cp.get("video"))?;
    value["video_quality"] = get(&video, "quality");
    value["aspect_ratio"] = get(&video, "aspect");
    value["stereo"] = get(object(cp.get("audio"))?.as_ref(), "stereo");
    value["is_previously_shown"] = truthy(cp.get("previously_shown")).into();
    value["production_date"] = get(&cp, "date");
    value["original_air_date"] = get(
        object(cp.get("previously_shown_details"))?.as_ref(),
        "start",
    );
    let length = object(cp.get("length"))?;
    value["runtime"] = get(&length, "value");
    value["runtime_units"] = get(&length, "units");
    for (name, source) in [
        ("imdb_id", "imdb.com_id"),
        ("tmdb_id", "themoviedb.org_id"),
        ("tvdb_id", "thetvdb.com_id"),
    ] {
        value[name] = get(&cp, source);
    }
    value["poster_url"] = if truthy(cp.get("sd_icon")) {
        let icon = cp.get("sd_icon").and_then(Value::as_str).ok_or_else(|| {
            StoreError::StatusBody(500, json!({"error":"Original poster URI is not a string"}))
        })?;
        let hash = format!("{:x}", Sha256::digest(icon.as_bytes()));
        format!(
            "{origin}/api/epg/programs/{}/poster/?v={}",
            value["id"],
            &hash[..12]
        )
        .into()
    } else {
        Value::Null
    };
    Ok(())
}
impl Store {
    /// Component-level incremental SQL projection. The eventual HTTP gateway
    /// must own admission, DB deadlines, row/response budgets and body leases.
    /// This draft is deliberately not wired into the application router.
    pub fn epg_read_rows(
        &self,
        resource: EpgReadResource,
        id: Option<i64>,
        origin: String,
    ) -> Pin<Box<dyn Stream<Item = Result<Value, StoreError>> + Send + '_>> {
        Box::pin(async_stream::try_stream! {
            let mut rows=sqlx::query(resource.query()).bind(id).bind(None::<i64>).fetch(self.pool());
            while let Some(row)=rows.try_next().await? {yield resource.project(row,id.is_some(),&origin)?;}
        })
    }
}
