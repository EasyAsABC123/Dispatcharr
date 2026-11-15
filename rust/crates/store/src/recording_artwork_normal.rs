//! Actual Source normal no-image/manual/channel-fallback artwork branches.
//! Original Dispatcharr/Django authors retain algorithm/application credit.
//! Existing legacy prepare remains unchanged; unsupported image/provider work
//! is explicit and is never converted to a fabricated successful no-op.
use super::*;
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use sqlx::Row;

pub(super) async fn prepare(
    store: &Store,
    recording: i64,
    owner: Arc<dyn Send + Sync>,
) -> Result<Prepared, Error> {
    // Retain the measured local-VOD-logo path and all its physical boundaries.
    match store.prepare(recording, owner.clone()).await {
        Ok(value) => return Ok(value),
        Err(Error::Unsupported(_)) => {}
        Err(error) => return Err(error),
    }
    let pool = store.pool.clone();
    let origin = store.origin.clone();
    tokio::spawn(async move {
        let _owner = owner;
        let mut connection = pool.acquire().await.map_err(sql)?.detach();
        let observed = resolve(&mut connection, recording, origin).await;
        let ready = connection.ping().await.err().map(Box::new);
        let close = connection.close().await.err().map(Box::new);
        match observed {
            Err(Error::Sql { primary, .. }) => Err(Error::Sql {
                primary,
                ready,
                close,
            }),
            other => {
                if ready.is_some() || close.is_some() {
                    Err(Error::Sql {
                        primary: None,
                        ready,
                        close,
                    })
                } else {
                    other
                }
            }
        }
    })
    .await
    .map_err(|_| Error::Supervisor)?
}
async fn resolve(
    connection: &mut sqlx::PgConnection,
    recording: i64,
    origin: Arc<()>,
) -> Result<Prepared, Error> {
    if recording <= 0 {
        return Err(Error::Unsupported("recording identity"));
    }
    let row=sqlx::query("SELECT r.channel_id::bigint,r.start_time,r.end_time,CASE WHEN r.custom_properties IS NULL THEN '{}'::jsonb WHEN octet_length(r.custom_properties::text)<=8192 THEN r.custom_properties END AS properties,(r.custom_properties IS NULL OR octet_length(r.custom_properties::text)<=8192) AS fits FROM dispatcharr_channels_recording r WHERE r.id=$1").bind(recording).fetch_optional(&mut *connection).await.map_err(sql)?;
    let Some(row) = row else {
        return Ok(Prepared {
            origin,
            recording,
            resolved: Value::Null,
            changed: false,
            skip: Some("error: Recording matching query does not exist.".into()),
        });
    };
    if !row.try_get::<bool, _>("fits").map_err(sql)? {
        return Err(Error::Unsupported("recording artwork snapshot capacity"));
    }
    let mut properties = row
        .try_get::<Option<Value>, _>("properties")
        .map_err(sql)?
        .ok_or(Error::Unsupported("recording artwork properties"))?;
    if !truth(&properties) {
        properties = json!({});
    }
    let fields = properties
        .as_object()
        .ok_or(Error::Unsupported("nonobject recording artwork properties"))?;
    if let Some(status) = fields.get("status").and_then(Value::as_str) {
        if matches!(
            status,
            "recording" | "completed" | "stopped" | "interrupted"
        ) {
            return Ok(Prepared {
                origin,
                recording,
                resolved: properties.clone(),
                changed: false,
                skip: Some(format!("skipped: status is {status}")),
            });
        }
    }
    let channel = row.try_get::<i64, _>("channel_id").map_err(sql)?;
    let start = row.try_get::<DateTime<Utc>, _>("start_time").map_err(sql)?;
    let end = row.try_get::<DateTime<Utc>, _>("end_time").map_err(sql)?;
    let channel=sqlx::query("SELECT CASE WHEN octet_length(name)<=2048 THEN name END AS name,epg_data_id::bigint,logo_id::bigint FROM dispatcharr_channels_channel WHERE id=$1").bind(channel).fetch_optional(&mut *connection).await.map_err(sql)?.ok_or(Error::Unsupported("recording artwork channel missing"))?;
    let channel_name = channel
        .try_get::<Option<String>, _>("name")
        .map_err(sql)?
        .ok_or(Error::Unsupported("artwork channel name capacity"))?;
    let epg = channel
        .try_get::<Option<i64>, _>("epg_data_id")
        .map_err(sql)?;
    let channel_logo = channel.try_get::<Option<i64>, _>("logo_id").map_err(sql)?;
    let mut program = fields
        .get("program")
        .filter(|value| truth(value))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let mut matched_program = false;
    if needs_match(&program) {
        if let Some(epg) = epg {
            if let Some(matched) = timeslot(connection, epg, start, end).await? {
                matched_program = true;
                let object = program
                    .as_object_mut()
                    .ok_or(Error::Unsupported("manual programme object"))?;
                for (key, value) in matched {
                    object.insert(key, value);
                }
            }
        }
    }
    let title = match program.as_object().and_then(|v| v.get("title")) {
        Some(value) if truth(value) => Some(
            strip(
                value
                    .as_str()
                    .ok_or(Error::Unsupported("artwork programme title type"))?,
            )
            .to_owned(),
        )
        .filter(|v| !v.is_empty()),
        _ => None,
    };
    if title.as_ref().is_some_and(|title| title.len() > 2048) {
        return Err(Error::Unsupported("artwork programme title capacity"));
    }
    // The pinned Source guard skips VOD and all external providers when no real
    // title exists or its normalized value is the actual channel name.
    let no_external = title
        .as_ref()
        .is_none_or(|title| normalized(title) == normalized(&channel_name));
    let epg_props = match program
        .as_object()
        .and_then(|value| value.get("id"))
        .filter(|value| truth(value))
        .and_then(program_id)
    {
        Some(id) => {
            let row=sqlx::query("SELECT CASE WHEN custom_properties IS NULL THEN '{}'::jsonb WHEN octet_length(custom_properties::text)<=8192 THEN custom_properties END AS properties,(custom_properties IS NULL OR octet_length(custom_properties::text)<=8192) AS fits FROM epg_programdata WHERE id=$1").bind(id).fetch_optional(&mut *connection).await.map_err(sql)?;
            match row {
                None => json!({}),
                Some(row) => {
                    if !row.try_get::<bool, _>("fits").map_err(sql)? {
                        return Err(Error::Unsupported("artwork EPG properties capacity"));
                    }
                    row.try_get::<Option<Value>, _>("properties")
                        .map_err(sql)?
                        .ok_or(Error::Unsupported("artwork EPG properties"))?
                }
            }
        }
        None => json!({}),
    };
    // Potential image branches must execute their real validation/create ports;
    // never pretend an image is missing just to reach channel fallback.
    if image_candidate(&epg_props) {
        return Err(Error::Unsupported(
            "artwork EPG image validation not composed",
        ));
    }
    let mut logo = None;
    if !no_external {
        let title = title.as_ref().ok_or(Error::Unsupported("artwork title"))?;
        // Actual Source first Movie, then first Series with a logo. No fixture
        // title/season or same-process parent receipt defines this SQL branch.
        for query in ["SELECT l.id::bigint FROM vod_movie m LEFT JOIN vod_vodlogo l ON l.id=m.logo_id WHERE UPPER(m.name::text)=UPPER($1) ORDER BY m.name LIMIT 1","SELECT l.id::bigint FROM vod_series m LEFT JOIN vod_vodlogo l ON l.id=m.logo_id WHERE UPPER(m.name::text)=UPPER($1) ORDER BY m.name LIMIT 1"]{
            logo=sqlx::query_scalar::<_,Option<i64>>(query).bind(title).fetch_optional(&mut *connection).await.map_err(sql)?.flatten();if logo.is_some(){break;}
        }
        if logo.is_none() {
            return Err(Error::Unsupported(
                "artwork provider fallback requires real HTTP composition",
            ));
        }
    }
    if logo.is_none() {
        if direct_candidate(&program) {
            return Err(Error::Unsupported(
                "artwork direct image validation/persistence not composed",
            ));
        }
        logo = channel_logo;
    }
    let (resolved, changed) =
        resolved_properties(properties, program, matched_program, logo, &epg_props)?;
    Ok(Prepared {
        origin,
        recording,
        resolved,
        changed,
        skip: None,
    })
}
fn resolved_properties(
    properties: Value,
    program: Value,
    matched_program: bool,
    logo: Option<i64>,
    epg_props: &Value,
) -> Result<(Value, bool), Error> {
    let mut resolved = properties;
    let out = resolved
        .as_object_mut()
        .ok_or(Error::Unsupported("recording artwork object"))?;
    if matched_program {
        out.insert("program".into(), program);
    }
    let mut changed = false;
    if let Some(logo) = logo.filter(|id| *id > 0) {
        set(out, "poster_logo_id", json!(logo), &mut changed);
    }
    if let Some(epg) = epg_props.as_object() {
        for key in [
            "rating",
            "rating_system",
            "season",
            "episode",
            "onscreen_episode",
        ] {
            if let Some(value) = epg.get(key) {
                if if matches!(key, "season" | "episode") {
                    !value.is_null()
                } else {
                    truth(value)
                } {
                    set(out, key, value.clone(), &mut changed);
                }
            }
        }
    }
    // A match alone modifies memory but Source only saves/emits when a poster
    // or metadata enrichment sets updated=True. Existing program null/absence
    // is not overwritten by the temporary `program or {}` without a real match.
    Ok((resolved, changed))
}
fn set(out: &mut serde_json::Map<String, Value>, key: &str, value: Value, changed: &mut bool) {
    if out.get(key).is_none_or(|old| !python_equal(old, &value)) {
        out.insert(key.into(), value);
        *changed = true;
    }
}
fn needs_match(program: &Value) -> bool {
    program.as_object().is_some_and(|program| {
        !["user_edited", "id", "title"]
            .iter()
            .any(|key| program.get(*key).is_some_and(truth))
    })
}
fn program_id(value: &Value) -> Option<i64> {
    match value {
        Value::Bool(value) => Some(i64::from(*value)),
        Value::Number(value) => value.as_i64().or_else(|| {
            value
                .as_f64()
                .filter(|v| v.is_finite() && *v >= i64::MIN as f64 && *v < (i64::MAX as f64))
                .map(|v| v.trunc() as i64)
        }),
        Value::String(value) => dispatcharr_ingest_core::source_integer_literal(value)?
            .parse()
            .ok(),
        _ => None,
    }
}
fn direct_candidate(program: &Value) -> bool {
    program.as_object().is_some_and(|program| {
        ["poster", "cover", "cover_big", "image", "icon"]
            .iter()
            .any(|key| match program.get(*key) {
                Some(Value::String(value)) => !value.is_empty(),
                Some(Value::Object(value)) => value
                    .get("url")
                    .and_then(Value::as_str)
                    .is_some_and(|v| !v.is_empty()),
                _ => false,
            })
    })
}
fn image_candidate(properties: &Value) -> bool {
    let Some(properties) = properties.as_object() else {
        return false;
    };
    if properties
        .get("icon")
        .and_then(Value::as_str)
        .is_some_and(|v| !v.is_empty())
    {
        return true;
    }
    // Conservatively preserve every potential Source-selected image. Invalid
    // nonstring/false entries cannot become a reachable URL in Source.
    properties
        .get("images")
        .and_then(Value::as_array)
        .is_some_and(|images| {
            images.iter().filter_map(Value::as_object).any(|image| {
                image
                    .get("url")
                    .and_then(Value::as_str)
                    .is_some_and(|url| !url.is_empty())
            })
        })
}
async fn timeslot(
    connection: &mut sqlx::PgConnection,
    epg: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Option<serde_json::Map<String, Value>>, Error> {
    let duration =
        end.signed_duration_since(start)
            .num_microseconds()
            .ok_or(Error::Unsupported("artwork recording duration capacity"))? as f64
            / 1_000_000.0;
    if duration <= 0.0 {
        return Ok(None);
    }
    let mut rows=sqlx::query("SELECT id::bigint,CASE WHEN octet_length(title)<=2048 THEN title END AS title,CASE WHEN sub_title IS NULL OR octet_length(sub_title)<=8192 THEN sub_title END AS sub_title,CASE WHEN description IS NULL OR octet_length(description)<=8192 THEN description END AS description,(octet_length(title)<=2048 AND (sub_title IS NULL OR octet_length(sub_title)<=8192) AND (description IS NULL OR octet_length(description)<=8192)) AS fits,start_time,end_time FROM epg_programdata WHERE epg_id=$1 AND start_time<$2 AND end_time>$3").bind(epg).bind(end).bind(start).fetch(&mut *connection);
    let mut best = None;
    let mut overlap = 0.0;
    while let Some(row) = rows.try_next().await.map_err(sql)? {
        if !row.try_get::<bool, _>("fits").map_err(sql)? {
            return Err(Error::Unsupported("artwork manual EPG text capacity"));
        }
        let from = start.max(row.try_get::<DateTime<Utc>, _>("start_time").map_err(sql)?);
        let until = end.min(row.try_get::<DateTime<Utc>, _>("end_time").map_err(sql)?);
        let actual = until
            .signed_duration_since(from)
            .num_microseconds()
            .ok_or(Error::Unsupported("artwork EPG overlap capacity"))? as f64
            / 1_000_000.0;
        if actual > overlap {
            overlap = actual;
            best = Some(serde_json::Map::from_iter([
                (
                    "id".into(),
                    json!(row.try_get::<i64, _>("id").map_err(sql)?),
                ),
                (
                    "title".into(),
                    json!(row
                        .try_get::<Option<String>, _>("title")
                        .map_err(sql)?
                        .unwrap_or_default()),
                ),
                (
                    "sub_title".into(),
                    json!(row
                        .try_get::<Option<String>, _>("sub_title")
                        .map_err(sql)?
                        .unwrap_or_default()),
                ),
                (
                    "description".into(),
                    json!(row
                        .try_get::<Option<String>, _>("description")
                        .map_err(sql)?
                        .unwrap_or_default()),
                ),
            ]));
        }
    }
    Ok(if overlap / duration >= 0.8 {
        best
    } else {
        None
    })
}

#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn actual_no_image_merge_preserves_absence_and_python_equal_values() {
        let (resolved, changed) = resolved_properties(
            json!({"status":"scheduled","program":null,"poster_logo_id":1}),
            json!({}),
            false,
            Some(1),
            &json!({}),
        )
        .unwrap();
        assert!(!changed);
        assert!(resolved["program"].is_null());
        let (resolved, changed) = resolved_properties(
            json!({"status":"scheduled"}),
            json!({}),
            false,
            None,
            &json!({}),
        )
        .unwrap();
        assert!(!changed);
        assert!(resolved.get("program").is_none());
        assert!(resolved.get("poster_logo_id").is_none());
        let (resolved, changed) = resolved_properties(
            json!({"poster_logo_id":true,"season":false}),
            json!({}),
            false,
            Some(1),
            &json!({"season":0}),
        )
        .unwrap();
        assert!(!changed);
        assert_eq!(resolved["poster_logo_id"], json!(true));
        assert_eq!(resolved["season"], json!(false));
    }
    #[test]
    fn matched_program_only_persists_when_poster_or_actual_rating_updates() {
        let program = json!({"id":23,"title":"Current channel","sub_title":"","description":""});
        let (resolved, changed) =
            resolved_properties(json!({}), program.clone(), true, None, &json!({})).unwrap();
        assert!(!changed);
        assert_eq!(resolved["program"], program);
        let (resolved, changed) = resolved_properties(
            json!({"status":"scheduled"}),
            program.clone(),
            true,
            Some(47),
            &json!({"rating":"TV-G","season":0,"episode":null}),
        )
        .unwrap();
        assert!(changed);
        assert_eq!(resolved["program"], program);
        assert_eq!(resolved["rating"], json!("TV-G"));
        assert_eq!(resolved["season"], json!(0));
        assert!(resolved.get("episode").is_none());
    }
    #[test]
    fn real_potential_image_never_becomes_a_silent_no_image_success() {
        assert!(!image_candidate(
            &json!({"images":[],"icon":"","season":2,"date":"20260101"})
        ));
        assert!(image_candidate(
            &json!({"images":[{"url":"https://actual.example/poster.jpg"}]})
        ));
        assert!(direct_candidate(
            &json!({"cover":{"url":"/data/cover.png"}})
        ));
        assert!(needs_match(&json!({})));
        assert!(!needs_match(&json!({"user_edited":true})));
        assert!(!needs_match(&json!({"id":7})));
        assert_eq!(program_id(&json!(" ١_٧ ")), Some(17));
    }
}
