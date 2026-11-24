//! Original Dispatcharr's bulk EPG assignment with scoped programme dispatch.
//! Original Dispatcharr authors and contributors retain full application credit.
use crate::{auto_sync_epg::Task, AuthService, StoreError};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Request, State},
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};

const UPDATE: &str = "
WITH requested AS (
    SELECT * FROM unnest($1::bigint[], $2::bigint[]) AS r(channel_id, epg_id)
), changed AS MATERIALIZED (
    SELECT c.id, r.epg_id
    FROM dispatcharr_channels_channel c
    JOIN requested r ON r.channel_id = c.id
    WHERE c.epg_data_id IS DISTINCT FROM r.epg_id
    ORDER BY c.id
    FOR UPDATE OF c
), guides AS MATERIALIZED (
    SELECT e.id::bigint AS epg_id, s.id::bigint AS source_id, s.source_type,
           s.source_type = 'schedules_direct' AND EXISTS (
               SELECT 1 FROM epg_programdata p WHERE p.epg_id = e.id
           ) AS has_programmes
    FROM epg_epgdata e
    LEFT JOIN epg_epgsource s ON s.id = e.epg_source_id
    WHERE e.id IN (SELECT epg_id FROM changed WHERE epg_id IS NOT NULL)
), domain AS MATERIALIZED (
    SELECT NOT EXISTS (
        SELECT 1
        FROM changed c
        WHERE c.epg_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM guides g WHERE g.epg_id = c.epg_id
        )
    ) AND NOT EXISTS (
        SELECT 1
        FROM guides g
        WHERE CASE g.source_type
            WHEN 'dummy' THEN false
            WHEN 'schedules_direct' THEN NOT g.has_programmes AND NOT $4::boolean
            WHEN 'xmltv' THEN NOT $3::boolean
            ELSE g.source_type IS NOT NULL OR NOT $3::boolean
        END
    ) AS allowed
), updated AS (
    UPDATE dispatcharr_channels_channel c
    SET epg_data_id = r.epg_id
    FROM changed r CROSS JOIN domain d
    WHERE c.id = r.id AND d.allowed
    RETURNING c.id
)
SELECT d.allowed, (SELECT count(*) FROM updated),
       COALESCE((SELECT jsonb_agg(jsonb_build_array(
           g.epg_id, g.source_id, g.source_type, COALESCE(g.has_programmes, false)
       )) FROM guides g WHERE d.allowed), '[]'::jsonb)
FROM domain d
";

fn required() -> StoreError {
    StoreError::InvalidInput(json!({"error": "associations list is required"}))
}

fn unsupported() -> StoreError {
    StoreError::Unsupported("Batch EPG assignment outside supported guide publication domain")
}

fn positive_id(value: &Value) -> Result<i64, StoreError> {
    value.as_i64().filter(|id| *id > 0).ok_or_else(unsupported)
}

fn associations(rows: &[Value]) -> Result<BTreeMap<i64, Option<i64>>, StoreError> {
    let mut pairs = BTreeMap::new();
    for row in rows {
        let row = row.as_object().ok_or_else(unsupported)?;
        let Some(channel) = row.get("channel_id").filter(|id| !id.is_null()) else {
            continue;
        };
        if channel.as_i64() == Some(0) {
            continue;
        }
        let channel = positive_id(channel)?;
        let epg = row
            .get("epg_data_id")
            .filter(|value| !value.is_null())
            .map(positive_id)
            .transpose()?;
        // Source builds one dictionary before loading any channel rows.
        pairs.insert(channel, epg);
    }
    Ok(pairs)
}

pub(crate) async fn mutation_value(request: Request) -> Result<Value, StoreError> {
    let media = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let raw = to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .map_err(|_| StoreError::StatusBody(413, json!({"detail": "Request body too large."})))?;
    if raw.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    if media.split(';').next().map(str::trim) != Some("application/json") {
        return Err(StoreError::StatusBody(
            415,
            json!({"detail": format!("Unsupported media type \"{media}\" in request.")}),
        ));
    }
    serde_json::from_slice::<Value>(&raw).map_err(|error| {
        StoreError::InvalidInput(json!({"detail": format!(
            "JSON parse error - {}", crate::source_json_error(&raw, error)
        )}))
    })
}

pub(crate) async fn batch_body(request: Request) -> Result<Map<String, Value>, StoreError> {
    match mutation_value(request).await? {
        Value::Object(fields) => Ok(fields),
        _ => Err(StoreError::UnhandledRestFault),
    }
}

type Guide = (i64, Option<i64>, Option<String>, bool);

fn programme_jobs(guides: Vec<Guide>) -> Result<Vec<Task>, StoreError> {
    let mut already_filled = BTreeSet::new();
    let rows = guides
        .into_iter()
        .map(|(epg, source, kind, has_programmes)| {
            if has_programmes {
                already_filled.insert(epg);
            }
            (epg, source, kind)
        })
        .collect();
    crate::channel_writes::guide_jobs(rows, &already_filled)
}

pub(super) async fn set(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    let permit = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(permit)?;
    tokio::spawn(async move {
        auth.account_user(peer, request.headers().clone(), true)
            .await?;
        let body = batch_body(request).await?;
        let rows = body
            .get("associations")
            .and_then(Value::as_array)
            .filter(|rows| !rows.is_empty())
            .ok_or_else(required)?;
        let (channels, epgs): (Vec<_>, Vec<_>) = associations(rows)?.into_iter().unzip();
        let publisher_ready =
            crate::channel_writes::channel_guide_publisher_ready(&auth, Some("xmltv"));
        let sd_ready = auth
            .sd_guide_root_publisher
            .get()
            .is_some_and(|publisher| publisher.ready());
        let (allowed, updated, sqlx::types::Json(guides)): (bool, i64, sqlx::types::Json<Vec<Guide>>) =
            sqlx::query_as(UPDATE)
                .bind(channels)
                .bind(epgs)
                .bind(publisher_ready)
                .bind(sd_ready)
                .fetch_one(auth.store.pool())
                .await?;
        if !allowed {
            return Err(unsupported());
        }
        // Original bulk_update commits before dispatch. It skips the
        // single-channel model save and cache signal.
        let programmes = programme_jobs(guides)?;
        let programs_refreshed = programmes.len();
        use crate::core_settings_special_broker::{publish_sd_guide_root, publish_tvg_programmes};
        for task in programmes {
            match task {
                Task::ParseProgrammes(epg) => publish_tvg_programmes(&auth, epg).await?,
                Task::ParseSdProgrammes(epg) => publish_sd_guide_root(&auth, false, epg).await?,
                Task::FetchSdMappedGuide(source) => publish_sd_guide_root(&auth, true, source).await?,
            }
        }
        Ok((
            [(header::ALLOW, "POST, OPTIONS")],
            Json(json!({"success": true, "channels_updated": updated, "programs_refreshed": programs_refreshed})),
        )
            .into_response())
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_last_association_and_treats_absent_guide_as_clear() {
        let rows = [
            json!({"channel_id": 9, "epg_data_id": 31}),
            json!({"channel_id": 12, "epg_data_id": 41}),
            json!({"channel_id": 9, "epg_data_id": null}),
            json!({"channel_id": 12}),
            json!({"channel_id": 15, "epg_data_id": 51}),
            json!({"channel_id": 0, "epg_data_id": 61}),
            json!({"epg_data_id": 71}),
        ];
        let expected = BTreeMap::from([(9, None), (12, None), (15, Some(51))]);
        assert_eq!(associations(&rows).unwrap(), expected);
    }

    #[test]
    fn batch_dispatch_reuses_source_sd_threshold_and_skips_filled_stations() {
        let station =
            |epg, source, filled| (epg, Some(source), Some("schedules_direct".into()), filled);
        let guides = vec![
            station(21, 6, false),
            station(22, 6, true),
            station(31, 7, false),
            station(32, 7, false),
            station(33, 7, false),
            station(41, 8, true),
            (51, Some(9), Some("dummy".into()), false),
            (61, Some(10), Some("xmltv".into()), false),
            (71, None, None, false),
        ];
        assert_eq!(
            programme_jobs(guides).unwrap(),
            [
                Task::ParseProgrammes(61),
                Task::ParseProgrammes(71),
                Task::ParseSdProgrammes(21),
                Task::FetchSdMappedGuide(7),
            ]
        );
    }

    #[test]
    fn supports_bigautofield_ids_and_declines_non_ui_identifier_shapes() {
        let rows = [json!({"channel_id": 4_000_000_000_i64, "epg_data_id": i64::MAX})];
        let expected = BTreeMap::from([(4_000_000_000, Some(i64::MAX))]);
        assert_eq!(associations(&rows).unwrap(), expected);
        for row in [
            json!({"channel_id": "1", "epg_data_id": 5}),
            json!({"channel_id": 1, "epg_data_id": -1}),
            json!(7),
        ] {
            assert!(matches!(
                associations(&[row]),
                Err(StoreError::Unsupported(_))
            ));
        }
    }
}
