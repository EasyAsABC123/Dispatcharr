//! Original Dispatcharr's bulk EPG assignment with scoped programme dispatch.
//! Original Dispatcharr authors and contributors retain full application credit.
use crate::{AuthService, StoreError};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Request, State},
    http::header,
    Json,
};
use serde_json::{json, Map, Value};
use std::{collections::BTreeMap, net::SocketAddr};

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
    SELECT assigned.epg_id, e.id IS NOT NULL AS guide_exists, s.source_type
    FROM (SELECT DISTINCT epg_id FROM changed WHERE epg_id IS NOT NULL) assigned
    LEFT JOIN epg_epgdata e ON e.id = assigned.epg_id
    LEFT JOIN epg_epgsource s ON s.id = e.epg_source_id
), domain AS MATERIALIZED (
    SELECT NOT EXISTS (
        SELECT 1
        FROM guides g
        WHERE g.source_type IS DISTINCT FROM 'dummy'
          AND (NOT $3::boolean OR NOT g.guide_exists
               OR (g.source_type IS NOT NULL AND g.source_type <> 'xmltv'))
    ) AS allowed
), updated AS (
    UPDATE dispatcharr_channels_channel c
    SET epg_data_id = r.epg_id
    FROM changed r CROSS JOIN domain d
    WHERE c.id = r.id AND d.allowed
    RETURNING c.id
)
SELECT d.allowed, (SELECT count(*) FROM updated),
       ARRAY(SELECT g.epg_id FROM guides g WHERE d.allowed AND g.source_type IS DISTINCT FROM 'dummy')
FROM domain d
";

fn required() -> StoreError {
    StoreError::InvalidInput(json!({"error": "associations list is required"}))
}

fn unsupported() -> StoreError {
    StoreError::Unsupported("Batch EPG assignment is limited to dummy guides and clearing")
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

pub(super) async fn set(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Json<Value>, StoreError> {
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
        let (allowed, updated, programmes): (bool, i64, Vec<i64>) = sqlx::query_as(UPDATE)
            .bind(channels)
            .bind(epgs)
            .bind(publisher_ready)
            .fetch_one(auth.store.pool())
            .await?;
        if !allowed {
            return Err(unsupported());
        }
        // Original bulk_update commits before dispatching once per changed
        // guide. It skips the single-channel model save and cache signal.
        let programs_refreshed = programmes.len();
        for guide_id in programmes {
            crate::core_settings_special_broker::publish_tvg_programmes(&auth, guide_id).await?;
        }
        Ok(Json(
            json!({"success": true, "channels_updated": updated, "programs_refreshed": programs_refreshed}),
        ))
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
