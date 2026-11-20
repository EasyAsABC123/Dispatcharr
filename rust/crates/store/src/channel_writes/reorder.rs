//! Dispatcharr channel reorder and managed number pins.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use super::{body_object, channel_object_query, missing_channel, Body};
use crate::{channels, json_compat, AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, OriginalUri, Path, Query, State},
    http::{header, HeaderMap},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use sqlx::PgConnection;
use std::{collections::HashMap, net::SocketAddr};

#[derive(Debug, PartialEq)]
struct Shift {
    delta: f64,
    lower: f64,
    include_lower: bool,
    upper: Option<f64>,
}

#[derive(Debug, PartialEq)]
enum Placement {
    Unchanged,
    Move { number: f64, shift: Option<Shift> },
}

fn place(old: Option<f64>, desired: f64) -> Placement {
    let upward = |upper| Placement::Move {
        number: desired,
        shift: Some(Shift {
            delta: 1.0,
            lower: desired,
            include_lower: true,
            upper,
        }),
    };
    match old {
        None => upward(None),
        Some(old) if old == desired => Placement::Unchanged,
        Some(old) if desired < old => upward(Some(old)),
        Some(old) => Placement::Move {
            number: desired - 1.0,
            shift: (desired > old + 1.0).then_some(Shift {
                delta: -1.0,
                lower: old,
                include_lower: false,
                upper: Some(desired),
            }),
        },
    }
}

fn missing_target() -> StoreError {
    StoreError::StatusBody(404, json!({"error":"Target channel not found"}))
}

fn anchor(value: Option<&Value>) -> Result<Option<i64>, StoreError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => json_compat::python_json_int(value)
            .ok()
            .and_then(|value| value.as_i64())
            .map(Some)
            .ok_or_else(missing_target),
    }
}

async fn shift(connection: &mut PgConnection, moved: i64, shift: Shift) -> Result<(), StoreError> {
    sqlx::query(
        "UPDATE dispatcharr_channels_channel AS c
         SET channel_number = c.channel_number + $1
         WHERE c.id <> $2 AND c.channel_number >= $3
           AND ($4 OR c.channel_number > $3)
           AND ($5::double precision IS NULL OR c.channel_number < $5)
           AND (NOT c.auto_created OR NOT EXISTS (
             SELECT 1 FROM dispatcharr_channels_channeloverride AS o
             WHERE o.channel_id = c.id AND o.channel_number IS NOT NULL
           ))",
    )
    .bind(shift.delta)
    .bind(moved)
    .bind(shift.lower)
    .bind(shift.include_lower)
    .bind(shift.upper)
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "UPDATE dispatcharr_channels_channeloverride AS o
         SET channel_number = o.channel_number + $1
         WHERE o.channel_id <> $2 AND o.channel_number >= $3
           AND ($4 OR o.channel_number > $3)
           AND ($5::double precision IS NULL OR o.channel_number < $5)",
    )
    .bind(shift.delta)
    .bind(moved)
    .bind(shift.lower)
    .bind(shift.include_lower)
    .bind(shift.upper)
    .execute(connection)
    .await?;
    Ok(())
}

async fn assign(
    connection: &mut PgConnection,
    id: i64,
    managed: bool,
    raw: Option<f64>,
    pinned: Option<f64>,
    number: f64,
) -> Result<(), StoreError> {
    if !managed {
        if raw != Some(number) {
            sqlx::query("UPDATE dispatcharr_channels_channel SET channel_number=$2 WHERE id=$1")
                .bind(id)
                .bind(number)
                .execute(connection)
                .await?;
        }
    } else if raw == Some(number) {
        if pinned.is_some() {
            sqlx::query(
                "UPDATE dispatcharr_channels_channeloverride SET channel_number=NULL WHERE channel_id=$1",
            )
            .bind(id)
            .execute(&mut *connection)
            .await?;
            sqlx::query(
                "DELETE FROM dispatcharr_channels_channeloverride
                 WHERE channel_id=$1 AND channel_number IS NULL AND name IS NULL
                   AND channel_group_id IS NULL AND logo_id IS NULL AND tvg_id IS NULL
                   AND tvc_guide_stationid IS NULL AND epg_data_id IS NULL
                   AND stream_profile_id IS NULL AND is_radio IS NULL",
            )
            .bind(id)
            .execute(connection)
            .await?;
        }
    } else if pinned != Some(number) {
        // Updating a present pin keeps its row, clocks and unrelated fields.
        let updated = sqlx::query(
            "UPDATE dispatcharr_channels_channeloverride SET channel_number=$2 WHERE channel_id=$1",
        )
        .bind(id)
        .bind(number)
        .execute(&mut *connection)
        .await?;
        if updated.rows_affected() == 0 {
            sqlx::query(
                "INSERT INTO dispatcharr_channels_channeloverride
                    (channel_id,channel_number,created_at,updated_at)
                 VALUES($1,$2,statement_timestamp(),statement_timestamp())
                 ON CONFLICT(channel_id) DO UPDATE SET channel_number=EXCLUDED.channel_number",
            )
            .bind(id)
            .bind(number)
            .execute(connection)
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn handle(
    State(auth): State<AuthService>,
    Path(raw_id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers.clone(), true).await?;
    let id = dispatcharr_ingest_core::source_integer_literal(&raw_id)
        .ok_or_else(missing_channel)?
        .parse()
        .map_err(|_| missing_channel())?;
    let base = channels::base_url(&headers, &uri)?;
    let mut scoped = channel_object_query(&auth, &user, id, &params).await?;
    let _writer = auth.task_sql_writer()?;
    let mut tx = auth.store.pool().begin().await?;

    // Moves can touch channels across groups. Keep overlapping reorder
    // requests serial while selecting the range and writing it atomically.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('dispatcharr.channels.reorder'), 0)")
        .execute(&mut *tx)
        .await?;
    let original = scoped
        .push(" FOR UPDATE OF c")
        .build()
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(missing_channel)?;
    let body = body_object(body)?;
    let after = anchor(body.get("insert_after_id"))?;
    let (raw, managed, name, pinned): (Option<f64>, bool, String, Option<f64>) = sqlx::query_as(
        "SELECT c.channel_number,c.auto_created,c.name,o.channel_number
         FROM dispatcharr_channels_channel c
         LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
         WHERE c.id=$1",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    let desired = if let Some(after) = after {
        let effective: Option<f64> = sqlx::query_scalar(
            "SELECT COALESCE(o.channel_number,c.channel_number)
             FROM dispatcharr_channels_channel c
             LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
             WHERE c.id=$1",
        )
        .bind(after)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(missing_target)?;
        effective.unwrap_or(0.0).trunc() + 1.0
    } else {
        1.0
    };
    if !desired.is_finite() {
        return Err(StoreError::UnhandledRestFault);
    }
    let (row, action, number) = match place(pinned.or(raw), desired) {
        Placement::Unchanged => (original, "already at position", desired),
        Placement::Move {
            number,
            shift: range,
        } => {
            if let Some(range) = range {
                shift(&mut tx, id, range).await?;
            }
            assign(&mut tx, id, managed, raw, pinned, number).await?;
            let row = sqlx::QueryBuilder::new(channels::SELECT)
                .push(" AND c.id=")
                .push_bind(id)
                .build()
                .fetch_one(&mut *tx)
                .await?;
            (row, "moved to position", number)
        }
    };
    let include = params
        .get("include_streams")
        .is_some_and(|value| value == "true");
    let channel = channels::serialize(row, include, true, &base)?;
    tx.commit().await?;
    let number = json_compat::float_string(number);
    Ok((
        [(header::ALLOW, "POST, OPTIONS")],
        Json(json!({"message":format!("Channel {name} {action} {number}"),"channel":channel})),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_moves_use_the_original_open_and_closed_ranges() {
        assert_eq!(
            place(Some(1001.0), 1006.0),
            Placement::Move {
                number: 1005.0,
                shift: Some(Shift {
                    delta: -1.0,
                    lower: 1001.0,
                    include_lower: false,
                    upper: Some(1006.0)
                }),
            },
        );
        assert_eq!(
            place(Some(1007.0), 1002.0),
            Placement::Move {
                number: 1002.0,
                shift: Some(Shift {
                    delta: 1.0,
                    lower: 1002.0,
                    include_lower: true,
                    upper: Some(1007.0)
                }),
            },
        );
        assert_eq!(place(Some(1.0), 1.0), Placement::Unchanged);
        assert_eq!(
            place(Some(2.0), 3.0),
            Placement::Move {
                number: 2.0,
                shift: None
            },
        );
        assert_eq!(
            place(None, 3.0),
            Placement::Move {
                number: 3.0,
                shift: Some(Shift {
                    delta: 1.0,
                    lower: 3.0,
                    include_lower: true,
                    upper: None
                }),
            },
        );
    }

    #[test]
    fn nullable_insert_after_selects_the_start() {
        assert_eq!(anchor(None).unwrap(), None);
        assert_eq!(anchor(Some(&Value::Null)).unwrap(), None);
        assert_eq!(anchor(Some(&json!(17))).unwrap(), Some(17));
        assert_eq!(anchor(Some(&json!("17"))).unwrap(), Some(17));
    }
}
