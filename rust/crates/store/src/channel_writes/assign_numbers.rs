//! Original ordered channel assignment and shared effective-number writes.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use super::{body_object, python_repr, Body};
use crate::{json_compat, AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, State},
    http::{header, HeaderMap},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use sqlx::PgConnection;
use std::{collections::HashMap, net::SocketAddr};

#[derive(Debug, PartialEq)]
pub(super) enum NumberWrite {
    Unchanged,
    Raw,
    Pin,
    ClearPin,
}

pub(super) fn number_write(
    managed: bool,
    raw: Option<f64>,
    pinned: Option<f64>,
    desired: f64,
) -> NumberWrite {
    if !managed {
        if raw == Some(desired) {
            NumberWrite::Unchanged
        } else {
            NumberWrite::Raw
        }
    } else if raw == Some(desired) {
        if pinned.is_some() {
            NumberWrite::ClearPin
        } else {
            NumberWrite::Unchanged
        }
    } else if pinned == Some(desired) {
        NumberWrite::Unchanged
    } else {
        NumberWrite::Pin
    }
}

pub(super) async fn lock_numbers(connection: &mut PgConnection) -> Result<(), StoreError> {
    // A range reorder and an explicit assignment can touch the same rows.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('dispatcharr.channels.reorder'), 0)")
        .execute(connection)
        .await?;
    Ok(())
}

pub(super) async fn prune_empty_pins(
    connection: &mut PgConnection,
    ids: &[i64],
) -> Result<(), StoreError> {
    sqlx::query(
        "DELETE FROM dispatcharr_channels_channeloverride
         WHERE channel_id = ANY($1) AND channel_number IS NULL AND name IS NULL
           AND channel_group_id IS NULL AND logo_id IS NULL AND tvg_id IS NULL
           AND tvc_guide_stationid IS NULL AND epg_data_id IS NULL
           AND stream_profile_id IS NULL AND is_radio IS NULL",
    )
    .bind(ids)
    .execute(connection)
    .await?;
    Ok(())
}

fn invalid(message: String) -> StoreError {
    StoreError::InvalidInput(json!({"error": message}))
}

fn channel_ids(value: Option<&Value>) -> Result<Vec<i64>, StoreError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let ids = value
        .as_array()
        .ok_or_else(|| invalid("channel_ids must be a list".into()))?;
    let mut result = Vec::with_capacity(ids.len());
    for value in ids {
        let parsed = json_compat::python_json_int(value)
            .map_err(|_| invalid(format!("Invalid channel id: {}", python_repr(value))))?;
        // IDs outside PostgreSQL's integer width cannot match a channel.
        result.extend(parsed.as_i64());
    }
    Ok(result)
}

fn start(value: Option<&Value>) -> f64 {
    value
        .and_then(|value| match value {
            Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
            Value::String(value) => dispatcharr_ingest_core::source_float_literal(value),
            _ => value.as_f64(),
        })
        .unwrap_or(1.0)
}

#[derive(Clone, Copy)]
struct ChannelNumber {
    managed: bool,
    raw: Option<f64>,
    pinned: Option<f64>,
}

fn assignments(
    requested: &[i64],
    channels: &HashMap<i64, ChannelNumber>,
    mut next: f64,
) -> Vec<(i64, f64)> {
    let mut assigned = Vec::<(i64, f64)>::new();
    let mut positions = HashMap::new();
    for &id in requested {
        if !channels.contains_key(&id) {
            continue;
        }
        if let Some(&index) = positions.get(&id) {
            let row: &mut (i64, f64) = &mut assigned[index];
            row.1 = next;
        } else {
            positions.insert(id, assigned.len());
            assigned.push((id, next));
        }
        next += 1.0;
    }
    assigned
}

async fn write_assignments(
    tx: &mut PgConnection,
    channels: &HashMap<i64, ChannelNumber>,
    numbers: Vec<(i64, f64)>,
) -> Result<(), StoreError> {
    let (mut raw, mut pins, mut clear) = (Vec::new(), Vec::new(), Vec::new());
    for (id, number) in numbers {
        let channel = channels[&id];
        match number_write(channel.managed, channel.raw, channel.pinned, number) {
            NumberWrite::Unchanged => {}
            NumberWrite::Raw => raw.push((id, number)),
            NumberWrite::Pin => pins.push((id, number)),
            NumberWrite::ClearPin => clear.push(id),
        }
    }
    if !raw.is_empty() {
        let (ids, values): (Vec<_>, Vec<_>) = raw.into_iter().unzip();
        sqlx::query(
            "UPDATE dispatcharr_channels_channel AS c SET channel_number = n.number
             FROM unnest($1::bigint[], $2::double precision[]) AS n(id, number)
             WHERE c.id = n.id",
        )
        .bind(ids)
        .bind(values)
        .execute(&mut *tx)
        .await?;
    }
    if !pins.is_empty() {
        let (ids, values): (Vec<_>, Vec<_>) = pins.into_iter().unzip();
        sqlx::query(
            "UPDATE dispatcharr_channels_channeloverride AS o SET channel_number = n.number
             FROM unnest($1::bigint[], $2::double precision[]) AS n(id, number)
             WHERE o.channel_id = n.id",
        )
        .bind(&ids)
        .bind(&values)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO dispatcharr_channels_channeloverride
                (channel_id, channel_number, created_at, updated_at)
             SELECT n.id, n.number, statement_timestamp(), statement_timestamp()
             FROM unnest($1::bigint[], $2::double precision[]) WITH ORDINALITY AS n(id, number, ordinal)
             WHERE NOT EXISTS (
                 SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.channel_id = n.id
             )
             ORDER BY n.ordinal
             ON CONFLICT(channel_id) DO UPDATE SET channel_number=EXCLUDED.channel_number",
        )
        .bind(ids)
        .bind(values)
        .execute(&mut *tx)
        .await?;
    }
    if !clear.is_empty() {
        sqlx::query(
            "UPDATE dispatcharr_channels_channeloverride SET channel_number=NULL WHERE channel_id = ANY($1)",
        )
        .bind(&clear)
        .execute(&mut *tx)
        .await?;
        prune_empty_pins(tx, &clear).await?;
    }
    Ok(())
}

pub(crate) async fn handle(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let body = body_object(body)?;
    let ids = channel_ids(body.get("channel_ids"))?;
    let starting_number = start(body.get("starting_number"));
    let _writer = auth.task_sql_writer()?;
    if !ids.is_empty() {
        let mut tx = auth.store.pool().begin().await?;
        lock_numbers(&mut tx).await?;
        let rows: Vec<(i64, bool, Option<f64>, Option<f64>)> = sqlx::query_as(
            "SELECT c.id::bigint, c.auto_created, c.channel_number, o.channel_number
             FROM dispatcharr_channels_channel c
             LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
             WHERE c.id = ANY($1)
             ORDER BY c.id FOR UPDATE OF c",
        )
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
        let channels: HashMap<_, _> = rows
            .into_iter()
            .map(|(id, managed, raw, pinned)| {
                (
                    id,
                    ChannelNumber {
                        managed,
                        raw,
                        pinned,
                    },
                )
            })
            .collect();
        write_assignments(
            &mut tx,
            &channels,
            assignments(&ids, &channels, starting_number),
        )
        .await?;
        tx.commit().await?;
    }
    Ok((
        [(header::ALLOW, "POST, OPTIONS")],
        Json(json!({"message": "Channels have been auto-assigned!"})),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignments_use_request_order_and_only_present_ids_consume_numbers() {
        let row = ChannelNumber {
            managed: false,
            raw: None,
            pinned: None,
        };
        let channels = HashMap::from([(1, row), (3, row), (4, row)]);
        assert_eq!(
            assignments(&[4, 19, 1, 4, 3], &channels, 20.5),
            vec![(4, 22.5), (1, 21.5), (3, 23.5)]
        );
        assert_eq!(start(None), 1.0);
        assert_eq!(start(Some(&json!("20.5"))), 20.5);
        assert_eq!(start(Some(&json!(" ２_０.٥ "))), 20.5);
        assert_eq!(start(Some(&json!("not a number"))), 1.0);
        assert_eq!(
            channel_ids(Some(&json!([4, "1", 4]))).unwrap(),
            vec![4, 1, 4]
        );
    }

    #[test]
    fn managed_numbers_preserve_raw_and_clear_only_a_present_number_pin() {
        assert_eq!(
            number_write(false, Some(99.0), None, 21.5),
            NumberWrite::Raw
        );
        assert_eq!(number_write(true, Some(99.0), None, 21.5), NumberWrite::Pin);
        assert_eq!(
            number_write(true, Some(99.0), Some(22.5), 99.0),
            NumberWrite::ClearPin
        );
        assert_eq!(
            number_write(true, Some(99.0), None, 99.0),
            NumberWrite::Unchanged
        );
        assert_eq!(
            number_write(true, Some(99.0), Some(22.5), 22.5),
            NumberWrite::Unchanged
        );
    }
}
