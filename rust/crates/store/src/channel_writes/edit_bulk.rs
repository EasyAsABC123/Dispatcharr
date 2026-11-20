//! Original Dispatcharr Channel Batch group and mature flag updates.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use super::{boolean, mark_group};
use crate::{channels, AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, OriginalUri, Query, Request, State},
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder, Transaction};
use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
};

#[derive(Debug, PartialEq, Eq)]
struct Edit {
    id: i64,
    group: Option<i64>,
    override_group: Option<i64>,
    mature: Option<bool>,
}

fn unsupported() -> StoreError {
    StoreError::Unsupported("This Channel Batch field is not implemented in Native.")
}

fn group(value: &Value) -> Result<i64, StoreError> {
    value.as_i64().filter(|id| *id > 0).ok_or_else(unsupported)
}

fn edits(value: Value) -> Result<Vec<Edit>, StoreError> {
    let Value::Array(items) = value else {
        return Err(StoreError::InvalidInput(
            json!({"error":"Expected a list of channel updates"}),
        ));
    };
    // Dict replacement keeps the first position while applying the last
    // item. Keep the response order separate from PostgreSQL lock order.
    let mut ordered = Vec::with_capacity(items.len());
    let mut positions = HashMap::with_capacity(items.len());
    let mut overrides = HashMap::new();
    let mut missing = Vec::new();
    for (index, item) in items.into_iter().enumerate() {
        let row = item.as_object().ok_or_else(unsupported)?;
        let Some(id) = row.get("id").and_then(Value::as_i64).filter(|id| *id != 0) else {
            missing.push(format!("Item {index}: Channel ID is required"));
            continue;
        };
        if let Some(value) = row.get("override") {
            overrides.insert(id, value.clone());
        }
        if let Some(&position) = positions.get(&id) {
            ordered[position] = (id, item);
        } else {
            positions.insert(id, ordered.len());
            ordered.push((id, item));
        }
    }
    if !missing.is_empty() {
        return Err(StoreError::InvalidInput(json!({"errors":missing})));
    }
    ordered
        .into_iter()
        .map(|(id, item)| {
            let row = item.as_object().expect("checked batch object");
            if row.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "id" | "channel_group_id" | "is_adult" | "override"
                )
            }) {
                return Err(unsupported());
            }
            let override_group = match overrides.get(&id) {
                None => None,
                Some(Value::Object(values)) if values.is_empty() => None,
                Some(Value::Object(values)) if values.len() == 1 => Some(group(
                    values.get("channel_group_id").ok_or_else(unsupported)?,
                )?),
                _ => return Err(unsupported()),
            };
            Ok(Edit {
                id,
                group: row.get("channel_group_id").map(group).transpose()?,
                override_group,
                mature: row
                    .get("is_adult")
                    .map(|value| boolean("is_adult", value))
                    .transpose()?,
            })
        })
        .collect()
}

async fn validate_and_lock(
    tx: &mut Transaction<'_, Postgres>,
    edits: &[Edit],
    ids: &[i64],
) -> Result<HashMap<i64, bool>, StoreError> {
    // Group update/delete already starts with the group lock. Match
    // that order before taking the channel rows in canonical order.
    let requested_groups: Vec<i64> = edits
        .iter()
        .flat_map(|edit| [edit.group, edit.override_group].into_iter().flatten())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let existing_groups: BTreeSet<i64> = sqlx::query_scalar(
        "SELECT id FROM dispatcharr_channels_channelgroup
         WHERE id = ANY($1) ORDER BY id FOR KEY SHARE",
    )
    .bind(&requested_groups)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .collect();
    let selected: HashMap<i64, bool> = sqlx::query_as(
        "SELECT id, auto_created FROM dispatcharr_channels_channel
         WHERE id = ANY($1) ORDER BY id FOR UPDATE",
    )
    .bind(ids)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .collect();
    let mut errors = Vec::new();
    for edit in edits {
        let Some(managed) = selected.get(&edit.id) else {
            errors.push(json!({"channel_id":edit.id,"error":"Channel not found"}));
            continue;
        };
        if !managed && edit.override_group.is_some() {
            return Err(unsupported());
        }
        let mut fields = serde_json::Map::new();
        for (id, nested) in [(edit.group, false), (edit.override_group, true)] {
            if let Some(missing) = id.filter(|id| !existing_groups.contains(id)) {
                let message = format!("Invalid pk \"{missing}\" - object does not exist.");
                if nested {
                    fields.insert("override".into(), json!({"channel_group_id":[message]}));
                } else {
                    fields.insert("channel_group_id".into(), json!([message]));
                }
            }
        }
        if !fields.is_empty() {
            errors.push(json!({"channel_id":edit.id,"errors":fields}));
        }
    }
    if errors.is_empty() {
        Ok(selected)
    } else {
        Err(StoreError::InvalidInput(
            json!({"updated_count":edits.len()-errors.len(),"errors":errors}),
        ))
    }
}

const CHANNEL_BATCH_SIZE: usize = 100;
const OVERRIDE_BATCH_SIZE: usize = 200;

async fn apply(
    tx: &mut Transaction<'_, Postgres>,
    edits: &[Edit],
    selected: &HashMap<i64, bool>,
) -> Result<(), StoreError> {
    let mut manual_groups = BTreeSet::new();
    let mut scalar_edits = Vec::new();
    let mut override_edits = Vec::new();
    for edit in edits {
        if edit.group.is_some() || edit.mature.is_some() {
            scalar_edits.push(edit);
        }
        if !selected[&edit.id] {
            manual_groups.extend(edit.group);
        }
        if let Some(group) = edit.override_group {
            override_edits.push((edit.id, group));
        }
    }
    for chunk in scalar_edits.chunks(CHANNEL_BATCH_SIZE) {
        let ids: Vec<_> = chunk.iter().map(|edit| edit.id).collect();
        let groups: Vec<_> = chunk.iter().map(|edit| edit.group).collect();
        let mature: Vec<_> = chunk.iter().map(|edit| edit.mature).collect();
        // Stock bulk_update leaves channel row clocks unchanged.
        sqlx::query(
            "UPDATE dispatcharr_channels_channel AS channel
             SET channel_group_id = COALESCE(change.group_id, channel.channel_group_id),
                 is_adult = COALESCE(change.mature, channel.is_adult)
             FROM unnest($1::bigint[], $2::bigint[], $3::boolean[]) AS change(id, group_id, mature)
             WHERE channel.id = change.id",
        )
        .bind(ids)
        .bind(groups)
        .bind(mature)
        .execute(&mut **tx)
        .await?;
    }
    for group in manual_groups {
        mark_group(tx, &json!(group)).await?;
    }
    for chunk in override_edits.chunks(OVERRIDE_BATCH_SIZE) {
        let (ids, groups): (Vec<_>, Vec<_>) = chunk.iter().copied().unzip();
        // A data-modifying CTE separates existing rows from newly inserted
        // overlays without spending an identity or updating clocks on a hit.
        sqlx::query(
            "WITH incoming AS (
                 SELECT * FROM unnest($1::bigint[], $2::bigint[]) AS change(id, group_id)
             ), existing AS (
                 UPDATE dispatcharr_channels_channeloverride AS overlay
                 SET channel_group_id = incoming.group_id
                 FROM incoming WHERE overlay.channel_id = incoming.id
                 RETURNING overlay.channel_id
             )
             INSERT INTO dispatcharr_channels_channeloverride
                 (channel_id, channel_group_id, created_at, updated_at)
             SELECT incoming.id, incoming.group_id, clock_timestamp(), clock_timestamp()
             FROM incoming
             WHERE NOT EXISTS (SELECT 1 FROM existing WHERE existing.channel_id = incoming.id)",
        )
        .bind(ids)
        .bind(groups)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

fn deadlock(error: &StoreError) -> bool {
    matches!(error, StoreError::Database(sqlx::Error::Database(error))
        if error.code().as_deref() == Some("40P01"))
}

async fn write(
    auth: &AuthService,
    edits: &[Edit],
    ids: &[i64],
    include: bool,
    base: &url::Url,
) -> Result<Vec<Value>, StoreError> {
    for attempt in 0..3 {
        let mut tx = auth.store.pool().begin().await?;
        let result = update_and_read(&mut tx, edits, ids, include, base).await;
        match result {
            Ok(rows) => {
                // A commit error can have an uncertain outcome. Never retry it.
                tx.commit().await?;
                return Ok(rows);
            }
            Err(error) if attempt < 2 && deadlock(&error) => tx.rollback().await?,
            Err(error) => return Err(error),
        }
    }
    unreachable!("final transaction attempt returns its result")
}

async fn update_and_read(
    tx: &mut Transaction<'_, Postgres>,
    edits: &[Edit],
    ids: &[i64],
    include: bool,
    base: &url::Url,
) -> Result<Vec<Value>, StoreError> {
    let selected = validate_and_lock(tx, edits, ids).await?;
    apply(tx, edits, &selected).await?;
    let fetched = QueryBuilder::new(channels::SELECT)
        .push(" AND c.id = ANY(")
        .push_bind(ids)
        .push(")")
        .build()
        .fetch_all(&mut **tx)
        .await?;
    let mut rows = HashMap::with_capacity(fetched.len());
    for row in fetched {
        let serialized = channels::serialize(row, include, true, base)?;
        let id = serialized["id"]
            .as_i64()
            .ok_or(StoreError::UnhandledRestFault)?;
        rows.insert(id, serialized);
    }
    ids.iter()
        .map(|id| rows.remove(id).ok_or(StoreError::UnhandledRestFault))
        .collect()
}

pub(crate) async fn handle(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    request: Request,
) -> Result<Response, StoreError> {
    let permit = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(permit)?;
    tokio::spawn(async move {
        let headers = request.headers().clone();
        auth.account_user(peer, headers.clone(), true).await?;
        let base = channels::base_url(&headers, &uri)?;
        let include = params.get("include_streams").is_some_and(|value| value == "true");
        let edits = edits(channels::mutation_value(request).await?)?;
        let ids: Vec<i64> = edits.iter().map(|edit| edit.id).collect();
        let result = write(&auth, &edits, &ids, include, &base).await?;
        Ok((
            [(header::ALLOW, "PATCH, OPTIONS"), (header::VARY, "Accept, origin")],
            Json(json!({"message":format!("Successfully updated {} channels", result.len()),"channels":result})),
        ).into_response())
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_react_group_and_mature_fields_keep_input_order() {
        assert_eq!(
            edits(json!([
                {"id":21,"override":{"channel_group_id":3},"is_adult":true},
                {"id":14,"channel_group_id":3,"is_adult":true}
            ]))
            .unwrap(),
            vec![
                Edit {
                    id: 21,
                    group: None,
                    override_group: Some(3),
                    mature: Some(true)
                },
                Edit {
                    id: 14,
                    group: Some(3),
                    override_group: None,
                    mature: Some(true)
                },
            ]
        );
    }

    #[test]
    fn duplicate_channel_uses_last_payload_at_its_first_position() {
        let result = edits(json!([
            {"id":21,"is_adult":true},
            {"id":14,"channel_group_id":3},
            {"id":21,"is_adult":false}
        ]))
        .unwrap();
        assert_eq!(result[0].id, 21);
        assert_eq!(result[0].mature, Some(false));
        assert_eq!(result[1].id, 14);
    }

    #[test]
    fn duplicate_channel_keeps_last_explicit_override_independently_of_scalars() {
        let result = edits(json!([
            {"id":21,"override":{"channel_group_id":3},"is_adult":true},
            {"id":14,"override":{"channel_group_id":3}},
            {"id":21,"override":{"channel_group_id":4}},
            {"id":14,"override":{}},
            {"id":21,"is_adult":false},
            {"id":14,"is_adult":true}
        ]))
        .unwrap();
        assert_eq!(
            result[0],
            Edit {
                id: 21,
                group: None,
                override_group: Some(4),
                mature: Some(false)
            }
        );
        assert_eq!(
            result[1],
            Edit {
                id: 14,
                group: None,
                override_group: None,
                mature: Some(true)
            }
        );
    }

    #[test]
    fn unrelated_mutations_cannot_be_silently_accepted() {
        for request in [
            json!([{"id":1,"streams":[4]}]),
            json!([{"id":1,"override":null}]),
            json!([{"id":1,"override":{"channel_group_id":3,"epg_data_id":7}}]),
        ] {
            assert!(matches!(edits(request), Err(StoreError::Unsupported(_))));
        }
    }
}
