//! Source-derived cached provider-info extension and host controls.
//! Actual Source90 TCP and owned-resource gates passed; canonical app,
//! provider refresh, image serving and full React modal gates are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_actions_account as account, vod_api_actions_sql as sql,
    vod_api_owned_read::{OwnedConnection, ReadError, ReadWork},
    vod_api_provider_info_dto as dto, vod_api_provider_info_policy as policy,
    vod_api_provider_info_sql as info_sql,
};
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Row};
use std::collections::HashMap;
fn fault(fault: dto::Fault) -> ReadError {
    match fault {
        dto::Fault::Framework => ReadError::SourceUnhandledFault("provider info projection failed"),
        dto::Fault::Capacity => ReadError::Capacity,
        dto::Fault::Series(message) => crate::StoreError::StatusBody(
            500,
            json!({"error":format!("Failed to fetch series information: {message}")}),
        )
        .into(),
    }
}
fn policy_fault(fault: policy::Fault) -> ReadError {
    match fault {
        policy::Fault::Framework => {
            ReadError::SourceUnhandledFault("provider info properties failed")
        }
        policy::Fault::Series(message) => crate::StoreError::StatusBody(
            500,
            json!({"error":format!("Failed to fetch series information: {message}")}),
        )
        .into(),
        policy::Fault::Capacity => ReadError::Capacity,
        policy::Fault::Uncaptured(message) => ReadError::Unsupported(message),
    }
}
fn budget(work: &ReadWork, value: &Value) -> Result<(), ReadError> {
    if account::heap(value)? > work.work_bytes() / 8 {
        Err(ReadError::Capacity)
    } else {
        Ok(())
    }
}
/// Snapshot the already-selected PgRow; NEVER replace the parent or its joined
/// logo with a later query. This is the Source get_object observation boundary.
fn parent(row: &PgRow, series: bool) -> Result<(Value, Option<Value>), ReadError> {
    let mut value = Map::new();
    for field in ["id", "year"] {
        let raw: Option<i64> = row.try_get(format!("content_{field}").as_str())?;
        value.insert(field.into(), raw.into());
    }
    for field in [
        "uuid",
        "name",
        "description",
        "rating",
        "genre",
        "tmdb_id",
        "imdb_id",
        "language",
    ] {
        let raw: Option<String> = row.try_get(format!("content_{field}").as_str())?;
        value.insert(field.into(), raw.into());
    }
    value.insert(
        "custom_properties".into(),
        row.try_get::<Option<Value>, _>("content_custom_properties")?
            .unwrap_or(Value::Null),
    );
    for field in ["created_at", "updated_at"] {
        let raw: chrono::DateTime<chrono::Utc> =
            row.try_get(format!("content_{field}").as_str())?;
        value.insert(field.into(), raw.to_rfc3339().into());
    }
    if !series {
        value.insert(
            "duration_secs".into(),
            row.try_get::<Option<i64>, _>("content_duration_secs")?
                .into(),
        );
    }
    let logo = if let Some(id) = row.try_get::<Option<i64>, _>("logo_id")? {
        Some(
            json!({"id":id,"name":row.try_get::<String,_>("logo_name")?,"url":row.try_get::<String,_>("logo_url")?}),
        )
    } else {
        None
    };
    Ok((value.into(), logo))
}
pub(super) async fn read(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    series: bool,
    id: i64,
    row: &PgRow,
    query: &[(String, String)],
    origin: &str,
) -> Result<Value, ReadError> {
    let (parent, logo) = parent(row, series)?;
    budget(work, &parent)?;
    let relation_id = query
        .iter()
        .rev()
        .find(|(key, _)| key == "relation_id")
        .map(|(_, value)| value);
    let selected = match relation_id {
        Some(value) => {
            let text = dispatcharr_ingest_core::source_integer_literal(value).ok_or_else(|| {
                crate::StoreError::StatusBody(400, json!({"error":"Invalid relation_id"}))
            })?;
            Some(text.parse::<i64>().map_err(|_| {
                crate::StoreError::StatusBody(
                    404,
                    json!({"error":"Relation not found or not active"}),
                )
            })?)
        }
        None => None,
    };
    let relation = info_sql::relation(work, connection, series, id, selected).await?;
    let Some(relation) = relation else {
        return Err(crate::StoreError::StatusBody(if selected.is_some(){404}else{400},json!({"error":if selected.is_some(){"Relation not found or not active".to_owned()}else{format!("No active M3U account associated with this {}",if series{"series"}else{"movie"})}})).into());
    };
    let eligibility = if series {
        policy::series(&relation["relation"], query, chrono::Utc::now())
    } else {
        policy::movie(&relation["relation"], query, chrono::Utc::now())
    }
    .map_err(policy_fault)?;
    if eligibility == policy::Eligibility::RefreshRequired {
        return Err(ReadError::Unsupported(
            "Provider refresh execution is not ported; cached-only lane cannot serve stale data",
        ));
    }
    let mut result = if series {
        dto::series_header(
            &parent,
            &relation["relation"],
            &relation["account"],
            relation.get("category").filter(|value| !value.is_null()),
            logo.as_ref(),
            origin,
        )
    } else {
        dto::movie(
            &parent,
            &relation["relation"],
            &relation["account"],
            logo.as_ref(),
            origin,
        )
    }
    .map_err(fault)?;
    if series && policy::include_episodes(query) {
        let account_id = relation["account"]["id"]
            .as_i64()
            .ok_or(ReadError::WorkerFailed)?;
        // Source's provider episode relation dictionary is fully constructed before
        // the ordered Episode query; duplicate relation IDs use last emitted row.
        let source=format!("SELECT jsonb_build_object('id',r.id,'episode_id',r.episode_id,'container_extension',r.container_extension,'custom_properties',r.custom_properties) AS payload FROM vod_m3uepisoderelation r JOIN vod_episode e ON e.id=r.episode_id WHERE r.m3u_account_id=$1 AND e.series_id={id}");
        let rows = sql::snapshot(
            work,
            connection,
            &source,
            account_id,
            work.work_bytes() / 16,
        )
        .await?;
        let mut relations = HashMap::new();
        let mut episode_ids = Vec::new();
        let mut retained = 0usize;
        for row in rows.into_rows() {
            let value = sql::decode(work, &row)?;
            let episode = value["episode_id"]
                .as_i64()
                .ok_or(ReadError::WorkerFailed)?;
            retained = retained
                .checked_add(account::heap(&value)?)
                .and_then(|n| n.checked_add(512))
                .ok_or(ReadError::Capacity)?;
            if retained > work.work_bytes() / 16 {
                return Err(ReadError::Capacity);
            }
            if !relations.contains_key(&episode) {
                episode_ids.push(episode);
            }
            relations.insert(episode, value);
        }
        let mut seasons = Map::new();
        let mut episode_heap = 0usize;
        if !relations.is_empty() {
            let ids = episode_ids
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let source=format!("SELECT to_jsonb(e) AS payload FROM vod_episode e WHERE e.series_id=$1 AND e.id IN ({ids}) ORDER BY e.season_number,e.episode_number");
            let rows = sql::snapshot(work, connection, &source, id, work.work_bytes() / 16).await?;
            for row in rows.into_rows() {
                work.check()?;
                let episode = sql::decode(work, &row)?;
                let episode_id = episode["id"].as_i64().ok_or(ReadError::WorkerFailed)?;
                let relation = relations
                    .remove(&episode_id)
                    .ok_or(ReadError::WorkerFailed)?;
                let projected = dto::episode(&episode, &relation, &parent, account_id, origin)
                    .map_err(fault)?;
                episode_heap = episode_heap
                    .checked_add(account::heap(&projected)?)
                    .and_then(|n| n.checked_add(1024))
                    .ok_or(ReadError::Capacity)?;
                if episode_heap > work.work_bytes() / 16 {
                    return Err(ReadError::Capacity);
                }
                let key = episode["season_number"].as_i64().unwrap_or(0).to_string();
                seasons
                    .entry(key)
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .ok_or(ReadError::WorkerFailed)?
                    .push(projected);
            }
        }
        result
            .as_object_mut()
            .ok_or(ReadError::WorkerFailed)?
            .insert("episodes".into(), seasons.into());
    }
    budget(work, &result)?;
    Ok(result)
}
