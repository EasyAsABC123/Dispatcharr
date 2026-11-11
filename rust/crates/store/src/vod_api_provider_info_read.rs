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
    // Source get_object retains the original ForeignKey even when a later
    // metadata refresh performs a full Series.save(). Projection-only logo
    // data is insufficient: missing fields in jsonb_populate_record become NULL.
    value.insert(
        "logo_id".into(),
        row.try_get::<Option<i64>, _>("logo_id")?.into(),
    );
    let logo = if let Some(id) = row.try_get::<Option<i64>, _>("logo_id")? {
        Some(
            json!({"id":id,"name":row.try_get::<String,_>("logo_name")?,"url":row.try_get::<String,_>("logo_url")?}),
        )
    } else {
        None
    };
    Ok((value.into(), logo))
}
pub(super) enum Preparation {
    Ready(Value),
    Refresh(super::vod_api_owned_read::FrozenSnapshot),
}
pub(super) async fn prepare(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    series: bool,
    id: i64,
    row: &PgRow,
    query: &[(String, String)],
    origin: &str,
) -> Result<Preparation, ReadError> {
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
        {
            use super::vod_api_provider_refresh_plan::{Plan, SeriesView};
            let relation_id = relation["relation"]["id"]
                .as_i64()
                .ok_or(ReadError::WorkerFailed)?;
            let force = query
                .iter()
                .rev()
                .find(|(key, _)| key == "force_refresh")
                .is_some_and(|(_, value)| value.to_lowercase() == "true");
            let mut relation = relation;
            let series_view = if series {
                Some(SeriesView {
                    parent,
                    account: relation["account"].take(),
                    external_id: relation["relation"]["external_series_id"]
                        .as_str()
                        .ok_or(ReadError::WorkerFailed)?
                        .to_owned(),
                })
            } else {
                None
            };
            drop(relation);
            drop(logo);
            return Ok(Preparation::Refresh(
                Plan {
                    series,
                    id,
                    relation_id,
                    force,
                    query: query.to_vec(),
                    origin: origin.to_owned(),
                    series_view,
                }
                .freeze(work)?,
            ));
        }
    }
    project(
        work,
        connection,
        Projection {
            series,
            id,
            parent: &parent,
            logo: logo.as_ref(),
            relation: &relation,
            query,
            origin,
        },
    )
    .await
    .map(Preparation::Ready)
}
struct Projection<'a> {
    series: bool,
    id: i64,
    parent: &'a Value,
    logo: Option<&'a Value>,
    relation: &'a Value,
    query: &'a [(String, String)],
    origin: &'a str,
}
async fn project(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    context: Projection<'_>,
) -> Result<Value, ReadError> {
    let Projection {
        series,
        id,
        parent,
        logo,
        relation,
        query,
        origin,
    } = context;
    let mut result = if series {
        dto::series_header(
            parent,
            &relation["relation"],
            &relation["account"],
            relation.get("category").filter(|value| !value.is_null()),
            logo,
            origin,
        )
    } else {
        dto::movie(
            parent,
            &relation["relation"],
            &relation["account"],
            logo,
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
                let projected =
                    dto::episode(&episode, &relation, parent, account_id, origin).map_err(fault)?;
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

#[allow(clippy::too_many_arguments)] // Compatibility wrapper; fair adapter uses prepare then drops its SQL/row scope.
pub(super) async fn read(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    auth: &crate::AuthService,
    series: bool,
    id: i64,
    row: &PgRow,
    query: &[(String, String)],
    origin: &str,
) -> Result<Value, ReadError> {
    let _ = auth;
    match prepare(work, connection, series, id, row, query, origin).await? {
        Preparation::Ready(value) => Ok(value),
        Preparation::Refresh(_snapshot) => Err(ReadError::Unsupported(
            "Refresh execution requires the retained fair phase adapter",
        )),
    }
}

/// Private semantic continuation. Caller must consume it only after the common
/// scheduler admits the refresh, and must suspend SQL/Auth children at each
/// transport header boundary once the common phase API is integrated.
pub(super) async fn execute(
    work: &ReadWork,
    auth: &crate::AuthService,
    snapshot: super::vod_api_owned_read::FrozenSnapshot,
) -> Result<Value, ReadError> {
    use super::{
        vod_api_provider_refresh_actor as refresh, vod_api_provider_refresh_plan::Plan,
        vod_api_provider_refresh_sql as refresh_sql,
    };
    let Plan {
        series,
        id,
        relation_id,
        force,
        query,
        origin,
        series_view,
    } = Plan::consume(work, snapshot)?;
    if series {
        let view = series_view.ok_or(ReadError::WorkerFailed)?;
        let snapshot = json!({"series":view.parent,"account":view.account,"relation":{"external_series_id":view.external_id}});
        refresh::series(work, auth, &snapshot, refresh::Clock { fixed: None }).await?;
        drop(snapshot);
    } else {
        refresh::movie(
            work,
            auth,
            relation_id,
            force,
            refresh::Clock { fixed: None },
        )
        .await?;
    }
    let mut connection = work.connection(auth.store.pool()).await?;
    let result = async {
        let parent = if series {
            refresh_sql::series_by_id(work, &mut connection, id).await?
        } else {
            refresh_sql::movie_by_id(work, &mut connection, id).await?
        }
        .ok_or(ReadError::Unsupported(
            "Concurrent parent deletion after refresh needs Source wire reference",
        ))?;
        let relation = if series {
            refresh_sql::series_relation_by_id(work, &mut connection, relation_id).await?
        } else {
            refresh_sql::movie_relation_by_id(work, &mut connection, relation_id).await?
        }
        .ok_or(ReadError::Unsupported(
            "Concurrent selected relation deletion after refresh needs Source wire reference",
        ))?;
        let art = super::vod_api_provider_artwork::preferred(
            &relation["custom_properties"],
            &parent["custom_properties"],
        );
        let logo = if !super::vod_api_provider_artwork::proxyable(&art.image) {
            if let Some(id) = parent["logo_id"].as_i64() {
                Some(
                    refresh_sql::logo_by_id(work, &mut connection, id)
                        .await?
                        .ok_or(ReadError::Unsupported(
                            "Concurrent logo deletion after refresh needs Source wire reference",
                        ))?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let category = if series {
            if let Some(id) = relation["category_id"].as_i64() {
                refresh_sql::category_by_id(work, &mut connection, id)
                    .await?
                    .ok_or(ReadError::Unsupported(
                        "Concurrent category deletion after refresh needs Source wire reference",
                    ))?
            } else {
                Value::Null
            }
        } else {
            Value::Null
        };
        let account = refresh_sql::account_by_id(
            work,
            &mut connection,
            relation["m3u_account_id"]
                .as_i64()
                .ok_or(ReadError::WorkerFailed)?,
        )
        .await?
        .ok_or(ReadError::Unsupported(
            "Concurrent account deletion after refresh needs Source wire reference",
        ))?;
        let relation = json!({"relation":relation,"account":account,"category":category});
        project(
            work,
            &mut connection,
            Projection {
                series,
                id,
                parent: &parent,
                logo: logo.as_ref(),
                relation: &relation,
                query: &query,
                origin: &origin,
            },
        )
        .await
    }
    .await;
    connection.finish().await?;
    result
}
